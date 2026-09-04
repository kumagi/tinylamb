/**
 * Copyright 2024 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "index/lsm_tree.hpp"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/status_or.hpp"
#include "index/lsm_detail/lsm_view.hpp"
#include "lsm_detail/sorted_run.hpp"

namespace tinylamb {

namespace {
std::filesystem::path BlobPath(const std::filesystem::path& dir) {
  return dir / "blob.db";
}

// D10 (docs/design.md): run files are named "<id>-<blob-high-water>" for
// flushed runs and "merged-<id>-<blob-high-water>" for merged runs.  The
// numeric id in a merged file name is a FILE id, not the inherited
// generation (that lives in the run header), so generation numbers are only
// ever read from the header; the name is validated for the restore scan and
// for picking a collision-free next id.
bool ParseRunFileName(const std::string& name, bool* is_merged,
                      unsigned long long* id,
                      unsigned long long* blob_high_water) {
  std::string body = name;
  *is_merged = false;
  constexpr std::string_view kPrefix = "merged-";
  if (body.rfind(kPrefix, 0) == 0) {
    *is_merged = true;
    body.erase(0, kPrefix.size());
  }
  const size_t dash = body.find('-');
  if (dash == std::string::npos || dash == 0 || dash + 1 >= body.size() ||
      body.find('-', dash + 1) != std::string::npos) {
    return false;
  }
  const std::string first = body.substr(0, dash);
  const std::string second = body.substr(dash + 1);
  const auto all_digits = [](const std::string& s) {
    return std::ranges::all_of(s, [](char c) {
      return std::isdigit(static_cast<unsigned char>(c)) != 0;
    });
  };
  if (!all_digits(first) || !all_digits(second)) {
    return false;
  }
  try {
    *id = std::stoull(first);
    *blob_high_water = std::stoull(second);
  } catch (const std::exception&) {
    return false;
  }
  return true;
}
}  // namespace

// D10 (docs/design.md): a restarted LSMTree reopens flushed data.  There is
// no MANIFEST: the directory itself is the source of truth.  Every entry is
// classified explicitly (valid run / bad name / corrupt or incomplete file /
// duplicate generation); nothing is silently ignored, and only valid runs
// are restored.  Runs are ordered by the numeric generation from the run
// HEADER (newest first), never by lexicographic file-name order.
void LSMTree::RestoreRuns() {
  struct Discovered {
    size_t generation;
    std::filesystem::path path;
    SortedRun run;
  };
  std::vector<Discovered> runs;
  std::set<size_t> names_seen;  // numeric ids used in file names
  std::set<size_t> gens_seen;   // generations from run headers
  const uint64_t blob_size = blob_.Written();
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(root_dir_, ec)) {
    if (!entry.is_regular_file()) {
      continue;
    }
    const std::string name = entry.path().filename().string();
    if (name == "blob.db") {
      continue;
    }
    bool is_merged = false;
    unsigned long long id = 0;
    unsigned long long blob_high_water = 0;
    if (!ParseRunFileName(name, &is_merged, &id, &blob_high_water)) {
      // Unparsable name: quarantine it instead of serving an unknown file.
      LOG(WARN) << "LSM restore: rejecting malformed run file " << name;
      std::filesystem::rename(entry.path(), entry.path().string() + ".bad", ec);
      continue;
    }
    names_seen.insert(static_cast<size_t>(id));
    if (blob_high_water > blob_size) {
      // The name references blob bytes that never reached disk: the run was
      // written by a crashed flush.  Not promotable to a valid run.
      LOG(WARN) << "LSM restore: quarantining run " << name
                << " (blob high water " << blob_high_water << " > " << blob_size
                << ")";
      std::filesystem::rename(entry.path(), entry.path().string() + ".bad", ec);
      continue;
    }
    try {
      SortedRun run(entry.path());  // throws on corrupt/incomplete header
      const size_t generation = run.Generation();
      if (!gens_seen.insert(generation).second) {
        LOG(WARN) << "LSM restore: duplicate generation " << generation
                  << " from " << name << "; quarantining";
        std::filesystem::rename(entry.path(), entry.path().string() + ".bad",
                                ec);
        continue;
      }
      runs.push_back({generation, entry.path(), std::move(run)});
    } catch (const std::exception& error) {
      LOG(WARN) << "LSM restore: quarantining unreadable run " << name << ": "
                << error.what();
      std::filesystem::rename(entry.path(), entry.path().string() + ".bad", ec);
    }
  }
  // Newest generation first: Read/Contains scan index_ front-to-back and
  // stop at the first hit, so ordering IS the visibility rule.
  std::ranges::sort(runs, [](const Discovered& a, const Discovered& b) {
    return a.generation > b.generation;
  });
  size_t high_water = 0;
  for (Discovered& run : runs) {
    files_.push_back(run.path);
    index_.push_back(std::move(run.run));
    high_water = std::max(high_water, run.generation + 1);
  }
  for (const size_t id : names_seen) {
    // Never reuse an id that ever appeared in a file name: a leftover
    // ".bad"-quarantined id must not collide with a fresh run either.
    high_water = std::max(high_water, id + 1);
  }
  generation_.store(high_water, std::memory_order_relaxed);
  if (!runs.empty()) {
    LOG(INFO) << "LSM restore: reopened " << runs.size()
              << " run(s), next generation " << high_water;
  }
}

LSMTree::LSMTree(std::filesystem::path directory_path)
    : every_us_(1000),
      root_dir_(std::move(directory_path)),
      blob_(BlobPath(root_dir_)) {
  std::filesystem::create_directory(root_dir_);
  // D10: restore flushed runs BEFORE the background threads can flush.
  RestoreRuns();
  try {
    flusher_ = std::thread([&]() { Flusher(this); });
    merger_ = std::thread([&]() { Merger(this); });
  } catch (...) {
    // Do not leak a half-constructed background thread pool.
    stop_ = true;
    if (flusher_.joinable()) {
      flusher_.join();
    }
    throw;
  }
}

LSMTree::~LSMTree() {
  stop_ = true;
  {
    std::scoped_lock lk(mem_tree_lock_);
    mem_tree_cv_.notify_all();
  }
  if (flusher_.joinable()) {
    flusher_.join();
  }
  if (merger_.joinable()) {
    merger_.join();
  }
  // Final durability barrier: a well-behaved close flushes whatever the
  // periodic flusher had not picked up yet, so a clean stop never drops
  // writes that a Read() had already acknowledged.
  try {
    Sync();
  } catch (const std::exception& e) {
    LOG(ERROR) << "LSMTree final flush failed: " << e.what();
  }
}

void Flusher(LSMTree* tree) {
  uint64_t flushed_version = 0;
  for (;;) {
    uint64_t target = 0;
    {
      // Wait for either a mem_tree_ mutation or the periodic tick; an idle
      // tree never reaches Sync() and therefore never takes this mutex on
      // the write path's behalf.
      std::unique_lock lk(tree->mem_tree_lock_);
      tree->mem_tree_cv_.wait_for(
          lk, std::chrono::microseconds(tree->every_us_), [&] {
            return tree->stop_.load(std::memory_order_relaxed) ||
                   flushed_version != tree->mem_tree_version_;
          });
      if (tree->stop_.load(std::memory_order_relaxed)) {
        break;
      }
      target = tree->mem_tree_version_;
    }
    if (target == flushed_version) {
      continue;
    }
    tree->Sync();
    // Record only the version observed before Sync(): writes that raced the
    // flush stay pending above `target` and trigger the next round.
    flushed_version = target;
  }
}

void Merger(LSMTree* tree) {
  for (;;) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    if (tree->stop_.load()) {
      break;
    }
    tree->MergeAll();
    LOG(TRACE) << "Merged";
  }
}

StatusOr<std::string> LSMTree::Read(std::string_view key) const {
  {
    std::scoped_lock lk(mem_tree_lock_);
    auto iter = mem_tree_.find(std::string(key));
    if (iter != mem_tree_.end()) {
      if (iter->second.is_delete) {
        return Status::kNotExists;
      }
      return iter->second.payload;
    }
    auto fiter = frozen_mem_tree_.find(std::string(key));
    if (fiter != frozen_mem_tree_.end()) {
      if (fiter->second.is_delete) {
        return Status::kNotExists;
      }
      return fiter->second.payload;
    }
  }

  std::unique_lock file_lk(file_tree_lock_);
  for (const auto& it : index_) {
    auto result = it.Find(key, blob_);
    if (result.GetStatus() == Status::kDeleted) {
      return Status::kNotExists;
    }
    if (result.HasValue()) {
      return result;
    }
  }
  return Status::kNotExists;
}

bool LSMTree::Contains(std::string_view key) const {
  {
    std::scoped_lock lk(mem_tree_lock_);
    auto iter = mem_tree_.find(std::string(key));
    if (iter != mem_tree_.end() && iter->second.is_delete) {
      return false;
    }
    if (iter != mem_tree_.end()) {
      return true;
    }
    auto fiter = frozen_mem_tree_.find(std::string(key));
    if (fiter != frozen_mem_tree_.end() && fiter->second.is_delete) {
      return false;
    }
    if (fiter != frozen_mem_tree_.end()) {
      return true;
    }
  }

  std::unique_lock file_lk(file_tree_lock_);
  for (const auto& it : index_) {
    auto result = it.Find(key, blob_);
    if (result.GetStatus() == Status::kDeleted) {
      return false;
    }
    if (result.HasValue()) {
      return true;
    }
  }
  return false;
}

void LSMTree::Write(std::string_view key, std::string_view value, bool sync) {
  {
    std::scoped_lock lk(mem_tree_lock_);
    mem_tree_[std::string(key)] = LSMValue(std::string(value));
    ++mem_tree_version_;
    mem_tree_cv_.notify_one();
  }
  // Flush after releasing mem_tree_lock_: Sync() re-acquires it (a
  // non-recursive mutex), so calling it while holding the lock deadlocked
  // the calling thread.
  if (sync) {
    Sync();
  }
}

void LSMTree::Delete(std::string_view key, bool flush) {
  {
    std::scoped_lock lk(mem_tree_lock_);
    mem_tree_[std::string(key)] = LSMValue::Delete();
    ++mem_tree_version_;
    mem_tree_cv_.notify_one();
  }
  if (flush) {
    Sync();
  }
}

void LSMTree::Sync() {
  // One flush at a time: snapshots must reach disk in mem_tree_ mutation
  // order or a newer run can shadow an older tombstone (deleted-key
  // resurrection) and identical runs get flushed twice.
  std::scoped_lock flush_lk(sync_lock_);
  std::map<std::string, LSMValue> to_flush;
  std::filesystem::path new_index_file;
  size_t generation = 0;
  {
    std::unique_lock lk(mem_tree_lock_);
    if (mem_tree_.empty()) {
      return;
    }
    std::swap(mem_tree_, frozen_mem_tree_);
    to_flush = frozen_mem_tree_;
    new_index_file = root_dir_ / (std::to_string(generation_) + "-" +
                                  std::to_string(blob_.Written()));
    generation = generation_.fetch_add(1);
  }
  if (const Status s =
          SortedRun::Construct(new_index_file, to_flush, blob_, generation);
      s != Status::kSuccess) {
    // Merge the frozen snapshot back into mem_tree_ so writes made while the
    // flush was failing stay newer than the failed snapshot on re-flush.
    // Leaving frozen_mem_tree_ populated made the next Sync() re-swap the
    // OLD snapshot back into mem_tree_, flushing newer data first and then
    // re-issuing the stale values as a newer generation (stale reads /
    // deleted-key resurrection).
    LOG(ERROR) << "flushing mem tree failed: " << s;
    std::scoped_lock lk(mem_tree_lock_);
    mem_tree_.merge(frozen_mem_tree_);
    frozen_mem_tree_.clear();
    return;
  }
  {
    // Register the new run BEFORE dropping the frozen tree: readers must
    // always find flushed keys in mem_tree_, frozen_mem_tree_ or index_.
    // Clearing first would open a window where a concurrent Read() misses
    // the data entirely (kNotExists). Both locks are taken in the canonical
    // mem_tree_lock_ -> file_tree_lock_ order.
    std::scoped_lock lk(mem_tree_lock_, file_tree_lock_);
    files_.push_front(new_index_file);
    index_.emplace_front(new_index_file);
    frozen_mem_tree_.clear();
  }
}

void LSMTree::MergeAll() {
  constexpr size_t kMaxRuns = 4;
  std::scoped_lock mem_lk(mem_tree_lock_);
  std::scoped_lock lk(file_tree_lock_);
  if (index_.size() <= kMaxRuns) {
    return;
  }
  // Copy the merge inputs first and only mutate the deques after the merged
  // file is durable: an exception mid-merge must not orphan the source runs.
  const SortedRun older = index_.back();
  const std::filesystem::path older_file = files_.back();
  const SortedRun newer = index_[index_.size() - 2];
  const std::filesystem::path newer_file = files_[files_.size() - 2];

  const std::vector<SortedRun> merge_inputs{older, newer};
  LSMView view(blob_, merge_inputs);
  // The merged run must NOT take a fresh generation: its payload is older
  // than every run flushed after these inputs, and a fresh number would let
  // it shadow newer tombstones (deleted keys resurface in scans). It instead
  // inherits the largest input generation, whose slot the inputs vacate.
  const size_t merged_generation =
      std::max(older.Generation(), newer.Generation());
  const size_t file_generation = generation_.fetch_add(1);
  std::filesystem::path path =
      root_dir_ / ("merged-" + std::to_string(file_generation) + "-" +
                   std::to_string(blob_.Written()));
  std::vector<SortedRun::Entry> merged;
  if (view.Size() != 0) {
    std::string min_key;
    std::string max_key;
    for (LSMView::Iterator it = view.Begin(); it.IsValid(); ++it) {
      if (merged.empty()) {
        min_key = it.Key();
      }
      merged.push_back(it.TopIterator().GetEntry());
      max_key = it.Key();
    }
    if (const Status s = SortedRun::FlushInternal(path, min_key, max_key,
                                                  merged, merged_generation);
        s != Status::kSuccess) {
      // Sources stay registered; the merge is retried on a later tick.
      LOG(ERROR) << "merge failed: " << s;
      return;
    }
  }

  files_.pop_back();
  files_.pop_back();
  index_.pop_back();
  index_.pop_back();
  std::filesystem::remove(older_file);
  std::filesystem::remove(newer_file);
  if (!merged.empty()) {
    index_.emplace_back(path);
    files_.push_back(std::move(path));
  }
}
}  // namespace tinylamb
