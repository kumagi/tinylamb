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
#include <exception>
#include <filesystem>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
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
  if (body.starts_with(kPrefix)) {
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
Status LSMTree::RestoreRuns() {
  struct Discovered {
    size_t generation;
    std::filesystem::path path;
    SortedRun run;
  };
  std::vector<Discovered> runs;
  std::set<size_t> names_seen;  // numeric ids used in file names
  std::set<size_t> gens_seen;   // generations from run headers
  const uint64_t blob_size = blob_->Written();
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(root_dir_, ec)) {
    if (!entry.is_regular_file()) {
      continue;
    }
    const std::string name = entry.path().filename().string();
    if (name == "blob.db") {
      continue;
    }
    if (name.ends_with(".pending")) {
      // A flush/merge that died before its blob payloads were durable.  The
      // bytes it references may be gone; drop it (the mem-tree side of the
      // write was acknowledged only after Sync registered the run).
      LOG(WARN) << "LSM restore: removing unfinished run " << name;
      std::filesystem::remove(entry.path(), ec);
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
    StatusOr<SortedRun> run = SortedRun::Restore(entry.path());
    if (!run.HasValue()) {  // corrupt/incomplete header
      LOG(WARN) << "LSM restore: quarantining unreadable run " << name << ": "
                << run.GetStatus();
      std::filesystem::rename(entry.path(), entry.path().string() + ".bad", ec);
      continue;
    }
    {
      const size_t generation = run.Value().Generation();
      if (!gens_seen.insert(generation).second) {
        LOG(WARN) << "LSM restore: duplicate generation " << generation
                  << " from " << name << "; quarantining";
        std::filesystem::rename(entry.path(), entry.path().string() + ".bad",
                                ec);
        continue;
      }
      runs.push_back(
          {generation, entry.path(), std::move(run.Value())});  // NOLINT
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
  return Status::kSuccess;
}

StatusOr<std::unique_ptr<LSMTree>> LSMTree::Create(
    std::filesystem::path directory_path) {
  std::error_code ec;
  std::filesystem::create_directory(directory_path, ec);
  if (ec) {
    return StatusError(StatusCode::kIOError, "failed to create LSM directory " +
                                                 directory_path.string() +
                                                 ": " + ec.message());
  }
  ASSIGN_OR_RETURN(std::unique_ptr<BlobFile>, blob,
                   BlobFile::Create(BlobPath(directory_path)));
  // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
  auto tree = std::unique_ptr<LSMTree>(
      new LSMTree(std::move(directory_path), std::move(blob)));  // NOLINT
  // D10: restore flushed runs BEFORE the background threads can flush.
  RETURN_IF_FAIL(tree->RestoreRuns());
  // std::thread construction is the one remaining exception source (EAGAIN
  // from pthread_create); translate it at this boundary.
  try {
    tree->flusher_ = std::thread([t = tree.get()]() { Flusher(t); });
    tree->merger_ = std::thread([t = tree.get()]() { Merger(t); });
  } catch (const std::system_error& error) {
    // Do not leak a half-constructed background thread pool.
    tree->stop_ = true;
    if (tree->flusher_.joinable()) {
      tree->flusher_.join();
    }
    return StatusError(
        StatusCode::kRuntimeError,
        "failed to start LSM background workers: " + std::string(error.what()));
  }
  return tree;
}

LSMTree::LSMTree(std::filesystem::path directory_path,
                 std::unique_ptr<BlobFile> blob)
    : every_us_(1000),
      root_dir_(std::move(directory_path)),
      blob_(std::move(blob)) {}

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
  if (const Status status = Sync(); status != Status::kSuccess) {
    LOG(ERROR) << "LSMTree final flush failed: " << status;
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
    // A dead blob writer or ENOSPC here is survivable (same contract as
    // the destructor's final flush): log and retry on the next tick.
    if (const Status status = tree->Sync(); status != Status::kSuccess) {
      LOG(ERROR) << "background flush failed: " << status;
      continue;
    }
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
    if (const Status status = tree->MergeAll(); status != Status::kSuccess) {
      LOG(ERROR) << "background merge failed: " << status;
    }
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
    auto result = it.Find(key, *blob_);
    if (result.GetStatus() == Status::kDeleted) {
      return Status::kNotExists;
    }
    if (result.HasValue()) {
      return result;
    }
  }
  return Status::kNotExists;
}

StatusOr<bool> LSMTree::Contains(std::string_view key) const {
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
    auto result = it.Find(key, *blob_);
    if (result.GetStatus() == Status::kDeleted) {
      return false;
    }
    if (result.HasValue()) {
      return true;
    }
  }
  return false;
}

Status LSMTree::Write(std::string_view key, std::string_view value, bool sync) {
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
    return Sync();
  }
  return Status::kSuccess;
}

Status LSMTree::Delete(std::string_view key, bool flush) {
  {
    std::scoped_lock lk(mem_tree_lock_);
    mem_tree_[std::string(key)] = LSMValue::Delete();
    ++mem_tree_version_;
    mem_tree_cv_.notify_one();
  }
  if (flush) {
    return Sync();
  }
  return Status::kSuccess;
}

Status LSMTree::Sync() {
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
      return Status::kSuccess;
    }
    std::swap(mem_tree_, frozen_mem_tree_);
    to_flush = frozen_mem_tree_;
    new_index_file = root_dir_ / (std::to_string(generation_) + "-" +
                                  std::to_string(blob_->Written()));
    generation = generation_.fetch_add(1);
  }
  // Build under a .pending name: until the blob payloads this run references
  // are durable, the file must not carry a parseable generation-highwater
  // name (see the rename below), or a crash could resurrect it.
  const std::filesystem::path pending_file =
      new_index_file.string() + ".pending";
  if (const Status s =
          SortedRun::Construct(pending_file, to_flush, *blob_, generation);
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
    return s;
  }
  // The blob payloads referenced by the new run must be durable BEFORE the
  // run is registered: the run file itself was already fsynced by
  // FlushInternal, and without this barrier a crash leaves a durable run
  // pointing at torn blob bytes (quarantined on restore: acked writes lost).
  if (const Status blob_sync = blob_->Sync(); blob_sync != Status::kSuccess) {
    // Do not leak the .pending run file on a failed blob sync: each retry
    // would otherwise strand another orphaned file (self-healing only at
    // the next startup's RestoreRuns cleanup).
    std::error_code remove_ec;
    std::filesystem::remove(pending_file, remove_ec);
    LOG(ERROR) << "flushing mem tree failed to sync blob: " << blob_sync;
    std::scoped_lock lk(mem_tree_lock_);
    mem_tree_.merge(frozen_mem_tree_);
    frozen_mem_tree_.clear();
    return blob_sync;
  }
  // The name's blob high-water must describe bytes that are durable: the
  // pre-append Written() would let a run whose blob bytes were lost pass the
  // RestoreRuns quarantine check.  After blob_.Sync(), CommittedLSN covers
  // every payload this run references.
  {
    const std::filesystem::path durable_name =
        root_dir_ /
        (std::to_string(generation) + "-" + std::to_string(blob_->Written()));
    std::error_code ec;
    std::filesystem::rename(pending_file, durable_name, ec);
    if (ec) {
      LOG(ERROR) << "flushing mem tree failed to rename run: " << ec.message();
      std::filesystem::remove(pending_file, ec);
      std::scoped_lock lk(mem_tree_lock_);
      mem_tree_.merge(frozen_mem_tree_);
      frozen_mem_tree_.clear();
      return StatusError(StatusCode::kIOError,
                         "failed to rename flushed run: " + ec.message());
    }
    new_index_file = durable_name;
  }
  {
    // Register the new run BEFORE dropping the frozen tree: readers must
    // always find flushed keys in mem_tree_, frozen_mem_tree_ or index_.
    // Clearing first would open a window where a concurrent Read() misses
    // the data entirely (kNotExists). Both locks are taken in the canonical
    // mem_tree_lock_ -> file_tree_lock_ order.
    std::scoped_lock lk(mem_tree_lock_, file_tree_lock_);
    StatusOr<SortedRun> run = SortedRun::Restore(new_index_file);
    if (!run.HasValue()) {
      // A durable but unregistered run file would never be re-read until the
      // next restart, and skipping the merge-back below would let the next
      // Sync() discard the frozen snapshot (acked writes lost). Roll the
      // frozen tree back and drop the orphan file instead; the flush retries.
      std::error_code remove_ec;
      std::filesystem::remove(new_index_file, remove_ec);
      LOG(ERROR) << "flushing mem tree failed to restore run: "
                 << run.GetStatus();
      mem_tree_.merge(frozen_mem_tree_);
      frozen_mem_tree_.clear();
      return run.GetStatus();
    }
    files_.push_front(new_index_file);
    index_.push_front(run.MoveValue());
    frozen_mem_tree_.clear();
  }
  return Status::kSuccess;
}

Status LSMTree::MergeAll() {
  constexpr size_t kMaxRuns = 4;
  // Single atomic acquisition in the canonical mem_tree_lock_ ->
  // file_tree_lock_ order (same as Sync): two-step locking here would admit
  // a future file->mem path and an ABBA deadlock.
  std::scoped_lock lk(mem_tree_lock_, file_tree_lock_);
  if (index_.size() <= kMaxRuns) {
    return Status::kSuccess;
  }
  // Copy the merge inputs first and only mutate the deques after the merged
  // file is durable: an exception mid-merge must not orphan the source runs.
  const SortedRun older = index_.back();
  const std::filesystem::path older_file = files_.back();
  const SortedRun newer = index_[index_.size() - 2];
  const std::filesystem::path newer_file = files_[files_.size() - 2];

  const std::vector<SortedRun> merge_inputs{older, newer};
  LSMView view(*blob_, merge_inputs);
  // The merged run must NOT take a fresh generation: its payload is older
  // than every run flushed after these inputs, and a fresh number would let
  // it shadow newer tombstones (deleted keys resurface in scans). It instead
  // inherits the largest input generation, whose slot the inputs vacate.
  const size_t merged_generation =
      std::max(older.Generation(), newer.Generation());
  const size_t file_generation = generation_.fetch_add(1);
  // Built under .pending; renamed to the durable high-water name only after
  // blob_.Sync() below (see LSMTree::Sync for the full rationale).
  std::filesystem::path path =
      root_dir_ / ("merged-" + std::to_string(file_generation) + "-" +
                   std::to_string(blob_->Written()) + ".pending");
  std::vector<SortedRun::Entry> merged;
  if (view.Size() != 0) {
    std::string min_key;
    std::string max_key;
    ASSIGN_OR_RETURN(LSMView::Iterator, it, view.Begin());
    for (; it.IsValid(); ++it) {
      RETURN_IF_FAIL(it.GetStatus());
      if (merged.empty()) {
        ASSIGN_OR_RETURN(std::string, key, it.Key());
        min_key = std::move(key);
      }
      ASSIGN_OR_RETURN(SortedRun::Entry, entry, it.GetEntry());
      merged.push_back(entry);
      ASSIGN_OR_RETURN(std::string, key, it.Key());
      max_key = std::move(key);
    }
    RETURN_IF_FAIL(it.GetStatus());
    if (const Status s = SortedRun::FlushInternal(path, min_key, max_key,
                                                  merged, merged_generation);
        s != Status::kSuccess) {
      // Sources stay registered; the merge is retried on a later tick.
      LOG(ERROR) << "merge failed: " << s;
      return s;
    }
  }

  if (!merged.empty()) {
    // The merged run references every input's blob bytes; make them durable
    // and stamp the final name with the post-Sync high-water before the
    // inputs are removed.
    if (const Status blob_sync = blob_->Sync(); blob_sync != Status::kSuccess) {
      // Remove the freshly written .pending run instead of leaking it (the
      // inputs stay registered, so the merge simply retries later).
      std::error_code remove_ec;
      std::filesystem::remove(path, remove_ec);
      LOG(ERROR) << "merge failed to sync blob: " << blob_sync;
      return blob_sync;
    }
    const std::filesystem::path durable_name =
        root_dir_ / ("merged-" + std::to_string(file_generation) + "-" +
                     std::to_string(blob_->Written()));
    std::error_code ec;
    std::filesystem::rename(path, durable_name, ec);
    if (ec) {
      LOG(ERROR) << "merge failed to rename run: " << ec.message();
      std::filesystem::remove(path, ec);
      return StatusError(StatusCode::kIOError,
                         "failed to rename merged run: " + ec.message());
    }
    path = durable_name;
  }
  files_.pop_back();
  files_.pop_back();
  index_.pop_back();
  index_.pop_back();
  // The inputs were already un-registered above, so a throwing remove here
  // would both terminate the merger thread and leak the files; degrade to a
  // logged error instead (the merged run supersedes their key ranges, and
  // RestoreRuns quarantines anything malformed on the next start).
  if (!merged.empty()) {
    // Open the merged run while the inputs are still registered: if the
    // Restore fails (fd pressure, malformed write), removing the inputs
    // first would leave their keys reachable only through the un-registered
    // merged file. Leaving the inputs in place lets the merge simply retry.
    ASSIGN_OR_RETURN(SortedRun, run, SortedRun::Restore(path));
    index_.push_back(std::move(run));
    files_.push_back(std::move(path));
  }
  std::error_code remove_ec;
  std::filesystem::remove(older_file, remove_ec);
  if (remove_ec) {
    LOG(ERROR) << "merge failed to remove " << older_file << ": "
               << remove_ec.message();
  }
  std::filesystem::remove(newer_file, remove_ec);
  if (remove_ec) {
    LOG(ERROR) << "merge failed to remove " << newer_file << ": "
               << remove_ec.message();
  }
  return Status::kSuccess;
}
}  // namespace tinylamb
