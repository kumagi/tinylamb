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

#include <bits/types/struct_iovec.h>
#include <bits/xopen_lim.h>
#include <endian.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/status_or.hpp"
#include "index/lsm_detail/lsm_view.hpp"
#include "index/lsm_detail/offset_index.hpp"
#include "lsm_detail/sorted_run.hpp"

namespace tinylamb {

void Flusher(const std::stop_token& st, LSMTree* tree);

std::filesystem::path BlobPath(const std::filesystem::path& dir) {
  return dir / "blob.db";
}

std::filesystem::path IndexPath(const std::filesystem::path& dir, int count) {
  return dir / std::string("index-" + std::to_string(count) + ".db");
}

std::filesystem::path MergedPath(const std::filesystem::path& dir, int count) {
  return dir / std::string("merged-" + std::to_string(count) + ".db");
}

int OpenIndexFile(const std::filesystem::path& dir, int count) {
  return ::open(IndexPath(dir, count).c_str(), O_RDWR | O_CREAT, 0666);
}

int OpenMergeFile(const std::filesystem::path& dir, int count) {
  return ::open(MergedPath(dir, count).c_str(), O_RDWR | O_CREAT, 0666);
}

LSMTree::LSMTree(std::string_view directory_path)
    : every_us_(10), root_dir_(directory_path), blob_(BlobPath(root_dir_)) {
  std::filesystem::path dir(root_dir_);
  std::filesystem::create_directory(dir);
  flusher_ = std::jthread(Flusher, this);
  // merger_ = std::jthread(Merger, this);
}

LSMTree::~LSMTree() {
  flusher_.join();
  // merger_.join();
}

void Flusher(const std::stop_token& st, LSMTree* tree) {
  for (;;) {
    std::unique_lock lk(tree->mem_tree_lock_, std::defer_lock);

    if (!lk.try_lock_for(std::chrono::microseconds(tree->every_us_))) {
      continue;
    }

    if (tree->mem_tree_.empty()) {
      if (!st.stop_requested()) {
        break;
      }
      lk.unlock();
      std::this_thread::sleep_for(std::chrono::microseconds(tree->every_us_));
      continue;
    }

    assert(tree->frozen_mem_tree_.empty());
    std::swap(tree->mem_tree_, tree->frozen_mem_tree_);
    SortedRun latest_run(tree->frozen_mem_tree_, tree->blob_,
                         tree->blob_.Written());

    std::filesystem::path new_index_file =
        tree->root_dir_ / std::to_string(tree->blob_.Written());
    auto flushed = latest_run.Flush(new_index_file);
    if (flushed.GetStatus() != Status::kSuccess) {
      exit(1);
    }
    tree->frozen_mem_tree_.clear();

    std::unique_lock file_lk(tree->file_tree_lock_);
    lk.unlock();
    tree->files_.push_back(new_index_file);
    tree->index_.push_back(latest_run);
  }
}

StatusOr<std::string> LSMTree::Read(std::string_view key) const {
  {
    std::scoped_lock lk(mem_tree_lock_);
    auto iter = mem_tree_.find(std::string(key));
    if (iter != mem_tree_.end()) {
      return iter->second.payload;
    }
  }

  std::unique_lock file_lk(file_tree_lock_);
  for (auto it = index_.rbegin(); it != index_.rend(); ++it) {
    auto result = it->Find(key, blob_);
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
    if (iter != mem_tree_.end()) {
      return true;
    }
  }

  std::unique_lock file_lk(file_tree_lock_);
  for (auto it = index_.rbegin(); it != index_.rend(); ++it) {
    auto result = it->Find(key, blob_);
    if (result.HasValue()) {
      return true;
    }
  }
  return false;
}

void LSMTree::Write(std::string_view key, std::string_view value, bool sync) {
  std::scoped_lock lk(mem_tree_lock_);
  mem_tree_.emplace(key, LSMValue{false, std::string(value)});
}

void LSMTree::Sync() {
  std::unique_lock lk(mem_tree_lock_, std::defer_lock);
  if (mem_tree_.empty()) {
    return;
  }
  std::swap(mem_tree_, frozen_mem_tree_);
  SortedRun latest_run(frozen_mem_tree_, blob_, blob_.Written());

  std::filesystem::path new_index_file =
      root_dir_ / std::to_string(blob_.Written());
  auto flushed = latest_run.Flush(new_index_file);
  if (flushed.GetStatus() != Status::kSuccess) {
    exit(1);
  }
  frozen_mem_tree_.clear();

  std::unique_lock file_lk(file_tree_lock_);
  files_.push_back(new_index_file);
  index_.push_back(latest_run);
}

void LSMTree::MergeAll() {
  LSMView view = GetView();
  SortedRun run = view.CreateSingleRun();
  std::scoped_lock lk(file_tree_lock_);
  for (const auto& file : files_) {
    std::filesystem::remove(file);
  }
  index_.clear();
  files_.clear();
  run.Flush(root_dir_ / std::to_string(blob_.Written()));
}

}  // namespace tinylamb