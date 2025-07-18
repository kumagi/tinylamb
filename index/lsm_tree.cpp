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

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/status_or.hpp"
#include "index/lsm_detail/lsm_view.hpp"
#include "lsm_detail/sorted_run.hpp"

namespace tinylamb {

std::filesystem::path BlobPath(const std::filesystem::path& dir) {
  return dir / "blob.db";
}

LSMTree::LSMTree(std::filesystem::path directory_path)
    : every_us_(1000),
      root_dir_(std::move(directory_path)),
      blob_(BlobPath(root_dir_)) {
  std::filesystem::create_directory(root_dir_);
  flusher_ = std::thread([&](){Flusher(this);});
  merger_ = std::thread([&](){Merger(this);});
}

LSMTree::~LSMTree() {
  stop_ = true;
  flusher_.join();
  merger_.join();
}

void Flusher(LSMTree* tree) {
  for (;;) {
    std::this_thread::sleep_for(std::chrono::microseconds(tree->every_us_));
    if (tree->stop_.load()) {
      break;
    }
    tree->Sync();
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
    if (fiter != mem_tree_.end() && fiter->second.is_delete) {
      return false;
    }
    if (fiter != frozen_mem_tree_.end()) {
      return true;
    }
  }

  std::unique_lock file_lk(file_tree_lock_);
  for (const auto& it : index_) {
    auto result = it.Find(key, blob_);
    if (result.GetStatus() == Status::kSuccess) {
      return true;
    }
  }
  return false;
}

void LSMTree::Write(std::string_view key, std::string_view value, bool sync) {
  std::scoped_lock lk(mem_tree_lock_);
  mem_tree_[std::string(key)] = LSMValue(std::string(value));
  if (sync) {
    Sync();
  }
}

void LSMTree::Delete(std::string_view key, bool flush) {
  std::scoped_lock lk(mem_tree_lock_);
  mem_tree_[std::string(key)] = LSMValue::Delete();
  if (flush) {
    Sync();
  }
}

void LSMTree::Sync() {
  std::unique_lock lk(mem_tree_lock_);
  if (mem_tree_.empty()) {
    return;
  }
  std::swap(mem_tree_, frozen_mem_tree_);
  std::filesystem::path new_index_file =
      root_dir_ /
      (std::to_string(generation_) + "-" + std::to_string(blob_.Written()));
  SortedRun::Construct(new_index_file, frozen_mem_tree_, blob_,
                       generation_.fetch_add(1));
  frozen_mem_tree_.clear();

  std::unique_lock file_lk(file_tree_lock_);
  files_.push_front(new_index_file);
  index_.emplace_front(new_index_file);
}

void LSMTree::MergeAll() {
  std::scoped_lock lk(file_tree_lock_);
  LSMView view = GetViewImpl();
  std::filesystem::path path = root_dir_ / std::to_string(blob_.Written());
  view.CreateSingleRun(path);
  for (const auto& file : files_) {
    std::filesystem::remove(file);
  }
  index_.clear();
  files_.clear();
  index_.emplace_front(path);
  files_.push_front(std::move(path));
}
}  // namespace tinylamb
