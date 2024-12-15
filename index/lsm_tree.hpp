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

#ifndef TINYLAMB_LSM_TREE_HPP
#define TINYLAMB_LSM_TREE_HPP

#include <atomic>
#include <climits>
#include <cstddef>
#include <deque>
#include <filesystem>
#include <map>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>

#include "common/status_or.hpp"
#include "index/lsm_detail/blob_file.hpp"
#include "index/lsm_detail/lsm_view.hpp"
#include "lsm_detail/sorted_run.hpp"

namespace tinylamb {
class LSMTree;
void Flusher(const std::stop_token& st, LSMTree* tree);
void Merger(const std::stop_token& st, LSMTree* tree);

class LSMTree final {
 public:
  LSMTree(std::filesystem::path directory_path);
  ~LSMTree();

  // Neither movable nor copyable.
  LSMTree(const LSMTree&) = delete;
  LSMTree& operator=(const LSMTree&) = delete;
  LSMTree(LSMTree&&) = delete;
  LSMTree& operator=(LSMTree&&) = delete;

  StatusOr<std::string> Read(std::string_view key) const;
  bool Contains(std::string_view key) const;
  void Write(std::string_view key, std::string_view value, bool flush = false);
  void Delete(std::string_view key, bool flush = false);
  void Sync();

  LSMView GetView() const {
    std::scoped_lock lk(file_tree_lock_);
    return GetViewImpl();
  }

  void MergeAll();

 private:
  friend void Flusher(const std::stop_token& st, LSMTree* tree);
  LSMView GetViewImpl() const { return {blob_, index_}; }

  struct FileAndIndex {
    std::filesystem::path filepath;
    SortedRun index;
  };

  int every_us_;
  std::filesystem::path root_dir_;
  std::map<std::string, LSMValue> mem_tree_;
  std::map<std::string, LSMValue> frozen_mem_tree_;
  std::atomic<size_t> generation_{0};

  BlobFile blob_;

  std::jthread flusher_;
  std::jthread merger_;

  mutable std::timed_mutex mem_tree_lock_;
  mutable std::timed_mutex file_tree_lock_;

  std::deque<std::filesystem::path> files_;
  std::deque<SortedRun> index_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LSM_TREE_HPP
