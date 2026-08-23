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
#include <condition_variable>
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
void Flusher(LSMTree* tree);
void Merger(LSMTree* tree);

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
  void Write(std::string_view key, std::string_view value, bool sync = false);
  void Delete(std::string_view key, bool flush = false);
  void Sync();

  LSMView GetView() const {
    std::scoped_lock lk(file_tree_lock_);
    return GetViewImpl();
  }

  void MergeAll();

  friend std::ostream& operator<<(std::ostream& o, const LSMTree& t) {
    o << "LSMTree(dir=" << t.root_dir_ << ", generation="
      << t.generation_.load(std::memory_order_relaxed) << ", blob=" << t.blob_;
    {
      std::scoped_lock lk(t.mem_tree_lock_);
      o << ", mem_tree=" << t.mem_tree_.size()
        << ", frozen=" << t.frozen_mem_tree_.size();
    }
    {
      std::scoped_lock lk(t.file_tree_lock_);
      o << ", files=" << t.files_.size() << ", runs=" << t.index_.size();
    }
    o << ")";
    return o;
  }

 private:
  friend void Flusher(LSMTree* tree);
  friend void Merger(LSMTree* tree);
  LSMView GetViewImpl() const { return {blob_, index_}; }

  struct FileAndIndex {
    std::filesystem::path filepath;
    SortedRun index;

    friend std::ostream& operator<<(std::ostream& o,
                                    const FileAndIndex& fi) {
      o << "FileAndIndex(" << fi.filepath << ": " << fi.index << ")";
      return o;
    }
  };

  int every_us_;
  std::filesystem::path root_dir_;
  std::map<std::string, LSMValue> mem_tree_;
  std::map<std::string, LSMValue> frozen_mem_tree_;
  std::atomic<size_t> generation_{0};
  // Bumped under mem_tree_lock_ on every mem_tree_ mutation so the Flusher
  // can skip empty Sync() attempts without taking the mutex.
  uint64_t mem_tree_version_{0};
  std::condition_variable_any mem_tree_cv_;

  BlobFile blob_;

  std::atomic<bool> stop_{false};
  // Serializes whole Sync() bodies (swap -> construct -> register). Two
  // overlapping Sync() calls used to re-install the first snapshot from
  // frozen_mem_tree_ back into mem_tree_, flushing identical runs twice;
  // worse, a skipped or reordered flush could write a key's value to a run
  // NEWER than the run holding its tombstone, resurrecting deleted keys.
  mutable std::mutex sync_lock_;
  std::thread flusher_;
  std::thread merger_;

  // Lock ordering contract: when both are held, acquire mem_tree_lock_
  // BEFORE file_tree_lock_ (MergeAll and Sync follow this order). Read/
  // Contains take them one at a time and never nest.
  mutable std::timed_mutex mem_tree_lock_;
  mutable std::timed_mutex file_tree_lock_;

  std::deque<std::filesystem::path> files_;
  std::deque<SortedRun> index_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LSM_TREE_HPP
