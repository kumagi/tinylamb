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

#include "lsm_view.hpp"

#include <endian.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <filesystem>
#include <initializer_list>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "blob_file.hpp"
#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/status_or.hpp"
#include "index/lsm_detail/sorted_run.hpp"

namespace tinylamb {

LSMView::Iterator LSMView::Begin() const { return {this, true}; }

StatusOr<std::string> LSMView::Find(std::string_view key) const {
  for (const auto& idx : indexes_) {
    auto result = idx.Find(key, blob_);
    if (result.GetStatus() == Status::kDeleted) {
      return Status::kNotExists;
    }
    if (result.HasValue()) {
      return result;
    }
  }
  return Status::kNotExists;
}

size_t LSMView::Size() const {
  size_t sum = 0;
  for (const auto& idx : indexes_) {
    sum += idx.Size();
  }
  return sum;
}

void LSMView::CreateSingleRun(const std::filesystem::path& path) const {
  std::vector<SortedRun::Entry> merged;
  merged.reserve(Size());
  Iterator it = Begin();
  std::string min_key = it.Key();
  std::string max_key;
  size_t max_generation = 0;
  for (const auto& run : indexes_) {
    max_generation = std::max(max_generation, run.Generation());
  }

  size_t pos = 0;
  while (it.IsValid()) {
    merged.push_back(it.TopIterator().Entry());
    if (++pos == merged.size()) {
      max_key = it.Key();
    }
    ++it;
  }
  SortedRun::FlushInternal(path, min_key, max_key, merged, max_generation + 1);
}

LSMView::Iterator::Iterator(const LSMView* vm, bool head) : vm_(vm) {
  if (head) {
    iters_.reserve(vm_->indexes_.size());
    for (const SortedRun& run : vm_->indexes_) {
      SortedRun::Iterator iter = run.Begin(vm->blob_);
      if (!iter.IsValid()) {
        continue;
      }
      iters_.emplace_back(iter);
      size_t curr = iters_.size() - 1;
      if (curr == 0) {
        continue;
      }
      while (0 < curr) {
        int result = iters_[curr].Compare(iters_[curr / 2]);
        if (result == 0) {
          if (iters_[curr].Generation() < iters_[curr / 2].Generation()) {
            // Bigger generation has priority.
            result--;
            ++iters_[curr];
          } else {
            result++;
            ++iters_[curr / 2];
          }
        }
        if (0 < result) {
          // curr/2 has smaller value.
          std::swap(iters_[curr], iters_[curr / 2]);
          curr /= 2;
        } else {
          break;
        }
      }
    }
    remaining_iters_ = iters_.size();

    if (iters_[0].IsDeleted()) {
      std::string deleted_key;
      do {
        deleted_key = iters_[0].Key();
        ++*this;
      } while (IsValid() &&
               (deleted_key == iters_[0].Key() || iters_[0].IsDeleted()));
    }
  }
}

std::string LSMView::Iterator::Value() const { return iters_[0].Value(); }

SortedRun::Entry LSMView::Iterator::Entry() const { return iters_[0].Entry(); }

// Treats invalid iterator as infinity big.
bool IsRightIteratorBigger(int left, int right,
                           const std::vector<SortedRun::Iterator>& iters) {
  if (!iters[left].IsValid()) {
    return false;
  }
  if (!iters[right].IsValid()) {
    return true;
  }
  int result = iters[left].Compare(iters[right]);
  if (result != 0) {
    return 0 < result;
  }
  return iters[right].Generation() < iters[left].Generation();
}

void LSMView::Iterator::Forward() {
  ++iters_[0];

  if (!iters_[0].IsValid()) {
    if (--remaining_iters_ == 0) {
      // Now all iterators finished.
      iters_.clear();
      vm_ = nullptr;
      return;
    }
  }

  // If this node is the last run, use this.
  if (iters_.size() == 1 || (2 <= iters_.size() && !iters_[1].IsValid())) {
    return;
  }

  size_t curr = 1;
  if (iters_[0].IsValid()) {
    assert(iters_[curr].IsValid());
    int result = iters_[0].Compare(iters_[curr]);
    if (result == 0 && iters_[0].Generation() < iters_[curr].Generation()) {
      ++iters_[0];
      if (!iters_[0].IsValid()) {
        --remaining_iters_;
      }
    } else if (0 <= result) {
      return;
    }
  }
  std::swap(iters_[0], iters_[curr]);
  while (curr * 2 < iters_.size()) {
    if (curr * 2 + 1 == iters_.size() ||
        IsRightIteratorBigger(curr * 2, curr * 2 + 1, iters_)) {
      if (!iters_[curr].IsValid() ||
          (iters_[curr * 2].IsValid() &&
           (0 < iters_[curr * 2].Compare(iters_[curr]) ||
            (0 == iters_[curr * 2].Compare(iters_[curr]) &&
             iters_[curr].Generation() < iters_[curr * 2].Generation())))) {
        std::swap(iters_[curr], iters_[curr * 2]);
        curr *= 2;
      } else {
        break;
      }
    } else {
      if (IsRightIteratorBigger(curr * 2 + 1, curr, iters_)) {
        std::swap(iters_[curr], iters_[curr * 2 + 1]);
        curr = curr * 2 + 1;
      } else {
        break;
      }
    }
  }
}

LSMView::Iterator& LSMView::Iterator::operator++() {
  std::string previous_key;
  do {
    previous_key = Key();
    Forward();
  } while (IsValid() && (Key() == previous_key || iters_[0].IsDeleted()));
  return *this;
}

bool LSMView::Iterator::operator==(const LSMView::Iterator& rhs) const {
  if (vm_ != rhs.vm_) {
    return false;
  }
  if (iters_.size() != rhs.iters_.size()) {
    return false;
  }
  for (size_t i = 0; i < iters_.size(); ++i) {
    if (iters_[i] != rhs.iters_[i]) {
      return false;
    }
  }
  return true;
}

bool LSMView::Iterator::IsValid() const { return remaining_iters_ != 0; }

std::ostream& operator<<(std::ostream& o, const LSMView::Iterator& it) {
  for (size_t i = 0; i < it.iters_.size(); ++i) {
    if (0 < i) {
      o << "\n";
    }
    o << "[" << i << "] ";
    if (it.iters_[i].IsValid()) {
      o << it.iters_[i] << " @" << it.iters_[i].Generation();
    } else {
      o << "(finished)";
    }
  }
  return o;
}

std::ostream& operator<<(std::ostream& o, const LSMView& v) {
  for (const auto& idx : v.indexes_) {
    o << idx;
  }
  return o;
}

}  // namespace tinylamb