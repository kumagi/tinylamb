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

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <filesystem>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "index/lsm_detail/sorted_run.hpp"

namespace tinylamb {
namespace {
// Treats invalid iterator as infinity big.
StatusOr<bool> IsRightIteratorBigger(
    int left, int right, const std::vector<SortedRun::Iterator>& iters) {
  if (!iters[static_cast<size_t>(left)].IsValid()) {
    return false;
  }
  if (!iters[static_cast<size_t>(right)].IsValid()) {
    return true;
  }
  ASSIGN_OR_RETURN(int, result,
                   iters[static_cast<size_t>(left)].Compare(
                       iters[static_cast<size_t>(right)]));
  if (result != 0) {
    return 0 < result;
  }
  return iters[static_cast<size_t>(right)].Generation() <
         iters[static_cast<size_t>(left)].Generation();
}
}  // namespace

StatusOr<LSMView::Iterator> LSMView::Begin() const {
  Iterator it(this);
  RETURN_IF_FAIL(it.Init());
  return it;
}

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

Status LSMView::CreateSingleRun(const std::filesystem::path& path) const {
  std::vector<SortedRun::Entry> merged;
  merged.reserve(Size());
  ASSIGN_OR_RETURN(Iterator, it, Begin());
  size_t max_generation = 0;
  for (const auto& run : indexes_) {
    max_generation = std::max(max_generation, run.Generation());
  }
  if (!it.IsValid()) {
    return SortedRun::FlushInternal(path, "", "", {}, max_generation + 1);
  }
  ASSIGN_OR_RETURN(std::string, min_key, it.Key());
  std::string max_key;

  while (it.IsValid()) {
    ASSIGN_OR_RETURN(SortedRun::Entry, entry, it.GetEntry());
    merged.push_back(entry);
    ASSIGN_OR_RETURN(std::string, key, it.Key());
    max_key = std::move(key);
    ++it;
    RETURN_IF_FAIL(it.GetStatus());
  }
  return SortedRun::FlushInternal(path, min_key, max_key, merged,
                                  max_generation + 1);
}

Status LSMView::Iterator::Init() {
  {
    iters_.reserve(vm_->indexes_.size());
    for (const SortedRun& run : vm_->indexes_) {
      SortedRun::Iterator iter = run.Begin(vm_->blob_);
      if (!iter.IsValid()) {
        continue;
      }
      iters_.emplace_back(iter);
    }
    remaining_iters_ = iters_.size();
    // Single min-heap build. Ordering: by key; equal keys keep the newest
    // run (largest generation number) on top so duplicates are emitted from
    // the newest source first and skipped by AdvanceSkippingTombstones().
    // Exhausted iterators act as +infinity sentinels.
    for (size_t i = 1; i < iters_.size(); ++i) {
      size_t curr = i;
      while (0 < curr) {
        ASSIGN_OR_RETURN(int, result, iters_[curr].Compare(iters_[curr / 2]));
        if (result == 0) {
          result = iters_[curr].Generation() < iters_[curr / 2].Generation()
                       ? -1
                       : 1;
        }
        if (0 < result) {
          std::swap(iters_[curr], iters_[curr / 2]);
          curr /= 2;
        } else {
          break;
        }
      }
    }

    if (!iters_.empty() && iters_[0].IsValid()) {
      ASSIGN_OR_RETURN(bool, deleted, iters_[0].IsDeleted());
      if (deleted) {
        return AdvanceSkippingTombstones();
      }
    }
  }
  return Status::kSuccess;
}

StatusOr<std::string> LSMView::Iterator::Value() const {
  return iters_[0].Value();
}

StatusOr<SortedRun::Entry> LSMView::Iterator::GetEntry() const {
  return iters_[0].GetEntry();
}

Status LSMView::Iterator::Forward() {
  ++iters_[0];

  if (!iters_[0].IsValid()) {
    if (--remaining_iters_ == 0) {
      // Now all iterators finished.
      iters_.clear();
      vm_ = nullptr;
      return Status::kSuccess;
    }
  }

  // If this node is the last run, use this.
  if (iters_.size() == 1 || (2 <= iters_.size() && !iters_[1].IsValid())) {
    return Status::kSuccess;
  }

  size_t curr = 1;
  if (iters_[0].IsValid()) {
    assert(iters_[curr].IsValid());
    ASSIGN_OR_RETURN(int, result, iters_[0].Compare(iters_[curr]));
    if (result == 0 && iters_[0].Generation() < iters_[curr].Generation()) {
      ++iters_[0];
      if (!iters_[0].IsValid()) {
        --remaining_iters_;
      }
    } else if (0 <= result) {
      return Status::kSuccess;
    }
  }
  std::swap(iters_[0], iters_[curr]);
  while (curr * 2 < iters_.size()) {
    // Short-circuit: with an odd heap size the right child does not exist,
    // so the left child wins without comparing (iters_[curr*2+1] is OOB).
    bool right_bigger_first = false;
    if ((curr * 2) + 1 != iters_.size()) {
      ASSIGN_OR_RETURN(
          bool, bigger,
          IsRightIteratorBigger(static_cast<int>(curr * 2),
                                static_cast<int>((curr * 2) + 1), iters_));
      right_bigger_first = bigger;
    }
    if ((curr * 2) + 1 == iters_.size() || right_bigger_first) {
      bool swap_left = !iters_[curr].IsValid();
      if (!swap_left && iters_[curr * 2].IsValid()) {
        ASSIGN_OR_RETURN(int, cmp1, iters_[curr * 2].Compare(iters_[curr]));
        if (0 < cmp1) {
          swap_left = true;
        } else if (0 == cmp1 &&
                   iters_[curr].Generation() < iters_[curr * 2].Generation()) {
          swap_left = true;
        }
      }
      if (swap_left) {
        std::swap(iters_[curr], iters_[curr * 2]);
        curr *= 2;
      } else {
        break;
      }
    } else {
      ASSIGN_OR_RETURN(bool, right_bigger,
                       IsRightIteratorBigger(static_cast<int>((curr * 2) + 1),
                                             static_cast<int>(curr), iters_));
      if (right_bigger) {
        std::swap(iters_[curr], iters_[(curr * 2) + 1]);
        curr = (curr * 2) + 1;
      } else {
        break;
      }
    }
  }
  return Status::kSuccess;
}

Status LSMView::Iterator::AdvanceSkippingTombstones() {
  assert(IsValid() && !iters_.empty() && iters_[0].IsValid());
  std::string previous_key;
  do {
    ASSIGN_OR_RETURN(std::string, key, Key());
    previous_key = std::move(key);
    RETURN_IF_FAIL(Forward());
    if (IsValid() && !iters_.empty() && iters_[0].IsValid()) {
      ASSIGN_OR_RETURN(bool, deleted, iters_[0].IsDeleted());
      ASSIGN_OR_RETURN(std::string, next_key, Key());
      if (next_key != previous_key && !deleted) {
        break;
      }
    } else {
      break;
    }
  } while (true);
  return Status::kSuccess;
}

LSMView::Iterator& LSMView::Iterator::operator++() {
  if (!IsValid() || iters_.empty() || !iters_[0].IsValid()) {
    remaining_iters_ = 0;
    return *this;
  }
  const Status status = AdvanceSkippingTombstones();
  if (status != Status::kSuccess) {
    status_ = status;
    remaining_iters_ = 0;
  }
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
