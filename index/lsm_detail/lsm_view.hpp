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

#ifndef TINYLAMB_LSM_VIEW_HPP
#define TINYLAMB_LSM_VIEW_HPP

#include <cstddef>
#include <filesystem>
#include <initializer_list>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include "common/status_or.hpp"
#include "index/lsm_detail/blob_file.hpp"
#include "index/lsm_detail/sorted_run.hpp"

namespace tinylamb {

class LSMView {
 public:
  template <typename FilesType>
  LSMView(const BlobFile& blob, const FilesType& files) : blob_(blob) {
    for (const auto& file : files) {
      indexes_.emplace_back(file);
    }
    std::sort(indexes_.begin(), indexes_.end(),
              [](const SortedRun& a, const SortedRun& b) -> bool {
                return a.Generation() > b.Generation();
              });
  }

  class Iterator {
   public:
    Iterator(const LSMView* vm, bool head);
    ~Iterator() = default;
    Iterator(Iterator&& o) = default;
    Iterator(const Iterator& o) = default;
    Iterator& operator=(Iterator&& o) = default;
    Iterator& operator=(const Iterator& o) = default;
    [[nodiscard]] const SortedRun::Iterator& TopIterator() const {
      return iters_[0];
    }
    [[nodiscard]] std::string Key() const { return iters_[0].Key(); }
    [[nodiscard]] std::string Value() const;
    Iterator& operator++();
    const SortedRun::Entry& operator*() const;
    bool operator==(const Iterator& rhs) const;
    [[nodiscard]] bool IsValid() const;
    friend std::ostream& operator<<(std::ostream& o,
                                    const LSMView::Iterator& it);

   private:
    void Forward();

    const LSMView* vm_;
    std::vector<SortedRun::Iterator> iters_;
    size_t remaining_iters_;
  };
  [[nodiscard]] Iterator Begin() const;
  [[nodiscard]] StatusOr<std::string> Find(std::string_view key) const;
  [[nodiscard]] size_t Size() const;
  [[nodiscard]] SortedRun CreateSingleRun() const;
  friend std::ostream& operator<<(std::ostream& o, const LSMView& v);

 private:
  const BlobFile& blob_;
  std::vector<SortedRun> indexes_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LSM_VIEW_HPP
