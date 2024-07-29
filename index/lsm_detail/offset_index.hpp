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
#ifndef TINYLAMB_OFFSET_INDEX_HPP
#define TINYLAMB_OFFSET_INDEX_HPP

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <ostream>
#include <vector>

namespace tinylamb {

class OffsetIndex {
 public:
  OffsetIndex(std::filesystem::path path);
  [[nodiscard]] size_t Size() const;
  const uint64_t& operator[](size_t i) const;

  class Iterator {
   public:
    explicit Iterator(const OffsetIndex* idx, size_t pos);
    ~Iterator() = default;
    Iterator(Iterator&& o) = default;
    Iterator(const Iterator& o) = default;
    Iterator& operator=(Iterator&& o) = default;
    Iterator& operator=(const Iterator& o) = default;
    [[nodiscard]] bool IsInvalid() const {
      return offset_ == nullptr || offset_->Size() == pos_;
    }

    uint64_t operator*() const;
    Iterator& operator++();
    bool operator==(const Iterator& rhs) const;
    bool operator!=(const Iterator& rhs) const;
    friend std::ostream& operator<<(std::ostream& o, const Iterator& it);

   private:
    size_t pos_;
    const OffsetIndex* offset_;
  };
  [[nodiscard]] Iterator Begin() const { return Iterator(this, 0); }
  [[nodiscard]] Iterator End() { return Iterator(this, offsets_.size()); }

 private:
  std::filesystem::path path_;
  std::vector<uint64_t> offsets_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_OFFSET_INDEX_HPP
