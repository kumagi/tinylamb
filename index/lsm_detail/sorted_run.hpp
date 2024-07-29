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

#ifndef TINYLAMB_SORTED_RUN_HPP
#define TINYLAMB_SORTED_RUN_HPP

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/log_message.hpp"
#include "common/status_or.hpp"
#include "index/lsm_detail/blob_file.hpp"
#include "recovery/logger.hpp"
namespace tinylamb {

struct LSMValue {
  bool is_delete{false};
  std::string payload;
};

class SortedRun {
 public:
  struct Entry {
    constexpr static size_t kIndirectThreshold = 12;
    Entry() = default;
    Entry(std::string_view key, const LSMValue& value, BlobFile& blob_writer);

    // Returns +1 if the Entry is smaller than `rhs`.
    // Returns  0 if the Entry is equal to `rhs`.
    // Returns -1 if the Entry is bigger than `rhs`
    [[nodiscard]] int Compare(std::string_view rhs, const BlobFile& blob) const;
    [[nodiscard]] int Compare(const Entry& rhs, const BlobFile& blob) const;

    // Caution: These are slow, use mainly for debug.
    [[nodiscard]] std::string BuildKey(const BlobFile& blob) const;
    [[nodiscard]] std::string BuildValue(const BlobFile& blob) const;

    [[nodiscard]] bool IsDeleted() const {
      return value_offset_ == kDeletedValue;
    }
    // If this is bigger than kIndirectThreshold, the key payload is stored
    // over offset.
    uint32_t length_{0};

    // memory comparable initial 4 bytes of key.
    // Note that this is integer comparable.
    uint32_t key_head_{0};

    // Rest payload of the key.
    // If the key length is shorter than kIndirectThreshold,
    union Payload {
      // Pointer to full length key.
      // Used only when the key length is longer than kIndirectThreshold.
      uint64_t reference_;

      // Integer comparable inlined key header. The length_ must be shorter or
      // equal to 12 bytes if this field used.
      uint64_t inline_;
    } key_{0};

    constexpr static uint64_t kDeletedValue = 0xffffffffffffffff;
    uint64_t value_offset_{0};
    uint64_t value_length_{0};
  };

  class Iterator {
   public:
    Iterator(const SortedRun* parent, const BlobFile& blob, size_t offset = 0)
        : parent_(parent), blob_(&blob), offset_(offset) {}
    ~Iterator() = default;
    Iterator(Iterator&& o) = default;
    Iterator(const Iterator& o) = default;
    Iterator& operator=(Iterator&& o) = default;
    Iterator& operator=(const Iterator& o) = default;

    [[nodiscard]] std::string Key() const {
      assert(IsValid());
      return parent_->index_[offset_].BuildKey(*blob_);
    }
    [[nodiscard]] std::string Value() const {
      assert(IsValid());
      return parent_->index_[offset_].BuildValue(*blob_);
    }
    [[nodiscard]] int Compare(const Iterator& rhs) const {
      assert(IsValid());
      assert(blob_ == rhs.blob_);
      return parent_->index_[offset_].Compare(rhs.parent_->index_[rhs.offset_],
                                              *blob_);
    }
    [[nodiscard]] const Entry& Entry() const {
      return parent_->index_[offset_];
    }
    [[nodiscard]] bool IsValid() const { return offset_ < parent_->Size(); }
    Iterator& operator++() {
      ++offset_;
      return *this;
    }
    bool operator==(const Iterator& rhs) const;
    bool operator!=(const Iterator& rhs) const;
    friend std::ostream& operator<<(std::ostream& o, const Iterator& it);
    [[nodiscard]] size_t Generation() const { return parent_->generation_; }
    [[nodiscard]] bool IsDeleted() const {
      return parent_->index_[offset_].IsDeleted();
    }

   private:
    const SortedRun* parent_;
    const BlobFile* blob_;
    size_t offset_;
  };

  explicit SortedRun(const std::filesystem::path& file);
  SortedRun(const std::map<std::string, LSMValue>& tree, BlobFile& blob_,
            size_t generation);
  SortedRun(std::string_view min_key, std::string_view max_key,
            std::vector<Entry> index, size_t generation)
      : min_key_(min_key),
        max_key_(max_key),
        index_(std::move(index)),
        generation_(generation) {}
  StatusOr<size_t> Flush(const std::filesystem::path& path);

  [[nodiscard]] StatusOr<std::string> Find(std::string_view key,
                                           const BlobFile& blob) const;

  [[nodiscard]] size_t Size() const { return index_.size(); }
  [[nodiscard]] size_t Generation() const { return generation_; }

  [[nodiscard]] Iterator Begin(const BlobFile& blob) const {
    return {this, blob, 0};
  }
  friend std::ostream& operator<<(std::ostream& o, const SortedRun& s);

 private:
  std::string min_key_;
  std::string max_key_;
  std::vector<Entry> index_;
  size_t generation_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_SORTED_RUN_HPP
