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

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/log_message.hpp"
#include "common/status_or.hpp"
#include "common/vm_cache.hpp"
#include "index/lsm_detail/blob_file.hpp"

namespace tinylamb {

struct LSMValue {
  bool is_delete{false};
  std::string payload;
  static LSMValue& Delete() {
    static LSMValue instance = LSMValue();
    return instance;
  }
  LSMValue() : is_delete(true) {}
  explicit LSMValue(std::string p) : is_delete(false), payload(std::move(p)) {}
  LSMValue(const LSMValue& rhs) = default;
  LSMValue(LSMValue&& rhs) = default;
  LSMValue& operator=(const LSMValue& rhs) = default;
  LSMValue& operator=(LSMValue&& rhs) = default;
  ~LSMValue() = default;
  friend std::ostream& operator<<(std::ostream& o, const LSMValue& v) {
    if (v.is_delete) {
      o << "(deleted)";
    } else {
      o << v.payload;
    }
    return o;
  }
};

class SortedRun {
  static std::string HeadString(uint32_t in) {
    std::string out(4, 0);
    *(reinterpret_cast<uint32_t*>(out.data())) = be32toh(in);
    return out;
  }

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
      return value_.offset_ == kDeletedValue;
    }
    friend std::ostream& operator<<(std::ostream& o, const Entry& e) {
      o << "length: " << e.length_ << " head: " << e.key_head_;
      if (kIndirectThreshold < e.length_) {
        o << " stored at offset: " << e.key_.reference_;
      } else {
        o << " key: " << SortedRun::HeadString(e.key_.inline_);
      }
      if (e.IsDeleted()) {
        o << "(deleted)";
      } else {
        o << " value_len: " << e.value_length_;
        if (e.value_length_ <= sizeof(uint64_t)) {
          o << " value: "
            << std::string_view(e.value_.inline_.data(), e.value_length_);
        } else {
          o << " offset: " << e.value_.offset_;
        }
      }
      return o;
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
    uint64_t value_length_{0};
    union Value {
      // Pointer to full length value.
      // Used only when the value length is longer than 8 bytes.
      uint64_t offset_;

      // Actual value inlined entry. The value_length_ must be shorter or equal
      // to 8 bytes when this field used.
      std::array<char, 8> inline_;
    } value_{0};
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
      return Entry().BuildKey(*blob_);
    }
    [[nodiscard]] std::string Value() const {
      assert(IsValid());
      return Entry().BuildValue(*blob_);
    }
    [[nodiscard]] int Compare(const Iterator& rhs) const {
      assert(IsValid());
      assert(blob_ == rhs.blob_);
      assert(offset_ < parent_->Size());
      if (rhs.offset_ >= rhs.parent_->Size()) {
        LOG(ERROR) << rhs.offset_ << " vs " << rhs.parent_->Size();
      }
      assert(rhs.offset_ < rhs.parent_->Size());
      return Entry().Compare(rhs.Entry(), *blob_);
    }
    [[nodiscard]] Entry Entry() const { return parent_->GetEntry(offset_); }
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
      return parent_->GetEntry(offset_).IsDeleted();
    }

   private:
    const SortedRun* parent_;
    const BlobFile* blob_;
    size_t offset_;
  };

  SortedRun() = default;
  explicit SortedRun(const std::filesystem::path& file);
  static void Construct(const std::filesystem::path& file,
                        const std::map<std::string, LSMValue>& tree,
                        BlobFile& blob_, size_t generation);

  SortedRun(const SortedRun&) = default;
  SortedRun(SortedRun&&) = default;
  SortedRun& operator=(const SortedRun&) = default;
  SortedRun& operator=(SortedRun&&) = default;
  ~SortedRun() = default;

  // StatusOr<size_t> Flush(const std::filesystem::path& path);

  [[nodiscard]] StatusOr<std::string> Find(std::string_view key,
                                           const BlobFile& blob) const;

  [[nodiscard]] size_t Size() const { return length_; }
  [[nodiscard]] size_t Generation() const { return generation_; }

  [[nodiscard]] Iterator Begin(const BlobFile& blob) const {
    return {this, blob, 0};
  }
  friend std::ostream& operator<<(std::ostream& o, const SortedRun& s);
  static void FlushInternal(const std::filesystem::path& path,
                            std::string_view min, std::string_view max,
                            const std::vector<Entry>& index, size_t generation);

 private:
  static size_t FileOffset() {
    return reinterpret_cast<intptr_t>(
        &reinterpret_cast<SortedRun*>(NULL)->index_);
  }
  [[nodiscard]] Entry GetEntry(size_t offset) const;

  std::string min_key_;
  std::string max_key_;
  size_t length_{0};
  size_t generation_{0};
  std::shared_ptr<VMCache<Entry>> index_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_SORTED_RUN_HPP
