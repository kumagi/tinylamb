/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#ifndef TINYLAMB_MEMTABLE_SKIPLIST_HPP
#define TINYLAMB_MEMTABLE_SKIPLIST_HPP

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "table/memtable/row_entry.hpp"

namespace tinylamb {

// Bump-pointer arena: nodes come from large chunks, so inserting N entries
// costs O(N/chunk) allocations instead of O(N) mallocs, and consecutive
// inserts land close together in cache.
class SkipArena final {
 public:
  SkipArena() = default;
  SkipArena(const SkipArena&) = delete;
  SkipArena& operator=(const SkipArena&) = delete;
  SkipArena(SkipArena&&) = delete;
  SkipArena& operator=(SkipArena&&) = delete;

  ~SkipArena() { FreeAll(); }

  void* Allocate(size_t bytes) {
    bytes = (bytes + 15U) & ~size_t{15};  // 16B alignment
    if (remaining_ < bytes) {
      const size_t cap = bytes > kChunkSize ? bytes : kChunkSize;
      // Each chunk owns its bytes even if growing the outer vector throws.
      chunks_.emplace_back(cap);
      cursor_ = chunks_.back().data();
      remaining_ = cap;
    }
    void* p = cursor_;
    cursor_ += bytes;
    remaining_ -= bytes;
    return p;
  }

  void FreeAll() {
    chunks_.clear();
    cursor_ = nullptr;
    remaining_ = 0;
  }

  // O(1) exchange of chunk ownership (SkipList::Swap).
  void Swap(SkipArena& other) noexcept {
    std::swap(chunks_, other.chunks_);
    std::swap(cursor_, other.cursor_);
    std::swap(remaining_, other.remaining_);
  }

 private:
  static constexpr size_t kChunkSize = size_t{1} << 18;  // 256 KiB
  std::vector<std::vector<char>> chunks_;
  char* cursor_{nullptr};
  size_t remaining_{0};
};

// Ordered map keyed by memcomparable byte strings, tuned for write-heavy
// memtable duty: one arena allocation per entry (key bytes stored inline),
// probabilistic balancing, and O(log n) search with short lock-free-friendly
// read paths. Values are RowEntries (row payload + tombstone flag).
//
// Not thread-safe by itself: MemTable serializes access.
class SkipList final {
 public:
  static constexpr int kMaxLevel = 16;

  SkipList();
  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;
  SkipList(SkipList&&) = delete;
  SkipList& operator=(SkipList&&) = delete;
  ~SkipList() { Clear(); }

  // Insert `entry` under `key`, replacing any existing value. Returns true
  // when the key is new.
  bool Upsert(std::string_view key, RowEntry&& entry);

  // Pointer to the live entry for `key`, or nullptr. Valid only until the
  // next Upsert/Clear/Swap on this list: MemTable::Sync swaps and clears
  // sealed lists, so a pointer retained across Sync dangles. Copy the
  // RowEntry out under the shard lock instead of retaining the pointer.
  // Reads are NOT lock-free (plain loads, non-atomic size_): MemTable's
  // per-shard mutex must be held despite the cache-friendly layout.
  [[nodiscard]] const RowEntry* Find(std::string_view key) const;

  [[nodiscard]] bool Contains(std::string_view key) const {
    return Find(key) != nullptr;
  }

  [[nodiscard]] size_t Size() const { return size_; }
  [[nodiscard]] bool Empty() const { return size_ == 0; }

  // Destroys all entries and releases arena memory.
  void Clear();

  // O(1) exchange of contents (used by MemTable::Sync to seal the active
  // list without moving any node).
  void Swap(SkipList& other) noexcept {
    arena_.Swap(other.arena_);
    std::swap(head_, other.head_);
    for (int level = 0; level < kMaxLevel; ++level) {
      std::swap(tail_[level], other.tail_[level]);
    }
    std::swap(rng_, other.rng_);
    std::swap(size_, other.size_);
    std::swap(max_height_, other.max_height_);
  }

  struct ConstIterator {
    const void* node{nullptr};

    [[nodiscard]] bool Valid() const { return node != nullptr; }
    void Next();
    [[nodiscard]] std::string_view Key() const;
    [[nodiscard]] const RowEntry& Entry() const;
  };

  [[nodiscard]] ConstIterator Begin() const;

 private:
  struct Node;
  // Defined in skiplist.cpp: RowEntry + key_len + height + inline key bytes
  // trailing after `height` next pointers, single arena block.
  Node* head_{nullptr};  // kMaxLevel-height sentinel, never carries data
  std::array<Node*, kMaxLevel> tail_{};

  [[nodiscard]] int PickHeight();
  Node* NewNode(std::string_view key, RowEntry&& entry, int height);
  void InitHead();
  // Node-key form of ComparePrepared fused with the caller's prepared key:
  // identical result, but the fixed9 comparison reads the node's cached
  // big-endian header word (word0/tail9) instead of memcpy+bswap-ing the
  // key bytes off their own cache line on every descent step. The prepared
  // key travels as its four fields so this private member's signature never
  // names the file-local key type.
  static int CompareNodePrepared(const Node& n, std::string_view original,
                                 uint64_t word, uint8_t tail, bool fixed9);

  SkipArena arena_;
  uint32_t rng_{0x9E3779B9U};
  size_t size_{0};
  int max_height_{1};
};

}  // namespace tinylamb

#endif  // TINYLAMB_MEMTABLE_SKIPLIST_HPP
