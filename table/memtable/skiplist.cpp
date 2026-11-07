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

#include "table/memtable/skiplist.hpp"

#include <cassert>
#include <limits>
#include <stdexcept>

namespace tinylamb {

namespace {

// Key pre-decoded for comparison: the original view plus, for the fixed
// 9-byte memcomparable shape, a big-endian word and the 9th byte.
struct PreparedKey {
  std::string_view original;
  uint64_t word{0};
  uint8_t tail{0};
  bool fixed9{false};
};

PreparedKey PrepareKey(std::string_view key) {
  PreparedKey prepared{key, 0, 0, false};
  if (key.size() == 9) {
    uint64_t word = 0;
    std::memcpy(&word, key.data(), sizeof(word));
    prepared.word = __builtin_bswap64(word);
    prepared.tail = static_cast<uint8_t>(key[8]);
    prepared.fixed9 = true;
  }
  return prepared;
}

}  // namespace

// Node: one arena block holds the header, `height` next pointers, and the key
// bytes inline. sizeof(Node) rounds to 8 (RowEntry contains Row => alignof 8),
// so nexts() stays pointer-aligned.
struct SkipList::Node {
  RowEntry entry;
  // Cached big-endian view of the first 8 key bytes (plus the 9th byte in
  // tail9), valid only when key_len == 9. Every descent step on a fixed9
  // key otherwise paid a memcpy + bswap of the key bytes -- which live on a
  // DIFFERENT cache line than this header -- per comparison; the header
  // line is already loaded for key_len/height, so comparing against the
  // cached word turns the step into two register compares. In-memory only:
  // nothing here reaches the wire.
  uint64_t word0;
  uint8_t tail9;
  uint32_t key_len;
  uint16_t height;

  [[nodiscard]] void** nexts() {
    return reinterpret_cast<void**>(reinterpret_cast<char*>(this) +
                                    sizeof(Node));
  }
  [[nodiscard]] void* const* nexts() const {
    return reinterpret_cast<void* const*>(reinterpret_cast<const char*>(this) +
                                          sizeof(Node));
  }
  [[nodiscard]] void*& next(int lvl) { return nexts()[lvl]; }
  [[nodiscard]] void* const& next(int lvl) const { return nexts()[lvl]; }
  [[nodiscard]] std::string_view Key() const {
    // No embedded pointer member: all `height` links live in the trailing
    // block, so key bytes start right after them.
    const char* kb = reinterpret_cast<const char*>(this) + sizeof(Node) +
                     static_cast<size_t>(height) * sizeof(void*);
    return {kb, key_len};
  }
};

int SkipList::CompareNodePrepared(const Node& n, std::string_view original,
                                  uint64_t word, uint8_t tail, bool fixed9) {
  if (fixed9 && n.key_len == 9) {
    if (n.word0 != word) return n.word0 < word ? -1 : 1;
    if (n.tail9 != tail) return n.tail9 < tail ? -1 : 1;
    return 0;
  }
  return n.Key().compare(original);
}

// ---- Iterator --------------------------------------------------------------

void SkipList::ConstIterator::Next() {
  if (node == nullptr) return;
  auto* n = static_cast<Node*>(const_cast<void*>(node));
  node = n->next(0);
}

std::string_view SkipList::ConstIterator::Key() const {
  assert(node != nullptr && "Key() on invalid iterator");
  return static_cast<const Node*>(node)->Key();
}

const RowEntry& SkipList::ConstIterator::Entry() const {
  assert(node != nullptr && "Entry() on invalid iterator");
  return static_cast<const Node*>(node)->entry;
}

SkipList::ConstIterator SkipList::Begin() const {
  ConstIterator it;
  it.node = head_->next(0);
  return it;
}

// ---- Construction / destruction -------------------------------------------

void SkipList::InitHead() {
  auto* mem = static_cast<char*>(arena_.Allocate(
      sizeof(Node) + static_cast<size_t>(kMaxLevel) * sizeof(void*)));
  auto* h = reinterpret_cast<Node*>(mem);
  new (&h->entry) RowEntry();
  h->key_len = 0;
  h->height = kMaxLevel;
  h->word0 = 0;
  h->tail9 = 0;
  for (int i = 0; i < kMaxLevel; ++i) {
    h->next(i) = nullptr;
    tail_[i] = h;
  }
  head_ = h;
}

int SkipList::PickHeight() {
  // Geometric distribution with p = 1/4 (RocksDB-style): expected tower
  // height ~1.33 keeps level-0 traversal cache-friendly.
  uint32_t r = rng_;
  rng_ = rng_ * 1664525u + 1013904223u;
  int h = 1;
  r >>= 2;  // drop low bits: least correlated with insertion patterns
  while (h < kMaxLevel && (r & 3u) == 0) {
    ++h;
    r >>= 2;
  }
  return h;
}

SkipList::Node* SkipList::NewNode(std::string_view key, RowEntry&& entry,
                                  int height) {
  // key_len is uint32: a key past 4 GiB would silently truncate and corrupt
  // ordering (shortened Key() view compares equal to a different key).
  if (key.size() > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("skiplist key exceeds uint32 range");
  }
  const size_t bytes =
      sizeof(Node) + static_cast<size_t>(height) * sizeof(void*) + key.size();
  auto* mem = static_cast<char*>(arena_.Allocate(bytes));
  auto* n = reinterpret_cast<Node*>(mem);
  new (&n->entry) RowEntry(std::move(entry));
  n->key_len = static_cast<uint32_t>(key.size());
  n->height = static_cast<uint16_t>(height);
  for (int i = 0; i < height; ++i) n->next(i) = nullptr;
  if (key.size() == 9) {
    uint64_t word = 0;
    std::memcpy(&word, key.data(), sizeof(word));
    n->word0 = __builtin_bswap64(word);
    n->tail9 = static_cast<uint8_t>(key[8]);
  } else {
    n->word0 = 0;
    n->tail9 = 0;
  }
  if (!key.empty()) {
    memcpy(const_cast<char*>(n->Key().data()), key.data(), key.size());
  }
  return n;
}

SkipList::SkipList() { InitHead(); }

void SkipList::Clear() {
  // Run entry destructors before releasing arena memory.
  ConstIterator it = Begin();
  while (it.Valid()) {
    auto* n = static_cast<Node*>(const_cast<void*>(it.node));
    it.Next();
    n->entry.~RowEntry();
  }
  // The sentinel was placement-new'd in InitHead like any other node:
  // destroy it too before FreeAll releases its chunk.
  if (head_ != nullptr) head_->entry.~RowEntry();
  arena_.FreeAll();
  // A fresh InitHead builds a brand-new sentinel (no destruction needed
  // for it here -- its object never existed yet).
  InitHead();
  size_ = 0;
  max_height_ = 1;
}

// ---- Core operations -------------------------------------------------------

bool SkipList::Upsert(std::string_view key, RowEntry&& entry) {
  const PreparedKey prepared = PrepareKey(key);
  Node* const tail = tail_[0];
  if (size_ != 0) {
    const int tail_cmp =
        CompareNodePrepared(*tail, prepared.original, prepared.word,
                            prepared.tail, prepared.fixed9);
    if (tail_cmp == 0) {
      tail->entry.~RowEntry();
      new (&tail->entry) RowEntry(std::move(entry));
      return false;
    }
    if (tail_cmp < 0) {
      const int height = PickHeight();
      Node* n = NewNode(key, std::move(entry), height);
      for (int level = 0; level < height; ++level) {
        Node* const previous = tail_[level];
        n->next(level) = previous->next(level);
        previous->next(level) = n;
        tail_[level] = n;
      }
      if (height > max_height_) max_height_ = height;
      ++size_;
      return true;
    }
  }
  Node* update[kMaxLevel];
  Node* x = head_;
  for (int lvl = max_height_ - 1; lvl >= 0; --lvl) {
    while (true) {
      void* nxt = x->next(lvl);
      if (nxt == nullptr) break;
      auto* nx = static_cast<Node*>(nxt);
      // The descent is a chain of dependent cache lines: the next level's
      // loop loads nx's pointer array while this level's compare reads nx's
      // header and key bytes, so pull the pointer-array line in early and
      // let it travel during the compare.
      __builtin_prefetch(reinterpret_cast<const char*>(nx) + sizeof(Node));
      if (CompareNodePrepared(*nx, prepared.original, prepared.word,
                              prepared.tail, prepared.fixed9) >= 0)
        break;
      x = nx;
    }
    update[lvl] = x;
  }
  void* cand_v = x->next(0);
  if (cand_v != nullptr) {
    auto* cand = static_cast<Node*>(cand_v);
    if (CompareNodePrepared(*cand, prepared.original, prepared.word,
                            prepared.tail, prepared.fixed9) == 0) {
      // Overwrite in place: newest value replaces the old one.
      cand->entry.~RowEntry();
      new (&cand->entry) RowEntry(std::move(entry));
      return false;
    }
  }

  const int height = PickHeight();
  Node* n = NewNode(key, std::move(entry), height);
  const int previous_max = max_height_;
  if (height > max_height_) max_height_ = height;
  for (int lvl = previous_max; lvl < height; ++lvl) update[lvl] = head_;
  for (int lvl = 0; lvl < height; ++lvl) {
    n->next(lvl) = update[lvl]->next(lvl);
    update[lvl]->next(lvl) = n;
    // tail_[lvl] must track the LAST node at every level. Leaving a stale
    // sentinel here (e.g. head, after this node extends past the old
    // max_height_) made the append fast path link its tower through head
    // while an existing node still hung off head->next(lvl) -- producing an
    // out-of-order higher-level chain. Find() stays correct (level 0 is
    // exhaustive and every comparison is conservative), but searches lose
    // the skipped levels. n->next(lvl) == nullptr exactly when n is the new
    // last node at that level (this also covers the empty-list insert,
    // where every update[lvl] is head).
    if (n->next(lvl) == nullptr) tail_[lvl] = n;
  }
  ++size_;
  return true;
}

const RowEntry* SkipList::Find(std::string_view key) const {
  const PreparedKey prepared = PrepareKey(key);
  const Node* x = head_;
  for (int lvl = max_height_ - 1; lvl >= 0; --lvl) {
    while (true) {
      void* const& nxt = x->next(lvl);
      if (nxt == nullptr) break;
      const auto* nx = static_cast<const Node*>(nxt);
      // Same prefetch as Upsert's descent: the next level reads nx's
      // pointer array while this level compares nx's key.
      __builtin_prefetch(reinterpret_cast<const char*>(nx) + sizeof(Node));
      if (CompareNodePrepared(*nx, prepared.original, prepared.word,
                              prepared.tail, prepared.fixed9) >= 0)
        break;
      x = nx;
    }
  }
  const void* cand_v = x->next(0);
  if (cand_v != nullptr) {
    const auto* cand = static_cast<const Node*>(cand_v);
    if (CompareNodePrepared(*cand, prepared.original, prepared.word,
                            prepared.tail, prepared.fixed9) == 0)
      return &cand->entry;
  }
  return nullptr;
}

}  // namespace tinylamb
