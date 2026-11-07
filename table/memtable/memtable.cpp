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

#include "table/memtable/memtable.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/platform/endian.hpp"
#include "common/status_or.hpp"
#include "common/version.hpp"
#include "table/memtable/skiplist.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

MemTable::MemTable(const Schema& sc)
    : schema_(sc), shards_(static_cast<size_t>(kNumShards)), fixed_key_(false) {
  key_idx_ = sc.KeyColumnIndex();
  key_type_ = sc.GetColumn(key_idx_).Type();
  fixed_key_ =
      key_type_ == ValueType::kInt64 || key_type_ == ValueType::kDouble;
}

StatusOr<Row> MemTable::Get(std::string_view key) const {
  Row row;
  Status st = Lookup(key, &row);
  if (st == Status::kSuccess) return Row(std::move(row));
  if (st == Status::kDeleted) return Status::kNotExists;
  return st;
}

// Point reads consult active first: it holds the newest values. sealed may be
// concurrently serialized by a drain thread; both walks are read-only so
// sharing the shard lock with writers keeps every access ordered.
namespace {
// Fixed-width memcomparable keys are 9 bytes (tag + one big-endian group).
// Word-order comparisons on the first 8 payload bytes decide the merge walk's
// hot ordering calls; equal prefixes and other widths fall through to the
// string_view forms. Byte-lexicographic order == big-endian word order, so
// the helpers are order-preserving and total.
inline bool MemKeyLess(std::string_view a, std::string_view b) {
  if (a.size() >= 8 && b.size() >= 8) {
    uint64_t x = 0;
    uint64_t y = 0;
    std::memcpy(&x, a.data(), sizeof(x));
    std::memcpy(&y, b.data(), sizeof(y));
    x = htobe64(x);
    y = htobe64(y);
    if (x != y) return x < y;
  }
  return a < b;
}

inline bool MemKeyEq(std::string_view a, std::string_view b) {
  if (a.size() == 9 && b.size() == 9) {
    uint64_t x = 0;
    uint64_t y = 0;
    std::memcpy(&x, a.data(), sizeof(x));
    std::memcpy(&y, b.data(), sizeof(y));
    if (x != y) return false;
    return static_cast<unsigned char>(a[8]) ==
           static_cast<unsigned char>(b[8]);  // tail byte
  }
  return a == b;
}
}  // namespace

Status MemTable::Lookup(std::string_view key, Row* row_out) const {
  const Shard& s = ShardFor(key);
  std::scoped_lock lk(s.lock);
  const RowEntry* e = s.active.Find(key);
  if (e == nullptr && s.draining) e = s.sealed.Find(key);
  if (e == nullptr) {
    return Status::kNotExists;
  }
  if (e->is_deleted) {
    return Status::kDeleted;
  }
  *row_out = e->row;
  return Status::kSuccess;
}

// Snapshot of one shard: merge-walk sealed (older) and active (newer); on
// equal keys the active version wins and both cursors advance. Tombstones
// are preserved. Entries with version > max_visible_version are invisible
// (version 0 = unversioned = always visible): skipping them HERE lets an
// older visible entry win the per-key collapse, which a post-hoc filter on
// the collapsed output could never recover. Caller holds the shard lock.
std::vector<std::pair<std::string, RowEntry>> MemTable::SnapshotShard(
    const Shard& s, uint64_t max_visible_version) {
  std::vector<std::pair<std::string, RowEntry>> out;
  out.reserve(s.active.Size() + s.sealed.Size());
  auto ai = s.active.Begin();
  auto si = s.sealed.Begin();
  const auto invisible =
      [max_visible_version](const SkipList::ConstIterator& i) {
        const uint64_t v = i.Entry().version;
        return v != 0 && v > max_visible_version;
      };
  while (ai.Valid() || si.Valid()) {
    // Invisible entries drop out before the ordering decision so the
    // newest-VISIBLE version wins the tie group.
    while (ai.Valid() && invisible(ai)) ai.Next();
    while (si.Valid() && invisible(si)) si.Next();
    if (!ai.Valid() && !si.Valid()) break;
    bool take_active;
    if (!ai.Valid()) {
      take_active = false;
    } else if (!si.Valid()) {
      take_active = true;
    } else {
      take_active = !MemKeyLess(si.Key(), ai.Key());
    }
    SkipList::ConstIterator& it = take_active ? ai : si;
    SkipList::ConstIterator& other = take_active ? si : ai;
    // When keys tie, emit once from the newer list and drop the stale one.
    const bool dupes_other = other.Valid() && MemKeyEq(other.Key(), it.Key());
    out.emplace_back(std::string(it.Key()), it.Entry());
    it.Next();
    if (dupes_other) other.Next();
  }
  return out;
}

std::vector<std::pair<std::string, RowEntry>> MemTable::Snapshot(
    uint64_t max_visible_version) const {
  // Capture shards sequentially, then k-way merge. Keys never move between
  // shards, so cross-shard ties are impossible; within a shard the newest
  // version wins.
  using Vec = std::vector<std::pair<std::string, RowEntry>>;
  std::vector<Vec> per_shard(static_cast<size_t>(kNumShards));
  size_t total = 0;
  for (int i = 0; i < kNumShards; ++i) {
    const Shard& s = shards_[static_cast<size_t>(i)];
    std::scoped_lock lk(s.lock);
    per_shard[static_cast<size_t>(i)] = SnapshotShard(s, max_visible_version);
    total += per_shard[static_cast<size_t>(i)].size();
  }

  Vec out;
  out.reserve(total);
  // Tournament merge over shard cursors (heap form: the previous
  // linear-min + vector::erase loop was O(n·k) — every emitted entry
  // shifted up to kNumShards cursors — while a pop/push_heap merge is
  // O(n log k), mirroring Sync's 16-way merge).
  struct Cursor {
    const std::pair<std::string, RowEntry>* begin;
    const std::pair<std::string, RowEntry>* end;
    bool Valid() const { return begin != end; }
  };
  std::vector<Cursor> cursors;
  cursors.reserve(static_cast<size_t>(kNumShards));
  for (auto& v : per_shard) {
    if (!v.empty()) {
      cursors.push_back(Cursor{v.data(), v.data() + v.size()});
    }
  }
  // Heap ordering through the same word-prefix fast path Sync uses: the
  // first 8 bytes of a memcomparable key decide the order wherever they
  // differ, and equal prefixes (plus short keys) fall back to string_view.
  auto key_greater = [](const Cursor& a, const Cursor& b) {
    return MemKeyLess(b.begin->first,
                      a.begin->first);  // min-heap via greater-than
  };
  std::make_heap(cursors.begin(), cursors.end(), key_greater);
  while (!cursors.empty()) {
    std::pop_heap(cursors.begin(), cursors.end(), key_greater);
    Cursor& cur = cursors.back();
    out.push_back(*cur.begin);
    ++cur.begin;
    if (cur.Valid()) {
      std::push_heap(cursors.begin(), cursors.end(), key_greater);
    } else {
      cursors.pop_back();
    }
  }
  return out;
}

bool MemTable::Contains(std::string_view key) const {
  const Shard& s = ShardFor(key);
  std::scoped_lock lk(s.lock);
  const RowEntry* e = s.active.Find(key);
  if (e == nullptr && s.draining) e = s.sealed.Find(key);
  return e != nullptr && !e->is_deleted;
}

size_t MemTable::Size() const {
  size_t n = 0;
  for (const Shard& s : shards_) {
    std::scoped_lock lk(s.lock);
    n += s.active.Size() + s.sealed.Size();
  }
  return n;
}

Status MemTable::Put(const Row& r, bool sync) {
  // The row-blob wire format length-prefixes varchars with bin_size_t
  // (uint16). A longer value passes through Put, only to have Row::Serialize
  // THROW from deep inside Sync() -- after the shard swap, with the entry
  // already invisible in `active` and the caller expecting a Status, not an
  // exception. Reject up front instead.
  const slot_t kci = schema_.KeyColumnIndex();
  for (size_t vi = 0; vi < r.values_.size(); ++vi) {
    const Value& v = r.values_[vi];
    if (v.type == ValueType::kVarChar &&
        v.AsStringView().size() > std::numeric_limits<bin_size_t>::max()) {
      return Status::kTooBigData;
    }
    // A NULL cell on the KEY column cannot be memcomparable-encoded
    // (Value::EncodeMemcomparableFormat throws for kNull), so it can never
    // be sorted into the skiplist. Reject at the gate instead of letting
    // the throw escape from EncodeKeyMemcomparableFormat below. NULLs on
    // non-key columns serialize via the row nullmap and round-trip fine.
    if (v.type == ValueType::kNull && vi == kci) {
      return Status::kUnknownType;
    }
    // Value::type is a plain public field, so a hand-built Value can carry a
    // tag outside the ValueType enumeration. Row::Size() and
    // Row::Serialize() switch on it with no default arm and THROW
    // ("undefined type") -- and Row::Size() is called per row from Sync()
    // (:381), after the shard swap. The other gates in this loop exist for
    // precisely that reason; without this one an undefined tag escaped a
    // Status-returning API, skipped Phase 3, and so left the bad entry sealed:
    // every later Sync threw too, permanently wedging the memtable. It also
    // skipped the kNoSpace path's "clear the outputs" discipline, leaving a
    // valid run PREFIX in the caller's vectors with no Status to stop them
    // persisting a corrupt run.
    if (v.type != ValueType::kNull && v.type != ValueType::kInt64 &&
        v.type != ValueType::kVarChar && v.type != ValueType::kDouble) {
      return Status::kUnknownType;
    }
  }
  // Row::Serialize length-prefixes the column count with slot_t (uint16),
  // whose top bit is the nullmap flag, and throws on overflow -- same
  // throw-from-deep-inside-Sync hazard as the two per-cell gates above.
  if (r.values_.size() >= 0x8000) {
    return Status::kTooBigData;
  }
  // A row shorter than the key column index has no key cell at all: the
  // general path would throw from EncodeKeyMemcomparableFormat and the
  // fixed-key fast path below would index r.values_[key_idx_] out of
  // bounds. Reject the malformed row at the gate like the checks above.
  if (r.values_.size() <= kci) {
    return Status::kCorruption;
  }
  const auto put_entry = [&](std::string_view key) {
    Shard& s = ShardFor(key);
    std::scoped_lock lk(s.lock);
    const uint64_t v = global_version_mode_ ? NextGlobalVersion() : 0;
    s.active.Upsert(key, RowEntry(r, false, v));
  };
  if (fixed_key_) {
    // A fixed-width key always encodes to 9 memcomparable bytes, and the
    // encode reduces to one inline call on the common type match (the gate
    // above already rejected a NULL key cell). Keeps the ingest loop off
    // both the heap and the encode call chain.
    char kbuf[9];
    const Value& kv = r.values_[key_idx_];
    size_t klen;
    if (kv.type == key_type_) {
      klen =
          key_type_ == ValueType::kInt64
              ? EncodeMemcomparableFormatIntegerTo(kbuf, kv.value.int_value)
              : EncodeMemcomparableFormatDoubleTo(kbuf, kv.value.double_value);
    } else if (kv.type == ValueType::kVarChar) {
      // A varchar encodes to 9 bytes PER 8-char group: calling
      // EncodeMemcomparableFormatTo on the 9-byte stack buffer would smash
      // it. Same malformed-row gate as the length/null checks above.
      return Status::kUnknownType;
    } else {
      // The remaining mismatch (kInt64 vs kDouble) still encodes to exactly
      // 9 bytes and matches the general path, which encodes by value type.
      klen = kv.EncodeMemcomparableFormatTo(kbuf);
    }
    put_entry(std::string_view(kbuf, klen));
  } else {
    put_entry(r.EncodeKeyMemcomparableFormat(schema_));
  }
  if (sync) {
    std::vector<char> blob;
    std::vector<uint32_t> kr;
    std::vector<uint32_t> rr;
    size_t gen = 0;
    Status st = Sync(&blob, &kr, &rr, &gen);
    if (st != Status::kSuccess) return st;
  }
  return Status::kSuccess;
}

Status MemTable::Delete(std::string_view key, bool sync) {
  Shard& s = ShardFor(key);
  {
    std::scoped_lock lk(s.lock);
    const uint64_t v = global_version_mode_ ? NextGlobalVersion() : 0;
    s.active.Upsert(key, RowEntry(Row{}, true, v));
  }
  if (sync) {
    std::vector<char> blob;
    std::vector<uint32_t> kr;
    std::vector<uint32_t> rr;
    size_t gen = 0;
    Status st = Sync(&blob, &kr, &rr, &gen);
    if (st != Status::kSuccess) return st;
  }
  return Status::kSuccess;
}

Status MemTable::Sync(std::vector<char>* blob_out,
                      std::vector<uint32_t>* key_run_out,
                      std::vector<uint32_t>* row_run_out,
                      size_t* generation_out,
                      std::vector<uint8_t>* deleted_out) {
  // One drain across all shards at a time; concurrent Sync waits for the
  // current flush to finish entirely (matches previous external contract).
  std::scoped_lock flush_lk(flush_mutex_);

  // Phase 1: O(1)-per-shard critical sections seal every non-empty shard.
  // Writers only ever contend here, briefly, on their own shard.
  bool any = false;
  bool any_sealed = false;
  for (Shard& s : shards_) {
    std::scoped_lock lk(s.lock);
    if (!s.sealed.Empty()) {
      // Leftover sealed data from a previous Sync that bailed out with
      // kNoSpace mid-merge. Keep it at the HEAD of the drain: swapping it
      // with incoming `active` data would flip the recency order (new
      // writes flushed before the leftovers they supersede), breaking both
      // the newest-wins point read and the generation ranking.
      any_sealed = true;
    } else if (!s.active.Empty()) {
      s.active.Swap(s.sealed);
      s.draining = true;
      any = true;
    }
  }
  if (!any && !any_sealed) {
    // Truly empty: no pending writes AND no leftover sealed data.
    blob_out->clear();
    key_run_out->clear();
    row_run_out->clear();
    if (deleted_out != nullptr) deleted_out->clear();
    *generation_out = generation_;
    return Status::kSuccess;
  }
  // When `any` is false but sealed data remains, fall through and drain the
  // leftovers. The old early-return stranded that data forever: every
  // subsequent Sync reported success with empty outputs while pending
  // entries were never persisted — a durability hole.

  // Phase 2: serialize immutable sealed lists with shard locks released --
  // Puts keep flowing into fresh active lists throughout. A 16-way heap
  // merge emits entries in global key order in a single pass (each shard's
  // stream is already ascending), preserving the run's sorted contract
  // without an O(n log n) sort pass.
  struct Cursor {
    SkipList::ConstIterator it;
    bool Valid() const { return it.Valid(); }
  };
  std::vector<Cursor> heap;
  heap.reserve(static_cast<size_t>(kNumShards));
  size_t pending = 0;
  {
    for (Shard& s : shards_) {
      std::scoped_lock lk(s.lock);
      pending += s.sealed.Size();
    }
  }
  blob_out->clear();
  key_run_out->clear();
  row_run_out->clear();
  if (deleted_out != nullptr) deleted_out->clear();
  blob_out->reserve(blob_out->size() + pending * 64);
  key_run_out->reserve(pending);
  row_run_out->reserve(pending);
  // Heap ordering by key. Memcomparable keys sort byte-lexicographically, so
  // the first 8 bytes decide the order whenever they differ -- comparing them
  // as big-endian words replaces a memcmp call with two loads and a compare
  // on the hot heap path, and falls back to the full string_view comparison
  // for equal prefixes (and short keys) where the fast path is silent.
  auto key_greater = [](const Cursor& a, const Cursor& b) {
    const std::string_view ak = a.it.Key();
    const std::string_view bk = b.it.Key();
    if (ak.size() >= 8 && bk.size() >= 8) {
      uint64_t x = 0;
      uint64_t y = 0;
      std::memcpy(&x, ak.data(), sizeof(x));
      std::memcpy(&y, bk.data(), sizeof(y));
      const uint64_t xb = htobe64(x);
      const uint64_t yb = htobe64(y);
      if (xb != yb) return yb < xb;  // min-heap via greater-than
    }
    return bk < ak;
  };
  for (Shard& s : shards_) {
    // Reading sealed here without the shard lock is safe by construction:
    // during this phase the ONLY mutator of `sealed` is this thread (the
    // Phase-1 Swap and the Phase-3 Clear, both under flush_mutex_, and at
    // most one Sync runs at a time). Every other thread reaches sealed
    // read-only through paths that take the shard lock. Should a future
    // change let writers mutate sealed, these reads become a data race --
    // take s.lock first.
    if (s.sealed.Empty()) continue;
    heap.push_back(Cursor{s.sealed.Begin()});
    std::push_heap(heap.begin(), heap.end(), key_greater);
  }
  const size_t drain_budget =
      std::min(drain_budget_.load(std::memory_order_relaxed),
               size_t{std::numeric_limits<uint32_t>::max()});
  while (!heap.empty()) {
    std::pop_heap(heap.begin(), heap.end(), key_greater);
    Cursor cur = heap.back();
    heap.pop_back();
    const std::string_view key = cur.it.Key();
    const RowEntry& entry = cur.it.Entry();
    // Offsets are stored as uint32 in the run blob (see SortedRun::Entry).
    // Silently wrapping past the budget produces a corrupt run; bail out.
    // The budget must include the ROW payload: the previous form reserved
    // space for the key only, so a large row appended after the check
    // could still push the blob past the cap and wrap row_off. The budget
    // is normally UINT32_MAX (the uint32-offset ceiling) and is injectable
    // so tests can exercise the kNoSpace recovery path cheaply. Clamped to
    // that ceiling here: a caller that sets a larger budget (e.g. SIZE_MAX
    // "to disable" the cap) would otherwise let blob_out grow past 4 GiB
    // and wrap the uint32 key/row offsets into a corrupt run.
    const size_t row_size = entry.is_deleted ? 0 : entry.row.Size();
    const size_t row_bytes = entry.is_deleted ? 0 : row_size + sizeof(uint32_t);
    if (blob_out->size() + key.size() + 16 + row_bytes > drain_budget) {
      // Outputs hold a prefix of the drain on this path. They must not be
      // mistaken for a valid run: clear them so a caller that ignores the
      // status cannot persist a corrupt prefix (recovery re-drains the
      // intact sealed lists on the next Sync).
      blob_out->clear();
      key_run_out->clear();
      row_run_out->clear();
      if (deleted_out != nullptr) deleted_out->clear();
      return Status::kNoSpace;
    }
    uint32_t key_off = static_cast<uint32_t>(blob_out->size());
    uint32_t be_len = htobe32(static_cast<uint32_t>(key.size()));
    const size_t key_start = blob_out->size();
    blob_out->resize(key_start + sizeof(uint32_t) + key.size());
    std::memcpy(blob_out->data() + key_start, &be_len, sizeof(be_len));
    if (!key.empty()) {
      std::memcpy(blob_out->data() + key_start + sizeof(be_len), key.data(),
                  key.size());
    }
    uint32_t row_off = static_cast<uint32_t>(blob_out->size());
    if (!entry.is_deleted) {
      const size_t row_start = blob_out->size();
      blob_out->resize(row_start + row_size);
      entry.row.Serialize(blob_out->data() + row_start);
    } else {
      // Tombstone sentinel: row_off == key_off (see SortedRun::Entry).
      row_off = key_off;
    }
    key_run_out->push_back(key_off);
    row_run_out->push_back(row_off);
    if (deleted_out != nullptr) {
      deleted_out->push_back(entry.is_deleted ? 1 : 0);
    }
    cur.it.Next();
    if (cur.Valid()) {
      heap.push_back(cur);
      std::push_heap(heap.begin(), heap.end(), key_greater);
    }
  }

  // Phase 3: release drained arenas. Shards were sealed together, so their
  // combined contents form one run with one generation bump.
  for (Shard& s : shards_) {
    std::scoped_lock lk(s.lock);
    s.draining = false;
    s.sealed.Clear();
  }
  *generation_out = global_version_mode_ ? NextGlobalVersion() : generation_++;
  // flush_mutex_ is held, so this read is ordered against
  // SetDurabilityCallback's store; (fn, ctx) are always a matched pair.
  if (durability_fn_ != nullptr) {
    durability_fn_(reinterpret_cast<const char*>(blob_out->data()),
                   blob_out->size(), durability_ctx_);
  }
  return Status::kSuccess;
}

}  // namespace tinylamb
