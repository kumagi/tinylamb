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
  key_type_ = key_idx_ < sc.ColumnCount() ? sc.GetColumn(key_idx_).Type()
                                          : ValueType::kNull;
  fixed_key_ =
      key_type_ == ValueType::kInt64 || key_type_ == ValueType::kDouble;
}

StatusOr<Row> MemTable::Get(std::string_view key) const {
  Row row;
  Status st = Lookup(key, &row);
  if (st == Status::kSuccess) {
    return Row(std::move(row));
  }
  if (st == Status::kDeleted) {
    return Status::kNotExists;
  }
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
    if (x != y) {
      return x < y;
    }
  }
  return a < b;
}

inline bool MemKeyEq(std::string_view a, std::string_view b) {
  if (a.size() == 9 && b.size() == 9) {
    uint64_t x = 0;
    uint64_t y = 0;
    std::memcpy(&x, a.data(), sizeof(x));
    std::memcpy(&y, b.data(), sizeof(y));
    if (x != y) {
      return false;
    }
    return static_cast<unsigned char>(a[8]) ==
           static_cast<unsigned char>(b[8]);  // tail byte
  }
  return a == b;
}

// Issue 2525: when a key is rewritten inside one pending generation, move
// the previous version (and its retained history) into the fresh entry's
// `older_versions` chain, newest-first, so Snapshot(V) can still answer a
// pinned read version even though the intermediate write was never separated
// from its successor by a Sync().  Only global-version mode builds chains
// (version 0 = unversioned: no pinned reader can distinguish the two writes,
// so the old in-place replace stays).  Chains are capped at
// kMaxKeptVersionsPerKey -- a hot key churned without Sync must not grow
// unboundedly; history older than the cap is unreachable for pinned reads.
// Caller holds the shard lock.
constexpr size_t kMaxKeptVersionsPerKey = 16;

void PushPriorVersions(SkipList& list, std::string_view key, RowEntry& fresh) {
  if (fresh.version == 0) {
    return;
  }
  RowEntry* prev = list.Find(key);
  if (prev == nullptr) {
    return;
  }
  std::vector<RowEntry> history = std::move(prev->older_versions);
  RowEntry stolen = std::move(*prev);
  stolen.older_versions.clear();  // moved-from; keep the node entry tidy
  std::vector<RowEntry> chain;
  chain.reserve(1 + history.size());
  chain.push_back(std::move(stolen));
  for (auto& old : history) {
    if (chain.size() >= kMaxKeptVersionsPerKey) {
      break;  // drop the oldest retained versions
    }
    chain.push_back(std::move(old));
  }
  fresh.older_versions = std::move(chain);
}
}  // namespace

Status MemTable::Lookup(std::string_view key, Row* row_out) const {
  const Shard& s = ShardFor(key);
  std::scoped_lock lk(s.lock);
  const RowEntry* e = s.active.Find(key);
  if (e == nullptr && s.draining) {
    e = s.sealed.Find(key);
  }
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
// older visible version win the per-key collapse, which a post-hoc filter on
// the collapsed output could never recover. Issue 2525: a node's
// `older_versions` chain participates in the same visibility collapse --
// the newest VISIBLE row among the entry and its retained history is
// emitted. Caller holds the shard lock.
std::vector<std::pair<std::string, RowEntry>> MemTable::SnapshotShard(
    const Shard& s, uint64_t max_visible_version) {
  std::vector<std::pair<std::string, RowEntry>> out;
  out.reserve(s.active.Size() + s.sealed.Size());
  auto ai = s.active.Begin();
  auto si = s.sealed.Begin();
  const auto invisible_entry = [max_visible_version](const RowEntry& e) {
    return e.version != 0 && e.version > max_visible_version;
  };
  // True only when NEITHER the entry nor any retained older version is
  // visible: such a node drops out before the ordering decision so the
  // newest-VISIBLE version wins the tie group.
  const auto node_invisible = [&](const SkipList::ConstIterator& i) {
    if (!invisible_entry(i.Entry())) {
      return false;
    }
    return std::ranges::all_of(i.Entry().older_versions, invisible_entry);
  };
  while (ai.Valid() || si.Valid()) {
    while (ai.Valid() && node_invisible(ai)) {
      ai.Next();
    }
    while (si.Valid() && node_invisible(si)) {
      si.Next();
    }
    if (!ai.Valid() && !si.Valid()) {
      break;
    }
    bool take_active = false;
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
    // `active` is strictly newer: Phase 1 swaps the old active into `sealed`
    // (or keeps an even older leftover sealed at the head), then writes go
    // into a fresh active. The node_invisible skip above already removed any
    // active node whose head AND older_versions are all invisible, so a
    // fully-invisible active node can never mask a visible version that only
    // survives in sealed's chain -- sealed wins the walk in that case.
    const bool dupes_other = other.Valid() && MemKeyEq(other.Key(), it.Key());
    const RowEntry& head = it.Entry();
    const RowEntry* visible = &head;
    if (invisible_entry(head)) {
      visible = nullptr;
      for (const RowEntry& old : head.older_versions) {
        if (!invisible_entry(old)) {
          visible = &old;  // chain is newest-first: first hit wins
          break;
        }
      }
    }
    if (visible != nullptr) {
      out.emplace_back(std::string(it.Key()), *visible);
    }
    it.Next();
    if (dupes_other) {
      other.Next();
    }
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
    std::pair<std::string, RowEntry>* begin;
    std::pair<std::string, RowEntry>* end;
    [[nodiscard]] bool Valid() const { return begin != end; }
  };
  std::vector<Cursor> cursors;
  cursors.reserve(static_cast<size_t>(kNumShards));
  for (auto& v : per_shard) {
    if (!v.empty()) {
      cursors.push_back(Cursor{.begin = v.data(), .end = v.data() + v.size()});
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
    out.push_back(std::move(*cur.begin));
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
  if (e == nullptr && s.draining) {
    e = s.sealed.Find(key);
  }
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
  // A short row would Serialize fine but fail DeserializeBounded on readback
  // (count != schema.ColumnCount()), turning the flushed run corrupt.
  if (r.values_.size() != schema_.ColumnCount()) {
    return Status::kCorruption;
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
    RowEntry entry(r, false, v);
    PushPriorVersions(s.active, key, entry);
    s.active.Upsert(key, std::move(entry));
  };
  if (fixed_key_) {
    // A fixed-width key always encodes to 9 memcomparable bytes, and the
    // encode reduces to one inline call on the common type match (the gate
    // above already rejected a NULL key cell). Keeps the ingest loop off
    // both the heap and the encode call chain.
    std::array<char, 9> kbuf{};
    const Value& kv = r.values_[key_idx_];
    if (kv.type == ValueType::kVarChar) {
      // A varchar encodes to 9 bytes PER 8-char group, so it cannot use the
      // 9-byte stack buffer. Delegate to the general path. That path also
      // encodes by the value's *runtime* type
      // (Row::EncodeKeyMemcomparableFormat only uses the schema to locate the
      // key column, then dispatches on Value::type), so this schema/value
      // mismatch yields the identical key image on both routes -- no
      // fast/general key-order divergence.
      put_entry(r.EncodeKeyMemcomparableFormat(schema_));
    } else {
      size_t klen = 0;
      if (kv.type == key_type_) {
        klen = key_type_ == ValueType::kInt64
                   ? (kv.IsUnsigned() ? EncodeMemcomparableFormatUint64To(
                                            kbuf.data(), kv.value.int_value)
                                      : EncodeMemcomparableFormatIntegerTo(
                                            kbuf.data(), kv.value.int_value))
                   : EncodeMemcomparableFormatDoubleTo(kbuf.data(),
                                                       kv.value.double_value);
      } else {
        // The remaining mismatch (kInt64 vs kDouble) still encodes to
        // exactly 9 bytes and matches the general path, which encodes by
        // value type.
        klen = kv.EncodeMemcomparableFormatTo(kbuf.data());
      }
      put_entry(std::string_view(kbuf.data(), klen));
    }
  } else {
    put_entry(r.EncodeKeyMemcomparableFormat(schema_));
  }
  if (sync) {
    // T4191 contract: sync=true means "the durability hook has run by the
    // time we return". The run outputs are DISCARDED here on purpose --
    // no in-process caller consumes them on this path (only the model
    // fuzzer drives it). A future sync-to-SortedRun design must extend
    // Put/Delete to hand the blob/runs back; it cannot assume they are
    // kept by this branch. See memtable.hpp Sync docs.
    std::vector<char> blob;
    std::vector<uint32_t> kr;
    std::vector<uint32_t> rr;
    size_t gen = 0;
    Status st = Sync(&blob, &kr, &rr, &gen);
    if (st != Status::kSuccess) {
      return st;
    }
  }
  return Status::kSuccess;
}

Status MemTable::Delete(std::string_view key, bool sync) {
  Shard& s = ShardFor(key);
  {
    std::scoped_lock lk(s.lock);
    const uint64_t v = global_version_mode_ ? NextGlobalVersion() : 0;
    RowEntry entry(Row{}, true, v);
    PushPriorVersions(s.active, key, entry);
    s.active.Upsert(key, std::move(entry));
  }
  if (sync) {
    // T4191: same contract as Put -- durability fired, run outputs
    // intentionally discarded (memtable.hpp Sync docs).
    std::vector<char> blob;
    std::vector<uint32_t> kr;
    std::vector<uint32_t> rr;
    size_t gen = 0;
    Status st = Sync(&blob, &kr, &rr, &gen);
    if (st != Status::kSuccess) {
      return st;
    }
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
    if (deleted_out != nullptr) {
      deleted_out->clear();
    }
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
    [[nodiscard]] bool Valid() const { return it.Valid(); }
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
  if (deleted_out != nullptr) {
    deleted_out->clear();
  }
  blob_out->reserve(blob_out->size() + (pending * 64));
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
      if (xb != yb) {
        return yb < xb;  // min-heap via greater-than
      }
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
    if (s.sealed.Empty()) {
      continue;
    }
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
    // is fixed at UINT32_MAX (the uint32-offset ceiling): drain_budget_ has
    // no setter, so the kNoSpace branch below is a defensive guard rather
    // than a path the current tests exercise. The clamp keeps the ceiling
    // explicit if injection is added later: a larger budget (e.g. SIZE_MAX
    // "to disable" the cap) would let blob_out grow past 4 GiB and wrap the
    // uint32 key/row offsets into a corrupt run.
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
      if (deleted_out != nullptr) {
        deleted_out->clear();
      }
      return Status::kNoSpace;
    }
    auto key_off = static_cast<uint32_t>(blob_out->size());
    uint32_t be_len = htobe32(static_cast<uint32_t>(key.size()));
    const size_t key_start = blob_out->size();
    blob_out->resize(key_start + sizeof(uint32_t) + key.size());
    std::memcpy(blob_out->data() + key_start, &be_len, sizeof(be_len));
    if (!key.empty()) {
      std::memcpy(blob_out->data() + key_start + sizeof(be_len), key.data(),
                  key.size());
    }
    auto row_off = static_cast<uint32_t>(blob_out->size());
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

  // Durability must precede the release of the drained arenas: if the hook
  // fails, the sealed lists have to stay intact so a retry can re-drain and
  // re-persist the same entries. Clearing them first made a failed hook
  // unrecoverable -- the data existed only in the returned blob.
  // flush_mutex_ is held, so this read is ordered against
  // SetDurabilityCallback's store; (fn, ctx) are always a matched pair.
  if (durability_fn_ != nullptr) {
    try {
      durability_fn_(reinterpret_cast<const char*>(blob_out->data()),
                     blob_out->size(), durability_ctx_);
    } catch (const std::exception& e) {
      return StatusError(
          StatusCode::kIOError,
          std::string("WAL durability hook failed: ") + e.what());
    } catch (...) {
      return StatusError(StatusCode::kIOError, "WAL durability hook failed");
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
  return Status::kSuccess;
}

}  // namespace tinylamb
