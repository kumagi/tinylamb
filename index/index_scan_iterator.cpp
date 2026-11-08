/**
 * Copyright 2023 KUMAZAKI Hiroki
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

#include "index_scan_iterator.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/decoder.hpp"
#include "common/status_or.hpp"
#include "index/index.hpp"
#include "index/lsm_index.hpp"
#include "index_schema.hpp"
#include "page/page_manager.hpp"
#include "table/iterator_base.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

std::string EncodeParts(const std::vector<Value>& parts) {
  std::string encoded;
  for (const Value& value : parts) {
    if (value.IsNull()) {
      break;
    }
    encoded += value.EncodeMemcomparableFormat();
  }
  return encoded;
}

std::string EncodeEndParts(const Index& index,
                           const std::vector<Value>& parts) {
  std::string encoded = EncodeParts(parts);
  if (!encoded.empty() && parts.size() < index.sc_.key_.size()) {
    encoded.push_back(static_cast<char>(0xff));
  }
  return encoded;
}

Value FirstOrNull(const std::vector<Value>& parts) {
  return parts.empty() ? Value() : parts.front();
}

// The decoded key image holds exactly the index key columns in key order;
// any NULL part means the entry was stored through the multi-value list
// encoding (see Table::IndexInsert), even on a kUnique index.
bool KeyImageHasNull(const Row& key_image) {
  return std::ranges::any_of(key_image.values_,
                             [](const Value& value) { return value.IsNull(); });
}

TableLease AcquireTableLease(const Table& table, Transaction& txn) {
  StatusOr<TableLease> lease = table.AcquireLease(txn.ID());
  return lease.HasValue() ? lease.MoveValue() : TableLease();
}

TransactionManager::IndexReadGuard AcquireIteratorIndexReadGuard(
    Transaction& txn, const Index& index, bool resolve_head,
    TransactionManager::IndexReadGuard* external_guard) {
  if (external_guard != nullptr || resolve_head) {
    return {};
  }
  return txn.AcquireIndexReadGuard(index.Root());
}

// The common no-INCLUDE entry is a fixed-width RowPosition followed by the
// empty Row's uint64 element count. Preserve the general decoder for payloads
// with INCLUDE values (and malformed/truncated records).
bool DecodePositionOnly(std::string_view encoded, IndexValueType* value) {
  constexpr size_t kSize = RowPosition::Size() + sizeof(uint64_t);
  if (encoded.size() != kSize) {
    return false;
  }
  uint64_t columns = 0;
  DeserializeU64(encoded.data() + RowPosition::Size(), &columns);
  if (columns != 0) {
    return false;
  }
  value->pos.Deserialize(
      encoded.data());  // NOLINT(bugprone-suspicious-stringview-data-usage)
  value->include.Clear();
  return true;
}

StatusOr<IndexValueType> DecodeIndexValue(std::string_view encoded) {
  IndexValueType value;
  if (DecodePositionOnly(encoded, &value)) {
    return value;
  }
  return Decode<IndexValueType>(encoded);
}

StatusOr<std::vector<IndexValueType>> DecodeIndexValues(
    std::string_view encoded) {
  constexpr size_t kSize = RowPosition::Size() + sizeof(uint64_t);
  if (encoded.size() >= sizeof(uint64_t)) {
    uint64_t count = 0;
    DeserializeU64(
        encoded.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
        &count);
    const std::string_view entries = encoded.substr(sizeof(uint64_t));
    if (count <= (1 << 20) && count == entries.size() / kSize &&
        entries.size() % kSize == 0) {
      std::vector<IndexValueType> values(static_cast<size_t>(count));
      bool positions_only = true;
      for (size_t i = 0; i < values.size(); ++i) {
        if (!DecodePositionOnly(entries.substr(i * kSize, kSize), &values[i])) {
          positions_only = false;
          break;
        }
      }
      if (positions_only) {
        return values;
      }
    }
  }
  return Decode<std::vector<IndexValueType>>(encoded);
}

}  // namespace

bool IndexScanIterator::StoredAsSingleValue() const {
  return is_unique_ && !KeyImageHasNull(keys_);
}

IndexScanIterator::IndexScanIterator(
    const Table& table, const Index& index, Transaction& txn,
    const Value& begin, const Value& end, bool ascending, bool resolve_head,
    TransactionManager::IndexReadGuard* index_read_guard)
    : IndexScanIterator(
          table, index, txn,
          begin.IsNull() ? std::vector<Value>{} : std::vector<Value>{begin},
          end.IsNull() ? std::vector<Value>{} : std::vector<Value>{end},
          ascending, resolve_head, index_read_guard) {}

IndexScanIterator::IndexScanIterator(
    const Table& table, const Index& index, Transaction& txn,
    const std::vector<Value>& begin_key, const std::vector<Value>& end_key,
    bool ascending, bool resolve_head,
    TransactionManager::IndexReadGuard* index_read_guard)
    : table_(table),
      index_(index),
      txn_(txn),
      begin_(FirstOrNull(begin_key)),
      end_(FirstOrNull(end_key)),
      ascending_(ascending),
      resolve_head_(resolve_head),
      is_unique_(index.StoresSingleValue()),
      table_lease_(AcquireTableLease(table, txn)),
      index_read_lock_(AcquireIteratorIndexReadGuard(txn, index, resolve_head,
                                                     index_read_guard)),
      bpt_(index.Root()),
      iter_() {
  if (!table_lease_.Valid()) {
    status_ = Status::kConflicts;
    return;
  }
  iter_ = BPlusTreeIterator(&bpt_, &txn, EncodeParts(begin_key),
                            EncodeEndParts(index, end_key), ascending);
  if (!iter_.IsValid()) {
    status_ = iter_.GetStatus();
    return;
  }
  // A complete equality seek reads one key: copying its neighbours would
  // add work to every OLTP lookup. Prefix/range walks amortize the snapshot.
  const bool exact_point =
      begin_key.size() == index.sc_.key_.size() && begin_key == end_key;
  if (!resolve_head_ && !exact_point &&
      (index_read_lock_.HoldsLock() ||
       ((index_read_guard != nullptr) && index_read_guard->HoldsLock()))) {
    iter_.EnableStableLeafCache();
    cache_heap_images_ = txn_.IsReadOnly();
  }
  UpdateIteratorState();
}

bool IndexScanIterator::IsValid() const {
  return !invalidated_ && iter_.IsValid();
}

std::string IndexScanIterator::GetValue() const { return iter_.Value(); }

RowPosition IndexScanIterator::Position() const {
  if (!IsValid()) {
    return {};
  }
  return pos_;
}

StatusOr<std::vector<Row>> IndexScanIterator::PartitionBoundaries(
    const Index& index, Transaction& txn, const std::vector<Value>& begin_key,
    const std::vector<Value>& end_key, size_t leaves_per_partition) {
  BPlusTree tree(index.Root());
  ASSIGN_OR_RETURN(std::vector<std::string>, encoded,
                   tree.ScanPartitionBoundaries(txn, EncodeParts(begin_key),
                                                EncodeEndParts(index, end_key),
                                                leaves_per_partition));
  std::vector<Row> boundaries;
  boundaries.reserve(encoded.size());
  for (std::string& key : encoded) {
    Row row;
    row.DecodeMemcomparableFormat(key);
    boundaries.push_back(std::move(row));
  }
  return boundaries;
}

bool IndexScanIterator::EncodedKeyMayOverlap(
    const Index& index, std::string_view key,
    const std::vector<Value>& begin_key, const std::vector<Value>& end_key) {
  if (key.empty() || begin_key.size() > index.sc_.key_.size() ||
      end_key.size() > index.sc_.key_.size() ||
      std::ranges::any_of(begin_key, &Value::IsNull) ||
      std::ranges::any_of(end_key, &Value::IsNull)) {
    return true;
  }
  const std::string begin = EncodeParts(begin_key);
  const std::string end = EncodeEndParts(index, end_key);
  return (begin.empty() || key >= begin) && (end.empty() || key <= end);
}

Status IndexScanIterator::SetProjection(const std::vector<slot_t>& columns) {
  if (resolve_head_ || index_.RetainsDeletedEntries()) {
    return Status::kInvalidArgument;
  }
  for (slot_t column : columns) {
    if (column >= table_.GetSchema().ColumnCount()) {
      return Status::kInvalidArgument;
    }
  }
  projection_ = columns;
  current_row_resolved_ = false;
  current_row_.Clear();
  return Status::kSuccess;
}

void IndexScanIterator::Clear() {
  // A cleared iterator must behave as exhausted: Position()/operator++ on it
  // used to leak a valid-looking {0,0} row position to IndexScan::Next.
  invalidated_ = true;
  pos_ = RowPosition();
  keys_.Clear();
  include_.Clear();
  current_row_.Clear();
  value_list_.clear();
  entry_loaded_ = false;
}

void IndexScanIterator::UpdateIteratorState() {
  if (!iter_.IsValid()) {
    if (iter_.GetStatus() != Status::kSuccess) {
      status_ = iter_.GetStatus();
    }
    Clear();
    return;
  }
  if (!entry_loaded_ || resolve_head_) {
    std::string key_copy;
    const std::string_view encoded_key =
        iter_.stable_leaf_cache_
            ? (iter_.EnsureLeafCache() ? iter_.CachedKey(iter_.idx_)
                                       : std::string_view{})
            : std::string_view(key_copy = iter_.Key());
    if (iter_.GetStatus() != Status::kSuccess) {
      status_ = iter_.GetStatus();
      Clear();
      return;
    }
    const Status decoded = keys_.TryDecodeMemcomparableFormat(encoded_key);
    if (decoded != Status::kSuccess) {
      status_ = decoded;
      Clear();
      return;
    }
    std::string value_copy;
    const std::string_view encoded_value =
        iter_.stable_leaf_cache_
            ? (iter_.EnsureLeafCache() ? iter_.CachedValue(iter_.idx_)
                                       : std::string_view{})
            : std::string_view(value_copy = iter_.Value());
    if (iter_.GetStatus() != Status::kSuccess) {
      status_ = iter_.GetStatus();
      Clear();
      return;
    }
    if (StoredAsSingleValue()) {
      auto value = DecodeIndexValue(encoded_value);
      if (!value.HasValue()) {
        status_ = value.GetStatus();
        Clear();
        return;
      }
      value_offset_ = -1;
      value_list_.clear();
      pos_ = value.Value().pos;
      include_ = value.Value().include;
    } else {
      constexpr size_t kSize = RowPosition::Size() + sizeof(uint64_t);
      if (distinct_keys_mode_ && ascending_ && !resolve_head_ &&
          !index_.RetainsDeletedEntries() &&
          encoded_value.size() >= sizeof(uint64_t) + kSize) {
        uint64_t count = 0;
        DeserializeU64(
            encoded_value
                .data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
            &count);
        const std::string_view entries = encoded_value.substr(sizeof(uint64_t));
        if (count > 0 && count <= (1 << 20) &&
            count == entries.size() / kSize && entries.size() % kSize == 0) {
          IndexValueType first_val;
          if (DecodePositionOnly(entries.substr(0, kSize), &first_val) &&
              !txn_.HasPageVersionChains(first_val.pos.page_id)) {
            value_list_.resize(1);
            value_list_[0] = first_val;
            value_offset_ = 0;
            pos_ = first_val.pos;
            include_.Clear();
            ++value_lists_decoded_;
            value_entries_decoded_ += static_cast<size_t>(count);
            entry_loaded_ = true;
            return;
          }
        }
      }
      auto values = DecodeIndexValues(encoded_value);
      if (!values.HasValue()) {
        status_ = values.GetStatus();
        Clear();
        return;
      }
      value_list_ = values.MoveValue();
      ++value_lists_decoded_;
      value_entries_decoded_ += value_list_.size();
    }
    entry_loaded_ = true;
  }
  if (StoredAsSingleValue()) {
    return;
  }
  if (value_offset_ < 0) {
    value_offset_ =
        ascending_ ? 0 : static_cast<int64_t>(value_list_.size()) - 1;
  }
  if (value_list_.empty() || value_offset_ < 0 ||
      static_cast<size_t>(value_offset_) >= value_list_.size()) {
    Clear();
    return;
  }
  const auto& value = value_list_[static_cast<size_t>(value_offset_)];
  pos_ = value.pos;
  include_ = value.include;
}

void IndexScanIterator::ResolveRow() const {
  if (cache_heap_images_ && heap_image_page_ == pos_.page_id &&
      pos_.slot < heap_image_slots_.size()) {
    const auto [offset, size] = heap_image_slots_[pos_.slot];
    DecodeVisibleRow(std::string_view(heap_images_).substr(offset, size));
    return;
  }
  StatusOr<PageRef> ref = txn_.GetPageManager()->GetPage(pos_.page_id, true);
  if (!ref.HasValue()) {
    // const method: record the failure through the mutable sticky status.
    // Clear the cached row as well: operator* must not re-emit the previous
    // row once resolution failed.
    const_cast<IndexScanIterator*>(this)->status_ = ref.GetStatus();
    current_row_.Clear();
    const_cast<IndexScanIterator*>(this)->Clear();
    current_row_resolved_ = true;
    return;
  }
  if (!ref.Value().IsValid() || ref.Value().IsNull() ||
      ref.Value()->Type() != PageType::kRowPage) {
    const_cast<IndexScanIterator*>(this)->status_ = Status::kCorrupt;
    current_row_.Clear();
    const_cast<IndexScanIterator*>(this)->Clear();
    current_row_resolved_ = true;
    return;
  }
  const bool repeated_page = last_heap_page_ == pos_.page_id;
  last_heap_page_ = pos_.page_id;
  if (cache_heap_images_ && repeated_page &&
      !txn_.HasPageVersionChains(pos_.page_id)) {
    // Check under the shared page latch, just like PhysicalReadEligible in
    // FullScanIterator. Chain-free physical images are visible to every live
    // snapshot; copy them while the latch excludes an arriving writer. Own
    // writes and head-resolution scans never enable this cache.
    const RowPage& page = ref.Value()->body.row_page;
    heap_images_.clear();
    heap_images_.reserve(kPageBodySize);
    heap_image_slots_.resize(page.RowMax());
    for (slot_t slot = 0; slot < page.RowMax(); ++slot) {
      const std::string_view image = page.GetRow(slot);
      heap_image_slots_[slot] = {heap_images_.size(), image.size()};
      heap_images_.append(image);
    }
    heap_image_page_ = pos_.page_id;
    ++heap_page_images_copied_;
    if (pos_.slot < heap_image_slots_.size()) {
      const auto [offset, size] = heap_image_slots_[pos_.slot];
      DecodeVisibleRow(std::string_view(heap_images_).substr(offset, size));
      return;
    }
  }
  StatusOr<std::string_view> row =
      ref.Value()->Read(txn_, pos_.slot, resolve_head_);
  if (!row.HasValue()) {
    if (row.GetStatus() != Status::kNotExists) {
      const_cast<IndexScanIterator*>(this)->status_ = row.GetStatus();
      const_cast<IndexScanIterator*>(this)->Clear();
    }
    current_row_.Clear();
    current_row_resolved_ = true;
    return;
  }
  DecodeVisibleRow(row.Value());
}

void IndexScanIterator::DecodeVisibleRow(std::string_view image) const {
  current_row_resolved_ = true;
  if (image.empty()) {
    current_row_.Clear();
    return;
  }
  // Keep fail-stop/error propagation even when the bytes came from our owned
  // page image. ReadVersion's idle-shard fast path is just atomic loads; on a
  // page changed since the copy it still resolves this fixed snapshot.
  if (cache_heap_images_ && heap_image_page_ == pos_.page_id) {
    auto visible = txn_.ReadVersion(pos_, image, false);
    if (!visible.HasValue()) {
      if (visible.GetStatus() != Status::kNotExists) {
        const_cast<IndexScanIterator*>(this)->status_ = visible.GetStatus();
        const_cast<IndexScanIterator*>(this)->Clear();
      }
      current_row_.Clear();
      return;
    }
    image = visible.Value();
  }
  for (const IntegerPeekCompare& peek : integer_peeks_) {
    const auto value = Row::TryPeekInteger(
        image.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
        table_.schema_, peek.column);
    if (!value || !MatchIntegerCompare(*value, peek.op, peek.constant)) {
      current_row_.Clear();
      return;
    }
  }
  // Only register rows that actually exist so the read set stays clean.
  txn_.AddReadSet(pos_);
  // Length-prefixed page images: decoded by offsets, never as C strings.
  if (projection_) {
    current_row_.DeserializeProjected(
        image.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
        table_.schema_, *projection_);
  } else {
    current_row_.Deserialize(
        image.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
        table_.schema_);
  }
  ++rows_decoded_;
  if (index_.RetainsDeletedEntries() &&
      index_.GenerateKey(current_row_) != iter_.Key()) {
    // A retained entry may point at a row whose indexed key was changed by a
    // newer visible version.  It is valid for an old snapshot, but not this
    // one.  Rechecking here also keeps direct BeginIndexScan callers correct
    // when no SQL predicate is available above the iterator.
    current_row_.Clear();
  }
  current_row_resolved_ = true;
}

IteratorBase& IndexScanIterator::operator++() {
  current_row_resolved_ = false;
  if (!IsValid()) {
    // Guard against advancing past the end (IndexScanIterator::operator++ is
    // called unconditionally by some loops).
    return *this;
  }
  if (!ascending_) {
    return operator--();
  }
  if (StoredAsSingleValue()) {
    ++iter_;
    entry_loaded_ = false;
  } else {
    if (resolve_head_) {
      // A streaming DML consumer may have removed the current entry. Read
      // its new length before stepping, without rejecting the old offset:
      // an emptied list must advance to the next key rather than end the scan.
      auto values = DecodeIndexValues(iter_.Value());
      if (!values.HasValue()) {
        status_ = values.GetStatus();
        Clear();
        return *this;
      }
      value_list_ = values.MoveValue();
      ++value_lists_decoded_;
      value_entries_decoded_ += value_list_.size();
    }
    ++value_offset_;
    if (std::cmp_less_equal(value_list_.size(), value_offset_)) {
      ++iter_;
      entry_loaded_ = false;
      value_offset_ = -1;
    }
  }
  UpdateIteratorState();
  return *this;
}

void IndexScanIterator::AdvanceToNextKey() {
  if (!IsValid()) {
    return;
  }
  current_row_resolved_ = false;
  if (ascending_) {
    ++iter_;
  } else {
    --iter_;
  }
  entry_loaded_ = false;
  value_offset_ = -1;
  UpdateIteratorState();
}

IteratorBase& IndexScanIterator::operator--() {
  current_row_resolved_ = false;
  if (!IsValid()) {
    return *this;
  }
  // NOTE: despite the name, operator-- on a descending scan steps toward
  // SMALLER keys, the same direction as operator++ (see ScanDescending /
  // NonUniqueDescending tests, which walk a DESC scan with --it from the
  // upper bound down to the lower bound).  The two operators are not
  // inverses here: ++ advances the scan in its iteration order and --
  // retreats one step in scan order, and for a DESC scan both move toward
  // smaller keys.  Do not "fix" this into a mirror of operator++ without
  // updating those tests and every caller that relies on it.
  if (StoredAsSingleValue()) {
    --iter_;
    entry_loaded_ = false;
  } else {
    if (0 < value_offset_) {
      --value_offset_;
    } else {
      --iter_;
      entry_loaded_ = false;
      value_offset_ = -1;
    }
  }
  UpdateIteratorState();
  return *this;
}

const Row& IndexScanIterator::operator*() const {
  if (!current_row_resolved_) {
    ResolveRow();
  }
  return current_row_;
}

Row& IndexScanIterator::operator*() {
  if (!current_row_resolved_) {
    ResolveRow();
  }
  return current_row_;
}

bool IndexScanIterator::TryReadIndexOnly(Row* dst) const {
  if (!IsValid() || index_.RetainsDeletedEntries()) {
    return false;
  }
  static const bool visibility_map_disabled =
      std::getenv("TINYLAMB_DISABLE_INDEX_ONLY_VISIBILITY_MAP") != nullptr;
  if (visibility_map_disabled) {
    return false;
  }
  // Why the entry alone is enough:
  //  * The caller's stale-key gate (checked before AND after taking this
  //    scan's index read guard) proved every committed index mutation is
  //    visible to this snapshot, and the guard keeps INCLUDE payloads and
  //    entry membership frozen for the scan's lifetime, so a present entry
  //    in a non-retained index points at a live row.
  //  * The write-before-latch / GC-drop invariant full scans trust
  //    (PhysicalReadEligible in table/full_scan_iterator.cpp) says a page
  //    with no version chains carries only committed images that every
  //    snapshot sees.  Same-key updates rewrite the entry's INCLUDE payload
  //    when they install the new committed image, so the payload is exactly
  //    that image.
  //  * A chain appearing mid-scan belongs to a writer whose commit postdates
  //    this snapshot: the entry's pre-write image is what the snapshot must
  //    see, so the memoized verdict cannot serve a wrong version.
  const page_id_t page_id = pos_.page_id;
  if (page_id != chain_checked_page_) {
    chain_checked_free_ = !txn_.HasPageVersionChains(page_id);
    chain_checked_page_ = page_id;
  }
  if (!chain_checked_free_) {
    return false;
  }
  // Batch consumers keep the Row and move only its cells. Reuse that
  // vector's capacity instead of allocating a concatenation for every
  // visible index entry; the returned Values remain owned copies.
  dst->values_.resize(keys_.Count() + include_.Count());
  std::copy(keys_.values_.begin(), keys_.values_.end(), dst->values_.begin());
  std::copy(include_.values_.begin(), include_.values_.end(),
            dst->values_.begin() + static_cast<ptrdiff_t>(keys_.Count()));
  if (!dst->IsValid()) {
    return false;
  }
  txn_.RecordIndexOnlyVisibleServe();
  return true;
}

void IndexScanIterator::Dump(std::ostream& o, int /*indent*/) const {
  o << index_.sc_.name_ << " on " << table_.GetSchema().Name() << ": {";
  for (size_t i = 0; i < index_.sc_.key_.size(); ++i) {
    if (0 < i) {
      o << ", ";
    }
    o << index_.sc_.key_[i];
  }
  o << "},";
  if (!index_.sc_.include_.empty()) {
    o << " Include: {";
    for (size_t i = 0; i < index_.sc_.include_.size(); ++i) {
      if (0 < i) {
        o << ", ";
      }
      o << index_.sc_.include_[i];
    }
    o << "},";
  }
  o << " [";
  if (begin_ == end_) {
    o << begin_;
  } else if (ascending_) {
    o << begin_ << " -> " << end_;
  } else {
    o << end_ << " -> " << begin_;
  }
  o << "]";
}
}  // namespace tinylamb
