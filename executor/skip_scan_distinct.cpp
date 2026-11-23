/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/skip_scan_distinct.hpp"

#include <cassert>
#include <cstddef>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/decoder.hpp"
#include "common/exception_status.hpp"
#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "expression/expression.hpp"
#include "index/index.hpp"
#include "index/lsm_index.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

std::string EncodeParts(const std::vector<Value>& parts) {
  std::string encoded;
  // Each value contributes a memcomparable image (several bytes), not a
  // single byte, so reserve a per-value estimate to avoid growth copies.
  encoded.reserve(parts.size() * 16);
  for (const Value& value : parts) {
    // NULL has a valid memcomparable encoding (the smallest marker). Breaking
    // at the first NULL truncates a composite prefix and makes the subsequent
    // seek skip whole DISTINCT groups.
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

// A kUnique key carrying a NULL is stored as a multi-value list (SQL says
// NULL != NULL, see Table::IndexInsert), so it must decode as one.
bool KeyImageHasNull(const Row& key_image) {
  return std::ranges::any_of(key_image.values_,
                             [](const Value& value) { return value.IsNull(); });
}

}  // namespace

SkipScanDistinct::SkipScanDistinct(
    Transaction& txn, const Table& table, const Index& index,
    const Value& begin, const Value& end, bool ascending, Expression where,
    Schema sc, size_t prefix_cols,
    TransactionManager::IndexReadGuard index_read_guard)
    : SkipScanDistinct(
          txn, table, index,
          begin.IsNull() ? std::vector<Value>{} : std::vector<Value>{begin},
          end.IsNull() ? std::vector<Value>{} : std::vector<Value>{end},
          ascending, std::move(where), std::move(sc), prefix_cols,
          std::move(index_read_guard)) {}

SkipScanDistinct::SkipScanDistinct(
    Transaction& txn, const Table& table, const Index& index,
    const std::vector<Value>& begin_key, const std::vector<Value>& end_key,
    bool ascending, Expression where, Schema sc, size_t prefix_cols,
    TransactionManager::IndexReadGuard index_read_guard)
    : txn_(txn),
      table_(table),
      index_(index),
      begin_key_(begin_key),
      end_key_(end_key),
      ascending_(ascending),
      where_(std::move(where)),
      schema_(std::move(sc)),
      prefix_cols_(prefix_cols),
      is_unique_(index.StoresSingleValue()),
      index_read_guard_(index_read_guard.HoldsLock()
                            ? std::move(index_read_guard)
                            : txn.AcquireIndexReadGuard(index.Root())),
      bpt_(index.Root()),
      iter_(&bpt_, &txn_, EncodeParts(begin_key_),
            EncodeEndParts(index_, end_key_), ascending_) {}

Status SkipScanDistinct::ResolveCurrentRow() {
  current_row_.Clear();
  const auto decode = [this](std::string_view image) -> Status {
    try {
      if (current_row_.DeserializeBounded(image.data(), image.size(),
                                          table_.GetSchema()) != image.size()) {
        current_row_.Clear();
        return StatusError(StatusCode::kCorrupt, "trailing bytes in row image");
      }
    } catch (const std::runtime_error& error) {
      current_row_.Clear();
      return StatusError(StatusCode::kCorrupt, error.what());
    } catch (...) {
      current_row_.Clear();
      return StatusFromCurrentException("could not decode DISTINCT row");
    }
    return Status::kSuccess;
  };
  StatusOr<PageRef> ref =
      txn_.GetPageManager()->GetPage(current_pos_.page_id, true);
  if (!ref.HasValue()) {
    return ref.GetStatus();
  }
  if (!ref.Value().IsValid()) {
    return Status::kSuccess;
  }
  if (ref.Value()->Type() != PageType::kRowPage) {
    return StatusError(StatusCode::kCorrupt,
                       "DISTINCT index entry points to a non-row page");
  }
  if (ref.Value()->body.row_page.IsColumnarStub()) {
    ref.Value().PageUnlock();
    std::string stub_image;
    const Status ds = table_.ReadEncodedFromStub(current_pos_, &stub_image);
    if (ds != Status::kSuccess) {
      return ds == Status::kNotExists ? Status(Status::kSuccess) : ds;
    }
    const StatusOr<std::string_view> visible =
        txn_.ReadVersion(current_pos_, std::string_view(stub_image));
    if (!visible.HasValue()) {
      return visible.GetStatus() == Status::kNotExists
                 ? Status(Status::kSuccess)
                 : visible.GetStatus();
    }
    return decode(visible.Value());
  }
  StatusOr<std::string_view> row = ref.Value()->Read(txn_, current_pos_.slot);
  if (!row.HasValue()) {
    // Stale index entries may legitimately name a row invisible to this
    // snapshot. Other storage failures must not turn into missing groups.
    return row.GetStatus() == Status::kNotExists ? Status(Status::kSuccess)
                                                 : row.GetStatus();
  }
  return decode(row.Value());
}

void SkipScanDistinct::SeekNextDistinct(const std::string& /*prev_encoded_key*/,
                                        const Row& prev_key) {
  if (prefix_cols_ == 0 || prefix_cols_ >= index_.sc_.key_.size()) {
    // Distinct on the entire index key:
    // Advancing the LsmIndexIterator jumps directly past all duplicate rows
    // stored under this key entry!
    if (ascending_) {
      ++iter_;
    } else {
      --iter_;
    }
    return;
  }

  // Prefix distinct: jump past all entries sharing the same leading prefix.
  if (ascending_) {
    ++iter_;
    while (iter_.IsValid()) {
      Row next_key;
      const Status decoded = next_key.TryDecodeMemcomparableFormat(iter_.Key());
      if (decoded != Status::kSuccess) {
        // A truncated/corrupt key image cannot define the next prefix
        // boundary. Stop instead of seeking on a half-decoded key, which
        // would skip or duplicate whole DISTINCT groups.
        finished_ = true;
        FailWith(decoded);
        return;
      }
      bool prefix_equal = true;
      for (size_t col = 0; col < prefix_cols_ && col < prev_key.Count() &&
                           col < next_key.Count();
           ++col) {
        if (prev_key[col] != next_key[col]) {
          prefix_equal = false;
          break;
        }
      }
      if (!prefix_equal) {
        break;
      }
      // Same prefix, seek past this prefix using LsmIndex seek with prefix
      // upper bound!
      std::vector<Value> prefix_values;
      prefix_values.reserve(prefix_cols_);
      for (size_t col = 0; col < prefix_cols_ && col < prev_key.Count();
           ++col) {
        prefix_values.push_back(prev_key[col]);
      }
      std::string seek_after = EncodeParts(prefix_values);
      seek_after.push_back(static_cast<char>(0xff));
      iter_ = BPlusTreeIterator(&bpt_, &txn_, seek_after,
                                EncodeEndParts(index_, end_key_), ascending_);
    }
  } else {
    std::vector<Value> prefix_values;
    prefix_values.reserve(prefix_cols_);
    for (size_t col = 0; col < prefix_cols_ && col < prev_key.Count(); ++col) {
      prefix_values.push_back(prev_key[col]);
    }
    const std::string prefix_begin = EncodeParts(prefix_values);
    (void)iter_.SeekBefore(prefix_begin);
  }
}

size_t SkipScanDistinct::NextBatch(DataChunk* destination, size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  // ExecutorBase::NextBatch contract: the destination is reset per batch.
  // Without it, consumers that reuse an input chunk (Projection) append on
  // top of the previous batch and re-emit every stale row once the DISTINCT
  // result exceeds one batch.
  destination->Reset();
  while (destination->Size() < max_rows && !finished_ && iter_.IsValid()) {
    if (iter_.Value().empty()) {
      if (ascending_) {
        ++iter_;
      } else {
        --iter_;
      }
      continue;
    }
    current_index_key_.Clear();
    const Status decoded =
        current_index_key_.TryDecodeMemcomparableFormat(iter_.Key());
    if (decoded != Status::kSuccess) {
      // Same contract as the seek sites above: an undecodable key image
      // cannot define the current position or the next seek boundary.
      finished_ = true;
      FailWith(decoded);
      // Error ⇒ void batch (FullScan/DistinctExecutor convention): drop any
      // rows already appended so an early-stopping parent (e.g. LIMIT) cannot
      // emit a partial DISTINCT result without ever observing the error.
      destination->Reset();
      return 0;
    }

    bool found_visible = false;
    auto check_candidate = [&](const RowPosition& pos) -> StatusOr<bool> {
      current_pos_ = pos;
      RETURN_IF_FAIL(ResolveCurrentRow());
      if (!current_row_.IsValid()) {
        return false;
      }
      if (where_) {
        StatusOr<Value> res = where_->TryEvaluate(current_row_, schema_);
        if (!res.HasValue()) {
          return res.GetStatus();
        }
        if (!res.Value().Truthy()) {
          return false;
        }
      }
      return true;
    };

    if (is_unique_ && !KeyImageHasNull(current_index_key_)) {
      StatusOr<Table::IndexValueType> val =
          Decode<Table::IndexValueType>(iter_.Value());
      if (!val.HasValue()) {
        finished_ = true;
        FailWith(val.GetStatus());
        destination->Reset();
        return 0;
      }
      StatusOr<bool> ok = check_candidate(val.Value().pos);
      if (!ok.HasValue()) {
        finished_ = true;
        FailWith(ok.GetStatus());
        destination->Reset();
        return 0;
      }
      found_visible = ok.Value();
    } else {
      StatusOr<std::vector<Table::IndexValueType>> val =
          Decode<std::vector<Table::IndexValueType>>(iter_.Value());
      if (!val.HasValue()) {
        finished_ = true;
        FailWith(val.GetStatus());
        destination->Reset();
        return 0;
      }
      for (const Table::IndexValueType& entry : val.Value()) {
        StatusOr<bool> ok = check_candidate(entry.pos);
        if (!ok.HasValue()) {
          finished_ = true;
          FailWith(ok.GetStatus());
          destination->Reset();
          return 0;
        }
        if (ok.Value()) {
          found_visible = true;
          break;
        }
      }
    }

    if (found_visible) {
      // SeekNextDistinct ignores the encoded-key argument; avoid copying the
      // whole index key on every emitted DISTINCT row.
      static const std::string kUnusedEncodedKey;
      Row prev_key = current_index_key_;
      destination->Append(current_row_, current_pos_);
      SeekNextDistinct(kUnusedEncodedKey, prev_key);
    } else {
      if (ascending_) {
        ++iter_;
      } else {
        --iter_;
      }
    }
  }
  if (!iter_.IsValid()) {
    finished_ = true;
  }
  return destination->Size();
}

void SkipScanDistinct::Dump(std::ostream& o, int /*indent*/) const {
  o << "SkipScanDistinct on " << table_.GetSchema().Name() << " using "
    << index_.sc_.name_ << " (prefix_cols=" << prefix_cols_ << ")";
}

void SkipScanDistinct::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
}

}  // namespace tinylamb
