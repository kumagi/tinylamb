/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/memory_budget.hpp"
#include "common/query_grant.hpp"
#include "common/set_operation.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/ooc/chunk_codec.hpp"
#include "executor/detail/ooc/external_sorter.hpp"
#include "executor/detail/ooc/recursive_partitioner.hpp"
#include "executor/detail/pipeline/ooc_sinks.hpp"
#include "executor/detail/pipeline/pipeline_ops.hpp"
#include "executor/detail/scan_filter.hpp"
#include "expression/expression.hpp"
#include "expression/rewrite.hpp"
#include "index/index.hpp"
#include "index/index_scan_iterator.hpp"
#include "page/row_position.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/collation.hpp"
#include "type/row.hpp"
#include "type/row_block_codec.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

StatusOr<bool> PullStreamChunk(
    PipelineContext* ctx, PipelineSource* src,
    const std::vector<std::shared_ptr<PipelineOperator>>& ops, DataChunk* out,
    size_t max_rows, bool* stream_eof) {
  out->Reset();
  if (*stream_eof || src == nullptr) {
    *stream_eof = true;
    return false;
  }
  while (true) {
    DataChunk batch;
    ASSIGN_OR_RETURN(bool, got, src->NextBatch(ctx, &batch, max_rows));
    if (!got || batch.Empty()) {
      *stream_eof = true;
      return false;
    }
    bool exhausted = false;
    DataChunk cur = std::move(batch);
    DataChunk next;
    for (const auto& op : ops) {
      if (cur.Empty()) {
        break;
      }
      next.Reset();
      RETURN_IF_FAIL(op->Process(ctx, cur, &next, &exhausted));
      // Swap so `next` keeps the consumed chunk's storage for the next
      // operator's `Reset` reuse path instead of moving into a husk.
      std::swap(cur, next);
      if (exhausted) {
        *stream_eof = true;
        break;
      }
    }
    if (!cur.Empty()) {
      *out = std::move(cur);
      return true;
    }
    if (*stream_eof) {
      return false;
    }
  }
}

void DumpStreamChain(
    std::ostream& out, int indent, bool is_dump,
    const std::shared_ptr<PipelineSource>& src,
    const std::vector<std::shared_ptr<PipelineOperator>>& ops) {
  for (const auto& op : std::views::reverse(ops)) {
    if (is_dump) {
      op->DumpExplain(out, indent);
    } else {
      op->Explain(out, indent);
    }
  }
  if (src != nullptr) {
    if (is_dump) {
      src->DumpExplain(out, indent);
    } else {
      src->Explain(out, indent);
    }
  }
}

StatusOr<ValueType> CommonSetValueType(ValueType left, ValueType right) {
  if (left == ValueType::kNull) {
    return right;
  }
  if (right == ValueType::kNull || left == right) {
    return left;
  }
  if ((left == ValueType::kInt64 && right == ValueType::kDouble) ||
      (left == ValueType::kDouble && right == ValueType::kInt64)) {
    return ValueType::kDouble;
  }
  const bool left_text = left == ValueType::kVarChar;
  const bool right_text = right == ValueType::kVarChar;
  if ((left == ValueType::kDate && right_text) ||
      (right == ValueType::kDate && left_text)) {
    return ValueType::kDate;
  }
  if ((left_text &&
       (right == ValueType::kInt64 || right == ValueType::kDouble ||
        right == ValueType::kDate)) ||
      (right_text && (left == ValueType::kInt64 || left == ValueType::kDouble ||
                      left == ValueType::kDate))) {
    return ValueType::kVarChar;
  }
  return StatusError(StatusCode::kInvalidArgument,
                     "set operation inputs have incompatible types");
}

Status CoerceRowToTypes(Row* row, const std::vector<ValueType>& common_types) {
  if (row->Count() != common_types.size()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "set operation inputs must have the same column count");
  }
  for (size_t c = 0; c < common_types.size(); ++c) {
    Value& value = row->values_[c];
    const ValueType target = common_types[c];
    if (value.IsNull() || target == ValueType::kNull || value.type == target) {
      continue;
    }
    if (target == ValueType::kDouble && value.type == ValueType::kInt64) {
      // UINT64 values use the signed bit pattern; promote through uint64_t
      // or a value >= 2^63 would become a negative double.
      value = Value(value.IsUnsigned()
                        ? static_cast<double>(
                              static_cast<uint64_t>(value.value.int_value))
                        : static_cast<double>(value.value.int_value));
      continue;
    }
    if (target == ValueType::kDate && value.type == ValueType::kVarChar) {
      ASSIGN_OR_RETURN(Value, d, Value::TryDate(value.value.varchar_value));
      value = std::move(d);
      continue;
    }
    if (target == ValueType::kVarChar) {
      value = Value(value.AsString());
      continue;
    }
    return StatusError(StatusCode::kInvalidArgument,
                       "set operation value cannot be coerced to common type");
  }
  return Status::kSuccess;
}

Status CoerceChunkToSchema(DataChunk* chunk, const Schema& output_schema,
                           const std::vector<ValueType>& common_types) {
  if (chunk->Empty()) {
    return Status::kSuccess;
  }
  if (chunk->ColumnCount() != common_types.size()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "set operation inputs must have the same column count");
  }
  bool need_rewrite = false;
  for (size_t c = 0; c < common_types.size(); ++c) {
    if (common_types[c] != ValueType::kNull &&
        chunk->ColumnAt(c).Type() != common_types[c]) {
      need_rewrite = true;
      break;
    }
  }
  if (!need_rewrite) {
    return Status::kSuccess;
  }
  DataChunk coerced;
  coerced.Reset(output_schema, chunk->Size());
  for (size_t r = 0; r < chunk->Size(); ++r) {
    Row row = chunk->RowAt(r);
    RETURN_IF_FAIL(CoerceRowToTypes(&row, common_types));
    coerced.Append(std::move(row), chunk->PositionAt(r));
  }
  *chunk = std::move(coerced);
  return Status::kSuccess;
}

bool SameSetRow(const Row& left, const Row& right, size_t compare_cols) {
  if (left.Count() < compare_cols || right.Count() < compare_cols) {
    return false;
  }
  for (size_t i = 0; i < compare_cols; ++i) {
    const Value& a = left[i];
    const Value& b = right[i];
    if (a.type != b.type) {
      return false;
    }
    if (a.type == ValueType::kDouble || a.type == ValueType::kArray) {
      if (CompareForOrderBy(a, b) != 0) {
        return false;
      }
    } else if (a.type == ValueType::kVarChar && !a.IsInterval() &&
               !b.IsInterval()) {
      // Same SQL-equality contract as the legacy SetOperationRowEqual: a
      // case-insensitive VARCHAR pair is one row, matching `=` and DISTINCT.
      // Interval-shaped text stays byte-exact: EqualUnderCollation would fold
      // its case, but SetRowPrefixHash hashes it without folding, so mixing
      // the two contracts would split peers into different buckets.
      if (!EqualUnderCollation(a, b)) {
        return false;
      }
    } else if (!(a == b)) {
      return false;
    }
  }
  return true;
}

struct SetRowPrefixHash {
  size_t compare_cols{0};
  size_t operator()(const Row& row) const {
    uint64_t seed = compare_cols;
    for (size_t i = 0; i < compare_cols && i < row.Count(); ++i) {
      const Value& v = row[i];
      uint64_t h = 0;
      // NULL / NaN / -0 canonicalisation is owned by std::hash<Value>; using
      // it directly keeps this hash in lockstep with EqualUnderCollation's
      // equality contract instead of duplicating the constants here.
      if (v.type == ValueType::kVarChar && !v.IsInterval()) {
        // EqualUnderCollation folds a case-insensitive VARCHAR pair, so both
        // peers must land in the same bucket. Fold every text cell: a single
        // CI tag resolves the comparison for both operands (distinct.cpp's
        // DistinctRowHash pattern).
        h = std::hash<std::string>{}(
            FoldCaseForCollation(v.value.varchar_value));
      } else {
        h = std::hash<Value>{}(v);
      }
      seed ^= h + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
    }
    return static_cast<size_t>(seed);
  }
};

struct SetRowPrefixEq {
  size_t compare_cols{0};
  bool operator()(const Row& a, const Row& b) const {
    return SameSetRow(a, b, compare_cols);
  }
};

// Output source for spilled/in-memory `OocSetOpCoordinator` (non-UNION ALL).
class OocSetOpOutputSource final : public PipelineSource {
 public:
  OocSetOpOutputSource(
      SetOperationKind op, Schema output_schema, size_t num_sources,
      size_t width, std::vector<ValueType> common_types, SpillStore* store,
      std::optional<PartitionLeafSet> leaves, std::vector<Row> ready_rows,
      size_t budget_bytes,
      std::optional<QueryMemoryGrant::ActiveConsumerGuard> guard,
      std::optional<QueryMemoryGrant::LocalBatchCache> grant_cache,
      std::vector<std::shared_ptr<PipelineSource>> explain_srcs)
      : op_(op),
        output_schema_(std::move(output_schema)),
        num_sources_(num_sources),
        width_(width),
        common_types_(std::move(common_types)),
        store_(store),
        leaves_(std::move(leaves)),
        ready_rows_(std::move(ready_rows)),
        budget_bytes_(std::max<size_t>(16384, budget_bytes)),
        guard_(std::move(guard)),
        grant_cache_(std::move(grant_cache)),
        explain_srcs_(std::move(explain_srcs)) {}

  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override {
    out->Reset(output_schema_, max_rows);
    while (out->Size() < max_rows) {
      if (ready_cursor_ < ready_rows_.size()) {
        const size_t avail = std::min(max_rows - out->Size(),
                                      ready_rows_.size() - ready_cursor_);
        for (size_t i = 0; i < avail; ++i) {
          out->Append(std::move(ready_rows_[ready_cursor_ + i]), RowPosition());
        }
        ready_cursor_ += avail;
        if (ready_cursor_ >= ready_rows_.size()) {
          ready_rows_.clear();
          ready_rows_.shrink_to_fit();
          ready_cursor_ = 0;
          if (grant_cache_.has_value()) {
            grant_cache_->ReleaseAll();
          }
        }
        continue;
      }

      if (skew_pending_emits_ > 0) {
        while (skew_pending_emits_ > 0 && out->Size() < max_rows) {
          out->Append(skew_current_row_, RowPosition());
          --skew_pending_emits_;
        }
        continue;
      }

      if (skew_sorter_ != nullptr) {
        RETURN_IF_FAIL(PumpSkewSorter());
        continue;
      }

      if (!leaves_.has_value() || leaf_idx_ >= leaves_->leaves.size()) {
        if (grant_cache_.has_value()) {
          grant_cache_->ReleaseAll();
          grant_cache_.reset();
        }
        guard_.reset();
        break;
      }

      const PartitionLeaf& leaf = leaves_->leaves[leaf_idx_++];
      bool use_skew = leaf.skew_fallback;
      if (!use_skew) {
        RETURN_IF_FAIL(LoadHashLeaf(ctx, leaf, &use_skew));
        if (!use_skew) {
          leaves_->DropLeaf(leaf_idx_ - 1);
          continue;
        }
      }
      {
        // Same T4854 bound as the incremental-sort keys: a wider set row
        // would wrap OocSortKey::column_index (slot_t) and make a sorter key
        // alias another column, corrupting EXCEPT/INTERSECT emit decisions.
        if (width_ > static_cast<size_t>(std::numeric_limits<slot_t>::max()) +
                         1) {
          return StatusError(StatusCode::kInvalidArgument,
                             "set operation sorter key column out of range");
        }
        std::vector<OocSortKey> keys;
        keys.reserve(width_ + 1);
        for (size_t c = 0; c < width_; ++c) {
          keys.push_back(OocSortKey{.column_index = static_cast<slot_t>(c)});
        }
        ExternalSorter::Options opts;
        opts.spill_store = store_;
        opts.memory_budget_bytes = budget_bytes_;
        opts.grant = ctx != nullptr ? &ctx->grant : nullptr;
        opts.keys = std::move(keys);
        skew_sorter_ = std::make_unique<ExternalSorter>(std::move(opts));
        RETURN_IF_FAIL(RecursivePartitioner::ReadLeafRows(
            store_, leaf, [&](Row&& tagged) -> Status {
              if (!common_types_.empty() && tagged.Count() >= width_) {
                Row prefix;
                prefix.values_.assign(
                    tagged.values_.begin(),
                    tagged.values_.begin() + static_cast<ptrdiff_t>(width_));
                RETURN_IF_FAIL(CoerceRowToTypes(&prefix, common_types_));
                for (size_t c = 0; c < width_; ++c) {
                  tagged.values_[c] = std::move(prefix.values_[c]);
                }
              }
              return skew_sorter_->AddRow(std::move(tagged));
            }));
        RETURN_IF_FAIL(skew_sorter_->Finish());
        skew_counts_.assign(num_sources_, 0);
        skew_has_row_ = false;
        skew_eof_ = false;
        leaves_->DropLeaf(leaf_idx_ - 1);
      }
    }
    return !out->Empty();
  }

  void DumpExplain(std::ostream& out, int indent) const override {
    const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
    out << pad << "SetOperation\n";
    for (const auto& s : explain_srcs_) {
      if (s != nullptr) {
        s->DumpExplain(out, indent + 2);
      }
    }
  }

  void Explain(std::ostream& out, int indent) const override {
    DumpExplain(out, indent);
  }

  [[nodiscard]] std::string Describe() const override { return "SetOperation"; }

 private:
  [[nodiscard]] uint64_t ComputeEmitCount(
      const std::vector<uint64_t>& counts) const {
    if (op_ == SetOperationKind::kUnion) {
      for (uint64_t c : counts) {
        if (c > 0) {
          return 1;
        }
      }
      return 0;
    }
    if (op_ == SetOperationKind::kIntersect ||
        op_ == SetOperationKind::kIntersectAll) {
      uint64_t m = counts.empty() ? 0 : counts[0];
      for (size_t s = 1; s < counts.size(); ++s) {
        m = std::min(m, counts[s]);
      }
      if (op_ == SetOperationKind::kIntersect) {
        return m > 0 ? 1 : 0;
      }
      return m;
    }
    // EXCEPT / EXCEPT ALL:
    const uint64_t left = counts.empty() ? 0 : counts[0];
    uint64_t sub = 0;
    for (size_t s = 1; s < counts.size(); ++s) {
      // Saturate instead of wrapping: once the subtractants reach left,
      // EXCEPT ALL emits nothing, and a wrap could make `left > sub` flip
      // back to true and resurrect rows that must have been subtracted.
      if (counts[s] > std::numeric_limits<uint64_t>::max() - sub) {
        sub = std::numeric_limits<uint64_t>::max();
        break;
      }
      sub += counts[s];
    }
    if (op_ == SetOperationKind::kExcept) {
      return (left > 0 && sub == 0) ? 1 : 0;
    }
    return left > sub ? (left - sub) : 0;
  }

  StatusOr<size_t> RemoveSourceTag(Row* tagged) const {
    if (tagged->Count() == 0 || tagged->Count() - 1 != width_) {
      return StatusError(StatusCode::kCorrupt,
                         "set operation spill row width mismatch");
    }
    const Value& tag = tagged->values_.back();
    if (tag.type != ValueType::kInt64 || tag.value.int_value < 0 ||
        std::cmp_greater_equal(tag.value.int_value, num_sources_)) {
      return StatusError(StatusCode::kCorrupt,
                         "set operation spill row has an invalid source tag");
    }
    const auto source = static_cast<size_t>(tag.value.int_value);
    tagged->values_.pop_back();
    return source;
  }

  Status PumpSkewSorter() {
    if (skew_lookahead_.has_value()) {
      skew_current_row_ = std::move(*skew_lookahead_);
      skew_lookahead_.reset();
      std::fill(skew_counts_.begin(), skew_counts_.end(), 0);
      if (skew_lookahead_src_ < skew_counts_.size()) {
        ++skew_counts_[skew_lookahead_src_];
      }
      skew_has_row_ = true;
    }
    if (skew_eof_) {
      if (skew_has_row_) {
        skew_pending_emits_ = ComputeEmitCount(skew_counts_);
        skew_has_row_ = false;
      } else {
        skew_sorter_.reset();
      }
      return Status::kSuccess;
    }
    Row tagged;
    while (true) {
      ASSIGN_OR_RETURN(bool, got, skew_sorter_->NextRow(&tagged));
      if (!got) {
        skew_eof_ = true;
        if (skew_has_row_) {
          skew_pending_emits_ = ComputeEmitCount(skew_counts_);
          skew_has_row_ = false;
        } else {
          skew_sorter_.reset();
        }
        return Status::kSuccess;
      }
      ASSIGN_OR_RETURN(size_t, src_idx, RemoveSourceTag(&tagged));
      if (!skew_has_row_) {
        skew_current_row_ = std::move(tagged);
        std::fill(skew_counts_.begin(), skew_counts_.end(), 0);
        if (src_idx < skew_counts_.size()) {
          ++skew_counts_[src_idx];
        }
        skew_has_row_ = true;
        continue;
      }
      if (SameSetRow(skew_current_row_, tagged, width_)) {
        if (src_idx < skew_counts_.size()) {
          ++skew_counts_[src_idx];
        }
        continue;
      }
      skew_pending_emits_ = ComputeEmitCount(skew_counts_);
      if (skew_pending_emits_ > 0) {
        skew_lookahead_ = std::move(tagged);
        skew_lookahead_src_ = src_idx;
        skew_has_row_ = false;
        return Status::kSuccess;
      }
      skew_current_row_ = std::move(tagged);
      std::fill(skew_counts_.begin(), skew_counts_.end(), 0);
      if (src_idx < skew_counts_.size()) {
        ++skew_counts_[src_idx];
      }
    }
  }

  Status LoadHashLeaf(PipelineContext* ctx, const PartitionLeaf& leaf,
                      bool* fell_back_to_skew) {
    *fell_back_to_skew = false;
    if (!grant_cache_.has_value() && ctx != nullptr) {
      const size_t refill =
          budget_bytes_ > 0
              ? std::min<size_t>(
                    QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes,
                    std::max<size_t>(65536, budget_bytes_ / 4))
              : QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes;
      grant_cache_.emplace(&ctx->grant, refill);
    }

    SetRowPrefixHash hasher{width_};
    SetRowPrefixEq eq{width_};
    std::unordered_map<Row, std::vector<uint64_t>, SetRowPrefixHash,
                       SetRowPrefixEq>
        counts(16, hasher, eq);
    std::vector<Row> ordered_keys;
    const auto discard_leaf_containers = [&] {
      decltype(counts) empty_counts(0, hasher, eq);
      counts.swap(empty_counts);
      std::vector<Row> empty_keys;
      ordered_keys.swap(empty_keys);
    };
    size_t leaf_reserved = 0;
    bool grant_exhausted = false;

    Status read_st = RecursivePartitioner::ReadLeafRows(
        store_, leaf, [&](Row&& tagged) -> Status {
          ASSIGN_OR_RETURN(size_t, src_idx, RemoveSourceTag(&tagged));
          if (!common_types_.empty()) {
            RETURN_IF_FAIL(CoerceRowToTypes(&tagged, common_types_));
          }
          auto it = counts.find(tagged);
          if (it == counts.end()) {
            const size_t add_bytes =
                (EstimateRowMemoryBytes(tagged) * 2) + (num_sources_ * 8) + 64;
            if (grant_cache_.has_value() && !grant_cache_->Reserve(add_bytes)) {
              grant_exhausted = true;
              return StatusError(StatusCode::kMemoryLimitExceeded,
                                 "set operation leaf grant exhausted");
            }
            leaf_reserved += add_bytes;
            std::vector<uint64_t> vec(num_sources_, 0);
            if (src_idx < vec.size()) {
              ++vec[src_idx];
            }
            ordered_keys.push_back(tagged);
            counts.emplace(std::move(tagged), std::move(vec));
          } else {
            if (src_idx < it->second.size()) {
              ++it->second[src_idx];
            }
          }
          return Status::kSuccess;
        });

    if (grant_exhausted) {
      discard_leaf_containers();
      if (grant_cache_.has_value()) {
        grant_cache_->ReleaseAll();
      }
      *fell_back_to_skew = true;
      return Status::kSuccess;
    }
    if (!read_st.ok()) {
      discard_leaf_containers();
      if (grant_cache_.has_value()) {
        grant_cache_->ReleaseAll();
      }
      return read_st;
    }

    ready_rows_.clear();
    ready_cursor_ = 0;
    size_t ready_bytes = 0;
    // leaf_reserved covers `counts` + `ordered_keys`; `ready_rows_` is a third
    // copy, so it must be charged *on top of* leaf_reserved while all three
    // coexist. Destroy the leaf containers before returning their reservation
    // below, leaving only `ready_rows_` resident.
    size_t charged_bytes = leaf_reserved;
    for (Row& key : ordered_keys) {
      auto it = counts.find(key);
      if (it == counts.end()) {
        continue;
      }
      const uint64_t emit_n = ComputeEmitCount(it->second);
      const size_t key_bytes = EstimateRowMemoryBytes(key) + 16;
      for (uint64_t i = 0; i < emit_n; ++i) {
        const size_t needed = leaf_reserved + ready_bytes + key_bytes;
        if (needed > charged_bytes && grant_cache_.has_value()) {
          const size_t deficit = needed - charged_bytes;
          if (!grant_cache_->Reserve(deficit)) {
            std::vector<Row>().swap(ready_rows_);
            discard_leaf_containers();
            grant_cache_->ReleaseAll();
            *fell_back_to_skew = true;
            return Status::kSuccess;
          }
          charged_bytes += deficit;
        }
        ready_bytes += key_bytes;
        ready_rows_.push_back(key);
      }
    }
    discard_leaf_containers();
    if (grant_cache_.has_value() && charged_bytes > ready_bytes) {
      grant_cache_->Release(charged_bytes - ready_bytes);
    }
    return Status::kSuccess;
  }

  SetOperationKind op_;
  Schema output_schema_;
  size_t num_sources_{0};
  size_t width_{0};
  std::vector<ValueType> common_types_;
  SpillStore* store_{nullptr};
  std::optional<PartitionLeafSet> leaves_;
  std::vector<Row> ready_rows_;
  size_t ready_cursor_{0};
  size_t budget_bytes_{0};
  std::optional<QueryMemoryGrant::ActiveConsumerGuard> guard_;
  std::optional<QueryMemoryGrant::LocalBatchCache> grant_cache_;
  std::vector<std::shared_ptr<PipelineSource>> explain_srcs_;

  size_t leaf_idx_{0};
  std::unique_ptr<ExternalSorter> skew_sorter_;
  std::vector<uint64_t> skew_counts_;
  Row skew_current_row_;
  bool skew_has_row_{false};
  bool skew_eof_{false};
  uint64_t skew_pending_emits_{0};
  std::optional<Row> skew_lookahead_;
  size_t skew_lookahead_src_{0};
};

template <typename SchemaAt>
Status InferCommonTypes(size_t input_count, const SchemaAt& schema_at,
                        std::vector<ValueType>* types) {
  if (!types->empty() || input_count == 0) {
    return Status::kSuccess;
  }
  const size_t cols = schema_at(0).ColumnCount();
  types->resize(cols, ValueType::kNull);
  for (size_t c = 0; c < cols; ++c) {
    ValueType cur = schema_at(0).GetColumn(c).Type();
    for (size_t s = 1; s < input_count; ++s) {
      const Schema& schema = schema_at(s);
      if (c < schema.ColumnCount()) {
        ASSIGN_OR_RETURN(ValueType, unified,
                         CommonSetValueType(cur, schema.GetColumn(c).Type()));
        cur = unified;
      }
    }
    (*types)[c] = cur;
  }
  return Status::kSuccess;
}

}  // namespace

// --- 1. OocConcatSource (`UNION ALL`) ---

OocConcatSource::OocConcatSource(std::vector<OocSetOpInputStream> inputs,
                                 Schema output_schema,
                                 const std::vector<ValueType>& common_types)
    : inputs_(std::move(inputs)),
      output_schema_(std::move(output_schema)),
      common_types_(common_types) {
  common_type_status_ = InferCommonTypes(
      inputs_.size(), [&](size_t i) -> const Schema& { return inputs_[i].schema; },
      &common_types_);
}

Status OocConcatSource::Open(PipelineContext* ctx) {
  RETURN_IF_FAIL(common_type_status_);
  opened_ = true;
  active_idx_ = 0;
  if (!inputs_.empty()) {
    if (inputs_[0].source != nullptr) {
      RETURN_IF_FAIL(inputs_[0].source->Open(ctx));
    }
    for (const auto& op : inputs_[0].operators) {
      RETURN_IF_FAIL(op->Open(ctx));
    }
  }
  return Status::kSuccess;
}

StatusOr<bool> OocConcatSource::NextBatch(PipelineContext* ctx, DataChunk* out,
                                          size_t max_rows) {
  if (!opened_) {
    RETURN_IF_FAIL(Open(ctx));
  }
  while (active_idx_ < inputs_.size()) {
    OocSetOpInputStream& cur = inputs_[active_idx_];
    bool stream_eof = false;
    DataChunk batch;
    ASSIGN_OR_RETURN(bool, got,
                     PullStreamChunk(ctx, cur.source.get(), cur.operators,
                                     &batch, max_rows, &stream_eof));
    if (got && !batch.Empty()) {
      RETURN_IF_FAIL(
          CoerceChunkToSchema(&batch, output_schema_, common_types_));
      *out = std::move(batch);
      return true;
    }
    // Exhausted current child: release its resources before advancing.
    cur.operators.clear();
    cur.source.reset();
    ++active_idx_;
    if (active_idx_ < inputs_.size()) {
      if (inputs_[active_idx_].source != nullptr) {
        RETURN_IF_FAIL(inputs_[active_idx_].source->Open(ctx));
      }
      for (const auto& op : inputs_[active_idx_].operators) {
        RETURN_IF_FAIL(op->Open(ctx));
      }
    }
  }
  return false;
}

void OocConcatSource::Explain(std::ostream& out, int indent) const {
  DumpExplain(out, indent);
}

void OocConcatSource::DumpExplain(std::ostream& out, int indent) const {
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  out << pad << "SetOperation\n";
  for (const OocSetOpInputStream& s : inputs_) {
    DumpStreamChain(out, indent + 2, /*is_dump=*/true, s.source, s.operators);
  }
}

// --- 2. OocMergeAppendSource (`UNION ALL` with `OrderKeys`) ---

OocMergeAppendSource::OocMergeAppendSource(
    std::vector<OocSetOpInputStream> inputs,
    std::vector<OocSortSink::KeySpec> order_keys, Schema output_schema,
    const std::vector<ValueType>& common_types)
    : order_keys_(std::move(order_keys)),
      output_schema_(std::move(output_schema)),
      common_types_(common_types) {
  common_type_status_ = InferCommonTypes(
      inputs.size(), [&](size_t i) -> const Schema& { return inputs[i].schema; },
      &common_types_);
  cursors_.reserve(inputs.size());
  for (OocSetOpInputStream& in : inputs) {
    Cursor cur;
    cur.stream = std::move(in);
    cursors_.push_back(std::move(cur));
  }
  key_orders_.reserve(order_keys_.size());
  for (const auto& k : order_keys_) {
    key_orders_.emplace_back(k.ascending, k.nulls_first, k.collation);
  }
}

Status OocMergeAppendSource::Open(PipelineContext* ctx) {
  RETURN_IF_FAIL(common_type_status_);
  auto pull_cursor = [&](size_t idx) -> Status {
    Cursor& c = cursors_[idx];
    if (c.stream.source != nullptr) {
      RETURN_IF_FAIL(c.stream.source->Open(ctx));
    }
    for (const auto& op : c.stream.operators) {
      RETURN_IF_FAIL(op->Open(ctx));
    }
    bool eof = false;
    ASSIGN_OR_RETURN(
        bool, got,
        PullStreamChunk(ctx, c.stream.source.get(), c.stream.operators,
                        &c.chunk, kDefaultVectorSize, &eof));
    if (!got || c.chunk.Empty()) {
      c.eof = true;
      return Status::kSuccess;
    }
    RETURN_IF_FAIL(
        CoerceChunkToSchema(&c.chunk, output_schema_, common_types_));
    c.row_idx = 0;
    const Row r = c.chunk.RowAt(0);
    c.cached_keys.reserve(order_keys_.size());
    for (const auto& k : order_keys_) {
      ASSIGN_OR_RETURN(Value, kv,
                       EvalRowExpression(k.expression, output_schema_, r));
      c.cached_keys.push_back(std::move(kv));
    }
    RETURN_IF_FAIL(ObserveSortKeys(key_orders_, c.cached_keys));
    return Status::kSuccess;
  };
  for (size_t i = 0; i < cursors_.size(); ++i) {
    RETURN_IF_FAIL(pull_cursor(i));
  }
  return Status::kSuccess;
}

int OocMergeAppendSource::CompareHeads(size_t lhs_idx, size_t rhs_idx) const {
  const auto& lk = cursors_[lhs_idx].cached_keys;
  const auto& rk = cursors_[rhs_idx].cached_keys;
  for (size_t i = 0; i < key_orders_.size(); ++i) {
    const Value& lv = i < lk.size() ? lk[i] : Value();
    const Value& rv = i < rk.size() ? rk[i] : Value();
    const int cmp = key_orders_[i].Compare(lv, rv);
    if (cmp != 0) {
      return cmp;
    }
  }
  return 0;
}

StatusOr<bool> OocMergeAppendSource::NextBatch(PipelineContext* ctx,
                                               DataChunk* out,
                                               size_t max_rows) {
  out->Reset(output_schema_, max_rows);
  while (out->Size() < max_rows) {
    size_t best = cursors_.size();
    for (size_t i = 0; i < cursors_.size(); ++i) {
      if (cursors_[i].eof) {
        continue;
      }
      if (best == cursors_.size() || CompareHeads(i, best) < 0) {
        best = i;
      }
    }
    if (best == cursors_.size()) {
      break;
    }
    Cursor& c = cursors_[best];
    out->Append(c.chunk.RowAt(c.row_idx), c.chunk.PositionAt(c.row_idx));
    ++c.row_idx;
    if (c.row_idx >= c.chunk.Size()) {
      c.chunk.Reset();
      c.row_idx = 0;
      c.cached_keys.clear();
      bool eof = false;
      ASSIGN_OR_RETURN(
          bool, got,
          PullStreamChunk(ctx, c.stream.source.get(), c.stream.operators,
                          &c.chunk, kDefaultVectorSize, &eof));
      if (!got || c.chunk.Empty()) {
        c.eof = true;
        continue;
      }
      RETURN_IF_FAIL(
          CoerceChunkToSchema(&c.chunk, output_schema_, common_types_));
    }
    const Row r = c.chunk.RowAt(c.row_idx);
    c.cached_keys.clear();
    c.cached_keys.reserve(order_keys_.size());
    for (const auto& k : order_keys_) {
      ASSIGN_OR_RETURN(Value, kv,
                       EvalRowExpression(k.expression, output_schema_, r));
      c.cached_keys.push_back(std::move(kv));
    }
    RETURN_IF_FAIL(ObserveSortKeys(key_orders_, c.cached_keys));
  }
  return !out->Empty();
}

void OocMergeAppendSource::Explain(std::ostream& out, int indent) const {
  DumpExplain(out, indent);
}

void OocMergeAppendSource::DumpExplain(std::ostream& out, int indent) const {
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  out << pad << "MergeAppend\n";
  for (const auto& c : cursors_) {
    DumpStreamChain(out, indent + 2, /*is_dump=*/true, c.stream.source,
                    c.stream.operators);
  }
}

// --- 3. OocSetOpCoordinator + OocSetOpInputSink ---

OocSetOpCoordinator::OocSetOpCoordinator(SetOperationKind operation,
                                         Schema output_schema,
                                         std::vector<Schema> child_schemas)
    : operation_(operation),
      output_schema_(std::move(output_schema)),
      num_sources_(child_schemas.size()),
      in_mem_rows_(child_schemas.size()) {
  if (!child_schemas.empty()) {
    width_ = child_schemas[0].ColumnCount();
    common_type_status_ = InferCommonTypes(
        child_schemas.size(),
        [&](size_t i) -> const Schema& { return child_schemas[i]; },
        &common_types_);
  }
}

OocSetOpCoordinator::~OocSetOpCoordinator() {
  partitioner_.reset();
  in_mem_rows_.clear();
}

void OocSetOpCoordinator::RegisterExplainSource(
    std::shared_ptr<PipelineSource> src) {
  explain_sources_.push_back(std::move(src));
}

Status OocSetOpCoordinator::CoerceRow(Row* row, size_t expected_cols) const {
  if (common_types_.size() != expected_cols) {
    return Status::kSuccess;
  }
  return CoerceRowToTypes(row, common_types_);
}

Status OocSetOpCoordinator::TransitionToSpill(PipelineContext* ctx) {
  if (spilled_) {
    return Status::kSuccess;
  }
  const size_t w = width_.value_or(output_schema_.ColumnCount());
  RecursivePartitioner::Options opts;
  opts.spill_store = &ctx->spill_store;
  opts.memory_budget_bytes = budget_bytes_;
  opts.grant = &ctx->grant;
  SetRowPrefixHash hasher{w};
  auto candidate = std::make_unique<RecursivePartitioner>(
      opts, [hasher](const Row& r) -> uint64_t { return hasher(r); });

  for (size_t s = 0; s < in_mem_rows_.size(); ++s) {
    for (Row r : in_mem_rows_[s]) {
      RETURN_IF_FAIL(CoerceRowToTypes(&r, common_types_));
      r.values_.emplace_back(static_cast<int64_t>(s));
      RETURN_IF_FAIL(candidate->AddRow(std::move(r)));
    }
  }
  partitioner_ = std::move(candidate);
  for (auto& rows : in_mem_rows_) {
    std::vector<Row>().swap(rows);
  }
  mem_bytes_ = 0;
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
  spilled_ = true;
  return Status::kSuccess;
}

Status OocSetOpCoordinator::ConsumeFromSource(PipelineContext* ctx,
                                              size_t source_idx,
                                              const DataChunk& input) {
  RETURN_IF_FAIL(common_type_status_);
  if (!initialized_) {
    initialized_ = true;
    consumer_guard_.emplace(&ctx->grant);
    budget_bytes_ = ctx->OperatorBudgetBytes();
    in_mem_budget_ =
        budget_bytes_ > 0 ? std::max<size_t>(budget_bytes_ / 2, 16384) : 0;
    const size_t refill =
        budget_bytes_ > 0
            ? std::min<size_t>(
                  QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes,
                  std::max<size_t>(65536, budget_bytes_ / 4))
            : QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes;
    grant_cache_.emplace(&ctx->grant, refill);
    type_widened_after_spill_ = false;
  }
  if (input.Empty()) {
    return Status::kSuccess;
  }
  const size_t cols = input.ColumnCount();
  if (!width_.has_value()) {
    width_ = cols;
    common_types_.assign(cols, ValueType::kNull);
  } else if (*width_ != cols) {
    return StatusError(StatusCode::kInvalidArgument,
                       "set operation inputs must have the same column count");
  }
  for (size_t r = 0; r < input.Size(); ++r) {
    Row row = input.RowAt(r);
    for (size_t c = 0; c < cols; ++c) {
      if (!row[c].IsNull()) {
        const ValueType prev_type = common_types_[c];
        ASSIGN_OR_RETURN(ValueType, unified,
                         CommonSetValueType(prev_type, row[c].type));
        if (spilled_ && prev_type != ValueType::kNull && prev_type != unified) {
          type_widened_after_spill_ = true;
        }
        common_types_[c] = unified;
      }
    }
    RETURN_IF_FAIL(CoerceRowToTypes(&row, common_types_));

    const size_t row_bytes = EstimateRowMemoryBytes(row) + 16;
    const bool always_spill =
        ProcessMemoryBudget::Global().AlwaysSpillEnabled();
    if (!spilled_ &&
        (always_spill ||
         (in_mem_budget_ > 0 && mem_bytes_ + row_bytes > in_mem_budget_) ||
         (grant_cache_.has_value() && !grant_cache_->Reserve(row_bytes)))) {
      RETURN_IF_FAIL(TransitionToSpill(ctx));
    }

    if (spilled_) {
      row.values_.emplace_back(static_cast<int64_t>(source_idx));
      RETURN_IF_FAIL(partitioner_->AddRow(std::move(row)));
    } else {
      mem_bytes_ += row_bytes;
      in_mem_rows_[source_idx].push_back(std::move(row));
    }
  }
  return Status::kSuccess;
}

StatusOr<std::shared_ptr<PipelineSource>> OocSetOpCoordinator::Finalize(
    PipelineContext* ctx) {
  RETURN_IF_FAIL(common_type_status_);
  if (!initialized_) {
    budget_bytes_ = ctx->OperatorBudgetBytes();
  }
  const size_t w = width_.value_or(output_schema_.ColumnCount());

  if (!spilled_) {
    SetRowPrefixHash hasher{w};
    SetRowPrefixEq eq{w};
    std::unordered_map<Row, std::vector<uint64_t>, SetRowPrefixHash,
                       SetRowPrefixEq>
        counts(16, hasher, eq);
    std::vector<Row> ordered;
    for (size_t s = 0; s < in_mem_rows_.size(); ++s) {
      for (Row& r : in_mem_rows_[s]) {
        RETURN_IF_FAIL(CoerceRowToTypes(&r, common_types_));
        auto it = counts.find(r);
        if (it == counts.end()) {
          std::vector<uint64_t> vec(num_sources_, 0);
          ++vec[s];
          ordered.push_back(r);
          counts.emplace(std::move(r), std::move(vec));
        } else {
          ++it->second[s];
        }
      }
      in_mem_rows_[s].clear();
      in_mem_rows_[s].shrink_to_fit();
    }
    std::vector<Row> ready;
    size_t ready_bytes = 0;
    for (Row& key : ordered) {
      auto it = counts.find(key);
      if (it == counts.end()) {
        continue;
      }
      const auto& cs = it->second;
      uint64_t emit_n = 0;
      if (operation_ == SetOperationKind::kUnion) {
        emit_n = 1;
      } else if (operation_ == SetOperationKind::kIntersect ||
                 operation_ == SetOperationKind::kIntersectAll) {
        uint64_t m = cs[0];
        for (size_t s = 1; s < cs.size(); ++s) {
          m = std::min(m, cs[s]);
        }
        emit_n =
            (operation_ == SetOperationKind::kIntersect) ? (m > 0 ? 1 : 0) : m;
      } else {
        uint64_t sub = 0;
        for (size_t s = 1; s < cs.size(); ++s) {
          // Same saturation as ComputeEmitCount on the spilled path: a wrap
          // would make `cs[0] > sub` true again and resurrect already
          // subtracted rows, i.e. change the answer based on spilling.
          if (cs[s] > std::numeric_limits<uint64_t>::max() - sub) {
            sub = std::numeric_limits<uint64_t>::max();
            break;
          }
          sub += cs[s];
        }
        emit_n = (operation_ == SetOperationKind::kExcept)
                     ? ((cs[0] > 0 && sub == 0) ? 1 : 0)
                     : (cs[0] > sub ? cs[0] - sub : 0);
      }
      const size_t key_bytes = EstimateRowMemoryBytes(key) + 16;
      for (uint64_t i = 0; i < emit_n; ++i) {
        ready_bytes += key_bytes;
        ready.push_back(key);
      }
    }
    if (grant_cache_.has_value()) {
      if (mem_bytes_ > ready_bytes) {
        grant_cache_->Release(mem_bytes_ - ready_bytes);
      } else if (ready_bytes > mem_bytes_) {
        // The output rows are already resident; a failed Reserve must not be
        // ignored, otherwise the ledger under-reports resident bytes and a
        // later stage over-admits against the same grant. There is no spill
        // path to fall back to at this point (the input was never spilled),
        // so surface the budget failure instead of silently overdrafting.
        if (!grant_cache_->Reserve(ready_bytes - mem_bytes_)) {
          return StatusError(StatusCode::kMemoryLimitExceeded,
                             "set operation finalize output exceeds grant");
        }
      }
    }
    mem_bytes_ = 0;
    return std::make_shared<OocSetOpOutputSource>(
        operation_, output_schema_, num_sources_, w, common_types_, nullptr,
        std::nullopt, std::move(ready), budget_bytes_,
        std::move(consumer_guard_), std::move(grant_cache_), explain_sources_);
  }

  PartitionLeafSet leaves;
  RETURN_IF_FAIL(partitioner_->Finalize(&leaves));
  partitioner_.reset();
  if (type_widened_after_spill_) {
    RecursivePartitioner::Options opts;
    opts.spill_store = &ctx->spill_store;
    opts.memory_budget_bytes = budget_bytes_;
    opts.grant = &ctx->grant;
    SetRowPrefixHash hasher{w};
    RecursivePartitioner repartitioner(
        opts, [hasher](const Row& r) -> uint64_t { return hasher(r); });
    for (size_t i = 0; i < leaves.leaves.size(); ++i) {
      RETURN_IF_FAIL(RecursivePartitioner::ReadLeafRows(
          &ctx->spill_store, leaves.leaves[i], [&](Row&& tagged) -> Status {
            if (!common_types_.empty() && tagged.Count() >= w) {
              Row prefix;
              prefix.values_.assign(
                  tagged.values_.begin(),
                  tagged.values_.begin() + static_cast<ptrdiff_t>(w));
              RETURN_IF_FAIL(CoerceRowToTypes(&prefix, common_types_));
              for (size_t c = 0; c < w; ++c) {
                tagged.values_[c] = std::move(prefix.values_[c]);
              }
            }
            return repartitioner.AddRow(std::move(tagged));
          }));
      leaves.DropLeaf(i);
    }
    PartitionLeafSet re_leaves;
    RETURN_IF_FAIL(repartitioner.Finalize(&re_leaves));
    leaves = std::move(re_leaves);
  }
  return std::make_shared<OocSetOpOutputSource>(
      operation_, output_schema_, num_sources_, w, common_types_,
      &ctx->spill_store, std::move(leaves), std::vector<Row>{}, budget_bytes_,
      std::move(consumer_guard_), std::move(grant_cache_), explain_sources_);
}

void OocSetOpCoordinator::DumpExplain(std::ostream& out, int indent) const {
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  out << pad << "SetOperation\n";
  for (const auto& s : explain_sources_) {
    if (s != nullptr) {
      s->DumpExplain(out, indent + 2);
    }
  }
}

OocSetOpInputSink::OocSetOpInputSink(
    std::shared_ptr<OocSetOpCoordinator> coord, size_t source_idx,
    bool is_final, std::shared_ptr<PipelineSource> explain_src)
    : coord_(std::move(coord)),
      source_idx_(source_idx),
      is_final_(is_final),
      explain_src_(std::move(explain_src)) {}

Status OocSetOpInputSink::Init(PipelineContext* /*ctx*/,
                               size_t /*worker_count*/) {
  if (explain_src_ != nullptr) {
    coord_->RegisterExplainSource(explain_src_);
  }
  return Status::kSuccess;
}

Status OocSetOpInputSink::Consume(PipelineContext* ctx, size_t /*worker_id*/,
                                  const DataChunk& input) {
  return coord_->ConsumeFromSource(ctx, source_idx_, input);
}

StatusOr<std::shared_ptr<PipelineSource>> OocSetOpInputSink::Finalize(
    PipelineContext* ctx) {
  if (!is_final_) {
    return std::make_shared<RowVectorSource>(std::vector<Row>{},
                                             "SetOpStagingDone");
  }
  return coord_->Finalize(ctx);
}

void OocSetOpInputSink::DumpExplain(std::ostream& out, int indent) const {
  if (!is_final_) {
    return;
  }
  coord_->DumpExplain(out, indent);
}

// --- 4. IncrementalSortSource ---

IncrementalSortSource::IncrementalSortSource(
    std::shared_ptr<PipelineSource> upstream_source,
    std::vector<std::shared_ptr<PipelineOperator>> ops, Schema schema,
    std::vector<OocSortSink::KeySpec> prefix_keys,
    std::vector<OocSortSink::KeySpec> suffix_keys)
    : upstream_source_(std::move(upstream_source)),
      upstream_ops_(std::move(ops)),
      schema_(std::move(schema)),
      prefix_keys_(std::move(prefix_keys)),
      suffix_keys_(std::move(suffix_keys)) {
  for (const auto& key : prefix_keys_) { prefix_orders_.push_back(MakeSortKeyOrder(key));
}
  for (const auto& key : suffix_keys_) { suffix_orders_.push_back(MakeSortKeyOrder(key));
}
}

Status IncrementalSortSource::Open(PipelineContext* ctx) {
  consumer_guard_.emplace(&ctx->grant);
  budget_bytes_ = ctx->OperatorBudgetBytes();
  txn_ = ctx->txn;
  if (upstream_source_ != nullptr) {
    RETURN_IF_FAIL(upstream_source_->Open(ctx));
  }
  for (const auto& op : upstream_ops_) {
    RETURN_IF_FAIL(op->Open(ctx));
  }
  return Status::kSuccess;
}

StatusOr<Row> IncrementalSortSource::ExtractPrefix(const Row& row) {
  std::vector<Value> vals;
  vals.reserve(prefix_keys_.size());
  for (const auto& k : prefix_keys_) {
    ASSIGN_OR_RETURN(Value, kv, EvalRowExpression(k.expression, schema_, row));
    vals.push_back(std::move(kv));
  }
  RETURN_IF_FAIL(ObserveSortKeys(prefix_orders_, vals));
  return Row(std::move(vals));
}

Status IncrementalSortSource::FillNextGroup(PipelineContext* ctx) {
  group_sorter_.reset();
  singleton_row_.reset();

  std::vector<OocSortKey> sort_keys;
  sort_keys.reserve(suffix_keys_.size());
  std::vector<size_t> computed_indices;
  const size_t base_cols = schema_.ColumnCount();
  const size_t max_slot = std::numeric_limits<slot_t>::max();
  for (size_t i = 0; i < suffix_keys_.size(); ++i) {
    int off = -1;
    if (suffix_keys_[i].expression &&
        suffix_keys_[i].expression->Type() == TypeTag::kColumnValue) {
      off = schema_.Offset(
          suffix_keys_[i].expression->AsColumnValue().GetColumnName());
    }
    size_t slot = 0;
    if (off < 0) {
      if (base_cols > max_slot || computed_indices.size() > max_slot - base_cols) {
        return StatusError(StatusCode::kInvalidArgument,
                           "sort key column index out of range");
      }
      slot = base_cols + computed_indices.size();
      computed_indices.push_back(i);
    } else {
      slot = static_cast<size_t>(off);
      if (slot > max_slot) {
        return StatusError(StatusCode::kInvalidArgument,
                           "sort key column index out of range");
      }
    }
    sort_keys.push_back(OocSortKey{.column_index = static_cast<slot_t>(slot),
                                   .ascending = suffix_keys_[i].ascending,
                                   .nulls_first = suffix_keys_[i].nulls_first,
                                   .collation = suffix_keys_[i].collation});
  }

  auto next_upstream_row = [&](Row* out_row) -> StatusOr<bool> {
    while (in_idx_ >= in_chunk_.Size()) {
      in_chunk_.Reset();
      in_idx_ = 0;
      if (upstream_eof_) {
        return false;
      }
      ASSIGN_OR_RETURN(
          bool, got,
          PullStreamChunk(ctx, upstream_source_.get(), upstream_ops_,
                          &in_chunk_, kDefaultVectorSize, &upstream_eof_));
      if (!got || in_chunk_.Empty()) {
        upstream_eof_ = true;
        return false;
      }
    }
    *out_row = in_chunk_.RowAt(in_idx_++);
    if (!computed_indices.empty()) { out_row->values_.resize(base_cols);
}
    for (size_t ci : computed_indices) {
      ASSIGN_OR_RETURN(Value, kv, EvalRowExpression(suffix_keys_[ci].expression, schema_, *out_row));
      out_row->values_.push_back(std::move(kv));
    }
    for (size_t i = 0; i < sort_keys.size(); ++i) {
      RETURN_IF_FAIL(suffix_orders_[i].Observe((*out_row)[sort_keys[i].column_index]));
    }
    return true;
  };

  if (!has_lookahead_) {
    Row first;
    ASSIGN_OR_RETURN(bool, got, next_upstream_row(&first));
    if (!got) {
      return Status::kSuccess;
    }
    ASSIGN_OR_RETURN(Row, fp, ExtractPrefix(first));
    lookahead_row_ = std::move(first);
    lookahead_prefix_ = std::move(fp);
    has_lookahead_ = true;
  }

  const Row group_prefix = std::move(lookahead_prefix_);
  Row first_row = std::move(lookahead_row_);
  has_lookahead_ = false;

  // Peek second row: if it belongs to a different prefix run, fast-emit
  // singleton without touching ExternalSorter!
  Row second_row;
  ASSIGN_OR_RETURN(bool, got_second, next_upstream_row(&second_row));
  if (!got_second) {
    ++singleton_runs_;
    singleton_row_ = std::move(first_row);
    return Status::kSuccess;
  }
  ASSIGN_OR_RETURN(Row, sp, ExtractPrefix(second_row));
  // The upstream prefix sort uses each prefix key's own collation (see the
  // OocSortKey built from prefix_keys_), so peer equality for run boundaries
  // must resolve the same collation. EqualUnderCollation alone only looks at
  // the operands' tags: a COLLATE('und:ci') key's static collation is not
  // visible there, which can split ci-equivalent prefixes into separate runs
  // and sort their suffixes independently.
  auto prefix_equal = [this](const Row& a, const Row& b) {
    if (a.Count() != b.Count() || a.Count() != prefix_keys_.size()) {
      return false;
    }
    for (size_t i = 0; i < a.Count(); ++i) {
      const SortKeyOrder order{true, std::nullopt,
                               prefix_keys_[i].collation};
      if (order.Compare(a[i], b[i]) != 0) {
        return false;
      }
    }
    return true;
  };
  if (!prefix_equal(group_prefix, sp)) {
    ++singleton_runs_;
    singleton_row_ = std::move(first_row);
    lookahead_row_ = std::move(second_row);
    lookahead_prefix_ = std::move(sp);
    has_lookahead_ = true;
    return Status::kSuccess;
  }

  // Group has >= 2 rows: sort suffix inside bounded ExternalSorter.
  ExternalSorter::Options opts;
  opts.spill_store = &ctx->spill_store;
  opts.memory_budget_bytes = budget_bytes_;
  opts.grant = &ctx->grant;
  opts.keys = sort_keys;
  opts.force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
  auto sorter = std::make_unique<ExternalSorter>(std::move(opts));

  RETURN_IF_FAIL(sorter->AddRow(std::move(first_row)));
  RETURN_IF_FAIL(sorter->AddRow(std::move(second_row)));

  while (true) {
    Row nxt;
    ASSIGN_OR_RETURN(bool, got, next_upstream_row(&nxt));
    if (!got) {
      break;
    }
    ASSIGN_OR_RETURN(Row, np, ExtractPrefix(nxt));
    if (!prefix_equal(group_prefix, np)) {
      lookahead_row_ = std::move(nxt);
      lookahead_prefix_ = std::move(np);
      has_lookahead_ = true;
      break;
    }
    RETURN_IF_FAIL(sorter->AddRow(std::move(nxt)));
  }

  RETURN_IF_FAIL(sorter->Finish());
  group_sorter_ = std::move(sorter);
  return Status::kSuccess;
}

StatusOr<bool> IncrementalSortSource::NextBatch(PipelineContext* ctx,
                                                DataChunk* out,
                                                size_t max_rows) {
  const size_t base_cols = schema_.ColumnCount();
  out->Reset(schema_, max_rows);
  while (out->Size() < max_rows) {
    if (singleton_row_.has_value()) {
      singleton_row_->values_.resize(base_cols);
      out->Append(std::move(*singleton_row_), RowPosition());
      singleton_row_.reset();
      continue;
    }
    if (group_sorter_ != nullptr) {
      Row r;
      ASSIGN_OR_RETURN(bool, has, group_sorter_->NextRow(&r));
      if (has) {
        if (r.Count() > base_cols) {
          r.values_.resize(base_cols);
        }
        out->Append(std::move(r), RowPosition());
        continue;
      }
      group_sorter_.reset();
    }
    RETURN_IF_FAIL(FillNextGroup(ctx));
    if (!singleton_row_.has_value() && group_sorter_ == nullptr) {
      consumer_guard_.reset();
      break;
    }
  }
  return !out->Empty();
}

void IncrementalSortSource::Explain(std::ostream& out, int indent) const {
  DumpExplain(out, indent);
}

void IncrementalSortSource::DumpExplain(std::ostream& out, int indent) const {
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  out << pad << "IncrementalSort";
  if (singleton_runs_ != 0) {
    out << " singleton_runs=" << singleton_runs_;
  }
  if (!prefix_keys_.empty() && prefix_keys_.front().expression) {
    const Expression& prefix = prefix_keys_.front().expression;
    if (prefix->Type() == TypeTag::kColumnValue) {
      out << " presorted=" << prefix->AsColumnValue().GetColumnName().name;
    } else {
      out << " presorted=" << prefix->ToString();
    }
  }
  out << " (prefix_keys=" << prefix_keys_.size()
      << ", suffix_keys=" << suffix_keys_.size() << ")\n";
  DumpStreamChain(out, indent + 2, /*is_dump=*/true, upstream_source_,
                  upstream_ops_);
}

// --- 5. IndexSkipScanSource & MinMaxIndexSource ---

IndexSkipScanSource::IndexSkipScanSource(
    std::shared_ptr<PipelineSource> inner_source,
    std::vector<std::shared_ptr<PipelineOperator>> ops, Schema schema)
    : inner_source_(std::move(inner_source)),
      ops_(std::move(ops)),
      schema_(std::move(schema)) {}

Status IndexSkipScanSource::Open(PipelineContext* ctx) {
  if (inner_source_ != nullptr) {
    RETURN_IF_FAIL(inner_source_->Open(ctx));
  }
  for (const auto& op : ops_) {
    RETURN_IF_FAIL(op->Open(ctx));
  }
  return Status::kSuccess;
}

StatusOr<bool> IndexSkipScanSource::NextBatch(PipelineContext* ctx,
                                              DataChunk* out, size_t max_rows) {
  bool eof = false;
  return PullStreamChunk(ctx, inner_source_.get(), ops_, out, max_rows, &eof);
}

void IndexSkipScanSource::Explain(std::ostream& out, int indent) const {
  DumpExplain(out, indent);
}

void IndexSkipScanSource::DumpExplain(std::ostream& out, int indent) const {
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  out << pad << "IndexSkipScan\n";
  DumpStreamChain(out, indent + 2, /*is_dump=*/true, inner_source_, ops_);
}

MinMaxIndexSource::MinMaxIndexSource(
    std::shared_ptr<PipelineSource> child_source,
    std::vector<std::shared_ptr<PipelineOperator>> ops, Schema out_schema,
    size_t value_slot)
    : child_source_(std::move(child_source)),
      ops_(std::move(ops)),
      out_schema_(std::move(out_schema)),
      value_slot_(value_slot) {}

Status MinMaxIndexSource::Open(PipelineContext* ctx) {
  emitted_ = false;
  if (child_source_ != nullptr) {
    RETURN_IF_FAIL(child_source_->Open(ctx));
  }
  for (const auto& op : ops_) {
    RETURN_IF_FAIL(op->Open(ctx));
  }
  return Status::kSuccess;
}

StatusOr<bool> MinMaxIndexSource::NextBatch(PipelineContext* ctx,
                                            DataChunk* out, size_t max_rows) {
  if (max_rows == 0 || emitted_) {
    return false;
  }
  emitted_ = true;
  out->Reset(out_schema_, 1);

  bool eof = false;
  while (!eof) {
    DataChunk batch;
    ASSIGN_OR_RETURN(
        bool, got,
        PullStreamChunk(ctx, child_source_.get(), ops_, &batch, 1, &eof));
    if (!got || batch.Empty()) {
      break;
    }
    for (size_t i = 0; i < batch.Size(); ++i) {
      const Row r = batch.RowAt(i);
      if (value_slot_ < r.Count() && !r[value_slot_].IsNull()) {
        out->Append(Row({r[value_slot_]}), RowPosition());
        return true;
      }
    }
  }
  out->Append(Row({Value()}), RowPosition());
  return true;
}

void MinMaxIndexSource::Explain(std::ostream& out, int indent) const {
  DumpExplain(out, indent);
}

void MinMaxIndexSource::DumpExplain(std::ostream& out, int indent) const {
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  out << pad << "MinMaxIndex limit=1\n";
  DumpStreamChain(out, indent + 2, /*is_dump=*/true, child_source_, ops_);
}

// --- 6. OocBitmapScanSource & OocMultiIndexCoveringSource ---

namespace {

Row EncodeRowPositionKey(RowPosition pos) {
  return Row({Value(static_cast<int64_t>(pos.page_id >> 32)),
              Value(static_cast<int64_t>(pos.page_id & 0xFFFFFFFFULL)),
              Value(static_cast<int64_t>(pos.slot))});
}

// Spilled position keys come back from ExternalSorter/SpillStore; a torn or
// mistyped row must surface as kCorrupt instead of reading an arbitrary int
// payload (possibly a VARCHAR pointer) as a RowPosition.
StatusOr<RowPosition> TryDecodeRowPositionKey(const Row& r) {
  if (r.Count() < 3) {
    return StatusError(StatusCode::kCorrupt, "truncated row-position key");
  }
  for (size_t i = 0; i < 3; ++i) {
    if (r[i].IsNull() || r[i].type != ValueType::kInt64) {
      return StatusError(StatusCode::kCorrupt, "invalid row-position key");
    }
  }
  constexpr auto kPageIdHalfMax =
      static_cast<int64_t>(std::numeric_limits<uint32_t>::max());
  const int64_t hi_value = r[0].value.int_value;
  const int64_t lo_value = r[1].value.int_value;
  const int64_t slot_value = r[2].value.int_value;
  if (hi_value < 0 || hi_value > kPageIdHalfMax || lo_value < 0 ||
      lo_value > kPageIdHalfMax || slot_value < 0 ||
      std::cmp_greater_equal(slot_value,std::numeric_limits<slot_t>::max()))) {
    return StatusError(StatusCode::kCorrupt, "out-of-range row-position key");
  }
  const RowPosition pos{(static_cast<uint64_t>(hi_value) << 32) |
                            static_cast<uint64_t>(lo_value),
                        static_cast<slot_t>(slot_value)};
  if (!pos.IsValid()) {
    return StatusError(StatusCode::kCorrupt, "invalid row-position key");
  }
  return pos;
}

bool PosBefore(const RowPosition& a, const RowPosition& b) noexcept {
  return a.page_id != b.page_id ? (a.page_id < b.page_id) : (a.slot < b.slot);
}

std::vector<OocSortKey> RowPositionSortKeys() {
  return {OocSortKey{.column_index = 0}, OocSortKey{.column_index = 1},
          OocSortKey{.column_index = 2}};
}

}  // namespace

OocBitmapScanSource::OocBitmapScanSource(
    const Table* table, std::vector<OocIndexRange> ranges, bool is_and,
    Expression where, Schema schema, OocScanFallbackFactory fallback_factory)
    : table_(table),
      ranges_(std::move(ranges)),
      is_and_(is_and),
      where_(std::move(where)),
      where_conjuncts_(SplitConjuncts(where_)),
      schema_(std::move(schema)),
      fallback_factory_(std::move(fallback_factory)) {}

Status OocBitmapScanSource::AdvanceCursor(SortedPosCursor* c) {
  Row r;
  while (true) {
    ASSIGN_OR_RETURN(bool, got, c->sorter->NextRow(&r));
    if (!got) {
      c->valid = false;
      return Status::kSuccess;
    }
    ASSIGN_OR_RETURN(RowPosition, pos, TryDecodeRowPositionKey(r));
    if (c->valid && pos == c->cur) {
      continue;  // Deduplicate repeated index keys on non-unique index.
    }
    c->cur = pos;
    c->valid = true;
    return Status::kSuccess;
  }
}

Status OocBitmapScanSource::Open(PipelineContext* ctx) {
  txn_ = ctx->txn;
  cursors_.clear();
  last_emitted_pos_.reset();
  scanned_positions_ = 0;
  fallback_full_scan_ = false;
  fallback_source_.reset();

  if (table_ == nullptr || txn_ == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid BitmapScan state");
  }

  std::vector<std::pair<page_id_t, TransactionManager::IndexReadGuard>> held;
  auto setup_fallback = [&]() -> Status {
    held.clear();
    cursors_.clear();
    fallback_full_scan_ = true;
    if (!fallback_factory_) {
      return StatusError(StatusCode::kInvalidArgument,
                         "BitmapScan stale fallback factory unavailable");
    }
    fallback_source_ = fallback_factory_(ctx);
    if (fallback_source_ == nullptr) {
      return StatusError(StatusCode::kInvalidArgument,
                         "BitmapScan fallback factory returned no source");
    }
    return fallback_source_->Open(ctx);
  };

  if (ranges_.empty()) {
    return Status::kSuccess;
  }

  std::vector<page_id_t> roots;
  roots.reserve(ranges_.size());
  for (const OocIndexRange& r : ranges_) {
    roots.push_back(r.index->Root());
  }
  std::ranges::sort(roots);
  roots.erase(std::unique(roots.begin(), roots.end()), roots.end());

  held.reserve(roots.size());
  for (const page_id_t root : roots) {
    if (txn_->txn_.IndexKeysMayBeStale(root)) {
      return setup_fallback();
    }
    auto guard = txn_->txn_.AcquireIndexReadGuard(root);
    held.emplace_back(root, std::move(guard));
    if (txn_->txn_.IndexKeysMayBeStale(root)) {
      return setup_fallback();
    }
  }

  const size_t total_budget = ctx->OperatorBudgetBytes();
  // A per-sorter 16 KiB floor would let N ranges hold 16 KiB * N.
  const size_t per_sorter_budget =
      total_budget / std::max<size_t>(1, ranges_.size());

  for (const OocIndexRange& range : ranges_) {
    const auto found = std::ranges::find_if(
        held, [root = range.index->Root()](const auto& entry) {
          return entry.first == root;
        });
    if (found == held.end()) {
      return setup_fallback();
    }
    ExternalSorter::Options opts;
    opts.spill_store = &ctx->spill_store;
    opts.memory_budget_bytes = per_sorter_budget;
    opts.grant = &ctx->grant;
    opts.keys = RowPositionSortKeys();
    opts.force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
    auto sorter = std::make_unique<ExternalSorter>(std::move(opts));

    IndexScanIterator it(*table_, *range.index, txn_->txn_, range.begin_key,
                         range.end_key, true, false, &found->second);
    for (; it.IsValid(); ++it) {
      if (it.Position().IsValid()) {
        RETURN_IF_FAIL(sorter->AddRow(EncodeRowPositionKey(it.Position())));
      }
    }
    if (it.GetStatus() != Status::kSuccess) {
      return it.GetStatus();
    }
    RETURN_IF_FAIL(sorter->Finish());

    SortedPosCursor cur;
    cur.sorter = std::move(sorter);
    Row first_row;
    ASSIGN_OR_RETURN(bool, first_ok, cur.sorter->NextRow(&first_row));
    if (first_ok) {
      ASSIGN_OR_RETURN(RowPosition, first_pos,
                       TryDecodeRowPositionKey(first_row));
      cur.cur = first_pos;
      cur.valid = true;
    }
    cursors_.push_back(std::move(cur));
  }
  return Status::kSuccess;
}

StatusOr<bool> OocBitmapScanSource::NextPosition(RowPosition* out) {
  if (cursors_.empty()) {
    return false;
  }
  if (!is_and_) {
    while (true) {
      size_t best = cursors_.size();
      for (size_t i = 0; i < cursors_.size(); ++i) {
        if (!cursors_[i].valid) {
          continue;
        }
        if (best == cursors_.size() ||
            PosBefore(cursors_[i].cur, cursors_[best].cur)) {
          best = i;
        }
      }
      if (best == cursors_.size()) {
        return false;
      }
      const RowPosition picked = cursors_[best].cur;
      for (SortedPosCursor& cursor : cursors_) {
        while (cursor.valid && cursor.cur == picked) {
          RETURN_IF_FAIL(AdvanceCursor(&cursor));
        }
      }
      if (last_emitted_pos_.has_value() && *last_emitted_pos_ == picked) {
        continue;
      }
      last_emitted_pos_ = picked;
      *out = picked;
      return true;
    }
  }

  // K-way sorted intersection:
  while (true) {
    RowPosition max_pos = cursors_[0].cur;
    bool all_valid = cursors_[0].valid;
    bool all_equal = true;
    for (size_t i = 1; i < cursors_.size(); ++i) {
      if (!cursors_[i].valid) {
        all_valid = false;
        break;
      }
      if (cursors_[i].cur != max_pos) {
        all_equal = false;
        if (PosBefore(max_pos, cursors_[i].cur)) {
          max_pos = cursors_[i].cur;
        }
      }
    }
    if (!all_valid) {
      return false;
    }
    if (all_equal) {
      *out = max_pos;
      for (auto& c : cursors_) {
        RETURN_IF_FAIL(AdvanceCursor(&c));
      }
      return true;
    }
    for (auto& c : cursors_) {
      while (c.valid && PosBefore(c.cur, max_pos)) {
        RETURN_IF_FAIL(AdvanceCursor(&c));
      }
    }
  }
}

StatusOr<bool> OocBitmapScanSource::NextBatch(PipelineContext* ctx,
                                              DataChunk* out, size_t max_rows) {
  if (fallback_full_scan_) {
    return fallback_source_->NextBatch(ctx, out, max_rows);
  }
  out->Reset(schema_, max_rows);
  while (out->Size() < max_rows) {
    RowPosition pos;
    ASSIGN_OR_RETURN(bool, got, NextPosition(&pos));
    if (!got) {
      break;
    }
    ++scanned_positions_;
    StatusOr<Row> row_or = table_->Read(txn_->txn_, pos);
    if (row_or.GetStatus() == Status::kNotExists) {
      continue;
    }
    if (row_or.GetStatus() != Status::kSuccess) {
      return row_or.GetStatus();
    }
    if (where_) {
      ASSIGN_OR_RETURN(bool, pass,
                       relational_detail::EvaluateConjunctsTolerant(
                           where_conjuncts_, [&](const Expression& conjunct) {
                             return conjunct->TryEvaluate(row_or.Value(),
                                                          schema_);
                           }));
      if (!pass) {
        continue;
      }
    }
    out->Append(std::move(row_or.Value()), pos);
  }
  return !out->Empty();
}

void OocBitmapScanSource::DumpExplain(std::ostream& out, int indent) const {
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  if (fallback_full_scan_ && fallback_source_ != nullptr) {
    fallback_source_->DumpExplain(out, indent);
    return;
  }
  out << pad << (is_and_ ? "BitmapAnd" : "BitmapOr") << "\n"
      << pad << "  BitmapHeapScan: (" << scanned_positions_ << " positions)\n";
  if (where_) {
    out << pad << "  Recheck: " << *where_ << "\n";
  }
}

OocMultiIndexCoveringSource::OocMultiIndexCoveringSource(
    const Table* table, std::vector<OocIndexRange> participants,
    Expression where, std::vector<slot_t> covered_slots, Schema output_schema,
    bool force_fallback, OocScanFallbackFactory fallback_factory)
    : table_(table),
      where_(std::move(where)),
      where_conjuncts_(SplitConjuncts(where_)),
      covered_slots_(std::move(covered_slots)),
      output_schema_(std::move(output_schema)),
      force_fallback_(force_fallback),
      fallback_factory_(std::move(fallback_factory)) {
  participants_.reserve(participants.size());
  for (OocIndexRange& range : participants) {
    ParticipantState ps;
    ps.range = std::move(range);
    std::unordered_map<slot_t, size_t> image_index;
    size_t img_off = 0;
    for (const slot_t k : ps.range.index->sc_.key_) {
      image_index.emplace(k, img_off++);
    }
    for (const slot_t inc : ps.range.index->sc_.include_) {
      image_index.emplace(inc, img_off++);
    }
    for (size_t out_col = 0; out_col < covered_slots_.size(); ++out_col) {
      const auto it = image_index.find(covered_slots_[out_col]);
      if (it != image_index.end()) {
        ps.fills.emplace_back(out_col, it->second);
      }
    }
    participants_.push_back(std::move(ps));
  }
}

Status OocMultiIndexCoveringSource::BuildFallbackScan(PipelineContext* ctx) {
  if (!fallback_factory_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "MultiIndexCoveringScan fallback factory unavailable");
  }
  fallback_source_ = fallback_factory_(ctx);
  if (fallback_source_ == nullptr) {
    return StatusError(
        StatusCode::kUnknown,
        "MultiIndexCoveringScan fallback factory returned no source");
  }
  return Status::kSuccess;
}

Status OocMultiIndexCoveringSource::AdvanceParticipant(ParticipantState* p) {
  Row r;
  while (true) {
    ASSIGN_OR_RETURN(bool, got, p->sorter->NextRow(&r));
    if (!got) {
      p->valid = false;
      return Status::kSuccess;
    }
    ASSIGN_OR_RETURN(RowPosition, pos, TryDecodeRowPositionKey(r));
    if (p->valid && pos == p->cur_pos) {
      continue;  // Deduplicate repeated index keys on non-unique index.
    }
    p->cur_pos = pos;
    p->cur_row = std::move(r);
    p->valid = true;
    return Status::kSuccess;
  }
}

Status OocMultiIndexCoveringSource::Open(PipelineContext* ctx) {
  txn_ = ctx->txn;
  fallback_source_.reset();
  if (table_ == nullptr || txn_ == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid MultiIndexCoveringScan state");
  }
  if (force_fallback_ || participants_.empty()) {
    RETURN_IF_FAIL(BuildFallbackScan(ctx));
    return fallback_source_->Open(ctx);
  }

  std::vector<page_id_t> roots;
  roots.reserve(participants_.size());
  for (const ParticipantState& p : participants_) {
    roots.push_back(p.range.index->Root());
  }
  std::ranges::sort(roots);
  roots.erase(std::unique(roots.begin(), roots.end()), roots.end());

  std::vector<std::pair<page_id_t, TransactionManager::IndexReadGuard>> held;
  held.reserve(roots.size());
  for (const page_id_t root : roots) {
    if (txn_->txn_.IndexKeysMayBeStale(root)) {
      RETURN_IF_FAIL(BuildFallbackScan(ctx));
      return fallback_source_->Open(ctx);
    }
    auto guard = txn_->txn_.AcquireIndexReadGuard(root);
    if (txn_->txn_.IndexKeysMayBeStale(root)) {
      RETURN_IF_FAIL(BuildFallbackScan(ctx));
      return fallback_source_->Open(ctx);
    }
    held.emplace_back(root, std::move(guard));
  }

  const size_t per_budget = std::max<size_t>(
      16384, ctx->OperatorBudgetBytes() / participants_.size());
  for (ParticipantState& ps : participants_) {
    const auto found = std::ranges::find_if(
        held, [root = ps.range.index->Root()](const auto& entry) {
          return entry.first == root;
        });
    if (found == held.end()) {
      held.clear();
      RETURN_IF_FAIL(BuildFallbackScan(ctx));
      return fallback_source_->Open(ctx);
    }
    ExternalSorter::Options opts;
    opts.spill_store = &ctx->spill_store;
    opts.memory_budget_bytes = per_budget;
    opts.grant = &ctx->grant;
    opts.keys = RowPositionSortKeys();
    opts.force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
    auto sorter = std::make_unique<ExternalSorter>(std::move(opts));

    IndexScanIterator iter(*table_, *ps.range.index, txn_->txn_,
                           ps.range.begin_key, ps.range.end_key, true, false,
                           &found->second);
    for (; iter.IsValid(); ++iter) {
      const RowPosition pos = iter.Position();
      if (!pos.IsValid()) {
        continue;
      }
      Row encoded = EncodeRowPositionKey(pos);
      Row image;
      const bool index_only = iter.TryReadIndexOnly(&image);
      encoded.values_.emplace_back(static_cast<int64_t>(index_only ? 0 : 1));
      if (index_only) {
        for (const auto& [out_col, entry_idx] : ps.fills) {
          (void)out_col;
          encoded.values_.push_back(entry_idx < image.Count() ? image[entry_idx]
                                                              : Value());
        }
      }
      RETURN_IF_FAIL(sorter->AddRow(std::move(encoded)));
    }
    if (iter.GetStatus() != Status::kSuccess) {
      return iter.GetStatus();
    }
    RETURN_IF_FAIL(sorter->Finish());
    ps.sorter = std::move(sorter);
    ps.valid = false;
    RETURN_IF_FAIL(AdvanceParticipant(&ps));
  }
  return Status::kSuccess;
}

StatusOr<bool> OocMultiIndexCoveringSource::NextBatch(PipelineContext* ctx,
                                                      DataChunk* out,
                                                      size_t max_rows) {
  if (fallback_source_ != nullptr) {
    return fallback_source_->NextBatch(ctx, out, max_rows);
  }
  out->Reset(output_schema_, max_rows);
  while (out->Size() < max_rows) {
    RowPosition max_pos = participants_[0].cur_pos;
    bool all_valid = participants_[0].valid;
    bool all_equal = true;
    for (size_t i = 1; i < participants_.size(); ++i) {
      if (!participants_[i].valid) {
        all_valid = false;
        break;
      }
      if (participants_[i].cur_pos != max_pos) {
        all_equal = false;
        if (PosBefore(max_pos, participants_[i].cur_pos)) {
          max_pos = participants_[i].cur_pos;
        }
      }
    }
    if (!all_valid) {
      break;
    }
    if (!all_equal) {
      for (ParticipantState& ps : participants_) {
        while (ps.valid && PosBefore(ps.cur_pos, max_pos)) {
          RETURN_IF_FAIL(AdvanceParticipant(&ps));
        }
      }
      continue;
    }

    bool need_heap = false;
    Row assembled;
    assembled.values_.resize(output_schema_.ColumnCount());
    for (const ParticipantState& ps : participants_) {
      const Row& r = ps.cur_row;
      if (r.Count() <= 3 || r[3].value.int_value != 0) {
        need_heap = true;
        break;
      }
      for (size_t k = 0; k < ps.fills.size(); ++k) {
        const size_t out_col = ps.fills[k].first;
        if (4 + k < r.Count() && out_col < assembled.values_.size()) {
          assembled.values_[out_col] = r[4 + k];
        }
      }
    }

    for (ParticipantState& ps : participants_) {
      RETURN_IF_FAIL(AdvanceParticipant(&ps));
    }

    if (need_heap) {
      StatusOr<Row> row_or = table_->Read(txn_->txn_, max_pos);
      if (row_or.GetStatus() == Status::kNotExists) {
        continue;
      }
      if (row_or.GetStatus() != Status::kSuccess) {
        return row_or.GetStatus();
      }
      const Row& heap = row_or.Value();
      for (size_t i = 0; i < covered_slots_.size(); ++i) {
        const slot_t slot = covered_slots_[i];
        if (slot >= heap.Count()) {
          return StatusError(StatusCode::kCorrupt,
                             "covering scan column is outside the heap row");
        }
        if (i >= assembled.values_.size()) {
          return StatusError(StatusCode::kCorrupt,
                             "covering scan slot is outside the output row");
        }
        assembled.values_[i] = heap[slot];
      }
    }

    if (where_) {
      ASSIGN_OR_RETURN(bool, pass,
                       relational_detail::EvaluateConjunctsTolerant(
                           where_conjuncts_, [&](const Expression& conjunct) {
                             return conjunct->TryEvaluate(assembled,
                                                          output_schema_);
                           }));
      if (!pass) {
        continue;
      }
    }
    out->Append(std::move(assembled), max_pos);
  }
  return !out->Empty();
}

void OocMultiIndexCoveringSource::DumpExplain(std::ostream& out,
                                              int indent) const {
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  if (fallback_source_ != nullptr) {
    fallback_source_->DumpExplain(out, indent);
    return;
  }
  out << pad << "MultiIndexCoveringScan: " << participants_.size()
      << " indexes\n";
  for (const ParticipantState& ps : participants_) {
    out << pad << "  Cover: " << ps.range.index->sc_.name_ << " on "
        << table_->GetSchema().Name() << "\n";
  }
  if (where_) {
    out << pad << "  Recheck: " << *where_ << "\n";
  }
}

}  // namespace tinylamb
