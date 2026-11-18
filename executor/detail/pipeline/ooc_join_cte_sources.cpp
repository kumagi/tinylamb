/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/join_kind.hpp"
#include "common/memory_budget.hpp"
#include "common/set_operation.hpp"
#include "common/spill/spill_store.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/ooc/chunk_codec.hpp"
#include "executor/detail/ooc/external_sorter.hpp"
#include "executor/detail/pipeline/ooc_sinks.hpp"
#include "executor/detail/pipeline/pipeline_ops.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/work_table_index.hpp"
#include "expression/binary_expression.hpp"
#include "expression/expression.hpp"
#include "expression/rewrite.hpp"
#include "page/row_position.hpp"
#include "query/statement.hpp"
#include "type/row.hpp"
#include "type/row_block_codec.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

constexpr size_t kMinJoinMemBudget = 16384;

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

void SetBit(std::vector<uint64_t>* bits, uint64_t idx) {
  const auto word = static_cast<size_t>(idx >> 6);
  if (word >= bits->size()) {
    bits->resize(word + 1, 0);
  }
  (*bits)[word] |= (uint64_t{1} << (idx & 63U));
}

bool GetBit(const std::vector<uint64_t>& bits, uint64_t idx) noexcept {
  const auto word = static_cast<size_t>(idx >> 6);
  if (word >= bits.size()) {
    return false;
  }
  return (bits[word] & (uint64_t{1} << (idx & 63U))) != 0;
}

bool JoinValueEqual(const Value& left, const Value& right) {
  if (left.IsNull() || right.IsNull()) {
    return false;
  }
  const auto result = TryEvaluateBinary(BinaryOperation::kEquals, left, right);
  return result.HasValue() && !result.Value().IsNull() &&
         result.Value().Truthy();
}

bool JoinValueUnmatchable(const Value& value) {
  return value.type == ValueType::kDouble &&
         std::isnan(value.value.double_value);
}

bool IdenticalValue(const Value& a, const Value& b);



bool EqualRowsExact(const Row& a, const Row& b) {
  if (a.Count() != b.Count()) {
    return false;
  }
  for (size_t i = 0; i < a.Count(); ++i) {
    if (!IdenticalValue(a[i], b[i])) {
      return false;
    }
  }
  return true;
}

// True bit-exact value equality for LRU cache keys: type, collation tag
// (unsigned / CI / interval) and payload must all agree.  Unlike
// Value::operator== this does not fold -0.0/+0.0 or NaN payloads, and unlike
// EqualUnderCollation it does not fold case-insensitive varchar pairs.
bool IdenticalValue(const Value& a, const Value& b) {
  if (a.type != b.type || a.Collation() != b.Collation()) {
    return false;
  }
  if (a.IsNull() || b.IsNull()) {
    return a.IsNull() && b.IsNull();
  }
  switch (a.type) {
    case ValueType::kInt64:
      return a.value.int_value == b.value.int_value;
    case ValueType::kDate:
      return a.value.int_value == b.value.int_value;
    case ValueType::kDouble:
      // Compare raw bits so -0.0 != +0.0 and distinct NaN payloads differ.
      return std::bit_cast<uint64_t>(a.value.double_value) ==
             std::bit_cast<uint64_t>(b.value.double_value);
    case ValueType::kVarChar:
      return a.value.varchar_value == b.value.varchar_value;
    default:
      return false;
  }
}

Row NullRow(size_t cols) { return Row(std::vector<Value>(cols)); }

}  // namespace

// --- 1. OocMergeJoinSource ---

OocMergeJoinSource::OocMergeJoinSource(
    OocSetOpInputStream left, OocSetOpInputStream right,
    std::vector<slot_t> left_cols, std::vector<slot_t> right_cols,
    JoinKind kind, size_t left_width, size_t right_width, Expression residual,
    Schema residual_schema, Schema output_schema)
    : left_cols_(std::move(left_cols)),
      right_cols_(std::move(right_cols)),
      kind_(kind),
      left_width_(left_width),
      right_width_(right_width),
      residual_(std::move(residual)),
      residual_conjuncts_(SplitConjuncts(residual_)),
      residual_schema_(std::move(residual_schema)),
      output_schema_(std::move(output_schema)) {
  left_.stream = std::move(left);
  right_.stream = std::move(right);
}

Status OocMergeJoinSource::AdvanceCursor(PipelineContext* ctx, RowCursor* c) {
  while (c->idx >= c->chunk.Size()) {
    c->chunk.Reset();
    c->idx = 0;
    if (c->eof) {
      c->has_cur = false;
      return Status::kSuccess;
    }
    ASSIGN_OR_RETURN(
        bool, got,
        PullStreamChunk(ctx, c->stream.source.get(), c->stream.operators,
                        &c->chunk, kDefaultVectorSize, &c->eof));
    if (!got || c->chunk.Empty()) {
      c->eof = true;
      c->has_cur = false;
      return Status::kSuccess;
    }
  }
  c->cur = c->chunk.RowAt(c->idx);
  c->cur_pos = c->chunk.PositionAt(c->idx);
  ++c->idx;
  c->has_cur = true;
  return Status::kSuccess;
}

bool OocMergeJoinSource::KeyCannotMatch(const Row& row,
                                        const std::vector<slot_t>& cols) {
  return std::ranges::any_of(cols, [&](slot_t col) {
    return col >= row.Count() || row[col].IsNull() ||
           JoinValueUnmatchable(row[col]);
  });
}

int OocMergeJoinSource::CompareKeys(const Row& l, const Row& r) const {
  for (size_t i = 0; i < left_cols_.size(); ++i) {
    const Value& lhs = l[left_cols_[i]];
    const Value& rhs = r[right_cols_[i]];
    if (JoinValueEqual(lhs, rhs)) {
      continue;
    }
    if (!lhs.IsNull() && !rhs.IsNull() && lhs.type != rhs.type) {
      return static_cast<int>(lhs.type) < static_cast<int>(rhs.type) ? -1 : 1;
    }
    return CompareForOrderBy(lhs, rhs) < 0 ? -1 : 1;
  }
  return 0;
}

int OocMergeJoinSource::CompareLeftKeys(const Row& a, const Row& b) const {
  for (const slot_t col : left_cols_) {
    const Value& lhs = a[col];
    const Value& rhs = b[col];
    if (JoinValueEqual(lhs, rhs)) {
      continue;
    }
    if (!lhs.IsNull() && !rhs.IsNull() && lhs.type != rhs.type) {
      return static_cast<int>(lhs.type) < static_cast<int>(rhs.type) ? -1 : 1;
    }
    return CompareForOrderBy(lhs, rhs) < 0 ? -1 : 1;
  }
  return 0;
}

StatusOr<bool> OocMergeJoinSource::PairPasses(const Row& l,
                                              const Row& r) const {
  if (!residual_) {
    return true;
  }
  const Row combined = l + r;
  if (kind_ == JoinKind::kInner) {
    return relational_detail::EvaluateConjunctsTolerant(
        residual_conjuncts_, [&](const Expression& conjunct) {
          return conjunct->TryEvaluate(combined, residual_schema_);
        });
  }
  ASSIGN_OR_RETURN(Value, res,
                   residual_->TryEvaluate(combined, residual_schema_));
  return !res.IsNull() && res.Truthy();
}

Status OocMergeJoinSource::Open(PipelineContext* ctx) {
  budget_bytes_ =
      std::max<size_t>(kMinJoinMemBudget, ctx->OperatorBudgetBytes());
  if (left_.stream.source != nullptr) {
    RETURN_IF_FAIL(left_.stream.source->Open(ctx));
  }
  for (const auto& op : left_.stream.operators) {
    RETURN_IF_FAIL(op->Open(ctx));
  }
  if (right_.stream.source != nullptr) {
    RETURN_IF_FAIL(right_.stream.source->Open(ctx));
  }
  for (const auto& op : right_.stream.operators) {
    RETURN_IF_FAIL(op->Open(ctx));
  }
  RETURN_IF_FAIL(AdvanceCursor(ctx, &left_));
  RETURN_IF_FAIL(AdvanceCursor(ctx, &right_));
  return Status::kSuccess;
}

StatusOr<bool> OocMergeJoinSource::NextBatch(PipelineContext* ctx,
                                             DataChunk* out, size_t max_rows) {
  const bool semi = kind_ == JoinKind::kSemi;
  const bool anti = kind_ == JoinKind::kAnti;
  const bool left_outer =
      kind_ == JoinKind::kLeftOuter || kind_ == JoinKind::kFullOuter;
  const bool right_outer =
      kind_ == JoinKind::kRightOuter || kind_ == JoinKind::kFullOuter;
  const bool keep_pos = semi || anti;

  out->Reset(output_schema_, max_rows);

  while (out->Size() < max_rows) {
    // 1. Drain unmatched right rows collected for `kRightOuter`/`kFullOuter`.
    if (draining_unmatched_right_) {
      while (out->Size() < max_rows) {
        if (unmatched_right_idx_ >= unmatched_right_chunk_.Size()) {
          unmatched_right_chunk_.Reset();
          unmatched_right_idx_ = 0;
          ASSIGN_OR_RETURN(
              bool, got, right_run_store_->NextChunk(&unmatched_right_chunk_));
          if (!got) {
            draining_unmatched_right_ = false;
            right_run_store_.reset();
            right_matched_bits_.clear();
            in_run_ = false;
            break;
          }
        }
        const Row r = unmatched_right_chunk_.RowAt(unmatched_right_idx_++);
        const uint64_t ridx = unmatched_right_global_idx_++;
        if (!GetBit(right_matched_bits_, ridx)) {
          out->Append(NullRow(left_width_) + r, RowPosition());
        }
      }
      continue;
    }

    // 2. Active probe of one left row across `right_run_store_`.
    if (scan_right_active_) {
      bool done_with_left_row = false;
      while (out->Size() < max_rows) {
        if (active_right_idx_ >= active_right_chunk_.Size()) {
          active_right_chunk_.Reset();
          active_right_idx_ = 0;
          ASSIGN_OR_RETURN(bool, got,
                           right_run_store_->NextChunk(&active_right_chunk_));
          if (!got) {
            done_with_left_row = true;
            break;
          }
        }
        const Row r = active_right_chunk_.RowAt(active_right_idx_++);
        const uint64_t ridx = active_right_global_idx_++;
        ASSIGN_OR_RETURN(bool, ok, PairPasses(active_left_row_, r));
        if (!ok) {
          continue;
        }
        active_left_matched_ = true;
        if (right_outer) {
          SetBit(&right_matched_bits_, ridx);
        }
        if (semi) {
          out->Append(active_left_row_, active_left_pos_);
          done_with_left_row = true;
          break;
        }
        if (anti) {
          done_with_left_row = true;
          break;
        }
        out->Append(active_left_row_ + r, RowPosition());
      }
      if (done_with_left_row) {
        scan_right_active_ = false;
        active_right_chunk_.Reset();
        active_right_idx_ = 0;
        if (!active_left_matched_) {
          if (anti) {
            out->Append(active_left_row_, active_left_pos_);
          } else if (left_outer) {
            out->Append(active_left_row_ + NullRow(right_width_),
                        RowPosition());
          }
        }
      }
      continue;
    }

    // 3. Inside an equal-key run: pull next left row matching
    // `right_run_store_`.
    if (in_run_) {
      if (left_.has_cur && !KeyCannotMatch(left_.cur, left_cols_) &&
          CompareLeftKeys(left_.cur, active_left_row_) == 0) {
        active_left_row_ = std::move(left_.cur);
        active_left_pos_ = left_.cur_pos;
        RETURN_IF_FAIL(AdvanceCursor(ctx, &left_));

        if ((semi || anti) && !residual_) {
          if (semi) {
            out->Append(active_left_row_, active_left_pos_);
          }
          continue;
        }
        active_left_matched_ = false;
        RETURN_IF_FAIL(right_run_store_->ResetCursor());
        active_right_chunk_.Reset();
        active_right_idx_ = 0;
        active_right_global_idx_ = 0;
        scan_right_active_ = true;
        continue;
      }
      // Left run finished. Check unmatched right rows if `right_outer`.
      if (right_outer && residual_ != nullptr && right_run_store_ != nullptr) {
        RETURN_IF_FAIL(right_run_store_->ResetCursor());
        unmatched_right_chunk_.Reset();
        unmatched_right_idx_ = 0;
        unmatched_right_global_idx_ = 0;
        draining_unmatched_right_ = true;
        continue;
      }
      right_run_store_.reset();
      right_matched_bits_.clear();
      in_run_ = false;
      continue;
    }

    // 4. Advance cursor pair outside equal-key run.
    if (!left_.has_cur && !right_.has_cur) {
      break;
    }
    if (!left_.has_cur) {
      if (right_outer) {
        out->Append(NullRow(left_width_) + right_.cur, RowPosition());
      }
      RETURN_IF_FAIL(AdvanceCursor(ctx, &right_));
      continue;
    }
    if (!right_.has_cur) {
      if (anti) {
        out->Append(left_.cur, keep_pos ? left_.cur_pos : RowPosition());
      } else if (left_outer) {
        out->Append(left_.cur + NullRow(right_width_), RowPosition());
      }
      RETURN_IF_FAIL(AdvanceCursor(ctx, &left_));
      continue;
    }

    if (KeyCannotMatch(left_.cur, left_cols_)) {
      if (anti) {
        out->Append(left_.cur, left_.cur_pos);
      } else if (left_outer) {
        out->Append(left_.cur + NullRow(right_width_), RowPosition());
      }
      RETURN_IF_FAIL(AdvanceCursor(ctx, &left_));
      continue;
    }
    if (KeyCannotMatch(right_.cur, right_cols_)) {
      if (right_outer) {
        out->Append(NullRow(left_width_) + right_.cur, RowPosition());
      }
      RETURN_IF_FAIL(AdvanceCursor(ctx, &right_));
      continue;
    }

    const int cmp = CompareKeys(left_.cur, right_.cur);
    if (cmp < 0) {
      if (anti) {
        out->Append(left_.cur, left_.cur_pos);
      } else if (left_outer) {
        out->Append(left_.cur + NullRow(right_width_), RowPosition());
      }
      RETURN_IF_FAIL(AdvanceCursor(ctx, &left_));
      continue;
    }
    if (cmp > 0) {
      if (right_outer) {
        out->Append(NullRow(left_width_) + right_.cur, RowPosition());
      }
      RETURN_IF_FAIL(AdvanceCursor(ctx, &right_));
      continue;
    }

    // Equal keys: start an equal-key run (`right_run_store_` bounded to B_op).
    active_left_row_ = std::move(left_.cur);
    active_left_pos_ = left_.cur_pos;
    RETURN_IF_FAIL(AdvanceCursor(ctx, &left_));

    if ((semi || anti) && !residual_) {
      // Skip materializing duplicate right rows: consume matching right run.
      while (right_.has_cur && !KeyCannotMatch(right_.cur, right_cols_) &&
             CompareKeys(active_left_row_, right_.cur) == 0) {
        RETURN_IF_FAIL(AdvanceCursor(ctx, &right_));
      }
      in_run_ = true;
      if (semi) {
        out->Append(active_left_row_, active_left_pos_);
      }
      continue;
    }

    SpillableStore::Options sopts;
    sopts.spill_store = &ctx->spill_store;
    sopts.memory_budget_bytes = budget_bytes_;
    sopts.grant = &ctx->grant;
    sopts.force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
    right_run_store_ = std::make_unique<SpillableStore>(sopts);
    right_matched_bits_.clear();

    while (right_.has_cur && !KeyCannotMatch(right_.cur, right_cols_) &&
           CompareKeys(active_left_row_, right_.cur) == 0) {
      RETURN_IF_FAIL(right_run_store_->AppendRow(right_.cur, right_.cur_pos));
      RETURN_IF_FAIL(AdvanceCursor(ctx, &right_));
    }
    RETURN_IF_FAIL(right_run_store_->Seal());

    if (right_outer) {
      right_matched_bits_.assign(
          static_cast<size_t>((right_run_store_->RowCount() + 63) / 64), 0);
    }

    in_run_ = true;
    active_left_matched_ = false;
    RETURN_IF_FAIL(right_run_store_->ResetCursor());
    active_right_chunk_.Reset();
    active_right_idx_ = 0;
    active_right_global_idx_ = 0;
    scan_right_active_ = true;
  }

  return !out->Empty();
}

void OocMergeJoinSource::DumpExplain(std::ostream& out, int indent) const {
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  out << pad << "MergeJoin (sorted keys)\n";
  DumpStreamChain(out, indent + 2, /*is_dump=*/true, left_.stream.source,
                  left_.stream.operators);
  DumpStreamChain(out, indent + 2, /*is_dump=*/true, right_.stream.source,
                  right_.stream.operators);
}

// --- 2. OocBandJoinSource ---

OocBandJoinSource::OocBandJoinSource(
    OocSetOpInputStream left, OocSetOpInputStream right, slot_t left_col,
    slot_t right_col, BinaryOperation comparison, Expression predicate,
    Schema residual_schema, JoinKind kind, size_t left_width,
    size_t right_width, Schema output_schema)
    : left_(std::move(left)),
      right_(std::move(right)),
      left_col_(left_col),
      right_col_(right_col),
      comparison_(comparison),
      predicate_(std::move(predicate)),
      predicate_conjuncts_(SplitConjuncts(predicate_)),
      residual_schema_(std::move(residual_schema)),
      kind_(kind),
      left_width_(left_width),
      right_width_(right_width),
      output_schema_(std::move(output_schema)) {}

StatusOr<std::optional<int64_t>> OocBandJoinSource::ReadKey(const Row& r,
                                                            slot_t col) const {
  if (col >= r.Count()) {
    return StatusError(StatusCode::kCorrupt,
                       "BandJoin key column is outside the row");
  }
  const Value& v = r[col];
  if (v.IsNull()) {
    return std::optional<int64_t>{};
  }
  if ((v.type != ValueType::kInt64 && v.type != ValueType::kDate) ||
      v.IsUnsigned()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "BandJoin supports only signed INT64 and DATE keys");
  }
  if (key_type_.has_value() && *key_type_ != v.type) {
    return StatusError(StatusCode::kInvalidArgument,
                       "BandJoin key columns must have the same type");
  }
  return v.value.int_value;
}

StatusOr<bool> OocBandJoinSource::PairPasses(const Row& l, const Row& r) const {
  if (!predicate_) {
    return true;
  }
  const Row combined = l + r;
  if (kind_ == JoinKind::kInner) {
    return relational_detail::EvaluateConjunctsTolerant(
        predicate_conjuncts_, [&](const Expression& conjunct) {
          return conjunct->TryEvaluate(combined, residual_schema_);
        });
  }
  ASSIGN_OR_RETURN(Value, res,
                   predicate_->TryEvaluate(combined, residual_schema_));
  return !res.IsNull() && res.Truthy();
}

Status OocBandJoinSource::Open(PipelineContext* ctx) {
  budget_bytes_ =
      std::max<size_t>(kMinJoinMemBudget, ctx->OperatorBudgetBytes());
  SpillableStore::Options sopts;
  sopts.spill_store = &ctx->spill_store;
  sopts.memory_budget_bytes = budget_bytes_ / 2;
  sopts.grant = &ctx->grant;
  sopts.force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
  right_store_ = std::make_unique<SpillableStore>(sopts);
  right_matched_bits_.clear();
  right_outer_done_ = false;

  if (right_.source != nullptr) {
    RETURN_IF_FAIL(right_.source->Open(ctx));
  }
  for (const auto& op : right_.operators) {
    RETURN_IF_FAIL(op->Open(ctx));
  }

  bool eof = false;
  int64_t prev_right = 0;
  bool have_prev_right = false;
  while (!eof) {
    DataChunk chunk;
    ASSIGN_OR_RETURN(bool, got,
                     PullStreamChunk(ctx, right_.source.get(), right_.operators,
                                     &chunk, kDefaultVectorSize, &eof));
    if (!got || chunk.Empty()) {
      break;
    }
    for (size_t i = 0; i < chunk.Size(); ++i) {
      const Row r = chunk.RowAt(i);
      ASSIGN_OR_RETURN(std::optional<int64_t>, k, ReadKey(r, right_col_));
      if (k.has_value()) {
        if (!key_type_.has_value()) {
          key_type_ = r[right_col_].type;
        }
        if (have_prev_right && *k < prev_right) {
          return StatusError(StatusCode::kInvalidArgument,
                             "BandJoin right input is not ordered");
        }
        prev_right = *k;
        have_prev_right = true;
      }
      RETURN_IF_FAIL(right_store_->AppendRow(r, chunk.PositionAt(i)));
    }
  }
  RETURN_IF_FAIL(right_store_->Seal());

  if (kind_ == JoinKind::kRightOuter || kind_ == JoinKind::kFullOuter) {
    right_matched_bits_.assign(
        static_cast<size_t>((right_store_->RowCount() + 63) / 64), 0);
  }

  if (left_.source != nullptr) {
    RETURN_IF_FAIL(left_.source->Open(ctx));
  }
  for (const auto& op : left_.operators) {
    RETURN_IF_FAIL(op->Open(ctx));
  }
  return Status::kSuccess;
}

Status OocBandJoinSource::PullNextLeftBlock(PipelineContext* ctx) {
  left_block_.clear();
  cur_left_idx_ = 0;
  left_key_index_.clear();
  cand_lo_ = cand_hi_ = cur_cand_idx_ = 0;
  right_row_current_ = false;
  size_t block_bytes = 0;
  const size_t max_block_bytes = std::max<size_t>(16384, budget_bytes_ / 2);

  while (block_bytes < max_block_bytes) {
    if (in_left_idx_ >= in_left_chunk_.Size()) {
      in_left_chunk_.Reset();
      in_left_idx_ = 0;
      if (left_eof_) {
        break;
      }
      ASSIGN_OR_RETURN(
          bool, got,
          PullStreamChunk(ctx, left_.source.get(), left_.operators,
                          &in_left_chunk_, kDefaultVectorSize, &left_eof_));
      if (!got || in_left_chunk_.Empty()) {
        left_eof_ = true;
        break;
      }
    }
    Row r = in_left_chunk_.RowAt(in_left_idx_);
    const RowPosition pos = in_left_chunk_.PositionAt(in_left_idx_);
    ++in_left_idx_;
    ASSIGN_OR_RETURN(std::optional<int64_t>, k, ReadKey(r, left_col_));
    if (k.has_value()) {
      if (!key_type_.has_value()) {
        key_type_ = r[left_col_].type;
      }
      if (have_prev_left_key_ && *k < prev_left_key_) {
        return StatusError(StatusCode::kInvalidArgument,
                           "BandJoin left input is not ordered");
      }
      prev_left_key_ = *k;
      have_prev_left_key_ = true;
    }
    block_bytes += EstimateRowMemoryBytes(r) + sizeof(LeftEntry);
    if (k.has_value()) {
      left_key_index_.emplace_back(*k, left_block_.size());
    }
    left_block_.push_back(LeftEntry{.row = std::move(r), .pos = pos, .key = k});
  }
  // The input is verified ordered (see above), but sort defensively so the
  // binary searches below are valid even if a NULL key split the run.
  std::sort(left_key_index_.begin(), left_key_index_.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });
  return Status::kSuccess;
}

StatusOr<bool> OocBandJoinSource::NextBatch(PipelineContext* ctx,
                                            DataChunk* out, size_t max_rows) {
  const bool left_outer = kind_ == JoinKind::kLeftOuter ||
                          kind_ == JoinKind::kFullOuter ||
                          kind_ == JoinKind::kSingle;
  const bool right_outer =
      kind_ == JoinKind::kRightOuter || kind_ == JoinKind::kFullOuter;
  const bool semi = kind_ == JoinKind::kSemi;
  const bool emits_pairs =
      kind_ == JoinKind::kInner || left_outer || right_outer;
  const bool anti = kind_ == JoinKind::kAnti;

  out->Reset(output_schema_, max_rows);

  while (out->Size() < max_rows) {
    if (flushing_right_outer_) {
      while (out->Size() < max_rows) {
        if (cur_right_idx_ >= cur_right_chunk_.Size()) {
          cur_right_chunk_.Reset();
          cur_right_idx_ = 0;
          ASSIGN_OR_RETURN(bool, got,
                           right_store_->NextChunk(&cur_right_chunk_));
          if (!got) {
            flushing_right_outer_ = false;
            right_outer_done_ = true;
            break;
          }
        }
        const Row rr = cur_right_chunk_.RowAt(cur_right_idx_);
        const RowPosition rpos = cur_right_chunk_.PositionAt(cur_right_idx_);
        ++cur_right_idx_;
        if (!GetBit(right_matched_bits_, right_outer_global_idx_++)) {
          out->Append(NullRow(left_width_) + rr, rpos);
        }
      }
      if (!flushing_right_outer_) {
        break;
      }
      continue;
    }

    if (flushing_left_outer_) {
      while (out->Size() < max_rows && left_outer_idx_ < left_block_.size()) {
        const LeftEntry& le = left_block_[left_outer_idx_++];
        if (!le.matched) {
          if (anti) {
            out->Append(le.row, le.pos);
          } else if (left_outer) {
            out->Append(le.row + NullRow(right_width_), le.pos);
          }
        } else if (semi) {
          out->Append(le.row, le.pos);
        }
      }
      if (left_outer_idx_ >= left_block_.size()) {
        flushing_left_outer_ = false;
        left_block_.clear();
      }
      continue;
    }

    if (emitting_pairs_) {
      // Replay the right-major pair stream left-major: the tags are (left
      // block index, right global index), and left_block_ is still alive.
      while (out->Size() < max_rows) {
        ASSIGN_OR_RETURN(bool, got, pair_sorter_->NextRow(&tagged_row_));
        if (!got) {
          pair_sorter_.reset();
          emitting_pairs_ = false;
          flushing_left_outer_ = true;
          left_outer_idx_ = 0;
          break;
        }
        const size_t total = tagged_row_.Count();
        if (total < 3) {
          return StatusError(StatusCode::kCorrupt,
                             "BandJoin tagged pair row is too narrow");
        }
        const auto le_idx =
            static_cast<size_t>(tagged_row_[0].value.int_value);
        if (le_idx >= left_block_.size()) {
          return StatusError(StatusCode::kCorrupt,
                             "BandJoin tagged pair row names a dead entry");
        }
        std::vector<Value> vals;
        vals.reserve(total - 2);
        for (size_t i = 2; i < total; ++i) {
          vals.push_back(tagged_row_[i]);
        }
        out->Append(Row(std::move(vals)), left_block_[le_idx].pos);
      }
      continue;
    }

    if (right_pass_active_) {
      while (out->Size() < max_rows) {
        if (right_row_current_ && cur_cand_idx_ >= cand_hi_) {
          // Finished the candidate list for this right row: move on.
          ++cur_right_idx_;
          ++cur_right_global_idx_;
          right_row_current_ = false;
          cand_lo_ = cand_hi_ = cur_cand_idx_ = 0;
        }
        while (cur_right_idx_ >= cur_right_chunk_.Size()) {
          cur_right_chunk_.Reset();
          cur_right_idx_ = 0;
          ASSIGN_OR_RETURN(bool, got,
                           right_store_->NextChunk(&cur_right_chunk_));
          if (!got) {
            right_pass_active_ = false;
            if (pair_sorter_ != nullptr) {
              RETURN_IF_FAIL(pair_sorter_->Finish());
              emitting_pairs_ = true;
            } else {
              flushing_left_outer_ = true;
              left_outer_idx_ = 0;
            }
            break;
          }
        }
        if (!right_pass_active_) {
          break;
        }
        const Row rr = cur_right_chunk_.RowAt(cur_right_idx_);
        if (!right_row_current_) {
          right_row_current_ = true;
          // Both sides arrive ordered by key, so only a contiguous range of
          // `left_key_index_` can satisfy the band predicate. Binary-search
          // it instead of comparing this right row against every left entry.
          ASSIGN_OR_RETURN(std::optional<int64_t>, rk, ReadKey(rr, right_col_));
          if (!rk.has_value()) {
            continue;  // no candidates; the next iteration advances the row
          }
          const auto lo_it = std::lower_bound(
              left_key_index_.begin(), left_key_index_.end(), *rk,
              [](const auto& e, int64_t key) { return e.first < key; });
          const auto hi_it = std::upper_bound(
              left_key_index_.begin(), left_key_index_.end(), *rk,
              [](int64_t key, const auto& e) { return key < e.first; });
          const size_t lo = lo_it - left_key_index_.begin();
          const size_t hi = hi_it - left_key_index_.begin();
          switch (comparison_) {
            case BinaryOperation::kLessThan:
              cand_lo_ = 0;
              cand_hi_ = lo;  // lv < rv
              break;
            case BinaryOperation::kLessThanEquals:
              cand_lo_ = 0;
              cand_hi_ = hi;  // lv <= rv
              break;
            case BinaryOperation::kGreaterThan:
              cand_lo_ = hi;  // lv > rv
              cand_hi_ = left_key_index_.size();
              break;
            case BinaryOperation::kGreaterThanEquals:
              cand_lo_ = lo;  // lv >= rv
              cand_hi_ = left_key_index_.size();
              break;
            default:
              cand_lo_ = 0;
              cand_hi_ = 0;
              break;
          }
          cur_cand_idx_ = cand_lo_;
          if (cur_cand_idx_ >= cand_hi_) {
            continue;  // empty band; next iteration advances the row
          }
        }
        const size_t le_idx = left_key_index_[cur_cand_idx_++].second;
        LeftEntry& le = left_block_[le_idx];
        ASSIGN_OR_RETURN(bool, passes, PairPasses(le.row, rr));
        if (!passes) {
          continue;
        }
        le.matched = true;
        ++le.matches;
        if (kind_ == JoinKind::kSingle && le.matches > 1) {
          return StatusError(StatusCode::kInvalidArgument,
                             "BandJoin single join matched more than once");
        }
        if (right_outer) {
          SetBit(&right_matched_bits_, cur_right_global_idx_);
        }
        if (emits_pairs) {
          // The pair stream arrives right-major; the contract emits it
          // left-major (left input order, right-key order inside one left
          // row). Prepend the (left block index, right global index) tags --
          // the right store is key-ordered so the second tag reproduces it --
          // and let the bounded sorter restore the order at replay.
          RETURN_IF_FAIL(pair_sorter_->AddRow(
              Row({Value(static_cast<int64_t>(le_idx)),
                   Value(static_cast<int64_t>(cur_right_global_idx_))}) +
              le.row + rr));
        }
      }
      continue;
    }

    RETURN_IF_FAIL(PullNextLeftBlock(ctx));
    if (left_block_.empty()) {
      if (right_outer && !right_outer_done_ && !right_store_->Empty()) {
        RETURN_IF_FAIL(right_store_->ResetCursor());
        cur_right_chunk_.Reset();
        cur_right_idx_ = 0;
        right_outer_global_idx_ = 0;
        flushing_right_outer_ = true;
        continue;
      }
      break;
    }
    if (right_store_->Empty()) {
      flushing_left_outer_ = true;
      left_outer_idx_ = 0;
      continue;
    }
    RETURN_IF_FAIL(right_store_->ResetCursor());
    cur_right_chunk_.Reset();
    cur_right_idx_ = 0;
    cur_right_global_idx_ = 0;
    cur_left_idx_ = 0;
    cand_lo_ = cand_hi_ = cur_cand_idx_ = 0;
    right_row_current_ = false;
    if (emits_pairs) {
      ExternalSorter::Options sopts;
      sopts.spill_store = &ctx->spill_store;
      sopts.memory_budget_bytes =
          std::max<size_t>(kMinJoinMemBudget, budget_bytes_ / 4);
      sopts.grant = &ctx->grant;
      sopts.force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
      sopts.keys = {OocSortKey{.column_index = 0},
                    OocSortKey{.column_index = 1}};
      pair_sorter_ = std::make_unique<ExternalSorter>(std::move(sopts));
    }
    right_pass_active_ = true;
  }

  return !out->Empty();
}

void OocBandJoinSource::DumpExplain(std::ostream& out, int indent) const {
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  out << pad << "BandJoin (comparison=" << ToString(comparison_)
      << ", kind=" << static_cast<int>(kind_) << ")\n";
  DumpStreamChain(out, indent + 2, /*is_dump=*/true, left_.source,
                  left_.operators);
  DumpStreamChain(out, indent + 2, /*is_dump=*/true, right_.source,
                  right_.operators);
}

// --- 3. OocCrossJoinBuildSink & OocCrossJoinProbeSource ---

OocCrossJoinBuildSink::OocCrossJoinBuildSink(
    std::shared_ptr<OocCrossJoinContext> ctx)
    : cross_ctx_(std::move(ctx)) {}

Status OocCrossJoinBuildSink::Init(PipelineContext* pctx,
                                   size_t /*worker_count*/) {
  cross_ctx_->build_guard.emplace(&pctx->grant);
  cross_ctx_->budget_bytes =
      std::max<size_t>(kMinJoinMemBudget, pctx->OperatorBudgetBytes());
  SpillableStore::Options sopts;
  sopts.spill_store = &pctx->spill_store;
  sopts.memory_budget_bytes = cross_ctx_->budget_bytes / 2;
  sopts.grant = &pctx->grant;
  sopts.force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
  cross_ctx_->right_store = std::make_unique<SpillableStore>(sopts);
  return Status::kSuccess;
}

Status OocCrossJoinBuildSink::Consume(PipelineContext* /*pctx*/,
                                      size_t /*worker_id*/,
                                      const DataChunk& input) {
  if (input.Empty()) {
    return Status::kSuccess;
  }
  return cross_ctx_->right_store->AppendChunk(input);
}

StatusOr<std::shared_ptr<PipelineSource>> OocCrossJoinBuildSink::Finalize(
    PipelineContext* /*pctx*/) {
  RETURN_IF_FAIL(cross_ctx_->right_store->Seal());
  cross_ctx_->build_guard.reset();
  return std::make_shared<RowVectorSource>(std::vector<Row>{},
                                           "CrossJoinBuildDone");
}

std::string OocCrossJoinBuildSink::Describe() const {
  return cross_ctx_->prefer_bnl ? "BatchNestedLoopBuild" : "CrossJoinBuild";
}

void OocCrossJoinBuildSink::DumpExplain(std::ostream& out, int indent) const {
  DumpStreamChain(out, indent, /*is_dump=*/true,
                  cross_ctx_->right_source_explain,
                  cross_ctx_->right_ops_explain);
}

OocCrossJoinProbeSource::OocCrossJoinProbeSource(
    std::shared_ptr<OocCrossJoinContext> cross_ctx,
    OocSetOpInputStream left_stream)
    : cross_ctx_(std::move(cross_ctx)), left_(std::move(left_stream)) {}

Status OocCrossJoinProbeSource::Open(PipelineContext* ctx) {
  if (left_.source != nullptr) {
    RETURN_IF_FAIL(left_.source->Open(ctx));
  }
  for (const auto& op : left_.operators) {
    RETURN_IF_FAIL(op->Open(ctx));
  }
  if (cross_ctx_->kind == JoinKind::kRightOuter ||
      cross_ctx_->kind == JoinKind::kFullOuter) {
    right_matched_bits_.assign(
        static_cast<size_t>((cross_ctx_->right_store->RowCount() + 63) / 64),
        0);
  }
  right_outer_done_ = false;
  return Status::kSuccess;
}

StatusOr<bool> OocCrossJoinProbeSource::PairPasses(const Row& left_row,
                                                   const Row& right_row) const {
  if (!cross_ctx_->residual) {
    return true;
  }
  const Row combined = left_row + right_row;
  if (cross_ctx_->kind == JoinKind::kInner) {
    return relational_detail::EvaluateConjunctsTolerant(
        cross_ctx_->residual_conjuncts, [&](const Expression& conjunct) {
          return conjunct->TryEvaluate(combined, cross_ctx_->combined_schema);
        });
  }
  ASSIGN_OR_RETURN(
      Value, res,
      cross_ctx_->residual->TryEvaluate(combined, cross_ctx_->combined_schema));
  return !res.IsNull() && res.Truthy();
}

Status OocCrossJoinProbeSource::FillNextLeftBlock(PipelineContext* ctx) {
  left_block_.clear();
  const size_t max_bytes =
      std::max<size_t>(kMinJoinMemBudget, cross_ctx_->budget_bytes / 2);
  size_t cur_bytes = 0;

  while (cur_bytes < max_bytes) {
    if (in_left_idx_ >= in_left_chunk_.Size()) {
      in_left_chunk_.Reset();
      in_left_idx_ = 0;
      if (left_eof_) {
        break;
      }
      ASSIGN_OR_RETURN(
          bool, got,
          PullStreamChunk(ctx, left_.source.get(), left_.operators,
                          &in_left_chunk_, kDefaultVectorSize, &left_eof_));
      if (!got || in_left_chunk_.Empty()) {
        left_eof_ = true;
        break;
      }
    }
    Row r = in_left_chunk_.RowAt(in_left_idx_);
    const RowPosition pos = in_left_chunk_.PositionAt(in_left_idx_);
    ++in_left_idx_;
    cur_bytes += EstimateRowMemoryBytes(r) + sizeof(LeftEntry);
    left_block_.push_back(LeftEntry{.row = std::move(r), .pos = pos});
  }
  return Status::kSuccess;
}

StatusOr<bool> OocCrossJoinProbeSource::NextBatch(PipelineContext* ctx,
                                                  DataChunk* out,
                                                  size_t max_rows) {
  const JoinKind kind = cross_ctx_->kind;
  const bool left_outer =
      kind == JoinKind::kLeftOuter || kind == JoinKind::kFullOuter;
  const bool right_outer =
      kind == JoinKind::kRightOuter || kind == JoinKind::kFullOuter;
  const bool semi = kind == JoinKind::kSemi;
  const bool anti = kind == JoinKind::kAnti;

  out->Reset(cross_ctx_->output_schema, max_rows);

  while (out->Size() < max_rows) {
    if (flushing_right_unmatched_) {
      while (out->Size() < max_rows) {
        if (cur_right_row_idx_ >= cur_right_chunk_.Size()) {
          cur_right_chunk_.Reset();
          cur_right_row_idx_ = 0;
          ASSIGN_OR_RETURN(
              bool, got, cross_ctx_->right_store->NextChunk(&cur_right_chunk_));
          if (!got) {
            flushing_right_unmatched_ = false;
            right_outer_done_ = true;
            break;
          }
        }
        const Row rr = cur_right_chunk_.RowAt(cur_right_row_idx_);
        const RowPosition rpos =
            cur_right_chunk_.PositionAt(cur_right_row_idx_);
        ++cur_right_row_idx_;
        if (!GetBit(right_matched_bits_, right_flush_global_idx_++)) {
          out->Append(NullRow(cross_ctx_->left_width) + rr, rpos);
        }
      }
      if (!flushing_right_unmatched_) {
        break;
      }
      continue;
    }

    if (flushing_left_unmatched_) {
      while (out->Size() < max_rows && left_flush_idx_ < left_block_.size()) {
        const LeftEntry& le = left_block_[left_flush_idx_++];
        if (le.matched) {
          if (semi) {
            out->Append(le.row, le.pos);
          }
        } else {
          if (anti) {
            out->Append(le.row, le.pos);
          } else if (left_outer) {
            out->Append(le.row + NullRow(cross_ctx_->right_width), le.pos);
          }
        }
      }
      if (left_flush_idx_ >= left_block_.size()) {
        flushing_left_unmatched_ = false;
        left_block_.clear();
      }
      continue;
    }

    if (scanning_right_) {
      // Left-major order (the legacy contract): emit every right partner of
      // one left row before advancing to the next left row. A single shared
      // right pass would instead be right-major and change the output of
      // LIMIT / window / streaming consumers.
      while (out->Size() < max_rows) {
        if (left_scan_idx_ >= left_block_.size()) {
          scanning_right_ = false;
          if (left_outer || semi || anti) {
            flushing_left_unmatched_ = true;
            left_flush_idx_ = 0;
          } else {
            left_block_.clear();
          }
          break;
        }
        if (!right_pass_at_start_) {
          RETURN_IF_FAIL(cross_ctx_->right_store->ResetCursor());
          cur_right_chunk_.Reset();
          cur_right_row_idx_ = 0;
          cur_right_global_idx_ = 0;
          right_pass_at_start_ = true;
        }
        if (cur_right_row_idx_ >= cur_right_chunk_.Size()) {
          cur_right_chunk_.Reset();
          cur_right_row_idx_ = 0;
          ASSIGN_OR_RETURN(
              bool, got, cross_ctx_->right_store->NextChunk(&cur_right_chunk_));
          if (!got) {
            // This left row has seen every right row; restart for the next.
            ++left_scan_idx_;
            right_pass_at_start_ = false;
            continue;
          }
        }
        const uint64_t right_gidx = cur_right_global_idx_++;
        const Row rr = cur_right_chunk_.RowAt(cur_right_row_idx_++);
        LeftEntry& le = left_block_[left_scan_idx_];
        if ((semi || anti) && le.matched) {
          ++left_scan_idx_;
          right_pass_at_start_ = false;
          continue;
        }
        ASSIGN_OR_RETURN(bool, ok, PairPasses(le.row, rr));
        if (!ok) {
          continue;
        }
        le.matched = true;
        if (right_outer) {
          SetBit(&right_matched_bits_, right_gidx);
        }
        if (!semi && !anti) {
          out->Append(le.row + rr, le.pos);
        }
      }
      continue;
    }

    RETURN_IF_FAIL(FillNextLeftBlock(ctx));
    if (left_block_.empty()) {
      if (right_outer && !right_outer_done_ &&
          !cross_ctx_->right_store->Empty()) {
        RETURN_IF_FAIL(cross_ctx_->right_store->ResetCursor());
        cur_right_chunk_.Reset();
        cur_right_row_idx_ = 0;
        right_flush_global_idx_ = 0;
        flushing_right_unmatched_ = true;
        continue;
      }
      break;
    }
    if (cross_ctx_->right_store->Empty()) {
      if (left_outer || anti) {
        flushing_left_unmatched_ = true;
        left_flush_idx_ = 0;
      } else {
        left_block_.clear();
        break;
      }
      continue;
    }
    RETURN_IF_FAIL(cross_ctx_->right_store->ResetCursor());
    cur_right_chunk_.Reset();
    cur_right_row_idx_ = 0;
    cur_right_global_idx_ = 0;
    left_scan_idx_ = 0;
    right_pass_at_start_ = false;
    scanning_right_ = true;
  }

  return !out->Empty();
}

std::string OocCrossJoinProbeSource::Describe() const {
  if (!cross_ctx_->residual && cross_ctx_->kind == JoinKind::kInner &&
      !cross_ctx_->prefer_bnl) {
    return "CrossJoin";
  }
  return "BatchNestedLoopJoin";
}

void OocCrossJoinProbeSource::DumpExplain(std::ostream& out, int indent) const {
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  if (!cross_ctx_->residual && cross_ctx_->kind == JoinKind::kInner &&
      !cross_ctx_->prefer_bnl) {
    out << pad << "CrossJoin:\n";
  } else {
    out << pad
        << "BatchNestedLoopJoin(block_size=" << cross_ctx_->block_size_hint
        << ", kind=" << static_cast<int>(cross_ctx_->kind) << ")\n";
  }
  DumpStreamChain(out, indent + 2, /*is_dump=*/true, left_.source,
                  left_.operators);
  DumpStreamChain(out, indent + 2, /*is_dump=*/true,
                  cross_ctx_->right_source_explain,
                  cross_ctx_->right_ops_explain);
}

// --- 4. OocRecursiveCteSource ---

OocRecursiveCteSource::OocRecursiveCteSource(
    std::string cte_name, std::shared_ptr<const SelectStatement> body,
    std::optional<RecursiveDepthSpec> depth_spec, Schema output_schema)
    : cte_name_(std::move(cte_name)),
      body_(std::move(body)),
      depth_spec_(std::move(depth_spec)),
      output_schema_(std::move(output_schema)) {}

Status OocRecursiveCteSource::Open(PipelineContext* ctx) {
  if (initialized_) {
    return result_store_->ResetCursor();
  }
  initialized_ = true;
  const size_t budget =
      std::max<size_t>(kMinJoinMemBudget, ctx->OperatorBudgetBytes());
  SpillableStore::Options res_opts;
  res_opts.spill_store = &ctx->spill_store;
  res_opts.memory_budget_bytes = budget / 2;
  res_opts.grant = &ctx->grant;
  res_opts.force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
  result_store_ = std::make_unique<SpillableStore>(res_opts);

  if (!body_) {
    return result_store_->Seal();
  }

  constexpr size_t kMaxIterations = 1024;
  constexpr size_t kMaxRows = 10'000'000;

  if (body_->UnionAll().empty()) {
    ASSIGN_OR_RETURN(
        relational_detail::Relation, executed,
        relational_detail::ExecuteQuery(*ctx->txn, *body_, nullptr, {}));
    Status store_st = Status::kSuccess;
    RETURN_IF_FAIL(executed.ForEachRow([&](const Row& r) {
      if (store_st == Status::kSuccess) {
        store_st = result_store_->AppendRow(r);
      }
    }));
    RETURN_IF_FAIL(store_st);
    RETURN_IF_FAIL(result_store_->Seal());
    return result_store_->ResetCursor();
  }

  std::vector<SetOperationMatch> term_matches;
  term_matches.reserve(body_->UnionAll().size());
  for (size_t i = 0; i < body_->UnionAll().size(); ++i) {
    term_matches.push_back(body_->MatchFor(i));
  }
  const bool any_name_matching = std::ranges::any_of(
      term_matches,
      [](const SetOperationMatch& m) { return m.by_name || m.corresponding; });
  if (any_name_matching) {
    // BY NAME / CORRESPONDING alignment: stream fixed-point result directly
    // into SpillableStore so output buffering still obeys O(B_op).
    const RecursiveDepthSpec* dspec =
        depth_spec_.has_value() ? &depth_spec_.value() : nullptr;
    ASSIGN_OR_RETURN(relational_detail::Relation, executed,
                     relational_detail::ExecuteRecursiveCte(
                         *ctx->txn, cte_name_, *body_, nullptr, {}, dspec));
    Status store_st = Status::kSuccess;
    RETURN_IF_FAIL(executed.ForEachRow([&](const Row& r) {
      if (store_st == Status::kSuccess) {
        store_st = result_store_->AppendRow(r);
      }
    }));
    RETURN_IF_FAIL(store_st);
    RETURN_IF_FAIL(result_store_->Seal());
    return result_store_->ResetCursor();
  }

  // Standard positional WITH RECURSIVE: keep OOC delta & sorted OOC reached
  // set!
  SelectStatement anchor = *body_;
  anchor.ClearUnionAll();
  anchor.SetOrderBy({});
  anchor.SetLimit(std::nullopt);
  anchor.SetOffset(0);
  ASSIGN_OR_RETURN(
      relational_detail::Relation, anchor_res,
      relational_detail::ExecuteQuery(*ctx->txn, anchor, nullptr, {}));
  anchor_res.FinishSpill();

  const bool track_depth = depth_spec_.has_value();
  const bool distinct = body_->UnionDistinct();
  Schema work_schema = anchor_res.schema;
  if (track_depth) {
    work_schema = work_schema +
                  Schema("", {Column(depth_spec_->column, ValueType::kInt64)});
  }
  const size_t working_cols = anchor_res.schema.ColumnCount();

  // Deduplication state: `WorkTableIndex` up to `seen_budget`, spilling into
  // sorted SpillSegmentId (`reached_seg`) + ExternalSorter anti-join merge.
  const size_t seen_budget = std::max<size_t>(kMinJoinMemBudget, budget / 2);
  bool spilled_seen = false;
  relational_detail::WorkTableIndex work_index(/*count_stats=*/true);
  size_t mem_seen_bytes = 0;
  SpillSegmentId reached_seg = kInvalidSpillSegmentId;

  auto spill_reached_from_mem = [&]() -> Status {
    if (spilled_seen) {
      return Status::kSuccess;
    }
    spilled_seen = true;
    if (work_index.size() == 0) {
      work_index = relational_detail::WorkTableIndex(/*count_stats=*/true);
      mem_seen_bytes = 0;
      return Status::kSuccess;
    }
    std::vector<OocSortKey> keys;
    keys.reserve(working_cols);
    for (size_t c = 0; c < working_cols; ++c) {
      keys.push_back(OocSortKey{.column_index = static_cast<slot_t>(c)});
    }
    ExternalSorter::Options sopts;
    sopts.spill_store = &ctx->spill_store;
    sopts.memory_budget_bytes = seen_budget;
    sopts.grant = &ctx->grant;
    sopts.keys = std::move(keys);
    sopts.force_spill = true;
    ExternalSorter sorter(std::move(sopts));
    std::optional<Status> add_error;
    work_index.ForEachRow([&](const Row& r) {
      if (!add_error.has_value()) {
        Status st = sorter.AddRow(r);
        if (st != Status::kSuccess) {
          add_error = std::move(st);
        }
      }
    });
    if (add_error.has_value()) {
      return *add_error;
    }
    work_index = relational_detail::WorkTableIndex(/*count_stats=*/true);
    mem_seen_bytes = 0;
    RETURN_IF_FAIL(sorter.Finish());

    SpillSegmentWriter w(&ctx->spill_store, SpillStore::kBlockSize);
    std::vector<Row> blk;
    std::string scratch;
    Row row;
    while (true) {
      ASSIGN_OR_RETURN(bool, got, sorter.NextRow(&row));
      if (!got) {
        break;
      }
      blk.push_back(std::move(row));
      if (blk.size() >= 128) {
        RETURN_IF_FAIL(WriteRowBlock(&w, blk, &scratch));
        blk.clear();
      }
    }
    if (!blk.empty()) {
      RETURN_IF_FAIL(WriteRowBlock(&w, blk, &scratch));
    }
    ASSIGN_OR_RETURN(SpillSegmentId, id, w.Finish(true));
    reached_seg = id;
    return Status::kSuccess;
  };

  auto accept_mem_seen = [&](const Row& payload) -> StatusOr<bool> {
    if (!distinct) {
      return true;
    }
    if (!spilled_seen && ProcessMemoryBudget::Global().AlwaysSpillEnabled()) {
      RETURN_IF_FAIL(spill_reached_from_mem());
    }
    if (!spilled_seen) {
      if (!work_index.Insert(payload)) {
        return false;
      }
      const size_t add = EstimateRowMemoryBytes(payload) + 32;
      if (mem_seen_bytes > 0 && mem_seen_bytes + add > seen_budget) {
        RETURN_IF_FAIL(spill_reached_from_mem());
      } else {
        // The row already lives in `work_index`; only account for its
        // approximate footprint here (no parallel copy).
        mem_seen_bytes += add;
        return true;
      }
    }
    return true;  // Deferred to ExternalSorter merge anti-join when spilled.
  };

  auto merge_spilled_candidates = [&](ExternalSorter* cand_sorter,
                                      int64_t row_depth,
                                      relational_detail::Relation* next_delta,
                                      uint64_t* next_count) -> Status {
    RETURN_IF_FAIL(cand_sorter->Finish());
    SpillSegmentWriter merged_w(&ctx->spill_store, SpillStore::kBlockSize);
    std::vector<Row> wblk;
    std::string wscratch;
    auto push_reached = [&](Row&& r) -> Status {
      wblk.push_back(std::move(r));
      if (wblk.size() >= 128) {
        RETURN_IF_FAIL(WriteRowBlock(&merged_w, wblk, &wscratch));
        wblk.clear();
      }
      return Status::kSuccess;
    };

    std::unique_ptr<SpillSegmentReader> rdr;
    std::vector<Row> rblk;
    size_t ridx = 0;
    std::string rscratch;
    bool r_valid = false;
    Row r_cur;
    auto adv_reached = [&]() -> Status {
      if (rdr == nullptr) {
        r_valid = false;
        return Status::kSuccess;
      }
      while (ridx >= rblk.size()) {
        rblk.clear();
        ridx = 0;
        ASSIGN_OR_RETURN(bool, got, ReadRowBlock(rdr.get(), &rblk, &rscratch));
        if (!got) {
          r_valid = false;
          return Status::kSuccess;
        }
      }
      r_cur = std::move(rblk[ridx++]);
      r_valid = true;
      return Status::kSuccess;
    };
    if (reached_seg != kInvalidSpillSegmentId) {
      rdr = std::make_unique<SpillSegmentReader>(&ctx->spill_store, reached_seg,
                                                 SpillStore::kBlockSize);
      RETURN_IF_FAIL(rdr->Open());
      RETURN_IF_FAIL(adv_reached());
    }

    auto cmp_rows = [&](const Row& a, const Row& b) {
      for (size_t c = 0; c < working_cols; ++c) {
        const SortKeyOrder ord(true, true, Collation::kNone);
        const int cm = ord.Compare(a[c], b[c]);
        if (cm != 0) {
          return cm;
        }
      }
      return 0;
    };

    Row cand;
    bool c_valid = false;
    Row prev_cand;
    bool have_prev_cand = false;
    auto adv_cand = [&]() -> Status {
      while (true) {
        ASSIGN_OR_RETURN(bool, got, cand_sorter->NextRow(&cand));
        if (!got) {
          c_valid = false;
          return Status::kSuccess;
        }
        if (have_prev_cand && cmp_rows(prev_cand, cand) == 0) {
          continue;
        }
        prev_cand = cand;
        have_prev_cand = true;
        c_valid = true;
        return Status::kSuccess;
      }
    };
    RETURN_IF_FAIL(adv_cand());

    while (c_valid && r_valid) {
      const int cm = cmp_rows(cand, r_cur);
      if (cm == 0) {
        RETURN_IF_FAIL(push_reached(std::move(r_cur)));
        r_cur = Row();
        RETURN_IF_FAIL(adv_reached());
        RETURN_IF_FAIL(adv_cand());
      } else if (cm < 0) {
        Row stored = cand;
        if (track_depth) {
          stored.values_.emplace_back(row_depth);
        }
        next_delta->AddRow(stored);
        if (!track_depth || (row_depth >= depth_spec_->lower &&
                             row_depth <= depth_spec_->upper)) {
          RETURN_IF_FAIL(result_store_->AppendRow(stored));
        }
        ++(*next_count);
        RETURN_IF_FAIL(push_reached(std::move(cand)));
        cand = Row();
        RETURN_IF_FAIL(adv_cand());
      } else {
        RETURN_IF_FAIL(push_reached(std::move(r_cur)));
        r_cur = Row();
        RETURN_IF_FAIL(adv_reached());
      }
    }
    while (c_valid) {
      Row stored = cand;
      if (track_depth) {
        stored.values_.emplace_back(row_depth);
      }
      next_delta->AddRow(stored);
      if (!track_depth || (row_depth >= depth_spec_->lower &&
                           row_depth <= depth_spec_->upper)) {
        RETURN_IF_FAIL(result_store_->AppendRow(stored));
      }
      ++(*next_count);
      RETURN_IF_FAIL(push_reached(std::move(cand)));
      cand = Row();
      RETURN_IF_FAIL(adv_cand());
    }
    while (r_valid) {
      RETURN_IF_FAIL(push_reached(std::move(r_cur)));
      r_cur = Row();
      RETURN_IF_FAIL(adv_reached());
    }
    if (!wblk.empty()) {
      RETURN_IF_FAIL(WriteRowBlock(&merged_w, wblk, &wscratch));
    }
    ASSIGN_OR_RETURN(SpillSegmentId, new_reached, merged_w.Finish(true));
    if (reached_seg != kInvalidSpillSegmentId) {
      ctx->spill_store.DropSegment(reached_seg);
    }
    reached_seg = new_reached;
    return Status::kSuccess;
  };

  auto cleanup_reached = [&]() {
    if (reached_seg != kInvalidSpillSegmentId) {
      ctx->spill_store.DropSegment(reached_seg);
      reached_seg = kInvalidSpillSegmentId;
    }
  };

  auto delta = std::make_shared<relational_detail::Relation>(
      ctx->txn->execution_runtime());
  delta->schema = work_schema;
  uint64_t total_emitted = 0;

  std::unique_ptr<ExternalSorter> anchor_sorter;
  auto ensure_anchor_sorter = [&]() {
    if (anchor_sorter != nullptr) {
      return;
    }
    std::vector<OocSortKey> keys;
    keys.reserve(working_cols);
    for (size_t c = 0; c < working_cols; ++c) {
      keys.push_back(OocSortKey{.column_index = static_cast<slot_t>(c)});
    }
    ExternalSorter::Options sopts;
    sopts.spill_store = &ctx->spill_store;
    sopts.memory_budget_bytes = seen_budget;
    sopts.grant = &ctx->grant;
    sopts.keys = std::move(keys);
    sopts.force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
    anchor_sorter = std::make_unique<ExternalSorter>(std::move(sopts));
  };

  Status anchor_st = Status::kSuccess;
  Status anchor_iter_st = anchor_res.ForEachRow([&](const Row& r) {
    if (anchor_st != Status::kSuccess) {
      return;
    }
    anchor_st = [&]() -> Status {
      if (distinct) {
        if (!spilled_seen) {
          ASSIGN_OR_RETURN(bool, accepted, accept_mem_seen(r));
          if (!accepted) {
            return Status::kSuccess;
          }
          if (spilled_seen) {
            ensure_anchor_sorter();
            return anchor_sorter->AddRow(r);
          }
        } else {
          ensure_anchor_sorter();
          return anchor_sorter->AddRow(r);
        }
      }
      Row stored = r;
      if (track_depth) {
        stored.values_.emplace_back(int64_t{0});
      }
      delta->AddRow(stored);
      if (!track_depth ||
          (depth_spec_->lower <= 0 && 0 <= depth_spec_->upper)) {
        RETURN_IF_FAIL(result_store_->AppendRow(stored));
      }
      ++total_emitted;
      return Status::kSuccess;
    }();
  });
  if (anchor_st == Status::kSuccess) {
    anchor_st = anchor_iter_st;
  }
  if (anchor_st == Status::kSuccess && spilled_seen &&
      anchor_sorter != nullptr) {
    uint64_t anchor_merged = 0;
    anchor_st = merge_spilled_candidates(anchor_sorter.get(), 0, delta.get(),
                                         &anchor_merged);
    total_emitted += anchor_merged;
  }
  if (anchor_st != Status::kSuccess) {
    cleanup_reached();
    return anchor_st;
  }
  RETURN_IF_FAIL(delta->FinishSpill());

  for (size_t iteration = 0; iteration < kMaxIterations; ++iteration) {
    if (delta->TotalRows() == 0) {
      break;
    }
    const int64_t row_depth = static_cast<int64_t>(iteration) + 1;
    if (track_depth && row_depth > depth_spec_->upper) {
      break;
    }

    relational_detail::CteMap loop_ctes;
    loop_ctes[cte_name_] = delta;

    auto next_delta = std::make_shared<relational_detail::Relation>(
        ctx->txn->execution_runtime());
    next_delta->schema = work_schema;
    std::unique_ptr<ExternalSorter> cand_sorter;
    auto ensure_cand_sorter = [&]() {
      if (cand_sorter != nullptr) {
        return;
      }
      std::vector<OocSortKey> keys;
      keys.reserve(working_cols);
      for (size_t c = 0; c < working_cols; ++c) {
        keys.push_back(OocSortKey{.column_index = static_cast<slot_t>(c)});
      }
      ExternalSorter::Options sopts;
      sopts.spill_store = &ctx->spill_store;
      sopts.memory_budget_bytes = seen_budget;
      sopts.grant = &ctx->grant;
      sopts.keys = std::move(keys);
      sopts.force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
      cand_sorter = std::make_unique<ExternalSorter>(std::move(sopts));
    };

    uint64_t added_this_round = 0;
    for (const auto& branch_stmt : body_->UnionAll()) {
      if (!branch_stmt) {
        continue;
      }
      StatusOr<relational_detail::Relation> branch_or =
          relational_detail::ExecuteQuery(*ctx->txn, *branch_stmt, nullptr,
                                          loop_ctes);
      if (!branch_or.HasValue()) {
        cleanup_reached();
        return branch_or.GetStatus();
      }
      relational_detail::Relation branch = branch_or.MoveValue();
      Status br_st = Status::kSuccess;
      Status br_iter_st = branch.ForEachRow([&](const Row& row) {
        if (br_st != Status::kSuccess) {
          return;
        }
        br_st = [&]() -> Status {
          if (row.Count() != working_cols) {
            return StatusError(StatusCode::kInvalidArgument,
                               "recursive CTE " + cte_name_ +
                                   " term produces " +
                                   std::to_string(row.Count()) +
                                   " columns but the working table reads " +
                                   std::to_string(working_cols));
          }
          if (distinct) {
            if (!spilled_seen) {
              ASSIGN_OR_RETURN(bool, accepted, accept_mem_seen(row));
              if (!accepted) {
                return Status::kSuccess;
              }
              if (spilled_seen) {
                ensure_cand_sorter();
                return cand_sorter->AddRow(row);
              }
            } else {
              ensure_cand_sorter();
              return cand_sorter->AddRow(row);
            }
          }
          Row stored = row;
          if (track_depth) {
            stored.values_.emplace_back(row_depth);
          }
          next_delta->AddRow(stored);
          if (!track_depth || (row_depth >= depth_spec_->lower &&
                               row_depth <= depth_spec_->upper)) {
            RETURN_IF_FAIL(result_store_->AppendRow(stored));
          }
          ++added_this_round;
          return Status::kSuccess;
        }();
      });
      if (br_st == Status::kSuccess) {
        br_st = br_iter_st;
      }
      if (br_st != Status::kSuccess) {
        cleanup_reached();
        return br_st;
      }
    }

    if (spilled_seen && cand_sorter != nullptr) {
      Status mst = merge_spilled_candidates(
          cand_sorter.get(), row_depth, next_delta.get(), &added_this_round);
      if (mst != Status::kSuccess) {
        cleanup_reached();
        return mst;
      }
    }
    RETURN_IF_FAIL(next_delta->FinishSpill());
    total_emitted += added_this_round;
    if (total_emitted > kMaxRows) {
      cleanup_reached();
      return StatusError(
          StatusCode::kInvalidArgument,
          "recursive CTE " + cte_name_ + " exceeded the row budget");
    }
    if (added_this_round == 0 && next_delta->TotalRows() == 0) {
      break;
    }
    delta = std::move(next_delta);
  }

  cleanup_reached();
  RETURN_IF_FAIL(result_store_->Seal());
  return result_store_->ResetCursor();
}

StatusOr<bool> OocRecursiveCteSource::NextBatch(PipelineContext* /*ctx*/,
                                                DataChunk* out,
                                                size_t max_rows) {
  (void)max_rows;
  if (!result_store_) {
    return false;
  }
  ASSIGN_OR_RETURN(bool, got, result_store_->NextChunk(out));
  if (!got) {
    result_store_->Reset();
    return false;
  }
  return true;
}

void OocRecursiveCteSource::DumpExplain(std::ostream& out, int indent) const {
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  out << pad << "RecursiveCteExecutor [" << cte_name_ << "]\n"
      << pad << "RecursiveUnion\n"
      << pad << "WorkTableScan [" << cte_name_ << "]\n";
}

// --- 5. OocApplySource ---

OocApplySource::OocApplySource(
    OocSetOpInputStream outer,
    std::optional<OocSetOpInputStream> inner_plan_stream, Schema outer_schema,
    std::shared_ptr<const SelectStatement> inner_query, std::string inner_alias,
    Expression predicate, JoinKind kind, Schema output_schema)
    : outer_(std::move(outer)),
      inner_plan_stream_(std::move(inner_plan_stream)),
      outer_schema_(std::move(outer_schema)),
      inner_query_(std::move(inner_query)),
      inner_alias_(std::move(inner_alias)),
      predicate_(std::move(predicate)),
      kind_(kind),
      output_schema_(std::move(output_schema)) {}

Status OocApplySource::Open(PipelineContext* ctx) {
  txn_ = ctx->txn;
  budget_bytes_ =
      std::max<size_t>(kMinJoinMemBudget, ctx->OperatorBudgetBytes());
  cache_budget_bytes_ = std::max<size_t>(kMinJoinMemBudget, budget_bytes_ / 2);
  if (outer_.source != nullptr) {
    RETURN_IF_FAIL(outer_.source->Open(ctx));
  }
  for (const auto& op : outer_.operators) {
    RETURN_IF_FAIL(op->Open(ctx));
  }

  if (inner_query_ == nullptr && inner_plan_stream_.has_value() &&
      !static_inner_ready_) {
    SpillableStore::Options sopts;
    sopts.spill_store = &ctx->spill_store;
    sopts.memory_budget_bytes = cache_budget_bytes_;
    sopts.grant = &ctx->grant;
    sopts.force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
    static_inner_store_ = std::make_unique<SpillableStore>(sopts);

    if (inner_plan_stream_->source != nullptr) {
      RETURN_IF_FAIL(inner_plan_stream_->source->Open(ctx));
    }
    for (const auto& op : inner_plan_stream_->operators) {
      RETURN_IF_FAIL(op->Open(ctx));
    }
    bool ieof = false;
    while (!ieof) {
      DataChunk ichunk;
      ASSIGN_OR_RETURN(bool, got,
                       PullStreamChunk(ctx, inner_plan_stream_->source.get(),
                                       inner_plan_stream_->operators, &ichunk,
                                       kDefaultVectorSize, &ieof));
      if (!got || ichunk.Empty()) {
        break;
      }
      RETURN_IF_FAIL(static_inner_store_->AppendChunk(ichunk));
    }
    RETURN_IF_FAIL(static_inner_store_->Seal());
    inner_schema_ = inner_alias_.empty()
                        ? inner_plan_stream_->schema
                        : relational_detail::QualifySchema(
                              inner_plan_stream_->schema, inner_alias_);
    combined_schema_ = outer_schema_ + inner_schema_;
    static_inner_ready_ = true;
  }
  return Status::kSuccess;
}

StatusOr<bool> OocApplySource::NextBatch(PipelineContext* ctx, DataChunk* out,
                                         size_t max_rows) {
  out->Reset(output_schema_, max_rows);

  auto eval_pred = [&](const Row& combined) -> StatusOr<bool> {
    if (!predicate_) {
      return true;
    }
    ASSIGN_OR_RETURN(Value, v,
                     predicate_->TryEvaluate(combined, combined_schema_));
    return !v.IsNull() && v.Truthy();
  };

  while (out->Size() < max_rows) {
    if (!has_outer_row_) {
      if (outer_idx_ >= outer_chunk_.Size()) {
        outer_chunk_.Reset();
        outer_idx_ = 0;
        if (outer_eof_) {
          break;
        }
        ASSIGN_OR_RETURN(
            bool, got,
            PullStreamChunk(ctx, outer_.source.get(), outer_.operators,
                            &outer_chunk_, kDefaultVectorSize, &outer_eof_));
        if (!got || outer_chunk_.Empty()) {
          outer_eof_ = true;
          break;
        }
      }
      cur_outer_row_ = outer_chunk_.RowAt(outer_idx_);
      cur_outer_pos_ = outer_chunk_.PositionAt(outer_idx_);
      ++outer_idx_;
      has_outer_row_ = true;
      matched_any_ = false;
      active_inner_rows_.clear();
      active_inner_view_.reset();
      active_inner_idx_ = 0;
      active_inner_spill_.reset();
      reading_inner_spill_ = false;
      active_inner_chunk_.Reset();
      active_inner_chunk_idx_ = 0;

      if (inner_query_ != nullptr) {
        ++cache_probes_;
        bool cache_hit = false;
        const size_t probe_hash =
            relational_detail::GroupKeyTransparentHash{}(cur_outer_row_);
        for (size_t i = lru_cache_.size(); i > 0; --i) {
          if (lru_cache_[i - 1].key_hash != probe_hash) {
            continue;
          }
          if (EqualRowsExact(lru_cache_[i - 1].key, cur_outer_row_)) {
            ++cache_hits_;
            active_inner_view_ = lru_cache_[i - 1].rows;
            if (i != lru_cache_.size()) {
              LruEntry moved = std::move(lru_cache_[i - 1]);
              lru_cache_.erase(lru_cache_.begin() +
                               static_cast<ptrdiff_t>(i - 1));
              lru_cache_.push_back(std::move(moved));
            }
            cache_hit = true;
            break;
          }
        }
        if (!cache_hit) {
          relational_detail::Scope scope{.row = &cur_outer_row_,
                                         .schema = &outer_schema_,
                                         .outer = nullptr};
          ASSIGN_OR_RETURN(relational_detail::Relation, rel,
                           relational_detail::ExecuteQuery(*txn_, *inner_query_,
                                                           &scope, {}));
          if (inner_schema_.ColumnCount() == 0) {
            inner_schema_ = inner_alias_.empty()
                                ? rel.schema
                                : relational_detail::QualifySchema(
                                      rel.schema, inner_alias_);
            combined_schema_ = outer_schema_ + inner_schema_;
          }

          size_t res_bytes = 0;
          bool overflowed = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
          if (overflowed) {
            SpillableStore::Options sopts;
            sopts.spill_store = &ctx->spill_store;
            sopts.memory_budget_bytes = cache_budget_bytes_;
            sopts.grant = &ctx->grant;
            sopts.force_spill = true;
            active_inner_spill_ = std::make_unique<SpillableStore>(sopts);
          }
          Status inner_st = Status::kSuccess;
          Status inner_iter_st = rel.ForEachRow([&](const Row& r) {
            if (inner_st != Status::kSuccess) {
              return;
            }
            inner_st = [&]() -> Status {
              const size_t rb = EstimateRowMemoryBytes(r);
              if (active_inner_spill_ != nullptr) {
                return active_inner_spill_->AppendRow(r);
              }
              if (res_bytes + rb > cache_budget_bytes_) {
                // A single row larger than the cache budget must spill too;
                // keeping it resident silently drops it from the budget.
                SpillableStore::Options sopts;
                sopts.spill_store = &ctx->spill_store;
                sopts.memory_budget_bytes = cache_budget_bytes_;
                sopts.grant = &ctx->grant;
                active_inner_spill_ = std::make_unique<SpillableStore>(sopts);
                for (Row& prev : active_inner_rows_) {
                  RETURN_IF_FAIL(active_inner_spill_->AppendRow(prev));
                }
                active_inner_rows_.clear();
                active_inner_rows_.shrink_to_fit();
                return active_inner_spill_->AppendRow(r);
              }
              res_bytes += rb;
              active_inner_rows_.push_back(r);
              return Status::kSuccess;
            }();
          });
          RETURN_IF_FAIL(inner_st);
          RETURN_IF_FAIL(inner_iter_st);

          if (active_inner_spill_ != nullptr) {
            RETURN_IF_FAIL(active_inner_spill_->Seal());
            RETURN_IF_FAIL(active_inner_spill_->ResetCursor());
            reading_inner_spill_ = true;
          } else if (res_bytes <= cache_budget_bytes_) {
            const size_t entry_bytes =
                EstimateRowMemoryBytes(cur_outer_row_) + res_bytes + 64;
            while (!lru_cache_.empty() &&
                   lru_bytes_ + entry_bytes > cache_budget_bytes_) {
              lru_bytes_ -= lru_cache_.front().bytes;
              lru_cache_.erase(lru_cache_.begin());
            }
            // Move the freshly built rows into one immutable shared view:
            // both the LRU entry and the active reader own this same buffer.
            auto shared_rows = std::make_shared<const std::vector<Row>>(
                std::move(active_inner_rows_));
            active_inner_view_ = shared_rows;
            if (entry_bytes <= cache_budget_bytes_) {
              lru_bytes_ += entry_bytes;
              lru_cache_.push_back(LruEntry{
                  .key = cur_outer_row_,
                  .rows = std::move(shared_rows),
                  .bytes = entry_bytes,
                  .key_hash = probe_hash});
            }
          }
        }
      } else if (static_inner_ready_) {
        RETURN_IF_FAIL(static_inner_store_->ResetCursor());
        reading_inner_spill_ = true;
      }
    }

    auto consume_inner_row = [&](const Row& ir) -> StatusOr<bool> {
      Row combined = cur_outer_row_ + ir;
      ASSIGN_OR_RETURN(bool, match, eval_pred(combined));
      if (!match) {
        return false;
      }
      matched_any_ = true;
      if (kind_ == JoinKind::kSemi) {
        out->Append(cur_outer_row_, cur_outer_pos_);
        has_outer_row_ = false;
        return true;
      }
      if (kind_ == JoinKind::kAnti) {
        has_outer_row_ = false;
        return true;
      }
      out->Append(std::move(combined), cur_outer_pos_);
      return false;
    };

    if (reading_inner_spill_) {
      SpillableStore* store = active_inner_spill_ != nullptr
                                  ? active_inner_spill_.get()
                                  : static_inner_store_.get();
      bool stop_outer = false;
      while (out->Size() < max_rows) {
        if (active_inner_chunk_idx_ >= active_inner_chunk_.Size()) {
          active_inner_chunk_.Reset();
          active_inner_chunk_idx_ = 0;
          ASSIGN_OR_RETURN(bool, got, store->NextChunk(&active_inner_chunk_));
          if (!got) {
            reading_inner_spill_ = false;
            break;
          }
        }
        const Row ir = active_inner_chunk_.RowAt(active_inner_chunk_idx_++);
        ASSIGN_OR_RETURN(bool, handled, consume_inner_row(ir));
        if (handled) {
          stop_outer = true;
          break;
        }
      }
      if (stop_outer) {
        continue;
      }
    } else {
      bool stop_outer = false;
      while (out->Size() < max_rows && active_inner_view_ != nullptr &&
             active_inner_idx_ < active_inner_view_->size()) {
        const Row& ir = (*active_inner_view_)[active_inner_idx_++];
        ASSIGN_OR_RETURN(bool, handled, consume_inner_row(ir));
        if (handled) {
          stop_outer = true;
          break;
        }
      }
      if (stop_outer) {
        continue;
      }
    }

    const bool finished_inner =
        !reading_inner_spill_ &&
        (active_inner_view_ == nullptr ||
         active_inner_idx_ >= active_inner_view_->size());
    if (has_outer_row_ && finished_inner) {
      has_outer_row_ = false;
      if (!matched_any_) {
        if (kind_ == JoinKind::kLeftOuter) {
          size_t ic = inner_schema_.ColumnCount();
          if (ic == 0 &&
              output_schema_.ColumnCount() > cur_outer_row_.Count()) {
            ic = output_schema_.ColumnCount() - cur_outer_row_.Count();
          }
          out->Append(cur_outer_row_ + NullRow(ic), cur_outer_pos_);
        } else if (kind_ == JoinKind::kAnti) {
          out->Append(cur_outer_row_, cur_outer_pos_);
        }
      }
    }
  }

  return !out->Empty();
}

void OocApplySource::DumpExplain(std::ostream& out, int indent) const {
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  out << pad << "ApplyExecutor [kind: " << static_cast<int>(kind_)
      << ", alias: " << inner_alias_ << "]\n";
  DumpStreamChain(out, indent + 2, /*is_dump=*/true, outer_.source,
                  outer_.operators);
}

}  // namespace tinylamb
