/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/memory_budget.hpp"
#include "common/platform/extended_float.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/spill/spill_store.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/ooc/chunk_codec.hpp"
#include "executor/detail/ooc/external_sorter.hpp"
#include "executor/detail/ooc/spillable_store.hpp"
#include "executor/detail/pipeline/ooc_sinks.hpp"
#include "executor/detail/window_eval.hpp"
#include "expression/binary_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/collation.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "page/row_position.hpp"
#include "type/collation.hpp"
#include "type/date.hpp"
#include "type/interval.hpp"
#include "type/row.hpp"
#include "type/row_block_codec.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

constexpr size_t kWindowBlockTargetBytes = 32768;
constexpr size_t kMinWindowMemBudget = 16384;

StatusOr<Value> EvalWindowExpr(const Expression& expr, const Row& row,
                               const Schema& schema, TransactionContext* txn) {
  if (!expr) {
    return Value();
  }
  if (txn != nullptr) {
    static const relational_detail::CteMap kEmptyCtes;
    relational_detail::Scope scope{.row = &row, .schema = &schema};
    return relational_detail::TryEvaluate(expr, scope, nullptr, *txn,
                                          kEmptyCtes);
  }
  if (expr->Type() == TypeTag::kColumnValue) {
    const ColumnName& col = expr->AsColumnValue().GetColumnName();
    const int offset = schema.Offset(col);
    if (offset >= 0 && static_cast<size_t>(offset) < row.Count()) {
      return row[static_cast<size_t>(offset)];
    }
    return StatusError(StatusCode::kNotExists,
                       "column not found in EvalWindowExpr: " + col.ToString());
  }
  return expr->TryEvaluate(row, schema);
}

bool SamePartitionTuple(const std::vector<Value>& a,
                        const std::vector<Value>& b) noexcept {
  if (a.size() != b.size()) {
    return false;
  }
  for (size_t i = 0; i < a.size(); ++i) {
    if (!EqualUnderCollation(a[i], b[i])) {
      return false;
    }
  }
  return true;
}

Value KeyAt(const Row& row, slot_t slot) {
  return slot < row.Count() ? row[slot] : Value();
}

std::vector<Value> ExtractKeys(const Row& row, std::span<const slot_t> slots) {
  std::vector<Value> out;
  out.reserve(slots.size());
  for (const slot_t slot : slots) {
    out.push_back(KeyAt(row, slot));
  }
  return out;
}

Row TrimToWidth(Row&& full_row, size_t width) {
  if (full_row.values_.size() > width) {
    full_row.values_.resize(width);
  }
  return std::move(full_row);
}

bool IsCountStarCall(const WindowFunctionCallExpression& window) {
  return window.function == "COUNT" && window.args.size() == 1 &&
         window.args[0] != nullptr &&
         window.args[0]->Type() == TypeTag::kColumnValue &&
         window.args[0]->AsColumnValue().GetColumnName().name == "*";
}

double NumericValueOf(const Value& value) noexcept {
  if (value.type == ValueType::kDouble) {
    return value.value.double_value;
  }
  if (value.type == ValueType::kInt64) {
    return value.IsUnsigned() ? static_cast<double>(static_cast<uint64_t>(
                                    value.value.int_value))
                              : static_cast<double>(value.value.int_value);
  }
  return std::numeric_limits<double>::quiet_NaN();
}

// Widen an INT64/UINT64 key without going through `double`, so 2^63-ish
// values keep their exact magnitude and ordering. Mirrors window_eval's
// FrameNumericOf (the canonical RANGE frame arithmetic).
platform::ExtendedFloat FrameNumericOf(const Value& value) noexcept {
  if (value.type == ValueType::kInt64) {
    return value.IsUnsigned()
               ? static_cast<platform::ExtendedFloat>(
                     static_cast<uint64_t>(value.value.int_value))
               : static_cast<platform::ExtendedFloat>(value.value.int_value);
  }
  return static_cast<platform::ExtendedFloat>(NumericValueOf(value));
}

std::string WindowOrderTermsKey(const std::vector<Expression>& partition_by,
                                const std::vector<WindowOrderTerm>& order_by) {
  auto collation_tag = [](Collation c) -> const char* {
    return c == Collation::kCaseInsensitive ? "@ci" : "";
  };
  std::string key;
  key += "P[";
  for (size_t i = 0; i < partition_by.size(); ++i) {
    if (i > 0) {
      key += ", ";
    }
    key += partition_by[i] ? partition_by[i]->ToString() : "";
    key += collation_tag(CollationOfExpression(partition_by[i]));
  }
  key += "]O[";
  for (size_t i = 0; i < order_by.size(); ++i) {
    if (i > 0) {
      key += ", ";
    }
    const WindowOrderTerm& t = order_by[i];
    key += t.expression ? t.expression->ToString() : "";
    key += collation_tag(CollationOfExpression(t.expression));
    key += t.ascending ? " ASC" : " DESC";
    if (t.nulls_first.has_value()) {
      key += *t.nulls_first ? " NF" : " NL";
    }
  }
  key += "]";
  return key;
}

// Batched SpillSegmentWriter encoding Rows via `WriteRowBlock`.
class SegmentRowWriter {
 public:
  SegmentRowWriter(SpillStore* store,
                   size_t block_target_bytes = kWindowBlockTargetBytes)
      : writer_(store, SpillStore::kBlockSize),
        block_target_bytes_(std::max<size_t>(4096, block_target_bytes)) {}

  Status Append(const Row& row) {
    const size_t row_bytes = EstimateRowMemoryBytes(row);
    pending_.push_back(row);
    pending_bytes_ += row_bytes;
    if (pending_bytes_ >= block_target_bytes_ || pending_.size() >= 256) {
      return FlushBlock();
    }
    return Status::kSuccess;
  }

  Status Append(Row&& row) {
    const size_t row_bytes = EstimateRowMemoryBytes(row);
    pending_.push_back(std::move(row));
    pending_bytes_ += row_bytes;
    if (pending_bytes_ >= block_target_bytes_ || pending_.size() >= 256) {
      return FlushBlock();
    }
    return Status::kSuccess;
  }

  StatusOr<SpillSegmentId> Finish() {
    RETURN_IF_FAIL(FlushBlock());
    return writer_.Finish(true);
  }

  void Abort() noexcept {
    pending_.clear();
    pending_bytes_ = 0;
    writer_.Abort();
  }

 private:
  Status FlushBlock() {
    if (pending_.empty()) {
      return Status::kSuccess;
    }
    RETURN_IF_FAIL(WriteRowBlock(&writer_, pending_, &scratch_));
    pending_.clear();
    pending_bytes_ = 0;
    return Status::kSuccess;
  }

  SpillSegmentWriter writer_;
  size_t block_target_bytes_;
  std::vector<Row> pending_;
  size_t pending_bytes_{0};
  std::string scratch_;
};

// Sequential SpillSegmentReader reading Rows decoded by `ReadRowBlock`.
class SegmentRowReader {
 public:
  SegmentRowReader(const SpillStore* store, SpillSegmentId id,
                   size_t buffer_bytes = SpillStore::kBlockSize)
      : reader_(store, id, buffer_bytes) {}

  Status Open() {
    idx_ = 0;
    block_rows_.clear();
    return reader_.Open();
  }

  StatusOr<bool> Next(Row* out) {
    while (idx_ >= block_rows_.size()) {
      block_rows_.clear();
      idx_ = 0;
      ASSIGN_OR_RETURN(bool, got_block,
                       ReadRowBlock(&reader_, &block_rows_, &scratch_));
      if (!got_block) {
        return false;
      }
    }
    *out = std::move(block_rows_[idx_++]);
    if (idx_ >= block_rows_.size()) {
      block_rows_.clear();
      idx_ = 0;
    }
    return true;
  }

 private:
  SpillSegmentReader reader_;
  std::vector<Row> block_rows_;
  size_t idx_{0};
  std::string scratch_;
};

// Block-indexed SpillSegmentReader allowing bounded-memory random row access
// by zero-based row index within a spilled sorted partition segment.
class SegmentIndexedReader {
 public:
  SegmentIndexedReader(const SpillStore* store, SpillSegmentId id,
                       size_t buffer_bytes = SpillStore::kBlockSize)
      : reader_(store, id, buffer_bytes) {}

  Status Open() {
    RETURN_IF_FAIL(reader_.Open());
    blocks_.clear();
    cached_rows_.clear();
    cached_block_idx_ = std::numeric_limits<size_t>::max();
    uint64_t next_row = 0;
    // Build the block directory from row-block headers only. Decoding every
    // row here made dynamic LAG/LEAD and NTH_VALUE read and rebuild the whole
    // spilled partition before producing the first result. The framed layout
    // is [uint32 row_count][uint32 payload_bytes][payload...], so we can read
    // the two header words and seek past the payload without touching a row.
    std::array<uint32_t, 2> header{0, 0};
    while (!reader_.Eof()) {
      const uint64_t off = reader_.Offset();
      RETURN_IF_FAIL(reader_.ReadExact(header.data(), sizeof(header)));
      const uint32_t row_count = header[0];
      const uint32_t payload_bytes = header[1];
      if (payload_bytes > reader_.Remaining() ||
          row_count > payload_bytes / sizeof(uint32_t)) {
        return StatusError(StatusCode::kCorrupt, "truncated row block payload");
      }
      blocks_.push_back(BlockEntry{
          .offset = off, .first_row = next_row, .row_count = row_count});
      next_row += row_count;
      RETURN_IF_FAIL(reader_.Seek(off + sizeof(header) + payload_bytes));
    }
    total_rows_ = next_row;
    return Status::kSuccess;
  }

  Status ReadAt(uint64_t row_idx, Row* out) {
    if (row_idx >= total_rows_) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SegmentIndexedReader row index out of range");
    }
    if (cached_block_idx_ < blocks_.size()) {
      const BlockEntry& cur = blocks_[cached_block_idx_];
      if (row_idx >= cur.first_row && row_idx < cur.first_row + cur.row_count) {
        *out = cached_rows_[static_cast<size_t>(row_idx - cur.first_row)];
        return Status::kSuccess;
      }
    }
    auto it = std::upper_bound(blocks_.begin(), blocks_.end(), row_idx,
                               [](uint64_t target, const BlockEntry& b) {
                                 return target < b.first_row;
                               });
    if (it == blocks_.begin()) {
      return StatusError(StatusCode::kCorrupt,
                         "SegmentIndexedReader block directory lookup failed");
    }
    --it;
    const size_t bidx = static_cast<size_t>(it - blocks_.begin());
    RETURN_IF_FAIL(reader_.Seek(it->offset));
    cached_rows_.clear();
    ASSIGN_OR_RETURN(bool, got_block,
                     ReadRowBlock(&reader_, &cached_rows_, &scratch_));
    if (!got_block) {
      cached_block_idx_ = std::numeric_limits<size_t>::max();
      return StatusError(StatusCode::kCorrupt,
                         "SegmentIndexedReader failed to read indexed block");
    }
    cached_block_idx_ = bidx;
    const auto local_idx = static_cast<size_t>(row_idx - it->first_row);
    if (local_idx >= cached_rows_.size()) {
      cached_block_idx_ = std::numeric_limits<size_t>::max();
      return StatusError(StatusCode::kCorrupt,
                         "SegmentIndexedReader block row count mismatch");
    }
    *out = cached_rows_[local_idx];
    return Status::kSuccess;
  }

  [[nodiscard]] uint64_t TotalRows() const noexcept { return total_rows_; }

 private:
  struct BlockEntry {
    uint64_t offset{0};
    uint64_t first_row{0};
    uint64_t row_count{0};
  };

  SpillSegmentReader reader_;
  std::vector<BlockEntry> blocks_;
  std::vector<Row> cached_rows_;
  size_t cached_block_idx_{std::numeric_limits<size_t>::max()};
  uint64_t total_rows_{0};
  std::string scratch_;
};

// Returns true iff `expr` does not reference table columns or volatile
// subexpressions and therefore evaluates identically on every row.
bool IsStaticScalarExpr(const Expression& expr) {
  if (!expr) {
    return true;
  }
  if (!expr->TouchedColumns().empty()) {
    return false;
  }
  switch (expr->Type()) {
    case TypeTag::kConstantValue:
    case TypeTag::kIntervalExp:
      return true;
    case TypeTag::kQueryExp:
      return false;
    case TypeTag::kBinaryExp: {
      const auto& b = expr->AsBinaryExpression();
      return IsStaticScalarExpr(b.Left()) && IsStaticScalarExpr(b.Right());
    }
    case TypeTag::kUnaryExp:
      return IsStaticScalarExpr(expr->AsUnaryExpression().Child());
    case TypeTag::kCastExp:
      return IsStaticScalarExpr(expr->AsCastExpression().Child());
    case TypeTag::kFunctionCallExp: {
      const auto& fn = expr->AsFunctionCallExpression();
      if (fn.FuncName() == "RAND" || fn.FuncName() == "RANDOM" ||
          fn.FuncName() == "UUID" || fn.FuncName() == "GENERATE_UUID") {
        return false;
      }
      // NOLINTNEXTLINE(readability-use-anyofallof) explicit loop is clearer
      // here
      for (const Expression& arg :  // NOLINT(readability-use-anyofallof)
           fn.Args()) {  // NOLINT(readability-use-anyofallof)
        if (!IsStaticScalarExpr(arg)) {
          return false;
        }
      }
      return true;
    }
    default:
      return false;
  }
}

StatusOr<int64_t> ValidateIntOffsetValue(const Value& val,
                                         std::string_view err_null,
                                         std::string_view err_non_int,
                                         std::string_view err_negative,
                                         bool disallow_zero = false) {
  if (val.IsNull()) {
    return StatusError(StatusCode::kInvalidArgument, std::string(err_null));
  }
  if (val.type != ValueType::kInt64) {
    return StatusError(StatusCode::kInvalidArgument, std::string(err_non_int));
  }
  const int64_t off = val.value.int_value;
  if (disallow_zero ? off <= 0 : off < 0) {
    return StatusError(StatusCode::kInvalidArgument, std::string(err_negative));
  }
  return off;
}

// Evaluates constant non-negative integer ROWS/LAG/LEAD offset arguments.
StatusOr<int64_t> ParseConstantIntOffset(const Expression& expr,
                                         TransactionContext* txn,
                                         std::string_view err_null,
                                         std::string_view err_non_int,
                                         std::string_view err_negative) {
  if (!expr) {
    return StatusError(StatusCode::kInvalidArgument, std::string(err_non_int));
  }
  if (expr->Type() == TypeTag::kConstantValue) {
    return ValidateIntOffsetValue(expr->AsConstantValue().GetValue(), err_null,
                                  err_non_int, err_negative);
  }
  if (!IsStaticScalarExpr(expr)) {
    return StatusError(StatusCode::kInvalidArgument, std::string(err_non_int));
  }
  static const Schema kEmptySchema;
  static const Row kEmptyRow;
  ASSIGN_OR_RETURN(Value, v,
                   EvalWindowExpr(expr, kEmptyRow, kEmptySchema, txn));
  return ValidateIntOffsetValue(v, err_null, err_non_int, err_negative);
}

bool IsDynamicNthCall(const WindowFunctionCallExpression& call) {
  return call.function == "NTH_VALUE" && call.args.size() >= 2 &&
         !IsStaticScalarExpr(call.args[1]);
}

bool RangeDistanceWithin(const Value& key, const Value& candidate, double off,
                         bool inclusive_le) {
  // Mirror window_eval.cpp's DistanceWithin: keep INT64 exact (including the
  // unsigned domain) and subtract in ExtendedFloat. Converting to double
  // first collapses differences near 2^63 and wrongly opens RANGE bounds.
  const auto numeric = [](const Value& v) -> platform::ExtendedFloat {
    if (v.type == ValueType::kInt64) {
      return v.IsUnsigned()
                 ? static_cast<platform::ExtendedFloat>(
                       static_cast<uint64_t>(v.value.int_value))
                 : static_cast<platform::ExtendedFloat>(v.value.int_value);
    }
    return static_cast<platform::ExtendedFloat>(NumericValueOf(v));
  };
  const platform::ExtendedFloat diff = numeric(key) - numeric(candidate);
  const auto bound = static_cast<platform::ExtendedFloat>(off);
  return inclusive_le ? diff <= bound : diff >= bound;
}

bool RangeCandidateAtMostKeyPlusOffset(const Value& candidate, const Value& key,
                                       double off) {
  const platform::ExtendedFloat limit =
      FrameNumericOf(key) + static_cast<platform::ExtendedFloat>(off);
  const platform::ExtendedFloat cand = FrameNumericOf(candidate);
  return cand <= limit;
}

bool RangeCandidateAtLeastKeyMinusOffset(const Value& candidate,
                                         const Value& key, double off) {
  const platform::ExtendedFloat limit =
      FrameNumericOf(key) - static_cast<platform::ExtendedFloat>(off);
  const platform::ExtendedFloat cand = FrameNumericOf(candidate);
  return cand >= limit;
}

// True for a DOUBLE NaN.  NaN sorts before (ASC) / after (DESC) the finite
// run yet never satisfies a bound comparison, so it breaks the ordered
// scan's early-stop assumption: window_eval has no early stop (it scans
// every candidate), so a NaN candidate must be skipped, not treated as the
// end of the matching run.
bool IsNaNOrderKey(const Value& value) {
  return value.type == ValueType::kDouble &&
         std::isnan(value.value.double_value);
}

struct RangeBoundSpec {
  WindowFrameBoundType type{WindowFrameBoundType::kCurrentRow};
  bool is_interval{false};
  IntervalValue interval;
  double numeric{0.0};
};

StatusOr<RangeBoundSpec> ParseRangeBoundSpec(const WindowFrameBound& bound,
                                             TransactionContext* txn) {
  RangeBoundSpec spec;
  spec.type = bound.type;
  if (bound.type != WindowFrameBoundType::kOffsetPreceding &&
      bound.type != WindowFrameBoundType::kOffsetFollowing) {
    return spec;
  }
  if (!bound.offset) {
    return StatusError(StatusCode::kInvalidArgument,
                       "RANGE offset must be numeric");
  }
  if (bound.offset->Type() == TypeTag::kIntervalExp) {
    const IntervalValue& iv =
        bound.offset->AsIntervalExpression().GetIntervalValue();
    spec.is_interval = true;
    spec.interval = iv;
    return spec;
  }
  Value off_val;
  if (bound.offset->Type() == TypeTag::kConstantValue) {
    off_val = bound.offset->AsConstantValue().GetValue();
  } else if (IsStaticScalarExpr(bound.offset)) {
    static const Schema kEmptySchema;
    static const Row kEmptyRow;
    ASSIGN_OR_RETURN(
        Value, eval_val,
        EvalWindowExpr(bound.offset, kEmptyRow, kEmptySchema, txn));
    off_val = std::move(eval_val);
  } else {
    return StatusError(StatusCode::kInvalidArgument,
                       "RANGE offset must be constant numeric");
  }
  if (off_val.IsNull()) {
    return StatusError(
        StatusCode::kInvalidArgument,
        "Window frame offset for PRECEDING or FOLLOWING cannot be NULL");
  }
  if (off_val.type == ValueType::kVarChar) {
    if (StatusOr<IntervalValue> interval =
            IntervalValue::TryParse(off_val.value.varchar_value);
        interval.HasValue()) {
      spec.is_interval = true;
      spec.interval = interval.MoveValue();
      return spec;
    }
  }
  if (off_val.type != ValueType::kInt64 && off_val.type != ValueType::kDouble) {
    return StatusError(StatusCode::kInvalidArgument,
                       "RANGE offset must be numeric");
  }
  const double off = NumericValueOf(off_val);
  if (std::isnan(off)) {
    return StatusError(
        StatusCode::kInvalidArgument,
        "Window frame offset for PRECEDING or FOLLOWING cannot be NaN");
  }
  if (off < 0) {
    return StatusError(
        StatusCode::kInvalidArgument,
        "Window frame offset for PRECEDING or FOLLOWING must be non-negative");
  }
  spec.numeric = off;
  return spec;
}

StatusOr<int64_t> RangeIntervalBoundaryDays(const Value& key, int sign,
                                            const IntervalValue& interval) {
  if (key.type != ValueType::kDate) {
    return StatusError(StatusCode::kInvalidArgument,
                       "RANGE INTERVAL offset requires a DATE order key");
  }
  if (interval.nanos != 0) {
    return StatusError(
        StatusCode::kInvalidArgument,
        "RANGE INTERVAL offset must be day-aligned for DATE keys");
  }
  int64_t days = key.value.int_value;
  if (interval.months != 0) {
    ASSIGN_OR_RETURN(
        int64_t, shifted_months,
        TryAddDateIntervalDays(days, sign * interval.months, "month"));
    days = shifted_months;
  }
  if (interval.days != 0) {
    ASSIGN_OR_RETURN(int64_t, shifted_days,
                     TryAddDateIntervalDays(days, sign * interval.days, "day"));
    days = shifted_days;
  }
  return days;
}

// Resolves streaming monotonically moving `[lo, hi]` bounds for `RANGE` frames
// over a sorted spilled partition segment using bounded `SegmentRowReader`
// lookaheads.
// NOLINTNEXTLINE(clang-analyzer-optin.performance.Padding) intentional pattern
// in this code
class RangeFrameScanner {  // NOLINT(clang-analyzer-optin.performance.Padding)
 public:
  RangeFrameScanner(const SpillStore* store, SpillSegmentId seg_id,
                    uint64_t total_rows, std::span<const slot_t> order_slots,
                    bool ascending, RangeBoundSpec start_bound,
                    RangeBoundSpec end_bound, size_t buffer_bytes)
      : total_rows_(total_rows),
        order_slots_(order_slots),
        ascending_(ascending),
        start_bound_(start_bound),
        end_bound_(end_bound),
        store_(store),
        seg_id_(seg_id),
        buffer_bytes_(buffer_bytes),
        peer_reader_(store, seg_id, buffer_bytes),
        lo_reader_(store, seg_id, buffer_bytes),
        hi_reader_(store, seg_id, buffer_bytes) {}

  Status Open() {
    RETURN_IF_FAIL(peer_reader_.Open());
    RETURN_IF_FAIL(lo_reader_.Open());
    RETURN_IF_FAIL(hi_reader_.Open());
    // NULL and NaN keys collapse offset bounds to their own peer group (see
    // ResolveForRow), and both sort runs sit at one end of the partition.
    // That lets a frame's lower bound restart at a LOWER position after
    // finite rows -- and empty frames -- have already pushed it past it (DESC
    // puts NULL/NaN last, so `15 FOLLOWING` frames of the finite run end
    // below the NULL/NaN runs while those rows' own collapsed frames sit on
    // top of them).  Consumers retire deque rows at a monotone floor, so
    // record where each collapse run starts; RestartFloor() tells them the
    // lowest position a later frame can still read.  One sequential key pass
    // over the segment, O(1) extra memory.
    if (order_slots_.size() == 1 &&
        (start_bound_.type == WindowFrameBoundType::kOffsetPreceding ||
         start_bound_.type == WindowFrameBoundType::kOffsetFollowing ||
         end_bound_.type == WindowFrameBoundType::kOffsetPreceding ||
         end_bound_.type == WindowFrameBoundType::kOffsetFollowing)) {
      SegmentRowReader scan(store_, seg_id_, buffer_bytes_);
      RETURN_IF_FAIL(scan.Open());
      uint64_t idx = 0;
      bool in_run = false;
      uint64_t run_start = 0;
      while (true) {
        Row r;
        ASSIGN_OR_RETURN(bool, got, scan.Next(&r));
        if (!got) {
          break;
        }
        const Value key = KeyAt(r, order_slots_.front());
        if (key.IsNull() || IsNaNOrderKey(key)) {
          if (!in_run) {
            in_run = true;
            run_start = idx;
          }
        } else if (in_run) {
          in_run = false;
          peer_collapse_runs_.emplace_back(run_start, idx - 1);
        }
        ++idx;
      }
      if (in_run) {
        peer_collapse_runs_.emplace_back(run_start, idx - 1);
      }
    }
    return Status::kSuccess;
  }

  // Lowest frame lower bound any row at index >= k can resolve to, or
  // nullopt when no NULL/NaN peer-collapse run can restart the scan from
  // below.
  [[nodiscard]] std::optional<uint64_t> RestartFloor(uint64_t k) const {
    for (const auto& g : peer_collapse_runs_) {
      if (g.second >= k) {
        return g.first;
      }
    }
    return std::nullopt;
  }

  Status ResolveForRow(uint64_t k, const Row& cur_row, bool* empty_out,
                       uint64_t* lo_out, uint64_t* hi_out) {
    if (total_rows_ == 0) {
      *empty_out = true;
      return Status::kSuccess;
    }
    const std::vector<Value> cur_keys = ExtractKeys(cur_row, order_slots_);
    if (!has_peer_ || k > peer_end_) {
      peer_start_ = k;
      peer_keys_ = cur_keys;
      peer_end_ = k;
      if (peer_next_idx_ <= k) {
        while (peer_next_idx_ <= k) {
          Row discard;
          ASSIGN_OR_RETURN(bool, got, peer_reader_.Next(&discard));
          if (!got) {
            break;
          }
          ++peer_next_idx_;
        }
      }
      while (true) {
        if (!peer_ahead_valid_) {
          Row nxt;
          ASSIGN_OR_RETURN(bool, got, peer_reader_.Next(&nxt));
          if (!got) {
            break;
          }
          peer_ahead_keys_ = ExtractKeys(nxt, order_slots_);
          peer_ahead_idx_ = peer_next_idx_++;
          peer_ahead_valid_ = true;
        }
        if (SamePartitionTuple(peer_keys_, peer_ahead_keys_)) {
          peer_end_ = peer_ahead_idx_;
          peer_ahead_valid_ = false;
        } else {
          break;
        }
      }
      has_peer_ = true;
    }

    const bool key_is_null =
        order_slots_.size() == 1 && !cur_keys.empty() && cur_keys[0].IsNull();
    if (order_slots_.empty() || key_is_null ||
        (start_bound_.type != WindowFrameBoundType::kOffsetPreceding &&
         start_bound_.type != WindowFrameBoundType::kOffsetFollowing &&
         end_bound_.type != WindowFrameBoundType::kOffsetPreceding &&
         end_bound_.type != WindowFrameBoundType::kOffsetFollowing)) {
      const uint64_t lo =
          start_bound_.type == WindowFrameBoundType::kUnboundedPreceding
              ? uint64_t{0}
              : (start_bound_.type == WindowFrameBoundType::kUnboundedFollowing
                     ? total_rows_ - 1
                     : peer_start_);
      const uint64_t hi =
          end_bound_.type == WindowFrameBoundType::kUnboundedFollowing
              ? total_rows_ - 1
              : (end_bound_.type == WindowFrameBoundType::kUnboundedPreceding
                     ? uint64_t{0}
                     : peer_end_);
      if (lo > hi) {
        *empty_out = true;
        // Publish the bounds even when the frame is empty: the deque-based
        // window modes use `lo` as the row-retirement floor, and an empty
        // frame whose bounds are dropped silently forces the floor to k_,
        // which retires rows a later (non-empty) frame still reads.
        *lo_out = lo;
        *hi_out = hi;
      } else {
        *empty_out = false;
        *lo_out = lo;
        *hi_out = hi;
      }
      return Status::kSuccess;
    }

    const Value& key = cur_keys[0];
    std::optional<uint64_t> lo_idx;
    switch (start_bound_.type) {
      case WindowFrameBoundType::kUnboundedPreceding:
        lo_idx = 0;
        break;
      case WindowFrameBoundType::kUnboundedFollowing:
        lo_idx = total_rows_ - 1;
        break;
      case WindowFrameBoundType::kCurrentRow:
        lo_idx = peer_start_;
        break;
      case WindowFrameBoundType::kOffsetPreceding: {
        if (IsNaNOrderKey(key)) {
          // NaNs form one peer group and every NaN comparison is false, so
          // no candidate can ever satisfy the bound: window_eval collapses
          // the start bound to the peer-group head.  Without this the scan
          // consumes rows it can never return and misplaces the frame start.
          lo_idx = peer_start_;
          break;
        }
        std::optional<int64_t> boundary;
        if (start_bound_.is_interval) {
          ASSIGN_OR_RETURN(int64_t, b,
                           RangeIntervalBoundaryDays(key, ascending_ ? -1 : 1,
                                                     start_bound_.interval));
          boundary = b;
        }
        while (true) {
          if (!lo_ahead_valid_) {
            Row r;
            ASSIGN_OR_RETURN(bool, got, lo_reader_.Next(&r));
            if (!got) {
              lo_idx = k;
              break;
            }
            lo_ahead_key_ = KeyAt(r, order_slots_.front());
            lo_ahead_idx_ = lo_next_idx_++;
            lo_ahead_valid_ = true;
          }
          if (lo_ahead_idx_ > k) {
            lo_idx = k;
            break;
          }
          if (lo_ahead_key_.IsNull()) {
            lo_ahead_valid_ = false;
            continue;
          }
          const bool ok =
              boundary.has_value()
                  ? (ascending_ ? lo_ahead_key_.value.int_value >= *boundary
                                : lo_ahead_key_.value.int_value <= *boundary)
                  : (ascending_
                         ? RangeDistanceWithin(key, lo_ahead_key_,
                                               start_bound_.numeric, true)
                         : RangeCandidateAtMostKeyPlusOffset(
                               lo_ahead_key_, key, start_bound_.numeric));
          if (ok) {
            lo_idx = lo_ahead_idx_;
            break;
          }
          if (lo_ahead_idx_ == k) {
            lo_idx = k;
            break;
          }
          lo_ahead_valid_ = false;
        }
        break;
      }
      case WindowFrameBoundType::kOffsetFollowing: {
        if (IsNaNOrderKey(key)) {
          // Same NaN collapse as the PRECEDING arm (window_eval: the offset
          // bound cannot distinguish NaNs, so it opens at the peer-group
          // head).  Critically, the scan must not run: it would drain the
          // shared sequential lo_reader_ to EOF for a bound that can never
          // be satisfied, leaving every later row's frame empty.
          lo_idx = peer_start_;
          break;
        }
        std::optional<int64_t> boundary;
        if (start_bound_.is_interval) {
          ASSIGN_OR_RETURN(int64_t, b,
                           RangeIntervalBoundaryDays(key, ascending_ ? 1 : -1,
                                                     start_bound_.interval));
          boundary = b;
        }
        while (true) {
          if (!lo_ahead_valid_) {
            Row r;
            ASSIGN_OR_RETURN(bool, got, lo_reader_.Next(&r));
            if (!got) {
              break;
            }
            lo_ahead_key_ = KeyAt(r, order_slots_.front());
            lo_ahead_idx_ = lo_next_idx_++;
            lo_ahead_valid_ = true;
          }
          if (lo_ahead_key_.IsNull()) {
            lo_ahead_valid_ = false;
            continue;
          }
          const bool ok =
              boundary.has_value()
                  ? (ascending_ ? lo_ahead_key_.value.int_value >= *boundary
                                : lo_ahead_key_.value.int_value <= *boundary)
                  : (ascending_
                         ? RangeDistanceWithin(lo_ahead_key_, key,
                                               start_bound_.numeric, false)
                         : RangeCandidateAtMostKeyPlusOffset(
                               lo_ahead_key_, key, -start_bound_.numeric));
          if (ok) {
            lo_idx = lo_ahead_idx_;
            break;
          }
          lo_ahead_valid_ = false;
        }
        break;
      }
    }

    std::optional<uint64_t> hi_idx;
    switch (end_bound_.type) {
      case WindowFrameBoundType::kUnboundedFollowing:
        hi_idx = total_rows_ - 1;
        break;
      case WindowFrameBoundType::kUnboundedPreceding:
        hi_idx = 0;
        break;
      case WindowFrameBoundType::kCurrentRow:
        hi_idx = peer_end_;
        break;
      case WindowFrameBoundType::kOffsetFollowing: {
        std::optional<int64_t> boundary;
        if (end_bound_.is_interval) {
          ASSIGN_OR_RETURN(int64_t, b,
                           RangeIntervalBoundaryDays(key, ascending_ ? 1 : -1,
                                                     end_bound_.interval));
          boundary = b;
        }
        if (!boundary.has_value() && IsNaNOrderKey(key)) {
          // NaN key: no candidate compares, so window_eval's end bound is
          // `reached.value_or(peer_end)` = peer_end.  Skip the scan and any
          // stale hi_last_reached_ carry-over.
          hi_idx = peer_end_;
          break;
        }
        while (true) {
          if (!hi_ahead_valid_) {
            Row r;
            ASSIGN_OR_RETURN(bool, got, hi_reader_.Next(&r));
            if (!got) {
              break;
            }
            hi_ahead_key_ = KeyAt(r, order_slots_.front());
            hi_ahead_idx_ = hi_next_idx_++;
            hi_ahead_valid_ = true;
          }
          if (hi_ahead_key_.IsNull()) {
            if (saw_non_null_in_hi_) {
              break;
            }
            hi_ahead_valid_ = false;
            continue;
          }
          saw_non_null_in_hi_ = true;
          const bool ok =
              boundary.has_value()
                  ? (ascending_ ? hi_ahead_key_.value.int_value <= *boundary
                                : hi_ahead_key_.value.int_value >= *boundary)
                  : (ascending_ ? RangeDistanceWithin(hi_ahead_key_, key,
                                                      end_bound_.numeric, true)
                                : RangeCandidateAtLeastKeyMinusOffset(
                                      hi_ahead_key_, key, end_bound_.numeric));
          if (ok) {
            hi_last_reached_ = hi_ahead_idx_;
            hi_ahead_valid_ = false;
            continue;
          }
          if (IsNaNOrderKey(hi_ahead_key_)) {
            // NaN candidate: never satisfies the comparison, but window_eval
            // scans past it (no early stop), so do not treat it as the end
            // of the matching run.
            hi_ahead_valid_ = false;
            continue;
          }
          break;
        }
        if (hi_last_reached_.has_value()) {
          // NOLINTNEXTLINE(bugprone-optional-value-conversion) deliberate
          hi_idx = *hi_last_reached_;
        } else if (!end_bound_.is_interval) {
          hi_idx = peer_end_;
        }
        break;
      }
      case WindowFrameBoundType::kOffsetPreceding: {
        const bool zero_interval = end_bound_.is_interval &&
                                   end_bound_.interval.months == 0 &&
                                   end_bound_.interval.days == 0;
        if ((!end_bound_.is_interval && end_bound_.numeric == 0.0) ||
            zero_interval) {
          hi_idx = peer_end_;
          break;
        }
        std::optional<int64_t> boundary;
        if (end_bound_.is_interval) {
          ASSIGN_OR_RETURN(int64_t, b,
                           RangeIntervalBoundaryDays(key, ascending_ ? -1 : 1,
                                                     end_bound_.interval));
          boundary = b;
        }
        if (!boundary.has_value() && IsNaNOrderKey(key)) {
          // NaN key: no candidate compares, so window_eval's end bound is
          // nullopt (empty frame).  Skip the scan and any stale
          // hi_last_reached_ carry-over.
          break;
        }
        while (true) {
          if (!hi_ahead_valid_) {
            Row r;
            ASSIGN_OR_RETURN(bool, got, hi_reader_.Next(&r));
            if (!got) {
              break;
            }
            hi_ahead_key_ = KeyAt(r, order_slots_.front());
            hi_ahead_idx_ = hi_next_idx_++;
            hi_ahead_valid_ = true;
          }
          if (hi_ahead_idx_ > k) {
            break;
          }
          if (hi_ahead_key_.IsNull()) {
            if (saw_non_null_in_hi_) {
              break;
            }
            hi_ahead_valid_ = false;
            continue;
          }
          saw_non_null_in_hi_ = true;
          bool ok = false;
          if (boundary.has_value()) {
            ok = ascending_ ? hi_ahead_key_.value.int_value <= *boundary
                            : hi_ahead_key_.value.int_value >= *boundary;
          } else {
            const double off = end_bound_.numeric;
            const bool key_int = key.type == ValueType::kInt64;
            const platform::ExtendedFloat asc_limit =
                key_int ? static_cast<platform::ExtendedFloat>(
                              key.value.int_value) -
                              static_cast<platform::ExtendedFloat>(off)
                        : static_cast<platform::ExtendedFloat>(
                              NumericValueOf(key) - off);
            const platform::ExtendedFloat desc_limit =
                key_int ? static_cast<platform::ExtendedFloat>(
                              key.value.int_value) +
                              static_cast<platform::ExtendedFloat>(off)
                        : static_cast<platform::ExtendedFloat>(
                              NumericValueOf(key) + off);
            const platform::ExtendedFloat cand =
                hi_ahead_key_.type == ValueType::kInt64
                    ? static_cast<platform::ExtendedFloat>(
                          hi_ahead_key_.value.int_value)
                    : static_cast<platform::ExtendedFloat>(
                          NumericValueOf(hi_ahead_key_));
            ok = ascending_ ? (cand <= asc_limit) : (cand >= desc_limit);
          }
          if (ok) {
            hi_last_reached_ = hi_ahead_idx_;
            hi_ahead_valid_ = false;
            continue;
          }
          if (IsNaNOrderKey(hi_ahead_key_)) {
            // NaN candidate: never satisfies the comparison; window_eval
            // keeps scanning (no early stop), so do not end the run here.
            hi_ahead_valid_ = false;
            continue;
          }
          break;
        }
        if (hi_last_reached_.has_value()) {
          // NOLINTNEXTLINE(bugprone-optional-value-conversion) deliberate
          hi_idx = *hi_last_reached_;
        }
        break;
      }
    }

    if (!lo_idx.has_value() || !hi_idx.has_value() || *lo_idx > *hi_idx) {
      *empty_out = true;
      // Same contract as the peer-scan branch above: publish the bound the
      // caller may retire the deque to. A bound the scan could not find sits
      // at (or past) the last row, so retiring everything is safe there.
      *lo_out = lo_idx.value_or(total_rows_);
      *hi_out = hi_idx.value_or(total_rows_ > 0 ? total_rows_ - 1 : 0);
    } else {
      *empty_out = false;
      *lo_out = *lo_idx;
      *hi_out = *hi_idx;
    }
    return Status::kSuccess;
  }

 private:
  uint64_t total_rows_{0};
  std::span<const slot_t> order_slots_;
  bool ascending_{true};
  RangeBoundSpec start_bound_;
  RangeBoundSpec end_bound_;

  const SpillStore* store_;
  SpillSegmentId seg_id_;
  size_t buffer_bytes_;

  // Sorted, disjoint [first,last] index runs of NULL / NaN order keys,
  // whose offset frames peer-collapse (see Open()).
  std::vector<std::pair<uint64_t, uint64_t>> peer_collapse_runs_;

  SegmentRowReader peer_reader_;
  bool has_peer_{false};
  uint64_t peer_start_{0};
  uint64_t peer_end_{0};
  std::vector<Value> peer_keys_;
  uint64_t peer_next_idx_{0};
  bool peer_ahead_valid_{false};
  uint64_t peer_ahead_idx_{0};
  std::vector<Value> peer_ahead_keys_;

  SegmentRowReader lo_reader_;
  uint64_t lo_next_idx_{0};
  bool lo_ahead_valid_{false};
  uint64_t lo_ahead_idx_{0};
  Value lo_ahead_key_;

  SegmentRowReader hi_reader_;
  uint64_t hi_next_idx_{0};
  bool hi_ahead_valid_{false};
  uint64_t hi_ahead_idx_{0};
  Value hi_ahead_key_;
  bool saw_non_null_in_hi_{false};
  std::optional<uint64_t> hi_last_reached_;
};

// Streaming O(1) window accumulator covering the core relational window
// aggregates and value functions for large/skewed partitions.
class StreamingWindowAggState {
 public:
  StreamingWindowAggState(const WindowFunctionCallExpression& window,
                          const Schema& input_schema, TransactionContext* txn)
      : window_(window),
        input_schema_(input_schema),
        txn_(txn),
        fn_(window.function),
        count_star_(IsCountStarCall(window)) {}

  [[nodiscard]] static bool SupportsStreaming(
      const WindowFunctionCallExpression& window) {
    if (window.distinct) {
      return false;
    }
    const std::string& fn = window.function;
    return fn == "COUNT" || fn == "COUNTIF" || fn == "SUM" || fn == "AVG" ||
           fn == "MIN" || fn == "MAX" || fn == "BIT_AND" || fn == "BIT_OR" ||
           fn == "BIT_XOR" || fn == "LOGICAL_AND" || fn == "LOGICAL_OR" ||
           fn == "ANY_VALUE" || fn == "FIRST_VALUE" || fn == "LAST_VALUE" ||
           fn == "NTH_VALUE";
  }

  [[nodiscard]] static bool SupportsInvertibleSubtract(
      const WindowFunctionCallExpression& window) {
    if (window.distinct) {
      return false;
    }
    const std::string& fn = window.function;
    // NOTE: SUM/AVG are excluded entirely. DOUBLE SUM/AVG is not invertible
    // under subtraction (cancellation loses bits later frames need), and a
    // sticky `any_double_` would keep a double lane even for int-only frames;
    // these functions use the generic frame-recompute path instead. IGNORE
    // NULLS is a no-op for the listed functions (all skip NULLs natively).
    // BIT_AND/BIT_OR subtract through 64 per-bit counters and LOGICAL_AND/OR
    // through true/false counts, so those are O(1) removable too.
    // SUM/AVG are excluded: DOUBLE addition is not invertible under
    // subtraction (cancellation loses bits that later frames would need), so
    // suffix/sliding frames with these functions must use the generic
    // frame-recompute path instead of blind `double_sum_ -= value`.
    return fn == "COUNT" || fn == "COUNTIF" || fn == "BIT_XOR" ||
           fn == "BIT_AND" || fn == "BIT_OR" || fn == "LOGICAL_AND" ||
           fn == "LOGICAL_OR";
  }

  Status InitNthConstant() {
    if (fn_ == "NTH_VALUE") {
      if (window_.args.size() < 2) {
        return StatusError(StatusCode::kInvalidArgument,
                           "NTH_VALUE requires two arguments");
      }
      if (window_.args[1] != nullptr && IsStaticScalarExpr(window_.args[1])) {
        Value v;
        if (window_.args[1]->Type() == TypeTag::kConstantValue) {
          v = window_.args[1]->AsConstantValue().GetValue();
        } else {
          static const Schema kEmptySchema;
          static const Row kEmptyRow;
          ASSIGN_OR_RETURN(
              Value, ev,
              EvalWindowExpr(window_.args[1], kEmptyRow, kEmptySchema, txn_));
          v = std::move(ev);
        }
        ASSIGN_OR_RETURN(
            int64_t, nth,
            ValidateIntOffsetValue(
                v, "The N value for the NthValue function must not be NULL",
                "The N value for the NthValue function must be an integer",
                "The N value for the NthValue function must be positive",
                /*disallow_zero=*/true));
        nth_target_ = static_cast<size_t>(nth);
      } else {
        return StatusError(StatusCode::kNotImplemented,
                           "Dynamic NTH_VALUE target on skewed partition "
                           "requires indexed frame resolution");
      }
    }
    return Status::kSuccess;
  }

  void Reset() noexcept {
    surviving_rows_ = 0;
    non_null_count_ = 0;
    trues_ = 0;
    int_sum_ = 0;
    uint_sum_ = 0;
    sum_is_uint64_ = false;
    double_sum_ = 0.0;
    bit_acc_ = 0;
    nth_seen_ = 0;
    best_val_ = Value();
    first_val_ = Value();
    last_val_ = Value();
    nth_val_ = Value();
    logical_trues_ = 0;
    logical_falses_ = 0;
    bit_ones_.fill(0);
    any_double_ = false;
    has_best_ = false;
    best_is_nan_ = false;
    has_first_ = false;
    has_last_ = false;
    has_nth_ = false;
  }

  Status AddRow(const Row& row) {
    if (window_.where_filter != nullptr) {
      ASSIGN_OR_RETURN(
          Value, wf,
          EvalWindowExpr(window_.where_filter, row, input_schema_, txn_));
      if (!relational_detail::Truthy(wf)) {
        return Status::kSuccess;
      }
    }
    ++surviving_rows_;
    if (count_star_) {
      return Status::kSuccess;
    }
    if (window_.args.empty()) {
      return Status::kSuccess;
    }
    ASSIGN_OR_RETURN(Value, v,
                     EvalWindowExpr(window_.args[0], row, input_schema_, txn_));
    if (fn_ == "FIRST_VALUE") {
      if (window_.ignore_nulls && v.IsNull()) {
        return Status::kSuccess;
      }
      if (!has_first_) {
        first_val_ = std::move(v);
        has_first_ = true;
      }
      return Status::kSuccess;
    }
    if (fn_ == "LAST_VALUE") {
      if (window_.ignore_nulls && v.IsNull()) {
        return Status::kSuccess;
      }
      last_val_ = std::move(v);
      has_last_ = true;
      return Status::kSuccess;
    }
    if (fn_ == "NTH_VALUE") {
      if (window_.ignore_nulls && v.IsNull()) {
        return Status::kSuccess;
      }
      ++nth_seen_;
      if (nth_seen_ == nth_target_) {
        nth_val_ = std::move(v);
        has_nth_ = true;
      }
      return Status::kSuccess;
    }
    if (v.IsNull()) {
      return Status::kSuccess;
    }
    ++non_null_count_;
    if (fn_ == "COUNT") {
      return Status::kSuccess;
    }
    if (fn_ == "ANY_VALUE") {
      if (!has_first_) {
        first_val_ = std::move(v);
        has_first_ = true;
      }
      return Status::kSuccess;
    }
    if (fn_ == "COUNTIF") {
      if (relational_detail::Truthy(v)) {
        ++trues_;
      }
      return Status::kSuccess;
    }
    if (fn_ == "LOGICAL_AND" || fn_ == "LOGICAL_OR") {
      if (relational_detail::Truthy(v)) {
        ++logical_trues_;
      } else {
        ++logical_falses_;
      }
      return Status::kSuccess;
    }
    if (fn_ == "SUM" || fn_ == "AVG") {
      if (v.type != ValueType::kInt64 && v.type != ValueType::kDouble) {
        return StatusError(StatusCode::kInvalidArgument,
                           fn_ + " requires a numeric argument");
      }
      if (v.type == ValueType::kDouble) {
        if (!any_double_) {
          double_sum_ = static_cast<double>(int_sum_);
          int_sum_ = 0;
          any_double_ = true;
        }
        double_sum_ += v.value.double_value;
      } else if (fn_ == "AVG") {
        // AVG sums through a double intermediate (AggregateAccumulator
        // ground truth): per-value tag-respecting cast, never an overflow
        // error. The exact int/uint totals stay untouched for the SUM arm.
        if (!any_double_) {
          double_sum_ = static_cast<double>(int_sum_);
          int_sum_ = 0;
          any_double_ = true;
        }
        double_sum_ +=
            v.IsUnsigned()
                ? static_cast<double>(static_cast<uint64_t>(v.value.int_value))
                : static_cast<double>(v.value.int_value);
      } else if (sum_is_uint64_ || v.IsUnsigned()) {
        // Mirror AggregateAccumulator's SUM: any UINT64 input switches the
        // total to an exact Int128 accumulation bounded to the uint64 range
        // and the result carries the UINT64 tag; a plain signed wrap would
        // make the streamed window disagree with the grouped SUM.
        const platform::Int128 prior =
            sum_is_uint64_ ? static_cast<platform::Int128>(uint_sum_)
                           : static_cast<platform::Int128>(int_sum_);
        const platform::Int128 input =
            v.IsUnsigned() ? static_cast<platform::Int128>(
                                 static_cast<uint64_t>(v.value.int_value))
                           : static_cast<platform::Int128>(v.value.int_value);
        const platform::Int128 next = prior + input;
        constexpr platform::Int128 max_uint64 =
            std::numeric_limits<uint64_t>::max();
        if (next < 0 || next > max_uint64) {
          return StatusError(StatusCode::kIsInfinity, "uint64 overflow in SUM");
        }
        sum_is_uint64_ = true;
        uint_sum_ = static_cast<uint64_t>(next);
        int_sum_ = 0;
      } else if (any_double_) {
        double_sum_ += static_cast<double>(v.value.int_value);
      } else {
        if (__builtin_add_overflow(int_sum_, v.value.int_value, &int_sum_)) {
          return StatusError(StatusCode::kIsInfinity,
                             "integer overflow in " + fn_);
        }
      }
      return Status::kSuccess;
    }
    if (fn_ == "MIN" || fn_ == "MAX") {
      if (v.type == ValueType::kDouble && std::isnan(v.value.double_value)) {
        best_val_ = v;
        has_best_ = true;
        best_is_nan_ = true;
        return Status::kSuccess;
      }
      if (best_is_nan_) {
        return Status::kSuccess;
      }
      if (!has_best_) {
        best_val_ = std::move(v);
        has_best_ = true;
        return Status::kSuccess;
      }
      const StatusOr<bool> lt =
          fn_ == "MIN" ? v.TryLess(best_val_) : best_val_.TryLess(v);
      if (lt.HasValue() && lt.Value()) {
        best_val_ = std::move(v);
      }
      return Status::kSuccess;
    }
    if (fn_ == "BIT_AND" || fn_ == "BIT_OR") {
      if (v.type != ValueType::kInt64) {
        return StatusError(StatusCode::kInvalidArgument,
                           fn_ + " requires an integer argument");
      }
      // Per-bit population counters make the aggregate O(1) removable: bit b
      // of BIT_AND is set iff every non-null argument has it, i.e.
      // bit_ones_[b] == non_null_count_; BIT_OR iff bit_ones_[b] > 0.
      const auto bits = static_cast<uint64_t>(v.value.int_value);
      for (size_t b = 0; b < bit_ones_.size(); ++b) {
        bit_ones_[b] += static_cast<int32_t>((bits >> b) & 1U);
      }
      return Status::kSuccess;
    }
    if (fn_ == "BIT_XOR") {
      if (v.type != ValueType::kInt64) {
        return StatusError(StatusCode::kInvalidArgument,
                           fn_ + " requires an integer argument");
      }
      bit_acc_ ^= v.value.int_value;
      return Status::kSuccess;
    }
    // T2131: "this aggregate has no streaming implementation" is a
    // capability gap (SQLSTATE 0A000 via kNotImplemented), not a resource
    // failure -- 53200 here made clients retry with more memory forever.
    return StatusError(StatusCode::kNotImplemented,
                       "Unsupported streaming window aggregate: " + fn_);
  }

  Status RemoveRow(const Row& row) {
    if (window_.where_filter != nullptr) {
      ASSIGN_OR_RETURN(
          Value, wf,
          EvalWindowExpr(window_.where_filter, row, input_schema_, txn_));
      if (!relational_detail::Truthy(wf)) {
        return Status::kSuccess;
      }
    }
    --surviving_rows_;
    if (count_star_) {
      return Status::kSuccess;
    }
    if (window_.args.empty()) {
      return Status::kSuccess;
    }
    ASSIGN_OR_RETURN(Value, v,
                     EvalWindowExpr(window_.args[0], row, input_schema_, txn_));
    if (v.IsNull()) {
      return Status::kSuccess;
    }
    --non_null_count_;
    if (fn_ == "COUNT") {
      return Status::kSuccess;
    }
    if (fn_ == "COUNTIF") {
      if (relational_detail::Truthy(v)) {
        --trues_;
      }
      return Status::kSuccess;
    }
    if (fn_ == "LOGICAL_AND" || fn_ == "LOGICAL_OR") {
      if (relational_detail::Truthy(v)) {
        --logical_trues_;
      } else {
        --logical_falses_;
      }
      return Status::kSuccess;
    }
    if (fn_ == "BIT_XOR") {
      if (v.type != ValueType::kInt64) {
        return StatusError(StatusCode::kInvalidArgument,
                           fn_ + " requires an integer argument");
      }
      bit_acc_ ^= v.value.int_value;
      return Status::kSuccess;
    }
    if (fn_ == "BIT_AND" || fn_ == "BIT_OR") {
      if (v.type != ValueType::kInt64) {
        return StatusError(StatusCode::kInvalidArgument,
                           fn_ + " requires an integer argument");
      }
      const auto bits = static_cast<uint64_t>(v.value.int_value);
      for (size_t b = 0; b < bit_ones_.size(); ++b) {
        bit_ones_[b] -= static_cast<int32_t>((bits >> b) & 1U);
      }
      return Status::kSuccess;
    }
    if (fn_ == "SUM" || fn_ == "AVG") {
      if (fn_ == "SUM" && v.type == ValueType::kInt64 &&
          (sum_is_uint64_ || v.IsUnsigned())) {
        // Invert the exact uint64-domain SUM accumulation.
        const platform::Int128 input =
            v.IsUnsigned() ? static_cast<platform::Int128>(
                                 static_cast<uint64_t>(v.value.int_value))
                           : static_cast<platform::Int128>(v.value.int_value);
        const platform::Int128 next =
            static_cast<platform::Int128>(uint_sum_) - input;
        constexpr platform::Int128 max_uint64 =
            std::numeric_limits<uint64_t>::max();
        if (next < 0 || next > max_uint64) {
          return StatusError(StatusCode::kIsInfinity, "uint64 overflow in SUM");
        }
        uint_sum_ = static_cast<uint64_t>(next);
        return Status::kSuccess;
      }
      if (v.type == ValueType::kDouble || any_double_) {
        double_sum_ -= NumericValueOf(v);
      } else {
        int_sum_ -= v.value.int_value;
      }
      return Status::kSuccess;
    }
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "Non-invertible remove on skewed frame: " + fn_);
  }

  [[nodiscard]] Value Result() const {
    if (fn_ == "COUNT") {
      return Value(count_star_ ? surviving_rows_ : non_null_count_);
    }
    if (fn_ == "COUNTIF") {
      return Value(trues_);
    }
    if (surviving_rows_ == 0) {
      return {};
    }
    if (fn_ == "FIRST_VALUE" || fn_ == "ANY_VALUE") {
      return has_first_ ? first_val_ : Value();
    }
    if (fn_ == "LAST_VALUE") {
      return has_last_ ? last_val_ : Value();
    }
    if (fn_ == "NTH_VALUE") {
      return has_nth_ ? nth_val_ : Value();
    }
    if (non_null_count_ == 0) {
      return {};
    }
    if (fn_ == "LOGICAL_AND") {
      return Value(static_cast<int64_t>(logical_falses_ == 0 ? 1 : 0));
    }
    if (fn_ == "LOGICAL_OR") {
      return Value(static_cast<int64_t>(logical_trues_ > 0 ? 1 : 0));
    }
    if (fn_ == "SUM") {
      if (sum_is_uint64_ && !any_double_) {
        return Value(static_cast<int64_t>(uint_sum_)).WithUnsigned();
      }
      if (any_double_) {
        // Doubles kept their own running total; the exact integer part
        // (signed or uint64) folds in once, as in AggregateAccumulator.
        return Value(double_sum_ +
                     (sum_is_uint64_ ? static_cast<double>(uint_sum_) : 0.0));
      }
      return Value(int_sum_);
    }
    if (fn_ == "AVG") {
      const double sum =
          any_double_ ? double_sum_ : static_cast<double>(int_sum_);
      return Value(sum / static_cast<double>(non_null_count_));
    }
    if (fn_ == "MIN" || fn_ == "MAX") {
      return has_best_ ? best_val_ : Value();
    }
    if (fn_ == "BIT_AND" || fn_ == "BIT_OR") {
      uint64_t acc = 0;
      for (size_t b = 0; b < bit_ones_.size(); ++b) {
        const bool set = fn_ == "BIT_AND" ? bit_ones_[b] == non_null_count_
                                          : bit_ones_[b] > 0;
        acc |= static_cast<uint64_t>(set ? 1 : 0) << b;
      }
      return Value(static_cast<int64_t>(acc));
    }
    if (fn_ == "BIT_XOR") {
      return non_null_count_ > 0 ? Value(bit_acc_) : Value();
    }
    return {};
  }

  [[nodiscard]] bool SawDouble() const noexcept { return any_double_; }

  // True once further AddRow calls cannot change Result(): positional
  // accumulators can stop scanning the frame early.
  [[nodiscard]] bool Done() const noexcept {
    if (fn_ == "FIRST_VALUE" || fn_ == "ANY_VALUE") {
      return has_first_;
    }
    if (fn_ == "NTH_VALUE") {
      return has_nth_;
    }
    return false;
  }

  [[nodiscard]] size_t NthTarget() const noexcept { return nth_target_; }

 private:
  const WindowFunctionCallExpression& window_;
  const Schema& input_schema_;
  TransactionContext* txn_;
  const std::string& fn_;

  int64_t surviving_rows_{0};
  int64_t non_null_count_{0};
  int64_t trues_{0};
  int64_t int_sum_{0};
  uint64_t uint_sum_{0};
  bool sum_is_uint64_{false};
  double double_sum_{0.0};
  int64_t bit_acc_{0};
  size_t nth_target_{1};
  size_t nth_seen_{0};
  Value best_val_;
  Value first_val_;
  Value last_val_;
  Value nth_val_;
  int64_t logical_trues_{0};
  int64_t logical_falses_{0};
  std::array<int32_t, 64> bit_ones_{};
  bool count_star_{false};
  bool any_double_{false};
  bool has_best_{false};
  bool best_is_nan_{false};
  bool has_first_{false};
  bool has_last_{false};
  bool has_nth_{false};
};

// O(1)-amortized sliding MIN/MAX for ROWS/RANGE frames: a monotone deque of
// evaluated argument values (front = current best) plus a FIFO of NaN
// positions, since a NaN poisons the result only while inside the frame.
// Each frame row's argument is evaluated exactly once, on load.
class SlidingMinMax {
 public:
  SlidingMinMax(const WindowFunctionCallExpression& window,
                const Schema& input_schema, TransactionContext* txn)
      : window_(window),
        input_schema_(input_schema),
        txn_(txn),
        is_min_(window.function == "MIN") {}

  // Feeds the frame row at absolute index `pos`, mirroring
  // StreamingWindowAggState::AddRow's filter / NULL / NaN handling.
  Status Push(uint64_t pos, const Row& row) {
    if (window_.where_filter != nullptr) {
      ASSIGN_OR_RETURN(
          Value, wf,
          EvalWindowExpr(window_.where_filter, row, input_schema_, txn_));
      if (!relational_detail::Truthy(wf)) {
        return Status::kSuccess;
      }
    }
    if (window_.args.empty()) {
      return Status::kSuccess;
    }
    ASSIGN_OR_RETURN(Value, v,
                     EvalWindowExpr(window_.args[0], row, input_schema_, txn_));
    if (v.IsNull()) {
      return Status::kSuccess;
    }
    if (v.type == ValueType::kDouble && std::isnan(v.value.double_value)) {
      nans_.push_back(Entry{.pos = pos, .v = std::move(v)});
      return Status::kSuccess;
    }
    while (!q_.empty()) {
      ASSIGN_OR_RETURN(
          bool, better,
          is_min_ ? v.TryLess(q_.back().v) : q_.back().v.TryLess(v));
      if (!better) {
        break;
      }
      q_.pop_back();
    }
    q_.push_back(Entry{.pos = pos, .v = std::move(v)});
    return Status::kSuccess;
  }

  void ExpireBefore(uint64_t lo) noexcept {
    while (!q_.empty() && q_.front().pos < lo) {
      q_.pop_front();
    }
    while (!nans_.empty() && nans_.front().pos < lo) {
      nans_.pop_front();
    }
  }

  // Matches StreamingWindowAggState::Result() for MIN/MAX: the FIRST NaN in
  // the frame wins (a later NaN never replaces an existing best NaN), then
  // the best value, then NULL for an all-NULL/empty frame.
  [[nodiscard]] Value Result() const {
    if (!nans_.empty()) {
      return nans_.front().v;
    }
    return q_.empty() ? Value() : q_.front().v;
  }

 private:
  struct Entry {
    uint64_t pos;
    Value v;
  };

  const WindowFunctionCallExpression& window_;
  const Schema& input_schema_;
  TransactionContext* txn_;
  bool is_min_;
  std::deque<Entry> q_;
  std::deque<Entry> nans_;
};

// Streaming evaluation over ONE single spilled, already-sorted partition
// segment `seg_id` of `total_rows` rows. Evaluates one
// `WindowFunctionCallExpression` in O(B) RAM without ever materializing the
// partition in memory.
class SkewCallStream {
 public:
  SkewCallStream(const SpillStore* store, SpillSegmentId seg_id,
                 uint64_t total_rows, const WindowFunctionCallExpression& call,
                 const Schema& input_schema,
                 std::span<const slot_t> order_slots, TransactionContext* txn,
                 size_t budget_bytes, QueryMemoryGrant* grant = nullptr)
      : store_(store),
        seg_id_(seg_id),
        total_rows_(total_rows),
        call_(call),
        input_schema_(input_schema),
        order_slots_(order_slots),
        txn_(txn),
        budget_bytes_(std::max<size_t>(kMinWindowMemBudget, budget_bytes)),
        grant_(grant),
        cur_reader_(store_, seg_id_,
                    std::max<size_t>(4096, budget_bytes_ / 4)) {}

  Status Init() {
    RETURN_IF_FAIL(cur_reader_.Open());
    if (grant_ != nullptr) {
      const size_t refill = std::min<size_t>(
          QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes,
          std::max<size_t>(65536, budget_bytes_ / 4));
      deque_grant_cache_.emplace(grant_, refill);
    }
    if (total_rows_ == 0) {
      mode_ = Mode::kConstantValue;
      const_val_ = Value();
      return Status::kSuccess;
    }
    const std::string& fn = call_.function;

    if (fn == "PERCENTILE_CONT" || fn == "PERCENTILE_DISC") {
      return StatusError(
          StatusCode::kMemoryLimitExceeded,
          "Skewed single partition overflowed operator memory budget for "
          "non-streaming window function: " +
              call_.function);
    }

    if (fn == "ROW_NUMBER") {
      mode_ = Mode::kRowNumber;
      return Status::kSuccess;
    }
    if (fn == "RANK" || fn == "DENSE_RANK" || fn == "PERCENT_RANK") {
      mode_ = Mode::kRankLike;
      return Status::kSuccess;
    }
    if (fn == "CUME_DIST") {
      if (order_slots_.empty()) {
        mode_ = Mode::kConstantValue;
        const_val_ = Value(1.0);
        return Status::kSuccess;
      }
      mode_ = Mode::kCumeDist;
      peer_reader_ = std::make_unique<SegmentRowReader>(
          store_, seg_id_, std::max<size_t>(4096, budget_bytes_ / 4));
      return peer_reader_->Open();
    }
    if (fn == "NTILE") {
      if (call_.args.empty()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "NTILE requires an argument");
      }
      Row first_row;
      ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(&first_row));
      if (!got) {
        return Status::kSuccess;
      }
      ASSIGN_OR_RETURN(
          Value, bv,
          EvalWindowExpr(call_.args[0], first_row, input_schema_, txn_));
      if (bv.IsNull()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "The N value (number of buckets) for the NTILE "
                           "function must not be NULL");
      }
      if (bv.type != ValueType::kInt64) {
        return StatusError(StatusCode::kInvalidArgument,
                           "NTILE requires an integer bucket count");
      }
      const int64_t buckets = bv.value.int_value;
      if (buckets <= 0) {
        return StatusError(StatusCode::kInvalidArgument,
                           "The N value (number of buckets) for the NTILE "
                           "function must be positive");
      }
      ntile_base_ = total_rows_ / static_cast<uint64_t>(buckets);
      ntile_extra_ = total_rows_ % static_cast<uint64_t>(buckets);
      ntile_bucket_ = 0;
      ntile_left_in_bucket_ = 0;
      buffered_first_row_ = std::move(first_row);
      mode_ = Mode::kNtile;
      return Status::kSuccess;
    }
    if (fn == "LAG" || fn == "LEAD") {
      if (call_.args.empty()) {
        return StatusError(StatusCode::kInvalidArgument,
                           fn + " requires at least one argument");
      }
      if (call_.args.size() > 1 && !IsStaticScalarExpr(call_.args[1])) {
        lag_lead_dynamic_ = true;
        indexed_reader_ = std::make_unique<SegmentIndexedReader>(
            store_, seg_id_, std::max<size_t>(4096, budget_bytes_ / 4));
        RETURN_IF_FAIL(indexed_reader_->Open());
        mode_ = (fn == "LAG") ? Mode::kLag : Mode::kLead;
        return Status::kSuccess;
      }
      int64_t off = 1;
      if (call_.args.size() > 1) {
        ASSIGN_OR_RETURN(
            int64_t, parsed_off,
            ParseConstantIntOffset(
                call_.args[1], txn_,
                fn == "LAG"
                    ? "The offset to the function LAG must not be null"
                    : "The offset to the function LEAD must not be null",
                fn == "LAG"
                    ? "The offset to the function LAG must be an integer"
                    : "The offset to the function LEAD must be an integer",
                fn == "LAG"
                    ? "The offset to the function LAG must not be negative"
                    : "The offset to the function LEAD must not be negative"));
        off = parsed_off;
      }
      lag_lead_offset_ = static_cast<uint64_t>(off);
      if (fn == "LAG") {
        mode_ = Mode::kLag;
        if (lag_lead_offset_ > 0 && lag_lead_offset_ < total_rows_) {
          peer_reader_ = std::make_unique<SegmentRowReader>(
              store_, seg_id_, std::max<size_t>(4096, budget_bytes_ / 4));
          RETURN_IF_FAIL(peer_reader_->Open());
        }
      } else {
        mode_ = Mode::kLead;
        if (lag_lead_offset_ < total_rows_) {
          peer_reader_ = std::make_unique<SegmentRowReader>(
              store_, seg_id_, std::max<size_t>(4096, budget_bytes_ / 4));
          RETURN_IF_FAIL(peer_reader_->Open());
          Row discard;
          for (uint64_t i = 0; i < lag_lead_offset_; ++i) {
            ASSIGN_OR_RETURN(bool, ok, peer_reader_->Next(&discard));
            if (!ok) {
              break;
            }
          }
        }
      }
      return Status::kSuccess;
    }

    // Aggregates / Value functions over frames.
    const bool is_whole_partition =
        (!call_.has_frame && call_.order_by.empty()) ||
        (call_.has_frame &&
         call_.frame_start.type == WindowFrameBoundType::kUnboundedPreceding &&
         call_.frame_end.type == WindowFrameBoundType::kUnboundedFollowing);

    const bool dynamic_nth = IsDynamicNthCall(call_);

    if (is_whole_partition && call_.exclusion == WindowFrameExclusion::kNone &&
        !call_.distinct && StreamingWindowAggState::SupportsStreaming(call_) &&
        !dynamic_nth) {
      StreamingWindowAggState agg(call_, input_schema_, txn_);
      RETURN_IF_FAIL(agg.InitNthConstant());
      SegmentRowReader scan(store_, seg_id_,
                            std::max<size_t>(4096, budget_bytes_ / 4));
      RETURN_IF_FAIL(scan.Open());
      Row r;
      while (true) {
        ASSIGN_OR_RETURN(bool, got, scan.Next(&r));
        if (!got) {
          break;
        }
        RETURN_IF_FAIL(agg.AddRow(r));
      }
      const_val_ = agg.Result();
      mode_ = Mode::kConstantValue;
      return Status::kSuccess;
    }

    // Standard single-pass or monotonic prefix frames:
    //   1. Default with ORDER BY: RANGE UNBOUNDED PRECEDING .. CURRENT ROW
    //   2. ROWS UNBOUNDED PRECEDING .. CURRENT ROW / k PRECEDING / k FOLLOWING
    //   3. RANGE UNBOUNDED PRECEDING .. CURRENT ROW
    const bool is_default_range_prefix =
        !call_.has_frame && !call_.order_by.empty();
    const bool is_explicit_unbounded_start =
        call_.has_frame &&
        call_.frame_start.type == WindowFrameBoundType::kUnboundedPreceding;
    if ((is_default_range_prefix || is_explicit_unbounded_start) &&
        call_.exclusion == WindowFrameExclusion::kNone && !call_.distinct &&
        StreamingWindowAggState::SupportsStreaming(call_) && !dynamic_nth) {
      prefix_agg_ =
          std::make_unique<StreamingWindowAggState>(call_, input_schema_, txn_);
      RETURN_IF_FAIL(prefix_agg_->InitNthConstant());

      if (is_default_range_prefix ||
          ((call_.frame_unit == WindowFrameUnit::kRange ||
            call_.frame_unit == WindowFrameUnit::kGroups) &&
           call_.frame_end.type == WindowFrameBoundType::kCurrentRow)) {
        if (order_slots_.empty()) {
          // Every row in the partition is a peer -> same as whole partition.
          SegmentRowReader scan(store_, seg_id_,
                                std::max<size_t>(4096, budget_bytes_ / 4));
          RETURN_IF_FAIL(scan.Open());
          Row r;
          while (true) {
            ASSIGN_OR_RETURN(bool, got, scan.Next(&r));
            if (!got) {
              break;
            }
            RETURN_IF_FAIL(prefix_agg_->AddRow(r));
          }
          const_val_ = prefix_agg_->Result();
          mode_ = Mode::kConstantValue;
          return Status::kSuccess;
        }
        peer_reader_ = std::make_unique<SegmentRowReader>(
            store_, seg_id_, std::max<size_t>(4096, budget_bytes_ / 4));
        RETURN_IF_FAIL(peer_reader_->Open());
        mode_ = Mode::kPrefixRangePeer;
        return Status::kSuccess;
      }

      if (call_.frame_unit == WindowFrameUnit::kRows) {
        if (call_.frame_end.type == WindowFrameBoundType::kCurrentRow) {
          mode_ = Mode::kPrefixRowsCurrent;
          return Status::kSuccess;
        }
        if (call_.frame_end.type == WindowFrameBoundType::kOffsetPreceding) {
          ASSIGN_OR_RETURN(
              int64_t, off,
              ParseConstantIntOffset(call_.frame_end.offset, txn_,
                                     "Window frame offset for PRECEDING or "
                                     "FOLLOWING cannot be NULL",
                                     "ROWS offset must be a constant integer",
                                     "Window frame offset for PRECEDING or "
                                     "FOLLOWING must be non-negative"));
          lag_lead_offset_ = static_cast<uint64_t>(off);
          peer_reader_ = std::make_unique<SegmentRowReader>(
              store_, seg_id_, std::max<size_t>(4096, budget_bytes_ / 4));
          RETURN_IF_FAIL(peer_reader_->Open());
          mode_ = Mode::kPrefixRowsPreceding;
          return Status::kSuccess;
        }
        if (call_.frame_end.type == WindowFrameBoundType::kOffsetFollowing) {
          ASSIGN_OR_RETURN(
              int64_t, off,
              ParseConstantIntOffset(call_.frame_end.offset, txn_,
                                     "Window frame offset for PRECEDING or "
                                     "FOLLOWING cannot be NULL",
                                     "ROWS offset must be a constant integer",
                                     "Window frame offset for PRECEDING or "
                                     "FOLLOWING must be non-negative"));
          lag_lead_offset_ = static_cast<uint64_t>(off);
          peer_reader_ = std::make_unique<SegmentRowReader>(
              store_, seg_id_, std::max<size_t>(4096, budget_bytes_ / 4));
          RETURN_IF_FAIL(peer_reader_->Open());
          // Pre-advance `off` rows into prefix_agg_ (rows 0..min(total_rows_-1,
          // off)).
          Row ahead;
          for (uint64_t i = 0; i < lag_lead_offset_ && i < total_rows_; ++i) {
            ASSIGN_OR_RETURN(bool, got, peer_reader_->Next(&ahead));
            if (!got) {
              break;
            }
            RETURN_IF_FAIL(prefix_agg_->AddRow(ahead));
            ++hi_pos_;
          }
          mode_ = Mode::kPrefixRowsFollowing;
          return Status::kSuccess;
        }
      }
    }

    // Invertible suffix `ROWS ... CURRENT ROW / k PRECEDING / k FOLLOWING ..
    // UNBOUNDED FOLLOWING`:
    if (call_.has_frame &&
        call_.frame_end.type == WindowFrameBoundType::kUnboundedFollowing &&
        call_.frame_unit == WindowFrameUnit::kRows &&
        call_.exclusion == WindowFrameExclusion::kNone &&
        StreamingWindowAggState::SupportsInvertibleSubtract(call_)) {
      int64_t start_delta = 0;  // lo = clamp(k + start_delta)
      bool clamped_empty_on_huge = false;
      if (call_.frame_start.type == WindowFrameBoundType::kOffsetPreceding) {
        ASSIGN_OR_RETURN(
            int64_t, off,
            ParseConstantIntOffset(
                call_.frame_start.offset, txn_,
                "Window frame offset for PRECEDING or FOLLOWING cannot be NULL",
                "ROWS offset must be a constant integer",
                "Window frame offset for PRECEDING or FOLLOWING must be "
                "non-negative"));
        start_delta = -off;
      } else if (call_.frame_start.type ==
                 WindowFrameBoundType::kOffsetFollowing) {
        ASSIGN_OR_RETURN(
            int64_t, off,
            ParseConstantIntOffset(
                call_.frame_start.offset, txn_,
                "Window frame offset for PRECEDING or FOLLOWING cannot be NULL",
                "ROWS offset must be a constant integer",
                "Window frame offset for PRECEDING or FOLLOWING must be "
                "non-negative"));
        start_delta = off;
        clamped_empty_on_huge = true;
      }
      prefix_agg_ =
          std::make_unique<StreamingWindowAggState>(call_, input_schema_, txn_);
      SegmentRowReader scan(store_, seg_id_,
                            std::max<size_t>(4096, budget_bytes_ / 4));
      RETURN_IF_FAIL(scan.Open());
      Row r;
      while (true) {
        ASSIGN_OR_RETURN(bool, got, scan.Next(&r));
        if (!got) {
          break;
        }
        RETURN_IF_FAIL(prefix_agg_->AddRow(r));
      }
      peer_reader_ = std::make_unique<SegmentRowReader>(
          store_, seg_id_, std::max<size_t>(4096, budget_bytes_ / 4));
      RETURN_IF_FAIL(peer_reader_->Open());
      if (clamped_empty_on_huge && start_delta > 0) {
        // Defer first removal until k=0 -> start_delta handled cleanly in
        // Next().
      }
      sliding_start_delta_ = start_delta;
      mode_ = Mode::kSuffixInvertibleRows;
      return Status::kSuccess;
    }

    auto parse_rows_delta = [&](const WindowFrameBound& b,
                                bool is_start) -> StatusOr<int64_t> {
      switch (b.type) {
        case WindowFrameBoundType::kCurrentRow:
          return int64_t{0};
        case WindowFrameBoundType::kOffsetPreceding: {
          ASSIGN_OR_RETURN(
              int64_t, off,
              ParseConstantIntOffset(b.offset, txn_,
                                     "Window frame offset for PRECEDING or "
                                     "FOLLOWING cannot be NULL",
                                     "ROWS offset must be a constant integer",
                                     "Window frame offset for PRECEDING or "
                                     "FOLLOWING must be non-negative"));
          return -off;
        }
        case WindowFrameBoundType::kOffsetFollowing: {
          ASSIGN_OR_RETURN(
              int64_t, off,
              ParseConstantIntOffset(b.offset, txn_,
                                     "Window frame offset for PRECEDING or "
                                     "FOLLOWING cannot be NULL",
                                     "ROWS offset must be a constant integer",
                                     "Window frame offset for PRECEDING or "
                                     "FOLLOWING must be non-negative"));
          return off;
        }
        case WindowFrameBoundType::kUnboundedPreceding:
          return is_start ? std::numeric_limits<int64_t>::min() / 2 : 0;
        case WindowFrameBoundType::kUnboundedFollowing:
          return is_start ? 0 : std::numeric_limits<int64_t>::max() / 2;
      }
      return int64_t{0};
    };

    // Dynamic `NTH_VALUE(expr, dynamic_n)` over whole / unbounded ROWS frames:
    if (dynamic_nth &&
        (!call_.has_frame || call_.frame_unit == WindowFrameUnit::kRows)) {
      if (is_whole_partition) {
        sliding_start_delta_ = std::numeric_limits<int64_t>::min() / 2;
        sliding_end_delta_ = std::numeric_limits<int64_t>::max() / 2;
      } else {
        if (call_.frame_start.type ==
                WindowFrameBoundType::kUnboundedFollowing ||
            call_.frame_end.type == WindowFrameBoundType::kUnboundedPreceding) {
          mode_ = Mode::kConstantValue;
          const_val_ = Value();
          return Status::kSuccess;
        }
        ASSIGN_OR_RETURN(int64_t, sd,
                         parse_rows_delta(call_.frame_start, true));
        sliding_start_delta_ = sd;
        ASSIGN_OR_RETURN(int64_t, ed, parse_rows_delta(call_.frame_end, false));
        sliding_end_delta_ = ed;
      }
      indexed_reader_ = std::make_unique<SegmentIndexedReader>(
          store_, seg_id_, std::max<size_t>(4096, budget_bytes_ / 4));
      RETURN_IF_FAIL(indexed_reader_->Open());
      dynamic_nth_use_range_ = false;
      mode_ = Mode::kDynamicNthRange;
      return Status::kSuccess;
    }

    // Finite sliding ROWS frame `[k + start_delta, k + end_delta]`:
    if (call_.has_frame && call_.frame_unit == WindowFrameUnit::kRows &&
        call_.frame_start.type != WindowFrameBoundType::kUnboundedFollowing &&
        call_.frame_end.type != WindowFrameBoundType::kUnboundedPreceding) {
      ASSIGN_OR_RETURN(int64_t, start_delta_val,
                       parse_rows_delta(call_.frame_start, true));
      sliding_start_delta_ = start_delta_val;
      ASSIGN_OR_RETURN(int64_t, end_delta_val,
                       parse_rows_delta(call_.frame_end, false));
      sliding_end_delta_ = end_delta_val;

      // Prefer O(1) two-cursor invertible mode whenever valid & clean!
      if (call_.exclusion == WindowFrameExclusion::kNone && !call_.distinct &&
          StreamingWindowAggState::SupportsInvertibleSubtract(call_)) {
        prefix_agg_ = std::make_unique<StreamingWindowAggState>(
            call_, input_schema_, txn_);
        peer_reader_ = std::make_unique<SegmentRowReader>(
            store_, seg_id_, std::max<size_t>(4096, budget_bytes_ / 4));
        RETURN_IF_FAIL(peer_reader_->Open());
        lo_reader_ = std::make_unique<SegmentRowReader>(
            store_, seg_id_, std::max<size_t>(4096, budget_bytes_ / 4));
        RETURN_IF_FAIL(lo_reader_->Open());
        hi_pos_ = 0;
        lo_pos_ = 0;
        mode_ = Mode::kSlidingInvertibleRows;
        return Status::kSuccess;
      }

      // Bounded deque sliding window capped at max(16 KiB, `budget_bytes_ /
      // 2`):
      peer_reader_ = std::make_unique<SegmentRowReader>(
          store_, seg_id_, std::max<size_t>(4096, budget_bytes_ / 4));
      RETURN_IF_FAIL(peer_reader_->Open());
      deque_lo_ = 0;
      deque_hi_loaded_ = 0;
      mode_ = Mode::kSlidingDequeRows;
      return Status::kSuccess;
    }

    // Streaming RANGE frames (`RANGE BETWEEN ... AND ...`), including numeric
    // & DATE/INTERVAL offset frames over single ORDER BY key and peer-only
    // RANGE frames (`CURRENT ROW .. CURRENT ROW / UNBOUNDED FOLLOWING`):
    if (!call_.has_frame || call_.frame_unit == WindowFrameUnit::kRange) {
      const WindowFrameBound start_b =
          call_.has_frame
              ? call_.frame_start
              : WindowFrameBound{
                    .type = WindowFrameBoundType::kUnboundedPreceding,
                    .offset = nullptr};
      const WindowFrameBound end_b =
          call_.has_frame
              ? call_.frame_end
              : (call_.order_by.empty()
                     ? WindowFrameBound{.type = WindowFrameBoundType::
                                            kUnboundedFollowing,
                                        .offset = nullptr}
                     : WindowFrameBound{
                           .type = WindowFrameBoundType::kCurrentRow,
                           .offset = nullptr});
      const bool has_offset =
          start_b.type == WindowFrameBoundType::kOffsetPreceding ||
          start_b.type == WindowFrameBoundType::kOffsetFollowing ||
          end_b.type == WindowFrameBoundType::kOffsetPreceding ||
          end_b.type == WindowFrameBoundType::kOffsetFollowing;
      if (has_offset && order_slots_.size() != 1) {
        return StatusError(
            StatusCode::kInvalidArgument,
            "RANGE with offset requires a single ORDER BY expression");
      }
      ASSIGN_OR_RETURN(RangeBoundSpec, sspec,
                       ParseRangeBoundSpec(start_b, txn_));
      ASSIGN_OR_RETURN(RangeBoundSpec, espec, ParseRangeBoundSpec(end_b, txn_));
      const bool asc =
          call_.order_by.empty() ? true : call_.order_by[0].ascending;
      range_scanner_ = std::make_unique<RangeFrameScanner>(
          store_, seg_id_, total_rows_, order_slots_, asc, sspec, espec,
          std::max<size_t>(4096, budget_bytes_ / 4));
      RETURN_IF_FAIL(range_scanner_->Open());

      if (dynamic_nth) {
        indexed_reader_ = std::make_unique<SegmentIndexedReader>(
            store_, seg_id_, std::max<size_t>(4096, budget_bytes_ / 4));
        RETURN_IF_FAIL(indexed_reader_->Open());
        dynamic_nth_use_range_ = true;
        mode_ = Mode::kDynamicNthRange;
        return Status::kSuccess;
      }

      if (call_.exclusion == WindowFrameExclusion::kNone && !call_.distinct &&
          StreamingWindowAggState::SupportsInvertibleSubtract(call_)) {
        prefix_agg_ = std::make_unique<StreamingWindowAggState>(
            call_, input_schema_, txn_);
        peer_reader_ = std::make_unique<SegmentRowReader>(
            store_, seg_id_, std::max<size_t>(4096, budget_bytes_ / 4));
        RETURN_IF_FAIL(peer_reader_->Open());
        lo_reader_ = std::make_unique<SegmentRowReader>(
            store_, seg_id_, std::max<size_t>(4096, budget_bytes_ / 4));
        RETURN_IF_FAIL(lo_reader_->Open());
        hi_pos_ = 0;
        lo_pos_ = 0;
        mode_ = Mode::kSlidingInvertibleRange;
        return Status::kSuccess;
      }

      if (StreamingWindowAggState::SupportsStreaming(call_) ||
          call_.frame_start.type != WindowFrameBoundType::kUnboundedPreceding ||
          call_.frame_end.type != WindowFrameBoundType::kUnboundedFollowing) {
        peer_reader_ = std::make_unique<SegmentRowReader>(
            store_, seg_id_, std::max<size_t>(4096, budget_bytes_ / 4));
        RETURN_IF_FAIL(peer_reader_->Open());
        deque_lo_ = 0;
        deque_hi_loaded_ = 0;
        mode_ = Mode::kSlidingDequeRange;
        return Status::kSuccess;
      }
    }

    if (call_.has_frame && call_.frame_unit == WindowFrameUnit::kGroups) {
      return StatusError(StatusCode::kNotImplemented,
                         "Unsupported streaming GROUPS frame on skewed "
                         "partition for: " +
                             call_.function);
    }

    // Small fallback check: non-streaming functions (e.g., ARRAY_AGG,
    // APPROX_*, STRING_AGG, COUNT(DISTINCT ...)) whose partition overflowed
    // `budget_bytes_` report clean OOM (`kMemoryLimitExceeded`).
    return StatusError(
        StatusCode::kMemoryLimitExceeded,
        "Skewed single partition overflowed operator memory budget for "
        "non-streaming window function: " +
            call_.function);
  }

  StatusOr<bool> Next(Row* row_out, Value* val_out) {
    if (k_ >= total_rows_) {
      return false;
    }

    switch (mode_) {
      case Mode::kConstantValue: {
        ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(row_out));
        if (!got) {
          return false;
        }
        *val_out = const_val_;
        ++k_;
        return true;
      }
      case Mode::kRowNumber: {
        ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(row_out));
        if (!got) {
          return false;
        }
        *val_out = Value(static_cast<int64_t>(++k_));
        return true;
      }
      case Mode::kRankLike: {
        ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(row_out));
        if (!got) {
          return false;
        }
        if (order_slots_.empty()) {
          peer_rank_ = 1;
          dense_rank_ = 1;
        } else {
          std::vector<Value> keys = ExtractKeys(*row_out, order_slots_);
          if (!has_prev_keys_ || !SamePartitionTuple(prev_keys_, keys)) {
            peer_rank_ = k_ + 1;
            ++dense_rank_;
            prev_keys_ = std::move(keys);
            has_prev_keys_ = true;
          }
        }
        if (call_.function == "RANK") {
          *val_out = Value(static_cast<int64_t>(peer_rank_));
        } else if (call_.function == "DENSE_RANK") {
          *val_out = Value(dense_rank_);
        } else {
          const double denom =
              total_rows_ > 1 ? static_cast<double>(total_rows_ - 1) : 1.0;
          *val_out = Value((static_cast<double>(peer_rank_) - 1.0) / denom);
        }
        ++k_;
        return true;
      }
      case Mode::kCumeDist: {
        ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(row_out));
        if (!got) {
          return false;
        }
        if (k_ >= cume_peer_end_rows_read_) {
          // Advance peer_reader_ through the next peer group starting at k_.
          if (!peer_lookahead_valid_) {
            ASSIGN_OR_RETURN(bool, pok,
                             peer_reader_->Next(&peer_lookahead_row_));
            if (pok) {
              peer_lookahead_keys_ =
                  ExtractKeys(peer_lookahead_row_, order_slots_);
              peer_lookahead_valid_ = true;
            }
          }
          uint64_t count_in_group = 0;
          if (peer_lookahead_valid_) {
            const std::vector<Value> group_keys = peer_lookahead_keys_;
            count_in_group = 1;
            peer_lookahead_valid_ = false;
            while (true) {
              Row nxt;
              ASSIGN_OR_RETURN(bool, pok, peer_reader_->Next(&nxt));
              if (!pok) {
                break;
              }
              std::vector<Value> nxt_keys = ExtractKeys(nxt, order_slots_);
              if (SamePartitionTuple(group_keys, nxt_keys)) {
                ++count_in_group;
              } else {
                peer_lookahead_row_ = std::move(nxt);
                peer_lookahead_keys_ = std::move(nxt_keys);
                peer_lookahead_valid_ = true;
                break;
              }
            }
          }
          cume_peer_end_rows_read_ = k_ + count_in_group;
        }
        *val_out = Value(static_cast<double>(cume_peer_end_rows_read_) /
                         static_cast<double>(total_rows_));
        ++k_;
        return true;
      }
      case Mode::kNtile: {
        if (k_ == 0 && buffered_first_row_.has_value()) {
          *row_out = std::move(*buffered_first_row_);
          buffered_first_row_.reset();
        } else {
          ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(row_out));
          if (!got) {
            return false;
          }
        }
        while (ntile_left_in_bucket_ == 0) {
          ntile_left_in_bucket_ =
              ntile_base_ +
              (std::cmp_less(ntile_bucket_, ntile_extra_) ? 1 : 0);
          ++ntile_bucket_;
        }
        --ntile_left_in_bucket_;
        *val_out = Value(ntile_bucket_);
        ++k_;
        return true;
      }
      case Mode::kLag: {
        ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(row_out));
        if (!got) {
          return false;
        }
        if (lag_lead_dynamic_) {
          ASSIGN_OR_RETURN(
              Value, ov,
              EvalWindowExpr(call_.args[1], *row_out, input_schema_, txn_));
          ASSIGN_OR_RETURN(
              int64_t, off_signed,
              ValidateIntOffsetValue(
                  ov, "The offset to the function LAG must not be null",
                  "The offset to the function LAG must be an integer",
                  "The offset to the function LAG must not be negative"));
          const auto off = static_cast<uint64_t>(off_signed);
          if (off == 0) {
            ASSIGN_OR_RETURN(
                Value, v,
                EvalWindowExpr(call_.args[0], *row_out, input_schema_, txn_));
            *val_out = std::move(v);
          } else if (off > k_) {
            if (call_.args.size() > 2) {
              ASSIGN_OR_RETURN(
                  Value, defv,
                  EvalWindowExpr(call_.args[2], *row_out, input_schema_, txn_));
              *val_out = std::move(defv);
            } else {
              *val_out = Value();
            }
          } else {
            Row src;
            RETURN_IF_FAIL(indexed_reader_->ReadAt(k_ - off, &src));
            ASSIGN_OR_RETURN(
                Value, v,
                EvalWindowExpr(call_.args[0], src, input_schema_, txn_));
            *val_out = std::move(v);
          }
          ++k_;
          return true;
        }
        if (k_ < lag_lead_offset_) {
          if (call_.args.size() > 2) {
            ASSIGN_OR_RETURN(
                Value, defv,
                EvalWindowExpr(call_.args[2], *row_out, input_schema_, txn_));
            *val_out = std::move(defv);
          } else {
            *val_out = Value();
          }
        } else if (lag_lead_offset_ == 0) {
          ASSIGN_OR_RETURN(
              Value, v,
              EvalWindowExpr(call_.args[0], *row_out, input_schema_, txn_));
          *val_out = std::move(v);
        } else {
          Row src;
          ASSIGN_OR_RETURN(bool, lok, peer_reader_->Next(&src));
          if (!lok) {
            *val_out = Value();
          } else {
            ASSIGN_OR_RETURN(
                Value, v,
                EvalWindowExpr(call_.args[0], src, input_schema_, txn_));
            *val_out = std::move(v);
          }
        }
        ++k_;
        return true;
      }
      case Mode::kLead: {
        ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(row_out));
        if (!got) {
          return false;
        }
        if (lag_lead_dynamic_) {
          ASSIGN_OR_RETURN(
              Value, ov,
              EvalWindowExpr(call_.args[1], *row_out, input_schema_, txn_));
          ASSIGN_OR_RETURN(
              int64_t, off_signed,
              ValidateIntOffsetValue(
                  ov, "The offset to the function LEAD must not be null",
                  "The offset to the function LEAD must be an integer",
                  "The offset to the function LEAD must not be negative"));
          const auto off = static_cast<uint64_t>(off_signed);
          if (off == 0) {
            ASSIGN_OR_RETURN(
                Value, v,
                EvalWindowExpr(call_.args[0], *row_out, input_schema_, txn_));
            *val_out = std::move(v);
          } else if (off >= total_rows_ || k_ >= total_rows_ - off) {
            if (call_.args.size() > 2) {
              ASSIGN_OR_RETURN(
                  Value, defv,
                  EvalWindowExpr(call_.args[2], *row_out, input_schema_, txn_));
              *val_out = std::move(defv);
            } else {
              *val_out = Value();
            }
          } else {
            Row dst;
            RETURN_IF_FAIL(indexed_reader_->ReadAt(k_ + off, &dst));
            ASSIGN_OR_RETURN(
                Value, v,
                EvalWindowExpr(call_.args[0], dst, input_schema_, txn_));
            *val_out = std::move(v);
          }
          ++k_;
          return true;
        }
        if (k_ + lag_lead_offset_ >= total_rows_) {
          if (call_.args.size() > 2) {
            ASSIGN_OR_RETURN(
                Value, defv,
                EvalWindowExpr(call_.args[2], *row_out, input_schema_, txn_));
            *val_out = std::move(defv);
          } else {
            *val_out = Value();
          }
        } else if (lag_lead_offset_ == 0) {
          ASSIGN_OR_RETURN(
              Value, v,
              EvalWindowExpr(call_.args[0], *row_out, input_schema_, txn_));
          *val_out = std::move(v);
        } else {
          Row dst;
          ASSIGN_OR_RETURN(bool, dok, peer_reader_->Next(&dst));
          if (!dok) {
            *val_out = Value();
          } else {
            ASSIGN_OR_RETURN(
                Value, v,
                EvalWindowExpr(call_.args[0], dst, input_schema_, txn_));
            *val_out = std::move(v);
          }
        }
        ++k_;
        return true;
      }
      case Mode::kPrefixRowsCurrent: {
        ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(row_out));
        if (!got) {
          return false;
        }
        RETURN_IF_FAIL(prefix_agg_->AddRow(*row_out));
        *val_out = prefix_agg_->Result();
        ++k_;
        return true;
      }
      case Mode::kPrefixRowsPreceding: {
        ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(row_out));
        if (!got) {
          return false;
        }
        if (k_ < lag_lead_offset_) {
          *val_out = (call_.function == "COUNT" || call_.function == "COUNTIF")
                         ? Value(int64_t{0})
                         : Value();
        } else {
          Row r;
          ASSIGN_OR_RETURN(bool, rok, peer_reader_->Next(&r));
          if (rok) {
            RETURN_IF_FAIL(prefix_agg_->AddRow(r));
          }
          *val_out = prefix_agg_->Result();
        }
        ++k_;
        return true;
      }
      case Mode::kPrefixRowsFollowing: {
        ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(row_out));
        if (!got) {
          return false;
        }
        if (hi_pos_ < total_rows_) {
          Row r;
          ASSIGN_OR_RETURN(bool, rok, peer_reader_->Next(&r));
          if (rok) {
            RETURN_IF_FAIL(prefix_agg_->AddRow(r));
            ++hi_pos_;
          }
        }
        *val_out = prefix_agg_->Result();
        ++k_;
        return true;
      }
      case Mode::kPrefixRangePeer: {
        ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(row_out));
        if (!got) {
          return false;
        }
        if (k_ >= cume_peer_end_rows_read_) {
          if (!peer_lookahead_valid_) {
            ASSIGN_OR_RETURN(bool, pok,
                             peer_reader_->Next(&peer_lookahead_row_));
            if (pok) {
              peer_lookahead_keys_ =
                  ExtractKeys(peer_lookahead_row_, order_slots_);
              peer_lookahead_valid_ = true;
            }
          }
          uint64_t count_in_group = 0;
          if (peer_lookahead_valid_) {
            const std::vector<Value> group_keys = peer_lookahead_keys_;
            RETURN_IF_FAIL(prefix_agg_->AddRow(peer_lookahead_row_));
            count_in_group = 1;
            peer_lookahead_valid_ = false;
            while (true) {
              Row nxt;
              ASSIGN_OR_RETURN(bool, pok, peer_reader_->Next(&nxt));
              if (!pok) {
                break;
              }
              std::vector<Value> nxt_keys = ExtractKeys(nxt, order_slots_);
              if (SamePartitionTuple(group_keys, nxt_keys)) {
                RETURN_IF_FAIL(prefix_agg_->AddRow(nxt));
                ++count_in_group;
              } else {
                peer_lookahead_row_ = std::move(nxt);
                peer_lookahead_keys_ = std::move(nxt_keys);
                peer_lookahead_valid_ = true;
                break;
              }
            }
          }
          cume_peer_end_rows_read_ = k_ + count_in_group;
          group_cached_val_ = prefix_agg_->Result();
        }
        *val_out = group_cached_val_;
        ++k_;
        return true;
      }
      case Mode::kSuffixInvertibleRows: {
        ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(row_out));
        if (!got) {
          return false;
        }
        const int64_t target_lo_signed =
            static_cast<int64_t>(k_) + sliding_start_delta_;
        const uint64_t target_lo =
            target_lo_signed <= 0
                ? uint64_t{0}
                : std::min<uint64_t>(static_cast<uint64_t>(target_lo_signed),
                                     total_rows_);
        while (lo_pos_ < target_lo) {
          Row old_row;
          ASSIGN_OR_RETURN(bool, lok, peer_reader_->Next(&old_row));
          if (!lok) {
            break;
          }
          RETURN_IF_FAIL(prefix_agg_->RemoveRow(old_row));
          ++lo_pos_;
        }
        *val_out =
            target_lo >= total_rows_
                ? ((call_.function == "COUNT" || call_.function == "COUNTIF")
                       ? Value(int64_t{0})
                       : Value())
                : prefix_agg_->Result();
        ++k_;
        return true;
      }
      case Mode::kSlidingInvertibleRows: {
        ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(row_out));
        if (!got) {
          return false;
        }
        const int64_t raw_lo = static_cast<int64_t>(k_) + sliding_start_delta_;
        const int64_t raw_hi = static_cast<int64_t>(k_) + sliding_end_delta_;
        if (raw_hi < 0 || std::cmp_greater_equal(raw_lo, total_rows_) ||
            raw_lo > raw_hi) {
          *val_out = (call_.function == "COUNT" || call_.function == "COUNTIF")
                         ? Value(int64_t{0})
                         : Value();
          ++k_;
          return true;
        }
        const uint64_t target_lo =
            raw_lo < 0 ? 0 : static_cast<uint64_t>(raw_lo);
        const uint64_t target_hi =
            std::min<uint64_t>(static_cast<uint64_t>(raw_hi), total_rows_ - 1);
        // Remove leaving rows BEFORE adding entering rows. The intermediate
        // execution sum is then always a subset of the next frame, so an
        // INT64 overflow raised here is a real frame overflow (matching the
        // canonical row-wise evaluator), not a transient from holding both.
        while (lo_pos_ < target_lo) {
          Row r;
          ASSIGN_OR_RETURN(bool, lok, lo_reader_->Next(&r));
          if (!lok) {
            break;
          }
          RETURN_IF_FAIL(prefix_agg_->RemoveRow(r));
          ++lo_pos_;
        }
        while (hi_pos_ <= target_hi) {
          Row r;
          ASSIGN_OR_RETURN(bool, rok, peer_reader_->Next(&r));
          if (!rok) {
            break;
          }
          RETURN_IF_FAIL(prefix_agg_->AddRow(r));
          ++hi_pos_;
        }
        *val_out = prefix_agg_->Result();
        ++k_;
        return true;
      }
      case Mode::kSlidingDequeRows: {
        ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(row_out));
        if (!got) {
          return false;
        }
        const int64_t raw_lo = static_cast<int64_t>(k_) + sliding_start_delta_;
        const int64_t raw_hi = static_cast<int64_t>(k_) + sliding_end_delta_;
        const bool empty_frame =
            (raw_hi < 0 || std::cmp_greater_equal(raw_lo, total_rows_) ||
             raw_lo > raw_hi);
        const bool direct_streaming =
            call_.exclusion == WindowFrameExclusion::kNone && !call_.distinct &&
            StreamingWindowAggState::SupportsStreaming(call_) &&
            !IsDynamicNthCall(call_);
        if (direct_streaming && !minmax_.has_value() &&
            (call_.function == "MIN" || call_.function == "MAX")) {
          minmax_.emplace(call_, input_schema_, txn_);
        }

        // target_lo doubles as the deque-retirement floor, so it must never
        // jump ahead of what a LATER frame can still ask for. raw_lo = k_ +
        // sliding_start_delta_ is non-decreasing in k_, so max(0, raw_lo) is a
        // monotone floor: an empty frame (ROWS BETWEEN 3 PRECEDING AND 1
        // PRECEDING is empty for k_ = 0..2 and then reads rows 0..2) used to
        // park the floor at k_, retiring rows the first non-empty frame still
        // needs. Once the window runs off the end (raw_lo >= total_rows_)
        // every later frame is empty too, and retiring everything is safe
        // there. target_hi keeps the empty-frame clamp because raw_hi can be
        // negative and would otherwise wrap.
        const uint64_t target_lo =
            static_cast<uint64_t>(std::max<int64_t>(0, raw_lo));
        const uint64_t target_hi =
            empty_frame ? k_
                        : std::min<uint64_t>(static_cast<uint64_t>(raw_hi),
                                             total_rows_ - 1);
        // The deque floor must only advance to the minimum frame start that
        // ANY future row can need. When the current frame is empty, clamping
        // to k_ discards rows that later (non-empty) frames still reference;
        // target_lo is already clamped to the never-decreasing raw bound.
        const uint64_t keep_lo = target_lo;
        const uint64_t keep_hi =
            direct_streaming ? target_hi : std::max(k_, target_hi);

        while (deque_lo_ < keep_lo && !window_deque_.empty()) {
          const size_t fb = EstimateRowMemoryBytes(window_deque_.front());
          deque_bytes_ -= fb;
          if (deque_grant_cache_.has_value()) {
            deque_grant_cache_->Release(fb);
          }
          window_deque_.pop_front();
          ++deque_lo_;
        }
        if (deque_lo_ < keep_lo) {
          while (deque_hi_loaded_ < keep_lo) {
            Row skip;
            ASSIGN_OR_RETURN(bool, sk, peer_reader_->Next(&skip));
            if (!sk) {
              break;
            }
            ++deque_hi_loaded_;
          }
          deque_lo_ = keep_lo;
        }
        const size_t max_deque_bytes =
            std::max<size_t>(kMinWindowMemBudget, budget_bytes_ / 2);
        while (deque_hi_loaded_ <= keep_hi) {
          Row r;
          ASSIGN_OR_RETURN(bool, rok, peer_reader_->Next(&r));
          if (!rok) {
            break;
          }
          const size_t rb = EstimateRowMemoryBytes(r);
          if (deque_bytes_ + rb > max_deque_bytes ||
              (deque_grant_cache_.has_value() && !window_deque_.empty() &&
               !deque_grant_cache_->Reserve(rb))) {
            return StatusError(
                StatusCode::kMemoryLimitExceeded,
                "Sliding window frame width exceeds operator memory budget");
          }
          deque_bytes_ += rb;
          if (minmax_.has_value()) {
            RETURN_IF_FAIL(minmax_->Push(deque_hi_loaded_, r));
          }
          window_deque_.push_back(std::move(r));
          ++deque_hi_loaded_;
        }

        if (empty_frame) {
          *val_out = (call_.function == "COUNT" || call_.function == "COUNTIF")
                         ? Value(int64_t{0})
                         : Value();
          ++k_;
          return true;
        }

        if (direct_streaming) {
          if (minmax_.has_value()) {
            minmax_->ExpireBefore(target_lo);
            *val_out = minmax_->Result();
          } else {
            ASSIGN_OR_RETURN(bool, positional,
                             TryPositionalValue(target_lo, target_hi, val_out));
            if (!positional) {
              // The rebuild evaluates arg+filter on every frame row; only
              // column references cannot error mid-frame, so only those may
              // stop scanning once Result() is fixed.
              const bool early_exit_ok =
                  (call_.args.empty() ||
                   (call_.args[0] != nullptr &&
                    call_.args[0]->Type() == TypeTag::kColumnValue)) &&
                  (call_.where_filter == nullptr ||
                   call_.where_filter->Type() == TypeTag::kColumnValue);
              if (rebuild_agg_ == nullptr) {
                rebuild_agg_ = std::make_unique<StreamingWindowAggState>(
                    call_, input_schema_, txn_);
                RETURN_IF_FAIL(rebuild_agg_->InitNthConstant());
              } else {
                rebuild_agg_->Reset();
              }
              StreamingWindowAggState& st = *rebuild_agg_;
              for (uint64_t idx = target_lo; idx <= target_hi; ++idx) {
                const auto d = static_cast<size_t>(idx - deque_lo_);
                if (d < window_deque_.size()) {
                  RETURN_IF_FAIL(st.AddRow(window_deque_[d]));
                  if (early_exit_ok && st.Done()) {
                    break;
                  }
                }
              }
              *val_out = st.Result();
            }
          }
        } else {
          std::vector<Row> sub_rows(window_deque_.begin(), window_deque_.end());
          const auto rel_k = static_cast<size_t>(k_ - deque_lo_);
          if (rel_k >= sub_rows.size()) {
            *val_out = Value();
          } else {
            static const relational_detail::CteMap kEmptyCtes;
            WindowFunctionCallExpression adj;
            adj.function = call_.function;
            adj.args = call_.args;
            adj.where_filter = call_.where_filter;
            adj.distinct = call_.distinct;
            adj.inner_order_by = call_.inner_order_by;
            adj.inner_limit = call_.inner_limit;
            adj.order_by = call_.order_by;
            adj.frame_unit = call_.frame_unit;
            adj.frame_start = call_.frame_start;
            adj.frame_end = call_.frame_end;
            adj.has_frame = call_.has_frame;
            adj.exclusion = call_.exclusion;
            adj.respect_nulls = call_.respect_nulls;
            adj.ignore_nulls = call_.ignore_nulls;
            std::vector<Value> res;
            RETURN_IF_FAIL(relational_detail::ComputeOneWindow(
                *txn_, adj, sub_rows, input_schema_, nullptr, kEmptyCtes,
                &res));
            *val_out = rel_k < res.size() ? std::move(res[rel_k]) : Value();
          }
        }
        ++k_;
        return true;
      }
      case Mode::kSlidingInvertibleRange: {
        ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(row_out));
        if (!got) {
          return false;
        }
        bool empty_frame = false;
        uint64_t target_lo = 0;
        uint64_t target_hi = 0;
        RETURN_IF_FAIL(range_scanner_->ResolveForRow(k_, *row_out, &empty_frame,
                                                     &target_lo, &target_hi));
        if (empty_frame) {
          *val_out = (call_.function == "COUNT" || call_.function == "COUNTIF")
                         ? Value(int64_t{0})
                         : Value();
          ++k_;
          return true;
        }
        const uint64_t target_hi_excl = target_hi + 1;
        if (target_lo >= hi_pos_) {
          // Non-overlapping jump: reset accumulator and fast-forward both
          // readers without churning intermediate rows.
          prefix_agg_->Reset();
          while (lo_pos_ < target_lo) {
            Row skip;
            ASSIGN_OR_RETURN(bool, ok, lo_reader_->Next(&skip));
            if (!ok) {
              break;
            }
            ++lo_pos_;
          }
          while (hi_pos_ < target_lo) {
            Row skip;
            ASSIGN_OR_RETURN(bool, ok, peer_reader_->Next(&skip));
            if (!ok) {
              break;
            }
            ++hi_pos_;
          }
        }
        // Same remove-first ordering as the ROWS sliding mode: keep every
        // intermediate sum inside the real frame to avoid transient overflow.
        while (lo_pos_ < target_lo) {
          Row r;
          ASSIGN_OR_RETURN(bool, lok, lo_reader_->Next(&r));
          if (!lok) {
            break;
          }
          RETURN_IF_FAIL(prefix_agg_->RemoveRow(r));
          ++lo_pos_;
        }
        while (hi_pos_ < target_hi_excl) {
          Row r;
          ASSIGN_OR_RETURN(bool, rok, peer_reader_->Next(&r));
          if (!rok) {
            break;
          }
          RETURN_IF_FAIL(prefix_agg_->AddRow(r));
          ++hi_pos_;
        }
        *val_out = prefix_agg_->Result();
        ++k_;
        return true;
      }
      case Mode::kSlidingDequeRange: {
        ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(row_out));
        if (!got) {
          return false;
        }
        bool empty_frame = false;
        uint64_t target_lo = 0;
        uint64_t target_hi = 0;
        RETURN_IF_FAIL(range_scanner_->ResolveForRow(k_, *row_out, &empty_frame,
                                                     &target_lo, &target_hi));
        const bool direct_streaming =
            call_.exclusion == WindowFrameExclusion::kNone && !call_.distinct &&
            StreamingWindowAggState::SupportsStreaming(call_) &&
            !IsDynamicNthCall(call_);
        if (direct_streaming && !minmax_.has_value() &&
            (call_.function == "MIN" || call_.function == "MAX")) {
          minmax_.emplace(call_, input_schema_, txn_);
        }
        // Same monotone-floor rule as the ROWS mode: an empty RANGE frame must
        // not retire deque rows below what a later frame reads. ResolveForRow
        // now publishes lo/hi for empty frames too, so target_lo is that
        // frame's own lower bound (non-decreasing in k_) rather than the
        // caller's placeholder zero -- the floor stays tight enough to keep the
        // deque bounded. eff_hi keeps the k_ clamp so an empty frame does not
        // try to load rows the resolver never resolved.
        //
        // One bound kind still breaks monotonicity: NULL and NaN order keys
        // collapse their offset bounds to their own peer group, which sorts
        // at the far end of the partition (last under DESC). A finite row's
        // frame -- or its empty-frame placeholder -- can therefore sit past
        // the NULL/NaN run that the trailing rows' frames restart at. Clamp
        // the retirement floor to the next collapse run's start while such a
        // run can still be read, so the deque keeps the rows those frames
        // need.
        uint64_t eff_lo = target_lo;
        if (auto floor = range_scanner_->RestartFloor(k_);
            floor.has_value() && *floor < eff_lo) {
          eff_lo = *floor;
        }
        const uint64_t eff_hi = empty_frame ? k_ : target_hi;
        // eff_lo already carries the monotonically non-decreasing future
        // floor (RestartFloor); never clamp it up to k_ on an empty frame.
        const uint64_t keep_lo = eff_lo;
        const uint64_t keep_hi =
            direct_streaming ? eff_hi : std::max(k_, eff_hi);

        while (deque_lo_ < keep_lo && !window_deque_.empty()) {
          const size_t fb = EstimateRowMemoryBytes(window_deque_.front());
          deque_bytes_ -= fb;
          if (deque_grant_cache_.has_value()) {
            deque_grant_cache_->Release(fb);
          }
          window_deque_.pop_front();
          ++deque_lo_;
        }
        if (deque_lo_ < keep_lo) {
          while (deque_hi_loaded_ < keep_lo) {
            Row skip;
            ASSIGN_OR_RETURN(bool, sk, peer_reader_->Next(&skip));
            if (!sk) {
              break;
            }
            ++deque_hi_loaded_;
          }
          deque_lo_ = keep_lo;
        }
        const size_t max_deque_bytes =
            std::max<size_t>(kMinWindowMemBudget, budget_bytes_ / 2);
        while (deque_hi_loaded_ <= keep_hi) {
          Row r;
          ASSIGN_OR_RETURN(bool, rok, peer_reader_->Next(&r));
          if (!rok) {
            break;
          }
          const size_t rb = EstimateRowMemoryBytes(r);
          if (deque_bytes_ + rb > max_deque_bytes ||
              (deque_grant_cache_.has_value() && !window_deque_.empty() &&
               !deque_grant_cache_->Reserve(rb))) {
            return StatusError(
                StatusCode::kMemoryLimitExceeded,
                "Sliding window frame width exceeds operator memory budget");
          }
          deque_bytes_ += rb;
          if (minmax_.has_value()) {
            RETURN_IF_FAIL(minmax_->Push(deque_hi_loaded_, r));
          }
          window_deque_.push_back(std::move(r));
          ++deque_hi_loaded_;
        }
        if (empty_frame) {
          *val_out = (call_.function == "COUNT" || call_.function == "COUNTIF")
                         ? Value(int64_t{0})
                         : Value();
          ++k_;
          return true;
        }
        if (direct_streaming) {
          if (minmax_.has_value()) {
            minmax_->ExpireBefore(target_lo);
            *val_out = minmax_->Result();
          } else {
            ASSIGN_OR_RETURN(bool, positional,
                             TryPositionalValue(target_lo, target_hi, val_out));
            if (!positional) {
              // The rebuild evaluates arg+filter on every frame row; only
              // column references cannot error mid-frame, so only those may
              // stop scanning once Result() is fixed.
              const bool early_exit_ok =
                  (call_.args.empty() ||
                   (call_.args[0] != nullptr &&
                    call_.args[0]->Type() == TypeTag::kColumnValue)) &&
                  (call_.where_filter == nullptr ||
                   call_.where_filter->Type() == TypeTag::kColumnValue);
              if (rebuild_agg_ == nullptr) {
                rebuild_agg_ = std::make_unique<StreamingWindowAggState>(
                    call_, input_schema_, txn_);
                RETURN_IF_FAIL(rebuild_agg_->InitNthConstant());
              } else {
                rebuild_agg_->Reset();
              }
              StreamingWindowAggState& st = *rebuild_agg_;
              for (uint64_t idx = target_lo; idx <= target_hi; ++idx) {
                const auto d = static_cast<size_t>(idx - deque_lo_);
                if (d < window_deque_.size()) {
                  RETURN_IF_FAIL(st.AddRow(window_deque_[d]));
                  if (early_exit_ok && st.Done()) {
                    break;
                  }
                }
              }
              *val_out = st.Result();
            }
          }
        } else {
          std::vector<Row> sub_rows(window_deque_.begin(), window_deque_.end());
          const auto rel_k = static_cast<size_t>(k_ - deque_lo_);
          if (rel_k >= sub_rows.size()) {
            *val_out = Value();
          } else {
            static const relational_detail::CteMap kEmptyCtes;
            WindowFunctionCallExpression adj;
            adj.function = call_.function;
            adj.args = call_.args;
            adj.where_filter = call_.where_filter;
            adj.distinct = call_.distinct;
            adj.inner_order_by = call_.inner_order_by;
            adj.inner_limit = call_.inner_limit;
            adj.order_by = call_.order_by;
            adj.frame_unit = call_.frame_unit;
            adj.frame_start = call_.frame_start;
            adj.frame_end = call_.frame_end;
            adj.has_frame = call_.has_frame;
            adj.exclusion = call_.exclusion;
            adj.respect_nulls = call_.respect_nulls;
            adj.ignore_nulls = call_.ignore_nulls;
            std::vector<Value> res;
            RETURN_IF_FAIL(relational_detail::ComputeOneWindow(
                *txn_, adj, sub_rows, input_schema_, nullptr, kEmptyCtes,
                &res));
            *val_out = rel_k < res.size() ? std::move(res[rel_k]) : Value();
          }
        }
        ++k_;
        return true;
      }
      case Mode::kDynamicNthRange: {
        ASSIGN_OR_RETURN(bool, got, cur_reader_.Next(row_out));
        if (!got) {
          return false;
        }
        ASSIGN_OR_RETURN(
            Value, nth_val,
            EvalWindowExpr(call_.args[1], *row_out, input_schema_, txn_));
        ASSIGN_OR_RETURN(
            int64_t, nth_signed,
            ValidateIntOffsetValue(
                nth_val,
                "The N value for the NthValue function must not be NULL",
                "The N value for the NthValue function must be an integer",
                "The N value for the NthValue function must be positive",
                /*disallow_zero=*/true));
        const auto nth = static_cast<uint64_t>(nth_signed);
        bool empty_frame = false;
        uint64_t lo = 0;
        uint64_t hi = 0;
        if (dynamic_nth_use_range_) {
          RETURN_IF_FAIL(range_scanner_->ResolveForRow(k_, *row_out,
                                                       &empty_frame, &lo, &hi));
        } else {
          const int64_t raw_lo =
              sliding_start_delta_ <= std::numeric_limits<int64_t>::min() / 4
                  ? int64_t{0}
                  : static_cast<int64_t>(k_) + sliding_start_delta_;
          const int64_t raw_hi =
              sliding_end_delta_ >= std::numeric_limits<int64_t>::max() / 4
                  ? static_cast<int64_t>(total_rows_ - 1)
                  : static_cast<int64_t>(k_) + sliding_end_delta_;
          if (raw_hi < 0 || std::cmp_greater_equal(raw_lo, total_rows_) ||
              raw_lo > raw_hi) {
            empty_frame = true;
          } else {
            lo = static_cast<uint64_t>(std::max<int64_t>(0, raw_lo));
            hi = std::min<uint64_t>(static_cast<uint64_t>(raw_hi),
                                    total_rows_ - 1);
          }
        }
        if (empty_frame) {
          *val_out = Value();
          ++k_;
          return true;
        }
        if (!call_.ignore_nulls && call_.where_filter == nullptr &&
            call_.exclusion == WindowFrameExclusion::kNone) {
          if (nth - 1 <= hi - lo) {
            Row src;
            RETURN_IF_FAIL(indexed_reader_->ReadAt(lo + (nth - 1), &src));
            ASSIGN_OR_RETURN(
                Value, v,
                EvalWindowExpr(call_.args[0], src, input_schema_, txn_));
            *val_out = std::move(v);
          } else {
            *val_out = Value();
          }
        } else {
          uint64_t seen = 0;
          Value result;
          const std::vector<Value> cur_order_keys =
              call_.order_by.empty() ? std::vector<Value>{}
                                     : ExtractKeys(*row_out, order_slots_);
          for (uint64_t p = lo; p <= hi; ++p) {
            Row cand;
            RETURN_IF_FAIL(indexed_reader_->ReadAt(p, &cand));
            if (call_.exclusion != WindowFrameExclusion::kNone) {
              bool excluded = false;
              if (call_.exclusion == WindowFrameExclusion::kCurrentRow) {
                excluded = (p == k_);
              } else {
                const bool peer =
                    call_.order_by.empty() ||
                    SamePartitionTuple(cur_order_keys,
                                       ExtractKeys(cand, order_slots_));
                excluded = (call_.exclusion == WindowFrameExclusion::kGroup)
                               ? peer
                               : (peer && p != k_);
              }
              if (excluded) {
                continue;
              }
            }
            if (call_.where_filter != nullptr) {
              ASSIGN_OR_RETURN(Value, wf,
                               EvalWindowExpr(call_.where_filter, cand,
                                              input_schema_, txn_));
              if (!relational_detail::Truthy(wf)) {
                continue;
              }
            }
            ASSIGN_OR_RETURN(
                Value, cv,
                EvalWindowExpr(call_.args[0], cand, input_schema_, txn_));
            if (call_.ignore_nulls && cv.IsNull()) {
              continue;
            }
            ++seen;
            if (seen == nth) {
              result = std::move(cv);
              break;
            }
          }
          *val_out = std::move(result);
        }
        ++k_;
        return true;
      }
    }
    return Status::kSuccess;
  }

 private:
  enum class Mode : uint8_t {
    kConstantValue,
    kRowNumber,
    kRankLike,
    kCumeDist,
    kNtile,
    kLag,
    kLead,
    kPrefixRowsCurrent,
    kPrefixRowsPreceding,
    kPrefixRowsFollowing,
    kPrefixRangePeer,
    kSuffixInvertibleRows,
    kSlidingInvertibleRows,
    kSlidingDequeRows,
    kSlidingInvertibleRange,
    kSlidingDequeRange,
    kDynamicNthRange,
  };

  const SpillStore* store_;
  SpillSegmentId seg_id_;
  uint64_t total_rows_{0};
  const WindowFunctionCallExpression& call_;
  const Schema& input_schema_;
  std::span<const slot_t> order_slots_;
  TransactionContext* txn_{nullptr};
  size_t budget_bytes_{0};
  QueryMemoryGrant* grant_{nullptr};
  std::optional<QueryMemoryGrant::LocalBatchCache> deque_grant_cache_;

  Mode mode_{Mode::kConstantValue};
  Value const_val_;
  uint64_t k_{0};

  SegmentRowReader cur_reader_;
  std::unique_ptr<SegmentRowReader> peer_reader_;
  std::unique_ptr<SegmentRowReader> lo_reader_;
  std::unique_ptr<SegmentIndexedReader> indexed_reader_;
  std::unique_ptr<RangeFrameScanner> range_scanner_;
  bool lag_lead_dynamic_{false};
  bool dynamic_nth_use_range_{false};

  // Ranking state:
  bool has_prev_keys_{false};
  std::vector<Value> prev_keys_;
  uint64_t peer_rank_{0};
  int64_t dense_rank_{0};

  // Lookahead peer group state (CUME_DIST & prefix RANGE):
  bool peer_lookahead_valid_{false};
  Row peer_lookahead_row_;
  std::vector<Value> peer_lookahead_keys_;
  uint64_t cume_peer_end_rows_read_{0};
  Value group_cached_val_;

  // NTILE state:
  std::optional<Row> buffered_first_row_;
  uint64_t ntile_base_{0};
  uint64_t ntile_extra_{0};
  int64_t ntile_bucket_{0};
  uint64_t ntile_left_in_bucket_{0};

  // LAG / LEAD / Prefix / Sliding bounds:
  uint64_t lag_lead_offset_{1};
  std::unique_ptr<StreamingWindowAggState> prefix_agg_;
  // Sliding deque modes rebuild the frame aggregate for the handful of
  // functions without an O(1) sliding path. Reuse one accumulator (Reset()
  // between frames) instead of reconstructing it for every output row.
  std::unique_ptr<StreamingWindowAggState> rebuild_agg_;
  uint64_t hi_pos_{0};
  uint64_t lo_pos_{0};
  int64_t sliding_start_delta_{0};
  int64_t sliding_end_delta_{0};

  std::deque<Row> window_deque_;
  size_t deque_bytes_{0};
  uint64_t deque_lo_{0};
  uint64_t deque_hi_loaded_{0};

  // O(1)-amortized sliding accumulators for the deque modes: a monotone
  // MIN/MAX queue, and a lazily-resolved NTH_VALUE static target for the
  // direct positional evaluation path.
  std::optional<SlidingMinMax> minmax_;
  std::optional<size_t> positional_nth_;

  // Evaluates FIRST_VALUE / LAST_VALUE / NTH_VALUE directly at the frame
  // position when no FILTER / IGNORE NULLS / DISTINCT forces a full scan.
  // Returns false when the function needs the generic per-frame rebuild.
  StatusOr<bool> TryPositionalValue(uint64_t lo, uint64_t hi, Value* out) {
    const std::string& fn = call_.function;
    // Only a bare column argument is eligible: the generic path evaluates the
    // argument on every frame row, so a fallible expression must keep running
    // to preserve its error surface. A column reference either fails for all
    // rows (schema lookup) or none.
    if (call_.where_filter != nullptr || call_.ignore_nulls || call_.distinct ||
        call_.args.empty() || call_.args[0] == nullptr ||
        call_.args[0]->Type() != TypeTag::kColumnValue) {
      return false;
    }
    uint64_t pos = 0;
    if (fn == "FIRST_VALUE") {
      pos = lo;
    } else if (fn == "LAST_VALUE") {
      pos = hi;
    } else if (fn == "NTH_VALUE") {
      if (!positional_nth_.has_value()) {
        StreamingWindowAggState st(call_, input_schema_, txn_);
        RETURN_IF_FAIL(st.InitNthConstant());
        positional_nth_ = st.NthTarget();
      }
      if (*positional_nth_ > hi - lo + 1) {
        *out = Value();
        return true;
      }
      pos = lo + *positional_nth_ - 1;
    } else {
      return false;
    }
    if (pos < deque_lo_ || pos - deque_lo_ >= window_deque_.size()) {
      // Frame position fell outside the retained slice: leave it to the
      // generic rebuild rather than guessing.
      return false;
    }
    ASSIGN_OR_RETURN(
        Value, v,
        EvalWindowExpr(call_.args[0], window_deque_[pos - deque_lo_],
                       input_schema_, txn_));
    *out = std::move(v);
    return true;
  }
};

// Write the base row, its original sequence, and keys that need evaluation.
Status EncodeRowForGroup(TransactionContext* txn, const Schema& input_schema,
                         Row&& base_row, size_t base_col_count, uint64_t seq,
                         const OocWindowSink::SpecGroup& group,
                         ExternalSorter* sorter) {
  base_row.values_.resize(base_col_count);
  base_row.values_.emplace_back(static_cast<int64_t>(seq));
  for (const Expression& key : group.computed_keys) {
    ASSIGN_OR_RETURN(Value, value,
                     EvalWindowExpr(key, base_row, input_schema, txn));
    base_row.values_.push_back(
        ApplyKeyCollation(value, CollationOfExpression(key)));
  }
  return sorter->AddRow(std::move(base_row));
}

StatusOr<std::vector<OocSortKey>> MakeSorterKeysForGroup(
    size_t seq_col, const OocWindowSink::SpecGroup& group) {
  std::vector<OocSortKey> keys;
  const size_t p_count = group.partition_by.size();
  const size_t o_count = group.order_by.size();
  if (seq_col > std::numeric_limits<slot_t>::max()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "sort key column index out of range");
  }
  keys.reserve(p_count + o_count + 1);
  for (size_t i = 0; i < p_count; ++i) {
    keys.push_back(
        OocSortKey{.column_index = group.partition_slots[i],
                   .ascending = true,
                   .nulls_first = true,
                   .collation = CollationOfExpression(group.partition_by[i])});
  }
  for (size_t i = 0; i < o_count; ++i) {
    const WindowOrderTerm& t = group.order_by[i];
    keys.push_back(
        OocSortKey{.column_index = group.order_slots[i],
                   .ascending = t.ascending,
                   .nulls_first = t.nulls_first,
                   .collation = CollationOfExpression(t.expression)});
  }
  keys.push_back(OocSortKey{.column_index = static_cast<slot_t>(seq_col),
                            .ascending = true,
                            .nulls_first = true,
                            .collation = Collation::kNone});
  return keys;
}

// Evaluates all calls in `group` on an in-memory partition `part_rows` (which
// contains the base row, sequence number and computed keys), storing
// the computed values into `part_rows[r][input_width + call.output_idx]`.
Status EvalInMemoryPartitionGroup(TransactionContext* txn,
                                  const Schema& input_schema,
                                  size_t input_width,
                                  const OocWindowSink::SpecGroup& group,
                                  std::vector<Row>* part_rows) {
  if (part_rows->empty()) {
    return Status::kSuccess;
  }
  // The evaluator resolves every column through `input_schema` (the first
  // `input_width` cells). Passing the resident rows directly avoids a second
  // full-row copy of the whole partition just to strip the trailing
  // sequence/partition/order prefix columns.
  static const relational_detail::CteMap kEmptyCtes;
  std::vector<Value> out_vals;
  for (const OocWindowSink::CallSpec& call : group.calls) {
    RETURN_IF_FAIL(relational_detail::ComputeOneWindow(
        *txn, *call.expr, *part_rows, input_schema, nullptr, kEmptyCtes,
        &out_vals));
    if (out_vals.size() != part_rows->size()) {
      return StatusError(StatusCode::kUnknown, "window output width mismatch");
    }
    const size_t target_slot = input_width + call.output_idx;
    for (size_t r = 0; r < part_rows->size(); ++r) {
      (*part_rows)[r].values_[target_slot] = std::move(out_vals[r]);
    }
  }
  return Status::kSuccess;
}

// Evaluates calls `[0 .. group.calls.size() - 2]` (or ALL calls if `eval_all`
// is true) on a spilled sorted segment, producing a final segment ready to
// stream.
StatusOr<SpillSegmentId> EvalSpilledCallsMaterializingSegment(
    SpillStore* store, SpillSegmentId cur_seg, uint64_t total_rows,
    const Schema& input_schema, size_t input_width,
    const OocWindowSink::SpecGroup& group, bool eval_all,
    TransactionContext* txn, size_t budget_bytes, QueryMemoryGrant* grant) {
  const size_t limit = eval_all
                           ? group.calls.size()
                           : (group.calls.empty() ? 0 : group.calls.size() - 1);
  SpillSegmentId active_seg = cur_seg;
  for (size_t ci = 0; ci < limit; ++ci) {
    const OocWindowSink::CallSpec& call = group.calls[ci];
    SkewCallStream stream(store, active_seg, total_rows, *call.expr,
                          input_schema, group.order_slots, txn, budget_bytes,
                          grant);
    RETURN_IF_FAIL(stream.Init());

    SegmentRowWriter writer(store);
    const size_t target_slot = input_width + call.output_idx;
    Row r;
    Value v;
    while (true) {
      ASSIGN_OR_RETURN(bool, got, stream.Next(&r, &v));
      if (!got) {
        break;
      }
      if (target_slot < r.values_.size()) {
        r.values_[target_slot] = std::move(v);
      }
      Status st = writer.Append(std::move(r));
      if (st != Status::kSuccess) {
        writer.Abort();
        if (active_seg != cur_seg) {
          store->DropSegment(active_seg);
        }
        return st;
      }
    }
    StatusOr<SpillSegmentId> next_seg = writer.Finish();
    if (active_seg != cur_seg) {
      store->DropSegment(active_seg);
    }
    ASSIGN_OR_RETURN(SpillSegmentId, finished_id, std::move(next_seg));
    active_seg = finished_id;
  }
  return active_seg;
}

// Emits already-computed buffered rows (from small inputs that fit inside B_op)
// freeing emitted slices progressively.
class WindowBufferedSource final : public PipelineSource {
 public:
  WindowBufferedSource(
      Schema output_schema, std::vector<Row> rows,
      std::optional<QueryMemoryGrant::ActiveConsumerGuard> guard,
      std::optional<QueryMemoryGrant::LocalBatchCache> grant_cache,
      size_t num_outputs, std::string label)
      : output_schema_(std::move(output_schema)),
        rows_(std::move(rows)),
        guard_(std::move(guard)),
        grant_cache_(std::move(grant_cache)),
        num_outputs_(num_outputs),
        label_(std::move(label)) {}

  StatusOr<bool> NextBatch(PipelineContext* /*ctx*/, DataChunk* out,
                           size_t max_rows) override {
    if (cursor_ >= rows_.size()) {
      rows_.clear();
      rows_.shrink_to_fit();
      if (grant_cache_.has_value()) {
        grant_cache_->ReleaseAll();
        grant_cache_.reset();
      }
      guard_.reset();
      return false;
    }
    const size_t count = std::min(max_rows, rows_.size() - cursor_);
    out->Reset(output_schema_, count);
    size_t freed_bytes = 0;
    for (size_t i = 0; i < count; ++i) {
      freed_bytes += EstimateRowMemoryBytes(rows_[cursor_ + i]);
      out->Append(rows_[cursor_ + i], RowPosition());
      rows_[cursor_ + i].values_.clear();
    }
    if (grant_cache_.has_value() && freed_bytes > 0) {
      grant_cache_->Release(freed_bytes);
    }
    cursor_ += count;
    if (cursor_ >= rows_.size()) {
      rows_.clear();
      rows_.shrink_to_fit();
      if (grant_cache_.has_value()) {
        grant_cache_->ReleaseAll();
        grant_cache_.reset();
      }
      guard_.reset();
    }
    return true;
  }

  void DumpExplain(std::ostream& out, int indent) const override {
    out << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
        << "WindowExecutor(outputs=" << num_outputs_ << ")\n";
  }

  [[nodiscard]] std::string Describe() const override { return label_; }

 private:
  Schema output_schema_;
  std::vector<Row> rows_;
  size_t cursor_{0};
  std::optional<QueryMemoryGrant::ActiveConsumerGuard> guard_;
  std::optional<QueryMemoryGrant::LocalBatchCache> grant_cache_;
  size_t num_outputs_{0};
  std::string label_;
};

// Passthrough source wrapping a finalized SpillableStore (used when outputs_ is
// empty but input exceeded in-mem budget or force_spill was active).
class WindowPassthroughSpillSource final : public PipelineSource {
 public:
  WindowPassthroughSpillSource(
      Schema output_schema, std::unique_ptr<SpillableStore> store,
      std::optional<QueryMemoryGrant::ActiveConsumerGuard> guard)
      : output_schema_(std::move(output_schema)),
        store_(std::move(store)),
        guard_(std::move(guard)) {}

  Status Open(PipelineContext* /*ctx*/) override {
    return store_->ResetCursor();
  }

  StatusOr<bool> NextBatch(PipelineContext* /*ctx*/, DataChunk* out,
                           size_t max_rows) override {
    (void)max_rows;
    ASSIGN_OR_RETURN(bool, got, store_->NextChunk(out));
    if (!got) {
      store_->Reset();
      guard_.reset();
      return false;
    }
    return true;
  }

  void DumpExplain(std::ostream& out, int indent) const override {
    out << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
        << "WindowExecutor(outputs=0)\n";
  }

  [[nodiscard]] std::string Describe() const override { return "Window"; }

 private:
  Schema output_schema_;
  std::unique_ptr<SpillableStore> store_;
  std::optional<QueryMemoryGrant::ActiveConsumerGuard> guard_;
};

// Streaming output source evaluating the LAST SpecGroup partition-by-partition
// directly from `sorter`, emitting small partitions after ComputeOneWindow and
// large overflowing skew partitions streamingly via `SkewCallStream`!
class OocWindowStreamSource final : public PipelineSource {
 public:
  OocWindowStreamSource(
      Schema input_schema, Schema output_schema, size_t num_outputs,
      std::unique_ptr<ExternalSorter> sorter, OocWindowSink::SpecGroup group,
      size_t budget_bytes,
      std::optional<QueryMemoryGrant::ActiveConsumerGuard> guard,
      std::shared_ptr<std::atomic<bool>> runtime_spilled,
      std::shared_ptr<std::atomic<uint64_t>> runtime_partitions,
      std::shared_ptr<std::atomic<size_t>> runtime_peak_resident_rows)
      : input_schema_(std::move(input_schema)),
        output_schema_(std::move(output_schema)),
        num_outputs_(num_outputs),
        input_width_(input_schema_.ColumnCount()),
        base_width_(input_width_ + num_outputs_),
        sorter_(std::move(sorter)),
        group_(std::move(group)),
        budget_bytes_(std::max<size_t>(kMinWindowMemBudget, budget_bytes)),
        part_budget_(std::max<size_t>(kMinWindowMemBudget, budget_bytes / 2)),
        guard_(std::move(guard)),
        runtime_spilled_(std::move(runtime_spilled)),
        runtime_partitions_(std::move(runtime_partitions)),
        runtime_peak_resident_rows_(std::move(runtime_peak_resident_rows)) {}

  OocWindowStreamSource(const OocWindowStreamSource&) = delete;
  OocWindowStreamSource& operator=(const OocWindowStreamSource&) = delete;
  OocWindowStreamSource(OocWindowStreamSource&&) = delete;
  OocWindowStreamSource& operator=(OocWindowStreamSource&&) = delete;
  ~OocWindowStreamSource() override { CleanupActiveSkew(); }

  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override {
    while (true) {
      // 1. Drain current in-memory partition rows if any.
      if (mem_cursor_ < active_mem_rows_.size()) {
        const size_t count =
            std::min(max_rows, active_mem_rows_.size() - mem_cursor_);
        out->Reset(output_schema_, count);
        for (size_t i = 0; i < count; ++i) {
          out->Append(TrimToWidth(std::move(active_mem_rows_[mem_cursor_ + i]),
                                  base_width_),
                      RowPosition());
        }
        mem_cursor_ += count;
        if (mem_cursor_ >= active_mem_rows_.size()) {
          active_mem_rows_.clear();
          active_mem_rows_.shrink_to_fit();
          mem_cursor_ = 0;
          if (part_grant_cache_.has_value()) {
            part_grant_cache_->ReleaseAll();
          }
        }
        return true;
      }

      // 2. Drain current skew partition stream if open.
      if (active_skew_stream_ != nullptr) {
        out->Reset(output_schema_, max_rows);
        const auto& last_call = group_.calls.back();
        const size_t target_slot = input_width_ + last_call.output_idx;
        size_t emitted = 0;
        while (emitted < max_rows) {
          Row r;
          Value v;
          ASSIGN_OR_RETURN(bool, got, active_skew_stream_->Next(&r, &v));
          if (!got) {
            CleanupActiveSkew();
            break;
          }
          if (target_slot < r.values_.size()) {
            r.values_[target_slot] = std::move(v);
          }
          out->Append(TrimToWidth(std::move(r), base_width_), RowPosition());
          ++emitted;
        }
        if (emitted > 0) {
          return true;
        }
      }

      if (sorter_eof_) {
        sorter_.reset();
        if (part_grant_cache_.has_value()) {
          part_grant_cache_->ReleaseAll();
          part_grant_cache_.reset();
        }
        guard_.reset();
        return false;
      }

      // 3. Read next partition from `sorter_`.
      ASSIGN_OR_RETURN(bool, loaded, LoadNextPartition(ctx));
      if (!loaded) {
        sorter_.reset();
        if (part_grant_cache_.has_value()) {
          part_grant_cache_->ReleaseAll();
          part_grant_cache_.reset();
        }
        guard_.reset();
        return false;
      }
    }
  }

  void DumpExplain(std::ostream& out, int indent) const override {
    out << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
        << "WindowExecutor(outputs=" << num_outputs_;
    if (runtime_spilled_->load(std::memory_order_relaxed)) {
      out << ", spill=OOC, partitions="
          << runtime_partitions_->load(std::memory_order_relaxed)
          << ", resident-rows="
          << runtime_peak_resident_rows_->load(std::memory_order_relaxed);
    }
    out << ")\n";
  }

  [[nodiscard]] std::string Describe() const override { return "Window"; }

 private:
  void CleanupActiveSkew() noexcept {
    active_skew_stream_.reset();
    if (skew_store_ != nullptr) {
      SpillStore::DropSegmentIfAlive(skew_store_, skew_store_alive_,
                                     active_skew_seg_);
      active_skew_seg_ = kInvalidSpillSegmentId;
      SpillStore::DropSegmentIfAlive(skew_store_, skew_store_alive_,
                                     raw_skew_seg_);
      raw_skew_seg_ = kInvalidSpillSegmentId;
    }
  }

  StatusOr<bool> LoadNextPartition(PipelineContext* ctx) {
    active_mem_rows_.clear();
    mem_cursor_ = 0;
    if (!part_grant_cache_.has_value() && ctx != nullptr) {
      const size_t refill = std::min<size_t>(
          QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes,
          std::max<size_t>(65536, budget_bytes_ / 4));
      part_grant_cache_.emplace(&ctx->grant, refill);
    }

    if (!has_lookahead_) {
      Row r;
      ASSIGN_OR_RETURN(bool, got, sorter_->NextRow(&r));
      if (!got) {
        sorter_eof_ = true;
        return false;
      }
      lookahead_row_ = std::move(r);
      has_lookahead_ = true;
    }

    const std::vector<Value> part_key =
        ExtractKeys(lookahead_row_, group_.partition_slots);

    std::vector<Row> part_rows;
    size_t part_bytes = 0;
    std::unique_ptr<SegmentRowWriter> skew_writer;
    uint64_t skew_rows = 0;

    auto push_row = [&](Row&& row) -> Status {
      const size_t rb = EstimateRowMemoryBytes(row);
      if (skew_writer == nullptr &&
          (part_bytes + rb > part_budget_ ||
           ProcessMemoryBudget::Global().AlwaysSpillEnabled() ||
           (part_grant_cache_.has_value() && !part_rows.empty() &&
            !part_grant_cache_->Reserve(rb)))) {
        // Partition exceeds its budget (max(B_op / 2, 16 KiB)) -> transition
        // this single partition to a
        // spill segment in exact sorted order (`ExternalSorter` output order).
        runtime_spilled_->store(true, std::memory_order_relaxed);
        skew_store_ = &ctx->spill_store;
        skew_store_alive_ = ctx->spill_store.LifetimeToken();
        skew_writer = std::make_unique<SegmentRowWriter>(&ctx->spill_store);
        for (Row& prev : part_rows) {
          RETURN_IF_FAIL(skew_writer->Append(std::move(prev)));
          ++skew_rows;
        }
        part_rows.clear();
        part_rows.shrink_to_fit();
        part_bytes = 0;
        if (part_grant_cache_.has_value()) {
          part_grant_cache_->ReleaseAll();
        }
      }
      if (skew_writer != nullptr) {
        RETURN_IF_FAIL(skew_writer->Append(std::move(row)));
        ++skew_rows;
      } else {
        if (part_rows.empty() && part_grant_cache_.has_value() &&
            !part_grant_cache_->Reserve(rb)) {
          return StatusError(StatusCode::kMemoryLimitExceeded,
                             "partition grant cache reserve failed");
        }
        part_bytes += rb;
        part_rows.push_back(std::move(row));
      }
      return Status::kSuccess;
    };

    RETURN_IF_FAIL(push_row(std::move(lookahead_row_)));
    has_lookahead_ = false;

    while (true) {
      Row nxt;
      ASSIGN_OR_RETURN(bool, got, sorter_->NextRow(&nxt));
      if (!got) {
        sorter_eof_ = true;
        break;
      }
      if (!group_.partition_slots.empty() &&
          !SamePartitionTuple(part_key,
                              ExtractKeys(nxt, group_.partition_slots))) {
        lookahead_row_ = std::move(nxt);
        has_lookahead_ = true;
        break;
      }
      RETURN_IF_FAIL(push_row(std::move(nxt)));
    }

    runtime_partitions_->fetch_add(1, std::memory_order_relaxed);

    if (skew_writer == nullptr) {
      size_t cur_peak =
          runtime_peak_resident_rows_->load(std::memory_order_relaxed);
      while (part_rows.size() > cur_peak &&
             !runtime_peak_resident_rows_->compare_exchange_weak(
                 cur_peak, part_rows.size(), std::memory_order_relaxed)) {
      }
      RETURN_IF_FAIL(EvalInMemoryPartitionGroup(
          ctx->txn, input_schema_, input_width_, group_, &part_rows));
      active_mem_rows_ = std::move(part_rows);
      return true;
    }

    // Skewed partition finished spilling cleanly.
    ASSIGN_OR_RETURN(SpillSegmentId, seg_id, skew_writer->Finish());
    skew_writer.reset();
    raw_skew_seg_ = seg_id;

    QueryMemoryGrant* grant_ptr = ctx != nullptr ? &ctx->grant : nullptr;
    if (group_.calls.empty()) {
      return StatusError(StatusCode::kUnknown, "empty spec group in skew");
    }
    if (group_.calls.size() > 1) {
      ASSIGN_OR_RETURN(
          SpillSegmentId, pre_seg,
          EvalSpilledCallsMaterializingSegment(
              &ctx->spill_store, raw_skew_seg_, skew_rows, input_schema_,
              input_width_, group_, /*eval_all=*/false, ctx->txn, budget_bytes_,
              grant_ptr));
      active_skew_seg_ = pre_seg;
      if (pre_seg != raw_skew_seg_) {
        ctx->spill_store.DropSegment(raw_skew_seg_);
        raw_skew_seg_ = kInvalidSpillSegmentId;
      }
    } else {
      active_skew_seg_ = raw_skew_seg_;
      raw_skew_seg_ = kInvalidSpillSegmentId;
    }

    const auto& last_call = group_.calls.back();
    active_skew_stream_ = std::make_unique<SkewCallStream>(
        &ctx->spill_store, active_skew_seg_, skew_rows, *last_call.expr,
        input_schema_, group_.order_slots, ctx->txn, budget_bytes_, grant_ptr);
    Status init_st = active_skew_stream_->Init();
    if (init_st != Status::kSuccess) {
      CleanupActiveSkew();
      return init_st;
    }
    return true;
  }

  Schema input_schema_;
  Schema output_schema_;
  size_t num_outputs_{0};
  size_t input_width_{0};
  size_t base_width_{0};
  std::unique_ptr<ExternalSorter> sorter_;
  OocWindowSink::SpecGroup group_;
  size_t budget_bytes_{0};
  size_t part_budget_{0};
  std::optional<QueryMemoryGrant::ActiveConsumerGuard> guard_;
  std::optional<QueryMemoryGrant::LocalBatchCache> part_grant_cache_;

  std::shared_ptr<std::atomic<bool>> runtime_spilled_;
  std::shared_ptr<std::atomic<uint64_t>> runtime_partitions_;
  std::shared_ptr<std::atomic<size_t>> runtime_peak_resident_rows_;

  bool sorter_eof_{false};
  bool has_lookahead_{false};
  Row lookahead_row_;

  std::vector<Row> active_mem_rows_;
  size_t mem_cursor_{0};

  SpillStore* skew_store_{nullptr};
  std::weak_ptr<const bool> skew_store_alive_;
  SpillSegmentId raw_skew_seg_{kInvalidSpillSegmentId};
  SpillSegmentId active_skew_seg_{kInvalidSpillSegmentId};
  std::unique_ptr<SkewCallStream> active_skew_stream_;
};

// Strips trailing `[seq, keys...]` columns when emitting from a restore-order
// `ExternalSorter` (used when `preserve_input_order` is true and rows were
// reordered by PARTITION BY / ORDER BY in OOC mode).
class WindowRestoredOrderSource final : public PipelineSource {
 public:
  WindowRestoredOrderSource(
      Schema output_schema, size_t num_outputs,
      std::unique_ptr<ExternalSorter> sorter,
      std::optional<QueryMemoryGrant::ActiveConsumerGuard> guard,
      std::shared_ptr<std::atomic<bool>> runtime_spilled,
      std::shared_ptr<std::atomic<uint64_t>> runtime_partitions,
      std::shared_ptr<std::atomic<size_t>> runtime_peak_resident_rows)
      : output_schema_(std::move(output_schema)),
        num_outputs_(num_outputs),
        base_width_(output_schema_.ColumnCount()),
        sorter_(std::move(sorter)),
        guard_(std::move(guard)),
        runtime_spilled_(std::move(runtime_spilled)),
        runtime_partitions_(std::move(runtime_partitions)),
        runtime_peak_resident_rows_(std::move(runtime_peak_resident_rows)) {}

  StatusOr<bool> NextBatch(PipelineContext* /*ctx*/, DataChunk* out,
                           size_t max_rows) override {
    if (eof_) {
      return false;
    }
    out->Reset(output_schema_, max_rows);
    size_t count = 0;
    while (count < max_rows) {
      Row r;
      ASSIGN_OR_RETURN(bool, got, sorter_->NextRow(&r));
      if (!got) {
        eof_ = true;
        sorter_.reset();
        guard_.reset();
        break;
      }
      out->Append(TrimToWidth(std::move(r), base_width_), RowPosition());
      ++count;
    }
    return count > 0;
  }

  void DumpExplain(std::ostream& out, int indent) const override {
    out << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
        << "WindowExecutor(outputs=" << num_outputs_;
    if (runtime_spilled_->load(std::memory_order_relaxed)) {
      out << ", spill=OOC, partitions="
          << runtime_partitions_->load(std::memory_order_relaxed)
          << ", resident-rows="
          << runtime_peak_resident_rows_->load(std::memory_order_relaxed);
    }
    out << ")\n";
  }

  [[nodiscard]] std::string Describe() const override { return "Window"; }

 private:
  Schema output_schema_;
  size_t num_outputs_{0};
  size_t base_width_{0};
  std::unique_ptr<ExternalSorter> sorter_;
  bool eof_{false};
  std::optional<QueryMemoryGrant::ActiveConsumerGuard> guard_;
  std::shared_ptr<std::atomic<bool>> runtime_spilled_;
  std::shared_ptr<std::atomic<uint64_t>> runtime_partitions_;
  std::shared_ptr<std::atomic<size_t>> runtime_peak_resident_rows_;
};

// Drains ONE completed sorted group `g` (using OocWindowStreamSource internally
// for partition splitting & skew handling) into a downstream ExternalSorter
// (either group `g + 1` or the final `orig_seq` restore sorter).
Status DrainSortedGroupIntoSorter(
    PipelineContext* ctx, const Schema& input_schema, size_t input_width,
    size_t base_width, std::unique_ptr<ExternalSorter> src_sorter,
    const OocWindowSink::SpecGroup& cur_group, size_t budget_bytes,
    std::shared_ptr<std::atomic<bool>> runtime_spilled,
    const std::shared_ptr<std::atomic<uint64_t>>& runtime_partitions,
    const std::shared_ptr<std::atomic<size_t>>& runtime_peak_resident_rows,
    const std::optional<OocWindowSink::SpecGroup>& next_group,
    ExternalSorter* dst_sorter) {
  Row next_row;
  bool have_next = false;
  bool eof = false;
  const size_t part_budget =
      std::max<size_t>(kMinWindowMemBudget, budget_bytes / 2);

  auto forward_row = [&](Row&& full_row) -> Status {
    const auto seq =
        static_cast<uint64_t>(full_row[base_width].value.int_value);
    if (next_group.has_value()) {
      return EncodeRowForGroup(ctx->txn, input_schema, std::move(full_row),
                               base_width, seq, *next_group, dst_sorter);
    }
    // Restoring original order by `base_width` (seq):
    full_row.values_.resize(base_width + 1);
    return dst_sorter->AddRow(std::move(full_row));
  };

  while (!eof) {
    Row first_row;
    if (have_next) {
      first_row = std::move(next_row);
      next_row = Row();
      have_next = false;
    } else {
      ASSIGN_OR_RETURN(bool, got, src_sorter->NextRow(&first_row));
      if (!got) {
        break;
      }
    }

    const std::vector<Value> part_key =
        ExtractKeys(first_row, cur_group.partition_slots);
    std::vector<Row> part_rows;
    size_t part_bytes = 0;
    const size_t refill =
        std::min<size_t>(QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes,
                         std::max<size_t>(65536, budget_bytes / 4));
    QueryMemoryGrant::LocalBatchCache part_cache(
        ctx != nullptr ? &ctx->grant : nullptr, refill);
    std::unique_ptr<SegmentRowWriter> skew_writer;
    uint64_t skew_rows = 0;

    auto push = [&](Row&& r) -> Status {
      const size_t rb = EstimateRowMemoryBytes(r);
      if (skew_writer == nullptr &&
          (part_bytes + rb > part_budget ||
           ProcessMemoryBudget::Global().AlwaysSpillEnabled() ||
           (!part_rows.empty() && !part_cache.Reserve(rb)))) {
        runtime_spilled->store(true, std::memory_order_relaxed);
        skew_writer = std::make_unique<SegmentRowWriter>(&ctx->spill_store);
        for (Row& prev : part_rows) {
          RETURN_IF_FAIL(skew_writer->Append(std::move(prev)));
          ++skew_rows;
        }
        part_rows.clear();
        part_rows.shrink_to_fit();
        part_bytes = 0;
        part_cache.ReleaseAll();
      }
      if (skew_writer != nullptr) {
        RETURN_IF_FAIL(skew_writer->Append(std::move(r)));
        ++skew_rows;
      } else {
        if (part_rows.empty() && !part_cache.Reserve(rb)) {
          return StatusError(StatusCode::kMemoryLimitExceeded,
                             "partition cache reserve failed");
        }
        part_bytes += rb;
        part_rows.push_back(std::move(r));
      }
      return Status::kSuccess;
    };

    RETURN_IF_FAIL(push(std::move(first_row)));

    while (true) {
      Row nxt;
      ASSIGN_OR_RETURN(bool, got, src_sorter->NextRow(&nxt));
      if (!got) {
        eof = true;
        break;
      }
      if (!cur_group.partition_slots.empty() &&
          !SamePartitionTuple(part_key,
                              ExtractKeys(nxt, cur_group.partition_slots))) {
        next_row = std::move(nxt);
        have_next = true;
        break;
      }
      RETURN_IF_FAIL(push(std::move(nxt)));
    }

    runtime_partitions->fetch_add(1, std::memory_order_relaxed);

    if (skew_writer == nullptr) {
      size_t cur_peak =
          runtime_peak_resident_rows->load(std::memory_order_relaxed);
      while (part_rows.size() > cur_peak &&
             !runtime_peak_resident_rows->compare_exchange_weak(
                 cur_peak, part_rows.size(), std::memory_order_relaxed)) {
      }
      RETURN_IF_FAIL(EvalInMemoryPartitionGroup(
          ctx->txn, input_schema, input_width, cur_group, &part_rows));
      for (Row& r : part_rows) {
        RETURN_IF_FAIL(forward_row(std::move(r)));
      }
      part_cache.ReleaseAll();
      continue;
    }
    part_cache.ReleaseAll();

    ASSIGN_OR_RETURN(SpillSegmentId, raw_seg, skew_writer->Finish());
    skew_writer.reset();

    QueryMemoryGrant* grant_ptr = ctx != nullptr ? &ctx->grant : nullptr;
    StatusOr<SpillSegmentId> eval_seg_or = EvalSpilledCallsMaterializingSegment(
        &ctx->spill_store, raw_seg, skew_rows, input_schema, input_width,
        cur_group, /*eval_all=*/false, ctx->txn, budget_bytes, grant_ptr);
    if (!eval_seg_or.HasValue()) {
      ctx->spill_store.DropSegment(raw_seg);
      return eval_seg_or.GetStatus();
    }
    const SpillSegmentId active_seg = eval_seg_or.Value();
    if (active_seg != raw_seg) {
      ctx->spill_store.DropSegment(raw_seg);
    }

    const auto& last_call = cur_group.calls.back();
    SkewCallStream stream(&ctx->spill_store, active_seg, skew_rows,
                          *last_call.expr, input_schema, cur_group.order_slots,
                          ctx->txn, budget_bytes, grant_ptr);
    Status st = stream.Init();
    if (st != Status::kSuccess) {
      ctx->spill_store.DropSegment(active_seg);
      return st;
    }
    const size_t target_slot = input_width + last_call.output_idx;
    Row r;
    Value v;
    while (true) {
      StatusOr<bool> next_or = stream.Next(&r, &v);
      if (!next_or.HasValue()) {
        ctx->spill_store.DropSegment(active_seg);
        return next_or.GetStatus();
      }
      if (!next_or.Value()) {
        break;
      }
      if (target_slot < r.values_.size()) {
        r.values_[target_slot] = std::move(v);
      }
      Status fwd_st = forward_row(std::move(r));
      if (fwd_st != Status::kSuccess) {
        ctx->spill_store.DropSegment(active_seg);
        return fwd_st;
      }
    }
    ctx->spill_store.DropSegment(active_seg);
  }
  return Status::kSuccess;
}

}  // namespace

OocWindowSink::OocWindowSink(Schema input_schema, Schema output_schema,
                             std::vector<NamedExpression> outputs,
                             bool preserve_input_order, bool spill_capable_plan,
                             size_t plan_memory_budget_bytes)
    : input_schema_(std::move(input_schema)),
      output_schema_(std::move(output_schema)),
      outputs_(std::move(outputs)),
      preserve_input_order_(preserve_input_order),
      spill_capable_plan_(spill_capable_plan),
      plan_memory_budget_bytes_(plan_memory_budget_bytes) {}

Status OocWindowSink::BuildGroups() {
  groups_.clear();
  const size_t base_width = input_schema_.ColumnCount() + outputs_.size();
  std::vector<std::string> group_keys;
  group_keys.reserve(outputs_.size());
  for (size_t i = 0; i < outputs_.size(); ++i) {
    const NamedExpression& item = outputs_[i];
    if (item.expression == nullptr ||
        item.expression->Type() != TypeTag::kWindowFunctionExp) {
      return StatusError(StatusCode::kUnknown,
                         "window output is not a window call");
    }
    const WindowFunctionCallExpression& call =
        item.expression->AsWindowFunctionCallExpression();
    const std::string group_key =
        WindowOrderTermsKey(call.partition_by, call.order_by);
    auto it = std::find(group_keys.begin(), group_keys.end(), group_key);
    if (it == group_keys.end()) {
      group_keys.push_back(group_key);
      SpecGroup grp;
      grp.partition_by = call.partition_by;
      grp.order_by = call.order_by;
      const auto bind_key = [&](const Expression& key,
                                std::vector<slot_t>& slots) -> Status {
        // A plain column is already in the row. Reuse it so repeated wide
        // keys do not turn an otherwise admissible row into an oversized one.
        if (key && key->Type() == TypeTag::kColumnValue &&
            CollationOfExpression(key) == Collation::kNone) {
          const int slot =
              input_schema_.Offset(key->AsColumnValue().GetColumnName());
          if (slot >= 0 && std::cmp_less_equal(slot, std::numeric_limits<slot_t>::max())) {
            slots.push_back(static_cast<slot_t>(slot));
            return Status::kSuccess;
          }
        }
        const size_t slot = base_width + 1 + grp.computed_keys.size();
        if (slot > std::numeric_limits<slot_t>::max()) {
          return StatusError(StatusCode::kInvalidArgument,
                             "sort key column index out of range");
        }
        slots.push_back(static_cast<slot_t>(slot));
        grp.computed_keys.push_back(key);
        return Status::kSuccess;
      };
      for (const Expression& key : grp.partition_by) {
        RETURN_IF_FAIL(bind_key(key, grp.partition_slots));
      }
      for (const auto& term : grp.order_by) {
        RETURN_IF_FAIL(bind_key(term.expression, grp.order_slots));
      }
      grp.calls.push_back(CallSpec{.output_idx = i, .expr = &call});
      groups_.push_back(std::move(grp));
    } else {
      const size_t gidx = static_cast<size_t>(it - group_keys.begin());
      groups_[gidx].calls.push_back(CallSpec{.output_idx = i, .expr = &call});
    }
  }
  return Status::kSuccess;
}

Status OocWindowSink::Init(PipelineContext* ctx, size_t /*worker_count*/) {
  consumer_guard_.emplace(&ctx->grant);
  budget_bytes_ = ctx->OperatorBudgetBytes();
  if (plan_memory_budget_bytes_ > 0) {
    budget_bytes_ = budget_bytes_ > 0
                        ? std::min(budget_bytes_, plan_memory_budget_bytes_)
                        : plan_memory_budget_bytes_;
  }
  in_mem_budget_ = budget_bytes_ > 0 ? std::max<size_t>(budget_bytes_ / 2,
                                                        kMinWindowMemBudget)
                                     : (size_t{64} << 20);
  const size_t refill =
      budget_bytes_ > 0
          ? std::min<size_t>(
                QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes,
                std::max<size_t>(65536, budget_bytes_ / 4))
          : QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes;
  grant_cache_.emplace(&ctx->grant, refill);
  RETURN_IF_FAIL(BuildGroups());
  ooc_mode_ = false;
  next_seq_ = 0;
  input_rows_ = 0;
  buffered_bytes_ = 0;
  in_mem_rows_.clear();
  stage0_sorter_.reset();
  passthrough_store_.reset();
  runtime_spilled_->store(false, std::memory_order_relaxed);
  runtime_partitions_->store(0, std::memory_order_relaxed);
  runtime_peak_resident_rows_->store(0, std::memory_order_relaxed);
  return Status::kSuccess;
}

Status OocWindowSink::AppendKeyedRow(PipelineContext* ctx, Row&& base_row,
                                     uint64_t seq, const SpecGroup& group,
                                     ExternalSorter* sorter) const {
  const size_t base_cols = input_schema_.ColumnCount() + outputs_.size();
  return EncodeRowForGroup(ctx->txn, input_schema_, std::move(base_row),
                           base_cols, seq, group, sorter);
}

Status OocWindowSink::TransitionToOoc(PipelineContext* ctx) {
  if (ooc_mode_) {
    return Status::kSuccess;
  }
  ooc_mode_ = true;
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
  if (groups_.empty()) {
    SpillableStore::Options sopts;
    sopts.spill_store = &ctx->spill_store;
    sopts.memory_budget_bytes = budget_bytes_;
    sopts.grant = &ctx->grant;
    sopts.force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
    passthrough_store_ = std::make_unique<SpillableStore>(sopts);
    for (Row& r : in_mem_rows_) {
      RETURN_IF_FAIL(passthrough_store_->AppendRow(r));
    }
    in_mem_rows_.clear();
    in_mem_rows_.shrink_to_fit();
    buffered_bytes_ = 0;
    return Status::kSuccess;
  }

  const size_t input_width = input_schema_.ColumnCount();
  const size_t base_cols = input_width + outputs_.size();
  ExternalSorter::Options opts;
  opts.spill_store = &ctx->spill_store;
  opts.memory_budget_bytes = budget_bytes_;
  opts.grant = &ctx->grant;
  ASSIGN_OR_RETURN(std::vector<OocSortKey>, stage0_keys,
                   MakeSorterKeysForGroup(base_cols, groups_[0]));
  opts.keys = std::move(stage0_keys);
  opts.force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
  stage0_sorter_ = std::make_unique<ExternalSorter>(std::move(opts));

  for (Row& row : in_mem_rows_) {
    row.values_.resize(base_cols);
    RETURN_IF_FAIL(AppendKeyedRow(ctx, std::move(row), next_seq_++, groups_[0],
                                  stage0_sorter_.get()));
  }
  in_mem_rows_.clear();
  in_mem_rows_.shrink_to_fit();
  buffered_bytes_ = 0;
  return Status::kSuccess;
}

Status OocWindowSink::Consume(PipelineContext* ctx, size_t /*worker_id*/,
                              const DataChunk& input) {
  if (input.Empty()) {
    return Status::kSuccess;
  }
  input_rows_ += input.Size();
  const bool always_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
  const size_t chunk_bytes = EstimateChunkMemoryBytes(input);
  if (!ooc_mode_ &&
      (always_spill || buffered_bytes_ + chunk_bytes > in_mem_budget_ ||
       (grant_cache_.has_value() && !grant_cache_->Reserve(chunk_bytes)))) {
    RETURN_IF_FAIL(TransitionToOoc(ctx));
  }

  if (!ooc_mode_) {
    for (size_t r = 0; r < input.Size(); ++r) {
      in_mem_rows_.push_back(input.RowAt(r));
    }
    buffered_bytes_ += chunk_bytes;
    return Status::kSuccess;
  }

  if (groups_.empty()) {
    return passthrough_store_->AppendChunk(input);
  }

  const size_t base_cols = input_schema_.ColumnCount() + outputs_.size();
  for (size_t r = 0; r < input.Size(); ++r) {
    Row row = input.RowAt(r);
    row.values_.resize(base_cols);
    RETURN_IF_FAIL(AppendKeyedRow(ctx, std::move(row), next_seq_++, groups_[0],
                                  stage0_sorter_.get()));
  }
  if (stage0_sorter_->Spilled()) {
    runtime_spilled_->store(true, std::memory_order_relaxed);
  }
  return Status::kSuccess;
}

StatusOr<std::shared_ptr<PipelineSource>> OocWindowSink::Finalize(
    PipelineContext* ctx) {
  // Fast in-memory path when all input stayed well within B_op / 2.
  if (!ooc_mode_) {
    if (!in_mem_rows_.empty() && !groups_.empty()) {
      static const relational_detail::CteMap kEmptyCtes;
      if (ctx == nullptr || ctx->txn == nullptr) {
        return StatusError(
            StatusCode::kInvalidArgument,
            "OocWindowSink requires non-null TransactionContext");
      }
      for (const NamedExpression& item : outputs_) {
        const WindowFunctionCallExpression& call =
            item.expression->AsWindowFunctionCallExpression();
        std::vector<Value> computed;
        RETURN_IF_FAIL(relational_detail::ComputeOneWindow(
            *ctx->txn, call, in_mem_rows_, input_schema_, nullptr, kEmptyCtes,
            &computed));
        if (computed.size() != in_mem_rows_.size()) {
          return StatusError(StatusCode::kUnknown,
                             "window output width mismatch");
        }
        for (size_t i = 0; i < in_mem_rows_.size(); ++i) {
          in_mem_rows_[i].values_.push_back(std::move(computed[i]));
        }
      }
    }
    runtime_peak_resident_rows_->store(in_mem_rows_.size(),
                                       std::memory_order_relaxed);
    if (!in_mem_rows_.empty()) {
      runtime_partitions_->store(1, std::memory_order_relaxed);
    }
    return std::make_shared<WindowBufferedSource>(
        output_schema_, std::move(in_mem_rows_), std::move(consumer_guard_),
        std::move(grant_cache_), outputs_.size(), "Window");
  }

  if (groups_.empty()) {
    grant_cache_.reset();
    RETURN_IF_FAIL(passthrough_store_->Seal());
    if (passthrough_store_->Spilled()) {
      runtime_spilled_->store(true, std::memory_order_relaxed);
    }
    return std::make_shared<WindowPassthroughSpillSource>(
        output_schema_, std::move(passthrough_store_),
        std::move(consumer_guard_));
  }

  // OOC streaming multi-pass window execution.
  std::unique_ptr<ExternalSorter> cur_sorter = std::move(stage0_sorter_);
  RETURN_IF_FAIL(cur_sorter->Finish());
  if (cur_sorter->Spilled()) {
    runtime_spilled_->store(true, std::memory_order_relaxed);
  }

  const size_t input_width = input_schema_.ColumnCount();
  const size_t base_cols = input_width + outputs_.size();
  const size_t half_budget =
      std::max<size_t>(kMinWindowMemBudget, budget_bytes_ / 2);

  for (size_t g = 0; g + 1 < groups_.size(); ++g) {
    ExternalSorter::Options next_opts;
    next_opts.spill_store = &ctx->spill_store;
    next_opts.memory_budget_bytes = half_budget;
    next_opts.grant = &ctx->grant;
    ASSIGN_OR_RETURN(std::vector<OocSortKey>, next_keys,
                     MakeSorterKeysForGroup(base_cols, groups_[g + 1]));
    next_opts.keys = std::move(next_keys);
    next_opts.force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
    auto next_sorter = std::make_unique<ExternalSorter>(std::move(next_opts));

    RETURN_IF_FAIL(DrainSortedGroupIntoSorter(
        ctx, input_schema_, input_width, base_cols, std::move(cur_sorter),
        groups_[g], half_budget, runtime_spilled_, runtime_partitions_,
        runtime_peak_resident_rows_, groups_[g + 1], next_sorter.get()));
    cur_sorter.reset();
    RETURN_IF_FAIL(next_sorter->Finish());
    if (next_sorter->Spilled()) {
      runtime_spilled_->store(true, std::memory_order_relaxed);
    }
    cur_sorter = std::move(next_sorter);
  }

  const SpecGroup& last_group = groups_.back();
  const bool maybe_reordered = groups_.size() > 1 ||
                               !last_group.partition_by.empty() ||
                               !last_group.order_by.empty();
  const bool restore_order =
      (preserve_input_order_ || !spill_capable_plan_) && maybe_reordered;
  if (restore_order) {
    // T4855: the seq column that restores input order is indexed through
    // the uint16 OocSortKey::column_index too; reject the wrap.
    if (base_cols > std::numeric_limits<slot_t>::max()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "sort key column index out of range");
    }
    ExternalSorter::Options restore_opts;
    restore_opts.spill_store = &ctx->spill_store;
    restore_opts.memory_budget_bytes = half_budget;
    restore_opts.grant = &ctx->grant;
    restore_opts.keys = {
        OocSortKey{.column_index = static_cast<slot_t>(base_cols),
                   .ascending = true,
                   .nulls_first = true,
                   .collation = Collation::kNone}};
    restore_opts.force_spill =
        ProcessMemoryBudget::Global().AlwaysSpillEnabled();
    auto restore_sorter =
        std::make_unique<ExternalSorter>(std::move(restore_opts));

    RETURN_IF_FAIL(DrainSortedGroupIntoSorter(
        ctx, input_schema_, input_width, base_cols, std::move(cur_sorter),
        last_group, half_budget, runtime_spilled_, runtime_partitions_,
        runtime_peak_resident_rows_, std::nullopt, restore_sorter.get()));
    cur_sorter.reset();
    RETURN_IF_FAIL(restore_sorter->Finish());
    if (restore_sorter->Spilled()) {
      runtime_spilled_->store(true, std::memory_order_relaxed);
    }
    return std::make_shared<WindowRestoredOrderSource>(
        output_schema_, outputs_.size(), std::move(restore_sorter),
        std::move(consumer_guard_), runtime_spilled_, runtime_partitions_,
        runtime_peak_resident_rows_);
  }

  return std::make_shared<OocWindowStreamSource>(
      input_schema_, output_schema_, outputs_.size(), std::move(cur_sorter),
      last_group, budget_bytes_, std::move(consumer_guard_), runtime_spilled_,
      runtime_partitions_, runtime_peak_resident_rows_);
}

bool OocWindowSink::Spilled() const noexcept {
  return runtime_spilled_->load(std::memory_order_relaxed) ||
         (stage0_sorter_ != nullptr && stage0_sorter_->Spilled());
}

std::string OocWindowSink::Describe() const {
  return "WindowExecutor(outputs=" + std::to_string(outputs_.size()) + ")";
}

void OocWindowSink::Explain(std::ostream& out, int indent) const {
  out << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
      << "WindowExecutor(outputs=" << outputs_.size() << ")\n";
}

void OocWindowSink::DumpExplain(std::ostream& out, int indent) const {
  out << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
      << "WindowExecutor(outputs=" << outputs_.size();
  if (Spilled()) {
    out << ", spill=OOC, partitions="
        << runtime_partitions_->load(std::memory_order_relaxed)
        << ", resident-rows="
        << runtime_peak_resident_rows_->load(std::memory_order_relaxed);
  }
  out << ")\n";
}

}  // namespace tinylamb
