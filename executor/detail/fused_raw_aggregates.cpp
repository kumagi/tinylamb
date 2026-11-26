/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/fused_raw_aggregates.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/process.hpp"
#include "executor/detail/fused_grouped_join.hpp"
#include "executor/detail/relation.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/case_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/jit.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {
namespace {
inline bool ColumnarColMayBeDouble(const ColumnarTable& table, slot_t col) {
  for (const ColumnarChunk& ch : table.chunks) {
    if (col >= ch.columns.size()) {
      continue;
    }
    const ColumnarColumn& cc = ch.columns[col];
    if (cc.is_uniform) {
      if (cc.type == ValueType::kDouble) {
        return true;
      }
      continue;
    }
    for (size_t r = 0; r < ch.rows; ++r) {
      Value tmp;
      cc.WriteValueInto(r, &tmp);
      if (tmp.type == ValueType::kDouble) {
        return true;
      }
    }
  }
  return false;
}
}  // namespace
const char* FusedRawAggregates::Classify(const FusedAggregateInputs& input) {
  count_star_only = true;
  for (const bool count_star : input.fusion->count_star) {
    count_star_only = count_star_only && count_star;
  }
  count_nonnull_only = false;

  // A COUNT over a left-side NOT NULL / PRIMARY KEY column also counts the
  // null-extended row of an unmatched LEFT JOIN; hoisted out of the setup
  // block so the accumulate paths can apply that rule.
  unconditional_matched_count = false;
  {
    bool all_count_nonnull = !input.fusion->aggregates.empty();
    bool any_nonnull = false;
    for (size_t i = 0; i < input.fusion->aggregates.size(); ++i) {
      if (input.fusion->count_star[i]) {
        all_count_nonnull = false;
        break;
      }
      const AggregateExpression* aggregate = input.fusion->aggregates[i];
      const bool plain_column_child =
          aggregate->GetType() == AggregationType::kCount &&
          input.child_shapes[i].direct_column >= 0 && !aggregate->Distinct() &&
          !aggregate->WhereFilter() &&
          aggregate->Having() == AggregateHavingModifier::kNone &&
          aggregate->InnerOrderBy().empty() && !aggregate->SecondaryArg() &&
          aggregate->ExtraArgs().empty();
      bool eligible_column = false;
      if (plain_column_child) {
        const auto col_idx =
            static_cast<size_t>(input.child_shapes[i].direct_column);
        const Column& column = input.result.schema.GetColumn(col_idx);
        const Constraint::ConstraintType ctype = column.GetConstraint().ctype;
        if ((ctype == Constraint::kNotNull ||
             ctype == Constraint::kPrimaryKey) &&
            (col_idx < input.left_width ||
             input.join_type == JoinType::kInner)) {
          // A shared counter requires identical NULL behavior. A left
          // not-null column also counts the unmatched row of a LEFT JOIN,
          // but a right-side column can still be NULL-padded by an outer
          // join, so its declared kNotNull is not proof there.
          eligible_column = !count_right_column.has_value();
          unconditional_matched_count = true;
        } else if (col_idx >= input.left_width &&
                   !unconditional_matched_count) {
          const auto right_col =
              static_cast<slot_t>(col_idx - input.left_width);
          if (!count_right_column.has_value() ||
              *count_right_column == right_col) {
            count_right_column = right_col;
            eligible_column = true;
          }
        }
      }
      if (!eligible_column) {
        all_count_nonnull = false;
        break;
      }
      any_nonnull = true;
    }
    count_nonnull_only = all_count_nonnull && any_nonnull;
  }
  static const bool count_fusion_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_COUNT_STAR_FUSION") !=
      nullptr;
  raw_count_fusion =
      !count_fusion_disabled && (count_star_only || count_nonnull_only);

  raw_sum_fusion = !raw_count_fusion && !input.fusion->aggregates.empty() &&
                   input.fusion->aggregates.size() <= kRawAggregateCapacity;
  has_count_distinct = false;
  // Several aggregates can read the same column (SUM(c), AVG(c), MIN(c)...).
  // The DOUBLE-admixture proof below scans the whole side once per column;
  // memoize it so a column is scanned only once per fused-join planning pass.
  std::vector<int8_t> left_double_cache(input.left_width, -1);
  std::vector<int8_t> right_double_cache(input.right_width, -1);
  const auto column_may_be_double = [&](bool is_left, size_t offset) -> bool {
    auto& cache = is_left ? left_double_cache : right_double_cache;
    if (offset >= cache.size()) {
      return false;
    }
    int8_t& cached = cache[offset];
    if (cached >= 0) {
      return cached != 0;
    }
    bool may = false;
    if (is_left) {
      if (input.left.HasColumnarOnly() && input.left.columnar != nullptr) {
        may = ColumnarColMayBeDouble(*input.left.columnar,
                                     static_cast<slot_t>(offset));
      } else {
        for (const Row& r : input.left.rows) {
          if (offset < r.Count() && r[offset].type == ValueType::kDouble) {
            may = true;
            break;
          }
        }
      }
    } else if (input.right_col_only && input.right_arg.columnar != nullptr) {
      may = ColumnarColMayBeDouble(*input.right_arg.columnar,
                                   static_cast<slot_t>(offset));
    } else {
      for (const Row& r : input.right_ptr->rows) {
        if (offset < r.Count() && r[offset].type == ValueType::kDouble) {
          may = true;
          break;
        }
      }
    }
    cached = may ? 1 : 0;
    return may;
  };
  if (raw_sum_fusion) {
    for (size_t i = 0; i < input.fusion->aggregates.size(); ++i) {
      if (input.fusion->count_star[i]) {
        raw_agg_kinds[i] = RawAggKind::kCountStar;
        continue;
      }
      const AggregateExpression* agg = input.fusion->aggregates[i];
      if (agg->WhereFilter() ||
          agg->Having() != AggregateHavingModifier::kNone ||
          !agg->InnerOrderBy().empty() || agg->SecondaryArg() ||
          !agg->ExtraArgs().empty() || !agg->Child()) {
        raw_sum_fusion = false;
        break;
      }
      // A uniform numeric input column makes MIN/MAX an existence test.
      // Prove uniformity from this snapshot's resident rows, preserving NULL
      // padding and empty groups; no catalog statistic is trusted as a value.
      if (!agg->Distinct() &&
          (agg->GetType() == AggregationType::kMin ||
           agg->GetType() == AggregationType::kMax) &&
          input.child_shapes[i].direct_column >= 0) {
        const auto column =
            static_cast<size_t>(input.child_shapes[i].direct_column);
        const Relation& side =
            column < input.left_width ? input.left : input.right;
        const size_t offset =
            column < input.left_width ? column : column - input.left_width;
        const auto valid_const_type = [](const Value& v) noexcept {
          return (v.type == ValueType::kInt64 || v.type == ValueType::kDouble ||
                  v.IsNull()) &&
                 (v.type != ValueType::kDouble ||
                  !std::isnan(v.value.double_value));
        };
        const auto matches_first = [](const Value& first_val,
                                      const Value& other) noexcept {
          return first_val.type == other.type &&
                 first_val.IsUnsigned() == other.IsUnsigned() &&
                 first_val == other &&
                 (first_val.type != ValueType::kDouble ||
                  std::signbit(first_val.value.double_value) ==
                      std::signbit(other.value.double_value));
        };
        if (!side.HasSpill() && !side.HasColumnarOnly() && !side.rows.empty() &&
            side.rows.size() <= 500000) {
          const Value& value = side.rows.front()[offset];
          if (valid_const_type(value) &&
              std::ranges::all_of(side.rows, [&](const Row& row) {
                return matches_first(value, row[offset]);
              })) {
            raw_agg_kinds[i] = RawAggKind::kConstantExtremum;
            raw_constants[i] = value;
            continue;
          }
        } else if (!side.HasSpill() && side.HasColumnarOnly() &&
                   side.columnar != nullptr) {
          const size_t total_col_rows = side.TotalRows();
          if (total_col_rows > 0 && total_col_rows <= 500000) {
            bool uniform_ok = true;
            bool has_first = false;
            Value first_val;
            for (const ColumnarChunk& chunk : side.columnar->chunks) {
              if (chunk.rows == 0) {
                continue;
              }
              if (offset >= chunk.columns.size()) {
                uniform_ok = false;
                break;
              }
              const ColumnarColumn& col = chunk.columns[offset];
              if (col.encoding == ColumnarColumn::Encoding::kConstant) {
                Value cv;
                col.WriteValueInto(0, &cv);
                if (!has_first) {
                  if (!valid_const_type(cv)) {
                    uniform_ok = false;
                    break;
                  }
                  first_val = cv;
                  has_first = true;
                } else if (!matches_first(first_val, cv)) {
                  uniform_ok = false;
                  break;
                }
                continue;
              }
              for (size_t r = 0; r < chunk.rows; ++r) {
                Value rv;
                col.WriteValueInto(r, &rv);
                if (!has_first) {
                  if (!valid_const_type(rv)) {
                    uniform_ok = false;
                    break;
                  }
                  first_val = rv;
                  has_first = true;
                } else if (!matches_first(first_val, rv)) {
                  uniform_ok = false;
                  break;
                }
              }
              if (!uniform_ok) {
                break;
              }
            }
            if (uniform_ok && has_first) {
              raw_agg_kinds[i] = RawAggKind::kConstantExtremum;
              raw_constants[i] = first_val;
              continue;
            }
          }
        }
      }
      const TypeTag child_tag =
          agg->Child()->ResultType(input.result.schema).GetType();
      bool child_may_unsigned = false;
      bool child_has_mixed_double = false;
      for (const BytecodeInstruction& inst :
           input.child_programs[i].Instructions()) {
        if (inst.opcode == BytecodeOp::kLoadColumn &&
            inst.operand < input.result.schema.ColumnCount()) {
          if (input.result.schema.GetColumn(inst.operand).IsUnsigned()) {
            child_may_unsigned = true;
            break;
          }
          if (input.result.schema.GetColumn(inst.operand).Type() ==
              ValueType::kInt64) {
            child_has_mixed_double =
                child_has_mixed_double ||
                column_may_be_double(
                    inst.operand < input.left_width,
                    inst.operand < input.left_width
                        ? static_cast<size_t>(inst.operand)
                        : static_cast<size_t>(inst.operand - input.left_width));
          }
        }
      }
      if (child_has_mixed_double) {
        raw_sum_fusion = false;
        break;
      }
      for (const Value& cval : input.child_programs[i].Constants()) {
        if (!cval.IsNull() && cval.IsUnsigned()) {
          child_may_unsigned = true;
          break;
        }
      }
      if (agg->Distinct()) {
        if (agg->GetType() == AggregationType::kCount && !has_count_distinct &&
            (child_tag == TypeTag::kBigInt || child_tag == TypeTag::kInteger) &&
            !child_may_unsigned) {
          raw_agg_kinds[i] = RawAggKind::kCountDistinctInt64;
          has_count_distinct = true;
          continue;
        }
        raw_sum_fusion = false;
        break;
      }
      if (agg->GetType() == AggregationType::kCount) {
        raw_agg_kinds[i] = RawAggKind::kCountValue;
      } else if (agg->GetType() == AggregationType::kSum) {
        bool mixed_double_case = false;
        if (child_tag == TypeTag::kInvalid && !child_may_unsigned &&
            agg->Child()->Type() == TypeTag::kCaseExp) {
          const auto& ce = agg->Child()->AsCaseExpression();
          bool saw_dbl = false;
          bool all_num = !ce.when_clauses_.empty();
          auto check_br = [&](const Expression& br) {
            if (!br) {
              return;
            }
            if (br->Type() == TypeTag::kConstantValue &&
                br->AsConstantValue().GetValue().IsNull()) {
              return;
            }
            const TypeTag bt = br->ResultType(input.result.schema).GetType();
            if (bt == TypeTag::kDouble) {
              saw_dbl = true;
            } else if (bt != TypeTag::kBigInt && bt != TypeTag::kInteger) {
              all_num = false;
            }
          };
          for (const auto& w : ce.when_clauses_) {
            check_br(w.second);
          }
          check_br(ce.else_clause_);
          mixed_double_case = all_num && saw_dbl;
        }
        if (child_tag == TypeTag::kDouble || mixed_double_case) {
          raw_agg_kinds[i] = RawAggKind::kSumDouble;
        } else if ((child_tag == TypeTag::kBigInt ||
                    child_tag == TypeTag::kInteger) &&
                   !child_may_unsigned) {
          raw_agg_kinds[i] = RawAggKind::kSumInt64;
        } else {
          raw_sum_fusion = false;
          break;
        }
      } else if (agg->GetType() == AggregationType::kAvg ||
                 agg->GetType() == AggregationType::kStddevSamp ||
                 agg->GetType() == AggregationType::kStddevPop ||
                 agg->GetType() == AggregationType::kVarSamp ||
                 agg->GetType() == AggregationType::kVarPop) {
        if (child_tag == TypeTag::kDouble ||
            ((child_tag == TypeTag::kBigInt ||
              child_tag == TypeTag::kInteger) &&
             !child_may_unsigned)) {
          switch (agg->GetType()) {
            case AggregationType::kAvg:
              raw_agg_kinds[i] = RawAggKind::kAvgDouble;
              break;
            case AggregationType::kStddevSamp:
              raw_agg_kinds[i] = RawAggKind::kStddevSamp;
              break;
            case AggregationType::kStddevPop:
              raw_agg_kinds[i] = RawAggKind::kStddevPop;
              break;
            case AggregationType::kVarSamp:
              raw_agg_kinds[i] = RawAggKind::kVarSamp;
              break;
            case AggregationType::kVarPop:
              raw_agg_kinds[i] = RawAggKind::kVarPop;
              break;
            default:
              break;
          }
        } else {
          raw_sum_fusion = false;
          break;
        }
      } else {
        raw_sum_fusion = false;
        break;
      }
    }
  }
  // A parent rollup may reassociate only exact partials. The Cartesian join
  // bound covers duplicate build keys and LEFT padding without catalog PK
  // assumptions; immutable scan bounds cover every possible filtered subset.
  size_t exact_left_rows_bound = input.effective_left_rows;
  if (input.deferred_left_probe != nullptr &&
      input.deferred_left_probe->store != nullptr &&
      !input.deferred_left_probe->store->blocks.empty()) {
    const auto& probe = *input.deferred_left_probe;
    const size_t base_proj_cols = probe.projection.size();
    const ScanRuntimeFilter& primary_rf = probe.runtime_filter;
    uint64_t zm_rows = 0;
    for (const LSTablePageBlock& blk : probe.store->blocks) {
      if (blk.row_count == 0) {
        continue;
      }
      // Zone-map metadata on a block-lazy deferred store can be older than the
      // decoded rows. Only the filter's own RejectsAll() is authoritative for
      // a row-count UPPER bound: rejecting whole blocks via min/max could
      // undercount and let a wrong exact-integer-total check pass.
      bool extra_zm_reject = false;
      for (const ScanRuntimeFilter& ext : primary_rf.extra) {
        if (!ext.enabled() || ext.column >= base_proj_cols) {
          continue;
        }
        if (ext.group->RejectsAll()) {
          extra_zm_reject = true;
          break;
        }
      }
      if (!extra_zm_reject) {
        zm_rows += blk.row_count;
      }
    }
    exact_left_rows_bound = std::min<size_t>(input.effective_left_rows,
                                             static_cast<size_t>(zm_rows));
  }
  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/ExactIntTotalOverflowGuard.lean (sound)
  const auto exact_integer_total = [&](size_t aggregate) {
    constexpr uint64_t kExactLimit = uint64_t{1} << 53;
    const size_t build_bound = std::max<size_t>(1, input.right_build_count);
    if ((exact_left_rows_bound > 0 &&
         exact_left_rows_bound > kExactLimit / build_bound) ||
        input.child_shapes[aggregate].direct_column < 0) {
      return false;
    }
    const auto slot =
        static_cast<size_t>(input.child_shapes[aggregate].direct_column);
    if (input.result.schema.GetColumn(slot).Type() != ValueType::kInt64 ||
        input.result.schema.GetColumn(slot).IsUnsigned()) {
      return false;
    }
    uint64_t maximum = 0;
    const auto magnitude = [](int64_t value) {
      const auto bits = static_cast<uint64_t>(value);
      return value < 0 ? uint64_t{0} - bits : bits;
    };
    if (slot < input.left_width && input.deferred_left_probe != nullptr) {
      const auto& probe = *input.deferred_left_probe;
      if (!probe.store || slot >= probe.projection.size() ||
          std::ranges::any_of(probe.pk_lookups, [&](const auto& lookup) {
            return lookup.target_proj_col == slot;
          })) {
        return false;
      }
      const auto& store = *probe.store;
      const slot_t column = probe.projection[slot];
      if (!store.HasColumn(column) || store.blocks.empty() ||
          column >= store.column_min_int.size() ||
          column >= store.column_max_int.size() ||
          column >= store.blocks.front().columns.size() ||
          store.blocks.front().columns[column].type != ValueType::kInt64 ||
          store.blocks.front().columns[column].is_unsigned ||
          store.column_min_int[column] > store.column_max_int[column]) {
        return false;
      }
      if (exact_left_rows_bound == 0) {
        // The deferred store path derived this bound from every block's
        // runtime-filter zone maps, so zero proves there are no joined pairs.
        return true;
      }
      maximum = std::max(magnitude(store.column_min_int[column]),
                         magnitude(store.column_max_int[column]));
      const uint64_t pairs =
          static_cast<uint64_t>(exact_left_rows_bound) * build_bound;
      const uint64_t max_allowed = kExactLimit / pairs;
      return maximum <= max_allowed;
    }
    // Without the deferred-store zone-map proof, keep the zero-bound case on
    // the conservative fallback path.
    if (exact_left_rows_bound == 0) {
      return false;
    }
    const uint64_t pairs =
        static_cast<uint64_t>(exact_left_rows_bound) * build_bound;
    const uint64_t max_allowed = kExactLimit / pairs;
    const Relation& side =
        slot < input.left_width ? input.left : *input.right_ptr;
    const size_t column =
        slot < input.left_width ? slot : slot - input.left_width;
    if (side.HasSpill()) {
      return false;
    }
    if (side.HasColumnarOnly()) {
      const auto check_chunk = [&](const ColumnarChunk& ch,
                                   uint64_t* local_max) -> bool {
        if (column >= ch.columns.size()) {
          return false;
        }
        if (ch.rows == 0) {
          return true;
        }
        const ColumnarColumn& cc = ch.columns[column];
        if (cc.encoding == ColumnarColumn::Encoding::kConstant) {
          if (cc.IsNull(0)) {
            return true;
          }
          if (cc.is_uniform) {
            if (cc.type != ValueType::kInt64 || cc.is_unsigned) {
              return false;
            }
            *local_max = std::max(*local_max, magnitude(cc.IntAt(0)));
          } else {
            Value tmp;
            cc.WriteValueInto(0, &tmp);
            if (!tmp.IsNull()) {
              if (tmp.type != ValueType::kInt64 || tmp.IsUnsigned()) {
                return false;
              }
              *local_max = std::max(*local_max, magnitude(tmp.value.int_value));
            }
          }
          return *local_max <= max_allowed;
        }
        if (cc.encoding == ColumnarColumn::Encoding::kFlat && cc.is_uniform &&
            cc.type == ValueType::kInt64 && !cc.is_unsigned &&
            cc.ints.size() >= ch.rows &&
            (!cc.has_null || cc.nulls.empty() || cc.nulls.size() >= ch.rows)) {
          const int64_t* ptr = cc.ints.data();
          if (!cc.has_null || cc.nulls.empty()) {
            int64_t lo = std::numeric_limits<int64_t>::max();
            int64_t hi = std::numeric_limits<int64_t>::min();
            for (size_t r = 0; r < ch.rows; ++r) {
              const int64_t v = ptr[r];
              lo = v < lo ? v : lo;
              hi = v > hi ? v : hi;
            }
            *local_max = std::max({*local_max, magnitude(lo), magnitude(hi)});
            return *local_max <= max_allowed;
          }
          const uint8_t* nul = cc.nulls.data();
          bool seen = false;
          int64_t lo = std::numeric_limits<int64_t>::max();
          int64_t hi = std::numeric_limits<int64_t>::min();
          for (size_t r = 0; r < ch.rows; ++r) {
            if (nul[r] == 0) {
              const int64_t v = ptr[r];
              if (!seen) {
                lo = hi = v;
                seen = true;
              } else {
                lo = v < lo ? v : lo;
                hi = v > hi ? v : hi;
              }
            }
          }
          if (seen) {
            *local_max = std::max({*local_max, magnitude(lo), magnitude(hi)});
            return *local_max <= max_allowed;
          }
          return true;
        }
        for (size_t r = 0; r < ch.rows; ++r) {
          if (cc.IsNull(r)) {
            continue;
          }
          if (cc.is_uniform) {
            if (cc.type != ValueType::kInt64 || cc.is_unsigned) {
              return false;
            }
            *local_max = std::max(*local_max, magnitude(cc.IntAt(r)));
          } else {
            Value tmp;
            cc.WriteValueInto(r, &tmp);
            if (tmp.IsNull()) {
              continue;
            }
            if (tmp.type != ValueType::kInt64 || tmp.IsUnsigned()) {
              return false;
            }
            *local_max = std::max(*local_max, magnitude(tmp.value.int_value));
          }
          if (*local_max > max_allowed) {
            return false;
          }
        }
        return true;
      };
      const auto& chunks = side.columnar->chunks;
      const size_t total_rows = side.TotalRows();
      if (total_rows >= 65536 && chunks.size() > 1) {
        const size_t workers = std::min(
            chunks.size(),
            std::max<size_t>(1, ParallelWorkerCount(total_rows / 65536)));
        if (workers > 1) {
          std::atomic<size_t> next_chunk{0};
          std::atomic<bool> ok{true};
          std::vector<uint64_t> local_maxes(workers, 0);
          const Status pst = RunParallelTasks(workers, [&](size_t w) {
            uint64_t lmax = 0;
            while (ok.load(std::memory_order_relaxed)) {
              const size_t ci =
                  next_chunk.fetch_add(1, std::memory_order_relaxed);
              if (ci >= chunks.size()) {
                break;
              }
              if (!check_chunk(chunks[ci], &lmax)) {
                ok.store(false, std::memory_order_relaxed);
                return;
              }
            }
            local_maxes[w] = lmax;
          });
          if (!pst.ok() || !ok.load(std::memory_order_relaxed)) {
            return false;
          }
          for (uint64_t lm : local_maxes) {
            maximum = std::max(maximum, lm);
          }
        } else {
          for (const ColumnarChunk& ch : chunks) {
            if (!check_chunk(ch, &maximum)) {
              return false;
            }
          }
        }
      } else {
        for (const ColumnarChunk& ch : chunks) {
          if (!check_chunk(ch, &maximum)) {
            return false;
          }
        }
      }
    } else {
      for (const Row& row : side.rows) {
        const Value& value = row[column];
        if (value.IsNull()) {
          continue;
        }
        if (value.type != ValueType::kInt64 || value.IsUnsigned()) {
          return false;
        }
        maximum = std::max(maximum, magnitude(value.value.int_value));
        if (maximum > max_allowed) {
          return false;
        }
      }
    }
    // Division avoids overflow even for INT64_MIN and excessive cardinality.
    return maximum <= max_allowed;
  };
  if (input.fusion->require_exact_integer_totals) {
    const size_t build_bound = std::max<size_t>(1, input.right_build_count);
    if (!raw_sum_fusion ||
        exact_left_rows_bound > (uint64_t{1} << 53) / build_bound) {
      return "unproved partial totals";
    }
    for (size_t a = 0; a < input.fusion->aggregates.size(); ++a) {
      if (raw_agg_kinds[a] == RawAggKind::kCountStar ||
          raw_agg_kinds[a] == RawAggKind::kCountValue) {
        continue;
      }
      if (raw_agg_kinds[a] != RawAggKind::kSumInt64 ||
          !exact_integer_total(a)) {
        return "unproved partial integer sum";
      }
    }
  }

  raw_agg_slot.fill(-1);

  raw_slot_count = 0;
  if (raw_sum_fusion) {
    const bool allow_const_extremum_fold =
        !input.fusion->group_keys.empty() &&
        input.join_type == JoinType::kInner &&
        !input.fusion->rollup_count_distribution;
    const auto raw_family = [](RawAggKind kind) noexcept -> uint8_t {
      switch (kind) {
        case RawAggKind::kSumDouble:
        case RawAggKind::kAvgDouble:
          return 1;
        case RawAggKind::kStddevSamp:
        case RawAggKind::kStddevPop:
        case RawAggKind::kVarSamp:
        case RawAggKind::kVarPop:
          return 2;
        case RawAggKind::kSumInt64:
          return 3;
        case RawAggKind::kCountStar:
          return 4;
        case RawAggKind::kCountValue:
          return 5;
        case RawAggKind::kConstantExtremum:
          return 6;
        case RawAggKind::kCountDistinctInt64:
          return 7;
        case RawAggKind::kNone:
          return 0;
      }
      return 0;
    };
    const auto same_value_bits = [](const Value& a,
                                    const Value& b) noexcept -> bool {
      if (a.type != b.type || a.IsUnsigned() != b.IsUnsigned() ||
          a.IsInterval() != b.IsInterval()) {
        return false;
      }
      if (a.type == ValueType::kDouble) {
        return std::bit_cast<uint64_t>(a.value.double_value) ==
               std::bit_cast<uint64_t>(b.value.double_value);
      }
      return a == b;
    };
    const auto same_child_program = [&](size_t a, size_t b) -> bool {
      if (input.fusion->count_star[a] != input.fusion->count_star[b]) {
        return false;
      }
      if (input.fusion->count_star[a]) {
        return true;
      }
      const BytecodeProgram& pa = input.child_programs[a];
      const BytecodeProgram& pb = input.child_programs[b];
      // Lean proof: lean/TinylambOpt/ResearchProofs/BytecodeSideTableEquiv.lean
      //   (sound)
      if (!pa.Casts().empty() || !pb.Casts().empty() ||
          !pa.StringSets().empty() || !pb.StringSets().empty() ||
          !pa.RowInMasks().empty() || !pb.RowInMasks().empty() ||
          pa.ResultType() != pb.ResultType()) {
        return false;
      }
      const auto& ia = pa.Instructions();
      const auto& ib = pb.Instructions();
      if (ia.size() != ib.size()) {
        return false;
      }
      for (size_t k = 0; k < ia.size(); ++k) {
        if (ia[k].opcode != ib[k].opcode || ia[k].operand != ib[k].operand ||
            ia[k].binary != ib[k].binary || ia[k].unary != ib[k].unary ||
            ia[k].jump_target != ib[k].jump_target) {
          return false;
        }
      }
      const auto& ca = pa.Constants();
      const auto& cb = pb.Constants();
      if (ca.size() != cb.size()) {
        return false;
      }
      for (size_t k = 0; k < ca.size(); ++k) {
        if (!same_value_bits(ca[k], cb[k])) {
          return false;
        }
      }
      const auto& ra = pa.Int64Ranges();
      const auto& rb = pb.Int64Ranges();
      if (ra.size() != rb.size()) {
        return false;
      }
      for (size_t k = 0; k < ra.size(); ++k) {
        if (ra[k].lo != rb[k].lo || ra[k].hi != rb[k].hi ||
            ra[k].expected_type != rb[k].expected_type) {
          return false;
        }
      }
      const auto& rca = pa.Reciprocals();
      const auto& rcb = pb.Reciprocals();
      if (rca.size() != rcb.size()) {
        return false;
      }
      for (size_t k = 0; k < rca.size(); ++k) {
        if (rca[k].divisor != rcb[k].divisor || rca[k].abs_d != rcb[k].abs_d ||
            rca[k].recip != rcb[k].recip) {
          return false;
        }
      }
      return true;
    };
    for (size_t i = 0; i < input.fusion->aggregates.size(); ++i) {
      if (allow_const_extremum_fold &&
          raw_agg_kinds[i] == RawAggKind::kConstantExtremum &&
          !raw_constants[i].IsNull()) {
        raw_const_folded[i] = true;
        raw_agg_slot[i] = -1;
        continue;
      }
      int8_t matched_slot = -1;
      const uint8_t fam = raw_family(raw_agg_kinds[i]);
      if (fam != 0 && fam != 7) {
        for (size_t s = 0; s < raw_slot_count; ++s) {
          const size_t src = raw_slot_origin[s];
          if (raw_family(raw_agg_kinds[src]) == fam &&
              same_child_program(i, src)) {
            matched_slot = static_cast<int8_t>(s);
            break;
          }
        }
      }
      if (matched_slot >= 0) {
        raw_agg_slot[i] = matched_slot;
      } else {
        const size_t s = raw_slot_count++;
        raw_slot_origin[s] = static_cast<uint8_t>(i);
        slot_agg_kinds[s] = raw_agg_kinds[i];
        raw_agg_slot[i] = static_cast<int8_t>(s);
      }
    }
  }
  // SUM raises kIsInfinity on an infinite total but AVG returns it as a value
  // (aggregate_accumulator.cpp), so a slot consumed only by AVG(DOUBLE) must
  // not turn its sticky inf flag into a query error.  A slot shared with any
  // other aggregate kind keeps raising.

  if (raw_sum_fusion) {
    for (size_t i = 0;
         i < input.fusion->aggregates.size() && i < raw_const_folded.size();
         ++i) {
      if (raw_const_folded[i] || raw_agg_slot[i] < 0) {
        continue;
      }
      if (raw_agg_kinds[i] != RawAggKind::kAvgDouble) {
        const auto slot_idx =
            static_cast<size_t>(static_cast<unsigned char>(raw_agg_slot[i]));
        if (slot_idx < slot_raises_inf.size()) {
          slot_raises_inf[slot_idx] = true;
        }
      }
    }
  }
  skip_accumulator_states =
      raw_count_fusion || raw_sum_fusion || input.fusion->aggregates.empty();
  return nullptr;
}
}  // namespace tinylamb::relational_detail
