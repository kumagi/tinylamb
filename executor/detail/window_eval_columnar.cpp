/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cctype>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/parallel_work.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/detail/window_eval_internal.hpp"
#include "expression/expression.hpp"
#include "expression/window_function_expression.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb::relational_detail {

namespace {

uint64_t SplitMix64Window(uint64_t x) noexcept {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

uint64_t EncodeSignedIntOrderKey(int64_t v, bool ascending) noexcept {
  const uint64_t u = static_cast<uint64_t>(v) ^ (1ULL << 63);
  return ascending ? u : ~u;
}

uint64_t EncodeDoubleOrderKey(double d, bool ascending) noexcept {
  uint64_t u = 0;
  if (std::isnan(d)) {
    u = 0;
  } else {
    const auto norm = std::bit_cast<uint64_t>(d == 0.0 ? 0.0 : d);
    constexpr uint64_t kSign = 1ULL << 63;
    u = (norm & kSign) != 0 ? ~norm : (norm ^ kSign);
  }
  return ascending ? u : ~u;
}

bool CanReadColumnAsFlatNumeric(const ColumnarTable& table, size_t slot,
                                ValueType* detected_type) noexcept {
  ValueType seen = ValueType::kNull;
  for (const ColumnarChunk& ch : table.chunks) {
    if (ch.rows == 0) {
      continue;
    }
    if (slot >= ch.columns.size()) {
      return false;
    }
    const ColumnarColumn& c = ch.columns[slot];
    if (!c.is_uniform || c.is_unsigned || c.is_interval) {
      return false;
    }
    if (c.type == ValueType::kNull) {
      continue;
    }
    if (c.type != ValueType::kInt64 && c.type != ValueType::kDate &&
        c.type != ValueType::kDouble) {
      return false;
    }
    if (seen == ValueType::kNull) {
      seen = c.type;
    } else if (seen != c.type) {
      return false;
    }
  }
  // Empty/all-NULL columns have no flat numeric/text representation.
  if (seen == ValueType::kNull) {
    return false;
  }
  *detected_type = seen;
  return true;
}

template <typename T>
struct FlatNumericColumn {
  const T* vals{nullptr};
  const uint8_t* nulls{nullptr};
  bool any_null{false};
  std::vector<T> owned_vals;
  std::vector<uint8_t> owned_nulls;

  Status Load(const ColumnarTable& table, size_t slot, size_t n) {
    if (table.chunks.size() == 1 && table.chunks[0].rows == n) {
      const ColumnarColumn& c = table.chunks[0].columns[slot];
      if (c.encoding == ColumnarColumn::Encoding::kFlat && c.is_uniform &&
          c.type != ValueType::kNull && (!c.has_null || c.nulls.size() == n)) {
        if constexpr (std::is_same_v<T, int64_t>) {
          if (c.ints.size() == n) {
            vals = c.ints.data();
            if (c.has_null) {
              any_null = true;
              nulls = c.nulls.data();
            }
            return Status::kSuccess;
          }
        } else {
          if (c.doubles.size() == n) {
            vals = c.doubles.data();
            if (c.has_null) {
              any_null = true;
              nulls = c.nulls.data();
            }
            return Status::kSuccess;
          }
        }
      }
    }

    owned_vals.assign(n, T{0});
    bool may_have_null = false;
    for (const ColumnarChunk& ch : table.chunks) {
      if (ch.rows == 0) {
        continue;
      }
      const ColumnarColumn& c = ch.columns[slot];
      if (c.type == ValueType::kNull || c.has_null ||
          c.encoding == ColumnarColumn::Encoding::kGathered ||
          (c.encoding == ColumnarColumn::Encoding::kConstant &&
           c.const_is_null)) {
        may_have_null = true;
        break;
      }
    }
    if (may_have_null) {
      owned_nulls.assign(n, 0);
    }

    std::vector<size_t> chunk_offsets(table.chunks.size(), 0);
    size_t acc = 0;
    for (size_t c_idx = 0; c_idx < table.chunks.size(); ++c_idx) {
      chunk_offsets[c_idx] = acc;
      acc += table.chunks[c_idx].rows;
    }

    std::atomic<bool> saw_null{false};
    const size_t workers =
        (table.chunks.size() < 4 || n < 16384)
            ? 1
            : std::min<size_t>({8, table.chunks.size(),
                                QueryParallelWorkerLimit(
                                    std::thread::hardware_concurrency())});
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
      bool local_null = false;
      for (size_t ci = worker; ci < table.chunks.size(); ci += workers) {
        const ColumnarChunk& ch = table.chunks[ci];
        if (ch.rows == 0) {
          continue;
        }
        const size_t off = chunk_offsets[ci];
        const ColumnarColumn& c = ch.columns[slot];
        if (c.type == ValueType::kNull ||
            (c.encoding == ColumnarColumn::Encoding::kConstant &&
             c.const_is_null)) {
          std::fill_n(owned_nulls.data() + off, ch.rows, uint8_t{1});
          local_null = true;
          continue;
        }
        if (c.encoding == ColumnarColumn::Encoding::kConstant) {
          T cv{};
          if constexpr (std::is_same_v<T, int64_t>) {
            cv = c.const_int;
          } else {
            cv = c.const_double;
          }
          std::fill_n(owned_vals.data() + off, ch.rows, cv);
          continue;
        }
        if (c.encoding == ColumnarColumn::Encoding::kFlat && c.is_uniform) {
          if constexpr (std::is_same_v<T, int64_t>) {
            std::memcpy(owned_vals.data() + off, c.ints.data(),
                        ch.rows * sizeof(T));
          } else {
            std::memcpy(owned_vals.data() + off, c.doubles.data(),
                        ch.rows * sizeof(T));
          }
          if (c.has_null && c.nulls.size() == ch.rows) {
            for (size_t r = 0; r < ch.rows; ++r) {
              if (c.nulls[r] != 0) {
                owned_nulls[off + r] = 1;
                local_null = true;
              }
            }
          }
          continue;
        }
        const bool check_null =
            c.has_null || c.encoding == ColumnarColumn::Encoding::kGathered;
        for (size_t r = 0; r < ch.rows; ++r) {
          if (check_null && c.IsNull(r)) {
            owned_nulls[off + r] = 1;
            local_null = true;
          } else if constexpr (std::is_same_v<T, int64_t>) {
            owned_vals[off + r] = c.IntAt(r);
          } else {
            owned_vals[off + r] = c.DoubleAt(r);
          }
        }
      }
      if (local_null) {
        saw_null.store(true, std::memory_order_relaxed);
      }
    }));

    any_null = saw_null.load(std::memory_order_relaxed);
    if (!any_null) {
      std::vector<uint8_t>().swap(owned_nulls);
      nulls = nullptr;
    } else {
      nulls = owned_nulls.data();
    }
    vals = owned_vals.data();
    return Status::kSuccess;
  }

  [[nodiscard]] bool IsNull(size_t r) const noexcept {
    return nulls != nullptr && nulls[r] != 0;
  }
};

struct FlatBucketItem {
  int64_t part_key{0};
  uint64_t ord_key{0};
  uint32_t row{0};
  uint8_t flags{0};  // bit 0: part null, bit 1: ord null

  static constexpr uint8_t kPartNull = 1;
  static constexpr uint8_t kOrdNull = 2;

  [[nodiscard]] bool PartNull() const noexcept {
    return (flags & kPartNull) != 0;
  }
  [[nodiscard]] bool OrdNull() const noexcept {
    return (flags & kOrdNull) != 0;
  }
};

struct FlatWindowSpec {
  enum class Kind : uint8_t { kRowNumber, kRank, kDenseRank, kPrefix };
  Kind kind{Kind::kPrefix};
  PrefixOp prefix_op{PrefixOp::kSum};
  PrefixFrameMode frame_mode{PrefixFrameMode::kRows};
  size_t arg_slot{SIZE_MAX};
  ValueType arg_type{ValueType::kNull};
  ValueType out_type{ValueType::kNull};
  size_t col_data_idx{SIZE_MAX};
  std::string fn_name;
};

Status AssembleColumnarWindowOutput(
    Relation&& input, std::span<const size_t> kept_slots,
    std::vector<Column> extended_columns, bool propagated_uniform,
    const std::function<void(size_t chunk_idx, size_t row_off,
                             size_t chunk_rows, ColumnarChunk* dst_chunk)>&
        append_win_cols,
    Relation* out) {
  const auto& src_table = *input.columnar;
  auto out_table = std::make_shared<ColumnarTable>();
  out_table->keepalives.reserve(src_table.keepalives.size() + 1);
  for (const auto& owner : src_table.keepalives) {
    if (owner != nullptr) {
      out_table->keepalives.push_back(owner);
    }
  }
  out_table->AppendKeepalive(input.columnar);
  out_table->chunks.reserve(src_table.chunks.size());

  size_t row_off = 0;
  for (size_t ci = 0; ci < src_table.chunks.size(); ++ci) {
    const ColumnarChunk& src_chunk = src_table.chunks[ci];
    if (src_chunk.rows == 0) {
      continue;
    }
    ColumnarChunk dst_chunk;
    dst_chunk.rows = src_chunk.rows;
    dst_chunk.columns.resize(extended_columns.size());
    for (size_t k = 0; k < kept_slots.size(); ++k) {
      dst_chunk.columns[k] = src_chunk.columns[kept_slots[k]];
    }
    append_win_cols(ci, row_off, src_chunk.rows, &dst_chunk);
    row_off += src_chunk.rows;
    out_table->AppendChunk(std::move(dst_chunk));
  }

  CopyExecutionStats(out, input);
  out->schema = Schema(input.schema.Name(), std::move(extended_columns));
  input.ResetContents();
  out->SetColumnarTable(std::move(out_table), propagated_uniform);
  return Status::kSuccess;
}

StatusOr<std::optional<Relation>> TryComputeFlatColumnarWindows(
    Relation& input,
    std::span<const WindowFunctionCallExpression* const> win_nodes,
    std::span<const std::string> win_names,
    std::span<const size_t> kept_slots) {
  const ColumnarTable& table = *input.columnar;
  const size_t n = table.total_rows;
  const size_t win_count = win_nodes.size();
  if (n == 0 || win_count == 0 || win_count > 32 ||
      n > std::numeric_limits<uint32_t>::max()) {
    return std::optional<Relation>{std::nullopt};
  }
  const Schema& base_schema = input.schema;
  const WindowFunctionCallExpression& w0 = *win_nodes[0];
  const std::string first_layout_key = WindowLayoutKey(w0);
  for (size_t w = 1; w < win_count; ++w) {
    if (WindowLayoutKey(*win_nodes[w]) != first_layout_key) {
      return std::optional<Relation>{std::nullopt};
    }
  }

  if (w0.partition_by.size() > 1 || w0.order_by.size() > 1) {
    return std::optional<Relation>{std::nullopt};
  }
  bool has_part = !w0.partition_by.empty();
  size_t part_slot = SIZE_MAX;
  ValueType part_vt = ValueType::kNull;
  if (has_part) {
    if (!w0.partition_by[0] ||
        w0.partition_by[0]->Type() != TypeTag::kColumnValue) {
      return std::optional<Relation>{std::nullopt};
    }
    const auto slot_opt = LocalColumnOffset(
        base_schema, w0.partition_by[0]->AsColumnValue().GetColumnName());
    if (!slot_opt.has_value() ||
        !CanReadColumnAsFlatNumeric(table, *slot_opt, &part_vt) ||
        (part_vt != ValueType::kInt64 && part_vt != ValueType::kDate &&
         part_vt != ValueType::kNull)) {
      return std::optional<Relation>{std::nullopt};
    }
    part_slot = *slot_opt;
    if (part_vt == ValueType::kNull) {
      has_part = false;
    }
  }

  bool has_order = !w0.order_by.empty();
  size_t ord_slot = SIZE_MAX;
  ValueType ord_vt = ValueType::kNull;
  bool ord_asc = true;
  bool ord_nulls_first = true;
  if (has_order) {
    if (!w0.order_by[0].expression ||
        w0.order_by[0].expression->Type() != TypeTag::kColumnValue) {
      return std::optional<Relation>{std::nullopt};
    }
    const auto slot_opt = LocalColumnOffset(
        base_schema,
        w0.order_by[0].expression->AsColumnValue().GetColumnName());
    if (!slot_opt.has_value() ||
        !CanReadColumnAsFlatNumeric(table, *slot_opt, &ord_vt)) {
      return std::optional<Relation>{std::nullopt};
    }
    ord_slot = *slot_opt;
    ord_asc = w0.order_by[0].ascending;
    ord_nulls_first = w0.order_by[0].nulls_first.value_or(ord_asc);
    if (ord_vt == ValueType::kNull) {
      has_order = false;
    }
  }

  std::vector<FlatWindowSpec> specs(win_count);
  std::vector<size_t> int_col_slots;
  std::vector<size_t> dbl_col_slots;
  auto register_int_col = [&](size_t slot) -> size_t {
    for (size_t i = 0; i < int_col_slots.size(); ++i) {
      if (int_col_slots[i] == slot) {
        return i;
      }
    }
    int_col_slots.push_back(slot);
    return int_col_slots.size() - 1;
  };
  auto register_dbl_col = [&](size_t slot) -> size_t {
    for (size_t i = 0; i < dbl_col_slots.size(); ++i) {
      if (dbl_col_slots[i] == slot) {
        return i;
      }
    }
    dbl_col_slots.push_back(slot);
    return dbl_col_slots.size() - 1;
  };

  for (size_t w = 0; w < win_count; ++w) {
    const WindowFunctionCallExpression& win = *win_nodes[w];
    FlatWindowSpec& spec = specs[w];
    spec.fn_name = win.function;
    if ((win.function == "ROW_NUMBER" || win.function == "RANK" ||
         win.function == "DENSE_RANK") &&
        win.args.empty() && !win.where_filter && !win.distinct &&
        win.inner_order_by.empty() && !win.inner_limit) {
      spec.kind =
          win.function == "ROW_NUMBER"
              ? FlatWindowSpec::Kind::kRowNumber
              : (win.function == "RANK" ? FlatWindowSpec::Kind::kRank
                                        : FlatWindowSpec::Kind::kDenseRank);
      spec.out_type = ValueType::kInt64;
      continue;
    }
    std::optional<size_t> arg_slot;
    if (!IsPrefixWindowEligible(win, base_schema, &arg_slot)) {
      return std::optional<Relation>{std::nullopt};
    }
    if (!arg_slot.has_value()) {
      return std::optional<Relation>{std::nullopt};
    }
    const size_t resolved_arg_slot = *arg_slot;
    ValueType arg_vt = ValueType::kNull;
    if (!CanReadColumnAsFlatNumeric(table, resolved_arg_slot, &arg_vt)) {
      return std::optional<Relation>{std::nullopt};
    }
    spec.kind = FlatWindowSpec::Kind::kPrefix;
    spec.prefix_op = ClassifyPrefixOp(win.function);
    spec.frame_mode = ClassifyPrefixFrame(win);
    spec.arg_slot = resolved_arg_slot;
    spec.arg_type = arg_vt;
    if (arg_vt == ValueType::kNull) {
      spec.out_type = spec.prefix_op == PrefixOp::kCount ? ValueType::kInt64
                                                         : ValueType::kNull;
    } else if (spec.prefix_op == PrefixOp::kCount) {
      spec.out_type = ValueType::kInt64;
      spec.col_data_idx = (arg_vt == ValueType::kDouble)
                              ? register_dbl_col(resolved_arg_slot)
                              : register_int_col(resolved_arg_slot);
    } else if (spec.prefix_op == PrefixOp::kSum) {
      if (arg_vt == ValueType::kDate) {
        return std::optional<Relation>{std::nullopt};
      }
      spec.out_type = arg_vt;
      spec.col_data_idx = (arg_vt == ValueType::kDouble)
                              ? register_dbl_col(resolved_arg_slot)
                              : register_int_col(resolved_arg_slot);
    } else if (spec.prefix_op == PrefixOp::kAvg) {
      if (arg_vt == ValueType::kDate) {
        return std::optional<Relation>{std::nullopt};
      }
      spec.out_type = ValueType::kDouble;
      spec.col_data_idx = (arg_vt == ValueType::kDouble)
                              ? register_dbl_col(resolved_arg_slot)
                              : register_int_col(resolved_arg_slot);
    } else {
      spec.out_type = arg_vt;
      spec.col_data_idx = (arg_vt == ValueType::kDouble)
                              ? register_dbl_col(resolved_arg_slot)
                              : register_int_col(resolved_arg_slot);
    }
  }

  FlatNumericColumn<int64_t> part_col;
  if (has_part) {
    RETURN_IF_FAIL(part_col.Load(table, part_slot, n));
  }
  FlatNumericColumn<int64_t> ord_int_col;
  FlatNumericColumn<double> ord_dbl_col;
  if (has_order) {
    if (ord_vt == ValueType::kDouble) {
      RETURN_IF_FAIL(ord_dbl_col.Load(table, ord_slot, n));
    } else {
      RETURN_IF_FAIL(ord_int_col.Load(table, ord_slot, n));
    }
  }
  std::vector<FlatNumericColumn<int64_t>> int_arg_cols(int_col_slots.size());
  for (size_t i = 0; i < int_col_slots.size(); ++i) {
    RETURN_IF_FAIL(int_arg_cols[i].Load(table, int_col_slots[i], n));
  }
  std::vector<FlatNumericColumn<double>> dbl_arg_cols(dbl_col_slots.size());
  for (size_t i = 0; i < dbl_col_slots.size(); ++i) {
    RETURN_IF_FAIL(dbl_arg_cols[i].Load(table, dbl_col_slots[i], n));
  }

  std::vector<std::vector<int64_t>> out_ints(win_count);
  std::vector<std::vector<double>> out_doubles(win_count);
  std::vector<std::vector<uint8_t>> out_nulls(win_count);
  std::vector<uint8_t> win_any_null(win_count, 0);
  std::vector<uint8_t> win_nullable(win_count, 0);
  for (size_t w = 0; w < win_count; ++w) {
    const FlatWindowSpec& spec = specs[w];
    if (spec.out_type == ValueType::kNull) {
      win_any_null[w] = 1;
      continue;
    }
    if (spec.out_type == ValueType::kDouble) {
      out_doubles[w].resize(n);
    } else {
      out_ints[w].resize(n);
    }
    const bool may_emit_null = (spec.kind == FlatWindowSpec::Kind::kPrefix &&
                                spec.prefix_op != PrefixOp::kCount);
    if (may_emit_null) {
      win_nullable[w] = 1;
      out_nulls[w].assign(n, 0);
    }
  }

  std::vector<std::string> overflow_errs(win_count);
  auto eval_partition = [&](const FlatBucketItem* items, size_t p_begin,
                            size_t p_end, std::vector<uint8_t>& local_any_null,
                            size_t& first_overflow_win) -> bool {
    const size_t m = p_end - p_begin;
    for (size_t w = 0; w < win_count; ++w) {
      const FlatWindowSpec& spec = specs[w];
      if (spec.out_type == ValueType::kNull) {
        continue;
      }
      if (spec.kind == FlatWindowSpec::Kind::kRowNumber) {
        int64_t* dst = out_ints[w].data();
        for (size_t k = 0; k < m; ++k) {
          dst[items[p_begin + k].row] = static_cast<int64_t>(k + 1);
        }
        continue;
      }
      if (spec.kind == FlatWindowSpec::Kind::kRank ||
          spec.kind == FlatWindowSpec::Kind::kDenseRank) {
        int64_t* dst = out_ints[w].data();
        if (!has_order) {
          for (size_t k = 0; k < m; ++k) {
            dst[items[p_begin + k].row] = 1;
          }
          continue;
        }
        int64_t dense = 0;
        size_t k = 0;
        while (k < m) {
          const FlatBucketItem& cur = items[p_begin + k];
          size_t end = k;
          while (end + 1 < m) {
            const FlatBucketItem& nxt = items[p_begin + end + 1];
            if (cur.OrdNull() != nxt.OrdNull() ||
                (!cur.OrdNull() && cur.ord_key != nxt.ord_key)) {
              break;
            }
            ++end;
          }
          ++dense;
          const int64_t val = spec.kind == FlatWindowSpec::Kind::kRank
                                  ? static_cast<int64_t>(k + 1)
                                  : dense;
          for (size_t p = k; p <= end; ++p) {
            dst[items[p_begin + p].row] = val;
          }
          k = end + 1;
        }
        continue;
      }

      if (spec.arg_type == ValueType::kNull) {
        if (spec.prefix_op == PrefixOp::kCount) {
          int64_t* dst = out_ints[w].data();
          for (size_t k = 0; k < m; ++k) {
            dst[items[p_begin + k].row] = 0;
          }
        }
        continue;
      }

      const PrefixFrameMode eff_mode =
          (!has_order && spec.frame_mode == PrefixFrameMode::kPeer)
              ? PrefixFrameMode::kWholePartition
              : spec.frame_mode;
      uint8_t* dst_null = win_nullable[w] != 0 ? out_nulls[w].data() : nullptr;

      auto run_prefix = [&](auto&& advance, auto&& emit_row) -> bool {
        if (eff_mode == PrefixFrameMode::kWholePartition) {
          for (size_t k = 0; k < m; ++k) {
            if (!advance(items[p_begin + k].row)) {
              return false;
            }
          }
          for (size_t k = 0; k < m; ++k) {
            emit_row(items[p_begin + k].row);
          }
          return true;
        }
        if (eff_mode == PrefixFrameMode::kRows) {
          for (size_t k = 0; k < m; ++k) {
            const uint32_t r = items[p_begin + k].row;
            if (!advance(r)) {
              return false;
            }
            emit_row(r);
          }
          return true;
        }
        size_t k = 0;
        while (k < m) {
          const FlatBucketItem& cur = items[p_begin + k];
          size_t end = k;
          while (end + 1 < m) {
            const FlatBucketItem& nxt = items[p_begin + end + 1];
            if (cur.OrdNull() != nxt.OrdNull() ||
                (!cur.OrdNull() && cur.ord_key != nxt.ord_key)) {
              break;
            }
            ++end;
          }
          for (size_t p = k; p <= end; ++p) {
            if (!advance(items[p_begin + p].row)) {
              return false;
            }
          }
          for (size_t p = k; p <= end; ++p) {
            emit_row(items[p_begin + p].row);
          }
          k = end + 1;
        }
        return true;
      };

      if (spec.prefix_op == PrefixOp::kCount) {
        int64_t count = 0;
        int64_t* dst = out_ints[w].data();
        if (spec.arg_type == ValueType::kDouble) {
          const auto& src = dbl_arg_cols[spec.col_data_idx];
          run_prefix(
              [&](uint32_t r) {
                if (!src.IsNull(r)) {
                  ++count;
                }
                return true;
              },
              [&](uint32_t r) { dst[r] = count; });
        } else {
          const auto& src = int_arg_cols[spec.col_data_idx];
          run_prefix(
              [&](uint32_t r) {
                if (!src.IsNull(r)) {
                  ++count;
                }
                return true;
              },
              [&](uint32_t r) { dst[r] = count; });
        }
      } else if (spec.arg_type == ValueType::kDouble) {
        const auto& src = dbl_arg_cols[spec.col_data_idx];
        double* dst = out_doubles[w].data();
        if (spec.prefix_op == PrefixOp::kSum ||
            spec.prefix_op == PrefixOp::kAvg) {
          double total = 0.0;
          int64_t count = 0;
          run_prefix(
              [&](uint32_t r) {
                if (!src.IsNull(r)) {
                  total += src.vals[r];
                  ++count;
                }
                return true;
              },
              [&](uint32_t r) {
                if (count == 0) {
                  dst_null[r] = 1;
                  local_any_null[w] = 1;
                } else {
                  dst[r] = spec.prefix_op == PrefixOp::kAvg
                               ? (total / static_cast<double>(count))
                               : total;
                }
              });
        } else {
          bool has_best = false;
          double best = 0.0;
          bool best_nan = false;
          const bool is_min = (spec.prefix_op == PrefixOp::kMin);
          run_prefix(
              [&](uint32_t r) {
                if (!src.IsNull(r)) {
                  const double cur = src.vals[r];
                  const bool cur_nan = std::isnan(cur);
                  if (!has_best) {
                    has_best = true;
                    best = cur;
                    best_nan = cur_nan;
                  } else if (!best_nan &&
                             (cur_nan ||
                              (is_min ? (cur < best) : (cur > best)))) {
                    best = cur;
                    best_nan = cur_nan;
                  }
                }
                return true;
              },
              [&](uint32_t r) {
                if (!has_best) {
                  dst_null[r] = 1;
                  local_any_null[w] = 1;
                } else {
                  dst[r] = best;
                }
              });
        }
      } else {
        const auto& src = int_arg_cols[spec.col_data_idx];
        if (spec.prefix_op == PrefixOp::kSum ||
            spec.prefix_op == PrefixOp::kAvg) {
          int64_t total = 0;
          int64_t count = 0;
          const bool is_avg = (spec.prefix_op == PrefixOp::kAvg);
          const bool ok = run_prefix(
              [&](uint32_t r) {
                if (!src.IsNull(r)) {
                  if (__builtin_add_overflow(total, src.vals[r], &total)) {
                    first_overflow_win = std::min(first_overflow_win, w);
                    return false;
                  }
                  ++count;
                }
                return true;
              },
              [&](uint32_t r) {
                if (count == 0) {
                  dst_null[r] = 1;
                  local_any_null[w] = 1;
                } else if (is_avg) {
                  out_doubles[w][r] =
                      static_cast<double>(total) / static_cast<double>(count);
                } else {
                  out_ints[w][r] = total;
                }
              });
          if (!ok) {
            return false;
          }
        } else {
          int64_t* dst = out_ints[w].data();
          bool has_best = false;
          int64_t best = 0;
          const bool is_min = (spec.prefix_op == PrefixOp::kMin);
          run_prefix(
              [&](uint32_t r) {
                if (!src.IsNull(r)) {
                  const int64_t cur = src.vals[r];
                  if (!has_best) {
                    has_best = true;
                    best = cur;
                  } else if (is_min ? (cur < best) : (cur > best)) {
                    best = cur;
                  }
                }
                return true;
              },
              [&](uint32_t r) {
                if (!has_best) {
                  dst_null[r] = 1;
                  local_any_null[w] = 1;
                } else {
                  dst[r] = best;
                }
              });
        }
      }
    }
    return true;
  };

  auto fill_ord = [&](uint32_t row, uint64_t* ok, uint8_t* flags) {
    if (!has_order) {
      return;
    }
    if (ord_vt == ValueType::kDouble) {
      if (ord_dbl_col.IsNull(row)) {
        *flags |= FlatBucketItem::kOrdNull;
      } else {
        *ok = EncodeDoubleOrderKey(ord_dbl_col.vals[row], ord_asc);
      }
    } else {
      if (ord_int_col.IsNull(row)) {
        *flags |= FlatBucketItem::kOrdNull;
      } else {
        *ok = EncodeSignedIntOrderKey(ord_int_col.vals[row], ord_asc);
      }
    }
  };

  const bool check_ord_null =
      has_order && (ord_vt == ValueType::kDouble ? ord_dbl_col.any_null
                                                 : ord_int_col.any_null);
  auto item_before = [has_order, check_ord_null, ord_nulls_first](
                         const FlatBucketItem& a, const FlatBucketItem& b) {
    if (a.PartNull() != b.PartNull()) {
      return a.PartNull();
    }
    if (!a.PartNull() && a.part_key != b.part_key) {
      return a.part_key < b.part_key;
    }
    if (has_order) {
      if (check_ord_null) {
        const bool an = a.OrdNull();
        const bool bn = b.OrdNull();
        if (an != bn) {
          return an ? ord_nulls_first : !ord_nulls_first;
        }
        if (an) {
          return a.row < b.row;
        }
      }
      if (a.ord_key != b.ord_key) {
        return a.ord_key < b.ord_key;
      }
    }
    return a.row < b.row;
  };

  std::atomic<size_t> overflow_win{win_count};
  if (!has_part) {
    std::vector<FlatBucketItem> items(n);
    const size_t fill_workers =
        n < 16384
            ? 1
            : std::min<size_t>({8, n / 8192,
                                QueryParallelWorkerLimit(
                                    std::thread::hardware_concurrency())});
    RETURN_IF_FAIL(RunParallelTasks(fill_workers, [&](size_t worker) {
      const size_t begin = n * worker / fill_workers;
      const size_t end = n * (worker + 1) / fill_workers;
      for (size_t r = begin; r < end; ++r) {
        auto& item = items[r];
        item.row = static_cast<uint32_t>(r);
        item.flags = 0;
        fill_ord(item.row, &item.ord_key, &item.flags);
      }
    }));
    if (has_order) {
      if (n >= 65536) {
        const auto sort_workers = std::min<size_t>(
            {8, n / 16384,
             QueryParallelWorkerLimit(std::thread::hardware_concurrency())});
        RETURN_IF_FAIL(RunParallelTasks(sort_workers, [&](size_t worker) {
          const auto begin =
              static_cast<std::ptrdiff_t>(n * worker / sort_workers);
          const auto end =
              static_cast<std::ptrdiff_t>(n * (worker + 1) / sort_workers);
          std::sort(items.begin() + begin, items.begin() + end, item_before);
        }));
        for (size_t step = 1; step < sort_workers; step *= 2) {
          std::vector<std::pair<size_t, size_t>> merges;
          merges.reserve(sort_workers);
          for (size_t w = 0; w + step < sort_workers; w += 2 * step) {
            const size_t lo = n * w / sort_workers;
            const size_t hi =
                n * std::min(sort_workers, w + (2 * step)) / sort_workers;
            merges.emplace_back(lo, hi);
          }
          RETURN_IF_FAIL(RunParallelTasks(merges.size(), [&](size_t idx) {
            const size_t w = idx * 2 * step;
            const auto lo = static_cast<std::ptrdiff_t>(n * w / sort_workers);
            const auto mid =
                static_cast<std::ptrdiff_t>(n * (w + step) / sort_workers);
            const auto hi = static_cast<std::ptrdiff_t>(
                n * std::min(sort_workers, w + (2 * step)) / sort_workers);
            std::inplace_merge(items.begin() + lo, items.begin() + mid,
                               items.begin() + hi, item_before);
          }));
        }
      } else {
        std::sort(items.begin(), items.end(), item_before);
      }
    }
    std::vector<uint8_t> local_null(win_count, 0);
    size_t local_ovf = win_count;
    if (!eval_partition(items.data(), 0, n, local_null, local_ovf)) {
      return StatusError(StatusCode::kIsInfinity,
                         "integer overflow in " + specs[local_ovf].fn_name);
    }
    for (size_t w = 0; w < win_count; ++w) {
      if (local_null[w] != 0) {
        win_any_null[w] = 1;
      }
    }
  } else {
    const bool nonnull_part_and_ord = !part_col.any_null && !check_ord_null;
    auto item_before_nonnull = [has_order](const FlatBucketItem& a,
                                           const FlatBucketItem& b) {
      if (a.part_key != b.part_key) {
        return a.part_key < b.part_key;
      }
      if (has_order && a.ord_key != b.ord_key) {
        return a.ord_key < b.ord_key;
      }
      return a.row < b.row;
    };
    const auto run_partitioned_buckets = [&]<size_t kBuckets>() -> Status {
      const size_t scatter_workers =
          n < 8192
              ? 1
              : std::min<size_t>({8, n / 4096,
                                  QueryParallelWorkerLimit(
                                      std::thread::hardware_concurrency())});
      std::vector<std::array<std::vector<FlatBucketItem>, kBuckets>>
          worker_buckets(scatter_workers);
      const size_t approx =
          std::max<size_t>(4, ((n / scatter_workers) / kBuckets) + 4);
      for (auto& wb : worker_buckets) {
        for (auto& vec : wb) {
          vec.reserve(approx);
        }
      }
      RETURN_IF_FAIL(RunParallelTasks(scatter_workers, [&](size_t worker) {
        const size_t begin = n * worker / scatter_workers;
        const size_t end = n * (worker + 1) / scatter_workers;
        auto& wb = worker_buckets[worker];
        if (nonnull_part_and_ord) {
          const int64_t* pvals = part_col.vals;
          if (!has_order) {
            for (size_t r = begin; r < end; ++r) {
              const int64_t pk = pvals[r];
              const size_t b = static_cast<size_t>(SplitMix64Window(
                                   static_cast<uint64_t>(pk))) &
                               (kBuckets - 1);
              wb[b].push_back(FlatBucketItem{.part_key = pk,
                                             .ord_key = 0,
                                             .row = static_cast<uint32_t>(r),
                                             .flags = 0});
            }
          } else if (ord_vt == ValueType::kDouble) {
            const double* ovals = ord_dbl_col.vals;
            for (size_t r = begin; r < end; ++r) {
              const int64_t pk = pvals[r];
              const size_t b = static_cast<size_t>(SplitMix64Window(
                                   static_cast<uint64_t>(pk))) &
                               (kBuckets - 1);
              wb[b].push_back(FlatBucketItem{
                  .part_key = pk,
                  .ord_key = EncodeDoubleOrderKey(ovals[r], ord_asc),
                  .row = static_cast<uint32_t>(r),
                  .flags = 0});
            }
          } else {
            const int64_t* ovals = ord_int_col.vals;
            for (size_t r = begin; r < end; ++r) {
              const int64_t pk = pvals[r];
              const size_t b = static_cast<size_t>(SplitMix64Window(
                                   static_cast<uint64_t>(pk))) &
                               (kBuckets - 1);
              wb[b].push_back(FlatBucketItem{
                  .part_key = pk,
                  .ord_key = EncodeSignedIntOrderKey(ovals[r], ord_asc),
                  .row = static_cast<uint32_t>(r),
                  .flags = 0});
            }
          }
          return;
        }
        for (size_t r = begin; r < end; ++r) {
          FlatBucketItem item;
          item.row = static_cast<uint32_t>(r);
          size_t b = 0;
          if (part_col.IsNull(r)) {
            item.flags = FlatBucketItem::kPartNull;
          } else {
            item.part_key = part_col.vals[r];
            b = static_cast<size_t>(
                    SplitMix64Window(static_cast<uint64_t>(item.part_key))) &
                (kBuckets - 1);
          }
          fill_ord(item.row, &item.ord_key, &item.flags);
          wb[b].push_back(item);
        }
      }));

      std::atomic<size_t> next_bucket{0};
      const size_t eval_workers = std::min<size_t>(
          kBuckets,
          QueryParallelWorkerLimit(std::thread::hardware_concurrency()));
      std::vector<std::vector<uint8_t>> worker_null(
          eval_workers, std::vector<uint8_t>(win_count, 0));
      RETURN_IF_FAIL(RunParallelTasks(eval_workers, [&](size_t worker) {
        auto& slot_null = worker_null[worker];
        size_t local_ovf = win_count;
        std::vector<FlatBucketItem> bucket_items;
        while (overflow_win.load(std::memory_order_relaxed) == win_count) {
          const size_t b = next_bucket.fetch_add(1, std::memory_order_relaxed);
          if (b >= kBuckets) {
            break;
          }
          size_t b_total = 0;
          for (size_t w = 0; w < scatter_workers; ++w) {
            b_total += worker_buckets[w][b].size();
          }
          if (b_total == 0) {
            continue;
          }
          bucket_items.clear();
          bucket_items.reserve(b_total);
          for (size_t w = 0; w < scatter_workers; ++w) {
            auto& src = worker_buckets[w][b];
            bucket_items.insert(bucket_items.end(), src.begin(), src.end());
            std::vector<FlatBucketItem>().swap(src);
          }
          if (nonnull_part_and_ord) {
            std::sort(bucket_items.begin(), bucket_items.end(),
                      item_before_nonnull);
          } else {
            std::sort(bucket_items.begin(), bucket_items.end(), item_before);
          }
          size_t pos = 0;
          const size_t bsize = bucket_items.size();
          const FlatBucketItem* bdata = bucket_items.data();
          if (nonnull_part_and_ord) {
            while (pos < bsize) {
              size_t end = pos + 1;
              const int64_t pk = bdata[pos].part_key;
              while (end < bsize && bdata[end].part_key == pk) {
                ++end;
              }
              if (!eval_partition(bdata, pos, end, slot_null, local_ovf)) {
                size_t cur = overflow_win.load(std::memory_order_relaxed);
                while (local_ovf < cur &&
                       !overflow_win.compare_exchange_weak(
                           cur, local_ovf, std::memory_order_relaxed)) {
                }
                return;
              }
              pos = end;
            }
          } else {
            while (pos < bsize) {
              size_t end = pos + 1;
              const FlatBucketItem& head = bdata[pos];
              while (
                  end < bsize && bdata[end].PartNull() == head.PartNull() &&
                  (head.PartNull() || bdata[end].part_key == head.part_key)) {
                ++end;
              }
              if (!eval_partition(bdata, pos, end, slot_null, local_ovf)) {
                size_t cur = overflow_win.load(std::memory_order_relaxed);
                while (local_ovf < cur &&
                       !overflow_win.compare_exchange_weak(
                           cur, local_ovf, std::memory_order_relaxed)) {
                }
                return;
              }
              pos = end;
            }
          }
        }
      }));
      const size_t ovf = overflow_win.load(std::memory_order_relaxed);
      if (ovf < win_count) {
        return StatusError(StatusCode::kIsInfinity,
                           "integer overflow in " + specs[ovf].fn_name);
      }
      for (size_t worker = 0; worker < eval_workers; ++worker) {
        for (size_t w = 0; w < win_count; ++w) {
          if (worker_null[worker][w] != 0) {
            win_any_null[w] = 1;
          }
        }
      }
      return Status::kSuccess;
    };
    if (n >= 65536) {
      RETURN_IF_FAIL(run_partitioned_buckets.template operator()<256>());
    } else {
      RETURN_IF_FAIL(run_partitioned_buckets.template operator()<64>());
    }
  }

  const size_t kept_width = kept_slots.size();
  std::vector<Column> extended_columns;
  extended_columns.reserve(kept_width + win_count);
  for (size_t slot : kept_slots) {
    extended_columns.push_back(base_schema.GetColumn(slot));
  }
  for (size_t w = 0; w < win_count; ++w) {
    extended_columns.emplace_back(win_names[w], specs[w].out_type);
  }

  bool out_uniform = true;
  for (const ColumnarChunk& ch : table.chunks) {
    if (ch.rows == 0) {
      continue;
    }
    for (size_t slot : kept_slots) {
      if (slot >= ch.columns.size() || !ch.columns[slot].is_uniform) {
        out_uniform = false;
        break;
      }
    }
    if (!out_uniform) {
      break;
    }
  }
  const bool single_chunk = (table.chunks.size() == 1);
  Relation out;
  RETURN_IF_FAIL(AssembleColumnarWindowOutput(
      std::move(input), kept_slots, std::move(extended_columns), out_uniform,
      [&](size_t /*ci*/, size_t row_off, size_t chunk_rows,
          ColumnarChunk* dst_chunk) {
        for (size_t w = 0; w < win_count; ++w) {
          ColumnarColumn& dst = dst_chunk->columns[kept_width + w];
          const ValueType ot = specs[w].out_type;
          if (ot == ValueType::kNull) {
            dst.SetConstantNull(ValueType::kNull, false, false, chunk_rows);
            continue;
          }
          dst.Init(ot, false, false, 0);
          bool chunk_has_null = false;
          if (win_any_null[w] != 0 && !out_nulls[w].empty()) {
            const uint8_t* src_n = out_nulls[w].data() + row_off;
            for (size_t r = 0; r < chunk_rows; ++r) {
              if (src_n[r] != 0) {
                chunk_has_null = true;
                break;
              }
            }
          }
          if (single_chunk) {
            if (ot == ValueType::kDouble) {
              dst.doubles = std::move(out_doubles[w]);
            } else {
              dst.ints = std::move(out_ints[w]);
            }
            if (chunk_has_null) {
              dst.has_null = true;
              dst.nulls = std::move(out_nulls[w]);
            }
          } else {
            const auto off_d = static_cast<std::ptrdiff_t>(row_off);
            const auto end_d =
                static_cast<std::ptrdiff_t>(row_off + chunk_rows);
            if (ot == ValueType::kDouble) {
              dst.doubles.assign(out_doubles[w].begin() + off_d,
                                 out_doubles[w].begin() + end_d);
            } else {
              dst.ints.assign(out_ints[w].begin() + off_d,
                              out_ints[w].begin() + end_d);
            }
            if (chunk_has_null) {
              dst.has_null = true;
              dst.nulls.assign(out_nulls[w].begin() + off_d,
                               out_nulls[w].begin() + end_d);
            }
          }
        }
      },
      &out));
  return std::optional<Relation>{std::move(out)};
}

void CollectWindowReferencedCols(const Expression& expr, const Schema& schema,
                                 std::vector<uint8_t>* used, bool* need_all) {
  if (!expr || *need_all) {
    return;
  }
  switch (expr->Type()) {
    case TypeTag::kColumnValue: {
      const auto slot =
          LocalColumnOffset(schema, expr->AsColumnValue().GetColumnName());
      if (slot.has_value() && *slot < used->size()) {
        (*used)[*slot] = 1;
      } else {
        *need_all = true;
      }
      return;
    }
    case TypeTag::kQueryExp:
    case TypeTag::kAggregateExp:
      *need_all = true;
      return;
    default: {
      for (const Expression& child : ExpressionChildren(expr)) {
        CollectWindowReferencedCols(child, schema, used, need_all);
      }
      return;
    }
  }
}

bool HasDuplicateColumnNamesCaseInsensitive(const Schema& schema) {
  std::vector<std::string> lower_names;
  lower_names.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    std::string s = schema.GetColumn(i).Name().name;
    for (char& ch : s) {
      ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    lower_names.push_back(std::move(s));
  }
  std::sort(lower_names.begin(), lower_names.end());
  for (size_t i = 1; i < lower_names.size(); ++i) {
    if (lower_names[i] == lower_names[i - 1]) {
      return true;
    }
  }
  return false;
}

struct FlatStringColumn {
  const std::string_view* vals{nullptr};
  const uint8_t* nulls{nullptr};
  bool any_null{false};
  std::vector<std::string_view> owned_vals;
  std::vector<uint8_t> owned_nulls;

  Status Load(const ColumnarTable& table, size_t slot, size_t n) {
    if (table.chunks.size() == 1 && table.chunks[0].rows == n) {
      const ColumnarColumn& c = table.chunks[0].columns[slot];
      if (c.encoding == ColumnarColumn::Encoding::kFlat && c.is_uniform &&
          c.type == ValueType::kVarChar && !c.is_interval &&
          c.strings.size() == n && (!c.has_null || c.nulls.size() == n)) {
        vals = c.strings.data();
        if (c.has_null) {
          any_null = true;
          nulls = c.nulls.data();
        }
        return Status::kSuccess;
      }
    }

    owned_vals.resize(n);
    bool may_have_null = false;
    for (const ColumnarChunk& ch : table.chunks) {
      if (ch.rows == 0) {
        continue;
      }
      const ColumnarColumn& c = ch.columns[slot];
      if (c.type == ValueType::kNull || c.has_null ||
          c.encoding == ColumnarColumn::Encoding::kGathered ||
          (c.encoding == ColumnarColumn::Encoding::kConstant &&
           c.const_is_null)) {
        may_have_null = true;
        break;
      }
    }
    if (may_have_null) {
      owned_nulls.assign(n, 0);
    }

    std::vector<size_t> chunk_offsets(table.chunks.size(), 0);
    size_t acc = 0;
    for (size_t c_idx = 0; c_idx < table.chunks.size(); ++c_idx) {
      chunk_offsets[c_idx] = acc;
      acc += table.chunks[c_idx].rows;
    }

    std::atomic<bool> saw_null{false};
    const size_t workers =
        (table.chunks.size() < 4 || n < 16384)
            ? 1
            : std::min<size_t>({8, table.chunks.size(),
                                QueryParallelWorkerLimit(
                                    std::thread::hardware_concurrency())});
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
      bool local_null = false;
      for (size_t ci = worker; ci < table.chunks.size(); ci += workers) {
        const ColumnarChunk& ch = table.chunks[ci];
        if (ch.rows == 0) {
          continue;
        }
        const size_t off = chunk_offsets[ci];
        const ColumnarColumn& c = ch.columns[slot];
        if (c.type == ValueType::kNull ||
            (c.encoding == ColumnarColumn::Encoding::kConstant &&
             c.const_is_null)) {
          std::fill_n(owned_nulls.data() + off, ch.rows, uint8_t{1});
          local_null = true;
          continue;
        }
        if (c.encoding == ColumnarColumn::Encoding::kConstant) {
          std::fill_n(owned_vals.data() + off, ch.rows, c.const_string);
          continue;
        }
        if (c.encoding == ColumnarColumn::Encoding::kFlat && c.is_uniform &&
            c.strings.size() == ch.rows) {
          std::memcpy(owned_vals.data() + off, c.strings.data(),
                      ch.rows * sizeof(std::string_view));
          if (c.has_null && c.nulls.size() == ch.rows) {
            for (size_t r = 0; r < ch.rows; ++r) {
              if (c.nulls[r] != 0) {
                owned_nulls[off + r] = 1;
                local_null = true;
              }
            }
          }
          continue;
        }
        const bool check_null =
            c.has_null || c.encoding == ColumnarColumn::Encoding::kGathered;
        for (size_t r = 0; r < ch.rows; ++r) {
          if (check_null && c.IsNull(r)) {
            owned_nulls[off + r] = 1;
            local_null = true;
          } else {
            owned_vals[off + r] = c.StringAt(r);
          }
        }
      }
      if (local_null) {
        saw_null.store(true, std::memory_order_relaxed);
      }
    }));

    any_null = saw_null.load(std::memory_order_relaxed);
    if (!any_null) {
      std::vector<uint8_t>().swap(owned_nulls);
      nulls = nullptr;
    } else {
      nulls = owned_nulls.data();
    }
    vals = owned_vals.data();
    return Status::kSuccess;
  }

  [[nodiscard]] bool IsNull(size_t r) const noexcept {
    return nulls != nullptr && nulls[r] != 0;
  }
};

bool ClassifyUniformColumnType(const ColumnarTable& table, size_t slot,
                               ValueType* detected_type) noexcept {
  ValueType seen = ValueType::kNull;
  for (const ColumnarChunk& ch : table.chunks) {
    if (ch.rows == 0) {
      continue;
    }
    if (slot >= ch.columns.size()) {
      return false;
    }
    const ColumnarColumn& c = ch.columns[slot];
    if (!c.is_uniform || c.is_unsigned || c.is_interval) {
      return false;
    }
    if (c.type == ValueType::kNull) {
      continue;
    }
    if (c.type != ValueType::kInt64 && c.type != ValueType::kDate &&
        c.type != ValueType::kDouble && c.type != ValueType::kVarChar) {
      return false;
    }
    if (seen == ValueType::kNull) {
      seen = c.type;
    } else if (seen != c.type) {
      return false;
    }
  }
  // Empty/all-NULL columns have no flat numeric/text representation.
  if (seen == ValueType::kNull) {
    return false;
  }
  *detected_type = seen;
  return true;
}

StatusOr<std::optional<Relation>> TryComputeMultiKeyColumnarWindows(
    Relation& input,
    std::span<const WindowFunctionCallExpression* const> win_nodes,
    std::span<const std::string> win_names,
    std::span<const size_t> kept_slots) {
  const ColumnarTable& table = *input.columnar;
  const size_t n = table.total_rows;
  const size_t win_count = win_nodes.size();
  if (n == 0 || win_count == 0 || win_count > 32 ||
      n > std::numeric_limits<uint32_t>::max()) {
    return std::nullopt;
  }
  const Schema& schema = input.schema;
  if (HasDuplicateColumnNamesCaseInsensitive(schema)) {
    return std::nullopt;
  }

  struct ColRef {
    size_t slot{SIZE_MAX};
    ValueType type{ValueType::kNull};
    size_t typed_idx{SIZE_MAX};
  };
  std::vector<ColRef> cols;
  std::unordered_map<size_t, size_t> slot_to_col_idx;
  auto ensure_col = [&](size_t slot) -> std::optional<size_t> {
    if (auto it = slot_to_col_idx.find(slot); it != slot_to_col_idx.end()) {
      return it->second;
    }
    ValueType vt = ValueType::kNull;
    if (!ClassifyUniformColumnType(table, slot, &vt)) {
      return std::nullopt;
    }
    const size_t idx = cols.size();
    cols.push_back(ColRef{.slot = slot, .type = vt});
    slot_to_col_idx.emplace(slot, idx);
    return idx;
  };

  struct MultiOrderTerm {
    size_t col_idx{SIZE_MAX};
    bool ascending{true};
    bool nulls_first{true};
  };

  struct MultiWinSpec {
    enum class Kind : uint8_t { kRowNumber, kRank, kDenseRank, kPrefix };
    size_t win_idx{0};
    Kind kind{Kind::kPrefix};
    PrefixOp prefix_op{PrefixOp::kSum};
    PrefixFrameMode frame_mode{PrefixFrameMode::kRows};
    size_t arg_col_idx{SIZE_MAX};  // SIZE_MAX for arg-less functions / COUNT(*)
    ValueType out_type{ValueType::kNull};
  };

  struct WindowGroup {
    std::vector<size_t> part_cols;
    std::vector<MultiOrderTerm> order_terms;
    std::vector<MultiWinSpec> specs;
    bool all_unsorted_fast{false};
  };

  std::vector<WindowGroup> groups;
  groups.reserve(win_count);
  std::vector<ValueType> out_types(win_count, ValueType::kNull);

  for (size_t w = 0; w < win_count; ++w) {
    const WindowFunctionCallExpression& win = *win_nodes[w];
    std::vector<size_t> p_cols;
    p_cols.reserve(win.partition_by.size());
    for (const Expression& expr : win.partition_by) {
      if (!expr || expr->Type() != TypeTag::kColumnValue) {
        return std::nullopt;
      }
      const auto slot_opt =
          LocalColumnOffset(schema, expr->AsColumnValue().GetColumnName());
      if (!slot_opt.has_value()) {
        return std::nullopt;
      }
      const auto col_idx = ensure_col(*slot_opt);
      if (!col_idx.has_value()) {
        return std::nullopt;
      }
      p_cols.push_back(*col_idx);
    }

    std::vector<MultiOrderTerm> o_terms;
    o_terms.reserve(win.order_by.size());
    for (const WindowOrderTerm& term : win.order_by) {
      if (!term.expression ||
          term.expression->Type() != TypeTag::kColumnValue) {
        return std::nullopt;
      }
      const auto slot_opt = LocalColumnOffset(
          schema, term.expression->AsColumnValue().GetColumnName());
      if (!slot_opt.has_value()) {
        return std::nullopt;
      }
      const auto col_idx = ensure_col(*slot_opt);
      if (!col_idx.has_value()) {
        return std::nullopt;
      }
      o_terms.push_back(MultiOrderTerm{
          .col_idx = *col_idx,
          .ascending = term.ascending,
          .nulls_first = term.nulls_first.value_or(term.ascending)});
    }

    MultiWinSpec spec;
    spec.win_idx = w;
    if ((win.function == "ROW_NUMBER" || win.function == "RANK" ||
         win.function == "DENSE_RANK") &&
        win.args.empty() && !win.distinct && !win.where_filter &&
        !win.has_frame && win.inner_order_by.empty() && !win.inner_limit &&
        win.exclusion == WindowFrameExclusion::kNone) {
      spec.kind = win.function == "ROW_NUMBER" ? MultiWinSpec::Kind::kRowNumber
                  : win.function == "RANK"     ? MultiWinSpec::Kind::kRank
                                               : MultiWinSpec::Kind::kDenseRank;
      spec.out_type = ValueType::kInt64;
    } else if (win.function == "COUNT" && win.args.empty() && !win.distinct &&
               !win.where_filter && win.inner_order_by.empty() &&
               !win.inner_limit &&
               win.exclusion == WindowFrameExclusion::kNone &&
               (win.frame_unit != WindowFrameUnit::kGroups ||
                !win.order_by.empty()) &&
               ((!win.has_frame && win.order_by.empty()) || !win.has_frame ||
                (win.frame_start.type ==
                     WindowFrameBoundType::kUnboundedPreceding &&
                 win.frame_end.type == WindowFrameBoundType::kCurrentRow &&
                 !win.frame_start.offset && !win.frame_end.offset))) {
      spec.kind = MultiWinSpec::Kind::kPrefix;
      spec.prefix_op = PrefixOp::kCount;
      spec.frame_mode = ClassifyPrefixFrame(win);
      spec.arg_col_idx = SIZE_MAX;
      spec.out_type = ValueType::kInt64;
    } else {
      std::optional<size_t> arg_slot;
      if (!IsPrefixWindowEligible(win, schema, &arg_slot)) {
        return std::nullopt;
      }
      const auto col_idx = ensure_col(*arg_slot);
      if (!col_idx.has_value()) {
        return std::nullopt;
      }
      spec.kind = MultiWinSpec::Kind::kPrefix;
      spec.prefix_op = ClassifyPrefixOp(win.function);
      spec.frame_mode = ClassifyPrefixFrame(win);
      spec.arg_col_idx = *col_idx;
      const ValueType avt = cols[*col_idx].type;
      if (spec.prefix_op == PrefixOp::kCount) {
        spec.out_type = ValueType::kInt64;
      } else if (avt == ValueType::kNull || avt == ValueType::kVarChar) {
        return std::nullopt;
      } else if (spec.prefix_op == PrefixOp::kAvg) {
        spec.out_type = ValueType::kDouble;
      } else if (spec.prefix_op == PrefixOp::kSum) {
        if (avt == ValueType::kDate) {
          return std::nullopt;
        }
        spec.out_type = avt;
      } else {
        spec.out_type = avt;
      }
    }
    out_types[w] = spec.out_type;

    size_t group_idx = groups.size();
    for (size_t g = 0; g < groups.size(); ++g) {
      if (groups[g].part_cols == p_cols &&
          groups[g].order_terms.size() == o_terms.size()) {
        bool eq = true;
        for (size_t i = 0; i < o_terms.size(); ++i) {
          if (groups[g].order_terms[i].col_idx != o_terms[i].col_idx ||
              groups[g].order_terms[i].ascending != o_terms[i].ascending ||
              groups[g].order_terms[i].nulls_first != o_terms[i].nulls_first) {
            eq = false;
            break;
          }
        }
        if (eq) {
          group_idx = g;
          break;
        }
      }
    }
    if (group_idx == groups.size()) {
      groups.push_back(WindowGroup{.part_cols = std::move(p_cols),
                                   .order_terms = std::move(o_terms),
                                   .specs = {},
                                   .all_unsorted_fast = false});
    }
    groups[group_idx].specs.push_back(spec);
  }

  for (WindowGroup& grp : groups) {
    if (!grp.order_terms.empty()) {
      grp.all_unsorted_fast = false;
      continue;
    }
    grp.all_unsorted_fast = true;
    for (const MultiWinSpec& sp : grp.specs) {
      if (sp.kind == MultiWinSpec::Kind::kPrefix &&
          sp.frame_mode == PrefixFrameMode::kRows) {
        grp.all_unsorted_fast = false;
        break;
      }
    }
  }

  std::vector<FlatNumericColumn<int64_t>> int_cols;
  int_cols.reserve(cols.size());
  std::vector<FlatNumericColumn<double>> dbl_cols;
  dbl_cols.reserve(cols.size());
  std::vector<FlatStringColumn> str_cols;
  str_cols.reserve(cols.size());
  for (ColRef& c : cols) {
    if (c.type == ValueType::kInt64 || c.type == ValueType::kDate) {
      c.typed_idx = int_cols.size();
      int_cols.emplace_back();
      RETURN_IF_FAIL(int_cols.back().Load(table, c.slot, n));
    } else if (c.type == ValueType::kDouble) {
      c.typed_idx = dbl_cols.size();
      dbl_cols.emplace_back();
      RETURN_IF_FAIL(dbl_cols.back().Load(table, c.slot, n));
    } else if (c.type == ValueType::kVarChar) {
      c.typed_idx = str_cols.size();
      str_cols.emplace_back();
      RETURN_IF_FAIL(str_cols.back().Load(table, c.slot, n));
    }
  }

  std::vector<std::vector<int64_t>> out_ints(win_count);
  std::vector<std::vector<double>> out_doubles(win_count);
  std::vector<std::vector<uint8_t>> out_nulls(win_count);
  std::vector<uint8_t> win_any_null(win_count, 0);
  for (size_t w = 0; w < win_count; ++w) {
    if (out_types[w] == ValueType::kDouble) {
      out_doubles[w].resize(n);
    } else {
      out_ints[w].resize(n);
    }
    out_nulls[w].assign(n, 0);
  }

  auto col_is_null = [&](size_t col_idx, uint32_t row) noexcept -> bool {
    const ColRef& c = cols[col_idx];
    if (c.type == ValueType::kNull) {
      return true;
    }
    if (c.type == ValueType::kVarChar) {
      return str_cols[c.typed_idx].IsNull(row);
    }
    if (c.type == ValueType::kDouble) {
      return dbl_cols[c.typed_idx].IsNull(row);
    }
    return int_cols[c.typed_idx].IsNull(row);
  };

  auto part_col_equal = [&](size_t col_idx, uint32_t a,
                            uint32_t b) noexcept -> bool {
    const ColRef& c = cols[col_idx];
    if (c.type == ValueType::kNull) {
      return true;
    }
    if (c.type == ValueType::kVarChar) {
      const auto& sc = str_cols[c.typed_idx];
      const bool an = sc.IsNull(a);
      const bool bn = sc.IsNull(b);
      if (an || bn) {
        return an == bn;
      }
      return sc.vals[a] == sc.vals[b];
    }
    if (c.type == ValueType::kDouble) {
      const auto& dc = dbl_cols[c.typed_idx];
      const bool an = dc.IsNull(a);
      const bool bn = dc.IsNull(b);
      if (an || bn) {
        return an == bn;
      }
      const double va = dc.vals[a];
      const double vb = dc.vals[b];
      return (std::isnan(va) && std::isnan(vb)) || va == vb;
    }
    const auto& ic = int_cols[c.typed_idx];
    const bool an = ic.IsNull(a);
    const bool bn = ic.IsNull(b);
    if (an || bn) {
      return an == bn;
    }
    return ic.vals[a] == ic.vals[b];
  };

  auto part_hash_row = [&](std::span<const size_t> part_cols,
                           uint32_t r) noexcept -> uint64_t {
    uint64_t h = 0x9e3779b97f4a7c15ULL ^ part_cols.size();
    for (size_t ci : part_cols) {
      const ColRef& c = cols[ci];
      uint64_t ph = 0x517cc1b727220a95ULL;
      if (c.type == ValueType::kVarChar) {
        const auto& sc = str_cols[c.typed_idx];
        if (!sc.IsNull(r)) {
          ph = SplitMix64Window(std::hash<std::string_view>{}(sc.vals[r]) ^
                                0x94d049bb133111ebULL);
        }
      } else if (c.type == ValueType::kDouble) {
        const auto& dc = dbl_cols[c.typed_idx];
        if (!dc.IsNull(r)) {
          const double d = dc.vals[r];
          if (std::isnan(d)) {
            ph = 0x7ff8000000000001ULL;
          } else {
            const double norm = (d == 0.0) ? 0.0 : d;
            ph = SplitMix64Window(std::bit_cast<uint64_t>(norm));
          }
        }
      } else if (c.type != ValueType::kNull) {
        const auto& ic = int_cols[c.typed_idx];
        if (!ic.IsNull(r)) {
          ph = SplitMix64Window(static_cast<uint64_t>(ic.vals[r]));
        }
      }
      h ^= ph + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
    }
    return h;
  };

  auto ord_compare_term = [&](const MultiOrderTerm& term, uint32_t a,
                              uint32_t b) noexcept -> int {
    const ColRef& c = cols[term.col_idx];
    if (c.type == ValueType::kNull) {
      return 0;
    }
    if (c.type == ValueType::kVarChar) {
      const auto& sc = str_cols[c.typed_idx];
      const bool an = sc.IsNull(a);
      const bool bn = sc.IsNull(b);
      if (an || bn) {
        if (an == bn) {
          return 0;
        }
        return (an ? term.nulls_first : !term.nulls_first) ? -1 : 1;
      }
      const std::string_view va = sc.vals[a];
      const std::string_view vb = sc.vals[b];
      if (va == vb) {
        return 0;
      }
      const bool less = term.ascending ? (va < vb) : (vb < va);
      return less ? -1 : 1;
    }
    if (c.type == ValueType::kDouble) {
      const auto& dc = dbl_cols[c.typed_idx];
      const bool an = dc.IsNull(a);
      const bool bn = dc.IsNull(b);
      if (an || bn) {
        if (an == bn) {
          return 0;
        }
        return (an ? term.nulls_first : !term.nulls_first) ? -1 : 1;
      }
      const double va = dc.vals[a];
      const double vb = dc.vals[b];
      const bool a_nan = std::isnan(va);
      const bool b_nan = std::isnan(vb);
      if (a_nan || b_nan) {
        if (a_nan && b_nan) {
          return 0;
        }
        const bool less = term.ascending ? a_nan : b_nan;
        return less ? -1 : 1;
      }
      if (va == vb) {
        return 0;
      }
      const bool less = term.ascending ? (va < vb) : (vb < va);
      return less ? -1 : 1;
    }
    const auto& ic = int_cols[c.typed_idx];
    const bool an = ic.IsNull(a);
    const bool bn = ic.IsNull(b);
    if (an || bn) {
      if (an == bn) {
        return 0;
      }
      return (an ? term.nulls_first : !term.nulls_first) ? -1 : 1;
    }
    const int64_t va = ic.vals[a];
    const int64_t vb = ic.vals[b];
    if (va == vb) {
      return 0;
    }
    const bool less = term.ascending ? (va < vb) : (vb < va);
    return less ? -1 : 1;
  };

  auto ord_peers_equal = [&](std::span<const MultiOrderTerm> terms, uint32_t a,
                             uint32_t b) noexcept -> bool {
    return std::ranges::all_of(terms, [&](const MultiOrderTerm& term) {
      return ord_compare_term(term, a, b) == 0;
    });
  };

  const size_t hw_limit =
      QueryParallelWorkerLimit(std::thread::hardware_concurrency());
  const size_t workers =
      n < 4096 ? 1 : std::min<size_t>({8, (n + 2047) / 2048, hw_limit});

  std::atomic<bool> int_overflowed{false};

  for (const WindowGroup& grp : groups) {
    const size_t num_shards =
        (grp.part_cols.empty() || workers <= 1 || n < 2048) ? 1 : 64;
    const size_t shard_mask = num_shards - 1;

    struct ShardItem {
      uint32_t row;
      uint64_t hash;
    };
    std::vector<std::vector<std::vector<ShardItem>>> worker_shards(
        workers, std::vector<std::vector<ShardItem>>(num_shards));

    if (num_shards == 1) {
      // Single shard: row indices stay [0..n-1] without extra hash/scatter if
      // part_cols is empty; otherwise compute hashes cleanly.
      worker_shards.resize(1);
      auto& items = worker_shards[0][0];
      items.resize(n);
      if (grp.part_cols.empty()) {
        for (size_t r = 0; r < n; ++r) {
          items[r] = {.row = static_cast<uint32_t>(r), .hash = 0};
        }
      } else if (workers <= 1) {
        for (size_t r = 0; r < n; ++r) {
          items[r] = {
              .row = static_cast<uint32_t>(r),
              .hash = part_hash_row(grp.part_cols, static_cast<uint32_t>(r))};
        }
      } else {
        RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t wid) {
          const size_t begin = (n * wid) / workers;
          const size_t end = (n * (wid + 1)) / workers;
          for (size_t r = begin; r < end; ++r) {
            items[r] = {static_cast<uint32_t>(r),
                        part_hash_row(grp.part_cols, static_cast<uint32_t>(r))};
          }
        }));
      }
    } else {
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t wid) {
        const size_t begin = (n * wid) / workers;
        const size_t end = (n * (wid + 1)) / workers;
        auto& dst = worker_shards[wid];
        const size_t per_shard_est = ((end - begin) / num_shards) + 8;
        for (size_t s = 0; s < num_shards; ++s) {
          dst[s].reserve(per_shard_est);
        }
        for (size_t r = begin; r < end; ++r) {
          const auto row = static_cast<uint32_t>(r);
          const uint64_t h = part_hash_row(grp.part_cols, row);
          dst[h & shard_mask].push_back(ShardItem{row, h});
        }
      }));
    }

    auto process_shard = [&](size_t s, std::vector<uint8_t>* local_any_null) {
      size_t shard_rows = 0;
      for (auto& worker_shard : worker_shards) {
        shard_rows += worker_shard[s].size();
      }
      if (shard_rows == 0) {
        return;
      }

      std::vector<uint32_t> item_row(shard_rows);
      std::vector<uint32_t> item_part(shard_rows);
      std::vector<uint32_t> part_counts;

      if (grp.part_cols.empty()) {
        // Guaranteed num_shards == 1: all rows belong to partition 0 in row
        // order.
        const auto& src = worker_shards[0][0];
        for (size_t i = 0; i < shard_rows; ++i) {
          item_row[i] = src[i].row;
          item_part[i] = 0;
        }
        part_counts.push_back(static_cast<uint32_t>(shard_rows));
      } else {
        struct HtSlot {
          uint64_t hash{0};
          uint32_t rep_row{0};
          uint32_t part_id{UINT32_MAX};
        };
        size_t cap = 16;
        while (cap < shard_rows * 2) {
          cap <<= 1;
        }
        const size_t mask = cap - 1;
        std::vector<HtSlot> ht(cap);
        part_counts.reserve(std::min<size_t>(shard_rows, 256));

        auto parts_match = [&](uint32_t a, uint32_t b) noexcept -> bool {
          return std::ranges::all_of(grp.part_cols, [&](size_t pc) {
            return part_col_equal(pc, a, b);
          });
        };

        size_t pos = 0;
        for (auto& worker_shard : worker_shards) {
          for (const ShardItem& it : worker_shard[s]) {
            uint32_t pid = UINT32_MAX;
            size_t slot = static_cast<size_t>(it.hash) & mask;
            while (true) {
              HtSlot& st = ht[slot];
              if (st.part_id == UINT32_MAX) {
                pid = static_cast<uint32_t>(part_counts.size());
                st.hash = it.hash;
                st.rep_row = it.row;
                st.part_id = pid;
                part_counts.push_back(0);
                break;
              }
              if (st.hash == it.hash && parts_match(st.rep_row, it.row)) {
                pid = st.part_id;
                break;
              }
              slot = (slot + 1) & mask;
            }
            item_row[pos] = it.row;
            item_part[pos] = pid;
            ++part_counts[pid];
            ++pos;
          }
        }
      }

      const size_t num_parts = part_counts.size();

      if (grp.all_unsorted_fast) {
        for (const MultiWinSpec& sp : grp.specs) {
          const size_t w = sp.win_idx;
          if (sp.kind == MultiWinSpec::Kind::kRank ||
              sp.kind == MultiWinSpec::Kind::kDenseRank) {
            int64_t* dst = out_ints[w].data();
            for (size_t i = 0; i < shard_rows; ++i) {
              dst[item_row[i]] = 1;
            }
            continue;
          }
          if (sp.kind == MultiWinSpec::Kind::kRowNumber) {
            std::vector<int64_t> cursor(num_parts, 0);
            int64_t* dst = out_ints[w].data();
            for (size_t i = 0; i < shard_rows; ++i) {
              dst[item_row[i]] = ++cursor[item_part[i]];
            }
            continue;
          }
          if (sp.prefix_op == PrefixOp::kCount) {
            int64_t* dst = out_ints[w].data();
            if (sp.arg_col_idx == SIZE_MAX) {
              for (size_t i = 0; i < shard_rows; ++i) {
                dst[item_row[i]] =
                    static_cast<int64_t>(part_counts[item_part[i]]);
              }
            } else {
              std::vector<int64_t> cnts(num_parts, 0);
              for (size_t i = 0; i < shard_rows; ++i) {
                if (!col_is_null(sp.arg_col_idx, item_row[i])) {
                  ++cnts[item_part[i]];
                }
              }
              for (size_t i = 0; i < shard_rows; ++i) {
                dst[item_row[i]] = cnts[item_part[i]];
              }
            }
            continue;
          }

          const ColRef& ac = cols[sp.arg_col_idx];
          if (ac.type == ValueType::kDouble) {
            const auto& src = dbl_cols[ac.typed_idx];
            double* dst = out_doubles[w].data();
            uint8_t* dst_null = out_nulls[w].data();
            if (sp.prefix_op == PrefixOp::kSum ||
                sp.prefix_op == PrefixOp::kAvg) {
              std::vector<double> sums(num_parts, 0.0);
              std::vector<int64_t> cnts(num_parts, 0);
              for (size_t i = 0; i < shard_rows; ++i) {
                const uint32_t r = item_row[i];
                if (!src.IsNull(r)) {
                  const uint32_t p = item_part[i];
                  sums[p] += src.vals[r];
                  ++cnts[p];
                }
              }
              if (sp.prefix_op == PrefixOp::kAvg) {
                for (size_t p = 0; p < num_parts; ++p) {
                  if (cnts[p] > 0) {
                    sums[p] /= static_cast<double>(cnts[p]);
                  }
                }
              }
              for (size_t i = 0; i < shard_rows; ++i) {
                const uint32_t r = item_row[i];
                const uint32_t p = item_part[i];
                if (cnts[p] == 0) {
                  dst_null[r] = 1;
                  (*local_any_null)[w] = 1;
                } else {
                  dst[r] = sums[p];
                }
              }
            } else {
              const bool is_min = (sp.prefix_op == PrefixOp::kMin);
              std::vector<double> best(num_parts, 0.0);
              std::vector<uint8_t> has_best(num_parts, 0);
              std::vector<uint8_t> best_nan(num_parts, 0);
              for (size_t i = 0; i < shard_rows; ++i) {
                const uint32_t r = item_row[i];
                if (src.IsNull(r)) {
                  continue;
                }
                const uint32_t p = item_part[i];
                const double cur = src.vals[r];
                const bool c_nan = std::isnan(cur);
                if (has_best[p] == 0) {
                  has_best[p] = 1;
                  best[p] = cur;
                  best_nan[p] = c_nan ? 1 : 0;
                } else if (best_nan[p] == 0 &&
                           (c_nan ||
                            (is_min ? (cur < best[p]) : (cur > best[p])))) {
                  best[p] = cur;
                  best_nan[p] = c_nan ? 1 : 0;
                }
              }
              for (size_t i = 0; i < shard_rows; ++i) {
                const uint32_t r = item_row[i];
                const uint32_t p = item_part[i];
                if (has_best[p] == 0) {
                  dst_null[r] = 1;
                  (*local_any_null)[w] = 1;
                } else {
                  dst[r] = best[p];
                }
              }
            }
          } else {
            const auto& src = int_cols[ac.typed_idx];
            uint8_t* dst_null = out_nulls[w].data();
            if (sp.prefix_op == PrefixOp::kSum ||
                sp.prefix_op == PrefixOp::kAvg) {
              std::vector<int64_t> sums(num_parts, 0);
              std::vector<int64_t> cnts(num_parts, 0);
              for (size_t i = 0; i < shard_rows; ++i) {
                const uint32_t r = item_row[i];
                if (!src.IsNull(r)) {
                  const uint32_t p = item_part[i];
                  if (__builtin_add_overflow(sums[p], src.vals[r], &sums[p])) {
                    int_overflowed.store(true, std::memory_order_relaxed);
                    return;
                  }
                  ++cnts[p];
                }
              }
              if (sp.prefix_op == PrefixOp::kAvg) {
                double* dst = out_doubles[w].data();
                std::vector<double> avgs(num_parts, 0.0);
                for (size_t p = 0; p < num_parts; ++p) {
                  if (cnts[p] > 0) {
                    avgs[p] = static_cast<double>(sums[p]) /
                              static_cast<double>(cnts[p]);
                  }
                }
                for (size_t i = 0; i < shard_rows; ++i) {
                  const uint32_t r = item_row[i];
                  const uint32_t p = item_part[i];
                  if (cnts[p] == 0) {
                    dst_null[r] = 1;
                    (*local_any_null)[w] = 1;
                  } else {
                    dst[r] = avgs[p];
                  }
                }
              } else {
                int64_t* dst = out_ints[w].data();
                for (size_t i = 0; i < shard_rows; ++i) {
                  const uint32_t r = item_row[i];
                  const uint32_t p = item_part[i];
                  if (cnts[p] == 0) {
                    dst_null[r] = 1;
                    (*local_any_null)[w] = 1;
                  } else {
                    dst[r] = sums[p];
                  }
                }
              }
            } else {
              const bool is_min = (sp.prefix_op == PrefixOp::kMin);
              std::vector<int64_t> best(num_parts, 0);
              std::vector<uint8_t> has_best(num_parts, 0);
              for (size_t i = 0; i < shard_rows; ++i) {
                const uint32_t r = item_row[i];
                if (src.IsNull(r)) {
                  continue;
                }
                const uint32_t p = item_part[i];
                const int64_t cur = src.vals[r];
                if (has_best[p] == 0 ||
                    (is_min ? (cur < best[p]) : (cur > best[p]))) {
                  has_best[p] = 1;
                  best[p] = cur;
                }
              }
              int64_t* dst = out_ints[w].data();
              for (size_t i = 0; i < shard_rows; ++i) {
                const uint32_t r = item_row[i];
                const uint32_t p = item_part[i];
                if (has_best[p] == 0) {
                  dst_null[r] = 1;
                  (*local_any_null)[w] = 1;
                } else {
                  dst[r] = best[p];
                }
              }
            }
          }
        }
        return;
      }

      // Case 2B: Build CSR partition arrays and sort per partition if needed.
      std::vector<size_t> part_offsets(num_parts + 1, 0);
      for (size_t p = 0; p < num_parts; ++p) {
        part_offsets[p + 1] = part_offsets[p] + part_counts[p];
      }
      std::vector<size_t> cursor = part_offsets;
      std::vector<uint32_t> part_rows(shard_rows);
      for (size_t i = 0; i < shard_rows; ++i) {
        part_rows[cursor[item_part[i]]++] = item_row[i];
      }

      const bool has_order = !grp.order_terms.empty();
      auto row_before = [&](uint32_t a, uint32_t b) noexcept -> bool {
        for (const MultiOrderTerm& term : grp.order_terms) {
          const int cmp = ord_compare_term(term, a, b);
          if (cmp != 0) {
            return cmp < 0;
          }
        }
        return a < b;
      };

      for (size_t p = 0; p < num_parts; ++p) {
        const size_t p_begin = part_offsets[p];
        const size_t p_end = part_offsets[p + 1];
        const size_t m = p_end - p_begin;
        if (m == 0) {
          continue;
        }
        if (has_order && m > 1) {
          std::sort(part_rows.begin() + static_cast<std::ptrdiff_t>(p_begin),
                    part_rows.begin() + static_cast<std::ptrdiff_t>(p_end),
                    row_before);
        }

        for (const MultiWinSpec& sp : grp.specs) {
          const size_t w = sp.win_idx;
          if (sp.kind == MultiWinSpec::Kind::kRowNumber) {
            int64_t* dst = out_ints[w].data();
            for (size_t k = 0; k < m; ++k) {
              dst[part_rows[p_begin + k]] = static_cast<int64_t>(k) + 1;
            }
            continue;
          }
          if (sp.kind == MultiWinSpec::Kind::kRank ||
              sp.kind == MultiWinSpec::Kind::kDenseRank) {
            int64_t* dst = out_ints[w].data();
            const bool dense = (sp.kind == MultiWinSpec::Kind::kDenseRank);
            int64_t rank = 1;
            dst[part_rows[p_begin]] = 1;
            for (size_t k = 1; k < m; ++k) {
              const uint32_t prev = part_rows[p_begin + k - 1];
              const uint32_t cur = part_rows[p_begin + k];
              if (has_order && !ord_peers_equal(grp.order_terms, prev, cur)) {
                rank = dense ? (rank + 1) : (static_cast<int64_t>(k) + 1);
              }
              dst[cur] = rank;
            }
            continue;
          }

          uint8_t* dst_null = out_nulls[w].data();
          const PrefixFrameMode eff_mode =
              (!has_order && sp.frame_mode == PrefixFrameMode::kPeer)
                  ? PrefixFrameMode::kWholePartition
                  : sp.frame_mode;
          auto run_prefix = [&](auto&& advance, auto&& emit_row) -> bool {
            if (eff_mode == PrefixFrameMode::kWholePartition) {
              for (size_t k = 0; k < m; ++k) {
                if (!advance(part_rows[p_begin + k])) {
                  return false;
                }
              }
              for (size_t k = 0; k < m; ++k) {
                emit_row(part_rows[p_begin + k]);
              }
              return true;
            }
            if (eff_mode == PrefixFrameMode::kRows) {
              for (size_t k = 0; k < m; ++k) {
                const uint32_t row = part_rows[p_begin + k];
                if (!advance(row)) {
                  return false;
                }
                emit_row(row);
              }
              return true;
            }
            size_t k = 0;
            while (k < m) {
              const uint32_t cur = part_rows[p_begin + k];
              size_t end = k;
              while (end + 1 < m &&
                     ord_peers_equal(grp.order_terms, cur,
                                     part_rows[p_begin + end + 1])) {
                ++end;
              }
              for (size_t q = k; q <= end; ++q) {
                if (!advance(part_rows[p_begin + q])) {
                  return false;
                }
              }
              for (size_t q = k; q <= end; ++q) {
                emit_row(part_rows[p_begin + q]);
              }
              k = end + 1;
            }
            return true;
          };

          if (sp.prefix_op == PrefixOp::kCount) {
            int64_t count = 0;
            int64_t* dst = out_ints[w].data();
            if (sp.arg_col_idx == SIZE_MAX) {
              run_prefix(
                  [&](uint32_t /*r*/) {
                    ++count;
                    return true;
                  },
                  [&](uint32_t r) { dst[r] = count; });
            } else {
              const size_t aci = sp.arg_col_idx;
              run_prefix(
                  [&](uint32_t r) {
                    if (!col_is_null(aci, r)) {
                      ++count;
                    }
                    return true;
                  },
                  [&](uint32_t r) { dst[r] = count; });
            }
          } else if (cols[sp.arg_col_idx].type == ValueType::kDouble) {
            const auto& src = dbl_cols[cols[sp.arg_col_idx].typed_idx];
            double* dst = out_doubles[w].data();
            if (sp.prefix_op == PrefixOp::kSum ||
                sp.prefix_op == PrefixOp::kAvg) {
              double total = 0.0;
              int64_t count = 0;
              run_prefix(
                  [&](uint32_t r) {
                    if (!src.IsNull(r)) {
                      total += src.vals[r];
                      ++count;
                    }
                    return true;
                  },
                  [&](uint32_t r) {
                    if (count == 0) {
                      dst_null[r] = 1;
                      (*local_any_null)[w] = 1;
                    } else {
                      dst[r] = sp.prefix_op == PrefixOp::kAvg
                                   ? (total / static_cast<double>(count))
                                   : total;
                    }
                  });
            } else {
              bool has_best = false;
              double best = 0.0;
              bool best_nan = false;
              const bool is_min = (sp.prefix_op == PrefixOp::kMin);
              run_prefix(
                  [&](uint32_t r) {
                    if (!src.IsNull(r)) {
                      const double cur = src.vals[r];
                      const bool cur_nan = std::isnan(cur);
                      if (!has_best) {
                        has_best = true;
                        best = cur;
                        best_nan = cur_nan;
                      } else if (!best_nan &&
                                 (cur_nan ||
                                  (is_min ? (cur < best) : (cur > best)))) {
                        best = cur;
                        best_nan = cur_nan;
                      }
                    }
                    return true;
                  },
                  [&](uint32_t r) {
                    if (!has_best) {
                      dst_null[r] = 1;
                      (*local_any_null)[w] = 1;
                    } else {
                      dst[r] = best;
                    }
                  });
            }
          } else {
            const auto& src = int_cols[cols[sp.arg_col_idx].typed_idx];
            if (sp.prefix_op == PrefixOp::kSum ||
                sp.prefix_op == PrefixOp::kAvg) {
              int64_t total = 0;
              int64_t count = 0;
              const bool is_avg = (sp.prefix_op == PrefixOp::kAvg);
              const bool ok = run_prefix(
                  [&](uint32_t r) {
                    if (!src.IsNull(r)) {
                      if (__builtin_add_overflow(total, src.vals[r], &total)) {
                        int_overflowed.store(true, std::memory_order_relaxed);
                        return false;
                      }
                      ++count;
                    }
                    return true;
                  },
                  [&](uint32_t r) {
                    if (count == 0) {
                      dst_null[r] = 1;
                      (*local_any_null)[w] = 1;
                    } else if (is_avg) {
                      out_doubles[w][r] = static_cast<double>(total) /
                                          static_cast<double>(count);
                    } else {
                      out_ints[w][r] = total;
                    }
                  });
              if (!ok) {
                return;
              }
            } else {
              int64_t* dst = out_ints[w].data();
              bool has_best = false;
              int64_t best = 0;
              const bool is_min = (sp.prefix_op == PrefixOp::kMin);
              run_prefix(
                  [&](uint32_t r) {
                    if (!src.IsNull(r)) {
                      const int64_t cur = src.vals[r];
                      if (!has_best) {
                        has_best = true;
                        best = cur;
                      } else if (is_min ? (cur < best) : (cur > best)) {
                        best = cur;
                      }
                    }
                    return true;
                  },
                  [&](uint32_t r) {
                    if (!has_best) {
                      dst_null[r] = 1;
                      (*local_any_null)[w] = 1;
                    } else {
                      dst[r] = best;
                    }
                  });
            }
          }
        }
      }
    };

    const size_t shard_workers = std::min<size_t>(workers, num_shards);
    std::vector<std::vector<uint8_t>> worker_any_null(
        shard_workers, std::vector<uint8_t>(win_count, 0));
    if (shard_workers <= 1) {
      for (size_t s = 0; s < num_shards; ++s) {
        process_shard(s, worker_any_null.data());
      }
    } else {
      RETURN_IF_FAIL(RunParallelTasks(shard_workers, [&](size_t wid) {
        for (size_t s = wid; s < num_shards; s += shard_workers) {
          if (int_overflowed.load(std::memory_order_relaxed)) {
            return;
          }
          process_shard(s, &worker_any_null[wid]);
        }
      }));
    }
    if (int_overflowed.load(std::memory_order_relaxed)) {
      return std::nullopt;
    }
    for (size_t wid = 0; wid < shard_workers; ++wid) {
      for (size_t w = 0; w < win_count; ++w) {
        win_any_null[w] |= worker_any_null[wid][w];
      }
    }
  }

  std::vector<Column> extended_columns;
  const size_t kept_width = kept_slots.size();
  extended_columns.reserve(kept_width + win_count);
  for (size_t slot : kept_slots) {
    extended_columns.push_back(schema.GetColumn(slot));
  }
  for (size_t w = 0; w < win_count; ++w) {
    extended_columns.emplace_back(win_names[w], out_types[w]);
  }
  bool out_uniform = true;
  for (const ColumnarChunk& ch : table.chunks) {
    if (ch.rows == 0) {
      continue;
    }
    for (size_t slot : kept_slots) {
      if (slot >= ch.columns.size() || !ch.columns[slot].is_uniform) {
        out_uniform = false;
        break;
      }
    }
    if (!out_uniform) {
      break;
    }
  }
  const bool single_chunk = (table.chunks.size() == 1);
  Relation out;
  RETURN_IF_FAIL(AssembleColumnarWindowOutput(
      std::move(input), kept_slots, std::move(extended_columns), out_uniform,
      [&](size_t /*ci*/, size_t row_off, size_t chunk_rows,
          ColumnarChunk* dst_chunk) {
        for (size_t w = 0; w < win_count; ++w) {
          ColumnarColumn& dst = dst_chunk->columns[kept_width + w];
          const ValueType ot = out_types[w];
          if (ot == ValueType::kNull) {
            dst.SetConstantNull(ValueType::kNull, false, false, chunk_rows);
            continue;
          }
          dst.Init(ot, false, false, 0);
          bool chunk_has_null = false;
          if (win_any_null[w] != 0 && !out_nulls[w].empty()) {
            const uint8_t* src_n = out_nulls[w].data() + row_off;
            for (size_t r = 0; r < chunk_rows; ++r) {
              if (src_n[r] != 0) {
                chunk_has_null = true;
                break;
              }
            }
          }
          if (single_chunk) {
            if (ot == ValueType::kDouble) {
              dst.doubles = std::move(out_doubles[w]);
            } else {
              dst.ints = std::move(out_ints[w]);
            }
            if (chunk_has_null) {
              dst.has_null = true;
              dst.nulls = std::move(out_nulls[w]);
            }
          } else {
            const auto off_d = static_cast<std::ptrdiff_t>(row_off);
            const auto end_d =
                static_cast<std::ptrdiff_t>(row_off + chunk_rows);
            if (ot == ValueType::kDouble) {
              dst.doubles.assign(out_doubles[w].begin() + off_d,
                                 out_doubles[w].begin() + end_d);
            } else {
              dst.ints.assign(out_ints[w].begin() + off_d,
                              out_ints[w].begin() + end_d);
            }
            if (chunk_has_null) {
              dst.has_null = true;
              dst.nulls.assign(out_nulls[w].begin() + off_d,
                               out_nulls[w].begin() + end_d);
            }
          }
        }
      },
      &out));
  return std::optional<Relation>{std::move(out)};
}

StatusOr<Relation> RunGeneralColumnarWindows(
    TransactionContext& context, Relation&& input,
    std::span<const WindowFunctionCallExpression* const> win_nodes,
    std::span<const std::string> win_names, const Scope* outer,
    const CteMap& ctes, std::span<const size_t> kept_slots) {
  const ColumnarTable& table = *input.columnar;
  const size_t n = table.total_rows;
  const size_t win_count = win_nodes.size();
  const Schema& base_schema = input.schema;
  const size_t base_width = base_schema.ColumnCount();

  std::vector<uint8_t> used(base_width, 0);
  bool need_all = (outer != nullptr);
  for (const WindowFunctionCallExpression* win : win_nodes) {
    for (const Expression& p : win->partition_by) {
      CollectWindowReferencedCols(p, base_schema, &used, &need_all);
    }
    for (const WindowOrderTerm& o : win->order_by) {
      CollectWindowReferencedCols(o.expression, base_schema, &used, &need_all);
    }
    for (const Expression& a : win->args) {
      CollectWindowReferencedCols(a, base_schema, &used, &need_all);
    }
    CollectWindowReferencedCols(win->where_filter, base_schema, &used,
                                &need_all);
    for (const WindowOrderTerm& io : win->inner_order_by) {
      CollectWindowReferencedCols(io.expression, base_schema, &used, &need_all);
    }
    if (need_all) {
      break;
    }
  }

  std::vector<size_t> eval_src_cols;
  eval_src_cols.reserve(base_width);
  bool compact_schema =
      !need_all && !HasDuplicateColumnNamesCaseInsensitive(base_schema);
  if (compact_schema) {
    for (size_t c = 0; c < base_width; ++c) {
      if (used[c] != 0) {
        eval_src_cols.push_back(c);
      }
    }
    if (eval_src_cols.size() == base_width) {
      compact_schema = false;
    }
  }

  std::vector<Column> eval_cols;
  const size_t row_width = compact_schema ? eval_src_cols.size() : base_width;
  if (compact_schema) {
    eval_cols.reserve(row_width);
    for (size_t c : eval_src_cols) {
      eval_cols.push_back(base_schema.GetColumn(c));
    }
  } else {
    eval_cols.reserve(base_width);
    for (size_t c = 0; c < base_width; ++c) {
      eval_cols.push_back(base_schema.GetColumn(c));
    }
  }
  const Schema eval_schema(base_schema.Name(), std::move(eval_cols));

  std::vector<Row> eval_rows(n);
  std::vector<size_t> chunk_offsets(table.chunks.size(), 0);
  size_t off_acc = 0;
  for (size_t ci = 0; ci < table.chunks.size(); ++ci) {
    chunk_offsets[ci] = off_acc;
    off_acc += table.chunks[ci].rows;
  }

  const size_t mat_workers =
      (n < 16384 && table.chunks.size() < 4)
          ? 1
          : std::min<size_t>(
                {8, table.chunks.size() > 1 ? table.chunks.size() : (n / 4096),
                 QueryParallelWorkerLimit(
                     std::thread::hardware_concurrency())});
  if (table.chunks.size() == 1 && mat_workers > 1) {
    const ColumnarChunk& ch = table.chunks[0];
    RETURN_IF_FAIL(RunParallelTasks(mat_workers, [&](size_t worker) {
      const size_t begin = n * worker / mat_workers;
      const size_t end = n * (worker + 1) / mat_workers;
      for (size_t r = begin; r < end; ++r) {
        auto& vals = eval_rows[r].values_;
        vals.resize(row_width);
        if (compact_schema) {
          for (size_t dc = 0; dc < row_width; ++dc) {
            ch.columns[eval_src_cols[dc]].WriteValueInto(r, &vals[dc]);
          }
        } else {
          for (size_t c = 0; c < base_width; ++c) {
            if (need_all || used[c] != 0) {
              ch.columns[c].WriteValueInto(r, &vals[c]);
            }
          }
        }
      }
    }));
  } else {
    const size_t chunk_workers = std::min(mat_workers, table.chunks.size());
    RETURN_IF_FAIL(RunParallelTasks(chunk_workers, [&](size_t worker) {
      for (size_t ci = worker; ci < table.chunks.size(); ci += chunk_workers) {
        const ColumnarChunk& ch = table.chunks[ci];
        const size_t off = chunk_offsets[ci];
        for (size_t r = 0; r < ch.rows; ++r) {
          auto& vals = eval_rows[off + r].values_;
          vals.resize(row_width);
          if (compact_schema) {
            for (size_t dc = 0; dc < row_width; ++dc) {
              ch.columns[eval_src_cols[dc]].WriteValueInto(r, &vals[dc]);
            }
          } else {
            for (size_t c = 0; c < base_width; ++c) {
              if (need_all || used[c] != 0) {
                ch.columns[c].WriteValueInto(r, &vals[c]);
              }
            }
          }
        }
      }
    }));
  }

  std::vector<std::vector<Value>> computed(win_count);
  std::vector<WindowColTypeSummary> final_types(win_count);
  std::unordered_map<std::string, WindowOrderLayout> layouts;
  bool fused_done = false;

  if (win_count > 0) {
    const std::string first_key = WindowLayoutKey(*win_nodes[0]);
    bool single_layout = true;
    bool all_prefix = true;
    std::vector<std::optional<size_t>> prefix_slots(win_count);
    for (size_t w = 0; w < win_count; ++w) {
      if (WindowLayoutKey(*win_nodes[w]) != first_key) {
        single_layout = false;
      }
      if (!IsPrefixWindowEligible(*win_nodes[w], eval_schema,
                                  &prefix_slots[w])) {
        all_prefix = false;
      }
    }
    if (single_layout && all_prefix) {
      bool need_peer_end = false;
      bool need_order_values = false;
      for (const auto* wn : win_nodes) {
        bool wp = true;
        const bool wv = WindowLayoutNeedsOrderValues(*wn, &wp);
        need_order_values = need_order_values || wv;
        need_peer_end = need_peer_end || wp;
      }
      WindowRuntime runtime{.context = context,
                            .outer = outer,
                            .ctes = &ctes,
                            .schema = &eval_schema,
                            .uniform_cell_types = input.uniform_cell_types};
      ASSIGN_OR_RETURN(WindowOrderLayout, built_layout,
                       (BuildWindowOrderLayout(
                           *win_nodes[0], eval_rows, runtime, std::nullopt,
                           need_order_values, need_peer_end)));
      for (size_t w = 0; w < win_count; ++w) {
        computed[w].resize(n);
      }
      const size_t fused_workers =
          n < 16384 || built_layout.partitions.size() <= 1
              ? 1
              : std::min<size_t>({8, built_layout.partitions.size(),
                                  QueryParallelWorkerLimit(
                                      std::thread::hardware_concurrency())});
      std::vector<std::vector<Status>> errors(
          win_count, std::vector<Status>(fused_workers, Status::kSuccess));
      std::vector<std::vector<size_t>> error_partitions(
          win_count, std::vector<size_t>(fused_workers, SIZE_MAX));
      std::vector<std::vector<WindowColTypeSummary>> worker_types(
          fused_workers, std::vector<WindowColTypeSummary>(win_count));
      std::atomic<bool> any_unhandled{false};
      RETURN_IF_FAIL(RunParallelTasks(fused_workers, [&](size_t worker) {
        auto& local_types = worker_types[worker];
        for (size_t p = worker; p < built_layout.partitions.size();
             p += fused_workers) {
          const auto& partition = built_layout.partitions[p];
          std::span<const size_t> ordered = built_layout.Ordered(partition);
          for (size_t w = 0; w < win_count; ++w) {
            WindowColTypeSummary& summary = local_types[w];
            const auto res = TryComputeColumnPrefixWithSummary(
                *win_nodes[w], eval_rows, *prefix_slots[w], ordered,
                partition.peer_end, eval_schema, input.uniform_cell_types,
                computed[w], summary);
            if (!res.HasValue()) {
              if (errors[w][worker].ok()) {
                errors[w][worker] = res.GetStatus();
                error_partitions[w][worker] = p;
              }
              break;
            }
            if (!res.Value()) {
              any_unhandled.store(true, std::memory_order_relaxed);
              break;
            }
          }
        }
      }));
      if (any_unhandled.load(std::memory_order_relaxed)) {
        for (size_t w = 0; w < win_count; ++w) {
          computed[w].clear();
        }
        layouts.emplace(first_key, std::move(built_layout));
      } else {
        for (size_t w = 0; w < win_count; ++w) {
          const auto first = std::ranges::min_element(error_partitions[w]);
          if (*first != SIZE_MAX) {
            return errors[w][static_cast<size_t>(first -
                                                 error_partitions[w].begin())];
          }
          for (size_t worker = 0; worker < fused_workers; ++worker) {
            final_types[w].MergeFrom(worker_types[worker][w]);
          }
        }
        fused_done = true;
      }
    }
  }

  if (!fused_done) {
    for (size_t w = 0; w < win_count; ++w) {
      const WindowFunctionCallExpression* wn = win_nodes[w];
      const std::string layout_key = WindowLayoutKey(*wn);
      auto [layout_it, inserted] = layouts.try_emplace(layout_key);
      if (inserted) {
        bool need_peer_end = false;
        bool need_order_values = false;
        for (const auto* other_wn : win_nodes) {
          if (WindowLayoutKey(*other_wn) == layout_key) {
            bool wp = true;
            const bool wv = WindowLayoutNeedsOrderValues(*other_wn, &wp);
            need_order_values = need_order_values || wv;
            need_peer_end = need_peer_end || wp;
          }
        }
        WindowRuntime runtime{.context = context,
                              .outer = outer,
                              .ctes = &ctes,
                              .schema = &eval_schema,
                              .uniform_cell_types = input.uniform_cell_types};
        ASSIGN_OR_RETURN(
            WindowOrderLayout, built_layout,
            (BuildWindowOrderLayout(*wn, eval_rows, runtime, std::nullopt,
                                    need_order_values, need_peer_end)));
        layout_it->second = std::move(built_layout);
      }
      RETURN_IF_FAIL(ComputeOneWindowImpl(
          context, *wn, eval_rows, eval_schema, outer, ctes, &computed[w],
          &layout_it->second, input.uniform_cell_types));
      for (const Value& v : computed[w]) {
        final_types[w].Observe(v);
      }
    }
  }

  std::vector<Row>().swap(eval_rows);

  const size_t kept_width = kept_slots.size();
  std::vector<Column> extended_columns;
  extended_columns.reserve(kept_width + win_count);
  for (size_t slot : kept_slots) {
    extended_columns.push_back(base_schema.GetColumn(slot));
  }
  bool propagated_uniform = input.uniform_cell_types;
  for (size_t w = 0; w < win_count; ++w) {
    extended_columns.emplace_back(win_names[w], ValueType::kNull);
    final_types[w].ApplyToColumn(&extended_columns.back(), &propagated_uniform);
  }

  Relation out;
  RETURN_IF_FAIL(AssembleColumnarWindowOutput(
      std::move(input), kept_slots, std::move(extended_columns),
      propagated_uniform,
      [&](size_t /*ci*/, size_t row_off, size_t chunk_rows,
          ColumnarChunk* dst_chunk) {
        ChunkStringArena* arena = nullptr;
        for (size_t w = 0; w < win_count; ++w) {
          ColumnarColumn& dst = dst_chunk->columns[kept_width + w];
          const WindowColTypeSummary& summary = final_types[w];
          if (summary.mixed) {
            dst.Init(ValueType::kArray, false, false, chunk_rows);
          } else if (summary.seen_non_null) {
            dst.Init(summary.type, summary.is_unsigned, summary.is_interval,
                     chunk_rows);
          } else {
            dst.SetConstantNull(ValueType::kNull, false, false, chunk_rows);
            continue;
          }
          for (size_t r = 0; r < chunk_rows; ++r) {
            Value v = std::move(computed[w][row_off + r]);
            if (!v.IsNull() && v.type == ValueType::kVarChar) {
              if (arena == nullptr) {
                arena = dst_chunk->EnsureArena();
              }
              if (!dst.is_uniform) {
                v.value.varchar_value = arena->Store(v.value.varchar_value);
              }
            }
            dst.AppendValue(v, arena, r);
          }
        }
      },
      &out));
  return out;
}

}  // namespace

StatusOr<Relation> ExtendColumnarRelationWithWindowsImpl(
    TransactionContext& context, Relation&& input,
    std::span<const WindowFunctionCallExpression* const> win_nodes,
    std::span<const std::string> win_names, const Scope* outer,
    const CteMap& ctes, std::optional<std::span<const size_t>> keep_base_slots,
    bool record_window_timer) {
  double* window_ms = nullptr;
  if (record_window_timer) {
    if (auto* runtime_stats = context.execution_runtime()) {
      window_ms = &runtime_stats->window_ms;
      runtime_stats->window_input_rows +=
          input.HasColumnarOnly() ? input.columnar->total_rows : 0;
    }
  }
  PhaseTimer window_timer(window_ms);

  const Schema& base_schema = input.schema;
  const size_t base_width = base_schema.ColumnCount();
  std::vector<size_t> default_slots;
  std::span<const size_t> kept_slots;
  if (keep_base_slots.has_value()) {
    kept_slots = *keep_base_slots;
  } else {
    default_slots.resize(base_width);
    std::iota(default_slots.begin(), default_slots.end(), 0);
    kept_slots = default_slots;
  }

  RETURN_IF_FAIL(input.FinishSpill());
  RETURN_IF_FAIL(input.GetStatus());
  if (!input.HasColumnarOnly()) {
    std::vector<Column> extended_columns;
    extended_columns.reserve(kept_slots.size() + win_names.size());
    for (size_t s : kept_slots) {
      extended_columns.push_back(base_schema.GetColumn(s));
    }
    for (const auto& name : win_names) {
      extended_columns.emplace_back(name, ValueType::kNull);
    }
    Relation empty(context.execution_runtime());
    empty.schema = Schema(base_schema.Name(), std::move(extended_columns));
    CopyExecutionStats(&empty, input);
    return empty;
  }

  ASSIGN_OR_RETURN(
      std::optional<Relation>, flat,
      (TryComputeFlatColumnarWindows(input, win_nodes, win_names, kept_slots)));
  if (flat.has_value()) {
    return std::move(*flat);
  }
  ASSIGN_OR_RETURN(std::optional<Relation>, multi_flat,
                   (TryComputeMultiKeyColumnarWindows(input, win_nodes,
                                                      win_names, kept_slots)));
  if (multi_flat.has_value()) {
    return std::move(*multi_flat);
  }
  return RunGeneralColumnarWindows(context, std::move(input), win_nodes,
                                   win_names, outer, ctes, kept_slots);
}

StatusOr<Relation> ExtendColumnarRelationWithWindows(
    TransactionContext& context, Relation&& input,
    std::span<const WindowFunctionCallExpression* const> win_nodes,
    std::span<const std::string> win_names, const Scope* outer,
    const CteMap& ctes,
    std::optional<std::span<const size_t>> keep_base_slots) {
  return ExtendColumnarRelationWithWindowsImpl(
      context, std::move(input), win_nodes, win_names, outer, ctes,
      keep_base_slots, /*record_window_timer=*/true);
}

}  // namespace tinylamb::relational_detail
