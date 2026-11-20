/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/window_function_expression.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb::relational_detail {

struct PhaseTimer {
  double* slot{nullptr};
  std::chrono::steady_clock::time_point begin{std::chrono::steady_clock::now()};
  explicit PhaseTimer(double* slot_in) : slot(slot_in) {}
  ~PhaseTimer() {
    if (slot == nullptr) {
      return;
    }
    *slot += std::chrono::duration<double, std::milli>(
                 std::chrono::steady_clock::now() - begin)
                 .count();
  }
  PhaseTimer(const PhaseTimer&) = delete;
  PhaseTimer& operator=(const PhaseTimer&) = delete;
  PhaseTimer(PhaseTimer&&) = delete;
  PhaseTimer& operator=(PhaseTimer&&) = delete;
};

struct WindowOrderValues {
  WindowOrderValues() = default;
  WindowOrderValues(size_t rows, size_t columns)
      : row_count(rows), width(columns), values(rows * columns) {}
  WindowOrderValues(std::vector<Row>& rows, std::span<const size_t> permutation,
                    size_t column)
      : row_count(permutation.size()),
        width(1),
        source(&rows),
        ordered(permutation),
        source_column(column) {}
  WindowOrderValues(const WindowOrderValues&) = delete;
  WindowOrderValues& operator=(const WindowOrderValues&) = delete;
  WindowOrderValues(WindowOrderValues&&) noexcept = default;
  WindowOrderValues& operator=(WindowOrderValues&&) noexcept = default;
  ~WindowOrderValues() = default;

  [[nodiscard]] bool empty() const { return row_count == 0; }
  std::span<Value> operator[](size_t row) {
    if (source != nullptr) {
      return {&(*source)[ordered[row]].values_[source_column], 1};
    }
    return width == 0 ? std::span<Value>{}
                      : std::span<Value>(values.data() + (row * width), width);
  }
  std::span<const Value> operator[](size_t row) const {
    if (source != nullptr) {
      return {&(*source)[ordered[row]].values_[source_column], 1};
    }
    return width == 0
               ? std::span<const Value>{}
               : std::span<const Value>(values.data() + (row * width), width);
  }
  size_t row_count{0};
  size_t width{0};
  std::vector<Value> values;
  std::vector<Row>* source{nullptr};
  std::span<const size_t> ordered;
  size_t source_column{0};
};

struct WindowPartitionLayout {
  std::vector<size_t> ordered;
  size_t flat_begin{0};
  size_t flat_end{0};
  size_t first_row{0};
  WindowOrderValues order_values;
  std::vector<size_t> peer_end;
  // End position of each peer group, one entry per group (ascending). Built
  // once alongside peer_end so GROUPS frames locate a group by binary search
  // instead of rebuilding the table for every row.
  std::vector<size_t> group_ends;
};

struct WindowOrderLayout {
  std::vector<size_t> flat_ordered;
  std::vector<WindowPartitionLayout> partitions;

  [[nodiscard]] std::span<const size_t> Ordered(
      const WindowPartitionLayout& partition) const {
    if (!flat_ordered.empty()) {
      return {flat_ordered.data() + partition.flat_begin,
              partition.flat_end - partition.flat_begin};
    }
    return {partition.ordered};
  }

  [[nodiscard]] std::span<size_t> MutableOrdered(
      WindowPartitionLayout& partition) {
    if (!flat_ordered.empty()) {
      return {flat_ordered.data() + partition.flat_begin,
              partition.flat_end - partition.flat_begin};
    }
    return {partition.ordered};
  }
};

struct WindowRuntime {
  TransactionContext& context;
  const Scope* outer{nullptr};
  const CteMap* ctes{nullptr};
  const Schema* schema{nullptr};
  bool uniform_cell_types{false};

  [[nodiscard]] StatusOr<Value> EvalAt(const Expression& expression,
                                       const std::vector<Row>& rows,
                                       size_t position) const {
    Scope scope{.row = &rows[position], .schema = schema, .outer = outer};
    return TryEvaluate(expression, scope, nullptr, context, *ctes);
  }

  [[nodiscard]] static StatusOr<std::pair<size_t, size_t>> ResolveFrame(
      const WindowFunctionCallExpression& window, const std::vector<Row>& rows,
      std::span<const size_t> ordered, const WindowOrderValues& order_values,
      size_t position, const std::vector<size_t>& peer_end,
      const std::vector<size_t>& group_ends);

  [[nodiscard]] StatusOr<std::vector<size_t>> FrameSurvivors(
      const WindowFunctionCallExpression& window, const std::vector<Row>& rows,
      std::span<const size_t> ordered, const WindowOrderValues& order_values,
      size_t current, size_t lo, size_t hi) const;

  [[nodiscard]] StatusOr<Value> AggregateOverFrame(
      const WindowFunctionCallExpression& window, const std::vector<Row>& rows,
      std::span<const size_t> ordered, const WindowOrderValues& order_values,
      size_t current, size_t lo, size_t hi) const;
};

struct WindowColTypeSummary {
  bool seen_non_null{false};
  bool mixed{false};
  // kArray cells or IsCaseInsensitive tags break the uniform_cell_types
  // contract ("never kArray, never IsCaseInsensitive()") even when every
  // cell shares one ValueType — mirror ColumnTypeTracker::saw_unsafe.
  bool saw_unsafe{false};
  bool is_unsigned{false};
  bool is_interval{false};
  ValueType first_type{ValueType::kNull};
  ValueType type{ValueType::kNull};

  void Observe(const Value& v) {
    if (v.type == ValueType::kArray || v.IsCaseInsensitive()) {
      saw_unsafe = true;
    }
    if (v.IsNull()) {
      return;
    }
    const ValueType vt = ValueTypeOf(v);
    if (vt == ValueType::kNull) {
      return;
    }
    const bool u = v.IsUnsigned();
    const bool iv = v.IsInterval();
    if (!seen_non_null) {
      seen_non_null = true;
      first_type = vt;
      type = vt;
      is_unsigned = u;
      is_interval = iv;
    } else if (type != vt || is_unsigned != u || is_interval != iv) {
      mixed = true;
    }
  }

  void MergeFrom(const WindowColTypeSummary& other) {
    const bool unsafe = saw_unsafe || other.saw_unsafe;
    if (!other.seen_non_null) {
      saw_unsafe = unsafe;
      return;
    }
    if (!seen_non_null) {
      *this = other;
      saw_unsafe = unsafe;
      return;
    }
    saw_unsafe = unsafe;
    if (other.mixed || type != other.type || is_unsigned != other.is_unsigned ||
        is_interval != other.is_interval) {
      mixed = true;
    }
  }

  void ApplyToColumn(Column* col, bool* all_uniform) const {
    if (mixed) {
      *col = Column(col->Name(), first_type);
      *all_uniform = false;
    } else if (seen_non_null) {
      *col = Column(col->Name(), type);
      col->SetInterval(is_interval);
      col->SetUnsigned(is_unsigned);
      if (saw_unsafe || type == ValueType::kArray) {
        *all_uniform = false;
      }
    } else if (saw_unsafe || col->Type() == ValueType::kNull ||
               col->Type() == ValueType::kArray) {
      *all_uniform = false;
    }
  }
};

enum class PrefixFrameMode { kWholePartition, kRows, kPeer };
enum class PrefixOp { kCount, kSum, kAvg, kMin, kMax };

PrefixFrameMode ClassifyPrefixFrame(const WindowFunctionCallExpression& window);
PrefixOp ClassifyPrefixOp(const std::string& fn);

bool IsPrefixWindowEligible(const WindowFunctionCallExpression& window,
                            const Schema& schema,
                            std::optional<size_t>* slot_out);

std::string WindowLayoutKey(const WindowFunctionCallExpression& window);

bool WindowLayoutNeedsOrderValues(const WindowFunctionCallExpression& window,
                                  bool* need_peer_end);

StatusOr<WindowOrderLayout> BuildWindowOrderLayout(
    const WindowFunctionCallExpression& window, std::vector<Row>& rows,
    const WindowRuntime& runtime,
    std::optional<size_t> rank_bound = std::nullopt,
    bool need_order_values = true, bool need_peer_end = true);

StatusOr<bool> TryComputeColumnPrefixWithSummary(
    const WindowFunctionCallExpression& window, const std::vector<Row>& rows,
    size_t slot, std::span<const size_t> ordered,
    const std::vector<size_t>& peer_end, const Schema& schema,
    bool uniform_cell_types, std::vector<Value>& computed,
    WindowColTypeSummary& summary);

Status ComputeOneWindowImpl(TransactionContext& context,
                            const WindowFunctionCallExpression& window,
                            std::vector<Row>& rows, const Schema& schema,
                            const Scope* outer, const CteMap& ctes,
                            std::vector<Value>* out,
                            const WindowOrderLayout* cached_layout = nullptr,
                            bool uniform_cell_types = false);

StatusOr<Relation> ExtendColumnarRelationWithWindowsImpl(
    TransactionContext& context, Relation&& input,
    std::span<const WindowFunctionCallExpression* const> win_nodes,
    std::span<const std::string> win_names, const Scope* outer,
    const CteMap& ctes,
    std::optional<std::span<const size_t>> keep_base_slots = std::nullopt,
    bool record_window_timer = false);

}  // namespace tinylamb::relational_detail
