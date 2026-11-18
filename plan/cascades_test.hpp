/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "gtest/gtest.h"
#include "plan/cascades.hpp"
#include "plan/implementation_rules.hpp"
#include "plan/set_operation_plan.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::cascades {

inline Expression EqExp(std::string_view column, const Value& value) {
  return BinaryExpressionExp(ColumnValueExp(column), BinaryOperation::kEquals,
                             ConstantValueExp(value));
}

inline std::shared_ptr<WindowFunctionCallExpression> PrefixWindowCall(
    std::string function, std::vector<Expression> args,
    std::vector<Expression> partition_by = {},
    std::vector<WindowOrderTerm> order_by = {}, bool has_frame = false) {
  auto call = std::make_shared<WindowFunctionCallExpression>();
  call->function = std::move(function);
  call->args = std::move(args);
  call->partition_by = std::move(partition_by);
  call->order_by = std::move(order_by);
  call->has_frame = has_frame;
  if (has_frame) {
    call->frame_unit = WindowFrameUnit::kRows;
    call->frame_start.type = WindowFrameBoundType::kUnboundedPreceding;
    call->frame_end.type = WindowFrameBoundType::kCurrentRow;
  }
  return call;
}

// True when some kWindow node in the memo has a child group holding a
// Limit/TopN over the window's input (the limit-prefix shape).  The
// negative tests assert this stays absent.
inline bool AnyWindowHasLimitedChild(const Memo& memo) {
  for (size_t id = 0; id < memo.GroupCount(); ++id) {
    for (const LogicalExpression& window : memo.Get(GroupId(id)).expressions) {
      if (window.operation != LogicalOperator::kWindow ||
          window.children.size() != 1) {
        continue;
      }
      for (const LogicalExpression& child :
           memo.Get(window.children[0]).expressions) {
        if (child.operation == LogicalOperator::kLimit ||
            child.operation == LogicalOperator::kTopN) {
          return true;
        }
      }
    }
  }
  return false;
}

inline bool GroupHasLimitWithDifferentChild(const Memo& memo,
                                            GroupId limit_group,
                                            GroupId original_child) {
  for (const LogicalExpression& limit : memo.Get(limit_group).expressions) {
    if (limit.operation != LogicalOperator::kLimit ||
        limit.children.size() != 1 || limit.children[0] == original_child) {
      continue;
    }
    for (const LogicalExpression& window :
         memo.Get(limit.children[0]).expressions) {
      if (window.operation == LogicalOperator::kWindow &&
          window.children.size() == 1) {
        return true;
      }
    }
  }
  return false;
}

// Shared memo shape for the TopN-over-1:1-join tests: fact(dim_id) JOIN
// dim(id) with TopN(fact.id, 5) directly above the join (i.e. the state
// after topn_push_through_projection). The caller decides the proof:
// - fk=true: fact.dim_id carries Constraint::kForeign -> "dim".
// - mark=true: memo.MarkProvenOneToOne("fact", "dim") (snapshot path).
// dim.id is always PRIMARY KEY so the <=1-match side is proven.
struct TopNProvenJoinFixture {
  Memo memo;
  GroupId fact{kInvalidGroup};
  GroupId dim{kInvalidGroup};
  GroupId topn{kInvalidGroup};

  TopNProvenJoinFixture(bool fk, bool mark,
                        std::string fk_reference = "dim(id)",
                        bool filter_null = true, bool residual = false,
                        bool other_side_filter = false) {
    std::vector<Column> fact_columns = {
        Column("id", ValueType::kInt64, Constraint(Constraint::kPrimaryKey)),
        Column("dim_id", ValueType::kInt64,
               fk ? Constraint(Constraint::kForeign,
                               Value(std::move(fk_reference)))
                  : Constraint(Constraint::kNothing)),
        Column("payload", ValueType::kInt64)};
    const Schema fact_schema("fact", std::move(fact_columns));
    const Schema dim_schema("dim", {Column("id", ValueType::kInt64,
                                           Constraint(Constraint::kPrimaryKey)),
                                    Column("val", ValueType::kInt64)});
    memo.SetTableSchemas({{"fact", fact_schema}, {"dim", dim_schema}});
    (void)memo.Build({"fact", "dim"});
    if (mark) {
      memo.MarkProvenOneToOne("fact", "dim");
    }
    const GroupId fact_base = memo.EnsureGroup({"fact"});
    memo.AddExpression(fact_base,
                       LogicalExpression{.operation = LogicalOperator::kScan,
                                         .table = "fact",
                                         .output_schema = fact_schema});
    if (filter_null) {
      fact = memo.EnsureDerivedGroup({"fact"}, "fact_not_null");
      memo.AddExpression(
          fact,
          LogicalExpression{.operation = LogicalOperator::kSelection,
                            .children = {fact_base},
                            .predicate = UnaryExpressionExp(
                                ColumnValueExp(ColumnName("fact", "dim_id")),
                                UnaryOperation::kIsNotNull),
                            .output_schema = fact_schema});
    } else {
      fact = fact_base;
    }
    dim = memo.EnsureGroup({"dim"});
    memo.AddExpression(dim,
                       LogicalExpression{.operation = LogicalOperator::kScan,
                                         .table = "dim",
                                         .output_schema = dim_schema});
    if (other_side_filter) {
      // The other side carries a single-relation WHERE conjunct, i.e. the
      // state after push_selection_through_join merged it into this group's
      // scan filter. It can remove exactly the row the FOREIGN KEY
      // certificate guarantees, so the scan-free FK fallback must not fire.
      memo.MergeScanFilter(dim,
                           BinaryExpressionExp(
                               ColumnValueExp(ColumnName("dim", "val")),
                               BinaryOperation::kGreaterThan,
                               ConstantValueExp(Value(int64_t{0}))));
    }
    const GroupId join = memo.EnsureDerivedGroup({"fact", "dim"}, "inner_join");
    const Expression equality = BinaryExpressionExp(
        ColumnValueExp(ColumnName("fact", "dim_id")), BinaryOperation::kEquals,
        ColumnValueExp(ColumnName("dim", "id")));
    memo.AddExpression(
        join,
        LogicalExpression{
            .operation = LogicalOperator::kJoin,
            .children = {fact, dim},
            .predicate =
                residual
                    ? BinaryExpressionExp(
                          equality, BinaryOperation::kAnd,
                          BinaryExpressionExp(
                              ColumnValueExp(ColumnName("fact", "payload")),
                              BinaryOperation::kGreaterThan,
                              ConstantValueExp(Value(int64_t{0}))))
                    : equality});
    topn = memo.EnsureDerivedGroup({"fact", "dim"}, "topn");
    memo.AddExpression(
        topn,
        LogicalExpression{.operation = LogicalOperator::kTopN,
                          .children = {join},
                          .target_list = {NamedExpression(
                              "", ColumnValueExp(ColumnName("fact", "id")))},
                          .sort_ascending = {true},
                          .sort_nulls_first = {std::nullopt},
                          .limit_count = 5,
                          .limit_offset = 0});
  }

  [[nodiscard]] bool HasPushedTopN(const Memo& searched) const {
    for (size_t i = 0; i < searched.GroupCount(); ++i) {
      const Group& group = searched.Get(i);
      if (!group.tag.starts_with("topn_proven_one_to_one:5:fact:")) {
        continue;
      }
      for (const auto& expr : group.expressions) {
        if (expr.operation == LogicalOperator::kTopN && expr.limit_count == 5 &&
            expr.limit_offset == 0 && expr.children.size() == 1 &&
            expr.children[0] == fact) {
          return true;
        }
      }
    }
    return false;
  }
};

}  // namespace tinylamb::cascades
