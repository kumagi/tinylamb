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

#ifndef TINYLAMB_QUERY_DATA_HPP
#define TINYLAMB_QUERY_DATA_HPP

#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "query/statement.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class TransactionContext;

// An eagerly-materialized CTE (M4): the defining query was planned and
// executed once during PrepareStatement (same transaction snapshot as the
// outer query), and every reference site scans these rows. Column types use
// first-non-null inference with a Null fallback: SQL columns are homogeneous
// in practice, and untyped columns only disable type-specialized rewrites
// (they never change row semantics, which evaluate dynamically per Value).
struct MaterializedCte {
  // Alias-qualified output schema (one entry per output column, in row
  // order) shared by every reference site.
  Schema schema;
  std::vector<Row> rows;
};

// An opaque recursive-CTE leaf (M4): the fixpoint still executes through
// the proven worktable driver, but the outer query plans joins, filters,
// and ordering around it through the memo. Types are Null (conservative:
// typed rewrites skip them, row semantics stay dynamic).
struct RecursiveCteRef {
  std::shared_ptr<const SelectStatement> body;
  // Site-alias-qualified output schema, one entry per body output column.
  Schema output_schema;
  std::optional<RecursiveDepthSpec> depth_spec;
};

// Lifted CTE layer (M4): every mapped CTE below is either materialized once
// (shared eager cells) or referenced as an opaque recursive leaf, with all
// reference sites covered. `fully_covered` is computed once by the lifter;
// routing treats a covered map as vestigial (relational fallbacks still
// execute through it, so it is never erased).
struct LiftedCtes {
  std::unordered_map<std::string, std::shared_ptr<MaterializedCte>> cells;
  std::unordered_map<std::string, RecursiveCteRef> recursive;
  // Mapped CTE names fully lifted (cells cover all sites, or an opaque
  // recursive entry exists). Computed once by the lifter.
  std::unordered_set<std::string> covered;
  // Every mapped CTE covered: routing treats the remaining map as vestigial.
  bool fully_covered = false;
};

struct QueryData {
 public:
  QueryData() = default;
  QueryData(std::vector<std::string> from, Expression where,
            std::vector<NamedExpression> select)
      : from_(std::move(from)),
        where_(std::move(where)),
        select_(std::move(select)) {}

  friend std::ostream& operator<<(std::ostream& o, const QueryData& q) {
    o << "SELECT\n  ";
    for (size_t i = 0; i < q.select_.size(); ++i) {
      if (0 < i) {
        o << ", ";
      }
      o << q.select_[i];
    }
    o << "\nFROM\n  ";
    for (size_t i = 0; i < q.from_.size(); ++i) {
      if (0 < i) {
        o << ", ";
      }
      o << q.from_[i];
    }
    o << "\nWHERE\n  ";
    // where_ is optional (e.g. QueryData{{"t"}, nullptr, {…}} built by
    // tests/UPDATE paths); logging must not crash on a missing predicate.
    if (q.where_) {
      o << *q.where_;
    } else {
      o << "(none)";
    }
    for (const OuterJoinEdge& edge : q.outer_joins_) {
      o << "\nOUTER JOIN " << q.from_[edge.right_index] << " ON ";
      if (edge.on_condition) {
        o << *edge.on_condition;
      } else {
        o << "(none)";
      }
    }
    for (const auto& [alias, cte] : q.lifted_ctes_.cells) {
      o << "\nMATERIALIZED " << alias
        << " rows=" << (cte ? cte->rows.size() : 0);
    }
    for (const auto& [alias, ref] : q.lifted_ctes_.recursive) {
      (void)ref;
      o << "\nRECURSIVE " << alias;
    }
    o << ";";
    return o;
  }
  Status Rewrite(TransactionContext& ctx);
  // Records `status` in the return slot; kAmbiguousQuery also captures the
  // colliding column name from `ambiguous_names` (single-entry sets only)
  // into ambiguous_column_ so error messages can name it.
  Status RecordAmbiguousColumn(
      Status status, const std::unordered_set<std::string>& ambiguous_names);
  std::vector<std::string> from_;
  Expression where_;
  std::vector<NamedExpression> select_;
  std::unordered_map<std::string, std::string> aliases_;
  // QUALIFY clause (window-filter). Like WHERE it filters rows, but it
  // evaluates after window functions; the optimizer lowers it to a Selection
  // above the kWindow nodes. Empty for statements without QUALIFY.
  Expression qualify_;
  // Outer-join edges in FROM order (M6+1). Entry i joins the accumulated
  // left-deep input with from_[right_index] using on_condition as the ON
  // predicate; join_kind mirrors LogicalExpression::join_type (0 = LEFT,
  // 1 = RIGHT, 2 = FULL). Empty for inner/cross-only queries, whose ON
  // conditions are folded into where_ instead. ON must never be folded into
  // where_: for an outer join ON filters before NULL padding while WHERE
  // filters after it.
  struct OuterJoinEdge {
    size_t right_index{0};
    uint8_t join_kind{0};
    Expression on_condition{};
  };
  std::vector<OuterJoinEdge> outer_joins_;
  // Lifted CTE layer (M4): shared eager cells and opaque recursive leaves
  // keyed by FROM-alias. Empty for statements without lifted CTEs. Plans
  // embedding eager cells must never enter the plan cache (rows are data);
  // the lifter disarms the fill site when it builds any.
  LiftedCtes lifted_ctes_;
  // UPDATE/DELETE executors need the physical row position. An index-only
  // scan deliberately has no table row position and must not be selected.
  bool require_row_position_{false};
  // UPDATE waits briefly for a conflicting writer; DELETE defaults to
  // NOWAIT so queue consumers can retry rather than convoy behind one row.
  bool wait_for_write_intent_{true};
  std::vector<Expression> order_expressions_;
  std::vector<bool> order_ascending_;
  // Explicit NULLS FIRST/LAST per ORDER BY term. An absent entry preserves
  // the SQL default (NULLS FIRST for ASC, NULLS LAST for DESC).
  std::vector<std::optional<bool>> order_nulls_first_;
  bool distinct_{false};
  // LIMIT/OFFSET made visible to the optimizer (Phase 5 Top-K). The plan may
  // fold them into a LimitPlan; the engine skips its own wrapper when the
  // plan reports EnforcesLimit (D6).
  size_t limit_count_{0};
  size_t limit_offset_{0};
  // Bare name of the ambiguous column that made Rewrite fail with
  // kAmbiguousQuery (empty when several names collide or nothing matched).
  std::string ambiguous_column_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_DATA_HPP
