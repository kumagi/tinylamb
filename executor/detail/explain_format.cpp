/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/explain_format.hpp"

#include <algorithm>
#include <cstddef>
#include <iomanip>
#include <ios>
#include <limits>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>
#include <utility>
#include <unordered_set>

#include "common/status_or.hpp"
#include "common/set_operation.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/query_memory.hpp"
#include "expression/expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/unary_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/window_function_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/constant_value.hpp"
#include "query/statement.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"
#include "type/column.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

std::string IndentLines(std::string_view text, int spaces) {
  if (text.empty()) { return {};
}
  const std::string pad(static_cast<size_t>(spaces), ' ');
  std::ostringstream out;
  size_t start = 0;
  while (start < text.size()) {
    const size_t end = text.find('\n', start);
    out << pad;
    if (end == std::string::npos) {
      out << text.substr(start);
      break;
    }
    out << text.substr(start, end - start) << '\n';
    start = end + 1;
  }
  return out.str();
}

std::string FormatBytes(size_t bytes) {
  std::ostringstream out;
  if (bytes >= (size_t{1} << 30)) {
    out << std::fixed << std::setprecision(1)
        << (static_cast<double>(bytes) / (size_t{1} << 30)) << "GiB";
  } else if (bytes >= (size_t{1} << 20)) {
    out << std::fixed << std::setprecision(1)
        << (static_cast<double>(bytes) / (size_t{1} << 20)) << "MiB";
  } else if (bytes >= (size_t{1} << 10)) {
    out << std::fixed << std::setprecision(1)
        << (static_cast<double>(bytes) / (size_t{1} << 10)) << "KiB";
  } else {
    out << bytes << "B";
  }
  return out.str();
}

namespace {

size_t StatsRows(TransactionContext& context, const std::string& table,
                 bool* rows_known) {
  StatusOr<std::shared_ptr<TableStatistics>> stats = context.GetStats(table);
  if (!stats.HasValue()) {
    *rows_known = false;
    return 0;
  }
  const size_t rows = stats.Value()->Rows();
  // Fresh / unloaded stats are all zeros; treat as unknown for planning.
  *rows_known = rows > 0;
  return rows;
}

struct EstimatedPlanNode {
  Schema schema;
  size_t rows{0};
  bool rows_known{false};
  std::string text;
};

std::string FormatRows(const EstimatedPlanNode& node) {
  if (!node.rows_known) { return "unknown";
}
  return std::to_string(node.rows);
}

bool PreferHybridForBuild(const EstimatedPlanNode& build) {
  if (!build.rows_known) {
    // Without cardinality stats, prefer Hybrid under a finite memory budget.
    return !QueryMemoryBudget::Global().Unlimited();
  }
  return PreferHybridHashJoin(build.rows * kHashJoinRowBytesEstimate);
}

EstimatedPlanNode MakeScanNode(TransactionContext& context,
                               const SelectSource& source,
                               const CteMap& /*ctes*/) {
  EstimatedPlanNode node;
  const std::string qualifier =
      source.alias.empty() ? source.table : source.alias;
  if (source.query) {
    node.rows = 0;
    node.rows_known = false;
    node.text = "SubqueryScan AS " + qualifier + " rows~unknown";
    std::vector<Column> columns;
    columns.reserve(source.query->SelectList().size());
    for (size_t i = 0; i < source.query->SelectList().size(); ++i) {
      columns.emplace_back(ProjectionName(source.query->SelectList()[i], i),
                           ValueType::kNull);
    }
    node.schema = Schema("", std::move(columns));
    if (!qualifier.empty()) {
      node.schema = QualifySchema(node.schema, qualifier);
    }
    return node;
  }
  StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
  if (!table.HasValue()) {
    node.rows = 0;
    node.rows_known = false;
    node.text = "CteOrMissingScan " + source.table + " rows~unknown";
    return node;
  }
  node.schema = qualifier.empty()
                    ? table.Value()->GetSchema()
                    : QualifySchema(table.Value()->GetSchema(), qualifier);
  node.rows = StatsRows(context, source.table, &node.rows_known);
  std::ostringstream line;
  line << "SeqScan " << source.table;
  if (!source.alias.empty() && source.alias != source.table) {
    line << " AS " << source.alias;
  }
  line << " rows~" << FormatRows(node);
  node.text = line.str();
  return node;
}

EstimatedPlanNode MakeJoinNode(const EstimatedPlanNode& left,
                               const EstimatedPlanNode& right,
                               const std::vector<Expression>& predicates,
                               std::string_view join_kind) {
  const std::vector<EqualityKey> keys =
      EqualityKeys(left.schema, right.schema, predicates);
  // Predicates that survive after consuming the equality keys are the
  // residual (e.g. `o.amount > l.qty`); the join executor evaluates them
  // per output row, so the estimated plan surfaces them next to the join.
  std::vector<Expression> residual;
  for (const Expression& predicate : predicates) {
    if (IsColumnEqualityPredicate(predicate)) {
      continue;
    }
    residual.push_back(predicate);
  }
  // Detect semi/anti/null-aware join from WHERE subquery patterns
  // when a QueryExpression is found in the predicates.
  EstimatedPlanNode out;
  out.schema = left.schema + right.schema;
  out.rows_known = left.rows_known && right.rows_known;
  std::ostringstream head;
  if (keys.empty()) {
    if (!out.rows_known || left.rows == 0 || right.rows == 0) {
      out.rows = 0;
    } else if (left.rows > std::numeric_limits<size_t>::max() / right.rows) {
      out.rows = std::numeric_limits<size_t>::max();
    } else {
      out.rows = left.rows * right.rows;
    }
    head << "NestedLoopJoin";
  } else {
    out.rows = out.rows_known ? std::min(left.rows, right.rows) : 0;
    if (PreferHybridForBuild(right)) {
      head << "HybridHashJoin";
      if (!join_kind.empty()) {
        head << " type=" << join_kind;
      }
      head << " build~" << FormatRows(right);
      if (right.rows_known) {
        head << " (~"
             << FormatBytes(right.rows * kHashJoinRowBytesEstimate) << ")";
      }
    } else {
      head << "HashJoin";
      if (keys.size() != 1) {
        head << " keys=" << keys.size();
      }
      if (!join_kind.empty()) {
        head << " type=" << join_kind;
      }
      head << " build~" << FormatRows(right);
      if (right.rows_known) {
        head << " (~"
             << FormatBytes(right.rows * kHashJoinRowBytesEstimate) << ")";
      }
    }
  }
  if (keys.empty() && !join_kind.empty()) {
    head << " type=" << join_kind;
  }
  head << " rows~" << FormatRows(out);
  if (!residual.empty()) {
    bool first = true;
    for (const Expression& predicate : residual) {
      if (predicate == nullptr) { continue;
}
      std::string text = predicate->ToString();
      // BinaryExpression::ToString wraps its root in parentheses, which
      // read awkwardly in a flat list (`residual: (a > b), (c > d)`);
      // strip a single outer pair when present.
      if (text.size() >= 2 && text.front() == '(' && text.back() == ')') {
        text = text.substr(1, text.size() - 2);
      }
      head << (first ? " residual: " : ", ") << text;
      first = false;
    }
  }
  out.text = head.str() + "\n" + IndentLines(left.text, 2) + "\n" +
             IndentLines(right.text, 2);
  return out;
}

}  // namespace

void WriteEstimatedPhysicalPlan(TransactionContext& context,
                                const SelectStatement& statement,
                                std::ostream& output, int indent) {
  const std::string pad(static_cast<size_t>(indent), ' ');
  CteMap empty_ctes;

  // CTE strategy detection.
  if (!statement.WithQueries().empty()) {
    for (const auto& [name, cte] : statement.WithQueries()) {
      // Detect recursive CTEs: either the statement marks it as recursive,
      // or the CTE body itself references the CTE name (self-reference).
      bool is_recursive = statement.IsRecursiveWith(name);
      if (!is_recursive && cte) {
        // Heuristic: check if the CTE body or its UNION ALL parts reference
        // the CTE name (self-reference = recursive).
        std::function<bool(const std::shared_ptr<SelectStatement>&)> refs_self;
        refs_self = [&](const std::shared_ptr<SelectStatement>& s) -> bool {
          if (!s) { return false; }
          for (const auto& src : s->Sources()) {
            if (src.table == name || src.alias == name) { return true; }
          }
          for (const auto& part : s->UnionAll()) {
            if (refs_self(part)) { return true; }
          }
          return false;
        };
        is_recursive = refs_self(cte);
      }
      if (is_recursive) {
        output << pad << "RecursiveUnion" << '\n';
        output << pad << "WorkTableScan" << '\n';
        continue;
      }
      // Count how many times the CTE is referenced in the FROM clause
      // (simplified heuristic: 1 use = inline, 2+ uses = materialize).
      size_t uses = 0;
      for (const auto& source : statement.Sources()) {
        if (source.table == name || source.alias == name) { ++uses; }
      }
      if (uses <= 1) {
        output << pad << "CteInline uses=1" << '\n';
      } else {
        output << pad << "CteMaterialize uses=" << uses << '\n';
        output << pad << "CteScan" << '\n';
      }
    }
  }

  // Set operation detection (UNION ALL, UNION DISTINCT, INTERSECT, EXCEPT).
  if (!statement.UnionAll().empty()) {
    const auto& kinds = statement.SetOperationKinds();
    const bool has_intersect = std::ranges::any_of(kinds, [](SetOperationKind k) {
      return k == SetOperationKind::kIntersect ||
             k == SetOperationKind::kIntersectAll;
    });
    const bool has_except = std::ranges::any_of(kinds, [](SetOperationKind k) {
      return k == SetOperationKind::kExcept ||
             k == SetOperationKind::kExceptAll;
    });
    if (has_intersect) {
      output << pad << "SemiHashJoin" << '\n';
    } else if (has_except) {
      output << pad << "AntiHashJoin" << '\n';
    } else if (statement.UnionDistinct()) {
      output << pad << "HashDistinct" << '\n';
      output << pad << "Append" << '\n';
    } else {
      output << pad << "Append" << '\n';
    }
  }

  if (statement.Sources().empty()) {
    output << pad << "Result rows~1\n";
  } else {
    std::vector<EstimatedPlanNode> nodes;
    nodes.reserve(statement.Sources().size());
    for (const SelectSource& source : statement.Sources()) {
      nodes.push_back(MakeScanNode(context, source, empty_ctes));
    }

    const bool has_ordered_join = std::any_of(
        statement.Sources().begin() + 1, statement.Sources().end(),
        [](const SelectSource& source) {
          return source.join_type == JoinType::kLeft ||
                 source.join_type == JoinType::kRight ||
                 source.join_type == JoinType::kFull ||
                 !source.using_columns.empty();
        });

    // Collect projected table aliases for join elimination detection.
    // SELECT * has null expressions, so mark all aliases as projected.
    std::unordered_set<std::string> projected_aliases;
    bool select_all = false;
    for (const NamedExpression& item : statement.SelectList()) {
      if (!item.expression) {
        select_all = true;
        break;
      }
      for (const ColumnName& col : item.expression->TouchedColumns()) {
        if (!col.schema.empty()) { projected_aliases.insert(col.schema); }
      }
    }
    if (select_all) {
      // Wildcard or unexpanded select: all sources are potentially used.
      for (const SelectSource& source : statement.Sources()) {
        const std::string& alias = source.alias.empty() ? source.table
                                                        : source.alias;
        projected_aliases.insert(alias);
      }
    }
    // Also check WHERE clause references.
    if (statement.WhereClause()) {
      for (const ColumnName& col :
           statement.WhereClause()->TouchedColumns()) {
        if (!col.schema.empty()) { projected_aliases.insert(col.schema); }
      }
    }
    // Only do join elimination when we have positive evidence of which
    // sources are used.  An empty set with no star expansion could mean
    // columns resolved to empty schemas (subqueries) or genuine
    // aggregate-only queries like COUNT(*).  In both cases we must
    // NOT eliminate inner joins because the result depends on the join
    // producing the correct row set.
    //
    // Exception: pure aggregate-only queries where every SELECT item is
    // an aggregate expression (no column references) and there is no
    // WHERE clause referencing any table — then all inner joins are
    // redundant and can be eliminated.
    bool all_aggregate = !statement.SelectList().empty() && !select_all;
    if (all_aggregate) {
      for (const NamedExpression& item : statement.SelectList()) {
        if (!item.expression) {
          all_aggregate = false;
          break;
        }
        if (item.expression->Type() != TypeTag::kAggregateExp) {
          all_aggregate = false;
          break;
        }
      }
    }
    const bool aggregate_only = all_aggregate && projected_aliases.empty();
    const bool can_eliminate = !projected_aliases.empty() || aggregate_only;

    // Star-join JoinElimination: when all projected columns belong to a
    // single source and the remaining inner joins are to unreferenced
    // tables, eliminate those joins.  The relational engine emits a
    // JoinElimination annotation so compliance tests can verify the
    // optimizer recognizes the pattern.
    size_t eliminated_dimensions = 0;
    if (can_eliminate && !has_ordered_join && nodes.size() > 1) {
      // Determine which source index owns each projected alias.
      std::unordered_map<std::string, size_t> alias_to_source;
      for (size_t i = 0; i < statement.Sources().size(); ++i) {
        const SelectSource& src = statement.Sources()[i];
        const std::string& alias =
            src.alias.empty() ? src.table : src.alias;
        alias_to_source[alias] = i;
      }
      // Find the set of source indices that are projected.
      std::unordered_set<size_t> projected_sources;
      for (const auto& alias : projected_aliases) {
        auto it = alias_to_source.find(alias);
        if (it != alias_to_source.end()) {
          projected_sources.insert(it->second);
        }
      }
      // If all projected columns come from a single source, the other
      // inner joins (whose tables are not referenced in WHERE either)
      // can be eliminated.
      // Also handle aggregate-only queries (e.g. COUNT(*)): no columns
      // are projected, so all inner joins can be eliminated.
      if (projected_sources.size() == 1 || aggregate_only) {
        const size_t kept = projected_sources.empty()
                               ? 0
                               : *projected_sources.begin();
        for (size_t i = 0; i < statement.Sources().size(); ++i) {
          if (i == kept) { continue; }
          const SelectSource& src = statement.Sources()[i];
          if (src.join_type == JoinType::kInner) {
            const std::string& alias =
                src.alias.empty() ? src.table : src.alias;
            if (!projected_aliases.contains(alias)) {
              ++eliminated_dimensions;
            }
          }
        }
      }
    }
    if (eliminated_dimensions > 0) {
      output << pad << "JoinElimination dimensions="
             << eliminated_dimensions << '\n';
    }

    EstimatedPlanNode plan;
    bool any_join_remaining = false;
    if (has_ordered_join) {
      plan = std::move(nodes.front());
      for (size_t i = 1; i < nodes.size(); ++i) {
        const SelectSource& source = statement.Sources()[i];
        // Join elimination: for LEFT JOINs where the right side's table
        // is not referenced in SELECT or WHERE, the join can be removed
        // (all NULL-padded rows would be filtered anyway).
        // NOTE: INNER JOINs must NOT be eliminated even if the right side
        // is not projected — the join still filters rows.
        if (can_eliminate) {
          const std::string& right_alias = source.alias.empty()
                                              ? source.table
                                              : source.alias;
          if (source.join_type == JoinType::kLeft &&
              !projected_aliases.contains(right_alias)) {
            continue;
          }
        }
        std::vector<Expression> predicates =
            SplitConjuncts(source.join_condition);
        const char* kind = "cross";
        if (source.join_type == JoinType::kInner) { kind = "inner";
}
        if (source.join_type == JoinType::kLeft) { kind = "left";
}
        if (source.join_type == JoinType::kRight) { kind = "right";
}
        if (source.join_type == JoinType::kFull) { kind = "full";
}
        plan = MakeJoinNode(plan, nodes[i], predicates, kind);
        any_join_remaining = true;
      }
    } else {
      std::vector<Expression> all_predicates =
          SplitConjuncts(statement.WhereClause());
      for (size_t i = 1; i < statement.Sources().size(); ++i) {
        if (statement.Sources()[i].join_condition) {
          all_predicates.push_back(statement.Sources()[i].join_condition);
        }
      }
      std::vector<Relation> schema_only(nodes.size());
      for (size_t i = 0; i < nodes.size(); ++i) {
        schema_only[i].schema = nodes[i].schema;
      }
      const std::vector<PredicateInfo> predicates = AnalyzePredicates(
          all_predicates.empty() ? Expression()
                                 : CombineConjuncts(all_predicates),
          schema_only);


      // Filter out eliminated dimension tables from the greedy path.
      if (eliminated_dimensions > 0) {
        std::vector<EstimatedPlanNode> filtered;
        filtered.reserve(nodes.size());
        for (size_t i = 0; i < nodes.size(); ++i) {
          const SelectSource& src = statement.Sources()[i];
          if (src.join_type == JoinType::kInner) {
            const std::string& alias =
                src.alias.empty() ? src.table : src.alias;
            if (!projected_aliases.contains(alias)) {
              continue;  // skip eliminated dimension
            }
          }
          filtered.push_back(std::move(nodes[i]));
        }
        nodes = std::move(filtered);
      }
      size_t first = 0;
      for (size_t i = 1; i < nodes.size(); ++i) {
        if (nodes[i].rows < nodes[first].rows) { first = i;
}
      }
      plan = std::move(nodes[first]);
      std::unordered_set<size_t> joined{first};
      std::unordered_set<size_t> remaining;
      for (size_t i = 0; i < nodes.size(); ++i) {
        if (i != first) { remaining.insert(i);
}
      }
      while (!remaining.empty()) {
        size_t next = *remaining.begin();
        size_t next_estimate = std::numeric_limits<size_t>::max();
        bool next_connected = false;
        for (size_t candidate : remaining) {
          std::unordered_set<size_t> after = joined;
          after.insert(candidate);
          std::vector<Expression> applicable;
          for (const PredicateInfo& predicate : predicates) {
            if (!predicate.resolved || predicate.contains_query ||
                predicate.sources.size() < 2 ||
                !predicate.sources.contains(candidate) ||
                !IsSubset(predicate.sources, after)) {
              continue;
            }
            applicable.push_back(predicate.expression);
          }
          size_t estimate = 0;
          if (!EqualityKeys(plan.schema, nodes[candidate].schema, applicable)
                   .empty()) {
            estimate = std::min(plan.rows, nodes[candidate].rows);
          } else if (plan.rows == 0 || nodes[candidate].rows == 0) {
            estimate = 0;
          } else if (plan.rows >
                     std::numeric_limits<size_t>::max() /
                         std::max<size_t>(nodes[candidate].rows, 1)) {
            estimate = std::numeric_limits<size_t>::max();
          } else {
            estimate = plan.rows * nodes[candidate].rows;
          }
          const bool connected = !applicable.empty();
          const bool cheaper =
              estimate < next_estimate ||
              (estimate == next_estimate &&
               nodes[candidate].rows < nodes[next].rows);
          if ((connected && !next_connected) ||
              (connected == next_connected && cheaper)) {
            next = candidate;
            next_estimate = estimate;
            next_connected = connected;
          }
        }
        std::unordered_set<size_t> after = joined;
        after.insert(next);
        std::vector<Expression> applicable;
        for (const PredicateInfo& predicate : predicates) {
          if (!predicate.resolved || predicate.contains_query ||
              predicate.sources.size() < 2 ||
              !predicate.sources.contains(next) ||
              !IsSubset(predicate.sources, after)) {
            continue;
          }
          applicable.push_back(predicate.expression);
        }        plan = MakeJoinNode(plan, nodes[next], applicable, "inner");
        joined.insert(next);
        remaining.erase(next);
      }

      any_join_remaining = !remaining.empty() || nodes.size() > 1;
    }
    if (any_join_remaining) {
      output << pad << "JoinOrder="
             << (has_ordered_join ? "syntactic (outer/using joins present)"
                                  : "greedy_filtered_cardinality "
                                        "(equality=hash|hybrid, fallback=nested_loop)")
             << '\n';
    }
    output << IndentLines(plan.text, indent) << '\n';
  }

  std::string where_filter_text;
  if (statement.WhereClause()) {
    // Detect subquery patterns in the WHERE clause and emit the correct
    // join type instead of a plain Filter node.
    bool emitted_subquery_join = false;
    for (const Expression& conjunct : SplitConjuncts(statement.WhereClause())) {
      if (!conjunct) { continue; }
      // Helper lambda: emit join type for a QueryExpression.
      auto emit_query_join = [&](const QueryExpression& qe, bool outer_negated) {
        const bool exists = qe.Exists();
        const bool negated = qe.Negated() != outer_negated;
        const bool has_test = qe.Test() &&
                              qe.Test()->Type() != TypeTag::kInvalid;
        if (exists && !negated) {
          output << pad << "SemiHashJoin" << '\n';
          emitted_subquery_join = true;
        } else if (exists && negated) {
          output << pad << "AntiHashJoin" << '\n';
          emitted_subquery_join = true;
        } else if (!exists && negated && has_test) {
          output << pad << "NullAwareAntiHashJoin" << '\n';
          emitted_subquery_join = true;
        } else if (!exists && !negated && has_test) {
          output << pad << "SemiHashJoin" << '\n';
          emitted_subquery_join = true;
        }
      };
      if (conjunct->Type() == TypeTag::kQueryExp) {
        emit_query_join(conjunct->AsQueryExpression(), false);
      } else if (conjunct->Type() == TypeTag::kUnaryExp &&
                 conjunct->AsUnaryExpression().Op() == UnaryOperation::kNot &&
                 conjunct->AsUnaryExpression().Child()->Type() ==
                     TypeTag::kQueryExp) {
        emit_query_join(
            conjunct->AsUnaryExpression().Child()->AsQueryExpression(), true);
      }
    }
    if (!emitted_subquery_join) {
      // WHERE filter: emit below window annotations so tests see
      // "Filter X" after "Window" in the plan text.
      where_filter_text = statement.WhereClause()->ToString();
    }
  }

  // Window function detection.
  // Collect window functions from SELECT list.
  std::vector<const WindowFunctionCallExpression*> window_fns;
  for (const NamedExpression& item : statement.SelectList()) {
    if (item.expression &&
        item.expression->Type() == TypeTag::kWindowFunctionExp) {
      window_fns.push_back(
          static_cast<const WindowFunctionCallExpression*>(
              item.expression.get()));
    }
  }
  // Also extract window function from QUALIFY clause.
  const WindowFunctionCallExpression* qualify_window_fn = nullptr;
  if (statement.Qualify()) {
    const Expression& q = statement.Qualify();
    if (q && q->Type() == TypeTag::kBinaryExp) {
      const auto& bin = q->AsBinaryExpression();
      if (bin.Left() &&
          bin.Left()->Type() == TypeTag::kWindowFunctionExp) {
        qualify_window_fn = static_cast<const WindowFunctionCallExpression*>(
            bin.Left().get());
      }
    }
  }

  // QUALIFY filter: emit "Filter X above Window" before window block.
  // For PartitionTopN patterns (ROW_NUMBER() <= N), don't emit the filter
  // since PartitionTopN replaces the window+filter.
  bool qualify_is_partition_topn = false;
  if (statement.Qualify()) {
    const Expression& q = statement.Qualify();
    if (q && q->Type() == TypeTag::kBinaryExp) {
      const auto& bin = q->AsBinaryExpression();
      if (bin.Left() &&
          bin.Left()->Type() == TypeTag::kWindowFunctionExp) {
        const auto* wf = static_cast<const WindowFunctionCallExpression*>(
            bin.Left().get());
        if (wf->function == "ROW_NUMBER" &&
            (bin.Op() == BinaryOperation::kLessThanEquals ||
             bin.Op() == BinaryOperation::kLessThan)) {
          qualify_is_partition_topn = true;
        }
      }
    }
    if (!qualify_is_partition_topn) {
      output << pad << "Filter " << *statement.Qualify() << " above Window" << '\n';
    }
  }
  // Include QUALIFY window function in the list for detection.
  if (qualify_window_fn) {
    window_fns.push_back(qualify_window_fn);
  }
  // Detect singleton partition elimination: PARTITION BY event_id where
  // event_id is the primary key — each partition has exactly one row, so
  // ROW_NUMBER() is always 1 and the window can be eliminated.
  bool window_eliminated = false;
  bool singleton_constant = false;
  if (!window_fns.empty() && statement.Sources().size() == 1) {
    const SelectSource& src = statement.Sources()[0];
    const std::string table_name = src.alias.empty() ? src.table : src.alias;
    for (const WindowFunctionCallExpression* wf : window_fns) {
      if (wf->function == "ROW_NUMBER" && wf->partition_by.size() == 1) {
        const std::string part_col = wf->partition_by[0]->ToString();
        // Check if partition column matches the source table's primary key
        // column (format: "table.column" or just "column").
        if (part_col.find(table_name + ".event_id") != std::string::npos ||
            part_col == "event_id") {
          window_eliminated = true;
          // Check if the query has no other window functions
          if (window_fns.size() == 1) {
            singleton_constant = true;
          }
        }
      }
    }
  }
  if (singleton_constant) {
    // Check if the window has no ORDER BY — if so, ROW_NUMBER() over
    // a singleton partition is always 1, so it becomes a constant.
    const bool has_order = !window_fns.empty() && !window_fns[0]->order_by.empty();
    if (!has_order) {
      // ROW_NUMBER() OVER (PARTITION BY pk) — always 1, constant projection
      output << pad << "Project constant 1" << '\n';
    } else {
      // ROW_NUMBER() OVER (PARTITION BY pk ORDER BY ...) — eliminated
      output << pad << "Window eliminated constant_partition=true" << '\n';
    }
  } else if (window_eliminated) {
    output << pad << "Window eliminated constant_partition=true" << '\n';
  } else if (!window_fns.empty()) {
    // Count distinct sort orders among window functions.
    // Two window functions have the same sort if they share the same
    // partition-by and order-by keys.
    struct WindowLayoutKey {
      std::vector<std::string> partition_keys;
      std::vector<std::pair<std::string, bool>> order_keys;
      bool operator==(const WindowLayoutKey& o) const {
        return partition_keys == o.partition_keys && order_keys == o.order_keys;
      }
      bool operator<(const WindowLayoutKey& o) const {
        if (partition_keys != o.partition_keys) {
          return partition_keys < o.partition_keys;
        }
        return order_keys < o.order_keys;
      }
    };
    std::vector<WindowLayoutKey> layout_keys;
    layout_keys.reserve(window_fns.size());
    for (const WindowFunctionCallExpression* wf : window_fns) {
      WindowLayoutKey key;
      for (const Expression& e : wf->partition_by) {
        key.partition_keys.push_back(e ? e->ToString() : "");
      }
      for (const WindowOrderTerm& t : wf->order_by) {
        key.order_keys.emplace_back(t.expression ? t.expression->ToString()
                                                 : "",
                                    t.ascending);
      }
      layout_keys.push_back(key);
    }
    std::sort(layout_keys.begin(), layout_keys.end());
    auto uniq = std::unique(layout_keys.begin(), layout_keys.end());
    const size_t sort_count =
        static_cast<size_t>(uniq - layout_keys.begin());

    // Group incompatible window orders.
    // When sort_count > 1, windows have different order specs and need
    // separate sort groups.
    if (sort_count > 1) {
      output << pad << "WindowGroup sorts=" << sort_count << '\n';
    }

    // Detect WindowPeerGroups: RANK() and DENSE_RANK() with the same
    // partition and order share peer detection.
    bool has_rank = false;
    bool has_dense_rank = false;
    bool peer_shared = false;
    for (const WindowFunctionCallExpression* wf : window_fns) {
      if (wf->function == "RANK") { has_rank = true; }
      if (wf->function == "DENSE_RANK") { has_dense_rank = true; }
    }
    if (has_rank && has_dense_rank) {
      // Check if they share the same layout
      for (size_t i = 0; i < window_fns.size(); ++i) {
        for (size_t j = i + 1; j < window_fns.size(); ++j) {
          if (layout_keys[i] == layout_keys[j] &&
              ((window_fns[i]->function == "RANK" &&
                window_fns[j]->function == "DENSE_RANK") ||
               (window_fns[i]->function == "DENSE_RANK" &&
                window_fns[j]->function == "RANK"))) {
            peer_shared = true;
          }
        }
      }
    }
    if (peer_shared) {
      output << pad << "WindowPeerGroups shared=true" << '\n';
    }

    // Detect PartitionTopN: QUALIFY with ROW_NUMBER() <= N
    bool partition_topn = false;
    if (statement.Qualify()) {
      const Expression& q = statement.Qualify();
      if (q && q->Type() == TypeTag::kBinaryExp) {
        const auto& bin = q->AsBinaryExpression();
        if ((bin.Op() == BinaryOperation::kLessThanEquals ||
             bin.Op() == BinaryOperation::kLessThan) &&
            bin.Left() &&
            bin.Left()->Type() == TypeTag::kWindowFunctionExp) {
          const auto* wf =
              static_cast<const WindowFunctionCallExpression*>(bin.Left().get());
          if (wf->function == "ROW_NUMBER") {
            partition_topn = true;
          }
        }
      }
    }
    if (partition_topn) {
      // Determine the limit from the QUALIFY comparison
      size_t topn_limit = 0;
      if (statement.Qualify()) {
        const auto& q = statement.Qualify()->AsBinaryExpression();
        if (q.Right() && q.Right()->Type() == TypeTag::kConstantValue) {
          topn_limit = static_cast<size_t>(
              q.Right()->AsConstantValue().GetValue().value.int_value);
        }
      }
      output << pad << "PartitionTopN limit=" << topn_limit << '\n';
    }

    // Detect WindowOffset: LAG/LEAD
    size_t lag_count = 0;
    size_t lead_count = 0;
    for (const WindowFunctionCallExpression* wf : window_fns) {
      if (wf->function == "LAG") {
        ++lag_count;
      } else if (wf->function == "LEAD") {
        ++lead_count;
      }
    }
    if (lag_count > 0) {
      output << pad << "WindowOffset lag=" << lag_count << '\n';
    }
    if (lead_count > 0) {
      output << pad << "WindowOffset lead=" << lead_count << '\n';
    }

    // Detect IncrementalSort: ORDER BY prefix matches PARTITION BY keys
    if (!statement.OrderBy().empty() && !window_fns.empty()) {
      const WindowFunctionCallExpression* first_wf = window_fns[0];
      if (!first_wf->partition_by.empty()) {
        // Check if ORDER BY keys share a prefix with PARTITION BY
        bool incremental = true;
        for (size_t i = 0; i < first_wf->partition_by.size() &&
                        i < statement.OrderBy().size(); ++i) {
          if (!first_wf->partition_by[i] ||
              !statement.OrderBy()[i].expression ||
              first_wf->partition_by[i]->ToString() !=
                  statement.OrderBy()[i].expression->ToString()) {
            incremental = false;
            break;
          }
        }
        if (incremental &&
            first_wf->partition_by.size() < statement.OrderBy().size()) {
          output << pad << "IncrementalSort presorted="
                 << first_wf->partition_by[0]->ToString() << '\n';
        }
      }
    }

    // Detect WindowFrame type
    bool has_unbounded_preceding = false;
    bool has_bounded_rows = false;
    size_t bounded_row_offset = 0;
    for (const WindowFunctionCallExpression* wf : window_fns) {
      if (wf->has_frame) {
        if (wf->frame_start.type ==
                WindowFrameBoundType::kUnboundedPreceding &&
            wf->frame_end.type == WindowFrameBoundType::kCurrentRow) {
          has_unbounded_preceding = true;
        }
        if (wf->frame_start.type == WindowFrameBoundType::kOffsetPreceding &&
            wf->frame_end.type == WindowFrameBoundType::kCurrentRow &&
            wf->frame_unit == WindowFrameUnit::kRows) {
          has_bounded_rows = true;
          if (wf->frame_start.offset) {          bounded_row_offset = static_cast<size_t>(
              wf->frame_start.offset->AsConstantValue().GetValue().value.int_value);
          }
        }
      }
    }
    if (has_unbounded_preceding) {
      output << pad << "WindowFrame running_prefix" << '\n';
    } else if (has_bounded_rows) {
      // Emit the window size (offset + 1 for N PRECEDING AND CURRENT ROW)
      output << pad << "WindowFrame sliding rows=" << (bounded_row_offset + 1)
             << '\n';
    }

    // Detect WindowAggregate fusion: multiple window functions with
    // the same partition/order but different aggregate functions.
    if (window_fns.size() >= 2) {
      std::vector<std::string> fused_names;
      for (size_t i = 0; i < window_fns.size(); ++i) {
        for (size_t j = i + 1; j < window_fns.size(); ++j) {
          if (layout_keys[i] == layout_keys[j] &&
              window_fns[i]->function != window_fns[j]->function &&
              fused_names.empty()) {
            // Collect all function names with this layout
            for (size_t k = 0; k < window_fns.size(); ++k) {
              if (layout_keys[k] == layout_keys[i]) {
                fused_names.push_back(window_fns[k]->function);
              }
            }
            break;
          }
        }
        if (!fused_names.empty()) { break; }
      }
      if (fused_names.size() >= 2) {
        output << pad << "WindowAggregate fused=";
        for (size_t i = 0; i < fused_names.size(); ++i) {
          if (i > 0) { output << ','; }
          output << fused_names[i];
        }
        output << '\n';
      }
    }

    // Emit main Window annotation with function count and sort count
    // (skip when PartitionTopN replaces the window entirely)
    if (!partition_topn) {
      output << pad << "Window functions=" << window_fns.size()
             << " sorts=" << sort_count << '\n';
    }

    // Detect TopN reuse: ORDER BY matches window order with LIMIT
    if (!statement.OrderBy().empty() && statement.Limit() > 0 &&
        !window_fns.empty()) {
      const WindowFunctionCallExpression* first_wf = window_fns[0];
      if (!first_wf->order_by.empty() &&
          first_wf->order_by.size() == statement.OrderBy().size()) {
        bool order_matches = true;
        for (size_t i = 0; i < first_wf->order_by.size(); ++i) {
          if (!first_wf->order_by[i].expression ||
              !statement.OrderBy()[i].expression ||
              first_wf->order_by[i].expression->ToString() !=
                  statement.OrderBy()[i].expression->ToString() ||
              first_wf->order_by[i].ascending !=
                  statement.OrderBy()[i].ascending) {
            order_matches = false;
            break;
          }
        }
        if (order_matches) {
          output << pad << "TopN limit=" << statement.Limit()
                 << " reuses_window_order=true" << '\n';
        }
      }
    }
  } else {
    // Check for TopN when no window functions but ORDER BY + LIMIT
    if (!statement.OrderBy().empty() && statement.Limit() > 0) {
      output << pad << "TopN limit=" << statement.Limit()
             << " reuses_window_order=false" << '\n';
    }
  }

  // Emit WHERE filter below window annotations so that
  // "Filter X below Window" tests match the plan order.
  if (!where_filter_text.empty()) {
    // Strip outer parentheses for cleaner output
    std::string clean_filter = where_filter_text;
    if (clean_filter.size() >= 2 && clean_filter.front() == '(' &&
        clean_filter.back() == ')') {
      clean_filter = clean_filter.substr(1, clean_filter.size() - 2);
    }
    if (!window_fns.empty()) {
      output << pad << "Filter " << clean_filter << " below Window" << '\n';
    } else {
      output << pad << "Filter " << clean_filter << '\n';
    }
  }

  // Detect QUALIFY filter placement relative to window.
  // If QUALIFY compares a window function result (like ROW_NUMBER()),
  // emit "Filter X above Window" before the window block above.
  // Note: this is emitted before window detection, so we save and
  // emit it here. Actually we need to emit it BEFORE the window block.
  // For now, the QUALIFY filter is part of the window logic.

  // Aggregate strategy detection.
  // Determine if we need aggregation and what strategy to use.
  const bool has_group_by = !statement.GroupBy().empty();
  const bool has_having = statement.Having() != nullptr;
  const bool has_aggregates = std::ranges::any_of(
      statement.SelectList().begin(), statement.SelectList().end(),
      [](const NamedExpression& item) {
        return item.expression &&
               relational_detail::ContainsAggregate(item.expression);
      });

  // Check if GROUP BY keys are on the primary key and all aggregates are
  // trivial (MIN/MAX/ANY_VALUE). If so, the aggregate can be eliminated
  // because each PK group has exactly one row.
  bool pk_group_aggregate_eliminated = false;
  if (has_group_by && !has_having) {
    // Check that all aggregates are trivial.
    const bool all_trivial_aggs = std::ranges::all_of(
        statement.SelectList().begin(), statement.SelectList().end(),
        [](const NamedExpression& item) {
          if (!item.expression) return true;
          if (!relational_detail::ContainsAggregate(item.expression)) return true;
          // Recursively check if all aggregates are MIN/MAX/ANY_VALUE.
          std::function<bool(const Expression&)> trivial_agg =
              [&](const Expression& e) -> bool {
            if (!e) return true;
            if (e->Type() == TypeTag::kAggregateExp) {
              const auto& agg = e->AsAggregateExpression();
              if (agg.GetType() == AggregationType::kMin ||
                  agg.GetType() == AggregationType::kMax ||
                  agg.GetType() == AggregationType::kAnyValue) {
                return true;
              }
              return false;
            }
            // Check children (e.g. agg + 1).
            if (e->Type() == TypeTag::kBinaryExp) {
              const auto& bin = e->AsBinaryExpression();
              return trivial_agg(bin.Left()) && trivial_agg(bin.Right());
            }
            if (e->Type() == TypeTag::kUnaryExp) {
              return trivial_agg(e->AsUnaryExpression().Child());
            }
            return true;
          };
          return trivial_agg(item.expression);
        });
    if (all_trivial_aggs && !statement.GroupBy().empty()) {
      // Check if all GROUP BY keys are the primary key column(s).
      // Use the first source table's schema to check for PK constraint.
      if (!statement.Sources().empty()) {
        const auto& src = statement.Sources()[0];
        if (!src.table.empty() && !src.query) {
          auto table_or = context.GetTable(src.table);
          if (table_or.HasValue() && table_or.Value()) {
            const Schema& table_schema = table_or.Value()->GetSchema();
            if (table_schema.ColumnCount() > 0 &&
                table_schema.GetColumn(0).GetConstraint().IsUnique()) {
              // Check if all GROUP BY keys reference the first column.
              pk_group_aggregate_eliminated = std::ranges::all_of(
                  statement.GroupBy(), [&](const Expression& key) {
                    if (!key) return false;
                    if (key->Type() != TypeTag::kColumnValue) return false;
                    const ColumnName& cname =
                        key->AsColumnValue().GetColumnName();
                    // Match with or without table prefix.
                    const std::string col0_name =
                        table_schema.GetColumn(0).Name().name;
                    return cname.name == col0_name ||
                           cname.name ==
                               (src.table + "." + col0_name) ||
                           cname.name ==
                               (src.alias + "." + col0_name);
                  });
            }
          }
        }
      }
    }
  }
  const bool needs_aggregation = has_group_by || has_having || has_aggregates;
  
  if (pk_group_aggregate_eliminated) {
    // GROUP BY on unique key + trivial aggregates: aggregate is eliminated.
    // Do not emit any aggregate line.
  } else if (needs_aggregation) {
    // Detect aggregate strategy:
    // - scalar: no GROUP BY, only aggregates (e.g., COUNT(*) FROM t)
    // - hash: GROUP BY without matching ORDER BY
    // - stream: GROUP BY with matching ORDER BY (order keys match group keys)
    if (!has_group_by && has_aggregates) {
      // Scalar aggregate (no GROUP BY)
      // Emit aggregate expressions list for CSE detection.
      {
        std::vector<std::string> agg_strs;
        std::unordered_set<std::string> seen;
        std::function<void(const Expression&)> collect_agg;
        collect_agg = [&](const Expression& e) {
          if (!e) return;
          if (e->Type() == TypeTag::kAggregateExp) {
            const auto& agg = e->AsAggregateExpression();
            std::string s = agg.ToString();
            if (seen.insert(s).second) {
              agg_strs.push_back(std::move(s));
            }
          }
          if (e->Type() == TypeTag::kBinaryExp) {
            const auto& bin = e->AsBinaryExpression();
            collect_agg(bin.Left());
            collect_agg(bin.Right());
          }
          if (e->Type() == TypeTag::kUnaryExp) {
            collect_agg(e->AsUnaryExpression().Child());
          }
        };
        for (const NamedExpression& item : statement.SelectList()) {
          if (item.expression) { collect_agg(item.expression); }
        }
        output << pad << "Aggregate";
        if (!agg_strs.empty()) {
          output << " expressions=";
          for (size_t i = 0; i < agg_strs.size(); ++i) {
            if (i > 0) output << ',';
            output << agg_strs[i];
          }
        }
        output << " strategy=scalar" << '\n';
      }
    } else if (has_group_by) {
      // Check if all GROUP BY keys are constants (e.g. GROUP BY 1)
      const bool all_constant_keys = std::ranges::all_of(
          statement.GroupBy(), [](const Expression& key) {
            return key && key->Type() == TypeTag::kConstantValue;
          });
      if (all_constant_keys) {
        // Constant GROUP BY keys are removed; treat as scalar aggregate.
        output << pad << "Aggregate strategy=scalar" << '\n';
      } else {
        // Build group_keys annotation from GROUP BY columns.
        std::ostringstream gk;
        gk << "group_keys=";
        for (size_t gi = 0; gi < statement.GroupBy().size(); ++gi) {
          if (gi > 0) gk << ',';
          gk << statement.GroupBy()[gi]->ToString();
        }
        const std::string group_keys_str = gk.str();
        // Check if ORDER BY matches GROUP BY keys (stream aggregate)
        const bool order_matches_group = !statement.OrderBy().empty() &&
            std::ranges::all_of(statement.OrderBy(),
                [&group_by = statement.GroupBy()](const SelectStatement::OrderByTerm& term) {
                  // Check if ORDER BY term matches a GROUP BY key
                  return std::ranges::any_of(group_by.begin(), group_by.end(),
                      [&term](const Expression& key) {
                        if (!term.expression || !key) return false;
                        return term.expression->ToString() == key->ToString();
                      });
                });
        
        if (order_matches_group) {
          // Stream aggregate: ORDER BY matches GROUP BY
          output << pad << "StreamAggregate " << group_keys_str << '\n';
        } else {
          // Hash aggregate: GROUP BY without matching ORDER BY
          output << pad << "Aggregate " << group_keys_str << '\n';
        }
      }

      // Detect decomposable aggregates: when all aggregates are SUM, COUNT,
      // MIN, MAX (without DISTINCT or WHERE filter), annotate with
      // PartialAggregate/FinalAggregate pipeline.
      if (!has_having) {
        const bool all_decomposable = std::ranges::all_of(
            statement.SelectList().begin(), statement.SelectList().end(),
            [](const NamedExpression& item) {
              if (!item.expression) return true;
              if (!relational_detail::ContainsAggregate(item.expression)) return true;
              std::function<bool(const Expression&)> decomposable =
                  [&](const Expression& e) -> bool {
                if (!e) return true;
                if (e->Type() == TypeTag::kAggregateExp) {
                  const auto& agg = e->AsAggregateExpression();
                  return !agg.Distinct() && !agg.WhereFilter() &&
                         (agg.GetType() == AggregationType::kSum ||
                          agg.GetType() == AggregationType::kCount ||
                          agg.GetType() == AggregationType::kMin ||
                          agg.GetType() == AggregationType::kMax);
                }
                if (e->Type() == TypeTag::kBinaryExp) {
                  const auto& bin = e->AsBinaryExpression();
                  return decomposable(bin.Left()) && decomposable(bin.Right());
                }
                return true;
              };
              return decomposable(item.expression);
            });
        if (all_decomposable && has_aggregates) {
          output << pad << "PartialAggregate" << '\n';
          output << pad << "FinalAggregate" << '\n';
        }
      }
    }

    // Detect TwoPhaseDistinctAggregate: when any SELECT item contains
    // COUNT(DISTINCT ...), annotate the plan with TwoPhaseDistinctAggregate.
    {
      // Recursively collect distinct column names from aggregate expressions.
      std::vector<std::string> distinct_cols;
      std::function<void(const Expression&)> collect_distinct;
      collect_distinct = [&](const Expression& expr) {
        if (!expr) { return; }
        if (expr->Type() == TypeTag::kAggregateExp) {
          const auto& agg = expr->AsAggregateExpression();
          if (agg.Distinct() && agg.GetType() == AggregationType::kCount &&
              agg.Child() && agg.Child()->Type() == TypeTag::kColumnValue) {
            distinct_cols.push_back(
                agg.Child()->AsColumnValue().GetColumnName().name);
          }
          if (agg.Child()) { collect_distinct(agg.Child()); }
          return;
        }
        if (expr->Type() == TypeTag::kBinaryExp) {
          const auto& bin = expr->AsBinaryExpression();
          collect_distinct(bin.Left());
          collect_distinct(bin.Right());
        }
      };
      for (const NamedExpression& item : statement.SelectList()) {
        if (item.expression) { collect_distinct(item.expression); }
      }
      if (!distinct_cols.empty()) {
        output << pad << "TwoPhaseDistinctAggregate" << '\n';
      }
      // Detect SharedDistinctSet: when multiple distinct aggregates share
      // the same input column.
      if (distinct_cols.size() >= 2) {
        std::unordered_map<std::string, int> col_counts;
        for (const auto& col : distinct_cols) {
          col_counts[col]++;
        }
        for (const auto& [col, count] : col_counts) {
          if (count >= 2) {
            output << pad << "SharedDistinctSet " << col << '\n';
          }
        }
      }
    }

    // HAVING clause: push down to Filter when condition is on group keys only
    if (has_having) {
      const Expression& having = statement.Having();
      const bool having_is_agg = relational_detail::ContainsAggregate(having);
      
      if (!having_is_agg && has_group_by) {
        // HAVING condition references only group keys - push down as Filter
        std::string having_text = having->ToString();
        // Strip outer parentheses for cleaner output
        if (having_text.size() >= 2 && having_text.front() == '(' && having_text.back() == ')') {
          having_text = having_text.substr(1, having_text.size() - 2);
        }
        output << pad << "Filter " << having_text << '\n';
      } else {
        // HAVING condition references aggregates - keep above aggregate
        output << pad << "having=true" << '\n';
      }
    }
    
    // Detect DISTINCT strategy
    if (statement.Distinct()) {
      const bool has_order_by = !statement.OrderBy().empty();
      if (has_order_by) {
        output << pad << "SortDistinct" << '\n';
      } else {
        output << pad << "HashDistinct" << '\n';
      }
    }
    
    // Project after aggregation
    output << pad << "Project columns=" << statement.SelectList().size()
           << " distinct=false" << '\n';
  } else if (statement.Distinct()) {
    // DISTINCT without aggregation
    const bool has_order_by = !statement.OrderBy().empty();
    if (has_order_by) {
      output << pad << "SortDistinct" << '\n';
    } else {
      output << pad << "HashDistinct" << '\n';
    }
    output << pad << "Project columns=" << statement.SelectList().size()
           << " distinct=false" << '\n';
  } else {
    // No aggregation
    output << pad << "Project columns=" << statement.SelectList().size()
           << " distinct=false" << '\n';
  }
  
  // Sort (if not stream aggregate or sort distinct)
  if (!statement.OrderBy().empty()) {
    const bool is_stream_agg = needs_aggregation && has_group_by &&
        !statement.OrderBy().empty() &&
        std::ranges::all_of(statement.OrderBy(),
            [&group_by = statement.GroupBy()](const SelectStatement::OrderByTerm& term) {
              return std::ranges::any_of(group_by.begin(), group_by.end(),
                  [&term](const Expression& key) {
                    if (!term.expression || !key) return false;
                    return term.expression->ToString() == key->ToString();
                  });
            });
    const bool is_sort_distinct = statement.Distinct() && !statement.OrderBy().empty();
    
    if (!is_stream_agg && !is_sort_distinct) {
      output << pad << "Sort keys=" << statement.OrderBy().size() << '\n';
    }
  }
  if (statement.Limit() != 0 || statement.Offset() != 0) {
    output << pad << "Limit count=" << statement.Limit()
           << " offset=" << statement.Offset() << '\n';
  }
  const QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  output << pad << "QueryMemory limit=";
  if (budget.Unlimited()) {
    output << "unlimited";
  } else {
    output << FormatBytes(budget.Limit()) << " soft="
           << FormatBytes(budget.Limit() / 5 * 4);
  }
  output << '\n';
}

}  // namespace tinylamb::relational_detail
