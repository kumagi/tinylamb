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
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/query_memory.hpp"
#include "expression/expression.hpp"
#include "expression/rewrite.hpp"
#include "query/statement.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

std::string IndentLines(std::string_view text, int spaces) {
  if (text.empty()) {
    return {};
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
  if (!node.rows_known) {
    return "unknown";
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
      head << "HybridHashJoin build~" << FormatRows(right);
      if (right.rows_known) {
        head << " (~" << FormatBytes(right.rows * kHashJoinRowBytesEstimate)
             << ")";
      }
    } else {
      head << "HashJoin build~" << FormatRows(right);
      if (right.rows_known) {
        head << " (~" << FormatBytes(right.rows * kHashJoinRowBytesEstimate)
             << ")";
      }
    }
  }
  if (!join_kind.empty()) {
    head << " type=" << join_kind;
  }
  head << " rows~" << FormatRows(out);
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
  if (statement.Sources().empty()) {
    output << pad << "Result rows~1\n";
  } else {
    std::vector<EstimatedPlanNode> nodes;
    nodes.reserve(statement.Sources().size());
    for (const SelectSource& source : statement.Sources()) {
      nodes.push_back(MakeScanNode(context, source, empty_ctes));
    }

    const bool has_outer_join =
        std::any_of(statement.Sources().begin() + 1, statement.Sources().end(),
                    [](const SelectSource& source) {
                      return source.join_type == JoinType::kLeft ||
                             source.join_type == JoinType::kRight ||
                             source.join_type == JoinType::kFull;
                    });

    EstimatedPlanNode plan;
    if (has_outer_join) {
      // Mirror the executor's outer-to-inner reduction so the displayed
      // join kinds match what actually runs.
      std::vector<Relation> schema_only(nodes.size());
      for (size_t i = 0; i < nodes.size(); ++i) {
        schema_only[i].schema = nodes[i].schema;
      }
      bool reduced_all = false;
      const std::vector<SelectSource> reduced_sources =
          ReduceOuterJoinsToInner(statement, schema_only, &reduced_all);
      const bool any_outer_remaining =
          std::any_of(reduced_sources.begin() + 1, reduced_sources.end(),
                      [](const SelectSource& source) {
                        return source.join_type == JoinType::kLeft ||
                               source.join_type == JoinType::kRight ||
                               source.join_type == JoinType::kFull;
                      });
      if (any_outer_remaining) {
        output << pad << "JoinOrder=syntactic (outer joins present)\n";
      } else {
        output << pad
               << "JoinOrder=greedy_filtered_cardinality "
                  "(outer joins reduced by null-rejecting WHERE)\n";
      }
      plan = std::move(nodes.front());
      for (size_t i = 1; i < nodes.size(); ++i) {
        const SelectSource& source = reduced_sources[i];
        std::vector<Expression> predicates =
            SplitConjuncts(source.join_condition);
        const char* kind = "cross";
        if (source.join_type == JoinType::kInner) {
          kind = "inner";
        }
        if (source.join_type == JoinType::kLeft) {
          kind = "left";
        }
        if (source.join_type == JoinType::kRight) {
          kind = "right";
        }
        if (source.join_type == JoinType::kFull) {
          kind = "full";
        }
        plan = MakeJoinNode(plan, nodes[i], predicates, kind);
      }
    } else {
      output << pad
             << "JoinOrder=greedy_filtered_cardinality "
                "(equality=hash|hybrid, fallback=nested_loop)\n";
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

      size_t first = 0;
      for (size_t i = 1; i < nodes.size(); ++i) {
        if (nodes[i].rows < nodes[first].rows) {
          first = i;
        }
      }
      plan = std::move(nodes[first]);
      std::unordered_set<size_t> joined{first};
      std::unordered_set<size_t> remaining;
      for (size_t i = 0; i < nodes.size(); ++i) {
        if (i != first) {
          remaining.insert(i);
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
          const bool cheaper = estimate < next_estimate ||
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
        }
        plan = MakeJoinNode(plan, nodes[next], applicable, "inner");
        joined.insert(next);
        remaining.erase(next);
      }
    }
    output << IndentLines(plan.text, indent) << '\n';
  }

  if (statement.WhereClause()) {
    output << pad << "Filter " << *statement.WhereClause() << '\n';
  }
  if (!statement.GroupBy().empty() || statement.Having()) {
    output << pad << "Aggregate group_keys=" << statement.GroupBy().size()
           << " having=" << (statement.Having() ? "true" : "false") << '\n';
  }
  output << pad << "Project columns=" << statement.SelectList().size()
         << " distinct=" << (statement.Distinct() ? "true" : "false") << '\n';
  if (!statement.OrderBy().empty()) {
    output << pad << "Sort keys=" << statement.OrderBy().size() << '\n';
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
    output << FormatBytes(budget.Limit())
           << " soft=" << FormatBytes(budget.Limit() / 5 * 4);
  }
  output << '\n';
}

}  // namespace tinylamb::relational_detail
