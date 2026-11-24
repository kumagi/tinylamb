/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/platform/process.hpp"
#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/query_parallelism.hpp"
#include "common/runtime_filter.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/build_input_internal.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_grouped_join.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/query_memory.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "query/statement.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb::relational_detail {

// Expands a lateral (correlated) UNNEST source against the relation
// accumulated from the preceding FROM items: for every prefix row the array
// expression is evaluated with that row in scope and each element becomes an
// output row joined with the prefix row.
// GCC 15 emits a spurious -Wfree-nonheap-object inside the inlined
// std::vector deallocation here even though every container lifetime is
// standard; suppress it just for this function.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfree-nonheap-object"

StatusOr<Relation> LateralExpandRelation(TransactionContext& context,
                                         const SelectSource& source,
                                         Relation& prefix, const Scope* outer,
                                         const CteMap& ctes) {
  Relation output(context.execution_runtime());
  const std::string qualifier = source.alias.empty() ? "unnest" : source.alias;
  auto element_schema_of = [&](const Value& array_val) {
    Relation elements = UnnestValueToRelation(source, array_val);
    if (!qualifier.empty()) {
      elements.schema = QualifySchema(elements.schema, qualifier);
    }
    return elements;
  };
  const Expression condition =
      source.join_condition ? source.join_condition : nullptr;
  // USING(name) equalities bind positionally: both operands share one bare
  // name, so combined-scope evaluation would be ambiguous (and after
  // coalescing, trivially true).  Each such conjunct resolves to a
  // prefix-side column and an element-side name. Bind that name against the
  // actual expansion schema, which includes STRUCT and subquery columns.
  struct UsingEquality {
    size_t prefix_column;
    ColumnName element_name;
  };
  std::vector<UsingEquality> using_equalities;
  std::vector<Expression> residual_condition =
      condition ? SplitConjuncts(condition) : std::vector<Expression>{};
  if (!source.using_columns.empty() && condition) {
    auto declared = [&](const ColumnName& name) {
      if (!name.schema.empty()) {
        return false;
      }
      return std::ranges::any_of(
          source.using_columns, [&](const std::string& candidate) {
            return candidate.size() == name.name.size() &&
                   std::equal(
                       candidate.begin(), candidate.end(), name.name.begin(),
                       [](char lhs, char rhs) {
                         return std::tolower(static_cast<unsigned char>(lhs)) ==
                                std::tolower(static_cast<unsigned char>(rhs));
                       });
          });
    };
    for (size_t conjunct_index = 0;
         conjunct_index < residual_condition.size();) {
      const Expression& conjunct = residual_condition[conjunct_index];
      bool handled = false;
      if (IsColumnEqualityPredicate(conjunct)) {
        const BinaryExpression& binary = conjunct->AsBinaryExpression();
        const ColumnName& ln = binary.Left()->AsColumnValue().GetColumnName();
        const ColumnName& rn = binary.Right()->AsColumnValue().GetColumnName();
        if (declared(ln) && declared(rn)) {
          const auto prefix_offset = LocalColumnOffset(prefix.schema, ln);
          if (!prefix_offset) {
            return StatusError(StatusCode::kInvalidArgument,
                               "unresolved LATERAL USING column: " + ln.name);
          }
          using_equalities.push_back({*prefix_offset, rn});
          handled = true;
        }
      }
      if (handled) {
        residual_condition.erase(residual_condition.begin() +
                                 static_cast<ptrdiff_t>(conjunct_index));
      } else {
        ++conjunct_index;
      }
    }
  }
  const bool want_left_nulls = source.join_type == JoinType::kLeft ||
                               source.join_type == JoinType::kFull;
  const bool want_right_nulls = source.join_type == JoinType::kRight ||
                                source.join_type == JoinType::kFull;
  if (!source.using_columns.empty()) {
    output.using_columns =
        std::make_shared<const std::vector<std::string>>(source.using_columns);
  }
  const bool left_outer = want_left_nulls;
  bool any_prefix_row = false;
  bool schema_initialized = false;
  // A LATERAL query that never references the prefix row yields the same
  // element set for every prefix row. For RIGHT/FULL joins the unmatched
  // element rows must then be emitted once globally: replaying them inside
  // each prefix row's expansion would duplicate them by the prefix
  // cardinality (and emit nothing at all for an empty prefix). The
  // uncorrelated-result cache both proves independence — it refuses
  // correlated, volatile, and CTE-scoped statements — and supplies the
  // shared element relation.
  const Relation* shared_elements = nullptr;
  Schema shared_elements_schema;
  std::vector<char> shared_element_matched;
  if (want_right_nulls && source.query && !source.unnest &&
      source.query->WithQueries().empty() &&
      context.execution_runtime() != nullptr) {
    shared_elements = ExecuteCachedUncorrelated(context, *source.query, ctes);
    if (shared_elements != nullptr) {
      shared_elements_schema = shared_elements->schema;
      if (!source.alias.empty()) {
        shared_elements_schema =
            QualifySchema(shared_elements_schema, source.alias);
      }
      shared_element_matched.assign(shared_elements->TotalRows(), 0);
    }
  }
  RETURN_IF_FAIL(prefix.FinishSpill());
  Status elements_status = Status::kSuccess;
  const Status prefix_status = prefix.ForEachRow([&](const Row& row) -> void {
    if (elements_status != Status::kSuccess) {
      return;
    }
    any_prefix_row = true;
    Scope scope{.row = &row, .schema = &prefix.schema, .outer = outer};
    Relation elements(context.execution_runtime());
    const Relation* elements_view = nullptr;
    const Schema* elements_schema = nullptr;
    if (source.unnest) {
      StatusOr<Value> array_or =
          TryEvaluate(source.unnest, scope, nullptr, context, ctes);
      if (!array_or.HasValue()) {
        if (elements_status == Status::kSuccess) {
          elements_status = array_or.GetStatus();
        }
        return;
      }
      const Value array_val = array_or.MoveValue();
      elements = element_schema_of(array_val);
      elements_view = &elements;
      elements_schema = &elements.schema;
    } else if (source.query) {
      if (shared_elements != nullptr) {
        elements_view = shared_elements;
        elements_schema = &shared_elements_schema;
      } else {
        StatusOr<Relation> elements_query =
            ExecuteQuery(context, *source.query, &scope, ctes);
        if (!elements_query.HasValue()) {
          elements_status = elements_query.GetStatus();
          return;
        }
        elements = elements_query.MoveValue();
        if (!source.alias.empty()) {
          elements.schema = QualifySchema(elements.schema, source.alias);
        }
        elements_view = &elements;
        elements_schema = &elements.schema;
      }
    }
    if (elements_view == nullptr) {
      elements_view = &elements;
      elements_schema = &elements.schema;
    }
    if (!schema_initialized) {
      output.schema = prefix.schema + *elements_schema;
      schema_initialized = true;
    }
    // The element relation's shape is derived from the array value at hand,
    // so a later prefix row can in principle expand to fewer columns than
    // the row that initialized output.schema.  NULL-fill (or trim) back to
    // the initialized arity so every emitted row matches output.schema
    // exactly; Lookup and projection index rows by schema offsets.
    elements_status = const_cast<Relation*>(elements_view)->FinishSpill();
    if (elements_status != Status::kSuccess) {
      return;
    }
    std::vector<size_t> element_columns;
    if (elements_view->TotalRows() != 0) {
      element_columns.reserve(using_equalities.size());
      for (const auto& key : using_equalities) {
        const auto column =
            LocalColumnOffset(*elements_schema, key.element_name);
        if (!column) {
          elements_status = StatusError(
              StatusCode::kInvalidArgument,
              "unresolved LATERAL USING column: " + key.element_name.name);
          return;
        }
        element_columns.push_back(*column);
      }
    }
    // For a shared (uncorrelated) element relation the matched flags
    // accumulate across every prefix row: an element row is preserved by
    // RIGHT/FULL only when no prefix row matched it.
    std::vector<char> row_element_matched(
        want_right_nulls && elements_view != shared_elements
            ? elements_view->TotalRows()
            : 0,
        0);
    std::vector<char>& element_matched = elements_view == shared_elements
                                             ? shared_element_matched
                                             : row_element_matched;
    size_t element_index = 0;
    bool matched = false;
    const Status iteration =
        elements_view->ForEachRow([&](const Row& element_row) {
          const size_t this_element = element_index++;
          Row combined = row + element_row;
          if (combined.values_.size() > output.schema.ColumnCount()) {
            // A later element row expanded to more columns than the schema
            // initialized from the first row; truncating would silently drop
            // data.
            elements_status =
                StatusError(StatusCode::kInvalidArgument,
                            "LATERAL element row is wider than output schema");
            return;
          }
          combined.values_.resize(output.schema.ColumnCount());
          if (condition) {
            Scope combined_scope{
                .row = &combined, .schema = &output.schema, .outer = outer};
            if (!source.using_columns.empty()) {
              combined_scope.using_columns = &source.using_columns;
            }
            bool all_match = true;
            for (size_t k = 0; k < element_columns.size(); ++k) {
              const size_t left = using_equalities[k].prefix_column;
              const size_t right = element_columns[k];
              if (left >= row.values_.size() ||
                  right >= element_row.values_.size()) {
                all_match = false;
                break;
              }
              const Value& left_value = row[left];
              const Value& right_value = element_row[right];
              if (!Truthy(EvaluateBinary(BinaryOperation::kEquals, left_value,
                                         right_value))) {
                all_match = false;
                break;
              }
            }
            if (all_match) {
              for (const Expression& conjunct : residual_condition) {
                StatusOr<Value> conjunct_value = TryEvaluate(
                    conjunct, combined_scope, nullptr, context, ctes);
                if (!conjunct_value.HasValue()) {
                  if (elements_status == Status::kSuccess) {
                    elements_status = conjunct_value.GetStatus();
                  }
                  return;
                }
                if (!Truthy(conjunct_value.Value())) {
                  all_match = false;
                  break;
                }
              }
            }
            if (!all_match) {
              return;
            }
          }
          if (want_right_nulls && this_element < element_matched.size()) {
            element_matched[this_element] = 1;
          }
          matched = true;
          output.AddRow(std::move(combined));
        });
    if (iteration != Status::kSuccess) {
      elements_status = iteration;
    }
    if (elements_status != Status::kSuccess) {
      return;
    }
    if (!matched && left_outer &&
        (condition != nullptr || elements_view->TotalRows() == 0)) {
      std::vector<Value> nulls(output.schema.ColumnCount() - row.values_.size(),
                               Value());
      output.AddRow(ConcatRows(row, Row(std::move(nulls))));
    }
    // RIGHT / FULL OUTER: within this prefix row's expansion, element rows
    // that matched nothing emit once, null-extended on the prefix side. A
    // shared (uncorrelated) element set is replayed once after the prefix
    // loop instead — see shared_element_matched above.
    if (want_right_nulls && elements_view != shared_elements &&
        !element_matched.empty()) {
      size_t replay_index = 0;
      elements_status = elements_view->ForEachRow([&](const Row& element_row) {
        const size_t index = replay_index++;
        if (index < element_matched.size() && element_matched[index] == 0) {
          std::vector<Value> nulls(prefix.schema.ColumnCount(), Value());
          Row unmatched = Row(std::move(nulls)) + element_row;
          // Later prefix rows can expand to fewer columns; normalize the
          // unmatched RIGHT/FULL row to the declared output arity just like
          // the matched-row path above.
          unmatched.values_.resize(output.schema.ColumnCount(), Value());
          output.AddRow(std::move(unmatched));
        }
      });
    }
  });
  RETURN_IF_FAIL(prefix_status);
  RETURN_IF_FAIL(elements_status);
  if (shared_elements != nullptr) {
    if (!schema_initialized) {
      output.schema = prefix.schema + shared_elements_schema;
      schema_initialized = true;
    }
    if (want_right_nulls && !shared_element_matched.empty()) {
      size_t replay_index = 0;
      const Status replay_status =
          shared_elements->ForEachRow([&](const Row& element_row) {
            const size_t index = replay_index++;
            if (index < shared_element_matched.size() &&
                shared_element_matched[index] == 0) {
              std::vector<Value> nulls(prefix.schema.ColumnCount(), Value());
              Row unmatched = Row(std::move(nulls)) + element_row;
              unmatched.values_.resize(output.schema.ColumnCount(), Value());
              output.AddRow(std::move(unmatched));
            }
          });
      if (replay_status != Status::kSuccess) {
        return replay_status;
      }
    }
  }
  if (!any_prefix_row || !schema_initialized) {
    // Empty prefix (or every prefix row skipped before initialization):
    // still derive the output schema so downstream projection sees the
    // unnest columns; evaluating on an all-NULL representative row keeps
    // this exception-free for constant arrays.
    if (!schema_initialized) {
      Row representative(
          std::vector<Value>(prefix.schema.ColumnCount(), Value()));
      Scope scope{
          .row = &representative, .schema = &prefix.schema, .outer = outer};
      Relation elements(context.execution_runtime());
      if (source.unnest) {
        StatusOr<Value> array_or =
            TryEvaluate(source.unnest, scope, nullptr, context, ctes);
        if (!array_or.HasValue()) {
          return array_or.GetStatus();
        }
        const Value array_val = array_or.MoveValue();
        elements = element_schema_of(array_val);
      } else if (source.query) {
        StatusOr<Relation> elements_query =
            ExecuteQuery(context, *source.query, &scope, ctes);
        if (!elements_query.HasValue()) {
          return elements_query.GetStatus();
        }
        elements = elements_query.MoveValue();
        if (!source.alias.empty()) {
          elements.schema = QualifySchema(elements.schema, source.alias);
        }
      }
      output.schema = prefix.schema + elements.schema;
      schema_initialized = true;
    }
  }
  RETURN_IF_FAIL(output.FinishSpill());
  if (elements_status != Status::kSuccess) {
    return elements_status;
  }
  return output;
}

#pragma GCC diagnostic pop

// A filtered dimension bounds the join key of every UNION ALL branch. Keep
// the original join (and its multiplicity); the range is only a necessary
// prefilter, derived from this execution's snapshot.
std::shared_ptr<SelectStatement> RestrictUnionInput(
    TransactionContext& context, const SelectStatement& parent, size_t target,
    const CteMap& ctes) {
  const auto& source = parent.Sources()[target];
  const SelectStatement* query = source.query.get();
  // A naming-only wrapper does not prevent dimension bounds from reaching
  // the UNION branches. Keep every row-changing or scope-changing boundary.
  for (size_t depth = 0; query != nullptr && depth < 16; ++depth) {
    if (query->Sources().size() != 1 || !query->Sources()[0].query ||
        !query->UnionAll().empty() || query->GetSetOperationTree() ||
        !query->WithQueries().empty() || !query->GroupBy().empty() ||
        query->IsGroupAll() || query->WhereClause() || query->Having() ||
        query->Qualify() || query->Distinct() || !query->DistinctOn().empty() ||
        query->HasLimit() || query->Offset() != 0 ||
        !query->OrderBy().empty() || query->AsStruct() || query->ForUpdate()) {
      break;
    }
    const auto& input = query->Sources()[0];
    const auto& nested = *input.query;
    if (input.sample || input.unnest || input.is_lateral ||
        input.from_nested_join || input.join_condition ||
        !input.using_columns.empty() || query->SelectList().empty() ||
        query->SelectList().size() != nested.SelectList().size()) {
      break;
    }
    bool identity = true;
    std::unordered_set<std::string> names;
    names.reserve(query->SelectList().size());
    for (size_t i = 0; i < query->SelectList().size(); ++i) {
      const auto& item = query->SelectList()[i];
      const auto name = ProjectionName(nested.SelectList()[i], i);
      if (!item.expression ||
          item.expression->Type() != TypeTag::kColumnValue ||
          ProjectionName(item, i) != name || !names.insert(name).second) {
        identity = false;
        break;
      }
      const auto& column = item.expression->AsColumnValue().GetColumnName();
      if (column.name != name || column.name == "*" ||
          (!column.schema.empty() && column.schema != input.alias)) {
        identity = false;
        break;
      }
    }
    if (!identity) {
      break;
    }
    query = &nested;
  }
  if (query == nullptr || source.sample || query->UnionAll().empty() ||
      query->UnionAll().size() > 32 || !query->WithQueries().empty() ||
      query->HasLimit() || query->Offset() != 0 || query->UnionByName() ||
      (query->GetSetOperationTree() && query->GetSetOperationTree()->grouped) ||
      std::ranges::any_of(query->SetOperationKinds(), [](auto kind) {
        return kind != SetOperationKind::kUnionAll;
      })) {
    return nullptr;
  }
  for (size_t i = 0; i < query->UnionAll().size(); ++i) {
    const auto match = query->MatchFor(i);
    if (match.by_name || match.corresponding || !match.columns.empty()) {
      return nullptr;
    }
  }
  std::vector<Schema> schemas(parent.Sources().size());
  for (size_t i = 0; i < parent.Sources().size(); ++i) {
    if (i == target) {
      continue;
    }
    const auto& other = parent.Sources()[i];
    if (other.query || other.unnest || other.sample ||
        ctes.contains(other.table)) {
      return nullptr;
    }
    auto table = context.GetTable(other.table);
    if (!table.HasValue()) {
      return nullptr;
    }
    schemas[i] = QualifySchema(table.Value()->GetSchema(),
                               other.alias.empty() ? other.table : other.alias);
  }
  std::vector<Column> projected;
  projected.reserve(query->SelectList().size());
  for (size_t i = 0; i < query->SelectList().size(); ++i) {
    const auto& item = query->SelectList()[i];
    if (!item.expression ||
        (item.expression->Type() == TypeTag::kColumnValue &&
         item.expression->AsColumnValue().GetColumnName().name == "*")) {
      return nullptr;
    }
    projected.emplace_back(ProjectionName(item, i), ValueType::kNull);
  }
  schemas[target] =
      QualifySchema(Schema("", std::move(projected)), source.alias);
  auto predicates = SplitConjuncts(parent.WhereClause());
  for (const auto& item : parent.Sources()) {
    const auto terms = SplitConjuncts(item.join_condition);
    predicates.insert(predicates.end(), terms.begin(), terms.end());
  }
  for (const auto& predicate : predicates) {
    if (!IsColumnEqualityPredicate(predicate) ||
        predicate->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      continue;
    }
    const auto& binary = predicate->AsBinaryExpression();
    Expression key = binary.Left();
    Expression dimension_key = binary.Right();
    if (!LocalColumnOffset(schemas[target],
                           key->AsColumnValue().GetColumnName())) {
      std::swap(key, dimension_key);
    }
    const auto slot = LocalColumnOffset(schemas[target],
                                        key->AsColumnValue().GetColumnName());
    if (!slot ||
        LocalColumnOffset(schemas[target],
                          dimension_key->AsColumnValue().GetColumnName())) {
      continue;
    }
    size_t dimension = schemas.size();
    std::optional<size_t> dimension_slot;
    bool ambiguous = false;
    for (size_t i = 0; i < schemas.size(); ++i) {
      if (i == target) {
        continue;
      }
      if (LocalColumnOffset(schemas[i], key->AsColumnValue().GetColumnName())) {
        ambiguous = true;
      }
      if (auto found = LocalColumnOffset(
              schemas[i], dimension_key->AsColumnValue().GetColumnName())) {
        ambiguous |= dimension_slot.has_value();
        dimension = i;
        dimension_slot = found;
      }
    }
    if (ambiguous || !dimension_slot) {
      continue;
    }
    const auto& column = schemas[dimension].GetColumn(*dimension_slot);
    if (column.Type() != ValueType::kInt64 || column.IsUnsigned()) {
      continue;
    }
    std::vector<Expression> local;
    for (const auto& term : SplitConjuncts(parent.WhereClause())) {
      if (IsNonThrowingScanComparison(term, schemas[dimension]) &&
          !term->TouchedColumns().empty() &&
          std::ranges::all_of(term->TouchedColumns(), [&](const auto& name) {
            return std::ranges::count_if(schemas, [&](const Schema& schema) {
                     return LocalColumnOffset(schema, name).has_value();
                   }) == 1;
          })) {
        local.push_back(term);
      }
    }
    if (local.empty()) {
      continue;
    }
    auto statistics = context.GetStats(parent.Sources()[dimension].table);
    const auto filter = CombineConjuncts(local);
    if (!statistics.HasValue() || statistics.Value()->Rows() == 0 ||
        (statistics.Value()->Rows() > 100000 &&
         static_cast<double>(statistics.Value()->Rows()) *
                 statistics.Value()->EstimateSelectivity(schemas[dimension],
                                                         filter) >
             4096)) {
      continue;
    }
    // Validate every branch's positional key before executing the extra scan.
    std::vector<const SelectStatement*> parts{query};
    parts.reserve(query->UnionAll().size() + 1);
    for (const auto& branch : query->UnionAll()) {
      parts.push_back(branch.get());
    }
    bool valid = true;
    for (const auto* part : parts) {
      if (part == nullptr ||
          part->SelectList().size() != query->SelectList().size() ||
          !part->GroupBy().empty() || part->IsGroupAll() || part->Having() ||
          part->Qualify() || part->Distinct() || part->HasLimit() ||
          part->Offset() != 0 || !part->WithQueries().empty() ||
          (part != query && !part->UnionAll().empty()) ||
          HasWindowFunctions(*part) ||
          StatementContainsVolatileFunction(*part) ||
          std::ranges::any_of(part->Sources(),
                              [&](const auto& input) {
                                return input.sample || input.unnest ||
                                       input.query ||
                                       ctes.contains(input.table) ||
                                       input.is_lateral;
                              }) ||
          std::ranges::any_of(part->SelectList(), [](const auto& item) {
            return ContainsAggregate(item.expression) ||
                   ContainsQuery(item.expression);
          })) {
        valid = false;
        break;
      }
      const auto& expr = part->SelectList()[*slot].expression;
      size_t matches = 0;
      bool integer_key = false;
      if (expr->Type() == TypeTag::kColumnValue) {
        for (const auto& input : part->Sources()) {
          if (input.query || input.unnest || ctes.contains(input.table)) {
            continue;
          }
          auto table = context.GetTable(input.table);
          if (!table.HasValue()) {
            continue;
          }
          const auto schema =
              QualifySchema(table.Value()->GetSchema(),
                            input.alias.empty() ? input.table : input.alias);
          if (const auto offset = LocalColumnOffset(
                  schema, expr->AsColumnValue().GetColumnName())) {
            const auto& physical = schema.GetColumn(*offset);
            ++matches;
            integer_key =
                physical.Type() == ValueType::kInt64 && !physical.IsUnsigned();
          }
        }
      }
      valid &= matches == 1 && integer_key;
    }
    if (!valid) {
      continue;
    }
    auto dimension_source = parent.Sources()[dimension];
    dimension_source.join_type = JoinType::kCross;
    dimension_source.join_condition = nullptr;
    auto domain = std::make_shared<SelectStatement>(
        std::vector<NamedExpression>{NamedExpression("key", dimension_key)},
        std::vector<std::string>{}, filter);
    domain->SetSources({dimension_source});
    domain->SetLimit(4097);
    if (auto* runtime = context.execution_runtime()) {
      runtime->retained_statements.push_back(domain);
    }
    auto selected = ExecuteQuery(context, *domain, nullptr, ctes);
    if (!selected.HasValue() || selected.Value().TotalRows() > 4096 ||
        selected.Value().HasSpill()) {
      continue;
    }
    std::optional<int64_t> minimum, maximum;
    for (const auto& row : selected.Value().rows) {
      if (!row[0].IsNull()) {
        const auto value = row[0].value.int_value;
        minimum = minimum ? std::min(*minimum, value) : value;
        maximum = maximum ? std::max(*maximum, value) : value;
      }
    }
    const auto restrict = [&](const SelectStatement& part) {
      auto result = std::make_shared<SelectStatement>(part);
      auto terms = SplitConjuncts(part.WhereClause());
      const auto& input_key = part.SelectList()[*slot].expression;
      if (minimum) {
        terms.push_back(BinaryExpressionExp(input_key,
                                            BinaryOperation::kGreaterThanEquals,
                                            ConstantValueExp(Value(*minimum))));
        terms.push_back(BinaryExpressionExp(input_key,
                                            BinaryOperation::kLessThanEquals,
                                            ConstantValueExp(Value(*maximum))));
      } else {
        terms.push_back(ConstantValueExp(Value(0)));
      }
      result->SetWhereClause(CombineConjuncts(terms));
      return result;
    };
    auto rewritten = restrict(*query);
    rewritten->ClearUnionAll();
    rewritten->SetSetOperationTree(nullptr);
    for (const auto& branch : query->UnionAll()) {
      rewritten->AddSetOperation(SetOperationKind::kUnionAll,
                                 restrict(*branch));
    }
    return rewritten;
  }
  return nullptr;
}

// WHERE right.key IS NULL after a strict LEFT equality join keeps precisely
// the unmatched left rows. Probe for existence and null-pad only survivors;
// right duplicates must never multiply either the work or the output.
StatusOr<std::optional<Relation>> TryUnmatchedLeftJoin(
    Relation& left, const Relation& right, const SelectSource& source,
    slot_t null_column, bool emit_right_columns) {
  if (source.join_type != JoinType::kLeft || left.HasSpill() ||
      right.HasSpill() || left.HasColumnarOnly() || right.HasColumnarOnly() ||
      left.using_columns || right.using_columns ||
      !source.using_columns.empty() || right.rows.size() > 1048576) {
    return std::optional<Relation>();
  }
  const auto predicates = SplitConjuncts(source.join_condition);
  if (predicates.empty() || predicates.size() > CompositeIntKey::kMaxParts ||
      !std::ranges::all_of(predicates, [](const Expression& predicate) {
        return IsColumnEqualityPredicate(predicate) &&
               predicate->AsBinaryExpression().Op() == BinaryOperation::kEquals;
      })) {
    return std::optional<Relation>();
  }
  const auto keys = EqualityKeys(left.schema, right.schema, predicates);
  if (keys.size() != predicates.size() ||
      std::ranges::none_of(keys, [&](const EqualityKey& key) {
        return key.right == null_column;
      })) {
    return std::optional<Relation>();
  }
  std::vector<slot_t> left_columns, right_columns;
  for (const auto& key : keys) {
    const auto& lcol = left.schema.GetColumn(key.left);
    const auto& rcol = right.schema.GetColumn(key.right);
    if (key.null_safe || key.left_offset != 0 || key.right_offset != 0 ||
        lcol.Type() != rcol.Type() || lcol.IsUnsigned() || rcol.IsUnsigned() ||
        (lcol.Type() != ValueType::kInt64 && lcol.Type() != ValueType::kDate)) {
      return std::optional<Relation>();
    }
    left_columns.push_back(static_cast<slot_t>(key.left));
    right_columns.push_back(static_cast<slot_t>(key.right));
  }
  bool packed_pair = keys.size() == 2;
  const auto uniform = [&](const Relation& relation,
                           const std::vector<slot_t>& columns) {
    return std::ranges::all_of(relation.rows, [&](const Row& row) {
      return std::ranges::all_of(columns, [&](slot_t column) {
        const Value& value = row[column];
        if (value.IsNull()) {
          return true;
        }
        if (value.type != relation.schema.GetColumn(column).Type() ||
            value.IsUnsigned()) {
          return false;
        }
        if (value.value.int_value < std::numeric_limits<int32_t>::min() ||
            value.value.int_value > std::numeric_limits<int32_t>::max()) {
          packed_pair = false;
        }
        return true;
      });
    });
  };
  if (!uniform(left, left_columns) || !uniform(right, right_columns)) {
    return std::optional<Relation>();
  }
  auto charge = TryReserveQueryMemory((right.rows.size() * 96) + 1024);
  if (!charge) {
    return std::optional<Relation>();
  }
  const auto started = std::chrono::steady_clock::now();
  // Existence needs neither payload pointers nor duplicate bucket chains.
  // One full-width key, or two proven signed-32-bit keys, fits injectively in
  // a compact integer set. The proof reads both inputs; a wide value on either
  // side retains the ordinary composite-key path.
  const bool compact_key = keys.size() == 1 || packed_pair;
  const auto encode_compact = [&](const Row& row,
                                  const std::vector<slot_t>& columns) {
    if (keys.size() == 1) {
      return row[columns[0]].value.int_value;
    }
    const uint64_t high =
        static_cast<uint32_t>(row[columns[0]].value.int_value);
    const uint64_t low = static_cast<uint32_t>(row[columns[1]].value.int_value);
    return std::bit_cast<int64_t>((high << 32) | low);
  };
  FlatIntKeyMap<uint8_t> present;
  FlatJoinBuckets<CompositeIntKey, CompositeIntKeyHash> buckets;
  if (compact_key) {
    present.Reserve(right.rows.size());
  } else {
    buckets.reserve(right.rows.size());
  }
  for (const Row& row : right.rows) {
    if (!HasNullKey(row, right_columns)) {
      if (compact_key) {
        *present.TryEmplace(encode_compact(row, right_columns)) = 1;
      } else {
        buckets.emplace(EncodeCompositeIntKey(row, right_columns), &row);
      }
    }
  }
  Relation result(left.runtime());
  CopyExecutionStats(&result, left);
  // The IS NULL filter already dropped every match. When no later expression
  // reads the right side, keep the left rows in place instead of allocating
  // a null-padded copy of every survivor.
  result.schema = emit_right_columns ? left.schema + right.schema : left.schema;
  result.hash_joins += right.hash_joins + 1;
  result.in_memory_hash_joins += right.in_memory_hash_joins + 1;
  result.hybrid_hash_joins += right.hybrid_hash_joins;
  result.nested_loop_joins += right.nested_loop_joins;
  result.join_comparisons += right.join_comparisons;
  const auto probe = [&](Relation& output, size_t begin, size_t end) {
    for (size_t i = begin; i < end; ++i) {
      Row& row = left.rows[i];
      if (!HasNullKey(row, left_columns)) {
        ++output.join_comparisons;
        if (compact_key) {
          if (present.Find(encode_compact(row, left_columns)) != nullptr) {
            continue;
          }
        } else {
          const auto [first, last] =
              buckets.equal_range(EncodeCompositeIntKey(row, left_columns));
          if (first != last) {
            continue;
          }
        }
      }
      if (emit_right_columns) {
        row.values_.resize(result.schema.ColumnCount());
      }
      output.AddRow(std::move(row));
    }
  };
  const size_t workers =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PARALLEL_ANTI_PROBE") != nullptr
          ? 1
          : ParallelWorkerCount(left.rows.size() / 16384);
  if (workers == 1) {
    probe(result, 0, left.rows.size());
  } else {
    // Disjoint contiguous ranges preserve both left multiplicity and encounter
    // order. Spill counters are worker-local, just like their output relations.
    std::vector<ExecutionRuntime> runtimes(workers);
    std::vector<Relation> shards(workers);
    for (size_t w = 0; w < workers; ++w) {
      shards[w].set_runtime(&runtimes[w]);
      shards[w].schema = result.schema;
    }
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
      const size_t chunk = left.rows.size() / workers;
      const size_t begin = w * chunk;
      const size_t end = w + 1 == workers ? left.rows.size() : begin + chunk;
      shards[w].rows.reserve(end - begin);
      probe(shards[w], begin, end);
    }));
    for (size_t w = 0; w < workers; ++w) {
      RETURN_IF_FAIL(shards[w].FinishSpill());
      RETURN_IF_FAIL(shards[w].GetStatus());
      result.join_comparisons += shards[w].join_comparisons;
      if (result.runtime() != nullptr) {
        result.runtime()->relation_spills += runtimes[w].relation_spills;
      }
      result.MergeInMemoryShard(std::move(shards[w]));
    }
  }
  result.peak_intermediate_rows =
      std::max({result.peak_intermediate_rows, right.peak_intermediate_rows,
                result.TotalRows()});
  RETURN_IF_FAIL(result.FinishSpill());
  if (auto* runtime = result.runtime()) {
    runtime->join_ms += ElapsedMs(started);
  }
  if (::tinylamb::platform::GetEnv("TINYLAMB_GROUP_PHASES") != nullptr &&
      left.rows.size() >= 100000) {
    std::cerr << "[anti] left=" << left.rows.size()
              << " right=" << right.rows.size() << " out=" << result.TotalRows()
              << " ms=" << ElapsedMs(started) << "\n";
  }
  return std::optional<Relation>(std::move(result));
}

// A resident star with unique dimension keys needs no pairwise intermediate
// rows. Validate every edge and actual build key before consuming any input;
// a failed proof leaves the ordinary duplicate-preserving joins untouched.
StatusOr<std::optional<Relation>> TryMaterializedStarJoin(
    std::vector<Relation>& relations, const std::vector<uint8_t>& absorbed,
    const std::vector<PredicateInfo>& predicates,
    const SelectStatement& statement, bool columnar_output) {
  std::vector<size_t> active;
  active.reserve(relations.size());
  for (size_t i = 0; i < relations.size(); ++i) {
    if (absorbed[i] == 0) {
      if (relations[i].HasSpill() || relations[i].using_columns) {
        return std::optional<Relation>();
      }
      active.push_back(i);
    }
  }
  if (active.size() < 3 || active.size() > 9) {
    return std::optional<Relation>();
  }
  const size_t fact = *std::ranges::max_element(
      active, {}, [&](size_t i) { return relations[i].TotalRows(); });
  constexpr uint32_t kStarJoinMiss = std::numeric_limits<uint32_t>::max();
  struct Dimension {
    size_t relation;
    std::optional<size_t> fact_column;
    size_t key_column{0};
    ValueType key_type{ValueType::kNull};
    FlatIntKeyMap<uint32_t> keys;
    int64_t dense_lo{0};
    std::vector<uint32_t> dense_rows;
  };
  std::vector<Dimension> dimensions;
  dimensions.reserve(active.size());
  size_t build_rows = 0;
  for (size_t i : active) {
    if (i != fact) {
      dimensions.push_back({i, std::nullopt, 0, ValueType::kNull, {}, 0, {}});
      build_rows += relations[i].TotalRows();
    }
  }
  if (build_rows > 262144) {
    return std::optional<Relation>();
  }
  for (const PredicateInfo& predicate : predicates) {
    if (!predicate.resolved || predicate.sources.size() < 2) {
      continue;  // Local filters and absorbed edges were already applied.
    }
    if (predicate.contains_query || predicate.sources.size() != 2 ||
        !predicate.sources.contains(fact) ||
        !IsColumnEqualityPredicate(predicate.expression) ||
        predicate.expression->AsBinaryExpression().Op() !=
            BinaryOperation::kEquals) {
      return std::optional<Relation>();
    }
    auto dim = std::ranges::find_if(dimensions, [&](const Dimension& d) {
      return predicate.sources.contains(d.relation);
    });
    if (dim == dimensions.end()) {
      return std::optional<Relation>();
    }
    const auto& equality = predicate.expression->AsBinaryExpression();
    const auto& lhs = equality.Left()->AsColumnValue().GetColumnName();
    const auto& rhs = equality.Right()->AsColumnValue().GetColumnName();
    const auto& fact_schema = relations[fact].schema;
    const auto& dim_schema = relations[dim->relation].schema;
    auto fc = LocalColumnOffset(fact_schema, lhs);
    auto dc = LocalColumnOffset(dim_schema, rhs);
    if (!fc || !dc) {
      fc = LocalColumnOffset(fact_schema, rhs);
      dc = LocalColumnOffset(dim_schema, lhs);
    }
    if (!fc || !dc ||
        (dim->fact_column &&
         (dim->fact_column != fc || dim->key_column != *dc))) {
      return std::optional<Relation>();
    }
    const auto& fcol = fact_schema.GetColumn(*fc);
    const auto& dcol = dim_schema.GetColumn(*dc);
    if ((fcol.Type() != ValueType::kInt64 && fcol.Type() != ValueType::kDate) ||
        fcol.Type() != dcol.Type() || fcol.IsUnsigned() || dcol.IsUnsigned()) {
      return std::optional<Relation>();
    }
    dim->fact_column = fc;
    dim->key_column = *dc;
  }
  auto charge = TryReserveQueryMemory((build_rows * 64) + 1024);
  if (!charge) {
    return std::optional<Relation>();
  }
  const auto read_star_key_col = [](const ColumnarColumn* col, size_t r,
                                    int64_t* out_kv) noexcept -> bool {
    if (!col->is_uniform) {
      if (col->IsFlat()) {
        const Value& fv = col->fallback[r];
        if (fv.IsNull()) {
          return false;
        }
        *out_kv = fv.value.int_value;
        return true;
      }
      Value fv;
      col->WriteValueInto(r, &fv);
      if (fv.IsNull()) {
        return false;
      }
      *out_kv = fv.value.int_value;
      return true;
    }
    if (col->type == ValueType::kNull) {
      return false;
    }
    switch (col->encoding) {
      case ColumnarColumn::Encoding::kFlat:
        if (col->has_null && col->nulls[r] != 0) {
          return false;
        }
        *out_kv = col->ints[r];
        return true;
      case ColumnarColumn::Encoding::kConstant:
        if (col->const_is_null) {
          return false;
        }
        *out_kv = col->const_int;
        return true;
      case ColumnarColumn::Encoding::kDictionary: {
        const uint32_t code = col->DictCodeAt(r);
        if (col->has_null && (code == ColumnarColumn::kNullDictCode ||
                              code >= col->DictEntryCount())) {
          return false;
        }
        *out_kv = col->dict->IntEntryAt(code);
        return true;
      }
      case ColumnarColumn::Encoding::kFrameOfReference:
        if (col->has_null && col->borrowed_nulls != nullptr &&
            col->borrowed_nulls[r] != 0) {
          return false;
        }
        *out_kv = col->ForIntAt(r);
        return true;
      case ColumnarColumn::Encoding::kGathered: {
        const ColumnarColumn* leaf = nullptr;
        size_t lr = 0;
        if (!col->ResolveLeaf(r, &leaf, &lr)) {
          return false;
        }
        *out_kv = leaf->IntAt(lr);
        return true;
      }
    }
    return false;
  };
  for (Dimension& dim : dimensions) {
    if (!dim.fact_column) {
      return std::optional<Relation>();
    }
    Relation& dim_rel = relations[dim.relation];
    if (!columnar_output && dim_rel.HasColumnarOnly()) {
      RETURN_IF_FAIL(dim_rel.EnsureRowsMaterialized());
    }
    const auto type = dim_rel.schema.GetColumn(dim.key_column).Type();
    dim.key_type = type;
    if (dim_rel.HasColumnarOnly()) {
      const ColumnarTable& dtable = *dim_rel.columnar;
      dim.keys.Reserve(dtable.total_rows);
      bool any_key = false;
      int64_t min_key = 0;
      int64_t max_key = 0;
      size_t global_r = 0;
      for (const ColumnarChunk& dc : dtable.chunks) {
        if (dc.rows == 0) {
          continue;
        }
        const ColumnarColumn& kcol = dc.columns[dim.key_column];
        if (kcol.is_uniform &&
            (kcol.is_unsigned ||
             (kcol.type != type && kcol.type != ValueType::kNull))) {
          return std::optional<Relation>();
        }
        if (kcol.is_uniform &&
            (kcol.type == ValueType::kNull ||
             (kcol.encoding == ColumnarColumn::Encoding::kConstant &&
              kcol.const_is_null))) {
          global_r += dc.rows;
          continue;
        }
        for (size_t r = 0; r < dc.rows; ++r, ++global_r) {
          int64_t kv = 0;
          bool is_null = false;
          if (!kcol.is_uniform) {
            Value v;
            if (kcol.IsFlat()) {
              v = kcol.fallback[r];
            } else {
              kcol.WriteValueInto(r, &v);
            }
            if (v.IsNull()) {
              is_null = true;
            } else if (v.type != type || v.IsUnsigned()) {
              return std::optional<Relation>();
            } else {
              kv = v.value.int_value;
            }
          } else if (kcol.encoding == ColumnarColumn::Encoding::kGathered) {
            const ColumnarColumn* leaf = nullptr;
            size_t lr = 0;
            if (!kcol.ResolveLeaf(r, &leaf, &lr)) {
              is_null = true;
            } else if (leaf->is_unsigned || leaf->type != type) {
              return std::optional<Relation>();
            } else {
              kv = leaf->IntAt(lr);
            }
          } else {
            is_null = !read_star_key_col(&kcol, r, &kv);
          }
          if (is_null) {
            continue;
          }
          bool inserted = false;
          uint32_t* slot = dim.keys.TryEmplace(kv, &inserted);
          if (!inserted) {
            return std::optional<Relation>();
          }
          *slot = static_cast<uint32_t>(global_r);
          if (!any_key) {
            any_key = true;
            min_key = kv;
            max_key = kv;
          } else {
            min_key = std::min(min_key, kv);
            max_key = std::max(max_key, kv);
          }
        }
      }
      if (any_key) {
        const uint64_t span =
            static_cast<uint64_t>(max_key) - static_cast<uint64_t>(min_key);
        if (span <= 262144ULL) {
          dim.dense_lo = min_key;
          dim.dense_rows.assign(span + 1, kStarJoinMiss);
          dim.keys.ForEach([&](int64_t kv, uint32_t slot) {
            const uint64_t off =
                static_cast<uint64_t>(kv) - static_cast<uint64_t>(min_key);
            dim.dense_rows[off] = slot;
          });
        }
      }
      continue;
    }
    const auto& rows = dim_rel.rows;
    dim.keys.Reserve(rows.size());
    bool any_key = false;
    int64_t min_key = 0;
    int64_t max_key = 0;
    for (size_t r = 0; r < rows.size(); ++r) {
      const Value& key = rows[r][dim.key_column];
      if (key.IsNull()) {
        continue;
      }
      if (key.type != type || key.IsUnsigned()) {
        return std::optional<Relation>();
      }
      const int64_t kv = key.value.int_value;
      bool inserted = false;
      uint32_t* slot = dim.keys.TryEmplace(kv, &inserted);
      if (!inserted) {
        return std::optional<Relation>();
      }
      *slot = static_cast<uint32_t>(r);
      if (!any_key) {
        any_key = true;
        min_key = kv;
        max_key = kv;
      } else {
        min_key = std::min(min_key, kv);
        max_key = std::max(max_key, kv);
      }
    }
    if (any_key) {
      const uint64_t span =
          static_cast<uint64_t>(max_key) - static_cast<uint64_t>(min_key);
      if (span <= 262144ULL) {
        dim.dense_lo = min_key;
        dim.dense_rows.assign(span + 1, kStarJoinMiss);
        for (size_t r = 0; r < rows.size(); ++r) {
          const Value& key = rows[r][dim.key_column];
          if (!key.IsNull()) {
            const uint64_t off = static_cast<uint64_t>(key.value.int_value) -
                                 static_cast<uint64_t>(min_key);
            dim.dense_rows[off] = static_cast<uint32_t>(r);
          }
        }
      }
    }
  }
  if (!columnar_output && relations[fact].HasColumnarOnly()) {
    RETURN_IF_FAIL(relations[fact].EnsureRowsMaterialized());
  }
  const bool fact_is_columnar = relations[fact].HasColumnarOnly();
  const size_t total = relations[fact].TotalRows();
  const size_t workers =
      total < 16384
          ? 1
          : std::min<size_t>({8, (total / 16384) + 1,
                              QueryParallelWorkerLimit(
                                  std::thread::hardware_concurrency())});
  if (fact_is_columnar) {
    for (const ColumnarChunk& fc : relations[fact].columnar->chunks) {
      if (fc.rows == 0) {
        continue;
      }
      for (const Dimension& dim : dimensions) {
        const ColumnarColumn& fcol = fc.columns[*dim.fact_column];
        if (fcol.is_uniform) {
          if (fcol.is_unsigned ||
              (fcol.type != dim.key_type && fcol.type != ValueType::kNull)) {
            return std::optional<Relation>();
          }
        } else if (fcol.IsFlat()) {
          for (const Value& kv : fcol.fallback) {
            if (!kv.IsNull() && (kv.type != dim.key_type || kv.IsUnsigned())) {
              return std::optional<Relation>();
            }
          }
        } else {
          Value kv;
          for (size_t r = 0; r < fc.rows; ++r) {
            fcol.WriteValueInto(r, &kv);
            if (!kv.IsNull() && (kv.type != dim.key_type || kv.IsUnsigned())) {
              return std::optional<Relation>();
            }
          }
        }
      }
    }
  } else {
    const auto& fact_rows = relations[fact].rows;
    if (workers == 1) {
      for (const Dimension& dim : dimensions) {
        const size_t fact_column = *dim.fact_column;
        const ValueType expected = dim.key_type;
        for (const Row& row : fact_rows) {
          const Value& key = row[fact_column];
          if (!key.IsNull() && (key.type != expected || key.IsUnsigned())) {
            return std::optional<Relation>();
          }
        }
      }
    } else {
      std::atomic<bool> fact_types_ok{true};
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
        const size_t chunk = total / workers;
        const size_t begin = worker * chunk;
        const size_t end = worker + 1 == workers ? total : begin + chunk;
        for (size_t ri = begin; ri < end; ++ri) {
          if (!fact_types_ok.load(std::memory_order_relaxed)) {
            return;
          }
          const Row& row = fact_rows[ri];
          for (const Dimension& dim : dimensions) {
            const Value& key = row[*dim.fact_column];
            if (!key.IsNull() &&
                (key.type != dim.key_type || key.IsUnsigned())) {
              fact_types_ok.store(false, std::memory_order_relaxed);
              return;
            }
          }
        }
      }));
      if (!fact_types_ok.load(std::memory_order_relaxed)) {
        return std::optional<Relation>();
      }
    }
  }
  Relation result(relations[fact].runtime());
  result.schema = relations[fact].schema;
  CopyExecutionStats(&result, relations[fact]);
  for (const Dimension& dim : dimensions) {
    const Relation& input = relations[dim.relation];
    result.schema = result.schema + input.schema;
    result.hash_joins += input.hash_joins + 1;
    result.in_memory_hash_joins += input.in_memory_hash_joins + 1;
    result.hybrid_hash_joins += input.hybrid_hash_joins;
    result.nested_loop_joins += input.nested_loop_joins;
    result.join_comparisons += input.join_comparisons;
    result.peak_intermediate_rows =
        std::max(result.peak_intermediate_rows, input.peak_intermediate_rows);
  }
  // Every local predicate and join edge has been applied by this point.
  // Materialize only columns needed above the join; retaining join keys and
  // filter-only dimension payloads needlessly widens every aggregate input.
  SelectStatement future(statement);
  future.SetWhereClause(nullptr);
  future.SetSources({});
  const auto output_columns = RequiredColumns(future, result.schema);
  const bool project_output =
      !statement.HasDistinctOn() && !statement.ForUpdate() &&
      output_columns.size() != result.schema.ColumnCount();
  std::vector<size_t> fact_output;
  fact_output.reserve(output_columns.size());
  std::vector<std::vector<size_t>> dimension_output(dimensions.size());
  if (project_output) {
    const size_t fact_width = relations[fact].schema.ColumnCount();
    for (const size_t column : output_columns) {
      if (column < fact_width) {
        fact_output.push_back(column);
        continue;
      }
      size_t base = fact_width;
      for (size_t d = 0; d < dimensions.size(); ++d) {
        const size_t width =
            relations[dimensions[d].relation].schema.ColumnCount();
        if (column < base + width) {
          dimension_output[d].push_back(column - base);
          break;
        }
        base += width;
      }
    }
    result.schema = ProjectSchema(result.schema, output_columns);
  }
  std::vector<size_t> eff_fact_output;
  if (project_output) {
    eff_fact_output = fact_output;
  } else {
    eff_fact_output.resize(relations[fact].schema.ColumnCount());
    for (size_t c = 0; c < eff_fact_output.size(); ++c) {
      eff_fact_output[c] = c;
    }
  }
  std::vector<std::vector<size_t>> eff_dim_output(dimensions.size());
  for (size_t d = 0; d < dimensions.size(); ++d) {
    if (project_output) {
      eff_dim_output[d] = dimension_output[d];
    } else {
      const size_t width =
          relations[dimensions[d].relation].schema.ColumnCount();
      eff_dim_output[d].resize(width);
      for (size_t c = 0; c < width; ++c) {
        eff_dim_output[d][c] = c;
      }
    }
  }
  const auto lookup_dim = [&](const Dimension& dim, int64_t kv,
                              uint32_t* out_slot) noexcept -> bool {
    if (!dim.dense_rows.empty()) {
      const uint64_t off =
          static_cast<uint64_t>(kv) - static_cast<uint64_t>(dim.dense_lo);
      if (off >= dim.dense_rows.size()) {
        return false;
      }
      const uint32_t s = dim.dense_rows[off];
      if (s == kStarJoinMiss) {
        return false;
      }
      *out_slot = s;
      return true;
    }
    const uint32_t* found = dim.keys.Find(kv);
    if (found == nullptr) {
      return false;
    }
    *out_slot = *found;
    return true;
  };
  const bool emit_columnar = columnar_output && result.schema.ColumnCount() > 0;
  if (emit_columnar) {
    struct DimColumnarSource {
      std::shared_ptr<const ColumnarTable> table;
      std::vector<size_t> col_indices;
      std::vector<ValueType> col_types;
      std::vector<uint8_t> col_unsigned;
      std::vector<uint8_t> col_interval;
      std::vector<uint8_t> col_uniform;
      std::vector<uint8_t> col_has_null;
      std::vector<uint32_t> slot_chunks;
      std::vector<uint32_t> slot_rows;
      bool single_chunk{true};
    };
    std::shared_ptr<ChunkStringArena> dim_string_arena;
    std::vector<DimColumnarSource> dim_sources(dimensions.size());
    for (size_t d = 0; d < dimensions.size(); ++d) {
      if (eff_dim_output[d].empty()) {
        continue;
      }
      DimColumnarSource& src = dim_sources[d];
      const Relation& dim_rel = relations[dimensions[d].relation];
      const size_t nout = eff_dim_output[d].size();
      src.col_indices.resize(nout);
      src.col_types.resize(nout);
      src.col_unsigned.resize(nout);
      src.col_interval.resize(nout);
      src.col_uniform.assign(nout, 1);
      src.col_has_null.assign(nout, 0);
      if (dim_rel.HasColumnarOnly()) {
        src.table = dim_rel.columnar;
        const auto& dchunks = src.table->chunks;
        const size_t drows = dim_rel.TotalRows();
        src.single_chunk = dchunks.size() <= 1;
        if (!src.single_chunk) {
          src.slot_chunks.resize(drows);
          src.slot_rows.resize(drows);
          size_t g = 0;
          for (size_t ci = 0; ci < dchunks.size(); ++ci) {
            const auto cui = static_cast<uint32_t>(ci);
            const size_t nrows = dchunks[ci].rows;
            for (size_t r = 0; r < nrows; ++r, ++g) {
              src.slot_chunks[g] = cui;
              src.slot_rows[g] = static_cast<uint32_t>(r);
            }
          }
        }
        for (size_t oi = 0; oi < nout; ++oi) {
          const size_t src_c = eff_dim_output[d][oi];
          const Column& sch_col = dim_rel.schema.GetColumn(src_c);
          src.col_indices[oi] = src_c;
          ValueType vt = sch_col.Type();
          bool uns = sch_col.IsUnsigned();
          bool ivl = sch_col.IsInterval();
          bool uni = true;
          bool has_null = false;
          ValueType seen_vt = ValueType::kNull;
          for (const ColumnarChunk& dc : dchunks) {
            if (dc.rows == 0) {
              continue;
            }
            const ColumnarColumn& c = dc.columns[src_c];
            if (!c.is_uniform) {
              uni = false;
              has_null = true;
            } else if (c.type == ValueType::kNull ||
                       (c.encoding == ColumnarColumn::Encoding::kConstant &&
                        c.const_is_null)) {
              has_null = true;
              if (c.type != ValueType::kNull && seen_vt == ValueType::kNull) {
                seen_vt = c.type;
                uns = c.is_unsigned;
                ivl = c.is_interval;
              }
            } else {
              if (seen_vt == ValueType::kNull) {
                seen_vt = c.type;
                uns = c.is_unsigned;
                ivl = c.is_interval;
              } else if (seen_vt != c.type) {
                uni = false;
              }
              if (c.has_null) {
                has_null = true;
              }
            }
          }
          if (vt == ValueType::kNull && seen_vt != ValueType::kNull && uni) {
            vt = seen_vt;
          }
          src.col_types[oi] = vt;
          src.col_unsigned[oi] = uns ? 1 : 0;
          src.col_interval[oi] = ivl ? 1 : 0;
          src.col_uniform[oi] = uni ? 1 : 0;
          src.col_has_null[oi] = has_null ? 1 : 0;
        }
      } else {
        const size_t drows = dim_rel.rows.size();
        bool need_arena = !dim_rel.uniform_cell_types;
        for (size_t oi = 0; oi < nout && !need_arena; ++oi) {
          const ValueType vt =
              dim_rel.schema.GetColumn(eff_dim_output[d][oi]).Type();
          if (vt == ValueType::kVarChar || vt == ValueType::kNull) {
            need_arena = true;
          }
        }
        ChunkStringArena* arena = nullptr;
        if (need_arena) {
          if (dim_string_arena == nullptr) {
            dim_string_arena = std::make_shared<ChunkStringArena>();
          }
          arena = dim_string_arena.get();
        }
        ColumnarChunk dim_chunk;
        dim_chunk.columns.resize(nout);
        if (dim_string_arena != nullptr && need_arena) {
          dim_chunk.keepalives.push_back(dim_string_arena);
        }
        for (size_t oi = 0; oi < nout; ++oi) {
          const size_t src_c = eff_dim_output[d][oi];
          const Column& sch_col = dim_rel.schema.GetColumn(src_c);
          ColumnarColumn& cc = dim_chunk.columns[oi];
          cc.Init(sch_col.Type(), sch_col.IsUnsigned(), sch_col.IsInterval(),
                  drows);
          for (size_t r = 0; r < drows; ++r) {
            cc.AppendValue(dim_rel.rows[r][src_c], arena, r);
          }
          src.col_indices[oi] = oi;
          src.col_types[oi] = cc.type;
          src.col_unsigned[oi] = cc.is_unsigned ? 1 : 0;
          src.col_interval[oi] = cc.is_interval ? 1 : 0;
          src.col_uniform[oi] = cc.is_uniform ? 1 : 0;
          src.col_has_null[oi] =
              (cc.has_null || !cc.is_uniform || cc.type == ValueType::kNull)
                  ? 1
                  : 0;
        }
        dim_chunk.rows = drows;
        auto dim_tbl = std::make_shared<ColumnarTable>();
        if (drows > 0) {
          dim_tbl->AppendChunk(std::move(dim_chunk));
        }
        src.table = std::move(dim_tbl);
        src.single_chunk = true;
      }
    }
    bool fact_may_have_strings = false;
    for (const size_t fc_idx : eff_fact_output) {
      const ValueType vt = relations[fact].schema.GetColumn(fc_idx).Type();
      if (vt == ValueType::kVarChar || vt == ValueType::kNull) {
        fact_may_have_strings = true;
        break;
      }
    }
    if (fact_is_columnar) {
      const std::shared_ptr<const ColumnarTable>& fact_table =
          relations[fact].columnar;
      const auto& fact_chunks = fact_table->chunks;
      if (!fact_may_have_strings && !relations[fact].uniform_cell_types) {
        fact_may_have_strings = true;
      }
      const size_t ndims = dimensions.size();
      const auto probe_chunks = [&](ColumnarTable& out_table, size_t& out_cmp,
                                    bool& out_uniform, size_t c_begin,
                                    size_t c_end) {
        ColumnarChunk cur_chunk;
        bool cur_init = false;
        std::vector<std::shared_ptr<ChunkGatherIndex>> cur_fact_gis;
        cur_fact_gis.reserve(eff_fact_output.size());
        std::vector<const ChunkGatherIndex*> cur_group_src_gi;
        cur_group_src_gi.reserve(eff_fact_output.size());
        std::vector<size_t> cur_fact_group_id;
        std::vector<const ColumnarTable*> cur_fact_target_table;
        std::vector<size_t> cur_fact_target_col;
        std::vector<uint8_t> cur_fact_was_gathered;
        std::vector<size_t> cur_fact_group_repr;
        cur_fact_group_repr.reserve(eff_fact_output.size());
        std::array<std::shared_ptr<ChunkGatherIndex>, 8> cur_dim_gis{};
        std::vector<uint32_t> sel;
        sel.reserve(kColumnarChunkRows);
        std::array<std::vector<uint32_t>, 8> match_rows;
        for (size_t d = 0; d < ndims; ++d) {
          if (!eff_dim_output[d].empty()) {
            match_rows[d].reserve(kColumnarChunkRows);
          }
        }
        const auto flush_cur_chunk = [&]() {
          if (!cur_init || cur_chunk.rows == 0) {
            return;
          }
          for (ColumnarColumn& col : cur_chunk.columns) {
            if (col.encoding == ColumnarColumn::Encoding::kGathered &&
                col.gather_index != nullptr) {
              col.has_null |= col.gather_index->has_null;
            }
            if (!col.is_uniform) {
              out_uniform = false;
            }
          }
          out_table.AppendChunk(std::move(cur_chunk));
          cur_chunk = ColumnarChunk{};
          cur_fact_gis.clear();
          cur_group_src_gi.clear();
          cur_fact_group_id.clear();
          cur_fact_target_table.clear();
          cur_fact_target_col.clear();
          cur_fact_was_gathered.clear();
          cur_fact_group_repr.clear();
          cur_dim_gis.fill(nullptr);
          cur_init = false;
        };
        std::vector<const ColumnarTable*> ci_target_table(
            eff_fact_output.size());
        std::vector<std::shared_ptr<const ColumnarTable>> ci_target_owner(
            eff_fact_output.size());
        std::vector<size_t> ci_target_col(eff_fact_output.size());
        std::vector<const ChunkGatherIndex*> ci_src_gi(eff_fact_output.size());
        for (size_t ci = c_begin; ci < c_end; ++ci) {
          const ColumnarChunk& fc = fact_chunks[ci];
          if (fc.rows == 0) {
            continue;
          }
          bool any_dim_miss = false;
          bool all_flat_uniform = true;
          std::array<bool, 8> dim_is_const{};
          std::array<uint32_t, 8> const_match{};
          std::array<const int64_t*, 8> ints_ptrs{};
          std::array<const uint8_t*, 8> nulls_ptrs{};
          std::array<const ColumnarColumn*, 8> fcols{};
          for (size_t d = 0; d < ndims; ++d) {
            const ColumnarColumn* col = &fc.columns[*dimensions[d].fact_column];
            fcols[d] = col;
            if (col->is_uniform &&
                (col->type == ValueType::kNull ||
                 (col->encoding == ColumnarColumn::Encoding::kConstant &&
                  col->const_is_null))) {
              any_dim_miss = true;
              break;
            }
            if (col->is_uniform &&
                col->encoding == ColumnarColumn::Encoding::kConstant) {
              dim_is_const[d] = true;
              if (!lookup_dim(dimensions[d], col->const_int, &const_match[d])) {
                any_dim_miss = true;
                break;
              }
              continue;
            }
            if (!col->IsFlat() || !col->is_uniform) {
              all_flat_uniform = false;
            } else {
              ints_ptrs[d] = col->ints.data();
              nulls_ptrs[d] = col->has_null ? col->nulls.data() : nullptr;
            }
          }
          if (any_dim_miss) {
            out_cmp += fc.rows;
            continue;
          }
          sel.clear();
          for (size_t d = 0; d < ndims; ++d) {
            if (!eff_dim_output[d].empty()) {
              match_rows[d].clear();
            }
          }
          std::array<uint32_t, 8> tmp_match{};
          if (all_flat_uniform) {
            for (size_t r = 0; r < fc.rows; ++r) {
              bool matched = true;
              for (size_t d = 0; d < ndims; ++d) {
                ++out_cmp;
                if (dim_is_const[d]) {
                  tmp_match[d] = const_match[d];
                  continue;
                }
                const uint8_t* nptr = nulls_ptrs[d];
                if ((nptr != nullptr && nptr[r] != 0) ||
                    !lookup_dim(dimensions[d], ints_ptrs[d][r],
                                &tmp_match[d])) {
                  matched = false;
                  break;
                }
              }
              if (matched) {
                sel.push_back(static_cast<uint32_t>(r));
                for (size_t d = 0; d < ndims; ++d) {
                  if (!eff_dim_output[d].empty()) {
                    match_rows[d].push_back(tmp_match[d]);
                  }
                }
              }
            }
          } else {
            for (size_t r = 0; r < fc.rows; ++r) {
              bool matched = true;
              for (size_t d = 0; d < ndims; ++d) {
                ++out_cmp;
                if (dim_is_const[d]) {
                  tmp_match[d] = const_match[d];
                  continue;
                }
                int64_t kv = 0;
                if (!read_star_key_col(fcols[d], r, &kv) ||
                    !lookup_dim(dimensions[d], kv, &tmp_match[d])) {
                  matched = false;
                  break;
                }
              }
              if (matched) {
                sel.push_back(static_cast<uint32_t>(r));
                for (size_t d = 0; d < ndims; ++d) {
                  if (!eff_dim_output[d].empty()) {
                    match_rows[d].push_back(tmp_match[d]);
                  }
                }
              }
            }
          }
          const size_t hit = sel.size();
          if (hit == 0) {
            continue;
          }
          for (size_t fi = 0; fi < eff_fact_output.size(); ++fi) {
            const size_t fc_idx = eff_fact_output[fi];
            const ColumnarColumn& fcol = fc.columns[fc_idx];
            if (fcol.encoding == ColumnarColumn::Encoding::kGathered &&
                fcol.gather_index != nullptr &&
                fcol.gather_index->source_table != nullptr) {
              ci_target_owner[fi] = fcol.gather_index->source_table;
              ci_target_table[fi] = ci_target_owner[fi].get();
              ci_target_col[fi] = fcol.gather_col;
              ci_src_gi[fi] = fcol.gather_index.get();
            } else {
              ci_target_owner[fi] = fact_table;
              ci_target_table[fi] = fact_table.get();
              ci_target_col[fi] = fc_idx;
              ci_src_gi[fi] = nullptr;
            }
          }
          if (cur_init) {
            bool compat = cur_chunk.rows + hit <= kColumnarChunkRows;
            for (size_t fi = 0; compat && fi < eff_fact_output.size(); ++fi) {
              const bool was_g = ci_src_gi[fi] != nullptr;
              const size_t gid = cur_fact_group_id[fi];
              const size_t repr = cur_fact_group_repr[gid];
              if (cur_fact_target_table[fi] != ci_target_table[fi] ||
                  cur_fact_target_col[fi] != ci_target_col[fi] ||
                  (cur_fact_was_gathered[fi] != 0) != was_g ||
                  ci_src_gi[fi] != ci_src_gi[repr]) {
                compat = false;
              }
            }
            if (!compat) {
              flush_cur_chunk();
            }
          }
          if (!cur_init) {
            cur_chunk.rows = 0;
            cur_chunk.columns.resize(result.schema.ColumnCount());
            cur_fact_group_id.resize(eff_fact_output.size());
            cur_fact_target_table.resize(eff_fact_output.size());
            cur_fact_target_col.resize(eff_fact_output.size());
            cur_fact_was_gathered.resize(eff_fact_output.size());
            for (size_t fi = 0; fi < eff_fact_output.size(); ++fi) {
              const size_t fc_idx = eff_fact_output[fi];
              const ColumnarColumn& fcol = fc.columns[fc_idx];
              const ChunkGatherIndex* sgi = ci_src_gi[fi];
              size_t gid = cur_fact_gis.size();
              for (size_t g = 0; g < cur_fact_gis.size(); ++g) {
                if (cur_group_src_gi[g] == sgi) {
                  gid = g;
                  break;
                }
              }
              if (gid == cur_fact_gis.size()) {
                auto gi = std::make_shared<ChunkGatherIndex>();
                gi->source_table = ci_target_owner[fi];
                cur_fact_gis.push_back(std::move(gi));
                cur_group_src_gi.push_back(sgi);
                cur_fact_group_repr.push_back(fi);
              }
              cur_fact_group_id[fi] = gid;
              cur_fact_target_table[fi] = ci_target_table[fi];
              cur_fact_target_col[fi] = ci_target_col[fi];
              cur_fact_was_gathered[fi] = sgi != nullptr ? 1 : 0;
              const ValueType fvt = fcol.type != ValueType::kNull
                                        ? fcol.type
                                        : result.schema.GetColumn(fi).Type();
              cur_chunk.columns[fi].SetGathered(
                  cur_fact_gis[gid], ci_target_col[fi], fvt, fcol.is_unsigned,
                  fcol.is_interval, fcol.is_uniform,
                  fcol.has_null || fcol.type == ValueType::kNull);
            }
            size_t out_c = eff_fact_output.size();
            for (size_t d = 0; d < ndims; ++d) {
              if (eff_dim_output[d].empty()) {
                continue;
              }
              auto dgi = std::make_shared<ChunkGatherIndex>();
              dgi->source_table = dim_sources[d].table;
              cur_dim_gis[d] = dgi;
              for (size_t oi = 0; oi < eff_dim_output[d].size(); ++oi) {
                cur_chunk.columns[out_c].SetGathered(
                    dgi, dim_sources[d].col_indices[oi],
                    dim_sources[d].col_types[oi],
                    dim_sources[d].col_unsigned[oi] != 0,
                    dim_sources[d].col_interval[oi] != 0,
                    dim_sources[d].col_uniform[oi] != 0,
                    dim_sources[d].col_has_null[oi] != 0);
                ++out_c;
              }
            }
            cur_init = true;
          } else {
            for (size_t fi = 0; fi < eff_fact_output.size(); ++fi) {
              const ColumnarColumn& fcol = fc.columns[eff_fact_output[fi]];
              ColumnarColumn& out_col = cur_chunk.columns[fi];
              if (out_col.type == ValueType::kNull &&
                  fcol.type != ValueType::kNull && fcol.is_uniform) {
                out_col.type = fcol.type;
                out_col.is_unsigned = fcol.is_unsigned;
                out_col.is_interval = fcol.is_interval;
              }
              if (!fcol.is_uniform ||
                  (out_col.type != fcol.type && fcol.type != ValueType::kNull &&
                   out_col.type != ValueType::kNull)) {
                out_col.is_uniform = false;
              }
              out_col.has_null |=
                  fcol.has_null || (fcol.type == ValueType::kNull);
            }
          }
          for (size_t g = 0; g < cur_fact_gis.size(); ++g) {
            ChunkGatherIndex& dst_gi = *cur_fact_gis[g];
            const ChunkGatherIndex* src_gi = ci_src_gi[cur_fact_group_repr[g]];
            const size_t old_sz = dst_gi.src_rows.size();
            dst_gi.src_chunks.resize(old_sz + hit);
            dst_gi.src_rows.resize(old_sz + hit);
            if (src_gi == nullptr) {
              std::fill_n(dst_gi.src_chunks.data() + old_sz, hit,
                          static_cast<uint32_t>(ci));
              std::memcpy(dst_gi.src_rows.data() + old_sz, sel.data(),
                          hit * sizeof(uint32_t));
            } else {
              const uint32_t* sc_ptr = src_gi->src_chunks.data();
              const uint32_t* sr_ptr = src_gi->src_rows.data();
              uint32_t* out_sc = dst_gi.src_chunks.data() + old_sz;
              uint32_t* out_sr = dst_gi.src_rows.data() + old_sz;
              for (size_t k = 0; k < hit; ++k) {
                const uint32_t r = sel[k];
                out_sc[k] = sc_ptr[r];
                out_sr[k] = sr_ptr[r];
              }
              if (src_gi->has_null) {
                for (size_t k = 0; k < hit; ++k) {
                  if (out_sr[k] == ChunkGatherIndex::kNullRow) {
                    dst_gi.has_null = true;
                    break;
                  }
                }
              }
            }
          }
          for (size_t d = 0; d < ndims; ++d) {
            if (eff_dim_output[d].empty()) {
              continue;
            }
            ChunkGatherIndex& dgi = *cur_dim_gis[d];
            const size_t old_sz = dgi.src_rows.size();
            dgi.src_chunks.resize(old_sz + hit);
            dgi.src_rows.resize(old_sz + hit);
            const uint32_t* m_ptr = match_rows[d].data();
            if (dim_sources[d].single_chunk) {
              std::fill_n(dgi.src_chunks.data() + old_sz, hit, uint32_t{0});
              std::memcpy(dgi.src_rows.data() + old_sz, m_ptr,
                          hit * sizeof(uint32_t));
            } else {
              const uint32_t* sc_map = dim_sources[d].slot_chunks.data();
              const uint32_t* sr_map = dim_sources[d].slot_rows.data();
              uint32_t* out_sc = dgi.src_chunks.data() + old_sz;
              uint32_t* out_sr = dgi.src_rows.data() + old_sz;
              for (size_t k = 0; k < hit; ++k) {
                const uint32_t slot = m_ptr[k];
                out_sc[k] = sc_map[slot];
                out_sr[k] = sr_map[slot];
              }
            }
          }
          cur_chunk.rows += hit;
        }
        flush_cur_chunk();
      };
      auto merged = std::make_shared<ColumnarTable>();
      bool all_uniform = true;
      if (workers == 1 || fact_chunks.size() <= 1) {
        size_t cmp = 0;
        probe_chunks(*merged, cmp, all_uniform, 0, fact_chunks.size());
        result.join_comparisons += cmp;
      } else {
        const size_t actual_workers = std::min(workers, fact_chunks.size());
        std::vector<size_t> chunk_splits(actual_workers + 1,
                                         fact_chunks.size());
        chunk_splits[0] = 0;
        size_t accum = 0;
        size_t next_w = 1;
        for (size_t ci = 0; ci < fact_chunks.size() && next_w < actual_workers;
             ++ci) {
          accum += fact_chunks[ci].rows;
          const size_t target_rows = (total * next_w) / actual_workers;
          if (accum >= target_rows) {
            chunk_splits[next_w] = ci + 1;
            ++next_w;
          }
        }
        std::vector<ColumnarTable> shard_tables(actual_workers);
        std::vector<size_t> shard_cmp(actual_workers, 0);
        std::vector<uint8_t> shard_uniform(actual_workers, 1);
        RETURN_IF_FAIL(RunParallelTasks(actual_workers, [&](size_t worker) {
          bool u = true;
          probe_chunks(shard_tables[worker], shard_cmp[worker], u,
                       chunk_splits[worker], chunk_splits[worker + 1]);
          shard_uniform[worker] = u ? 1 : 0;
        }));
        for (size_t w = 0; w < actual_workers; ++w) {
          result.join_comparisons += shard_cmp[w];
          all_uniform &= (shard_uniform[w] != 0);
          merged->AppendTable(std::move(shard_tables[w]));
        }
      }
      if (merged->total_rows > 0) {
        for (size_t c = 0; c < result.schema.ColumnCount(); ++c) {
          if (result.schema.GetColumn(c).Type() == ValueType::kNull) {
            for (const ColumnarChunk& chunk : merged->chunks) {
              const ColumnarColumn& col = chunk.columns[c];
              if (col.is_uniform && col.type != ValueType::kNull) {
                Column updated = result.schema.GetColumn(c);
                updated.SetType(col.type);
                updated.SetUnsigned(col.is_unsigned);
                updated.SetInterval(col.is_interval);
                std::vector<Column> cols;
                cols.reserve(result.schema.ColumnCount());
                for (size_t k = 0; k < result.schema.ColumnCount(); ++k) {
                  cols.push_back(k == c ? updated : result.schema.GetColumn(k));
                }
                result.schema = Schema(result.schema.Name(), std::move(cols));
                break;
              }
            }
          }
        }
        result.SetColumnarTable(std::move(merged), all_uniform);
      }
      result.peak_intermediate_rows =
          std::max(result.peak_intermediate_rows, result.TotalRows());
      return std::optional<Relation>(std::move(result));
    }
    const size_t ndims = dimensions.size();
    const auto probe_rows_to_columnar = [&](ColumnarTable& out_table,
                                            size_t& out_cmp, bool& out_uniform,
                                            size_t begin, size_t end) {
      std::vector<size_t> matched_fact;
      matched_fact.reserve(kColumnarChunkRows);
      std::array<std::vector<uint32_t>, 8> match_rows;
      for (size_t d = 0; d < ndims; ++d) {
        if (!eff_dim_output[d].empty()) {
          match_rows[d].reserve(kColumnarChunkRows);
        }
      }
      const auto flush_batch = [&]() {
        const size_t hit = matched_fact.size();
        if (hit == 0) {
          return;
        }
        ColumnarChunk chunk;
        chunk.rows = 0;
        chunk.columns.resize(result.schema.ColumnCount());
        ChunkStringArena* fact_arena =
            fact_may_have_strings ? chunk.EnsureArena() : nullptr;
        size_t out_c = 0;
        for (const size_t fc_idx : eff_fact_output) {
          const Column& sch_col = relations[fact].schema.GetColumn(fc_idx);
          ColumnarColumn& dst = chunk.columns[out_c];
          dst.Init(sch_col.Type(), sch_col.IsUnsigned(), sch_col.IsInterval(),
                   hit);
          for (size_t i = 0; i < hit; ++i) {
            dst.AppendValue(relations[fact].rows[matched_fact[i]][fc_idx],
                            fact_arena, i);
          }
          ++out_c;
        }
        for (size_t d = 0; d < ndims; ++d) {
          if (eff_dim_output[d].empty()) {
            continue;
          }
          auto dgi = std::make_shared<ChunkGatherIndex>();
          dgi->source_table = dim_sources[d].table;
          dgi->src_chunks.resize(hit);
          dgi->src_rows.resize(hit);
          const uint32_t* m_ptr = match_rows[d].data();
          if (dim_sources[d].single_chunk) {
            std::fill_n(dgi->src_chunks.data(), hit, uint32_t{0});
            std::memcpy(dgi->src_rows.data(), m_ptr, hit * sizeof(uint32_t));
          } else {
            const uint32_t* sc_map = dim_sources[d].slot_chunks.data();
            const uint32_t* sr_map = dim_sources[d].slot_rows.data();
            for (size_t k = 0; k < hit; ++k) {
              const uint32_t slot = m_ptr[k];
              dgi->src_chunks[k] = sc_map[slot];
              dgi->src_rows[k] = sr_map[slot];
            }
          }
          for (size_t oi = 0; oi < eff_dim_output[d].size(); ++oi) {
            chunk.columns[out_c].SetGathered(
                dgi, dim_sources[d].col_indices[oi],
                dim_sources[d].col_types[oi],
                dim_sources[d].col_unsigned[oi] != 0,
                dim_sources[d].col_interval[oi] != 0,
                dim_sources[d].col_uniform[oi] != 0,
                dim_sources[d].col_has_null[oi] != 0);
            ++out_c;
          }
        }
        chunk.rows = hit;
        for (const ColumnarColumn& col : chunk.columns) {
          if (!col.is_uniform) {
            out_uniform = false;
            break;
          }
        }
        out_table.AppendChunk(std::move(chunk));
        matched_fact.clear();
        for (size_t d = 0; d < ndims; ++d) {
          if (!eff_dim_output[d].empty()) {
            match_rows[d].clear();
          }
        }
      };
      std::array<uint32_t, 8> tmp_match{};
      for (size_t ri = begin; ri < end; ++ri) {
        const Row& frow = relations[fact].rows[ri];
        bool matched = true;
        for (size_t d = 0; d < ndims; ++d) {
          ++out_cmp;
          const Value& kv = frow[*dimensions[d].fact_column];
          if (kv.IsNull() ||
              !lookup_dim(dimensions[d], kv.value.int_value, &tmp_match[d])) {
            matched = false;
            break;
          }
        }
        if (matched) {
          matched_fact.push_back(ri);
          for (size_t d = 0; d < ndims; ++d) {
            if (!eff_dim_output[d].empty()) {
              match_rows[d].push_back(tmp_match[d]);
            }
          }
          if (matched_fact.size() == kColumnarChunkRows) {
            flush_batch();
          }
        }
      }
      flush_batch();
    };
    auto merged = std::make_shared<ColumnarTable>();
    bool all_uniform = true;
    if (workers == 1) {
      size_t cmp = 0;
      probe_rows_to_columnar(*merged, cmp, all_uniform, 0, total);
      result.join_comparisons += cmp;
    } else {
      std::vector<ColumnarTable> shard_tables(workers);
      std::vector<size_t> shard_cmp(workers, 0);
      std::vector<uint8_t> shard_uniform(workers, 1);
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
        const size_t chunk = total / workers;
        const size_t begin = worker * chunk;
        const size_t end = worker + 1 == workers ? total : begin + chunk;
        bool u = true;
        probe_rows_to_columnar(shard_tables[worker], shard_cmp[worker], u,
                               begin, end);
        shard_uniform[worker] = u ? 1 : 0;
      }));
      for (size_t w = 0; w < workers; ++w) {
        result.join_comparisons += shard_cmp[w];
        all_uniform &= (shard_uniform[w] != 0);
        merged->AppendTable(std::move(shard_tables[w]));
      }
    }
    if (merged->total_rows > 0) {
      result.SetColumnarTable(std::move(merged), all_uniform);
    }
    result.peak_intermediate_rows =
        std::max(result.peak_intermediate_rows, result.TotalRows());
    return std::optional<Relation>(std::move(result));
  }
  for (const Dimension& dim : dimensions) {
    if (relations[dim.relation].HasColumnarOnly()) {
      RETURN_IF_FAIL(relations[dim.relation].EnsureRowsMaterialized());
    }
  }
  bool all_inputs_uniform =
      relations[fact].uniform_cell_types && !relations[fact].HasSpill();
  for (const Dimension& dim : dimensions) {
    const Relation& dim_rel = relations[dim.relation];
    all_inputs_uniform &= dim_rel.uniform_cell_types && !dim_rel.HasSpill();
  }
  bool keep_uniform = all_inputs_uniform && result.schema.ColumnCount() > 0;
  bool all_fixed_scalar = keep_uniform;
  for (size_t c = 0; c < result.schema.ColumnCount(); ++c) {
    const ValueType vt = result.schema.GetColumn(c).Type();
    if (vt == ValueType::kNull || vt == ValueType::kArray) {
      keep_uniform = false;
      all_fixed_scalar = false;
      break;
    }
    if (vt != ValueType::kInt64 && vt != ValueType::kDate &&
        vt != ValueType::kDouble) {
      all_fixed_scalar = false;
    }
  }
  const size_t fixed_row_bytes =
      all_fixed_scalar ? (64 + (result.schema.ColumnCount() * 40)) : 0;
  const size_t flush_granularity = Relation::ChargeFlushGranularity();
  const auto probe_range = [&](Relation& output, size_t begin, size_t end) {
    std::array<const Row*, 8> matches{};
    for (size_t row_index = begin; row_index < end; ++row_index) {
      Row& fact_row = relations[fact].rows[row_index];
      bool matched = true;
      for (size_t d = 0; d < dimensions.size(); ++d) {
        const Dimension& dim = dimensions[d];
        const Value& key = fact_row[*dim.fact_column];
        uint32_t slot = 0;
        const bool hit =
            !key.IsNull() && lookup_dim(dim, key.value.int_value, &slot);
        ++output.join_comparisons;
        if (!hit) {
          matched = false;
          break;
        }
        matches[d] = &relations[dim.relation].rows[slot];
      }
      if (!matched) {
        continue;
      }
      if (project_output) {
        for (size_t c = 0; c < fact_output.size(); ++c) {
          if (c != fact_output[c]) {
            fact_row.values_[c] = std::move(fact_row.values_[fact_output[c]]);
          }
        }
        fact_row.values_.resize(fact_output.size());
        fact_row.values_.reserve(result.schema.ColumnCount());
        for (size_t d = 0; d < dimensions.size(); ++d) {
          for (const size_t column : dimension_output[d]) {
            fact_row.values_.push_back((*matches[d])[column]);
          }
        }
      } else {
        fact_row.values_.reserve(result.schema.ColumnCount());
        for (size_t d = 0; d < dimensions.size(); ++d) {
          const auto& values = matches[d]->values_;
          fact_row.values_.insert(fact_row.values_.end(), values.begin(),
                                  values.end());
        }
      }
      const size_t bytes = fixed_row_bytes != 0
                               ? fixed_row_bytes
                               : EstimateRowBytesFast(fact_row);
      output.AddRowWithKnownBytes(std::move(fact_row), bytes,
                                  flush_granularity);
    }
    output.uniform_cell_types = keep_uniform && !output.HasSpill();
  };
  if (workers == 1) {
    probe_range(result, 0, total);
  } else {
    // Contiguous ranges followed by worker-order concatenation retain the
    // serial fact order, including floating aggregate prefixes downstream.
    std::vector<Relation> shards(workers);
    std::vector<ExecutionRuntime> shard_runtimes(workers);
    for (size_t w = 0; w < workers; ++w) {
      shards[w].set_runtime(&shard_runtimes[w]);
      shards[w].schema = result.schema;
    }
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
      const size_t chunk = total / workers;
      const size_t begin = worker * chunk;
      const size_t end = worker + 1 == workers ? total : begin + chunk;
      shards[worker].rows.reserve(end - begin);
      probe_range(shards[worker], begin, end);
    }));
    for (size_t w = 0; w < workers; ++w) {
      result.join_comparisons += shards[w].join_comparisons;
      if (result.runtime() != nullptr) {
        result.runtime()->relation_spills += shard_runtimes[w].relation_spills;
      }
    }
    RETURN_IF_FAIL(Relation::MergeShardsInto(shards, &result));
  }
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.TotalRows());
  RETURN_IF_FAIL(result.FinishSpill());
  return std::optional<Relation>(std::move(result));
}

// A WHERE predicate referencing only the nullable sides can require at least
// one LEFT join to match. When every join uses the same preserved key tuple,
// their union of build keys is a necessary condition for a surviving probe.
// Keep the joins and WHERE predicate: membership proves neither multiplicity
// nor that a matched row satisfies the predicate.
void ReduceUnmatchedOuterProbes(TransactionContext& context,
                                const SelectStatement& statement,
                                std::vector<Relation>& relations,
                                const std::vector<PredicateInfo>& predicates,
                                const CteMap& ctes) {
  if (relations.size() < 3 || relations.size() > 5 ||
      relations[0].HasColumnarOnly() || relations[0].TotalRows() < 4096 ||
      std::ranges::any_of(relations, [](const Relation& relation) {
        return relation.HasSpill() || relation.HasColumnarOnly();
      })) {
    return;
  }
  Schema combined = relations[0].schema;
  std::vector<slot_t> probe_columns;
  std::vector<std::vector<slot_t>> build_columns(relations.size());
  size_t build_rows = 0;
  for (size_t i = 1; i < relations.size(); ++i) {
    const auto& source = statement.Sources()[i];
    if (source.join_type != JoinType::kLeft || !source.using_columns.empty()) {
      return;
    }
    const auto terms = SplitConjuncts(source.join_condition);
    auto keys = EqualityKeys(relations[0].schema, relations[i].schema, terms);
    if (keys.empty() || keys.size() > CompositeIntKey::kMaxParts ||
        keys.size() != terms.size()) {
      return;
    }
    std::ranges::sort(keys, {}, &EqualityKey::left);
    std::vector<slot_t> candidate_columns;
    candidate_columns.reserve(keys.size());
    for (const auto& key : keys) {
      if (key.null_safe || key.left_offset != 0 || key.right_offset != 0) {
        return;
      }
      if (relations[0].schema.GetColumn(key.left).IsUnsigned() ||
          relations[i].schema.GetColumn(key.right).IsUnsigned()) {
        return;
      }
      candidate_columns.push_back(static_cast<slot_t>(key.left));
      build_columns[i].push_back(static_cast<slot_t>(key.right));
    }
    if (!CompositeIntJoinKey(relations[0].schema, candidate_columns,
                             relations[i].schema, build_columns[i]) ||
        (!probe_columns.empty() && candidate_columns != probe_columns)) {
      return;
    }
    probe_columns = std::move(candidate_columns);
    if (relations[i].rows.size() > 1048576 - build_rows) {
      return;
    }
    build_rows += relations[i].rows.size();
    combined = combined + relations[i].schema;
  }
  const Row nulls(std::vector<Value>(combined.ColumnCount()));
  const Scope scope{.row = &nulls, .schema = &combined};
  bool requires_match = false;
  for (const auto& predicate : predicates) {
    if (!predicate.resolved || predicate.contains_query ||
        predicate.sources.size() != relations.size() - 1 ||
        predicate.sources.contains(0) ||
        !SafeToReduceEvaluationCountWithSchema(predicate.expression,
                                               combined)) {
      continue;
    }
    const auto value =
        TryEvaluate(predicate.expression, scope, nullptr, context, ctes);
    if (value.HasValue() && !Truthy(value.Value())) {
      requires_match = true;
      break;
    }
  }
  if (!requires_match) {
    return;
  }
  const auto keys_have_declared_type = [](const Relation& relation,
                                          const std::vector<slot_t>& columns) {
    return std::ranges::all_of(relation.rows, [&](const Row& row) {
      return std::ranges::all_of(columns, [&](slot_t column) {
        return column < row.Count() &&
               (row[column].IsNull() ||
                (!row[column].IsUnsigned() &&
                 row[column].type == relation.schema.GetColumn(column).Type()));
      });
    });
  };
  if (!keys_have_declared_type(relations[0], probe_columns)) {
    return;
  }
  for (size_t i = 1; i < relations.size(); ++i) {
    if (!keys_have_declared_type(relations[i], build_columns[i])) {
      return;
    }
  }
  const size_t bit_count = BloomRuntimeFilter::BitsForKeys(build_rows);
  auto charge = TryReserveQueryMemory((bit_count / 8) + 4096);
  if (!charge) {
    return;
  }
  // Hash collisions and Bloom false positives only retain extra probe rows;
  // the original joins and predicate remain the authority. A compact bitset
  // avoids allocating a node and copying a tuple for every build key.
  const auto hash_key = [](const Row& row, const std::vector<slot_t>& columns) {
    uint64_t hash = 0x9e3779b97f4a7c15ULL;
    for (const slot_t column : columns) {
      // Mix each payload before combining: adjacent small integers otherwise
      // collapse many distinct tuples into the same fingerprint.
      hash ^= BloomRuntimeFilter::Hash(row[column].value.int_value, 0);
      hash *= 0x100000001b3ULL;
    }
    return std::bit_cast<int64_t>(hash);
  };
  std::vector<uint64_t> bits(bit_count / 64, 0);
  for (size_t i = 1; i < relations.size(); ++i) {
    for (const auto& row : relations[i].rows) {
      if (HasNullKey(row, build_columns[i])) {
        continue;
      }
      const int64_t key = hash_key(row, build_columns[i]);
      for (unsigned probe = 0; probe < BloomRuntimeFilter::kHashProbes;
           ++probe) {
        const size_t bit =
            static_cast<size_t>(BloomRuntimeFilter::Hash(key, probe)) &
            (bit_count - 1);
        bits[bit / 64] |= uint64_t{1} << (bit % 64);
      }
    }
  }
  const BloomRuntimeFilter filter(std::move(bits), bit_count, build_rows);
  auto& rows = relations[0].rows;
  size_t kept = 0;
  for (size_t i = 0; i < rows.size(); ++i) {
    if (HasNullKey(rows[i], probe_columns) ||
        filter.Reject(hash_key(rows[i], probe_columns))) {
      continue;
    }
    if (kept != i) {
      rows[kept] = std::move(rows[i]);
    }
    ++kept;
  }
  if (::tinylamb::platform::GetEnv("TINYLAMB_DEBUG_ORDERED") != nullptr) {
    std::cerr << "[ordered] match-domain rows=" << rows.size()
              << " kept=" << kept << " build=" << build_rows << '\n';
  }
  rows.resize(kept);
}

}  // namespace tinylamb::relational_detail
