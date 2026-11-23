/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_QUERY_EVALUATION_CONTEXT_IMPL_HPP
#define TINYLAMB_QUERY_EVALUATION_CONTEXT_IMPL_HPP

// Production implementation of the abstract interface in
// expression/evaluation_context.hpp -- specifically the "Abstract boundary
// between expression evaluation and query execution" contract block at the
// top of that header (which supersedes the old A1/S3 design-note labels).
// RelationalEvaluationContext is the transient adapter that binds the
// relational execution state of one statement --
// TransactionContext, the outer Scope chain, inherited CTEs and prepared
// aggregates -- to the abstract EvaluationContext interface.  Expression
// nodes evaluate subqueries through that interface and never see
// TransactionContext, CteMap or executor types.
//
// Header-only by design. This adapter describes the intended boundary, but
// it is not currently instantiated by SqlEngine; production subquery
// evaluation still uses relational_detail's direct runtime helpers.

#include <optional>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "database/catalog_reader.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/evaluation_context.hpp"
#include "query/statement.hpp"
#include "type/function.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

class RelationalEvaluationContext : public EvaluationContext {
 public:
  // `scope` may be null for uncorrelated evaluations; `aggregates` may be
  // null outside grouping contexts.  Both must outlive this object.
  RelationalEvaluationContext(TransactionContext& context,
                              const relational_detail::Scope* scope,
                              const relational_detail::CteMap& ctes,
                              const AggregateResultMap* aggregates)
      : context_(context),
        scope_(scope),
        ctes_(ctes),
        aggregates_(aggregates) {}

  // Executes the statement with the same strategy as the relational_detail
  // interpreter: indexed correlated execution when the outer scope allows it,
  // the cached uncorrelated relation otherwise, and a full ExecuteQuery as
  // the fallback.  Projects the first output column in row order.
  [[nodiscard]] StatusOr<std::vector<Value>> RunSubquery(
      const SelectStatement& statement, const Row* /*outer_row*/) override {
    last_element_type_hint_.clear();
    std::optional<relational_detail::Relation> indexed;
    if (scope_ != nullptr) {
      indexed = relational_detail::ExecuteCorrelatedSingleSource(
          context_, statement, *scope_, ctes_);
    }
    const relational_detail::Relation* relation =
        indexed.has_value() ? &*indexed : nullptr;
    if (relation == nullptr) {
      relation = relational_detail::ExecuteCachedUncorrelated(context_,
                                                              statement, ctes_);
    }
    std::optional<relational_detail::Relation> executed;
    if (relation == nullptr) {
      StatusOr<relational_detail::Relation> executed_query =
          relational_detail::ExecuteQuery(context_, statement, scope_, ctes_);
      if (!executed_query.HasValue()) {
        return executed_query.GetStatus();
      }
      executed = executed_query.MoveValue();
      relation = &*executed;
    }
    std::vector<Value> projected;
    projected.reserve(relation->TotalRows());
    const bool as_struct = statement.AsStruct();
    const Schema& subquery_schema = relation->schema;
    RETURN_IF_FAIL(relation->ForEachRow([&](const Row& row) {
      projected.push_back(relational_detail::ProjectSubqueryRow(
          row, as_struct, &subquery_schema));
    }));
    if (subquery_schema.ColumnCount() > 0) {
      last_element_type_hint_ =
          std::string(ElementSqlTypeName(subquery_schema.GetColumn(0).Type()));
    }
    return {std::move(projected)};
  }

  // Supply the declared element type through the base class's hook rather
  // than overriding RunSubqueryDetailed. That keeps one execution path in
  // production and tests, and RunSubquery has already cached the type from
  // the result schema by the time the hook is called.
  [[nodiscard]] std::string SubqueryElementTypeHint(
      const SelectStatement& /*statement*/) override {
    return last_element_type_hint_;
  }

  [[nodiscard]] const AggregateResultMap* CurrentAggregates() const override {
    return aggregates_;
  }

  // Forwards to Database::GetOrAddFunction so FunctionCallExpression::Validate
  // keeps registering UDFs in the catalog without depending on database
  // types.
  [[nodiscard]] Status GetOrAddFunction(std::string_view function_name,
                                        int argument_count) override {
    if (context_.GetCatalog() == nullptr) {
      return Status::kNotExists;
    }
    return context_.GetCatalog()
        ->GetOrAddFunction(context_, function_name, argument_count)
        .GetStatus();
  }

 private:
  TransactionContext& context_;
  const relational_detail::Scope* scope_;
  const relational_detail::CteMap& ctes_;
  const AggregateResultMap* aggregates_;
  std::string last_element_type_hint_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_EVALUATION_CONTEXT_IMPL_HPP
