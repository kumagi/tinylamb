/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_QUERY_EVALUATION_CONTEXT_IMPL_HPP
#define TINYLAMB_QUERY_EVALUATION_CONTEXT_IMPL_HPP

// Production implementation of expression/evaluation_context.hpp
// (improvement3.md A1 / S3).  RelationalEvaluationContext is the transient
// adapter that binds the relational execution state of one statement --
// TransactionContext, the outer Scope chain, inherited CTEs and prepared
// aggregates -- to the abstract EvaluationContext interface.  Expression
// nodes evaluate subqueries through that interface and never see
// TransactionContext, CteMap or executor types.
//
// Header-only by design: the query layer owns the wiring and no new build
// target is introduced.  SqlEngine attaches an instance per statement
// execution (also reachable through
// TransactionContext::set_evaluation_context()).

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
      : context_(context), scope_(scope), ctes_(ctes), aggregates_(aggregates) {}

  // Executes the statement with the same strategy as the relational_detail
  // interpreter: indexed correlated execution when the outer scope allows it,
  // the cached uncorrelated relation otherwise, and a full ExecuteQuery as
  // the fallback.  Projects the first output column in row order.
  [[nodiscard]] StatusOr<std::vector<Value>> RunSubquery(
      const SelectStatement& statement, const Row* /*outer_row*/) override {
    std::optional<relational_detail::Relation> indexed;
    if (scope_ != nullptr) {
      indexed = relational_detail::ExecuteCorrelatedSingleSource(
          context_, statement, *scope_, ctes_);
    }
    const relational_detail::Relation* relation =
        indexed.has_value() ? &*indexed : nullptr;
    if (relation == nullptr) {
      relation =
          relational_detail::ExecuteCachedUncorrelated(context_, statement,
                                                       ctes_);
    }
    std::optional<relational_detail::Relation> executed;
    if (relation == nullptr) {
      executed =
          relational_detail::ExecuteQuery(context_, statement, scope_, ctes_);
      relation = &*executed;
    }
    std::vector<Value> projected;
    projected.reserve(relation->TotalRows());
    relation->ForEachRow([&](const Row& row) {
      if (!row.values_.empty()) { projected.push_back(row[0]);
}
    });
    return StatusOr<std::vector<Value>>(std::move(projected));
  }

  [[nodiscard]] const AggregateResultMap* CurrentAggregates() const override {
    return aggregates_;
  }

  // Forwards to Database::GetOrAddFunction so FunctionCallExpression::Validate
  // keeps registering UDFs in the catalog without depending on database
  // types.
  [[nodiscard]] Status GetOrAddFunction(std::string_view function_name,
                                        int argument_count) override {
    if (context_.GetCatalog() == nullptr) { return Status::kNotExists;
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
};

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_EVALUATION_CONTEXT_IMPL_HPP
