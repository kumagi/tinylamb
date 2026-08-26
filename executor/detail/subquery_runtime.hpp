/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_SUBQUERY_RUNTIME_HPP
#define TINYLAMB_EXECUTOR_DETAIL_SUBQUERY_RUNTIME_HPP

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "executor/detail/relation.hpp"
#include "query/statement.hpp"
#include "type/column_name.hpp"
#include "type/value.hpp"

namespace tinylamb {
class TransactionContext;
}

namespace tinylamb::relational_detail {

struct Scope {
  const Row* row{nullptr};
  const Schema* schema{nullptr};
  const Scope* outer{nullptr};
  // USING join merged column names (lowercased): a bare reference matching
  // several schema columns coalesces them when its name is listed here.
  const std::vector<std::string>* using_columns{nullptr};
  // Aggregate values of the enclosing grouped query.  Subqueries nested in
  // HAVING / SELECT-over-groups resolve correlated aggregate references
  // through this chain (e.g. FROM UNNEST(agg_alias) after alias inlining).
  // Same type as relational_detail::AggregateResultMap, spelled inline to
  // keep this header free of the expression_eval.hpp cycle.
  const std::unordered_map<const class AggregateExpression*, Value>*
      aggregates{nullptr};
};

using CteMap = std::unordered_map<std::string, RelationPtr>;

struct CorrelatedIndex {
  Schema schema;
  std::unordered_multimap<std::string, Row> rows;
  std::vector<slot_t> local_columns;
  std::vector<ColumnName> outer_columns;
  std::vector<ColumnName> cache_outer_columns;
  std::unordered_map<std::string, RelationPtr> cached_results;
  bool preaggregated{false};
};

// Integer semi-join keys stashed by BuildInput while planning one statement.
// `owner` scopes the stash to the generating statement so unrelated scans of
// the same table (e.g. sibling subqueries) are never filtered by it.
struct TableKeyFilter {
  std::unordered_set<int64_t> keys;
  slot_t column{0};
  const SelectStatement* owner{nullptr};
};

struct ExecutionRuntime {
  std::unordered_map<std::string, RelationPtr> base_relations;
  std::unordered_set<std::string> reusable_base_relations;
  std::unordered_map<std::string, std::vector<slot_t>> reusable_projections;
  std::unordered_map<std::string, TableKeyFilter> table_key_filters;
  const SelectStatement* root_statement{nullptr};
  std::unordered_map<const SelectStatement*, std::unique_ptr<CorrelatedIndex>>
      correlated_indexes;
  std::unordered_set<const SelectStatement*> unindexable_queries;
  std::unordered_map<const SelectStatement*, RelationPtr> uncorrelated_results;
  std::unordered_map<const SelectStatement*, std::unordered_set<Value>>
      uncorrelated_membership;
  std::unordered_set<const SelectStatement*> noncacheable_queries;
  // Keep-alives for every subquery statement whose address keys the caches
  // above.  Prepared executors are freed between statements, and a later
  // allocation could otherwise reuse a dead statement's address, silently
  // binding stale cache entries to an unrelated subquery.
  std::vector<std::shared_ptr<const SelectStatement>> retained_statements;
  // Fingerprint recorded when a cache entry was created; a mismatch on probe
  // means the address was recycled by an unrelated statement and the entry
  // must be discarded.
  std::unordered_map<const SelectStatement*, std::string> cache_fingerprints;
  size_t correlated_index_builds{0};
  size_t correlated_index_probes{0};
  size_t correlated_result_cache_hits{0};
  size_t uncorrelated_cache_hits{0};
  size_t uncorrelated_hash_builds{0};
  size_t uncorrelated_hash_probes{0};
  double scan_ms{0};
  double filter_ms{0};
  double join_ms{0};
  double project_ms{0};
  double sort_ms{0};
  size_t base_scan_cache_hits{0};
  size_t aggregate_input_rows{0};
  size_t aggregate_groups{0};
  size_t aggregate_updates{0};
  size_t scan_rows{0};
  size_t scan_output_rows{0};
  size_t scan_values_decoded{0};
  size_t scan_values_available{0};
  size_t relation_spills{0};
  size_t key_filter_scans{0};
  size_t key_filter_keys{0};
  size_t key_filter_rejected{0};
};

double ElapsedMs(std::chrono::steady_clock::time_point begin);

void CountStatementTables(const SelectStatement& statement,
                          std::unordered_map<std::string, size_t>* counts);

void EnsureReusableProjections(TransactionContext& context,
                               ExecutionRuntime* runtime);

const std::vector<slot_t>* ReusableProjection(TransactionContext& context,
                                              std::string_view table);

Relation ExecuteQuery(TransactionContext& context,
                      const SelectStatement& statement, const Scope* outer,
                      const CteMap& inherited_ctes);

Relation FinishQuery(TransactionContext& context,
                     const SelectStatement& statement, Relation input,
                     const Scope* outer, const CteMap& ctes,
                     bool apply_where = true,
                     size_t hidden_columns = 0);

// GoogleSQL name resolution for grouped queries: GROUP BY / HAVING items may
// reference SELECT-list aliases or ordinals when they do not resolve against
// the input columns.  Returns nullptr when nothing needed rewriting.
std::shared_ptr<SelectStatement> ResolveGroupingAliases(
    const SelectStatement& statement, const Schema& input_schema);

// Projects one subquery result row for expression consumption: the first
// column value, or -- for SELECT AS STRUCT subqueries -- the whole row
// encoded as struct-like JSON so quantified comparisons and membership see a
// single comparable value.  `schema` (optional) supplies the declared field
// names; without it positional "fN" keys are used.
Value ProjectSubqueryRow(const Row& row, bool as_struct,
                         const Schema* schema = nullptr);

std::optional<Relation> ExecuteCorrelatedSingleSource(
    TransactionContext& context, const SelectStatement& statement,
    const Scope& outer, const CteMap& ctes);

const Relation* ExecuteCachedUncorrelated(TransactionContext& context,
                                          const SelectStatement& statement,
                                          const CteMap& ctes);

bool ExpressionUsesOnlyScopes(TransactionContext& context,
                              const Expression& expression,
                              const std::vector<Relation>& sources,
                              const CteMap& ctes);

bool StatementUsesOnlyScopes(TransactionContext& context,
                             const SelectStatement& statement,
                             const std::vector<Relation>& outer_sources,
                             const CteMap& ctes);

bool ContainsOnlyUncorrelatedQueries(TransactionContext& context,
                                     const Expression& expression,
                                     const CteMap& ctes);

bool ExpressionsAreLocal(TransactionContext& context,
                         const SelectStatement& statement,
                         const std::vector<Relation>& sources,
                         const CteMap& ctes);

// Guards runtime caches against recycled statement addresses (see
// ExecutionRuntime::cache_fingerprints).
bool CacheEntryIsCurrent(const SelectStatement& statement,
                         ExecutionRuntime& runtime);
void DropCacheEntry(ExecutionRuntime& runtime, const SelectStatement* key);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_SUBQUERY_RUNTIME_HPP
