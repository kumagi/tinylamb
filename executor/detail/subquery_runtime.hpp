/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_SUBQUERY_RUNTIME_HPP
#define TINYLAMB_EXECUTOR_DETAIL_SUBQUERY_RUNTIME_HPP

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/runtime_filter.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/relation.hpp"
#include "query/statement.hpp"
#include "type/column_name.hpp"
#include "type/value.hpp"

namespace tinylamb {
class TransactionContext;
}

namespace tinylamb::relational_detail {

struct ColumnLookupCache {
  struct Result {
    int strict{-2};
    int fallback{-2};
  };
  std::unordered_map<std::uint64_t, std::unordered_map<ColumnName, Result>>
      schemas;
};

inline thread_local ColumnLookupCache* active_lookup_cache = nullptr;

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
  const std::unordered_map<const class AggregateExpression*, Value>* aggregates{
      nullptr};
  // True for a grouped-emission OUTPUT scope: its row is the projected
  // select list, so a bare name that matches nothing here can only be an
  // output column, an alias or an aggregate -- never a relation alias
  // denoting the whole row.  Without this flag the whole-row fallback would
  // silently turn a typo'd ORDER BY key into a STRUCT of the output row and
  // "sort" by it instead of erroring.
  bool no_row_alias{false};
  ColumnLookupCache* lookup_cache{nullptr};
};

using CteMap = std::unordered_map<std::string, RelationPtr>;

struct CorrelatedIndex {
  Schema schema;
  std::unordered_multimap<std::string, Row> rows;
  std::vector<slot_t> local_columns;
  std::vector<ColumnName> outer_columns;
  std::vector<ColumnName> cache_outer_columns;
  std::unordered_map<std::string, RelationPtr> cached_results;
  // Every parameter tuple observed by the Apply operator, including the
  // NULL tuple which is intentionally not inserted into cached_results.
  // Keeping this separate from cached_results lets EXPLAIN ANALYZE report the
  // actual distinct-key cardinality without pretending NULL is cacheable.
  std::unordered_set<std::string> parameter_keys_seen;
  std::unordered_set<int64_t> parameter_int_keys_seen;
  bool null_parameter_seen{false};
  bool preaggregated{false};
  bool has_preagg_int_scalars{false};
  bool preagg_two_int_key{false};
  bool preagg_empty_has_row{false};
  FlatIntKeyMap<Value> preagg_int_scalars{};
  FlatIntKeyMap<uint8_t> preagg_seen_keys{};
  size_t preagg_distinct_keys{0};
  Value preagg_empty_scalar{};
  // EXISTS-only fast path: when the subquery result is consumed purely as
  // row-existence (single INT64/DATE correlation key, no outer-referencing
  // expressions, no HAVING/GROUP BY/LIMIT/OFFSET/DISTINCT), the build keeps
  // just the set of surviving local key values instead of every matching
  // row.  A Q4-sized build otherwise materializes millions of projected Rows
  // plus one multimap entry (and one string key) per row.
  std::unordered_set<int64_t> exists_keys;
  bool exists_key_set{false};
  std::vector<std::pair<int64_t, Row>> int_rows;
  // Rows shared with sibling exists builds over the same base table and
  // correlation column (ExecutionRuntime::shared_exists_builds).  The
  // pointed-to vector is immutable after publish, so every consumer probes
  // the same storage instead of re-scanning the base table.
  std::shared_ptr<const std::vector<std::pair<int64_t, Row>>> shared_int_rows;

  // The keyed rows this build probes, whichever storage owns them.
  [[nodiscard]] const std::vector<std::pair<int64_t, Row>>& ExistRows() const {
    return shared_int_rows != nullptr ? *shared_int_rows : int_rows;
  }
  std::vector<Expression> exists_residual_predicates;
  bool exists_residual_fast_path{false};
  // The outer context this build was correlation-domain-filtered against
  // (ScopedCorrelatedOuter registration).  Nonzero `outer_filter_serial`
  // means the build only contains rows whose correlation key appears in that
  // context's relation; a probe from any other context must rebuild, or it
  // would miss keys the filtered build never loaded.
  const void* outer_filter_relation{nullptr};
  uint64_t outer_filter_serial{0};
  const Schema* cached_outer_schema{nullptr};
  std::array<slot_t, 2> cached_outer_slots{};
  bool cached_outer_slots_valid{false};
};

// Dynamic ("shared information passing") semi-join keys stashed by BuildInput
// while planning one statement.  `filter` is the pruning structure handed to
// the scan: the distinct build keys as an exact set while the domain is small,
// and a min/max enclosure plus a Bloom filter once it grows
// (common/runtime_filter.hpp).  `owner` scopes the stash to the generating
// statement so unrelated scans of the same table (e.g. sibling subqueries) are
// never filtered by it.  `column` is a full-schema slot; consumers remap it
// into their own projection.
// `ExecutionRuntime::table_key_filters` is keyed by the source's in-statement
// name (alias, else table name), never the bare base table: a self-join names
// the same table twice, and a key domain derived for one alias (from that
// alias's join predicate) must never prune another alias's scan.
inline std::string TableKeyFilterKey(const SelectSource& source) {
  return source.alias.empty() ? source.table : source.alias;
}

struct TableKeyFilter {
  std::shared_ptr<const RuntimeFilterGroup> filter;
  // ValueType of `column`. A filter built over INT64 keys must never prune a
  // DATE probe column (or vice versa): the two share the int64 bit domain but
  // not the comparison domain, so an unmatched type leaves the column
  // unfiltered.
  ValueType type{ValueType::kInt64};
  size_t key_count{0};
  slot_t column{0};
  const SelectStatement* owner{nullptr};
  const ExpressionBase* source_expr{nullptr};
  bool exact_in{false};

  [[nodiscard]] bool empty() const {
    return filter == nullptr || filter->empty();
  }
};

// Row-major typed mirror of a cached base relation.  The typed parallel fold
// streams this arena instead of visiting per-Row heap blocks: fixed-width
// cells for row r, column c live at cells[r * stride + c] (DOUBLE payloads
// bit-cast through int64), with per-column null bitmaps (one bit per row).
// VARCHAR cells live in per-slot byte arenas (`varchars`, parallel to the
// VARCHAR slots in slot order) with cumulative per-row end offsets.
struct BaseRelationArena {
  size_t stride{0};
  std::vector<ValueType> slot_types;
  std::vector<int64_t> cells;
  std::vector<std::vector<uint64_t>> null_bits;
  struct VarcharRun {
    std::vector<uint8_t> bytes;
    std::vector<uint32_t> ends;  // rows entries, cumulative byte offsets
  };
  std::vector<VarcharRun> varchars;
  size_t rows{0};
  // Arena-only cache contract (typed fill): the owning Relation::rows is
  // empty until a consumer boxes rows back.  `published_rowsless` is fixed at
  // publish time; `rows_boxed` latches once the rows exist.  Both are written
  // only from single-threaded phases (publish/setup/after worker join), so
  // plain bools keep the arena move-constructible.
  bool published_rowsless{false};
  bool rows_boxed{false};

  [[nodiscard]] bool IsNull(size_t row, size_t column) const {
    return ((null_bits[column][row >> 6] >> (row & 63)) & 1U) != 0U;
  }
  // VARCHAR ordinal of `column` (its index among VARCHAR slots).
  [[nodiscard]] size_t VarcharOrdinal(size_t column) const {
    size_t ordinal = 0;
    for (size_t i = 0; i < column; ++i) {
      if (slot_types[i] == ValueType::kVarChar) {
        ++ordinal;
      }
    }
    return ordinal;
  }
  // View into the arena's copy of cell (row, column); the arena must outlive
  // the view.
  [[nodiscard]] std::string_view Varchar(size_t row, size_t column) const {
    const VarcharRun& run = varchars[VarcharOrdinal(column)];
    const uint32_t begin = row == 0 ? 0U : run.ends[row - 1];
    return {reinterpret_cast<const char*>(run.bytes.data()) + begin,
            run.ends[row] - begin};
  }
  // Append a contiguous run of rows (morsel buffer).  `cells` holds
  // rows * stride values in row-major order; `nulls` one byte per (row,
  // column), same order.  Called once per morsel at publish time so the
  // per-row cost stays on the worker thread.  (All-fixed-width shape used by
  // the generic fill.)
  void AppendRun(const int64_t* run_cells, const uint8_t* run_nulls,
                 size_t run_rows) {
    if (run_rows == 0) {
      return;
    }
    const size_t first = rows;
    cells.insert(cells.end(), run_cells, run_cells + (run_rows * stride));
    const size_t last_word = (first + (run_rows - 1)) >> 6;
    for (size_t c = 0; c < stride; ++c) {
      auto& bits = null_bits[c];
      bits.resize(last_word + 1, 0);
      for (size_t r = 0; r < run_rows; ++r) {
        if (run_nulls[(r * stride) + c] != 0) {
          const size_t row = first + r;
          bits[row >> 6] |= uint64_t{1} << (row & 63);
        }
      }
    }
    rows += run_rows;
  }
  // Append a typed run (same layout as this arena, independent buffers) — the
  // typed fill's per-morsel mirrors, cut to the written row count.
  void AppendTyped(const BaseRelationArena& run) {
    if (run.rows == 0) {
      return;
    }
    cells.insert(cells.end(), run.cells.begin(), run.cells.end());
    for (size_t c = 0; c < stride; ++c) {
      auto& bits = null_bits[c];
      bits.resize(((rows + (run.rows - 1)) >> 6) + 1, 0);
      for (size_t r = 0; r < run.rows; ++r) {
        if (((run.null_bits[c][r >> 6] >> (r & 63)) & 1U) != 0U) {
          const size_t row = rows + r;
          bits[row >> 6] |= uint64_t{1} << (row & 63);
        }
      }
    }
    for (size_t v = 0; v < varchars.size(); ++v) {
      VarcharRun& dst = varchars[v];
      const VarcharRun& src = run.varchars[v];
      const auto dst_bytes = static_cast<uint32_t>(dst.bytes.size());
      dst.bytes.insert(dst.bytes.end(), src.bytes.begin(), src.bytes.end());
      dst.ends.reserve(dst.ends.size() + src.ends.size());
      for (const uint32_t end : src.ends) {
        dst.ends.push_back(dst_bytes + end);
      }
    }
    rows += run.rows;
  }
};

struct ExecutionRuntime {
  std::unordered_map<std::string, RelationPtr> base_relations;
  std::unordered_map<std::string, std::shared_ptr<BaseRelationArena>>
      base_relation_arenas;
  std::unordered_set<std::string> reusable_base_relations;
  std::unordered_map<std::string, std::vector<slot_t>> reusable_projections;
  std::unordered_map<std::string, TableKeyFilter> table_key_filters;
  const SelectStatement* root_statement{nullptr};
  ColumnLookupCache lookup_cache;
  std::unordered_map<const SelectStatement*, std::unique_ptr<CorrelatedIndex>>
      correlated_indexes;
  // Keyed rows shared by sibling exists-only subqueries that scan the same
  // base table with the same single INT64/DATE correlation column (TPC-H
  // Q21's EXISTS/NOT EXISTS pair over lineitem).  A published build is
  // always UNFILTERED — the publisher had no build-time predicates — so any
  // consumer can defer its full residual conjunction to probe time.  The
  // schema digest in the key keeps differently-projected loads apart; the
  // exact Schema equality is re-checked on lookup.  A build made under a
  // correlation-domain filter carries only that context's keys, so the
  // consumer must present the same (relation, serial) registration as the
  // publisher (unfiltered entries — null relation — are universally valid,
  // mirroring the per-statement cache rule).
  struct SharedExistsBuild {
    Schema schema;
    std::shared_ptr<const std::vector<std::pair<int64_t, Row>>> rows;
    const void* outer_filter_relation{nullptr};
    uint64_t outer_filter_serial{0};
  };
  std::unordered_map<std::string, SharedExistsBuild> shared_exists_builds;
  size_t shared_exists_build_reuses{0};
  std::unordered_set<const SelectStatement*> unindexable_queries;
  std::unordered_map<const SelectStatement*, RelationPtr> uncorrelated_results;
  // Structural cache for equivalent uncorrelated subqueries.  The pointer
  // keyed cache above is retained for address-local bookkeeping, while this
  // map lets separately allocated but identical QueryExpression nodes share
  // one execution (CSE across a SELECT list).
  std::unordered_map<std::string, RelationPtr>
      uncorrelated_results_by_fingerprint;
  // Hashed membership for uncorrelated `x IN (SELECT ...)` probes.  The
  // NULL flag is recorded once while the hash is built; per-probe NULL
  // ambiguity must never rescan the build relation (Q22-sized builds make a
  // per-row rescan quadratic).
  struct UncorrelatedMembership {
    std::unordered_set<Value> values;
    bool contains_null{false};
    // Hash membership resolves equality through Value::operator==, which is
    // exact only for pairs in the same comparison domain (type tag plus
    // unsigned tag).  A mixed-domain probe (an INT64 test against a DOUBLE
    // build) must fall back to the rescan path, whose canonical `=`
    // promotion decides it; this sample records the build's domain.
    bool uniform_domain{true};
    bool has_domain{false};
    Value domain_sample;
  };
  // Membership builds keyed by the kQueryExp AST node pointer: the node's
  // owning statement is alive in the executor alongside this runtime, so the
  // pointer is stable for the execution and a repeat probe skips the
  // fingerprint-key string rebuild entirely (Q18 pays it per outer row).
  std::unordered_map<const void*, UncorrelatedMembership>
      uncorrelated_membership_by_node;
  // Projected scalar results of uncorrelated scalar subqueries, keyed by the
  // kQueryExp node pointer (same liveness contract as the membership map).
  // A scalar subquery re-enters the evaluator once per outer row (Q22:
  // 61128 probes of two AVG subqueries); the value is invariant across
  // those probes, so one map hit replaces the whole per-row chain.
  std::unordered_map<const void*, Value> uncorrelated_scalar_by_node;
  // Results for correlated subqueries which cannot use the specialized
  // single-table equality index.
  std::unordered_map<std::string, RelationPtr> correlated_apply_results;
  std::unordered_set<const SelectStatement*> noncacheable_queries;
  // StatementContainsVolatileFunctionCached memo.  AST nodes probed through
  // this map are all pinned by retained_statements (see its comment), so
  // address keys cannot be recycled mid-execution.
  std::unordered_map<const SelectStatement*, bool> volatile_memo;
  // Keep-alives for every subquery statement whose address keys the caches
  // above.  Prepared executors are freed between statements, and a later
  // allocation could otherwise reuse a dead statement's address, silently
  // binding stale cache entries to an unrelated subquery.
  std::vector<std::shared_ptr<const SelectStatement>> retained_statements;
  // Fingerprint recorded when a cache entry was created; a mismatch on probe
  // means the address was recycled by an unrelated statement and the entry
  // must be discarded.
  std::unordered_map<const SelectStatement*, std::string> cache_fingerprints;
  // Per-execution memos for the two structural-string rebuilds the subquery
  // caches pay per outer-row probe:
  //  - SelectStatement::Fingerprint() walks the whole statement AST into an
  //    ostringstream on every CacheEntryIsCurrent call (Q22: ~61K probes of
  //    one cached subquery; Q4: ~57K correlated probes).
  //  - UncorrelatedCacheKey() likewise walks the AST per probe of the
  //    uncorrelated fingerprint cache.
  // Every statement reaching these probes is retained for this runtime via
  // retained_statements and the AST is immutable while one execution runs,
  // so the pointer keys a stable value.
  std::unordered_map<const SelectStatement*, std::string> fingerprint_memo;
  std::unordered_map<const SelectStatement*, std::string> uncorrelated_key_memo;
  size_t correlated_index_builds{0};
  size_t prebuilt_exists_index_builds{0};
  size_t prebuilt_exists_index_rows{0};
  size_t prebuilt_exists_keys{0};
  size_t correlated_index_probes{0};
  size_t correlated_result_cache_hits{0};
  size_t uncorrelated_cache_hits{0};
  size_t uncorrelated_hash_builds{0};
  size_t uncorrelated_hash_probes{0};
  size_t exists_short_circuit_queries{0};
  double scan_ms{0};
  double filter_ms{0};
  double join_ms{0};
  double project_ms{0};
  double sort_ms{0};
  size_t topn_finish_uses{0};
  size_t topn_finish_capacity{0};
  size_t topn_finish_input_rows{0};
  size_t base_scan_cache_hits{0};
  size_t aggregate_input_rows{0};
  size_t aggregate_groups{0};
  size_t aggregate_updates{0};
  double aggregate_ms{0};
  double planning_ms{0};
  double join_estimate_ms{0};
  size_t typed_parallel_aggregations{0};
  size_t typed_parallel_agg_fallbacks{0};
  // Statements whose final join step fused with their GROUP BY (the joined
  // relation was never materialized).
  size_t grouped_join_fusions{0};
  // Probe table scanned directly into groups without a materialized input.
  size_t streamed_grouped_joins{0};
  size_t selective_index_scans{0};
  size_t selective_index_rows{0};
  size_t scan_rows{0};
  size_t scan_output_rows{0};
  size_t scan_values_decoded{0};
  size_t scan_values_available{0};
  size_t relation_spills{0};
  // Relational inner hash joins that keep a resident build and may stream a
  // spilled probe. Counts aggregate across the statement's join steps.
  size_t inner_hash_build_rows{0};
  size_t inner_hash_index_estimate_bytes{0};
  size_t inner_hash_left_builds{0};
  size_t inner_hash_spilled_probe_rows{0};
  size_t key_filter_scans{0};
  size_t key_filter_keys{0};
  size_t key_filter_rejected{0};
  size_t key_filter_null_rejected{0};
  // Dynamic filter composition (Phase 2 "Dynamic Filter Join / SIP / Bloom"):
  // how many stashed filters carried a range enclosure / a Bloom part, and
  // their combined resident footprint. EXPLAIN renders these so a plan that
  // only prunes through the exact set is distinguishable from one that also
  // carries SIP.
  size_t key_filter_minmax{0};
  size_t key_filter_bloom{0};
  size_t key_filter_bytes{0};
  bool empty_build_short_circuit{false};
  bool null_aware_anti_build_contains_null{false};
  // The materialized relation a WHERE/HAVING filter is currently iterating
  // (FilterRelation in scan_filter.cpp).  A correlated-subquery build consults
  // it to derive its own correlation-key domain and prune the build scan the
  // same way a hash join prunes its probe side.  `correlated_outer_serial`
  // identifies the registration; it advances on every scope so a recycled
  // relation address can never alias a stale build context.
  const Relation* correlated_outer_relation{nullptr};
  uint64_t correlated_outer_serial{0};
  uint64_t correlated_outer_serials{0};
  size_t correlated_domain_filter_builds{0};
};

double ElapsedMs(std::chrono::steady_clock::time_point begin);

// Boxes one arena row-range into Rows for consumers that cannot stream cells
// (see BaseRelationArena).  `rows` must already be resized to arena.rows;
// callers own disjoint ranges when materializing in parallel.
void MaterializeArenaRange(const BaseRelationArena& arena, const Schema& schema,
                           size_t begin, size_t end, std::vector<Row>* rows);

// A typed fill publishes base-relation caches with the arena only; this boxes
// the full row set back (once, serially) for a consumer that iterates rows.
// No-op when the cache already carries rows or has no registered arena.
void MaterializeCacheRowsIfNeeded(const std::string& cache_key,
                                  Relation* cached, ExecutionRuntime* runtime);

// Parallel variant for multi-million-row caches: boxes disjoint row ranges
// across worker threads, then marks the arena rows_boxed.
void MaterializeCacheRowsParallel(const std::string& cache_key,
                                  Relation* cached, ExecutionRuntime* runtime);

// Cache key shared by scalar/EXISTS result caching and IN membership caching.
// The inherited CTE identities are part of the key because the same query
// text can resolve a CTE name to different materialized relations at nested
// scopes.  EXISTS uses a capped execution representation, so it has its own
// mode and must never share a relation with scalar/IN consumers.
std::string UncorrelatedCacheKey(const SelectStatement& statement,
                                 const CteMap& ctes, bool exists_only);

// General correlated Apply fallback. The key contains the complete outer
// scope chain, which is conservative (it may miss reuse for irrelevant outer
// columns) but is safe for arbitrary correlated expressions.
std::string CorrelatedApplyCacheKey(const SelectStatement& statement,
                                    const Scope* outer, const CteMap& ctes,
                                    bool exists_only);

// Volatile functions make a correlated result unsafe to reuse. Stable and
// immutable functions are statement-stable and may participate in the cache.
bool StatementContainsVolatileFunction(const SelectStatement& statement);

// Memoized form for per-outer-row probes: the walk visits every function
// node of the statement AST, so repeated checks of one statement pay the
// same tree again each row.  `runtime` may be null (uncached fallback).
// Statements reaching the executor are retained via retained_statements and
// immutable during one execution, so the pointer keys a stable value.
bool StatementContainsVolatileFunctionCached(ExecutionRuntime* runtime,
                                             const SelectStatement& statement);

void CountStatementTables(const SelectStatement& statement,
                          std::unordered_map<std::string, size_t>* counts);

void EnsureReusableProjections(TransactionContext& context,
                               ExecutionRuntime* runtime);

const std::vector<slot_t>* ReusableProjection(TransactionContext& context,
                                              std::string_view table);

// Registers `relation` as the outer context for correlated-subquery builds
// while alive.  FilterRelation holds this around its row loop, so a build
// triggered by one of the filter's predicates can read the complete domain of
// outer values its probes will ever present (sound pruning, exactly like a
// hash join's build-side key domain).  Nested scopes restore the previous
// registration.
class ScopedCorrelatedOuter {
 public:
  ScopedCorrelatedOuter(TransactionContext& context, const Relation& relation);
  ~ScopedCorrelatedOuter();
  ScopedCorrelatedOuter(const ScopedCorrelatedOuter&) = delete;
  ScopedCorrelatedOuter& operator=(const ScopedCorrelatedOuter&) = delete;
  ScopedCorrelatedOuter(ScopedCorrelatedOuter&&) = delete;
  ScopedCorrelatedOuter& operator=(ScopedCorrelatedOuter&&) = delete;

 private:
  TransactionContext* context_;
  const Relation* saved_relation_;
  uint64_t saved_serial_;
};

// Builds a sound pre-decode domain filter for one correlation equality
// (`source_schema[local_column] = outer_column`) from the distinct non-NULL
// values of `outer_column` in the registered outer relation.  Returns
// std::nullopt when no outer context is registered, the outer column does not
// resolve there, the correlation column is not INT64/DATE, the domain is too
// large to be worth extracting, or `local_column` does not map into
// `projection` (the filter binds by projection position).  The filter column
// indexes the projection layout, matching the LoadSource contract.
std::optional<ScanRuntimeFilter> CorrelationDomainFilter(
    const Schema& source_schema, slot_t local_column,
    const ColumnName& outer_column, const std::vector<slot_t>& projection,
    const ExecutionRuntime& runtime);

StatusOr<Relation> ExecuteQuery(TransactionContext& context,
                                const SelectStatement& statement,
                                const Scope* outer,
                                const CteMap& inherited_ctes);

StatusOr<Relation> ExecuteRecursiveCte(
    TransactionContext& context, const std::string& name,
    const SelectStatement& body, const Scope* outer,
    const CteMap& inherited_ctes,
    const RecursiveDepthSpec* depth_spec = nullptr);

// Every table name a query tree reads, following nested WITH scope: a name an
// inner WITH declares shadows the outer one, so it is not a reference to the
// outer CTE.  The scan is deep (derived tables, scalar subqueries,
// set-operation branches, nested WITH bodies) because the WITH dependency
// order needs every reference, not just the FROM list -- a CTE that only
// names a sibling inside a subquery depends on it just the same.
//
// A non-recursive entry is not visible inside its own definition (a self
// reference there resolves to the enclosing WITH, or to a catalog table), so
// descending into a WITH body drops that entry's own name from the scope.
void CollectStatementTableRefs(const SelectStatement& statement,
                               const std::unordered_set<std::string>& shadowed,
                               std::unordered_set<std::string>* names);

// Names one WITH entry's body reads from the enclosing scope, i.e. the
// entries it depends on.  Siblings count (the list resolves in dependency
// order), the entry's own name does not: a self reference either reads the
// work table (WITH RECURSIVE) or resolves outward, never to itself.
void CollectCteBodyOuterRefs(const SelectStatement& statement,
                             const std::string& entry_name,
                             std::unordered_set<std::string>* names);

// Reports a WITH list whose entries wait on each other; `names` are the
// entries that never became resolvable.
Status CteCycleStatus(std::vector<std::string> names);

// Checks one WITH scope: an entry may reference a sibling declared before or
// after it (the list is resolved in dependency order, not declaration order),
// and a WITH RECURSIVE entry may reference itself, but a cycle among distinct
// entries (a mutual reference) can never be evaluated.  Returns kSuccess when
// every entry is resolvable.
Status ValidateCteDependencies(const SelectStatement& statement);

// Applies one safe top-level derived-table boundary rewrite.  The relational
// executor repeatedly calls this until it reaches a fixed point; EXPLAIN uses
// the same helper so its displayed shape matches the executable query shape.
std::shared_ptr<SelectStatement> OptimizeDerivedBoundaries(
    const SelectStatement& statement, const CteMap& inherited_ctes);

StatusOr<Relation> FinishQuery(TransactionContext& context,
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
    const Scope& outer, const CteMap& ctes, bool exists_only = false,
    std::optional<bool>* exists_answer = nullptr,
    std::optional<Value>* scalar_answer = nullptr);

const Relation* ExecuteCachedUncorrelated(TransactionContext& context,
                                          const SelectStatement& statement,
                                          const CteMap& ctes,
                                          bool exists_only = false);

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
