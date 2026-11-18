/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_COLUMNAR_TLCS_FAST_PATH_HPP
#define TINYLAMB_EXECUTOR_DETAIL_COLUMNAR_TLCS_FAST_PATH_HPP

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "common/status_or.hpp"
#include "executor/detail/typed_stream_aggregate.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/expression.hpp"
#include "query/statement.hpp"
#include "table/lstable_bridge.hpp"
#include "table/scan_options.hpp"
#include "table/table.hpp"
#include "table/tlcs_file.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
class TransactionContext;
}

namespace tinylamb::relational_detail {

struct ContainsPrecheck {
  slot_t full_col{0};
  std::string needle;
};

struct DictFilterMask {
  slot_t full_col{0};
  std::array<uint64_t, 4> allowed{};
  bool all_allowed{false};
};

struct ColumnarPredicateSet {
  std::vector<TypedFilterCompare> compares;
  std::vector<TypedInListCompare> inlists;
  std::vector<TypedNullCompare> nulls;
  std::vector<ContainsPrecheck> contains_prechecks;
  std::vector<DictFilterMask> dict_masks;
  std::vector<uint8_t> col_referenced;
  std::vector<uint8_t> col_needs_varchar_bytes;

  [[nodiscard]] bool Empty() const noexcept {
    return compares.empty() && inlists.empty() && nulls.empty();
  }
};

// Resolves a backing TLCS store only when the table and transaction have a
// completely clean read snapshot with zero uncommitted/in-place deltas.
std::shared_ptr<const TlcsFile> ResolveCleanBackingTlcs(
    const Table& table, TransactionContext& ctx) noexcept;

void FinalizePredicateMetadata(const TlcsFile& tlcs, const Schema& table_schema,
                               ColumnarPredicateSet* set);

bool CompilePredicatesFromExpressions(
    const std::vector<Expression>& predicates,
    const std::vector<IntegerPeekCompare>& full_peeks, const TlcsFile& tlcs,
    const Schema& scan_schema, std::span<const slot_t> proj_to_full,
    const Schema& table_schema, ColumnarPredicateSet* out,
    bool allow_parameter_slots = false);

StatusOr<std::vector<uint32_t>> SelectCandidateBlocks(
    const TlcsFile& tlcs, const ColumnarPredicateSet& preds);

bool RowMatchesColPtrs(
    const ColumnarPredicateSet& preds,
    const std::vector<const TlcsFile::DecodedColumnChunk*>& col_ptrs,
    uint32_t row) noexcept;

bool BlockPassesContainsPrecheck(const TlcsFile::DecodedColumnChunk& chunk,
                                 std::string_view needle) noexcept;

Value ValueFromChunkRow(const TlcsFile::DecodedColumnChunk& chunk,
                        uint32_t row);

// Streaming / block-pruned parallel columnar aggregation directly over
// `TlcsFile` blocks (avoiding `Row` materialization, buffer-pool stub walks,
// and oversized resident `LSTableStore` allocations on large tables).
// Supports standard slim/BYTE_LENGTH typed plans as well as fallback
// computed-key shapes (UTC TIMESTAMP_TRUNC / EXTRACT, simple CASE WHEN,
// and single-column constant-pattern REGEXP_REPLACE).
StatusOr<std::optional<Relation>> TryRunColumnarTlcsAggregate(
    TransactionContext& context, const SelectStatement& stmt,
    const Expression& having_expr, const Table& table,
    const std::vector<slot_t>& projection, const Schema& qualified_schema,
    const Schema& table_schema, const TypedAggregationPlan* typed_plan,
    const std::vector<const AggregateExpression*>& aggregate_expressions,
    const std::vector<Expression>& scan_predicates,
    const LSTableStore* resident_store_hint, ExecutionRuntime* runtime,
    bool publish_lstable_marker, const std::string& fold_cache_key,
    std::chrono::steady_clock::time_point scan_begin);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_COLUMNAR_TLCS_FAST_PATH_HPP
