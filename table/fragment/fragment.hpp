/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#ifndef TINYLAMB_FRAGMENT_FRAGMENT_HPP
#define TINYLAMB_FRAGMENT_FRAGMENT_HPP

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "table/memtable/row_entry.hpp"
#include "table/pax_chunk/pax_chunk.hpp"
#include "table/sorted_run/sorted_run.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// ---------------------------------------------------------------------------
// The fragment IR.
//
// A fragment is ONE pipeline that a single storage call can execute:
//
//   ArtifactSet
//     -> read generation (MVCC bound)
//     -> access (point / key range / non-key scan)
//     -> conjunctive filter
//     -> projection
//     -> aggregate (optional)
//     -> limit (optional)
//
// The SAME fragment description is handed to Estimate() and Execute(): the
// contract here forbids splitting the estimate formula from
// the execution kernel, because a scheme added on one side only would make
// the optimizer compare a cost against a capability the executor does not
// have.
//
// Nothing here is SQL. The optimizer above this layer knows the schema and
// builds fragments; this layer knows the physical layout and prices them.
// ---------------------------------------------------------------------------

// Which runtime prices and runs the fragment. The runtime is an argument
// (not a compile-time constant) because the native library and the WASM
// module expose the same contract with different kernel coverage.
enum class Runtime { kNative, kWasm };

// The capability bits. Taken once per session; a bit
// answers "does THIS build of THIS runtime have the kernel for that
// operator", never "which scheme is this column encoded with".
struct Capabilities {
  uint32_t interface_version = 1;
  Runtime runtime = Runtime::kNative;

  bool predicates_compare = true;  // col OP lit
  bool predicates_in = true;       // col IN (lit...)
  bool predicates_is_null = true;  // col IS [NOT] NULL
  // varchar compare / half-open range. The native side has the kernels
  // (VarcharFilterSelection / VarcharRangeSelection); the WASM module has
  // the COUNT-only kernels, so the bit is off there until the selection
  // export lands.
  bool predicates_varchar = true;

  bool agg_count_star = true;
  bool agg_count_col = true;
  bool agg_sum = true;  // monomial sum
  bool agg_min = true;
  bool agg_max = true;
  bool agg_approx_count_distinct = true;

  bool group_by_single_column = true;
  bool group_by_multi_column = false;  // enabled by the N-key group-by work
  bool order_limit_single_column = true;

  bool artifacts_chunk = true;
  bool artifacts_run = true;
  bool artifacts_memtable = true;
};

// The capability bits of a runtime.
[[nodiscard]] Capabilities GetCapabilities(Runtime runtime);

// Calibrate(runtime) -> ns_per_cpu_unit. One cpu_unit is
// "compare one PLAIN int64 against one literal". The factor is measured
// against a fixed PLAIN microbenchmark so the optimizer can compare a native
// fragment with a WASM one in the same unit.
[[nodiscard]] double Calibrate(Runtime runtime);

// How the fragment reaches its rows.
enum class AccessMode {
  kPoint,       // key equality (hash index / PGM / zone-map binary search)
  kKeyRange,    // closed interval on the key column
  kFilterScan,  // key order independent
};

// The comparison operators. kPrefix is the byte-prefix comparison (the
// value starts with the literal); it applies to the varchar domain only.
enum class CompareOp { kEq, kNe, kLt, kLe, kGt, kGe, kPrefix };

// The literal domain of a comparison.
enum class LiteralKind {
  kInt64,       // integer domain (decimal arrives pre-scaled by the caller)
  kDoubleBits,  // raw IEEE-754 bit pattern
  kBytes,       // byte string (varchar)
};

enum class ConjunctKind {
  kCompare,  // col OP literal
  kIn,       // col IN (lit...) -- set semantics, duplicates removed by caller
  kIsNull,   // col IS [NOT] NULL
  kVarcharRange,  // varchar col IN [lo, hi) -- the half-open range form
};

// One conjunct of the filter. The AND order carries no meaning: the storage
// side decides the evaluation order from the physical layout.
struct Conjunct {
  ConjunctKind kind = ConjunctKind::kCompare;
  slot_t col = 0;
  CompareOp op = CompareOp::kEq;
  LiteralKind literal_kind = LiteralKind::kInt64;
  int64_t int64_literal = 0;
  uint64_t double_bits = 0;
  std::string bytes_literal;
  // kIn: the literal set. Empty means "no literal", which is rejected.
  std::vector<int64_t> in_literals;
  // kVarcharRange: the upper bound of the half-open range. The lower bound
  // is `bytes_literal`. An empty `bytes_literal_hi` means "unbounded above";
  // an empty `bytes_literal` with a non-empty `bytes_literal_hi` means
  // "unbounded below".
  std::string bytes_literal_hi;
  // kIsNull: true = IS NULL, false = IS NOT NULL.
  bool negated = false;
};

// A SUM monomial (coeff * (col0 * col1 * ...)) is the chunk's own AggTerm
// (pax_chunk.hpp), so the fragment IR and the existing fused-aggregate
// kernel speak the same term shape.

enum class AggregateKind {
  kNone,
  kCountStar,
  kCountCol,
  kSum,
  kMin,
  kMax,
  kApproxCountDistinct,
};

// The aggregate to push down, if any.
struct Aggregate {
  AggregateKind kind = AggregateKind::kNone;
  slot_t col = 0;              // kCountCol / kMin / kMax / kApproxDistinct
  int64_t coeff = 1;           // kSum scalar multiplier
  std::vector<AggTerm> terms;  // kSum monomials
  int out_scale = 0;           // decimal scale of the reported value
};

// ORDER BY <single column> LIMIT k.
//
// The SCOPE of the Top-K pushdown, stated once:
//
//   IN SCOPE : `ORDER BY <one column> [ASC|DESC] LIMIT k` over ONE
//              artifact set, with no other ORDER BY column and no OFFSET.
//              The storage answers with the chunk's Top-K kernel, whose
//              block-max pruning skips the row groups that cannot improve
//              the current threshold.
//   OUT OF SCOPE (residual execution, never a wrong answer):
//              * a MULTI-column ORDER BY -- the IR has no representation
//                for it, so the caller must sort the rows itself;
//              * an OFFSET -- the IR's limit is a row COUNT, not a
//                "skip the first n" window;
//              * a GLOBAL sort of an unsorted column -- the pushdown
//                still answers it (the kernel prunes what it can), but
//                the storage offers no sortedness guarantee, so the
//                win/loss is reported per shape (sorted vs unsorted).
//
// A fragment that asks for both a Top-K and a row LIMIT is malformed:
// the two limits would have to be composed, and the IR does not define
// that composition. Execute() rejects it.
struct TopKSpec {
  slot_t col = 0;
  size_t k = 0;
  bool descending = false;
};

// One artifact the fragment reads. The pointed-to objects are NOT owned and
// must outlive the fragment's use.
struct ArtifactRef {
  enum class Kind { kChunk, kRun, kMemSnapshot };
  Kind kind = Kind::kChunk;
  const PaxChunk* chunk = nullptr;
  const SortedRun* run = nullptr;
  // MemTable::Snapshot() output (already collapsed to newest-wins per key).
  const std::vector<std::pair<std::string, RowEntry>>* mem_rows = nullptr;
  // LSM generation of the artifact. The newest generation wins on a key
  // collision; a tombstone hides the whole key group.
  uint64_t generation = 0;
  // Per-artifact schema (nullptr = the fragment's schema).
  const Schema* schema = nullptr;
};

using ArtifactSet = std::vector<ArtifactRef>;

// The fragment itself.
struct Fragment {
  AccessMode access = AccessMode::kFilterScan;
  // kPoint: the key value.
  std::optional<int64_t> point_key;
  // kKeyRange: the closed interval (either bound may be absent).
  std::optional<int64_t> range_lo;
  std::optional<int64_t> range_hi;
  // The filter. Empty means "no filter" (every row passes).
  std::vector<Conjunct> conjuncts;
  // The projection. Columns not listed are NOT read, and their bytes do not
  // enter bytes_touched. Empty with no aggregate means "no column output".
  std::vector<slot_t> projection;
  // The aggregate to push down.
  std::optional<Aggregate> aggregate;
  // GROUP BY columns. One column in v1; the N-key form is accepted when the
  // runtime reports group_by_multi_column.
  std::vector<slot_t> group_cols;
  // ORDER BY <col> LIMIT k. Mutually exclusive with limit_rows.
  std::optional<TopKSpec> top_k;
  // Row limit after the filter. Mutually exclusive with top_k.
  std::optional<size_t> limit_rows;
  // MVCC bound: artifacts with a newer generation are invisible.
  uint64_t read_version = UINT64_MAX;
};

enum class OutputKind {
  kScalar,          // aggregate value + count, no rows
  kGroups,          // group keys + aggregate values
  kSelection,       // row bitmask of the surviving rows
  kProjectedBatch,  // the projected columns, expanded
  kRowBatch,        // whole rows (runs / memtable snapshots)
  kTopK,            // (value, row index) pairs
};

enum class Cardinality { kExact, kHistogram, kNotAccounted };
enum class Coverage { kFull, kPartial, kNone };
enum class DuplicateElimination { kApplied, kNotAccounted };

// The price of a fragment.
struct CostReport {
  double startup_units = 0;
  double cpu_units = 0;
  uint64_t bytes_touched = 0;
  uint64_t rows_in = 0;
  uint64_t rows_proved_pass = 0;
  uint64_t rows_proved_fail = 0;
  uint64_t rows_unknown = 0;
  uint64_t rows_decoded = 0;
  uint64_t output_rows = 0;
  uint64_t output_bytes = 0;
  OutputKind output_kind = OutputKind::kRowBatch;
  Cardinality cardinality = Cardinality::kNotAccounted;
  Coverage coverage = Coverage::kNone;
  DuplicateElimination duplicate_elimination =
      DuplicateElimination::kNotAccounted;
  // Indices into Fragment::conjuncts of the conjuncts the storage did NOT
  // evaluate. The executor applies them to the residual rows.
  std::vector<size_t> residual_conjuncts;
};

// The classification of one conjunct, used by the optimizer's search
//. The labels are stable: adding a scheme classifies it
// under an existing label instead of inventing one.
enum class QuoteClass {
  kMetadata,        // zone maps, constant cells, closed forms, footer sums
  kIndex,           // inverted index, dictionary code window, PGM, hash
  kCompressedScan,  // run folding, brackets, bit slices, wavelet rank
  kDecodeScan,      // decode to values, then compare
  kUnsupported,     // this runtime has no kernel for it
};

struct ConjunctQuote {
  size_t index = 0;
  QuoteClass klass = QuoteClass::kUnsupported;
  double cpu_units_if_first = 0;
  double bytes_if_first = 0;
  // Fraction of row groups the conjunct can prove false, in [0, 1].
  double rg_skip_fraction = 0;
};

// Prices one conjunct as if it were the only conjunct (an upper bound:
// interactions between conjuncts are not modelled).
[[nodiscard]] std::vector<ConjunctQuote> Quote(
    Runtime runtime, const ArtifactSet& artifacts,
    const std::vector<Conjunct>& conjuncts);

// The price of the whole fragment.
[[nodiscard]] CostReport Estimate(Runtime runtime, const ArtifactSet& artifacts,
                                  const Fragment& fragment);

// One output group.
struct GroupOutput {
  std::vector<int64_t> key;
  int64_t sum = 0;  // SUM uses two's-complement int64 truncation.
  int64_t min_val = 0;
  int64_t max_val = 0;
  uint64_t count = 0;
};

// The result of executing a fragment.
struct ExecResult {
  Status status = Status::kSuccess;
  // True when the fragment could not be executed at all (hostile footer,
  // capability miss, type mismatch). The executor must then run the whole
  // fragment over the residual path; a rejection is NEVER a zero-row
  // success.
  bool rejected = false;
  Coverage coverage = Coverage::kNone;
  OutputKind output_kind = OutputKind::kRowBatch;
  uint64_t rows_proved_pass = 0;
  uint64_t rows_proved_fail = 0;
  uint64_t rows_unknown = 0;
  uint64_t rows_decoded = 0;
  uint64_t output_rows = 0;
  std::vector<size_t> residual_conjuncts;
  // The surviving rows, as a bitmask over the artifact's rows (Selection).
  std::vector<uint64_t> selection_bits;
  uint64_t selection_count = 0;
  // Scalar output (kScalar).
  int64_t scalar_value = 0;
  uint64_t scalar_count = 0;
  // Group output (kGroups).
  std::vector<GroupOutput> groups;
  // Top-K output (kTopK).
  std::vector<TopKEntry> top_k;
  // Projected output (kProjectedBatch): the values in column-major order,
  // `projected_width` columns per row.
  std::vector<int64_t> projected_values;
  std::vector<uint32_t> projected_rows;
  uint32_t projected_width = 0;
  // Row output (kRowBatch).
  std::vector<Row> rows;
};

// Executes the fragment. The artifact set and the fragment are the same
// arguments Estimate() takes, so the two cannot drift apart.
[[nodiscard]] StatusOr<ExecResult> Execute(Runtime runtime,
                                           const ArtifactSet& artifacts,
                                           const Fragment& fragment,
                                           const Schema& schema);

// The rows of an artifact set that survive a fragment's filter, projected
// onto two columns. This is the surface the join builder sits on: a JOIN
// needs the key column and one payload column of each side, materialized
// as plain arrays, not a chunk.
//
// The row indices are the SOURCE row indices (the global row index inside
// a chunk, the entry index inside a run, the entry index inside a memtable
// snapshot), so a join result can be traced back to its artifact.
struct KeyValueRows {
  std::vector<int64_t> keys;
  std::vector<int64_t> values;
  std::vector<uint8_t> nulls;
  // The source of each row: the artifact index and the row index inside it.
  std::vector<uint32_t> artifacts;
  std::vector<uint32_t> rows;
};

// Materializes `value_col` and the key column of the rows that survive the
// fragment's filter. kNotImplemented when the key column or the value
// column is not an int64 column (the v1 IR's join surface is the integer
// domain).
[[nodiscard]] StatusOr<KeyValueRows> MaterializeKeyValue(
    Runtime runtime, const ArtifactSet& artifacts, const Fragment& fragment,
    const Schema& schema, slot_t value_col);

}  // namespace tinylamb

#endif  // TINYLAMB_FRAGMENT_FRAGMENT_HPP
