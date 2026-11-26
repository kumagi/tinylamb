/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tinylamb::execution_strategies {

// ============================================================================
// 1. FrameIndexes
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/ExecutionStrategies/FrameIndexes.lean
// ============================================================================

struct SparseMinimumTable {
  std::vector<std::vector<int64_t>> st;
  void Build(const std::vector<int64_t>& xs);
  [[nodiscard]] int64_t Query(size_t l, size_t r) const;  // [l, r), l < r
};

struct PrefixNullableSum {
  std::vector<int64_t> prefix_sum;
  std::vector<size_t> prefix_count;
  void Build(const std::vector<std::optional<int64_t>>& xs);
  // Returns (sum, non_null_count) over [l, r).
  [[nodiscard]] std::pair<int64_t, size_t> Query(size_t l, size_t r) const;
};

// Monotonic-deque O(n) sliding-window minimum over window width `w > 0`.
[[nodiscard]] std::vector<int64_t> SlidingMinimum(
    const std::vector<int64_t>& xs, size_t w);

// Exact NTILE(m) bucket index (0-based) for row `i` in `n` rows (`m > 0`).
[[nodiscard]] size_t TileArithmetic(size_t n, size_t m, size_t i);

// Count session boundaries across sorted timestamps where `ts[i] - ts[i-1] >
// gap`.
[[nodiscard]] size_t SessionBoundaryCount(const std::vector<int64_t>& ts,
                                          int64_t gap);

// PeerRankRuns: given (value, run_length) pairs, returns (1-based RANK,
// 1-based DENSE_RANK) per run.
[[nodiscard]] std::vector<std::pair<size_t, size_t>> PeerRankRuns(
    const std::vector<std::pair<int64_t, size_t>>& runs);

// ExcludePeerSelect: select k-th element from `xs` excluding peer range [a, b).
[[nodiscard]] std::optional<int64_t> ExcludePeerSelect(
    const std::vector<int64_t>& xs, size_t a, size_t b, size_t k);

// DisjointRangeFold: associative range string concatenation over [l, r).
[[nodiscard]] std::string DisjointRangeFold(const std::vector<std::string>& xs,
                                            size_t l, size_t r);

// WaveletQuantile: k-th smallest (0-based) element in xs[l..r).
[[nodiscard]] int64_t WaveletQuantile(const std::vector<uint64_t>& xs, size_t l,
                                      size_t r, size_t k);

// ============================================================================
// 2. CorrelatedProbes
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/ExecutionStrategies/CorrelatedProbes.lean
// ============================================================================

// RelationalDivision: returns true iff every non-null element of `need` appears
// in `supplied`.
[[nodiscard]] bool RelationalDivision(
    const std::vector<std::optional<int64_t>>& need,
    const std::vector<std::optional<int64_t>>& supplied);

// OffsetScalar: returns the k-th row's value (0-based) matching key `d`.
[[nodiscard]] std::optional<int64_t> OffsetScalar(
    const std::vector<std::pair<uint64_t, std::optional<int64_t>>>& rows,
    uint64_t d, size_t k);

// ExcludingMinimum: prefix/suffix minimum excluding index `i`.
[[nodiscard]] std::optional<int64_t> ExcludingMinimum(
    const std::vector<int64_t>& xs, size_t i);

// CountDistinctStop: returns min(k, count_distinct(non-null xs)) stopping early
// once `k` distinct values are observed.
[[nodiscard]] size_t CountDistinctStop(
    const std::vector<std::optional<int64_t>>& xs, size_t k);

// NeWitnesses: O(1) two-witness summary for `EXISTS (y IN ys : y != x)`.
struct NeWitnessSummary {
  bool has_first{false};
  int64_t first{0};
  bool has_second{false};
  int64_t second{0};
  void Build(const std::vector<std::optional<int64_t>>& ys);
  [[nodiscard]] bool HasNotEqual(std::optional<int64_t> x) const noexcept;
};

// OrExists: filters `(k1, k2)` rows matching `k1 IN a OR k2 IN b`.
[[nodiscard]] std::vector<std::pair<uint64_t, uint64_t>> OrExistsFilter(
    const std::vector<std::pair<uint64_t, uint64_t>>& xs,
    const std::vector<uint64_t>& a, const std::vector<uint64_t>& b);

// PrefixAntichain: removes redundant prefixes (if p1 is a prefix of p2, p2 is
// redundant under disjunctive `LIKE 'p1%' OR LIKE 'p2%'`).
[[nodiscard]] std::vector<std::string> PrefixAntichain(
    std::vector<std::string> prefixes);

// ============================================================================
// 3. IndexPreimages
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/ExecutionStrategies/IndexPreimages.lean
// ============================================================================

struct IntInterval {
  int64_t lo{0};
  int64_t hi{0};
  bool empty{false};
  bool operator==(const IntInterval&) const = default;
};

// TruncBucket: exact integer preimage of `x / d == q` (truncation toward zero,
// d > 0).
[[nodiscard]] IntInterval TruncBucketPreimage(int64_t d, int64_t q);

// RoundedBucket: exact integer preimage of floor-rounded `(n + d/2) / d == k`
// (`d > 0`).
[[nodiscard]] IntInterval RoundedBucketPreimage(int64_t d, int64_t k);

// ModularLookup: returns `x mod m` solving `(a * x + bias) ≡ target (mod m)`
// given modular inverse `inv` of `a` mod `m` (`m > 0`).
[[nodiscard]] int64_t ModularLookup(int64_t m, int64_t inv, int64_t bias,
                                    int64_t target);

// DescendingBand: exact preimage of `lo <= (bias - x) / d <= hi` (`d > 0`,
// Euclidean floor division).
[[nodiscard]] IntInterval DescendingBandPreimage(int64_t lo, int64_t hi,
                                                 int64_t bias, int64_t d);

// CheckedDistanceBand: exact preimage of `|x - center| <= rad` (overflow-safe).
[[nodiscard]] IntInterval CheckedDistanceBand(int64_t center, int64_t rad);

// DistanceRing: exact preimage of `l <= |x - o| <= h` (`0 <= l <= h`) as 2
// disjoint intervals `[o - h, o - l]` and `[o + l, o + h]`.
[[nodiscard]] std::pair<IntInterval, IntInterval> DistanceRingPreimage(
    int64_t origin, int64_t l, int64_t h);

// CursorRanges: keyset pagination `(a, b) > (cur_a, cur_b)` filter.
[[nodiscard]] std::vector<std::pair<int64_t, int64_t>> CursorRangeFilter(
    const std::vector<std::pair<int64_t, int64_t>>& rows,
    std::pair<int64_t, int64_t> cursor);

// MortonSquare: 2D Z-order (Morton) encoding + aligned square block range.
[[nodiscard]] uint64_t MortonEncode2D(uint32_t x, uint32_t y) noexcept;
[[nodiscard]] std::pair<uint64_t, uint64_t> MortonSquareRange(
    uint32_t x, uint32_t y, uint32_t shift_bits) noexcept;

// PeriodicSeek: smallest `x >= lo` with `x <= hi` and `x ≡ phase (mod period)`.
[[nodiscard]] std::optional<int64_t> PeriodicSeek(int64_t lo, int64_t hi,
                                                  int64_t period,
                                                  int64_t phase);

// ============================================================================
// 4. OrderedStreams
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/ExecutionStrategies/OrderedStreams.lean
// ============================================================================

// MergeDiagonal: O(log(min(|a|, |b|))) exact merge-path partition `(i, j)` such
// that `i + j = n` and the first `n` elements of `merge(a, b)` are `a[0..i) ∪
// b[0..j)`.
[[nodiscard]] std::pair<size_t, size_t> MergeDiagonal(
    const std::vector<int64_t>& a, const std::vector<int64_t>& b, size_t n);

// MonotoneProbes: galloping/monotone pointer lookup for sorted `ks` in sorted
// `rows`, returning indices of first occurrence (or `std::nullopt`).
[[nodiscard]] std::vector<std::optional<size_t>> MonotoneProbes(
    const std::vector<int64_t>& ks, const std::vector<int64_t>& rows);

// WeightedOffset: find value at 0-based position `k` in RLE `(value, count)`.
[[nodiscard]] std::optional<int64_t> WeightedOffset(
    const std::vector<std::pair<int64_t, size_t>>& runs, size_t k);

// RangeCoalescing: coalesce sorted `keys` within gap `g` into `[lo, hi]` spans.
[[nodiscard]] std::vector<std::pair<uint64_t, uint64_t>> RangeCoalescing(
    const std::vector<uint64_t>& sorted_keys, uint64_t gap);

// RunIntersection: O(|a| + |b|) intersection of two sorted disjoint interval
// lists `[lo, hi)`.
[[nodiscard]] std::vector<std::pair<int64_t, int64_t>> RunIntersection(
    const std::vector<std::pair<int64_t, int64_t>>& a,
    const std::vector<std::pair<int64_t, int64_t>>& b);

// ============================================================================
// 5. PairAggregates
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/ExecutionStrategies/PairAggregates.lean
// ============================================================================

// AbsoluteDistanceSum: O(n log n) exact `sum_{i, j} |xs[i] - xs[j]|`.
[[nodiscard]] int64_t AbsoluteDistanceSum(std::vector<int64_t> xs);

// SquaredDistanceSum: O(n + m) exact `sum_{x in xs, y in ys} (x - y)^2`.
[[nodiscard]] int64_t SquaredDistanceSum(const std::vector<int64_t>& xs,
                                         const std::vector<int64_t>& ys);

// ResidualInequalityCount: O((n+m) log m) count of `(l, r)` with `l + r <= t`.
[[nodiscard]] uint64_t ResidualInequalityCount(std::vector<int64_t> l,
                                               std::vector<int64_t> r,
                                               int64_t t);

// SameGroupPairsCount: exact count of unordered pairs `{i < j}` sharing key.
[[nodiscard]] uint64_t SameGroupPairsCount(const std::vector<int64_t>& keys);

// ComplementProbeCount: exact count of `(x, y)` with `x + y == target`.
[[nodiscard]] uint64_t ComplementProbeCount(const std::vector<int64_t>& xs,
                                            const std::vector<int64_t>& ys,
                                            int64_t target);

// IncreasingTriplesCount: O((|a|+|b|+|c|) log) exact count of `a < b < c`.
[[nodiscard]] uint64_t IncreasingTriplesCount(std::vector<int64_t> a,
                                              const std::vector<int64_t>& b,
                                              std::vector<int64_t> c);

// XorDistanceSum: O(n + m) exact bit-plane `sum_{x in xs, y in ys} (x ^ y)`.
[[nodiscard]] uint64_t XorDistanceSum(const std::vector<uint8_t>& xs,
                                      const std::vector<uint8_t>& ys);

// LayerCakeMinimumSum: O((n+m) log) exact `sum_{x in a, y in b} min(x, y)`.
[[nodiscard]] uint64_t LayerCakeMinimumSum(std::vector<uint64_t> a,
                                           std::vector<uint64_t> b);

// ============================================================================
// 6. PlaneKernels
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/ExecutionStrategies/PlaneKernels.lean
// ============================================================================

// 8-bit x 64-lane bit-sliced column (8 `uint64_t` bit planes).
struct BitPlane8x64 {
  std::array<uint64_t, 8> planes{};
  static BitPlane8x64 FromBytes(const std::array<uint8_t, 64>& vals) noexcept;
  [[nodiscard]] uint8_t At(size_t lane) const noexcept;
};

[[nodiscard]] uint64_t PlaneParallelLess(const BitPlane8x64& x,
                                         const BitPlane8x64& y,
                                         uint64_t mask) noexcept;
[[nodiscard]] BitPlane8x64 PlaneAdd(const BitPlane8x64& x,
                                    const BitPlane8x64& y) noexcept;
[[nodiscard]] uint64_t PlaneArgMinMask(const BitPlane8x64& x,
                                       uint64_t mask) noexcept;
[[nodiscard]] uint8_t PlaneRadixSelect(const BitPlane8x64& x, uint64_t mask,
                                       size_t k) noexcept;
[[nodiscard]] uint64_t PlaneDot(const BitPlane8x64& x, const BitPlane8x64& y,
                                uint64_t mask) noexcept;
[[nodiscard]] BitPlane8x64 PlaneMedianOfThree(const BitPlane8x64& x,
                                              const BitPlane8x64& y,
                                              const BitPlane8x64& z) noexcept;
[[nodiscard]] uint64_t PlaneRunBoundary(const BitPlane8x64& x, uint8_t v,
                                        uint64_t mask) noexcept;

// ============================================================================
// 7. EncodedBlocks
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/ExecutionStrategies/EncodedBlocks.lean
// ============================================================================

// PatchedRange: PFOR range filter over base + uint8 deltas + exception patches.
[[nodiscard]] std::vector<size_t> PatchedRangeFilter(
    int64_t base, int64_t lo, int64_t hi, const std::vector<uint8_t>& deltas,
    const std::unordered_map<size_t, int64_t>& patches);

// OverlapDot: O(|a| + |b|) inner product of two RLE vectors `(value, length)`.
[[nodiscard]] int64_t OverlapDot(
    const std::vector<std::pair<int64_t, size_t>>& a,
    const std::vector<std::pair<int64_t, size_t>>& b);

// RangeEdits: O(n + |edits|) difference-array batch range additions `[l, r)`.
[[nodiscard]] std::vector<int64_t> ApplyRangeEdits(
    std::vector<int64_t> xs,
    const std::vector<std::tuple<size_t, size_t, int64_t>>& edits);

// DistinctSeams: O(|pages|) distinct count across globally sorted non-empty
// pages (given each page's internal distinct count + first/last values).
[[nodiscard]] size_t DistinctSeamsCount(
    const std::vector<std::vector<int64_t>>& sorted_pages);

// SecondDifferencesDecode: reconstructs values from `(p0, p1, d2...)`.
[[nodiscard]] std::vector<int64_t> SecondDifferencesDecode(
    int64_t p0, int64_t p1, const std::vector<int64_t>& d2);

// ============================================================================
// 8. SearchBounds
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/ExecutionStrategies/SearchBounds.lean
// ============================================================================

// CollisionBound: Cauchy-Schwarz join size upper bound `sqrt(sum(fa^2) *
// sum(fb^2))` (returned as ceiling uint64_t).
[[nodiscard]] uint64_t CollisionJoinBound(const std::vector<int64_t>& a,
                                          const std::vector<int64_t>& b);

// TriangleBound: AGM / Loomis-Whitney upper bound `floor(sqrt(|R|*|S|*|T|))`.
[[nodiscard]] uint64_t TriangleAglBound(size_t nr, size_t ns, size_t nt);

// FrequentCandidates: Misra-Gries heavy hitters sketch with `slots` counters.
[[nodiscard]] std::unordered_map<int64_t, size_t> MisraGriesCandidates(
    const std::vector<int64_t>& xs, size_t slots);

// CappedJoin: exact `min(cap, join_fanout(a, b))` stopping early once `cap`
// matches are accounted for.
[[nodiscard]] size_t CappedJoinCount(const std::vector<int64_t>& a,
                                     const std::vector<int64_t>& b, size_t cap);

// PredicateExchange: returns true iff evaluating predicate A (cost ca,
// pass_count sa) before predicate B (cost cb, pass_count sb) over `n` rows has
// lower or equal expected cost.
[[nodiscard]] bool PredicateExchangePreferFirst(uint64_t n, uint64_t ca,
                                                uint64_t sa, uint64_t cb,
                                                uint64_t sb) noexcept;

// ============================================================================
// 9. DeltaMaintenance
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/ExecutionStrategies/DeltaMaintenance.lean
// ============================================================================

using KeyBag = std::unordered_map<int64_t, int64_t>;

// JoinBatch: `Δ(A ⋈ B) = ΔA ⋈ B + A ⋈ ΔB + ΔA ⋈ ΔB` on key multiplicities.
[[nodiscard]] KeyBag DeltaJoinBatch(const KeyBag& a, const KeyBag& b,
                                    const KeyBag& da, const KeyBag& db);

// OuterPadding: incremental LEFT JOIN multiplicity maintenance for delta `db`.
[[nodiscard]] KeyBag DeltaOuterPadding(const KeyBag& a, const KeyBag& b,
                                       const KeyBag& db);

// MinimumRepair: O(log n) incremental minimum maintenance; returns `nullopt` if
// a full rescan is required (the previous minimum was deleted and no smaller
// insert replaced it).
[[nodiscard]] std::optional<int64_t> DeltaMinimumRepair(
    int64_t old_min, size_t old_min_mult, const std::vector<int64_t>& dels,
    const std::vector<int64_t>& adds);

// ============================================================================
// 10. RecursiveJoins
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/ExecutionStrategies/RecursiveJoins.lean
// ============================================================================

// MeetFrontiers: bidirectional BFS shortest-path existence check in directed
// graph `adj`.
[[nodiscard]] bool MeetFrontiersReachable(
    size_t n, const std::vector<std::pair<size_t, size_t>>& edges, size_t s,
    size_t t);

// BinaryLifting: O(log k) k-th functional jump table (`f[u] = next node`).
struct BinaryLiftingTable {
  std::vector<std::vector<size_t>> up;
  void Build(const std::vector<size_t>& next, size_t max_log);
  [[nodiscard]] size_t Jump(size_t s, uint64_t k) const;
};

// ButterflyCount: exact 4-cycle (butterfly) count in a bipartite graph
// `(left_u, right_v)`.
[[nodiscard]] uint64_t ButterflyCount(
    size_t n_left, size_t n_right,
    const std::vector<std::pair<size_t, size_t>>& edges);

// TriangleOrientation: Chiba-Nishizeki degree-oriented exact triangle count in
// an undirected simple graph.
[[nodiscard]] uint64_t TriangleOrientationCount(
    size_t n, const std::vector<std::pair<size_t, size_t>>& edges);

// ============================================================================
// 11. VersionedIndexes
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/ExecutionStrategies/VersionedIndexes.lean
// ============================================================================

struct VersionedEntry {
  int64_t key{0};
  uint64_t seq{0};
  bool is_tombstone{false};
  int64_t value{0};
};

// SnapshotAnchor: visible value for `key` at snapshot `t` from entries sorted
// by `(key ASC, seq DESC)`.
[[nodiscard]] std::optional<int64_t> SnapshotAnchorLookup(
    const std::vector<VersionedEntry>& ordered_entries, int64_t key,
    uint64_t t);

// RangeTombstone: visible value for `key` at snapshot `t` respecting range
// tombstones `(lo, hi, seq)` where `[lo, hi]` with `seq <= t` deletes older
// writes.
struct RangeTombstoneMark {
  int64_t lo{0};
  int64_t hi{0};
  uint64_t seq{0};
};
[[nodiscard]] std::optional<int64_t> RangeTombstoneLookup(
    const std::vector<VersionedEntry>& ordered_entries,
    const std::vector<RangeTombstoneMark>& tombstones, int64_t key, uint64_t t);

// BottomTombstoneGC: drops tombstones with `seq <= oldest_active_txn` at the
// bottom LSM level, preserving visibility for all snapshots `t >=
// oldest_active_txn`.
[[nodiscard]] std::vector<VersionedEntry> BottomTombstoneGC(
    const std::vector<VersionedEntry>& ordered_entries,
    uint64_t oldest_active_txn);

// DominatedRunCompact: drops shadowed older versions of the same key with
// `seq <= oldest_active_txn` when a newer entry with `seq' <=
// oldest_active_txn` already dominates it.
[[nodiscard]] std::vector<VersionedEntry> DominatedRunCompact(
    const std::vector<VersionedEntry>& ordered_entries,
    uint64_t oldest_active_txn);

// ============================================================================
// 12. SpillScheduling (Frontiers/SpillScheduling.lean, SpillSchedulingLpt.lean)
// ============================================================================

// ExcessFirst (ExcessFirst_sound): orders operator spill victims by descending
// memory excess to minimize the number of victims spilled to recover `deficit`
// bytes. Returns the chosen victim indices.
[[nodiscard]] std::vector<size_t> ExcessFirstSpillVictims(
    const std::vector<uint64_t>& resident_bytes, uint64_t deficit_bytes);

// LargestFirst (LargestFirst_sound): Longest-Processing-Time-First (LPT)
// scheduling of `work_units` across `workers` threads, returning worker index
// per task.
[[nodiscard]] std::vector<size_t> LptAssignWorkers(
    const std::vector<uint64_t>& work_units, size_t workers);

// ============================================================================
// 13. RankedJoins (Frontiers/RankedJoins.lean, RankedJoinsNear.lean)
// ============================================================================

// WeightedSeek (WeightedSeek_sound, BoxCertificate_sound): returns the top-k
// largest sums `a[i] + b[j]` from descending sorted inputs `a`, `b` in
// O(k log k) time without materializing the Cartesian product.
[[nodiscard]] std::vector<int64_t> RankedJoinTopKSums(
    const std::vector<int64_t>& desc_a, const std::vector<int64_t>& desc_b,
    size_t k);

// NearPairs (NearPairs_sound): exact count of `(x, y)` with `|x - y| <= eps`
// (`eps >= 0`) via O((n + m) log) sorted two-pointer sweep.
[[nodiscard]] uint64_t NearPairsCount(std::vector<int64_t> a,
                                      std::vector<int64_t> b, int64_t eps);

// ============================================================================
// 14. Arithmetic & SetTransforms (Frontiers/Arithmetic.lean,
// SetTransforms.lean,
//     AnalyticsWorkloads/UserRolesAndCommentJoins.lean,
//     AnalyticalOperators/RollupsAndChannelSetCounts.lean)
// ============================================================================

// BinnedCheckedSum (sound_BinnedAccumulator, sound_FirstOverflow): sums `xs`
// in int64 with overflow checking; returns `(sum, nullopt)` on success or
// `(0, first_overflow_index)` on the exact first overflowing lane.
[[nodiscard]] std::pair<int64_t, std::optional<size_t>> CheckedSumFirstOverflow(
    const std::vector<int64_t>& xs);

// RepeatedScore (AnalyticsWorkloads/UserRolesAndCommentJoins.lean:
// sound_RepeatedScore): closed-form O(1) checked repeated addition of `v`
// starting at `acc` up to `reps` times; returns `(final_acc, completed_reps)`
// where `completed_reps < reps` iff the next addition would overflow int64.
[[nodiscard]] std::pair<int64_t, uint64_t> RepeatedScoreCheckedAdd(
    int64_t acc, int64_t v, uint64_t reps) noexcept;

// AverageFence (AnalyticalOperators/RollupsAndChannelSetCounts.lean:
// AverageFence_sound; Frontiers/Arithmetic.lean: sound_QuantizedAverage): exact
// integer cross-multiplied comparison `grp_sum / grp_cnt >= num / den` (for
// `grp_cnt > 0`, `den > 0`) using `__int128` with zero floating-point rounding
// error.
[[nodiscard]] bool ExactAverageAtLeast(int64_t grp_sum, uint64_t grp_cnt,
                                       int64_t threshold_num,
                                       uint64_t threshold_den) noexcept;

// ============================================================================
// 15. Gray encodings, tombstone epochs, and restarted-delta rank
// Atlas/EncodedPages/GrayTileOrderAndTombstoneEpochs.lean
// Atlas/EncodedPages/RestartedDeltaPredicateRank.lean
// ============================================================================

// PagedTombstones.SkipEpochs (sound_PagedTombstones_SkipEpochs): returns true
// when an entire tombstone page covering `[min_epoch, max_epoch]` can be
// skipped for a reader at `snapshot_epoch` because `min_epoch >
// snapshot_epoch`.
[[nodiscard]] inline bool CanSkipTombstoneEpochPage(
    uint64_t page_min_epoch, uint64_t snapshot_epoch) noexcept {
  return page_min_epoch > snapshot_epoch;
}

// GrayTile.PowerOfTwo (sound_GrayTile_PowerOfTwo): bijective Gray code map
// `i ^ (i >> 1)` and its exact inverse on power-of-two tile indices.
[[nodiscard]] inline uint64_t GrayTileEncode(uint64_t i) noexcept {
  return i ^ (i >> 1U);
}
[[nodiscard]] uint64_t GrayTileDecode(uint64_t g) noexcept;

// RestartedDelta.PredicateRank (sound_RestartedDelta_PredicateRank): counts
// values `<= target` in a restarted-delta encoded sorted page using restart
// anchors `[anchor_pos, anchor_val]` to skip whole restart blocks.
[[nodiscard]] size_t RestartedDeltaRankLE(
    const std::vector<int64_t>& sorted_values, size_t restart_interval,
    int64_t target);

// ============================================================================
// 16. Slice Extrema & Complement Groups
// (AnalyticalOperators/DictionarySlicesAndExtrema.lean:
//     sound_ComplementGroups, sound_WitnessedExtrema; CloudBI/PublicBI.lean:
//     sound_CodeExtrema, sound_PartialExtrema)
// ============================================================================

// ComplementGroups (sound_ComplementGroups): given the grand total sum over a
// partition and the sum of the minority subset, returns the exact complement
// group sum `grand_total - minority_sum`.
[[nodiscard]] inline int64_t ComplementGroupSum(int64_t grand_total,
                                                int64_t minority_sum) noexcept {
  return grand_total - minority_sum;
}

// WitnessedBlockExtrema (sound_WitnessedExtrema, sound_CodeExtrema): returns
// true when a block with `[blk_min, blk_max]` can be skipped while maintaining
// a running global `MIN` (`is_min=true`) or `MAX` (`is_min=false`).
[[nodiscard]] inline bool CanSkipBlockForExtremum(int64_t blk_min,
                                                  int64_t blk_max,
                                                  int64_t current_extremum,
                                                  bool is_min) noexcept {
  return is_min ? (blk_min >= current_extremum) : (blk_max <= current_extremum);
}

}  // namespace tinylamb::execution_strategies
