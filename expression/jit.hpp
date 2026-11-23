/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXPRESSION_JIT_HPP
#define TINYLAMB_EXPRESSION_JIT_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "common/constants.hpp"

namespace tinylamb {

class BytecodeProgram;
class Schema;
struct DelimitedRegexCapture;

// Contiguous unit-stride column view for branchless JIT kernels.
// When `has_nulls` is true, `nulls` points to `count` 0/1 bytes (1 = IS NULL);
// when `has_nulls` is false, `nulls` may be nullptr and the kernel skips all
// null-flag memory loads. When `has_bounds` is true, every non-null cell in
// `values[0..count)` lies in `[min_value, max_value]` (e.g. from ZoneMap).
struct JitColumnInput {
  const int64_t* values{nullptr};
  const uint8_t* nulls{nullptr};
  bool has_nulls{false};
  bool has_bounds{false};
  int64_t min_value{0};
  int64_t max_value{0};
};

// Unpacks a bit-packed `ColumnVector::NullBitmap()` (`1ULL << (i % 64)` at
// word `i / 64`) into unit-stride `0/1` bytes so LLVM LoopVectorize emits
// contiguous vector loads instead of scalar/gather bit-shifts.
void UnpackNullBitmapToBytes(std::span<const uint64_t> bitmap, size_t count,
                             uint8_t* out_bytes) noexcept;

class JitLiteralSearch {
 public:
  [[nodiscard]] static std::optional<JitLiteralSearch> Compile(
      std::string_view needle);

  // Return the first matching byte offset, or bytes.size() on a miss.
  [[nodiscard]] size_t Find(std::span<const uint8_t> bytes) const noexcept;

 private:
  using FindFn = uint64_t (*)(const uint8_t*, uint64_t);
  JitLiteralSearch(FindFn find, size_t length) : find_(find), length_(length) {}
  FindFn find_;  // Owned by the process-lifetime ORC cache.
  size_t length_;
};

class JitDelimitedCapture {
 public:
  [[nodiscard]] static std::optional<JitDelimitedCapture> Compile(
      const DelimitedRegexCapture& capture);
  [[nodiscard]] std::string_view Replace(std::string_view input) const noexcept;

 private:
  using CaptureFn = uint64_t (*)(const char*, uint64_t);
  explicit JitDelimitedCapture(CaptureFn capture) : capture_(capture) {}
  CaptureFn capture_;  // Owned by the process-lifetime ORC cache.
};

class JitGroupHash {
 public:
  [[nodiscard]] static std::optional<JitGroupHash> Compile();
  void Hash(const int64_t* keys, uint64_t* hashes, size_t size,
            uint64_t seed) const noexcept;

 private:
  using HashFn = void (*)(const int64_t*, uint64_t*, uint64_t, uint64_t);
  explicit JitGroupHash(HashFn hash) : hash_(hash) {}
  HashFn hash_;  // Owned by the process-lifetime ORC cache.
};

class JitDatePartHash {
 public:
  [[nodiscard]] static std::optional<JitDatePartHash> Compile(unsigned offset);
  // Each start identifies a checked canonical UTC timestamp with >=19 bytes.
  void Hash(const int64_t* keys, const char* bytes, const uint32_t* starts,
            int64_t* date_parts, uint64_t* hashes, size_t size,
            uint64_t seed) const noexcept;

 private:
  using HashFn = void (*)(const int64_t*, const char*, const uint32_t*,
                          int64_t*, uint64_t*, uint64_t, uint64_t);
  explicit JitDatePartHash(HashFn hash) : hash_(hash) {}
  HashFn hash_;
};

class JitStringFingerprint {
 public:
  [[nodiscard]] static std::optional<JitStringFingerprint> Compile();
  // Approximate fingerprint. The consumer must check exact key equality.
  static uint64_t Scalar(std::string_view value, uint64_t seed) noexcept;
  void Hash(const char* bytes, size_t byte_count, const uint32_t* starts,
            const uint32_t* ends, uint64_t* hashes, size_t size,
            uint64_t seed) const noexcept;

 private:
  using HashFn = void (*)(const char*, uint64_t, const uint32_t*,
                          const uint32_t*, uint64_t*, uint64_t, uint64_t);
  explicit JitStringFingerprint(HashFn hash) : hash_(hash) {}
  HashFn hash_;
};

class JitInt64Kernels {
 public:
  JitInt64Kernels(const JitInt64Kernels&) = delete;
  JitInt64Kernels& operator=(const JitInt64Kernels&) = delete;
  struct Impl;
  using FilterFn = void (*)(const int64_t*, uint8_t*, uint64_t, int64_t);
  using ProjectionFn = void (*)(const int64_t*, int64_t*, uint64_t, int64_t,
                                int64_t);
  using SumFn = int64_t (*)(const int64_t*, uint64_t);
  using MaskedSumFn = int64_t (*)(const int64_t*, const uint8_t*, uint64_t);
  using FilterProjectSumFn = int64_t (*)(const int64_t*, const int64_t*,
                                         uint64_t, int64_t, int64_t, int64_t,
                                         int64_t, uint64_t*);
  using FilterHashProbeFn = size_t (*)(const int64_t*, const int64_t*, size_t,
                                       int64_t, const size_t*, const int64_t*,
                                       size_t, uint64_t*, size_t*, size_t*);
  // Overflow-checked variants: the trailing out-params report whether any
  // lane wrapped, so callers can throw exactly like the AST evaluator
  // instead of silently returning wrapped values.
  using SumCheckedFn = int64_t (*)(const int64_t*, uint64_t, uint8_t*);
  using ProjectionCheckedFn = void (*)(const int64_t*, int64_t*, uint64_t,
                                       int64_t, int64_t, uint8_t*, uint8_t*);

  static std::optional<JitInt64Kernels> CompileFilter(BinaryOperation op);
  static std::optional<JitInt64Kernels> CompileProjection();
  static std::optional<JitInt64Kernels> CompileSum();
  static std::optional<JitInt64Kernels> CompileMaskedSum();
  static std::optional<JitInt64Kernels> CompileFilterProjectSum(
      BinaryOperation op, bool skip_rejected_loads = false);
  static std::optional<JitInt64Kernels> CompileFilterHashProbe(
      BinaryOperation op);
  static std::optional<JitInt64Kernels> CompileProjectionChecked();
  static std::optional<JitInt64Kernels> CompileSumChecked();
  static void WarmUpAll();

  // Lean proof: lean/TinylambOpt/BranchlessJitCodegen.lean
  //   (zonemap_prefix_sum_masked_sound)
  // Returns true iff `running_total + prefix_sum` is guaranteed to remain in
  // `[INT64_MIN, INT64_MAX]` at EVERY intermediate row of a `count`-row batch
  // whose non-null values lie in `[min_value, max_value]`.
  [[nodiscard]] static bool CanUseUncheckedSum(int64_t running_total,
                                               int64_t min_value,
                                               int64_t max_value,
                                               size_t count) noexcept;

  JitInt64Kernels(JitInt64Kernels&&) noexcept;
  JitInt64Kernels& operator=(JitInt64Kernels&&) noexcept;
  ~JitInt64Kernels();

  // Lifetime contract: the returned function pointers point into code owned
  // by process-lifetime caches inside the JIT; they remain valid as long as
  // no cache eviction is introduced and must not be called during/after
  // process shutdown.
  void Filter(const int64_t* input, uint8_t* output, size_t count,
              int64_t constant) const;
  void Project(const int64_t* input, int64_t* output, size_t count,
               int64_t multiplier, int64_t addend) const;
  [[nodiscard]] int64_t Sum(const int64_t* input, size_t count) const;
  [[nodiscard]] int64_t MaskedSum(const int64_t* input, const uint8_t* nulls,
                                  size_t count) const;
  // Non-null signed columns. The caller proves the affine intermediates
  // and running SUM prefixes fit INT64 before calling this unchecked loop.
  [[nodiscard]] int64_t FilterProjectSum(const int64_t* filter,
                                         const int64_t* input, size_t count,
                                         int64_t constant, int64_t multiplier,
                                         int64_t addend, int64_t initial,
                                         uint64_t* selected) const;
  // Cursor starts at zero and retains the block end and its pending mask.
  // Return the first matching chain head and its row, or SIZE_MAX on
  // exhaustion. count must fit in the upper 56 bits of the cursor.
  // *accepted reports the number of predicate hits consumed by this call.
  [[nodiscard]] size_t FilterHashProbe(const int64_t* filter,
                                       const int64_t* keys, size_t count,
                                       int64_t constant, const size_t* heads,
                                       const int64_t* slot_keys,
                                       size_t slot_mask, uint64_t* cursor,
                                       size_t* row, size_t* accepted) const;
  [[nodiscard]] static bool CanUseFilterProjectSum(
      int64_t min_value, int64_t max_value, int64_t multiplier, int64_t addend,
      int64_t initial, size_t count) noexcept;
  void ProjectChecked(const int64_t* input, int64_t* output, size_t count,
                      int64_t multiplier, int64_t addend,
                      bool* multiply_overflowed, bool* add_overflowed) const;
  [[nodiscard]] int64_t SumChecked(const int64_t* input, size_t count,
                                   bool* overflowed) const;
  [[nodiscard]] double CompileMilliseconds() const;

 private:
  explicit JitInt64Kernels(std::shared_ptr<Impl> impl);
  std::shared_ptr<Impl> impl_;
};

// Proof-backed general branchless INT64 bytecode JIT compiler.
//
// Lowers arbitrary forward-jumping signed-INT64 `BytecodeProgram` DAGs
// (including short-circuit `AND`/`OR`, `Searched CASE WHEN`, `COALESCE`,
// `IN (...)`, `IS [NOT] NULL/TRUE/FALSE`, `IS [NOT] DISTINCT FROM`,
// `ABS`, unary `-`/`~`/`NOT`, `+`, `-`, `*`, `%`, and all 6 comparisons)
// into a SINGLE straight-line loop-body basic block using predicated SSA
// `select` merges and trap-sanitized speculative arithmetic.
//
// Lean proofs: lean/TinylambOpt/BranchlessJitCodegen.lean
//   - `jit_3vl_lane_sound`
//   - `branchlessAbsBV_exact`
//   - `safeDivisorBV_never_traps_and_exact`
//   - `jitCheckedAdd_null_garbage_immune`
//   - `if_conversion_sound_and_complete`
//   - `zonemap_interval_mul_4corners_sound`
//   - `zonemap_interval_add_sub_abs_neg_sound`
class JitBranchlessProgram {
 public:
  struct Impl;
  JitBranchlessProgram(const JitBranchlessProgram&) = delete;
  JitBranchlessProgram& operator=(const JitBranchlessProgram&) = delete;
  JitBranchlessProgram(JitBranchlessProgram&&) noexcept;
  JitBranchlessProgram& operator=(JitBranchlessProgram&&) noexcept;
  ~JitBranchlessProgram();

  [[nodiscard]] static std::optional<JitBranchlessProgram> Compile(
      const BytecodeProgram& program, const Schema& schema);

  // Ordered, deduplicated schema column indices read by this program.
  // Callers pass `columns` of exact length `ReferencedColumns().size()` in
  // this order to `ProveOverflowFree` / `Evaluate` / `Filter`.
  [[nodiscard]] const std::vector<uint16_t>& ReferencedColumns() const noexcept;

  // Batch-level O(1) interval range check (ZoneMap -> pure `nsw` SIMD upgrade).
  [[nodiscard]] bool ProveOverflowFree(
      std::span<const JitColumnInput> columns) const;

  // Evaluates `count` rows branchlessly. Returns `true` iff no reachable
  // subexpression raised an arithmetic/domain error on any row; when `false`,
  // at least one row genuinely raises an error under sequential short-circuit
  // evaluation and the caller should re-run the cold per-row path to report
  // the exact first-row `Status`.
  [[nodiscard]] bool Evaluate(std::span<const JitColumnInput> columns,
                              size_t count, int64_t* out_values,
                              uint8_t* out_nulls, bool* out_has_any_null,
                              bool* out_used_unchecked = nullptr) const;

  // Predicate-specialized path (`WHERE` / `Selection`): writes `0/1` keep
  // bytes (`!is_null && bits != 0`) directly without materializing `i64`
  // output payloads or null bytes. Returns `false` iff at least one row
  // raised a non-short-circuited error.
  [[nodiscard]] bool Filter(std::span<const JitColumnInput> columns,
                            size_t count, uint8_t* out_keep,
                            bool* out_used_unchecked = nullptr) const;

  [[nodiscard]] double CompileMilliseconds() const noexcept;
  [[nodiscard]] bool HasUncheckedFastPath() const noexcept;

 private:
  explicit JitBranchlessProgram(std::shared_ptr<Impl> impl);
  std::shared_ptr<Impl> impl_;
};

// Shared SplitMix64 finalizer for packed 2x32 keys so C++ hash-table/radix
// builders and LLVM ORC JIT kernels always agree on slot and bloom bits.
[[nodiscard]] inline uint64_t HashPacked2x32Key(uint64_t k) noexcept {
  uint64_t h = (k ^ (k >> 30U)) * 0xbf58476d1ce4e5b9ULL;
  h = (h ^ (h >> 27U)) * 0x94d049bb133111ebULL;
  return h ^ (h >> 31U);
}

inline void InsertPacked2x32Bloom(uint64_t hash, uint64_t* bloom_words,
                                  uint64_t bloom_word_mask) noexcept {
  const auto w0 = static_cast<size_t>((hash >> 6U) & bloom_word_mask);
  const auto w1 = static_cast<size_t>((hash >> 30U) & bloom_word_mask);
  bloom_words[w0] |= uint64_t{1} << (hash & 63U);
  bloom_words[w1] |= uint64_t{1} << ((hash >> 24U) & 63U);
}

// Proof-backed LLVM ORC JIT kernels for 1-PK dense-split 2-phase radix
// grouped aggregation over decoded LSTable blocks (TPC-DS Q11/Q74/Q04/etc.).
// Lean proof: lean/TinylambOpt/TpchSf10VectorizedExec.lean
//   (jit_dense_split_radix_agg_sound)
class JitDenseSplitRadixAgg {
 public:
  static constexpr uint8_t kMaxPartShift = 24;
  static constexpr uint32_t kInvalidExtractCount = UINT32_MAX;
  enum class ExprOp : uint8_t {
    kColDouble = 0,
    kColInt64 = 1,
    kSubDouble = 2,
    kAddDouble = 3,
    kSubDivConst = 4,
    kSubSubAddDivConst = 5,
    kMulDoubleInt = 6,
    kCoalesceMulZero = 7,
    kMulDoubleDouble = 8,
    kCoalesceMulDoubleZero = 9,
    kMulIntInt = 10,
    kCoalesceMulIntZero = 11,
  };

  struct ExtractSpec {
    ExprOp op{ExprOp::kColDouble};
    bool use_sel{false};
    bool jk_has_null{false};
    bool pk0_has_null{false};
    // Bitmask (bits 0..3) indicating which of v0..v3 have non-null null-byte
    // pointers (`n0..n3`) to check per row.
    uint8_t val_null_mask{0};
    uint8_t part_shift{15};
  };

  using ExtractFn = uint32_t (*)(
      uint32_t count, const uint32_t* sel, const int64_t* jk_vals,
      const uint8_t* jk_nulls, int64_t jk_min, uint64_t jk_span,
      const uint32_t* key_to_rsubgid, const int64_t* pk0_vals,
      const uint8_t* pk0_nulls, int64_t pk0_min, uint64_t pk0_span,
      const uint32_t* pk0_subgid, uint32_t pk0_card, const int64_t* v0,
      const uint8_t* n0, const int64_t* v1, const uint8_t* n1,
      const int64_t* v2, const uint8_t* n2, const int64_t* v3,
      const uint8_t* n3, double div_const, uint32_t* out_tagged_gids,
      double* out_vals, uint32_t* part_hist, uint32_t source_count,
      uint64_t part_count);

  using ScatterFn = void (*)(uint32_t count, const uint32_t* in_tagged_gids,
                             const double* in_vals, uint32_t* const* dst_gids,
                             double* const* dst_vals, uint32_t* dst_pos);

  using AccumFn = void (*)(uint32_t count, const uint32_t* local_tagged_gids,
                           const double* vals, double* sums, uint64_t* counts,
                           uint64_t* matched_bits, uint64_t* nonnull_bits,
                           uint8_t* inf_flag);

  class ExtractKernel {
   public:
    // Input columns contain source_count rows. A selection, when enabled,
    // supplies count indices into those columns. The output arrays hold
    // count entries. part_count is the length of part_hist, not a logical
    // partition count. Each histogram index is gid >> part_shift. Initialize
    // the histogram before the first batch and use the same shift for Scatter.
    // Discard partial outputs when Run returns kInvalidExtractCount.
    [[nodiscard]] uint32_t Run(
        uint32_t count, uint32_t source_count, const uint32_t* sel,
        const int64_t* jk_vals,
        const uint8_t* jk_nulls, int64_t jk_min, uint64_t jk_span,
        const uint32_t* key_to_rsubgid, const int64_t* pk0_vals,
        const uint8_t* pk0_nulls, int64_t pk0_min, uint64_t pk0_span,
        const uint32_t* pk0_subgid, uint32_t pk0_card, const int64_t* v0,
        const uint8_t* n0, const int64_t* v1, const uint8_t* n1,
        const int64_t* v2, const uint8_t* n2, const int64_t* v3,
        const uint8_t* n3, double div_const, uint32_t* out_tagged_gids,
        double* out_vals, std::span<uint32_t> part_hist) const noexcept;

   private:
    friend class JitDenseSplitRadixAgg;
    explicit ExtractKernel(ExtractFn fn) noexcept : fn_(fn) {}
    ExtractFn fn_;  // Owned by the process-lifetime ORC cache.
  };

  class ScatterKernel {
   public:
    void Run(uint32_t count, const uint32_t* in_tagged_gids,
             const double* in_vals, uint32_t* const* dst_gids,
             double* const* dst_vals, uint32_t* dst_pos) const noexcept;

   private:
    friend class JitDenseSplitRadixAgg;
    explicit ScatterKernel(ScatterFn fn) noexcept : fn_(fn) {}
    ScatterFn fn_;  // Owned by the process-lifetime ORC cache.
  };

  class AccumKernel {
   public:
    // With overflow tracking enabled, initialize *inf_flag to zero per
    // partition. The flag stays set across batches of that partition.
    void Run(uint32_t count, const uint32_t* local_tagged_gids,
             const double* vals, double* sums, uint64_t* counts,
             uint64_t* matched_bits, uint64_t* nonnull_bits,
             uint8_t* inf_flag) const noexcept;

   private:
    friend class JitDenseSplitRadixAgg;
    explicit AccumKernel(AccumFn fn) noexcept : fn_(fn) {}
    AccumFn fn_;  // Owned by the process-lifetime ORC cache.
  };

  [[nodiscard]] static std::optional<ExtractKernel> CompileExtract(
      ExtractSpec spec);
  [[nodiscard]] static std::optional<ScatterKernel> CompileScatter(
      uint8_t part_shift);
  [[nodiscard]] static std::optional<AccumKernel> CompileAccum(bool is_avg,
                                                               bool check_inf);
};

// Proof-backed LLVM ORC JIT kernel for 2-key packed int32x32 open-addressing
// hash probing with optional L2 bloom filter and hardware prefetch (Q78/Q75).
// Lean proof: lean/TinylambOpt/TpchSf10VectorizedExec.lean
//   (jit_packed2x32_hash_probe_sound)
class JitPacked2x32HashProbe {
 public:
  using ProbeFn = void (*)(uint32_t count, const int64_t* k0, const uint8_t* n0,
                           const int64_t* k1, const uint8_t* n1, int64_t bias0,
                           int64_t bias1, const uint64_t* bloom_words,
                           uint64_t bloom_word_mask, const uint64_t* ht_keys,
                           const uint32_t* ht_heads, uint64_t ht_mask,
                           uint32_t* out_heads);

  [[nodiscard]] static std::optional<JitPacked2x32HashProbe> Compile(
      bool has_null0, bool has_null1, bool has_bloom, bool has_bias);

  void Probe(uint32_t count, const int64_t* k0, const uint8_t* n0,
             const int64_t* k1, const uint8_t* n1, int64_t bias0, int64_t bias1,
             const uint64_t* bloom_words, uint64_t bloom_word_mask,
             const uint64_t* ht_keys, const uint32_t* ht_heads,
             uint64_t ht_mask, uint32_t* out_heads) const noexcept;

 private:
  explicit JitPacked2x32HashProbe(ProbeFn fn) noexcept : fn_(fn) {}
  ProbeFn fn_;  // Owned by the process-lifetime ORC cache.
};

// Proof-backed LLVM ORC JIT kernels for 2-key packed int32x32 radix partition
// pack/scatter and generation-stamped open-addressing deduplication (Q97).
// Lean proof: lean/TinylambOpt/TpchSf10VectorizedExec.lean
//   (jit_packed2x32_radix_dedup_sound)
class JitPacked2x32RadixDedup {
 public:
  using PackFn = void (*)(uint32_t count, const uint32_t* sel,
                          const int64_t* k0, const uint8_t* n0,
                          const int64_t* k1, const uint8_t* n1,
                          uint64_t* out_packed, uint32_t* out_hist,
                          uint32_t* out_fb_rows, uint32_t* out_packed_count,
                          uint32_t* out_fb_count);

  using ScatterFn = void (*)(uint32_t count, const uint64_t* in_packed,
                             uint64_t* const* dst_ptrs, uint32_t* dst_pos);

  using DedupFn = uint32_t (*)(uint32_t count, const uint64_t* in_keys,
                               uint64_t* ht_keys, uint32_t* ht_gens,
                               uint32_t cur_gen, uint64_t ht_mask,
                               uint32_t max_load, uint32_t* n_occ,
                               int64_t* out_k0, int64_t* out_k1);

  class PackKernel {
   public:
    // Caller must zero-initialize `out_hist` before calling.
    void Run(uint32_t count, const uint32_t* sel, const int64_t* k0,
             const uint8_t* n0, const int64_t* k1, const uint8_t* n1,
             uint64_t* out_packed, uint32_t* out_hist, uint32_t* out_fb_rows,
             uint32_t* out_packed_count, uint32_t* out_fb_count) const noexcept;

   private:
    friend class JitPacked2x32RadixDedup;
    explicit PackKernel(PackFn fn) noexcept : fn_(fn) {}
    PackFn fn_;
  };

  class ScatterKernel {
   public:
    void Run(uint32_t count, const uint64_t* in_packed,
             uint64_t* const* dst_ptrs, uint32_t* dst_pos) const noexcept;

   private:
    friend class JitPacked2x32RadixDedup;
    explicit ScatterKernel(ScatterFn fn) noexcept : fn_(fn) {}
    ScatterFn fn_;
  };

  class DedupKernel {
   public:
    // Initialize ht_gens to zero and use a nonzero cur_gen for active rows.
    // Clear ht_gens before reusing a generation after counter wrap.
    [[nodiscard]] uint32_t Run(uint32_t count, const uint64_t* in_keys,
                               uint64_t* ht_keys, uint32_t* ht_gens,
                               uint32_t cur_gen, uint64_t ht_mask,
                               uint32_t max_load, uint32_t* n_occ,
                               int64_t* out_k0, int64_t* out_k1) const noexcept;

   private:
    friend class JitPacked2x32RadixDedup;
    explicit DedupKernel(DedupFn fn) noexcept : fn_(fn) {}
    DedupFn fn_;
  };

  [[nodiscard]] static std::optional<PackKernel> CompilePack(
      bool has_null0, bool has_null1, bool use_sel, uint8_t part_shift);
  [[nodiscard]] static std::optional<ScatterKernel> CompileScatter(
      uint8_t part_shift);
  [[nodiscard]] static std::optional<DedupKernel> CompileOpenAddrDedup();
};

// Proof-backed LLVM ORC JIT kernel for generation-stamped open-addressing
// radix-partition double SUM/AVG accumulation with software prefetch (Q67/Q65).
// Lean proof: lean/TinylambOpt/TpchSf10VectorizedExec.lean
//   (jit_radix_key_double_agg_sound)
class JitRadixKeyDoubleAgg {
 public:
  using AggFn = uint32_t (*)(uint32_t count, const uint64_t* in_keys,
                             const double* in_vals, const uint8_t* in_valids,
                             uint64_t* ht_keys, double* ht_sums,
                             uint64_t* ht_counts, uint32_t* ht_gens,
                             uint8_t* ht_inf, uint32_t cur_gen,
                             uint64_t ht_mask, uint32_t max_load,
                             uint32_t* occupied, uint32_t* n_occ);

  [[nodiscard]] static std::optional<JitRadixKeyDoubleAgg> Compile(
      bool check_inf);

  // Initialize ht_gens to zero and use a nonzero cur_gen for active rows.
  // Clear ht_gens before reusing a generation after counter wrap.
  [[nodiscard]] uint32_t Accumulate(uint32_t count, const uint64_t* in_keys,
                                    const double* in_vals,
                                    const uint8_t* in_valids, uint64_t* ht_keys,
                                    double* ht_sums, uint64_t* ht_counts,
                                    uint32_t* ht_gens, uint8_t* ht_inf,
                                    uint32_t cur_gen, uint64_t ht_mask,
                                    uint32_t max_load, uint32_t* occupied,
                                    uint32_t* n_occ) const noexcept;

 private:
  explicit JitRadixKeyDoubleAgg(AggFn fn) noexcept : fn_(fn) {}
  AggFn fn_;  // Owned by the process-lifetime ORC cache.
};

}  // namespace tinylamb
#endif
