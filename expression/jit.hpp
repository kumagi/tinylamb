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
  using HashFn = void (*)(const int64_t*, const char*, const uint32_t*, int64_t*,
                          uint64_t*, uint64_t, uint64_t);
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
      std::span<const JitColumnInput> columns) const noexcept;

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

}  // namespace tinylamb
#endif
