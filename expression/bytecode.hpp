/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXPRESSION_BYTECODE_HPP
#define TINYLAMB_EXPRESSION_BYTECODE_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

enum class BytecodeOp : uint8_t {
  kLoadColumn,
  kLoadConstant,
  kBinaryInt64,
  kBinaryDouble,
  kBinaryVarchar,
  kBinaryDate,
  kUnaryInt64,
  kUnaryDouble,
  // D7 (docs/design.md): short-circuit control flow.  The jump target is a
  // relative offset from the jump instruction itself (0 would re-execute the
  // jump, i.e. an infinite loop; the compiler must always patch it).  These
  // let AND/OR skip the right-hand side exactly like the AST evaluator and
  // keep the three-valued logic table identical across AST / VM / JIT.
  kJumpIfFalse,
  kJumpIfTrue,
  // COALESCE/IFNULL: preserve a non-NULL value and skip later arguments.
  kJumpIfNotNull,
  kJump,
  // EXTRACT(year|month|day FROM date): pops one DATE/VARCHAR value and pushes
  // the extracted part as INT64.  `operand` selects the part (0 = year,
  // 1 = month, 2 = day). Both DATE and VARCHAR use the AST's calendar
  // validation (VARCHAR via TryParseDateDays, DATETIME/TIMESTAMP text
  // truncated to its date part) and error contract.
  kExtractDatePart,
  // Byte-oriented OCTET_LENGTH/BYTE_LENGTH, including NULL and the AST's
  // textual conversion for non-string runtime values.
  kByteLength,
  // Character (Unicode code point) oriented LENGTH/CHAR_LENGTH. Counts every
  // byte that is not a UTF-8 continuation byte as one character, matching the
  // AST evaluator and the GoogleSQL compliance goldens.
  kCharLength,
  kAbs,
  // Discards the stack element `operand` slots below the top (0 drops the
  // top, the historical behaviour).  The searched-CASE lowering uses it to
  // drop a WHEN condition that was already peeked by its conditional jump
  // (the jump decides without popping), keeping the stack depth invariant
  // across every branch path; the IN lowering drops the child value kept
  // under the running result.
  kPop,
  // Pushes a copy of the stack element `operand` slots below the top
  // (0 duplicates the top).  The IN lowering uses it to compare every
  // candidate against the one child value the AST's TryMembership evaluates,
  // without re-evaluating the child expression.
  kDup,
  // Applies CastSpec casts_[operand] to the stack top.
  kCast,
  // Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean (in_const_set_3vl_sound)
  // Pops one uncollated VARCHAR value and pushes 3VL membership
  // (TRUE/FALSE/NULL) against StringSetSpec string_sets_[operand] in O(1) flat
  // hash lookup.
  kInStringSet,
  // Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
  //   (ascii_substr_1based_slice_sound)
  // Pops `operand` (2 or 3) slots (str, start [, length]) and pushes SUBSTR.
  kSubstr,
  // Lean proof: lean/TinylambOpt/SimdInRange.lean
  //   (simd_in_range_sound, int64_range_lowering)
  // Pops one signed INT64/DATE value and checks wrapping range [lo, hi].
  kInt64InRange,
  // Lean proof: lean/TinylambOpt/ResearchProofs/ReciprocalDivMod.lean
  //   (est_bounds, divModConst_sound)
  // Pops one signed INT64 value and divides/modulos by non-trivial constant
  // (|d| >= 2) using ceil(2^64 / |d|) 128-bit multiply-high + 1-step fixup.
  kDivInt64Const,
  kModInt64Const,
  // Lean proof: lean/TinylambOpt/ResearchProofs/RowInMask.lean (sound)
  // Pops `row_in_masks_[operand].num_fields` values and pushes 3VL composite
  // tuple membership evaluated via two 64-bit masks per chunk of candidates.
  kRowInMask,
};

// Lean proof: lean/TinylambOpt/ResearchProofs/RowInMask.lean (sound)
struct RowInMaskSpec {
  struct Column {
    uint64_t null_mask{0};
    uint64_t non_null_mask{0};
    std::array<int64_t, 64> values{};
  };
  struct Chunk {
    uint64_t enabled{0};
    uint32_t count{0};
    std::vector<Column> columns;
  };
  uint32_t num_fields{0};
  std::vector<Chunk> chunks;
};

struct Int64RangeSpec {
  int64_t lo{0};
  int64_t hi{0};
  ValueType expected_type{ValueType::kInt64};

  // Callers must validate lo <= hi before using this range. Converting the
  // signed endpoints to uint64_t and subtracting modulo 2^64 maps the whole
  // signed interval (including INT64_MIN..INT64_MAX) to a contiguous unsigned
  // width; the same wrapping subtraction tests x's distance from lo without
  // signed overflow. With lo > hi the width would wrap to a misleading range.
  [[nodiscard]] bool Contains(int64_t x) const noexcept {
    const auto ux = static_cast<uint64_t>(x);
    const auto ulo = static_cast<uint64_t>(lo);
    const uint64_t uwidth = static_cast<uint64_t>(hi) - ulo;
    return (ux - ulo) <= uwidth;
  }
};

struct ReciprocalDivModSpec {
  int64_t divisor{1};
  uint64_t abs_d{1};
  uint64_t recip{0};

  static ReciprocalDivModSpec FromDivisor(int64_t d) noexcept {
    const uint64_t ad = (d < 0) ? (uint64_t{0} - static_cast<uint64_t>(d))
                                : static_cast<uint64_t>(d);
    const uint64_t r =
        (ad >= 2)
            ? static_cast<uint64_t>(((__uint128_t{1} << 64) + ad - 1) / ad)
            : 0;
    return {.divisor = d, .abs_d = ad, .recip = r};
  }

  [[nodiscard]] int64_t Div(int64_t x) const noexcept {
    const bool x_neg = x < 0;
    const uint64_t ax = x_neg ? (uint64_t{0} - static_cast<uint64_t>(x))
                              : static_cast<uint64_t>(x);
    auto q =
        static_cast<uint64_t>((static_cast<__uint128_t>(ax) * recip) >> 64);
    if (static_cast<__uint128_t>(q) * abs_d > ax) {
      --q;
    }
    const bool q_neg = x_neg != (divisor < 0);
    return q_neg ? static_cast<int64_t>(uint64_t{0} - q)
                 : static_cast<int64_t>(q);
  }

  [[nodiscard]] int64_t Mod(int64_t x) const noexcept {
    const bool x_neg = x < 0;
    const uint64_t ax = x_neg ? (uint64_t{0} - static_cast<uint64_t>(x))
                              : static_cast<uint64_t>(x);
    auto q =
        static_cast<uint64_t>((static_cast<__uint128_t>(ax) * recip) >> 64);
    if (static_cast<__uint128_t>(q) * abs_d > ax) {
      --q;
    }
    const uint64_t r = ax - (q * abs_d);
    return x_neg ? static_cast<int64_t>(uint64_t{0} - r)
                 : static_cast<int64_t>(r);
  }
};

struct StringSetSpec {
  struct Slot {
    uint64_t hash{0};
    uint32_t offset{std::numeric_limits<uint32_t>::max()};
    uint32_t len{0};
  };
  std::string arena;
  std::vector<Slot> table;
  uint32_t mask{0};
  bool has_null{false};
  bool non_empty{false};

  [[nodiscard]] static uint64_t HashView(std::string_view sv) noexcept {
    uint64_t h = 14695981039346656037ULL;
    for (const char c : sv) {
      h ^= static_cast<unsigned char>(c);
      h *= 1099511628211ULL;
    }
    h ^= h >> 33U;
    return h;
  }

  [[nodiscard]] bool Contains(std::string_view sv) const noexcept;
};

struct CastSpec {
  std::string target_type_name;
  ValueType target_value_type{ValueType::kNull};
  bool return_null_on_error{false};
};

struct BytecodeInstruction {
  BytecodeOp opcode;
  uint16_t operand{0};
  BinaryOperation binary{BinaryOperation::kAdd};
  UnaryOperation unary{UnaryOperation::kNot};
  // Relative jump target (D7): positive = forward, negative = backward.
  // Held in a dedicated int32_t field; only the control-flow ops read it.
  int32_t jump_target{0};
};

struct BytecodeColumnView {
  ValueType type{ValueType::kNull};
  bool is_unsigned{false};
  bool is_interval{false};
  bool is_uniform{true};
  bool has_null{false};
  const uint8_t* nulls{nullptr};
  const int64_t* ints{nullptr};
  const double* doubles{nullptr};
  const int64_t* raw_double_bits{nullptr};
  const std::string_view* strings{nullptr};
  const Value* fallback{nullptr};
};

struct BytecodeColumnSink {
  size_t initial_rows{0};
  ValueType type{ValueType::kNull};
  bool is_uniform{true};
  bool is_unsigned{false};
  bool is_interval{false};
  bool has_null{false};
  bool may_borrow_constant_strings{false};
  std::vector<uint8_t>* nulls{nullptr};
  std::vector<int64_t>* ints{nullptr};
  std::vector<double>* doubles{nullptr};
  std::vector<std::string_view>* strings{nullptr};
  std::vector<Value>* fallback{nullptr};
};

class BytecodeProgram {
 public:
  [[nodiscard]] StatusOr<ColumnVector> TryEvaluateBatch(
      const DataChunk& input) const;
  // EXC-SHIM: deprecated throwing wrapper (common/exc_shim.hpp); library
  // callers use TryEvaluateBatch.
  [[nodiscard]] ColumnVector EvaluateBatch(const DataChunk& input) const;
  // Single-row evaluation for use in row-by-row filter paths (e.g.
  // MatchScanFilter).  Returns the result of evaluating the compiled
  // expression against the given row.
  [[nodiscard]] StatusOr<Value> TryEvaluateRow(const Row& row) const;
  // EXC-SHIM: deprecated throwing wrapper; library callers use TryEvaluateRow.
  [[nodiscard]] Value EvaluateRow(const Row& row) const;

  // Boxing-free interpreter result: kDone fills *out, kError fills *error,
  // kUnsupported means the program shape or an operand needs the canonical
  // interpreter (the caller retries with TryEvaluateRow; nothing was
  // consumed).
  enum class FastEval { kDone, kUnsupported, kError };
  // Fast-path evaluation straight on the row cells without constructing
  // intermediate Values.  The opcode set covers loads/stores, int64+double
  // arithmetic, comparisons, unary ops, control-flow jumps, EXTRACT date parts
  // and CASTs (implemented natively on the fast stack). Collated/interval
  // VARCHAR and other unsupported operand shapes return kUnsupported: the
  // caller retries through the canonical bytecode VM, which may itself fall
  // back to Expression::Evaluate (AST) when the shape is not compilable.
  // "Canonical" here means that VM, not a direct call to the AST evaluator;
  // semantics still mirror it instruction-for-instruction, and
  // differential_test compares both paths. Only valid after a successful
  // Compile (the shape flag is computed there; hand-built programs report
  // kUnsupported).
  [[nodiscard]] FastEval TryEvaluateRowFast(const Row& row, Value* out,
                                            Status* error) const;
  // Vectorized SoA columnar evaluation over `row_count` tuples (indexed via
  // `sel[i]` when `sel != nullptr`, or `0..row_count-1`). Appends directly to
  // `sink`; restores vector sizes on `kUnsupported` or `kError`.
  [[nodiscard]] FastEval TryEvaluateColumnarBatch(
      std::span<const BytecodeColumnView> cols, const uint32_t* sel,
      size_t row_count, BytecodeColumnSink* sink, Status* error) const;
  // True when every instruction is in the fast interpreter's supported set
  // and every loaded constant is a supported scalar (NULL/INT64/DOUBLE/DATE,
  // or a non-collated VARCHAR) -- the Compile-time shape scan.
  [[nodiscard]] bool FastInterpretable() const { return fast_interpretable_; }
  [[nodiscard]] const std::vector<BytecodeInstruction>& Instructions() const {
    return instructions_;
  }
  [[nodiscard]] const std::vector<Value>& Constants() const {
    return constants_;
  }
  [[nodiscard]] ValueType ResultType() const { return result_type_; }
  void AddInstruction(BytecodeInstruction instruction) {
    instructions_.push_back(instruction);
  }
  // D7: the compiler emits a conditional jump before knowing the target,
  // then patches it once the short-circuit label is reached.
  [[nodiscard]] size_t Size() const { return instructions_.size(); }
  // Returns false (leaving the program unchanged) for an out-of-range index,
  // e.g. when a compiler bug patches a label before emitting its instruction.
  bool SetJumpTarget(size_t index, int32_t relative_offset) {
    if (index >= instructions_.size()) {
      return false;
    }
    instructions_[index].jump_target = relative_offset;
    return true;
  }
  bool AddConstant(Value value, uint16_t* offset) {
    if (constants_.size() >= std::numeric_limits<uint16_t>::max()) {
      return false;
    }
    // Only VARCHAR constants can contribute borrowed string views; tracking
    // this avoids copying every row's column strings when a program merely
    // contains a numeric constant.
    if (value.type == ValueType::kVarChar) {
      has_varchar_constant_ = true;
    }
    constants_.push_back(std::move(value));
    *offset = static_cast<uint16_t>(constants_.size() - 1);
    return true;
  }
  bool AddCast(CastSpec spec, uint16_t* offset) {
    if (casts_.size() >= std::numeric_limits<uint16_t>::max()) {
      return false;
    }
    casts_.push_back(std::move(spec));
    *offset = static_cast<uint16_t>(casts_.size() - 1);
    return true;
  }
  [[nodiscard]] const std::vector<CastSpec>& Casts() const { return casts_; }
  bool AddStringSet(StringSetSpec spec, uint16_t* offset) {
    if (string_sets_.size() >= std::numeric_limits<uint16_t>::max()) {
      return false;
    }
    string_sets_.push_back(std::move(spec));
    *offset = static_cast<uint16_t>(string_sets_.size() - 1);
    return true;
  }
  [[nodiscard]] const std::vector<StringSetSpec>& StringSets() const {
    return string_sets_;
  }
  void MarkHasSubstr() { has_substr_ = true; }
  [[nodiscard]] bool HasSubstr() const { return has_substr_; }
  bool AddInt64Range(Int64RangeSpec spec, uint16_t* offset) {
    if (int64_ranges_.size() >= std::numeric_limits<uint16_t>::max()) {
      return false;
    }
    int64_ranges_.push_back(spec);
    *offset = static_cast<uint16_t>(int64_ranges_.size() - 1);
    return true;
  }
  [[nodiscard]] const std::vector<Int64RangeSpec>& Int64Ranges() const {
    return int64_ranges_;
  }
  bool AddReciprocal(ReciprocalDivModSpec spec, uint16_t* offset) {
    if (reciprocals_.size() >= std::numeric_limits<uint16_t>::max()) {
      return false;
    }
    reciprocals_.push_back(spec);
    *offset = static_cast<uint16_t>(reciprocals_.size() - 1);
    return true;
  }
  [[nodiscard]] const std::vector<ReciprocalDivModSpec>& Reciprocals() const {
    return reciprocals_;
  }
  bool AddRowInMask(RowInMaskSpec spec, uint16_t* offset) {
    if (row_in_masks_.size() >= std::numeric_limits<uint16_t>::max()) {
      return false;
    }
    row_in_masks_.push_back(std::move(spec));
    *offset = static_cast<uint16_t>(row_in_masks_.size() - 1);
    return true;
  }
  [[nodiscard]] const std::vector<RowInMaskSpec>& RowInMasks() const {
    return row_in_masks_;
  }
  void SetResultType(ValueType type) { result_type_ = type; }
  // Compile-time shape scan: marks whether TryEvaluateRowFast can take this
  // program.  Only the compiler may call it (hand-built programs stay
  // canonical-only, which keeps the flag an under-approximation).
  void SetFastInterpretable(bool fast) { fast_interpretable_ = fast; }

 private:
  friend class BytecodeCompiler;
  std::vector<BytecodeInstruction> instructions_;
  std::vector<Value> constants_;
  bool has_varchar_constant_{false};
  std::vector<CastSpec> casts_;
  std::vector<StringSetSpec> string_sets_;
  std::vector<Int64RangeSpec> int64_ranges_;
  std::vector<ReciprocalDivModSpec> reciprocals_;
  std::vector<RowInMaskSpec> row_in_masks_;
  ValueType result_type_{ValueType::kNull};
  bool fast_interpretable_{false};
  bool has_substr_{false};
};

// Exact schema/operator/current-literal identity for bounded, context-free
// scalar caches. Unsupported/context-bearing trees decline. A retained AST
// must snapshot its literals with FreezeScalarBindings first.
[[nodiscard]] std::optional<std::string> ScalarExpressionCacheKey(
    const std::vector<Expression>& expressions, const Schema& schema);
[[nodiscard]] Expression FreezeScalarBindings(const Expression& expression);

class BytecodeCompiler {
 public:
  // kValue: select items / sort keys — a rewrite that turns a NULL result
  // into FALSE (inner_join_not_null_inference) must not apply there.
  // kFilter: WHERE/ON and other truthiness-only contexts, where the same
  // rewrite is sound and kept enabled.
  enum class Context { kValue, kFilter };
  [[nodiscard]] static std::optional<BytecodeProgram> Compile(
      const Expression& expression, const Schema& schema,
      Context context = Context::kValue);
  // Bounded thread-local reuse for context-free scalar trees. The key owns
  // schema metadata, operators and exact current literal payloads (including
  // collation/unsigned tags), so rebinding never reuses old constants.
  // Row consumers also accept CASE / function calls whose result type is
  // not statically known. Such a program has ResultType()==kNull and cannot
  // feed a typed batch.
  [[nodiscard]] static std::optional<BytecodeProgram> CompileRowCached(
      const Expression& expression, const Schema& schema,
      Context context = Context::kValue);
};

// Bitmask of bytecode optimization disable toggles for cache invalidation.
[[nodiscard]] uint8_t BytecodeOptToggleBits() noexcept;

// Differential-testing hook: false when TINYLAMB_DISABLE_BYTECODE is set in
// the environment. Compile call sites gate on this so the same query can be
// executed through the AST evaluation path (the semantic reference) and
// compared against the bytecode/JIT fast path.
[[nodiscard]] bool BytecodeEnabled();
// Thread-safe in-process override for differential harnesses. Prefer this
// over mutating TINYLAMB_DISABLE_BYTECODE with setenv/unsetenv, which races
// getenv in parallel executor workers. The environment variable still wins
// when it is set externally and this override is false.
void SetBytecodeDisabledForProcess(bool disabled);

// Row evaluation through the boxing-free interpreter with a canonical
// fallback: fast-shaped programs (columns/constants/int64+double arithmetic)
// skip intermediate Value construction; every other shape runs
// BytecodeProgram::TryEvaluateRow.  Semantics are identical on both legs
// (differential_test pins this).
[[nodiscard]] StatusOr<Value> TryEvaluateRowAuto(const BytecodeProgram& program,
                                                 const Row& row);

// Conservative "cannot raise an error on any row" analysis used by the
// filter-order pass. Exposed for audits/tests of the error-free contract.
[[nodiscard]] bool AuditIsErrorFreePredicateForOrdering(const Expression& expr,
                                                        const Schema& schema);

}  // namespace tinylamb
#endif
