/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Pure in-process libFuzzer for ZoneMap, BlockKleene, TrigramBlockSummary,
// BlockBloomSignature, and CompileScanFilter/MatchScanFilterWorker soundness.
//
// Oracles:
//   1. EvaluateBlockKleene:
//        - BlockKleeneCanSkip(mask) => NO row in block evaluates to true OR
//        error
//        - BlockKleeneAllPass(mask) => EVERY row in block evaluates to true
//   2. ZoneMap::MayMatch:
//        - !MayMatch(op, const) => NO row in block satisfies (col op const)
//   3. TrigramBlockSummary:
//        - CanSkipLikeBlockByTrigrams(pat, summary) => NO non-null row matches
//          FastLikeMatch(row_s, pat)
//   4. BlockBloomSignature:
//        - CanSkipBlockByBloomIntersection(a, b, null_safe) => NO pair matches
//   5. CompileScanFilter + MatchScanFilterWorker vs EvaluateConjunctsTolerant:
//        - Decided worker verdicts (0 reject / 1 accept) must match exact AST
//          conjunct evaluation (including never swallowing an error).

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/zone_map.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/unary_expression.hpp"
#include "table/lstable_bridge.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::fuzz {
namespace {

struct Cursor {
  const uint8_t* data{};
  size_t size{};
  size_t pos{0};

  uint8_t NextU8() noexcept { return pos < size ? data[pos++] : 0U; }

  int64_t NextInterestingInt() noexcept {
    switch (NextU8() % 12U) {
      case 0:
        return 0;
      case 1:
        return 1;
      case 2:
        return -1;
      case 3:
        return std::numeric_limits<int64_t>::min();
      case 4:
        return std::numeric_limits<int64_t>::max();
      case 5:
        return std::numeric_limits<int64_t>::min() + 1;
      case 6:
        return std::numeric_limits<int64_t>::max() - 1;
      case 7:
        return -100;
      case 8:
        return 100;
      default:
        return static_cast<int8_t>(NextU8());
    }
  }

  double NextInterestingDouble() noexcept {
    switch (NextU8() % 10U) {
      case 0:
        return 0.0;
      case 1:
        return -0.0;
      case 2:
        return 1.5;
      case 3:
        return -1.5;
      case 4:
        return std::numeric_limits<double>::quiet_NaN();
      case 5:
        return std::numeric_limits<double>::infinity();
      case 6:
        return -std::numeric_limits<double>::infinity();
      default:
        return static_cast<double>(static_cast<int8_t>(NextU8())) * 0.25;
    }
  }

  std::string NextSmallString() {
    static constexpr std::string_view kAlphabet = "abcABC012_%\\";
    const size_t len = NextU8() % 10U;
    std::string out;
    out.reserve(len);
    for (size_t i = 0; i < len; ++i) {
      out.push_back(kAlphabet[NextU8() % kAlphabet.size()]);
    }
    return out;
  }
};

[[noreturn]] void Fail(const char* oracle, const char* msg) {
  (void)std::fprintf(stderr, "[ZONEMAP-FILTER-FUZZ FAIL][%s] %s\n", oracle,
                     msg);
  (void)std::fflush(stderr);
  std::abort();
}

Schema MakeSchema() {
  return Schema("t", {
                         Column(ColumnName("t", "i"), ValueType::kInt64),
                         Column(ColumnName("t", "d"), ValueType::kDouble),
                         Column(ColumnName("t", "s"), ValueType::kVarChar),
                         Column(ColumnName("t", "dt"), ValueType::kDate),
                     });
}

BinaryOperation PickCmpOp(uint8_t b) {
  static constexpr std::array<BinaryOperation, 6> kOps = {
      BinaryOperation::kEquals,      BinaryOperation::kNotEquals,
      BinaryOperation::kLessThan,    BinaryOperation::kLessThanEquals,
      BinaryOperation::kGreaterThan, BinaryOperation::kGreaterThanEquals,
  };
  return kOps[b % kOps.size()];
}

Expression MakeAtom(Cursor& c) {
  const uint8_t kind = c.NextU8() % 11U;
  switch (kind) {
    case 0:  // i <op> const
      return BinaryExpressionExp(
          ColumnValueExp("i"), PickCmpOp(c.NextU8()),
          ConstantValueExp(Value(c.NextInterestingInt())));
    case 1:  // d <op> const
      return BinaryExpressionExp(
          ColumnValueExp("d"), PickCmpOp(c.NextU8()),
          ConstantValueExp(Value(c.NextInterestingDouble())));
    case 2:  // s <op> const
      return BinaryExpressionExp(ColumnValueExp("s"), PickCmpOp(c.NextU8()),
                                 ConstantValueExp(Value(c.NextSmallString())));
    case 3:  // dt <op> const
      return BinaryExpressionExp(
          ColumnValueExp("dt"), PickCmpOp(c.NextU8()),
          ConstantValueExp(Value::DateFromDays(c.NextInterestingInt() % 1000)));
    case 4:  // col IS NULL / IS NOT NULL
    {
      static constexpr std::array<const char*, 4> kCols = {"i", "d", "s", "dt"};
      const char* col = kCols[c.NextU8() % kCols.size()];
      const UnaryOperation uop = (c.NextU8() & 1U) != 0
                                     ? UnaryOperation::kIsNull
                                     : UnaryOperation::kIsNotNull;
      return UnaryExpressionExp(ColumnValueExp(col), uop);
    }
    case 5:  // i [NOT] IN (c1, c2, ...)
    {
      std::vector<Expression> list;
      const size_t n = c.NextU8() % 4U;
      for (size_t j = 0; j < n; ++j) {
        if ((c.NextU8() & 7U) == 0) {
          list.push_back(ConstantValueExp(Value()));
        } else {
          list.push_back(ConstantValueExp(Value(c.NextInterestingInt())));
        }
      }
      Expression in_expr =
          InExpressionExp(ColumnValueExp("i"), std::move(list));
      return (c.NextU8() & 1U) != 0
                 ? UnaryExpressionExp(std::move(in_expr), UnaryOperation::kNot)
                 : in_expr;
    }
    case 6:  // ABS(i) <op> const (can throw on INT64_MIN!)
      return BinaryExpressionExp(
          FunctionCallExp("abs", {ColumnValueExp("i")}), PickCmpOp(c.NextU8()),
          ConstantValueExp(Value(c.NextInterestingInt())));
    case 7:  // i / const = 0 (can throw on divide by zero or INT64_MIN / -1)
    {
      const int64_t denom =
          (c.NextU8() % 5U) == 0 ? 0 : ((c.NextU8() & 1U) ? -1 : 2);
      return BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kDivide,
                              ConstantValueExp(Value(denom))),
          BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{0})));
    }
    case 8:  // s LIKE pat
      return BinaryExpressionExp(ColumnValueExp("s"), BinaryOperation::kLike,
                                 ConstantValueExp(Value(c.NextSmallString())));
    case 9:  // i <op> d (mixed numeric)
      return BinaryExpressionExp(ColumnValueExp("i"), PickCmpOp(c.NextU8()),
                                 ColumnValueExp("d"));
    default:  // constant boolean or NULL
    {
      const uint8_t v = c.NextU8() % 3U;
      if (v == 0) {
        return ConstantValueExp(Value());
      }
      return ConstantValueExp(Value(static_cast<int64_t>(v == 1 ? 1 : 0)));
    }
  }
}

Expression MakeCompoundPredicate(Cursor& c, int depth) {
  if (depth <= 0 || (c.NextU8() % 3U) == 0) {
    return MakeAtom(c);
  }
  const uint8_t op = c.NextU8() % 3U;
  if (op == 0) {
    return UnaryExpressionExp(MakeCompoundPredicate(c, depth - 1),
                              UnaryOperation::kNot);
  }
  return BinaryExpressionExp(
      MakeCompoundPredicate(c, depth - 1),
      op == 1 ? BinaryOperation::kAnd : BinaryOperation::kOr,
      MakeCompoundPredicate(c, depth - 1));
}

bool ValueIsTrue(const Value& v) {
  if (v.IsNull()) {
    return false;
  }
  if (v.type == ValueType::kInt64) {
    return v.value.int_value != 0;
  }
  if (v.type == ValueType::kDouble) {
    return v.value.double_value != 0.0;
  }
  return false;
}

}  // namespace
}  // namespace tinylamb::fuzz

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  using namespace tinylamb;
  using namespace tinylamb::fuzz;
  using namespace tinylamb::relational_detail;

  if (size < 8 || size > 4096) {
    return 0;
  }
  Cursor c{data, size, 0};
  const Schema schema = MakeSchema();

  const uint32_t row_count = 1U + (c.NextU8() % 20U);
  std::vector<Row> rows;
  rows.reserve(row_count);

  std::vector<int64_t> i_cells(row_count, 0);
  std::vector<int64_t> d_cells(row_count, 0);
  std::vector<int64_t> dt_cells(row_count, 0);

  LSTablePageBlock blk;
  blk.row_count = row_count;
  blk.identity_slots = true;
  blk.columns.resize(4);

  auto& col_i = blk.columns[0];
  col_i.type = ValueType::kInt64;
  col_i.nulls.assign(row_count, 0);

  auto& col_d = blk.columns[1];
  col_d.type = ValueType::kDouble;
  col_d.nulls.assign(row_count, 0);

  auto& col_s = blk.columns[2];
  col_s.type = ValueType::kVarChar;
  col_s.nulls.assign(row_count, 0);

  auto& col_dt = blk.columns[3];
  col_dt.type = ValueType::kDate;
  col_dt.nulls.assign(row_count, 0);

  ZoneMap zm_i;
  ZoneMap zm_d;
  ZoneMap zm_s;
  ZoneMap zm_dt;
  TrigramBlockSummary trigram_summary;
  BlockBloomSignature bloom_a;
  BlockBloomSignature bloom_b;
  bloom_a.Init(4, 0x123456789ABCDEF0ULL);
  bloom_b.Init(4, 0x123456789ABCDEF0ULL);
  std::vector<std::optional<int64_t>> keys_a;
  std::vector<std::optional<int64_t>> keys_b;

  for (uint32_t r = 0; r < row_count; ++r) {
    // Column 0: INT64
    Value vi;
    if ((c.NextU8() & 7U) == 0) {
      col_i.nulls[r] = 1;
      col_i.has_null = true;
      blk.has_any_null = true;
      zm_i.AddNull();
      bloom_a.AddNull();
      keys_a.push_back(std::nullopt);
    } else {
      const int64_t v = c.NextInterestingInt();
      vi = Value(v);
      i_cells[r] = v;
      col_i.min_int = std::min(col_i.min_int, v);
      col_i.max_int = std::max(col_i.max_int, v);
      col_i.has_min_max = true;
      zm_i.AddInt(v);
      bloom_a.AddInt64(v);
      keys_a.push_back(v);
    }

    // Column 1: DOUBLE
    Value vd;
    if ((c.NextU8() & 7U) == 0) {
      col_d.nulls[r] = 1;
      col_d.has_null = true;
      blk.has_any_null = true;
      zm_d.AddNull();
    } else {
      const double d = c.NextInterestingDouble();
      vd = Value(d);
      int64_t bits = 0;
      std::memcpy(&bits, &d, sizeof(bits));
      d_cells[r] = bits;
      if (std::isnan(d)) {
        col_d.has_nan = true;
      } else {
        col_d.min_double = std::min(col_d.min_double, d);
        col_d.max_double = std::max(col_d.max_double, d);
        col_d.has_double_min_max = true;
      }
      zm_d.AddDouble(d);
    }

    // Column 2: VARCHAR
    Value vs;
    if ((c.NextU8() & 7U) == 0) {
      col_s.nulls[r] = 1;
      col_s.has_null = true;
      blk.has_any_null = true;
      col_s.varchar_ends.push_back(
          static_cast<uint32_t>(col_s.varchar_bytes.size()));
      zm_s.AddNull();
    } else {
      const std::string s = c.NextSmallString();
      vs = Value(std::string(s));
      col_s.varchar_bytes.insert(col_s.varchar_bytes.end(), s.begin(), s.end());
      col_s.varchar_ends.push_back(
          static_cast<uint32_t>(col_s.varchar_bytes.size()));
      zm_s.AddString(s);
      trigram_summary.AddString(s);
    }

    // Column 3: DATE
    Value vdt;
    if ((c.NextU8() & 7U) == 0) {
      col_dt.nulls[r] = 1;
      col_dt.has_null = true;
      blk.has_any_null = true;
      zm_dt.AddNull();
    } else {
      const int64_t days = c.NextInterestingInt() % 1000;
      vdt = Value::DateFromDays(days);
      dt_cells[r] = days;
      col_dt.min_int = std::min(col_dt.min_int, days);
      col_dt.max_int = std::max(col_dt.max_int, days);
      col_dt.has_min_max = true;
      zm_dt.AddDate(days);
    }

    rows.emplace_back(std::vector<Value>{vi, vd, vs, vdt});

    // Build second bloom signature for Oracle 4
    if ((c.NextU8() & 7U) == 0) {
      bloom_b.AddNull();
      keys_b.push_back(std::nullopt);
    } else {
      const int64_t kb = c.NextInterestingInt();
      bloom_b.AddInt64(kb);
      keys_b.push_back(kb);
    }
  }

  col_i.int_cells = i_cells;
  col_d.int_cells = d_cells;
  col_dt.int_cells = dt_cells;

  // Oracle 1: EvaluateBlockKleene soundness
  const Expression pred = MakeCompoundPredicate(c, 2);
  const KleeneMask mask = EvaluateBlockKleene(pred, schema, blk);
  bool any_true = false;
  bool any_error = false;
  bool all_true = true;
  for (const Row& row : rows) {
    StatusOr<Value> eval = pred->TryEvaluate(row, schema);
    if (!eval.HasValue()) {
      any_error = true;
      all_true = false;
    } else if (ValueIsTrue(eval.Value())) {
      any_true = true;
    } else {
      all_true = false;
    }
  }
  if (BlockKleeneCanSkip(mask) && (any_true || any_error)) {
    Fail("BlockKleene", "BlockKleeneCanSkip returned true despite match/error");
  }
  if (BlockKleeneAllPass(mask) && !all_true) {
    Fail("BlockKleene",
         "BlockKleeneAllPass returned true despite non-true row");
  }

  // Oracle 2: ZoneMap::MayMatch soundness
  {
    const BinaryOperation op = PickCmpOp(c.NextU8());
    const Value ci(c.NextInterestingInt());
    if (!zm_i.MayMatch(op, ci)) {
      const Expression e =
          BinaryExpressionExp(ColumnValueExp("i"), op, ConstantValueExp(ci));
      for (const Row& row : rows) {
        auto r = e->TryEvaluate(row, schema);
        if (r.HasValue() && ValueIsTrue(r.Value())) {
          Fail("ZoneMap", "MayMatch(int) falsely pruned matching row");
        }
      }
    }
    const Value cd(c.NextInterestingDouble());
    if (!zm_d.MayMatch(op, cd)) {
      const Expression e =
          BinaryExpressionExp(ColumnValueExp("d"), op, ConstantValueExp(cd));
      for (const Row& row : rows) {
        auto r = e->TryEvaluate(row, schema);
        if (r.HasValue() && ValueIsTrue(r.Value())) {
          Fail("ZoneMap", "MayMatch(double) falsely pruned matching row");
        }
      }
    }
    const Value cs(c.NextSmallString());
    if (!zm_s.MayMatch(op, cs)) {
      const Expression e =
          BinaryExpressionExp(ColumnValueExp("s"), op, ConstantValueExp(cs));
      for (const Row& row : rows) {
        auto r = e->TryEvaluate(row, schema);
        if (r.HasValue() && ValueIsTrue(r.Value())) {
          Fail("ZoneMap", "MayMatch(varchar) falsely pruned matching row");
        }
      }
    }
  }

  // Oracle 3: TrigramBlockSummary LIKE pruning soundness
  {
    const std::string pat = c.NextSmallString();
    if (CanSkipLikeBlockByTrigrams(pat, trigram_summary, '\\')) {
      for (const Row& row : rows) {
        if (!row[2].IsNull() &&
            FastLikeMatch(row[2].value.varchar_value, pat)) {
          Fail("TrigramSkip", "CanSkipLikeBlockByTrigrams falsely pruned row");
        }
      }
    }
  }

  // Oracle 4: BlockBloomSignature intersection soundness
  for (const bool null_safe : {false, true}) {
    if (CanSkipBlockByBloomIntersection(bloom_a, bloom_b, null_safe)) {
      for (const auto& ka : keys_a) {
        for (const auto& kb : keys_b) {
          const bool match = (ka.has_value() && kb.has_value() && *ka == *kb) ||
                             (null_safe && !ka.has_value() && !kb.has_value());
          if (match) {
            Fail(
                "BloomIntersection",
                "CanSkipBlockByBloomIntersection falsely pruned matching pair");
          }
        }
      }
    }
  }

  // Oracle 5: CompileScanFilter + MatchScanFilterWorker vs
  // EvaluateConjunctsTolerant (over top-level flattened AND conjuncts, matching
  // CompileScanFilter's commutative WHERE conjunct contract).
  {
    std::vector<Expression> conjuncts;
    conjuncts.push_back(pred);
    conjuncts.push_back(MakeCompoundPredicate(c, 1));
    std::vector<Expression> flat_conjuncts;
    auto flatten = [&](auto& self, const Expression& e) -> void {
      if (e && e->Type() == TypeTag::kBinaryExp &&
          e->AsBinaryExpression().Op() == BinaryOperation::kAnd) {
        self(self, e->AsBinaryExpression().Left());
        self(self, e->AsBinaryExpression().Right());
      } else if (e) {
        flat_conjuncts.push_back(e);
      }
    };
    for (const Expression& p : conjuncts) {
      flatten(flatten, p);
    }
    const CompiledScanFilter compiled = CompileScanFilter(conjuncts, schema);
    const WorkerScanFilterPrograms programs =
        CompileWorkerFilter(compiled, schema);
    if (programs.complete) {
      for (const Row& row : rows) {
        const int worker_verdict =
            MatchScanFilterWorker(row, schema, compiled, programs);
        if (worker_verdict == 2) {
          continue;  // deferred to contextual path
        }
        const StatusOr<bool> ast_verdict = EvaluateConjunctsTolerant(
            flat_conjuncts, [&](const Expression& e) -> StatusOr<Value> {
              return e->TryEvaluate(row, schema);
            });
        if (!ast_verdict.HasValue()) {
          // In kFilter context, filter-only rules such as
          // inner_join_not_null_inference may convert a NULL comparison into
          // FALSE and short-circuit a sibling error (returning reject = 0).
          // Accepting a row whose predicate raised an error (verdict = 1) is
          // never sound.
          if (worker_verdict == 1) {
            Fail("ScanFilterWorker",
                 "worker accepted row when AST raised error");
          }
          continue;
        }
        if ((worker_verdict == 1) != ast_verdict.Value()) {
          Fail("ScanFilterWorker",
               "worker accept/reject disagreed with EvaluateConjunctsTolerant");
        }
      }
    }
  }

  return 0;
}
