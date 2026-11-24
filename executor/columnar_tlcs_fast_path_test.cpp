/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/columnar_tlcs_fast_path.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "common/memory_budget.hpp"
#include "common/test_util.hpp"
#include "common/query_parallelism.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_tlcs_internal.hpp"
#include "executor/detail/typed_stream_aggregate.hpp"
#include "executor/detail/typed_stream_aggregate_partitioned.hpp"
#include "executor/executor_base.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "table/tlcs_file.hpp"
#include "type/collation.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

using relational_detail::ColAggKind;
using relational_detail::ColAggSpec;
using relational_detail::ColGroupKeySpec;
using relational_detail::ColKeyKind;
using relational_detail::ColumnarOrderBinding;
using relational_detail::ColumnarPredicateSet;
using relational_detail::OutputSlotBinding;
using relational_detail::PartitionedColumnarAggRequest;
using relational_detail::Relation;
using relational_detail::TryRunPartitionedColumnarTlcsAggregate;

struct BudgetResetGuard {
  size_t prev_limit{ProcessMemoryBudget::kDefaultLimitBytes};
  size_t prev_storage{size_t{4} << 30};
  size_t prev_columnar{size_t{2} << 30};
  BudgetResetGuard()
      : prev_limit(ProcessMemoryBudget::Global().Limit()),
        prev_storage(ProcessMemoryBudget::Global().StoragePoolLimit()),
        prev_columnar(ProcessMemoryBudget::Global().ColumnarPoolLimit()) {}
  BudgetResetGuard(const BudgetResetGuard&) = delete;
  BudgetResetGuard& operator=(const BudgetResetGuard&) = delete;
  BudgetResetGuard(BudgetResetGuard&&) = delete;
  BudgetResetGuard& operator=(BudgetResetGuard&&) = delete;
  ~BudgetResetGuard() {
    auto& budget = ProcessMemoryBudget::Global();
    budget.ResetTestKnobs();
    budget.ResetForTest(prev_limit, prev_storage, prev_columnar, true);
  }
};

std::vector<Row> RunSql(SqlEngine* engine, TransactionContext* ctx,
                        const std::string& sql) {
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    ADD_FAILURE() << "Prepare failed: " << engine->LastError()
                  << "\nSQL: " << sql;
    return {};
  }
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(std::move(row));
    row = Row();
  }
  EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
  return rows;
}

TEST(ColumnarTlcsFastPathTest, HavingComparisonErrorPropagates) {
  const std::vector<ColAggSpec> specs{{.kind = ColAggKind::kMinVarChar}};
  const std::vector<relational_detail::PositionalHaving> having{{
      .agg_index = 0,
      .op = BinaryOperation::kGreaterThan,
      .constant = Value(123),
      .has_constant = true,
  }};
  relational_detail::RuntimeAggState state;
  state.str_ptr = "abc";
  state.str_len = 3;
  state.has_value = true;
  std::vector<Value> scratch(1);
  const auto result = relational_detail::EvalPositionalHavingOnStates(
      having, specs, &state, &scratch);
  EXPECT_FALSE(result.HasValue());
}

TEST(ColumnarTlcsFastPathTest, JitLiteralSelectionPreservesRowBoundaries) {
  const auto kernel = JitLiteralSearch::Compile("abc");
  if (!kernel) {
    GTEST_SKIP() << "literal JIT unavailable";
  }
  ColumnarPredicateSet preds;
  preds.compares.push_back({
      .left = 0,
      .op = BinaryOperation::kLike,
      .constant_payload = true,
      .string_payload = true,
      .string_constant = "abc",
      .string_match =
          relational_detail::TypedFilterCompare::StringMatchKind::kContains,
  });
  preds.contains_prechecks.push_back(
      {.full_col = 0, .needle = "abc", .kernel = kernel});
  TlcsFile::DecodedColumnChunk chunk;
  chunk.type = ValueType::kVarChar;
  chunk.has_null = true;
  const std::vector<std::string> input = {
      "ab", "", "cxyz", "abcabc", "", "abc", "aBc", "xabc", "ab", "c",
  };
  chunk.row_count = static_cast<uint32_t>(input.size());
  chunk.nulls.resize(input.size(), 0);
  chunk.nulls[5] = 1;
  for (const auto& value : input) {
    chunk.varchar_bytes.insert(chunk.varchar_bytes.end(), value.begin(),
                               value.end());
    chunk.varchar_ends.push_back(
        static_cast<uint32_t>(chunk.varchar_bytes.size()));
  }
  const std::vector<const TlcsFile::DecodedColumnChunk*> columns{&chunk};
  std::vector<uint32_t> expected;
  expected.reserve(chunk.row_count);
  for (uint32_t row = 0; row < chunk.row_count; ++row) {
    if (relational_detail::RowMatchesColPtrs(preds, columns, row)) {
      expected.push_back(row);
    }
  }
  EXPECT_EQ(expected, (std::vector<uint32_t>{3, 7}));
  std::vector<uint32_t> rows;
  ASSERT_TRUE(relational_detail::TryJitContainsRows(
      preds, columns, chunk.row_count, false, &rows));
  EXPECT_EQ(rows, expected);
  rows = {0, 3, 5, 8, 9};
  ASSERT_TRUE(relational_detail::TryJitContainsRows(
      preds, columns, chunk.row_count, true, &rows));
  EXPECT_EQ(rows, (std::vector<uint32_t>{3}));

  TlcsFile::DecodedColumnChunk integers;
  integers.int_cells.resize(input.size(), 0);
  integers.int_cells[7] = 1;
  preds.compares.push_back({
      .left = 1,
      .op = BinaryOperation::kEquals,
      .constant_payload = true,
      .int_constant = 1,
  });
  ASSERT_TRUE(relational_detail::TryJitContainsRows(
      preds, {&chunk, &integers}, chunk.row_count, false, &rows));
  EXPECT_EQ(rows, (std::vector<uint32_t>{7}));
  preds.compares.pop_back();

  chunk.varchar_ends[3] = chunk.varchar_ends.back();
  EXPECT_FALSE(relational_detail::TryJitContainsRows(
      preds, columns, chunk.row_count, false, &rows));

  chunk.has_null = false;
  chunk.row_count = 100;
  chunk.varchar_ends.clear();
  chunk.varchar_bytes.clear();
  for (uint32_t row = 0; row < chunk.row_count; ++row) {
    chunk.varchar_bytes.insert(chunk.varchar_bytes.end(), {'a', 'b', 'c'});
    chunk.varchar_ends.push_back(3 * (row + 1));
  }
  rows = {3, 7};
  EXPECT_FALSE(relational_detail::TryJitContainsRows(
      preds, columns, chunk.row_count, true, &rows));
  EXPECT_EQ(rows, (std::vector<uint32_t>{3, 7}));
}

ColGroupKeySpec MakeKeySpec(ColKeyKind kind, slot_t col, std::string expr_str,
                            int64_t delta = 0) {
  ColGroupKeySpec spec;
  spec.kind = kind;
  spec.full_col = col;
  spec.const_int = delta;
  spec.original_expr_str = std::move(expr_str);
  return spec;
}

ColAggSpec MakeAggSpec(const AggregateExpression* expr, ColAggKind kind,
                       slot_t col) {
  ColAggSpec spec;
  spec.expr = expr;
  spec.kind = kind;
  spec.full_col = col;
  return spec;
}

OutputSlotBinding MakeSlotBinding(OutputSlotBinding::Kind kind, size_t index) {
  OutputSlotBinding b;
  b.kind = kind;
  b.index = index;
  return b;
}

TEST(ColumnarTlcsFastPathTest, UnboundedFilteredScanStreams) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "execute_query unavailable";
  }
  const std::string prefix = "tlcs_stream_filter_" + RandomString();
  auto db = Database::Create(prefix).MoveValue();
  {
    auto ctx = db->BeginContext();
    Schema schema("events", {Column("id", ValueType::kInt64)});
    ASSERT_TRUE(db->CreateTable(ctx, schema).HasValue());
    Table table = db->GetTable(ctx, "events").MoveValue();
    for (int64_t i = 0; i < 5000; ++i) {
      ASSERT_TRUE(table.Insert(ctx.txn_, Row({Value(i)})).HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    auto ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "events"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    auto ctx = db->BeginReadOnlyContext();
    SqlEngine engine(*db);
    auto prepared = engine.Prepare(ctx, "SELECT id FROM events WHERE id > 0");
    ASSERT_TRUE(prepared.HasValue()) << engine.LastError();
    std::ostringstream plan;
    prepared.Value()->Dump(plan, 0);
    EXPECT_EQ(plan.str().find("ColumnarTlcsScan"), std::string::npos);
    size_t count = 0;
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
      ++count;
    }
    EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess);
    EXPECT_EQ(count, 4999U);
  }
  db->DeleteAll();
}

TEST(ColumnarTlcsFastPathTest, ScalarSumInt64OverflowRaisesCanonicalError) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "execute_query unavailable";
  }
  BudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(size_t{256} << 20, size_t{64} << 20, size_t{64} << 20,
                      /*check_rss_pressure=*/false);

  const std::string prefix = "tlcs_fp_sumovf_" + RandomString();
  Schema schema("events", {Column("id", ValueType::kInt64),
                           Column("pad", ValueType::kInt64)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  constexpr int64_t kRows = 40000;
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CreateTable(ctx, schema).GetStatus(), Status::kSuccess);
    Table tbl = db->GetTable(ctx, "events").MoveValue();
    for (int64_t i = 0; i < kRows; ++i) {
      // Non-uniform so the scalar SUM takes the per-row checked path. Row 1
      // pushes the running total past INT64_MAX.
      const int64_t id =
          i == 0 ? std::numeric_limits<int64_t>::max() : (i == 1 ? 1 : i);
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_, Row({Value(id), Value(i % 7)})).HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "events"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine engine(*db);
    StatusOr<Executor> prepared =
        engine.Prepare(ctx, "SELECT SUM(id) FROM events");
    ASSERT_TRUE(prepared.HasValue()) << engine.LastError();
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    EXPECT_EQ(prepared.Value()->GetStatus(), Status::kIsInfinity);
  }
  db->DeleteAll();
}

// P603: a TLCS kSumDouble accumulator that overflowed to infinity on finite
// inputs must raise at finish, matching the canonical accumulator and the
// INT64 overflow test above, while a group that genuinely contains an
// infinite input propagates inf without raising.  The old FinishAggState
// arm returned the overflowed inf as a value.
TEST(ColumnarTlcsFastPathTest, SumDoubleOverflowRaisesButInfInputPropagates) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "execute_query unavailable";
  }
  // The columnar TLCS aggregate fast path is driven by the legacy
  // relational executor; pipeline lowering bypasses
  // TryRunColumnarTlcsAggregate.
  // ScopedEnvVar restores a pre-existing TINYLAMB_EXEC_ENGINE instead of
  // unconditionally unsetting it, so running under `=legacy` stays legacy
  // for later tests in the same process.
  const ScopedEnvVar legacy_guard("TINYLAMB_EXEC_ENGINE", "legacy");
  BudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(size_t{256} << 20, size_t{64} << 20, size_t{64} << 20,
                      /*check_rss_pressure=*/false);

  const std::string prefix = "tlcs_fp_sumdbl_" + RandomString();
  Schema schema("events", {Column("g", ValueType::kInt64),
                           Column("x", ValueType::kDouble),
                           Column("pad", ValueType::kInt64)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  const double kHuge = 1e308;
  const double kInf = std::numeric_limits<double>::infinity();
  constexpr int64_t kRows = 1200;
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CreateTable(ctx, schema).GetStatus(), Status::kSuccess);
    Table tbl = db->GetTable(ctx, "events").MoveValue();
    // g=1: finite inputs whose sum leaves the double range -> raise.
    // g=2: an actual infinity input -> inf propagates, no raise.
    for (int64_t i = 0; i < kRows; ++i) {
      const int64_t g = i < kRows / 2 ? 1 : 2;
      const double x = g == 1 ? kHuge : (i == kRows / 2 ? kInf : 5.0);
      ASSERT_TRUE(tbl.Insert(ctx.txn_, Row({Value(g), Value(x), Value(i % 7)}))
                      .HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "events"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine engine(*db);
    StatusOr<Executor> prepared =
        engine.Prepare(ctx, "SELECT g, SUM(x) FROM events GROUP BY g");
    ASSERT_TRUE(prepared.HasValue()) << engine.LastError();
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    EXPECT_EQ(prepared.Value()->GetStatus(), Status::kIsInfinity);
  }
  {
    // The infinity-input group alone must still answer inf cleanly.
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine engine(*db);
    const std::vector<Row> rows = RunSql(
        &engine, &ctx, "SELECT g, SUM(x) FROM events WHERE g = 2 GROUP BY g");
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_TRUE(std::isinf(rows[0][1].value.double_value));
  }
  db->DeleteAll();
}

// P625: the L1 pre-aggregator's "not worth it" self-disable used to be an
// exact probe-count comparison that only the miss arm evaluated; a worker
// whose 8192nd probe happened to be a hit kept a ~1.6%-hit L1 forever.  The
// disable path is result-neutral either way, so the pin locks the results
// for a stream built so the threshold-crossing probe IS a hit — the exact
// distribution where old-vs-new self-disable behavior diverges internally.
TEST(ColumnarTlcsFastPathTest, L1SelfDisableHitAtThresholdKeepsResultsExact) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "execute_query unavailable";
  }
  const ScopedEnvVar legacy_guard("TINYLAMB_EXEC_ENGINE", "legacy");
  BudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(size_t{256} << 20, size_t{64} << 20, size_t{64} << 20,
                      /*check_rss_pressure=*/false);

  const std::string prefix = "tlcs_fp_l1sd_" + RandomString();
  Schema schema("hits", {Column("k", ValueType::kInt64),
                         Column("pad", ValueType::kInt64)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  constexpr int64_t kRows = 12000;
  std::unordered_map<int64_t, int64_t> expected;
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CreateTable(ctx, schema).GetStatus(), Status::kSuccess);
    Table tbl = db->GetTable(ctx, "hits").MoveValue();
    for (int64_t i = 0; i < kRows; ++i) {
      // Every 64th row repeats a key seen 448 rows earlier: ~1.6% hit rate
      // (< the 1/16 disable ratio) with the 4096th and 8192nd probes
      // themselves hits.
      const int64_t k = (i % 64 == 63 && i >= 448) ? i - 448 : i;
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_, Row({Value(k), Value(i % 7)})).HasValue());
      ++expected[k];
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "hits"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine engine(*db);
    const std::vector<Row> rows =
        RunSql(&engine, &ctx, "SELECT k, COUNT(*) FROM hits GROUP BY k");
    ASSERT_EQ(rows.size(), expected.size());
    int64_t seen = 0;
    for (const Row& row : rows) {
      const int64_t key = row[0].value.int_value;
      auto it = expected.find(key);
      ASSERT_NE(it, expected.end()) << key;
      EXPECT_EQ(row[1].value.int_value, it->second) << key;
      ++seen;
    }
    EXPECT_EQ(seen, static_cast<int64_t>(expected.size()));
  }
  db->DeleteAll();
}

TEST(ColumnarTlcsFastPathTest, TopNVarCharCutoffParameterSlotAndScalarAggs) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "execute_query unavailable";
  }
  BudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(size_t{256} << 20, size_t{64} << 20, size_t{64} << 20,
                      /*check_rss_pressure=*/false);

  const std::string prefix = "tlcs_fp_topn_" + RandomString();
  Schema schema(
      "events",
      {Column("id", ValueType::kInt64), Column("uid", ValueType::kInt64),
       Column("v", ValueType::kVarChar), Column("ts", ValueType::kVarChar),
       Column("bucket", ValueType::kInt64)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  constexpr int64_t kRows = 34000;
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CreateTable(ctx, schema).GetStatus(), Status::kSuccess);
    Table tbl = db->GetTable(ctx, "events").MoveValue();
    for (int64_t i = 0; i < kRows; ++i) {
      Value v =
          (i % 100 == 0)
              ? Value()
              : Value((i < 32768 ? "a_" : "z_") + std::to_string(100000 + i));
      Value ts =
          (i % 50 == 0)
              ? Value()
              : Value("2013-07-14 12:" + std::string((i % 60) < 10 ? "0" : "") +
                      std::to_string(i % 60) + ":05");
      Value bucket = i % 50 == 0 ? Value() : Value((i % 293) - 150);
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_, Row({Value(i), Value(i % 37), std::move(v),
                                    std::move(ts), std::move(bucket)}))
              .HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "events"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  {
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine engine(*db);

    // TopN over VarChar column spanning 2 blocks + OFFSET (prunes lower
    // block).
    const auto topn_vc = RunSql(
        &engine, &ctx,
        "SELECT id, v FROM events WHERE v IS NOT NULL ORDER BY v DESC LIMIT 4 "
        "OFFSET 2");
    ASSERT_EQ(topn_vc.size(), 4U);
    EXPECT_EQ(topn_vc[0][0].AsInt64(), 33997);
    EXPECT_EQ(topn_vc[3][0].AsInt64(), 33994);

    // Fused scalar COUNT(DISTINCT uid), SUM(id), COUNT(*).
    const auto scalar =
        RunSql(&engine, &ctx,
               "SELECT COUNT(DISTINCT uid), SUM(id), COUNT(*) FROM events");
    ASSERT_EQ(scalar.size(), 1U);
    EXPECT_EQ(scalar[0][0].AsInt64(), 37);
    EXPECT_EQ(scalar[0][1].AsInt64(), (kRows - 1) * kRows / 2);
    EXPECT_EQ(scalar[0][2].AsInt64(), kRows);

    const auto extrema = RunSql(
        &engine, &ctx, "SELECT MIN(id), MAX(id), COUNT(uid) FROM events");
    ASSERT_EQ(extrema.size(), 1U);
    EXPECT_EQ(extrema[0][0].AsInt64(), 0);
    EXPECT_EQ(extrema[0][1].AsInt64(), kRows - 1);
    EXPECT_EQ(extrema[0][2].AsInt64(), kRows);

    const auto exact_counts =
        RunSql(&engine, &ctx,
               "SELECT uid, COUNT(*) FROM events WHERE uid <> 0 GROUP BY uid "
               "ORDER BY uid LIMIT 3 OFFSET 1");
    ASSERT_EQ(exact_counts.size(), 3U);
    for (size_t i = 0; i < exact_counts.size(); ++i) {
      const int64_t key = static_cast<int64_t>(i) + 2;
      EXPECT_EQ(exact_counts[i][0].AsInt64(), key);
      EXPECT_EQ(exact_counts[i][1].AsInt64(), ((kRows - 1 - key) / 37) + 1);
    }
    const auto null_group = RunSql(
        &engine, &ctx,
        "SELECT bucket, MIN(v), COUNT(*) AS c FROM events GROUP BY bucket "
        "ORDER BY c DESC LIMIT 1");
    ASSERT_EQ(null_group.size(), 1U);
    EXPECT_TRUE(null_group[0][0].IsNull());
    EXPECT_EQ(null_group[0][1].AsStringView(), "a_100050");
    EXPECT_EQ(null_group[0][2].AsInt64(), kRows / 50);

    // ParameterSlot correlated EXISTS filter inside subquery.
    const auto param = RunSql(
        &engine, &ctx,
        "SELECT COUNT(*) FROM events e WHERE e.uid = 5 AND EXISTS ("
        "SELECT 1 FROM events e2 WHERE e2.id = e.id AND e2.uid = 5 LIMIT 1)");
    ASSERT_EQ(param.size(), 1U);
    EXPECT_GT(param[0][0].AsInt64(), 0);

    // Delta group keys (kColInt64Delta): uid, uid - 1.
    const auto delta = RunSql(&engine, &ctx,
                              "SELECT uid, uid - 1, COUNT(*) AS c FROM events "
                              "GROUP BY uid, uid - 1 ORDER BY uid ASC LIMIT 3");
    ASSERT_EQ(delta.size(), 3U);
    EXPECT_EQ(delta[0][0].AsInt64(), 0);
    EXPECT_EQ(delta[0][1].AsInt64(), -1);
    EXPECT_EQ(delta[1][0].AsInt64(), 1);
    EXPECT_EQ(delta[1][1].AsInt64(), 0);
  }

  db->DeleteAll();
}

TEST(ColumnarTlcsFastPathTest, ParameterSlotInsideArrayStillFolds) {
  // A bound ParameterSlot nested inside an ARRAY literal must be unwrapped
  // before compilation: downstream constant folding refuses any expression
  // that still ContainsParameterSlot, so a slot left inside the array would
  // reject the whole TLCS fast-path predicate.
  Schema scan_schema("t", {Column("x", ValueType::kVarChar)});
  Expression slot = std::make_shared<ParameterSlot>(0, Value("a"));
  Expression array = ArrayExpressionExp(
      {std::move(slot), ConstantValueExp(Value("b"))}, "STRING");
  Expression element = FunctionCallExp(
      "array_to_string", {std::move(array), ConstantValueExp(Value(","))});
  Expression predicate = BinaryExpressionExp(
      ColumnValueExp("x"), BinaryOperation::kEquals, std::move(element));

  TlcsFile tlcs;
  ColumnarPredicateSet set;
  ASSERT_TRUE(relational_detail::CompilePredicatesFromExpressions(
      {predicate}, {}, tlcs, scan_schema, {}, scan_schema, &set,
      /*allow_parameter_slots=*/true));
  ASSERT_EQ(set.compares.size(), 1U);
  EXPECT_EQ(set.compares[0].string_constant, "a,b");
}

TEST(ColumnarTlcsFastPathTest, MixedGroupTableRespectsQueryMemoryBudget) {
  using namespace relational_detail;
  BudgetResetGuard budget_guard;
  const auto& budget = QueryMemoryBudget::Global();
  const size_t baseline = budget.Used();
  ProcessMemoryBudget::Global().ResetForTest(static_cast<size_t>(24 * 1024));
  const std::vector<ColGroupKeySpec> keys{
      MakeKeySpec(ColKeyKind::kColInt64, 0, "a"),
      MakeKeySpec(ColKeyKind::kColInt64, 1, "b")};
  const std::vector<ColAggSpec> aggs{
      MakeAggSpec(nullptr, ColAggKind::kCountStar, 0)};
  {
    QueryMemoryCharge charge;
    WorkerGroupTable table(&keys, &aggs, &charge);
    ASSERT_EQ(table.Reserve(64), Status::kSuccess);
    const size_t slot_bytes = charge.Bytes();
    for (int64_t i = 0; i < 64; ++i) {
      const std::array<RuntimeKeyCell, 2> key{{{.int_val = i},
                                                {.int_val = i + 1}}};
      const uint64_t hash = GroupKeySliceHasher::HashTyped(key.data(), keys);
      auto group = table.FindOrInsert(key.data(), hash);
      if (!group.HasValue()) {
        EXPECT_EQ(group.GetStatus(), StatusCode::kMemoryLimitExceeded);
        EXPECT_EQ(table.NumGroups(), static_cast<size_t>(i));
        break;
      }
      table.AggsAt(group.Value())[0].count = i + 1;
    }
    EXPECT_GT(table.NumGroups(), 0U);
    EXPECT_LT(table.NumGroups(), 64U);
    EXPECT_GT(charge.Bytes(), slot_bytes);
    const std::array<RuntimeKeyCell, 2> first{{{.int_val = 0},
                                                {.int_val = 1}}};
    auto group = table.FindOrInsert(
        first.data(), GroupKeySliceHasher::HashTyped(first.data(), keys));
    ASSERT_TRUE(group.HasValue());
    EXPECT_EQ(table.AggsAt(group.Value())[0].count, 1);
  }
  EXPECT_EQ(budget.Used(), baseline);
}

TEST(ColumnarTlcsFastPathTest, GroupMergeRejectsIncompleteAggregateState) {
  using namespace relational_detail;
  const std::vector<ColGroupKeySpec> keys{
      MakeKeySpec(ColKeyKind::kColInt64, 0, "a")};
  const std::vector<ColAggSpec> aggs{
      MakeAggSpec(nullptr, ColAggKind::kCountStar, 0),
      MakeAggSpec(nullptr, ColAggKind::kCountCol, 0)};
  WorkerGroupTable table(&keys, &aggs, nullptr);
  const RuntimeKeyCell key{.int_val = 1};
  RuntimeAggState count;
  count.count = 1;
  count.has_value = true;

  EXPECT_EQ(table.MergeFromGroup(&key, 0, {&count, 1}),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(table.NumGroups(), 0U);
}

TEST(ColumnarTlcsFastPathTest, DistinctAggregateMemoryFollowsMovedState) {
  using namespace relational_detail;
  BudgetResetGuard budget_guard;
  auto& process = ProcessMemoryBudget::Global();
  const auto& budget = QueryMemoryBudget::Global();
  const size_t baseline = budget.Used();
  process.ResetForTest(512);
  {
    PagedStringArena arena;
    RuntimeAggState worker;
    RuntimeAggState merged;
    TlcsFile::DecodedColumnChunk ints;
    ints.type = ValueType::kInt64;
    ints.int_cells.resize(64);
    const auto spec = MakeAggSpec(nullptr, ColAggKind::kCountDistinctInt64, 0);
    const std::vector<const TlcsFile::DecodedColumnChunk*> columns{&ints};
    ASSERT_EQ(FoldRowIntoAggState(spec, columns, 0, worker, arena),
              Status::kSuccess);
    const size_t used = budget.Used();
    EXPECT_GT(used, baseline);
    ASSERT_EQ(MergeAggState(spec, merged, worker, arena), Status::kSuccess);
    EXPECT_EQ(budget.Used(), used);
    ASSERT_EQ(FoldRowIntoAggState(spec, columns, 0, merged, arena),
              Status::kSuccess);
    EXPECT_EQ(budget.Used(), used);
    Status status = Status::kSuccess;
    for (uint32_t i = 1; i < ints.int_cells.size() && status.ok(); ++i) {
      ints.int_cells[i] = i;
      status = FoldRowIntoAggState(spec, columns, i, merged, arena);
    }
    EXPECT_EQ(status, StatusCode::kMemoryLimitExceeded);
  }
  EXPECT_EQ(budget.Used(), baseline);

  process.ResetForTest(static_cast<size_t>(4 * 1024 * 1024));
  {
    QueryMemoryCharge arena_charge;
    PagedStringArena arena(&arena_charge);
    RuntimeAggState worker;
    RuntimeAggState merged;
    TlcsFile::DecodedColumnChunk strings;
    strings.type = ValueType::kVarChar;
    strings.varchar_bytes = {'a'};
    strings.varchar_ends = {1};
    const auto spec =
        MakeAggSpec(nullptr, ColAggKind::kCountDistinctVarChar, 0);
    const std::vector<const TlcsFile::DecodedColumnChunk*> columns{&strings};
    ASSERT_EQ(FoldRowIntoAggState(spec, columns, 0, worker, arena),
              Status::kSuccess);
    const size_t used = budget.Used();
    EXPECT_GT(used, baseline + arena_charge.Bytes());
    ASSERT_EQ(MergeAggState(spec, merged, worker, arena), Status::kSuccess);
    ASSERT_EQ(FoldRowIntoAggState(spec, columns, 0, merged, arena),
              Status::kSuccess);
    EXPECT_EQ(budget.Used(), used);
  }
  EXPECT_EQ(budget.Used(), baseline);
}

TEST(ColumnarTlcsFastPathTest, EmptyVarcharExtremaSurviveAWorkerMerge) {
  using namespace relational_detail;
  TlcsFile::DecodedColumnChunk strings;
  strings.type = ValueType::kVarChar;
  strings.varchar_bytes = {'x'};
  strings.varchar_ends = {0, 1};
  const std::vector<const TlcsFile::DecodedColumnChunk*> columns{&strings};
  for (ColAggKind kind : {ColAggKind::kMinVarChar, ColAggKind::kMaxVarChar}) {
    const auto spec = MakeAggSpec(nullptr, kind, 0);
    PagedStringArena worker_arena;
    RuntimeAggState worker;
    ASSERT_EQ(FoldRowIntoAggState(spec, columns, 0, worker, worker_arena),
              Status::kSuccess);
    ASSERT_TRUE(worker.has_value);
    EXPECT_NE(worker.str_ptr, nullptr);
    EXPECT_EQ(worker.str_len, 0U);
    EXPECT_EQ(worker_arena.AllocatedBytes(), 0U);
    PagedStringArena output_arena;
    RuntimeAggState merged;
    ASSERT_EQ(MergeAggState(spec, merged, worker, output_arena),
              Status::kSuccess);
    auto result = FinishAggState(spec, merged);
    ASSERT_TRUE(result.HasValue()) << result.GetStatus();
    EXPECT_EQ(result.Value(), Value(""));
  }
}

TEST(ColumnarTlcsFastPathTest, PartitionedAggStrategiesAndTightBudgetFallback) {
  BudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(size_t{256} << 20, size_t{64} << 20, size_t{64} << 20,
                      /*check_rss_pressure=*/false);

  const std::string prefix = "tlcs_part_start_" + RandomString();
  Schema schema(
      "hits",
      {Column("ip", ValueType::kInt64), Column("uid", ValueType::kInt64),
       Column("tag", ValueType::kVarChar), Column("ts", ValueType::kVarChar)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  page_id_t first_pid = 0;
  constexpr int64_t kRows = 1200;
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CreateTable(ctx, schema).GetStatus(), Status::kSuccess);
    Table tbl = db->GetTable(ctx, "hits").MoveValue();
    first_pid = tbl.FirstPageId();
    for (int64_t i = 0; i < kRows; ++i) {
      // Group ip=999 has NULL uid on all rows so COUNT(DISTINCT uid) == 0.
      const int64_t ip = (i < 30) ? 999 : (10 + (i % 15));
      Value uid = (i < 30) ? Value() : Value(1000 + (i % 7));
      Value tag = Value("m_" + std::to_string(ip));
      Value ts = Value("2013-07-14 08:15:00");
      ASSERT_TRUE(tbl.Insert(ctx.txn_, Row({Value(ip), std::move(uid),
                                            std::move(tag), std::move(ts)}))
                      .HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "hits"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  const std::string tlcs_path =
      prefix + ".db.tlcs." + std::to_string(first_pid);
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TlcsFile>, tlcs,
                        TlcsFile::Open(tlcs_path, schema));
  const std::vector<uint32_t> blocks = {0};
  const ColumnarPredicateSet empty_preds;
  const std::vector<relational_detail::PositionalHaving> empty_having;

  // 1) Path A: Pair-Sharded COUNT(DISTINCT uid) including NULL group
  // distinct=0.
  {
    SelectStatement stmt;
    const Expression count_dist = std::make_shared<AggregateExpression>(
        AggregationType::kCount, ColumnValueExp("uid"), true);
    const Expression count_star = std::make_shared<AggregateExpression>(
        AggregationType::kCount, ColumnValueExp("*"), false);
    const std::vector<ColGroupKeySpec> keys = {
        MakeKeySpec(ColKeyKind::kColVarChar, 2, "tag")};
    const std::vector<ColAggSpec> aggs = {
        MakeAggSpec(&count_dist->AsAggregateExpression(),
                    ColAggKind::kCountDistinctInt64, 1),
        MakeAggSpec(&count_star->AsAggregateExpression(),
                    ColAggKind::kCountStar, 0)};
    stmt.SetSelectList({NamedExpression("tag", ColumnValueExp("tag")),
                        NamedExpression("u", count_dist),
                        NamedExpression("c", count_star)});
    const std::vector<OutputSlotBinding> emits = {
        MakeSlotBinding(OutputSlotBinding::Kind::kKey, 0),
        MakeSlotBinding(OutputSlotBinding::Kind::kAgg, 0),
        MakeSlotBinding(OutputSlotBinding::Kind::kAgg, 1)};
    const std::vector<ColumnarOrderBinding> orders = {
        {MakeSlotBinding(OutputSlotBinding::Kind::kAgg, 0),
         SortKeyOrder(/*ascending=*/true, /*nulls_first=*/true,
                      Collation::kNone)}};

    PartitionedColumnarAggRequest req{
        .stmt = stmt,
        .tlcs = tlcs,
        .qualified_schema = schema,
        .table_schema = schema,
        .preds = empty_preds,
        .candidate_blocks = blocks,
        .key_specs = keys,
        .agg_specs = aggs,
        .positional_having = empty_having,
        .emit_bindings = emits,
        .order_bindings = orders,
        .runtime = nullptr,
        .publish_lstable_marker = false,
        .fold_cache_key = "",
        .scan_begin = std::chrono::steady_clock::now(),
        .dir_ms = 0.0,
        .any_scalar_distinct = true};
    ASSIGN_OR_ASSERT_FAIL(std::optional<Relation>, res,
                          TryRunPartitionedColumnarTlcsAggregate(req));
    if (!res.has_value()) {
      ADD_FAILURE() << "expected optional value";
      return;
    }
    if (!res.has_value()) {
      return;
    }
    ASSERT_EQ(res->rows.size(), 16U);
    EXPECT_EQ(res->rows[0][0].AsStringView(), "m_999");
    EXPECT_EQ(res->rows[0][1].AsInt64(), 0);
    EXPECT_EQ(res->rows[0][2].AsInt64(), 30);
  }

  // 2) Path B: Fixed-keys with collapsed `kColInt64Delta` (`ip, ip - 1`).
  {
    SelectStatement stmt;
    const Expression count_star = std::make_shared<AggregateExpression>(
        AggregationType::kCount, ColumnValueExp("*"), false);
    const std::vector<ColGroupKeySpec> keys = {
        MakeKeySpec(ColKeyKind::kColInt64, 0, "ip"),
        MakeKeySpec(ColKeyKind::kColInt64Delta, 0, "(ip - 1)", -1)};
    const std::vector<ColAggSpec> aggs = {MakeAggSpec(
        &count_star->AsAggregateExpression(), ColAggKind::kCountStar, 0)};
    stmt.SetSelectList(
        {NamedExpression("ip", ColumnValueExp("ip")),
         NamedExpression(
             "d", BinaryExpressionExp(ColumnValueExp("ip"),
                                      BinaryOperation::kSubtract,
                                      ConstantValueExp(Value(int64_t{1})))),
         NamedExpression("c", count_star)});
    stmt.SetLimit(2);
    const std::vector<OutputSlotBinding> emits = {
        MakeSlotBinding(OutputSlotBinding::Kind::kKey, 0),
        MakeSlotBinding(OutputSlotBinding::Kind::kKey, 1),
        MakeSlotBinding(OutputSlotBinding::Kind::kAgg, 0)};
    const std::vector<ColumnarOrderBinding> orders = {
        {MakeSlotBinding(OutputSlotBinding::Kind::kKey, 0),
         SortKeyOrder(/*ascending=*/false, /*nulls_first=*/false,
                      Collation::kNone)}};

    PartitionedColumnarAggRequest req{
        .stmt = stmt,
        .tlcs = tlcs,
        .qualified_schema = schema,
        .table_schema = schema,
        .preds = empty_preds,
        .candidate_blocks = blocks,
        .key_specs = keys,
        .agg_specs = aggs,
        .positional_having = empty_having,
        .emit_bindings = emits,
        .order_bindings = orders,
        .runtime = nullptr,
        .publish_lstable_marker = false,
        .fold_cache_key = "",
        .scan_begin = std::chrono::steady_clock::now(),
        .dir_ms = 0.0,
        .any_scalar_distinct = false};
    ASSIGN_OR_ASSERT_FAIL(std::optional<Relation>, res,
                          TryRunPartitionedColumnarTlcsAggregate(req));
    if (!res.has_value()) {
      ADD_FAILURE() << "expected optional value";
      return;
    }
    if (!res.has_value()) {
      return;
    }
    ASSERT_EQ(res->rows.size(), 2U);
    EXPECT_EQ(res->rows[0][0].AsInt64(), 999);
    EXPECT_EQ(res->rows[0][1].AsInt64(), 998);
    EXPECT_EQ(res->rows[0][2].AsInt64(), 30);
  }

  // 3) Path C + OOC tight-budget fallback returns std::nullopt without leak.
  {
    SelectStatement stmt;
    const Expression count_star = std::make_shared<AggregateExpression>(
        AggregationType::kCount, ColumnValueExp("*"), false);
    ColGroupKeySpec ts_key =
        MakeKeySpec(ColKeyKind::kUtcTimestampExtract, 3, "m");
    ts_key.trunc_unit = relational_detail::UtcTimestampTruncUnit::kMinute;
    const std::vector<ColGroupKeySpec> keys = {
        MakeKeySpec(ColKeyKind::kColVarChar, 2, "tag"), ts_key};
    const std::vector<ColAggSpec> aggs = {MakeAggSpec(
        &count_star->AsAggregateExpression(), ColAggKind::kCountStar, 0)};
    stmt.SetSelectList(
        {NamedExpression("tag", ColumnValueExp("tag")),
         NamedExpression(
             "m", FunctionCallExp("extract_minute", {ColumnValueExp("ts")})),
         NamedExpression("c", count_star)});
    const std::vector<OutputSlotBinding> emits = {
        MakeSlotBinding(OutputSlotBinding::Kind::kKey, 0),
        MakeSlotBinding(OutputSlotBinding::Kind::kKey, 1),
        MakeSlotBinding(OutputSlotBinding::Kind::kAgg, 0)};
    const std::vector<ColumnarOrderBinding> orders;

    const size_t avail = budget.Available();
    ASSERT_GT(avail, size_t{64} << 10);
    const size_t hold = avail - 4096;
    ASSERT_TRUE(budget.TryReserve(MemoryPoolKind::kQuery, hold, false));
    const size_t before_query_used = budget.QueryUsed();

    PartitionedColumnarAggRequest req{
        .stmt = stmt,
        .tlcs = tlcs,
        .qualified_schema = schema,
        .table_schema = schema,
        .preds = empty_preds,
        .candidate_blocks = blocks,
        .key_specs = keys,
        .agg_specs = aggs,
        .positional_having = empty_having,
        .emit_bindings = emits,
        .order_bindings = orders,
        .runtime = nullptr,
        .publish_lstable_marker = false,
        .fold_cache_key = "",
        .scan_begin = std::chrono::steady_clock::now(),
        .dir_ms = 0.0,
        .any_scalar_distinct = false};
    ASSIGN_OR_ASSERT_FAIL(std::optional<Relation>, fallback,
                          TryRunPartitionedColumnarTlcsAggregate(req));
    EXPECT_FALSE(fallback.has_value());
    EXPECT_EQ(budget.QueryUsed(), before_query_used);
    budget.Release(MemoryPoolKind::kQuery, hold);
  }

  // 4) Path C under AlwaysSpill fault injection: every completed pass-1
  // slab chunk is written to a SpillStore segment and read back in pass 2,
  // and the query still completes with correct results. `COUNT(uid)`
  // disables the COUNT(*)-only L1 combiner so every row flows through the
  // shard writers.
  {
    SelectStatement stmt;
    const Expression count_star = std::make_shared<AggregateExpression>(
        AggregationType::kCount, ColumnValueExp("*"), false);
    const Expression count_uid = std::make_shared<AggregateExpression>(
        AggregationType::kCount, ColumnValueExp("uid"), false);
    ColGroupKeySpec ts_key =
        MakeKeySpec(ColKeyKind::kUtcTimestampExtract, 3, "m");
    ts_key.trunc_unit = relational_detail::UtcTimestampTruncUnit::kMinute;
    const std::vector<ColGroupKeySpec> keys = {
        MakeKeySpec(ColKeyKind::kColVarChar, 2, "tag"), ts_key};
    const std::vector<ColAggSpec> aggs = {
        MakeAggSpec(&count_star->AsAggregateExpression(),
                    ColAggKind::kCountStar, 0),
        MakeAggSpec(&count_uid->AsAggregateExpression(), ColAggKind::kCountCol,
                    1)};
    stmt.SetSelectList(
        {NamedExpression("tag", ColumnValueExp("tag")),
         NamedExpression(
             "m", FunctionCallExp("extract_minute", {ColumnValueExp("ts")})),
         NamedExpression("c", count_star), NamedExpression("uc", count_uid)});
    const std::vector<OutputSlotBinding> emits = {
        MakeSlotBinding(OutputSlotBinding::Kind::kKey, 0),
        MakeSlotBinding(OutputSlotBinding::Kind::kKey, 1),
        MakeSlotBinding(OutputSlotBinding::Kind::kAgg, 0),
        MakeSlotBinding(OutputSlotBinding::Kind::kAgg, 1)};
    const std::vector<ColumnarOrderBinding> orders;

    budget.SetTestAlwaysSpill(true);

    PartitionedColumnarAggRequest req{
        .stmt = stmt,
        .tlcs = tlcs,
        .qualified_schema = schema,
        .table_schema = schema,
        .preds = empty_preds,
        .candidate_blocks = blocks,
        .key_specs = keys,
        .agg_specs = aggs,
        .positional_having = empty_having,
        .emit_bindings = emits,
        .order_bindings = orders,
        .runtime = nullptr,
        .publish_lstable_marker = false,
        .fold_cache_key = "",
        .scan_begin = std::chrono::steady_clock::now(),
        .dir_ms = 0.0,
        .any_scalar_distinct = false};
    ASSIGN_OR_ASSERT_FAIL(std::optional<Relation>, res,
                          TryRunPartitionedColumnarTlcsAggregate(req));
    budget.SetTestAlwaysSpill(false);
    if (!res.has_value()) {
      ADD_FAILURE() << "expected optional value";
      return;
    }
    if (res.has_value()) {
      ASSERT_EQ(res->rows.size(), 16U);
      int64_t total = 0;
      int64_t total_uid = 0;
      int64_t m999 = -1;
      for (const Row& row : res->rows) {
        total += row[2].AsInt64();
        total_uid += row[3].AsInt64();
        if (row[0].AsStringView() == "m_999") {
          m999 = row[2].AsInt64();
        }
      }
      EXPECT_EQ(total, kRows);
      EXPECT_EQ(total_uid, kRows - 30);
      EXPECT_EQ(m999, 30);
    }
  }

  db->DeleteAll();
}

TEST(ColumnarTlcsFastPathTest,
     NullableDictionaryBlocksKeepNullFiltersAndCounts) {
  const std::string prefix = "tlcs_null_dict_" + RandomString();
  Schema schema("events", {Column("id", ValueType::kInt64),
                           Column("s", ValueType::kVarChar),
                           Column("g", ValueType::kInt64)});
  auto db = Database::Create(prefix).MoveValue();
  page_id_t first_pid = 0;
  {
    auto context = db->BeginContext();
    ASSERT_EQ(db->CreateTable(context, schema).GetStatus(), Status::kSuccess);
    auto table = db->GetTable(context, "events").MoveValue();
    first_pid = table.FirstPageId();
    for (int64_t i = 0; i < 300; ++i) {
      Value text = i % 3 == 0 ? Value() : Value(i % 3 == 1 ? "a" : "b");
      const int64_t group = i % 3 == 0 ? 0 : 1;
      ASSERT_TRUE(table
                      .Insert(context.txn_,
                              Row({Value(i), std::move(text), Value(group)}))
                      .HasValue());
    }
    ASSERT_EQ(context.PreCommit(), Status::kSuccess);
  }
  {
    auto context = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(context, "events"), Status::kSuccess);
    ASSERT_EQ(context.PreCommit(), Status::kSuccess);
  }
  ASSIGN_OR_ASSERT_FAIL(
      std::shared_ptr<TlcsFile>, tlcs,
      TlcsFile::Open(prefix + ".db.tlcs." + std::to_string(first_pid), schema));
  ASSERT_EQ(tlcs->DictKinds().at(1), TlcsDictKind::kCompleteNarrow);
  auto directory = tlcs->GetDirGroup(0);
  ASSERT_TRUE(directory.HasValue());
  ASSERT_FALSE(directory.Value()->blocks.empty());
  const auto& column = directory.Value()->blocks[0].columns[1];
  ASSERT_EQ(column.encoding, TlcsColumnEncoding::kNarrowDict);
  ASSERT_TRUE(column.has_null);
  ColumnarPredicateSet predicates;
  predicates.nulls.push_back({.column = 1, .is_null = true});
  relational_detail::FinalizePredicateMetadata(*tlcs, schema, &predicates);
  auto blocks = relational_detail::SelectCandidateBlocks(*tlcs, predicates);
  ASSERT_TRUE(blocks.HasValue());
  EXPECT_FALSE(blocks.Value().empty());
  auto context = db->BeginReadOnlyContext();
  SqlEngine engine(*db);
  EXPECT_EQ(
      RunSql(&engine, &context, "SELECT COUNT(*) FROM events WHERE s IS NULL"),
      (std::vector<Row>{Row({Value(100)})}));
  EXPECT_EQ(RunSql(&engine, &context,
                   "SELECT g, AVG(BYTE_LENGTH(s)), COUNT(*) FROM events "
                   "GROUP BY g ORDER BY g"),
            (std::vector<Row>{Row({Value(0), Value(), Value(100)}),
                              Row({Value(1), Value(1.0), Value(200)})}));
  ASSERT_EQ(context.PreCommit(), Status::kSuccess);
}

// The TLCS dictionary is file-wide. A scan restricted to one block must not
// emit groups for dictionary values that only occur in other blocks (those
// used to show up as `(key, COUNT(DISTINCT x) = 0)` "ghost" rows).
TEST(ColumnarTlcsFastPathTest, DistinctAggregateSkipsAbsentDictionaryKeys) {
  BudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(size_t{256} << 20, size_t{64} << 20, size_t{64} << 20,
                      /*check_rss_pressure=*/false);

  const std::string prefix = "tlcs_ghost_dict_" + RandomString();
  Schema schema("events", {Column("k", ValueType::kVarChar),
                           Column("x", ValueType::kInt64)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  page_id_t first_pid = 0;
  constexpr int64_t kRows = 33000;  // > 2 * RowPage::kMaxColumnarRows
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CreateTable(ctx, schema).GetStatus(), Status::kSuccess);
    Table tbl = db->GetTable(ctx, "events").MoveValue();
    first_pid = tbl.FirstPageId();
    // NOLINTNEXTLINE(modernize-avoid-c-arrays) runtime-sized buffer
    static const char* kFirst[] = {"a", "b", "c", "d"};
    // NOLINTNEXTLINE(modernize-avoid-c-arrays) runtime-sized buffer
    static const char* kSecond[] = {"e", "f", "g", "h"};
    for (int64_t i = 0; i < kRows; ++i) {
      const char* key = (i < 32768) ? kFirst[i % 4] : kSecond[i % 4];
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_, Row({Value(key), Value(i % 10)})).HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "events"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  const std::string tlcs_path =
      prefix + ".db.tlcs." + std::to_string(first_pid);
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TlcsFile>, tlcs,
                        TlcsFile::Open(tlcs_path, schema));
  ASSERT_GE(tlcs->BlockCount(), 2U);

  SelectStatement stmt;
  const Expression count_dist = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("x"), true);
  const std::vector<ColGroupKeySpec> keys = {
      MakeKeySpec(ColKeyKind::kColVarChar, 0, "k")};
  const std::vector<ColAggSpec> aggs = {
      MakeAggSpec(&count_dist->AsAggregateExpression(),
                  ColAggKind::kCountDistinctInt64, 1)};
  stmt.SetSelectList({NamedExpression("k", ColumnValueExp("k")),
                      NamedExpression("d", count_dist)});
  const std::vector<OutputSlotBinding> emits = {
      MakeSlotBinding(OutputSlotBinding::Kind::kKey, 0),
      MakeSlotBinding(OutputSlotBinding::Kind::kAgg, 0)};
  const std::vector<ColumnarOrderBinding> orders = {
      {MakeSlotBinding(OutputSlotBinding::Kind::kKey, 0),
       SortKeyOrder(/*ascending=*/true, /*nulls_first=*/true,
                    Collation::kNone)}};

  PartitionedColumnarAggRequest req{
      .stmt = stmt,
      .tlcs = tlcs,
      .qualified_schema = schema,
      .table_schema = schema,
      .preds = ColumnarPredicateSet{},
      .candidate_blocks = {0},  // only the first block's keys are present
      .key_specs = keys,
      .agg_specs = aggs,
      .positional_having = std::vector<relational_detail::PositionalHaving>{},
      .emit_bindings = emits,
      .order_bindings = orders,
      .runtime = nullptr,
      .publish_lstable_marker = false,
      .fold_cache_key = "",
      .scan_begin = std::chrono::steady_clock::now(),
      .dir_ms = 0.0,
      .any_scalar_distinct = true};
  ASSIGN_OR_ASSERT_FAIL(std::optional<Relation>, res,
                        TryRunPartitionedColumnarTlcsAggregate(req));
  if (!res.has_value()) {
      ADD_FAILURE() << "expected optional value";
      return;
    }
  if (!res.has_value()) {
    return;
  }
  ASSERT_EQ(res->rows.size(), 4U) << "ghost dictionary groups leaked";
  for (const Row& row : res->rows) {
    const auto key = row[0].AsStringView();
    EXPECT_TRUE(key == "a" || key == "b" || key == "c" || key == "d")
        << "unexpected key: " << key;
    // i % 4 selects the key, so i % 10 takes 5 values per key over a
    // complete lcm(4, 10) = 20 cycle.
    EXPECT_EQ(row[1].AsInt64(), 5);
  }

  db->DeleteAll();
}

TEST(ColumnarTlcsFastPathTest, ClickBenchSixOptimizationsAndSemanticTraps) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "execute_query unavailable";
  }
  BudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  // Small columnar pool cap forces paged TLCS mode so multi-block pruning,
  // key-fiber subsetting, two-stage Top-N, and monotone cutoff are exercised.
  budget.ResetForTest(size_t{512} << 20, size_t{128} << 20, size_t{2} << 20,
                      /*check_rss_pressure=*/false);

  const std::string prefix = "tlcs_cb6_" + RandomString();
  Schema schema(
      "hits_cb",
      {Column("uid", ValueType::kInt64), Column("ip", ValueType::kInt64),
       Column("counter_id", ValueType::kInt64),
       Column("se_id", ValueType::kInt64), Column("ae_id", ValueType::kInt64),
       Column("phrase", ValueType::kVarChar),
       Column("url", ValueType::kVarChar), Column("title", ValueType::kVarChar),
       Column("referer", ValueType::kVarChar),
       Column("event_time", ValueType::kVarChar)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  // ~10 blocks (> 8 blocks so CB-2 key-fiber & CB-6 monotone cutoff activate).
  // NOLINTNEXTLINE(bugprone-implicit-widening-of-multiplication-result)
  // operands bounded
  constexpr int64_t kRows =
      10 *    // NOLINT(bugprone-implicit-widening-of-multiplication-result)
      16384;  // NOLINT(bugprone-implicit-widening-of-multiplication-result)
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CreateTable(ctx, schema).GetStatus(), Status::kSuccess);
    Table tbl = db->GetTable(ctx, "hits_cb").MoveValue();
    for (int64_t i = 0; i < kRows; ++i) {
      const int64_t blk = i / 16384;
      const int64_t uid = (blk * 1000) + (i % 25);
      const int64_t ip = 1000 + (i % 50);
      const int64_t cid = (i % 8 == 0) ? 62 : 10;
      // Only block 0 has se_id = 0 AND ae_id = 0; blocks 1..9 have se_id = 1
      // and NULL referer on odd rows (testing CB-5 conditional column elision +
      // `CASE WHEN false THEN NULL ELSE '' END == ''`!).
      const int64_t se_id = (blk == 0 && (i % 4 == 0)) ? 0 : 1;
      const int64_t ae_id = 0;
      // Make NULL `phrase` the #1 most frequent group so CB-3's Top-N winner
      // set includes a NULL group (`two_stage_late_agg_sql_in_null_group_bug`)!
      Value phrase_v =
          (i % 3 == 0) ? Value() : Value("kw_" + std::to_string(i % 40));
      Value url_v("https://example.org/google/p" +
                  std::to_string(999 - (i % 200)));
      Value title_v("Google Search " + std::to_string(i % 50));
      Value ref_v = (se_id == 1 && (i % 2 == 1))
                        ? Value()
                        : Value("https://www.dom" + std::to_string(i % 12) +
                                ".com/path/" + std::to_string(i % 7));
      // Strictly non-decreasing canonical 19-byte UTC timestamps across blocks:
      // 1 minute per 512 rows (~32 distinct minutes per block, 320 total).
      const int64_t total_min = i / 512;
      const int64_t hh = (total_min / 60) % 24;
      const int64_t mm = total_min % 60;
      const int64_t day = 14 + ((total_min / 60) / 24);
      // NOLINTNEXTLINE(modernize-avoid-c-arrays) runtime-sized buffer
      char ts_buf[32];
      (void)std::snprintf(
          ts_buf, sizeof(ts_buf), "2013-07-%02lld %02lld:%02lld:%02lld",
          static_cast<long long>(day), static_cast<long long>(hh),
          static_cast<long long>(mm), static_cast<long long>(i % 60));
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_,
                     Row({Value(uid), Value(ip), Value(cid), Value(se_id),
                          Value(ae_id), std::move(phrase_v), std::move(url_v),
                          std::move(title_v), std::move(ref_v),
                          Value(std::string(ts_buf))}))
              .HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "hits_cb"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  auto run_fast = [&](const std::string& sql) {
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine eng(*db);
    return RunSql(&eng, &ctx, sql);
  };
  auto run_ref = [&](const std::string& sql) {
    const ScopedEnvVar guard("TINYLAMB_DISABLE_COLUMNAR_TLCS_AGG", "1");
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine eng(*db);
    eng.SetForceRelationalFallback(true);
    return RunSql(&eng, &ctx, sql);
  };

  // CB-1: Q35 (`GROUP BY 1, url`) & Q36 (`GROUP BY ip, ip-1, ip-2, ip-3`).
  const std::string q35 =
      "SELECT 1, url, COUNT(*) AS c FROM hits_cb GROUP BY 1, url "
      "ORDER BY c DESC, url ASC LIMIT 5";
  EXPECT_EQ(run_fast(q35), run_ref(q35));

  const std::string q36 =
      "SELECT ip, ip - 1, ip - 2, ip - 3, COUNT(*) AS c FROM hits_cb "
      "GROUP BY ip, ip - 1, ip - 2, ip - 3 ORDER BY c DESC, ip ASC LIMIT 5";
  EXPECT_EQ(run_fast(q36), run_ref(q36));

  // CB-2: Q18 (`SELECT uid, phrase, COUNT(*) FROM hits_cb GROUP BY uid, phrase
  // LIMIT 10`) — verify exact subset membership against full-table GROUP BY!
  const auto full_q18 = run_fast(
      "SELECT uid, phrase, COUNT(*) FROM hits_cb GROUP BY uid, phrase");
  TlcsFile::ResetDecodeCallsForTest();
  const auto fast_q18 = run_fast(
      "SELECT uid, phrase, COUNT(*) FROM hits_cb GROUP BY uid, phrase "
      "LIMIT 10");
  ASSERT_EQ(fast_q18.size(), 10U);
  for (size_t i = 0; i < fast_q18.size(); ++i) {
    for (size_t j = i + 1; j < fast_q18.size(); ++j) {
      EXPECT_FALSE(fast_q18[i][0] == fast_q18[j][0] &&
                   fast_q18[i][1] == fast_q18[j][1]);
    }
    bool found_exact = false;
    for (const Row& f : full_q18) {
      if (f == fast_q18[i]) {
        found_exact = true;
        break;
      }
    }
    EXPECT_TRUE(found_exact) << "CB-2 fiber group count mismatch at row " << i;
  }

  // CB-3: Two-Stage Top-N Late Aggregate Materialization WITH a winning `NULL`
  // group (`two_stage_late_agg_sql_in_null_group_bug` regression guard!).
  // Several phrases have the same count. Rank those ties by the group key
  // so the fast and reference queries select the same four groups.
  const std::string q23 =
      "SELECT phrase, MIN(url), MIN(title), COUNT(*) AS c, "
      "COUNT(DISTINCT uid) FROM hits_cb "
      "WHERE title LIKE '%Google%' "
      "GROUP BY phrase ORDER BY c DESC, phrase ASC LIMIT 4";
  const auto fast_q23 = run_fast(q23);
  const auto ref_q23 = run_ref(q23);
  ASSERT_EQ(fast_q23.size(), 4U);
  EXPECT_TRUE(fast_q23[0][0].IsNull()) << "Top group should be NULL phrase";
  EXPECT_FALSE(fast_q23[0][1].IsNull()) << "NULL group MIN(url) must survive";
  EXPECT_EQ(fast_q23, ref_q23);

  const std::string q10 =
      "SELECT se_id, SUM(ae_id), COUNT(*) AS c, AVG(ip), COUNT(DISTINCT uid) "
      "FROM hits_cb GROUP BY se_id ORDER BY c DESC LIMIT 2";
  EXPECT_EQ(run_fast(q10), run_ref(q10));

  // CB-4: Dictionary / memoized `REGEXP_REPLACE` + late `MIN(referer)` (Q29).
  const std::string q29 =
      "SELECT REGEXP_REPLACE(referer, '^https?://(?:www\\\\.)?([^/]+)/.*$', "
      "'\\\\1') AS k, AVG(BYTE_LENGTH(referer)) AS l, COUNT(*) AS c, "
      "MIN(referer) FROM hits_cb WHERE referer IS NOT NULL AND referer <> '' "
      "GROUP BY k HAVING COUNT(*) > 10 ORDER BY l DESC, k ASC LIMIT 5";
  EXPECT_EQ(run_fast(q29), run_ref(q29));

  // CB-5: Conditional `CASE WHEN` key partition + `ELSE ''` on NULL `referer`
  // (Q40).
  const std::string q40 =
      "SELECT counter_id, se_id, ae_id, "
      "CASE WHEN (se_id = 0 AND ae_id = 0) THEN referer ELSE '' END AS src, "
      "url, COUNT(*) AS pv FROM hits_cb WHERE counter_id = 62 "
      "GROUP BY counter_id, se_id, ae_id, src, url "
      "ORDER BY pv DESC, src ASC, url ASC LIMIT 8";
  EXPECT_EQ(run_fast(q40), run_ref(q40));

  // CB-6: Monotone `TIMESTAMP_TRUNC` early cutoff (Q43) vs periodic `EXTRACT`
  // (Q19 `periodic_extract_breaks_monotone_runs_bug`).
  // First warm VarcharBlockBound cache on `hits_cb`, then reset decode counter
  // and measure `run_fast(q43)` BEFORE `run_ref(q43)`!
  const std::string q43 =
      "SELECT TIMESTAMP_TRUNC(event_time, MINUTE) AS m, COUNT(*) AS pv "
      "FROM hits_cb GROUP BY m ORDER BY m ASC LIMIT 5 OFFSET 2";
  (void)run_fast(q43);
  TlcsFile::ResetDecodeCallsForTest();
  const auto fast_q43 = run_fast(q43);
  const size_t q43_decode_calls = TlcsFile::DecodeCallsForTest();
  EXPECT_LE(q43_decode_calls, 2U)
      << "CB-6 monotone cutoff should visit only the leading block(s)";
  EXPECT_EQ(fast_q43, run_ref(q43));

  const std::string q19_extract =
      "SELECT EXTRACT(MINUTE FROM TIMESTAMP(event_time)) AS m, COUNT(*) AS pv "
      "FROM hits_cb GROUP BY m ORDER BY m ASC LIMIT 5";
  EXPECT_EQ(run_fast(q19_extract), run_ref(q19_extract));

  db->DeleteAll();

  // CB-1 Trap test (`group_by_overflow_expr_drop_bug`): `ip - 3` on a non-top
  // `INT64_MIN` group MUST still raise an overflow error (`kInvalidArgument`)
  // even under `ORDER BY c DESC LIMIT 1`!
  const std::string ovf_prefix = "tlcs_cb1_ovf_" + RandomString();
  std::unique_ptr<Database> ovf_db = Database::Create(ovf_prefix).MoveValue();
  Schema ovf_schema("t_ovf", {Column("ip", ValueType::kInt64)});
  {
    TransactionContext ctx = ovf_db->BeginContext();
    ASSERT_EQ(ovf_db->CreateTable(ctx, ovf_schema).GetStatus(),
              Status::kSuccess);
    Table tbl = ovf_db->GetTable(ctx, "t_ovf").MoveValue();
    for (int i = 0; i < 100; ++i) {
      ASSERT_TRUE(tbl.Insert(ctx.txn_, Row({Value(int64_t{500})})).HasValue());
    }
    ASSERT_TRUE(
        tbl.Insert(ctx.txn_, Row({Value(std::numeric_limits<int64_t>::min())}))
            .HasValue());
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    TransactionContext ctx = ovf_db->BeginContext();
    ASSERT_EQ(ovf_db->CompactTableToTlcs(ctx, "t_ovf"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    TransactionContext ctx = ovf_db->BeginReadOnlyContext();
    SqlEngine eng(*ovf_db);
    StatusOr<Executor> prep =
        eng.Prepare(ctx,
                    "SELECT ip, ip - 3, COUNT(*) AS c FROM t_ovf "
                    "GROUP BY ip, ip - 3 ORDER BY c DESC LIMIT 1");
    ASSERT_TRUE(prep.HasValue());
    Row row;
    while (prep.Value()->Next(&row, nullptr)) {
    }
    EXPECT_EQ(prep.Value()->GetStatus(), Status::kInvalidArgument);
  }
  ovf_db->DeleteAll();
}

TEST(ColumnarTlcsFastPathTest, BlockMonotonicRunAggAndDoubleArithExpressions) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "execute_query unavailable";
  }
  BudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(size_t{512} << 20, size_t{128} << 20, size_t{2} << 20,
                      /*check_rss_pressure=*/false);

  const std::string prefix = "tlcs_tpch_mono_" + RandomString();
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  Schema schema(
      "lineitem_mono",
      {Column("orderkey", ValueType::kInt64),
       Column("suppkey", ValueType::kInt64),
       Column("shipdate", ValueType::kInt64), Column("qty", ValueType::kDouble),
       Column("price", ValueType::kDouble), Column("disc", ValueType::kDouble),
       Column("tax", ValueType::kDouble)});
  constexpr int64_t kRows =
      5 * 16384;  // NOLINT(bugprone-implicit-widening-of-multiplication-result)
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CreateTable(ctx, schema).GetStatus(), Status::kSuccess);
    Table tbl = db->GetTable(ctx, "lineitem_mono").MoveValue();
    // 5 TLCS blocks (5 * 16,384 = 81,920 rows), exercising:
    //  - Intra-block out-of-order keys (still satisfying block_max[m-1] <=
    //    block_min[m]),
    //  - Equal keys straddling morsel boundaries (16,384 and 32,768),
    //  - NULL keys scattered across blocks.
    for (int64_t r = 0; r < kRows; ++r) {
      const size_t blk = static_cast<size_t>(r) / 16384;
      const size_t off = static_cast<size_t>(r) % 16384;
      Value okey;
      if (off == 100 || off == 200) {
        okey = Value();                       // NULL key across all 5 blocks!
      } else if (blk == 0 && off >= 16378) {  // NOLINT(bugprone-branch-clone)
        okey = Value(int64_t{10000});         // straddles block 0 -> block 1!
      } else if (blk == 1 && off < 6) {
        okey = Value(int64_t{10000});
      } else if (blk == 1 && off >= 16378) {  // NOLINT(bugprone-branch-clone)
        okey = Value(int64_t{20000});         // straddles block 1 -> block 2!
      } else if (blk == 2 && off < 6) {
        okey = Value(int64_t{20000});
      } else {
        const int64_t base =
            (static_cast<int64_t>(
                blk) *   // NOLINT(readability-math-missing-parentheses)
                10000) +  // NOLINT(readability-math-missing-parentheses)
            static_cast<int64_t>((off / 4) * 2);
        const int64_t jitter = (off % 2 == 0) ? 1 : 0;
        okey = Value(std::min<int64_t>(base + jitter,
                                       static_cast<int64_t>(blk + 1) * 10000));
      }
      const int64_t skey = (r % 37) + 1;
      const int64_t sdate = 8990 + (r % 150);
      const double qty =
          (okey.IsNull() ? 90.0 : 50.0) + static_cast<double>((r * 7) % 45);
      const double price =
          100.0 +
          (static_cast<double>(
              r % 250) *  // NOLINT(readability-math-missing-parentheses)
              1.25);       // NOLINT(readability-math-missing-parentheses)
      const double disc = static_cast<double>(r % 11) * 0.01;
      const double tax = static_cast<double>(r % 9) * 0.01;
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_, Row({okey, Value(skey), Value(sdate), Value(qty),
                                    Value(price), Value(disc), Value(tax)}))
              .HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "lineitem_mono"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  auto run_fast = [&](const std::string& sql) {
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine eng(*db);
    return RunSql(&eng, &ctx, sql);
  };
  auto run_ref = [&](const std::string& sql) {
    const ScopedEnvVar guard("TINYLAMB_DISABLE_COLUMNAR_TLCS_AGG", "1");
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine eng(*db);
    eng.SetForceRelationalFallback(true);
    return RunSql(&eng, &ctx, sql);
  };

  // 1. Block-monotonic streaming run aggregation with HAVING + ORDER BY + LIMIT
  //    (verifies boundary-straddling keys 10000 and 20000 and NULL group!).
  const std::string q_having =
      "SELECT orderkey, ROUND(SUM(qty), 2) AS s, COUNT(*) AS c "
      "FROM lineitem_mono GROUP BY orderkey HAVING SUM(qty) > 300.0 "
      "ORDER BY s DESC, orderkey ASC";
  const auto fast_having = run_fast(q_having);
  const auto ref_having = run_ref(q_having);
  EXPECT_FALSE(fast_having.empty());
  EXPECT_EQ(fast_having, ref_having);

  // 2. Block-monotonic streaming run aggregation with bounded Top-K.
  const std::string q_topk =
      "SELECT orderkey, ROUND(SUM(qty), 2) AS s "
      "FROM lineitem_mono WHERE shipdate >= 9000 "
      "GROUP BY orderkey ORDER BY s DESC, orderkey ASC LIMIT 10";
  EXPECT_EQ(run_fast(q_topk), run_ref(q_topk));

  // 3. Grouped RPN ColDoubleArithExpr (`SUM(price * (1 - disc))`, `AVG(...)`).
  const std::string q_arith_grp =
      "SELECT suppkey, ROUND(SUM(price * (1.0 - disc)), 4) AS rev, "
      "ROUND(AVG(price * (1.0 - disc) * (1.0 + tax)), 4) AS avg_chg "
      "FROM lineitem_mono WHERE shipdate >= 9000 AND shipdate < 9100 "
      "GROUP BY suppkey ORDER BY rev DESC, suppkey ASC LIMIT 8";
  EXPECT_EQ(run_fast(q_arith_grp), run_ref(q_arith_grp));

  // 4. Scalar RPN ColDoubleArithExpr (`SUM(price * disc)`).
  const std::string q_arith_scalar =
      "SELECT ROUND(SUM(price * disc), 4) FROM lineitem_mono "
      "WHERE shipdate >= 9000 AND shipdate < 9100 AND qty < 80.0";
  EXPECT_EQ(run_fast(q_arith_scalar), run_ref(q_arith_scalar));

  db->DeleteAll();
}

TEST(ColumnarTlcsFastPathTest,
     ClickBenchRound2HeavyHittersMarkovConstAndNonEmptyTopK) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "execute_query unavailable";
  }
  BudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(size_t{512} << 20, size_t{128} << 20, size_t{2} << 20,
                      /*check_rss_pressure=*/false);

  // --------------------------------------------------------------------------
  // Part A: PAX `LSTableStore` CB-11 wide/multi-column dictionary marginal fold
  //   (`dict_exact_freq_filtered_count_sound`,
  //    `filtered_extremum_bound_unsound_bug`)
  // --------------------------------------------------------------------------
  const std::string pax_prefix = "tlcs_cbr2_pax_" + RandomString();
  std::unique_ptr<Database> pax_db = Database::Create(pax_prefix).MoveValue();
  Schema pax_schema(
      "hits_pax",
      {Column("adv", ValueType::kInt64), Column("res", ValueType::kInt64),
       Column("eday", ValueType::kDate), Column("url", ValueType::kVarChar)});
  {
    TransactionContext ctx = pax_db->BeginContext();
    ASSERT_EQ(pax_db->CreateTable(ctx, pax_schema).GetStatus(),
              Status::kSuccess);
    Table tbl = pax_db->GetTable(ctx, "hits_pax").MoveValue();
    // > 300 distinct `res` values so `wide_code_bytes = 2` dictionary is built,
    // plus NULLs to test exact NULL-skipping marginal `code_counts` folding!
    for (int64_t i = 0; i < 2000; ++i) {
      const int64_t adv = (i % 5 == 0) ? 0 : (1 + (i % 7));
      Value res_v = (i % 20 == 0) ? Value() : Value(1000 + (i % 400));
      Value eday_v = Value::DateFromDays(15900 + (i % 30));
      Value url_v = Value("https://site.test/p" + std::to_string(i % 50));
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_, Row({Value(adv), std::move(res_v),
                                    std::move(eday_v), std::move(url_v)}))
              .HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    // Give columnar pool headroom so PAX `LSTableStore` builds with wide dicts.
    budget.ResetForTest(size_t{512} << 20, size_t{128} << 20, size_t{64} << 20,
                        /*check_rss_pressure=*/false);
    auto run_pax = [&](bool fallback, const std::string& sql) {
      TransactionContext ctx = pax_db->BeginReadOnlyContext();
      SqlEngine eng(*pax_db);
      eng.SetForceRelationalFallback(fallback);
      return RunSql(&eng, &ctx, sql);
    };
    // Q02: Filtered wide-dict `COUNT(*)` (`adv <> 0`).
    const std::string q02 = "SELECT COUNT(*) FROM hits_pax WHERE adv <> 0";
    EXPECT_EQ(run_pax(false, q02), run_pax(true, q02));
    // Q03: Unfiltered multi-column marginal scalar `SUM(adv), COUNT(*),
    // AVG(res)`.
    const std::string q03 =
        "SELECT SUM(adv), COUNT(*), ROUND(AVG(res), 4) FROM hits_pax";
    EXPECT_EQ(run_pax(false, q03), run_pax(true, q03));
    // Q07: Unfiltered `MIN(eday), MAX(eday)`.
    const std::string q07 = "SELECT MIN(eday), MAX(eday) FROM hits_pax";
    EXPECT_EQ(run_pax(false, q07), run_pax(true, q07));
    // Trap `filtered_extremum_bound_unsound_bug`: filtered `MIN(res), MAX(res)`
    // where `adv = 999` matches 0 rows MUST return `(NULL, NULL)`, and where
    // `adv = 0` restricts rows MUST NOT return unfiltered dictionary min/max!
    const std::string trap_filt_ext_empty =
        "SELECT MIN(res), MAX(res) FROM hits_pax WHERE adv = 999";
    const auto fe_empty = run_pax(false, trap_filt_ext_empty);
    ASSERT_EQ(fe_empty.size(), 1U);
    EXPECT_TRUE(fe_empty[0][0].IsNull());
    EXPECT_TRUE(fe_empty[0][1].IsNull());
    const std::string trap_filt_ext_sub =
        "SELECT MIN(res), MAX(res) FROM hits_pax WHERE adv = 3";
    EXPECT_EQ(run_pax(false, trap_filt_ext_sub),
              run_pax(true, trap_filt_ext_sub));
  }
  pax_db->DeleteAll();

  // --------------------------------------------------------------------------
  // Part B: Paged `.tlcs` CB-7, CB-8, CB-10, CB-12 + Semantic Traps
  // --------------------------------------------------------------------------
  budget.ResetForTest(size_t{512} << 20, size_t{128} << 20, size_t{2} << 20,
                      /*check_rss_pressure=*/false);
  const std::string prefix = "tlcs_cbr2_" + RandomString();
  Schema schema("hits_r2", {Column("const_v", ValueType::kInt64),
                            Column("val", ValueType::kInt64),
                            Column("signed_v", ValueType::kInt64),
                            Column("phrase", ValueType::kVarChar),
                            Column("url", ValueType::kVarChar),
                            Column("title", ValueType::kVarChar)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  page_id_t first_pid = 0;
  // 3 blocks (49,152 rows):
  // - Block 0: `phrase` is ALL `""` (empty string) except 2 rows `'zzz_tail'`;
  //   `const_v` = 42 in every row (`kConstInt` block!).
  // - Block 1: `phrase` mixes `""` and `'mmm_mid_' || ...`; `const_v` = 42.
  // - Block 2: `phrase` mixes `""` and `'aaa_best_01'..'aaa_best_05'`!
  //   Without CB-12 (`nonempty_min_prefix`), all 3 blocks tie at
  //   `min_prefix=""`! With CB-12 under `WHERE phrase <> '' ORDER BY phrase
  //   LIMIT 3`, Block 0 is ordered after Block 2 (`'aaa_best_01' < 'mmm_mid' <
  //   'zzz_tail'`) and pruned once Top-3 is filled from Block 2!
  constexpr int64_t kRows =
      (2 * 32768) + 1024;  // 3 blocks: [0..32767], [32768..65535], [65536..]
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CreateTable(ctx, schema).GetStatus(), Status::kSuccess);
    Table tbl = db->GetTable(ctx, "hits_r2").MoveValue();
    first_pid = tbl.FirstPageId();
    for (int64_t i = 0; i < kRows; ++i) {
      const int64_t blk = i / 32768;
      const int64_t off = i % 32768;
      const int64_t const_v = 42;  // uniform constant across blocks!
      const int64_t val = (i % 100) + 1;
      // Trap `having_sum_signed_unbounded_groups_bug`: negative values make
      // total table SUM negative while many groups still have positive SUM!
      const int64_t signed_v = (i % 2 == 0) ? 50 : -100;

      std::string phrase;
      if (blk == 0) {
        phrase = (off >= 32766) ? "zzz_tail" : "";
      } else if (blk == 1) {
        phrase = (off % 2 == 0) ? "" : ("mmm_mid_" + std::to_string(off % 20));
      } else {
        if (off < 5) {
          phrase = "aaa_best_0" + std::to_string(off + 1);
        } else {
          phrase =
              (off % 2 == 0) ? "" : ("nnn_other_" + std::to_string(off % 30));
        }
      }

      // High-cardinality `url`: 5 heavy hitters (`hh_0..hh_4`) + 3,500 rare
      // URLs so `TryRunQuotientBucketHeavyHittersTopK` has > 2,048 non-empty
      // buckets (`rest_max_bucket > 0` and `tau > rest_max_bucket`).
      std::string url;
      if (i % 4 == 0) {
        url = "https://heavy.example/hh_" + std::to_string(i % 5);
      } else {
        url = "https://rare.example/u_" + std::to_string(i % 3500);
      }
      std::string title = "PageTitle_" + std::to_string(i % 200);
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_,
                     Row({Value(const_v), Value(val), Value(signed_v),
                          Value(std::move(phrase)), Value(std::move(url)),
                          Value(std::move(title))}))
              .HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "hits_r2"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  auto run_fast = [&](const std::string& sql) {
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine eng(*db);
    return RunSql(&eng, &ctx, sql);
  };
  auto run_ref = [&](const std::string& sql) {
    const ScopedEnvVar guard("TINYLAMB_DISABLE_COLUMNAR_TLCS_AGG", "1");
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine eng(*db);
    eng.SetForceRelationalFallback(true);
    return RunSql(&eng, &ctx, sql);
  };

  // 1. CB-10 (`bounded_int64_sum_parallel_sound`,
  // `const_block_sum_fold_sound`):
  //    Unfiltered scalar `SUM(const_v), SUM(val), COUNT(*)` folds `kConstInt`
  //    blocks in O(1) metadata and runs bounded `SUM(val)` across workers!
  const std::string q_scalar_const =
      "SELECT SUM(const_v), SUM(val), COUNT(*) FROM hits_r2";
  const auto fast_const = run_fast(q_scalar_const);
  ASSERT_EQ(fast_const.size(), 1U);
  EXPECT_EQ(fast_const[0][0], Value(kRows * 42));
  EXPECT_EQ(fast_const, run_ref(q_scalar_const));

  // 2. CB-8 (`having_count_markov_bound_sound`,
  // `having_two_stage_late_agg_sound`):
  //    - Q29 shape: `HAVING COUNT(*) > 500` WITHOUT `LIMIT`, with deferred
  //      `MIN(title)` and `AVG(BYTE_LENGTH(url))` in `ORDER BY`!
  const std::string q29_nolimit =
      "SELECT url, AVG(BYTE_LENGTH(url)) AS l, COUNT(*) AS c, MIN(title) "
      "FROM hits_r2 GROUP BY url HAVING COUNT(*) > 500 ORDER BY l DESC, url "
      "ASC";
  const auto fast_q29 = run_fast(q29_nolimit);
  EXPECT_EQ(fast_q29.size(), 5U);
  EXPECT_EQ(fast_q29, run_ref(q29_nolimit));

  //    - Q28 shape: `HAVING COUNT(*) > 500 ORDER BY l DESC LIMIT 3` (`ORDER BY`
  //      references deferred `AVG(BYTE_LENGTH(title))`).
  const std::string q28_shape =
      "SELECT url, AVG(BYTE_LENGTH(title)) AS l, COUNT(*) AS c, MIN(phrase) "
      "FROM hits_r2 GROUP BY url HAVING COUNT(*) > 500 "
      "ORDER BY l DESC, url ASC LIMIT 3";
  EXPECT_EQ(run_fast(q28_shape), run_ref(q28_shape));

  //    - Trap `having_sum_signed_unbounded_groups_bug`: `HAVING SUM(signed_v) >
  //    10`
  //      on signed values MUST NOT apply Markov cardinality pruning!
  const std::string trap_signed_having =
      "SELECT url, SUM(signed_v) AS s, MIN(title) FROM hits_r2 "
      "GROUP BY url HAVING SUM(signed_v) > 10 ORDER BY s DESC, url ASC LIMIT 5";
  EXPECT_EQ(run_fast(trap_signed_having), run_ref(trap_signed_having));

  // 3. CB-12 (`bottom_exclusion_tightens_min_bound_sound`):
  //    Warm `VarcharBlockBound` cache, then verify `WHERE phrase <> '' ORDER BY
  //    phrase ASC LIMIT 3` visits Block 2 first (`aaa_best_01..03`) and prunes
  //    Block 0 (`zzz_tail`) and Block 1 (`mmm_mid_*`) without decoding them!
  const std::string q26_shape =
      "SELECT phrase FROM hits_r2 WHERE phrase <> '' "
      "ORDER BY phrase ASC NULLS LAST LIMIT 3";
  (void)run_fast(q26_shape);
  std::atomic<size_t> one_outstanding{1};
  uint64_t q26_decodes = 0;
  std::vector<Row> fast_q26;
  {
    ScopedQueryParallelismBudget single_worker(&one_outstanding,
                                               /*cpu_slots=*/1);
    TlcsFile::ResetDecodeCallsForTest();
    fast_q26 = run_fast(q26_shape);
    q26_decodes = TlcsFile::DecodeCallsForTest();
  }
  ASSERT_EQ(fast_q26.size(), 3U);
  EXPECT_EQ(fast_q26[0][0].AsStringView(), "aaa_best_01");
  EXPECT_EQ(fast_q26[1][0].AsStringView(), "aaa_best_02");
  EXPECT_EQ(fast_q26[2][0].AsStringView(), "aaa_best_03");
  // Exactly 2 decodes on Block 2 (1 Stage-1b scan + 1 late-materialization
  // fetch), with 0 decodes on pruned Block 0 and Block 1 (vs 4 without CB-12).
  EXPECT_EQ(q26_decodes, 2U)
      << "CB-12 nonempty_min_prefix should prune blocks 0 and 1!";

  //    - Trap `interior_exclusion_min_bound_unsound_bug`: excluding a
  //    non-bottom
  //      string (`WHERE phrase <> 'mmm_mid_0'`) MUST still include `""` rows!
  const std::string trap_interior =
      "SELECT phrase FROM hits_r2 WHERE phrase <> 'mmm_mid_0' "
      "ORDER BY phrase ASC NULLS LAST LIMIT 3";
  const auto fast_interior = run_fast(trap_interior);
  ASSERT_EQ(fast_interior.size(), 3U);
  EXPECT_EQ(fast_interior[0][0].AsStringView(), "");

  // 4. CB-7 (`quotient_bucket_topk_pruning_sound` + Trap
  //    `quotient_bucket_asc_order_pruning_bug`):
  //    Invoke `TryRunPartitionedColumnarTlcsAggregate` with 129 candidate block
  //    entries (43x `{0, 1, 2}`) to exercise the `>= 128`-block quotient
  //    heavy-hitter sketch (`> 2,048` non-empty buckets, `tau >
  //    rest_max_bucket`) and verify `ORDER BY COUNT(*) ASC` declines
  //    heavy-hitter pruning!
  const std::string tlcs_path =
      prefix + ".db.tlcs." + std::to_string(first_pid);
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TlcsFile>, tlcs,
                        TlcsFile::Open(tlcs_path, schema));
  ASSERT_GE(tlcs->BlockCount(), 3U);
  std::vector<uint32_t> blocks_129;
  blocks_129.reserve(129);
  for (size_t rep = 0; rep < 43; ++rep) {
    blocks_129.push_back(0);
    blocks_129.push_back(1);
    blocks_129.push_back(2);
  }
  const Expression count_star = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("*"), false);
  const std::vector<ColGroupKeySpec> keys = {
      MakeKeySpec(ColKeyKind::kColVarChar, 4, "url")};
  const std::vector<ColAggSpec> aggs = {MakeAggSpec(
      &count_star->AsAggregateExpression(), ColAggKind::kCountStar, 0)};
  const std::vector<OutputSlotBinding> emits = {
      MakeSlotBinding(OutputSlotBinding::Kind::kKey, 0),
      MakeSlotBinding(OutputSlotBinding::Kind::kAgg, 0)};

  // 4a. `ORDER BY c DESC, url ASC LIMIT 5` -> exact top-5 heavy hitters
  // (`hh_0..4`).
  {
    SelectStatement stmt;
    stmt.SetSelectList({NamedExpression("url", ColumnValueExp("url")),
                        NamedExpression("c", count_star)});
    stmt.SetLimit(5);
    const std::vector<ColumnarOrderBinding> orders_desc = {
        {MakeSlotBinding(OutputSlotBinding::Kind::kAgg, 0),
         SortKeyOrder(/*ascending=*/false, /*nulls_first=*/false,
                      Collation::kNone)},
        {MakeSlotBinding(OutputSlotBinding::Kind::kKey, 0),
         SortKeyOrder(/*ascending=*/true, /*nulls_first=*/false,
                      Collation::kNone)}};
    PartitionedColumnarAggRequest req{
        .stmt = stmt,
        .tlcs = tlcs,
        .qualified_schema = schema,
        .table_schema = schema,
        .preds = ColumnarPredicateSet{},
        .candidate_blocks = blocks_129,
        .key_specs = keys,
        .agg_specs = aggs,
        .positional_having = {},
        .emit_bindings = emits,
        .order_bindings = orders_desc,
        .runtime = nullptr,
        .publish_lstable_marker = false,
        .fold_cache_key = "",
        .scan_begin = std::chrono::steady_clock::now(),
        .dir_ms = 0.0,
        .any_scalar_distinct = false};
    ScopedQueryParallelismBudget single_worker(&one_outstanding, 1);
    TlcsFile::ResetDecodeCallsForTest();
    ASSIGN_OR_ASSERT_FAIL(std::optional<Relation>, res_desc,
                          TryRunPartitionedColumnarTlcsAggregate(req));
    if (!res_desc.has_value()) {
      ADD_FAILURE() << "expected optional value";
      return;
    }
    EXPECT_EQ(TlcsFile::DecodeCallsForTest(), 8U + blocks_129.size());
    ASSERT_EQ(res_desc->rows.size(),
              5U);  // NOLINT(bugprone-unchecked-optional-access)
    for (size_t r = 0; r < 5; ++r) {
      EXPECT_TRUE(
          res_desc->rows[r][0]
              .AsStringView()
              .starts_with(  // NOLINT(bugprone-unchecked-optional-access)
                  "https://heavy.example/hh_"))
          << "Unexpected winner: "
          << res_desc->rows[r][0]
                 .AsStringView();  // NOLINT(bugprone-unchecked-optional-access)
      EXPECT_GT(res_desc->rows[r][1].AsInt64(),
                43 * 2000);  // NOLINT(bugprone-unchecked-optional-access)
    }

    // Read two projected streams on the multi-key replay path. The second
    // key is constant across all rows, so the groups and counts stay exact.
    const std::vector<ColGroupKeySpec> multi_key = {
        keys[0], MakeKeySpec(ColKeyKind::kColInt64, 0, "const_v")};
    PartitionedColumnarAggRequest compound{
        .stmt = stmt,
        .tlcs = tlcs,
        .qualified_schema = schema,
        .table_schema = schema,
        .preds = ColumnarPredicateSet{},
        .candidate_blocks = blocks_129,
        .key_specs = multi_key,
        .agg_specs = aggs,
        .positional_having = {},
        .emit_bindings = emits,
        .order_bindings = orders_desc,
        .fold_cache_key = "",
        .scan_begin = std::chrono::steady_clock::now()};
    ASSIGN_OR_ASSERT_FAIL(std::optional<Relation>, res_compound,
                          TryRunPartitionedColumnarTlcsAggregate(compound));
    if (!res_compound || !res_desc) {
      ADD_FAILURE() << "expected both aggregate results";
      return;
    }
    EXPECT_EQ(res_compound->rows, res_desc->rows);
  }

  // 4b. Trap `quotient_bucket_asc_order_pruning_bug`: `ORDER BY c ASC, url ASC
  //     LIMIT 5` MUST return the rare `u_*` groups, never heavy hitters!
  {
    SelectStatement stmt;
    stmt.SetSelectList({NamedExpression("url", ColumnValueExp("url")),
                        NamedExpression("c", count_star)});
    stmt.SetLimit(5);
    const std::vector<ColumnarOrderBinding> orders_asc = {
        {MakeSlotBinding(OutputSlotBinding::Kind::kAgg, 0),
         SortKeyOrder(/*ascending=*/true, /*nulls_first=*/true,
                      Collation::kNone)},
        {MakeSlotBinding(OutputSlotBinding::Kind::kKey, 0),
         SortKeyOrder(/*ascending=*/true, /*nulls_first=*/true,
                      Collation::kNone)}};
    PartitionedColumnarAggRequest req{
        .stmt = stmt,
        .tlcs = tlcs,
        .qualified_schema = schema,
        .table_schema = schema,
        .preds = ColumnarPredicateSet{},
        .candidate_blocks = blocks_129,
        .key_specs = keys,
        .agg_specs = aggs,
        .positional_having = {},
        .emit_bindings = emits,
        .order_bindings = orders_asc,
        .runtime = nullptr,
        .publish_lstable_marker = false,
        .fold_cache_key = "",
        .scan_begin = std::chrono::steady_clock::now(),
        .dir_ms = 0.0,
        .any_scalar_distinct = false};
    ASSIGN_OR_ASSERT_FAIL(std::optional<Relation>, res_asc,
                          TryRunPartitionedColumnarTlcsAggregate(req));
    if (!res_asc.has_value()) {
      ADD_FAILURE() << "expected optional value";
      return;
    }
    ASSERT_EQ(res_asc->rows.size(),
              5U);  // NOLINT(bugprone-unchecked-optional-access)
    for (size_t r = 0; r < 5; ++r) {
      EXPECT_TRUE(
          res_asc->rows[r][0]
              .AsStringView()
              .starts_with(  // NOLINT(bugprone-unchecked-optional-access)
                  "https://rare.example/u_"))
          << "ASC order pruned rare group: "
          << res_asc->rows[r][0]
                 .AsStringView();  // NOLINT(bugprone-unchecked-optional-access)
      EXPECT_LT(res_asc->rows[r][1].AsInt64(),
                43 * 50);  // NOLINT(bugprone-unchecked-optional-access)
    }
  }

  // 4c. Make the sample see only "" and "zzz_tail". Its second group
  // cannot beat the unseen mmm_mid_* groups. Replaying the original sketch
  // must produce the exact count and use the secondary order at the tie.
  {
    SelectStatement stmt;
    stmt.SetSelectList({NamedExpression("phrase", ColumnValueExp("phrase")),
                        NamedExpression("c", count_star)});
    stmt.SetLimit(1);
    stmt.SetOffset(1);
    const std::vector<ColumnarOrderBinding> order = {
        {MakeSlotBinding(OutputSlotBinding::Kind::kAgg, 0),
         SortKeyOrder(false, false, Collation::kNone)},
        {MakeSlotBinding(OutputSlotBinding::Kind::kKey, 0),
         SortKeyOrder(true, false, Collation::kNone)}};
    const std::vector<ColGroupKeySpec> phrase_key = {
        MakeKeySpec(ColKeyKind::kColVarChar, 3, "phrase")};
    std::vector<uint32_t> blocks(128, 1);
    for (size_t i = 0; i < blocks.size(); i += 16) {
      blocks[i] = 0;
    }
    PartitionedColumnarAggRequest req{
        .stmt = stmt,
        .tlcs = tlcs,
        .qualified_schema = schema,
        .table_schema = schema,
        .preds = ColumnarPredicateSet{},
        .candidate_blocks = blocks,
        .key_specs = phrase_key,
        .agg_specs = aggs,
        .positional_having = {},
        .emit_bindings = emits,
        .order_bindings = order,
        .fold_cache_key = "",
        .scan_begin = std::chrono::steady_clock::now()};
    ScopedQueryParallelismBudget single_worker(&one_outstanding, 1);
    TlcsFile::ResetDecodeCallsForTest();
    ASSIGN_OR_ASSERT_FAIL(std::optional<Relation>, result,
                          TryRunPartitionedColumnarTlcsAggregate(req));
    if (!result) {
      ADD_FAILURE() << "expected aggregate result";
      return;
    }
    // The partitioned stage hands the first OFFSET + LIMIT rows to its caller.
    ASSERT_EQ(result->rows.size(), 2U);
    EXPECT_EQ(result->rows[1][0].AsStringView(), "mmm_mid_1");
    EXPECT_EQ(result->rows[1][1].AsInt64(), 120 * 1639);
    EXPECT_EQ(TlcsFile::DecodeCallsForTest(), 8U + (2 * blocks.size()));
  }

  tlcs.reset();
  db->DeleteAll();
}

// Regression for two TPC-H-on-TLCS bugs found while moving TPC-H onto
// `--load-compact` fixtures (olap_strategy P0):
//  * Q21: the pair-sharded partitioned COUNT(DISTINCT) stored its final count
//    in `RuntimeAggState::count`, but FinishAggState read `distinct_ints`, so
//    every `HAVING COUNT(DISTINCT ...)` group compared against 0.
//  * Q13: persistent (mmap-resident) `.tlcs` stores expose columns only through
//    zero-copy views; scan and aggregate code that read the owned vectors
//    (`varchar_bytes`, `varchar_ends`, `int_cells`, `nulls`) saw empty spans
//    and crashed or mis-sized buffers.
// The same SQL must answer identically on the row store and after compaction.
TEST(ColumnarTlcsFastPathTest, TpchShapesMatchRowStoreAfterCompaction) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "execute_query unavailable";
  }
  BudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  // Roomy columnar pool: the store stays resident (persistent mmap views).
  budget.ResetForTest(size_t{512} << 20, size_t{128} << 20, size_t{256} << 20,
                      /*check_rss_pressure=*/false);

  const std::string prefix = "tlcs_tpch_shapes_" + RandomString();
  Schema schema(
      "li",
      {Column("k", ValueType::kInt64), Column("s", ValueType::kInt64),
       Column("late", ValueType::kInt64), Column("c", ValueType::kVarChar)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  constexpr int64_t kLinesPerOrder = 4;
  constexpr int64_t kOrders = 12000;
  constexpr int64_t kRows = kOrders * kLinesPerOrder;  // several TLCS blocks
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CreateTable(ctx, schema).GetStatus(), Status::kSuccess);
    Table tbl = db->GetTable(ctx, "li").MoveValue();
    for (int64_t i = 0; i < kRows; ++i) {
      const int64_t k = i / kLinesPerOrder;
      // Orders with k % 3 == 0 use a single supplier; the rest use a distinct
      // supplier per line.
      const int64_t s = (k % 3 == 0) ? (k % 7) : (k * 10) + (i % 4);
      // Late lines only on the first line of even orders.
      const int64_t late = (k % 2 == 0 && i % kLinesPerOrder == 0) ? 1 : 0;
      std::string c = (i % 5 == 0) ? "foo unusual bar packages baz"
                                   : "plain comment " + std::to_string(i);
      ASSERT_TRUE(tbl.Insert(ctx.txn_, Row({Value(k), Value(s), Value(late),
                                            Value(std::move(c))}))
                      .HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  struct Case {
    std::string sql;
    int64_t expected;
  };
  constexpr int64_t kSingleSupplierOrders = (kOrders + 2) / 3;
  constexpr int64_t kUnusualRows = (kRows + 4) / 5;
  const std::vector<Case> cases = {
      // Q21 `multi` shape.
      {"SELECT COUNT(*) FROM (SELECT k FROM li GROUP BY k "
       "HAVING COUNT(DISTINCT s) > 1) m",
       kOrders - kSingleSupplierOrders},
      // Q21 `only_late` shape (filtered input, `= 1`).
      {"SELECT COUNT(*) FROM (SELECT k FROM li WHERE late = 1 GROUP BY k "
       "HAVING COUNT(DISTINCT s) = 1) m",
       (kOrders + 1) / 2},
      // The distinct count itself must survive as an output column.
      {"SELECT SUM(d) FROM (SELECT k, COUNT(DISTINCT s) AS d FROM li "
       "GROUP BY k) m",
       kSingleSupplierOrders +
           ((kOrders - kSingleSupplierOrders) * kLinesPerOrder)},
      // Q13 shape: multi-wildcard NOT LIKE over a VARCHAR column.
      {"SELECT COUNT(*) FROM li WHERE c NOT LIKE '%unusual%packages%'",
       kRows - kUnusualRows},
      {"SELECT COUNT(*) FROM (SELECT k, COUNT(*) AS n FROM li "
       "WHERE c NOT LIKE '%unusual%packages%' GROUP BY k) m",
       kOrders},
      // VARCHAR grouping and byte lengths read straight from the views.
      {"SELECT COUNT(*) FROM (SELECT c FROM li GROUP BY c) m",
       (kRows - kUnusualRows) + 1},
  };

  auto run_all = [&]() {
    std::vector<std::vector<Row>> out;
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine eng(*db);
    out.reserve(cases.size());
    for (const Case& c : cases) {
      out.push_back(RunSql(&eng, &ctx, c.sql));
    }
    return out;
  };

  const std::vector<std::vector<Row>> row_store = run_all();
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "li"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  // Twice: the second run exercises cached TLCS stores and markers.
  for (int round = 0; round < 2; ++round) {
    const std::vector<std::vector<Row>> tlcs = run_all();
    for (size_t i = 0; i < cases.size(); ++i) {
      ASSERT_EQ(row_store[i].size(), 1U) << cases[i].sql;
      ASSERT_EQ(tlcs[i].size(), 1U)
          << "round " << round << ": " << cases[i].sql;
      EXPECT_EQ(row_store[i][0][0].AsInt64(), cases[i].expected)
          << "row store: " << cases[i].sql;
      EXPECT_EQ(tlcs[i][0][0].AsInt64(), cases[i].expected)
          << "tlcs round " << round << ": " << cases[i].sql;
    }
  }

  db->DeleteAll();
}

// The block-monotonic run aggregation now also folds COUNT(DISTINCT int)
// (TPC-H Q21's `GROUP BY l_orderkey HAVING COUNT(DISTINCT l_suppkey)`).
// Groups of 1..5 rows on a key sorted across >= 4 TLCS blocks put some groups
// across block boundaries, where per-block partial sets must merge; run-to-run
// reuse of the distinct set must not leak values between groups.
TEST(ColumnarTlcsFastPathTest, MonotonicRunCountDistinctMatchesRowStore) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "execute_query unavailable";
  }
  BudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  // Small columnar pool: paged TLCS mode, where the run aggregation lives.
  budget.ResetForTest(size_t{512} << 20, size_t{128} << 20, size_t{2} << 20,
                      /*check_rss_pressure=*/false);

  const std::string prefix = "tlcs_mono_distinct_" + RandomString();
  Schema schema("li",
                {Column("k", ValueType::kInt64), Column("s", ValueType::kInt64),
                 Column("late", ValueType::kInt64)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  constexpr int64_t kOrders = 60000;  // ~180k rows: >= 4 TLCS blocks
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CreateTable(ctx, schema).GetStatus(), Status::kSuccess);
    Table tbl = db->GetTable(ctx, "li").MoveValue();
    for (int64_t k = 0; k < kOrders; ++k) {
      const int64_t lines = 1 + (k % 5);
      for (int64_t line = 0; line < lines; ++line) {
        // k % 3 == 0: one supplier repeated; otherwise one per line, with a
        // repeat on the last line of every 4th order.
        int64_t s = (k % 3 == 0) ? (k % 7) : (k * 10) + line;
        if (k % 4 == 1 && line == lines - 1 && line > 0) {
          s = (k * 10);
        }
        const Value late = (line == 0 && k % 2 == 0) ? Value(int64_t{1})
                           : (k % 11 == 0)           ? Value()
                                                     : Value(int64_t{0});
        ASSERT_TRUE(
            tbl.Insert(ctx.txn_, Row({Value(k), Value(s), late})).HasValue());
      }
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  const std::vector<std::string> queries = {
      "SELECT COUNT(*) FROM (SELECT k FROM li GROUP BY k "
      "HAVING COUNT(DISTINCT s) > 1) m",
      "SELECT COUNT(*) FROM (SELECT k FROM li WHERE late = 1 GROUP BY k "
      "HAVING COUNT(DISTINCT s) = 1) m",
      "SELECT SUM(d), COUNT(*) FROM (SELECT k, COUNT(DISTINCT s) AS d FROM li "
      "GROUP BY k HAVING COUNT(*) >= 1) m",
      "SELECT SUM(d * k) FROM (SELECT k, COUNT(DISTINCT s) AS d FROM li "
      "WHERE late IS NOT NULL GROUP BY k HAVING COUNT(DISTINCT s) >= 2) m",
  };
  auto run_all = [&]() {
    std::vector<std::vector<std::string>> out;
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine eng(*db);
    for (const std::string& q : queries) {
      std::vector<std::string> rows;
      for (const Row& r : RunSql(&eng, &ctx, q)) {
        rows.push_back(r.ToString());
      }
      out.push_back(std::move(rows));
    }
    return out;
  };
  const auto row_store = run_all();
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "li"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  for (int round = 0; round < 2; ++round) {
    const auto tlcs = run_all();
    for (size_t i = 0; i < queries.size(); ++i) {
      ASSERT_EQ(row_store[i].size(), 1U) << queries[i];
      EXPECT_EQ(tlcs[i], row_store[i])
          << "round " << round << ": " << queries[i];
    }
  }
  // Brute-force expectation for the first query, mirroring the generator.
  int64_t multi = 0;
  for (int64_t k = 0; k < kOrders; ++k) {
    const int64_t lines = 1 + (k % 5);
    std::vector<int64_t> seen;
    for (int64_t line = 0; line < lines; ++line) {
      int64_t s = (k % 3 == 0) ? (k % 7) : (k * 10) + line;
      if (k % 4 == 1 && line == lines - 1 && line > 0) {
        s = (k * 10);
      }
      if (std::find(seen.begin(), seen.end(), s) == seen.end()) {
        seen.push_back(s);
      }
    }
    if (seen.size() > 1) {
      ++multi;
    }
  }
  EXPECT_EQ(row_store[0][0], Row({Value(multi)}).ToString());

  db->DeleteAll();
}

}  // namespace
}  // namespace tinylamb
