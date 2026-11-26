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
#include "common/query_parallelism.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/benchmark_rematch.hpp"
#include "executor/detail/columnar_tlcs_internal.hpp"
#include "executor/detail/engine_decisions.hpp"
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
#include "plan/optimizer_decisions.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "table/rematch_storage_codecs.hpp"
#include "table/tlcs_file.hpp"
#include "transaction/gc_wakeup.hpp"
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
  // ClickstreamLayout.Execution.ConcatSearch: dense hits (> 64) are now
  // resolved directly in a single concatenated-stream pass without bailout.
  EXPECT_TRUE(relational_detail::TryJitContainsRows(
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
      const std::array<RuntimeKeyCell, 2> key{
          {{.int_val = i}, {.int_val = i + 1}}};
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
    const std::array<RuntimeKeyCell, 2> first{{{.int_val = 0}, {.int_val = 1}}};
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
                 blk) *  // NOLINT(readability-math-missing-parentheses)
             10000) +    // NOLINT(readability-math-missing-parentheses)
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
           1.25);          // NOLINT(readability-math-missing-parentheses)
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

// Lean proofs:
//   lean/TinylambOpt/ResearchProofs/AnalyticalOperators/FrequencyBoundsAndOrderedFetch.lean
//     (FrontCodedContains_sound, OrderedCodeMin_sound, OwnerDistinct_sound)
//   lean/TinylambOpt/ResearchProofs/AnalyticalOperators/DictionarySlicesAndExtrema.lean
//     (sound_DominatedColumn)
TEST(ColumnarTlcsFastPathTest,
     FrontCodedDictContainsAndOrderedCodeMinMatchRowStore) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "execute_query unavailable";
  }
  const std::string prefix = "tlcs_hits_" + RandomString();
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  Schema schema("hits", {Column("SearchPhrase", ValueType::kVarChar),
                         Column("URL", ValueType::kVarChar),
                         Column("UserID", ValueType::kInt64)});
  const std::vector<std::string> urls = {
      "http://example.com/a/1",       "http://example.com/a/2",
      "http://example.com/goo/1",     "http://example.com/google/1",
      "http://example.com/google/2",  "http://example.com/google/3",
      "http://google.com/search?q=a", "http://google.com/search?q=b",
      "http://news.org/story",        "http://z.org/goo",
      "http://z.org/google",
  };
  const std::vector<std::string> phrases = {
      "", "alpha", "beta", "gamma", "delta", "epsilon", "zeta",
  };
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CreateTable(ctx, schema).GetStatus(), Status::kSuccess);
    Table tbl = db->GetTable(ctx, "hits").MoveValue();
    for (int64_t i = 0; i < 6000; ++i) {
      Value ph = (i % 41 == 0)
                     ? Value()
                     : Value(std::string(
                           phrases[static_cast<size_t>(i % phrases.size())]));
      Value url =
          (i % 37 == 0)
              ? Value()
              : Value(std::string(urls[static_cast<size_t>(i % urls.size())]));
      Value uid = (i % 29 == 0) ? Value() : Value((i * 13) % 500);
      ASSERT_TRUE(tbl.Insert(ctx.txn_, Row(std::vector<Value>{ph, url, uid}))
                      .HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  SqlEngine engine(*db);
  const std::vector<std::string> queries = {
      "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'",
      "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c "
      "FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' "
      "GROUP BY SearchPhrase ORDER BY c DESC, SearchPhrase ASC LIMIT 5",
      "SELECT SearchPhrase, MIN(URL), COUNT(DISTINCT UserID), COUNT(*) AS c "
      "FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' "
      "GROUP BY SearchPhrase ORDER BY c DESC, SearchPhrase ASC LIMIT 5",
      "SELECT MIN(UserID), MAX(UserID) FROM hits",
  };
  auto run_all = [&]() {
    std::vector<std::vector<std::string>> out;
    TransactionContext ctx = db->BeginReadOnlyContext();
    for (const std::string& sql : queries) {
      StatusOr<Executor> prep = engine.Prepare(ctx, sql);
      EXPECT_TRUE(prep.HasValue()) << sql << ": " << prep.GetStatus();
      std::vector<std::string> rows;
      Row r;
      while (prep.HasValue() && prep.Value()->Next(&r, nullptr)) {
        rows.push_back(r.ToString());
      }
      out.push_back(std::move(rows));
    }
    return out;
  };
  const auto row_store = run_all();
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "hits"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  const auto tlcs = run_all();
  for (size_t i = 0; i < queries.size(); ++i) {
    EXPECT_EQ(tlcs[i], row_store[i]) << queries[i];
  }
  db->DeleteAll();
}

// Lean proofs:
//   lean/TinylambOpt/ResearchProofs/StorageExecution/DerivedZonesA.lean
//     (Product, SaturatingLoss, Distance, BucketSum: SoundInterval, SkipRange)
//   lean/TinylambOpt/ResearchProofs/StorageExecution/BagKernelsA.lean
//     (DictionaryAliases.DistinctOutput, DictionaryAliases.FilteredMoment)
TEST(ColumnarTlcsFastPathTest,
     DerivedZonesAndBagKernelsMatchRowStoreAfterCompaction) {
  const std::string db_name = "tlcs_se_derived_bag_" + RandomString();
  ASSIGN_OR_ASSERT_FAIL(std::unique_ptr<Database>, db,
                        Database::Create(db_name));
  {
    TransactionContext ctx = db->BeginContext();
    Schema schema(
        "metrics",
        {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64),
         Column("x", ValueType::kDouble), Column("y", ValueType::kDouble),
         Column("grp", ValueType::kVarChar),
         Column("tag", ValueType::kVarChar)});
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    const std::vector<std::string> tags = {"alpha", "beta", "gamma_long",
                                           "delta_very_long"};
    for (int64_t i = 0; i < 600; ++i) {
      const int64_t a = (i < 300) ? (i - 150) : (1000 + i);
      const int64_t b = (i < 300) ? ((i % 17) - 8) : (20 + (i % 11));
      const Value av = (i % 41 == 0) ? Value() : Value(a);
      const Value bv = (i % 53 == 0) ? Value() : Value(b);
      const Value xv =
          (i % 47 == 0) ? Value() : Value(static_cast<double>(a) * 0.5);
      const Value yv =
          (i % 59 == 0) ? Value() : Value(static_cast<double>(b) * 0.25);
      const Value tagv =
          (i % 29 == 0) ? Value()
                        : Value(std::string(tags[static_cast<size_t>(i) % 4]));
      Row r({av, bv, xv, yv, Value("g" + std::to_string(i % 7)), tagv});
      ASSERT_TRUE(tbl.Insert(ctx.txn_, r).HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  SqlEngine engine(*db);
  const std::vector<std::string> queries = {
      // 2-column direct comparison block pruning.
      "SELECT COUNT(*), SUM(a) FROM metrics WHERE a < b",
      "SELECT COUNT(*) FROM metrics WHERE x >= y",
      // DerivedZones INT64: Product, BucketSum, SaturatingLoss, Distance.
      "SELECT COUNT(*), SUM(a) FROM metrics WHERE a * b > 30000",
      "SELECT COUNT(*), SUM(a) FROM metrics WHERE a + b < -100",
      "SELECT COUNT(*), SUM(a) FROM metrics WHERE a - b >= 1400",
      "SELECT COUNT(*), SUM(a) FROM metrics WHERE ABS(a - b) <= 10",
      // DerivedZones DOUBLE: Product, Distance.
      "SELECT COUNT(*) FROM metrics WHERE x * y > 4000.0",
      "SELECT COUNT(*) FROM metrics WHERE ABS(x - y) < 5.0",
      // BagKernels: dictionary-encoded late COUNT(DISTINCT) + BYTE_LENGTH.
      "SELECT grp, COUNT(DISTINCT tag), SUM(BYTE_LENGTH(tag)), COUNT(*) AS c "
      "FROM metrics GROUP BY grp ORDER BY c DESC, grp ASC LIMIT 4",
  };
  auto run_all = [&]() {
    std::vector<std::vector<std::string>> out;
    TransactionContext ctx = db->BeginReadOnlyContext();
    for (const std::string& sql : queries) {
      StatusOr<Executor> prep = engine.Prepare(ctx, sql);
      EXPECT_TRUE(prep.HasValue()) << sql << ": " << prep.GetStatus();
      std::vector<std::string> rows;
      Row r;
      while (prep.HasValue() && prep.Value()->Next(&r, nullptr)) {
        rows.push_back(r.ToString());
      }
      out.push_back(std::move(rows));
    }
    return out;
  };
  const auto row_store = run_all();
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "metrics"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  const auto tlcs = run_all();
  for (size_t i = 0; i < queries.size(); ++i) {
    EXPECT_EQ(tlcs[i], row_store[i]) << queries[i];
  }
  db->DeleteAll();
}

// Lean proofs:
//   lean/TinylambOpt/ResearchProofs/StorageExecution/PostingJoinsA.lean
//     (Following.OffsetCount, Following.IntegralSum,
//     Following.LeftMultiplicity)
//   lean/TinylambOpt/ResearchProofs/StorageExecution/DerivedZonesA.lean
//     (Product.DominatedTop, BucketSum.TakeAll)
//   lean/TinylambOpt/ResearchProofs/StorageExecution/OrderedFootersA.lean
//     (MonotoneBreaks.SortingCertificate)
TEST(ColumnarTlcsFastPathTest, PostingBandJoinIntegralSumAndDoubleTopKPruning) {
  const std::string db_name = "tlcs_se_posting_topk_" + RandomString();
  ASSIGN_OR_ASSERT_FAIL(std::unique_ptr<Database>, db,
                        Database::Create(db_name));
  {
    TransactionContext ctx = db->BeginContext();
    Schema ls("lhs",
              {Column("k", ValueType::kInt64), Column("w", ValueType::kInt64),
               Column("d", ValueType::kDouble)});
    Schema rs("rhs",
              {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64),
               Column("p", ValueType::kDouble)});
    ASSIGN_OR_ASSERT_FAIL(Table, lt, db->CreateTable(ctx, ls));
    ASSIGN_OR_ASSERT_FAIL(Table, rt, db->CreateTable(ctx, rs));
    for (int64_t i = 0; i < 80; ++i) {
      const Value lk = (i % 13 == 0) ? Value() : Value((i * 3) - 20);
      const Value lw = (i % 11 == 0) ? Value() : Value(i * 10);
      const Value ld =
          (i % 17 == 0) ? Value() : Value(static_cast<double>(i - 40) * 1.25);
      ASSERT_TRUE(lt.Insert(ctx.txn_, Row({lk, lw, ld})).HasValue());
    }
    for (int64_t i = 0; i < 120; ++i) {
      const Value rk = (i % 19 == 0) ? Value() : Value(((i % 60) * 2) - 10);
      const Value rv = (i % 7 == 0) ? Value() : Value(i + 1);
      const Value rp =
          (i % 9 == 0) ? Value() : Value(static_cast<double>(i) * 0.5);
      ASSERT_TRUE(rt.Insert(ctx.txn_, Row({rk, rv, rp})).HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  SqlEngine engine(*db);
  auto run_sql = [&](const std::string& sql) {
    TransactionContext ctx = db->BeginReadOnlyContext();
    StatusOr<Executor> prep = engine.Prepare(ctx, sql);
    EXPECT_TRUE(prep.HasValue()) << sql << ": " << prep.GetStatus();
    std::vector<std::string> rows;
    Row r;
    while (prep.HasValue() && prep.Value()->Next(&r, nullptr)) {
      rows.push_back(r.ToString());
    }
    return rows;
  };

  // Compare PostingJoins fast path (no WHERE) against general join (WHERE 1=1).
  const std::vector<std::pair<std::string, std::string>> band_pairs = {
      {"SELECT COUNT(*), COUNT(r.v), SUM(r.v), SUM(l.w), SUM(r.p) "
       "FROM lhs l JOIN rhs r ON r.k > l.k AND r.k <= l.k + 5",
       "SELECT COUNT(*), COUNT(r.v), SUM(r.v), SUM(l.w), SUM(r.p) "
       "FROM lhs l JOIN rhs r ON r.k > l.k AND r.k <= l.k + 5 WHERE 1 = 1"},
      {"SELECT COUNT(*), COUNT(r.v), SUM(r.v), COUNT(l.w), SUM(l.w) "
       "FROM lhs l LEFT JOIN rhs r ON r.k >= l.k - 2 AND r.k <= l.k + 4",
       "SELECT COUNT(*), COUNT(r.v), SUM(r.v), COUNT(l.w), SUM(l.w) "
       "FROM lhs l LEFT JOIN rhs r ON r.k >= l.k - 2 AND r.k <= l.k + 4 "
       "WHERE 1 = 1"},
      {"SELECT COUNT(*), SUM(r.v) FROM lhs l JOIN rhs r ON l.k < r.k",
       "SELECT COUNT(*), SUM(r.v) FROM lhs l JOIN rhs r ON l.k < r.k "
       "WHERE 1 = 1"},
  };
  for (const auto& [fast_sql, ref_sql] : band_pairs) {
    EXPECT_EQ(run_sql(fast_sql), run_sql(ref_sql)) << fast_sql;
  }

  // DOUBLE Top-K block pruning + DerivedZones.TakeAll parity after compaction.
  const std::vector<std::string> tlcs_queries = {
      "SELECT d, k FROM lhs ORDER BY d ASC LIMIT 5",
      "SELECT d, k FROM lhs ORDER BY d DESC LIMIT 5",
      "SELECT COUNT(*), SUM(k) FROM lhs WHERE k + w > -100000",
  };
  std::vector<std::vector<std::string>> row_store;
  row_store.reserve(tlcs_queries.size());
  for (const std::string& q : tlcs_queries) {
    row_store.push_back(run_sql(q));
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "lhs"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  for (size_t i = 0; i < tlcs_queries.size(); ++i) {
    EXPECT_EQ(run_sql(tlcs_queries[i]), row_store[i]) << tlcs_queries[i];
  }
  db->DeleteAll();
}

// Lean proofs:
//   lean/TinylambOpt/ResearchProofs/StorageExecution/PostingJoinsA.lean
//     (Annulus.TwoSlices, Annulus.OffsetCount, Annulus.IntegralSum,
//      Annulus.LeftMultiplicity)
//   lean/TinylambOpt/ResearchProofs/StorageExecution/DerivedZonesA.lean
//     (Distance.BandInverse, FloorRatio.SoundInterval, FloorRatio.SkipRange,
//      FloorRatio.TakeAll)
//   lean/TinylambOpt/ResearchProofs/StorageExecution/DerivedZonesB.lean
//     (BitwiseXor.SoundInterval, BitwiseXor.SkipRange, BitwiseXor.TakeAll)
TEST(ColumnarTlcsFastPathTest,
     AnnulusPostingJoinAndFloorDivBitXorDerivedZones) {
  const std::string db_name = "tlcs_se_annulus_xor_" + RandomString();
  ASSIGN_OR_ASSERT_FAIL(std::unique_ptr<Database>, db,
                        Database::Create(db_name));
  {
    TransactionContext ctx = db->BeginContext();
    Schema ls("lhs",
              {Column("k", ValueType::kInt64), Column("a", ValueType::kInt64),
               Column("b", ValueType::kInt64)});
    Schema rs("rhs",
              {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64)});
    ASSIGN_OR_ASSERT_FAIL(Table, lt, db->CreateTable(ctx, ls));
    ASSIGN_OR_ASSERT_FAIL(Table, rt, db->CreateTable(ctx, rs));
    for (int64_t i = 0; i < 100; ++i) {
      const Value lk = (i % 13 == 0) ? Value() : Value((i * 2) - 40);
      // Non-negative a and strictly positive b so FloorRatio & BitwiseXor fire.
      const Value la = (i % 17 == 0) ? Value() : Value(8 + (i % 4));
      const Value lb = (i % 19 == 0) ? Value() : Value(4 + (i % 4));
      ASSERT_TRUE(lt.Insert(ctx.txn_, Row({lk, la, lb})).HasValue());
    }
    for (int64_t i = 0; i < 120; ++i) {
      const Value rk = (i % 11 == 0) ? Value() : Value((i * 2) - 50);
      const Value rv = (i % 7 == 0) ? Value() : Value(i + 1);
      ASSERT_TRUE(rt.Insert(ctx.txn_, Row({rk, rv})).HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  SqlEngine engine(*db);
  auto run_sql = [&](const std::string& sql) {
    TransactionContext ctx = db->BeginReadOnlyContext();
    StatusOr<Executor> prep = engine.Prepare(ctx, sql);
    EXPECT_TRUE(prep.HasValue()) << sql << ": " << prep.GetStatus();
    std::vector<std::string> rows;
    Row r;
    while (prep.HasValue() && prep.Value()->Next(&r, nullptr)) {
      rows.push_back(r.ToString());
    }
    return rows;
  };

  // Wave 9: Distance (`ABS(r.k - l.k) <= 6`) and Annulus (`> 2 AND <= 8`),
  // INNER and LEFT JOIN, compared against general join (`WHERE 1 = 1`).
  const std::vector<std::pair<std::string, std::string>> annulus_pairs = {
      {"SELECT COUNT(*), COUNT(r.v), SUM(r.v), SUM(l.a) "
       "FROM lhs l JOIN rhs r ON ABS(r.k - l.k) <= 6",
       "SELECT COUNT(*), COUNT(r.v), SUM(r.v), SUM(l.a) "
       "FROM lhs l JOIN rhs r ON ABS(r.k - l.k) <= 6 WHERE 1 = 1"},
      {"SELECT COUNT(*), COUNT(r.v), SUM(r.v), SUM(l.a) "
       "FROM lhs l JOIN rhs r ON ABS(r.k - l.k) > 2 AND ABS(r.k - l.k) <= 8",
       "SELECT COUNT(*), COUNT(r.v), SUM(r.v), SUM(l.a) "
       "FROM lhs l JOIN rhs r ON ABS(r.k - l.k) > 2 AND ABS(r.k - l.k) <= 8 "
       "WHERE 1 = 1"},
      {"SELECT COUNT(*), COUNT(r.v), SUM(r.v), COUNT(l.a), SUM(l.a) "
       "FROM lhs l LEFT JOIN rhs r "
       "ON ABS(l.k - r.k) >= 3 AND ABS(l.k - r.k) < 10",
       "SELECT COUNT(*), COUNT(r.v), SUM(r.v), COUNT(l.a), SUM(l.a) "
       "FROM lhs l LEFT JOIN rhs r "
       "ON ABS(l.k - r.k) >= 3 AND ABS(l.k - r.k) < 10 WHERE 1 = 1"},
      {"SELECT COUNT(*), SUM(r.v) FROM lhs l JOIN rhs r ON ABS(r.k - l.k) < 0",
       "SELECT COUNT(*), SUM(r.v) FROM lhs l JOIN rhs r ON ABS(r.k - l.k) < 0 "
       "WHERE 1 = 1"},
  };
  for (const auto& [fast_sql, ref_sql] : annulus_pairs) {
    EXPECT_EQ(run_sql(fast_sql), run_sql(ref_sql)) << fast_sql;
  }

  // Wave 10: FloorRatio (`DIV(a, b)`) and BitwiseXor (`a ^ b`) derived zones
  // (SoundInterval, SkipRange, TakeAll) parity after compaction.
  const std::vector<std::string> derived_queries = {
      "SELECT COUNT(*), SUM(a) FROM lhs WHERE DIV(a, b) = 2",
      "SELECT COUNT(*), SUM(a) FROM lhs WHERE DIV(a, b) > 10",
      "SELECT COUNT(*), SUM(a) FROM lhs WHERE DIV(a, b) >= 1",
      "SELECT COUNT(*), SUM(a) FROM lhs WHERE (a ^ b) >= 12",
      "SELECT COUNT(*), SUM(a) FROM lhs WHERE (a ^ b) = 12",
      "SELECT COUNT(*), SUM(a) FROM lhs WHERE (a ^ b) < 12",
      "SELECT COUNT(*), SUM(a) FROM lhs WHERE (a ^ b) > 15",
  };
  std::vector<std::vector<std::string>> row_store;
  row_store.reserve(derived_queries.size());
  for (const std::string& q : derived_queries) {
    row_store.push_back(run_sql(q));
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "lhs"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  for (size_t i = 0; i < derived_queries.size(); ++i) {
    EXPECT_EQ(run_sql(derived_queries[i]), row_store[i]) << derived_queries[i];
  }

  // Wave 13 (GrammarScans.DictionaryMask +
  // BitLayouts.RowMajor.DictionaryProbe): Row-level narrow-dictionary bitmask
  // probe for LIKE / IN / = with NULLs.
  {
    TransactionContext ctx = db->BeginContext();
    Schema ds("urls", {Column("id", ValueType::kInt64),
                       Column("url", ValueType::kVarChar)});
    ASSIGN_OR_ASSERT_FAIL(Table, ut, db->CreateTable(ctx, ds));
    const std::array<std::string, 5> pool = {
        "https://a.example/path/one", "https://b.example/search?q=1",
        "https://c.example/path/two", "https://d.example/home", ""};
    for (int64_t i = 0; i < 200; ++i) {
      const Value uv =
          (i % 17 == 0) ? Value() : Value(std::string(pool[i % pool.size()]));
      ASSERT_TRUE(ut.Insert(ctx.txn_, Row({Value(i), uv})).HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  const std::vector<std::string> dict_queries = {
      "SELECT COUNT(*), SUM(id) FROM urls WHERE url LIKE '%/path/%'",
      "SELECT COUNT(*), SUM(id) FROM urls WHERE url IN "
      "('https://b.example/search?q=1', 'https://d.example/home')",
      "SELECT COUNT(*), SUM(id) FROM urls WHERE url = ''",
      "SELECT COUNT(*), SUM(id) FROM urls WHERE url <> ''",
      "SELECT COUNT(*), SUM(id) FROM urls WHERE url IS NULL",
  };
  std::vector<std::vector<std::string>> dict_row_store;
  dict_row_store.reserve(dict_queries.size());
  for (const std::string& q : dict_queries) {
    dict_row_store.push_back(run_sql(q));
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "urls"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  for (size_t i = 0; i < dict_queries.size(); ++i) {
    EXPECT_EQ(run_sql(dict_queries[i]), dict_row_store[i]) << dict_queries[i];
  }
  db->DeleteAll();
}

// Lean proofs:
//   lean/TinylambOpt/ResearchProofs/LayerContracts/CalendarRanges_All.lean
//     (Month.PredicateRange, Quarter.PredicateRange, CivilYear.PredicateRange)
//   lean/TinylambOpt/ResearchProofs/LayerContracts/SeekIntervals_All.lean
//     (DistanceBand.ExactSeek)
//   lean/TinylambOpt/ResearchProofs/LayerContracts/FrameRetractions_All.lean
//     (Sum.FastRetraction, Minimum.FastRetraction, Maximum.FastRetraction)
TEST(ColumnarTlcsFastPathTest,
     LayerContractsCalendarRangesDistanceBandAndFrameRetractions) {
  const std::string db_name = "tlcs_lc_cal_win_" + RandomString();
  ASSIGN_OR_ASSERT_FAIL(std::unique_ptr<Database>, db,
                        Database::Create(db_name));
  {
    TransactionContext ctx = db->BeginContext();
    Schema s("ev",
             {Column("id", ValueType::kInt64), Column("d", ValueType::kDate),
              Column("v", ValueType::kInt64)});
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, s));
    const int64_t base_day = Value::Date("2020-01-01").value.int_value;
    for (int64_t i = 0; i < 400; ++i) {
      const Value dv =
          (i % 23 == 0) ? Value() : Value::DateFromDays(base_day + i);
      const Value vv = (i % 19 == 0) ? Value() : Value(((i * 7) % 50) - 20);
      ASSERT_TRUE(tbl.Insert(ctx.txn_, Row({Value(i), dv, vv})).HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  SqlEngine engine(*db);
  auto run_sql = [&](const std::string& sql) {
    TransactionContext ctx = db->BeginReadOnlyContext();
    StatusOr<Executor> prep = engine.Prepare(ctx, sql);
    EXPECT_TRUE(prep.HasValue()) << sql << ": " << prep.GetStatus();
    std::vector<std::string> rows;
    Row r;
    while (prep.HasValue() && prep.Value()->Next(&r, nullptr)) {
      rows.push_back(r.ToString());
    }
    return rows;
  };

  // CalendarRanges + SeekIntervals.DistanceBand parity before vs after TLCS
  // compaction.
  const std::vector<std::string> tlcs_queries = {
      "SELECT COUNT(*), SUM(v) FROM ev "
      "WHERE date_trunc(d, 'MONTH') = DATE '2020-02-01'",
      "SELECT COUNT(*), SUM(v) FROM ev "
      "WHERE date_trunc(d, 'QUARTER') = DATE '2020-04-01'",
      "SELECT COUNT(*), SUM(v) FROM ev "
      "WHERE date_trunc(d, 'YEAR') = DATE '2020-01-01'",
      "SELECT COUNT(*), SUM(v) FROM ev "
      "WHERE date_trunc(d, 'MONTH') = DATE '2020-02-15'",
      "SELECT COUNT(*), SUM(id) FROM ev WHERE ABS(v - 5) <= 4",
      "SELECT COUNT(*), SUM(id) FROM ev WHERE ABS(5 - v) < 4",
      "SELECT COUNT(*), SUM(id) FROM ev WHERE ABS(v - 5) < 0",
  };
  std::vector<std::vector<std::string>> row_store;
  row_store.reserve(tlcs_queries.size());
  for (const std::string& q : tlcs_queries) {
    row_store.push_back(run_sql(q));
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "ev"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  for (size_t i = 0; i < tlcs_queries.size(); ++i) {
    EXPECT_EQ(run_sql(tlcs_queries[i]), row_store[i]) << tlcs_queries[i];
  }

  // FrameRetractions: compare sliding ROWS window (fast path) against
  // expression-wrapped argument (v + 0, general path) across SUM/AVG/COUNT/
  // MIN/MAX with NULLs and both preceding/following offsets.
  const std::vector<std::pair<std::string, std::string>> win_pairs = {
      {"SELECT id, "
       "SUM(v) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 2 FOLLOWING), "
       "COUNT(v) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 2 FOLLOWING), "
       "MIN(v) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 2 FOLLOWING), "
       "MAX(v) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 2 FOLLOWING) "
       "FROM ev WHERE id < 60 ORDER BY id",
       "SELECT id, "
       "SUM(v + 0) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 2 "
       "FOLLOWING), "
       "COUNT(v + 0) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 2 "
       "FOLLOWING), "
       "MIN(v + 0) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 2 "
       "FOLLOWING), "
       "MAX(v + 0) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 2 "
       "FOLLOWING) "
       "FROM ev WHERE id < 60 ORDER BY id"},
      {"SELECT id, "
       "COUNT(*) OVER (ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 5 FOLLOWING), "
       "AVG(v) OVER (ORDER BY id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) "
       "FROM ev WHERE id < 40 ORDER BY id",
       "SELECT id, "
       "COUNT(*) OVER (ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 5 FOLLOWING), "
       "AVG(v + 0) OVER (ORDER BY id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) "
       "FROM ev WHERE id < 40 ORDER BY id"},
  };
  for (const auto& [fast_sql, ref_sql] : win_pairs) {
    EXPECT_EQ(run_sql(fast_sql), run_sql(ref_sql)) << fast_sql;
  }
  db->DeleteAll();
}

TEST(ColumnarTlcsFastPathTest,
     DerivedZonesSeekIntervalsAndBucketPostingJoinsParity) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "execute_query unavailable";
  }
  BudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(size_t{512} << 20, size_t{128} << 20, size_t{2} << 20,
                      /*check_rss_pressure=*/false);

  const std::string prefix = "tlcs_dz_seek_pj_" + RandomString();
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  Schema sc_t(
      "t",
      {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64),
       Column("da", ValueType::kDouble), Column("db", ValueType::kDouble)});
  Schema sc_l("jl",
              {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64)});
  Schema sc_r("jr",
              {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64)});
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CreateTable(ctx, sc_t).GetStatus(), Status::kSuccess);
    ASSERT_EQ(db->CreateTable(ctx, sc_l).GetStatus(), Status::kSuccess);
    ASSERT_EQ(db->CreateTable(ctx, sc_r).GetStatus(), Status::kSuccess);
    Table tbl = db->GetTable(ctx, "t").MoveValue();
    for (int64_t i = 0; i < 300; ++i) {
      Value a = (i % 19 == 0) ? Value() : Value(i % 100);
      Value b = (i % 23 == 0) ? Value() : Value((i * 7) % 100);
      Value da = (i % 19 == 0) ? Value() : Value(static_cast<double>(i % 100));
      Value dbv =
          (i % 23 == 0) ? Value() : Value(static_cast<double>((i * 7) % 100));
      ASSERT_TRUE(tbl.Insert(ctx.txn_, Row({a, b, da, dbv})).HasValue());
    }
    Table tl = db->GetTable(ctx, "jl").MoveValue();
    for (int64_t i = 0; i < 60; ++i) {
      Value k = (i % 13 == 0) ? Value() : Value(i % 25);
      ASSERT_TRUE(tl.Insert(ctx.txn_, Row({k, Value(i + 1)})).HasValue());
    }
    Table tr = db->GetTable(ctx, "jr").MoveValue();
    for (int64_t i = 0; i < 90; ++i) {
      Value k = (i % 17 == 0) ? Value() : Value(i);
      ASSERT_TRUE(tr.Insert(ctx.txn_, Row({k, Value((i * 3) + 1)})).HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  auto run_sql = [&](const std::string& sql) {
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine eng(*db);
    return RunSql(&eng, &ctx, sql);
  };

  // Part 1: DerivedZones (CappedPair + SaturatingLoss) & SeekIntervals
  // (QuotientBand + MinClamp) parity before vs after TLCS compaction.
  const std::vector<std::string> tlcs_queries = {
      "SELECT COUNT(*), SUM(a) FROM t WHERE LEAST(a, b) > 40",
      "SELECT COUNT(*), SUM(a) FROM t WHERE GREATEST(a, b) < 35",
      "SELECT COUNT(*), SUM(a) FROM t WHERE GREATEST(a - b, 0) >= 25",
      "SELECT COUNT(*), SUM(a) FROM t WHERE GREATEST(0, a - b) = 0",
      "SELECT COUNT(*), SUM(a) FROM t WHERE LEAST(da, db) >= 45.0",
      "SELECT COUNT(*), SUM(a) FROM t WHERE GREATEST(da - db, 0.0) > 30.0",
      "SELECT COUNT(*), SUM(a) FROM t WHERE DIV(a, 10) = 5",
      "SELECT COUNT(*), SUM(a) FROM t WHERE DIV(a, 10) >= 8",
      "SELECT COUNT(*), SUM(a) FROM t WHERE DIV(a, 10) < 3",
      "SELECT COUNT(*), SUM(a) FROM t WHERE DIV(a, 10) = -1",
      "SELECT COUNT(*), SUM(a) FROM t WHERE LEAST(a, 50) = 50",
      "SELECT COUNT(*), SUM(a) FROM t WHERE LEAST(a, 50) = 20",
      "SELECT COUNT(*), SUM(a) FROM t WHERE LEAST(a, 50) = 60",
      "SELECT COUNT(*), SUM(a) FROM t WHERE LEAST(a, 50) > 40",
      "SELECT COUNT(*), SUM(a) FROM t WHERE GREATEST(a, 50) = 50",
      "SELECT COUNT(*), SUM(a) FROM t WHERE GREATEST(a, 50) = 80",
      "SELECT COUNT(*), SUM(a) FROM t WHERE GREATEST(a, 50) = 10",
      "SELECT COUNT(*), SUM(a) FROM t WHERE GREATEST(da, 50.0) <= 60.0",
  };
  std::vector<std::vector<Row>> row_store;
  row_store.reserve(tlcs_queries.size());
  for (const std::string& q : tlcs_queries) {
    row_store.push_back(run_sql(q));
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "t"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  for (size_t i = 0; i < tlcs_queries.size(); ++i) {
    EXPECT_EQ(run_sql(tlcs_queries[i]), row_store[i]) << tlcs_queries[i];
  }

  // Part 2: PostingJoins (SameBucket, ScaledFloor, Apart) parity against
  // general join path (expression-wrapped key disables posting fast path).
  const std::vector<std::pair<std::string, std::string>> join_pairs = {
      {"SELECT COUNT(*), SUM(jr.v), SUM(jl.v) "
       "FROM jl JOIN jr ON DIV(jr.k, 10) = DIV(jl.k, 10)",
       "SELECT COUNT(*), SUM(jr.v), SUM(jl.v) "
       "FROM jl JOIN jr ON DIV(jr.k + 0, 10) = DIV(jl.k + 0, 10)"},
      {"SELECT COUNT(*), COUNT(jr.v), SUM(jr.v), SUM(jl.v) "
       "FROM jl LEFT JOIN jr ON DIV(jr.k, 10) = DIV(jl.k, 10)",
       "SELECT COUNT(*), COUNT(jr.v), SUM(jr.v), SUM(jl.v) "
       "FROM jl LEFT JOIN jr ON DIV(jr.k + 0, 10) = DIV(jl.k + 0, 10)"},
      {"SELECT COUNT(*), SUM(jr.v), SUM(jl.v) "
       "FROM jl JOIN jr ON DIV(jr.k, 5) = jl.k",
       "SELECT COUNT(*), SUM(jr.v), SUM(jl.v) "
       "FROM jl JOIN jr ON DIV(jr.k + 0, 5) = jl.k + 0"},
      {"SELECT COUNT(*), COUNT(jr.v), SUM(jr.v) "
       "FROM jl LEFT JOIN jr ON jl.k = DIV(jr.k, 5)",
       "SELECT COUNT(*), COUNT(jr.v), SUM(jr.v) "
       "FROM jl LEFT JOIN jr ON jl.k + 0 = DIV(jr.k + 0, 5)"},
      {"SELECT COUNT(*), SUM(jr.v), SUM(jl.v) "
       "FROM jl JOIN jr ON ABS(jr.k - jl.k) > 20",
       "SELECT COUNT(*), SUM(jr.v), SUM(jl.v) "
       "FROM jl JOIN jr ON ABS((jr.k + 0) - jl.k) > 20"},
  };
  for (const auto& [fast_sql, ref_sql] : join_pairs) {
    EXPECT_EQ(run_sql(fast_sql), run_sql(ref_sql)) << fast_sql;
  }
  db->DeleteAll();
}

TEST(ColumnarTlcsFastPathTest, EngineDecisionsRangeFingersParity) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "execute_query unavailable";
  }
  BudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(size_t{512} << 20, size_t{128} << 20, size_t{2} << 20,
                      /*check_rss_pressure=*/false);

  const std::string prefix = "ed_rf_" + RandomString();
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  Schema sc_l("sl",
              {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64)});
  Schema sc_r("sr",
              {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64)});
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CreateTable(ctx, sc_l).GetStatus(), Status::kSuccess);
    ASSERT_EQ(db->CreateTable(ctx, sc_r).GetStatus(), Status::kSuccess);
    // Sorted non-decreasing left table with duplicates & NULLs to exercise
    // RangeFingers linear two-finger sweep.
    Table tl = db->GetTable(ctx, "sl").MoveValue();
    for (int64_t i = 0; i < 50; ++i) {
      Value k = (i % 12 == 0) ? Value() : Value(i / 2);
      ASSERT_TRUE(tl.Insert(ctx.txn_, Row({k, Value(i + 1)})).HasValue());
    }
    Table tr = db->GetTable(ctx, "sr").MoveValue();
    for (int64_t i = 0; i < 80; ++i) {
      Value k = (i % 15 == 0) ? Value() : Value((i * 13) % 60);
      ASSERT_TRUE(tr.Insert(ctx.txn_, Row({k, Value((i * 2) + 1)})).HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  auto run_sql = [&](const std::string& sql) {
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine eng(*db);
    return RunSql(&eng, &ctx, sql);
  };

  // EngineDecisions.Executor.RangeFingers (monotone sorted left keys)
  const std::vector<std::pair<std::string, std::string>> join_pairs = {
      {"SELECT COUNT(*), SUM(sr.v), SUM(sl.v) "
       "FROM sl JOIN sr ON sr.k > sl.k AND sr.k <= sl.k + 10",
       "SELECT COUNT(*), SUM(sr.v), SUM(sl.v) "
       "FROM sl JOIN sr ON (sr.k + 0) > sl.k AND (sr.k + 0) <= sl.k + 10"},
      {"SELECT COUNT(*), COUNT(sr.v), SUM(sr.v), SUM(sl.v) "
       "FROM sl LEFT JOIN sr ON sr.k >= sl.k - 2 AND sr.k < sl.k + 8",
       "SELECT COUNT(*), COUNT(sr.v), SUM(sr.v), SUM(sl.v) "
       "FROM sl LEFT JOIN sr ON (sr.k + 0) >= sl.k - 2 AND (sr.k + 0) < sl.k + "
       "8"},
      {"SELECT COUNT(*), SUM(sr.v), SUM(sl.v) "
       "FROM sl JOIN sr ON DIV(sr.k, 10) = DIV(sl.k, 10)",
       "SELECT COUNT(*), SUM(sr.v), SUM(sl.v) "
       "FROM sl JOIN sr ON DIV(sr.k + 0, 10) = DIV(sl.k + 0, 10)"},
      {"SELECT COUNT(*), SUM(sr.v) "
       "FROM sl JOIN sr ON DIV(sr.k, 3) = sl.k",
       "SELECT COUNT(*), SUM(sr.v) "
       "FROM sl JOIN sr ON DIV(sr.k + 0, 3) = sl.k + 0"},
  };
  for (const auto& [fast_sql, ref_sql] : join_pairs) {
    EXPECT_EQ(run_sql(fast_sql), run_sql(ref_sql)) << fast_sql;
  }
  db->DeleteAll();
}

TEST(ColumnarTlcsFastPathTest, ClickBenchLayoutConcatSearchDenseAndBoundary) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "execute_query unavailable";
  }
  BudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(size_t{512} << 20, size_t{128} << 20, size_t{2} << 20,
                      /*check_rss_pressure=*/false);

  const std::string prefix = "cb_concat_" + RandomString();
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  Schema sc("urls", {Column("id", ValueType::kInt64),
                     Column("u", ValueType::kVarChar)});
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CreateTable(ctx, sc).GetStatus(), Status::kSuccess);
    Table tbl = db->GetTable(ctx, "urls").MoveValue();
    // 400 distinct strings (> 255 to avoid narrow dictionary encoding),
    // > 120 matches for 'google' (exceeding the legacy 64-hit cap), plus
    // cross-row boundary pairs ("...goo" followed by "gle..."), empty strings,
    // repeated occurrences, and NULLs.
    for (int64_t i = 0; i < 400; ++i) {
      Value u;
      if (i % 29 == 0) {
        u = Value();  // NULL
      } else if (i % 23 == 0) {
        u = Value("");
      } else if (i % 3 == 0) {
        u = Value("https://www.google.com/search?q=google_" +
                  std::to_string(i));
      } else if (i % 5 == 1) {
        u = Value("prefix_" + std::to_string(i) + "_goo");
      } else if (i % 5 == 2) {
        u = Value("gle_suffix_" + std::to_string(i));
      } else {
        u = Value("https://example.org/page/" + std::to_string(i));
      }
      ASSERT_TRUE(tbl.Insert(ctx.txn_, Row({Value(i), u})).HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  auto run_sql = [&](const std::string& sql) {
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine eng(*db);
    return RunSql(&eng, &ctx, sql);
  };

  const std::vector<std::string> queries = {
      "SELECT COUNT(*), SUM(id) FROM urls WHERE u LIKE '%google%'",
      "SELECT COUNT(*), SUM(id) FROM urls WHERE u LIKE '%google%' AND id >= 50",
      "SELECT COUNT(*), SUM(id) FROM urls WHERE u LIKE '%zz%'",
      "SELECT COUNT(*), SUM(id) FROM urls WHERE u LIKE '%e%'",
  };
  std::vector<std::vector<Row>> row_store;
  row_store.reserve(queries.size());
  for (const std::string& q : queries) {
    row_store.push_back(run_sql(q));
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "urls"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  for (size_t i = 0; i < queries.size(); ++i) {
    EXPECT_EQ(run_sql(queries[i]), row_store[i]) << queries[i];
  }
  db->DeleteAll();
}

TEST(ColumnarTlcsFastPathTest, EngineDecisionsAndClickBenchLayoutPrimitives) {
  using namespace engine_decisions;
  using namespace optimizer_decisions;
  using namespace mvcc;

  // 1. TupleAnti: exhaustive 3VL parity against reference SQL `(a,b) NOT IN`.
  const std::vector<std::pair<Value, Value>> build_pairs = {
      {Value(int64_t{2}), Value()},
      {Value(int64_t{1}), Value(int64_t{5})},
  };
  TupleAntiIndex anti;
  for (const auto& [a, b] : build_pairs) {
    anti.Insert(a, b);
  }
  auto ref_not_in = [&](const Value& qa, const Value& qb) -> bool {
    return std::ranges::all_of(build_pairs, [&](const auto& rp) {
      const bool diff_a =
          (!qa.IsNull() && !rp.first.IsNull() && qa != rp.first);
      const bool diff_b =
          (!qb.IsNull() && !rp.second.IsNull() && qb != rp.second);
      return diff_a || diff_b;
    });
  };
  const std::vector<Value> dom = {Value(), Value(int64_t{1}), Value(int64_t{2}),
                                  Value(int64_t{3}), Value(int64_t{5})};
  for (const Value& qa : dom) {
    for (const Value& qb : dom) {
      EXPECT_EQ(anti.Keep(qa, qb), ref_not_in(qa, qb));
    }
  }

  // 2. FlushChoice: Lean #guard [(9,4),(4,1),(3,1)], need=7 -> cost=2.
  const std::array<FlushBucket, 3> buckets = {
      FlushBucket{.reclaim_units = 9, .io_cost = 4},
      FlushBucket{.reclaim_units = 4, .io_cost = 1},
      FlushBucket{.reclaim_units = 3, .io_cost = 1},
  };
  const auto plan = SelectFlushSubset(buckets, 7);
  if (!plan.has_value()) {
    ADD_FAILURE();
    return;
  }
  EXPECT_EQ(plan->total_io_cost, 2ULL);
  EXPECT_EQ(plan->selected_indices, (std::vector<size_t>{1, 2}));
  EXPECT_FALSE(SelectFlushSubset(buckets, 100).has_value());

  // 3. LengthRollup: Lean #guard [none, "ab", "", "c"], slices [(1,2),(3,40)].
  const std::array<uint32_t, 4> ends = {0, 2, 2, 3};
  const std::array<uint8_t, 4> nulls = {1, 0, 0, 0};
  LengthRollupIndex rollup;
  rollup.Build(ends, nulls.data());
  const auto s0 = rollup.QuerySlice(1, 2);
  const auto s1 = rollup.QuerySlice(3, 40);
  EXPECT_EQ(s0.byte_sum + s1.byte_sum, 3ULL);
  EXPECT_EQ(s0.non_empty_count + s1.non_empty_count, 2ULL);

  // 4. NullableGroupKey: non-NULL det strips dep; NULL det keeps dep.
  const auto k_nonnull =
      CompactNullableDependentKey(Value(int64_t{3}), Value(int64_t{99}));
  EXPECT_EQ(k_nonnull.first, Value(int64_t{3}));
  EXPECT_TRUE(k_nonnull.second.IsNull());
  const auto k_null = CompactNullableDependentKey(Value(), Value(int64_t{7}));
  EXPECT_TRUE(k_null.first.IsNull());
  EXPECT_EQ(k_null.second, Value(int64_t{7}));

  // 5. HotRunTiles: 2x3 run split into width-2 tiles with residual hit (0,1).
  EXPECT_EQ(HotRunTileCount(2, 3, 2), 3U);
  std::vector<std::pair<size_t, size_t>> all_pairs;
  for (size_t t = 0; t < 3; ++t) {
    auto tile = HotRunTilePairs(2, 3, 2, t);
    all_pairs.insert(all_pairs.end(), tile.begin(), tile.end());
  }
  EXPECT_EQ(all_pairs.size(), 6U);
  const std::array<std::pair<size_t, size_t>, 1> hits = {{{0, 1}}};
  const HotRunOuterResult outer = FinishHotRunOuter(1, 2, hits);
  EXPECT_TRUE(outer.unmatched_left.empty());
  EXPECT_EQ(outer.unmatched_right, (std::vector<size_t>{0}));

  // 6. SealedBuild: exact early decision before global barrier.
  EXPECT_EQ(DecideSealedBuild(true, false, false),
            SealedBuildVerdict::kMatchFalse);
  EXPECT_EQ(DecideSealedBuild(false, true, false),
            SealedBuildVerdict::kMatchTrue);
  EXPECT_EQ(DecideSealedBuild(false, false, true),
            SealedBuildVerdict::kMatchFalse);
  EXPECT_EQ(DecideSealedBuild(false, false, false),
            SealedBuildVerdict::kPending);

  // 7. SharedBuild & SearchFence.
  const SharedBuildAlt alt_a{.private_cost = 6, .builds = {}};
  const SharedBuildAlt alt_b{.private_cost = 0, .builds = {1}};
  auto unit_price = [](uint64_t) -> uint64_t { return 10; };
  EXPECT_FALSE(SharedBuildDominates(alt_a, alt_b, unit_price));
  EXPECT_FALSE(SharedBuildDominates(alt_b, alt_a, unit_price));
  const SharedBuildAlt alt_c{.private_cost = 12, .builds = {1}};
  EXPECT_TRUE(SharedBuildDominates(alt_a, alt_c, unit_price));
  const std::array<uint64_t, 1> done = {11};
  const std::array<uint64_t, 1> open = {4};
  const auto cert = SearchFenceCertificate(done, open);
  if (!cert.has_value()) {
    ADD_FAILURE();
    return;
  }
  EXPECT_EQ(*cert, 4ULL);
  EXPECT_TRUE(SearchFenceBlocksCutoff(cert, 4));
  EXPECT_FALSE(SearchFenceBlocksCutoff(cert, 6));

  // 8. GcWakeup: Lean #guard [(3, 40)], unpinned, snap=9: due iff durable>=40.
  const std::array<GcVersionStamp, 1> chain = {
      GcVersionStamp{.ts = 3, .lsn = 40}};
  EXPECT_FALSE(IsGcChainWakeupDue(chain, false, 9, 39));
  EXPECT_TRUE(IsGcChainWakeupDue(chain, false, 9, 40));
  GcSweepWakeupFence fence;
  fence.Observe(chain, false);
  EXPECT_FALSE(fence.MayReleaseAt(9, 39));
  EXPECT_TRUE(fence.MayReleaseAt(9, 40));

  // 9. SparseMark: brute-force 2D lex grid parity.
  for (int64_t fx = 0; fx <= 4; ++fx) {
    for (int64_t fy = 0; fy <= 4; ++fy) {
      for (int64_t lx = fx; lx <= 4; ++lx) {
        for (int64_t ly = 0; ly <= 4; ++ly) {
          const LexMarkKey first{.x = fx, .y = fy};
          const LexMarkKey last{.x = lx, .y = ly};
          if (!LexMarkLE(first, last)) {
            continue;
          }
          constexpr int64_t kXl = 1;
          constexpr int64_t kXh = 3;
          constexpr int64_t kYl = 1;
          constexpr int64_t kYh = 2;
          bool exists = false;
          for (int64_t px = kXl; px <= kXh && !exists; ++px) {
            for (int64_t py = kYl; py <= kYh && !exists; ++py) {
              const LexMarkKey p{.x = px, .y = py};
              if (LexMarkLE(first, p) && LexMarkLE(p, last)) {
                exists = true;
              }
            }
          }
          EXPECT_EQ(SparseMarkMayOverlap(first, last, kXl, kXh, kYl, kYh),
                    exists);
        }
      }
    }
  }

  // 10. DefaultGroups: Lean #guard with NULL defaults & 6 total rows.
  const std::vector<SparseGroupLane> lanes = {
      {.default_value = Value(), .exceptions = {{2, Value(int64_t{0})}}},
      {.default_value = Value(int64_t{0}),
       .exceptions = {{2, Value()}, {4, Value(int64_t{1})}}},
  };
  const auto groups = ComputeDefaultGroupsMultiplicity(lanes, 6);
  size_t total_cnt = 0;
  for (const DefaultGroupEntry& g : groups) {
    total_cnt += g.count;
  }
  EXPECT_EQ(total_cnt, 6U);
  EXPECT_EQ(groups.size(), 3U);

  // 11. EngineDecisions.Optimizer.{ProjectExcept, JoinPrefix, ProjectedOrder,
  // JoinRevalidation}
  using optimizer_decisions::CanElideSuffixResortAfterProjection;
  using optimizer_decisions::CanPushProjectionBelowExceptAll;
  using optimizer_decisions::ComputeJoinPrefixDemand;
  using optimizer_decisions::FiberMultiplicity;
  using optimizer_decisions::OrderedKeyRun;
  using optimizer_decisions::RevalidateOneMatchJoinElimination;

  const std::vector<FiberMultiplicity> mixed_fiber = {{1, 0}, {0, 1}};
  const std::vector<FiberMultiplicity> pos_fiber = {{3, 1}, {2, 2}};
  EXPECT_FALSE(CanPushProjectionBelowExceptAll(mixed_fiber));
  EXPECT_TRUE(CanPushProjectionBelowExceptAll(pos_fiber));

  const std::vector<size_t> lbs = {0, 3, 3};
  EXPECT_EQ(ComputeJoinPrefixDemand(lbs, /*offset=*/0, /*limit=*/2), 2U);

  const std::vector<OrderedKeyRun> bad_runs = {
      {.leading_key = 11, .suffix_keys = {9}},
      {.leading_key = 12, .suffix_keys = {1}},
  };
  const std::vector<OrderedKeyRun> good_runs = {
      {.leading_key = 11, .suffix_keys = {1, 5}},
      {.leading_key = 12, .suffix_keys = {5, 9}},
  };
  EXPECT_FALSE(CanElideSuffixResortAfterProjection(
      bad_runs, [](int64_t k) { return k / 10; }));
  EXPECT_TRUE(CanElideSuffixResortAfterProjection(
      good_runs, [](int64_t k) { return k / 10; }));

  const std::vector<uint64_t> f_old = {3, 3};
  const std::vector<uint64_t> d_old = {3};
  const std::vector<uint64_t> d_empty = {};
  EXPECT_TRUE(RevalidateOneMatchJoinElimination(f_old, f_old, d_old, d_old));
  EXPECT_FALSE(RevalidateOneMatchJoinElimination(f_old, f_old, d_old, d_empty));
}

// Lean proofs: lean/TinylambOpt/ResearchProofs/QueryRewrites/AllGoalProofs.lean
//   (Analytics, Tpch, Tpcds, Click, Cloud, Storage)
TEST(ColumnarTlcsFastPathTest, BenchmarkRematchAllFamiliesParity) {
  // 1. Analytics (B3, H2, H5, J3, B4)
  EXPECT_EQ(benchmark_rematch::EncodeThreeKeyDense(2, 3, 4, 5, 6), 82U);
  EXPECT_EQ(benchmark_rematch::EncodeTwoKeyDense(7, 13, 100), 713U);
  const auto bilat = benchmark_rematch::CombineBilateralNullableSums(
      2, std::optional<int64_t>(10), 3, std::optional<int64_t>(20));
  EXPECT_EQ(bilat.left_sum, std::optional<int64_t>(30));
  EXPECT_EQ(bilat.right_sum, std::optional<int64_t>(40));
  const auto empty_bilat = benchmark_rematch::CombineBilateralNullableSums(
      0, std::nullopt, 3, std::optional<int64_t>(20));
  EXPECT_FALSE(empty_bilat.left_sum.has_value());
  EXPECT_FALSE(empty_bilat.right_sum.has_value());

  const std::unordered_map<uint64_t, uint64_t> freq = {{10, 3}, {20, 2}};
  const std::unordered_map<uint64_t, benchmark_rematch::WeightedDimAggregate>
      dims = {{10, {.count = 1, .sum = 1995}}, {20, {.count = 2, .sum = 4000}}};
  const auto j3 = benchmark_rematch::FoldFrequencyWeightedDim(freq, dims);
  EXPECT_EQ(j3.count, 7U);
  EXPECT_EQ(j3.sum, 13985);

  // 2. TPC-H (Q03 Top-K merge, Q15 shard local-maxima)
  const std::vector<std::vector<int>> shards = {{9, 2, 7}, {5, 1, 8}, {4, 3}};
  EXPECT_EQ(benchmark_rematch::MergeShardTopK<int>(shards, 3),
            (std::vector<int>{1, 2, 3}));
  const std::vector<std::vector<std::pair<uint64_t, int64_t>>> sup_shards = {
      {{1, 100}, {2, 200}},
      {{3, 200}, {4, 50}},
  };
  const auto winners =
      benchmark_rematch::SelectGlobalMaximaViaShardCandidates(sup_shards);
  ASSERT_EQ(winners.size(), 2U);
  EXPECT_EQ(winners[0].first, 2U);
  EXPECT_EQ(winners[1].first, 3U);

  // 3. TPC-DS (Q04, Q16, Q40/Q75, Q62/Q99)
  EXPECT_EQ(benchmark_rematch::EncodeChannelYearSlot(2, 1), 5U);
  EXPECT_TRUE(benchmark_rematch::HasOtherNonNullWarehouse(1, 5, 3));
  EXPECT_FALSE(benchmark_rematch::HasOtherNonNullWarehouse(3, 3, 3));
  EXPECT_EQ(benchmark_rematch::ClassifyShippingDelayBucket(30), 0U);
  EXPECT_EQ(benchmark_rematch::ClassifyShippingDelayBucket(60), 1U);
  EXPECT_EQ(benchmark_rematch::ClassifyShippingDelayBucket(90), 2U);
  EXPECT_EQ(benchmark_rematch::ClassifyShippingDelayBucket(120), 3U);
  EXPECT_EQ(benchmark_rematch::ClassifyShippingDelayBucket(121), 4U);
  EXPECT_EQ(benchmark_rematch::LeftJoinNetDelta(100, 0, 0), 100);
  EXPECT_EQ(benchmark_rematch::LeftJoinNetDelta(100, 2, 35), 165);

  // 4. ClickBench & Cloud (Q28, Q37, Q42, Public108)
  const std::vector<uint32_t> lens = {0, 5, 0, 12};
  const auto lsum = benchmark_rematch::SummarizeNonEmptyLengths(lens);
  EXPECT_EQ(lsum.sum, 17U);
  EXPECT_EQ(lsum.non_empty_count, 2U);
  EXPECT_TRUE(benchmark_rematch::CanPruneTopKGroup(5, 4, 10));
  EXPECT_FALSE(benchmark_rematch::CanPruneTopKGroup(5, 5, 10));
  EXPECT_TRUE(benchmark_rematch::OffsetExceedsDistinctGroups(100, 10000));

  benchmark_rematch::PageExtrema pe{.min_val = 10, .max_val = 20};
  pe.Merge({.min_val = 5, .max_val = 30});
  EXPECT_EQ(pe.min_val, std::optional<int64_t>(5));
  EXPECT_EQ(pe.max_val, std::optional<int64_t>(30));

  // 5. Storage codecs (all nine column codec proofs)
  std::vector<uint8_t> wire;
  rematch_storage::EncodeShipAnchoredDates(1000, 995, 1012, &wire);
  size_t pos = 0;
  int64_t dec_commit = 0;
  int64_t dec_receipt = 0;
  ASSERT_TRUE(rematch_storage::DecodeShipAnchoredDates(
      1000, wire, &pos, &dec_commit, &dec_receipt));
  EXPECT_EQ(dec_commit, 995);
  EXPECT_EQ(dec_receipt, 1012);

  rematch_storage::RunEndIndex rle;
  const std::vector<std::pair<uint64_t, size_t>> runs = {{10, 2}, {20, 3}};
  rle.Build(runs);
  EXPECT_EQ(rle.Lookup(0), std::optional<uint64_t>(10));
  EXPECT_EQ(rle.Lookup(2), std::optional<uint64_t>(20));
  EXPECT_FALSE(rle.Lookup(5).has_value());

  rematch_storage::GridWithExceptions grid{
      .start = 0, .warehouses = 4, .exceptions = {{2, {99, 1}}}};
  EXPECT_EQ(grid.At(1), (std::pair<uint64_t, uint64_t>{0, 1}));
  EXPECT_EQ(grid.At(2), (std::pair<uint64_t, uint64_t>{99, 1}));

  rematch_storage::RankIndexedNullableColumn rcol;
  const std::vector<std::optional<uint64_t>> nvals = {std::nullopt, 105,
                                                      std::nullopt, 112};
  rcol.Build(nvals, 100);
  EXPECT_FALSE(rcol.At(0).has_value());
  EXPECT_EQ(rcol.At(1), std::optional<uint64_t>(105));
  EXPECT_EQ(rcol.At(3), std::optional<uint64_t>(112));

  rematch_storage::StridedStringColumn scol;
  const std::vector<std::string_view> strs = {"alpha", "beta", "gamma",
                                              "delta"};
  scol.Build(strs, 2);
  EXPECT_EQ(scol.At(0), "alpha");
  EXPECT_EQ(scol.At(3), "delta");

  EXPECT_EQ(rematch_storage::MedianOfThree(30, 10, 20), 20);

  const std::vector<uint8_t> raw_page = {1, 2, 3, 4, 5, 6, 7};
  const auto tr = rematch_storage::TransposeByteLanes(raw_page, 3);
  for (size_t i = 0; i < raw_page.size(); ++i) {
    EXPECT_EQ(rematch_storage::ReadTransposedByte(tr, raw_page.size(), 3, i),
              raw_page[i]);
  }

  const std::vector<rematch_storage::CompressedFrameEntry> frames = {
      {.logical_start = 0, .logical_len = 3, .encoded = {10, 11, 12}},
      {.logical_start = 3, .logical_len = 3, .encoded = {13, 14, 15}},
  };
  const auto span_bytes = rematch_storage::ReadLogicalFrameSpan(
      frames, 2, 3, [](const std::vector<uint8_t>& b) { return b; });
  EXPECT_EQ(span_bytes, (std::vector<uint8_t>{12, 13, 14}));

  std::vector<uint8_t> pref_wire;
  rematch_storage::EncodeCommonPrefixResidual(
      "https://example.com/a/b", "https://example.com/a/xyz", &pref_wire);
  size_t pref_pos = 0;
  std::string orig_out;
  ASSERT_TRUE(rematch_storage::DecodeCommonPrefixResidual(
      "https://example.com/a/b", pref_wire, &pref_pos, &orig_out));
  EXPECT_EQ(orig_out, "https://example.com/a/xyz");
}

// Lean proofs:
//   lean/TinylambOpt/ResearchGoals/QueryRewrites/RevenueAndShipments.lean
//   (Q1, Q3, Q6, Q7, Q8, Q10, Q19, Q20)
TEST(ColumnarTlcsFastPathTest, QueryRewritesTpchParity) {
  namespace qr = query_rewrites;
  // Q1: DateBoundBlockSubtotalReuse
  const std::vector<qr::DateValuePage> pages = {
      {.max_date = 100, .whole_page_nsum = 30, .rows = {{90, 10}, {100, 20}}},
      {.max_date = 120,
       .whole_page_nsum = 50,
       .rows = {{100, 15}, {105, std::nullopt}, {120, 35}}},
      {.max_date = 95,
       .whole_page_nsum = std::nullopt,
       .rows = {{95, std::nullopt}}},
  };
  EXPECT_EQ(qr::SumDateBoundBlockSubtotals(pages, 100),
            std::optional<int64_t>(45));
  EXPECT_EQ(qr::SumDateBoundBlockSubtotals(pages, 80), std::nullopt);

  // Q3: NonnegativeRevenueUpperBoundPruning
  const std::vector<int64_t> known = {100, 90, 80, 70, 60, 50, 40, 30, 20, 10};
  EXPECT_TRUE(qr::CanPruneNonnegativeRevenueOrder(known, 9));
  EXPECT_FALSE(qr::CanPruneNonnegativeRevenueOrder(known, 10));

  // Q6: DiscountGroupedRevenueSum
  const std::vector<std::pair<int64_t, int64_t>> disc_rows = {
      {5, 100}, {6, 200}, {5, 300}, {8, 999}, {4, 500}};
  EXPECT_EQ(qr::DiscountGroupedRevenueSum(disc_rows, 5, 7),
            (5 * 100) + (6 * 200) + (5 * 300));

  // Q7: DirectionalNationKeyPrefilter
  const std::vector<std::pair<uint64_t, bool>> orders = {
      {1, false}, {1, false}, {2, true}, {3, false}};
  const std::vector<std::pair<uint64_t, bool>> lines = {
      {1, true}, {2, true}, {3, false}, {1, true}};
  EXPECT_EQ(qr::CountDirectionalNationJoinPairs(orders, lines, true), 4U);

  // Q8: SupplierYearVolumePreaggregation
  const std::vector<std::pair<uint64_t, int64_t>> sales = {
      {1, 10}, {2, 20}, {1, 30}, {3, 40}};
  EXPECT_EQ(qr::SumSupplierYearVolumeByPredicate(
                sales, [](uint64_t s) { return s == 1 || s == 3; }),
            80);

  // Q10: TopCustomersBeforeWideAttributeLookup
  const std::vector<uint64_t> cids = {30, 10, 20, 40};
  const auto top2 = qr::TopCustomersThenAttachAttrs(
      cids, [](uint64_t id) { return id; },
      [](uint64_t id) { return "addr_" + std::to_string(id); }, 2U);
  ASSERT_EQ(top2.size(), 2U);
  EXPECT_EQ(top2[0], (std::pair<uint64_t, std::string>{10U, "addr_10"}));
  EXPECT_EQ(top2[1], (std::pair<uint64_t, std::string>{20U, "addr_20"}));

  // Q19: CommonShipmentPredicateFactoring
  EXPECT_TRUE(qr::MatchesFactoredAirShipmentBand(
      {.is_air = true, .brand_tag = 0, .quantity = 10}));
  EXPECT_FALSE(qr::MatchesFactoredAirShipmentBand(
      {.is_air = false, .brand_tag = 0, .quantity = 10}));
  EXPECT_TRUE(qr::MatchesFactoredAirShipmentBand(
      {.is_air = true, .brand_tag = 2, .quantity = 39}));
  EXPECT_FALSE(qr::MatchesFactoredAirShipmentBand(
      {.is_air = true, .brand_tag = 3, .quantity = 10}));

  // Q20: CandidateRestrictedNullableSupplierQuantity
  const std::vector<qr::PartSuppKey> cands = {{1, 10}, {2, 20}, {1, 10}};
  const std::vector<std::pair<qr::PartSuppKey, std::optional<int64_t>>>
      ps_lines = {{{1, 10}, 7},
                  {{1, 10}, std::nullopt},
                  {{1, 10}, 5},
                  {{2, 20}, std::nullopt},
                  {{9, 99}, 100}};
  const auto ps_res = qr::CandidateRestrictedNullableSum(cands, ps_lines);
  ASSERT_TRUE(ps_res.contains(qr::PartSuppKey{1, 10}));
  EXPECT_EQ(ps_res.at(qr::PartSuppKey{1, 10}), std::optional<int64_t>(12));
  EXPECT_FALSE(ps_res.contains(qr::PartSuppKey{2, 20}));
  EXPECT_FALSE(ps_res.contains(qr::PartSuppKey{9, 99}));
}

// Lean proofs:
//   lean/TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean
//   (37 TPC-DS proofs: Q2..Q99)
TEST(ColumnarTlcsFastPathTest, QueryRewritesTpcdsParity) {
  namespace qr = query_rewrites;
  // Q2: DateKeyPreaggregationBeforeWeekLookup
  const std::vector<std::pair<uint64_t, std::optional<int64_t>>> q2_sales = {
      {1, 10}, {1, 20}, {2, 5}, {3, std::nullopt}, {4, 7}};
  EXPECT_EQ(qr::PreaggregateDateKeysThenRollupCell(
                q2_sales,
                [](uint64_t d) {
                  return d == 1 || d == 4 ? std::pair<uint64_t, uint64_t>{1, 2}
                                          : std::pair<uint64_t, uint64_t>{9, 9};
                },
                {1, 2}),
            std::optional<int64_t>(37));

  // Q4: PositivePriorYearDenominatorFilter
  const std::unordered_map<uint64_t, int64_t> prior = {
      {1, 50}, {2, 0}, {3, -5}};
  const std::vector<std::pair<uint64_t, std::optional<int64_t>>> yr2 = {
      {1, 100}, {2, 200}, {3, 300}};
  const auto q4_kept = qr::FilterSalesByPositivePriorYear(prior, yr2);
  ASSERT_EQ(q4_kept.size(), 1U);
  EXPECT_EQ(q4_kept[0].first, 1U);

  // Q11: PositiveDenominatorRatioCrossProduct
  EXPECT_TRUE(qr::ComparePositiveDenominatorRatios(3, 2, 4, 3));
  EXPECT_FALSE(qr::ComparePositiveDenominatorRatios(4, 3, 3, 2));

  // Q14: HavingFilteredNullableRollup
  const std::vector<std::vector<std::optional<int64_t>>> q14_cells = {
      {10, 20}, {5, std::nullopt}, {std::nullopt}};
  EXPECT_EQ(qr::RollupHavingFilteredNullableGroups(q14_cells, 10),
            std::optional<int64_t>(30));

  // Q15: PostalPrefixDictionaryFilter
  const std::vector<std::string> zips = {"85669-1234", "10001", "86197-0001"};
  const std::vector<std::string> allowed = {"85669", "86197"};
  const std::vector<uint32_t> codes = {0, 1, 2, 0};
  EXPECT_EQ(qr::FilterCodesByPostalPrefixDict(zips, allowed, codes),
            (std::vector<uint32_t>{0, 2, 0}));

  // Q16: CandidateRestrictedNullAwareReturnAntijoin
  const std::vector<std::optional<uint64_t>> q16_ord = {1, std::nullopt, 2, 3};
  const std::vector<std::optional<uint64_t>> q16_ret = {2, std::nullopt, 99};
  const auto q16_out =
      qr::CandidateRestrictedNullAwareAntijoin(q16_ord, q16_ret);
  ASSERT_EQ(q16_out.size(), 3U);
  EXPECT_EQ(q16_out[0], std::optional<uint64_t>(1));
  EXPECT_FALSE(q16_out[1].has_value());
  EXPECT_EQ(q16_out[2], std::optional<uint64_t>(3));

  // Q17: ShiftedVarianceNumerator
  const std::vector<int64_t> q17_vals = {1001, 1003, 1007, 1005};
  EXPECT_EQ(qr::ShiftedVarianceNumerator(q17_vals, 1004),
            qr::ShiftedVarianceNumerator(q17_vals, 0));

  // Q21: EligibleItemRangeBlockPruning
  const std::vector<std::vector<uint64_t>> inv_blocks = {
      {1, 2, 3}, {10, 15, 20}, {100, 105}};
  const std::vector<uint64_t> cheap = {15, 105};
  EXPECT_EQ(qr::PruneBlocksByEligibleItemRanges(inv_blocks, cheap),
            (std::vector<uint64_t>{15, 105}));

  // Q25: SalesKeyFilterOnReturnJoin
  const std::vector<qr::CustItemTicketKey> q25_s = {{1, 10, 100}, {1, 10, 100}};
  const std::vector<qr::CustItemTicketKey> q25_r = {{1, 10, 100}, {2, 20, 200}};
  EXPECT_EQ(
      qr::CountSalesFilteredReturnCatalogJoin(
          q25_s, q25_r,
          [](uint64_t c, uint64_t i) { return c == 1 && i == 10 ? 3U : 9U; }),
      6U);

  // Q29: CachedCatalogQuantityByCustomerItem
  const std::vector<std::pair<uint64_t, uint64_t>> q29_t = {
      {1, 2}, {1, 2}, {3, 4}};
  const std::vector<std::optional<int64_t>> cat_12 = {5, std::nullopt, 7};
  const std::vector<std::optional<int64_t>> cat_34 = {std::nullopt};
  EXPECT_EQ(qr::SumCachedCatalogQuantityByPair(
                q29_t,
                [&](std::pair<uint64_t, uint64_t> p) {
                  return p.first == 1
                             ? std::span<const std::optional<int64_t>>(cat_12)
                             : std::span<const std::optional<int64_t>>(cat_34);
                }),
            std::optional<int64_t>(24));

  // Q30: AverageRatioComparisonByCrossProduct
  EXPECT_TRUE(qr::ExceedsOnePointTwoStateAverage(13, 20, 2));
  EXPECT_FALSE(qr::ExceedsOnePointTwoStateAverage(12, 20, 2));

  // Q35: StoreCustomerRestrictedExistenceMarks
  const std::vector<uint64_t> c35 = {1, 2, 3, 4};
  const std::vector<uint64_t> s35 = {1, 2, 3};
  const std::vector<uint64_t> w35 = {2, 9};
  const std::vector<uint64_t> k35 = {3, 10};
  EXPECT_EQ(qr::FilterStoreCustomersWithRemoteMarks(c35, s35, w35, k35),
            (std::vector<uint64_t>{2, 3}));

  // Q38: NameCodeIntersectionCardinality
  const std::vector<std::pair<std::string, uint64_t>> s38 = {{"a", 1},
                                                             {"b", 2}};
  const std::vector<std::pair<std::string, uint64_t>> c38 = {{"a", 1},
                                                             {"c", 3}};
  const std::vector<std::pair<std::string, uint64_t>> w38 = {{"a", 1},
                                                             {"b", 2}};
  EXPECT_EQ(
      qr::CountNameCodeThreeWayIntersection(
          s38, c38, w38,
          [](const std::string& s) { return static_cast<uint64_t>(s[0]); }),
      1U);

  // Q39: ConstantGroupVarianceRejection
  EXPECT_TRUE(qr::IsConstantGroupZeroVariance(42, 42));
  EXPECT_FALSE(qr::IsConstantGroupZeroVariance(41, 42));

  // Q40: OuterJoinBuildRestrictedToSalesKeys
  const std::vector<std::pair<uint64_t, uint64_t>> sold40 = {{1, 1}, {2, 2}};
  const std::vector<std::pair<std::pair<uint64_t, uint64_t>, int64_t>> ret40 = {
      {{1, 1}, 50}, {{9, 9}, 999}};
  const auto q40_out = qr::RestrictedLeftJoinReturns(sold40, ret40);
  ASSERT_EQ(q40_out.size(), 2U);
  EXPECT_EQ(q40_out[0], std::optional<int64_t>(50));
  EXPECT_FALSE(q40_out[1].has_value());

  // Q41: CorrelatedPositiveCountAsMembership
  const std::vector<std::optional<std::string>> ok41 = {"m1", std::nullopt,
                                                        "m2"};
  const std::vector<std::pair<std::optional<std::string>, bool>> it41 = {
      {"m1", true}, {"m2", false}, {std::nullopt, true}};
  const auto q41_out = qr::FilterByQualifiedMakerMembership(ok41, it41);
  ASSERT_EQ(q41_out.size(), 1U);
  EXPECT_EQ(q41_out[0], std::optional<std::string>("m1"));

  // Q45: ExpandPhysicalKeysByLogicalItem (multi-version history!)
  const std::vector<std::pair<uint64_t, std::optional<std::string>>> ver45 = {
      {2, "ITEM_A"}, {102, "ITEM_A"}, {3, "ITEM_B"}, {4, std::nullopt}};
  const std::vector<uint64_t> seed45 = {2};
  const auto exp45 = qr::ExpandItemHistoryKeysByLogicalItem(ver45, seed45);
  EXPECT_TRUE(exp45.contains(2U));
  EXPECT_TRUE(exp45.contains(102U));
  EXPECT_FALSE(exp45.contains(3U));

  // Q49: NullRejectingRefundOuterJoinReduction
  const std::vector<uint64_t> sold49 = {1, 2};
  const std::vector<std::optional<int64_t>> r49_1 = {15000, 5000, std::nullopt};
  const std::vector<std::optional<int64_t>> r49_2 = {};
  EXPECT_EQ(qr::ReduceNullRejectingRefundOuterJoin(
                sold49,
                [&](uint64_t k) {
                  return k == 1
                             ? std::span<const std::optional<int64_t>>(r49_1)
                             : std::span<const std::optional<int64_t>>(r49_2);
                }),
            (std::vector<int64_t>{15000}));

  // Q57: YearPartitionAverageBeforeNeighborLookup
  const std::vector<qr::YearMonthVal> m57 = {
      {1999, 12, 10}, {2000, 1, 20}, {2000, 2, std::nullopt}, {2001, 1, 30}};
  EXPECT_EQ(qr::ExtractTargetYearFromExtendedMonths(m57, 2000),
            (std::vector<int64_t>{20}));

  // Q58: PairwiseRatioBoundsFromExtrema
  EXPECT_TRUE(qr::ThreeChannelPairwiseWithinTenPercent(100, 105, 110));
  EXPECT_FALSE(qr::ThreeChannelPairwiseWithinTenPercent(100, 105, 112));

  // Q59: WeekPairMultiplicityProduct
  EXPECT_EQ(qr::WeekPairMultiplicity(2, 3, 5), 30U);

  // Q62: WarehousePrefixNullableSubtotalMerge
  const std::vector<std::pair<std::string, uint32_t>> wh62 = {
      {"12345678901234567890_A", 1},
      {"12345678901234567890_B", 1},
      {"12345678901234567890_A", 2}};
  EXPECT_EQ(qr::MergeWarehousePrefixBinCounts(wh62, "12345678901234567890", 1),
            2U);

  // Q63: OverlappingItemPredicateUnion
  const std::vector<uint64_t> f63 = {1, 2, 3, 2};
  const std::vector<uint64_t> a63 = {1, 2};
  const std::vector<uint64_t> b63 = {2, 3};
  EXPECT_EQ(qr::SumFactsMatchingItemUnion(
                f63, a63, b63,
                [](uint64_t k) { return static_cast<int64_t>(k * 10); }),
            80);

  // Q66: FunctionalWarehouseAttributesAfterMonthlySum
  const std::vector<std::pair<std::pair<uint64_t, uint32_t>, int64_t>> f66 = {
      {{1, 5}, 10}, {{1, 5}, 25}, {{1, 6}, 99}};
  const auto res66 = qr::SumMonthlyThenAttachWarehouseAttr(
      f66, 1, 5, [](uint64_t w) { return "wh_" + std::to_string(w); });
  EXPECT_EQ(res66, (std::pair<std::string, int64_t>{"wh_1", 35}));

  // Q72: ReturnMultiplicityAndPromotionCounts
  const auto p72_zero = qr::SynthesizeReturnPromoCounts(0, 3);
  EXPECT_EQ(p72_zero.no_promo_count, 3U);
  EXPECT_EQ(p72_zero.promo_count, 0U);
  EXPECT_EQ(p72_zero.total_count, 3U);
  const auto p72_pos = qr::SynthesizeReturnPromoCounts(2, 0);
  EXPECT_EQ(p72_pos.no_promo_count, 0U);
  EXPECT_EQ(p72_pos.promo_count, 2U);
  EXPECT_EQ(p72_pos.total_count, 2U);

  // Q74: OrderedTopKAfterPriorYearRatioFilter
  const std::vector<uint64_t> o74 = {1, 2, 3, 4, 5, 6};
  EXPECT_EQ(qr::OrderedPrefixTopKFilter(
                o74, [](uint64_t x) { return x % 2 == 0; }, 2U),
            (std::vector<uint64_t>{2, 4}));

  // Q75: ChannelLocalDedupBeforeUnionDistinct
  const std::vector<std::vector<qr::ChannelRowTuple>> ch75 = {
      {{1, 10, 100}, {1, 10, 100}}, {{1, 10, 100}, {1, 5, 50}}};
  EXPECT_EQ(qr::ChannelLocalDedupUnionQuantity(ch75, 1), 15);

  // Q79: AddressKeyElisionByFunctionalDependency
  const std::vector<std::pair<uint64_t, std::optional<int64_t>>> r79 = {
      {7, 10}, {7, std::nullopt}, {7, 20}};
  EXPECT_EQ(qr::ElideFunctionalAddressKeySum(r79),
            (std::vector<std::optional<int64_t>>{30}));
  EXPECT_TRUE(qr::ElideFunctionalAddressKeySum({}).empty());

  // Q80: NullableTotalFromChannelSubtotals
  const std::vector<std::vector<std::vector<std::optional<int64_t>>>> ch80 = {
      {{10, std::nullopt}, {std::nullopt}}, {{20, 5}}};
  EXPECT_EQ(qr::MergeNullableChannelSubtotals(ch80),
            std::optional<int64_t>(35));
  const std::vector<std::vector<std::vector<std::optional<int64_t>>>>
      ch80_null = {{{std::nullopt}}};
  EXPECT_FALSE(qr::MergeNullableChannelSubtotals(ch80_null).has_value());

  // Q81: StateAverageFromCustomerTotals
  const std::vector<qr::StateCustomerTotal> t81 = {
      {1, 100, 50}, {1, 101, 70}, {2, 102, 999}};
  const auto c81 = qr::BuildStateTotalsCacheForCustomers(
      t81, [](uint64_t c) { return c == 100; });
  ASSERT_TRUE(c81.contains(1U));
  EXPECT_EQ(c81.at(1U), (std::pair<int64_t, uint64_t>{120, 2U}));
  EXPECT_FALSE(c81.contains(2U));

  // Q83: CommonItemTopKBeforeQuantityLookup
  const std::vector<uint64_t> sr83 = {5, 1, 3, 9};
  const std::vector<uint64_t> cr83 = {3, 1, 9};
  const std::vector<uint64_t> wr83 = {9, 3, 1, 2};
  const auto w83 = qr::PrefixCommonThreeChannelTopK(
      sr83, cr83, wr83, 5,
      [](uint64_t k) {
        return std::array<int64_t, 3>{static_cast<int64_t>(k),
                                      static_cast<int64_t>(k * 2),
                                      static_cast<int64_t>(k * 3)};
      },
      2U);
  ASSERT_EQ(w83.size(), 2U);
  EXPECT_EQ(w83[0].first, 1U);
  EXPECT_EQ(w83[1].first, 3U);

  // Q85: EqualNonNullDemographicCodes
  EXPECT_TRUE(qr::EqualNonNullCodeHit(5, 5));
  EXPECT_FALSE(qr::EqualNonNullCodeHit(std::nullopt, std::nullopt));

  // Q86: RankFromDistinctValueFrequencies
  const std::vector<std::pair<int64_t, uint64_t>> f86 = {
      {100, 2}, {80, 3}, {50, 1}};
  EXPECT_EQ(qr::RankFromDistinctFrequencies(f86, 80), 3U);

  // Q87: CandidateOnlyDifferenceMarks
  const std::vector<uint64_t> s87 = {1, 2, 2, 3, 4};
  const std::vector<uint64_t> c87 = {2, 9};
  const std::vector<uint64_t> w87 = {4};
  EXPECT_EQ(qr::CountStoreMinusRemoteCandidates(s87, c87, w87), 2U);

  // Q89: MonthlyDeviationBandPruning
  EXPECT_TRUE(qr::CanPruneBrandWithinTenPercentBand(12, 1200, 95, 105));
  EXPECT_FALSE(qr::CanPruneBrandWithinTenPercentBand(12, 1200, 85, 105));

  // Q90: WeightedTimeBandJoinCounts
  const std::vector<qr::TimeDemoPageKey> k90 = {
      {8, 1, 1}, {8, 1, 1}, {19, 1, 1}, {12, 1, 1}};
  const auto [am90, pm90] = qr::ComputeWeightedTimeBandCounts(
      k90, [](const qr::TimeDemoPageKey&) { return 1U; },
      [](uint64_t t) { return t; });
  EXPECT_EQ(am90, 2U);
  EXPECT_EQ(pm90, 1U);

  // Q99: RunLengthShippingDelayHistogram
  const std::vector<qr::DatePairRun> r99 = {
      {100, 120, 4}, {100, 145, 2}, {100, 250, 3}};
  const auto h99 = qr::RunLengthShippingDelayHistogram(r99);
  EXPECT_EQ(h99, (std::array<uint64_t, 5>{4, 2, 0, 0, 3}));
}

// Lean proofs:
//   lean/TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean
//   (20 ClickBench proofs: Q12..Q43)
TEST(ColumnarTlcsFastPathTest, QueryRewritesClickBenchParity) {
  namespace qr = query_rewrites;
  // Q12: PartitionedUserBitmapDistinct
  const std::vector<uint64_t> u12 = {1, 2, 65, 1, 130, 65, 2};
  EXPECT_EQ(qr::CountPartitionedUserBitmapDistinct(u12, 64U), 4U);
  EXPECT_EQ(qr::CountPartitionedUserBitmapDistinct(u12, 16U), 4U);

  // Q14: DistinctTopKPruningByRemainingRows
  const std::vector<uint64_t> fin14 = {50, 45, 40, 35, 30, 25, 20, 18, 15, 12};
  EXPECT_TRUE(qr::CanPruneDistinctGroupByRemainingRows(5, 6, fin14, 10U));
  EXPECT_FALSE(qr::CanPruneDistinctGroupByRemainingRows(5, 7, fin14, 10U));

  // Q16: CandidateTopKWithOmittedCountBound
  const std::unordered_map<uint64_t, uint64_t> c16 = {
      {1, 50}, {2, 40}, {3, 30}};
  const auto w16 = qr::ResolveCandidateTopKWithOmittedBound(c16, 30, 2U);
  ASSERT_TRUE(w16.has_value());
  EXPECT_EQ(w16.value_or(std::vector<uint64_t>{}),
            (std::vector<uint64_t>{1, 2}));
  EXPECT_FALSE(
      qr::ResolveCandidateTopKWithOmittedBound(c16, 45, 2U).has_value());

  // Q18: RunLengthGroupCounts
  const std::vector<qr::PairRun> r18 = {{1, 2, 5}, {3, 4, 2}, {1, 2, 3}};
  EXPECT_EQ(qr::RunLengthPairGroupCount(r18, 1, 2), 8U);

  // Q20: SortedEqualRangeCount
  const std::vector<int64_t> s20 = {1, 2, 2, 2, 5, 9};
  EXPECT_EQ(qr::SortedEqualRangeCount(s20, 2), 3U);
  EXPECT_EQ(qr::SortedEqualRangeCount(s20, 4), 0U);

  // Q21: ChunkedSubstringBoundaryCarry
  EXPECT_TRUE(qr::ChunkedContainsWithBoundaryCarry("http://www.goo", "gle.com/",
                                                   "google"));
  EXPECT_FALSE(qr::ChunkedContainsWithBoundaryCarry("http://www.go", "le.com/",
                                                    "google"));

  // Q22: CountTopKBeforePayloadMinimum
  const std::vector<std::pair<uint64_t, std::string>> m22 = {
      {1, "http://z"}, {1, "http://a"}, {2, "http://b"}};
  EXPECT_EQ(qr::FilteredCountCutoffMinPayload(m22, 1, 2),
            std::optional<std::string>("http://a"));
  EXPECT_FALSE(qr::FilteredCountCutoffMinPayload(m22, 2, 2).has_value());

  // Q23: DictionaryPairPredicateMemoization
  const std::vector<std::string> titles = {"Google News", "Other"};
  const std::vector<std::string> urls = {"http://ex.com",
                                         "http://a.google.com"};
  const std::vector<std::pair<uint32_t, uint32_t>> p23 = {
      {0, 0}, {0, 1}, {1, 0}, {0, 0}};
  const auto f23 = qr::FilterMemoizedTitleUrlCodePairs(
      p23, [&](uint32_t i) -> std::string_view { return titles[i]; },
      [&](uint32_t i) -> std::string_view { return urls[i]; });
  EXPECT_EQ(f23.size(), 2U);

  // Q24: TimestampFenceBeforeSubstringRead
  const std::vector<uint64_t> ts24 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  EXPECT_TRUE(qr::CanSkipPageByTimestampFence(ts24, 10, 11, 10U));
  EXPECT_FALSE(qr::CanSkipPageByTimestampFence(ts24, 10, 10, 10U));

  // Q27: TimestampTieBoundaryForSecondarySort
  const std::vector<std::pair<uint64_t, uint64_t>> r27 = {
      {1, 9}, {2, 5}, {2, 1}, {2, 3}, {3, 0}};
  const auto top27 = qr::TopKWithinTimestampTieBoundary(r27, 2, 3U);
  EXPECT_EQ(top27, (std::vector<std::pair<uint64_t, uint64_t>>{
                       {1, 9}, {2, 1}, {2, 3}}));

  // Q28: TotalFrequencyBoundsNonemptyHaving
  EXPECT_FALSE(qr::CanPassNonemptyHavingByTotalFreq(100000U));
  EXPECT_TRUE(qr::CanPassNonemptyHavingByTotalFreq(100001U));

  // Q31: SplitIpKeyReconstruction
  const auto sk31 = qr::SplitEngineIpKey(7, 0x0A0B0C0DULL);
  EXPECT_EQ(qr::ReconstructEngineIpKey(sk31),
            (std::pair<uint64_t, uint64_t>{7U, 0x0A0B0C0DULL}));

  // Q32: SingleValueGroupWithConflictFallback
  qr::SingleValueWithExceptions sv32;
  sv32.Insert(100);
  sv32.Insert(100);
  sv32.Insert(200);
  sv32.Insert(100);
  EXPECT_EQ(sv32.CountOf(100), 3U);
  EXPECT_EQ(sv32.CountOf(200), 1U);
  EXPECT_EQ(sv32.CountOf(300), 0U);

  // Q36: DerivedIpKeysAfterGrouping
  const std::vector<int64_t> ips36 = {10, 20, 10};
  const auto d36 = qr::AttachDerivedIpKeysAfterGrouping(ips36);
  ASSERT_EQ(d36.size(), 2U);
  EXPECT_EQ(d36[0], (qr::DerivedIpGroupRow{10, 9, 8, 7, 2}));
  EXPECT_EQ(d36[1], (qr::DerivedIpGroupRow{20, 19, 18, 17, 1}));

  // Q37: IntervalCountFromSortedPostings
  const std::vector<uint64_t> d37 = {1, 5, 10, 15, 20, 25, 31};
  EXPECT_EQ(qr::SortedPostingsIntervalCount(d37, 10, 25), 4U);

  // Q38: PageFrequencyBoundsForTitleCandidates
  EXPECT_TRUE(qr::MustBeInTitleCandidatesByPageCap(5, 10, 51));
  EXPECT_FALSE(qr::MustBeInTitleCandidatesByPageCap(5, 10, 50));

  // Q39: OffsetSelectionByCountBuckets
  const std::vector<std::pair<uint64_t, uint64_t>> g39 = {
      {1, 100}, {2, 50}, {3, 50}, {4, 50}, {5, 10}};
  const auto w39 = qr::SelectOffsetWindowFromCountBuckets(g39, 50, 2U, 2U);
  ASSERT_TRUE(w39.has_value());
  EXPECT_EQ(w39.value_or(std::vector<std::pair<uint64_t, uint64_t>>{}),
            (std::vector<std::pair<uint64_t, uint64_t>>{{3, 50}, {4, 50}}));

  // Q40: DisjointReferrerBranchKeys
  const std::vector<qr::ReferrerBranchRow> r40 = {
      {0, 0, "refA"}, {0, 0, "refA"}, {1, 0, "ignored"}, {1, 0, "other"}};
  EXPECT_EQ(qr::CountDisjointReferrerBranchKey(r40, 0, 0, "refA"), 2U);
  EXPECT_EQ(qr::CountDisjointReferrerBranchKey(r40, 1, 0, ""), 2U);
  EXPECT_EQ(qr::CountDisjointReferrerBranchKey(r40, 1, 0, "ignored"), 0U);

  // Q42: RowCountBoundsDistinctOffset
  EXPECT_TRUE(qr::RowCountForcesEmptyDistinctOffset(10000U, 10000U));
  EXPECT_FALSE(qr::RowCountForcesEmptyDistinctOffset(10001U, 10000U));

  // Q43: MinuteTruncationFromBlockBounds
  EXPECT_EQ(qr::TryConstantBlockMinuteTruncation(120, 179),
            std::optional<int64_t>(2));
  EXPECT_FALSE(qr::TryConstantBlockMinuteTruncation(120, 180).has_value());
  EXPECT_EQ(qr::TryConstantBlockMinuteTruncation(-60, -1),
            std::optional<int64_t>(-1));
}

// Lean proofs:
//   lean/TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean (16)
//   lean/TinylambOpt/ResearchGoals/QueryRewrites/FileRangesAndExtrema.lean (2)
TEST(ColumnarTlcsFastPathTest, QueryRewritesAnalyticsAndCloudParity) {
  namespace qr = query_rewrites;
  // JOB J1: CompanyWeightedMovieYearTotals
  const std::vector<std::pair<uint64_t, int64_t>> my_j1 = {
      {1, 1995}, {2, 2001}, {3, 2010}};
  const std::vector<uint64_t> comp_j1 = {1, 1, 3};
  EXPECT_EQ(qr::CompanyWeightedMovieYearTotals(my_j1, comp_j1),
            (qr::CountAndSum{.count = 3, .sum = (2 * 1995) + 2010}));

  // JOB J2: RatingAndGenreJoinMultiplicity
  const std::vector<uint64_t> t_j2 = {1, 2, 1};
  const std::vector<uint64_t> d_j2 = {1, 1, 2};
  const std::vector<uint64_t> r_j2 = {1, 1, 1};
  EXPECT_EQ(qr::RatingAndGenreJoinMultiplicity(t_j2, d_j2, r_j2), 12U);

  // JOB J3: OptionalYearByteCodeTotals
  const std::vector<uint8_t> codes_j3 = {
      qr::EncodeOptionalYearByteCode(1970),
      qr::EncodeOptionalYearByteCode(std::nullopt),
      qr::EncodeOptionalYearByteCode(2010),
      qr::EncodeOptionalYearByteCode(1969)};
  EXPECT_EQ(codes_j3[0], 1U);
  EXPECT_EQ(codes_j3[1], 0U);
  EXPECT_EQ(codes_j3[2], 41U);
  EXPECT_EQ(codes_j3[3], 0U);
  EXPECT_EQ(qr::SumOptionalYearByteCodes(codes_j3),
            (qr::CountAndSum{.count = 2, .sum = 1970 + 2010}));

  // JOB J5: AliasPrefixHalfOpenRange
  EXPECT_TRUE(qr::MatchesAka1PrefixRange("aka-1"));
  EXPECT_TRUE(qr::MatchesAka1PrefixRange("aka-199"));
  EXPECT_FALSE(qr::MatchesAka1PrefixRange("aka-2"));
  EXPECT_FALSE(qr::MatchesAka1PrefixRange("aka-09"));

  // STATS-CEB C1: ScoreAndViewCountZonePruning
  const std::vector<std::vector<qr::PostZoneRow>> pages_c1 = {
      {{1, 500, 10}, {5, 100, 11}},
      {{10, 10000, 20}, {12, 20000, 21}},
      {{8, 9999, 30}, {4, 100, 31}, {9, 500, 32}},
  };
  EXPECT_EQ(qr::PrunePostPagesByScoreAndViews(pages_c1),
            (std::vector<uint64_t>{30, 32}));

  // STATS-CEB C2: OwnerCommentFrequencyJoinCount
  const std::vector<std::pair<uint64_t, uint64_t>> op_c2 = {
      {1, 10}, {1, 10}, {1, 20}, {2, 10}};
  const std::vector<uint64_t> pc_c2 = {10, 10, 10, 20};
  EXPECT_EQ(qr::OwnerCommentFrequencyJoinCount(op_c2, pc_c2, 1), 7U);

  // H2O H2: StableDescendingCountBuckets
  const std::vector<std::pair<uint64_t, uint64_t>> g_h2 = {
      {1, 2}, {2, 5}, {3, 2}, {4, 0}};
  EXPECT_EQ(qr::StableDescendingCountBucketSort(g_h2, 5),
            (std::vector<std::pair<uint64_t, uint64_t>>{
                {2, 5}, {1, 2}, {3, 2}, {4, 0}}));

  // H2O H4: OrderedNullableValuesFromSelectedRuns
  const std::vector<qr::KeyedNullableRun> runs_h4 = {
      {.key = 2, .values = {10, std::nullopt, 20}},
      {.key = 4, .values = {99}},
      {.key = 1, .values = {30}},
  };
  EXPECT_EQ(qr::OrderedNullableValuesFromSelectedRuns(runs_h4),
            (std::vector<int64_t>{10, 20, 30}));

  // H2O H5: BoundedCartesianProductTiles
  const std::vector<int64_t> xs_h5 = {1, 2};
  const std::vector<int64_t> ys_h5 = {10, 20, 30};
  const auto tiles_h5 = qr::BuildBoundedCartesianProductTiles(xs_h5, ys_h5, 4U);
  ASSERT_EQ(tiles_h5.size(), 2U);
  EXPECT_EQ(tiles_h5[0].size(), 4U);
  EXPECT_EQ(tiles_h5[1].size(), 2U);
  EXPECT_EQ(tiles_h5[0][0], (std::pair<int64_t, int64_t>{1, 10}));
  EXPECT_EQ(tiles_h5[1][1], (std::pair<int64_t, int64_t>{2, 30}));

  // LDBC SNB L2: ForumMembershipSemijoinWeights
  const std::vector<std::pair<uint64_t, uint64_t>> mem_l2 = {
      {1, 10}, {2, 20}, {1, 30}};
  const std::vector<uint64_t> for_l2 = {1, 1, 9};
  EXPECT_EQ(qr::ForumMembershipSemijoinWeights(
                mem_l2, for_l2, [](uint64_t p) { return p == 10 ? 3U : 1U; }),
            8U);

  // LDBC SNB L3: ParentReplyCountAndScoreTotals
  const std::vector<std::pair<std::optional<uint64_t>, int64_t>> ch_l3 = {
      {1, 5}, {1, 0}, {1, 7}, {std::nullopt, 100}, {2, 9}};
  const std::vector<uint64_t> par_l3 = {1, 1, 3};
  EXPECT_EQ(qr::ParentReplyCountAndScoreTotals(ch_l3, par_l3),
            (qr::CountAndSum{.count = 4, .sum = 24}));

  // LDBC SNB L4: NullableParentAndYearPositionGather
  const std::vector<qr::YearParentScoreRow> rows_l4 = {
      {2012, 10, 50},
      {2012, std::nullopt, 99},
      {2011, 10, 88},
      {2012, 20, std::nullopt}};
  const auto sc_l4 = qr::GatherScoresByYearAndValidParent(rows_l4, 2012U);
  ASSERT_EQ(sc_l4.size(), 2U);
  EXPECT_EQ(sc_l4[0], std::optional<int64_t>(50));
  EXPECT_FALSE(sc_l4[1].has_value());

  // LDBC SNB L5: PlaceGroupedCountryAgeTotals
  const std::vector<std::pair<uint64_t, std::optional<int64_t>>> ea_l5 = {
      {1, 20}, {1, std::nullopt}, {2, 30}, {3, 40}};
  EXPECT_EQ(qr::PlaceGroupedCountryAgeTotals(
                ea_l5, [](uint64_t p) { return p <= 2 ? 100U : 200U; }, 100U),
            (qr::PlaceAgeAggregate{
                .total_count = 3, .nullable_sum = 50, .non_null_count = 2}));

  // BigBench B3: PhysicalKeySubtotalRollup
  const std::vector<
      std::pair<std::pair<uint64_t, uint64_t>, std::optional<int64_t>>>
      s_b3 = {{{1, 1}, 10}, {{1, 1}, std::nullopt}, {{2, 1}, 25}, {{3, 2}, 99}};
  EXPECT_EQ(qr::RollupPhysicalKeySubtotals(
                s_b3,
                [](std::pair<uint64_t, uint64_t> pk) {
                  return pk.second == 1 ? std::pair<uint64_t, uint64_t>{7, 8}
                                        : std::pair<uint64_t, uint64_t>{9, 9};
                },
                {7, 8}),
            std::optional<int64_t>(35));

  // BigBench B4: DictionarySubstringOrFlagSelection
  const std::vector<std::pair<bool, std::string>> rows_b4 = {
      {true, "none"}, {false, "big_data_sql"}, {false, "other"}};
  EXPECT_EQ(qr::DictionarySubstringOrFlagSelection(rows_b4),
            (std::vector<size_t>{0, 1}));

  // BigBench B5: SortedPriceSuffixKeyCounts
  const std::vector<std::pair<int64_t, uint64_t>> it_b5 = {
      {50, 1}, {100, 1}, {101, 1}, {120, 2}, {150, 1}};
  EXPECT_EQ(qr::SortedPriceSuffixKeyCount(it_b5, 1, 100), 2U);

  // Public BI MLB Q108: PairedExtremaReduction
  const std::vector<std::optional<int64_t>> v_108 = {std::nullopt, 15, -3, 42,
                                                     std::nullopt, 7,  100};
  EXPECT_EQ(qr::PairedExtremaReduction(v_108),
            (qr::MinMaxPair{.min_val = -3, .max_val = 100}));
  const std::vector<std::optional<int64_t>> v_empty = {std::nullopt};
  EXPECT_EQ(qr::PairedExtremaReduction(v_empty),
            (qr::MinMaxPair{.min_val = std::nullopt, .max_val = std::nullopt}));

  // Iceberg SQL base: ContiguousFileRowIdTotals
  // 96 files x 48 contiguous rows (matching fixture in november26-queries.md)
  std::vector<qr::ContiguousFileRun> runs_ice;
  std::vector<uint64_t> manifest_ice;
  for (uint64_t f = 0; f < 96; ++f) {
    runs_ice.push_back({.file_id = f, .base_rid = f * 48U, .count = 48U});
    manifest_ice.push_back(f);
  }
  const auto tot_ice = qr::ContiguousFileRowIdTotals(runs_ice, manifest_ice);
  EXPECT_EQ(tot_ice.row_count, 4608U);
  EXPECT_EQ(tot_ice.rid_sum, std::optional<uint64_t>(10614528ULL));
  const std::vector<uint64_t> empty_manifest;
  const auto empty_ice =
      qr::ContiguousFileRowIdTotals(runs_ice, empty_manifest);
  EXPECT_EQ(empty_ice.row_count, 0U);
  EXPECT_FALSE(empty_ice.rid_sum.has_value());
}

TEST(ColumnarTlcsFastPathTest, AnalyticalOperatorsProofKernelsParity) {
  namespace bm = tinylamb::benchmark_rematch;

  // IncrementalRefreshAndFlush: InsertSideState, ChangedSymbols, OverlapReplay,
  // LaneOrSpill, VisibleFlush, DirtyFlush.
  bm::BatchSymGroups groups;
  const std::vector<bm::BatchTick> old_ticks = {
      {.batch = 1, .sym = 10, .bid = 100},
      {.batch = 1, .sym = 10, .bid = std::nullopt},
      {.batch = 1, .sym = 20, .bid = 50},
  };
  const std::vector<bm::BatchTick> fresh_ticks = {
      {.batch = 1, .sym = 10, .bid = 25},
      {.batch = 2, .sym = 10, .bid = 70},
  };
  bm::ApplyInsertSideState(&groups, old_ticks);
  const auto dirty = bm::CollectDirtyKeysAndUpdate(&groups, fresh_ticks);
  EXPECT_EQ(dirty.size(), 2U);
  EXPECT_EQ(groups[(bm::BatchSymKey{.batch = 1, .sym = 10})],
            (bm::NullableTotals{.cnt = 3, .nn = 2, .sum = 125}));

  const auto replaced = bm::ReplaceSymbolState({.cnt = 3, .nn = 2, .sum = 125},
                                               {.cnt = 2, .nn = 1, .sum = 100},
                                               {.cnt = 1, .nn = 1, .sum = 40});
  if (!replaced.has_value()) {
    ADD_FAILURE() << "expected replaced to have a value";
    return;
  }
  EXPECT_EQ(*replaced, (bm::NullableTotals{.cnt = 2, .nn = 2, .sum = 65}));
  EXPECT_FALSE(bm::ReplaceSymbolState({.cnt = 1, .nn = 1, .sum = 10},
                                      {.cnt = 1, .nn = 1, .sum = 10}, {})
                   .has_value());

  bm::BatchSymGroups replay_groups;
  const std::vector<bm::BatchTick> stream_ticks = {
      {.batch = 1, .sym = 5, .bid = 10},
      {.batch = 1, .sym = 5, .bid = 20},
      {.batch = 1, .sym = 5, .bid = 30},
  };
  bm::ApplyInsertSideState(
      &replay_groups,
      std::span<const bm::BatchTick>(stream_ticks).subspan(0, 2));
  bm::FoldOverlapReplaySuffix(&replay_groups, stream_ticks, /*offset=*/0,
                              /*acknowledged=*/2);
  EXPECT_EQ(replay_groups[(bm::BatchSymKey{.batch = 1, .sym = 5})],
            (bm::NullableTotals{.cnt = 3, .nn = 3, .sum = 60}));

  bm::LaneOrSpillAccumulator lane_acc(/*base_sym=*/10, /*width=*/4);
  lane_acc.Add(11, 15);
  lane_acc.Add(11, std::nullopt);
  lane_acc.Add(99, 42);
  EXPECT_EQ(lane_acc.Lookup(11),
            (bm::NullableTotals{.cnt = 2, .nn = 1, .sum = 15}));
  EXPECT_EQ(lane_acc.Lookup(99),
            (bm::NullableTotals{.cnt = 1, .nn = 1, .sum = 42}));

  const std::vector<std::pair<uint64_t, std::vector<bm::BatchTick>>> chunks = {
      {1, {{.batch = 1, .sym = 7, .bid = 12}}},
      {3, {{.batch = 3, .sym = 7, .bid = 99}}},
  };
  EXPECT_EQ(bm::CoalesceVisibleBatches(chunks, /*pin=*/2, /*sym=*/7),
            (bm::NullableTotals{.cnt = 1, .nn = 1, .sum = 12}));

  // RevenueAggregatesAndJoinWitnesses: MarketShareWeights, YearRoutes,
  // DirectionBuckets, RetireWitness, RebucketOrders, RevenueRuns.
  const std::vector<std::pair<uint64_t, std::optional<int64_t>>> q8_sales = {
      {1, 100}, {1, std::nullopt}, {2, 50}};
  const std::vector<std::pair<uint64_t, std::optional<std::string>>> q8_supp = {
      {1, "BRAZIL"}, {1, "FRANCE"}, {2, "BRAZIL"}};
  const auto share =
      bm::ComputeMarketShareByWeights(q8_sales, q8_supp, "BRAZIL");
  EXPECT_EQ(share.numerator,
            (bm::NullableTotals{.cnt = 5, .nn = 4, .sum = 150}));
  EXPECT_EQ(share.denominator,
            (bm::NullableTotals{.cnt = 5, .nn = 3, .sum = 250}));

  const std::vector<bm::OrderCustomerYear> q8_orders = {
      {.order_key = 10, .cust_key = 1, .year = 1995},
      {.order_key = 11, .cust_key = 2, .year = 1996},
  };
  const std::vector<uint64_t> q8_custs = {1, 1};
  const std::vector<std::pair<uint64_t, std::optional<int64_t>>> q8_lines = {
      {10, 30}, {11, 50}};
  EXPECT_EQ(bm::EvalYearRouteTotals(q8_orders, q8_custs, q8_lines, 1995),
            (bm::NullableTotals{.cnt = 2, .nn = 2, .sum = 60}));

  const std::vector<bm::NationShipment> q7_rows = {
      {.supp_nation = "FRANCE",
       .cust_nation = "GERMANY",
       .year = 1995,
       .volume = 40},
      {.supp_nation = "GERMANY",
       .cust_nation = "FRANCE",
       .year = 1995,
       .volume = 70},
      {.supp_nation = std::nullopt,
       .cust_nation = "GERMANY",
       .year = 1995,
       .volume = 999},
  };
  EXPECT_EQ(
      bm::SumDirectedNationBucket(q7_rows, "FRANCE", "GERMANY", true, 1995),
      (bm::NullableTotals{.cnt = 1, .nn = 1, .sum = 40}));
  EXPECT_EQ(
      bm::SumDirectedNationBucket(q7_rows, "FRANCE", "GERMANY", false, 1995),
      (bm::NullableTotals{.cnt = 1, .nn = 1, .sum = 70}));

  const std::vector<std::pair<uint64_t, uint64_t>> q4_orders = {
      {100, 1}, {101, 1}, {102, 2}};
  const std::vector<uint64_t> q4_late = {100, 100, 102};
  const auto q4_counts =
      bm::CountPriorityWithRetiredWitness(q4_orders, q4_late);
  EXPECT_EQ(q4_counts.at(1), 1U);
  EXPECT_EQ(q4_counts.at(2), 1U);

  const std::vector<uint64_t> q13_custs = {1, 2, 3};
  const std::vector<std::pair<uint64_t, bool>> q13_orders = {
      {1, false}, {1, true}, {2, false}, {2, false}};
  const auto q13_hist =
      bm::RebucketCustomerOrderHistogram(q13_custs, q13_orders);
  EXPECT_EQ(q13_hist.at(0), 1U);
  EXPECT_EQ(q13_hist.at(1), 1U);
  EXPECT_EQ(q13_hist.at(2), 1U);

  const std::vector<std::vector<std::pair<uint64_t, std::optional<int64_t>>>>
      q3_morsels = {{{1, 10}, {2, 20}}, {{2, 5}, {3, std::nullopt}}};
  const auto q3_runs = bm::MergeContiguousRevenueRuns(q3_morsels);
  ASSERT_EQ(q3_runs.size(), 3U);
  EXPECT_EQ(q3_runs[1].second,
            (bm::NullableTotals{.cnt = 2, .nn = 2, .sum = 25}));

  // SharedJoinStatesAndWeights: H5, C3, C2, C4, L1.
  const std::vector<bm::H5LeftRow> h5_l = {
      {.group_id = 1, .join_key = 7, .val = 10},
      {.group_id = 1, .join_key = 8, .val = std::nullopt}};
  const std::vector<bm::H5RightRow> h5_r = {{.join_key = 7, .val = 3},
                                            {.join_key = 7, .val = 5},
                                            {.join_key = 8, .val = 9}};
  const auto [h5_sx, h5_sy] = bm::EvalFactorizedH5Group(h5_l, h5_r, 1);
  EXPECT_EQ(h5_sx, (bm::NullableTotals{.cnt = 3, .nn = 2, .sum = 20}));
  EXPECT_EQ(h5_sy, (bm::NullableTotals{.cnt = 3, .nn = 3, .sum = 17}));

  const std::unordered_map<uint64_t, std::optional<int64_t>> c3_scores = {
      {1, 10}, {2, 20}};
  const std::unordered_set<uint64_t> c3_valid = {2};
  const std::vector<bm::PostLinkEdge> c3_edges = {
      {.post_id = 1, .related_id = 2, .link_type = 1},
      {.post_id = 1, .related_id = 2, .link_type = 1},
      {.post_id = 2, .related_id = 99, .link_type = 1}};
  EXPECT_EQ(bm::EvalEndpointWeightedLinkScore(c3_scores, c3_valid, c3_edges, 1),
            (bm::NullableTotals{.cnt = 2, .nn = 2, .sum = 20}));

  const std::vector<std::pair<uint64_t, uint64_t>> c2_posts = {
      {10, 100}, {10, 101}, {20, 102}};
  const std::vector<uint64_t> c2_comments = {100, 100, 101};
  EXPECT_EQ(bm::EvalCommentQuotientForOwner(c2_posts, c2_comments, 10, 2), 6U);

  const std::vector<std::optional<std::string>> c4_tags = {"sql", "sql",
                                                           std::nullopt, "db"};
  const auto c4_buckets = bm::BuildClassifiedStringBuckets(c4_tags);
  EXPECT_GE(c4_buckets.size(), 1U);
}

TEST(ColumnarTlcsFastPathTest, SocialGraphAndCloudBiProofKernelsParity) {
  namespace bm = tinylamb::benchmark_rematch;

  // SocialGraphJoinWeights & ForumRunsAndGraphDegrees (L1-L5).
  const std::vector<std::pair<uint64_t, uint64_t>> l1_edges = {
      {1, 10}, {1, 20}, {2, 10}, {3, 30}};
  const std::vector<uint64_t> l1_active = {1, 1, 2};
  const auto l1_res = bm::PullActiveFriendCountries(l1_edges, l1_active);
  EXPECT_EQ(l1_res.country_counts.at(10), 3U);
  EXPECT_EQ(l1_res.country_counts.at(20), 2U);
  EXPECT_EQ(l1_res.edge_reads, 3U);

  const std::vector<bm::ForumMessage> l2_msgs = {
      {.forum_id = 5, .year = 2012, .score = 10},
      {.forum_id = 5, .year = 2012, .score = 20},
      {.forum_id = 6, .year = 2012, .score = 7},
  };
  const auto l2_res = bm::FoldCarriedForumRuns(
      l2_msgs, [](uint64_t fid) -> uint64_t { return fid == 5 ? 3 : 0; });
  EXPECT_EQ(l2_res.member_probes, 2U);
  EXPECT_EQ(l2_res.groups.at({.forum_id = 5, .year = 2012}),
            (bm::NullableTotals{.cnt = 6, .nn = 6, .sum = 90}));

  const std::vector<bm::ReplyRow> l3_rows = {
      {.id = 2, .parent_id = 1, .year = 2013, .score = 15},  // forward ref
      {.id = 1, .parent_id = std::nullopt, .year = 2012, .score = 5},
      {.id = 3, .parent_id = 1, .year = 2013, .score = 25},  // backward ref
  };
  EXPECT_EQ(bm::SinglePassForwardReplyTotals(l3_rows, 2012),
            (bm::NullableTotals{.cnt = 2, .nn = 2, .sum = 40}));

  const std::unordered_map<uint64_t, uint64_t> l4_forums = {{10, 2}};
  const std::vector<std::pair<uint64_t, std::optional<int64_t>>> l4_msgs = {
      {10, 4}, {10, std::nullopt}, {11, 99}};
  const auto l4_res = bm::ApplyLateForumMembership(l4_forums, l4_msgs);
  EXPECT_EQ(l4_res.size(), 1U);
  EXPECT_EQ(l4_res.at(10), (bm::NullableTotals{.cnt = 4, .nn = 2, .sum = 8}));

  const std::vector<std::pair<uint64_t, uint64_t>> l5_edges = {
      {0, 1}, {0, 2}, {2, 3}};
  const auto csr = bm::BuildCsrOffsets(l5_edges, 4);
  EXPECT_EQ(bm::CsrOutDegree(csr, 0), 2U);
  EXPECT_EQ(bm::CsrOutDegree(csr, 1), 0U);
  EXPECT_EQ(bm::CsrOutDegree(csr, 2), 1U);

  // CloudBI DeleteFiles & Quotes.
  const bm::IcebergPart part{.spec_id = 1, .part_id = 0};
  const std::vector<bm::IcebergEqDelete> eq_dels = {
      {.scope = part, .seq = 5, .key = 10},
      {.scope = std::nullopt, .seq = 3, .key = 20},
      {.scope = part, .seq = 7, .key = std::nullopt},
  };
  const auto horizon = bm::BuildEqualityDeleteHorizon(eq_dels);
  const std::vector<bm::IcebergRow> ice_rows = {
      {.file = 1, .pos = 0, .part = part, .seq = 4, .key = 10},  // eq-deleted
      {.file = 1, .pos = 1, .part = part, .seq = 5, .key = 10},  // survives
      {.file = 1, .pos = 2, .part = part, .seq = 2, .key = 20},  // global eq
      {.file = 1,
       .pos = 3,
       .part = part,
       .seq = 6,
       .key = std::nullopt},  // null eq
  };
  const std::vector<bm::IcebergPosDelete> pos_dels = {
      {.file = 1, .pos = 1, .part = part, .seq = 5},
      {.file = 1, .pos = 1, .part = part, .seq = 6},   // duplicate
      {.file = 1, .pos = 99, .part = part, .seq = 6},  // out of range
  };
  EXPECT_EQ(bm::MetadataOnlyPosDeleteCount(4, 1, part, 5, pos_dels), 3U);
  EXPECT_EQ(bm::SurvivingRowsAfterUnionDeletes(ice_rows, pos_dels, horizon),
            0U);
  EXPECT_FALSE(
      bm::CanPruneEqDeleteByBounds(100, 200, /*file_has_null=*/true, eq_dels));
  EXPECT_TRUE(
      bm::CanPruneEqDeleteByBounds(100, 200, /*file_has_null=*/false, eq_dels));

  const std::vector<uint64_t> layout = {10, 20, 30};
  const std::vector<uint64_t> probe = {30, 99, 10};
  const auto field_offs = bm::ResolveFieldIdOffsets(layout, probe);
  EXPECT_EQ(field_offs[0], std::optional<size_t>(2));
  EXPECT_FALSE(field_offs[1].has_value());
  EXPECT_EQ(field_offs[2], std::optional<size_t>(0));

  const std::vector<size_t> gsizes = {10, 20, 30};
  const std::vector<uint8_t> gkeep = {0, 1, 1};
  EXPECT_EQ(bm::ComputeSurvivingGroupBaseOrdinals(gsizes, gkeep),
            (std::vector<uint64_t>{10, 30}));

  const std::vector<bm::QuoteTick> qticks = {
      {.batch = 1, .ts = 100, .tie = 1, .sym = 1, .bid = 50, .ask = 55},
      {.batch = 2, .ts = 100, .tie = 2, .sym = 1, .bid = 52, .ask = 58},
      {.batch = 2,
       .ts = 105,
       .tie = 0,
       .sym = 1,
       .bid = std::nullopt,
       .ask = 60},
  };
  const auto merged_q = bm::MergeStaleRollupWithDelta(
      {.cnt = 1, .nn = 1, .sum = 50}, qticks, 1, 90, 110, /*watermark=*/1,
      /*snapshot=*/2);
  EXPECT_EQ(merged_q, (bm::NullableTotals{.cnt = 3, .nn = 2, .sum = 102}));
  const auto close = bm::ReduceCloseRun(qticks);
  if (!close.has_value()) {
    ADD_FAILURE() << "expected close to have a value";
    return;
  }
  EXPECT_EQ(*close, (bm::CloseWinner{.ts = 100, .tie = 2, .price = 52}));
  const auto spread = bm::ComputeSpreadMoments(qticks);
  EXPECT_EQ(spread.count, 2U);
  EXPECT_EQ(spread.SpreadSum(), 11);
  EXPECT_TRUE(bm::ShouldUseMaintainedLayout(10, 100, 50, 20, 40));
}

TEST(ColumnarTlcsFastPathTest, ClickstreamExecProofKernelsParity) {
  namespace bm = tinylamb::benchmark_rematch;

  // ClockRange: interior delta decides without dependent decode.
  const auto interior = bm::ProbeDependentClockRange(
      10, 20, -1, 1, 15, {.kind = bm::ClockSlotKind::kDelta, .payload = 0});
  EXPECT_TRUE(interior.matched);
  EXPECT_FALSE(interior.decoded_dependent);
  const auto abs_hit = bm::ProbeDependentClockRange(
      10, 20, 0, 2, 1, {.kind = bm::ClockSlotKind::kAbsolute, .payload = 11});
  EXPECT_TRUE(abs_hit.matched);
  EXPECT_TRUE(abs_hit.decoded_dependent);

  // ConcatSearch: rejects cross-row matches and handles empty needle & NULLs.
  // Rows: ["a", NULL, "", "b", "abab"]
  const std::string concat =
      "ab"
      "abab";
  const std::vector<size_t> ends = {1, 1, 1, 2, 6};
  const std::vector<uint8_t> is_null = {0, 1, 0, 0, 0};
  EXPECT_EQ(bm::SearchConcatStreamRowIds(concat, ends, is_null, "ab"),
            (std::vector<size_t>{4}));
  EXPECT_EQ(bm::SearchConcatStreamRowIds(concat, ends, is_null, ""),
            (std::vector<size_t>{0, 2, 3, 4}));

  // AnchorDemand: minimal deduplicated frame set for copyBase tags.
  const std::vector<uint8_t> tags = {0, 1, 1};
  const std::vector<size_t> req = {2, 0, 2};
  EXPECT_EQ(
      bm::ComputeMinimalAnchorFrames(tags, req, [](size_t i) { return i / 2; }),
      (std::vector<size_t>{1}));

  // SparseMark: exact O(1) 2D lex granule pruning.
  EXPECT_EQ(bm::LeastKeyInRectAboveFence({3, 99}, 2, 7, 9),
            (bm::LexPairKey{4, 7}));
  EXPECT_EQ(bm::LeastKeyInRectAboveFence({3, 9}, 2, 7, 9),
            (bm::LexPairKey{3, 9}));
  EXPECT_TRUE(bm::GranuleIntersectsRect({3, 9}, {5, 0}, 2, 4, 7, 9));
  EXPECT_FALSE(bm::GranuleIntersectsRect({3, 99}, {3, 120}, 2, 4, 7, 9));

  // DefaultGroups: untouched rows credited to all-default key in O(1).
  const std::vector<bm::SparseDefaultLane> lanes = {
      {.default_val = std::nullopt, .exceptions = {{2, 0}}},
      {.default_val = 0, .exceptions = {{2, std::nullopt}, {4, 1}}},
  };
  const std::vector<std::optional<int64_t>> def_key = {std::nullopt, 0};
  const std::vector<std::optional<int64_t>> exc_key = {0, std::nullopt};
  EXPECT_EQ(bm::CountTupleWithDefaultShortCut(lanes, 6, def_key), 4U);
  EXPECT_EQ(bm::CountTupleWithDefaultShortCut(lanes, 6, exc_key), 1U);

  // LengthRollup: exact (byte_sum, nonempty_count) from prefix sums.
  // Rows: [NULL, "ab", "", "c"] -> lens=[0,2,0,1], present=[0,1,0,1]
  const std::vector<uint64_t> prefix_ends = {0, 0, 2, 2, 3};
  const std::vector<uint64_t> prefix_present = {0, 0, 1, 1, 2};
  const std::vector<std::pair<size_t, size_t>> slices = {{1, 2}, {3, 40}};
  EXPECT_EQ(bm::RollupNonEmptyLengthsFromPrefixSums(prefix_ends, prefix_present,
                                                    4, slices),
            (bm::LengthRollupTotals{.byte_sum = 3, .nonempty_count = 2}));
}

TEST(ColumnarTlcsFastPathTest, ClickstreamStorageProofKernelsParity) {
  namespace bm = tinylamb::benchmark_rematch;

  // RunSidecar: exact round-trip and minimum run count.
  const std::vector<uint32_t> perm = {0, 3, 1, 2};
  const auto enc = bm::EncodeRunSidecar(perm);
  EXPECT_EQ(enc.num_runs, 2U);
  EXPECT_EQ(enc.labels, (std::vector<uint32_t>{0, 1, 1, 0}));
  EXPECT_EQ(bm::RecoverRunSidecar(enc.labels, enc.num_runs), perm);

  // TieOrientation: reversing a tie group reduces front-coded byte cost.
  const std::vector<std::vector<std::string>> groups = {
      {"abc"}, {}, {"z", "abd"}};
  const size_t opt_bytes = bm::OptimalTieOrientationBytes(groups);
  const size_t naive_bytes = bm::FrontCodeTransitionBytes("", "abc") +
                             bm::FrontCodeTransitionBytes("abc", "z") +
                             bm::FrontCodeTransitionBytes("z", "abd");
  EXPECT_LT(opt_bytes, naive_bytes);

  // DictionaryAdmission: laminar tree DP matches Lean #guard fixture.
  const std::vector<bm::DictAdmissionNode> tree = {
      {.is_frame = false, .activation_fee = 6, .left = 1, .right = 2},
      {.is_frame = true, .frame_saved = 4},
      {.is_frame = true, .frame_saved = 4},
  };
  EXPECT_EQ(bm::BestDictionaryAdmissionSavings(tree, 0), 2);

  // HashExceptions: mode hash minimizes exceptions and preserves selection.
  const std::vector<std::pair<uint32_t, uint64_t>> rows = {
      {1, 7}, {1, 8}, {2, 7}, {1, 7}};
  const auto htbl = bm::BuildOptimalHashExceptionTable(rows);
  EXPECT_EQ(htbl.exceptions.size(), 1U);
  EXPECT_EQ(htbl.exceptions.at(1), 8ULL);
  const std::vector<uint32_t> codes = {1, 1, 2, 1};
  EXPECT_EQ(bm::SelectRowsByHashExceptions(
                codes, htbl, [](uint64_t h) { return h == 8ULL; }),
            (std::vector<size_t>{1}));

  // ClockWindow: anchor search finds optimal window base (62 beats 64).
  const std::vector<int64_t> deltas = {63, 64, 65};
  const auto cw = bm::ChooseOptimalClockWindow(deltas, /*bit_width=*/2,
                                               /*exception_bytes=*/10);
  EXPECT_LE(cw.low, 63);
  EXPECT_GE(cw.low, 62);

  // ClockForest: subset DP over 3 clocks matches Lean #guard (= 10).
  const size_t forest_cost = bm::OptimalClockForestBytes(
      3, [](size_t, std::optional<size_t> p) -> size_t {
        return p.has_value() ? 1U : 8U;
      });
  EXPECT_EQ(forest_cost, 10U);
}

TEST(ColumnarTlcsFastPathTest, EngineDecisionsProofKernelsParity) {
  namespace bm = tinylamb::benchmark_rematch;

  // Goal_RunOffset: skip runs covered by OFFSET and take LIMIT.
  const std::vector<std::vector<int>> runs = {{30, 10}, {40, 20, 50}, {60}};
  const auto page = bm::SkipRunsBeforeOffsetAndTake(
      runs, /*offset=*/2, /*limit=*/2, std::less<int>{});
  EXPECT_EQ(page, (std::vector<int>{20, 40}));

  // Goal_HotRunTiles: pair-ordinal tiling over 2x3 hot key.
  const auto tile0 = bm::EnumerateHotKeyTilePairs(2, 3, 2, 0);
  const auto tile1 = bm::EnumerateHotKeyTilePairs(2, 3, 2, 1);
  EXPECT_EQ(tile0, (std::vector<std::pair<size_t, size_t>>{{0, 0}, {0, 1}}));
  EXPECT_EQ(tile1, (std::vector<std::pair<size_t, size_t>>{{0, 2}, {1, 0}}));

  // Goal_DistinctExcludeTies: keep current row value while excluding peers.
  const std::unordered_map<int64_t, size_t> frame = {{10, 2}, {20, 1}, {30, 2}};
  const std::unordered_map<int64_t, size_t> peers = {{10, 2}, {20, 1}};
  EXPECT_EQ(bm::CountDistinctExcludeTies(frame, peers, /*current_row_val=*/10),
            2U);

  // Goal_RangeFingers: monotone two-finger band spans.
  const std::vector<int64_t> left = {10, 20, 30};
  const std::vector<int64_t> right = {5, 11, 15, 22, 35};
  const auto spans = bm::ComputeSortedRangeFingerSpans(left, right, -2, 2);
  EXPECT_EQ(spans,
            (std::vector<std::pair<size_t, size_t>>{{1, 2}, {3, 4}, {4, 4}}));

  // Goal_FlushChoice: 0/1 knapsack min-cost flush subset.
  const std::vector<bm::FlushBufferCandidate> bufs = {{.bytes = 4, .cost = 5},
                                                      {.bytes = 6, .cost = 7},
                                                      {.bytes = 3, .cost = 2}};
  EXPECT_EQ(bm::MinFlushCostToReclaim(bufs, 7), std::optional<uint64_t>(7));

  // Goal_SealedBuild: decide early on hit or sealed partition.
  const std::unordered_set<size_t> sealed = {1};
  const std::unordered_set<uint64_t> built = {42};
  EXPECT_EQ(bm::DecideBeforeBuildBarrier(0, sealed, built, 42),
            std::optional<bool>(true));
  EXPECT_EQ(bm::DecideBeforeBuildBarrier(1, sealed, built, 99),
            std::optional<bool>(false));
  EXPECT_FALSE(bm::DecideBeforeBuildBarrier(0, sealed, built, 99).has_value());

  // Goal_DemandCache + Goal_Exec_DemandCache: evaluate once per unique key.
  const std::vector<uint64_t> keys = {5, 3, 5, 3, 7};
  size_t evals = 0;
  const auto vals = bm::EvalWithDemandCache(
      keys, [](uint64_t k) { return static_cast<int>(k * 2); }, &evals);
  EXPECT_EQ(evals, 3U);
  EXPECT_EQ(vals, (std::vector<int>{10, 6, 10, 6, 14}));

  // Goal_ExistsDurability: last committed truth flip LSN.
  const std::vector<bm::ExistsWalEvent> log = {
      {.wal_end_lsn = 10, .committed = true, .new_truth = true},
      {.wal_end_lsn = 20, .committed = false, .new_truth = false},
      {.wal_end_lsn = 30, .committed = true, .new_truth = true},  // no flip
  };
  EXPECT_EQ(bm::MinExistsDurabilityFence(false, log), 10ULL);

  // Goal_NullableGroupKey: SQL-null-aware functional dependency check.
  const std::vector<std::pair<std::optional<int64_t>, std::optional<int64_t>>>
      fd_ok = {{std::nullopt, 1}, {std::nullopt, 1}, {2, std::nullopt}};
  const std::vector<std::pair<std::optional<int64_t>, std::optional<int64_t>>>
      fd_bad = {{std::nullopt, 1}, {std::nullopt, 2}};
  EXPECT_TRUE(bm::ObservedNullableFunctionalDependency(fd_ok));
  EXPECT_FALSE(bm::ObservedNullableFunctionalDependency(fd_bad));
}

TEST(ColumnarTlcsFastPathTest, EncodedScanAndSparseSetProofKernelsParity) {
  namespace bm = tinylamb::benchmark_rematch;

  // EncodedScanAggregates (ClickBench).
  const std::vector<int64_t> patches = {3, -1, 2};
  EXPECT_EQ(bm::SumDefaultLaneWithPatches(5, 10, patches), 54);
  EXPECT_EQ(bm::FoldFrameResidualsInOrder(
                patches, 10, int64_t{0},
                [](int64_t acc, int64_t v) { return acc + v; }),
            34);
  EXPECT_EQ(bm::MergeSortedDistinctSeam(3, 50, 2, 50), 4U);
  EXPECT_EQ(bm::MergeSortedDistinctSeam(3, 50, 2, 60), 5U);

  const std::vector<std::pair<uint32_t, uint64_t>> model_rows = {
      {1, 100}, {2, 100}, {2, 200}, {3, 300}};
  EXPECT_EQ(bm::CountDistinctUsersAcrossModelAliases(model_rows, {1, 2}), 2U);

  const std::vector<uint64_t> page_max = {10, 25, 5};
  EXPECT_EQ(bm::GlobalOmittedFrequencyUpperBound(page_max), 40U);

  const std::vector<uint64_t> sorted_u = {1, 1, 2, 5, 5, 9};
  EXPECT_EQ(bm::CountSortedUserRunTransitions(sorted_u), 4U);
  EXPECT_TRUE(bm::PassesMarginalFrequencyScreen(12, 15, 10));
  EXPECT_FALSE(bm::PassesMarginalFrequencyScreen(9, 15, 10));

  const std::vector<int64_t> deltas = {-2, 0, 5, 0, 7};
  EXPECT_EQ(bm::CountResidualEqualsTarget(deltas, 100, 100), 2U);

  const std::vector<uint32_t> codes = {9, 4, 7};
  EXPECT_EQ(bm::MinAcceptedDictCode(codes), std::optional<uint32_t>(4));

  const std::vector<std::vector<uint64_t>> sruns = {{1, 5, 9}, {2, 4, 8}};
  EXPECT_EQ(bm::MergeTopKFromSortedRuns(sruns, 3,
                                        [](uint64_t v) { return v % 2 == 1; }),
            (std::vector<uint64_t>{1, 5, 9}));

  const std::vector<uint64_t> ends = {10, 13, 13, 18};
  EXPECT_EQ(bm::TelescopeMonotoneStringOffsets(10, ends),
            (bm::OffsetTelescopeTotals{.byte_sum = 8, .nonempty_count = 2}));
  EXPECT_EQ(bm::SubtractSparseFlagExceptions(100, 7), 93U);
  EXPECT_FALSE(bm::ResolutionProductExceedsOffset(50, 100, 10000));
  EXPECT_TRUE(bm::ResolutionProductExceedsOffset(200, 100, 10000));

  const std::vector<uint64_t> mins = {0, 3, 0, 7};
  EXPECT_EQ(bm::SelectOccupiedMinuteBuckets(mins),
            (std::vector<std::pair<size_t, uint64_t>>{{1, 3}, {3, 7}}));

  // SparseSetAndWindowEvaluation (TPC-DS).
  const std::vector<uint64_t> d_store = {0b10110ULL};
  const std::vector<uint64_t> d_cat = {0b11010ULL};
  const std::vector<uint64_t> d_web = {0b10111ULL};
  EXPECT_EQ(bm::TripleDayBitmapIntersectPopcount(d_store, d_cat, d_web), 2U);

  const std::vector<bm::RankPageMeta> rpages = {
      {.min_key = 1, .max_key = 5, .row_count = 3},
      {.min_key = 6, .max_key = 12, .row_count = 3},
      {.min_key = 15, .max_key = 20, .row_count = 2},
  };
  const std::vector<std::vector<uint64_t>> rkeys = {
      {1, 3, 5}, {6, 10, 12}, {15, 20}};
  EXPECT_EQ(bm::CountStrictlySmallerWithPageExtrema(rpages, rkeys, 10), 4U);

  const std::vector<int64_t> st_totals = {10, 20, 30,
                                          100};  // sum=160, 1.2*40=48
  const auto suf = bm::SelectAboveAverageThresholdSuffix(st_totals, 160);
  ASSERT_EQ(suf.size(), 1U);
  EXPECT_EQ(suf[0], 100);

  const std::vector<std::pair<std::optional<uint64_t>, std::optional<int64_t>>>
      leaves = {{std::nullopt, 10}, {1, std::nullopt}, {2, 25}};
  EXPECT_EQ(bm::RollupNullableLeafPartitions(leaves),
            std::optional<int64_t>(35));

  const std::vector<std::pair<uint64_t, uint8_t>> trows = {
      {8, 1}, {9, 0}, {19, 1}, {20, 1}};
  EXPECT_EQ(bm::CountSharedDimAmPmSplit(
                trows, [](uint64_t t) { return t; }, 8, 9, 19, 20),
            (bm::AmPmSplitCounts{.am_count = 1, .pm_count = 2}));

  const std::vector<uint64_t> rweeks = {50, 53, 60, 70};
  EXPECT_EQ(bm::FilterShiftedWeekHalo(rweeks, 1, 10, 52),
            (std::vector<uint64_t>{53, 60}));

  EXPECT_TRUE(bm::CanAbandonNonPositivePriorYearGroup(-10, 2, 5));
  EXPECT_FALSE(bm::CanAbandonNonPositivePriorYearGroup(-10, 2, 6));
}

TEST(ColumnarTlcsFastPathTest, CompressionAndRestartRacesProofKernelsParity) {
  namespace bm = tinylamb::benchmark_rematch;

  // Compression: XorChain, SignificantWindow, ByteShuffle, LinearPredictor,
  // BitPackBeatsRaw, RangeReadFrameSpan, FrontCodedPage, TokenSubstitution.
  const std::vector<uint64_t> raw_bits = {
      0x8000000000000000ULL,  // -0.0
      0x7ff8000000000001ULL,  // NaN payload 1
      0x7ff8000000000002ULL,  // NaN payload 2
      0x3ff0000000000000ULL,  // 1.0
  };
  const auto xored = bm::EncodeXorBitChain(0, raw_bits);
  EXPECT_EQ(bm::DecodeXorBitChain(0, xored), raw_bits);

  for (const uint64_t x : raw_bits) {
    EXPECT_EQ(bm::ExtractXorSignificantWindow(x).Reconstruct(), x);
  }
  const auto shuf = bm::ByteShuffle64(raw_bits);
  EXPECT_EQ(bm::ByteUnshuffle64(shuf, raw_bits.size()), raw_bits);

  const std::vector<int64_t> series = {100, 110, 120, 131, 140};
  const auto res = bm::EncodeLinearPredictorResiduals(series);
  EXPECT_EQ(bm::DecodeLinearPredictorResiduals(res), series);

  EXPECT_TRUE(bm::BitPackBeatsRaw(1024, 12, 8, 16));
  EXPECT_EQ(bm::RangeReadFrameSpan(64, 60, 10),
            (std::pair<size_t, size_t>{0, 2}));

  const std::vector<std::string> words = {"https://a.com/1", "https://a.com/2",
                                          "https://b.com/1", "https://b.com/2"};
  const auto fc = bm::EncodeFrontCodedPage(words, /*restart_interval=*/2);
  EXPECT_EQ(bm::DecodeFrontCodedPage(fc), words);
  EXPECT_TRUE(bm::TokenSubstitutionSavesBytes(20, 10, 2, 4));
  EXPECT_FALSE(bm::TokenSubstitutionSavesBytes(1, 10, 2, 4));

  // RestartRaces: RenameMoves, OnlineUnique, ShadowBind, PackedWrites,
  // RankOneWeightMatrix, OwnerBufferedRedo, GenerationGC.
  // Permutation [1, 0, 2] has one 2-cycle -> 2 changed + 1 cycle = 3 moves.
  const std::vector<size_t> perm = {1, 0, 2};
  EXPECT_EQ(bm::MinAtomicRenameMoves(perm), 3U);

  const std::unordered_map<uint64_t, std::optional<int64_t>> base_u = {{1, 10},
                                                                       {2, 20}};
  const std::vector<bm::RidKeyDelta> u_deltas = {
      {.rid = 1, .ts = 5, .key = 20}, {.rid = 2, .ts = 6, .key = 10}};
  EXPECT_TRUE(bm::ValidateOnlineUniqueAfterDeltas(base_u, u_deltas, 6));
  EXPECT_FALSE(bm::ValidateOnlineUniqueAfterDeltas(base_u, u_deltas, 5));

  const std::vector<uint64_t> spath = {1, 2, 3};
  EXPECT_TRUE(bm::CanReuseShadowBind(spath, 2, {2, 3}));
  EXPECT_FALSE(bm::CanReuseShadowBind(spath, 2, {1, 2, 3}));

  EXPECT_TRUE(bm::PackedFieldWritesCommute(0b0101ULL, 0b1010ULL));
  EXPECT_FALSE(bm::PackedFieldWritesCommute(0b0110ULL, 0b0011ULL));

  const std::vector<std::vector<int64_t>> r1 = {{2, 4, 6}, {3, 6, 9}};
  const std::vector<std::vector<int64_t>> r2 = {{2, 4, 6}, {3, 6, 10}};
  EXPECT_TRUE(bm::IsRankOneWeightMatrix(r1));
  EXPECT_FALSE(bm::IsRankOneWeightMatrix(r2));

  const std::vector<bm::WalOwnerRecord> wal = {
      {.owner_id = 2, .key = 10, .val = 200},
      {.owner_id = 1, .key = 10, .val = 100},
      {.owner_id = 3, .key = 10, .val = 999},  // uncommitted
  };
  const std::vector<uint64_t> commits = {1, 2};
  const auto redone = bm::SinglePassOwnerBufferedRedo(wal, commits);
  EXPECT_EQ(redone.at(10), 200);

  const std::vector<uint64_t> gens = {1, 3, 5, 7};
  const std::vector<uint64_t> cuts = {6, 4};
  EXPECT_EQ(bm::SelectRetirableGenerations(gens, cuts),
            (std::vector<uint64_t>{1, 3}));
}

TEST(ColumnarTlcsFastPathTest, BenchmarkRematchBatch5AndJitFrontiersParity) {
  namespace bm = tinylamb::benchmark_rematch;

  // BenchmarkRematch wave 2 core combinators.
  const std::vector<uint64_t> d_in = {5, 1, 5, 3, 1};
  EXPECT_EQ(bm::StableDedupPreserveFirst(d_in),
            (std::vector<uint64_t>{5, 1, 3}));
  const std::vector<std::optional<int64_t>> n_in = {std::nullopt, 10, -3};
  EXPECT_EQ(bm::NullableSumChecked(n_in), std::optional<int64_t>(7));
  const std::vector<std::pair<uint64_t, int64_t>> g_in = {
      {1, 5}, {2, 3}, {1, 4}};
  EXPECT_EQ(bm::GroupByKeySum(g_in).at(1), 9);
  const std::vector<std::pair<bm::PairKey64, int64_t>> g2_in = {
      {{1, 2}, 10}, {{1, 2}, 5}, {{2, 1}, 7}};
  EXPECT_EQ(bm::GroupByPairKeySum(g2_in).at({1, 2}), 15);
  const std::vector<int64_t> t_in = {9, 2, 7, 1, 5};
  EXPECT_EQ(bm::TopNAscending(t_in, 3), (std::vector<int64_t>{1, 2, 5}));
  EXPECT_TRUE(bm::BlockZoneOverlapsRange(10, 20, 15, 25));
  EXPECT_FALSE(bm::BlockZoneOverlapsRange(10, 20, 21, 30));

  // Batch5 resource policy kernels.
  const std::vector<uint64_t> waiters = {10, 20, 30, 40};
  EXPECT_EQ(bm::CountWokenCommitWaiters(waiters, 25), 2U);
  EXPECT_FALSE(bm::EvictionNeedsWalFence(0xabcULL, 0xabcULL));
  EXPECT_TRUE(bm::EvictionNeedsWalFence(0xabcULL, 0xdefULL));

  bm::MemoryGrantTracker tracker{.limit_bytes = 100};
  EXPECT_TRUE(tracker.TryAcquire(60));
  EXPECT_FALSE(tracker.TryAcquire(50));
  tracker.Release(60);
  EXPECT_TRUE(tracker.TryAcquire(100));

  EXPECT_EQ(bm::ReserveUniqueJoinOutputRows(128, true), 128U);
  EXPECT_EQ(bm::BoundedMergeFanIn(4096, 512), 8U);
  EXPECT_EQ(bm::AdvanceContiguousMorselPrefix(0, {0, 1, 2, 4}), 3U);

  const std::vector<bm::PredicateCostModel> pmodels = {
      {.id = 1, .cost = 10.0, .reject_rate = 0.5},
      {.id = 2, .cost = 2.0, .reject_rate = 0.8},
  };
  EXPECT_EQ(bm::OrderPredicatesByRejectPerCost(pmodels),
            (std::vector<size_t>{2, 1}));
  EXPECT_TRUE(bm::ShouldCoalesceSpillReadGap(4096, 8192));
  const std::vector<uint64_t> snaps = {50, 30, 40};
  EXPECT_EQ(bm::SafeVacuumFloor(snaps, 100), 30ULL);
  const std::vector<uint32_t> l2g = {2, 0};
  const std::vector<uint32_t> g2t = {10, 20, 30};
  EXPECT_EQ(bm::ComposeDictionaryRemap(0, l2g, g2t),
            std::optional<uint32_t>(30));

  // JitFrontiers Core & SIMD kernels.
  EXPECT_TRUE(bm::CheckedAddRangeSafe(-100, 100, 50));
  EXPECT_FALSE(
      bm::CheckedAddRangeSafe(0, std::numeric_limits<int64_t>::max(), 1));
  EXPECT_FALSE(bm::DemandSsaGuardNeeded(/*known_false_prefix=*/true, true));
  EXPECT_TRUE(bm::DemandSsaGuardNeeded(/*known_false_prefix=*/false, true));
  EXPECT_EQ(bm::TryPackKeyPair64(3, 5, 8),
            std::optional<uint64_t>((3ULL << 8U) | 5ULL));
  EXPECT_EQ(bm::AntiPredicateBallot(0b1111ULL, 0b0101ULL), 0b1010ULL);
  EXPECT_TRUE(bm::BallotLimitReachedBeforeFault(0b0011ULL, 0b1000ULL, 2));
  EXPECT_FALSE(bm::BallotLimitReachedBeforeFault(0b1101ULL, 0b0010ULL, 2));

  const std::vector<int64_t> pvals = {5, 10, 20, 3};
  const std::vector<uint8_t> pmask = {1, 0, 1, 1};
  EXPECT_EQ(bm::MaskedPrefixSum(pvals, pmask),
            (std::vector<int64_t>{5, 5, 25, 28}));
  EXPECT_TRUE(bm::AbsLagWithinBand(12, 10, 2));
  EXPECT_FALSE(bm::AbsLagWithinBand(13, 10, 2));

  bm::PinnedSpecCache cache;
  EXPECT_EQ(cache.GetOrCompile(7, 101), 101ULL);
  EXPECT_FALSE(cache.TryEvict(7));
  cache.Unpin(7);
  EXPECT_TRUE(cache.TryEvict(7));

  const std::vector<int64_t> cvals = {10, 13, 15};
  EXPECT_EQ(bm::ChunkLocalBackwardDelta(cvals, 8),
            (std::vector<int64_t>{2, 3, 2}));
  EXPECT_TRUE(bm::DisjointOrCarryFree(0b1010ULL, 0b0101ULL));
  EXPECT_FALSE(bm::DisjointOrCarryFree(0b1010ULL, 0b0011ULL));
}

TEST(ColumnarTlcsFastPathTest,
     EngineBoundariesLayerContractsAndStorageExecutionParity) {
  namespace bm = tinylamb::benchmark_rematch;

  // EngineBoundaries (100/100) + November26 (83/83).
  const std::vector<uint64_t> lsns = {12, 45, 30};
  EXPECT_EQ(bm::GroupCommitCoalescedFrontier(lsns), 45ULL);
  const std::vector<uint64_t> snaps = {50, 20, 40};
  EXPECT_EQ(bm::EarliestRetainedIndexSnapshot(snaps, 100), 20ULL);
  EXPECT_EQ(bm::JoinCardinalityUpperBound(100, 4), 400ULL);
  EXPECT_EQ(bm::MergePassCount(16, 4), 2U);
  EXPECT_EQ(bm::RadixPartitionsWithinBudget(4096, 256), 16U);
  EXPECT_TRUE(bm::ReplyQueueAcceptsBytes(1000, 500, 2000));
  EXPECT_FALSE(bm::ReplyQueueAcceptsBytes(1600, 500, 2000));
  const std::vector<std::optional<size_t>> cyc = {1, 2, 0};
  const std::vector<std::optional<size_t>> dag = {1, 2, std::nullopt};
  EXPECT_TRUE(bm::WaitForChainHasCycle(0, cyc));
  EXPECT_FALSE(bm::WaitForChainHasCycle(0, dag));
  EXPECT_EQ(bm::kNovember26ConnectedProofCount, 83U);

  // LayerContracts Core & State (1036/1036).
  const std::vector<uint32_t> offs = {0, 3, 3, 8};
  EXPECT_EQ(bm::ArraySliceLength(offs, 0), 3U);
  EXPECT_EQ(bm::ArraySliceLength(offs, 1), 0U);
  EXPECT_EQ(bm::ArraySliceLength(offs, 2), 5U);
  EXPECT_TRUE(bm::DayInHalfOpenCalendarRange(10, 10, 20));
  EXPECT_FALSE(bm::DayInHalfOpenCalendarRange(20, 10, 20));
  EXPECT_EQ(bm::RetractXorParity(0b1100ULL, 0b0100ULL, 0b0011ULL), 0b1011ULL);
  EXPECT_EQ(bm::FindDepthOneCommaPositions("[1,[2,3],4]"),
            (std::vector<size_t>{2, 8}));
  std::unordered_set<uint64_t> visited = {1, 2};
  const std::vector<uint64_t> next_cands = {2, 3, 3, 4};
  EXPECT_EQ(bm::StepDeltaRecursiveFrontier(next_cands, &visited),
            (std::vector<uint64_t>{3, 4}));

  const bm::AffinePatch f1{.mul = 2, .add = 3};
  const bm::AffinePatch f2{.mul = 5, .add = -1};
  const bm::AffinePatch fused = bm::FuseAffinePatches(f1, f2);
  EXPECT_EQ(fused.Apply(7), f2.Apply(f1.Apply(7)));

  const std::vector<std::optional<uint64_t>> scopes = {10, std::nullopt, 20,
                                                       std::nullopt};
  EXPECT_EQ(bm::ResolveInnermostScopeId(scopes), std::optional<uint64_t>(20));
  EXPECT_EQ(bm::ClippedDistanceBandSeek(10, 4, 8, 12),
            (std::pair<int64_t, int64_t>{8, 12}));

  // StorageExecution Part 1 & 2 (1127/1127).
  const std::vector<std::pair<int64_t, uint64_t>> mruns = {{10, 3}, {-5, 2}};
  EXPECT_EQ(bm::FoldMultiplicityRunSum(mruns), 20);
  for (uint64_t x : {0ULL, 1ULL, 7ULL, 42ULL, 123456789ULL}) {
    EXPECT_EQ(bm::GrayDecode64(bm::GrayEncode64(x)), x);
  }
  EXPECT_EQ(bm::FloorDivZoneBounds(100, 199, 60),
            (std::pair<uint64_t, uint64_t>{1, 3}));
  EXPECT_EQ(bm::PrefixMaskPopcount(0b101101ULL, 4), 3U);
  EXPECT_EQ(bm::ContiguousOrdinalSum(3, 7), 18ULL);  // 3+4+5+6
  EXPECT_TRUE(bm::PostingPairInAnnulus(10, 14, 2, 5));
  EXPECT_FALSE(bm::PostingPairInAnnulus(10, 11, 2, 5));
}

TEST(ColumnarTlcsFastPathTest,
     QueryRewritesAtlasClickBenchLayoutAndStrategiesParity) {
  namespace bm = tinylamb::benchmark_rematch;

  // QueryRewrites remaining.
  const std::vector<std::unordered_map<uint64_t, int64_t>> shards = {
      {{1, 10}, {2, 20}}, {{1, 5}, {3, 7}}};
  const auto merged = bm::MergePartitionedAggregateStates(shards);
  EXPECT_EQ(merged.at(1), 15);
  EXPECT_EQ(merged.at(2), 20);
  EXPECT_EQ(merged.at(3), 7);

  const std::vector<bm::RunEndEntry> runs = {{.end_excl = 3, .value = 100},
                                             {.end_excl = 7, .value = 200}};
  EXPECT_EQ(bm::LookupRunEndOrdinal(runs, 0), std::optional<int64_t>(100));
  EXPECT_EQ(bm::LookupRunEndOrdinal(runs, 3), std::optional<int64_t>(200));
  EXPECT_FALSE(bm::LookupRunEndOrdinal(runs, 7).has_value());

  // Atlas.
  const std::vector<bm::PlanChoice> plans = {
      {.id = 1, .benefit = 10, .cost = 5},
      {.id = 2, .benefit = 9, .cost = 3},
      {.id = 3, .benefit = 4, .cost = 4},
  };
  EXPECT_EQ(bm::OrderPlansByBenefitCostRatio(plans),
            (std::vector<size_t>{2, 1, 3}));
  const std::vector<std::pair<int64_t, int64_t>> pts = {
      {1, 10}, {2, 8}, {3, 9}, {4, 5}};
  EXPECT_EQ(
      bm::Extract2DParetoSkyline(pts),
      (std::vector<std::pair<int64_t, int64_t>>{{1, 10}, {2, 8}, {4, 5}}));

  // ClickBenchLayout & ExecutionStrategies.
  const std::vector<uint32_t> keys = {0, 2, 2, 5, 1};
  EXPECT_EQ(bm::BoundedDomainHistogram(keys, 4),
            (std::vector<uint64_t>{1, 1, 2, 0}));
  EXPECT_TRUE(bm::ClosedIntervalsOverlap(1, 5, 5, 10));
  EXPECT_FALSE(bm::ClosedIntervalsOverlap(1, 4, 5, 10));
}

TEST(ColumnarTlcsFastPathTest,
     ResourcePoliciesJitKernelsFrontiersAndCorrectnessParity) {
  namespace bm = tinylamb::benchmark_rematch;

  // ResourcePolicies.
  const std::vector<size_t> classes = {64, 256, 128, 512};
  EXPECT_EQ(bm::SelectBestFitSlabClass(classes, 100), 2U);
  EXPECT_EQ(bm::SelectBestFitSlabClass(classes, 1000), classes.size());
  EXPECT_TRUE(bm::ConnectionBackpressureActive(4096, 4096));
  EXPECT_FALSE(bm::ConnectionBackpressureActive(4095, 4096));

  // JitKernels.
  const std::vector<int64_t> keys = {5, 10, 15, 20, 25};
  const std::vector<int64_t> payloads = {1, 2, 3, 4, 5};
  EXPECT_EQ(bm::FusedRangeFilterSumInt64(keys, payloads, 10, 20), 9);
  const std::vector<uint8_t> mask = {0, 1, 0, 1, 1};
  EXPECT_EQ(bm::CompactMaskToSelectionVector(mask),
            (std::vector<uint32_t>{1, 3, 4}));

  // Root & Frontiers.
  const std::vector<std::string_view> terms = {"alpha", "beta", "gamma"};
  EXPECT_EQ(bm::FindFirstMatchingPrefixIndex(terms, "be"),
            std::optional<size_t>(1));
  EXPECT_FALSE(bm::FindFirstMatchingPrefixIndex(terms, "delta").has_value());

  // ProposedGoals & DatabaseCorrectness.
  EXPECT_TRUE(bm::MvccVersionVisibleAtSnapshot(10, 20, 10));
  EXPECT_TRUE(bm::MvccVersionVisibleAtSnapshot(10, 20, 19));
  EXPECT_FALSE(bm::MvccVersionVisibleAtSnapshot(10, 20, 20));
  EXPECT_EQ(bm::DiscretePercentileIndex(10, 50, 100), std::optional<size_t>(4));
  EXPECT_EQ(bm::DiscretePercentileIndex(10, 99, 100), std::optional<size_t>(9));
  EXPECT_FALSE(bm::DiscretePercentileIndex(0, 50, 100).has_value());
}

}  // namespace
}  // namespace tinylamb
