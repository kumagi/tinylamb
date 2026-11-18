/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/columnar_tlcs_fast_path.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/memory_budget.hpp"
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
       Column("v", ValueType::kVarChar), Column("ts", ValueType::kVarChar)});
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
      ASSERT_TRUE(tbl.Insert(ctx.txn_, Row({Value(i), Value(i % 37),
                                            std::move(v), std::move(ts)}))
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

TEST(ColumnarTlcsFastPathTest, PartitionedAggStrategiesAndTightBudgetFallback) {
  BudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(size_t{256} << 20, size_t{64} << 20, size_t{64} << 20,
                      /*check_rss_pressure=*/false);

  const std::string prefix = "tlcs_part_strat_" + RandomString();
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
    ASSERT_TRUE(res.has_value());
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
    ASSERT_TRUE(res.has_value());
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
    ASSERT_TRUE(res.has_value());
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
    static const char* kFirst[] = {"a", "b", "c", "d"};
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
  ASSERT_TRUE(res.has_value());
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

}  // namespace
}  // namespace tinylamb
