/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/pipeline/pipeline_p3b_test.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/join_kind.hpp"
#include "common/memory_budget.hpp"
#include "common/query_grant.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/pipeline/ooc_sinks.hpp"
#include "executor/detail/pipeline/pipeline_ir.hpp"
#include "executor/detail/pipeline/pipeline_ops.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/named_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {
using namespace pipeline_p3b_test;

TEST(PipelineP3bTest, OocWindowSkewPartitionStreamingAndRejectsNonStreaming) {
  const std::string prefix = "p3b_win_" + RandomString();
  auto db_or = Database::Create(prefix);
  ASSERT_EQ(db_or.GetStatus(), Status::kSuccess);
  auto db = db_or.MoveValue();
  TransactionContext txn = db->BeginContext();

  Schema in_schema(
      "w", {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64),
            Column("pad", ValueType::kVarChar)});
  constexpr int kRows = 2500;
  std::vector<Row> input;
  input.reserve(kRows);
  const std::string pad(256, 'W');
  for (int i = 0; i < kRows; ++i) {
    // Include ties in `k` so RANK / DENSE_RANK / PERCENT_RANK / CUME_DIST
    // peer groups are all exercised under OOC skew streaming.
    const int64_t k = (kRows - 1 - i) / 3;
    const int64_t v = (i % 7 == 0) ? 0 : ((i % 19) - 5);
    Value val = (i % 13 == 0) ? Value() : Value(v);
    input.push_back(Row({Value(k), val, Value(std::string(pad))}));
  }

  std::vector<WindowOrderTerm> order_k{
      WindowOrderTerm{.expression = ColumnValueExp("k"), .ascending = true}};
  WindowFrameBound two_prec{.type = WindowFrameBoundType::kOffsetPreceding,
                            .offset = ConstantValueExp(Value(int64_t{2}))};
  WindowFrameBound two_foll{.type = WindowFrameBoundType::kOffsetFollowing,
                            .offset = ConstantValueExp(Value(int64_t{2}))};
  WindowFrameBound unb_prec{.type = WindowFrameBoundType::kUnboundedPreceding,
                            .offset = nullptr};
  WindowFrameBound cur_row{.type = WindowFrameBoundType::kCurrentRow,
                           .offset = nullptr};

  std::vector<NamedExpression> outputs;
  outputs.emplace_back("rn", MakeWindowCall("ROW_NUMBER", {}, order_k));
  outputs.emplace_back("rk", MakeWindowCall("RANK", {}, order_k));
  outputs.emplace_back("drk", MakeWindowCall("DENSE_RANK", {}, order_k));
  outputs.emplace_back("prk", MakeWindowCall("PERCENT_RANK", {}, order_k));
  outputs.emplace_back("cdist", MakeWindowCall("CUME_DIST", {}, order_k));
  outputs.emplace_back(
      "nt7",
      MakeWindowCall("NTILE", {ConstantValueExp(Value(int64_t{7}))}, order_k));
  outputs.emplace_back(
      "lag2",
      MakeWindowCall("LAG",
                     {ColumnValueExp("v"), ConstantValueExp(Value(int64_t{2})),
                      ConstantValueExp(Value(int64_t{-1}))},
                     order_k));
  outputs.emplace_back(
      "lead2",
      MakeWindowCall("LEAD",
                     {ColumnValueExp("v"), ConstantValueExp(Value(int64_t{2})),
                      ConstantValueExp(Value(int64_t{-1}))},
                     order_k));
  outputs.emplace_back(
      "pref_sum",
      MakeWindowCall("SUM", {ColumnValueExp("v")}, order_k,
                     WindowFrameUnit::kRows, unb_prec, cur_row, true));
  outputs.emplace_back(
      "win_cnt",
      MakeWindowCall("COUNT", {ColumnValueExp("v")}, order_k,
                     WindowFrameUnit::kRows, two_prec, two_foll, true));
  outputs.emplace_back(
      "win_min",
      MakeWindowCall("MIN", {ColumnValueExp("v")}, order_k,
                     WindowFrameUnit::kRows, two_prec, two_foll, true));
  outputs.emplace_back(
      "win_max",
      MakeWindowCall("MAX", {ColumnValueExp("v")}, order_k,
                     WindowFrameUnit::kRows, two_prec, two_foll, true));
  outputs.emplace_back(
      "win_fv",
      MakeWindowCall("FIRST_VALUE", {ColumnValueExp("v")}, order_k,
                     WindowFrameUnit::kRows, two_prec, two_foll, true));
  outputs.emplace_back(
      "win_lv",
      MakeWindowCall("LAST_VALUE", {ColumnValueExp("v")}, order_k,
                     WindowFrameUnit::kRows, two_prec, two_foll, true));

  Schema out_schema =
      in_schema +
      Schema(
          "w_out",
          {Column("rn", ValueType::kInt64), Column("rk", ValueType::kInt64),
           Column("drk", ValueType::kInt64), Column("prk", ValueType::kDouble),
           Column("cdist", ValueType::kDouble),
           Column("nt7", ValueType::kInt64), Column("lag2", ValueType::kInt64),
           Column("lead2", ValueType::kInt64),
           Column("pref_sum", ValueType::kInt64),
           Column("win_cnt", ValueType::kInt64),
           Column("win_min", ValueType::kInt64),
           Column("win_max", ValueType::kInt64),
           Column("win_fv", ValueType::kInt64),
           Column("win_lv", ValueType::kInt64)});

  // 1. Unbounded oracle run (fits in RAM, ComputeOneWindow).
  std::vector<Row> expected;
  {
    TestKnobsGuard oracle_guard(size_t{64} << 20);
    auto sink = std::make_shared<OocWindowSink>(in_schema, out_schema, outputs,
                                                /*preserve_input_order=*/true,
                                                /*spill_capable_plan=*/true);
    Status st;
    expected = ExecuteSink(out_schema, input, sink, &st, nullptr, &txn);
    ASSERT_EQ(st.GetCode(), StatusCode::kSuccess);
    ASSERT_EQ(expected.size(), static_cast<size_t>(kRows));
    EXPECT_FALSE(sink->Spilled());
  }

  // 2. Tight 96 KiB budget -> forces OOC single-partition skew stream!
  std::vector<Row> actual;
  {
    TestKnobsGuard tight_guard(96ULL << 10);
    auto sink = std::make_shared<OocWindowSink>(in_schema, out_schema, outputs,
                                                /*preserve_input_order=*/true,
                                                /*spill_capable_plan=*/true);
    Status st;
    PipelineExecutionMetrics metrics;
    actual = ExecuteSink(out_schema, input, sink, &st, &metrics, &txn);
    ASSERT_EQ(st.GetCode(), StatusCode::kSuccess) << st.GetMessage();
    EXPECT_TRUE(sink->Spilled());
    EXPECT_LT(sink->PeakResidentRows(), static_cast<size_t>(kRows));
    EXPECT_GT(metrics.ooc.spill_bytes, 0U);
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
      ASSERT_TRUE(RowsApproxEqual(expected[i], actual[i]))
          << "Mismatch at row " << i << ": expected=" << expected[i]
          << " actual=" << actual[i];
    }
  }

  // 3. Unbounded non-streaming ARRAY_AGG on an overflowing single partition
  //    must fail explicitly with kMemoryLimitExceeded rather than OOM.
  {
    TestKnobsGuard tight_guard(96ULL << 10);
    std::vector<NamedExpression> arr_out;
    arr_out.emplace_back(
        "arr",
        MakeWindowCall(
            "ARRAY_AGG", {ColumnValueExp("v")}, order_k, WindowFrameUnit::kRows,
            unb_prec,
            WindowFrameBound{.type = WindowFrameBoundType::kUnboundedFollowing,
                             .offset = nullptr},
            true));
    Schema arr_schema =
        in_schema + Schema("arr_out", {Column("arr", ValueType::kVarChar)});
    auto sink = std::make_shared<OocWindowSink>(in_schema, arr_schema, arr_out,
                                                /*preserve_input_order=*/false,
                                                /*spill_capable_plan=*/true);
    Status st;
    std::vector<Row> out =
        ExecuteSink(arr_schema, input, sink, &st, nullptr, &txn);
    EXPECT_TRUE(out.empty());
    EXPECT_EQ(st.GetCode(), StatusCode::kMemoryLimitExceeded);
  }
  EXPECT_EQ(Status::kSuccess, txn.txn_.Abort());
}

TEST(PipelineP3bTest, OocWindowSkewSlidingAggregatesStayIncremental) {
  const std::string prefix = "p3b_win_slide_" + RandomString();
  auto db_or = Database::Create(prefix);
  ASSERT_EQ(db_or.GetStatus(), Status::kSuccess);
  auto db = db_or.MoveValue();
  TransactionContext txn = db->BeginContext();

  Schema in_schema(
      "w", {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64),
            Column("pad", ValueType::kVarChar)});
  constexpr int kRows = 2500;
  std::vector<Row> input;
  input.reserve(kRows);
  const std::string pad(256, 'W');
  for (int i = 0; i < kRows; ++i) {
    const int64_t v = (i % 7 == 0) ? 0 : ((i % 19) - 5);
    Value val = (i % 13 == 0) ? Value() : Value(v);
    input.push_back(Row({Value(i), val, Value(std::string(pad))}));
  }

  std::vector<WindowOrderTerm> order_k{
      WindowOrderTerm{.expression = ColumnValueExp("k"), .ascending = true}};
  WindowFrameBound two_prec{.type = WindowFrameBoundType::kOffsetPreceding,
                            .offset = ConstantValueExp(Value(int64_t{2}))};
  WindowFrameBound five_foll{.type = WindowFrameBoundType::kOffsetFollowing,
                             .offset = ConstantValueExp(Value(int64_t{5}))};
  WindowFrameBound one_prec{.type = WindowFrameBoundType::kOffsetPreceding,
                            .offset = ConstantValueExp(Value(int64_t{1}))};
  WindowFrameBound one_foll{.type = WindowFrameBoundType::kOffsetFollowing,
                            .offset = ConstantValueExp(Value(int64_t{1}))};

  // Sliding ROWS frames cover SlidingMinMax (MIN/MAX), direct positional
  // evaluation (FIRST/LAST/NTH_VALUE), the rebuild loop with early exit
  // (ANY_VALUE), and the invertible bit/logical counters; the RANGE frame
  // exercises the deque RANGE sliding path.
  std::vector<NamedExpression> outputs;
  outputs.emplace_back(
      "win_min",
      MakeWindowCall("MIN", {ColumnValueExp("v")}, order_k,
                     WindowFrameUnit::kRows, two_prec, five_foll, true));
  outputs.emplace_back(
      "win_max",
      MakeWindowCall("MAX", {ColumnValueExp("v")}, order_k,
                     WindowFrameUnit::kRows, two_prec, five_foll, true));
  outputs.emplace_back(
      "win_fv",
      MakeWindowCall("FIRST_VALUE", {ColumnValueExp("v")}, order_k,
                     WindowFrameUnit::kRows, two_prec, five_foll, true));
  outputs.emplace_back(
      "win_lv",
      MakeWindowCall("LAST_VALUE", {ColumnValueExp("v")}, order_k,
                     WindowFrameUnit::kRows, two_prec, five_foll, true));
  outputs.emplace_back(
      "win_nth",
      MakeWindowCall("NTH_VALUE",
                     {ColumnValueExp("v"), ConstantValueExp(Value(int64_t{3}))},
                     order_k, WindowFrameUnit::kRows, two_prec, five_foll,
                     true));
  outputs.emplace_back(
      "win_any",
      MakeWindowCall("ANY_VALUE", {ColumnValueExp("v")}, order_k,
                     WindowFrameUnit::kRows, two_prec, five_foll, true));
  outputs.emplace_back(
      "win_band",
      MakeWindowCall("BIT_AND", {ColumnValueExp("v")}, order_k,
                     WindowFrameUnit::kRows, two_prec, five_foll, true));
  outputs.emplace_back(
      "win_bor",
      MakeWindowCall("BIT_OR", {ColumnValueExp("v")}, order_k,
                     WindowFrameUnit::kRows, two_prec, five_foll, true));
  outputs.emplace_back(
      "win_land",
      MakeWindowCall("LOGICAL_AND", {ColumnValueExp("v")}, order_k,
                     WindowFrameUnit::kRows, two_prec, five_foll, true));
  outputs.emplace_back(
      "win_lor",
      MakeWindowCall("LOGICAL_OR", {ColumnValueExp("v")}, order_k,
                     WindowFrameUnit::kRows, two_prec, five_foll, true));
  outputs.emplace_back("r_min", MakeWindowCall("MIN", {ColumnValueExp("v")},
                                               order_k, WindowFrameUnit::kRange,
                                               one_prec, one_foll, true));

  Schema out_schema =
      in_schema + Schema("w_out", {Column("win_min", ValueType::kInt64),
                                   Column("win_max", ValueType::kInt64),
                                   Column("win_fv", ValueType::kInt64),
                                   Column("win_lv", ValueType::kInt64),
                                   Column("win_nth", ValueType::kInt64),
                                   Column("win_any", ValueType::kInt64),
                                   Column("win_band", ValueType::kInt64),
                                   Column("win_bor", ValueType::kInt64),
                                   Column("win_land", ValueType::kInt64),
                                   Column("win_lor", ValueType::kInt64),
                                   Column("r_min", ValueType::kInt64)});

  std::vector<Row> expected;
  {
    TestKnobsGuard oracle_guard(size_t{64} << 20);
    auto sink = std::make_shared<OocWindowSink>(in_schema, out_schema, outputs,
                                                /*preserve_input_order=*/true,
                                                /*spill_capable_plan=*/true);
    Status st;
    expected = ExecuteSink(out_schema, input, sink, &st, nullptr, &txn);
    ASSERT_EQ(st.GetCode(), StatusCode::kSuccess);
    ASSERT_EQ(expected.size(), static_cast<size_t>(kRows));
    EXPECT_FALSE(sink->Spilled());
  }

  std::vector<Row> actual;
  {
    TestKnobsGuard tight_guard(96ULL << 10);
    auto sink = std::make_shared<OocWindowSink>(in_schema, out_schema, outputs,
                                                /*preserve_input_order=*/true,
                                                /*spill_capable_plan=*/true);
    Status st;
    PipelineExecutionMetrics metrics;
    actual = ExecuteSink(out_schema, input, sink, &st, &metrics, &txn);
    ASSERT_EQ(st.GetCode(), StatusCode::kSuccess) << st.GetMessage();
    EXPECT_TRUE(sink->Spilled());
    EXPECT_LT(sink->PeakResidentRows(), static_cast<size_t>(kRows));
    EXPECT_GT(metrics.ooc.spill_bytes, 0U);
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
      ASSERT_TRUE(RowsApproxEqual(expected[i], actual[i]))
          << "Mismatch at row " << i << ": expected=" << expected[i]
          << " actual=" << actual[i];
    }
  }
  EXPECT_EQ(Status::kSuccess, txn.txn_.Abort());
}

TEST(PipelineP3bTest, SpillTransitionRetryKeepsResidentRows) {
  TestKnobsGuard knobs(4ULL << 20);
  PipelineContext ctx(nullptr, QueryMemoryGrant{});
  Schema schema("input", {Column("k", ValueType::kInt64),
                          Column("v", ValueType::kVarChar)});
  auto aggregate = std::make_shared<AggregateExpression>(
      AggregationType::kArrayAgg, ColumnValueExp("v"), false);
  OocGroupAggSink grouped(schema, {ColumnValueExp("k")}, {aggregate.get()},
                          Schema("out", {Column("k", ValueType::kInt64),
                                         Column("a", ValueType::kArray)}));
  OocDistinctSink distinct(schema, {0}, "Distinct", {}, true);
  auto coordinator = std::make_shared<OocSetOpCoordinator>(
      SetOperationKind::kUnion, schema, std::vector<Schema>{schema, schema});
  OocSetOpInputSink set_op(coordinator, 0, true, nullptr);
  for (PipelineSink* sink :
       std::vector<PipelineSink*>{&grouped, &distinct, &set_op}) {
    SCOPED_TRACE(sink->Describe());
    ProcessMemoryBudget::Global().SetTestAlwaysSpill(false);
    ASSERT_EQ(sink->Init(&ctx, 1), Status::kSuccess);
    DataChunk resident, incoming;
    for (int k = 0; k < 4; ++k) {
      resident.Append(Row({Value(k), Value(std::string(65536, 'x'))}));
    }
    incoming.Append(Row({Value(4), Value("y")}));
    ASSERT_EQ(sink->Consume(&ctx, 0, resident), Status::kSuccess);
    auto& budget = ProcessMemoryBudget::Global();
    budget.SetTestAlwaysSpill(true);
    budget.SetTestSpillIoFailCountdown(2);
    const Status failed = sink->Consume(&ctx, 0, incoming);
    budget.SetTestSpillIoFailCountdown(0);
    ASSERT_EQ(failed, Status::kIOError);
    EXPECT_EQ(ctx.spill_store.Stats().live_segments, 0U);
    ASSERT_EQ(sink->Consume(&ctx, 0, incoming), Status::kSuccess);
    auto source = sink->Finalize(&ctx);
    ASSERT_TRUE(source.HasValue()) << source.GetStatus();
    ASSERT_EQ(source.Value()->Open(&ctx), Status::kSuccess);
    std::vector<int64_t> keys;
    DataChunk out;
    for (;;) {
      auto next = source.Value()->NextBatch(&ctx, &out);
      ASSERT_TRUE(next.HasValue()) << next.GetStatus();
      if (!next.Value()) {
        break;
      }
      for (size_t i = 0; i < out.Size(); ++i) {
        const auto row = out.RowAt(i);
        ASSERT_GT(row.Count(), 0U);
        ASSERT_EQ(row[0].type, ValueType::kInt64);
        keys.push_back(row[0].AsInt64());
      }
    }
    std::ranges::sort(keys);
    EXPECT_EQ(keys, (std::vector<int64_t>{0, 1, 2, 3, 4}));
  }
}

TEST(PipelineP3bTest, WindowCanRetryFirstSorterFinishOnSameSink) {
  TestKnobsGuard knobs(64ULL << 10, true);
  auto db = Database::Create("window_retry_" + RandomString()).MoveValue();
  {
    auto txn = db->BeginContext();
    PipelineContext ctx(&txn, QueryMemoryGrant{});
    Schema schema("in", {Column("k", ValueType::kInt64),
                         Column("v", ValueType::kVarChar)});
    const auto rn = MakeWindowCall(
        "ROW_NUMBER", {},
        {{.expression = ColumnValueExp("k"), .ascending = true}});
    OocWindowSink sink(
        schema, schema + Schema("out", {Column("rn", ValueType::kInt64)}),
        {NamedExpression("rn", rn)}, false, true);
    ASSERT_EQ(sink.Init(&ctx, 1), Status::kSuccess);
    DataChunk input;
    for (int i = 4; i > 0; --i) {
      input.Append(Row({Value(i), Value(std::string(12000, 'x'))}));
    }
    ASSERT_EQ(sink.Consume(&ctx, 0, input), Status::kSuccess);
    auto& budget = ProcessMemoryBudget::Global();
    budget.SetTestSpillIoFailCountdown(1);
    const auto failed = sink.Finalize(&ctx);
    budget.SetTestSpillIoFailCountdown(0);
    ASSERT_EQ(failed.GetStatus(), Status::kIOError);
    auto output = sink.Finalize(&ctx);
    ASSERT_TRUE(output.HasValue()) << output.GetStatus();
    ASSERT_EQ(output.Value()->Open(&ctx), Status::kSuccess);
    DataChunk rows;
    int64_t expected = 1;
    for (;;) {
      auto next = output.Value()->NextBatch(&ctx, &rows);
      ASSERT_TRUE(next.HasValue()) << next.GetStatus();
      if (!next.Value()) {
        break;
      }
      for (size_t i = 0; i < rows.Size(); ++i, ++expected) {
        EXPECT_EQ(rows.RowAt(i)[0], Value(expected));
        EXPECT_EQ(rows.RowAt(i)[2], Value(expected));
      }
    }
    EXPECT_EQ(expected, 5);
  }
  db->DeleteAll();
}

TEST(PipelineP3bTest, OocWindowFaultInjectionPropagatesCleanly) {
  const std::string prefix = "p3b_win_fault_" + RandomString();
  auto db_or = Database::Create(prefix);
  ASSERT_EQ(db_or.GetStatus(), Status::kSuccess);
  auto db = db_or.MoveValue();
  TransactionContext txn = db->BeginContext();
  TestKnobsGuard guard(64ULL << 10, /*always_spill=*/false,
                       /*temp_limit_bytes=*/2ULL * SpillStore::kBlockSize);
  Schema schema("wf", {Column("k", ValueType::kInt64),
                       Column("pad", ValueType::kVarChar)});
  std::vector<Row> input;
  input.reserve(1500);
  const std::string pad(256, 'F');
  for (int i = 0; i < 1500; ++i) {
    input.push_back(Row({Value(int64_t{i}), Value(std::string(pad))}));
  }
  std::vector<WindowOrderTerm> order_k{
      WindowOrderTerm{.expression = ColumnValueExp("k"), .ascending = true}};
  std::vector<NamedExpression> outputs;
  outputs.emplace_back("rn", MakeWindowCall("ROW_NUMBER", {}, order_k));
  Schema out_schema =
      schema + Schema("wf_out", {Column("rn", ValueType::kInt64)});
  auto sink = std::make_shared<OocWindowSink>(schema, out_schema, outputs,
                                              /*preserve_input_order=*/false,
                                              /*spill_capable_plan=*/true);
  const std::vector<Row> input_copy = input;
  Status st;
  std::vector<Row> out =
      ExecuteSink(out_schema, std::move(input), sink, &st, nullptr, &txn);
  EXPECT_TRUE(out.empty());
  EXPECT_EQ(st.GetCode(), StatusCode::kNoSpace);
  // "Cleanly" also means the failed sink released its spill/grant resources:
  // an identical sink under a normal budget must still be able to run.
  {
    // The default-constructed guard only resets on destruction; clear the
    // injected limits now so the retry really runs with normal budgets.
    ProcessMemoryBudget::Global().ResetTestKnobs();
    auto retry = std::make_shared<OocWindowSink>(schema, out_schema, outputs,
                                                 /*preserve_input_order=*/false,
                                                 /*spill_capable_plan=*/true);
    Status retry_status;
    std::vector<Row> retry_out = ExecuteSink(out_schema, input_copy, retry,
                                             &retry_status, nullptr, &txn);
    EXPECT_EQ(retry_status, Status::kSuccess);
    EXPECT_FALSE(retry_out.empty());
  }
  EXPECT_EQ(Status::kSuccess, txn.txn_.Abort());
  db->DeleteAll();
}

TEST(PipelineP3bTest, OocGroupAggNonDecomposableSkewReturnsTooBigData) {
  TestKnobsGuard guard(64ULL << 10);
  Schema schema("ga", {Column("grp", ValueType::kInt64),
                       Column("v", ValueType::kVarChar)});
  std::vector<Row> input;
  input.reserve(1200);
  const std::string pad(256, 'A');
  for (int i = 0; i < 1200; ++i) {
    input.push_back(
        Row({Value(int64_t{1}), Value(std::to_string(i) + "_" + pad)}));
  }
  auto arr_agg = std::make_shared<AggregateExpression>(
      AggregationType::kArrayAgg, ColumnValueExp("v"), false);
  arr_agg->SetInnerOrderBy(
      {WindowOrderTerm{.expression = ColumnValueExp("v"), .ascending = true}});
  Schema out_schema("ga_out", {Column("grp", ValueType::kInt64),
                               Column("arr", ValueType::kVarChar)});
  auto sink = std::make_shared<OocGroupAggSink>(
      schema, std::vector<Expression>{ColumnValueExp("grp")},
      std::vector<const AggregateExpression*>{arr_agg.get()}, out_schema);
  Status st;
  std::vector<Row> out = ExecuteSink(out_schema, std::move(input), sink, &st);
  EXPECT_TRUE(out.empty());
  EXPECT_EQ(st.GetCode(), StatusCode::kTooBigData);
}

TEST(PipelineP3bTest, OocGroupAggMixedDistinctSkewReturnsTooBigData) {
  // COUNT(DISTINCT) combined with other aggregates cannot take the sorted
  // single-distinct path, so its per-group distinct-value set must obey the
  // operator memory budget instead of growing without bound. The skew path
  // charges DistinctRetainedBytes() -- each live set entry's real VARCHAR
  // payload plus node overhead -- rather than a fixed per-entry estimate, so
  // the 256-byte pad below (~300 KiB over 1200 entries) genuinely exercises
  // the byte budget instead of merely tripping a nominal estimate.
  TestKnobsGuard guard(64ULL << 10);
  Schema schema(
      "gdm", {Column("grp", ValueType::kInt64), Column("k", ValueType::kInt64),
              Column("v", ValueType::kVarChar)});
  std::vector<Row> input;
  input.reserve(1200);
  const std::string pad(256, 'M');
  for (int i = 0; i < 1200; ++i) {
    input.push_back(Row({Value(int64_t{1}), Value(int64_t{i}),
                         Value(std::to_string(i) + "_" + pad)}));
  }
  auto cnt_distinct = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("v"), true);
  auto sum_k = std::make_shared<AggregateExpression>(
      AggregationType::kSum, ColumnValueExp("k"), false);
  Schema out_schema("gdm_out", {Column("grp", ValueType::kInt64),
                                Column("cd", ValueType::kInt64),
                                Column("s", ValueType::kInt64)});
  auto sink = std::make_shared<OocGroupAggSink>(
      schema, std::vector<Expression>{ColumnValueExp("grp")},
      std::vector<const AggregateExpression*>{cnt_distinct.get(), sum_k.get()},
      out_schema);
  Status st;
  std::vector<Row> out = ExecuteSink(out_schema, std::move(input), sink, &st);
  EXPECT_TRUE(out.empty());
  EXPECT_EQ(st.GetCode(), StatusCode::kTooBigData);
}

TEST(PipelineP3bTest, OocGroupAggSingleDistinctSkewStripsAndStaysExact) {
  // A lone plain COUNT/SUM/AVG(DISTINCT col) takes the sorted skew path
  // with a DISTINCT-stripped accumulator: adjacent dedup feeds each
  // distinct value once, keeping per-group state O(1) while preserving
  // exact DISTINCT semantics (duplicates and NULLs handled).
  TestKnobsGuard guard(64ULL << 10);
  Schema schema("gds",
                {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64),
                 Column("pad", ValueType::kVarChar)});
  constexpr int kRows = 2500;
  std::vector<Row> input;
  input.reserve(kRows);
  const std::string pad(120, 'T');
  std::unordered_map<int64_t, std::unordered_set<int64_t>> expected_distinct;
  for (int i = 0; i < kRows; ++i) {
    // 80% heavy key 7 (skew), 20% light keys; v cycles i % 37 with
    // periodic NULLs so dedup and NULL-skipping are both exercised.
    const int64_t k = (i % 5 != 0) ? 7 : i;
    const bool is_null = (i % 11 == 0);
    const int64_t v = i % 37;
    input.push_back(
        Row({Value(k), is_null ? Value() : Value(v), Value(std::string(pad))}));
    auto& distinct_vals = expected_distinct[k];
    if (!is_null) {
      distinct_vals.insert(v);
    }
  }
  ASSERT_GT(expected_distinct.size(), 1U);

  struct AggCase {
    AggregationType type;
    ValueType out_type;
  };
  const std::array<AggCase, 3> cases = {{
      {.type = AggregationType::kCount, .out_type = ValueType::kInt64},
      {.type = AggregationType::kSum, .out_type = ValueType::kInt64},
      {.type = AggregationType::kAvg, .out_type = ValueType::kDouble},
  }};
  for (const auto& tc : cases) {
    auto agg =
        std::make_shared<AggregateExpression>(tc.type, ColumnValueExp("v"),
                                              /*distinct=*/true);
    Schema out_schema("gds_out", {Column("k", ValueType::kInt64),
                                  Column("agg", tc.out_type)});
    auto sink = std::make_shared<OocGroupAggSink>(
        schema, std::vector<Expression>{ColumnValueExp("k")},
        std::vector<const AggregateExpression*>{agg.get()}, out_schema);
    Status st;
    const std::vector<Row> out =
        ExecuteSink(out_schema, input, sink, &st, nullptr, nullptr);
    ASSERT_EQ(st, Status::kSuccess);
    ASSERT_EQ(out.size(), expected_distinct.size());
    for (const Row& r : out) {
      const auto it = expected_distinct.find(r[0].AsInt64());
      ASSERT_NE(it, expected_distinct.end());
      if (it->second.empty()) {
        // A group whose every input was NULL: COUNT is 0, SUM/AVG NULL.
        if (tc.type == AggregationType::kCount) {
          EXPECT_EQ(r[1].AsInt64(), 0);
        } else {
          EXPECT_TRUE(r[1].IsNull());
        }
        continue;
      }
      int64_t sum = 0;
      for (const int64_t d : it->second) {
        sum += d;
      }
      const double avg =
          static_cast<double>(sum) / static_cast<double>(it->second.size());
      switch (tc.type) {
        case AggregationType::kCount:
          EXPECT_EQ(r[1].AsInt64(), static_cast<int64_t>(it->second.size()));
          break;
        case AggregationType::kSum:
          EXPECT_EQ(r[1].AsInt64(), sum);
          break;
        case AggregationType::kAvg:
          EXPECT_NEAR(r[1].AsDouble(), avg, 1e-9);
          break;
        default:
          FAIL() << "unexpected aggregate type";
      }
    }
  }
}

}  // namespace
}  // namespace tinylamb
