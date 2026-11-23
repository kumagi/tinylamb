/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <gtest/gtest.h>

#include <algorithm>
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

class TestKnobsGuard {
 public:
  TestKnobsGuard() = default;
  explicit TestKnobsGuard(size_t op_budget_bytes, bool always_spill = false,
                          size_t temp_limit_bytes = 0) {
    ProcessMemoryBudget::Global().SetTestOpMemoryBytes(op_budget_bytes);
    ProcessMemoryBudget::Global().SetTestAlwaysSpill(always_spill);
    ProcessMemoryBudget::Global().SetTestTempLimitBytes(temp_limit_bytes);
  }
  TestKnobsGuard(const TestKnobsGuard&) = delete;
  TestKnobsGuard& operator=(const TestKnobsGuard&) = delete;
  TestKnobsGuard(TestKnobsGuard&&) = delete;
  TestKnobsGuard& operator=(TestKnobsGuard&&) = delete;
  ~TestKnobsGuard() { ProcessMemoryBudget::Global().ResetTestKnobs(); }
};

std::vector<Row> DrainExecutor(ExecutorBase* exec) {
  std::vector<Row> out;
  DataChunk chunk;
  while (exec->NextBatch(&chunk, kDefaultVectorSize) > 0) {
    for (size_t i = 0; i < chunk.Size(); ++i) {
      out.push_back(chunk.RowAt(i));
    }
    chunk.Reset();
  }
  return out;
}

std::vector<Row> ExecuteSink(Schema out_schema, std::vector<Row> input_rows,
                             std::shared_ptr<PipelineSink> sink,
                             Status* status_out,
                             PipelineExecutionMetrics* metrics_out = nullptr,
                             TransactionContext* txn = nullptr) {
  auto binding = std::make_shared<DeferredPipelineSource>("SinkOut");
  PipelineGraph g;
  g.output_schema = std::move(out_schema);
  g.stages.push_back(PipelineStage{
      .id = 0,
      .name = "Stage0",
      .source = std::make_shared<RowVectorSource>(std::move(input_rows)),
      .operators = {},
      .sink = std::move(sink),
      .output_binding = binding,
  });
  g.stages.push_back(PipelineStage{
      .id = 1,
      .name = "Root",
      .source = binding,
      .operators = {},
      .sink = nullptr,
      .output_binding = nullptr,
  });
  PipelineExecutor exec(txn, std::move(g));
  std::vector<Row> out = DrainExecutor(&exec);
  if (status_out != nullptr) {
    *status_out = exec.GetStatus();
  }
  if (metrics_out != nullptr) {
    *metrics_out = exec.Metrics();
  }
  return out;
}

bool RowsApproxEqual(const Row& a, const Row& b) {
  if (a.Count() != b.Count()) {
    return false;
  }
  for (size_t i = 0; i < a.Count(); ++i) {
    if (a[i].IsNull() && b[i].IsNull()) {
      continue;
    }
    if (a[i].IsNull() || b[i].IsNull()) {
      return false;
    }
    if (a[i].type == ValueType::kDouble && b[i].type == ValueType::kDouble) {
      if (std::abs(a[i].AsDouble() - b[i].AsDouble()) > 1e-9) {
        return false;
      }
      continue;
    }
    if (!(a[i] == b[i])) {
      return false;
    }
  }
  return true;
}

Expression MakeWindowCall(
    std::string fn, std::vector<Expression> args,
    std::vector<WindowOrderTerm> order_by,
    WindowFrameUnit unit = WindowFrameUnit::kDefault,
    WindowFrameBound start = {.type = WindowFrameBoundType::kUnboundedPreceding,
                              .offset = nullptr},
    WindowFrameBound end = {.type = WindowFrameBoundType::kCurrentRow,
                            .offset = nullptr},
    bool has_frame = false) {
  auto call = std::make_shared<WindowFunctionCallExpression>();
  call->function = std::move(fn);
  call->args = std::move(args);
  call->order_by = std::move(order_by);
  call->frame_unit = unit;
  call->frame_start = std::move(start);
  call->frame_end = std::move(end);
  call->has_frame = has_frame;
  return call;
}

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
      if (!next.Value()) { break;
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

  const struct {
    AggregationType type;
    ValueType out_type;
  } cases[] = {
      {.type=AggregationType::kCount, .out_type=ValueType::kInt64},
      {.type=AggregationType::kSum, .out_type=ValueType::kInt64},
      {.type=AggregationType::kAvg, .out_type=ValueType::kDouble},
  };
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

TEST(PipelineP3bTest, SqlEngineP3bOperatorsZeroLegacyAndBudgetInvariant) {
  const std::string prefix = "p3b_sql_" + RandomString();
  auto db_or = Database::Create(prefix);
  ASSERT_EQ(db_or.GetStatus(), Status::kSuccess);
  auto db = db_or.MoveValue();

  {
    TransactionContext setup = db->BeginContext();
    SqlEngine eng(*db);
    ASSERT_TRUE(eng.Execute(setup,
                            "CREATE TABLE p3b_t (id INT64 PRIMARY KEY, g "
                            "INT64, k INT64, s STRING(64));")
                    .HasValue());
    ASSERT_TRUE(eng.Execute(setup,
                            "CREATE TABLE p3b_u (id INT64 PRIMARY KEY, g "
                            "INT64, k INT64, s STRING(64));")
                    .HasValue());
    ASSERT_TRUE(eng.Execute(setup, "CREATE INDEX idx_p3b_gk ON p3b_t (g, k);")
                    .HasValue());

    std::string ins_t = "INSERT INTO p3b_t VALUES ";
    std::string ins_u = "INSERT INTO p3b_u VALUES ";
    ins_t.reserve(120);
    ins_u.reserve(120);
    for (int i = 0; i < 120; ++i) {
      if (i > 0) {
        ins_t += ',';
        ins_u += ',';
      }
      const int g = i % 8;
      const int k = (i * 13) % 50;
      ins_t += "(" + std::to_string(i) + "," + std::to_string(g) + "," +
               std::to_string(k) + ",'row_" + std::to_string(i) + "')";
      ins_u += "(" + std::to_string(i + 60) + "," + std::to_string(g) + "," +
               std::to_string(k) + ",'u_" + std::to_string(i) + "')";
    }
    ASSERT_TRUE(eng.Execute(setup, ins_t + ";").HasValue());
    ASSERT_TRUE(eng.Execute(setup, ins_u + ";").HasValue());
    ASSERT_EQ(setup.PreCommit(), Status::kSuccess);
  }

  const std::vector<std::string> queries = {
      // Window functions + ordering
      "SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY k, id), "
      "SUM(k) OVER (ORDER BY k, id) FROM p3b_t ORDER BY id;",
      // Set operations: UNION / INTERSECT / EXCEPT / ALL
      "SELECT g, k FROM p3b_t UNION DISTINCT SELECT g, k FROM p3b_u "
      "ORDER BY g, k;",
      "SELECT g, k FROM p3b_t INTERSECT ALL SELECT g, k FROM p3b_u "
      "ORDER BY g, k;",
      "SELECT g, k FROM p3b_t EXCEPT DISTINCT SELECT g, k FROM p3b_u "
      "ORDER BY g, k;",
      // DISTINCT ON
      "SELECT DISTINCT ON (g) g, k, id FROM p3b_t ORDER BY g, k, id;",
      // Non-decomposable ordered ARRAY_AGG / STRING_AGG
      "SELECT g, ARRAY_AGG(k ORDER BY id), STRING_AGG(s, '|' ORDER BY id) "
      "FROM p3b_t GROUP BY g ORDER BY g;",
      // BandJoin / non-equi ProductPlan
      "SELECT a.id, b.id FROM p3b_t a LEFT JOIN p3b_u b "
      "ON a.id < b.id AND a.id + 2 >= b.id WHERE a.id < 25 ORDER BY a.id, "
      "b.id;",
      // Recursive CTE (UNION ALL + UNION DISTINCT)
      "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r "
      "WHERE n < 20) SELECT n FROM r ORDER BY n;",
      "WITH RECURSIVE r AS (SELECT 1 AS n UNION DISTINCT "
      "SELECT MOD(n, 7) + 1 FROM r) SELECT COUNT(*) FROM r;",
      // Correlated CROSS JOIN LATERAL (Apply)
      "SELECT a.id, b.mx FROM p3b_t a CROSS JOIN LATERAL "
      "(SELECT MAX(u.k) AS mx FROM p3b_u u WHERE u.g = a.g) b "
      "WHERE a.id < 30 ORDER BY a.id;",
      // MinMax / IndexSkipScan shapes
      "SELECT MIN(g), MAX(g) FROM p3b_t;",
  };

  auto run_all = [&](size_t op_budget, bool always_spill,
                     bool check_zero_legacy) {
    TestKnobsGuard guard(op_budget, always_spill);
    std::vector<std::vector<Row>> results;
    results.reserve(queries.size());
    TransactionContext ctx = db->BeginContext();
    SqlEngine eng(*db);
    for (const std::string& sql : queries) {
      StatusOr<Executor> exec_or = eng.Prepare(ctx, sql);
      EXPECT_TRUE(exec_or.HasValue()) << sql << " err=" << eng.LastError();
      if (!exec_or.HasValue()) {
        results.emplace_back();
        continue;
      }
      Executor exec = exec_or.MoveValue();
      std::vector<Row> rows = DrainExecutor(exec.get());
      EXPECT_EQ(exec->GetStatus().GetCode(), StatusCode::kSuccess) << sql;
      if (check_zero_legacy) {
        if (const auto* pe =
                dynamic_cast<const PipelineExecutor*>(exec.get())) {
          EXPECT_DOUBLE_EQ(pe->Metrics().legacy_source_ms, 0.0)
              << "Legacy fallback observed on SQL: " << sql << "\n"
              << pe->ExplainPipeline();
        }
      }
      results.push_back(std::move(rows));
    }
    EXPECT_EQ(Status::kSuccess, ctx.txn_.Abort());
    return results;
  };

  const auto oracle = run_all(/*op_budget=*/0, /*always_spill=*/false, true);
  const auto tight =
      run_all(/*op_budget=*/64ULL << 10, /*always_spill=*/false, true);
  const auto spilled = run_all(/*op_budget=*/64ULL << 10,
                               /*always_spill=*/true, true);

  ASSERT_EQ(tight.size(), oracle.size());
  ASSERT_EQ(spilled.size(), oracle.size());
  for (size_t i = 0; i < queries.size(); ++i) {
    EXPECT_EQ(tight[i], oracle[i]) << "G2 tight mismatch for: " << queries[i];
    EXPECT_EQ(spilled[i], oracle[i])
        << "G2 always-spill mismatch for: " << queries[i];
  }
}

TEST(PipelineP3bTest, OocRecursiveCteUnionDistinctSpillBoundaryKeepsRow) {
  // Regression: the UNION DISTINCT reached-set spills once `work_index`
  // outgrows `seen_budget`. The row that crosses the budget must be spilled
  // *before* it enters `reached_seg`; inserting it first wrote it into both
  // the spilled reached set and the candidate sorter, and the merge
  // anti-join then dropped it from the output and the next delta.
  // The keys here are integers; the collation-peer variant of the same spill
  // boundary relies on the comparator contract in ooc_join_cte_sources.cpp:
  // the sorter keys carry the static Collation::kNone, but SortKeyOrder::
  // Compare resolves the runtime collation from the operand tags, so the
  // spilled anti-join peer rule matches WorkTableIndex's EqualUnderCollation.
  const std::string prefix = "p3b_rcte_" + RandomString();
  auto db_or = Database::Create(prefix);
  ASSERT_EQ(db_or.GetStatus(), Status::kSuccess);
  auto db = db_or.MoveValue();

  const std::string sql =
      "WITH RECURSIVE r AS (SELECT 1 AS n UNION DISTINCT "
      "SELECT n + 1 FROM r WHERE n < 900) SELECT n FROM r ORDER BY n;";

  auto run = [&](size_t op_budget) -> std::vector<Row> {
    TestKnobsGuard guard(op_budget);
    TransactionContext ctx = db->BeginContext();
    SqlEngine eng(*db);
    StatusOr<Executor> exec_or = eng.Prepare(ctx, sql);
    EXPECT_TRUE(exec_or.HasValue()) << eng.LastError();
    if (!exec_or.HasValue()) {
      EXPECT_EQ(Status::kSuccess, ctx.txn_.Abort());
      return {};
    }
    Executor exec = exec_or.MoveValue();
    std::vector<Row> rows = DrainExecutor(exec.get());
    EXPECT_EQ(exec->GetStatus().GetCode(), StatusCode::kSuccess);
    EXPECT_EQ(Status::kSuccess, ctx.txn_.Abort());
    return rows;
  };

  const auto oracle = run(size_t{64} << 20);
  ASSERT_EQ(oracle.size(), 900U);
  const auto tight = run(64ULL << 10);
  EXPECT_EQ(tight, oracle);
}

TEST(PipelineP3bTest, MergeJoinUsesSortOrderForMixedTypes) {
  Schema input("input", {Column("k", ValueType::kVarChar)});
  Schema output("output", {Column("lk", ValueType::kVarChar),
                           Column("rk", ValueType::kVarChar)});
  OocSetOpInputStream left{
      .source = std::make_shared<RowVectorSource>(
          std::vector<Row>{Row({Value("a")}), Row({Value("b")})}),
      .operators = {},
      .schema = input};
  OocSetOpInputStream right{
      .source = std::make_shared<RowVectorSource>(
          std::vector<Row>{Row({Value(1.0)}), Row({Value("b")})}),
      .operators = {},
      .schema = input};
  auto join = std::make_shared<OocMergeJoinSource>(
      std::move(left), std::move(right), std::vector<slot_t>{0},
      std::vector<slot_t>{0}, JoinKind::kInner, 1, 1, Expression(),
      input + input, output);
  PipelineGraph graph;
  graph.output_schema = output;
  graph.stages.push_back(PipelineStage{.id = 0,
                                       .name = "join",
                                       .source = join,
                                       .operators = {},
                                       .sink = nullptr,
                                       .output_binding = nullptr});
  PipelineExecutor exec(nullptr, std::move(graph));
  const auto rows = DrainExecutor(&exec);
  ASSERT_EQ(exec.GetStatus().GetCode(), StatusCode::kSuccess);
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0], Row({Value("b"), Value("b")}));
}

TEST(PipelineP3bTest, OocMergeBandCrossJoinHandleOverflowRunsAndOuterModes) {
  TestKnobsGuard guard(64ULL << 10, /*always_spill=*/true);

  // 1. OocMergeJoinSource with large equal-key right run overflowing 64 KiB.
  {
    Schema lr_schema("lr", {Column("k", ValueType::kInt64),
                            Column("pad", ValueType::kVarChar)});
    Schema out_schema(
        "mj",
        {Column("lk", ValueType::kInt64), Column("lpad", ValueType::kVarChar),
         Column("rk", ValueType::kInt64), Column("rpad", ValueType::kVarChar)});
    const std::string pad(256, 'M');
    std::vector<Row> lrows{
        Row({Value(int64_t{1}), Value(std::string(pad))}),
        Row({Value(int64_t{2}), Value(std::string(pad))}),
        Row({Value(int64_t{2}), Value(std::string(pad))}),
        Row({Value(int64_t{5}), Value(std::string(pad))}),
    };
    std::vector<Row> rrows;
    rrows.reserve(401);
    for (int i = 0; i < 400; ++i) {
      rrows.push_back(Row({Value(int64_t{2}), Value(std::string(pad))}));
    }
    rrows.push_back(Row({Value(int64_t{9}), Value(std::string(pad))}));

    OocSetOpInputStream left{
        .source = std::make_shared<RowVectorSource>(std::move(lrows)),
        .operators = {},
        .schema = lr_schema};
    OocSetOpInputStream right{
        .source = std::make_shared<RowVectorSource>(std::move(rrows)),
        .operators = {},
        .schema = lr_schema};
    auto mj = std::make_shared<OocMergeJoinSource>(
        std::move(left), std::move(right), std::vector<slot_t>{0},
        std::vector<slot_t>{0}, JoinKind::kFullOuter, 2, 2, Expression(),
        lr_schema + lr_schema, out_schema);
    PipelineGraph g;
    g.output_schema = out_schema;
    g.stages.push_back(PipelineStage{.id = 0,
                                     .name = "MJ",
                                     .source = mj,
                                     .operators = {},
                                     .sink = nullptr,
                                     .output_binding = nullptr});
    PipelineExecutor exec(nullptr, std::move(g));
    std::vector<Row> out = DrainExecutor(&exec);
    ASSERT_EQ(exec.GetStatus().GetCode(), StatusCode::kSuccess);
    // 2 left key=2 * 400 right key=2 = 800 matches
    // + 2 unmatched left (k=1, k=5) + 1 unmatched right (k=9) = 803 rows.
    EXPECT_EQ(out.size(), 803U);
  }

  // 2. OocBandJoinSource kRightOuter & kFullOuter with AlwaysSpill.
  {
    Schema s("bj", {Column("k", ValueType::kInt64),
                    Column("pad", ValueType::kVarChar)});
    Schema out_s(
        "bj_out",
        {Column("lk", ValueType::kInt64), Column("lpad", ValueType::kVarChar),
         Column("rk", ValueType::kInt64), Column("rpad", ValueType::kVarChar)});
    const std::string pad(128, 'B');
    std::vector<Row> lrows{
        Row({Value(int64_t{1}), Value(std::string(pad))}),
        Row({Value(int64_t{4}), Value(std::string(pad))}),
        Row({Value(int64_t{10}), Value(std::string(pad))}),
    };
    std::vector<Row> rrows{
        Row({Value(int64_t{0}), Value(std::string(pad))}),
        Row({Value(int64_t{2}), Value(std::string(pad))}),
        Row({Value(int64_t{5}), Value(std::string(pad))}),
    };
    OocSetOpInputStream l_in{.source = std::make_shared<RowVectorSource>(lrows),
                             .operators = {},
                             .schema = s};
    OocSetOpInputStream r_in{.source = std::make_shared<RowVectorSource>(rrows),
                             .operators = {},
                             .schema = s};
    auto bj = std::make_shared<OocBandJoinSource>(
        std::move(l_in), std::move(r_in), 0, 0, BinaryOperation::kLessThan,
        Expression(), s + s, JoinKind::kFullOuter, 2, 2, out_s);
    PipelineGraph g;
    g.output_schema = out_s;
    g.stages.push_back(PipelineStage{.id = 0,
                                     .name = "BJ",
                                     .source = bj,
                                     .operators = {},
                                     .sink = nullptr,
                                     .output_binding = nullptr});
    PipelineExecutor exec(nullptr, std::move(g));
    std::vector<Row> out = DrainExecutor(&exec);
    ASSERT_EQ(exec.GetStatus().GetCode(), StatusCode::kSuccess);
    // Contract order: matched pairs left-major (right-key order inside one
    // left row), then unmatched left rows padded, then unmatched right rows
    // padded.
    EXPECT_EQ(
        out,
        (std::vector<Row>{
            Row({Value(int64_t{1}), Value(std::string(pad)), Value(int64_t{2}),
                 Value(std::string(pad))}),
            Row({Value(int64_t{1}), Value(std::string(pad)), Value(int64_t{5}),
                 Value(std::string(pad))}),
            Row({Value(int64_t{4}), Value(std::string(pad)), Value(int64_t{5}),
                 Value(std::string(pad))}),
            Row({Value(int64_t{10}), Value(std::string(pad)), Value(),
                 Value()}),
            Row({Value(), Value(), Value(int64_t{0}), Value(std::string(pad))}),
        }));
  }
}

TEST(PipelineP3bTest, OocCrossJoinProbeEmitsLeftMajorOrder) {
  TestKnobsGuard guard(64ULL << 10, /*always_spill=*/true);
  const Schema ls("l", {Column("id", ValueType::kInt64)});
  const Schema rs("r", {Column("id", ValueType::kInt64)});
  const Schema out("j", {Column("lid", ValueType::kInt64),
                         Column("rid", ValueType::kInt64)});
  auto ctx = std::make_shared<OocCrossJoinContext>();
  ctx->kind = JoinKind::kInner;
  ctx->left_width = 1;
  ctx->right_width = 1;
  ctx->left_schema = ls;
  ctx->right_schema = rs;
  ctx->combined_schema = ls + rs;
  ctx->output_schema = out;
  ctx->budget_bytes = 64ULL << 10;

  auto build_done = std::make_shared<DeferredPipelineSource>("CrossBuildDone");
  PipelineGraph g;
  g.output_schema = out;
  g.stages.push_back(PipelineStage{
      .id = 0,
      .name = "Build",
      .source = std::make_shared<RowVectorSource>(std::vector<Row>{
          Row({Value(int64_t{10})}), Row({Value(int64_t{20})})}),
      .operators = {},
      .sink = std::make_shared<OocCrossJoinBuildSink>(ctx),
      .output_binding = build_done,
  });
  g.stages.push_back(PipelineStage{
      .id = 1,
      .name = "Probe",
      .source = std::make_shared<OocCrossJoinProbeSource>(
          ctx,
          OocSetOpInputStream{
              .source = std::make_shared<RowVectorSource>(std::vector<Row>{
                  Row({Value(int64_t{1})}), Row({Value(int64_t{2})})}),
              .operators = {},
              .schema = ls}),
      .operators = {},
      .sink = nullptr,
      .output_binding = nullptr,
  });
  PipelineExecutor exec(nullptr, std::move(g));
  const std::vector<Row> rows = DrainExecutor(&exec);
  EXPECT_EQ(exec.GetStatus().GetCode(), StatusCode::kSuccess);
  EXPECT_EQ(rows,
            (std::vector<Row>{Row({Value(int64_t{1}), Value(int64_t{10})}),
                              Row({Value(int64_t{1}), Value(int64_t{20})}),
                              Row({Value(int64_t{2}), Value(int64_t{10})}),
                              Row({Value(int64_t{2}), Value(int64_t{20})})}));
}

TEST(PipelineP3bTest,
     WindowSkewSupportsCountifNullDynamicLagNthRangeAndPlanPolicy) {
  const std::string prefix = "p3b_win_audit_" + RandomString();
  auto db_or = Database::Create(prefix);
  ASSERT_EQ(db_or.GetStatus(), Status::kSuccess);
  auto db = db_or.MoveValue();
  TransactionContext txn = db->BeginContext();

  Schema in_schema(
      "w", {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64),
            Column("off", ValueType::kInt64), Column("cond", ValueType::kInt64),
            Column("pad", ValueType::kVarChar)});
  constexpr int kRows = 1600;
  std::vector<Row> input;
  input.reserve(kRows);
  const std::string pad(256, 'A');
  for (int i = 0; i < kRows; ++i) {
    const int64_t step = (i % 4) + 1;
    input.push_back(
        Row({Value(static_cast<int64_t>(i * 2)),
             Value(static_cast<int64_t>((i * 7) % 101)), Value(step),
             Value(),  // all-NULL cond for C1
             Value(std::string(pad))}));
  }

  std::vector<WindowOrderTerm> order_k{
      WindowOrderTerm{.expression = ColumnValueExp("k"),
                      .ascending = true,
                      .nulls_first = std::nullopt}};
  WindowFrameBound unb_prec{.type = WindowFrameBoundType::kUnboundedPreceding,
                            .offset = nullptr};
  WindowFrameBound unb_foll{.type = WindowFrameBoundType::kUnboundedFollowing,
                            .offset = nullptr};
  WindowFrameBound cur_row{.type = WindowFrameBoundType::kCurrentRow,
                           .offset = nullptr};
  WindowFrameBound two_prec{.type = WindowFrameBoundType::kOffsetPreceding,
                            .offset = ConstantValueExp(Value(int64_t{2}))};
  WindowFrameBound one_prec{.type = WindowFrameBoundType::kOffsetPreceding,
                            .offset = ConstantValueExp(Value(int64_t{1}))};
  WindowFrameBound five_prec{.type = WindowFrameBoundType::kOffsetPreceding,
                             .offset = ConstantValueExp(Value(int64_t{5}))};
  WindowFrameBound one_foll{.type = WindowFrameBoundType::kOffsetFollowing,
                            .offset = ConstantValueExp(Value(int64_t{1}))};
  WindowFrameBound four_foll{.type = WindowFrameBoundType::kOffsetFollowing,
                             .offset = ConstantValueExp(Value(int64_t{4}))};
  Expression one_plus_one = std::make_shared<BinaryExpression>(
      ConstantValueExp(Value(int64_t{1})), BinaryOperation::kAdd,
      ConstantValueExp(Value(int64_t{1})));

  std::vector<NamedExpression> outputs;
  outputs.emplace_back("cif_all",
                       MakeWindowCall("COUNTIF", {ColumnValueExp("cond")}, {}));
  outputs.emplace_back(
      "cif_empty",
      MakeWindowCall("COUNTIF", {ColumnValueExp("cond")}, order_k,
                     WindowFrameUnit::kRows, two_prec, one_prec, true));
  outputs.emplace_back(
      "lag_dyn", MakeWindowCall("LAG",
                                {ColumnValueExp("v"), ColumnValueExp("off"),
                                 ConstantValueExp(Value(int64_t{-1}))},
                                order_k));
  outputs.emplace_back("lead_const_expr",
                       MakeWindowCall("LEAD",
                                      {ColumnValueExp("v"), one_plus_one,
                                       ConstantValueExp(Value(int64_t{-9}))},
                                      order_k));
  outputs.emplace_back(
      "nth_dyn_rows",
      MakeWindowCall("NTH_VALUE", {ColumnValueExp("v"), ColumnValueExp("off")},
                     order_k, WindowFrameUnit::kRows, unb_prec, four_foll,
                     true));
  outputs.emplace_back(
      "range_sum",
      MakeWindowCall("SUM", {ColumnValueExp("v")}, order_k,
                     WindowFrameUnit::kRange, five_prec, cur_row, true));
  outputs.emplace_back(
      "range_max",
      MakeWindowCall("MAX", {ColumnValueExp("v")}, order_k,
                     WindowFrameUnit::kRange, five_prec, one_foll, true));
  outputs.emplace_back(
      "nth_dyn_range",
      MakeWindowCall("NTH_VALUE", {ColumnValueExp("v"), ColumnValueExp("off")},
                     order_k, WindowFrameUnit::kRange, five_prec, four_foll,
                     true));

  Schema out_schema =
      in_schema +
      Schema("wf_out", {Column("cif_all", ValueType::kInt64),
                        Column("cif_empty", ValueType::kInt64),
                        Column("lag_dyn", ValueType::kInt64),
                        Column("lead_const_expr", ValueType::kInt64),
                        Column("nth_dyn_rows", ValueType::kInt64),
                        Column("range_sum", ValueType::kInt64),
                        Column("range_max", ValueType::kInt64),
                        Column("nth_dyn_range", ValueType::kInt64)});

  std::vector<Row> expected;
  {
    TestKnobsGuard oracle_guard(size_t{64} << 20);
    auto sink = std::make_shared<OocWindowSink>(in_schema, out_schema, outputs,
                                                /*preserve_input_order=*/true,
                                                /*spill_capable_plan=*/true);
    Status st;
    expected = ExecuteSink(out_schema, input, sink, &st, nullptr, &txn);
    ASSERT_EQ(st.GetCode(), StatusCode::kSuccess) << st.GetMessage();
    ASSERT_EQ(expected.size(), static_cast<size_t>(kRows));
    EXPECT_FALSE(sink->Spilled());
    // C1: COUNTIF over all-NULL / empty frame must be 0, never NULL.
    EXPECT_EQ(expected[0][in_schema.ColumnCount()], Value(int64_t{0}));
    EXPECT_EQ(expected[0][in_schema.ColumnCount() + 1], Value(int64_t{0}));
  }

  {
    TestKnobsGuard tight_guard(96ULL << 10);
    auto sink = std::make_shared<OocWindowSink>(in_schema, out_schema, outputs,
                                                /*preserve_input_order=*/true,
                                                /*spill_capable_plan=*/true);
    Status st;
    std::vector<Row> actual =
        ExecuteSink(out_schema, input, sink, &st, nullptr, &txn);
    ASSERT_EQ(st.GetCode(), StatusCode::kSuccess) << st.GetMessage();
    EXPECT_TRUE(sink->Spilled());
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
      ASSERT_TRUE(RowsApproxEqual(expected[i], actual[i]))
          << "Mismatch at row " << i << ": expected=" << expected[i]
          << " actual=" << actual[i];
    }
  }

  // C8: Unsupported GROUPS sliding frame returns kNotImplemented (not OOM).
  {
    TestKnobsGuard tight_guard(96ULL << 10);
    std::vector<NamedExpression> grp_out;
    grp_out.emplace_back(
        "grp_sum",
        MakeWindowCall("SUM", {ColumnValueExp("v")}, order_k,
                       WindowFrameUnit::kGroups, two_prec, cur_row, true));
    Schema grp_schema =
        in_schema + Schema("g_out", {Column("grp_sum", ValueType::kInt64)});
    auto sink = std::make_shared<OocWindowSink>(in_schema, grp_schema, grp_out,
                                                /*preserve_input_order=*/false,
                                                /*spill_capable_plan=*/true);
    Status st;
    std::vector<Row> out =
        ExecuteSink(grp_schema, input, sink, &st, nullptr, &txn);
    EXPECT_TRUE(out.empty());
    EXPECT_EQ(st.GetCode(), StatusCode::kNotImplemented);
  }

  // C7 & C10: Non-spill-capable plan forced by plan budget / tight B_op still
  // spills cleanly via ExternalSorter AND preserves input row order.
  {
    TestKnobsGuard large_op(size_t{64} << 20);
    std::vector<Row> rev_input = input;
    std::reverse(rev_input.begin(), rev_input.end());
    std::vector<NamedExpression> ord_out;
    ord_out.emplace_back("rn", MakeWindowCall("ROW_NUMBER", {}, order_k));
    Schema ord_schema =
        in_schema + Schema("ord_out", {Column("rn", ValueType::kInt64)});
    auto sink = std::make_shared<OocWindowSink>(
        in_schema, ord_schema, ord_out,
        /*preserve_input_order=*/false,
        /*spill_capable_plan=*/false,
        /*plan_memory_budget_bytes=*/64ULL << 10);
    Status st;
    std::vector<Row> actual =
        ExecuteSink(ord_schema, rev_input, sink, &st, nullptr, &txn);
    ASSERT_EQ(st.GetCode(), StatusCode::kSuccess) << st.GetMessage();
    EXPECT_TRUE(sink->Spilled());
    ASSERT_EQ(actual.size(), rev_input.size());
    for (size_t i = 0; i < rev_input.size(); ++i) {
      ASSERT_EQ(actual[i][0], rev_input[i][0])
          << "Input order not restored at row " << i;
    }
  }
  EXPECT_EQ(Status::kSuccess, txn.txn_.Abort());
}

TEST(PipelineP3bTest, SetOperationsAcceptEntirelyNullColumns) {
  const Schema schema("n", {Column("x", ValueType::kNull)});
  auto input_stream = [&] {
    return OocSetOpInputStream{
        .source = std::make_shared<RowVectorSource>(
            std::vector<Row>{Row({Value()})}),
        .operators = {},
        .schema = schema};
  };
  DataChunk output;
  OocConcatSource concat({input_stream(), input_stream()}, schema);
  ASSERT_EQ(concat.Open(nullptr), Status::kSuccess);
  for (int i = 0; i < 2; ++i) {
    auto next = concat.NextBatch(nullptr, &output, 1);
    ASSERT_TRUE(next.HasValue() && next.Value());
    ASSERT_EQ(output.Size(), 1U);
    EXPECT_TRUE(output.RowAt(0)[0].IsNull());
  }

  for (bool always_spill : {false, true}) {
    TestKnobsGuard guard(96ULL << 10, always_spill);
    PipelineContext ctx(nullptr, QueryMemoryGrant{});
    OocSetOpCoordinator coord(SetOperationKind::kIntersect, schema,
                              {schema, schema});
    DataChunk input;
    input.Append(Row({Value()}));
    ASSERT_EQ(coord.ConsumeFromSource(&ctx, 0, input), Status::kSuccess);
    ASSERT_EQ(coord.ConsumeFromSource(&ctx, 1, input), Status::kSuccess);
    auto source = coord.Finalize(&ctx);
    ASSERT_TRUE(source.HasValue()) << source.GetStatus();
    ASSERT_EQ(source.Value()->Open(&ctx), Status::kSuccess);
    auto next = source.Value()->NextBatch(&ctx, &output, 2);
    ASSERT_TRUE(next.HasValue() && next.Value());
    ASSERT_EQ(output.Size(), 1U);
    EXPECT_TRUE(output.RowAt(0)[0].IsNull());
  }
}

TEST(PipelineP3bTest,
     DistinctPreserveOrderWithoutKeyColsAndSetOpTypeWideningGrant) {
  // D2: OocDistinctSink with preserve_input_order=true and key_cols={} must
  // ignore internal seq column when extracting partition keys during spill.
  {
    TestKnobsGuard guard(96ULL << 10, /*always_spill=*/true);
    Schema s("d", {Column("k", ValueType::kInt64),
                   Column("pad", ValueType::kVarChar)});
    std::vector<Row> rows;
    const std::string pad(128, 'D');
    rows.reserve(60);
    for (int i = 0; i < 60; ++i) {
      rows.push_back(Row({Value(int64_t{i % 5}), Value(std::string(pad))}));
    }
    auto sink = std::make_shared<OocDistinctSink>(
        s, std::vector<slot_t>{}, "Distinct", std::vector<Expression>{},
        /*preserve_input_order=*/true);
    Status st;
    std::vector<Row> out = ExecuteSink(s, std::move(rows), sink, &st);
    ASSERT_EQ(st.GetCode(), StatusCode::kSuccess) << st.GetMessage();
    ASSERT_EQ(out.size(), 5U);
    for (size_t i = 0; i < 5; ++i) {
      EXPECT_EQ(out[i][0], Value(static_cast<int64_t>(i)));
    }
  }

  // D1 & D3: OocSetOpCoordinator type widening INT64 -> DOUBLE *after* spill
  // re-partitions leaves consistently so UNION/INTERSECT deduce exact equality.
  {
    TestKnobsGuard guard(96ULL << 10, /*always_spill=*/true);
    Schema left_s("l", {Column("x", ValueType::kInt64)});
    Schema right_s("r", {Column("x", ValueType::kDouble)});
    Schema out_s("o", {Column("x", ValueType::kDouble)});
    std::vector<Row> lrows{Row({Value(int64_t{1})}), Row({Value(int64_t{2})})};
    std::vector<Row> rrows{Row({Value(1.0)}), Row({Value(3.0)})};

    auto coord = std::make_shared<OocSetOpCoordinator>(
        SetOperationKind::kIntersect, out_s,
        std::vector<Schema>{left_s, right_s});
    auto binding = std::make_shared<DeferredPipelineSource>("SetOut");
    auto lsrc = std::make_shared<RowVectorSource>(lrows);
    auto rsrc = std::make_shared<RowVectorSource>(rrows);
    PipelineGraph g;
    g.output_schema = out_s;
    g.stages.push_back(PipelineStage{
        .id = 0,
        .name = "L",
        .source = lsrc,
        .operators = {},
        .sink = std::make_shared<OocSetOpInputSink>(coord, 0, false, nullptr),
        .output_binding = nullptr,
    });
    g.stages.push_back(PipelineStage{
        .id = 1,
        .name = "R",
        .source = rsrc,
        .operators = {},
        .sink = std::make_shared<OocSetOpInputSink>(coord, 1, true, nullptr),
        .output_binding = binding,
    });
    g.stages.push_back(PipelineStage{
        .id = 2,
        .name = "Root",
        .source = binding,
        .operators = {},
        .sink = nullptr,
        .output_binding = nullptr,
    });
    PipelineExecutor exec(nullptr, std::move(g));
    std::vector<Row> out = DrainExecutor(&exec);
    ASSERT_EQ(exec.GetStatus().GetCode(), StatusCode::kSuccess)
        << exec.GetStatus().GetMessage();
    EXPECT_EQ(out, (std::vector<Row>{Row({Value(1.0)})}));
  }

  // C2 & D3: Verify grant byte accounting (`UsedBytes > 0` during buffering,
  // `UsedBytes == 0` and `ActiveConsumerCount == 0` after source reaches EOF)
  // and UNION post-spill type widening deduplication (`1` vs `1.0` collapse).
  {
    ProcessMemoryBudget& mb = ProcessMemoryBudget::Global();
    // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is
    // non-copyable by design
    struct ResetGuard {  // NOLINT(cppcoreguidelines-special-member-functions)
      explicit ResetGuard(ProcessMemoryBudget* b) : budget(b) {}
      ~ResetGuard() {
        budget->ResetTestKnobs();
        budget->ResetForTest(0);
      }
      ProcessMemoryBudget* budget;
    } mb_guard(&mb);
    mb.ResetForTest(64ULL << 20, 64ULL << 20, 64ULL << 20, false);
    mb.SetTestOpMemoryBytes(512ULL << 10);

    const std::string db_prefix = "p3b_grant_" + RandomString();
    auto db_or = Database::Create(db_prefix);
    ASSERT_EQ(db_or.GetStatus(), Status::kSuccess);
    auto db = db_or.MoveValue();
    TransactionContext txn = db->BeginContext();

    QueryMemoryCoordinator qmc(&mb);
    auto grant_or = qmc.AcquireGrant(4ULL << 20, 4ULL << 20);
    ASSERT_TRUE(grant_or.HasValue()) << grant_or.GetStatus();
    auto grant = grant_or.MoveValue();
    PipelineContext ctx(&txn, std::move(grant), 1);

    Schema s("w", {Column("k", ValueType::kInt64),
                   Column("pad", ValueType::kVarChar)});
    DataChunk chunk;
    chunk.Reserve(32);
    const std::string pad(2048, 'G');
    for (int i = 0; i < 32; ++i) {
      chunk.Append(Row({Value(int64_t{i}), Value(std::string(pad))}));
    }

    // Window sink holds buffered bytes in grant, releases on source EOF.
    std::vector<WindowOrderTerm> order_k{
        WindowOrderTerm{.expression = ColumnValueExp("k"),
                        .ascending = true,
                        .nulls_first = std::nullopt}};
    std::vector<NamedExpression> win_out;
    win_out.emplace_back("rn", MakeWindowCall("ROW_NUMBER", {}, order_k));
    Schema ws = s + Schema("wo", {Column("rn", ValueType::kInt64)});
    auto wsink = std::make_shared<OocWindowSink>(s, ws, win_out, false, true);
    ASSERT_EQ(wsink->Init(&ctx, 1), Status::kSuccess);
    ASSERT_EQ(wsink->Consume(&ctx, 0, chunk), Status::kSuccess);
    EXPECT_GT(ctx.grant.UsedBytes(), 0U);
    EXPECT_EQ(ctx.grant.ActiveConsumerCount(), 1U);
    auto wsrc_or = wsink->Finalize(&ctx);
    ASSERT_TRUE(wsrc_or.HasValue()) << wsrc_or.GetStatus();
    auto wsrc = wsrc_or.MoveValue();
    ASSERT_EQ(wsrc->Open(&ctx), Status::kSuccess);
    DataChunk out_c;
    while (true) {
      auto more = wsrc->NextBatch(&ctx, &out_c, kDefaultVectorSize);
      ASSERT_TRUE(more.HasValue()) << more.GetStatus();
      if (!more.Value()) {
        break;
      }
    }
    wsrc.reset();
    EXPECT_EQ(ctx.grant.UsedBytes(), 0U);
    EXPECT_EQ(ctx.grant.ActiveConsumerCount(), 0U);

    // OocSetOpCoordinator (UNION): holds grant bytes while buffering in-memory,
    // releases cleanly on EOF, and deduplicates post-spill widened INT64 vs
    // DOUBLE after crossing `in_mem_budget_`.
    Schema left_s("l", {Column("x", ValueType::kInt64),
                        Column("pad", ValueType::kVarChar)});
    Schema right_s("r", {Column("x", ValueType::kDouble),
                         Column("pad", ValueType::kVarChar)});
    Schema out_s("o", {Column("x", ValueType::kDouble),
                       Column("pad", ValueType::kVarChar)});
    const std::string sp(1024, 'U');
    DataChunk lc_small;
    lc_small.Reserve(2);
    lc_small.Append(Row({Value(int64_t{1}), Value(std::string(sp))}));
    lc_small.Append(Row({Value(int64_t{2}), Value(std::string(sp))}));

    DataChunk lc_spill;
    lc_spill.Reserve(400);
    for (int i = 0; i < 400; ++i) {
      lc_spill.Append(
          Row({Value(int64_t{(i % 2) + 1}), Value(std::string(sp))}));
    }
    DataChunk rc;
    rc.Reserve(2);
    rc.Append(Row({Value(1.0), Value(std::string(sp))}));
    rc.Append(Row({Value(3.0), Value(std::string(sp))}));

    auto coord = std::make_shared<OocSetOpCoordinator>(
        SetOperationKind::kUnion, out_s, std::vector<Schema>{left_s, right_s});
    ASSERT_EQ(coord->ConsumeFromSource(&ctx, 0, lc_small), Status::kSuccess);
    EXPECT_GT(ctx.grant.UsedBytes(), 0U);
    EXPECT_EQ(ctx.grant.ActiveConsumerCount(), 1U);
    ASSERT_EQ(coord->ConsumeFromSource(&ctx, 0, lc_spill), Status::kSuccess);
    ASSERT_EQ(coord->ConsumeFromSource(&ctx, 1, rc), Status::kSuccess);
    auto ssrc_or = coord->Finalize(&ctx);
    ASSERT_TRUE(ssrc_or.HasValue()) << ssrc_or.GetStatus();
    auto ssrc = ssrc_or.MoveValue();
    ASSERT_EQ(ssrc->Open(&ctx), Status::kSuccess);
    std::vector<double> vals;
    while (true) {
      auto more = ssrc->NextBatch(&ctx, &out_c, kDefaultVectorSize);
      ASSERT_TRUE(more.HasValue()) << more.GetStatus();
      if (!more.Value()) {
        break;
      }
      for (size_t i = 0; i < out_c.Size(); ++i) {
        vals.push_back(out_c.RowAt(i)[0].AsDouble());
      }
    }
    ssrc.reset();
    std::sort(vals.begin(), vals.end());
    EXPECT_EQ(vals, (std::vector<double>{1.0, 2.0, 3.0}));
    EXPECT_EQ(ctx.grant.UsedBytes(), 0U);
    EXPECT_EQ(ctx.grant.ActiveConsumerCount(), 0U);
  }
}

// The band candidate range must be correct for every comparison, including
// when equal keys are present (lower_bound vs upper_bound) and when a left
// block contains NULL keys that never match.
TEST(PipelineP3bTest, OocBandJoinCandidatePruningMatchesEveryComparison) {
  TestKnobsGuard guard(64ULL << 10, /*always_spill=*/true);
  Schema s("bj", {Column("k", ValueType::kInt64)});
  Schema out_s("out", {Column("lk", ValueType::kInt64),
                       Column("rk", ValueType::kInt64)});
  const std::vector<int64_t> right_keys{0, 1, 2, 7, 7, 9};
  const auto make_rows = [](const std::vector<int64_t>& keys) {
    std::vector<Row> rows;
    rows.reserve(keys.size());
    for (int64_t k : keys) {
      rows.push_back(Row({Value(k)}));
    }
    return rows;
  };
  // Value() is NULL; the helper above cannot express it via int64_t, so build
  // the left rows explicitly.
  std::vector<Row> lrows{Row({Value()})};
  lrows.reserve(5);
  for (int64_t k : {int64_t{1}, int64_t{1}, int64_t{3}, int64_t{7}}) {
    lrows.push_back(Row({Value(k)}));
  }

  for (BinaryOperation op :
       {BinaryOperation::kLessThan, BinaryOperation::kLessThanEquals,
        BinaryOperation::kGreaterThan, BinaryOperation::kGreaterThanEquals}) {
    size_t expected = 0;
    // The contract order is left-major: every pair of a left row precedes
    // the next left row's, and pairs inside one left row arrive in right-key
    // order (the right stream is already key-ordered).
    std::vector<Row> expected_rows;
    expected_rows.reserve(lrows.size() * right_keys.size());
    for (const auto& l : lrows) {
      if (l[0].IsNull()) {
        continue;
      }
      for (int64_t r : right_keys) {
        const int64_t lv = l[0].value.int_value;
        bool in_band = false;
        switch (op) {
          case BinaryOperation::kLessThan:
            in_band = (lv < r);
            break;
          case BinaryOperation::kLessThanEquals:
            in_band = (lv <= r);
            break;
          case BinaryOperation::kGreaterThan:
            in_band = (lv > r);
            break;
          case BinaryOperation::kGreaterThanEquals:
            in_band = (lv >= r);
            break;
          default:
            break;
        }
        expected += static_cast<size_t>(in_band);
        if (in_band) {
          expected_rows.push_back(Row({Value(lv), Value(r)}));
        }
      }
    }

    OocSetOpInputStream l_in{.source = std::make_shared<RowVectorSource>(lrows),
                             .operators = {},
                             .schema = s};
    OocSetOpInputStream r_in{
        .source = std::make_shared<RowVectorSource>(make_rows(right_keys)),
        .operators = {},
        .schema = s};
    auto bj = std::make_shared<OocBandJoinSource>(
        std::move(l_in), std::move(r_in), 0, 0, op, Expression(), s + s,
        JoinKind::kInner, 2, 2, out_s);
    PipelineGraph g;
    g.output_schema = out_s;
    g.stages.push_back(PipelineStage{.id = 0,
                                     .name = "BJ",
                                     .source = bj,
                                     .operators = {},
                                     .sink = nullptr,
                                     .output_binding = nullptr});
    PipelineExecutor exec(nullptr, std::move(g));
    const std::vector<Row> out = DrainExecutor(&exec);
    ASSERT_EQ(exec.GetStatus().GetCode(), StatusCode::kSuccess);
    EXPECT_EQ(out.size(), static_cast<size_t>(expected))
        << "op=" << static_cast<int>(op);
    EXPECT_EQ(out, expected_rows) << "op=" << static_cast<int>(op);
  }
}

// T3511: a null store is a misconfiguration and must fail Open loudly at
// the Status boundary instead of reporting success and silently yielding
// zero rows from NextBatch.
TEST(PipelineP3bTest, SpillableStoreSourceWithoutStoreFailsOpen) {
  SpillableStoreSource source(nullptr);
  const Status status = source.Open(nullptr);
  EXPECT_NE(status, Status::kSuccess);
  EXPECT_EQ(status.GetCode(), StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace tinylamb
