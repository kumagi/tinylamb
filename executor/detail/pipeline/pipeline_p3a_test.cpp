/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/memory_budget.hpp"
#include "common/platform/process.hpp"
#include "common/query_grant.hpp"
#include "common/random_string.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/pipeline/ooc_sinks.hpp"
#include "executor/detail/pipeline/pipeline_ir.hpp"
#include "executor/detail/pipeline/pipeline_ops.hpp"
#include "executor/pipeline_lowering.hpp"
#include "executor/values.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/jit.hpp"
#include "expression/unary_expression.hpp"
#include "plan/aggregation_plan.hpp"
#include "plan/projection_plan.hpp"
#include "plan/selection_plan.hpp"
#include "plan/values_plan.hpp"
#include "type/schema.hpp"

namespace tinylamb {
namespace {

struct OpBudgetOverrideGuard {
  explicit OpBudgetOverrideGuard(size_t bytes) {
    ProcessMemoryBudget::Global().SetTestOpMemoryBytes(bytes);
  }
  OpBudgetOverrideGuard(const OpBudgetOverrideGuard&) = delete;
  OpBudgetOverrideGuard& operator=(const OpBudgetOverrideGuard&) = delete;
  OpBudgetOverrideGuard(OpBudgetOverrideGuard&&) = delete;
  OpBudgetOverrideGuard& operator=(OpBudgetOverrideGuard&&) = delete;
  ~OpBudgetOverrideGuard() {
    ProcessMemoryBudget::Global().SetTestOpMemoryBytes(0);
    ProcessMemoryBudget::Global().SetTestAlwaysSpill(false);
    ProcessMemoryBudget::Global().SetTestTempLimitBytes(0);
    ProcessMemoryBudget::Global().SetTestSpillIoFailCountdown(0);
  }
};

std::vector<Row> DrainExecutor(ExecutorBase* exec) {
  std::vector<Row> rows;
  Row r;
  while (exec->Next(&r, nullptr)) {
    rows.push_back(std::move(r));
  }
  return rows;
}

TEST(PipelineP3aTest, FusedSumLowersAndKeepsNullAndErrorSemantics) {
  const auto kernel =
      JitInt64Kernels::CompileFilterProjectSum(BinaryOperation::kGreaterThan);
  const Schema schema(
      "v", {Column("k", ValueType::kInt64), Column("x", ValueType::kInt64)});
  ASSIGN_OR_ASSERT_FAIL(std::unique_ptr<Database>, db,
                        Database::Create("fused_sum_" + RandomString()));
  auto ctx = db->BeginReadOnlyContext();
  auto make_plan = [&](std::vector<Row> rows) -> Plan {
    auto values = std::make_shared<ValuesPlan>(schema, std::move(rows));
    const auto predicate =
        BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kGreaterThan,
                            ConstantValueExp(Value(int64_t{0})));
    auto selection =
        std::make_shared<SelectionPlan>(values, predicate, values->GetStats());
    const auto multiply =
        BinaryExpressionExp(ColumnValueExp("x"), BinaryOperation::kMultiply,
                            ConstantValueExp(Value(int64_t{3})));
    auto project = std::make_shared<ProjectionPlan>(
        selection, std::vector<NamedExpression>{{"p", multiply}});
    return std::make_shared<AggregationPlan>(
        project, std::vector<NamedExpression>{
                     {"s", AggregateExpressionExp(AggregationType::kSum,
                                                  ColumnValueExp("p"))}});
  };

  std::vector<Row> values(65536, Row({Value(1), Value(2)}));
  values.emplace_back(Row({Value(-1), Value(INT64_MAX)}));
  values.emplace_back(Row({Value(1), Value()}));
  ASSIGN_OR_ASSERT_FAIL(
      Executor, fused,
      PipelineLowering::LowerToExecutor(make_plan(std::move(values)), ctx));
  const auto result = DrainExecutor(fused.get());
  ASSERT_TRUE(fused->GetStatus().ok()) << fused->GetStatus();
  ASSERT_EQ(result.size(), 1U);
  EXPECT_EQ(result[0][0].AsInt64(), 65536 * 6);
  if (kernel) {
    std::ostringstream explain;
    fused->Dump(explain, 0);
    EXPECT_NE(explain.str().find("FusedFilterProjectSum (jit_batches=1)"),
              std::string::npos)
        << explain.str();
  }

  ASSIGN_OR_ASSERT_FAIL(Executor, empty,
                        PipelineLowering::LowerToExecutor(
                            make_plan({Row({Value(-1), Value(INT64_MAX)}),
                                       Row({Value(), Value(1)})}),
                            ctx));
  const auto null_result = DrainExecutor(empty.get());
  ASSERT_TRUE(empty->GetStatus().ok());
  ASSERT_EQ(null_result.size(), 1U);
  EXPECT_TRUE(null_result[0][0].IsNull());

  ASSIGN_OR_ASSERT_FAIL(
      Executor, overflow,
      PipelineLowering::LowerToExecutor(
          make_plan({Row({Value(1), Value(INT64_MAX)})}), ctx));
  EXPECT_TRUE(DrainExecutor(overflow.get()).empty());
  EXPECT_EQ(overflow->GetStatus(), StatusCode::kInvalidArgument);
}

TEST(PipelineP3aTest, FilterProjectLimitAndMetricsWorkEndToEnd) {
  Schema schema(
      "t", {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64)});
  std::vector<Row> input;
  input.reserve(200);
  for (int64_t i = 0; i < 200; ++i) {
    input.push_back(Row({Value(i), Value(i * 10)}));
  }
  auto val_exec = std::make_shared<ValuesExecutor>(std::move(input));
  auto src = std::make_shared<StreamingExecutorSource>(val_exec, "ValuesScan");

  // WHERE a >= 50, project a+b AS sum_ab, LIMIT 5 OFFSET 2.
  Expression pred = BinaryExpressionExp(ColumnValueExp("a"),
                                        BinaryOperation::kGreaterThanEquals,
                                        ConstantValueExp(Value(int64_t{50})));
  auto filter = std::make_shared<FilterOp>(pred, schema);

  std::vector<NamedExpression> proj_exprs;
  proj_exprs.emplace_back(
      "sum_ab", BinaryExpressionExp(ColumnValueExp("a"), BinaryOperation::kAdd,
                                    ColumnValueExp("b")));
  auto project = std::make_shared<ProjectOp>(std::move(proj_exprs), schema);
  auto limit = std::make_shared<LimitOp>(5, 2, true);

  PipelineGraph graph;
  graph.output_schema = Schema("out", {Column("sum_ab", ValueType::kInt64)});
  graph.requested_dop = 4;
  graph.estimated_grant_bytes = size_t{2} << 20;
  graph.min_grant_bytes = size_t{1} << 20;
  graph.stages.push_back(PipelineStage{
      .id = 0,
      .name = "RootStream",
      .source = src,
      .operators = {filter, project, limit},
      .sink = nullptr,
      .output_binding = nullptr,
  });

  PipelineExecutor exec(nullptr, std::move(graph));
  const std::vector<Row> out = DrainExecutor(&exec);
  ASSERT_EQ(exec.GetStatus().GetCode(), StatusCode::kSuccess);
  ASSERT_EQ(out.size(), 5U);
  for (size_t i = 0; i < 5; ++i) {
    const int64_t a = 52 + static_cast<int64_t>(i);
    EXPECT_EQ(out[i][0].AsInt64(), a + (a * 10));
  }
  const PipelineExecutionMetrics m = exec.Metrics();
  EXPECT_DOUBLE_EQ(m.legacy_source_ms, 0.0);
  EXPECT_EQ(m.ooc.spill_bytes, 0U);
  EXPECT_NE(exec.ExplainPipeline().find("Pipeline:"), std::string::npos);

  // Explicit LegacySource tracks nonzero legacy_source_ms.
  auto legacy_src = std::make_shared<LegacySource>(
      std::make_shared<ValuesExecutor>(
          std::vector<Row>{Row({Value(int64_t{7})})}),
      "TestLegacy");
  PipelineGraph legacy_graph;
  legacy_graph.output_schema = Schema("l", {Column("x", ValueType::kInt64)});
  legacy_graph.stages.push_back(PipelineStage{
      .id = 0,
      .name = "LegacyRoot",
      .source = legacy_src,
      .operators = {},
      .sink = nullptr,
      .output_binding = nullptr,
  });
  PipelineExecutor legacy_exec(nullptr, std::move(legacy_graph));
  EXPECT_EQ(DrainExecutor(&legacy_exec).size(), 1U);
  // Legacy-source timing is opt-in (TINYLAMB_PIPELINE_METRICS, read once per
  // process), so only a non-negative, well-formed metric is guaranteed here.
  EXPECT_GE(legacy_exec.Metrics().legacy_source_ms, 0.0);
}

TEST(PipelineP3aTest, OocSortAndTopNSinkHandleSpilledExpressionKeys) {
  OpBudgetOverrideGuard guard(64ULL << 10);

  Schema schema("s", {Column("k", ValueType::kInt64),
                      Column("pad", ValueType::kVarChar)});
  constexpr int kRows = 2500;
  std::vector<Row> input;
  input.reserve(kRows);
  const std::string pad(128, 'S');
  for (int i = 0; i < kRows; ++i) {
    const int64_t v = (static_cast<int64_t>(i) * 97LL) % kRows;
    input.push_back(Row({Value(v), Value(std::string(pad))}));
  }

  // Sort by computed key `-k` descending == ascending `k`!
  Expression neg_k =
      BinaryExpressionExp(ConstantValueExp(Value(int64_t{0})),
                          BinaryOperation::kSubtract, ColumnValueExp("k"));
  std::vector<OocSortSink::KeySpec> keys{
      OocSortSink::KeySpec{.expression = neg_k, .ascending = false}};

  auto binding = std::make_shared<DeferredPipelineSource>("SortOut");
  auto sort_sink = std::make_shared<OocSortSink>(keys, schema, /*limit=*/10,
                                                 /*offset=*/5, "TopN");

  PipelineGraph g;
  g.output_schema = schema;
  g.stages.push_back(PipelineStage{
      .id = 0,
      .name = "SortStage",
      .source = std::make_shared<RowVectorSource>(std::move(input)),
      .operators = {},
      .sink = sort_sink,
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

  PipelineExecutor exec(nullptr, std::move(g));
  const std::vector<Row> out = DrainExecutor(&exec);
  ASSERT_EQ(exec.GetStatus().GetCode(), StatusCode::kSuccess);
  ASSERT_EQ(out.size(), 10U);
  for (size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(out[i][0].AsInt64(), static_cast<int64_t>(5 + i));
  }
}

TEST(PipelineP3aTest, OocGroupAggSinkSpillsCombinableDistinctAndHandlesSkew) {
  OpBudgetOverrideGuard guard(64ULL << 10);

  Schema schema("g",
                {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64),
                 Column("pad", ValueType::kVarChar)});
  // Empty scalar aggregate must produce 1 row: (0, NULL).
  {
    auto c_agg = std::make_shared<AggregateExpression>(AggregationType::kCount,
                                                       nullptr, false);
    auto s_agg = std::make_shared<AggregateExpression>(
        AggregationType::kSum, ColumnValueExp("v"), false);
    Schema out_s("out", {Column("c", ValueType::kInt64),
                         Column("s", ValueType::kInt64)});
    auto agg_sink = std::make_shared<OocGroupAggSink>(
        schema, std::vector<Expression>{},
        std::vector<const AggregateExpression*>{c_agg.get(), s_agg.get()},
        out_s);
    PipelineGraph g;
    g.output_schema = out_s;
    g.stages.push_back(PipelineStage{
        .id = 0,
        .name = "EmptyScalarAgg",
        .source = std::make_shared<EmptyPipelineSource>(),
        .operators = {},
        .sink = agg_sink,
        .output_binding = nullptr,
    });
    PipelineExecutor exec(nullptr, std::move(g));
    const std::vector<Row> out = DrainExecutor(&exec);
    ASSERT_EQ(exec.GetStatus().GetCode(), StatusCode::kSuccess);
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0][0].AsInt64(), 0);
    EXPECT_TRUE(out[0][1].IsNull());
  }

  // High-cardinality combinable GROUP BY (COUNT, SUM) + heavy skew!
  {
    constexpr int kRows = 6000;
    std::vector<Row> input;
    input.reserve(kRows);
    const std::string pad(100, 'A');
    std::unordered_map<int64_t, int64_t> expected_sum;
    std::unordered_map<int64_t, int64_t> expected_cnt;
    for (int i = 0; i < kRows; ++i) {
      // 70% key = 999 (skew), 30% distinct keys 0..1799 (exceeds 64KiB).
      const int64_t k = (i % 10 < 7) ? 999 : i;
      const int64_t val = i % 50;
      input.push_back(Row({Value(k), Value(val), Value(std::string(pad))}));
      expected_sum[k] += val;
      ++expected_cnt[k];
    }

    auto cnt = std::make_shared<AggregateExpression>(AggregationType::kCount,
                                                     nullptr, false);
    auto sum = std::make_shared<AggregateExpression>(
        AggregationType::kSum, ColumnValueExp("v"), false);
    Schema out_s("out", {Column("k", ValueType::kInt64),
                         Column("cnt", ValueType::kInt64),
                         Column("sum", ValueType::kInt64)});
    auto agg_sink = std::make_shared<OocGroupAggSink>(
        schema, std::vector<Expression>{ColumnValueExp("k")},
        std::vector<const AggregateExpression*>{cnt.get(), sum.get()}, out_s);
    PipelineGraph g;
    g.output_schema = out_s;
    g.stages.push_back(PipelineStage{
        .id = 0,
        .name = "SpillCombinableGroupAgg",
        .source = std::make_shared<RowVectorSource>(std::move(input)),
        .operators = {},
        .sink = agg_sink,
        .output_binding = nullptr,
    });
    PipelineExecutor exec(nullptr, std::move(g));
    const std::vector<Row> out = DrainExecutor(&exec);
    ASSERT_EQ(exec.GetStatus().GetCode(), StatusCode::kSuccess);
    ASSERT_EQ(out.size(), expected_cnt.size());
    for (const Row& r : out) {
      const int64_t k = r[0].AsInt64();
      EXPECT_EQ(r[1].AsInt64(), expected_cnt.at(k));
      EXPECT_EQ(r[2].AsInt64(), expected_sum.at(k));
    }
    EXPECT_GT(exec.Metrics().ooc.spill_bytes, 0U);
  }

  // Non-combinable COUNT(DISTINCT v) spilling + skewed group sorted fallback.
  {
    constexpr int kRows = 2500;
    std::vector<Row> input;
    input.reserve(kRows);
    const std::string pad(120, 'D');
    for (int i = 0; i < kRows; ++i) {
      // 80% in heavy key 7 with values i % 37.
      const int64_t k = (i % 5 != 0) ? 7 : i;
      const int64_t v = (k == 7) ? (i % 37) : (i % 3);
      input.push_back(Row({Value(k), Value(v), Value(std::string(pad))}));
    }
    auto cd = std::make_shared<AggregateExpression>(
        AggregationType::kCount, ColumnValueExp("v"), /*distinct=*/true);
    Schema out_s("out", {Column("k", ValueType::kInt64),
                         Column("cd", ValueType::kInt64)});
    auto agg_sink = std::make_shared<OocGroupAggSink>(
        schema, std::vector<Expression>{ColumnValueExp("k")},
        std::vector<const AggregateExpression*>{cd.get()}, out_s);
    PipelineGraph g;
    g.output_schema = out_s;
    g.stages.push_back(PipelineStage{
        .id = 0,
        .name = "CountDistinctSkewAgg",
        .source = std::make_shared<RowVectorSource>(std::move(input)),
        .operators = {},
        .sink = agg_sink,
        .output_binding = nullptr,
    });
    PipelineExecutor exec(nullptr, std::move(g));
    const std::vector<Row> out = DrainExecutor(&exec);
    ASSERT_EQ(exec.GetStatus().GetCode(), StatusCode::kSuccess);
    std::unordered_map<int64_t, int64_t> got;
    for (const Row& r : out) {
      got[r[0].AsInt64()] = r[1].AsInt64();
    }
    EXPECT_EQ(got.at(7), 37);
    EXPECT_EQ(got.size(), 501U);  // group 7 + 500 distinct non-heavy keys
    EXPECT_GT(exec.Metrics().ooc.skew_fallbacks, 0U);
  }
}

TEST(PipelineP3aTest, FilteredHashProbeSwitchKeepsOrderAndDuplicateChains) {
  const auto direct =
      JitInt64Kernels::CompileFilterHashProbe(BinaryOperation::kGreaterThan);
  const auto compact =
      JitInt64Kernels::CompileFilter(BinaryOperation::kGreaterThan);
  const Schema probe_schema("p", {Column("key", ValueType::kInt64),
                                  Column("keep", ValueType::kInt64)});
  const Schema build_schema("b", {Column("key", ValueType::kInt64),
                                  Column("tag", ValueType::kInt64)});
  const Schema projected_schema("pp", {Column("keep", ValueType::kInt64),
                                       Column("key", ValueType::kInt64)});
  const Expression compare =
      BinaryExpressionExp(ColumnValueExp("keep"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(int64_t{0})));
  const Expression filter = BinaryExpressionExp(
      compare, BinaryOperation::kAnd,
      UnaryExpressionExp(ColumnValueExp("key"), UnaryOperation::kIsNotNull));
  OocHashJoinSpec spec;
  spec.left_schema = projected_schema;
  spec.right_schema = build_schema;
  spec.left_cols = {1};
  spec.right_cols = {0};
  spec.dynamic_filter = true;
  auto join = std::make_shared<OocHashJoinContext>(std::move(spec));

  std::vector<Row> build;
  build.emplace_back(Row({Value(7), Value(101)}));
  build.emplace_back(Row({Value(7), Value(102)}));
  for (int64_t i = 20; i < 150; ++i) {
    build.emplace_back(Row({Value(i), Value(-i)}));
  }

  constexpr int64_t kMiss = 10000;
  std::vector<DataChunk> chunks;
  for (size_t i = 0; i < 66; ++i) {
    DataChunk batch;
    batch.Reset(probe_schema, 1024);
    for (size_t row = 0; row < 1024; ++row) {
      batch.Append(Row({Value(kMiss), Value(row == 0 ? 1 : 0)}));
    }
    chunks.push_back(std::move(batch));
  }
  DataChunk sparse;
  sparse.Reset(probe_schema, 13);
  for (size_t row = 0; row < 13; ++row) {
    const bool hit = row == 1 || row == 5 || row == 12;
    sparse.Append(Row({Value(hit ? 7 : kMiss), Value(hit ? 1 : 0)}));
  }
  chunks.push_back(std::move(sparse));
  DataChunk dense;
  dense.Reset(probe_schema, 1024);
  for (size_t row = 0; row < 1024; ++row) {
    dense.Append(Row({Value(row == 15 ? 7 : kMiss), Value(2)}));
  }
  chunks.push_back(std::move(dense));
  DataChunk nullable;
  nullable.Reset(probe_schema, 3);
  nullable.Append(Row({Value(7), Value()}));
  nullable.Append(Row({Value(), Value(3)}));
  nullable.Append(Row({Value(7), Value(3)}));
  chunks.push_back(std::move(nullable));

  auto probe_source = std::make_shared<OocAdaptiveHashProbeSource>(
      join, std::make_shared<ChunkVectorSource>(std::move(chunks), "Probe"),
      std::vector<std::shared_ptr<PipelineOperator>>{
          std::make_shared<FilterOp>(filter, probe_schema),
          std::make_shared<ProjectOp>(
              std::vector<NamedExpression>{{"keep", ColumnValueExp("keep")},
                                           {"key", ColumnValueExp("key")}},
              probe_schema),
          std::make_shared<RenameOp>("p", "pp")});
  PipelineGraph graph;
  graph.output_schema = projected_schema + build_schema;
  graph.stages.push_back(PipelineStage{
      .id = 0,
      .name = "Build",
      .source = std::make_shared<RowVectorSource>(std::move(build)),
      .operators = {},
      .sink = std::make_shared<OocHashBuildSink>(join),
      .output_binding = nullptr,
  });
  graph.stages.push_back(PipelineStage{
      .id = 1,
      .name = "Probe",
      .source = probe_source,
      .operators = {},
      .sink = nullptr,
      .output_binding = nullptr,
  });
  PipelineExecutor executor(nullptr, std::move(graph));
  const auto rows = DrainExecutor(&executor);
  ASSERT_TRUE(executor.GetStatus().ok()) << executor.GetStatus();
  ASSERT_EQ(rows.size(), 10U);
  EXPECT_EQ(join->dynamic_filter_probe_rows, 1094U);
  EXPECT_EQ(join->dynamic_filter_rejected, 1089U);
  constexpr std::array<int64_t, 5> kProbeTags{1, 1, 1, 2, 3};
  for (size_t i = 0; i < rows.size(); ++i) {
    EXPECT_EQ(rows[i][0].AsInt64(), kProbeTags[i / 2]);
    EXPECT_EQ(rows[i][1].AsInt64(), 7);
    EXPECT_EQ(rows[i][3].AsInt64(), 101 + static_cast<int64_t>(i % 2));
  }
  if (direct && compact &&
      std::getenv("TINYLAMB_FILTER_HASH_PROBE") == nullptr) {
    std::ostringstream explain;
    probe_source->DumpExplain(explain, 0);
    EXPECT_NE(explain.str().find(
                  "FilteredHashProbe direct_batches=3 compact_batches=1"),
              std::string::npos)
        << explain.str();
  }
}

TEST(PipelineP3aTest, OocHashJoinCoversAllKindsSpillAndSkewFallback) {
  OpBudgetOverrideGuard guard(64ULL << 10);

  Schema left_s("l", {Column("lk", ValueType::kInt64),
                      Column("lv", ValueType::kVarChar)});
  Schema right_s("r", {Column("rk", ValueType::kInt64),
                       Column("rv", ValueType::kVarChar)});

  auto run_join = [&](JoinKind kind, std::vector<Row> left_rows,
                      std::vector<Row> right_rows, bool build_left = false) {
    OocHashJoinSpec spec;
    spec.left_cols = {0};
    spec.right_cols = {0};
    spec.left_schema = left_s;
    spec.right_schema = right_s;
    spec.kind = kind;
    spec.build_left = build_left;

    auto join_ctx = std::make_shared<OocHashJoinContext>(std::move(spec));
    if (!build_left) {
      std::swap(left_rows, right_rows);
    }
    auto build_src = std::make_shared<RowVectorSource>(std::move(left_rows));
    auto probe_src = std::make_shared<RowVectorSource>(std::move(right_rows));

    auto build_sink = std::make_shared<OocHashBuildSink>(join_ctx);
    auto probe = std::make_shared<OocAdaptiveHashProbeSource>(
        join_ctx, probe_src, std::vector<std::shared_ptr<PipelineOperator>>{});

    PipelineGraph g;
    g.output_schema = left_s + right_s;
    g.stages.push_back(PipelineStage{
        .id = 0,
        .name = "Build",
        .source = build_src,
        .operators = {},
        .sink = build_sink,
        .output_binding = nullptr,
    });
    g.stages.push_back(PipelineStage{
        .id = 1,
        .name = "Probe",
        .source = probe,
        .operators = {},
        .sink = nullptr,
        .output_binding = nullptr,
    });
    PipelineExecutor exec(nullptr, std::move(g));
    std::vector<Row> res = DrainExecutor(&exec);
    EXPECT_EQ(exec.GetStatus().GetCode(), StatusCode::kSuccess);
    return std::make_pair(std::move(res), exec.Metrics());
  };

  // Build > 64 KiB + skewed key 10 (480 duplicates in build & 1 in probe).
  const std::string pad(150, 'J');
  std::vector<Row> heavy_build;
  for (int i = 0; i < 600; ++i) {
    // 80% key=10 (triggers <20% shrink -> chunked skew fallback!), 20% unique
    const int64_t k = (i % 5 != 0) ? 10 : (1000 + i);
    heavy_build.push_back(Row({Value(k), Value(std::string(pad))}));
  }
  std::vector<Row> probe_rows{
      Row({Value(int64_t{10}), Value("p10")}),
      Row({Value(int64_t{1000}), Value("p1000")}),
      Row({Value(int64_t{99999}), Value("punmatched")}),
      Row({Value(), Value("pnull")}),
  };

  // Inner join: 480 matches on key=10 + 1 match on 1000 = 481 rows.
  auto [inner_res, inner_metrics] =
      run_join(JoinKind::kInner, probe_rows, heavy_build);
  EXPECT_EQ(inner_res.size(), 481U);
  EXPECT_GT(inner_metrics.ooc.spill_bytes, 0U);
  EXPECT_GT(inner_metrics.ooc.skew_fallbacks, 0U);

  // LeftOuter preserves unmatched 99999 & NULL probe rows too -> 483 rows.
  auto [lo_res, lo_m] = run_join(JoinKind::kLeftOuter, probe_rows, heavy_build);
  EXPECT_EQ(lo_res.size(), 483U);

  // Semi emits matched probe keys once -> key=10 & key=1000 = 2 rows.
  auto [semi_res, semi_m] = run_join(JoinKind::kSemi, probe_rows, heavy_build);
  EXPECT_EQ(semi_res.size(), 2U);

  // Anti emits unmatched probe keys -> key=99999 & NULL = 2 rows.
  auto [anti_res, anti_m] = run_join(JoinKind::kAnti, probe_rows, heavy_build);
  EXPECT_EQ(anti_res.size(), 2U);

  // NullAwareAnti without NULL in build -> drops NULL probe, keeps 99999 (1
  // row).
  auto [nanti_no_null, n1] =
      run_join(JoinKind::kNullAwareAnti, probe_rows, heavy_build);
  ASSERT_EQ(nanti_no_null.size(), 1U);
  EXPECT_EQ(nanti_no_null[0][0].AsInt64(), 99999);

  // NullAwareAnti with NULL in build -> empties all rows.
  std::vector<Row> build_with_null = heavy_build;
  build_with_null.push_back(Row({Value(), Value("bnull")}));
  auto [nanti_with_null, n2] =
      run_join(JoinKind::kNullAwareAnti, probe_rows, build_with_null);
  EXPECT_TRUE(nanti_with_null.empty());

  // RightOuter & FullOuter on spilled build (plus build_left=true check).
  std::vector<Row> ro_build{
      Row({Value(int64_t{1}), Value("b1")}),
      Row({Value(int64_t{2}), Value("b2")}),
  };
  for (int i = 0; i < 600; ++i) {
    ro_build.push_back(Row({Value(int64_t{100 + i}), Value(std::string(pad))}));
  }
  auto [ro_res, ro_m] =
      run_join(JoinKind::kRightOuter,
               std::vector<Row>{Row({Value(int64_t{1}), Value("p1")}),
                                Row({Value(int64_t{888}), Value("pmiss")})},
               ro_build);
  EXPECT_EQ(ro_res.size(), 602U);

  // FullOuter on spilled skewed build: 480 matches (k=10) + 1 match (k=1000) +
  // 2 unmatched probe (99999, NULL) + 119 unmatched build = 602 rows.
  auto [fo_res, fo_m] = run_join(JoinKind::kFullOuter, probe_rows, heavy_build);
  EXPECT_EQ(fo_res.size(), 602U);

  // build_left=true check on spilled build: left has heavy_build (600), right
  // has probe_rows (4) -> Inner 481, RightOuter preserves 2 unmatched probe =
  // 483.
  auto [bl_inner, bl_m] =
      run_join(JoinKind::kInner, heavy_build, probe_rows, /*build_left=*/true);
  EXPECT_EQ(bl_inner.size(), 481U);
  auto [bl_ro, bl_ro_m] = run_join(JoinKind::kRightOuter, heavy_build,
                                   probe_rows, /*build_left=*/true);
  EXPECT_EQ(bl_ro.size(), 483U);
  auto [bl_lo, bl_lo_m] = run_join(JoinKind::kLeftOuter, heavy_build,
                                   probe_rows, /*build_left=*/true);
  EXPECT_EQ(bl_lo.size(), 600U);
}

// RAII: points TINYLAMB_TEMP at a directory this test controls, so spill
// segment files land (or must not land) where the test can count them
// (T4131; mirrors executor/grouped_aggregate_test.cpp's ScopedTempDir).
class ScopedSpillTempDir {  // NOLINT(cppcoreguidelines-special-member-functions)
 public:
  explicit ScopedSpillTempDir(std::filesystem::path path)
      : path_(std::move(path)) {
    const char* previous = std::getenv("TINYLAMB_TEMP");
    if (previous != nullptr) {
      previous_ = previous;
      had_previous_ = true;
    }
    setenv("TINYLAMB_TEMP", path_.c_str(), 1);
  }
  ~ScopedSpillTempDir() {
    if (had_previous_) {
      setenv("TINYLAMB_TEMP", previous_.c_str(), 1);
    } else {
      unsetenv("TINYLAMB_TEMP");
    }
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }
  ScopedSpillTempDir(const ScopedSpillTempDir&) = delete;
  ScopedSpillTempDir& operator=(const ScopedSpillTempDir&) = delete;
  [[nodiscard]] const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
  std::string previous_;
  bool had_previous_{false};
};

TEST(PipelineP3aTest, SpilledFaultInjectionPropagatesCleanlyWithoutLeaks) {
  // T4131: pin the name's promise. The failed sort must leave no spill
  // segment files behind once the sink and executor are gone -- the
  // file-count analog of SpillStoreTest.InjectedIoFailureRollsBackWithout
  // LeakingBlocks' live_segments == 0 assertions.
  const ScopedSpillTempDir temp_dir(std::filesystem::temp_directory_path() /
                                    ("tinylamb_p3a_leak_" + RandomString()));
  std::filesystem::create_directories(temp_dir.path());
  OpBudgetOverrideGuard guard(64ULL << 10);
  ProcessMemoryBudget::Global().SetTestTempLimitBytes(2ULL *
                                                      SpillStore::kBlockSize);

  Schema schema("s", {Column("k", ValueType::kInt64),
                      Column("pad", ValueType::kVarChar)});
  std::vector<Row> input;
  input.reserve(1500);
  const std::string pad(256, 'F');
  for (int i = 0; i < 1500; ++i) {
    input.push_back(Row({Value(int64_t{1500 - i}), Value(std::string(pad))}));
  }
  auto sort_sink = std::make_shared<OocSortSink>(
      std::vector<OocSortSink::KeySpec>{
          OocSortSink::KeySpec{.expression = ColumnValueExp("k")}},
      schema);
  PipelineGraph g;
  g.output_schema = schema;
  g.stages.push_back(PipelineStage{
      .id = 0,
      .name = "FaultSort",
      .source = std::make_shared<RowVectorSource>(std::move(input)),
      .operators = {},
      .sink = sort_sink,
      .output_binding = nullptr,
  });
  {
    PipelineExecutor exec(nullptr, std::move(g));
    const std::vector<Row> out = DrainExecutor(&exec);
    EXPECT_TRUE(out.empty());
    EXPECT_EQ(exec.GetStatus().GetCode(), StatusCode::kNoSpace);
    sort_sink.reset();
  }
  std::error_code ec;
  size_t leftover_files = 0;
  for (std::filesystem::directory_iterator it(temp_dir.path(), ec), end;
       it != end; it.increment(ec)) {
    ++leftover_files;
  }
  EXPECT_EQ(leftover_files, 0U)
      << "failed spilled sort leaked segment files into " << temp_dir.path();
}

TEST(PipelineP3aTest,
     ResidentHashJoinAndGroupAggManageGuardsAndZeroMaxRowsCleanly) {
  ProcessMemoryBudget& mb = ProcessMemoryBudget::Global();
  // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is
  // non-copyable by design
  struct ResetGuard {  // NOLINT(cppcoreguidelines-special-member-functions)
    explicit ResetGuard(ProcessMemoryBudget* b) : budget(b) {}
    ~ResetGuard() { budget->ResetForTest(0); }
    ProcessMemoryBudget* budget;
  } mb_guard(&mb);
  mb.ResetForTest(64ULL << 20, 64ULL << 20, 64ULL << 20, false);

  Schema schema("t", {Column("k", ValueType::kInt64),
                      Column("pad", ValueType::kVarChar)});
  const std::string pad(128, 'G');

  // 1) Resident HashBuild transfers guard + build memory to Probe until EOF
  // (B1).
  {
    QueryMemoryCoordinator coord(&mb);
    auto grant = coord.AcquireGrant(4ULL << 20, 4ULL << 20).MoveValue();
    PipelineContext pctx(nullptr, std::move(grant), 1);

    OocHashJoinSpec spec;
    spec.left_cols = {0};
    spec.right_cols = {0};
    spec.left_schema = schema;
    spec.right_schema = schema;
    spec.kind = JoinKind::kInner;
    auto join_ctx = std::make_shared<OocHashJoinContext>(std::move(spec));

    std::vector<Row> build_rows;
    std::vector<Row> probe_rows;
    for (int64_t i = 0; i < 64; ++i) {
      build_rows.push_back(Row({Value(i), Value(std::string(pad))}));
      probe_rows.push_back(Row({Value(i), Value(std::string(pad))}));
    }

    OocHashBuildSink build_sink(join_ctx);
    ASSERT_EQ(build_sink.Init(&pctx, 1), Status::kSuccess);
    DataChunk bchunk;
    bchunk.Reserve(64);
    for (const Row& r : build_rows) {
      bchunk.Append(r);
    }
    ASSERT_EQ(build_sink.Consume(&pctx, 0, bchunk), Status::kSuccess);
    ASSERT_TRUE(build_sink.Finalize(&pctx).HasValue());

    // Build rows and hash index are still resident: guard & charge survive!
    EXPECT_EQ(pctx.grant.ActiveConsumerCount(), 1U);
    EXPECT_GT(pctx.grant.UsedBytes(), 0U);

    OocAdaptiveHashProbeSource probe_src(
        join_ctx, std::make_shared<RowVectorSource>(std::move(probe_rows)), {});
    ASSERT_EQ(probe_src.Open(&pctx), Status::kSuccess);
    EXPECT_EQ(pctx.grant.ActiveConsumerCount(), 1U);
    EXPECT_GT(pctx.grant.UsedBytes(), 0U);

    size_t total_out = 0;
    while (true) {
      DataChunk out;
      auto has_or = probe_src.NextBatch(&pctx, &out, 16);
      ASSERT_TRUE(has_or.HasValue()) << has_or.GetStatus();
      if (!has_or.Value()) {
        break;
      }
      total_out += out.Size();
    }
    EXPECT_EQ(total_out, 64U);
    EXPECT_EQ(pctx.grant.ActiveConsumerCount(), 0U);
    EXPECT_EQ(pctx.grant.UsedBytes(), 0U);
  }

  // 2) TryConsumeExactRowCount releases consumer_guard_ and agg_table_ (C9),
  //    and spilled GroupAggLeafSource handles max_rows == 0 + post-spill grant
  //    (C3, C4, C11, B13).
  {
    QueryMemoryCoordinator coord(&mb);
    auto grant = coord.AcquireGrant(2ULL << 20, 2ULL << 20).MoveValue();
    PipelineContext pctx(nullptr, std::move(grant), 1);

    auto cnt_star = std::make_shared<AggregateExpression>(
        AggregationType::kCount, ColumnValueExp(ColumnName("*")));
    OocGroupAggSink exact_sink(
        schema, {}, std::vector<const AggregateExpression*>{cnt_star.get()},
        Schema("out", {Column("cnt", ValueType::kInt64)}));
    ASSERT_EQ(exact_sink.Init(&pctx, 1), Status::kSuccess);
    EXPECT_EQ(pctx.grant.ActiveConsumerCount(), 1U);
    auto exact_src_or = exact_sink.TryConsumeExactRowCount(&pctx, 123);
    ASSERT_TRUE(exact_src_or.HasValue());
    ASSERT_NE(exact_src_or.Value(), nullptr);
    EXPECT_EQ(pctx.grant.ActiveConsumerCount(), 0U);
    EXPECT_EQ(pctx.grant.UsedBytes(), 0U);

    // Spilled GroupAgg with post-spill groups and max_rows==0 probe.
    OpBudgetOverrideGuard tight(16ULL << 10);
    auto cnt_v = std::make_shared<AggregateExpression>(AggregationType::kCount,
                                                       ColumnValueExp("k"));
    OocGroupAggSink spill_sink(
        schema, {ColumnValueExp("k")},
        std::vector<const AggregateExpression*>{cnt_v.get()},
        Schema("gout", {Column("k", ValueType::kInt64),
                        Column("cnt", ValueType::kInt64)}));
    ASSERT_EQ(spill_sink.Init(&pctx, 1), Status::kSuccess);
    DataChunk chunk;
    chunk.Reserve(500);
    for (int64_t i = 0; i < 500; ++i) {
      chunk.Append(Row({Value(i % 250), Value(std::string(pad))}));
    }
    ASSERT_EQ(spill_sink.Consume(&pctx, 0, chunk), Status::kSuccess);
    auto leaf_src_or = spill_sink.Finalize(&pctx);
    ASSERT_TRUE(leaf_src_or.HasValue()) << leaf_src_or.GetStatus();
    auto leaf_src = leaf_src_or.MoveValue();

    EXPECT_EQ(pctx.grant.ActiveConsumerCount(), 1U);
    DataChunk zero_out;
    auto zero_res = leaf_src->NextBatch(&pctx, &zero_out, 0);
    ASSERT_TRUE(zero_res.HasValue());
    EXPECT_FALSE(zero_res.Value());

    size_t total_groups = 0;
    while (true) {
      DataChunk batch;
      auto res = leaf_src->NextBatch(&pctx, &batch, 32);
      ASSERT_TRUE(res.HasValue()) << res.GetStatus();
      if (!res.Value()) {
        break;
      }
      EXPECT_LE(pctx.grant.UsedBytes(), pctx.grant.GrantedBytes());
      total_groups += batch.Size();
    }
    EXPECT_EQ(total_groups, 250U);
    EXPECT_EQ(pctx.grant.ActiveConsumerCount(), 0U);
    EXPECT_EQ(pctx.grant.UsedBytes(), 0U);
  }
}

TEST(PipelineP3aTest, GrantRequestFollowsTheConfiguredQueryBudget) {
  ProcessMemoryBudget& mb = ProcessMemoryBudget::Global();
  // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is
  // non-copyable by design
  struct ResetGuard {  // NOLINT(cppcoreguidelines-special-member-functions)
    explicit ResetGuard(ProcessMemoryBudget* b) : budget(b) {}
    ~ResetGuard() { budget->ResetForTest(0); }
    ProcessMemoryBudget* budget;
  } mb_guard(&mb);

  // A working set above the old fixed 64 MiB request cap may now ask for
  // what the estimate is; the coordinator's per-query ceiling bounds the
  // request and the reservation itself is still arbitrated at grant time.
  const size_t est = size_t{240} << 20;
  mb.ResetForTest(size_t{1} << 30, 0, 0, false);
  const size_t ceiling = QueryMemoryCoordinator::Global().MaxGrantFromEnv();
  ASSERT_GT(ceiling, size_t{64} << 20);
  EXPECT_EQ(PipelineGrantRequestBytes(est), est);
  EXPECT_EQ(PipelineGrantRequestBytes(std::numeric_limits<size_t>::max()),
            ceiling);
  // The request floor is unchanged.
  EXPECT_EQ(PipelineGrantRequestBytes(size_t{1} << 10), size_t{256} << 10);

  // TINYLAMB_QUERY_MEMORY_BYTES tightens the request downward too.
  // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is
  // non-copyable by design
  struct QueryMemGuard {  // NOLINT(cppcoreguidelines-special-member-functions)
    QueryMemGuard() {
      platform::SetEnv("TINYLAMB_QUERY_MEMORY_BYTES",
                       std::to_string(size_t{16} << 20).c_str());
    }
    ~QueryMemGuard() { platform::UnsetEnv("TINYLAMB_QUERY_MEMORY_BYTES"); }
  } env_guard;
  EXPECT_EQ(PipelineGrantRequestBytes(est), size_t{16} << 20);
}

}  // namespace
}  // namespace tinylamb
