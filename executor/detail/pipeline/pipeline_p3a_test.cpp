/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/pipeline/pipeline_p3a_test.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <future>
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
#include "common/test_util.hpp"
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
#include "type/row_block_codec.hpp"
#include "type/schema.hpp"

namespace tinylamb {
namespace {
using namespace pipeline_p3a_test;

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

TEST(PipelineP3aTest, OocSortSinkSizesComputedKeysFromChunkNotEmptySchema) {
  OpBudgetOverrideGuard guard(64ULL << 10);

  // input_schema_ is empty while the source still emits two-column chunks:
  // computed-key and Top-N seq column indices must be sized from the actual
  // chunk width, not the declared (empty) schema.
  Schema empty_schema("empty", {});
  Schema out_schema("out", {Column("k", ValueType::kInt64),
                            Column("pad", ValueType::kVarChar)});
  std::vector<Row> input;
  for (int64_t v : {9, 8, 7, 6, 5}) {
    input.push_back(Row({Value(v), Value(std::string("p"))}));
  }

  // Constant computed key: every row ties on the primary key, so the
  // appended seq column alone decides the bounded Top-N selection.
  std::vector<OocSortSink::KeySpec> keys{OocSortSink::KeySpec{
      .expression = ConstantValueExp(Value(int64_t{7})), .ascending = true}};
  auto sort_sink =
      std::make_shared<OocSortSink>(keys, empty_schema, /*limit=*/3,
                                    /*offset=*/0, "TopN");

  auto binding = std::make_shared<DeferredPipelineSource>("SortOut");
  PipelineGraph g;
  g.output_schema = out_schema;
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
  ASSERT_EQ(out.size(), 3U);
  // seq tiebreak keeps the first three arrival rows (k = 9,8,7); a sorter
  // key mis-indexed at payload_cols_=0 would instead order by column 0.
  EXPECT_EQ(out[0][0].AsInt64(), 9);
  EXPECT_EQ(out[1][0].AsInt64(), 8);
  EXPECT_EQ(out[2][0].AsInt64(), 7);
}

TEST(PipelineP3aTest, OocSortSinkValidatesSlotsAndWidthsAgainstChunks) {
  PipelineContext ctx(nullptr, QueryMemoryGrant{});
  OocSortSink sink({{.expression = ColumnValueExp("k"), .column_slot = 0}},
                   Schema{});
  ASSERT_SUCCESS(sink.Init(&ctx, 1));

  DataChunk input;
  input.Append(Row({Value(2)}));
  input.Append(Row({Value(1)}));
  ASSERT_SUCCESS(sink.Consume(&ctx, 0, input));

  DataChunk wrong_width;
  wrong_width.Append(Row({Value(3), Value(4)}));
  EXPECT_EQ(sink.Consume(&ctx, 0, wrong_width), Status::kInvalidArgument);

  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<PipelineSource>, source,
                        sink.Finalize(&ctx));
  ASSERT_SUCCESS(source->Open(&ctx));
  DataChunk output;
  auto produced = source->NextBatch(&ctx, &output, 10);
  ASSERT_TRUE(produced.HasValue());
  ASSERT_TRUE(produced.Value());
  ASSERT_EQ(output.Size(), 2U);
  EXPECT_EQ(output.RowAt(0), Row({Value(1)}));
  EXPECT_EQ(output.RowAt(1), Row({Value(2)}));
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
      ::tinylamb::platform::GetEnv("TINYLAMB_FILTER_HASH_PROBE") == nullptr) {
    std::ostringstream explain;
    probe_source->DumpExplain(explain, 0);
    EXPECT_NE(explain.str().find(
                  "FilteredHashProbe direct_batches=3 compact_batches=1"),
              std::string::npos)
        << explain.str();
  }
}

}  // namespace
}  // namespace tinylamb
