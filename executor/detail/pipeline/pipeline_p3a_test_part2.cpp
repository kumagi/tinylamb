/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
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
#include "executor/detail/pipeline/pipeline_p3a_test.hpp"
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
  ro_build.reserve(602);
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
class
    ScopedSpillTempDir {  // NOLINT(cppcoreguidelines-special-member-functions)
 public:
  explicit ScopedSpillTempDir(std::filesystem::path path)
      : path_(std::move(path)) {
    const char* previous = ::tinylamb::platform::GetEnv("TINYLAMB_TEMP");
    if (previous != nullptr) {
      previous_ = previous;
      had_previous_ = true;
    }
    ::tinylamb::platform::SetEnv("TINYLAMB_TEMP", path_.c_str(), 1);
  }
  ~ScopedSpillTempDir() {
    if (had_previous_) {
      ::tinylamb::platform::SetEnv("TINYLAMB_TEMP", previous_.c_str(), 1);
    } else {
      ::tinylamb::platform::UnsetEnv("TINYLAMB_TEMP");
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
    auto grant_or = coord.AcquireGrant(4ULL << 20, 4ULL << 20);
    ASSERT_TRUE(grant_or.HasValue()) << grant_or.GetStatus();
    auto grant = grant_or.MoveValue();
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
    probe_rows.reserve(64);
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
    auto grant_or = coord.AcquireGrant(2ULL << 20, 2ULL << 20);
    ASSERT_TRUE(grant_or.HasValue()) << grant_or.GetStatus();
    auto grant = grant_or.MoveValue();
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

TEST(PipelineP3aTest, UnlimitedGrantOperatorBudgetStaysAQueryPoolSlice) {
  ProcessMemoryBudget& mb = ProcessMemoryBudget::Global();
  struct ResetGuard {  // NOLINT(cppcoreguidelines-special-member-functions)
    explicit ResetGuard(ProcessMemoryBudget* b) : budget(b) {}
    ~ResetGuard() { budget->ResetForTest(0); }
    ProcessMemoryBudget* budget;
  } mb_guard(&mb);
  OpBudgetOverrideGuard no_override(0);

  mb.ResetForTest(size_t{1} << 30, 0, 0, false);
  const size_t pool_max = mb.EffectivePoolMaxBytes(MemoryPoolKind::kQuery);
  ASSERT_GT(pool_max, 0U);

  // An unlimited grant must not hand a single operator the whole pool: ADR
  // 0004 #5 requires the pool to be shared among buffering sinks (and across
  // concurrent queries). A default-constructed grant has zero active
  // consumers, so the slice is pool_max / (4 * max(1, 0)) = pool_max / 4.
  PipelineContext ctx(nullptr, QueryMemoryGrant{}, /*dop_in=*/4);
  ASSERT_TRUE(ctx.grant.Unlimited());
  EXPECT_EQ(ctx.grant.ActiveConsumerCount(), 0U);
  EXPECT_EQ(ctx.OperatorBudgetBytes(), pool_max / 4);
  EXPECT_EQ(ctx.OperatorBudgetBytes(size_t{1} << 20),
            std::max(pool_max / 4, size_t{1} << 20));
}

TEST(PipelineP3aTest, ZeroRowRequestsPreserveUnreadSourceRows) {
  PipelineContext ctx(nullptr, QueryMemoryGrant{});
  const Row first({Value(11)});
  const Row second({Value(12)});
  RowVectorSource rows({first, second});
  DataChunk chunk;
  chunk.Append(first);
  chunk.Append(second);
  ChunkVectorSource chunks({chunk}, "chunks");
  auto store = std::make_unique<SpillableStore>(SpillableStore::Options{
      .spill_store = &ctx.spill_store, .force_spill = true});
  ASSERT_SUCCESS(store->AppendChunk(chunk));
  SpillableStoreSource spilled(std::move(store));

  for (PipelineSource* source : {static_cast<PipelineSource*>(&rows),
                                 static_cast<PipelineSource*>(&chunks),
                                 static_cast<PipelineSource*>(&spilled)}) {
    ASSERT_SUCCESS(source->Open(&ctx));
    DataChunk output;
    ASSERT_SUCCESS_AND_EQ(source->NextBatch(&ctx, &output, 0), false);
    EXPECT_TRUE(output.Empty());
    ASSERT_SUCCESS_AND_EQ(source->NextBatch(&ctx, &output, 1), true);
    ASSERT_EQ(output.Size(), 1U);
    EXPECT_EQ(output.RowAt(0), first);
    ASSERT_SUCCESS_AND_EQ(source->NextBatch(&ctx, &output, 0), false);
    EXPECT_TRUE(output.Empty());
    ASSERT_SUCCESS_AND_EQ(
        source->NextBatch(&ctx, &output, std::numeric_limits<size_t>::max()),
        true);
    ASSERT_EQ(output.Size(), 1U);
    EXPECT_EQ(output.RowAt(0), second);
    ASSERT_SUCCESS_AND_EQ(source->NextBatch(&ctx, &output, 1), false);
  }
}

TEST(PipelineP3aTest, ChunkVectorSourcePartialPathPreservesColumnMetadata) {
  // A chunk wider than max_rows takes the row-wise Append path instead of the
  // whole-chunk move; column metadata (collation, unsigned) must survive it
  // the same way it does on the move path.
  DataChunk chunk;
  chunk.Append(Row({Value(std::string("Alpha")).WithCollation(2),
                    Value(int64_t{7}).WithUnsigned()}),
               RowPosition(1, 0));
  chunk.Append(Row({Value(std::string("beta")).WithCollation(2),
                    Value(int64_t{8}).WithUnsigned()}),
               RowPosition(2, 0));
  chunk.Append(Row({Value(std::string("Gamma")).WithCollation(2),
                    Value(int64_t{9}).WithUnsigned()}),
               RowPosition(3, 0));
  ASSERT_EQ(chunk.ColumnAt(0).ColumnCollation(), Collation::kCaseInsensitive);
  ASSERT_TRUE(chunk.ColumnAt(1).IsUnsigned());

  ChunkVectorSource source(std::vector<DataChunk>{std::move(chunk)}, "Meta");
  DataChunk out;
  size_t total = 0;
  for (;;) {
    auto has = source.NextBatch(nullptr, &out, 2);
    ASSERT_TRUE(has.HasValue()) << has.GetStatus();
    if (!has.Value()) {
      break;
    }
    ASSERT_FALSE(out.Empty());
    EXPECT_EQ(out.ColumnAt(0).ColumnCollation(), Collation::kCaseInsensitive);
    EXPECT_TRUE(out.ColumnAt(1).IsUnsigned());
    for (size_t r = 0; r < out.Size(); ++r) {
      EXPECT_EQ(out.ColumnAt(0).ValueAt(r).Collation(),
                static_cast<uint8_t>(Collation::kCaseInsensitive));
    }
    total += out.Size();
  }
  EXPECT_EQ(total, 3U);
}

TEST(PipelineP3aTest, FailedHashBuildTransitionRetainsRowsAndByteCount) {
  OpBudgetOverrideGuard guard(4ULL << 20);
  PipelineContext ctx(nullptr, QueryMemoryGrant{});
  const Schema schema(
      "t", {Column("k", ValueType::kInt64), Column("v", ValueType::kVarChar)});
  OocHashJoinSpec spec;
  spec.build_left = true;
  spec.left_schema = spec.right_schema = schema;
  spec.left_cols = spec.right_cols = {0};
  auto join = std::make_shared<OocHashJoinContext>(spec);
  OocHashBuildSink build(join);
  ASSERT_EQ(build.Init(&ctx, 1), Status::kSuccess);
  DataChunk resident, incoming;
  size_t expected_bytes = 0;
  for (int i = 0; i < 5; ++i) {
    Row row({Value(i), Value(std::string(180000, 'x'))});
    expected_bytes += EstimateRowMemoryBytes(row) + sizeof(RowPosition);
    (i < 4 ? resident : incoming).Append(row);
  }
  ASSERT_EQ(build.Consume(&ctx, 0, resident), Status::kSuccess);
  auto& budget = ProcessMemoryBudget::Global();
  budget.SetTestAlwaysSpill(true);
  budget.SetTestSpillIoFailCountdown(2);
  const Status io_error = build.Consume(&ctx, 0, incoming);
  budget.SetTestSpillIoFailCountdown(0);
  ASSERT_EQ(io_error, Status::kIOError);
  EXPECT_EQ(ctx.spill_store.Stats().live_segments, 0U);
  EXPECT_EQ(join->total_build_bytes, 0U);
  ASSERT_EQ(join->build_rows.size(), 4U);
  for (size_t i = 0; i < 4; ++i) {
    EXPECT_EQ(join->build_rows[i], resident.RowAt(i));
  }
  ASSERT_EQ(build.Consume(&ctx, 0, incoming), Status::kSuccess);
  ASSERT_TRUE(build.Finalize(&ctx).HasValue());
  EXPECT_EQ(join->build_total_rows, 5U);
  EXPECT_EQ(join->total_build_bytes, expected_bytes);
}

TEST(PipelineP3aTest, GraceProbeRetriesPartitionAndSliceReads) {
  OpBudgetOverrideGuard guard(64ULL << 10);
  auto& budget = ProcessMemoryBudget::Global();
  budget.SetTestAlwaysSpill(true);
  Schema schema(
      "t", {Column("k", ValueType::kInt64), Column("v", ValueType::kVarChar)});
  OocHashJoinSpec spec;
  spec.kind = JoinKind::kInner;
  spec.build_left = true;
  spec.left_schema = spec.right_schema = schema;
  spec.left_cols = spec.right_cols = {0};
  constexpr int count = 25;
  for (const int fault : {1, 3, 7, 12, 18, 30, 50, 70}) {
    SCOPED_TRACE(fault);
    PipelineContext ctx(nullptr, QueryMemoryGrant{});
    auto join = std::make_shared<OocHashJoinContext>(spec);
    OocHashBuildSink build(join);
    ASSERT_EQ(build.Init(&ctx, 1), Status::kSuccess);
    DataChunk chunk;
    std::vector<Row> expected;
    for (int i = 0; i < count; ++i) {
      Row row(
          {Value(i % 2), Value(std::to_string(i) + std::string(6000, 'x'))});
      expected.push_back(row);
      chunk.Append(row);
    }
    ASSERT_EQ(build.Consume(&ctx, 0, chunk), Status::kSuccess);
    ASSERT_TRUE(build.Finalize(&ctx).HasValue());
    auto probe_rows = std::make_shared<RowVectorSource>(std::vector<Row>{
        Row({Value(0), Value("p")}), Row({Value(1), Value("q")})});
    OocAdaptiveHashProbeSource source(join, probe_rows, {});
    ASSERT_EQ(source.Open(&ctx), Status::kSuccess);
    budget.SetTestSpillIoFailCountdown(fault);
    std::vector<std::string> found;
    bool saw_error = false;
    for (;;) {
      DataChunk out;
      auto next = source.NextBatch(&ctx, &out, 4);
      if (!next.HasValue()) {
        ASSERT_EQ(next.GetStatus(), Status::kIOError);
        ASSERT_FALSE(saw_error);
        saw_error = true;
        budget.SetTestSpillIoFailCountdown(0);
        continue;
      }
      if (!next.Value()) {
        break;
      }
      for (size_t i = 0; i < out.Size(); ++i) {
        const Row row = out.RowAt(i);
        ASSERT_EQ(row.Count(), 4U);
        EXPECT_EQ(row[0], row[2]);
        found.emplace_back(row[1].AsStringView());
      }
    }
    budget.SetTestSpillIoFailCountdown(0);
    ASSERT_EQ(found.size(), expected.size());
    for (const auto& row : expected) {
      EXPECT_EQ(std::ranges::count(found, row[1].AsStringView()), 1);
    }
    EXPECT_EQ(ctx.spill_store.Stats().live_segments, 0U);
  }
}

TEST(PipelineP3aTest, SpilledHashProbeRejectsMissingContext) {
  auto join = std::make_shared<OocHashJoinContext>(OocHashJoinSpec{});
  join->spilled = true;
  OocAdaptiveHashProbeSource source(join, nullptr, {});
  EXPECT_EQ(source.Open(nullptr).GetCode(), StatusCode::kInvalidArgument);
  DataChunk out;
  const auto next = source.NextBatch(nullptr, &out, 1);
  ASSERT_FALSE(next.HasValue());
  EXPECT_EQ(next.GetStatus().GetCode(), StatusCode::kInvalidArgument);
}

TEST(PipelineP3aTest, DumpAndExplainShareLazyPlanText) {
  std::atomic<int> renders{0};
  PipelineGraph graph;
  graph.plan_explain_producer = [&] {
    ++renders;
    return std::string("Lazy test plan");
  };
  const PipelineExecutor executor(nullptr, std::move(graph));
  EXPECT_EQ(renders.load(), 0);
  auto dump = std::async(std::launch::async, [&] {
    std::ostringstream out;
    executor.Dump(out, 0);
    return out.str();
  });
  std::ostringstream explained;
  executor.Explain(explained, 0);
  EXPECT_EQ(explained.str(), "Lazy test plan");
  EXPECT_TRUE(dump.get().starts_with("Lazy test plan\n"));
  EXPECT_EQ(renders.load(), 1);
}

}  // namespace
}  // namespace tinylamb
