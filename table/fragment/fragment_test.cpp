/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "table/fragment/fragment.hpp"

#include <gtest/gtest.h>

#include <limits>
#include <vector>

#include "table/fuzz/fuzz_chunk_common.hpp"
#include "table/memtable/memtable.hpp"

namespace tinylamb {
namespace {
class FragmentAuditTest : public ::testing::Test {
 protected:
  Schema schema_{"audit",
                 {Column(ColumnName{"", "id"}, ValueType::kInt64),
                  Column(ColumnName{"", "v"}, ValueType::kInt64),
                  Column(ColumnName{"", "s"}, ValueType::kVarChar)}};
  std::vector<Row> rows_{Row({Value(1), Value(), Value()}),
                         Row({Value(2), Value(0), Value("zero")}),
                         Row({Value(3), Value(5), Value("five")}),
                         Row({Value(std::numeric_limits<int64_t>::max()),
                              Value(5), Value("last")})};
  SortedRun run_ = fuzz::MakeRun(rows_, schema_);
  PaxChunk chunk_ = PaxChunk::BuildFromSortedRun(run_, schema_).MoveValue();
  std::vector<std::pair<std::string, RowEntry>> mem_;
  void SetUp() override {
    MemTable table(schema_);
    for (const Row& row : rows_) ASSERT_EQ(table.Put(row), Status::kSuccess);
    mem_ = table.Snapshot();
  }
  std::vector<ArtifactRef> Artifacts() {
    return {{.kind = ArtifactRef::Kind::kChunk,
             .chunk = &chunk_,
             .schema = &schema_},
            {.kind = ArtifactRef::Kind::kRun, .run = &run_, .schema = &schema_},
            {.kind = ArtifactRef::Kind::kMemSnapshot,
             .mem_rows = &mem_,
             .schema = &schema_}};
  }
};

TEST(FragmentDomainAuditTest, UnsignedArtifactsDeclineSignedOnlyPushdown) {
  Column key("id", ValueType::kInt64);
  key.SetUnsigned(true);
  Schema schema("unsigned_audit", {key, Column("v", ValueType::kInt64)});
  const std::vector<Row> rows{Row({Value(1).WithUnsigned(), Value(10)}),
                              Row({Value(-1).WithUnsigned(), Value(20)})};
  const auto run = fuzz::MakeRun(rows, schema);
  const auto chunk = PaxChunk::BuildFromSortedRun(run, schema).MoveValue();
  MemTable table(schema);
  for (const Row& row : rows) {
    ASSERT_EQ(table.Put(row), Status::kSuccess);
  }
  const auto memory = table.Snapshot();
  const std::vector<ArtifactRef> artifacts{
      {.kind = ArtifactRef::Kind::kChunk, .chunk = &chunk, .schema = &schema},
      {.kind = ArtifactRef::Kind::kRun, .run = &run, .schema = &schema},
      {.kind = ArtifactRef::Kind::kMemSnapshot,
       .mem_rows = &memory,
       .schema = &schema}};
  for (const auto& artifact : artifacts) {
    for (AccessMode access :
         {AccessMode::kFilterScan, AccessMode::kPoint, AccessMode::kKeyRange}) {
      Fragment fragment;
      fragment.access = access;
      fragment.point_key = 1;
      fragment.range_lo = 1;
      for (Runtime runtime : {Runtime::kNative, Runtime::kWasm}) {
        auto result = Execute(runtime, {artifact}, fragment, schema);
        ASSERT_TRUE(result.HasValue());
        EXPECT_TRUE(result.Value().rejected);
        EXPECT_EQ(result.Value().status, Status::kNotImplemented);
        EXPECT_EQ(Estimate(runtime, {artifact}, fragment).coverage,
                  Coverage::kNone);
      }
    }
  }
}

TEST_F(FragmentAuditTest, NullComparisonsAgreeAcrossArtifacts) {
  for (const auto& artifact : Artifacts()) {
    for (bool in : {false, true}) {
      Fragment fragment;
      fragment.conjuncts.push_back(
          {.kind = in ? ConjunctKind::kIn : ConjunctKind::kCompare,
           .col = 1,
           .op = CompareOp::kEq,
           .int64_literal = 0,
           .bytes_literal = {},
           .in_literals = {0},
           .bytes_literal_hi = {}});
      auto result = Execute(Runtime::kNative, {artifact}, fragment, schema_);
      ASSERT_TRUE(result.HasValue());
      EXPECT_EQ(result.Value().rows_proved_pass, 1U);
    }
  }
}

TEST_F(FragmentAuditTest, NonIntegerNullPredicateIsEvaluatedOrResidual) {
  Fragment fragment;
  fragment.conjuncts.push_back({.kind = ConjunctKind::kIsNull,
                                .col = 2,
                                .bytes_literal = {},
                                .in_literals = {},
                                .bytes_literal_hi = {}});
  for (const auto& artifact : Artifacts()) {
    auto result = Execute(Runtime::kNative, {artifact}, fragment, schema_);
    ASSERT_TRUE(result.HasValue());
    if (result.Value().coverage == Coverage::kFull) {
      EXPECT_EQ(result.Value().rows_proved_pass, 1U);
    } else {
      EXPECT_EQ(result.Value().residual_conjuncts, std::vector<size_t>({0}));
    }
  }
}

TEST_F(FragmentAuditTest, ClosedRangesIncludeUpperBoundAndUnboundedMaximum) {
  for (const auto& artifact : Artifacts()) {
    Fragment fragment;
    fragment.access = AccessMode::kKeyRange;
    fragment.range_lo = 2;
    fragment.range_hi = 3;
    auto result = Execute(Runtime::kNative, {artifact}, fragment, schema_);
    ASSERT_TRUE(result.HasValue());
    EXPECT_EQ(result.Value().rows_proved_pass, 2U);
    fragment.range_hi.reset();
    result = Execute(Runtime::kNative, {artifact}, fragment, schema_);
    ASSERT_TRUE(result.HasValue());
    EXPECT_EQ(result.Value().rows_proved_pass, 3U);
  }
}

TEST_F(FragmentAuditTest, PointAccessReturnsOnlyRequestedKey) {
  for (const auto& artifact : Artifacts()) {
    Fragment fragment;
    fragment.access = AccessMode::kPoint;
    fragment.point_key = 2;
    auto result = Execute(Runtime::kNative, {artifact}, fragment, schema_);
    ASSERT_TRUE(result.HasValue());
    EXPECT_EQ(result.Value().rows_proved_pass, 1U);
  }
}

TEST_F(FragmentAuditTest, DistinctAggregateDoesNotCountDuplicatesOrNull) {
  for (const auto& artifact : Artifacts()) {
    Fragment fragment;
    fragment.aggregate = Aggregate{
        .kind = AggregateKind::kApproxCountDistinct, .col = 1, .terms = {}};
    auto result = Execute(Runtime::kNative, {artifact}, fragment, schema_);
    ASSERT_TRUE(result.HasValue());
    EXPECT_EQ(result.Value().scalar_count, 2U);
  }
}

TEST_F(FragmentAuditTest, TopKPreservesRowIdentity) {
  Fragment fragment;
  fragment.top_k = TopKSpec{.col = 0, .k = 3, .descending = true};
  auto artifacts = Artifacts();
  for (size_t i : {size_t{1}, size_t{2}}) {
    auto result = Execute(Runtime::kNative, {artifacts[i]}, fragment, schema_);
    ASSERT_TRUE(result.HasValue());
    ASSERT_EQ(result.Value().top_k.size(), 3U);
    EXPECT_EQ(result.Value().top_k[0].row, 3U);
    EXPECT_EQ(result.Value().top_k[1].row, 2U);
    EXPECT_EQ(result.Value().top_k[2].row, 1U);
  }
  fragment.top_k->col = 2;
  auto result = Execute(Runtime::kNative, {artifacts[1]}, fragment, schema_);
  ASSERT_TRUE(result.HasValue());
  EXPECT_TRUE(result.Value().rejected);
}
TEST_F(FragmentAuditTest, SumWrapsWithoutSignedOverflow) {
  const Schema schema("sums", {Column(ColumnName{"", "k"}, ValueType::kInt64),
                               Column(ColumnName{"", "v"}, ValueType::kInt64)});
  std::vector<Row> rows{Row({Value(1), Value(INT64_MAX)}),
                        Row({Value(2), Value(INT64_MAX)})};
  SortedRun run = fuzz::MakeRun(rows, schema);
  const ArtifactRef artifact{
      .kind = ArtifactRef::Kind::kRun, .run = &run, .schema = &schema};
  Fragment fragment;
  fragment.aggregate =
      Aggregate{.kind = AggregateKind::kSum, .col = 1, .coeff = 2, .terms = {}};
  auto result = Execute(Runtime::kNative, {artifact}, fragment, schema);
  ASSERT_TRUE(result.HasValue());
  EXPECT_EQ(result.Value().scalar_value, -4);
  fragment.aggregate->terms = {{.coeff = 2, .cols = {1, 1, 1, 1}}};
  result = Execute(Runtime::kNative, {artifact}, fragment, schema);
  ASSERT_TRUE(result.HasValue());
  EXPECT_EQ(result.Value().scalar_value, 4);
}

TEST_F(FragmentAuditTest, GroupAggregateHonorsAccessMultiplierAndGeneration) {
  auto artifact = Artifacts()[0];
  Fragment fragment;
  fragment.group_cols = {1};
  fragment.aggregate =
      Aggregate{.kind = AggregateKind::kSum, .col = 0, .coeff = 2, .terms = {}};
  fragment.access = AccessMode::kPoint;
  fragment.point_key = 3;
  auto result = Execute(Runtime::kNative, {artifact}, fragment, schema_);
  ASSERT_TRUE(result.HasValue());
  ASSERT_EQ(result.Value().groups.size(), 1U);
  EXPECT_EQ(result.Value().groups[0].sum, 6);
  fragment.access = AccessMode::kFilterScan;
  fragment.read_version = 1;
  artifact.generation = 2;
  result = Execute(Runtime::kNative, {artifact}, fragment, schema_);
  ASSERT_TRUE(result.HasValue());
  EXPECT_TRUE(result.Value().groups.empty());
}

}  // namespace
}  // namespace tinylamb
