/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "table/fragment/fragment.hpp"

#include <gtest/gtest.h>

#include <limits>
#include <optional>
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
    for (const Row& row : rows_) {
      ASSERT_EQ(table.Put(row), Status::kSuccess);
    }
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

TEST(FragmentTombstoneAuditTest, NonInt64KeyTombstoneIsRejectedNotEmitted) {
  Schema sc("varchar_key",
            {Column(ColumnName{"", "k"}, ValueType::kVarChar,
                    Constraint{Constraint::kPrimaryKey}),
             Column(ColumnName{"", "v"}, ValueType::kInt64)},
            0, 0);
  MemTable mt(sc);
  const Row live({Value("a"), Value(int64_t{1})});
  ASSERT_EQ(mt.Put(live), Status::kSuccess);
  ASSERT_EQ(mt.Delete(live.EncodeKeyMemcomparableFormat(sc)), Status::kSuccess);
  const auto snap = mt.Snapshot();
  const ArtifactRef ref{.kind = ArtifactRef::Kind::kMemSnapshot,
                        .mem_rows = &snap,
                        .schema = &sc};
  Fragment fragment;

  // A non-int64 key cannot tell which older live rows the tombstone hides, so
  // the fragment must be rejected (executor runs the residual path) instead
  // of returning the tombstone or a garbage value as a proved live row.
  auto result = Execute(Runtime::kNative, {ref}, fragment, sc);
  ASSERT_TRUE(result.HasValue());
  EXPECT_TRUE(result.Value().rejected);
  EXPECT_EQ(result.Value().status, Status::kNotImplemented);
  EXPECT_EQ(result.Value().coverage, Coverage::kNone);
  EXPECT_FALSE(
      MaterializeKeyValue(Runtime::kNative, {ref}, fragment, sc, 1).HasValue());
}

TEST_F(FragmentAuditTest, NonIntegerAggregateColumnDeclinesPushdown) {
  for (const auto& artifact : Artifacts()) {
    for (const auto kind :
         {AggregateKind::kCountCol, AggregateKind::kSum, AggregateKind::kMin,
          AggregateKind::kMax, AggregateKind::kApproxCountDistinct}) {
      Fragment fragment;
      fragment.aggregate = Aggregate{.kind = kind, .col = 2};
      auto result = Execute(Runtime::kNative, {artifact}, fragment, schema_);
      ASSERT_TRUE(result.HasValue());
      EXPECT_EQ(result.Value().status, Status::kNotImplemented);
      EXPECT_EQ(result.Value().coverage, Coverage::kNone);
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
      fragment.projection = {0};
      auto result = Execute(Runtime::kNative, {artifact}, fragment, schema_);
      ASSERT_TRUE(result.HasValue());
      EXPECT_EQ(result.Value().rows_proved_pass, 1U);
      EXPECT_EQ(result.Value().projected_values, (std::vector<int64_t>{2}));
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

TEST_F(FragmentAuditTest, NullKeyNeverSatisfiesKeyRange) {
  // A NULL key cell decodes as raw 0 inside the chunk kernels, but a NULL
  // key never satisfies a range: the run path drops the kNull-tagged key
  // blob before its range window and the memtable path fails
  // MemcomparableInt64Key on it. The chunk path must agree.
  const Schema schema("null_keys",
                      {Column(ColumnName{"", "k"}, ValueType::kInt64),
                       Column(ColumnName{"", "v"}, ValueType::kInt64)});
  // Sorted order puts the NULL key (the bare kNull tag) ahead of every
  // memcomparable int64 key.
  const std::vector<Row> rows{Row({Value(), Value(99)}),
                              Row({Value(3), Value(1)}),
                              Row({Value(10), Value(2)})};
  const SortedRun run = fuzz::MakeRun(rows, schema);
  const PaxChunk chunk = PaxChunk::BuildFromSortedRun(run, schema).MoveValue();
  // MemTable::Put rejects NULL keys, so the snapshot pairs are built by
  // hand: the key blob is the bare kNull tag and the entry holds the row.
  std::vector<std::pair<std::string, RowEntry>> mem;
  for (const Row& row : rows) {
    mem.emplace_back(row.EncodeKeyMemcomparableFormat(schema), RowEntry(row));
  }
  const std::vector<ArtifactRef> artifacts{
      {.kind = ArtifactRef::Kind::kChunk, .chunk = &chunk, .schema = &schema},
      {.kind = ArtifactRef::Kind::kRun, .run = &run, .schema = &schema},
      {.kind = ArtifactRef::Kind::kMemSnapshot,
       .mem_rows = &mem,
       .schema = &schema}};
  for (const auto& artifact : artifacts) {
    Fragment fragment;
    fragment.access = AccessMode::kKeyRange;
    // The decoded-0 NULL cell falls inside this interval; only key 3 does.
    fragment.range_lo = -5;
    fragment.range_hi = 5;
    auto result = Execute(Runtime::kNative, {artifact}, fragment, schema);
    ASSERT_TRUE(result.HasValue());
    EXPECT_EQ(result.Value().rows_proved_pass, 1U)
        << "artifact kind " << static_cast<int>(artifact.kind);
    // An interval that excludes 0 agrees on every artifact as well.
    fragment.range_lo = 4;
    fragment.range_hi = 8;
    result = Execute(Runtime::kNative, {artifact}, fragment, schema);
    ASSERT_TRUE(result.HasValue());
    EXPECT_EQ(result.Value().rows_proved_pass, 0U)
        << "artifact kind " << static_cast<int>(artifact.kind);
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
  for (const auto& artifact : artifacts) {
    auto result = Execute(Runtime::kNative, {artifact}, fragment, schema_);
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
TEST_F(FragmentAuditTest, SumRejectsSignedOverflow) {
  const Schema schema("sums", {Column(ColumnName{"", "k"}, ValueType::kInt64),
                               Column(ColumnName{"", "v"}, ValueType::kInt64)});
  std::vector<Row> rows{Row({Value(1), Value(INT64_MAX)}),
                        Row({Value(2), Value(INT64_MAX)})};
  SortedRun run = fuzz::MakeRun(rows, schema);
  auto chunk = PaxChunk::BuildFromSortedRun(run, schema);
  ASSERT_TRUE(chunk.HasValue());
  MemTable table(schema);
  for (const auto& row : rows) {
    ASSERT_EQ(table.Put(row), Status::kSuccess);
  }
  const auto snapshot = table.Snapshot();
  const std::vector<ArtifactRef> artifacts{
      {.kind = ArtifactRef::Kind::kChunk,
       .chunk = &chunk.Value(),
       .schema = &schema},
      {.kind = ArtifactRef::Kind::kRun, .run = &run, .schema = &schema},
      {.kind = ArtifactRef::Kind::kMemSnapshot,
       .mem_rows = &snapshot,
       .schema = &schema}};
  for (const auto& artifact : artifacts) {
    Fragment fragment;
    fragment.aggregate = Aggregate{
        .kind = AggregateKind::kSum, .col = 1, .coeff = 2, .terms = {}};
    EXPECT_EQ(
        Execute(Runtime::kNative, {artifact}, fragment, schema).GetStatus(),
        Status::kIsInfinity);
    fragment.aggregate->terms = {{.coeff = 2, .cols = {1, 1, 1, 1}}};
    EXPECT_EQ(
        Execute(Runtime::kNative, {artifact}, fragment, schema).GetStatus(),
        Status::kIsInfinity);
  }
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

TEST_F(FragmentAuditTest, GroupSumSkipsNullSummandsOnEveryArtifactKind) {
  // rows_ has v NULL for id 1: the single-chunk artifact goes through the
  // GroupBySumColumns kernel while the run/mem artifacts take the generic
  // AccumulateRow path -- both must agree that the id=1 group exists with
  // count == 0 (the "SUM is NULL" contract) instead of counting the row.
  for (const auto& artifact : Artifacts()) {
    Fragment fragment;
    fragment.group_cols = {0};
    fragment.aggregate =
        Aggregate{.kind = AggregateKind::kSum, .col = 1, .terms = {}};
    auto result = Execute(Runtime::kNative, {artifact}, fragment, schema_);
    ASSERT_TRUE(result.HasValue());
    const std::vector<GroupOutput>& groups = result.Value().groups;
    ASSERT_EQ(groups.size(), 4U);
    for (const GroupOutput& group : groups) {
      ASSERT_EQ(group.key.size(), 1U);
      if (group.key[0] == 1) {
        EXPECT_EQ(group.count, 0U) << "a NULL summand still forms the group";
        EXPECT_EQ(group.sum, 0);
      } else if (group.key[0] == 2) {
        EXPECT_EQ(group.count, 1U);
        EXPECT_EQ(group.sum, 0);
      } else {
        EXPECT_EQ(group.count, 1U);
        EXPECT_EQ(group.sum, 5);
      }
    }
  }
}

// T3108: the empty string is a real bound (SQL byte-order minimum), not
// "unbounded".  `col < ''` must reject every non-null row on all three
// artifacts, while a flagged lo='' stays permissive and a plain
// [lo, hi) range keeps filtering exactly.
TEST_F(FragmentAuditTest, EmptyStringVarcharRangeBoundIsHonored) {
  auto range_conjunct = [](bool lo_set, const char* lo, bool hi_set,
                           const char* hi) {
    return Conjunct{.kind = ConjunctKind::kVarcharRange,
                    .col = 2,
                    .bytes_literal = lo,
                    .in_literals = {},
                    .bytes_literal_hi = hi,
                    .bytes_lo_set = lo_set,
                    .bytes_hi_set = hi_set};
  };
  struct Case {
    Conjunct conjunct;
    size_t expected_pass;
  };
  const std::vector<Case> cases{
      // s values: NULL, "zero", "five", "last".  hi='' => nothing is
      // strictly below it (old code read the empty hi as unbounded and
      // passed all three non-null rows).
      {range_conjunct(false, "", true, ""), 0},
      // lo='' is permissive either way, but must now flow as a real bound.
      {range_conjunct(true, "", false, ""), 3},
      // Ordinary inclusive/exclusive bounds still prune.
      {range_conjunct(true, "a", true, "g"), 1},
      // Unset flags ignore the strings entirely.
      {range_conjunct(false, "", false, ""), 3},
  };
  for (const Case& case_ : cases) {
    for (const auto& artifact : Artifacts()) {
      Fragment fragment;
      fragment.conjuncts.push_back(case_.conjunct);
      auto result = Execute(Runtime::kNative, {artifact}, fragment, schema_);
      ASSERT_TRUE(result.HasValue());
      EXPECT_EQ(result.Value().coverage, Coverage::kFull);
      EXPECT_EQ(result.Value().rows_proved_pass, case_.expected_pass)
          << "artifact kind " << static_cast<int>(artifact.kind);
    }
  }
}

}  // namespace
}  // namespace tinylamb
