/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// WorkTableIndex (executor/detail/work_table_index.hpp): the hash index the
// recursive CTE loop uses for UNION (DISTINCT) dedupe.  The contract under
// test: one Insert per new row, a repeat is reported as a duplicate and not
// stored twice, and the peer rule is the DISTINCT rule (NaN collapses, -0.0
// equals +0.0, NULL equals NULL, width matters) -- so a recursive UNION
// DISTINCT and a top-level DISTINCT agree on which rows are duplicates.
#include "executor/detail/work_table_index.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <string>
#include <vector>

#include "type/collation.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

Row MakeRow(std::vector<Value> values) { return Row(std::move(values)); }

}  // namespace

TEST(WorkTableIndexTest, InsertReportsNewRowsOnce) {
  relational_detail::WorkTableIndex index;
  EXPECT_TRUE(index.Insert(MakeRow({Value(int64_t{1})})));
  EXPECT_FALSE(index.Insert(MakeRow({Value(int64_t{1})})));
  EXPECT_TRUE(index.Insert(MakeRow({Value(int64_t{2})})));
  EXPECT_EQ(index.size(), 2U);
  EXPECT_TRUE(index.Contains(MakeRow({Value(int64_t{2})})));
  EXPECT_FALSE(index.Contains(MakeRow({Value(int64_t{3})})));
}

TEST(WorkTableIndexTest, ZeroWidthRowsAreOnePeer) {
  // `SELECT` with no columns produces a zero-width row; every round yields the
  // same row, so UNION DISTINCT must see it once instead of looping.
  relational_detail::WorkTableIndex index;
  EXPECT_TRUE(index.Insert(Row{}));
  EXPECT_FALSE(index.Insert(Row{}));
  EXPECT_EQ(index.size(), 1U);
}

TEST(WorkTableIndexTest, WidthAndTypeSeparateRows) {
  relational_detail::WorkTableIndex index;
  EXPECT_TRUE(index.Insert(MakeRow({Value(int64_t{1})})));
  // Same bit pattern, different width.
  EXPECT_TRUE(index.Insert(MakeRow({Value(int64_t{1}), Value(int64_t{0})})));
  // Same numeric value, different type tag (INT64 vs FLOAT64).
  EXPECT_TRUE(index.Insert(MakeRow({Value(1.0)})));
  EXPECT_FALSE(index.Insert(MakeRow({Value(int64_t{1})})));
}

TEST(WorkTableIndexTest, NaNAndNegativeZeroCollapseLikeDistinct) {
  relational_detail::WorkTableIndex index;
  EXPECT_TRUE(index.Insert(MakeRow({Value(std::nan(""))})));
  EXPECT_FALSE(index.Insert(MakeRow({Value(std::nan(""))})));
  // -0.0 and +0.0 are peers for DISTINCT/GROUP BY.
  EXPECT_TRUE(index.Insert(MakeRow({Value(0.0)})));
  EXPECT_FALSE(index.Insert(MakeRow({Value(-0.0)})));
  EXPECT_TRUE(index.Insert(MakeRow({Value(1.0)})));
  EXPECT_EQ(index.size(), 3U);
}

TEST(WorkTableIndexTest, NullEqualsNull) {
  relational_detail::WorkTableIndex index;
  EXPECT_TRUE(index.Insert(MakeRow({Value(), Value(int64_t{1})})));
  EXPECT_FALSE(index.Insert(MakeRow({Value(), Value(int64_t{1})})));
  // NULL and 0 are different peers.
  EXPECT_TRUE(index.Insert(MakeRow({Value(), Value(int64_t{0})})));
  EXPECT_EQ(index.size(), 2U);
}

TEST(WorkTableIndexTest, CaseInsensitiveCollationFoldsLikeDistinct) {
  // A collated VARCHAR pair is one DISTINCT peer, so the hash has to fold the
  // same way: otherwise 'a' and 'A' land in different buckets and the index
  // never compares them.
  relational_detail::WorkTableIndex index;
  const Value lower = Value("a").WithCollation(
      static_cast<uint8_t>(Collation::kCaseInsensitive));
  const Value upper = Value("A").WithCollation(
      static_cast<uint8_t>(Collation::kCaseInsensitive));
  EXPECT_TRUE(index.Insert(MakeRow({lower})));
  EXPECT_FALSE(index.Insert(MakeRow({upper})));
  EXPECT_EQ(index.size(), 1U);
  // Binary collation keeps them apart.
  relational_detail::WorkTableIndex binary;
  EXPECT_TRUE(binary.Insert(MakeRow({Value("a")})));
  EXPECT_TRUE(binary.Insert(MakeRow({Value("A")})));
}

TEST(WorkTableIndexTest, CountersOnlyPublishWhenEnabled) {
  // Top-level DISTINCT shares the class but must not touch the counters (hot
  // path); only the recursive loop opts in.
  relational_detail::WorkTableIndexCounters& counters =
      relational_detail::WorkTableIndexStats();
  const uint64_t inserts_before = counters.inserts.load();
  relational_detail::WorkTableIndex index;
  index.Insert(MakeRow({Value(int64_t{9})}));
  EXPECT_EQ(counters.inserts.load(), inserts_before);
}

TEST(WorkTableIndexTest, CountersTrackInsertsAndDuplicates) {
  relational_detail::WorkTableIndexCounters& counters =
      relational_detail::WorkTableIndexStats();
  const uint64_t inserts_before = counters.inserts.load();
  const uint64_t duplicates_before = counters.duplicates.load();
  relational_detail::WorkTableIndex index(/*count_stats=*/true);
  index.Insert(MakeRow({Value(int64_t{7})}));
  index.Insert(MakeRow({Value(int64_t{7})}));
  index.Insert(MakeRow({Value(int64_t{8})}));
  EXPECT_EQ(counters.inserts.load() - inserts_before, 3U);
  EXPECT_EQ(counters.duplicates.load() - duplicates_before, 1U);
}

}  // namespace tinylamb
