/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// Extra Row/Schema coverage beyond row_test.cpp and schema_test.cpp:
// qualified-name lookup, rename chains, projection extraction, and
// constraint-preserving schema round trips.
#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

TEST(RowSchemaExtraTest, OffsetResolvesQualifiedAndBareNames) {
  const Schema schema("lineitem", {Column("lkey", ValueType::kInt64),
                                   Column("value", ValueType::kDouble)});
  EXPECT_EQ(schema.Offset(ColumnName("lineitem", "lkey")), 0);
  EXPECT_EQ(schema.Offset(ColumnName("lineitem", "value")), 1);
  // An unqualified name resolves against the bare column name.
  EXPECT_EQ(schema.Offset(ColumnName("", "lkey")), 0);
  // Unknown and wrong-qualification lookups stay negative.
  EXPECT_LT(schema.Offset(ColumnName("", "missing")), 0);
  EXPECT_LT(schema.Offset(ColumnName("other", "lkey")), 0);
}

TEST(RowSchemaExtraTest, RenameChainsPreserveEveryOtherColumn) {
  const Schema schema(
      "base", {Column("a", ValueType::kInt64), Column("b", ValueType::kVarChar),
               Column("c", ValueType::kDouble)});
  const Schema renamed = schema.WithRenamedColumn(1, "beta");
  EXPECT_EQ(renamed.Name(), "base");
  EXPECT_EQ(renamed.GetColumn(1).Name().name, "beta");
  EXPECT_EQ(renamed.GetColumn(0).Name().name, "a");
  EXPECT_EQ(renamed.GetColumn(2).Name().name, "c");
  // Renaming keeps the types and the column count stable.
  EXPECT_EQ(renamed.GetColumn(1).Type(), ValueType::kVarChar);
  EXPECT_EQ(renamed.ColumnCount(), 3);

  const Schema renamed_twice = renamed.WithName("aliased");
  EXPECT_EQ(renamed_twice.Name(), "aliased");
  EXPECT_EQ(renamed_twice.ColumnCount(), renamed.ColumnCount());
}

TEST(RowSchemaExtraTest, ConstraintsSurviveSchemaSerialization) {
  const Schema schema(
      "orders",
      {Column("id", ValueType::kInt64, Constraint(Constraint::kPrimaryKey)),
       Column("ref", ValueType::kInt64, Constraint(Constraint::kUnique)),
       Column("note", ValueType::kVarChar)});
  SerializeDeserializeTest(schema);
  // The primary key counts as unique; a constraint-free column stays so.
  EXPECT_TRUE(schema.GetColumn(0).GetConstraint().IsUnique());
  EXPECT_EQ(schema.GetColumn(0).GetConstraint().ctype, Constraint::kPrimaryKey);
  EXPECT_TRUE(schema.GetColumn(1).GetConstraint().IsUnique());
  EXPECT_TRUE(schema.GetColumn(2).GetConstraint().IsNothing());
}

TEST(RowSchemaExtraTest, ExtractBuildsAnOrderedProjection) {
  const Row row({Value(1), Value("middle"), Value(2.5), Value("tail")});
  const Row picked = row.Extract({3, 0});
  ASSERT_EQ(picked.values_.size(), 2U);
  EXPECT_EQ(picked.values_[0], Value("tail"));
  EXPECT_EQ(picked.values_[1], Value(1));
  // Repeating an index duplicates the column; an empty list yields an empty
  // row rather than a failure.
  EXPECT_EQ(row.Extract({2, 2}).values_[0], Value(2.5));
  EXPECT_TRUE(row.Extract({}).values_.empty());
}

TEST(RowSchemaExtraTest, RowBufferRoundTripPreservesSchemaBoundValues) {
  const Schema schema("bounds", {Column("n", ValueType::kInt64),
                                 Column("text", ValueType::kVarChar),
                                 Column("real", ValueType::kDouble)});
  const Row original(
      {Value(std::numeric_limits<int64_t>::min()), Value(""), Value()});
  std::vector<char> buffer(original.Size());
  const size_t written = original.Serialize(buffer.data());
  Row restored;
  const size_t read = restored.Deserialize(buffer.data(), schema);
  EXPECT_EQ(written, original.Size());
  EXPECT_EQ(read, original.Size());
  EXPECT_EQ(restored[0], Value(std::numeric_limits<int64_t>::min()));
  EXPECT_EQ(restored[1], Value(""));
  EXPECT_TRUE(restored[2].IsNull());
}

}  // namespace tinylamb
