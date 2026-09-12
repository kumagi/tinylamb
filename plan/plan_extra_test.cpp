/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/random_string.hpp"
#include "common/set_operation.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "gtest/gtest.h"
#include "index/index.hpp"
#include "plan/distinct_plan.hpp"
#include "plan/group_by_plan.hpp"
#include "plan/incremental_sort_plan.hpp"
#include "plan/max1_row_plan.hpp"
#include "plan/plan.hpp"
#include "plan/relation_rename_plan.hpp"
#include "plan/relational_plan.hpp"
#include "plan/set_operation_plan.hpp"
#include "plan/skip_scan_distinct_plan.hpp"
#include "plan/sort_distinct_plan.hpp"
#include "plan/sort_plan.hpp"
#include "plan/values_plan.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

Plan MakeValuesChild(size_t rows = 2) {
  const Schema schema(
      "t", {Column("a", ValueType::kInt64), Column("b", ValueType::kVarChar)});
  std::vector<Row> data;
  data.reserve(rows);
  for (size_t i = 0; i < rows; ++i) {
    data.emplace_back(Row({Value(static_cast<int64_t>(i)), Value("r")}));
  }
  return std::make_shared<ValuesPlan>(schema, std::move(data));
}

std::string DumpToString(const PlanBase& plan, int indent = 0) {
  std::ostringstream oss;
  plan.Dump(oss, indent);
  return oss.str();
}

}  // namespace

TEST(DistinctPlanExtraTest, DumpToStringAndDelegation) {
  Plan child = MakeValuesChild();
  DistinctPlan plain(child);
  EXPECT_EQ(plain.ToString(), "HashDistinct");
  EXPECT_FALSE(plain.HasDistinctOn());
  std::string dump = DumpToString(plain);
  EXPECT_NE(dump.find("HashDistinct"), std::string::npos);
  EXPECT_EQ(dump.find("on=["), std::string::npos);
  EXPECT_NE(dump.find("Values (rows=2)"), std::string::npos);

  // Delegation to the child for every structural property.
  EXPECT_EQ(plain.AccessRowCount(), 2U);
  EXPECT_EQ(plain.EmitRowCount(), 2U);
  EXPECT_EQ(plain.GetSchema().ColumnCount(), 2U);
  EXPECT_EQ(plain.ScanSource(), nullptr);
  EXPECT_TRUE(plain.IsOrderedBy({}, {}));
  EXPECT_TRUE(plain.IsOrderedBy({}, {}, {}));
  EXPECT_EQ(plain.Child().get(), child.get());

  DistinctPlan keyed(child, {ColumnValueExp("a")});
  EXPECT_TRUE(keyed.HasDistinctOn());
  ASSERT_EQ(keyed.DistinctOn().size(), 1U);
  dump = DumpToString(keyed);
  EXPECT_NE(dump.find("on=["), std::string::npos);
  EXPECT_NE(dump.find('a'), std::string::npos);
}

TEST(SortDistinctPlanExtraTest, DumpToStringAndDelegation) {
  Plan child = MakeValuesChild();
  SortDistinctPlan plan(child);
  EXPECT_EQ(plan.ToString(), "SortDistinct");
  std::string dump = DumpToString(plan);
  EXPECT_NE(dump.find("SortDistinct"), std::string::npos);
  EXPECT_NE(dump.find("Values (rows=2)"), std::string::npos);
  EXPECT_EQ(plan.AccessRowCount(), 2U);
  EXPECT_EQ(plan.EmitRowCount(), 2U);
  EXPECT_EQ(plan.GetSchema().ColumnCount(), 2U);
  EXPECT_EQ(plan.Child().get(), child.get());
}

TEST(Max1RowPlanExtraTest, EmitRowCountIsCappedAndDumpNests) {
  Plan child = MakeValuesChild(/*rows=*/5);
  Max1RowPlan plan(child);
  EXPECT_EQ(plan.ToString(), "Max1Row");
  EXPECT_EQ(plan.EmitRowCount(), 1U);
  EXPECT_EQ(plan.AccessRowCount(), 5U);
  std::string dump = DumpToString(plan);
  EXPECT_NE(dump.find("Max1Row"), std::string::npos);
  EXPECT_NE(dump.find("Values (rows=5)"), std::string::npos);

  // An empty child stays empty (min(0, 1)).
  Max1RowPlan empty(MakeValuesChild(/*rows=*/0));
  EXPECT_EQ(empty.EmitRowCount(), 0U);
}

TEST(SetOperationPlanExtraTest, SchemaCoercionAndErrors) {
  const Schema ints("t", {Column("a", ValueType::kInt64)});
  const Schema doubles("t", {Column("a", ValueType::kDouble)});
  Plan int_child =
      std::make_shared<ValuesPlan>(ints, std::vector<Row>{Row({Value(1)})});
  Plan double_child = std::make_shared<ValuesPlan>(
      doubles, std::vector<Row>{Row({Value(1.5)})});

  // INT64 + DOUBLE coerces to DOUBLE.
  SetOperationPlan coerced({int_child, double_child}, SetOperationKind::kUnion);
  ASSERT_EQ(coerced.GetSchema().ColumnCount(), 1U);
  EXPECT_EQ(coerced.GetSchema().GetColumn(0).Type(), ValueType::kDouble);

  // Identical types stay put.
  SetOperationPlan same({int_child, int_child}, SetOperationKind::kUnionAll);
  EXPECT_EQ(same.GetSchema().GetColumn(0).Type(), ValueType::kInt64);

  // Column-count mismatch and missing children are rejected at construction.
  const Schema wide(
      "t", {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64)});
  Plan wide_child = std::make_shared<ValuesPlan>(
      wide, std::vector<Row>{Row({Value(1), Value(2)})});
  // Width and emptiness are now validated by SetOperationExecutor at
  // execution time; the plan builds a best-effort schema without throwing
  // (no-exception-rule-migration.md).
  EXPECT_NO_THROW(
      SetOperationPlan({int_child, wide_child}, SetOperationKind::kUnion));
  EXPECT_NO_THROW(SetOperationPlan({}, SetOperationKind::kUnion));
}

TEST(SetOperationPlanExtraTest, RowCountsOrderingAndDump) {
  Plan left = MakeValuesChild(/*rows=*/2);
  Plan right = MakeValuesChild(/*rows=*/3);

  SetOperationPlan union_all({left, right}, SetOperationKind::kUnionAll);
  EXPECT_EQ(union_all.AccessRowCount(), 5U);
  EXPECT_EQ(union_all.EmitRowCount(), 5U);  // UNION ALL adds everything

  SetOperationPlan union_dedup({left, right}, SetOperationKind::kUnion);
  EXPECT_EQ(union_dedup.AccessRowCount(), 5U);
  EXPECT_EQ(union_dedup.EmitRowCount(), 2U);  // distinct: front child's cap

  // ToString per operation kind.
  EXPECT_EQ(union_dedup.ToString(), "Union");
  EXPECT_EQ(SetOperationPlan({left}, SetOperationKind::kIntersect).ToString(),
            "Intersect");
  EXPECT_EQ(
      SetOperationPlan({left}, SetOperationKind::kIntersectAll).ToString(),
      "IntersectAll");
  EXPECT_EQ(SetOperationPlan({left}, SetOperationKind::kExcept).ToString(),
            "Except");
  EXPECT_EQ(SetOperationPlan({left}, SetOperationKind::kExceptAll).ToString(),
            "ExceptAll");
  EXPECT_EQ(union_all.ToString(), "UnionAll");

  // UNION ALL with ORDER BY runs as MergeAppend and claims the order.
  std::vector<SortKey> order{SortKey{.expression = ColumnValueExp("a"),
                                     .ascending = true,
                                     .nulls_first = std::nullopt}};
  SetOperationPlan merge_append({left, right}, SetOperationKind::kUnionAll,
                                order);
  EXPECT_EQ(merge_append.ToString(), "MergeAppend");
  EXPECT_TRUE(merge_append.IsOrderedBy({ColumnValueExp("a")}, {true}));
  EXPECT_FALSE(merge_append.IsOrderedBy({ColumnValueExp("a")}, {false}));
  EXPECT_FALSE(merge_append.IsOrderedBy({ColumnValueExp("b")}, {true}));
  EXPECT_FALSE(merge_append.IsOrderedBy(
      {ColumnValueExp("a"), ColumnValueExp("b")}, {true, true}));
  EXPECT_FALSE(merge_append.IsOrderedBy({ColumnValueExp("a")}, {true, false}));
  // Without order keys nothing is promised.
  EXPECT_FALSE(union_all.IsOrderedBy({ColumnValueExp("a")}, {true}));

  std::string dump = DumpToString(merge_append);
  EXPECT_NE(dump.find("MergeAppend\n"), std::string::npos);
  // Children render two columns deeper than the set-operation header.
  EXPECT_NE(dump.find("  Values (rows=2)"), std::string::npos);
  EXPECT_NE(dump.find("  Values (rows=3)"), std::string::npos);
  EXPECT_EQ(merge_append.Operation(), SetOperationKind::kUnionAll);
  EXPECT_EQ(merge_append.Children().size(), 2U);
  EXPECT_EQ(merge_append.OrderKeys().size(), 1U);
}

TEST(IncrementalSortPlanExtraTest, IsOrderedByNullsFirstAndDump) {
  Plan child = MakeValuesChild();
  std::vector<SortKey> prefix{SortKey{.expression = ColumnValueExp("a"),
                                      .ascending = true,
                                      .nulls_first = std::nullopt}};
  std::vector<SortKey> suffix{SortKey{.expression = ColumnValueExp("b"),
                                      .ascending = false,
                                      .nulls_first = std::nullopt}};
  IncrementalSortPlan plan(child, prefix, suffix);

  // Full key list must match both keys in order.
  EXPECT_TRUE(plan.IsOrderedBy({ColumnValueExp("a"), ColumnValueExp("b")},
                               {true, false}));
  EXPECT_FALSE(plan.IsOrderedBy({ColumnValueExp("a")}, {true}));
  EXPECT_FALSE(plan.IsOrderedBy({ColumnValueExp("a"), ColumnValueExp("b")},
                                {true, true}));
  EXPECT_FALSE(plan.IsOrderedBy({ColumnValueExp("b"), ColumnValueExp("a")},
                                {false, true}));

  // Three-arg overload: nulls_first defaults to the ascending direction, so
  // an explicit request must agree with the plan's effective placement.
  EXPECT_TRUE(plan.IsOrderedBy({ColumnValueExp("a"), ColumnValueExp("b")},
                               {true, false}, {std::nullopt, std::nullopt}));
  EXPECT_TRUE(plan.IsOrderedBy({ColumnValueExp("a"), ColumnValueExp("b")},
                               {true, false}, {true, false}));
  EXPECT_FALSE(plan.IsOrderedBy({ColumnValueExp("a"), ColumnValueExp("b")},
                                {true, false}, {false, std::nullopt}));
  // Trailing nulls_first entries fall back to the ascending direction, so a
  // short vector behaves exactly like the all-nullopt one.
  EXPECT_TRUE(plan.IsOrderedBy({ColumnValueExp("a"), ColumnValueExp("b")},
                               {true, false}, {true}));

  const std::string expected =
      "IncrementalSort presorted=" + ColumnValueExp("a")->ToString();
  EXPECT_EQ(plan.ToString(), expected);
  std::string dump = DumpToString(plan);
  EXPECT_NE(dump.find(expected), std::string::npos);

  // No prefix keys: ToString keeps the bare name.
  IncrementalSortPlan bare(child, {}, suffix);
  EXPECT_EQ(bare.ToString(), "IncrementalSort");
}

TEST(GroupByPlanExtraTest, DumpAndCardinalityCaps) {
  Plan child = MakeValuesChild(/*rows=*/4);
  GroupByPlan plan(child, nullptr,
                   Schema("out", {Column("c", ValueType::kInt64)}));
  EXPECT_EQ(plan.ToString(), "GroupByFinish");
  EXPECT_EQ(plan.EmitRowCount(), 1U);  // scalar aggregation yields one row
  EXPECT_EQ(plan.AccessRowCount(), 4U);
  EXPECT_EQ(plan.GetSchema().ColumnCount(), 1U);
  EXPECT_EQ(plan.Child().get(), child.get());
  EXPECT_EQ(plan.Statement(), nullptr);
  std::string dump = DumpToString(plan);
  EXPECT_NE(dump.find("GroupByFinish"), std::string::npos);
  EXPECT_NE(dump.find("Values (rows=4)"), std::string::npos);
}

TEST(RelationalPlanExtraTest, DumpToStringAndRowCounts) {
  RelationalPlan plan(nullptr, Schema("out", {Column("c", ValueType::kInt64)}));
  EXPECT_EQ(plan.ToString(), "RelationalPlan(memo-selected)");
  std::string dump = DumpToString(plan);
  EXPECT_EQ(dump, "RelationalPlan(memo-selected)");
  EXPECT_EQ(DumpToString(plan, 2), "  RelationalPlan(memo-selected)");
  EXPECT_EQ(plan.AccessRowCount(), 0U);
  EXPECT_EQ(plan.EmitRowCount(), 0U);
  EXPECT_EQ(plan.ScanSource(), nullptr);
  EXPECT_EQ(plan.GetStats().Rows(), 0U);
}

class SkipScanDistinctPlanExtraTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "plan_extra-" + RandomString();
    db_ = Database::Create(prefix_).MoveValue();
    ctx_ = std::make_unique<TransactionContext>(db_->BeginContext());
    ASSIGN_OR_ASSERT_FAIL(
        Table, created,
        db_->CreateTable(*ctx_,
                         Schema("t", {Column("a", ValueType::kInt64),
                                      Column("b", ValueType::kVarChar)})));
    table_ = std::make_unique<Table>(std::move(created));
    index_ = std::make_unique<Index>("t_idx", std::vector<slot_t>{0}, 0);
  }
  void TearDown() override {
    ctx_.reset();
    index_.reset();
    table_.reset();
    db_->DeleteAll();
    db_.reset();
  }

  std::string prefix_;
  std::unique_ptr<Database> db_;
  std::unique_ptr<TransactionContext> ctx_;
  std::unique_ptr<Table> table_;
  std::unique_ptr<Index> index_;
};

TEST_F(SkipScanDistinctPlanExtraTest, StatsDrivenRowCountsAndOrdering) {
  const Schema out("out", {Column("a", ValueType::kInt64)});
  TableStatistics stats(Schema("t", {Column("a", ValueType::kInt64)}));
  stats.Assign(123, {ColumnStats(ValueType::kInt64)});
  ASSERT_EQ(stats.Rows(), 123U);
  ASSERT_EQ(stats.Columns(), 1U);
  ASSERT_EQ(stats.Column(0).Distinct(), 0U);

  SkipScanDistinctPlan plan(*table_, *index_, stats, /*ascending=*/true,
                            {NamedExpression("a", ColumnValueExp("a"))}, out);

  EXPECT_EQ(plan.ToString(), "SkipScanDistinct");
  EXPECT_EQ(plan.AccessRowCount(), 123U);
  // Zero recorded distinct values fall back to the one-row lower bound.
  EXPECT_EQ(plan.EmitRowCount(), 1U);
  EXPECT_EQ(plan.ScanSource(), table_.get());
  EXPECT_EQ(plan.GetStats().Rows(), 123U);
  EXPECT_EQ(plan.GetSchema().ColumnCount(), 1U);

  // Asc skip scan only claims the ascending order of its distinct column.
  EXPECT_TRUE(plan.IsOrderedBy({ColumnValueExp("a")}, {true}));
  EXPECT_FALSE(plan.IsOrderedBy({ColumnValueExp("a")}, {false}));
  EXPECT_FALSE(plan.IsOrderedBy({ColumnValueExp("b")}, {true}));
  EXPECT_FALSE(plan.IsOrderedBy({}, {}));

  std::string dump = DumpToString(plan);
  EXPECT_NE(dump.find("SkipScanDistinct"), std::string::npos);

  // A descending walk claims nothing.
  SkipScanDistinctPlan desc(*table_, *index_, stats, /*ascending=*/false,
                            {NamedExpression("a", ColumnValueExp("a"))}, out);
  EXPECT_FALSE(desc.IsOrderedBy({ColumnValueExp("a")}, {true}));

  // A schema-less stats object has no column-0 distinct either, and the
  // one-row lower bound applies again.
  TableStatistics empty(Schema("none", {}));
  SkipScanDistinctPlan none(*table_, *index_, empty, true,
                            {NamedExpression("a", ColumnValueExp("a"))}, out);
  EXPECT_EQ(none.EmitRowCount(), 1U);
  EXPECT_EQ(none.AccessRowCount(), 0U);
}

TEST(PlanExtraTest, RelationRenamePreservesColumnTypeAndConstraint) {
  Column col1("c1", ValueType::kInt64, Constraint(Constraint::kUnique));
  col1.SetUnsigned(true);
  Column col2("c2", ValueType::kVarChar, Constraint(Constraint::kNothing));
  Schema src_schema("orig_table", {col1, col2});
  Plan values = std::make_shared<ValuesPlan>(src_schema, std::vector<Row>{});
  RelationRenamePlan rename(values, "renamed_alias", "orig_table");

  const Schema& renamed_schema = rename.GetSchema();
  ASSERT_EQ(renamed_schema.ColumnCount(), 2U);

  const Column& rcol1 = renamed_schema.GetColumn(0);
  EXPECT_EQ(rcol1.Name().schema, "renamed_alias");
  EXPECT_EQ(rcol1.Name().name, "c1");
  EXPECT_EQ(rcol1.Type(), ValueType::kInt64);
  EXPECT_TRUE(rcol1.IsUnsigned());
  EXPECT_TRUE(rcol1.GetConstraint().IsUnique());

  const Column& rcol2 = renamed_schema.GetColumn(1);
  EXPECT_EQ(rcol2.Name().schema, "renamed_alias");
  EXPECT_EQ(rcol2.Name().name, "c2");
  EXPECT_EQ(rcol2.Type(), ValueType::kVarChar);
  EXPECT_FALSE(rcol2.IsUnsigned());

  EXPECT_EQ(rename.ToString(), "Rename: orig_table AS renamed_alias");
}

}  // namespace tinylamb
