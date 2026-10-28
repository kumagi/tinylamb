/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0. */
// The plan half of the grouped aggregation shapes (plan/aggregation_plan.hpp):
// one GROUP BY payload, three physical algorithms, and the order contract each
// shape may promise.
//
// What the plan layer owns:
//   * the payload every shape receives (group keys, aggregates, HAVING, the
//     estimated group count, the memory policy) and the output row shape it
//     implies (keys then aggregates);
//   * the properties the shapes differ on: no order for the hash table, the
//     group-key order for the sort runs, and the group-key order for the
//     streaming shape under its input-order contract;
//   * the memory contract (budget -> group capacity) and the EXPLAIN name;
//   * which shapes the `aggregation` implementation rule offers, and when.
//
// The operator half (executor/grouped_aggregate_test.cpp) drives what each
// shape does with the memory it was handed.
#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/constant_executor.hpp"
#include "executor/executor_base.hpp"
#include "executor/grouped_aggregate.hpp"
#include "executor/parallel_aggregation.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "plan/aggregation_plan.hpp"
#include "plan/cascades.hpp"
#include "plan/full_scan_plan.hpp"
#include "plan/implementation_rules.hpp"
#include "plan/parallel_thresholds.hpp"
#include "plan/plan.hpp"
#include "plan/sort_plan.hpp"
#include "query/sql_engine.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

using cascades::BestPlan;
using cascades::LogicalExpression;
using cascades::LogicalOperator;
using cascades::PhysicalProperties;
using cascades::PlanAlternative;
using cascades::RuleContext;

const Schema& LeafSchema() {
  static const Schema schema("leaf", {Column("k", ValueType::kInt64),
                                      Column("name", ValueType::kVarChar),
                                      Column("v", ValueType::kDouble)});
  return schema;
}

const TableStatistics& Stats() {
  static const TableStatistics stats{LeafSchema()};
  return stats;
}

const Expression& K() {
  static const Expression key = ColumnValueExp("k");
  return key;
}

const Expression& Name() {
  static const Expression key = ColumnValueExp("name");
  return key;
}

const Expression& V() {
  static const Expression key = ColumnValueExp("v");
  return key;
}

Expression CiName() {
  return std::make_shared<FunctionCallExpression>(
      "collate",
      std::vector<Expression>{Name(), ConstantValueExp(Value("und:ci"))});
}

NamedExpression CountStar() {
  return {"n", AggregateExpressionExp(AggregationType::kCount, nullptr)};
}

NamedExpression SumV() {
  return {"total", AggregateExpressionExp(AggregationType::kSum, V())};
}

Expression HavingGreaterThan(const std::string& column, double threshold) {
  return BinaryExpressionExp(ColumnValueExp(column),
                             BinaryOperation::kGreaterThan,
                             ConstantValueExp(Value(threshold)));
}

// A leaf that reports no order (standing in for a scan) and hands its rows to
// the operator, so the plan -> factory -> operator path can be driven without
// a table.
class LeafPlan final : public PlanBase {
 public:
  LeafPlan() = default;
  explicit LeafPlan(std::vector<Row> rows) : rows_(std::move(rows)) {}

  Executor EmitExecutor(TransactionContext& /*ctx*/) const override {
    return std::make_shared<ConstantExecutor>(rows_);
  }
  [[nodiscard]] const Table* ScanSource() const override { return nullptr; }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return Stats();
  }
  [[nodiscard]] const Schema& GetSchema() const override {
    return LeafSchema();
  }
  [[nodiscard]] size_t AccessRowCount() const override { return rows_.size(); }
  [[nodiscard]] size_t EmitRowCount() const override { return rows_.size(); }
  void Dump(std::ostream& out, int indent) const override {
    out << Indent(static_cast<size_t>(indent)) << "Leaf\n";
  }
  [[nodiscard]] std::string ToString() const override { return "Leaf"; }

 private:
  std::vector<Row> rows_{};
};

Plan Leaf() { return std::make_shared<LeafPlan>(); }

Plan LeafWith(std::vector<Row> rows) {
  return std::make_shared<LeafPlan>(std::move(rows));
}

// A leaf whose statistics claim `rows` rows with every key distinct, so the
// rule's group estimate is the row count. A small one is what a normal scan
// looks like; a huge one is the case the hash table cannot hold, because its
// groups outgrow the memory budget's group capacity and it has to partition.
Plan WideLeaf(size_t rows) {
  class WideLeafPlan final : public PlanBase {
   public:
    explicit WideLeafPlan(size_t rows)
        : rows_(rows),
          stats_(TableStatistics(LeafSchema()).ScaleToRows(rows)) {}
    Executor EmitExecutor(TransactionContext& /*ctx*/) const override {
      return std::make_shared<ConstantExecutor>(std::vector<Row>{});
    }
    [[nodiscard]] const Table* ScanSource() const override { return nullptr; }
    [[nodiscard]] const TableStatistics& GetStats() const override {
      return stats_;
    }
    [[nodiscard]] const Schema& GetSchema() const override {
      return LeafSchema();
    }
    [[nodiscard]] size_t AccessRowCount() const override { return rows_; }
    [[nodiscard]] size_t EmitRowCount() const override { return rows_; }
    void Dump(std::ostream& out, int indent) const override {
      out << Indent(static_cast<size_t>(indent)) << "WideLeaf\n";
    }
    [[nodiscard]] std::string ToString() const override { return "WideLeaf"; }

   private:
    size_t rows_{0};
    TableStatistics stats_;
  };
  return std::make_shared<WideLeafPlan>(rows);
}

// An ordered leaf, so the stream shape's input-order contract can hold.
Plan OrderedLeaf() {
  class OrderedLeafPlan final : public PlanBase {
   public:
    Executor EmitExecutor(TransactionContext& ctx) const override {
      return Leaf()->EmitExecutor(ctx);
    }
    [[nodiscard]] const Table* ScanSource() const override { return nullptr; }
    [[nodiscard]] const TableStatistics& GetStats() const override {
      return Stats();
    }
    [[nodiscard]] const Schema& GetSchema() const override {
      return LeafSchema();
    }
    [[nodiscard]] size_t AccessRowCount() const override { return 0; }
    [[nodiscard]] size_t EmitRowCount() const override { return 0; }
    [[nodiscard]] bool IsOrderedBy(
        const std::vector<Expression>& expressions,
        const std::vector<bool>& ascending) const override {
      if (expressions.size() != 1 || ascending.size() != 1 || !ascending[0]) {
        return false;
      }
      return expressions[0]->ToString() == K()->ToString();
    }
    [[nodiscard]] bool IsOrderedBy(
        const std::vector<Expression>& expressions,
        const std::vector<bool>& ascending,
        const std::vector<std::optional<bool>>& nulls_first) const override {
      if (!IsOrderedBy(expressions, ascending)) {
        return false;
      }
      return nulls_first.empty() || nulls_first[0].value_or(true);
    }
    void Dump(std::ostream& out, int indent) const override {
      out << Indent(static_cast<size_t>(indent)) << "Leaf (ordered by k)\n";
    }
    [[nodiscard]] std::string ToString() const override {
      return "Leaf (ordered by k)";
    }
  };
  return std::make_shared<OrderedLeafPlan>();
}

AggregatePayload GroupedPayload(std::vector<Expression> keys = {K()},
                                double estimated_groups = 3.0) {
  AggregatePayload payload;
  payload.group_keys.emplace_back("k", keys.front());
  payload.aggregates = {CountStar(), SumV()};
  payload.estimated_groups = estimated_groups;
  return payload;
}

const cascades::ImplementationRule& RuleByName(const std::string& name) {
  for (const auto& rule : DefaultImplementationRules().Rules()) {
    if (rule.Name() == name) {
      return rule;
    }
  }
  throw std::runtime_error("no implementation rule named " + name);
}

LogicalExpression AggregationLogical(cascades::GroupId child,
                                     std::vector<Expression> keys,
                                     bool having) {
  LogicalExpression logical;
  logical.operation = LogicalOperator::kAggregation;
  logical.children = {child};
  logical.target_list = {CountStar(), SumV()};
  logical.grouping_sets = std::move(keys);
  if (having) {
    logical.predicate = HavingGreaterThan("total", 10.0);
  }
  return logical;
}

std::vector<PlanAlternative> ApplyAggregationRule(Plan child,
                                                  std::vector<Expression> keys,
                                                  double estimated_rows = 100.0,
                                                  bool having = false) {
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"leaf"});
  const LogicalExpression logical =
      AggregationLogical(root, std::move(keys), having);
  const std::vector<BestPlan> children{
      BestPlan{.plan = std::move(child), .estimated_rows = estimated_rows}};
  return RuleByName("aggregation")
      .Apply(memo, root, logical, children, PhysicalProperties{},
             RuleContext::Empty());
}

// The exact shape name, the way the memo reports it as a physical operator.
std::string ShapeName(const Plan& plan) {
  const auto aggregation = std::dynamic_pointer_cast<AggregationPlan>(plan);
  return aggregation == nullptr ? plan->ToString()
                                : aggregation->StrategyName();
}

std::vector<std::string> ShapeNames(
    const std::vector<PlanAlternative>& alternatives) {
  std::vector<std::string> names;
  names.reserve(alternatives.size());
  for (const PlanAlternative& alternative : alternatives) {
    names.push_back(ShapeName(alternative.plan));
  }
  return names;
}

// gtest has no operator<< for a vector of strings, so the shape list is
// rendered once for a failure message.
std::string ShapeList(const std::vector<PlanAlternative>& alternatives) {
  std::ostringstream out;
  const std::vector<std::string> names = ShapeNames(alternatives);
  for (size_t i = 0; i < names.size(); ++i) {
    out << (i == 0 ? "" : ", ") << names[i];
  }
  return out.str();
}

class AggregateShapeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    database_ =
        Database::Create("aggregate_shape_test-" + RandomString()).MoveValue();
  }
  void TearDown() override {
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
  }
  std::unique_ptr<Database> database_;
};

}  // namespace

TEST_F(AggregateShapeTest, ComputedGroupingDoesNotProveAtMostOneRow) {
  for (const int mode : {0, 1, 2}) {
    SCOPED_TRACE(mode);
    cascades::Memo memo;
    (void)memo.Build({"leaf"});
    const auto values = memo.EnsureDerivedGroup({"leaf"}, "values");
    const Schema input("leaf", {Column("k", ValueType::kInt64),
                                Column("v", ValueType::kInt64)});
    ASSERT_TRUE(memo.AddExpression(
        values, LogicalExpression{.operation = LogicalOperator::kValues,
                                  .values = {Row({Value(0), Value(1)}),
                                             Row({Value(0), Value(2)}),
                                             Row({Value(0), Value(3)})},
                                  .output_schema = input}));
    const auto aggregate = memo.EnsureDerivedGroup({"leaf"}, "grouped");
    LogicalExpression grouping;
    grouping.operation = LogicalOperator::kAggregation;
    grouping.children = {values};
    grouping.target_list = {CountStar()};
    const Expression key = FunctionCallExp("abs", {ColumnValueExp("leaf.v")});
    (mode == 1 ? grouping.partition_by : grouping.grouping_sets) = {key};
    if (mode == 2) {
      grouping.grouping_sets.push_back(ColumnValueExp("leaf.k"));
    }
    ASSERT_TRUE(memo.AddExpression(aggregate, grouping));
    EXPECT_FALSE(memo.Get(aggregate).logical_properties.max_1_row);
    EXPECT_FALSE(memo.Get(aggregate).logical_properties.IsUniqueOn({"leaf.k"}));
    const auto projection = memo.EnsureDerivedGroup({"leaf"}, "counts");
    ASSERT_TRUE(memo.AddExpression(
        projection, LogicalExpression{.operation = LogicalOperator::kProjection,
                                      .children = {aggregate},
                                      .target_list = {NamedExpression("n")}}));
    const auto root = memo.EnsureDerivedGroup({"leaf"}, "distinct_counts");
    ASSERT_TRUE(memo.AddExpression(
        root, LogicalExpression{.operation = LogicalOperator::kDistinct,
                                .children = {projection}}));
    cascades::SearchEngine search(std::move(memo),
                                  cascades::RuleSet::Default());
    search.Explore(root);
    const auto best = search.Optimize(root, PhysicalProperties{},
                                      DefaultImplementationRules());
    ASSERT_TRUE(best.has_value());
    auto context = database_->BeginContext();
    const auto executor = best->plan->EmitExecutor(context);
    std::vector<Row> rows;
    Row row;
    while (executor->Next(&row, nullptr)) {
      rows.push_back(row);
    }
    ASSERT_EQ(executor->GetStatus(), Status::kSuccess);
    EXPECT_EQ(rows, (std::vector<Row>{Row({Value(1)})}));
  }
}

TEST_F(AggregateShapeTest, CountOfNullSupplyingOuterColumnKeepsItsArgument) {
  for (const bool project_right : {false, true}) {
    SCOPED_TRACE(project_right);
    cascades::Memo memo;
    const Schema left_schema("l", {Column("id", ValueType::kInt64,
                                          Constraint(Constraint::kNotNull))});
    const Schema right_schema("r", {Column("id", ValueType::kInt64,
                                           Constraint(Constraint::kNotNull))});
    memo.SetTableSchemas({{"l", left_schema}, {"r", right_schema}});
    (void)memo.Build({"l", "r"});
    const auto left = memo.EnsureDerivedGroup({"l"}, "left_values");
    const auto right = memo.EnsureDerivedGroup({"r"}, "right_values");
    ASSERT_TRUE(memo.AddExpression(
        left, LogicalExpression{.operation = LogicalOperator::kValues,
                                .values = {Row({Value(1)}), Row({Value(2)})},
                                .output_schema = left_schema}));
    ASSERT_TRUE(memo.AddExpression(
        right, LogicalExpression{.operation = LogicalOperator::kValues,
                                 .values = {Row({Value(1)})},
                                 .output_schema = right_schema}));
    const auto outer = memo.EnsureDerivedGroup({"l", "r"}, "outer");
    ASSERT_TRUE(memo.AddExpression(
        outer,
        LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                          .children = {left, right},
                          .predicate = BinaryExpressionExp(
                              ColumnValueExp("l.id"), BinaryOperation::kEquals,
                              ColumnValueExp("r.id")),
                          .join_type = 0,
                          .output_schema = left_schema + right_schema}));
    EXPECT_FALSE(memo.Get(outer).logical_properties.IsNotNull("r.id"));
    auto input = outer;
    if (project_right) {
      input = memo.EnsureDerivedGroup({"l", "r"}, "project_outer");
      ASSERT_TRUE(memo.AddExpression(
          input, LogicalExpression{.operation = LogicalOperator::kProjection,
                                   .children = {outer},
                                   .target_list = {NamedExpression(
                                       "id", ColumnValueExp("r.id"))},
                                   .output_schema = right_schema}));
      EXPECT_FALSE(memo.Get(input).logical_properties.IsNotNull("r.id"));
    }
    const auto root = memo.EnsureDerivedGroup({"l", "r"}, "count_outer");
    ASSERT_TRUE(memo.AddExpression(
        root, LogicalExpression{
                  .operation = LogicalOperator::kAggregation,
                  .children = {input},
                  .target_list = {NamedExpression(
                      "n", AggregateExpressionExp(AggregationType::kCount,
                                                  ColumnValueExp("r.id")))}}));
    cascades::SearchEngine search(std::move(memo),
                                  cascades::RuleSet::Default());
    search.Explore(root);
    EXPECT_FALSE(
        search.AppliedRuleNames().contains("count_star_rewrite_on_not_null"));
    const auto best = search.Optimize(root, PhysicalProperties{},
                                      DefaultImplementationRules());
    ASSERT_TRUE(best.has_value());
    auto context = database_->BeginContext();
    const auto executor = best->plan->EmitExecutor(context);
    Row row;
    ASSERT_TRUE(executor->Next(&row, nullptr));
    EXPECT_EQ(row, Row({Value(1)}));
    EXPECT_FALSE(executor->Next(&row, nullptr));
    EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
  }
}

TEST_F(AggregateShapeTest, ReplacingANotNullColumnWithNullDoesNotRewriteCount) {
  cascades::Memo memo;
  const Schema base(
      "t", {Column("id", ValueType::kInt64, Constraint(Constraint::kNotNull))});
  memo.SetTableSchemas({{"t", base}});
  (void)memo.Build({"t"});
  const auto values = memo.EnsureDerivedGroup({"t"}, "values");
  ASSERT_TRUE(memo.AddExpression(
      values, LogicalExpression{.operation = LogicalOperator::kValues,
                                .values = {Row({Value(1)}), Row({Value(2)})},
                                .output_schema = base}));
  const auto project = memo.EnsureDerivedGroup({"t"}, "null_projection");
  ASSERT_TRUE(memo.AddExpression(
      project,
      LogicalExpression{
          .operation = LogicalOperator::kProjection,
          .children = {values},
          .target_list = {NamedExpression("id", ConstantValueExp(Value()))},
          .output_schema = Schema("t", {Column("id", ValueType::kInt64)})}));
  EXPECT_FALSE(memo.Get(project).logical_properties.IsNotNull("t.id"));
  const auto root = memo.EnsureDerivedGroup({"t"}, "count_null");
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{
                .operation = LogicalOperator::kAggregation,
                .children = {project},
                .target_list = {NamedExpression(
                    "n", AggregateExpressionExp(AggregationType::kCount,
                                                ColumnValueExp("t.id")))}}));
  cascades::SearchEngine search(std::move(memo), cascades::RuleSet::Default());
  search.Explore(root);
  EXPECT_FALSE(
      search.AppliedRuleNames().contains("count_star_rewrite_on_not_null"));
  const auto best =
      search.Optimize(root, PhysicalProperties{}, DefaultImplementationRules());
  ASSERT_TRUE(best.has_value());
  auto context = database_->BeginContext();
  const auto executor = best->plan->EmitExecutor(context);
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(0)}));
  EXPECT_FALSE(executor->Next(&row, nullptr));
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
}

TEST_F(AggregateShapeTest, JoinFanoutDoesNotPreserveBasePrimaryKeyUniqueness) {
  cascades::Memo memo;
  const Schema left_schema("l", {Column("id", ValueType::kInt64,
                                        Constraint(Constraint::kPrimaryKey))});
  const Schema right_schema("r", {Column("id", ValueType::kInt64)});
  memo.SetTableSchemas({{"l", left_schema}, {"r", right_schema}});
  (void)memo.Build({"l", "r"});
  const auto left = memo.EnsureDerivedGroup({"l"}, "one_left");
  const auto right = memo.EnsureDerivedGroup({"r"}, "many_right");
  ASSERT_TRUE(memo.AddExpression(
      left, LogicalExpression{.operation = LogicalOperator::kValues,
                              .values = {Row({Value(1)})},
                              .output_schema = left_schema}));
  ASSERT_TRUE(memo.AddExpression(
      right, LogicalExpression{.operation = LogicalOperator::kValues,
                               .values = {Row({Value(1)}), Row({Value(1)})},
                               .output_schema = right_schema}));
  const auto join = memo.EnsureDerivedGroup({"l", "r"}, "fanout");
  ASSERT_TRUE(memo.AddExpression(
      join,
      LogicalExpression{.operation = LogicalOperator::kJoin,
                        .children = {left, right},
                        .predicate = BinaryExpressionExp(
                            ColumnValueExp("l.id"), BinaryOperation::kEquals,
                            ColumnValueExp("r.id")),
                        .output_schema = left_schema + right_schema}));
  EXPECT_FALSE(memo.Get(join).logical_properties.IsUniqueOn({"l.id"}));
  const auto project = memo.EnsureDerivedGroup({"l", "r"}, "project_left");
  ASSERT_TRUE(memo.AddExpression(
      project, LogicalExpression{.operation = LogicalOperator::kProjection,
                                 .children = {join},
                                 .target_list = {NamedExpression(
                                     "id", ColumnValueExp("l.id"))},
                                 .output_schema = left_schema}));
  EXPECT_FALSE(memo.Get(project).logical_properties.IsUniqueOn({"l.id"}));
  const auto root = memo.EnsureDerivedGroup({"l", "r"}, "unique_left");
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{.operation = LogicalOperator::kDistinct,
                              .children = {project}}));
  cascades::SearchEngine search(std::move(memo), cascades::RuleSet::Default());
  search.Explore(root);
  const auto best =
      search.Optimize(root, PhysicalProperties{}, DefaultImplementationRules());
  ASSERT_TRUE(best.has_value());
  auto context = database_->BeginContext();
  const auto executor = best->plan->EmitExecutor(context);
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(1)}));
  EXPECT_FALSE(executor->Next(&row, nullptr));
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
}

TEST_F(AggregateShapeTest, StreamingRequiresTheGroupKeysCollation) {
  const Plan input = std::make_shared<BufferedSortPlan>(
      LeafWith({Row({Value(1), Value("a"), Value(1.0)}),
                Row({Value(1), Value("A"), Value(1.0)}),
                Row({Value(1), Value("a"), Value(1.0)})}),
      std::vector<SortKey>{SortKey{.expression = Name(),
                                   .ascending = true,
                                   .nulls_first = std::nullopt,
                                   .collation = Collation::kCaseInsensitive}});
  const auto alternatives = ApplyAggregationRule(input, {Name()}, 3);
  auto context = database_->BeginContext();
  for (const auto& alternative : alternatives) {
    SCOPED_TRACE(ShapeName(alternative.plan));
    EXPECT_EQ(std::dynamic_pointer_cast<StreamAggregatePlan>(alternative.plan),
              nullptr);
    const auto executor = alternative.plan->EmitExecutor(context);
    Row row;
    size_t groups = 0;
    while (executor->Next(&row, nullptr)) {
      ++groups;
    }
    EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
    EXPECT_EQ(groups, 2U);
  }
}

TEST_F(AggregateShapeTest, SearchCanChooseOrderedInputAndStreamAggregate) {
  cascades::Memo memo;
  const auto scan = memo.Build({"leaf"});
  const auto root = memo.EnsureDerivedGroup({"leaf"}, "grouped");
  memo.AddExpression(root, AggregationLogical(scan, {K()}, false));
  cascades::RuleSet logical_rules;
  cascades::SearchEngine search(std::move(memo), logical_rules);
  auto best = search.Optimize(
      root, PhysicalProperties{},
      [&](cascades::GroupId group, const cascades::Memo& current,
          const LogicalExpression& logical,
          const std::vector<BestPlan>& children,
          const PhysicalProperties& required, const RuleContext& context) {
        if (logical.operation == LogicalOperator::kScan) {
          return std::vector<PlanAlternative>{
              {.plan = Leaf(), .local_cost = 100.0, .estimated_rows = 100.0},
              {.plan = OrderedLeaf(),
               .local_cost = 105.0,
               .estimated_rows = 100.0}};
        }
        return RuleByName("aggregation")
            .Apply(current, group, logical, children, required, context);
      },
      RuleContext{});
  ASSERT_TRUE(best);
  // gtest ASSERT_TRUE is the guard; clang-tidy does not model it.
  EXPECT_NE(  // NOLINT(bugprone-unchecked-optional-access)
      std::dynamic_pointer_cast<StreamAggregatePlan>(best->plan), nullptr);
  EXPECT_DOUBLE_EQ(  // NOLINT(bugprone-unchecked-optional-access)
      best->cost, 185.0);
}

TEST_F(AggregateShapeTest,
       GroupingOnlyAlternativeHasAnExecutableImplementation) {
  cascades::Memo memo;
  const auto scan = memo.Build({"leaf"});
  LogicalExpression logical;
  logical.operation = LogicalOperator::kAggregation;
  logical.children = {scan};
  logical.grouping_sets = {K()};
  logical.target_list = {NamedExpression("key", K())};
  const auto alternatives =
      RuleByName("aggregation")
          .Apply(memo, scan, logical,
                 {BestPlan{
                     .plan = LeafWith({Row({Value(1), Value("a"), Value(2)}),
                                       Row({Value(1), Value("b"), Value(3)})}),
                     .estimated_rows = 2.0}},
                 PhysicalProperties{}, RuleContext{});
  ASSERT_EQ(alternatives.size(), 1U);
  auto context = database_->BeginContext();
  auto executor = alternatives.front().plan->EmitExecutor(context);
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(1)}));
  EXPECT_FALSE(executor->Next(&row, nullptr));
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
}

// ------------------------------------------------------------ payload contract
// --

TEST_F(AggregateShapeTest, EveryShapeCarriesTheSamePayloadAndOutputShape) {
  AggregatePayload payload = GroupedPayload();
  payload.having = HavingGreaterThan("total", 10.0);
  const std::shared_ptr<HashAggregatePlan> hash =
      std::make_shared<HashAggregatePlan>(Leaf(), payload);
  const std::shared_ptr<SortAggregatePlan> sort =
      std::make_shared<SortAggregatePlan>(Leaf(), payload);
  const std::shared_ptr<StreamAggregatePlan> stream =
      std::make_shared<StreamAggregatePlan>(Leaf(), payload);

  for (const std::shared_ptr<AggregationPlan>& plan :
       std::vector<std::shared_ptr<AggregationPlan>>{hash, sort, stream}) {
    EXPECT_EQ(plan->GroupKeys().size(), 1U);
    EXPECT_EQ(plan->GroupKeys().front().name, "k");
    EXPECT_EQ(plan->Aggregates().size(), 2U);
    EXPECT_EQ(plan->GroupKeysToString(), "k");
    EXPECT_TRUE(plan->Having() != nullptr);
    EXPECT_EQ(plan->EstimatedGroups(), 3.0);
    EXPECT_EQ(plan->AccessRowCount(), plan->Child()->AccessRowCount());
    EXPECT_EQ(plan->ScanSource(), plan->Child()->ScanSource());
    // One row per group, so the estimate the rule computed is the row count the
    // parent sees.
    EXPECT_EQ(plan->EmitRowCount(), 3U);
    EXPECT_EQ(plan->GetStats().Rows(), 3U);
    EXPECT_EQ(plan->GetStats().Columns(), plan->GetSchema().ColumnCount());
    // keys then aggregates: the output row shape the HAVING scope and the
    // projection above both rely on.
    const Schema& schema = plan->GetSchema();
    ASSERT_EQ(schema.ColumnCount(), 3U) << plan->ToString();
    EXPECT_EQ(schema.GetColumn(0).Name().name, "k");
    EXPECT_EQ(schema.GetColumn(0).Type(), ValueType::kInt64);
    EXPECT_EQ(schema.GetColumn(1).Name().name, "n");
    EXPECT_EQ(schema.GetColumn(2).Name().name, "total");
    // Group keys are unique by construction, so a consumer must not add a
    // DISTINCT above this node.
    EXPECT_TRUE(plan->EnforcesDistinct());
    EXPECT_EQ(plan->StrategyName(),
              plan == hash
                  ? "HashAggregate"
                  : (plan == sort ? "SortAggregate" : "StreamAggregate"));
  }
  EXPECT_EQ(hash->Strategy(), AggregationStrategy::kHash);
  EXPECT_EQ(sort->Strategy(), AggregationStrategy::kSort);
  EXPECT_EQ(stream->Strategy(), AggregationStrategy::kStream);
}

TEST_F(AggregateShapeTest, TheLegacyScalarPayloadKeepsItsHistoricalShape) {
  // A payload with no keys is one group: the rendering, the schema and the
  // single-row estimate are exactly what they were before the payload existed,
  // so every existing caller (the single-relation fast path, the Cascades
  // rule) keeps working unchanged.
  const std::vector<NamedExpression> aggregates = {CountStar(), SumV()};
  const AggregationPlan base(Leaf(), aggregates);
  EXPECT_EQ(base.ToString(), "HashAggregate {n: COUNT(*), total: SUM(v)}");
  EXPECT_EQ(base.StrategyName(), "HashAggregate");
  EXPECT_EQ(base.GetSchema().ColumnCount(), 2U);
  EXPECT_EQ(base.EmitRowCount(), 1U);
  EXPECT_EQ(base.GetStats().Rows(), 1U);
  EXPECT_TRUE(base.GroupKeys().empty());
  std::ostringstream dump;
  base.Dump(dump, 0);
  EXPECT_NE(dump.str().find("HashAggregate {"), std::string::npos)
      << dump.str();
  // The three shape types still accept the keyless payload directly, and their
  // names stay distinct so EXPLAIN can tell them apart.
  const HashAggregatePlan hash(Leaf(), aggregates);
  const SortAggregatePlan sort(Leaf(), aggregates);
  const StreamAggregatePlan stream(Leaf(), aggregates);
  EXPECT_NE(hash.ToString(), sort.ToString());
  EXPECT_NE(sort.ToString(), stream.ToString());
  EXPECT_EQ(hash.StrategyName(), "HashAggregate");
  EXPECT_EQ(sort.StrategyName(), "SortAggregate");
  EXPECT_EQ(stream.StrategyName(), "StreamAggregate");
}

TEST_F(AggregateShapeTest, ParameterSlotsCoverKeysAggregatesAndHaving) {
  AggregatePayload payload;
  payload.group_keys.emplace_back(
      "k", std::make_shared<ParameterSlot>(0, Value(int64_t{1})));
  payload.aggregates = {NamedExpression(
      "n",
      AggregateExpressionExp(AggregationType::kSum,
                             std::make_shared<ParameterSlot>(1, Value(2.0))))};
  payload.having = BinaryExpressionExp(
      std::make_shared<ParameterSlot>(2, Value(1.0)),
      BinaryOperation::kGreaterThan, ConstantValueExp(Value(0.0)));
  for (const Plan& plan :
       {std::static_pointer_cast<PlanBase>(
            std::make_shared<HashAggregatePlan>(Leaf(), payload)),
        std::static_pointer_cast<PlanBase>(
            std::make_shared<SortAggregatePlan>(Leaf(), payload)),
        std::static_pointer_cast<PlanBase>(
            std::make_shared<StreamAggregatePlan>(Leaf(), payload))}) {
    EXPECT_TRUE(plan->ContainsParameterSlots());
    std::vector<Expression> slots;
    size_t cursor = 0;
    EXPECT_TRUE(plan->InstallParameterSlots(
        {Value(int64_t{7}), Value(3.5), Value(2.5)}, &cursor, &slots));
    EXPECT_EQ(slots.size(), 3U) << plan->ToString();
    EXPECT_EQ(cursor, 0U);
  }
}

// ------------------------------------------------------------- order contract
// --

TEST_F(AggregateShapeTest, OnlyTheOrderedShapesPromiseAnOrder) {
  const std::shared_ptr<HashAggregatePlan> hash =
      std::make_shared<HashAggregatePlan>(Leaf(), GroupedPayload());
  const std::shared_ptr<SortAggregatePlan> sort =
      std::make_shared<SortAggregatePlan>(Leaf(), GroupedPayload());
  const std::shared_ptr<StreamAggregatePlan> stream =
      std::make_shared<StreamAggregatePlan>(Leaf(), GroupedPayload());
  const std::vector<bool> ascending{true};
  const std::vector<std::optional<bool>> nulls_first{std::nullopt};
  const std::vector<std::optional<bool>> explicit_nulls_first{true};
  const std::vector<std::optional<bool>> nulls_last{false};

  // A hash table promises nothing: its groups come out in first-appearance
  // order, which is not an order a consumer may rely on.
  EXPECT_FALSE(hash->IsOrderedBy({K()}, ascending));
  EXPECT_FALSE(hash->IsOrderedBy({K()}, ascending, nulls_first));
  // The sort runs produce the group-key order, and so does the streaming pass
  // under its input-order contract.
  for (const std::shared_ptr<AggregationPlan>& plan :
       std::vector<std::shared_ptr<AggregationPlan>>{sort, stream}) {
    EXPECT_TRUE(plan->IsOrderedBy({K()}, ascending)) << plan->ToString();
    EXPECT_TRUE(plan->IsOrderedBy({K()}, ascending, nulls_first))
        << plan->ToString();
    EXPECT_TRUE(plan->IsOrderedBy({K()}, ascending, explicit_nulls_first))
        << plan->ToString();
    // NULLS LAST is a different order: the group-key sort is ascending, which
    // places NULLs first.
    EXPECT_FALSE(plan->IsOrderedBy({K()}, ascending, nulls_last))
        << plan->ToString();
    // A prefix of the delivered order is delivered too, but only a prefix.
    EXPECT_FALSE(plan->IsOrderedBy({K(), Name()}, {true, true}));
    EXPECT_FALSE(plan->IsOrderedBy({Name()}, ascending));
    // ...but not a different direction, a different key or a different
    // NULL placement.
    EXPECT_FALSE(plan->IsOrderedBy({K()}, {false}));
    EXPECT_FALSE(plan->IsOrderedBy({Name()}, ascending));
    EXPECT_FALSE(plan->IsOrderedBy({K()}, ascending, nulls_last));
  }
  // A keyless payload delivers no order either: it produces one row, which is
  // not a claim any consumer needs.
  const std::shared_ptr<StreamAggregatePlan> scalar =
      std::make_shared<StreamAggregatePlan>(
          Leaf(), AggregatePayload{.aggregates = {CountStar()}});
  EXPECT_FALSE(scalar->IsOrderedBy({K()}, ascending));
  EXPECT_FALSE(scalar->RequiresOrderedInput());
  EXPECT_TRUE(stream->RequiresOrderedInput());
}

TEST_F(AggregateShapeTest, TheStreamShapesOrderIsACollatedOne) {
  const std::shared_ptr<StreamAggregatePlan> folded =
      std::make_shared<StreamAggregatePlan>(Leaf(), GroupedPayload({CiName()}));
  EXPECT_EQ(folded->GroupKeysToString(), "collate(name, \"und:ci\")");
  // The key carries a case-insensitive collation, so the delivered order folds
  // too: a case-insensitive requirement is answered, a binary one is not.
  PhysicalProperties folded_requirement;
  folded_requirement.ordering = {ColumnName("name")};
  folded_requirement.collation = "und:ci";
  PhysicalProperties binary_requirement;
  binary_requirement.ordering = {ColumnName("name")};
  binary_requirement.collation = "";
  EXPECT_TRUE(cascades::SatisfiesOrderingCollation(folded, folded_requirement));
  EXPECT_FALSE(
      cascades::SatisfiesOrderingCollation(folded, binary_requirement));
}

// ------------------------------------------------------------- memory contract
// --

TEST_F(AggregateShapeTest, ThePolicyDefinesTheGroupCapacityOfEachShape) {
  AggregateMemoryPolicy memory;
  memory.memory_budget_bytes = 64 * kAggregateEstimatedRowBytes;  // 64 groups
  memory.spill_partition_count = 8;
  memory.merge_window_rows = 16;

  AggregatePayload spilled = GroupedPayload();
  spilled.memory = memory;
  const HashAggregatePlan hash(Leaf(), spilled);
  const SortAggregatePlan sort(Leaf(), spilled);
  const StreamAggregatePlan stream(Leaf(), spilled);

  EXPECT_EQ(AggregateGroupCapacity(memory), 64U);
  EXPECT_EQ(hash.RowCapacity(), 64U);
  EXPECT_EQ(sort.RowCapacity(), 64U);
  // The streaming shape keeps one group, so it has no group table to overflow.
  EXPECT_EQ(stream.RowCapacity(), 1U);
  EXPECT_EQ(hash.SpillPartitionCount(), 8U);
  EXPECT_EQ(sort.SpillThresholdRows(), 64U);
  EXPECT_EQ(sort.MergeWindowRows(), 16U);
  EXPECT_EQ(hash.Memory().memory_budget_bytes,
            64U * kAggregateEstimatedRowBytes);
  // The default policy is the optimizer's memory budget expressed in groups.
  const AggregateMemoryPolicy defaults;
  EXPECT_EQ(defaults.memory_budget_bytes, kAggregateDefaultMemoryBudgetBytes);
  EXPECT_EQ(defaults.spill_partition_count, kAggregateDefaultSpillPartitions);
  EXPECT_EQ(defaults.merge_window_rows, kAggregateDefaultMergeWindowRows);
  EXPECT_EQ(AggregateGroupCapacity(),
            defaults.memory_budget_bytes / kAggregateEstimatedRowBytes);
  EXPECT_EQ(AggregateSpillPartitionCount(), defaults.spill_partition_count);
  // The operator's own defaults must match the plan's, or a directly
  // constructed operator would be planned differently.
  EXPECT_EQ(kGroupedAggregateDefaultMemoryBudgetBytes,
            kAggregateDefaultMemoryBudgetBytes);
  EXPECT_EQ(kGroupedAggregateDefaultSpillPartitions,
            kAggregateDefaultSpillPartitions);
  EXPECT_EQ(kGroupedAggregateDefaultMergeWindowRows,
            kAggregateDefaultMergeWindowRows);
  EXPECT_EQ(kGroupedAggregateEstimatedRowBytes, kAggregateEstimatedRowBytes);
  // A zero budget is the unbounded policy: no division by zero, and a single
  // partition is still a legal (if degenerate) spill shape.
  AggregateMemoryPolicy unbounded;
  unbounded.memory_budget_bytes = 0;
  unbounded.spill_partition_count = 0;
  EXPECT_EQ(AggregateGroupCapacity(unbounded),
            std::numeric_limits<size_t>::max());
  EXPECT_EQ(AggregateSpillPartitionCount(unbounded), 1U);
  // A budget narrower than one estimated group still admits one group.
  AggregateMemoryPolicy tiny;
  tiny.memory_budget_bytes = 1;
  EXPECT_EQ(AggregateGroupCapacity(tiny), 1U);
}

// -------------------------------------------------------------------- EXPLAIN
// --

TEST_F(AggregateShapeTest, ExplainNamesTheShapeItsKeysAndItsMemoryContract) {
  AggregateMemoryPolicy memory;
  memory.memory_budget_bytes = 320;  // 5 groups of the estimated width
  memory.spill_partition_count = 4;
  memory.merge_window_rows = 8;
  AggregatePayload budgeted = GroupedPayload();
  budgeted.memory = memory;
  const Plan hash = std::make_shared<HashAggregatePlan>(Leaf(), budgeted);
  const Plan sort = std::make_shared<SortAggregatePlan>(Leaf(), budgeted);
  const Plan stream = std::make_shared<StreamAggregatePlan>(Leaf(), budgeted);

  EXPECT_EQ(hash->ToString(),
            "HashAggregate: [k] {n: COUNT(*), total: SUM(v)} (hash table, "
            "budget=320B, groups=5)");
  EXPECT_EQ(sort->ToString(),
            "SortAggregate: [k] {n: COUNT(*), total: SUM(v)} (sort runs, "
            "budget=320B, run_rows=5, window=8)");
  EXPECT_EQ(stream->ToString(),
            "StreamAggregate: [k] {n: COUNT(*), total: SUM(v)} (streaming, "
            "groups=1)");
  // The name before the first colon is what the memo reports as the physical
  // operator, so no shape may hide a colon in its name.
  for (const Plan& plan : {hash, sort, stream}) {
    const std::string text = plan->ToString();
    ASSERT_NE(text.find(':'), std::string::npos) << text;
    EXPECT_EQ(text.substr(0, text.find(':')),
              std::dynamic_pointer_cast<AggregationPlan>(plan)->StrategyName());
    std::ostringstream dump;
    plan->Dump(dump, 0);
    EXPECT_NE(dump.str().find("Leaf"), std::string::npos) << dump.str();
    EXPECT_NE(
        dump.str().find(
            std::dynamic_pointer_cast<AggregationPlan>(plan)->StrategyName()),
        std::string::npos)
        << dump.str();
  }
}

TEST_F(AggregateShapeTest, ExplainInfoCarriesTheRuleAndTheDeliveredOrdering) {
  PhysicalProperties required;
  required.ordering = {ColumnName("k")};
  required.sort_ascending = {true};

  // An ordered child, so all three shapes are on the table.
  std::vector<PlanAlternative> alternatives =
      ApplyAggregationRule(OrderedLeaf(), {K()});
  ASSERT_EQ(alternatives.size(), 3U) << ShapeList(alternatives);
  const Expression& key = K();
  const std::vector<bool> ascending{true};
  const std::vector<std::optional<bool>> nulls_first{true};

  for (PlanAlternative& alternative : alternatives) {
    alternative.rule_name = "aggregation";
    cascades::AttachPlanExplainInfo(alternative, required,
                                    RuleContext::Empty());
    const PlanExplainInfo& info = alternative.plan->ExplainInfo();
    EXPECT_EQ(info.rule_name, "aggregation");
    EXPECT_NE(info.required_properties.find("k ASC"), std::string::npos)
        << info.required_properties;
    EXPECT_NE(info.provided_properties.find("k ASC"), std::string::npos)
        << info.provided_properties;
    // The delivered order is what the shape promises, and only the two ordered
    // shapes promise it: a consumer that needs the group-key order gets it from
    // them for free, while the hash table makes the search pay for a sort.
    const bool ordered =
        alternative.plan->IsOrderedBy({key}, ascending, nulls_first);
    EXPECT_EQ(ordered, info.physical_operator != "HashAggregate")
        << info.physical_operator;
  }
}

// ------------------------------------------------------- implementation rule
// --

TEST_F(AggregateShapeTest, TheRuleOffersBothGroupedShapesAndTheKeylessStream) {
  const std::vector<PlanAlternative> alternatives =
      ApplyAggregationRule(Leaf(), {K()});
  // A grouped node offers the hash table and the sort runs; the streaming shape
  // needs an ordered input, and this leaf is unordered.
  EXPECT_EQ(ShapeNames(alternatives),
            (std::vector<std::string>{"HashAggregate", "SortAggregate"}));
  const auto hash =
      std::dynamic_pointer_cast<HashAggregatePlan>(alternatives[0].plan);
  ASSERT_NE(hash, nullptr);
  EXPECT_EQ(hash->GroupKeys().size(), 1U);
  EXPECT_EQ(hash->Aggregates().size(), 2U);
  const auto sort =
      std::dynamic_pointer_cast<SortAggregatePlan>(alternatives[1].plan);
  ASSERT_NE(sort, nullptr);
  EXPECT_EQ(sort->GroupKeys().size(), 1U);
  // The sort runs materialize an order, so they cost more than the hash table
  // unless the estimate says the table has to spill.
  EXPECT_GT(alternatives[1].local_cost, alternatives[0].local_cost);
  // Both report the estimated group count the parent costs against.
  EXPECT_EQ(alternatives[0].estimated_rows, alternatives[1].estimated_rows);
  EXPECT_EQ(alternatives[0].plan->EmitRowCount(),
            alternatives[0].plan->GetStats().Rows());
}

TEST_F(AggregateShapeTest, TheStreamShapeNeedsAnOrderedInput) {
  // The same node over a child that already delivers the group-key order may
  // use the streaming shape, which is a single pass with no table and no sort.
  const std::vector<PlanAlternative> ordered =
      ApplyAggregationRule(OrderedLeaf(), {K()});
  EXPECT_EQ(ShapeNames(ordered),
            (std::vector<std::string>{"HashAggregate", "SortAggregate",
                                      "StreamAggregate"}));
  const auto stream =
      std::dynamic_pointer_cast<StreamAggregatePlan>(ordered[2].plan);
  ASSERT_NE(stream, nullptr);
  EXPECT_TRUE(stream->RequiresOrderedInput());
  EXPECT_EQ(stream->GroupKeys().size(), 1U);
  // A keyless payload is one group, so the streaming shape is valid over any
  // input order and is always offered.
  const std::vector<PlanAlternative> scalar = ApplyAggregationRule(Leaf(), {});
  EXPECT_EQ(ShapeNames(scalar),
            (std::vector<std::string>{"HashAggregate", "SortAggregate",
                                      "StreamAggregate"}));
  const auto scalar_stream =
      std::dynamic_pointer_cast<StreamAggregatePlan>(scalar[2].plan);
  ASSERT_NE(scalar_stream, nullptr);
  EXPECT_FALSE(scalar_stream->RequiresOrderedInput());
  EXPECT_EQ(scalar[0].plan->EmitRowCount(), 1U);
}

TEST_F(AggregateShapeTest, AGroupedNodeWithAHavingCarriesTheFilter) {
  const std::vector<PlanAlternative> alternatives =
      ApplyAggregationRule(Leaf(), {K()}, 100.0, /*having=*/true);
  ASSERT_EQ(alternatives.size(), 2U) << ShapeList(alternatives);
  EXPECT_EQ(ShapeNames(alternatives),
            (std::vector<std::string>{"HashAggregate", "SortAggregate"}));
  for (const PlanAlternative& alternative : alternatives) {
    const auto plan =
        std::dynamic_pointer_cast<AggregationPlan>(alternative.plan);
    ASSERT_NE(plan, nullptr);
    EXPECT_TRUE(plan->Having() != nullptr) << alternative.plan->ToString();
    EXPECT_FALSE(plan->ContainsParameterSlots());
    // The HAVING is a predicate over the group's output row, so it must be
    // evaluated against the schema the shape produces (keys then aggregates).
    EXPECT_EQ(plan->GetSchema().GetColumn(0).Name().name, "k");
    EXPECT_EQ(plan->GetSchema()
                  .GetColumn(plan->GetSchema().ColumnCount() - 1)
                  .Name()
                  .name,
              "total");
  }
  // The rule still declines the shapes the engine cannot express: a
  // row-position requirement cannot survive an aggregation.
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"leaf"});
  PhysicalProperties positioned;
  positioned.require_row_position = true;
  const std::vector<BestPlan> children{
      BestPlan{.plan = Leaf(), .estimated_rows = 100.0}};
  EXPECT_TRUE(RuleByName("aggregation")
                  .Apply(memo, root, AggregationLogical(root, {K()}, false),
                         children, positioned, RuleContext::Empty())
                  .empty());
  EXPECT_TRUE(DefaultImplementationRules().Contains("aggregation"));
}

TEST_F(AggregateShapeTest, TheGroupedEstimateIsTheDistinctKeyCount) {
  // A grouped node's row count is the number of groups, not the number of input
  // rows: the parent operators above it cost against the group count. A real
  // measured table is the honest way to check the estimate, so this builds one.
  TransactionContext setup = database_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      database_->CreateTable(
          setup, Schema("wide", {Column("k", ValueType::kInt64),
                                 Column("name", ValueType::kVarChar),
                                 Column("v", ValueType::kDouble)})));
  constexpr size_t kRows = 1000;
  constexpr size_t kDistinct = 7;
  for (size_t i = 0; i < kRows; ++i) {
    ASSERT_SUCCESS(
        table
            .Insert(setup.txn_, Row({Value(static_cast<int64_t>(i % kDistinct)),
                                     Value("row"), Value(1.0)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(setup.txn_.PreCommit());
  {
    TransactionContext analyze = database_->BeginContext();
    SqlEngine engine(*database_);
    std::vector<Row> ignored;
    StatusOr<Executor> prepared = engine.Prepare(analyze, "ANALYZE");
    ASSERT_TRUE(prepared.HasValue());
    Row analyzed;
    ASSERT_TRUE(prepared.Value()->Next(&analyzed, nullptr));
    ASSERT_EQ(prepared.Value()->GetStatus(), Status::kSuccess);
    ASSERT_SUCCESS(analyze.PreCommit());
  }
  TransactionContext ctx = database_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                        ctx.GetStats("wide"));
  ASSERT_EQ(stats->Rows(), kRows);
  const Plan wide = std::make_shared<FullScanPlan>(table, *stats);
  const std::vector<PlanAlternative> alternatives =
      ApplyAggregationRule(wide, {ColumnValueExp("wide.k")}, kRows);
  ASSERT_EQ(alternatives.size(), 2U) << ShapeList(alternatives);
  // kDistinct distinct keys over kRows rows: the grouped output is one row per
  // distinct key, not one row per input row.
  EXPECT_EQ(alternatives[0].plan->EmitRowCount(), kDistinct)
      << alternatives[0].plan->ToString();
  EXPECT_EQ(alternatives[0].estimated_rows, static_cast<double>(kDistinct));
  EXPECT_EQ(alternatives[1].plan->EmitRowCount(), kDistinct);
  EXPECT_EQ(ctx.GetStats("wide").Value()->Rows(), kRows);
}

TEST_F(AggregateShapeTest, EveryAlternativeIsExactlyOneStrategy) {
  // The rule offers the shapes as separate alternatives, never as one plan that
  // is two shapes at once: each alternative's dynamic type, its Strategy() and
  // the name the memo reports have to agree, or a consumer (EXPLAIN, the cost
  // model, the factory) could not tell which algorithm it is looking at. The
  // three plan types are final, so a cross cast is a compile error rather than
  // a silent second algorithm.
  for (const Plan& child : {Leaf(), OrderedLeaf()}) {
    const std::vector<PlanAlternative> alternatives =
        ApplyAggregationRule(child, {K()});
    ASSERT_GE(alternatives.size(), 2U) << ShapeList(alternatives);
    std::vector<std::string> names;
    for (const PlanAlternative& alternative : alternatives) {
      const auto plan =
          std::dynamic_pointer_cast<AggregationPlan>(alternative.plan);
      ASSERT_NE(plan, nullptr) << alternative.plan->ToString();
      const std::string name = plan->StrategyName();
      names.push_back(name);
      EXPECT_EQ(plan->ToString().substr(0, plan->ToString().find(':')), name);
      EXPECT_EQ(alternative.plan->ToString().find(name), 0U)
          << alternative.plan->ToString();
      if (name == "HashAggregate") {
        EXPECT_NE(
            std::dynamic_pointer_cast<HashAggregatePlan>(alternative.plan),
            nullptr);
        EXPECT_EQ(plan->Strategy(), AggregationStrategy::kHash);
        EXPECT_EQ(
            std::dynamic_pointer_cast<SortAggregatePlan>(alternative.plan),
            nullptr);
        EXPECT_EQ(
            std::dynamic_pointer_cast<StreamAggregatePlan>(alternative.plan),
            nullptr);
      } else if (name == "SortAggregate") {
        EXPECT_NE(
            std::dynamic_pointer_cast<SortAggregatePlan>(alternative.plan),
            nullptr);
        EXPECT_EQ(plan->Strategy(), AggregationStrategy::kSort);
        EXPECT_EQ(
            std::dynamic_pointer_cast<HashAggregatePlan>(alternative.plan),
            nullptr);
        EXPECT_EQ(
            std::dynamic_pointer_cast<StreamAggregatePlan>(alternative.plan),
            nullptr);
      } else if (name == "StreamAggregate") {
        EXPECT_NE(
            std::dynamic_pointer_cast<StreamAggregatePlan>(alternative.plan),
            nullptr);
        EXPECT_EQ(plan->Strategy(), AggregationStrategy::kStream);
        EXPECT_EQ(
            std::dynamic_pointer_cast<HashAggregatePlan>(alternative.plan),
            nullptr);
        EXPECT_EQ(
            std::dynamic_pointer_cast<SortAggregatePlan>(alternative.plan),
            nullptr);
      } else {
        FAIL() << "unexpected aggregation shape " << name;
      }
      // Both grouped shapes are always on the table: the choice between them is
      // a cost decision over the same payload, never an availability one.
      EXPECT_EQ(plan->GroupKeys().size(), 1U) << name;
      EXPECT_EQ(plan->Aggregates().size(), 2U) << name;
      EXPECT_EQ(alternative.estimated_rows, plan->EmitRowCount()) << name;
    }
    EXPECT_EQ(names[0], "HashAggregate") << ShapeList(alternatives);
    EXPECT_EQ(names[1], "SortAggregate") << ShapeList(alternatives);
    // No duplicates: the same shape offered twice is a cost tie that would let
    // the search pick an arbitrary one.
    std::vector<std::string> unique(names);
    std::sort(unique.begin(), unique.end());
    EXPECT_EQ(std::adjacent_find(unique.begin(), unique.end()), unique.end())
        << ShapeList(alternatives);
  }
}

TEST_F(AggregateShapeTest,
       TheStrategyChoiceFollowsTheGroupTableMemoryContract) {
  // The two grouped shapes are priced by their own memory contract, so which
  // one the search prefers follows the group table's fit: while the estimated
  // groups stay inside the budget's group capacity the hash table is the
  // cheaper single pass, and once they outgrow it the partitioning it would
  // have to do costs more than the sort runs -- which is the whole reason the
  // sort shape exists.
  const size_t kCapacity = AggregateGroupCapacity();
  const std::vector<PlanAlternative> narrow =
      ApplyAggregationRule(WideLeaf(1000), {K()}, 1000.0);
  ASSERT_EQ(narrow.size(), 2U) << ShapeList(narrow);
  const std::vector<PlanAlternative> wide =
      ApplyAggregationRule(WideLeaf(20000000), {K()}, 20000000.0);
  ASSERT_EQ(wide.size(), 2U) << ShapeList(wide);

  // 1000 estimated groups fit the capacity, so the hash table pays nothing for
  // the spill it does not do.
  EXPECT_LT(narrow[0].plan->EmitRowCount(), kCapacity);
  EXPECT_DOUBLE_EQ(narrow[0].local_cost, 1000.0)
      << "a group table inside the budget costs one pass over the input";
  EXPECT_GT(narrow[1].local_cost, narrow[0].local_cost);
  // 20M estimated groups do not, so the hash table's own spill is priced in and
  // the sort runs win: the decision is the memory contract, not a preference.
  EXPECT_GT(wide[0].plan->EmitRowCount(), kCapacity);
  EXPECT_GT(wide[0].local_cost, 20000000.0)
      << "an oversized group table must not be priced as one pass";
  EXPECT_LT(wide[1].local_cost, wide[0].local_cost)
      << "the sort runs must become the cheaper shape once the hash table has "
         "to partition";
  // Whichever shape wins, both deliver the same estimated output.
  EXPECT_EQ(wide[0].estimated_rows, wide[1].estimated_rows);
  EXPECT_EQ(wide[0].plan->EmitRowCount(), wide[1].plan->EmitRowCount());
  EXPECT_EQ(wide[0].plan->GetSchema().ColumnCount(),
            wide[1].plan->GetSchema().ColumnCount());
  // The priced spill is the optimizer's own memory model over the policy's
  // units, so the rule and the operator cannot disagree about when the table
  // outgrows the budget.
  cascades::MemoryBudget budget;
  budget.max_memory_bytes =
      static_cast<double>(kAggregateDefaultMemoryBudgetBytes);
  budget.row_size_bytes = static_cast<double>(kAggregateEstimatedRowBytes);
  EXPECT_DOUBLE_EQ(cascades::EstimateMemorySpillCost(
                       cascades::OperatorCostKind::kSort, 1000.0, budget),
                   0.0);
  EXPECT_GT(cascades::EstimateMemorySpillCost(cascades::OperatorCostKind::kSort,
                                              20000000.0, budget),
            0.0);
  EXPECT_EQ(AggregateGroupCapacity(),
            kAggregateDefaultMemoryBudgetBytes / kAggregateEstimatedRowBytes);
}

// --------------------------------------------------- factory wiring (V4)
// -------

TEST_F(AggregateShapeTest, EachShapeEmitsItsOwnOperator) {
  std::vector<Row> rows;
  rows.reserve(32);
  for (int64_t i = 0; i < 32; ++i) {
    rows.emplace_back(
        Row({Value(i % 4), Value("row"), Value(static_cast<double>(i))}));
  }
  TransactionContext ctx = database_->BeginContext();
  AggregateMemoryPolicy memory;
  memory.memory_budget_bytes = 8 * kAggregateEstimatedRowBytes;
  memory.spill_partition_count = 4;
  memory.merge_window_rows = 4;

  AggregatePayload budgeted = GroupedPayload();
  budgeted.memory = memory;
  const Executor hash =
      std::make_shared<HashAggregatePlan>(LeafWith(rows), budgeted)
          ->EmitExecutor(ctx);
  const Executor sort =
      std::make_shared<SortAggregatePlan>(LeafWith(rows), budgeted)
          ->EmitExecutor(ctx);
  const Executor stream =
      std::make_shared<StreamAggregatePlan>(LeafWith(rows), budgeted)
          ->EmitExecutor(ctx);

  const auto* hash_operator =
      dynamic_cast<const HashAggregateExecutor*>(hash.get());
  ASSERT_NE(hash_operator, nullptr) << "the hash shape must not emit the sort "
                                       "or stream operator";
  const auto* sort_operator =
      dynamic_cast<const SortAggregateExecutor*>(sort.get());
  ASSERT_NE(sort_operator, nullptr);
  const auto* stream_operator =
      dynamic_cast<const StreamAggregateExecutor*>(stream.get());
  ASSERT_NE(stream_operator, nullptr);
  // The memory contract reaches the operator unchanged.
  EXPECT_EQ(hash_operator->Options().memory_budget_bytes,
            memory.memory_budget_bytes);
  EXPECT_EQ(hash_operator->Options().spill_partition_count,
            memory.spill_partition_count);
  EXPECT_EQ(hash_operator->GroupCapacity(), 8U);
  EXPECT_EQ(sort_operator->Options().merge_window_rows,
            memory.merge_window_rows);
  EXPECT_EQ(sort_operator->SpillThresholdRows(), 8U);
  // The streaming shape's own contract is set by its factory wiring.
  EXPECT_TRUE(stream_operator->Options().require_ordered_input);
  EXPECT_FALSE(hash_operator->Options().require_ordered_input);
  std::ostringstream dump;
  hash_operator->Dump(dump, 0);
  EXPECT_NE(dump.str().find("HashAggregate"), std::string::npos) << dump.str();
  EXPECT_NE(dump.str().find("spill=in-memory"), std::string::npos)
      << dump.str();
  sort_operator->Dump(dump, 0);
  EXPECT_NE(dump.str().find("SortAggregate"), std::string::npos) << dump.str();
  EXPECT_NE(dump.str().find("run_rows=8"), std::string::npos) << dump.str();
  stream_operator->Dump(dump, 0);
  EXPECT_NE(dump.str().find("StreamAggregate"), std::string::npos)
      << dump.str();
}

TEST_F(AggregateShapeTest, EachShapeEmitsItsOwnStrategySettings) {
  // The factory is where the three strategies stop being logical information,
  // so each setting must reach exactly the operator that owns it: the worker
  // count is the hash table's, the run threshold is the sort runs', and the
  // input-order check is the streaming shape's. A knob handed to the wrong
  // operator would either be ignored (a silently unparallel hash) or change an
  // answer.
  constexpr size_t kManyRows = kParallelAggregationMinRows;
  class ManyRowLeafPlan final : public PlanBase {
   public:
    Executor EmitExecutor(TransactionContext& /*ctx*/) const override {
      std::vector<Row> rows;
      rows.reserve(kParallelAggregationMinRows);
      for (int64_t i = 0; std::cmp_less(i, kParallelAggregationMinRows); ++i) {
        rows.emplace_back(Row({Value(i), Value("row"), Value(1.0)}));
      }
      return std::make_shared<ConstantExecutor>(std::move(rows));
    }
    [[nodiscard]] const Table* ScanSource() const override { return nullptr; }
    [[nodiscard]] const TableStatistics& GetStats() const override {
      return Stats();
    }
    [[nodiscard]] const Schema& GetSchema() const override {
      return LeafSchema();
    }
    [[nodiscard]] size_t AccessRowCount() const override { return kManyRows; }
    [[nodiscard]] size_t EmitRowCount() const override { return kManyRows; }
    void Dump(std::ostream& out, int indent) const override {
      out << Indent(static_cast<size_t>(indent)) << "ManyRowLeaf\n";
    }
    [[nodiscard]] std::string ToString() const override {
      return "ManyRowLeaf";
    }
  };
  TransactionContext ctx = database_->BeginContext();
  AggregateMemoryPolicy memory;
  memory.memory_budget_bytes = 8 * kAggregateEstimatedRowBytes;
  memory.spill_partition_count = 4;
  memory.merge_window_rows = 4;
  AggregatePayload budgeted = GroupedPayload();
  budgeted.memory = memory;

  // The operator shared_ptr is kept alive: the assertions below interrogate the
  // shape's own state, not a freed one.
  const Executor hash_executor =
      std::make_shared<HashAggregatePlan>(std::make_shared<ManyRowLeafPlan>(),
                                          budgeted)
          ->EmitExecutor(ctx);
  const auto* hash_operator =
      dynamic_cast<const HashAggregateExecutor*>(hash_executor.get());
  ASSERT_NE(hash_operator, nullptr);
  // The hash shape may fan out: above the parallel threshold the factory asks
  // for workers, and the operator keeps the serial build only if the payload
  // cannot be read positionally.
  EXPECT_TRUE(hash_operator->ParallelCapable());
  EXPECT_GE(hash_operator->WorkerCount(), 2U);
  EXPECT_EQ(hash_operator->Options().worker_count,
            hash_operator->WorkerCount());
  EXPECT_EQ(hash_operator->Options().spill_partition_count,
            memory.spill_partition_count);
  EXPECT_EQ(hash_operator->GroupCapacity(), 8U);
  // The sort runs' knob is not the hash shape's: it never writes a run.
  EXPECT_EQ(hash_operator->Options().spill_threshold_rows, 0U);
  EXPECT_FALSE(hash_operator->Options().require_ordered_input);
  std::ostringstream hash_dump;
  hash_operator->Dump(hash_dump, 0);
  EXPECT_NE(hash_dump.str().find("workers="), std::string::npos)
      << hash_dump.str();

  const Executor sort_executor =
      std::make_shared<SortAggregatePlan>(std::make_shared<ManyRowLeafPlan>(),
                                          budgeted)
          ->EmitExecutor(ctx);
  const auto* sort_operator =
      dynamic_cast<const SortAggregateExecutor*>(sort_executor.get());
  ASSERT_NE(sort_operator, nullptr);
  // The sort shape's own memory contract: a run at the plan's row capacity,
  // merged through the plan's window. It has no worker phase and no partition
  // spill, so neither knob is handed to it as a decision.
  EXPECT_EQ(sort_operator->SpillThresholdRows(), 8U);
  EXPECT_EQ(sort_operator->Options().spill_threshold_rows, 8U);
  EXPECT_EQ(sort_operator->Options().merge_window_rows,
            memory.merge_window_rows);
  EXPECT_EQ(sort_operator->Options().worker_count, 1U)
      << "the sorted runs fold in one pass";
  EXPECT_FALSE(sort_operator->Options().require_ordered_input);
  std::ostringstream sort_dump;
  sort_operator->Dump(sort_dump, 0);
  EXPECT_EQ(sort_dump.str().find("workers"), std::string::npos)
      << sort_dump.str();
  EXPECT_NE(sort_dump.str().find("run_rows=8"), std::string::npos)
      << sort_dump.str();

  const Executor stream_executor =
      std::make_shared<StreamAggregatePlan>(std::make_shared<ManyRowLeafPlan>(),
                                            budgeted)
          ->EmitExecutor(ctx);
  const auto* stream_operator =
      dynamic_cast<const StreamAggregateExecutor*>(stream_executor.get());
  ASSERT_NE(stream_operator, nullptr);
  // The streaming shape keeps its own contract -- it verifies the input order
  // instead of assuming it -- and nothing else: no workers, and the run
  // threshold stays 0 because it has neither runs nor a budget to spill.
  EXPECT_TRUE(stream_operator->Options().require_ordered_input);
  EXPECT_EQ(stream_operator->Options().worker_count, 1U);
  EXPECT_EQ(stream_operator->Options().spill_threshold_rows, 0U);

  // The same payload through each shape runs the two grouped algorithms for
  // real: over the same rows the hash table holds every group and the sort runs
  // hold one, which is the operator half of the separation the plan half
  // advertises. The budget here is the default one, so the hash table is
  // resident and neither shape is pushed onto its spill path -- what is being
  // compared is the state, not the spill.
  std::vector<Row> rows;
  rows.reserve(8);
  for (int64_t i = 0; i < 8; ++i) {
    rows.emplace_back(Row({Value(i % 4), Value("row"), Value(1.0)}));
  }
  AggregatePayload keyed = budgeted;
  keyed.memory = AggregateMemoryPolicy{};
  const Executor hash =
      std::make_shared<HashAggregatePlan>(LeafWith(rows), keyed)
          ->EmitExecutor(ctx);
  const Executor sort =
      std::make_shared<SortAggregatePlan>(LeafWith(rows), keyed)
          ->EmitExecutor(ctx);
  Row row;
  while (hash->Next(&row, nullptr)) {
  }
  EXPECT_EQ(hash->GetStatus(), Status::kSuccess);
  const auto* hash_run = dynamic_cast<const HashAggregateExecutor*>(hash.get());
  ASSERT_NE(hash_run, nullptr);
  EXPECT_EQ(hash_run->GroupCount(), 4U);
  EXPECT_EQ(hash_run->PeakResidentGroups(), 4U)
      << "the hash shape's state is the group table";
  while (sort->Next(&row, nullptr)) {
  }
  EXPECT_EQ(sort->GetStatus(), Status::kSuccess);
  const auto* sort_run = dynamic_cast<const SortAggregateExecutor*>(sort.get());
  ASSERT_NE(sort_run, nullptr);
  EXPECT_EQ(sort_run->GroupCount(), 4U);
  EXPECT_EQ(sort_run->PeakResidentGroups(), 1U)
      << "the sort runs' state is the run being folded";
}

TEST_F(AggregateShapeTest, AKeylessShapeKeepsTheHistoricalScalarOperators) {
  // No GROUP BY keys: every shape routes to the historical scalar operators,
  // so the single-relation fast path and the existing plan tests are
  // unaffected by the shape split.
  TransactionContext ctx = database_->BeginContext();
  std::vector<Row> rows;
  rows.reserve(8);
  for (int64_t i = 0; i < 8; ++i) {
    rows.emplace_back(Row({Value(i), Value("row"), Value(1.0)}));
  }
  const auto payload = AggregatePayload{.aggregates = {CountStar(), SumV()}};
  for (const Plan& plan :
       {std::static_pointer_cast<PlanBase>(
            std::make_shared<HashAggregatePlan>(LeafWith(rows), payload)),
        std::static_pointer_cast<PlanBase>(
            std::make_shared<SortAggregatePlan>(LeafWith(rows), payload)),
        std::static_pointer_cast<PlanBase>(
            std::make_shared<StreamAggregatePlan>(LeafWith(rows), payload))}) {
    const Executor executor = plan->EmitExecutor(ctx);
    const auto* serial =
        dynamic_cast<const AggregationExecutor*>(executor.get());
    ASSERT_NE(serial, nullptr) << plan->ToString();
    Row row;
    ASSERT_TRUE(executor->Next(&row, nullptr)) << plan->ToString();
    EXPECT_EQ(row[0], Value(int64_t{8})) << plan->ToString();
  }
  // Above the parallel-aggregation threshold the historical promotion to the
  // parallel scalar operator still happens, on every shape.
  class ManyRowLeafPlan final : public PlanBase {
   public:
    Executor EmitExecutor(TransactionContext& /*ctx*/) const override {
      std::vector<Row> rows;
      rows.reserve(kParallelAggregationMinRows);
      for (int64_t i = 0; std::cmp_less(i, kParallelAggregationMinRows); ++i) {
        rows.emplace_back(Row({Value(i), Value("row"), Value(1.0)}));
      }
      return std::make_shared<ConstantExecutor>(std::move(rows));
    }
    [[nodiscard]] const Table* ScanSource() const override { return nullptr; }
    [[nodiscard]] const TableStatistics& GetStats() const override {
      return Stats();
    }
    [[nodiscard]] const Schema& GetSchema() const override {
      return LeafSchema();
    }
    [[nodiscard]] size_t AccessRowCount() const override {
      return kParallelAggregationMinRows;
    }
    [[nodiscard]] size_t EmitRowCount() const override {
      return kParallelAggregationMinRows;
    }
    void Dump(std::ostream& out, int indent) const override {
      out << Indent(static_cast<size_t>(indent)) << "ManyRowLeaf\n";
    }
    [[nodiscard]] std::string ToString() const override {
      return "ManyRowLeaf";
    }
  };
  for (const Plan& plan :
       {std::static_pointer_cast<PlanBase>(std::make_shared<HashAggregatePlan>(
            std::make_shared<ManyRowLeafPlan>(), payload)),
        std::static_pointer_cast<PlanBase>(std::make_shared<SortAggregatePlan>(
            std::make_shared<ManyRowLeafPlan>(), payload)),
        std::static_pointer_cast<PlanBase>(
            std::make_shared<StreamAggregatePlan>(
                std::make_shared<ManyRowLeafPlan>(), payload))}) {
    const Executor executor = plan->EmitExecutor(ctx);
    EXPECT_NE(dynamic_cast<const ParallelAggregationExecutor*>(executor.get()),
              nullptr)
        << plan->ToString();
  }
}

}  // namespace tinylamb
