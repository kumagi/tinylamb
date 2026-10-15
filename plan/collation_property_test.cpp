/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
//
// The collation half of the optimizer's ordering contract: which collation a
// plan produced its order under, whether that satisfies a requirement stated
// under a different one, and how the answer travels through
// PhysicalProperties (its key, its EXPLAIN text and the provided-properties
// inference a memo alternative reports).
#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "plan/cascades.hpp"
#include "plan/limit_plan.hpp"
#include "plan/plan.hpp"
#include "plan/projection_plan.hpp"
#include "plan/selection_plan.hpp"
#include "plan/sort_plan.hpp"
#include "plan/topn_plan.hpp"
#include "table/table_statistics.hpp"
#include "type/collation.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb::cascades {
namespace {

Expression Collate(Expression inner, std::string spec) {
  return std::make_shared<FunctionCallExpression>(
      "collate",
      std::vector<Expression>{std::move(inner),
                              ConstantValueExp(Value(std::move(spec)))});
}

const Expression& NameKey() {
  static const Expression key = ColumnValueExp("name");
  return key;
}

SortKey CiSortKey() {
  return SortKey{.expression = Collate(NameKey(), "und:ci"),
                 .ascending = true,
                 .nulls_first = std::nullopt,
                 .collation = Collation::kCaseInsensitive};
}

TopNKey CiTopNKey() {
  return TopNKey{.expression = Collate(NameKey(), "und:ci"),
                 .ascending = true,
                 .nulls_first = std::nullopt,
                 .collation = Collation::kCaseInsensitive};
}

SortKey BinarySortKey() {
  return SortKey{.expression = NameKey(),
                 .ascending = true,
                 .nulls_first = std::nullopt,
                 .collation = Collation::kNone};
}

// The synthetic relation every plan in this file sits on.
const Schema& LeafSchema() {
  static const Schema schema("", {Column("name", ValueType::kVarChar),
                                  Column("k", ValueType::kInt64)});
  return schema;
}

// Its statistics, shared by the leaf and the selection wrapper below
// (SelectionPlan filters them at construction).
const TableStatistics& Stats() {
  static const TableStatistics stats{LeafSchema()};
  return stats;
}

// A leaf plan: reports no order, so it stands in for the scan / index orders
// that every storage-produced ordering delivers.
class LeafPlan final : public PlanBase {
 public:
  Executor EmitExecutor(TransactionContext& /*ctx*/) const override {
    return nullptr;
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
  void Dump(std::ostream& out, int indent) const override {
    out << Indent(static_cast<size_t>(indent)) << "Leaf\n";
  }
  [[nodiscard]] std::string ToString() const override { return "Leaf"; }
};

Plan Leaf() { return std::make_shared<LeafPlan>(); }

PhysicalProperties Required(const std::string& collation) {
  PhysicalProperties required;
  required.ordering = {ColumnName("name")};
  required.sort_ascending = {true};
  required.collation = collation;
  return required;
}

}  // namespace

// ------------------------------------------------- what a plan reports -------

TEST(CollationPropertyTest, StorageAndDefaultPlansReportTheEngineDefault) {
  // Every scan / index order is a byte order, so it reports kNone (the engine
  // default) and never claims a collation it did not produce.
  EXPECT_EQ(Leaf()->OrderCollation(), Collation::kNone);
  Plan sorted =
      std::make_shared<SortPlan>(Leaf(), std::vector<SortKey>{BinarySortKey()});
  EXPECT_EQ(sorted->OrderCollation(), Collation::kNone);
  Plan topn = std::make_shared<TopNPlan>(
      Leaf(),
      std::vector<TopNKey>{TopNKey{.expression = NameKey(),
                                   .ascending = true,
                                   .nulls_first = std::nullopt,
                                   .collation = Collation::kNone}},
      3, 0);
  EXPECT_EQ(topn->OrderCollation(), Collation::kNone);
}

TEST(CollationPropertyTest, CollatedSortAndTopNReportTheirCollation) {
  Plan sorted =
      std::make_shared<SortPlan>(Leaf(), std::vector<SortKey>{CiSortKey()});
  EXPECT_EQ(sorted->OrderCollation(), Collation::kCaseInsensitive);
  Plan topn = std::make_shared<TopNPlan>(
      Leaf(), std::vector<TopNKey>{CiTopNKey()}, 3, 0);
  EXPECT_EQ(topn->OrderCollation(), Collation::kCaseInsensitive);
}

TEST(CollationPropertyTest, OneCollatedKeyMakesTheWholeOrderCollated) {
  // A sort compares its keys left to right, so the delivered ordering is
  // case-insensitive as soon as one key asks for it.
  Plan sorted = std::make_shared<SortPlan>(
      Leaf(), std::vector<SortKey>{BinarySortKey(), CiSortKey()});
  EXPECT_EQ(sorted->OrderCollation(), Collation::kCaseInsensitive);
  Plan topn = std::make_shared<TopNPlan>(
      Leaf(),
      std::vector<TopNKey>{TopNKey{.expression = NameKey(),
                                   .ascending = true,
                                   .nulls_first = std::nullopt,
                                   .collation = Collation::kNone},
                           CiTopNKey()},
      3, 0);
  EXPECT_EQ(topn->OrderCollation(), Collation::kCaseInsensitive);
}

TEST(CollationPropertyTest, RowShapingWrappersForwardTheCollation) {
  // A projection renames columns but preserves the order (and the collation
  // it was produced under), so the wrappers must forward the claim instead of
  // silently degrading it to the engine default.
  Plan sorted =
      std::make_shared<SortPlan>(Leaf(), std::vector<SortKey>{CiSortKey()});
  Plan projected = std::make_shared<ProjectionPlan>(
      sorted,
      std::vector<NamedExpression>{NamedExpression("$order0", NameKey())});
  Plan selected =
      std::make_shared<SelectionPlan>(projected, ColumnValueExp("k"), Stats());
  Plan limited = std::make_shared<LimitPlan>(selected, 10, 0);
  EXPECT_EQ(projected->OrderCollation(), Collation::kCaseInsensitive);
  EXPECT_EQ(selected->OrderCollation(), Collation::kCaseInsensitive);
  EXPECT_EQ(limited->OrderCollation(), Collation::kCaseInsensitive);
}

// ------------------------------------------------ satisfying a requirement --

TEST(CollationPropertyTest, ACollatedPlanOnlySatisfiesACollatedRequirement) {
  Plan collated =
      std::make_shared<SortPlan>(Leaf(), std::vector<SortKey>{CiSortKey()});
  Plan binary =
      std::make_shared<SortPlan>(Leaf(), std::vector<SortKey>{BinarySortKey()});
  // A case-insensitive order is NOT a binary order: under 'und:ci' 'a' sorts
  // before 'B', under binary 'B' sorts before 'a'.
  EXPECT_TRUE(SatisfiesOrderingCollation(collated, Required("und:ci")));
  EXPECT_FALSE(SatisfiesOrderingCollation(collated, Required("")));
  EXPECT_FALSE(SatisfiesOrderingCollation(collated, Required("binary")));
  // An explicit 'binary' requirement and the engine default ask for the same
  // byte order, so either plan satisfies both.
  EXPECT_TRUE(SatisfiesOrderingCollation(binary, Required("")));
  EXPECT_TRUE(SatisfiesOrderingCollation(binary, Required("binary")));
  EXPECT_FALSE(SatisfiesOrderingCollation(binary, Required("und:ci")));
  // Every non-'binary' spec resolves to the same case-insensitive collator, so
  // a plan built for one of them serves them all (there is no locale collation
  // in this engine to distinguish them).
  EXPECT_TRUE(SatisfiesOrderingCollation(collated, Required("und:cs")));
  EXPECT_TRUE(SatisfiesOrderingCollation(collated, Required("en_US")));
  // A storage order is byte order, so it satisfies exactly the default.
  EXPECT_TRUE(SatisfiesOrderingCollation(Leaf(), Required("")));
  EXPECT_FALSE(SatisfiesOrderingCollation(Leaf(), Required("und:ci")));
  EXPECT_FALSE(SatisfiesOrderingCollation(nullptr, Required("")));
}

TEST(CollationPropertyTest, ACollatedRequirementPartitionsTheMemoKey) {
  // The requirement is part of the property key, so a group searched for a
  // case-insensitive order and the same group searched for a binary order get
  // separate best-plan entries instead of sharing one answer.
  const PhysicalProperties ci = Required("und:ci");
  const PhysicalProperties binary = Required("");
  EXPECT_NE(ci.Key(), binary.Key());
  EXPECT_NE(ci, binary);
  EXPECT_EQ(binary, Required(""));
  EXPECT_EQ(ci.Key(), Required("und:ci").Key());
}

TEST(CollationPropertyTest, ProvidedPropertiesCarryTheRequestCollation) {
  Plan collated =
      std::make_shared<SortPlan>(Leaf(), std::vector<SortKey>{CiSortKey()});
  PlanAlternative alternative;
  alternative.plan = collated;
  alternative.rule_name = "sort";
  // With a collated requirement the order is provided, and the provided
  // properties echo the collation it was provided in.
  AttachPlanExplainInfo(alternative, Required("und:ci"), RuleContext::Empty());
  EXPECT_EQ(alternative.provided_properties.ordering,
            (std::vector<ColumnName>{ColumnName("name")}));
  EXPECT_EQ(alternative.provided_properties.collation, "und:ci");

  // The same plan under a binary requirement cannot serve it: the claim is
  // dropped (including the collation) instead of advertising an order the
  // consumer may not use.
  PlanAlternative mismatched;
  mismatched.plan = collated;
  mismatched.rule_name = "sort";
  AttachPlanExplainInfo(mismatched, Required(""), RuleContext::Empty());
  EXPECT_TRUE(mismatched.provided_properties.ordering.empty());
  EXPECT_TRUE(mismatched.provided_properties.collation.empty());
}

TEST(CollationPropertyTest, ExplainTextNamesTheCollationOnlyWhenSet) {
  Plan collated =
      std::make_shared<SortPlan>(Leaf(), std::vector<SortKey>{CiSortKey()});
  PlanAlternative alternative;
  alternative.plan = collated;
  alternative.rule_name = "sort";
  AttachPlanExplainInfo(alternative, Required("und:ci"), RuleContext::Empty());
  const PlanExplainInfo& info = alternative.plan->ExplainInfo();
  EXPECT_NE(info.required_properties.find("collation=und:ci"),
            std::string::npos);
  EXPECT_NE(info.provided_properties.find("collation=und:ci"),
            std::string::npos);

  PlanAlternative binary;
  binary.plan = collated;
  binary.rule_name = "sort";
  AttachPlanExplainInfo(binary, Required(""), RuleContext::Empty());
  EXPECT_EQ(binary.plan->ExplainInfo().required_properties.find("collation="),
            std::string::npos);
}

}  // namespace tinylamb::cascades
