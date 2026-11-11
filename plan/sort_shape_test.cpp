/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
// The buffered / external-merge split of the sort implementation rule
// (plan/sort_plan.hpp): the two shapes are one logical ORDER BY, so what the
// plan layer may differ on is the memory contract (budget, row capacity, merge
// window) and the name EXPLAIN shows. The rule half of the split is pinned
// here too: at or below the row capacity only the buffered rule offers an
// alternative, above it only the external-merge rule does, and an already
// ordered child is elided once (by the buffered rule).
//
// The operator half (executor/sort_shape_executor_test.cpp) drives what each
// shape does with the memory it was handed.
#include <gtest/gtest.h>

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
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/constant_executor.hpp"
#include "executor/executor_base.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/join_kind.hpp"
#include "executor/query_memory.hpp"
#include "executor/sort.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "plan/cascades.hpp"
#include "plan/distinct_plan.hpp"
#include "plan/implementation_rules.hpp"
#include "plan/incremental_sort_plan.hpp"
#include "plan/limit_plan.hpp"
#include "plan/plan.hpp"
#include "plan/product_plan.hpp"
#include "plan/relation_rename_plan.hpp"
#include "plan/sample_plan.hpp"
#include "plan/set_operation_plan.hpp"
#include "plan/sort_distinct_plan.hpp"
#include "plan/sort_plan.hpp"
#include "plan/window_plan.hpp"
#include "table/table_statistics.hpp"
#include "type/collation.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

using cascades::BestPlan;
using cascades::LogicalExpression;
using cascades::LogicalOperator;
using cascades::PhysicalProperties;
using cascades::PlanAlternative;
using cascades::RuleContext;

const Schema& LeafSchema() {
  static const Schema schema("", {Column("k", ValueType::kInt64),
                                  Column("name", ValueType::kVarChar)});
  return schema;
}

const TableStatistics& Stats() {
  static const TableStatistics stats{LeafSchema()};
  return stats;
}

const Expression& NameKey() {
  static const Expression key = ColumnValueExp("name");
  return key;
}

const Expression& KKey() {
  static const Expression key = ColumnValueExp("k");
  return key;
}

// make_shared cannot deduce a braced list, so the key list goes through here.
std::vector<SortKey> KeysOf(std::initializer_list<SortKey> keys) {
  return {keys};
}

SortKey BinaryKey(const Expression& expression) {
  return SortKey{.expression = expression,
                 .ascending = true,
                 .nulls_first = std::nullopt,
                 .collation = Collation::kNone};
}

Expression CiName() {
  return std::make_shared<FunctionCallExpression>(
      "collate",
      std::vector<Expression>{NameKey(), ConstantValueExp(Value("und:ci"))});
}

SortKey CiKey() {
  return SortKey{.expression = CiName(),
                 .ascending = true,
                 .nulls_first = std::nullopt,
                 .collation = Collation::kCaseInsensitive};
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

const cascades::ImplementationRule& RuleByName(const std::string& name) {
  for (const auto& rule : DefaultImplementationRules().Rules()) {
    if (rule.Name() == name) {
      return rule;
    }
  }
  throw std::runtime_error("no implementation rule named " + name);
}

// The sort node as the rules see it: one key on the child's `k` column.
LogicalExpression SortLogical(cascades::GroupId child, const Expression& key) {
  LogicalExpression logical;
  logical.operation = LogicalOperator::kSort;
  logical.children = {child};
  logical.target_list = {NamedExpression("", key)};
  logical.sort_ascending = {true};
  logical.sort_nulls_first = {std::nullopt};
  return logical;
}

std::vector<PlanAlternative> ApplySortRule(const std::string& name, Plan child,
                                           double estimated_rows) {
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"leaf"});
  const LogicalExpression logical = SortLogical(root, ColumnValueExp("k"));
  const std::vector<BestPlan> children{
      BestPlan{.plan = std::move(child), .estimated_rows = estimated_rows}};
  return RuleByName(name).Apply(memo, root, logical, children,
                                PhysicalProperties{}, RuleContext::Empty());
}

std::vector<Row> Drain(
    Executor executor) {  // NOLINT(performance-unnecessary-value-param)
  std::vector<Row> rows;
  Row row;
  while (executor->Next(&row, nullptr)) {
    rows.push_back(row);
    row = Row();
  }
  return rows;
}

class SortShapeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    database_ =
        Database::Create("sort_shape_test-" + RandomString()).MoveValue();
  }
  void TearDown() override {
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
  }
  std::unique_ptr<Database> database_;
};

}  // namespace

// ------------------------------------------------------- logical contract ---

TEST_F(SortShapeTest, SpillingSemiAntiJoinCannotElideParentSort) {
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t saved = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(saved); }
  } restore;
  TransactionContext context = database_->BeginContext();
  for (const JoinKind kind :
       {JoinKind::kSemi, JoinKind::kAnti, JoinKind::kNullAwareAnti}) {
    for (const HashJoinMode mode :
         {HashJoinMode::kInMemory, HashJoinMode::kHybrid}) {
      for (const bool reduction : {false, true}) {
        SCOPED_TRACE(::testing::Message()
                     << static_cast<int>(kind) << "," << static_cast<int>(mode)
                     << "," << reduction);
        QueryMemoryBudget::Global().ResetForTest(1024);
        std::vector<Row> probe;
        std::vector<Row> build;
        for (int64_t key = 0; key < 128; ++key) {
          probe.emplace_back(Row({Value(key), Value(std::string(64, 'x'))}));
          if (key % 2 == 0) {
            build.emplace_back(Row({Value(key), Value("build")}));
          }
        }
        const Plan sorted = std::make_shared<BufferedSortPlan>(
            LeafWith(std::move(probe)), KeysOf({BinaryKey(KKey())}));
        const auto join = std::make_shared<ProductPlan>(
            sorted, std::vector<ColumnName>{ColumnName("k")},
            LeafWith(std::move(build)),
            std::vector<ColumnName>{ColumnName("k")}, mode, kind);
        if (reduction) {
          join->PreferSemiReduction();
        }
        const auto alternatives = ApplySortRule("sort", join, 64);
        ASSERT_EQ(alternatives.size(), 1U);
        const Plan limited =
            std::make_shared<LimitPlan>(alternatives[0].plan, 4, 1);
        const Executor executor = limited->EmitExecutor(context);
        const std::vector<Row> output = Drain(executor);
        ASSERT_EQ(executor->GetStatus(), Status::kSuccess);
        ASSERT_EQ(output.size(), 4U);
        for (size_t i = 0; i < output.size(); ++i) {
          EXPECT_EQ(output[i][0], Value(static_cast<int64_t>(2 * (i + 1)) +
                                        (kind == JoinKind::kSemi ? 0 : 1)));
        }
        EXPECT_FALSE(join->IsOrderedBy({KKey()}, {true}));
      }
    }
  }
}

TEST_F(SortShapeTest, OffsetOnlyLimitDoesNotProveDistinctness) {
  const Plan limited = std::make_shared<LimitPlan>(
      LeafWith({Row({Value(1), Value("x")}), Row({Value(1), Value("x")}),
                Row({Value(1), Value("x")})}),
      0, 1);
  EXPECT_EQ(limited->EmitRowCount(), 2U);
  EXPECT_FALSE(limited->EnforcesDistinct());
  // Mirror the SQL facade's DISTINCT safety net: an unbounded limit must
  // not convince its consumer to omit duplicate elimination.
  Plan output = limited;
  if (!output->EnforcesDistinct()) {
    output = std::make_shared<DistinctPlan>(output);
  }
  TransactionContext context = database_->BeginContext();
  const Executor executor = output->EmitExecutor(context);
  EXPECT_EQ(Drain(executor), (std::vector<Row>{Row({Value(1), Value("x")})}));
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
  EXPECT_TRUE(std::make_shared<LimitPlan>(limited, 1, 0)->EnforcesDistinct());
}

TEST_F(SortShapeTest, AuditLimitFoldPreservesExplicitSortCollation) {
  const SortKey folded{.expression = NameKey(),
                       .ascending = true,
                       .nulls_first = std::nullopt,
                       .collation = Collation::kCaseInsensitive};
  const Plan sorted = std::make_shared<BufferedSortPlan>(
      LeafWith({Row({Value(1), Value("a")}), Row({Value(2), Value("B")})}),
      KeysOf({folded}));
  cascades::Memo memo;
  const auto root = memo.Build({"leaf"});
  LogicalExpression limit;
  limit.operation = LogicalOperator::kLimit;
  limit.children = {root};
  limit.limit_count = 1;
  const auto alternatives = RuleByName("limit").Apply(
      memo, root, limit, {BestPlan{.plan = sorted, .estimated_rows = 2}},
      PhysicalProperties{}, RuleContext::Empty());
  ASSERT_EQ(alternatives.size(), 1U);
  EXPECT_EQ(alternatives[0].plan->OrderCollation(),
            Collation::kCaseInsensitive);
  TransactionContext context = database_->BeginContext();
  const Executor executor = alternatives[0].plan->EmitExecutor(context);
  EXPECT_EQ(Drain(executor), (std::vector<Row>{Row({Value(1), Value("a")})}));
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
}

TEST_F(SortShapeTest, OrderedWrappersKeepTheChildCollation) {
  const SortKey folded{.expression = NameKey(),
                       .ascending = true,
                       .nulls_first = std::nullopt,
                       .collation = Collation::kCaseInsensitive};
  for (int wrapper = 0; wrapper < 6; ++wrapper) {
    SCOPED_TRACE(wrapper);
    const Plan sorted = std::make_shared<BufferedSortPlan>(
        LeafWith({Row({Value(1), Value("a")}), Row({Value(2), Value("B")})}),
        KeysOf({folded}));
    Plan input;
    switch (wrapper) {
      case 0:
        input = std::make_shared<DistinctPlan>(sorted);
        break;
      case 1:
        input = std::make_shared<SortDistinctPlan>(sorted);
        break;
      case 2:
        input = std::make_shared<RelationRenamePlan>(sorted, "alias", "");
        break;
      case 3:
        input = std::make_shared<WindowPlan>(sorted,
                                             std::vector<NamedExpression>{});
        break;
      case 4:
        input = std::make_shared<SamplePlan>(
            sorted, TableSampleSpec{.size = ConstantValueExp(Value(100)),
                                    .repeatable_seed = std::nullopt,
                                    .partition_by = {}});
        break;
      default:
        input = std::make_shared<SetOperationPlan>(std::vector<Plan>{sorted},
                                                   SetOperationKind::kUnionAll,
                                                   KeysOf({folded}));
        break;
    }
    EXPECT_EQ(input->OrderCollation(), Collation::kCaseInsensitive);
    cascades::Memo memo;
    const auto root = memo.Build({"leaf"});
    const auto alternatives =
        RuleByName("sort").Apply(memo, root, SortLogical(root, NameKey()),
                                 {BestPlan{.plan = input, .estimated_rows = 2}},
                                 PhysicalProperties{}, RuleContext::Empty());
    ASSERT_EQ(alternatives.size(), 1U);
    auto context = database_->BeginContext();
    const Executor executor = alternatives[0].plan->EmitExecutor(context);
    const auto rows = Drain(executor);
    ASSERT_EQ(executor->GetStatus(), Status::kSuccess);
    ASSERT_EQ(rows.size(), 2U);
    EXPECT_EQ(rows[0][1], Value("B"));
    EXPECT_EQ(rows[1][1], Value("a"));
  }
}

TEST_F(SortShapeTest, IncrementalSortKeepsItsSuffixCollation) {
  const auto input = std::make_shared<IncrementalSortPlan>(
      LeafWith({Row({Value(1), Value("a")}), Row({Value(1), Value("B")})}),
      KeysOf({BinaryKey(KKey())}),
      KeysOf({SortKey{.expression = NameKey(),
                      .ascending = true,
                      .nulls_first = std::nullopt,
                      .collation = Collation::kCaseInsensitive}}));
  EXPECT_EQ(input->OrderCollation(), Collation::kCaseInsensitive);
  cascades::Memo memo;
  const auto root = memo.Build({"leaf"});
  auto logical = SortLogical(root, KKey());
  logical.target_list.emplace_back("name", NameKey());
  logical.sort_ascending.push_back(true);
  const auto alternatives = RuleByName("sort").Apply(
      memo, root, logical, {BestPlan{.plan = input, .estimated_rows = 2}},
      PhysicalProperties{}, RuleContext::Empty());
  ASSERT_EQ(alternatives.size(), 1U);
  auto context = database_->BeginContext();
  const auto executor = alternatives[0].plan->EmitExecutor(context);
  EXPECT_EQ(Drain(executor), (std::vector<Row>{Row({Value(1), Value("B")}),
                                               Row({Value(1), Value("a")})}));
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
}

TEST_F(SortShapeTest, SortedDistinctRequiresTheSameCollationAsItsInput) {
  const Plan input = std::make_shared<BufferedSortPlan>(
      LeafWith({Row({Value(1), Value("a")}), Row({Value(1), Value("A")}),
                Row({Value(1), Value("a")})}),
      KeysOf({BinaryKey(KKey()),
              SortKey{.expression = NameKey(),
                      .ascending = true,
                      .nulls_first = std::nullopt,
                      .collation = Collation::kCaseInsensitive}}));
  cascades::Memo memo;
  const auto root = memo.Build({"leaf"});
  const LogicalExpression logical{.operation = LogicalOperator::kDistinct,
                                  .children = {root}};
  const auto alternatives =
      RuleByName("sort_distinct")
          .Apply(memo, root, logical,
                 {BestPlan{.plan = input, .estimated_rows = 3}},
                 PhysicalProperties{}, RuleContext::Empty());
  ASSERT_EQ(alternatives.size(), 1U);
  auto context = database_->BeginContext();
  const auto executor = alternatives[0].plan->EmitExecutor(context);
  const auto rows = Drain(executor);
  ASSERT_EQ(executor->GetStatus(), Status::kSuccess);
  EXPECT_EQ(rows, (std::vector<Row>{Row({Value(1), Value("A")}),
                                    Row({Value(1), Value("a")})}));
}

TEST_F(SortShapeTest, BothShapesCarryTheSameOrderContract) {
  const std::shared_ptr<SortPlan> buffered =
      std::make_shared<BufferedSortPlan>(Leaf(), KeysOf({BinaryKey(KKey())}));
  const std::shared_ptr<SortPlan> spilling =
      std::make_shared<ExternalMergeSortPlan>(Leaf(),
                                              KeysOf({BinaryKey(KKey())}));
  const std::vector<std::optional<bool>> defaulted{std::nullopt};

  for (const std::shared_ptr<SortPlan>& plan : {buffered, spilling}) {
    EXPECT_TRUE(plan->IsOrderedBy({KKey()}, {true}));
    EXPECT_TRUE(plan->IsOrderedBy({KKey()}, {true}, defaulted));
    EXPECT_FALSE(plan->IsOrderedBy({KKey()}, {false}));
    EXPECT_FALSE(plan->IsOrderedBy({NameKey()}, {true}));
    EXPECT_FALSE(plan->IsOrderedBy({KKey(), NameKey()}, {true, true}));
    // NULL placement is part of the ORDER BY contract, not of the shape.
    EXPECT_TRUE(
        plan->IsOrderedBy({KKey()}, {true}, {std::optional<bool>(true)}));
    EXPECT_FALSE(
        plan->IsOrderedBy({KKey()}, {true}, {std::optional<bool>(false)}));
    // Reordering never adds or removes rows.
    EXPECT_EQ(plan->EmitRowCount(), plan->Child()->EmitRowCount());
    EXPECT_EQ(plan->AccessRowCount(), plan->Child()->AccessRowCount());
    EXPECT_EQ(&plan->GetSchema(), &plan->Child()->GetSchema());
    EXPECT_EQ(plan->ScanSource(), plan->Child()->ScanSource());
    EXPECT_FALSE(plan->EnforcesDistinct());
  }
}

TEST_F(SortShapeTest, BothShapesReportTheCollationTheyProduced) {
  const Plan binary = std::make_shared<BufferedSortPlan>(
      Leaf(), KeysOf({BinaryKey(NameKey())}));
  const Plan spilled_binary = std::make_shared<ExternalMergeSortPlan>(
      Leaf(), KeysOf({BinaryKey(NameKey())}));
  const Plan ci = std::make_shared<BufferedSortPlan>(Leaf(), KeysOf({CiKey()}));
  const Plan spilled_ci =
      std::make_shared<ExternalMergeSortPlan>(Leaf(), KeysOf({CiKey()}));

  EXPECT_EQ(binary->OrderCollation(), Collation::kNone);
  EXPECT_EQ(spilled_binary->OrderCollation(), Collation::kNone);
  EXPECT_EQ(ci->OrderCollation(), Collation::kCaseInsensitive);
  EXPECT_EQ(spilled_ci->OrderCollation(), Collation::kCaseInsensitive);

  // Under 'und:ci' 'a' sorts before 'B', so a byte order may not stand in for
  // a folded one (nor the other way round) whichever shape produced it.
  PhysicalProperties binary_requirement;
  binary_requirement.ordering = {ColumnName("name")};
  binary_requirement.collation = "";
  PhysicalProperties ci_requirement;
  ci_requirement.ordering = {ColumnName("name")};
  ci_requirement.collation = "und:ci";
  for (const Plan& shape : {ci, spilled_ci}) {
    EXPECT_TRUE(cascades::SatisfiesOrderingCollation(shape, ci_requirement));
    EXPECT_FALSE(
        cascades::SatisfiesOrderingCollation(shape, binary_requirement));
  }
  for (const Plan& shape : {binary, spilled_binary}) {
    EXPECT_TRUE(
        cascades::SatisfiesOrderingCollation(shape, binary_requirement));
    EXPECT_FALSE(cascades::SatisfiesOrderingCollation(shape, ci_requirement));
  }
}

TEST_F(SortShapeTest, BothShapesForwardParameterSlots) {
  const std::vector<SortKey> keys = KeysOf(
      {BinaryKey(NameKey()),
       BinaryKey(std::make_shared<ParameterSlot>(0, Value(int64_t{1})))});
  const Plan buffered = std::make_shared<BufferedSortPlan>(Leaf(), keys);
  const Plan spilling = std::make_shared<ExternalMergeSortPlan>(Leaf(), keys);

  for (const Plan& plan : {buffered, spilling}) {
    EXPECT_TRUE(plan->ContainsParameterSlots());
    std::vector<Expression> slots;
    size_t cursor = 0;
    EXPECT_TRUE(
        plan->InstallParameterSlots({Value(int64_t{7})}, &cursor, &slots));
    // The key's literal was collected, so the replay can rebind it.
    EXPECT_EQ(slots.size(), 1U);
    EXPECT_EQ(cursor, 0U);
    EXPECT_NE(std::dynamic_pointer_cast<ParameterSlot>(slots.front()), nullptr);
  }
}

// ------------------------------------------------------------- memory info ---

TEST_F(SortShapeTest, ThePolicyDefinesTheRowCapacityOfEachShape) {
  SortMemoryPolicy memory;
  memory.memory_budget_bytes = 64 * kSortEstimatedRowBytes;  // exactly 64 rows
  memory.merge_window_rows = 8;

  const BufferedSortPlan buffered(Leaf(), KeysOf({BinaryKey(KKey())}), memory);
  const ExternalMergeSortPlan spilling(Leaf(), KeysOf({BinaryKey(KKey())}),
                                       memory);

  EXPECT_EQ(SortRowCapacity(memory), 64U);
  EXPECT_EQ(buffered.Memory().memory_budget_bytes,
            64U * kSortEstimatedRowBytes);
  EXPECT_EQ(buffered.RowCapacity(), 64U);
  EXPECT_EQ(spilling.RowCapacity(), 64U);
  EXPECT_EQ(spilling.SpillThresholdRows(), 64U);
  EXPECT_EQ(spilling.MergeWindowRows(), 8U);
  // The default policy is the optimizer's MemoryBudget expressed in rows.
  const SortMemoryPolicy defaults;
  EXPECT_EQ(SortRowCapacity(),
            defaults.memory_budget_bytes / kSortEstimatedRowBytes);
  EXPECT_EQ(defaults.memory_budget_bytes, kSortDefaultMemoryBudgetBytes);
  EXPECT_EQ(defaults.merge_window_rows, kSortDefaultMergeWindowRows);
  EXPECT_EQ(buffered.Strategy(), SortStrategy::kBuffered);
  // A zero budget is the unbounded policy: it must not divide by zero, and it
  // must not push every input onto the spill shape either.
  SortMemoryPolicy unbounded;
  unbounded.memory_budget_bytes = 0;
  EXPECT_EQ(SortRowCapacity(unbounded), std::numeric_limits<size_t>::max());
  EXPECT_NE(std::dynamic_pointer_cast<BufferedSortPlan>(MakeSortPlan(
                Leaf(), KeysOf({BinaryKey(KKey())}), 1e12, unbounded)),
            nullptr);
  // A budget narrower than one estimated row still admits one row.
  SortMemoryPolicy tiny;
  tiny.memory_budget_bytes = 1;
  EXPECT_EQ(SortRowCapacity(tiny), 1U);
}

TEST_F(SortShapeTest, TheBaseSortNodeStaysAPlainSortPlan) {
  // Existing callers (and dynamic_pointer_cast<SortPlan> in the limit rule)
  // keep working: both shapes are SortPlans, and the base type still renders as
  // the historical "Sort" and emits the historical operator configuration (no
  // shape bound, the executor's own spill selection only).
  const SortPlan legacy(Leaf(), KeysOf({BinaryKey(KKey())}));
  EXPECT_EQ(legacy.StrategyName(), "Sort");
  EXPECT_EQ(legacy.ToString(), "Sort: [k ASC]");
  TransactionContext ctx = database_->BeginContext();
  const Executor executor = legacy.EmitExecutor(ctx);
  const auto* sort = dynamic_cast<const SortExecutor*>(executor.get());
  ASSERT_NE(sort, nullptr);
  EXPECT_EQ(sort->ActiveSpillMode(), SortExecutor::SpillMode::kAuto);
  EXPECT_EQ(sort->SortOptions().memory_budget_bytes, 0U);
  EXPECT_EQ(sort->MergeWindowRows(), SortExecutor::kDefaultMergeWindowRows);
  EXPECT_NE(
      std::dynamic_pointer_cast<SortPlan>(std::make_shared<BufferedSortPlan>(
          Leaf(), KeysOf({BinaryKey(KKey())}))),
      nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<SortPlan>(
                std::make_shared<ExternalMergeSortPlan>(
                    Leaf(), KeysOf({BinaryKey(KKey())}))),
            nullptr);
}

// ---------------------------------------------------------------- EXPLAIN --

TEST_F(SortShapeTest, ExplainNamesTheShapeAndItsMemoryContract) {
  SortMemoryPolicy memory;
  memory.memory_budget_bytes = 320;  // 5 rows of the estimated width
  memory.merge_window_rows = 16;
  const Plan buffered = std::make_shared<BufferedSortPlan>(
      Leaf(), KeysOf({BinaryKey(KKey())}), memory);
  const Plan spilling = std::make_shared<ExternalMergeSortPlan>(
      Leaf(), KeysOf({BinaryKey(KKey())}), memory);

  EXPECT_EQ(buffered->ToString(),
            "BufferedSort: [k ASC] (buffered, budget=320B, rows=5)");
  EXPECT_EQ(spilling->ToString(),
            "ExternalMergeSort: [k ASC] (spill runs, budget=320B, rows=5, "
            "window=16)");
  std::ostringstream dump;
  spilling->Dump(dump, 0);
  EXPECT_NE(dump.str().find("ExternalMergeSort: [k ASC]"), std::string::npos)
      << dump.str();
  EXPECT_NE(dump.str().find("Leaf"), std::string::npos) << dump.str();
  // The name before the first colon is what the memo reports as the physical
  // operator, so neither rendering may hide its own colon.
  for (const std::shared_ptr<SortPlan>& plan :
       {std::dynamic_pointer_cast<SortPlan>(buffered),
        std::dynamic_pointer_cast<SortPlan>(spilling)}) {
    // The rendered name is what the memo reports, so both are read as a
    // SortPlan and compared as one.
    const std::string text = plan->ToString();
    EXPECT_NE(text.find(':'), std::string::npos);
    EXPECT_EQ(text.substr(0, text.find(':')), plan->StrategyName());
  }
}

TEST_F(SortShapeTest, ExplainInfoCarriesTheRuleAndTheDeliveredOrdering) {
  PhysicalProperties required;
  required.ordering = {ColumnName("k")};
  required.sort_ascending = {true};

  std::vector<PlanAlternative> alternatives =
      ApplySortRule("sort", Leaf(), 10.0);
  ASSERT_EQ(alternatives.size(), 1U);
  // The search engine stamps the rule it applied on the alternative before it
  // builds the explain info; applying a rule directly skips that step.
  alternatives.front().rule_name = "sort";
  EXPECT_NE(
      std::dynamic_pointer_cast<BufferedSortPlan>(alternatives.front().plan),
      nullptr);
  cascades::AttachPlanExplainInfo(alternatives.front(), required,
                                  RuleContext::Empty());
  const PlanExplainInfo& buffered = alternatives.front().plan->ExplainInfo();
  EXPECT_EQ(buffered.rule_name, "sort");
  EXPECT_EQ(buffered.physical_operator, "BufferedSort");
  EXPECT_NE(buffered.provided_properties.find("k ASC"), std::string::npos)
      << buffered.provided_properties;
  EXPECT_NE(buffered.required_properties.find("k ASC"), std::string::npos)
      << buffered.required_properties;

  // The same contract for the spill shape, under its own rule and name: the
  // ordering a consumer may rely on does not change with the shape.
  const double rows = static_cast<double>(SortRowCapacity()) + 1.0;
  std::vector<PlanAlternative> spilled =
      ApplySortRule("sort_external_merge", Leaf(), rows);
  ASSERT_EQ(spilled.size(), 1U);
  spilled.front().rule_name = "sort_external_merge";
  cascades::AttachPlanExplainInfo(spilled.front(), required,
                                  RuleContext::Empty());
  const PlanExplainInfo& merging = spilled.front().plan->ExplainInfo();
  EXPECT_EQ(merging.rule_name, "sort_external_merge");
  EXPECT_EQ(merging.physical_operator, "ExternalMergeSort");
  EXPECT_EQ(merging.provided_properties, buffered.provided_properties);
  EXPECT_EQ(merging.required_properties, buffered.required_properties);
}

// ----------------------------------------------------- selection contract ----

TEST_F(SortShapeTest, MakeSortPlanPicksTheShapeFromTheRowEstimate) {
  const size_t capacity = SortRowCapacity();
  const Plan at_capacity = MakeSortPlan(Leaf(), KeysOf({BinaryKey(KKey())}),
                                        static_cast<double>(capacity));
  EXPECT_NE(std::dynamic_pointer_cast<BufferedSortPlan>(at_capacity), nullptr)
      << at_capacity->ToString();
  const Plan above = MakeSortPlan(Leaf(), KeysOf({BinaryKey(KKey())}),
                                  static_cast<double>(capacity) + 1.0);
  EXPECT_NE(std::dynamic_pointer_cast<ExternalMergeSortPlan>(above), nullptr)
      << above->ToString();
  // A degenerate (NaN) estimate must not push the choice to the spill-heavy
  // shape by accident.
  const Plan nan_rows = MakeSortPlan(Leaf(), KeysOf({BinaryKey(KKey())}),
                                     std::numeric_limits<double>::quiet_NaN());
  EXPECT_NE(std::dynamic_pointer_cast<BufferedSortPlan>(nan_rows), nullptr)
      << nan_rows->ToString();
  // The ORDER BY contract does not depend on the shape.
  const auto at = std::dynamic_pointer_cast<SortPlan>(at_capacity);
  const auto over = std::dynamic_pointer_cast<SortPlan>(above);
  ASSERT_NE(at, nullptr);
  ASSERT_NE(over, nullptr);
  EXPECT_EQ(at->KeysToString(), over->KeysToString());
  EXPECT_EQ(at->Keys().size(), over->Keys().size());
}

TEST_F(SortShapeTest, EachRuleOwnsOneSideOfTheSplit) {
  const size_t capacity = SortRowCapacity();
  const double below = static_cast<double>(capacity) - 1.0;
  const double above = static_cast<double>(capacity) + 1.0;

  // Below the row capacity: only the buffered rule offers an alternative.
  const std::vector<PlanAlternative> buffered =
      ApplySortRule("sort", Leaf(), below);
  ASSERT_EQ(buffered.size(), 1U);
  EXPECT_NE(std::dynamic_pointer_cast<BufferedSortPlan>(buffered.front().plan),
            nullptr);
  EXPECT_TRUE(ApplySortRule("sort_external_merge", Leaf(), below).empty());
  // Above it: only the external merge rule does.
  EXPECT_TRUE(ApplySortRule("sort", Leaf(), above).empty());
  const std::vector<PlanAlternative> spilled =
      ApplySortRule("sort_external_merge", Leaf(), above);
  ASSERT_EQ(spilled.size(), 1U);
  const auto merging =
      std::dynamic_pointer_cast<ExternalMergeSortPlan>(spilled.front().plan);
  ASSERT_NE(merging, nullptr);
  EXPECT_EQ(merging->SpillThresholdRows(), capacity);
  EXPECT_EQ(merging->MergeWindowRows(), SortMemoryPolicy{}.merge_window_rows);
  EXPECT_EQ(merging->Memory().memory_budget_bytes,
            SortMemoryPolicy{}.memory_budget_bytes);
  EXPECT_EQ(spilled.front().estimated_rows, above);
  // The spill shape is priced above the in-memory one: the runs it writes and
  // re-merges are exactly what the buffered shape avoids.
  EXPECT_GT(spilled.front().local_cost, below * std::log2(below));
  // Neither rule leaves a bare SortPlan behind.
  EXPECT_EQ(std::dynamic_pointer_cast<SortPlan>(buffered.front().plan)
                ->StrategyName(),
            "BufferedSort");
  EXPECT_EQ(merging->StrategyName(), "ExternalMergeSort");
  // Both halves are registered for the sort node.
  EXPECT_TRUE(DefaultImplementationRules().Contains("sort"));
  EXPECT_TRUE(DefaultImplementationRules().Contains("sort_external_merge"));
}

TEST_F(SortShapeTest, AnOrderedChildNeedsNoSortInEitherShape) {
  // A child that already delivers the order needs no sort at all, on whichever
  // side of the memory split the estimate falls. The elision is offered once
  // (by the buffered rule) so the memo never holds the same plan twice under
  // two rule names, and the spill rule never inserts a sort the child answered.
  const Plan ordered =
      std::make_shared<BufferedSortPlan>(Leaf(), KeysOf({BinaryKey(KKey())}));
  for (const double rows : {1.0, 1e9}) {
    const std::vector<PlanAlternative> buffered =
        ApplySortRule("sort", ordered, rows);
    ASSERT_EQ(buffered.size(), 1U) << rows;
    EXPECT_EQ(buffered.front().local_cost, 0.0) << rows;
    EXPECT_EQ(buffered.front().plan, ordered) << rows;
    EXPECT_TRUE(ApplySortRule("sort_external_merge", ordered, rows).empty())
        << rows;
  }
}

TEST_F(SortShapeTest, ACollatedRequirementIsNeverElidedByABinaryChild) {
  // The elision check is shared by both shapes, so a case-insensitive
  // requirement is never satisfied by a binary-ordered child: whichever rule
  // owns the estimate has to insert a sort that folds.
  const Plan binary_child = std::make_shared<BufferedSortPlan>(
      Leaf(), KeysOf({BinaryKey(NameKey())}));
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"leaf"});
  const LogicalExpression logical = SortLogical(root, CiName());
  PhysicalProperties required;
  required.ordering = {ColumnName("name")};
  required.collation = "und:ci";
  const std::vector<BestPlan> children{
      BestPlan{.plan = binary_child, .estimated_rows = 10.0}};
  const std::vector<PlanAlternative> buffered = RuleByName("sort").Apply(
      memo, root, logical, children, required, RuleContext::Empty());
  ASSERT_EQ(buffered.size(), 1U);
  EXPECT_EQ(buffered.front().plan->OrderCollation(),
            Collation::kCaseInsensitive);
  EXPECT_NE(buffered.front().plan->ToString().find("collate"),
            std::string::npos)
      << "a byte-ordered child must not answer a folded requirement: "
      << buffered.front().plan->ToString();
  // Above the row capacity the buffered rule declines and the spill shape owns
  // the node -- with the same folded key.
  const std::vector<BestPlan> huge{
      BestPlan{.plan = binary_child, .estimated_rows = 1e9}};
  const std::vector<PlanAlternative> spilled =
      RuleByName("sort_external_merge")
          .Apply(memo, root, logical, huge, required, RuleContext::Empty());
  ASSERT_EQ(spilled.size(), 1U);
  EXPECT_NE(
      std::dynamic_pointer_cast<ExternalMergeSortPlan>(spilled.front().plan),
      nullptr);
  EXPECT_EQ(spilled.front().plan->OrderCollation(),
            Collation::kCaseInsensitive);
}

// --------------------------------------------------- factory wiring (V4) -----

TEST_F(SortShapeTest, EachShapeEmitsItsOwnOperatorConfiguration) {
  TransactionContext ctx = database_->BeginContext();
  SortMemoryPolicy memory;
  memory.memory_budget_bytes = 64 * kSortEstimatedRowBytes;  // 64-row capacity
  memory.merge_window_rows = 4;

  std::vector<Row> rows;
  rows.reserve(32);
  for (int64_t i = 0; i < 32; ++i) {
    rows.push_back(Row({Value(i), Value("row")}));
  }
  const Executor buffered =
      std::make_shared<BufferedSortPlan>(LeafWith(rows),
                                         KeysOf({BinaryKey(KKey())}), memory)
          ->EmitExecutor(ctx);
  const Executor spilling =
      std::make_shared<ExternalMergeSortPlan>(
          LeafWith(rows), KeysOf({BinaryKey(KKey())}), memory)
          ->EmitExecutor(ctx);

  const auto* sort = dynamic_cast<const SortExecutor*>(buffered.get());
  ASSERT_NE(sort, nullptr);
  const auto* merge = dynamic_cast<const SortExecutor*>(spilling.get());
  ASSERT_NE(merge, nullptr);
  // The dedicated wiring hands the plan's memory contract down and keeps the
  // two shapes apart at the operator.
  EXPECT_EQ(sort->ActiveSpillMode(), SortExecutor::SpillMode::kBuffered);
  EXPECT_EQ(sort->SortOptions().memory_budget_bytes,
            memory.memory_budget_bytes);
  EXPECT_EQ(sort->SortOptions().spill_threshold_rows, 0U);
  EXPECT_EQ(sort->MergeWindowRows(), memory.merge_window_rows);
  EXPECT_EQ(merge->ActiveSpillMode(), SortExecutor::SpillMode::kExternalMerge);
  EXPECT_EQ(merge->SortOptions().memory_budget_bytes,
            memory.memory_budget_bytes);
  EXPECT_EQ(merge->SortOptions().spill_threshold_rows, 64U);
  EXPECT_EQ(merge->MergeWindowRows(), memory.merge_window_rows);
  EXPECT_EQ(std::string(SortExecutor::SpillModeLabel(sort->ActiveSpillMode())),
            "buffered");
  EXPECT_EQ(std::string(SortExecutor::SpillModeLabel(merge->ActiveSpillMode())),
            "external-merge");
  std::ostringstream dump;
  merge->Dump(dump, 0);
  EXPECT_NE(dump.str().find("spill=external-merge"), std::string::npos)
      << dump.str();
  EXPECT_NE(dump.str().find("budget="), std::string::npos) << dump.str();
}

TEST_F(SortShapeTest, BothShapesReturnTheSameRowsInTheSameOrder) {
  // ORDER BY, NULL placement and ties must be shape independent: the split only
  // decides where the rows live while they are ordered.
  std::vector<Row> rows;
  rows.reserve(64);
  for (int64_t i = 0; i < 64; ++i) {
    // Ties (i % 4) plus NULLs (every eighth row) on an ascending key.
    rows.push_back(Row({i % 8 == 0 ? Value() : Value(i % 4),
                        Value("row" + std::to_string(i))}));
  }
  SortMemoryPolicy memory;
  memory.memory_budget_bytes = 8 * kSortEstimatedRowBytes;  // 8-row capacity
  memory.merge_window_rows = 4;

  TransactionContext ctx = database_->BeginContext();
  const std::vector<Row> from_buffered =
      Drain(std::make_shared<BufferedSortPlan>(
                LeafWith(rows), KeysOf({BinaryKey(KKey())}), memory)
                ->EmitExecutor(ctx));
  const std::vector<Row> from_spill =
      Drain(std::make_shared<ExternalMergeSortPlan>(
                LeafWith(rows), KeysOf({BinaryKey(KKey())}), memory)
                ->EmitExecutor(ctx));
  ASSERT_EQ(from_buffered.size(), rows.size());
  EXPECT_EQ(from_buffered, from_spill);

  // NULLS FIRST is the engine default for an ascending key, then a
  // non-decreasing key.
  size_t first_non_null = 0;
  while (first_non_null < from_buffered.size() &&
         from_buffered[first_non_null][0].IsNull()) {
    ++first_non_null;
  }
  EXPECT_EQ(first_non_null, 8U);
  for (size_t i = first_non_null; i < from_buffered.size(); ++i) {
    EXPECT_FALSE(from_buffered[i][0].IsNull());
    if (i > first_non_null) {
      EXPECT_LE(from_buffered[i - 1][0].value.int_value,
                from_buffered[i][0].value.int_value);
    }
  }
  // Both shapes are stable, so the peers of a key keep their input order: the
  // payload column (the input index) rises inside every tie group.
  int64_t previous_source = -1;
  for (size_t i = 0; i < from_buffered.size(); ++i) {
    if (i > 0 && !from_buffered[i][0].IsNull() &&
        !from_buffered[i - 1][0].IsNull() &&
        from_buffered[i][0].value.int_value ==
            from_buffered[i - 1][0].value.int_value) {
      const std::string payload(from_buffered[i][1].value.varchar_value);
      const int64_t source = std::stoll(payload.substr(3));
      EXPECT_GT(source, previous_source)
          << "tie group lost its input order at row " << i;
      previous_source = source;
    } else {
      previous_source = -1;
    }
  }
}

}  // namespace tinylamb
