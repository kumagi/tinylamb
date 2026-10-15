#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "executor/full_scan.hpp"
#include "executor/projection.hpp"
#include "executor/selection.hpp"
#include "executor/sort.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "gtest/gtest.h"
#include "plan/cascades.hpp"
#include "plan/optimizer.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/query_data.hpp"
#include "query/sql_engine.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

using LogicalExpression = cascades::LogicalExpression;
using LogicalOperator = cascades::LogicalOperator;

enum class Semantic : uint8_t {
  kSuccess,
  kNull,
  kEmpty,
  kDuplicate,
  kBoundary,
  kError,
};

constexpr std::array<Semantic, 6> kSemantics = {
    Semantic::kSuccess,   Semantic::kNull,     Semantic::kEmpty,
    Semantic::kDuplicate, Semantic::kBoundary, Semantic::kError};

std::string_view SemanticSql(Semantic semantic) {
  switch (semantic) {
    case Semantic::kSuccess:
      return "SELECT id, grp FROM phase0_contract WHERE id = 2";
    case Semantic::kNull:
      return "SELECT id, grp FROM phase0_contract WHERE grp IS NULL";
    case Semantic::kEmpty:
      return "SELECT id, grp FROM phase0_contract WHERE id = 999";
    case Semantic::kDuplicate:
      return "SELECT id, grp FROM phase0_contract WHERE id IN (1, 2) "
             "ORDER BY id";
    case Semantic::kBoundary:
      return "SELECT id, grp FROM phase0_contract WHERE id >= 2 AND id <= 5 "
             "ORDER BY id";
    case Semantic::kError:
      return "SELECT missing_column FROM phase0_contract";
  }
  return {};
}

std::vector<Row> ExpectedRows(Semantic semantic) {
  switch (semantic) {
    case Semantic::kSuccess:
      return {Row({Value(int64_t{2}), Value(int64_t{10})})};
    case Semantic::kNull:
      return {Row({Value(int64_t{3}), Value()})};
    case Semantic::kEmpty:
      return {};
    case Semantic::kDuplicate:
      return {Row({Value(int64_t{1}), Value(int64_t{10})}),
              Row({Value(int64_t{2}), Value(int64_t{10})})};
    case Semantic::kBoundary:
      return {Row({Value(int64_t{2}), Value(int64_t{10})}),
              Row({Value(int64_t{3}), Value()}),
              Row({Value(int64_t{4}), Value(int64_t{20})}),
              Row({Value(int64_t{5}), Value(int64_t{20})})};
    case Semantic::kError:
      return {};
  }
  return {};
}

std::string SchemaFingerprint(const Schema& schema) {
  std::string out;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    if (i > 0) {
      out.push_back('|');
    }
    const Column& column = schema.GetColumn(i);
    out += column.Name().ToString();
    out.push_back(':');
    out += std::to_string(static_cast<int>(column.Type()));
    out.push_back(':');
    out += std::to_string(static_cast<int>(column.GetConstraint().ctype));
  }
  return out;
}

std::vector<Row> EvaluateSet(const Schema& schema, const Expression& predicate,
                             const std::vector<Row>& input) {
  std::vector<Row> result;
  for (const Row& row : input) {
    if (predicate == nullptr) {
      result.push_back(row);
      continue;
    }
    const StatusOr<Value> value = predicate->TryEvaluate(row, schema);
    if (value.HasValue() && value.Value().Truthy()) {
      result.push_back(row);
    }
  }
  return result;
}

struct RuleSnapshot {
  std::string schema;
  std::string ordering;
  std::vector<Row> rows;
  size_t estimated_rows{0};
};

RuleSnapshot CaptureRuleSnapshot(const cascades::Memo& memo,
                                 cascades::GroupId root, const Schema& schema,
                                 const Expression& predicate,
                                 const TableStatistics& statistics,
                                 const std::vector<Row>& input) {
  const cascades::LogicalExpression& expression =
      memo.Get(root).expressions.front();
  cascades::PhysicalProperties required;
  required.ordering = {ColumnName("phase0_contract", "id")};
  required.sort_ascending = {true};
  required.sort_nulls_first = {true};
  const std::vector<cascades::PhysicalProperties> child_properties =
      cascades::SearchEngine::RequiredChildProperties(expression, required);
  std::string ordering = "none";
  if (!child_properties.empty()) {
    ordering = child_properties.front().ordering.empty()
                   ? "none"
                   : child_properties.front().ordering.front().ToString();
  }
  return RuleSnapshot{
      .schema = expression.output_schema.ColumnCount() == 0
                    ? SchemaFingerprint(schema)
                    : SchemaFingerprint(expression.output_schema),
      .ordering = ordering,
      .rows = EvaluateSet(schema, predicate, input),
      .estimated_rows = statistics.Filter(schema, predicate).Rows(),
  };
}

class Phase0ContractTest : public ::testing::Test {
 protected:
  void SetUp() override {
    database_ =
        Database::Create("phase0_contract-" + RandomString()).MoveValue();
    TransactionContext setup = database_->BeginContext();
    const Schema schema(
        "phase0_contract",
        {Column("id", ValueType::kInt64), Column("grp", ValueType::kInt64),
         Column("label", ValueType::kVarChar)});
    ASSIGN_OR_ASSERT_FAIL(Table, table, database_->CreateTable(setup, schema));
    for (const Row& row :
         {Row({Value(int64_t{1}), Value(int64_t{10}), Value("x")}),
          Row({Value(int64_t{2}), Value(int64_t{10}), Value("y")}),
          Row({Value(int64_t{3}), Value(), Value("n")}),
          Row({Value(int64_t{4}), Value(int64_t{20}), Value("x")}),
          Row({Value(int64_t{5}), Value(int64_t{20}), Value()}),
          Row({Value(int64_t{6}), Value(int64_t{30}), Value("z")})}) {
      ASSERT_SUCCESS(table.Insert(setup.txn_, row).GetStatus());
    }
    ASSERT_SUCCESS(setup.txn_.PreCommit());

    TransactionContext statistics_context = database_->BeginContext();
    ASSERT_SUCCESS(
        database_->RefreshStatistics(statistics_context, "phase0_contract"));
    ASSERT_SUCCESS(statistics_context.txn_.PreCommit());

    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table_ptr,
                          context_->GetTable("phase0_contract"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats_ptr,
                          context_->GetStats("phase0_contract"));
    table_ = std::move(table_ptr);
    statistics_ = std::move(stats_ptr);
  }

  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  static std::vector<Row> Collect(
      Executor executor) {  // NOLINT(performance-unnecessary-value-param)
    std::vector<Row> rows;
    Row row;
    while (executor != nullptr && executor->Next(&row, nullptr)) {
      rows.push_back(row);
      row = Row();
    }
    if (executor != nullptr) {
      EXPECT_EQ(executor->GetStatus(), Status::kSuccess)
          << executor->GetStatus().GetMessage();
    }
    return rows;
  }

  std::vector<Row> RunSql(std::string_view sql) {
    StatusOr<Executor> prepared = engine_->Prepare(*context_, sql);
    if (!prepared.HasValue()) {
      ADD_FAILURE() << engine_->LastError();
      return {};
    }
    return Collect(std::move(prepared.Value()));
  }

  std::vector<Row> RunCascades(QueryData query) {
    const Status rewrite = query.Rewrite(*context_);
    if (rewrite != Status::kSuccess) {
      ADD_FAILURE() << rewrite.GetMessage();
      return {};
    }
    StatusOr<Plan> plan = Optimizer::Optimize(query, *context_);
    if (!plan.HasValue()) {
      ADD_FAILURE() << plan.GetStatus().GetMessage();
      return {};
    }
    return Collect(plan.Value()->EmitExecutor(*context_));
  }
  std::vector<Row> RunDirect(const Expression& predicate) {
    Executor scan = std::make_shared<FullScan>(context_->txn_, *table_);
    Executor filtered = std::make_shared<Selection>(
        predicate, table_->GetSchema(), std::move(scan));
    Executor projected = std::make_shared<Projection>(
        std::vector<NamedExpression>{
            NamedExpression("id", ColumnValueExp("id")),
            NamedExpression("grp", ColumnValueExp("grp")),
            NamedExpression("label", ColumnValueExp("label"))},
        table_->GetSchema(), std::move(filtered));
    Executor sorted = std::make_shared<SortExecutor>(
        std::move(projected), table_->GetSchema(),
        std::vector<SortExecutor::Key>{
            {ColumnValueExp("id"), true, std::nullopt}});
    return Collect(std::move(sorted));
  }

  std::string Explain(std::string_view sql) {
    std::string text;
    for (const Row& row : RunSql(std::string("EXPLAIN ") + std::string(sql))) {
      text += row[0].AsString();
      text.push_back('\n');
    }
    return text;
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
  std::shared_ptr<Table> table_;
  std::shared_ptr<TableStatistics> statistics_;
};

}  // namespace

TEST_F(Phase0ContractTest, SqlCascadesAndDirectExecutorShareExpectedRows) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP();
  }
  const Expression predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("phase0_contract", "id")),
      BinaryOperation::kGreaterThanEquals, ConstantValueExp(Value(int64_t{2})));
  const Expression range_predicate = BinaryExpressionExp(
      predicate, BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("phase0_contract", "id")),
                          BinaryOperation::kLessThanEquals,
                          ConstantValueExp(Value(int64_t{5}))));
  QueryData query({"phase0_contract"}, range_predicate,
                  {NamedExpression("id", ColumnValueExp("id")),
                   NamedExpression("grp", ColumnValueExp("grp")),
                   NamedExpression("label", ColumnValueExp("label"))});
  const Status rewrite = query.Rewrite(*context_);
  ASSERT_EQ(rewrite, Status::kSuccess) << rewrite.GetMessage();
  query.order_expressions_ = {ColumnValueExp("id")};
  query.order_ascending_ = {true};
  query.order_nulls_first_ = {true};
  StatusOr<Plan> cascades_plan = Optimizer::Optimize(query, *context_);
  ASSERT_TRUE(cascades_plan.HasValue())
      << cascades_plan.GetStatus().GetMessage();
  const std::vector<Row> expected = {
      Row({Value(int64_t{2}), Value(int64_t{10}), Value("y")}),
      Row({Value(int64_t{3}), Value(), Value("n")}),
      Row({Value(int64_t{4}), Value(int64_t{20}), Value("x")}),
      Row({Value(int64_t{5}), Value(int64_t{20}), Value()})};

  const std::vector<Row> sql_rows = RunSql(
      "SELECT id, grp, label FROM phase0_contract WHERE id >= 2 "
      "AND id <= 5 ORDER BY id");
  const std::vector<Row> cascades_rows =
      Collect(cascades_plan.Value()->EmitExecutor(*context_));
  const std::vector<Row> direct_rows = RunDirect(BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp(ColumnName("phase0_contract", "id")),
                          BinaryOperation::kGreaterThanEquals,
                          ConstantValueExp(Value(int64_t{2}))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("phase0_contract", "id")),
                          BinaryOperation::kLessThanEquals,
                          ConstantValueExp(Value(int64_t{5})))));

  EXPECT_EQ(sql_rows, expected);
  EXPECT_EQ(cascades_rows, expected);
  EXPECT_EQ(direct_rows, expected);
}

TEST_F(Phase0ContractTest, ManifestCoversEveryTodoAndSemanticClass) {
  struct Entry {
    size_t todo;
    Semantic semantic;
  };
  std::vector<Entry> manifest;
  for (size_t todo = 55; todo <= 59; ++todo) {
    for (const Semantic semantic : kSemantics) {
      manifest.push_back(Entry{.todo = todo, .semantic = semantic});
    }
  }
  ASSERT_EQ(manifest.size(), 30U);
  for (size_t todo = 55; todo <= 59; ++todo) {
    for (const Semantic semantic : kSemantics) {
      const auto found =
          std::ranges::find_if(manifest, [todo, semantic](const Entry& entry) {
            return entry.todo == todo && entry.semantic == semantic;
          });
      ASSERT_NE(found, manifest.end());
    }
  }

  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP();
  }
  std::array<std::vector<Row>, 6> results;
  std::array<bool, 6> ran{};
  for (const Entry& entry : manifest) {
    const auto index = static_cast<size_t>(entry.semantic);
    if (!ran[index]) {
      if (entry.semantic == Semantic::kError) {
        const StatusOr<Executor> prepared =
            engine_->Prepare(*context_, SemanticSql(entry.semantic));
        EXPECT_FALSE(prepared.HasValue());
        EXPECT_NE(prepared.GetStatus(), Status::kSuccess);
      } else {
        results[index] = RunSql(SemanticSql(entry.semantic));
      }
      ran[index] = true;
    }
    if (entry.semantic != Semantic::kError) {
      EXPECT_EQ(results[index], ExpectedRows(entry.semantic))
          << "todo=" << entry.todo;
    }
  }
}

TEST_F(Phase0ContractTest, RuleRewritePreservesSchemaOrderSetAndEstimate) {
  const Schema schema(
      "phase0_contract",
      {Column("id", ValueType::kInt64), Column("grp", ValueType::kInt64),
       Column("label", ValueType::kVarChar)});
  const Expression predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("phase0_contract", "id")),
      BinaryOperation::kGreaterThanEquals, ConstantValueExp(Value(int64_t{2})));
  cascades::Memo memo;
  memo.SetTableSchemas({{"phase0_contract", schema}});
  const cascades::GroupId base = memo.Build({"phase0_contract"});
  const cascades::GroupId root =
      memo.EnsureDerivedGroup({"phase0_contract"}, "phase0_selection");
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{.operation = LogicalOperator::kSelection,
                              .children = {base},
                              .predicate = predicate,
                              .output_schema = schema}));

  const std::vector<Row> input = {
      Row({Value(int64_t{1}), Value(int64_t{10}), Value("x")}),
      Row({Value(int64_t{2}), Value(int64_t{10}), Value("y")}),
      Row({Value(int64_t{3}), Value(), Value("n")}),
      Row({Value(int64_t{4}), Value(int64_t{20}), Value("x")})};
  const RuleSnapshot before =
      CaptureRuleSnapshot(memo, root, schema, predicate, *statistics_, input);

  cascades::SearchEngine search(std::move(memo), cascades::RuleSet::Default());
  search.Explore(root);
  const cascades::Memo& explored = search.GetMemo();
  const RuleSnapshot after = CaptureRuleSnapshot(
      explored, root, schema, predicate, *statistics_, input);

  EXPECT_TRUE(search.AppliedRuleNames().contains("push_selection_into_scan"));
  EXPECT_EQ(after.schema, before.schema);
  EXPECT_EQ(after.ordering, before.ordering);
  EXPECT_EQ(after.rows, before.rows);
  EXPECT_EQ(after.estimated_rows, before.estimated_rows);
  EXPECT_FALSE(explored.Get(base).filter);
  ASSERT_TRUE(explored.Get(root).filter);
  EXPECT_EQ(after.estimated_rows,
            statistics_->Filter(schema, explored.Get(root).filter).Rows());
  EXPECT_EQ(explored.Get(root).filter->ToString(), predicate->ToString());
}

TEST_F(Phase0ContractTest, ExplainShowsRuleOperatorAndPropertyContract) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP();
  }
  const std::string join_plan = Explain(
      "SELECT a.id, b.grp FROM phase0_contract a JOIN phase0_contract b "
      "ON a.id = b.id WHERE a.id >= 1 ORDER BY a.id");
  EXPECT_NE(join_plan.find("Rule: "), std::string::npos) << join_plan;
  EXPECT_NE(join_plan.find("Physical operator: "), std::string::npos)
      << join_plan;
  EXPECT_NE(join_plan.find("Required properties: "), std::string::npos)
      << join_plan;
  EXPECT_NE(join_plan.find("Provided properties: "), std::string::npos)
      << join_plan;
  EXPECT_NE(join_plan.find("Applied rules: "), std::string::npos) << join_plan;
  EXPECT_NE(join_plan.find("Rule: sort"), std::string::npos) << join_plan;

  const std::string single_plan = Explain("SELECT id FROM phase0_contract");
  EXPECT_NE(single_plan.find("Rule: projection"), std::string::npos)
      << single_plan;
  EXPECT_NE(single_plan.find("Physical operator: "), std::string::npos)
      << single_plan;
  EXPECT_NE(single_plan.find("Required properties: "), std::string::npos)
      << single_plan;
  EXPECT_NE(single_plan.find("Provided properties: "), std::string::npos)
      << single_plan;

  const std::string no_from_plan = Explain("SELECT 1 AS one");
  EXPECT_NE(no_from_plan.find("Rule: relational_ir"), std::string::npos)
      << no_from_plan;
  EXPECT_NE(no_from_plan.find("Physical operator: "), std::string::npos)
      << no_from_plan;
}

TEST(CascadesNonConvergenceTest, FingerprintsRejectDuplicateRuleExpansion) {
  cascades::Memo memo;
  const cascades::GroupId base = memo.Build({"t"});
  const cascades::GroupId root = memo.EnsureDerivedGroup({"t"}, "root");
  const Expression predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("t", "id")), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(int64_t{0})));
  const LogicalExpression expression{.operation = LogicalOperator::kSelection,
                                     .children = {base},
                                     .predicate = predicate};
  ASSERT_TRUE(memo.AddExpression(root, expression));
  EXPECT_FALSE(memo.AddExpression(root, expression));
  EXPECT_FALSE(memo.Degraded());

  LogicalExpression limited{.operation = LogicalOperator::kLimit,
                            .children = {base},
                            .limit_count = 1};
  LogicalExpression different_limit = limited;
  different_limit.limit_count = 2;
  EXPECT_NE(limited.Fingerprint(), different_limit.Fingerprint());

  cascades::RuleSet rules;
  rules.Add(cascades::Rule(
      "reinsert_same_fingerprint",
      cascades::dsl::Selection(cascades::dsl::Any()),
      [](const cascades::Bindings&, cascades::Memo& target_memo,
         cascades::GroupId group, const LogicalExpression& current) {
        target_memo.AddExpression(group, current);
      },
      LogicalOperator::kSelection));
  cascades::SearchEngine search(std::move(memo), rules);
  search.Explore(root);
  EXPECT_EQ(search.GetMemo().ExpressionCount(root), 1U);
  EXPECT_FALSE(search.BudgetExhausted());
}

TEST(CascadesNonConvergenceTest, SearchStepBudgetStopsUniqueExpansion) {
  cascades::Memo memo(64);
  const cascades::GroupId base = memo.Build({"t"});
  const cascades::GroupId root = memo.EnsureDerivedGroup({"t"}, "growth_root");
  const Expression predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("t", "id")), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(int64_t{0})));
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{.operation = LogicalOperator::kSelection,
                              .children = {base},
                              .predicate = predicate}));

  cascades::RuleSet rules;
  rules.Add(cascades::Rule(
      "unique_growth", cascades::dsl::Selection(cascades::dsl::Any()),
      [base, predicate](const cascades::Bindings&, cascades::Memo& target_memo,
                        cascades::GroupId group,
                        const LogicalExpression& current) {
        if (target_memo.Get(group).tag != "growth_root" ||
            current.children.empty()) {
          return;
        }
        const cascades::GroupId child = target_memo.EnsureDerivedGroup(
            {"t"}, "growth_child_" + std::to_string(target_memo.GroupCount()));
        target_memo.AddExpression(
            child, LogicalExpression{.operation = LogicalOperator::kSelection,
                                     .children = {base},
                                     .predicate = predicate});
        target_memo.AddExpression(
            group, LogicalExpression{.operation = LogicalOperator::kSelection,
                                     .children = {child},
                                     .predicate = predicate});
      },
      LogicalOperator::kSelection));
  cascades::SearchEngine search(std::move(memo), rules);
  search.SetStepBudget(4);
  search.Explore(root);
  EXPECT_TRUE(search.BudgetExhausted());
  EXPECT_LE(search.GetMemo().GroupCount(), 12U);
}

}  // namespace tinylamb
