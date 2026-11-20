/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/join_reduction.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/in_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "query/googlesql_ast.hpp"
#include "query/googlesql_ast_visitor.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/statement.hpp"

namespace tinylamb {

namespace {

std::unique_ptr<SelectStatement> ParseSelect(const std::string& sql) {
  GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(sql);
  if (!parsed.ok) {
    return nullptr;
  }
  auto ast = GoogleSqlAstParser::Parse(parsed.ast);
  if (!ast.HasValue()) {
    return nullptr;
  }
  std::unique_ptr<Statement> statement =
      GoogleSqlAstVisitor::Visit(*ast.Value()).MoveValue();
  if (statement == nullptr || statement->Type() != StatementType::kSelect) {
    return nullptr;
  }
  return std::unique_ptr<SelectStatement>(
      static_cast<SelectStatement*>(statement.release()));  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
}

TEST(JoinReductionTest, NullRejectingWhereReducesLeftJoin) {
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.customer_id, o.order_id FROM customers c LEFT JOIN orders o "
      "ON c.customer_id = o.customer_id WHERE o.amount >= 60");
  ASSERT_NE(statement, nullptr);
  ASSERT_EQ(statement->Sources().size(), 2U);
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kLeft);
  EXPECT_TRUE(ReduceOuterJoins(statement.get()));
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kInner);
}

TEST(JoinReductionTest, NullAcceptingWhereKeepsLeftJoin) {
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.customer_id, o.order_id FROM customers c LEFT JOIN orders o "
      "ON c.customer_id = o.customer_id "
      "WHERE o.amount IS NULL OR o.amount >= 90");
  ASSERT_NE(statement, nullptr);
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kLeft);
  EXPECT_FALSE(ReduceOuterJoins(statement.get()));
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kLeft);
}

TEST(JoinReductionTest, WhereOnLeftSideOnlyKeepsLeftJoin) {
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.customer_id FROM customers c LEFT JOIN orders o "
      "ON c.customer_id = o.customer_id WHERE c.name = 'x'");
  ASSERT_NE(statement, nullptr);
  EXPECT_FALSE(ReduceOuterJoins(statement.get()));
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kLeft);
}

TEST(JoinReductionTest, AndCombinationOfRejectingConjunctsReduces) {
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.customer_id, o.order_id FROM customers c LEFT JOIN orders o "
      "ON c.customer_id = o.customer_id "
      "WHERE o.amount >= 60 AND o.region = 2");
  ASSERT_NE(statement, nullptr);
  EXPECT_TRUE(ReduceOuterJoins(statement.get()));
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kInner);
}

TEST(JoinReductionTest, AndWithNullAcceptingConjunctStillRejectsPadding) {
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.customer_id, o.order_id FROM customers c LEFT JOIN orders o "
      "ON c.customer_id = o.customer_id "
      "WHERE o.amount >= 60 AND o.amount IS NULL");
  ASSERT_NE(statement, nullptr);
  EXPECT_TRUE(ReduceOuterJoins(statement.get()));
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kInner);
}

TEST(JoinReductionTest, NotWrappingRejectingPredicateReduces) {
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.customer_id, o.order_id FROM customers c LEFT JOIN orders o "
      "ON c.customer_id = o.customer_id WHERE NOT o.amount >= 60");
  ASSERT_NE(statement, nullptr);
  EXPECT_TRUE(ReduceOuterJoins(statement.get()));
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kInner);
}

// NOT over an AND is NOT rejecting: on a NULL-padded row the padded
// comparison is NULL, the other conjunct can be FALSE, `NULL AND FALSE` is
// FALSE, and `NOT FALSE` is TRUE — the padded row must survive, so the
// outer join must stay.
TEST(JoinReductionTest, NotOverAndKeepsLeftJoin) {
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.customer_id, o.order_id FROM customers c LEFT JOIN orders o "
      "ON c.customer_id = o.customer_id "
      "WHERE NOT (o.amount >= 60 AND c.customer_id >= 0)");
  ASSERT_NE(statement, nullptr);
  EXPECT_FALSE(ReduceOuterJoins(statement.get()));
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kLeft);
}

TEST(JoinReductionTest, InnerJoinSourceIsUntouched) {
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.customer_id, o.order_id FROM customers c JOIN orders o "
      "ON c.customer_id = o.customer_id WHERE o.amount >= 60");
  ASSERT_NE(statement, nullptr);
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kInner);
  EXPECT_FALSE(ReduceOuterJoins(statement.get()));
}

TEST(JoinReductionTest, MissingWhereClauseIsNoOp) {
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.customer_id, o.order_id FROM customers c LEFT JOIN orders o "
      "ON c.customer_id = o.customer_id");
  ASSERT_NE(statement, nullptr);
  EXPECT_FALSE(ReduceOuterJoins(statement.get()));
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kLeft);
}

TEST(JoinReductionTest, RightNullRejectingWhereReducesRightJoin) {
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.customer_id FROM customers c RIGHT JOIN orders o "
      "ON c.customer_id = o.customer_id WHERE c.name = 'x'");
  ASSERT_NE(statement, nullptr);
  EXPECT_TRUE(ReduceOuterJoins(statement.get()));
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kInner);
}

TEST(JoinReductionTest, FullNonRejectingFilterKeepsFullJoin) {
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.customer_id FROM customers c FULL JOIN orders o "
      "ON c.customer_id = o.customer_id "
      "WHERE COALESCE(o.amount, 0) = 0");
  ASSERT_NE(statement, nullptr);
  EXPECT_FALSE(ReduceOuterJoins(statement.get()));
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kFull);
}

TEST(JoinReductionTest, ComplexNullAcceptingFilterKeepsLeftJoin) {
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.customer_id FROM customers c LEFT JOIN orders o "
      "ON c.customer_id = o.customer_id "
      "WHERE (o.amount >= 60 AND c.name = 'x') IS NOT NULL");
  ASSERT_NE(statement, nullptr);
  EXPECT_FALSE(ReduceOuterJoins(statement.get()));
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kLeft);
}

TEST(JoinReductionTest, NullRejectingHavingOnStrictAggReducesLeftJoin) {
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.region, AVG(o.amount), COUNT(o.order_id) "
      "FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id "
      "GROUP BY c.region HAVING AVG(o.amount) > 50");
  ASSERT_NE(statement, nullptr);
  EXPECT_TRUE(ReduceOuterJoins(statement.get()));
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kInner);
}

TEST(JoinReductionTest, HavingPositiveCountOnRightColumnReducesLeftJoin) {
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.region, COUNT(o.order_id) AS cnt "
      "FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id "
      "GROUP BY c.region HAVING cnt > 0");
  ASSERT_NE(statement, nullptr);
  EXPECT_TRUE(ReduceOuterJoins(statement.get()));
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kInner);
}

TEST(JoinReductionTest, HavingDemotionRefusesWhenCountStarPresent) {
  // Lean counterexample (`having_demote_breaks_count_star_on_coarse_group`):
  // unmatched rows inflate COUNT(*) in mixed groups, so LEFT JOIN must stay.
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.region, COUNT(*), MAX(o.amount) "
      "FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id "
      "GROUP BY c.region HAVING MAX(o.amount) > 50");
  ASSERT_NE(statement, nullptr);
  EXPECT_FALSE(ReduceOuterJoins(statement.get()));
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kLeft);
}

TEST(JoinReductionTest, EmptyInIsFalseDoesNotReduceLeftJoin) {
  // Lean audit (`cpp_empty_in_isFalse_bug`): in SQL, `NULL IN ()` evaluates to
  // FALSE (not NULL!), so `(o.amount IN ()) IS FALSE` is TRUE on NULL-padded
  // outer-join rows and MUST NOT demote LEFT JOIN to INNER JOIN.
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.customer_id, o.order_id FROM customers c LEFT JOIN orders o "
      "ON c.customer_id = o.customer_id");
  ASSERT_NE(statement, nullptr);
  const Expression empty_in = std::make_shared<InExpression>(
      ColumnValueExp(ColumnName("o", "amount")), std::vector<Expression>{});
  const Expression empty_in_is_false =
      UnaryExpressionExp(empty_in, UnaryOperation::kIsFalse);
  statement->SetWhereClause(empty_in_is_false);
  EXPECT_FALSE(ReduceOuterJoins(statement.get()));
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kLeft);

  const bool rewrite_rejects = ExpressionRejectsNullsWhere(
      empty_in_is_false,
      [](const ColumnName& col) { return col.schema == "o"; },
      /*bare_column_rejects_null=*/true);
  EXPECT_FALSE(rewrite_rejects);
}

TEST(JoinReductionTest, NeverFalseArithmeticAndBitNotZeroDoNotReduceLeftJoin) {
  // Lean audit (`cpp_neverFalse_sub_and_bitNot_bug`): non-zero integer
  // operands can cancel to 0 (`1 - 1 = 0`, `~(-1) = 0`), making `NOT (1 - 1)`
  // and `NOT (~(-1))` evaluate to TRUE on NULL-padded rows.
  for (const Expression& truthy_zero_trap : {
           UnaryExpressionExp(
               BinaryExpressionExp(ConstantValueExp(Value(int64_t{1})),
                                   BinaryOperation::kSubtract,
                                   ConstantValueExp(Value(int64_t{1}))),
               UnaryOperation::kNot),
           UnaryExpressionExp(
               UnaryExpressionExp(ConstantValueExp(Value(int64_t{-1})),
                                  UnaryOperation::kBitwiseNot),
               UnaryOperation::kNot),
       }) {
    const std::unique_ptr<SelectStatement> statement = ParseSelect(
        "SELECT c.customer_id, o.order_id FROM customers c LEFT JOIN orders o "
        "ON c.customer_id = o.customer_id");
    ASSERT_NE(statement, nullptr);
    statement->SetWhereClause(truthy_zero_trap);
    EXPECT_FALSE(ReduceOuterJoins(statement.get()));
    EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kLeft);
  }
}

}  // namespace

}  // namespace tinylamb
