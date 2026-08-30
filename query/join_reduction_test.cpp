/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/join_reduction.hpp"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "query/googlesql_ast.hpp"
#include "query/googlesql_ast_visitor.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/statement.hpp"

namespace tinylamb {

namespace {

std::unique_ptr<SelectStatement> ParseSelect(const std::string& sql) {
  GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(sql);
  if (!parsed.ok) { return nullptr;
}
  auto ast = GoogleSqlAstParser::Parse(parsed.ast);
  if (!ast.HasValue()) { return nullptr;
}
  std::unique_ptr<Statement> statement =
      GoogleSqlAstVisitor::Visit(*ast.Value());
  if (statement == nullptr ||
      statement->Type() != StatementType::kSelect) {
    return nullptr;
  }
  return std::unique_ptr<SelectStatement>(
      static_cast<SelectStatement*>(statement.release()));
}

TEST(JoinReductionTest, NullRejectingWhereReducesLeftJoin) {
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.customer_id, o.order_id FROM customers c LEFT JOIN orders o "
      "ON c.customer_id = o.customer_id WHERE o.amount >= 60");
  ASSERT_NE(statement, nullptr);
  ASSERT_EQ(statement->Sources().size(), 2u);
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

TEST(JoinReductionTest, OneNullAcceptingConjunctKeepsLeftJoin) {
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.customer_id, o.order_id FROM customers c LEFT JOIN orders o "
      "ON c.customer_id = o.customer_id "
      "WHERE o.amount >= 60 AND o.amount IS NULL");
  ASSERT_NE(statement, nullptr);
  EXPECT_FALSE(ReduceOuterJoins(statement.get()));
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kLeft);
}

TEST(JoinReductionTest, NotWrappingRejectingPredicateReduces) {
  const std::unique_ptr<SelectStatement> statement = ParseSelect(
      "SELECT c.customer_id, o.order_id FROM customers c LEFT JOIN orders o "
      "ON c.customer_id = o.customer_id WHERE NOT o.amount >= 60");
  ASSERT_NE(statement, nullptr);
  EXPECT_TRUE(ReduceOuterJoins(statement.get()));
  EXPECT_EQ(statement->Sources()[1].join_type, JoinType::kInner);
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

}  // namespace

}  // namespace tinylamb
