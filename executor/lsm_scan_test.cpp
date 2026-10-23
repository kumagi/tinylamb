/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */

// LsmScan: the plan/executor half of the LSM/WiscKey path.  The scan streams
// a tree's live (key, value) pairs (newest generation wins, tombstones
// skipped) and the `lsm_scan` implementation rule turns a directly
// constructed kLsmScan memo node into the plan.  SQL does not produce the
// node yet.

#include "executor/lsm_scan.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "index/lsm_tree.hpp"
#include "plan/cascades.hpp"
#include "plan/implementation_rules.hpp"
#include "plan/lsm_scan_plan.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb::cascades {
namespace {

std::vector<Row> Drain(const Executor& executor) {
  std::vector<Row> rows;
  Row row;
  while (executor->Next(&row, nullptr)) {
    rows.push_back(row);
  }
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
  return rows;
}

}  // namespace

class LsmScanTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_path_ = "lsm_scan_test-" + RandomString();
    tree_ = LSMTree::Create(dir_path_).MoveValue();
    db_ = Database::Create("lsm_scan_db-" + RandomString()).MoveValue();
  }

  void TearDown() override {
    tree_.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::filesystem::remove_all(dir_path_);
    if (db_ != nullptr) {
      db_->DeleteAll();
    }
  }

  // Writes a small live set: an overwritten key, a deleted key, and a key
  // long enough to take the blob path (beyond the inline threshold).  Sync
  // publishes the memtable as a run so the view (which covers registered
  // runs, not the memtable) sees the writes deterministically.
  void SeedTree() {
    ASSERT_SUCCESS(tree_->Write("b", "beta"));
    ASSERT_SUCCESS(tree_->Write("a", "alpha"));
    ASSERT_SUCCESS(tree_->Write("d", "delta-old"));
    ASSERT_SUCCESS(tree_->Write("d", "delta"));
    ASSERT_SUCCESS(tree_->Write("c", "charlie"));
    ASSERT_SUCCESS(tree_->Delete("c"));
    ASSERT_SUCCESS(tree_->Write(std::string(40, 'k'), std::string(64, 'v')));
    ASSERT_SUCCESS(tree_->Sync());
  }

  std::string dir_path_;
  std::unique_ptr<LSMTree> tree_;
  std::unique_ptr<Database> db_;
};

TEST_F(LsmScanTest, StreamsLivePairsInKeyOrder) {
  SeedTree();
  LsmScan scan(*tree_);
  Row row;
  std::vector<Row> rows;
  while (scan.Next(&row, nullptr)) {
    rows.push_back(row);
  }
  ASSERT_EQ(scan.GetStatus(), Status::kSuccess);
  // Live keys in byte order: "a" < "b" < "d" < the 40-byte key.
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0], Row({Value("a"), Value("alpha")}));
  EXPECT_EQ(rows[1], Row({Value("b"), Value("beta")}));
  EXPECT_EQ(rows[2], Row({Value("d"), Value("delta")}));
  EXPECT_EQ(rows[3],
            Row({Value(std::string(40, 'k')), Value(std::string(64, 'v'))}));
}

TEST_F(LsmScanTest, RuleSelectsThePlanAndEmitsTheSameRows) {
  SeedTree();
  Memo memo;
  const GroupId group = memo.EnsureDerivedGroup({}, "lsm");
  LogicalExpression expression;
  expression.operation = LogicalOperator::kLsmScan;
  expression.lsm_tree = tree_.get();
  expression.lsm_estimated_rows = 4;
  ASSERT_TRUE(memo.AddExpression(group, expression));
  SearchEngine search(std::move(memo), RuleSet::Default());
  const auto plan = search.Optimize(group, PhysicalProperties{},
                                    tinylamb::DefaultImplementationRules());
  ASSERT_TRUE(plan.has_value());
  if (!plan) {
    return;
  }
  EXPECT_NE(plan->plan->ToString().find("LsmScan"), std::string::npos);
  EXPECT_EQ(plan->plan->EmitRowCount(), 4U);
  // The view merges runs by key, so the plan claims ascending key order and
  // nothing else.
  EXPECT_TRUE(plan->plan->IsOrderedBy({ColumnValueExp("key")}, {true}));
  EXPECT_FALSE(plan->plan->IsOrderedBy({ColumnValueExp("value")}, {true}));
  EXPECT_FALSE(plan->plan->IsOrderedBy({ColumnValueExp("key")}, {false}));

  TransactionContext context = db_->BeginContext();
  const std::vector<Row> rows = Drain(plan->plan->EmitExecutor(context));
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0], Row({Value("a"), Value("alpha")}));
  EXPECT_EQ(rows[2], Row({Value("d"), Value("delta")}));
  ASSERT_SUCCESS(context.PreCommit());
}

// Top-N propagation (todo.md Phase 4): the LSM view streams keys in
// structural order, so a caller whose required properties carry a limit
// hint and query-ordering expectation may price the lazy prefix read
// instead of the full walk. The helper's gates live in ApplyLimitHint;
// this test pins that the hint reaches the lsm_scan rule (through an
// explicit RuleContext) and that an unrelated ordering gets the full cost.
TEST_F(LsmScanTest, LimitHintDiscountsTheStructuredOrderedScan) {
  SeedTree();
  Memo memo;
  const GroupId group = memo.EnsureDerivedGroup({}, "lsm");
  LogicalExpression expression;
  expression.operation = LogicalOperator::kLsmScan;
  expression.lsm_tree = tree_.get();
  expression.lsm_estimated_rows = 4;
  ASSERT_TRUE(memo.AddExpression(group, expression));

  QueryData context_query;
  context_query.order_expressions_ = {ColumnValueExp("key")};
  context_query.order_ascending_ = {true};
  cascades::RuleContext context;
  context.query = &context_query;

  PhysicalProperties prefix;
  prefix.limit_hint = 2;
  SearchEngine prefixed(std::move(memo), RuleSet::Default());
  const auto discounted =
      prefixed.Optimize(group, prefix, DefaultImplementationRules(), context);
  // The hint caps the prefix read: cost joins the estimated rows at the
  // prefix size, well under the 4-row full walk.
  ASSERT_TRUE(discounted.has_value());
  EXPECT_EQ(discounted->estimated_rows, 2.0);
  EXPECT_NEAR(discounted->cost, 2.0, 1e-9);

  // Value equality is untouched: the prefix plan still runs to completion
  // for a caller that keeps draining (the discount only changes pricing).
  TransactionContext run = db_->BeginContext();
  const std::vector<Row> rows = Drain(discounted->plan->EmitExecutor(run));
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0], Row({Value("a"), Value("alpha")}));
  EXPECT_EQ(rows[2], Row({Value("d"), Value("delta")}));
  ASSERT_SUCCESS(run.PreCommit());

  // The discount is ordering-specific: no query ordering means no prefix
  // (the planner cannot know the caller would stop early), so the full
  // walk keeps its cost and row estimate.
  Memo plain_memo;
  const GroupId plain_group = plain_memo.EnsureDerivedGroup({}, "lsm");
  ASSERT_TRUE(plain_memo.AddExpression(plain_group, expression));
  SearchEngine plain(std::move(plain_memo), RuleSet::Default());
  const auto undiscounted = plain.Optimize(
      plain_group, PhysicalProperties{}, DefaultImplementationRules(), context);
  ASSERT_TRUE(undiscounted.has_value());
  EXPECT_EQ(undiscounted->cost, 4.0);
  EXPECT_EQ(undiscounted->estimated_rows, 4.0);
}

TEST_F(LsmScanTest, EmptyTreeYieldsNoRows) {
  ASSERT_SUCCESS(tree_->Sync());
  LsmScan scan(*tree_);
  Row row;
  EXPECT_FALSE(scan.Next(&row, nullptr));
  EXPECT_EQ(scan.GetStatus(), Status::kSuccess);
}

TEST_F(LsmScanTest, FingerprintSeparatesTreesAndEstimates) {
  SeedTree();
  LogicalExpression first;
  first.operation = LogicalOperator::kLsmScan;
  first.lsm_tree = tree_.get();
  first.lsm_estimated_rows = 4;
  const LogicalExpression same = first;
  EXPECT_EQ(first.Fingerprint(), same.Fingerprint());
  LogicalExpression other_estimate = first;
  other_estimate.lsm_estimated_rows = 5;
  EXPECT_NE(first.Fingerprint(), other_estimate.Fingerprint());
}

}  // namespace tinylamb::cascades
