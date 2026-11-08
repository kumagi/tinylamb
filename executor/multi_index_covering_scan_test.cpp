/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// End-to-end tests for the multi-index covering scan (Phase 2 "複数インデックス
// を用いたcovering 判定を実装する").
//
// What is under test, in the order the layers own it:
//
//  * plan/implementation_rules.cpp decides the shape: two or more indexes
//    whose STORED columns (key + INCLUDE) together cover every column the
//    predicate and the projection touch, while no single index covers them
//    alone -- that case stays the index-only path.  Every participant
//    contributes a leading-key range taken from the filter, so the
//    intersection of their entries is a superset of the matching rows and the
//    recheck on the covered columns decides.
//  * plan/multi_index_covering_scan_plan.cpp carries the participants and
//    publishes the covered layout (the union of their stored columns, in table
//    slot order) as the node's schema.
//  * executor/multi_index_covering_scan.cpp intersects the participants by
//    row position and assembles each row from the index images alone when the
//    entry's page carries no version chains (the same visibility argument as
//    IndexScanIterator::TryReadIndexOnly); a chain-bearing page falls back to
//    the snapshot-resolving heap read.
//  * executor/relational_factory.cpp takes the same stale-key gate
//    IndexOnlyScanPlan::EmitExecutor takes, around an index read guard for
//    EVERY participant acquired in one global order.
//
// The RESULT is the thing under test everywhere: an access path may never
// change which rows a statement returns.  Every SQL case compares against a
// plain full scan of an identical, index-free twin table, and the direct
// executor cases compare against the same query over that twin.
#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
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
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "index/index_schema.hpp"
#include "plan/bitmap_scan_plan.hpp"
#include "plan/multi_index_covering_scan_plan.hpp"
#include "plan/plan.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
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

constexpr size_t kRows = 400;
// The filter the multi-index cases share: a range on `part` (served by
// IdxPart) AND a range on `qty` (served by IdxQty).  Neither index covers the
// projected `region` on its own, so the pair has to.
constexpr int64_t kPartLo = 40;
constexpr int64_t kPartHi = 320;
constexpr int64_t kQtyLo = 30;
constexpr int64_t kQtyHi = 270;

// The filter the multi-index cases share: a range on `part` (served by
// IdxPart) AND a range on `qty` (served by IdxQty).  Neither index covers the
// projected `region` on its own, so the pair has to.  The `region` conjuncts
// are deliberately NOT the leading key of any index: the participants' ranges
// admit rows they exclude, so the recheck on the covered columns is what
// decides the answer (a predicate that is exactly the conjunction of the two
// ranges would make the recheck a no-op and prove nothing).
constexpr const char* kRangePredicate =
    "part >= 40 AND part < 320 AND qty >= 30 AND qty < 270 AND region >= 200 "
    "AND region < 260";

// Just the two leading-key ranges -- i.e. what the participants' index ranges
// encode on their own.  It is a strict superset of kRangePredicate, which is
// exactly why the recheck is load-bearing.
constexpr const char* kWideRanges =
    "part >= 40 AND part < 320 AND qty >= 30 AND qty < 270";

std::vector<Row> Drain(ExecutorBase* executor) {
  std::vector<Row> rows;
  Row row;
  RowPosition position;
  while (executor->Next(&row, &position)) {
    rows.push_back(std::move(row));
    row = Row();
  }
  return rows;
}

// Renders rows as "a|b|c" so a case asserts with one readable comparison.
// Sorted: an index intersection emits in row-position order, which need not
// match the order a full scan of the twin table produces.
std::vector<std::string> SortedText(const std::vector<Row>& rows) {
  std::vector<std::string> out;
  out.reserve(rows.size());
  for (const Row& row : rows) {
    std::ostringstream cell;
    cell << row[0] << "|" << row[1] << "|" << row[2];
    out.push_back(cell.str());
  }
  std::ranges::sort(out);
  return out;
}

class MultiIndexCoveringScanTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "multi_index_covering_test-" + RandomString();
    database_ = Database::Create(prefix_).MoveValue();
    TransactionContext setup = database_->BeginContext();
    // `parts` carries the two indexes that only cover the query TOGETHER;
    // `parts_plain` holds the identical rows with no index at all, so every
    // case has a full-scan ground truth.
    ASSIGN_OR_ASSERT_FAIL(
        Table, parts,
        database_->CreateTable(
            setup, Schema("parts", {Column("part", ValueType::kInt64),
                                    Column("qty", ValueType::kInt64),
                                    Column("region", ValueType::kInt64)})));
    ASSIGN_OR_ASSERT_FAIL(
        Table, plain,
        database_->CreateTable(
            setup,
            Schema("parts_plain", {Column("part", ValueType::kInt64),
                                   Column("qty", ValueType::kInt64),
                                   Column("region", ValueType::kInt64)})));
    for (size_t i = 0; i < kRows; ++i) {
      const auto part = static_cast<int64_t>(i);
      const auto qty = static_cast<int64_t>((i * 37) % kRows);
      // `region` is a permutation of 0..kRows (53 is coprime with kRows), so a
      // unique index over it is buildable -- which is what the retaining-index
      // case needs to prove it is skipped on its MODE, not on its data.
      const auto region = static_cast<int64_t>((i * 53) % kRows);
      ASSERT_SUCCESS(
          parts
              .Insert(setup.txn_, Row({Value(part), Value(qty), Value(region)}))
              .GetStatus());
      ASSERT_SUCCESS(
          plain
              .Insert(setup.txn_, Row({Value(part), Value(qty), Value(region)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(database_->CreateIndex(
        setup, "parts",
        IndexSchema("IdxPart", {0}, {1}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(database_->CreateIndex(
        setup, "parts",
        IndexSchema("IdxQty", {1}, {2}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(setup.txn_.PreCommit());
    // Row counts and NDVs drive the cost model; without statistics no access
    // path is ever priced below a full scan.
    std::vector<Row> analyzed;
    ASSERT_SUCCESS(Run("ANALYZE", &analyzed));
  }

  void TearDown() override { database_->DeleteAll(); }

  Status Run(std::string_view sql, std::vector<Row>* rows) {
    TransactionContext context = database_->BeginContext();
    SqlEngine engine(*database_);
    StatusOr<Executor> prepared = engine.Prepare(context, sql);
    if (!prepared.HasValue()) {
      context.Abort();
      return prepared.GetStatus();
    }
    *rows = Drain(prepared.Value().get());
    if (prepared.Value()->GetStatus() != Status::kSuccess) {
      const Status status = prepared.Value()->GetStatus();
      context.Abort();
      return status;
    }
    return context.PreCommit();
  }

  std::string Explain(std::string_view sql) {
    TransactionContext context = database_->BeginContext();
    SqlEngine engine(*database_);
    StatusOr<Executor> prepared =
        engine.Prepare(context, "EXPLAIN " + std::string(sql));
    if (!prepared.HasValue()) {
      context.Abort();
      return "ERROR: " + engine.LastError();
    }
    std::ostringstream text;
    for (const Row& row : Drain(prepared.Value().get())) {
      text << row[0] << "\n";
    }
    context.Abort();
    return text.str();
  }

  // The same predicate over the index-free twin, for the ground-truth
  // comparison. |columns| selects the projection.
  std::vector<Row> Twin(const char* columns, const char* predicate) {
    std::vector<Row> rows;
    EXPECT_SUCCESS(Run(std::string("SELECT ") + columns +
                           " FROM parts_plain WHERE " + predicate,
                       &rows));
    return rows;
  }

  std::string prefix_;
  std::unique_ptr<Database> database_;
};

// The chosen access path: the pair covers the predicate AND the projection,
// no single index does, and the cost model prefers two index walks over a
// clustering-penalized index seek plus every heap fetch the recheck needs.
TEST_F(MultiIndexCoveringScanTest, PlanChoosesTheMultiIndexCoveringScan) {
  const std::string plan =
      Explain("SELECT region FROM parts WHERE " + std::string(kRangePredicate));
  EXPECT_NE(plan.find("MultiIndexCoveringScan"), std::string::npos) << plan;
  EXPECT_EQ(plan.find("FullScan"), std::string::npos) << plan;
  // Both participants are named, so the shape is auditable from EXPLAIN.
  EXPECT_NE(plan.find("IdxPart"), std::string::npos) << plan;
  EXPECT_NE(plan.find("IdxQty"), std::string::npos) << plan;
}

// The access path may never change the answer: the identical rows in an
// index-free twin table, answered by a plain full scan, must match row for
// row.
TEST_F(MultiIndexCoveringScanTest, ResultMatchesTheFullScanOfTheSameRows) {
  std::vector<Row> indexed;
  ASSERT_SUCCESS(Run("SELECT part, qty, region FROM parts WHERE " +
                         std::string(kRangePredicate),
                     &indexed));
  const std::vector<Row> scanned = Twin("part, qty, region", kRangePredicate);
  EXPECT_FALSE(indexed.empty());
  EXPECT_EQ(SortedText(indexed), SortedText(scanned));
}

// The intersection is a SUPERSET filter, never a shortcut: a row inside both
// index ranges but outside the full predicate must not be served, and the
// empty result is the easy direction to get wrong in the covering path.
TEST_F(MultiIndexCoveringScanTest, RecheckRejectsRowsTheRangesAdmit) {
  std::vector<Row> rows;
  // Every case below keeps both leading-key ranges WIDE (so both participants
  // admit rows) and narrows only the residual conjuncts.
  const char* const kWide = kWideRanges;
  // The ranges admit rows; the residual rejects every one of them.
  ASSERT_SUCCESS(Run(std::string("SELECT region FROM parts WHERE ") + kWide +
                         " AND region > 1000",
                     &rows));
  EXPECT_TRUE(rows.empty());
  // One conjunct wider than any row's region: same shape, opposite direction
  // on the residual.
  ASSERT_SUCCESS(Run(std::string("SELECT region FROM parts WHERE ") + kWide +
                         " AND region >= -1 AND region < 0",
                     &rows));
  EXPECT_TRUE(rows.empty());
  // Dropping the residual conjuncts makes the same plan return rows: the
  // emptiness above came from the recheck, not from a lost participant.  The
  // projection is the full triple so both sides render column for column.
  ASSERT_SUCCESS(
      Run(std::string("SELECT part, qty, region FROM parts WHERE ") + kWide,
          &rows));
  EXPECT_FALSE(rows.empty());
  EXPECT_EQ(SortedText(rows), SortedText(Twin("part, qty, region", kWide)));
}

// NULL keys never satisfy a range comparison and are never probed by an
// equality seek, so they must simply be absent -- not surfaced, and not
// breaking the recheck's three-valued evaluation.
TEST_F(MultiIndexCoveringScanTest, NullKeysAreAbsentFromTheResult) {
  {
    TransactionContext setup = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, parts, database_->GetTable(setup, "parts"));
    ASSERT_SUCCESS(parts
                       .Insert(setup.txn_, Row({Value(int64_t{100}), Value(),
                                                Value(int64_t{9})}))
                       .GetStatus());
    ASSERT_SUCCESS(parts
                       .Insert(setup.txn_, Row({Value(), Value(int64_t{100}),
                                                Value(int64_t{9})}))
                       .GetStatus());
    ASSERT_SUCCESS(setup.txn_.PreCommit());
  }
  std::vector<Row> indexed;
  ASSERT_SUCCESS(Run("SELECT part, qty, region FROM parts WHERE " +
                         std::string(kRangePredicate),
                     &indexed));
  // The twin has no NULLs at all, so the two new rows must be absent from the
  // covered answer -- proving the ranges and the recheck both reject them.
  EXPECT_EQ(SortedText(indexed),
            SortedText(Twin("part, qty, region", kRangePredicate)));
  // ...and the NULL rows really are on the table.
  std::vector<Row> nulls;
  ASSERT_SUCCESS(
      Run("SELECT part FROM parts WHERE part IS NULL AND qty = 100", &nulls));
  ASSERT_EQ(nulls.size(), 1U);
  EXPECT_TRUE(nulls[0][0].IsNull());
}

// A single covering index is the index-only path's job, never a two-index
// intersection: the plan must stay on IndexOnlyScan.
TEST_F(MultiIndexCoveringScanTest, SingleCoveringIndexKeepsTheIndexOnlyPath) {
  TransactionContext setup = database_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, solo,
      database_->CreateTable(setup,
                             Schema("solo", {Column("k", ValueType::kInt64),
                                             Column("v", ValueType::kInt64)})));
  for (int64_t i = 0; i < 400; ++i) {
    ASSERT_SUCCESS(
        solo.Insert(setup.txn_, Row({Value(i), Value(i * 2)})).GetStatus());
  }
  ASSERT_SUCCESS(database_->CreateIndex(
      setup, "solo", IndexSchema("SoloIdx", {0}, {1}, IndexMode::kNonUnique)));
  ASSERT_SUCCESS(setup.txn_.PreCommit());
  const std::string plan =
      Explain("SELECT v FROM solo WHERE k >= 40 AND k < 320");
  EXPECT_NE(plan.find("IndexOnlyScan"), std::string::npos) << plan;
  EXPECT_EQ(plan.find("MultiIndexCoveringScan"), std::string::npos) << plan;
}

// An entry-retaining (versioned unique) index is not the committed image the
// covering read serves, so it must not become a participant -- and the
// statement still answers correctly without it.
TEST_F(MultiIndexCoveringScanTest, RetainingIndexIsNotAParticipant) {
  {
    TransactionContext setup = database_->BeginContext();
    ASSERT_SUCCESS(database_->CreateIndex(
        setup, "parts",
        IndexSchema("IdxRegion", {2}, {0}, IndexMode::kVersionedUnique)));
    ASSERT_SUCCESS(setup.txn_.PreCommit());
  }
  // `region` carries a range too, so IdxRegion has everything a participant
  // needs except the one thing that disqualifies it: its entries are RETAINED
  // across delete, so they are not the committed image the covering read
  // serves.  The two non-retained indexes must answer without it.
  constexpr const char* kThreeWayPredicate =
      "part >= 40 AND part < 320 AND qty >= 30 AND qty < 270 AND region >= 40 "
      "AND region < 320";
  const std::string plan = Explain("SELECT region FROM parts WHERE " +
                                   std::string(kThreeWayPredicate));
  EXPECT_EQ(plan.find("IdxRegion"), std::string::npos) << plan;
  std::vector<Row> indexed;
  ASSERT_SUCCESS(Run("SELECT part, qty, region FROM parts WHERE " +
                         std::string(kThreeWayPredicate),
                     &indexed));
  EXPECT_FALSE(indexed.empty());
  EXPECT_EQ(SortedText(indexed),
            SortedText(Twin("part, qty, region", kThreeWayPredicate)));
}

// An UPDATE installs a new committed image: its page grows version chains (so
// the heap fallback takes over for those positions) and it rewrites the
// entries' INCLUDE payloads.  The covering read must answer the post-update
// image, which is what the chain check and the guard exist for.
TEST_F(MultiIndexCoveringScanTest, UpdatedRowsAreReadAtTheirNewImage) {
  std::vector<Row> affected;
  ASSERT_SUCCESS(Run("UPDATE parts SET qty = 7 WHERE part = 100", &affected));
  std::vector<Row> indexed;
  ASSERT_SUCCESS(Run("SELECT part, qty, region FROM parts WHERE " +
                         std::string(kRangePredicate),
                     &indexed));
  ASSERT_SUCCESS(
      Run("UPDATE parts_plain SET qty = 7 WHERE part = 100", &affected));
  EXPECT_EQ(SortedText(indexed),
            SortedText(Twin("part, qty, region", kRangePredicate)));
  // The new value is the one served (part=100, qty=100 is now outside the
  // qty range, so it must be gone from the answer entirely).
  for (const Row& row : indexed) {
    if (row[0].value.int_value == 100) {
      EXPECT_EQ(row[1].value.int_value, 7);
    }
  }
  std::vector<Row> gone;
  ASSERT_SUCCESS(
      Run("SELECT COUNT(*) FROM parts WHERE part = 100 AND qty = "
          "100",
          &gone));
  ASSERT_EQ(gone.size(), 1U);
  EXPECT_EQ(gone[0][0].value.int_value, 0);
}

// A row deleted after the index entries were written must not survive the
// covering read: the surviving positions are rechecked against the heap's
// visibility.
TEST_F(MultiIndexCoveringScanTest, DeletedRowsDisappear) {
  std::vector<Row> affected;
  ASSERT_SUCCESS(Run("DELETE FROM parts WHERE part = 100", &affected));
  ASSERT_SUCCESS(Run("DELETE FROM parts_plain WHERE part = 100", &affected));
  std::vector<Row> indexed;
  ASSERT_SUCCESS(Run("SELECT part, qty, region FROM parts WHERE " +
                         std::string(kRangePredicate),
                     &indexed));
  EXPECT_EQ(SortedText(indexed),
            SortedText(Twin("part, qty, region", kRangePredicate)));
  for (const Row& row : indexed) {
    EXPECT_NE(row[0].value.int_value, 100);
  }
}

// The direct executor: the node answers exactly what the same query over the
// index-free twin answers, so the operator contract is pinned without the
// cost model in the way.
TEST_F(MultiIndexCoveringScanTest, DirectExecutorMatchesTheFullScan) {
  TransactionContext context = database_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, parts, database_->GetTable(context, "parts"));
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<TableStatistics>, stats,
                              context.GetStats("parts"));
  std::vector<BitmapIndexRange> ranges;
  BitmapIndexRange by_part;
  by_part.index = &parts.GetIndex(0);  // IdxPart, key `part`
  by_part.begin_key.emplace_back(kPartLo);
  by_part.end_key.emplace_back(kPartHi);
  ranges.push_back(std::move(by_part));
  BitmapIndexRange by_qty;
  by_qty.index = &parts.GetIndex(1);  // IdxQty, key `qty`
  by_qty.begin_key.emplace_back(kQtyLo);
  by_qty.end_key.emplace_back(kQtyHi);
  ranges.push_back(std::move(by_qty));

  const auto cmp = [](std::string_view column, BinaryOperation op,
                      int64_t bound) {
    return BinaryExpressionExp(ColumnValueExp(ColumnName(column)), op,
                               ConstantValueExp(Value(bound)));
  };
  // The predicate names only `region` -- a column NO participant's range
  // encodes.  The two seeks therefore admit a strict superset of the answer
  // and the intersection of their entries is what narrows it, so this case
  // pins the intersection independently of the recheck (a recheck-only test
  // cannot tell a working intersection from a skipped one).
  constexpr int64_t kRegionLo = 200;
  constexpr int64_t kRegionHi = 260;
  Expression predicate = BinaryExpressionExp(
      cmp("region", BinaryOperation::kGreaterThanEquals, kRegionLo),
      BinaryOperation::kAnd,
      cmp("region", BinaryOperation::kLessThan, kRegionHi));

  std::unique_ptr<MultiIndexCoveringScanPlan> plan(
      new MultiIndexCoveringScanPlan(parts, *stats, ranges, predicate, 64,
                                     128));
  // The covered layout is the union of the participants' stored columns in
  // table slot order -- all three here, so no projection is needed on top.
  ASSERT_EQ(plan->GetSchema().ColumnCount(), 3U);
  EXPECT_EQ(plan->GetSchema().GetColumn(0).Name().name, "part");
  EXPECT_EQ(plan->GetSchema().GetColumn(1).Name().name, "qty");
  EXPECT_EQ(plan->GetSchema().GetColumn(2).Name().name, "region");
  // Both participants and the recheck are auditable from the dump.
  const std::string dump = plan->ToString();
  EXPECT_NE(dump.find("MultiIndexCoveringScan"), std::string::npos) << dump;
  EXPECT_NE(dump.find("IdxPart"), std::string::npos) << dump;
  EXPECT_NE(dump.find("IdxQty"), std::string::npos) << dump;
  // The recheck carries ONLY the region conjunct, not the ranges -- proof the
  // narrowing came from the seeks rather than from a duplicated filter.
  EXPECT_EQ(dump.find("part >="), std::string::npos) << dump;

  Executor executor = plan->EmitExecutor(context);
  const std::vector<Row> rows = Drain(executor.get());
  ASSERT_SUCCESS(context.PreCommit());
  EXPECT_EQ(SortedText(rows),
            SortedText(Twin("part, qty, region", kRangePredicate)));
  EXPECT_FALSE(rows.empty());
  // Strictly fewer rows than either seek alone would admit, or the
  // intersection never happened.
  const std::vector<Row> only_part =
      Twin("part, qty, region",
           "part >= 40 AND part < 320 AND region >= 200 "
           "AND region < 260");
  EXPECT_LT(rows.size(), only_part.size());
}

}  // namespace
}  // namespace tinylamb