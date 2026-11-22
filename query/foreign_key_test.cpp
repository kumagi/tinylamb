/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "executor/foreign_key.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/memory_budget.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "type/column.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {
// Executes one statement and returns the materialized rows.  A prepare or
// execution failure surfaces as a runtime_error carrying the diagnostic.
std::vector<Row> RunSql(SqlEngine* engine, TransactionContext* ctx,
                        const std::string& sql) {
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    // Prepare-time DDL failures carry the diagnostic in the Status;
    // LastError is only populated on some paths.
    const std::string& last = engine->LastError();
    throw std::runtime_error(last.empty() ? prepared.GetStatus().GetMessage()
                                          : last);
  }
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
    row = Row();
  }
  const Status st = prepared.Value()->GetStatus();
  if (st != Status::kSuccess) {
    throw std::runtime_error(st.GetMessage());
  }
  return rows;
}

std::string ExpectThrow(SqlEngine* engine, TransactionContext* ctx,
                        const std::string& sql) {
  try {
    RunSql(engine, ctx, sql);
  } catch (const std::runtime_error& e) {
    return e.what();
  }
  return "";
}

void ExpectOk(SqlEngine* engine, TransactionContext* ctx,
              const std::string& sql) {
  EXPECT_EQ(ExpectThrow(engine, ctx, sql), "") << sql;
}

void ExpectViolation(SqlEngine* engine, TransactionContext* ctx,
                     const std::string& sql) {
  const std::string error = ExpectThrow(engine, ctx, sql);
  EXPECT_NE(error.find("foreign key"), std::string::npos)
      << sql << " :: " << error;
}
}  // namespace

class ForeignKeyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    // ctest runs each case as its own process in the same directory and may
    // run them in parallel: the database file name must be unique per test,
    // otherwise concurrent cases stomp on each other's files.
    const ::testing::TestInfo* info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    db_name_ = std::string("sql_foreign_key_test_") +
               (info != nullptr ? info->name() : "unknown");
    database_ = Database::Create(db_name_).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }
  void MakeParentChild() {
    ExpectOk(engine_.get(), context_.get(),
             "CREATE TABLE fk_parent (id INT64)");
    ExpectOk(engine_.get(), context_.get(),
             "CREATE TABLE fk_child (id INT64, pid INT64, FOREIGN KEY (pid) "
             "REFERENCES fk_parent(id))");
    ExpectOk(engine_.get(), context_.get(),
             "INSERT INTO fk_parent VALUES (1), (2)");
  }
  std::string db_name_;
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(ForeignKeyTest, TableLevelDefinitionStoredCanonically) {
  ASSERT_NO_THROW(MakeParentChild());
  StatusOr<std::shared_ptr<Table>> child = context_->GetTable("fk_child");
  ASSERT_TRUE(child.HasValue());
  const Column& pid = child.Value()->GetSchema().GetColumn(1);
  EXPECT_EQ(pid.GetConstraint().ctype, Constraint::kForeign);
  std::string table;
  std::vector<std::string> columns;
  ASSERT_TRUE(ParseForeignKeyReference(pid.GetConstraint(), &table, &columns));
  EXPECT_EQ(table, "fk_parent");
  ASSERT_EQ(columns.size(), 1U);
  EXPECT_EQ(columns[0], "id");
}

TEST_F(ForeignKeyTest, ColumnLevelReferencesAccepted) {
  ExpectOk(engine_.get(), context_.get(), "CREATE TABLE fk_p2 (id INT64)");
  ExpectOk(engine_.get(), context_.get(),
           "CREATE TABLE fk_c2 (id INT64, pid INT64 REFERENCES fk_p2(id))");
  ExpectOk(engine_.get(), context_.get(), "INSERT INTO fk_p2 VALUES (1)");
  ExpectOk(engine_.get(), context_.get(), "INSERT INTO fk_c2 VALUES (1, 1)");
  ExpectViolation(engine_.get(), context_.get(),
                  "INSERT INTO fk_c2 VALUES (2, 9)");
}

TEST_F(ForeignKeyTest, InsertValidAndNullPass) {
  ASSERT_NO_THROW(MakeParentChild());
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_child VALUES (10, 1), (11, NULL)");
  const std::vector<Row> rows = RunSql(engine_.get(), context_.get(),
                                       "SELECT id FROM fk_child ORDER BY id");
  ASSERT_EQ(rows.size(), 2U);
}

TEST_F(ForeignKeyTest, InsertViolationRejected) {
  ASSERT_NO_THROW(MakeParentChild());
  ExpectViolation(engine_.get(), context_.get(),
                  "INSERT INTO fk_child VALUES (10, 9)");
}

TEST_F(ForeignKeyTest, UpdateChildKeyEnforced) {
  ASSERT_NO_THROW(MakeParentChild());
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_child VALUES (10, 1)");
  ExpectOk(engine_.get(), context_.get(),
           "UPDATE fk_child SET pid = 2 WHERE id = 10");
  ExpectViolation(engine_.get(), context_.get(),
                  "UPDATE fk_child SET pid = 9 WHERE id = 10");
}

TEST_F(ForeignKeyTest, DeleteReferencedParentRejected) {
  ASSERT_NO_THROW(MakeParentChild());
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_child VALUES (10, 1)");
  ExpectViolation(engine_.get(), context_.get(),
                  "DELETE FROM fk_parent WHERE id = 1");
  ExpectOk(engine_.get(), context_.get(), "DELETE FROM fk_parent WHERE id = 2");
}

TEST_F(ForeignKeyTest, DeleteKeepsReferencesToSurvivingDuplicateParentKeys) {
  // The referenced column need not be unique: deleting one of several
  // parent rows that share a key leaves the key supplied by a surviving
  // row, so the child reference must not reject. The NO ACTION check used
  // to emit every pending row's key without subtracting the keys that
  // remain in the parent (the UPDATE side already applies the equivalent
  // supplied_keys correction).
  ExpectOk(engine_.get(), context_.get(),
           "CREATE TABLE fk_repeat_parent (a INT64, tag INT64)");
  ExpectOk(
      engine_.get(), context_.get(),
      "CREATE TABLE fk_repeat_child (a INT64 REFERENCES fk_repeat_parent(a))");
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_repeat_parent VALUES (1, 10), (1, 20), (1, 30)");
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_repeat_child VALUES (1)");
  // Two of the three a=1 rows go away; (1, 30) keeps the key alive.
  ExpectOk(engine_.get(), context_.get(),
           "DELETE FROM fk_repeat_parent WHERE tag < 30");
  // Vacating the last row that supplies a=1 still rejects.
  ExpectViolation(engine_.get(), context_.get(),
                  "DELETE FROM fk_repeat_parent WHERE tag = 30");
  EXPECT_EQ(
      RunSql(engine_.get(), context_.get(), "SELECT a FROM fk_repeat_parent"),
      (std::vector<Row>{Row({Value(1)})}));
}

TEST_F(ForeignKeyTest, UpdateReferencedParentKeyRejected) {
  ASSERT_NO_THROW(MakeParentChild());
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_child VALUES (10, 1)");
  ExpectViolation(engine_.get(), context_.get(),
                  "UPDATE fk_parent SET id = 7 WHERE id = 1");
  ExpectOk(engine_.get(), context_.get(),
           "UPDATE fk_parent SET id = 8 WHERE id = 2");
}

TEST_F(ForeignKeyTest, UpdateKeySwapKeepsReferencedKeysAlive) {
  // A key the statement itself re-supplies is not vacated: swapping the
  // referenced values between two parent rows (1->2, 2->1) leaves both
  // present at statement end, so the child row referencing 1 must not
  // reject the statement. The check used to emit every old key of a changed
  // row as a referenced candidate without subtracting the new keys.
  ASSERT_NO_THROW(MakeParentChild());
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_child VALUES (10, 1)");
  ExpectOk(engine_.get(), context_.get(),
           "UPDATE fk_parent SET id = CASE id WHEN 1 THEN 2 ELSE 1 END");
  const std::vector<Row> rows =
      RunSql(engine_.get(), context_.get(), "SELECT id FROM fk_parent");
  ASSERT_EQ(rows.size(), 2U);

  // Vacating a key the statement does not re-supply still rejects.
  ExpectViolation(engine_.get(), context_.get(),
                  "UPDATE fk_parent SET id = 9 WHERE id = 1");
}

TEST_F(ForeignKeyTest, UpdateKeepsReferencesToUntouchedDuplicateParentKeys) {
  ExpectOk(engine_.get(), context_.get(),
           "CREATE TABLE fk_repeat_parent (a INT64, tag INT64)");
  ExpectOk(
      engine_.get(), context_.get(),
      "CREATE TABLE fk_repeat_child (a INT64 REFERENCES fk_repeat_parent(a))");
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_repeat_parent VALUES (1, 10), (1, 20), (1, 30)");
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_repeat_child VALUES (1)");
  ExpectOk(engine_.get(), context_.get(),
           "UPDATE fk_repeat_parent SET a = 99 WHERE tag < 30");
  ExpectViolation(engine_.get(), context_.get(),
                  "UPDATE fk_repeat_parent SET a = 99 WHERE tag = 30");
  EXPECT_EQ(
      RunSql(engine_.get(), context_.get(),
             "SELECT a FROM fk_repeat_parent ORDER BY tag"),
      (std::vector<Row>{Row({Value(99)}), Row({Value(99)}), Row({Value(1)})}));
}

TEST_F(ForeignKeyTest, UpdateSelfReferenceCanSupplyNewParentKey) {
  ExpectOk(engine_.get(), context_.get(),
           "CREATE TABLE self_rekey (id INT64, parent INT64, "
           "FOREIGN KEY (parent) REFERENCES self_rekey(id))");
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO self_rekey VALUES (1, NULL)");
  ExpectOk(engine_.get(), context_.get(),
           "UPDATE self_rekey SET id = 5, parent = 5 WHERE id = 1");
  ExpectViolation(engine_.get(), context_.get(),
                  "UPDATE self_rekey SET id = 6, parent = 5 WHERE id = 5");
}

TEST_F(ForeignKeyTest, UpdateSelfReferenceUsesPostImages) {
  // Self-referencing NO ACTION: the probe skips the statement's own
  // pre-images, and a rewritten row violates only when its POST-image still
  // references a vacated key.
  ExpectOk(engine_.get(), context_.get(),
           "CREATE TABLE fk_emp (id INT64, mgr INT64, FOREIGN KEY (mgr) "
           "REFERENCES fk_emp(id))");
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_emp VALUES (1, NULL), (2, 1)");
  // Re-key a row while dropping its reference: the pre-image reference to
  // the vacated key must not reject.
  ExpectOk(engine_.get(), context_.get(),
           "UPDATE fk_emp SET id = 29, mgr = NULL WHERE id = 2");
  // Re-key both rows of a self-edge in one statement; the row that keeps
  // referencing the vacated key 1 rejects through its post-image.
  ExpectOk(engine_.get(), context_.get(), "INSERT INTO fk_emp VALUES (2, 1)");
  ExpectViolation(engine_.get(), context_.get(),
                  "UPDATE fk_emp SET id = CASE id WHEN 1 THEN 39 ELSE 49 END "
                  "WHERE id IN (1, 2)");
}

TEST_F(ForeignKeyTest, DropReferencedTableBlocked) {
  ASSERT_NO_THROW(MakeParentChild());
  const std::string error =
      ExpectThrow(engine_.get(), context_.get(), "DROP TABLE fk_parent");
  EXPECT_NE(error.find("FOREIGN KEY"), std::string::npos) << error;
  ExpectOk(engine_.get(), context_.get(), "DROP TABLE fk_child");
  ExpectOk(engine_.get(), context_.get(), "DROP TABLE fk_parent");
}

TEST_F(ForeignKeyTest, MissingParentTableRejected) {
  const std::string error = ExpectThrow(
      engine_.get(), context_.get(),
      "CREATE TABLE fk_orphan (id INT64, pid INT64, FOREIGN KEY (pid) "
      "REFERENCES fk_missing(id))");
  EXPECT_NE(error.find("fk_missing"), std::string::npos) << error;
}

TEST_F(ForeignKeyTest, MissingParentColumnRejected) {
  ExpectOk(engine_.get(), context_.get(), "CREATE TABLE fk_p3 (id INT64)");
  const std::string error =
      ExpectThrow(engine_.get(), context_.get(),
                  "CREATE TABLE fk_c3 (id INT64, pid INT64, FOREIGN KEY (pid) "
                  "REFERENCES fk_p3(nope))");
  EXPECT_NE(error.find("nope"), std::string::npos) << error;
}

TEST_F(ForeignKeyTest, TypeMismatchRejected) {
  ExpectOk(engine_.get(), context_.get(), "CREATE TABLE fk_p4 (id VARCHAR(8))");
  const std::string error =
      ExpectThrow(engine_.get(), context_.get(),
                  "CREATE TABLE fk_c4 (id INT64, pid INT64, FOREIGN KEY (pid) "
                  "REFERENCES fk_p4(id))");
  EXPECT_NE(error.find("type mismatch"), std::string::npos) << error;
}

TEST_F(ForeignKeyTest, UnsupportedActionsRejected) {
  ExpectOk(engine_.get(), context_.get(), "CREATE TABLE fk_p5 (id INT64)");
  EXPECT_NE(ExpectThrow(engine_.get(), context_.get(),
                        "CREATE TABLE fk_c5 (id INT64, pid INT64, FOREIGN KEY "
                        "(pid) REFERENCES fk_p5(id) ON DELETE CASCADE)")
                .find("NO ACTION"),
            std::string::npos);
  ExpectOk(engine_.get(), context_.get(),
           "CREATE TABLE fk_p6 (x INT64, y INT64)");
  EXPECT_NE(ExpectThrow(engine_.get(), context_.get(),
                        "CREATE TABLE fk_c6 (a INT64, b INT64, FOREIGN KEY "
                        "(a, b) REFERENCES fk_p6(x, y))")
                .find("multi-column"),
            std::string::npos);
}

TEST_F(ForeignKeyTest, SelfReferenceEnforced) {
  ExpectOk(engine_.get(), context_.get(),
           "CREATE TABLE fk_emp (id INT64, mgr INT64, FOREIGN KEY (mgr) "
           "REFERENCES fk_emp(id))");
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_emp VALUES (1, NULL), (2, 1)");
  ExpectViolation(engine_.get(), context_.get(),
                  "INSERT INTO fk_emp VALUES (3, 9)");
  ExpectViolation(engine_.get(), context_.get(),
                  "DELETE FROM fk_emp WHERE id = 1");
}

TEST_F(ForeignKeyTest, ConstraintSurvivesCatalogRoundTrip) {
  ASSERT_NO_THROW(MakeParentChild());
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_child VALUES (10, 1)");
  ASSERT_EQ(context_->PreCommit(), Status::kSuccess);
  engine_.reset();
  context_.reset();
  database_.reset();
  database_ = Database::Create(db_name_).MoveValue();
  context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  engine_ = std::make_unique<SqlEngine>(*database_);
  // The reloaded schema still carries the edge: violations keep failing.
  ExpectViolation(engine_.get(), context_.get(),
                  "INSERT INTO fk_child VALUES (11, 9)");
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_child VALUES (11, 2)");
}

TEST_F(ForeignKeyTest,
       RejectsCombiningForeignKeyWithDefaultOrPrimaryKeyOnSameColumn) {
  ExpectOk(engine_.get(), context_.get(),
           "CREATE TABLE fk_base (id BIGINT PRIMARY KEY)");
  // Combining DEFAULT with FOREIGN KEY on the same column would overwrite the
  // single Column::constraint_ slot; must be rejected at DDL time.
  const std::string err_default =
      ExpectThrow(engine_.get(), context_.get(),
                  "CREATE TABLE fk_bad_def (id BIGINT PRIMARY KEY, "
                  "pid BIGINT DEFAULT 1 REFERENCES fk_base(id))");
  EXPECT_FALSE(err_default.empty());

  // Combining PRIMARY KEY with FOREIGN KEY on the same column must also be
  // rejected instead of silently dropping PRIMARY KEY / NOT NULL semantics.
  const std::string err_pk =
      ExpectThrow(engine_.get(), context_.get(),
                  "CREATE TABLE fk_bad_pk ("
                  "pid BIGINT PRIMARY KEY REFERENCES fk_base(id))");
  EXPECT_FALSE(err_pk.empty());
}

TEST_F(ForeignKeyTest, RejectsCombiningForeignKeyWithNotNullOnSameColumn) {
  ExpectOk(engine_.get(), context_.get(),
           "CREATE TABLE fk_base (id BIGINT PRIMARY KEY)");
  // NOT NULL cannot share the single Column::constraint_ slot with FOREIGN
  // KEY.  Rejecting the combination explicitly prevents the NOT NULL slot
  // from being silently overwritten by kForeign (which would let NULL rows
  // through Table::CheckRowConstraints).
  for (const std::string& sql :
       {"CREATE TABLE fk_nn_col (pid BIGINT NOT NULL REFERENCES fk_base(id))",
        "CREATE TABLE fk_nn_tbl (pid BIGINT NOT NULL, "
        "FOREIGN KEY (pid) REFERENCES fk_base(id))"}) {
    const std::string err = ExpectThrow(engine_.get(), context_.get(), sql);
    EXPECT_FALSE(err.empty()) << sql;
    EXPECT_NE(err.find("cannot combine"), std::string::npos)
        << sql << " :: " << err;
  }
}

TEST_F(ForeignKeyTest,
       SpillsLargeParentKeysSelfReferenceAndDeletesViaExternalSorter) {
  struct KnobGuard {
    KnobGuard() {
      ForeignKeyCounts::SetBudgetBytesForTest(512);
      ProcessMemoryBudget::Global().SetTestAlwaysSpill(true);
    }
    KnobGuard(const KnobGuard&) = delete;
    KnobGuard& operator=(const KnobGuard&) = delete;
    KnobGuard(KnobGuard&&) = delete;
    KnobGuard& operator=(KnobGuard&&) = delete;
    ~KnobGuard() {
      ForeignKeyCounts::SetBudgetBytesForTest(0);
      ProcessMemoryBudget::Global().ResetTestKnobs();
    }
  } guard;

  ExpectOk(engine_.get(), context_.get(),
           "CREATE TABLE fk_big_parent (id INT64, pad VARCHAR(128))");
  ExpectOk(engine_.get(), context_.get(),
           "CREATE TABLE fk_big_child (id INT64, pid INT64, "
           "FOREIGN KEY (pid) REFERENCES fk_big_parent(id))");
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_big_parent "
           "SELECT value, REPEAT('p', 64) FROM GENERATE_SERIES(1, 150)");

  // Valid child insert (including NULL) and out-of-range violation under spill.
  ExpectOk(
      engine_.get(), context_.get(),
      "INSERT INTO fk_big_child "
      "SELECT value, CASE WHEN MOD(value, 10) = 0 THEN NULL ELSE value END "
      "FROM GENERATE_SERIES(1, 80)");
  ExpectViolation(engine_.get(), context_.get(),
                  "INSERT INTO fk_big_child VALUES (999, 9999)");

  // Update child key to another valid spilled parent key, then reject invalid.
  ExpectOk(engine_.get(), context_.get(),
           "UPDATE fk_big_child SET pid = 140 WHERE id = 10");
  ExpectViolation(engine_.get(), context_.get(),
                  "UPDATE fk_big_child SET pid = 9999 WHERE id = 11");

  // Batch delete unreferenced parents (> 80 except 140) succeeds via OOC merge.
  ExpectOk(engine_.get(), context_.get(),
           "DELETE FROM fk_big_parent WHERE id > 80 AND id != 140");
  // Batch delete overlapping referenced parent 140 rejects.
  ExpectViolation(engine_.get(), context_.get(),
                  "DELETE FROM fk_big_parent WHERE id >= 135");

  // Self-reference bulk insert & statement-end delete removing parent+children.
  ExpectOk(engine_.get(), context_.get(),
           "CREATE TABLE fk_self_ooc (id INT64, mgr INT64, "
           "FOREIGN KEY (mgr) REFERENCES fk_self_ooc(id))");
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_self_ooc "
           "SELECT value, CASE WHEN value = 1 THEN NULL ELSE value - 1 END "
           "FROM GENERATE_SERIES(1, 40)");
  ExpectViolation(engine_.get(), context_.get(),
                  "DELETE FROM fk_self_ooc WHERE id <= 10");
  ExpectOk(engine_.get(), context_.get(),
           "DELETE FROM fk_self_ooc WHERE id >= 1");
}

TEST_F(ForeignKeyTest,
       DirectForeignKeyCountsDeltaRebuildAndSpillIoErrorPropagation) {
  struct KnobGuard {
    KnobGuard() { ForeignKeyCounts::SetBudgetBytesForTest(256); }
    KnobGuard(const KnobGuard&) = delete;
    KnobGuard& operator=(const KnobGuard&) = delete;
    KnobGuard(KnobGuard&&) = delete;
    KnobGuard& operator=(KnobGuard&&) = delete;
    ~KnobGuard() {
      ForeignKeyCounts::SetBudgetBytesForTest(0);
      ProcessMemoryBudget::Global().ResetTestKnobs();
    }
  } guard;

  ExpectOk(engine_.get(), context_.get(),
           "CREATE TABLE fk_dir_parent (id INT64)");
  ExpectOk(engine_.get(), context_.get(),
           "CREATE TABLE fk_dir_child (id INT64, pid INT64)");
  ExpectOk(
      engine_.get(), context_.get(),
      "INSERT INTO fk_dir_parent SELECT value FROM GENERATE_SERIES(1, 80)");
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_dir_child SELECT value, value FROM "
           "GENERATE_SERIES(1, 20)");

  auto parent_tbl = context_->GetTable("fk_dir_parent");
  ASSERT_EQ(parent_tbl.GetStatus(), Status::kSuccess);
  auto child_tbl = context_->GetTable("fk_dir_child");
  ASSERT_EQ(child_tbl.GetStatus(), Status::kSuccess);

  StatusOr<ForeignKeyCounts> built =
      BuildForeignKeyCounts(*parent_tbl.Value(), context_->txn_, 0);
  ASSERT_EQ(built.GetStatus(), Status::kSuccess);
  ForeignKeyCounts counts = built.MoveValue();
  EXPECT_TRUE(counts.Spilled());

  // Existing keys and missing keys under spilled 16 KiB segment lookup.
  EXPECT_EQ(counts.Contains(Value(int64_t{1})).Value(), true);
  EXPECT_EQ(counts.Contains(Value(int64_t{80})).Value(), true);
  EXPECT_EQ(counts.Contains(Value(int64_t{81})).Value(), false);
  EXPECT_EQ(IsForeignKeyViolation(counts, Value()).Value(), false);

  // Decrement key 50 -> 0, increment 1000..1120 -> forces RebuildWithDeltas.
  ASSERT_EQ(counts.DecrementKey(Value(int64_t{50})), Status::kSuccess);
  EXPECT_EQ(counts.Contains(Value(int64_t{50})).Value(), false);
  for (int64_t k = 1000; k < 1120; ++k) {
    ASSERT_EQ(counts.IncrementKey(Value(k)), Status::kSuccess);
  }
  EXPECT_EQ(counts.Contains(Value(int64_t{50})).Value(), false);
  EXPECT_EQ(counts.Contains(Value(int64_t{1119})).Value(), true);
  EXPECT_EQ(counts.Contains(Value(int64_t{49})).Value(), true);

  // Injected spill read I/O failure propagates as Status instead of silent
  // bool.
  ProcessMemoryBudget::Global().SetTestSpillIoFailCountdown(1);
  auto read_fail = counts.Contains(Value(int64_t{10}));
  EXPECT_NE(read_fail.GetStatus(), Status::kSuccess);
  ProcessMemoryBudget::Global().SetTestSpillIoFailCountdown(0);

  // Direct FindReferencedParentKey respects ignore positions under AlwaysSpill.
  ProcessMemoryBudget::Global().SetTestAlwaysSpill(true);
  SpillableRowPositionList child_positions;
  auto it = child_tbl.Value()->BeginFullScan(context_->txn_);
  for (; it.IsValid(); ++it) {
    ASSERT_EQ(child_positions.PushBack(it.Position()), Status::kSuccess);
  }
  ASSERT_EQ(it.GetStatus(), Status::kSuccess);
  EXPECT_TRUE(child_positions.Spilled());

  auto unignored = FindReferencedParentKey(
      *child_tbl.Value(), context_->txn_, 1,
      [&](const std::function<Status(const Value&)>& emit) -> Status {
        return emit(Value(int64_t{15}));
      },
      [&](const std::function<Status(const RowPosition&)>&) -> Status {
        return Status::kSuccess;
      });
  ASSERT_EQ(unignored.GetStatus(), Status::kSuccess);
  EXPECT_TRUE(unignored.Value().has_value());

  auto ignored_all = FindReferencedParentKey(
      *child_tbl.Value(), context_->txn_, 1,
      [&](const std::function<Status(const Value&)>& emit) -> Status {
        return emit(Value(int64_t{15}));
      },
      [&](const std::function<Status(const RowPosition&)>& emit) -> Status {
        return child_positions.ForEach(emit);
      });
  ASSERT_EQ(ignored_all.GetStatus(), Status::kSuccess);
  EXPECT_FALSE(ignored_all.Value().has_value());
}

}  // namespace tinylamb
