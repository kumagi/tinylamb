/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "common/test_util.hpp"
#include "common/platform/process.hpp"
#include "executor/executor_test.hpp"

namespace tinylamb {
TEST_F(ExecutorTest, Construct) {
  // Arrange -- nothing to set up; default database created by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- nothing to verify; gtest death on crash, gtest green on pass
}

TEST_F(ExecutorTest, FullScan) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  FullScan fs(ctx.txn_, *tbl);
  std::unordered_set rows({Row({Value(0), Value("hello"), Value(1.2)}),
                           Row({Value(3), Value("piyo"), Value(12.2)}),
                           Row({Value(1), Value("world"), Value(4.9)}),
                           Row({Value(2), Value("arise"), Value(4.14)})});
  DumpLog(fs);
  Row got;
  RowPosition pos;

  // Act -- iterate FullScan cursor through all rows
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_NE(rows.find(got), rows.end());
  rows.erase(got);
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_NE(rows.find(got), rows.end());
  rows.erase(got);
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_NE(rows.find(got), rows.end());
  rows.erase(got);
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_NE(rows.find(got), rows.end());
  rows.erase(got);

  // Assert -- cursor exhausted and all rows consumed
  ASSERT_TRUE(rows.empty());
  ASSERT_FALSE(fs.Next(&got, &pos));
}

TEST_F(ExecutorTest, IndexScan) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  ASSERT_EQ(tbl->IndexCount(), 2);
  const Schema sc = tbl->GetSchema();
  IndexScan fs(ctx.txn_, *tbl, tbl->GetIndex(0), Value("he"), Value("q"), true,
               BinaryExpressionExp(ColumnValueExp("score"),
                                   BinaryOperation::kGreaterThan,
                                   ConstantValueExp(Value(10.0))),
               sc);
  Row target({Value(3), Value("piyo"), Value(12.2)});
  DumpLog(fs);
  Row got;
  RowPosition pos;

  // Act -- advance IndexScan cursor to first match
  ASSERT_TRUE(fs.Next(&got, &pos));
  LOG(INFO) << got;

  // Assert -- matched row equals target and cursor exhausts after first match
  ASSERT_EQ(got, target);
  ASSERT_FALSE(fs.Next(&got, &pos));
}

TEST_F(ExecutorTest, IndexScanRejectedPredicateReleasesWriteIntent) {
  TransactionContext scan_context = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, scan_table,
                              scan_context.GetTable(kTableName));
  Row row;
  {
    IndexScan scan(scan_context.txn_, *scan_table, scan_table->GetIndex(0),
                   Value(), Value(), true,
                   BinaryExpressionExp(ColumnValueExp("score"),
                                       BinaryOperation::kGreaterThan,
                                       ConstantValueExp(Value(100.0))),
                   scan_table->GetSchema(), true);
    while (scan.Next(&row, nullptr)) {
    }
    ASSERT_EQ(scan.GetStatus(), Status::kSuccess);
  }

  TransactionContext find_context = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, find_table,
                              find_context.GetTable(kTableName));
  RowPosition candidate;
  FullScan find(find_context.txn_, *find_table);
  ASSERT_TRUE(find.Next(&row, &candidate));

  TransactionContext contender = rs_->BeginContext();
  EXPECT_TRUE(contender.txn_.TryAddWriteSet(candidate));
  EXPECT_SUCCESS(contender.Abort());
  EXPECT_EQ(tinylamb::Status::kSuccess, find_context.Abort());
  EXPECT_EQ(tinylamb::Status::kSuccess, scan_context.Abort());
}

TEST_F(ExecutorTest, IndexOnlyScan) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  ASSERT_EQ(tbl->IndexCount(), 2);
  const Schema sc = tbl->GetSchema();
  IndexOnlyScan fs(ctx.txn_, *tbl, tbl->GetIndex(0), Value("he"), Value("q"),
                   true,
                   BinaryExpressionExp(ColumnValueExp("score"),
                                       BinaryOperation::kGreaterThan,
                                       ConstantValueExp(Value(10.0))),
                   sc);
  Row expected({Value("piyo"), Value(12.2)});
  DumpLog(fs);
  Row got;
  RowPosition pos;

  // Act -- advance IndexOnlyScan cursor to first match
  ASSERT_TRUE(fs.Next(&got, &pos));
  LOG(INFO) << got;

  // Assert -- matched projected row equals expected and cursor exhausts
  ASSERT_EQ(got, expected);
  ASSERT_FALSE(fs.Next(&got, &pos));
}

TEST_F(ExecutorTest, IndexOnlyFullScan) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  ASSERT_EQ(tbl->IndexCount(), 2);
  const Schema sc = tbl->GetSchema();
  IndexOnlyScan fs(ctx.txn_, *tbl, tbl->GetIndex(0), Value(), Value(), true,
                   BinaryExpressionExp(ColumnValueExp("score"),
                                       BinaryOperation::kGreaterThan,
                                       ConstantValueExp(Value(1.0))),
                   sc);
  DumpLog(fs);
  Row got;
  RowPosition pos;

  // Act -- iterate IndexOnlyScan cursor through all projected rows
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_EQ(got, Row({Value("arise"), Value(4.14)}));
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_EQ(got, Row({Value("hello"), Value(1.20)}));
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_EQ(got, Row({Value("piyo"), Value(12.2)}));
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_EQ(got, Row({Value("world"), Value(4.9)}));

  // Assert -- cursor exhausted after all projected rows consumed
  ASSERT_FALSE(fs.Next(&got, &pos));
}

TEST_F(ExecutorTest, IndexOnlyScanAndBitmapScanMvccSnapshotVisibility) {
  Schema mvcc_schema("MvccTable", {Column("id", ValueType::kInt64),
                                   Column("name", ValueType::kVarChar),
                                   Column("val", ValueType::kDouble)});
  TransactionContext init_ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->CreateTable(init_ctx, mvcc_schema));
  ASSERT_SUCCESS(
      tbl.Insert(init_ctx.txn_, Row({Value(1), Value("one"), Value(1.1)}))
          .GetStatus());
  ASSERT_SUCCESS(
      tbl.Insert(init_ctx.txn_, Row({Value(2), Value("two"), Value(2.2)}))
          .GetStatus());
  ASSERT_SUCCESS(
      tbl.Insert(init_ctx.txn_, Row({Value(3), Value("three"), Value(3.3)}))
          .GetStatus());
  ASSERT_SUCCESS(rs_->CreateIndex(
      init_ctx, "MvccTable",
      IndexSchema("IdxMvcc", {0}, {1, 2}, IndexMode::kVersionedUnique)));
  ASSERT_SUCCESS(init_ctx.txn_.PreCommit());

  TransactionContext reader_ctx = rs_->BeginReadOnlyContext();

  TransactionContext writer_ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, w_tbl, rs_->GetTable(writer_ctx, "MvccTable"));
  Row found_row;
  RowPosition pos1;
  {
    IndexScan find_row(writer_ctx.txn_, w_tbl, w_tbl.GetIndex(0), Value(1),
                       Value(1), true, Expression(), mvcc_schema);
    ASSERT_TRUE(find_row.Next(&found_row, &pos1));
  }
  ASSERT_SUCCESS(w_tbl
                     .Update(writer_ctx.txn_, pos1,
                             Row({Value(1), Value("one_updated"), Value(99.9)}))
                     .GetStatus());

  RowPosition pos2;
  {
    IndexScan find_row2(writer_ctx.txn_, w_tbl, w_tbl.GetIndex(0), Value(2),
                        Value(2), true, Expression(), mvcc_schema);
    ASSERT_TRUE(find_row2.Next(&found_row, &pos2));
  }
  ASSERT_SUCCESS(w_tbl.Delete(writer_ctx.txn_, pos2));

  ASSERT_SUCCESS(
      w_tbl.Insert(writer_ctx.txn_, Row({Value(4), Value("four"), Value(4.4)}))
          .GetStatus());
  ASSERT_SUCCESS(writer_ctx.txn_.PreCommit());

  ASSIGN_OR_ASSERT_FAIL(Table, r_tbl, rs_->GetTable(reader_ctx, "MvccTable"));
  IndexOnlyScan ios(reader_ctx.txn_, r_tbl, r_tbl.GetIndex(0), Value(), Value(),
                    true, Expression(), mvcc_schema);
  std::vector<Row> ios_rows;
  Row r;
  while (ios.Next(&r, nullptr)) {
    ios_rows.push_back(r);
  }
  ASSERT_EQ(ios_rows.size(), 3U);
  EXPECT_EQ(ios_rows[0][0], Value(1));
  EXPECT_EQ(ios_rows[0][1], Value("one"));
  EXPECT_EQ(ios_rows[0][2], Value(1.1));
  EXPECT_EQ(ios_rows[1][0], Value(2));
  EXPECT_EQ(ios_rows[1][1], Value("two"));
  EXPECT_EQ(ios_rows[2][0], Value(3));
  EXPECT_EQ(ios_rows[2][1], Value("three"));

  BitmapIndexScan bis(reader_ctx.txn_, r_tbl, r_tbl.GetIndex(0), Value(),
                      Value());
  ASSIGN_OR_ASSERT_FAIL(std::vector<RowPosition>, positions,
                        bis.ScanPositions());
  BitmapHeapScan bhs(reader_ctx.txn_, r_tbl, std::move(positions), Expression(),
                     mvcc_schema);
  std::vector<Row> bhs_rows;
  while (bhs.Next(&r, nullptr)) {
    bhs_rows.push_back(r);
  }
  ASSERT_EQ(bhs_rows.size(), 3U);
  std::ranges::sort(bhs_rows, [](const Row& a, const Row& b) {
    return a[0].value.int_value < b[0].value.int_value;
  });
  EXPECT_EQ(bhs_rows[0][0], Value(1));
  EXPECT_EQ(bhs_rows[0][1], Value("one"));
  EXPECT_EQ(bhs_rows[0][2], Value(1.1));
  EXPECT_EQ(bhs_rows[1][0], Value(2));
  EXPECT_EQ(bhs_rows[1][1], Value("two"));
  EXPECT_EQ(bhs_rows[2][0], Value(3));
  EXPECT_EQ(bhs_rows[2][1], Value("three"));

  ASSERT_SUCCESS(reader_ctx.txn_.PreCommit());

  TransactionContext fresh_ctx = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(Table, f_tbl, rs_->GetTable(fresh_ctx, "MvccTable"));
  IndexOnlyScan fresh_ios(fresh_ctx.txn_, f_tbl, f_tbl.GetIndex(0), Value(),
                          Value(), true, Expression(), mvcc_schema);
  std::vector<Row> fresh_ios_rows;
  while (fresh_ios.Next(&r, nullptr)) {
    fresh_ios_rows.push_back(r);
  }
  ASSERT_EQ(fresh_ios_rows.size(), 3U);
  EXPECT_EQ(fresh_ios_rows[0][0], Value(1));
  EXPECT_EQ(fresh_ios_rows[0][1], Value("one_updated"));
  EXPECT_EQ(fresh_ios_rows[0][2], Value(99.9));
  EXPECT_EQ(fresh_ios_rows[1][0], Value(3));
  EXPECT_EQ(fresh_ios_rows[2][0], Value(4));

  BitmapIndexScan fresh_bis(fresh_ctx.txn_, f_tbl, f_tbl.GetIndex(0), Value(),
                            Value());
  ASSIGN_OR_ASSERT_FAIL(std::vector<RowPosition>, fresh_positions,
                        fresh_bis.ScanPositions());
  BitmapHeapScan fresh_bhs(fresh_ctx.txn_, f_tbl, std::move(fresh_positions),
                           Expression(), mvcc_schema);
  std::vector<Row> fresh_bhs_rows;
  while (fresh_bhs.Next(&r, nullptr)) {
    fresh_bhs_rows.push_back(r);
  }
  ASSERT_EQ(fresh_bhs_rows.size(), 3U);
  std::ranges::sort(fresh_bhs_rows, [](const Row& a, const Row& b) {
    return a[0].value.int_value < b[0].value.int_value;
  });
  EXPECT_EQ(fresh_bhs_rows[0][0], Value(1));
  EXPECT_EQ(fresh_bhs_rows[0][1], Value("one_updated"));
  EXPECT_EQ(fresh_bhs_rows[0][2], Value(99.9));
  EXPECT_EQ(fresh_bhs_rows[1][0], Value(3));
  EXPECT_EQ(fresh_bhs_rows[2][0], Value(4));
  ASSERT_SUCCESS(fresh_ctx.txn_.PreCommit());
}

TEST_F(ExecutorTest, IndexOnlyDistinctKeysBatchesReverseAndResidualGate) {
  {
    TransactionContext writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, table,
        rs_->CreateTable(
            writer, Schema("DistinctScan", {Column("k", ValueType::kInt64),
                                            Column("v", ValueType::kInt64)})));
    for (int i = 0; i < 1000; ++i) {
      ASSERT_SUCCESS(
          table
              .Insert(writer.txn_,
                      Row({i % 6 == 5 ? Value() : Value(i % 6), Value(i)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(
        rs_->CreateIndex(writer, "DistinctScan",
                         IndexSchema("keys", {0}, {}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(rs_->CreateIndex(
        writer, "DistinctScan",
        IndexSchema("included", {0}, {1}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(writer.PreCommit());
  }
  for (bool ascending : {true, false}) {
    TransactionContext reader = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->GetTable(reader, "DistinctScan"));
    {
      IndexOnlyScan scan(reader.txn_, table, table.GetIndex(0), Value(),
                         Value(), ascending, Expression(), table.GetSchema());
      ASSERT_TRUE(scan.TryEnableKeyDeduplication());
      std::vector<Row> actual;
      DataChunk batch;
      while (scan.NextBatch(&batch, 2) != 0) {
        for (size_t i = 0; i < batch.Size(); ++i) {
          actual.push_back(batch.RowAt(i));
        }
      }
      std::vector<Row> expected{Row({Value()}),  Row({Value(0)}),
                                Row({Value(1)}), Row({Value(2)}),
                                Row({Value(3)}), Row({Value(4)})};
      if (!ascending) {
        std::ranges::reverse(expected);
      }
      EXPECT_EQ(actual, expected);
      Row row;
      EXPECT_FALSE(scan.Next(&row, nullptr));
      ASSERT_SUCCESS(scan.GetStatus());
    }
    {
      IndexOnlyScan included(reader.txn_, table, table.GetIndex(1), Value(),
                             Value(), ascending, Expression(),
                             table.GetSchema());
      EXPECT_FALSE(included.TryEnableKeyDeduplication());
    }
    {
      IndexOnlyScan residual(reader.txn_, table, table.GetIndex(0), Value(),
                             Value(), ascending,
                             BinaryExpressionExp(ColumnValueExp("k"),
                                                 BinaryOperation::kGreaterThan,
                                                 ConstantValueExp(Value(2))),
                             table.GetSchema());
      EXPECT_FALSE(residual.TryEnableKeyDeduplication());
    }
  }
}

TEST_F(ExecutorTest, IndexOnlyResidualFailureDiscardsPartialBatch) {
  auto seed = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      rs_->CreateTable(seed, Schema("IndexResidualError",
                                    {Column("k", ValueType::kInt64)})));
  ASSERT_SUCCESS(table.Insert(seed.txn_, Row({Value(1)})).GetStatus());
  ASSERT_SUCCESS(table.Insert(seed.txn_, Row({Value(2)})).GetStatus());
  ASSERT_SUCCESS(rs_->CreateIndex(seed, "IndexResidualError",
                                  IndexSchema("i", {0}, {})));
  ASSERT_SUCCESS(seed.PreCommit());

  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(Table, current,
                        rs_->GetTable(reader, "IndexResidualError"));
  const Expression divisor = BinaryExpressionExp(
      ConstantValueExp(Value(2)), BinaryOperation::kSubtract, ColumnValueExp("k"));
  const Expression predicate = BinaryExpressionExp(
      ConstantValueExp(Value(1)), BinaryOperation::kDivide, divisor);
  IndexOnlyScan scan(reader.txn_, current, current.GetIndex(0), Value(),
                     Value(), true, predicate, current.GetSchema());
  DataChunk output;
  EXPECT_EQ(scan.NextBatch(&output), 0U);
  EXPECT_EQ(output.Size(), 0U);
  const Status status = scan.GetStatus();
  ASSERT_FALSE(status.ok());
  EXPECT_NE(status.GetMessage().find("division by zero"), std::string::npos);
  EXPECT_EQ(scan.NextBatch(&output), 0U);
  EXPECT_EQ(scan.GetStatus(), status);
}

TEST_F(ExecutorTest, IndexOnlyKeyPredicatesKeepMultiplicityAndIncludeVersions) {
  auto seed = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      rs_->CreateTable(
          seed, Schema("PredicateKeys", {Column("k", ValueType::kInt64),
                                         Column("v", ValueType::kInt64)})));
  std::vector<Row> original;
  original.reserve(100);
  RowPosition updated;
  for (int i = 0; i < 100; ++i) {
    original.emplace_back(
        Row({((i % 10) != 0) ? Value(i % 4) : Value(), Value(i)}));
    ASSIGN_OR_ASSERT_FAIL(RowPosition, position,
                          table.Insert(seed.txn_, original.back()));
    if (i == 1) {
      updated = position;
    }
  }
  ASSERT_SUCCESS(
      rs_->CreateIndex(seed, "PredicateKeys",
                       IndexSchema("kv", {0}, {1}, IndexMode::kNonUnique)));
  ASSERT_SUCCESS(seed.PreCommit());
  auto old = rs_->BeginReadOnlyContext();
  {
    auto writer = rs_->BeginContext();
    ASSERT_SUCCESS(
        table.Update(writer.txn_, updated, Row({Value(1), Value(998)}))
            .GetStatus());
    ASSERT_SUCCESS(writer.PreCommit());
  }
  const auto compare = [](const std::string& name, BinaryOperation op,
                          const Value& value) {
    return BinaryExpressionExp(ColumnValueExp(name), op,
                               ConstantValueExp(value));
  };
  const std::vector<Expression> predicates{
      BinaryExpressionExp(
          compare("k", BinaryOperation::kGreaterThanEquals, Value(1)),
          BinaryOperation::kAnd,
          compare("k", BinaryOperation::kLessThan, Value(3))),
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kModulo,
                              ConstantValueExp(Value(2))),
          BinaryOperation::kEquals, ConstantValueExp(Value(0))),
      UnaryExpressionExp(ColumnValueExp("k"), UnaryOperation::kIsNull),
      BinaryExpressionExp(FunctionCallExp("abs", {ColumnValueExp("v")}),
                          BinaryOperation::kLessThan,
                          ConstantValueExp(Value(30)))};
  for (int phase = 0; phase < 2; ++phase) {
    auto fresh = rs_->BeginReadOnlyContext();
    auto& context = phase == 0 ? old : fresh;
    ASSIGN_OR_ASSERT_FAIL(Table, current,
                          rs_->GetTable(context, "PredicateKeys"));
    auto visible = original;
    if (phase == 1) {
      visible[1][1] = Value(998);
    }
    for (size_t predicate = 0; predicate < predicates.size(); ++predicate) {
      for (bool ascending : {true, false}) {
        std::vector<Row> expected;
        expected.reserve(visible.size());
        for (const auto& row : visible) {
          const auto value =
              predicates[predicate]->TryEvaluate(row, current.GetSchema());
          ASSERT_TRUE(value.HasValue());
          if (!value.Value().IsNull() && value.Value().Truthy()) {
            expected.push_back(row);
          }
        }
        IndexOnlyScan scan(context.txn_, current, current.GetIndex(0), Value(),
                           Value(), ascending, predicates[predicate],
                           current.GetSchema());
        std::vector<Row> actual;
        DataChunk batch;
        while (scan.NextBatch(&batch, 7) != 0U) {
          for (size_t i = 0; i < batch.Size(); ++i) {
            actual.push_back(batch.RowAt(i));
          }
        }
        ASSERT_SUCCESS(scan.GetStatus());
        const auto by_payload = [](const Row& a, const Row& b) {
          return a[1] < b[1];
        };
        std::sort(expected.begin(), expected.end(), by_payload);
        std::sort(actual.begin(), actual.end(), by_payload);
        EXPECT_EQ(actual, expected)
            << "predicate=" << predicate << " phase=" << phase;
      }
    }
    const auto raises = BinaryExpressionExp(
        BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kDivide,
                            ConstantValueExp(Value(0))),
        BinaryOperation::kGreaterThan, ConstantValueExp(Value(0)));
    const auto rejects = compare("k", BinaryOperation::kLessThan, Value(0));
    for (const auto& predicate :
         {BinaryExpressionExp(raises, BinaryOperation::kAnd, rejects),
          BinaryExpressionExp(rejects, BinaryOperation::kAnd, raises)}) {
      IndexOnlyScan scan(context.txn_, current, current.GetIndex(0), Value(),
                         Value(), true, predicate, current.GetSchema());
      Row row;
      EXPECT_FALSE(scan.Next(&row, nullptr));
      EXPECT_EQ(scan.GetStatus(), Status::kSuccess);
    }
    IndexOnlyScan erroneous(context.txn_, current, current.GetIndex(0), Value(),
                            Value(), true, raises, current.GetSchema());
    Row row;
    EXPECT_FALSE(erroneous.Next(&row, nullptr));
    EXPECT_NE(erroneous.GetStatus(), Status::kSuccess);
  }
}

TEST_F(ExecutorTest, IndexOnlyScanServesVisibilityMapWithoutHeapFetch) {
  // The fixture's committed rows are chain-free once the background GC has
  // dropped their version chains, so every index entry is served from the
  // index image (key + INCLUDE payload) without touching the heap.
  WaitForChainFreePages();
  TransactionContext ctx = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->GetTable(ctx, kTableName));
  const uint64_t serves_before =
      rs_->TransactionStats().index_only_visible_serves;

  // Act -- Idx1 (kUnique {1,2}, no include) delivers [name, score].
  IndexOnlyScan key_only(ctx.txn_, tbl, tbl.GetIndex(0), Value(), Value(), true,
                         Expression(), tbl.GetSchema());
  std::vector<Row> key_rows;
  Row row;
  while (key_only.Next(&row, nullptr)) {
    key_rows.push_back(row);
  }

  // Act -- Idx2 (kNonUnique {1} include {1,2}): the key column repeated in
  // INCLUDE is dropped when the schema is built, so it delivers
  // [name, score] like Idx1.
  IndexOnlyScan with_include(ctx.txn_, tbl, tbl.GetIndex(1), Value(), Value(),
                             true, Expression(), tbl.GetSchema());
  std::vector<Row> include_rows;
  while (with_include.Next(&row, nullptr)) {
    include_rows.push_back(row);
  }

  // Assert -- rows and INCLUDE payloads match the fixture, one serve each.
  ASSERT_EQ(key_rows.size(), 4U);
  EXPECT_EQ(key_rows[0], Row({Value("arise"), Value(4.14)}));
  EXPECT_EQ(key_rows[1], Row({Value("hello"), Value(1.2)}));
  EXPECT_EQ(key_rows[2], Row({Value("piyo"), Value(12.2)}));
  EXPECT_EQ(key_rows[3], Row({Value("world"), Value(4.9)}));
  ASSERT_EQ(include_rows.size(), 4U);
  EXPECT_EQ(include_rows[0], Row({Value("arise"), Value(4.14)}));
  EXPECT_EQ(include_rows[1], Row({Value("hello"), Value(1.2)}));
  EXPECT_EQ(include_rows[2], Row({Value("piyo"), Value(12.2)}));
  EXPECT_EQ(include_rows[3], Row({Value("world"), Value(4.9)}));
  EXPECT_EQ(rs_->TransactionStats().index_only_visible_serves,
            serves_before + 8);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(ExecutorTest, IndexScanPrefetchesHeapPagesAhead) {
  // IndexScan prefetch: a look-ahead cursor warms the heap pages the scan
  // is about to fetch, so the main path's fetch is a buffer-pool hit.  The
  // A/B hatch (TINYLAMB_DISABLE_INDEX_SCAN_PREFETCH) must change only the
  // warmed-page count, never the rows.
  const std::string kPrefetchTable = "PrefetchTable";
  {
    TransactionContext writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, tbl,
        rs_->CreateTable(
            writer,
            Schema(kPrefetchTable, {Column("key", ValueType::kInt64),
                                    Column("payload", ValueType::kInt64)})));
    for (int64_t i = 0; i < 64; ++i) {
      ASSERT_SUCCESS(
          tbl.Insert(writer.txn_, Row({Value(i), Value(i * 2)})).GetStatus());
    }
    ASSERT_SUCCESS(rs_->CreateIndex(writer, kPrefetchTable,
                                    IndexSchema("PrefetchIdx", {0})));
    ASSERT_SUCCESS(writer.PreCommit());
  }
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->GetTable(ctx, kPrefetchTable));
  const uint64_t warm_before =
      rs_->TransactionStats().index_scan_prefetch_pages;

  IndexScan scan(ctx.txn_, tbl, tbl.GetIndex(0), Value(int64_t{0}),
                 Value(int64_t{63}), true, Expression(), tbl.GetSchema());
  std::vector<Row> rows;
  Row row;
  while (scan.Next(&row, nullptr)) {
    rows.push_back(row);
  }
  const uint64_t warmed =
      rs_->TransactionStats().index_scan_prefetch_pages - warm_before;

  ASSERT_EQ(rows.size(), 64U);
  for (size_t i = 0; i < rows.size(); ++i) {
    EXPECT_EQ(rows[i], Row({Value(static_cast<int64_t>(i)),
                            Value(static_cast<int64_t>(i) * 2)}));
  }
  static const bool prefetch_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_INDEX_SCAN_PREFETCH") != nullptr;
  if (prefetch_disabled) {
    EXPECT_EQ(warmed, 0U);
  } else {
    EXPECT_GT(warmed, 0U);
  }
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(ExecutorTest, Projection) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  auto fs = std::make_shared<FullScan>(ctx.txn_, *tbl);
  Projection proj({NamedExpression("key"), NamedExpression("score")},
                  tbl->GetSchema(), std::move(fs));
  std::unordered_set rows(
      {Row({Value(0), Value(1.2)}), Row({Value(3), Value(12.2)}),
       Row({Value(1), Value(4.9)}), Row({Value(2), Value(4.14)})});
  DumpLog(proj);
  Row got;

  // Act -- iterate Projection cursor through all projected rows
  ASSERT_TRUE(proj.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(rows.erase(got));
  ASSERT_TRUE(proj.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(rows.erase(got));
  ASSERT_TRUE(proj.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(rows.erase(got));
  ASSERT_TRUE(proj.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(rows.erase(got));

  // Assert -- cursor exhausted and all projected rows consumed
  ASSERT_FALSE(proj.Next(&got, nullptr));
  ASSERT_TRUE(rows.empty());
}

TEST_F(ExecutorTest, Selection) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  Expression key_is_1 =
      BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(1)));
  Selection sel(key_is_1, tbl->GetSchema(),
                std::make_shared<FullScan>(ctx.txn_, *tbl));
  std::unordered_set rows({Row({Value(1), Value("world"), Value(4.9)})});
  DumpLog(sel);
  Row got;

  // Act -- advance Selection cursor to first matching row
  ASSERT_TRUE(sel.Next(&got, nullptr));

  // Assert -- matched row is in expected set and cursor exhausts after one
  // match
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_FALSE(sel.Next(&got, nullptr));
}

TEST_F(ExecutorTest, SelectionSkipsImpossibleZoneMapBatch) {
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> table_status =
      ctx.GetTable(kTableName);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();
  Selection selection(
      BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(1000))),
      table->GetSchema(), std::make_shared<FullScan>(ctx.txn_, *table));
  DataChunk output;
  EXPECT_EQ(selection.NextBatch(&output), 0U);
  EXPECT_EQ(selection.SkippedBatches(), 1U);
}

TEST_F(ExecutorTest, SelectionUsesJitForLargeIntegerBatches) {
  std::vector<Row> rows;
  rows.reserve(2048);
  for (int64_t value = 0; value < 2048; ++value) {
    rows.emplace_back(std::vector<Value>{Value(value)});
  }
  const Schema schema("jit", {Column("value", ValueType::kInt64)});
  Selection selection(
      BinaryExpressionExp(ColumnValueExp("value"),
                          BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(1000))),
      schema, std::make_shared<ConstantExecutor>(std::move(rows)), 1024);
  DataChunk output;
  size_t selected = 0;
  while (selection.NextBatch(&output) != 0) {
    selected += output.Size();
  }
  EXPECT_EQ(selected, 1047U);
  EXPECT_GE(selection.JitBatches(), 1U);
}

TEST_F(ExecutorTest, ProjectionUsesJitForLargeAffineIntegerBatches) {
  std::vector<Row> rows;
  rows.reserve(2048);
  for (int64_t value = 0; value < 2048; ++value) {
    rows.emplace_back(std::vector<Value>{Value(value)});
  }
  const Schema schema("jit", {Column("value", ValueType::kInt64)});
  std::vector<NamedExpression> expressions = {NamedExpression(
      "affine",
      BinaryExpressionExp(BinaryExpressionExp(ColumnValueExp("value"),
                                              BinaryOperation::kMultiply,
                                              ConstantValueExp(Value(3))),
                          BinaryOperation::kAdd, ConstantValueExp(Value(7))))};
  Projection projection(std::move(expressions), schema,
                        std::make_shared<ConstantExecutor>(std::move(rows)),
                        1024);
  DataChunk output;
  size_t offset = 0;
  while (projection.NextBatch(&output) != 0) {
    for (size_t row = 0; row < output.Size(); ++row) {
      EXPECT_EQ(output.ColumnAt(0).ValueAt(row),
                Value(static_cast<int64_t>(((offset + row) * 3) + 7)));
    }
    offset += output.Size();
  }
  EXPECT_EQ(offset, 2048U);
  EXPECT_GE(projection.JitBatches(), 1U);
}

TEST_F(ExecutorTest, AggregationUsesJitForLargeIntegerSumBatches) {
  std::vector<Row> rows;
  rows.reserve(2048);
  int64_t expected = 0;
  for (int64_t value = 0; value < 2048; ++value) {
    rows.emplace_back(std::vector<Value>{Value(value)});
    expected += value;
  }
  const Schema schema("jit", {Column("value", ValueType::kInt64)});
  std::vector<NamedExpression> aggregates = {NamedExpression(
      "sum",
      AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("value")))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::move(rows)), schema,
      std::move(aggregates), 1024);
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(expected));
  EXPECT_GE(aggregate.JitBatches(), 1U);
}

TEST_F(ExecutorTest, BasicJoin) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_tbl,
      rs_->CreateTable(ctx, Schema{"RightTable",
                                   {Column("key2", ValueType::kInt64),
                                    Column("score2", ValueType::kDouble),
                                    Column("name2", ValueType::kVarChar)}}));
  BulkInsert(ctx.txn_, right_tbl,
             {{Row({Value(9), Value(1.2), Value("troop")})},
              {Row({Value(7), Value(3.9), Value("arise")})},
              {Row({Value(1), Value(4.9), Value("probe")})},
              {Row({Value(3), Value(12.4), Value("ought")})},
              {Row({Value(3), Value(99.9), Value("extra")})},
              {Row({Value(232), Value(40.9), Value("out")})},
              {Row({Value(0), Value(9.2), Value("arise")})}});
  HashJoin hj(std::make_shared<FullScan>(ctx.txn_, *tbl), {0},
              std::make_shared<FullScan>(ctx.txn_, right_tbl), {0});
  DumpLog(hj);
  std::unordered_set expected({Row({Value(0), Value("hello"), Value(1.2),
                                    Value(0), Value(9.2), Value("arise")}),
                               Row({Value(3), Value("piyo"), Value(12.2),
                                    Value(3), Value(12.4), Value("ought")}),
                               Row({Value(3), Value("piyo"), Value(12.2),
                                    Value(3), Value(99.9), Value("extra")}),
                               Row({Value(1), Value("world"), Value(4.9),
                                    Value(1), Value(4.9), Value("probe")})});
  Row got;

  // Act -- iterate HashJoin cursor through all matched pairs
  ASSERT_TRUE(hj.Next(&got, nullptr));
  ASSERT_NE(expected.find(got), expected.end());
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(hj.Next(&got, nullptr));
  ASSERT_NE(expected.find(got), expected.end());
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(hj.Next(&got, nullptr));
  ASSERT_NE(expected.find(got), expected.end());
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(hj.Next(&got, nullptr));
  ASSERT_NE(expected.find(got), expected.end());
  ASSERT_TRUE(expected.erase(got));

  // Assert -- cursor exhausted and all matched pairs consumed
  ASSERT_FALSE(hj.Next(&got, nullptr));
  ASSERT_TRUE(expected.empty());
}

TEST_F(ExecutorTest, LookupJoin) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_tbl,
      rs_->CreateTable(ctx, Schema{"RightTable",
                                   {Column("key", ValueType::kInt64),
                                    Column("score", ValueType::kDouble),
                                    Column("name", ValueType::kVarChar)}}));
  BulkInsert(ctx.txn_, right_tbl,
             {{Row({Value(1), Value(4.9), Value("right one")})},
              {Row({Value(3), Value(12.4), Value("right three")})},
              {Row({Value(3), Value(99.9), Value("right duplicated three")})},
              {Row({Value(2), Value(99.9), Value("right two")})},
              {Row({Value(232), Value(40.9), Value("right ignored")})},
              {Row({Value(0), Value(9.2), Value("right zero")})}});
  ASSERT_SUCCESS(rs_->CreateIndex(
      ctx, "RightTable",
      IndexSchema("RightIdx", {0}, {}, IndexMode::kNonUnique)));
  const StatusOr<std::shared_ptr<Table>> reload_right_status =
      ctx.GetTable("RightTable");
  ASSERT_EQ(reload_right_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& reload_right = reload_right_status.Value();
  ASSERT_EQ(reload_right->IndexCount(), 1);
  LookupJoin ij(ctx.txn_, std::make_shared<FullScan>(ctx.txn_, *tbl), {0},
                *reload_right, reload_right->GetIndex(0), {0});
  LOG(INFO) << ij;
  std::unordered_set expected(
      {Row({Value(0), Value("hello"), Value(1.2), Value(0), Value(9.2),
            Value("right zero")}),
       Row({Value(3), Value("piyo"), Value(12.2), Value(3), Value(12.4),
            Value("right three")}),
       Row({Value(3), Value("piyo"), Value(12.2), Value(3), Value(99.9),
            Value("right duplicated three")}),
       Row({Value(1), Value("world"), Value(4.9), Value(1), Value(4.9),
            Value("right one")}),
       Row({Value(2), Value("arise"), Value(4.14), Value(2), Value(99.9),
            Value("right two")})});
  Row got;

  // Act -- iterate LookupJoin cursor through all matched pairs
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));

  // Assert -- cursor exhausted and all matched pairs consumed
  ASSERT_FALSE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.empty());
}

TEST_F(ExecutorTest, AuditLookupJoinRejectsMissingProjectionColumn) {
  TransactionContext ctx = rs_->BeginContext();
  ASSERT_SUCCESS(rs_->CreateIndex(
      ctx, kTableName,
      IndexSchema("audit_projection", {0}, {}, IndexMode::kNonUnique)));
  auto table = ctx.GetTable(kTableName);
  ASSERT_TRUE(table.HasValue());
  LookupJoin join(
      ctx.txn_, std::make_shared<FullScan>(ctx.txn_, *table.Value()), {99},
      *table.Value(), table.Value()->GetIndex(table.Value()->IndexCount() - 1),
      {0});
  DataChunk chunk;
  EXPECT_EQ(join.NextBatch(&chunk), 0U);
  EXPECT_EQ(join.GetStatus(), Status::kInvalidArgument);
}

TEST_F(ExecutorTest, LookupJoinWithCompositeKey) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_tbl,
      rs_->CreateTable(ctx, Schema{"RightTable",
                                   {Column("key", ValueType::kInt64),
                                    Column("score", ValueType::kDouble),
                                    Column("name", ValueType::kVarChar)}}));
  BulkInsert(ctx.txn_, right_tbl,
             {{Row({Value(1), Value(4.9), Value("right one")})},
              {Row({Value(3), Value(12.4), Value("right three")})},
              {Row({Value(3), Value(99.9), Value("piyo")})},
              {Row({Value(2), Value(12.3), Value("arise")})},
              {Row({Value(232), Value(40.9), Value("right ignored")})},
              {Row({Value(0), Value(9.2), Value("hello")})},
              {Row({Value(0), Value(0.1), Value("build")})}});
  ASSERT_SUCCESS(rs_->CreateIndex(
      ctx, "RightTable",
      IndexSchema("RightIdx", {0, 2}, {}, IndexMode::kNonUnique)));
  const StatusOr<std::shared_ptr<Table>> reload_right_status =
      ctx.GetTable("RightTable");
  ASSERT_EQ(reload_right_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& reload_right = reload_right_status.Value();
  ASSERT_EQ(reload_right->IndexCount(), 1);
  // The probe columns are aligned with the index's key order (key, name), not
  // with the order the join happens to list its keys in: the seek key has to be
  // a prefix of the stored key image.
  LookupJoin ij(ctx.txn_, std::make_shared<FullScan>(ctx.txn_, *tbl), {0, 1},
                *reload_right, reload_right->GetIndex(0), {0, 2});
  LOG(INFO) << ij;
  std::unordered_set expected({Row({Value(0), Value("hello"), Value(1.2),
                                    Value(0), Value(9.2), Value("hello")}),
                               Row({Value(3), Value("piyo"), Value(12.2),
                                    Value(3), Value(99.9), Value("piyo")}),
                               Row({Value(2), Value("arise"), Value(4.14),
                                    Value(2), Value(12.3), Value("arise")})});
  Row got;

  // Act -- iterate LookupJoin cursor through all composite-key matched pairs
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));

  // Assert -- cursor exhausted and all matched pairs consumed
  ASSERT_FALSE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.empty());
}

// The unique-inner short-circuit. A point probe on a COMPLETE single-value
// unique key can name at most one row, so the cursor emits it and steps the
// OUTER cursor instead of walking on. The four cases below pin the gate from
// both sides: the two shapes that qualify, and the two that must not.
TEST_F(ExecutorTest, LookupJoinUniquePointProbeShortCircuits) {
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_tbl,
      rs_->CreateTable(ctx, Schema{"UniqueInner",
                                   {Column("key", ValueType::kInt64),
                                    Column("label", ValueType::kVarChar)}}));
  BulkInsert(ctx.txn_, right_tbl,
             {{Row({Value(1), Value("one")})},
              {Row({Value(2), Value("two")})},
              {Row({Value(3), Value("three")})}});
  // Default mode of IndexSchema is kUnique: one stored value per key.
  ASSERT_SUCCESS(
      rs_->CreateIndex(ctx, "UniqueInner", IndexSchema("UniqueKey", {0})));
  const StatusOr<std::shared_ptr<Table>> right_status =
      ctx.GetTable("UniqueInner");
  ASSERT_EQ(right_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& right = right_status.Value();
  ASSERT_EQ(right->IndexCount(), 1);
  ASSERT_TRUE(right->GetIndex(0).StoresSingleValue());
  LookupJoin join(ctx.txn_, std::make_shared<FullScan>(ctx.txn_, *tbl), {0},
                  *right, right->GetIndex(0), {0});
  EXPECT_TRUE(join.AtMostOneProbe());
  // The shape is also auditable in an executor dump, where the operator is
  // explained next to the columns it probes.
  std::ostringstream dump;
  join.Dump(dump, 0);
  EXPECT_NE(dump.str().find("at-most-one"), std::string::npos) << dump.str();

  std::unordered_set<Row> expected(
      {Row({Value(3), Value("piyo"), Value(12.2), Value(3), Value("three")}),
       Row({Value(1), Value("world"), Value(4.9), Value(1), Value("one")}),
       Row({Value(2), Value("arise"), Value(4.14), Value(2), Value("two")})});
  Row got;
  while (join.Next(&got, nullptr)) {
    ASSERT_TRUE(expected.erase(got)) << got;
  }
  ASSERT_SUCCESS(join.GetStatus());
  EXPECT_TRUE(expected.empty());
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(ExecutorTest, LookupJoinNonUniqueInnerKeepsTheFullMatchSet) {
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_tbl,
      rs_->CreateTable(ctx, Schema{"SharedInner",
                                   {Column("key", ValueType::kInt64),
                                    Column("label", ValueType::kVarChar)}}));
  BulkInsert(ctx.txn_, right_tbl,
             {{Row({Value(3), Value("three-a")})},
              {Row({Value(3), Value("three-b")})},
              {Row({Value(1), Value("one")})}});
  ASSERT_SUCCESS(rs_->CreateIndex(
      ctx, "SharedInner",
      IndexSchema("SharedKey", {0}, {}, IndexMode::kNonUnique)));
  const StatusOr<std::shared_ptr<Table>> right_status =
      ctx.GetTable("SharedInner");
  ASSERT_EQ(right_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& right = right_status.Value();
  LookupJoin join(ctx.txn_, std::make_shared<FullScan>(ctx.txn_, *tbl), {0},
                  *right, right->GetIndex(0), {0});
  // A non-unique index holds a vector of positions per key, so the probe has
  // to walk all of them: the short-circuit must stay off.
  EXPECT_FALSE(join.AtMostOneProbe());

  std::unordered_set<Row> expected(
      {Row({Value(3), Value("piyo"), Value(12.2), Value(3), Value("three-a")}),
       Row({Value(3), Value("piyo"), Value(12.2), Value(3), Value("three-b")}),
       Row({Value(1), Value("world"), Value(4.9), Value(1), Value("one")})});
  Row got;
  while (join.Next(&got, nullptr)) {
    ASSERT_TRUE(expected.erase(got)) << got;
  }
  ASSERT_SUCCESS(join.GetStatus());
  EXPECT_TRUE(expected.empty());
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(ExecutorTest, LookupJoinUniqueCompositeKeyNeedsTheWholeKey) {
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_tbl,
      rs_->CreateTable(ctx, Schema{"CompositeInner",
                                   {Column("key", ValueType::kInt64),
                                    Column("tag", ValueType::kVarChar),
                                    Column("payload", ValueType::kInt64)}}));
  BulkInsert(ctx.txn_, right_tbl,
             {{Row({Value(3), Value("piyo"), Value(30)})},
              {Row({Value(3), Value("other"), Value(31)})},
              {Row({Value(1), Value("world"), Value(10)})}});
  ASSERT_SUCCESS(rs_->CreateIndex(ctx, "CompositeInner",
                                  IndexSchema("CompositeKey", {0, 1})));
  const StatusOr<std::shared_ptr<Table>> right_status =
      ctx.GetTable("CompositeInner");
  ASSERT_EQ(right_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& right = right_status.Value();
  // Only the leading key is pinned: the seek is a prefix walk, not a point
  // lookup, so every (3, *) entry has to be emitted.
  LookupJoin prefix(ctx.txn_, std::make_shared<FullScan>(ctx.txn_, *tbl), {0},
                    *right, right->GetIndex(0), {0});
  EXPECT_FALSE(prefix.AtMostOneProbe());
  std::unordered_set<Row> prefix_expected(
      {Row({Value(3), Value("piyo"), Value(12.2), Value(3), Value("piyo"),
            Value(30)}),
       Row({Value(3), Value("piyo"), Value(12.2), Value(3), Value("other"),
            Value(31)}),
       Row({Value(1), Value("world"), Value(4.9), Value(1), Value("world"),
            Value(10)})});
  Row got;
  while (prefix.Next(&got, nullptr)) {
    ASSERT_TRUE(prefix_expected.erase(got)) << got;
  }
  ASSERT_SUCCESS(prefix.GetStatus());
  EXPECT_TRUE(prefix_expected.empty());

  // The complete key is a single-value point probe again.
  LookupJoin whole(ctx.txn_, std::make_shared<FullScan>(ctx.txn_, *tbl), {0, 1},
                   *right, right->GetIndex(0), {0, 1});
  EXPECT_TRUE(whole.AtMostOneProbe());
  std::unordered_set<Row> whole_expected(
      {Row({Value(3), Value("piyo"), Value(12.2), Value(3), Value("piyo"),
            Value(30)}),
       Row({Value(1), Value("world"), Value(4.9), Value(1), Value("world"),
            Value(10)})});
  while (whole.Next(&got, nullptr)) {
    ASSERT_TRUE(whole_expected.erase(got)) << got;
  }
  ASSERT_SUCCESS(whole.GetStatus());
  EXPECT_TRUE(whole_expected.empty());
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(ExecutorTest, LookupJoinRetainedUniqueIndexIsNotASingleValue) {
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_tbl,
      rs_->CreateTable(ctx, Schema{"RetainedInner",
                                   {Column("key", ValueType::kInt64),
                                    Column("label", ValueType::kVarChar)}}));
  BulkInsert(
      ctx.txn_, right_tbl,
      {{Row({Value(3), Value("three")})}, {Row({Value(1), Value("one")})}});
  ASSERT_SUCCESS(rs_->CreateIndex(
      ctx, "RetainedInner",
      IndexSchema("RetainedKey", {0}, {}, IndexMode::kVersionedUnique)));
  const StatusOr<std::shared_ptr<Table>> right_status =
      ctx.GetTable("RetainedInner");
  ASSERT_EQ(right_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& right = right_status.Value();
  // kVersionedUnique stores a VECTOR per key (deleted positions are retained
  // so an older snapshot can still reach the row version), so "at most one"
  // does not hold and the cursor must walk the entry list.
  EXPECT_TRUE(right->GetIndex(0).IsUnique());
  EXPECT_FALSE(right->GetIndex(0).StoresSingleValue());
  LookupJoin join(ctx.txn_, std::make_shared<FullScan>(ctx.txn_, *tbl), {0},
                  *right, right->GetIndex(0), {0});
  EXPECT_FALSE(join.AtMostOneProbe());
  std::unordered_set<Row> expected(
      {Row({Value(3), Value("piyo"), Value(12.2), Value(3), Value("three")}),
       Row({Value(1), Value("world"), Value(4.9), Value(1), Value("one")})});
  Row got;
  while (join.Next(&got, nullptr)) {
    ASSERT_TRUE(expected.erase(got)) << got;
  }
  ASSERT_SUCCESS(join.GetStatus());
  EXPECT_TRUE(expected.empty());
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(ExecutorTest, LookupJoinRejectsNullAndNaNKeys) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_table,
      rs_->CreateTable(ctx, Schema{"IndexedDouble",
                                   {Column("key", ValueType::kDouble),
                                    Column("value", ValueType::kInt64)}}));
  const double nan = std::numeric_limits<double>::quiet_NaN();
  ASSERT_SUCCESS(
      right_table.Insert(ctx.txn_, Row({Value(nan), Value(7)})).GetStatus());
  ASSERT_SUCCESS(
      right_table.Insert(ctx.txn_, Row({Value(1.0), Value(8)})).GetStatus());
  ASSERT_SUCCESS(
      right_table.Insert(ctx.txn_, Row({Value(2.0), Value(9)})).GetStatus());
  ASSERT_EQ(rs_->CreateIndex(ctx, "IndexedDouble",
                             IndexSchema("IndexedDoubleKey", {0}, {},
                                         IndexMode::kNonUnique)),
            Status::kSuccess);
  const StatusOr<std::shared_ptr<Table>> reloaded_status =
      ctx.GetTable("IndexedDouble");
  ASSERT_EQ(reloaded_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& reloaded = reloaded_status.Value();
  auto left = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(nan)}), Row({Value(1.0)}), Row({Value()})});
  LookupJoin join(ctx.txn_, left, {0}, *reloaded, reloaded->GetIndex(0), {0});
  std::vector<Row> rows;
  Row row;
  while (join.Next(&row, nullptr)) {
    rows.push_back(row);
  }
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(1.0));
  EXPECT_EQ(rows[0][1], Value(1.0));
  EXPECT_EQ(rows[0][2], Value(8));
}

// The short-circuit clears the held outer row on every match, so a repeated
// probe key must seek again instead of reusing the previous landing. This is
// the state machine an outer side with duplicate keys exercises.
TEST_F(ExecutorTest, LookupJoinUniquePointProbeHandlesRepeatedOuterKeys) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_tbl,
      rs_->CreateTable(ctx, Schema{"RepeatedTarget",
                                   {Column("k", ValueType::kInt64),
                                    Column("v", ValueType::kInt64)}}));
  BulkInsert(ctx.txn_, right_tbl,
             {{Row({Value(7), Value(70)})}, {Row({Value(8), Value(80)})}});
  ASSERT_SUCCESS(
      rs_->CreateIndex(ctx, "RepeatedTarget",
                       IndexSchema("RepeatedK", {0}, {}, IndexMode::kUnique)));
  const StatusOr<std::shared_ptr<Table>> right_status =
      ctx.GetTable("RepeatedTarget");
  ASSERT_EQ(right_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& right = right_status.Value();
  // 7 twice, a miss, then 8: every left row is answered by its own probe.
  LookupJoin join(ctx.txn_,
                  RangeProbeInput({Row({Value(7)}), Row({Value(7)}),
                                   Row({Value(9)}), Row({Value(8)})}),
                  {0}, *right, right->GetIndex(0), {0});
  EXPECT_TRUE(join.AtMostOneProbe());
  const std::vector<Row> rows = Drain(join);
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0], Row({Value(7), Value(7), Value(70)}));
  EXPECT_EQ(rows[1], Row({Value(7), Value(7), Value(70)}));
  EXPECT_EQ(rows[2], Row({Value(8), Value(8), Value(80)}));
  ASSERT_SUCCESS(join.GetStatus());
  ASSERT_SUCCESS(ctx.PreCommit());
}

// A composite index (a, b) probed with an equality on `a` and a range bound on
// `b`: the seek opens the half-open key range and the re-check resolves the
// inclusive/exclusive edges, duplicates included.
TEST_F(ExecutorTest, LookupJoinCompositeInt64RangeProbe) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_table,
      rs_->CreateTable(ctx, Schema{"RangeTarget",
                                   {Column("a", ValueType::kInt64),
                                    Column("b", ValueType::kInt64),
                                    Column("v", ValueType::kInt64)}}));
  for (const auto& row :
       {Row({Value(1), Value(10), Value(100)}),
        Row({Value(1), Value(20), Value(200)}),
        // Duplicate keys on a non-unique index: both must come back.
        Row({Value(1), Value(20), Value(201)}),
        Row({Value(1), Value(30), Value(300)}),
        Row({Value(2), Value(5), Value(400)}),
        Row({Value(2), Value(6), Value(500)})}) {
    ASSERT_SUCCESS(right_table.Insert(ctx.txn_, row).GetStatus());
  }
  ASSERT_EQ(rs_->CreateIndex(
                ctx, "RangeTarget",
                IndexSchema("RangeAB", {0, 1}, {}, IndexMode::kNonUnique)),
            Status::kSuccess);
  const auto reloaded_status = ctx.GetTable("RangeTarget");
  ASSERT_EQ(reloaded_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& reloaded = reloaded_status.Value();

  // Output rows are `left(2) + right(3)`, so the payload `v` is column 4.
  // a = 1 AND b < 20 -> only b = 10.
  LookupJoin less(ctx.txn_, RangeProbeInput({Row({Value(1), Value(20)})}),
                  {0, 1}, *reloaded, reloaded->GetIndex(0), {0, 1}, 1,
                  BinaryOperation::kLessThan);
  const std::vector<Row> less_rows = Drain(less);
  ASSERT_EQ(less_rows.size(), 1U);
  EXPECT_EQ(less_rows[0],
            Row({Value(1), Value(20), Value(1), Value(10), Value(100)}));

  // a = 1 AND b <= 20 -> the boundary row is inside the seek range and the
  // re-check keeps it; the duplicate key comes back too.
  LookupJoin less_equal(ctx.txn_, RangeProbeInput({Row({Value(1), Value(20)})}),
                        {0, 1}, *reloaded, reloaded->GetIndex(0), {0, 1}, 1,
                        BinaryOperation::kLessThanEquals);
  const std::vector<Row> less_equal_rows = Drain(less_equal);
  ASSERT_EQ(less_equal_rows.size(), 3U);
  EXPECT_EQ(less_equal_rows[0],
            Row({Value(1), Value(20), Value(1), Value(10), Value(100)}));
  EXPECT_EQ(less_equal_rows[1],
            Row({Value(1), Value(20), Value(1), Value(20), Value(200)}));
  EXPECT_EQ(less_equal_rows[2],
            Row({Value(1), Value(20), Value(1), Value(20), Value(201)}));

  // a = 2 AND b > 6 -> nothing (the mirrored seek starts at the bound).
  LookupJoin greater(ctx.txn_, RangeProbeInput({Row({Value(2), Value(6)})}),
                     {0, 1}, *reloaded, reloaded->GetIndex(0), {0, 1}, 1,
                     BinaryOperation::kGreaterThan);
  EXPECT_TRUE(Drain(greater).empty());

  // a = 2 AND b >= 5 -> both rows of a = 2.
  LookupJoin greater_equal(
      ctx.txn_, RangeProbeInput({Row({Value(2), Value(5)})}), {0, 1}, *reloaded,
      reloaded->GetIndex(0), {0, 1}, 1, BinaryOperation::kGreaterThanEquals);
  const std::vector<Row> greater_equal_rows = Drain(greater_equal);
  ASSERT_EQ(greater_equal_rows.size(), 2U);
  EXPECT_EQ(greater_equal_rows[0],
            Row({Value(2), Value(5), Value(2), Value(5), Value(400)}));
  EXPECT_EQ(greater_equal_rows[1],
            Row({Value(2), Value(5), Value(2), Value(6), Value(500)}));

  // A NULL bound compares UNKNOWN against every right row, so it cannot match;
  // the probe must fall back to the wider prefix scan and reject there.
  LookupJoin null_bound(ctx.txn_, RangeProbeInput({Row({Value(1), Value()})}),
                        {0, 1}, *reloaded, reloaded->GetIndex(0), {0, 1}, 1,
                        BinaryOperation::kLessThan);
  EXPECT_TRUE(Drain(null_bound).empty());
}

// The interval probe: an equality prefix plus BOTH sides of an inequality on
// the next key column.  The seek narrows to `[lo, hi]` and the per-candidate
// re-check resolves the inclusive/exclusive edges, so the two bounds are order
// independent and either side may be the one adopted first.
TEST_F(ExecutorTest, LookupJoinCompositeIntervalProbe) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_table,
      rs_->CreateTable(ctx, Schema{"IntervalTarget",
                                   {Column("a", ValueType::kInt64),
                                    Column("b", ValueType::kInt64),
                                    Column("v", ValueType::kInt64)}}));
  for (const auto& row :
       {Row({Value(1), Value(10), Value(100)}),
        Row({Value(1), Value(20), Value(200)}),
        // Duplicate keys on a non-unique index: both must come back.
        Row({Value(1), Value(20), Value(201)}),
        Row({Value(1), Value(30), Value(300)}),
        Row({Value(2), Value(5), Value(400)}),
        Row({Value(2), Value(6), Value(500)})}) {
    ASSERT_SUCCESS(right_table.Insert(ctx.txn_, row).GetStatus());
  }
  ASSERT_EQ(rs_->CreateIndex(
                ctx, "IntervalTarget",
                IndexSchema("IntervalAB", {0, 1}, {}, IndexMode::kNonUnique)),
            Status::kSuccess);
  const auto reloaded_status = ctx.GetTable("IntervalTarget");
  ASSERT_EQ(reloaded_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& reloaded = reloaded_status.Value();

  // Output rows are `left(3) + right(3)`, so the payload `v` is column 5.
  // a = 1 AND 15 < b <= 25 -> the two b = 20 rows.
  LookupJoin interval(
      ctx.txn_, RangeProbeInput({Row({Value(1), Value(15), Value(25)})}),
      {0, 1}, *reloaded, reloaded->GetIndex(0), {0, 1}, 1,
      BinaryOperation::kGreaterThan,
      /*second_range_column=*/2,
      /*second_range_operation=*/BinaryOperation::kLessThanEquals);
  const std::vector<Row> interval_rows = Drain(interval);
  ASSERT_EQ(interval_rows.size(), 2U);
  EXPECT_EQ(interval_rows[0], Row({Value(1), Value(15), Value(25), Value(1),
                                   Value(20), Value(200)}));
  EXPECT_EQ(interval_rows[1], Row({Value(1), Value(15), Value(25), Value(1),
                                   Value(20), Value(201)}));

  // The bounds are order independent: an upper bound adopted first still
  // produces the same interval.
  LookupJoin reversed(ctx.txn_,
                      RangeProbeInput({Row({Value(1), Value(25), Value(15)})}),
                      {0, 1}, *reloaded, reloaded->GetIndex(0), {0, 1}, 1,
                      BinaryOperation::kLessThanEquals,
                      /*second_range_column=*/2,
                      /*second_range_operation=*/BinaryOperation::kGreaterThan);
  const std::vector<Row> reversed_rows = Drain(reversed);
  ASSERT_EQ(reversed_rows.size(), 2U);
  EXPECT_EQ(reversed_rows[0], Row({Value(1), Value(25), Value(15), Value(1),
                                   Value(20), Value(200)}));
  EXPECT_EQ(reversed_rows[1], Row({Value(1), Value(25), Value(15), Value(1),
                                   Value(20), Value(201)}));

  // Strict upper edge: b < 20 excludes the boundary rows.
  LookupJoin strict_upper(
      ctx.txn_, RangeProbeInput({Row({Value(1), Value(15), Value(20)})}),
      {0, 1}, *reloaded, reloaded->GetIndex(0), {0, 1}, 1,
      BinaryOperation::kGreaterThan,
      /*second_range_column=*/2,
      /*second_range_operation=*/BinaryOperation::kLessThan);
  EXPECT_TRUE(Drain(strict_upper).empty());

  // Inclusive lower edge: b >= 15 AND b <= 20 keeps the duplicate pair.
  LookupJoin inclusive_lower(
      ctx.txn_, RangeProbeInput({Row({Value(1), Value(15), Value(20)})}),
      {0, 1}, *reloaded, reloaded->GetIndex(0), {0, 1}, 1,
      BinaryOperation::kGreaterThanEquals, /*second_range_column=*/2,
      /*second_range_operation=*/BinaryOperation::kLessThanEquals);
  EXPECT_EQ(Drain(inclusive_lower).size(), 2U);

  // A NULL second bound can never compare TRUE, so the left row matches
  // nothing -- the same skip a NULL equality key gets.
  LookupJoin null_upper(
      ctx.txn_, RangeProbeInput({Row({Value(1), Value(15), Value()})}), {0, 1},
      *reloaded, reloaded->GetIndex(0), {0, 1}, 1,
      BinaryOperation::kGreaterThan,
      /*second_range_column=*/2,
      /*second_range_operation=*/BinaryOperation::kLessThanEquals);
  EXPECT_TRUE(Drain(null_upper).empty());
}

// The range allowlist covers DATE as well as INT64: both encode as
// order-preserving signed integers, so a DATE key column takes a range bound
// exactly like an INT64 one.
TEST_F(ExecutorTest, LookupJoinDateRangeProbe) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_table,
      rs_->CreateTable(ctx, Schema{"DateTarget",
                                   {Column("a", ValueType::kInt64),
                                    Column("d", ValueType::kDate),
                                    Column("v", ValueType::kInt64)}}));
  for (const auto& row :
       {Row({Value(1), Value::DateFromDays(20000), Value(1)}),
        Row({Value(1), Value::DateFromDays(20112), Value(2)}),
        Row({Value(1), Value::DateFromDays(20365), Value(3)}),
        Row({Value(2), Value::DateFromDays(20000), Value(4)})}) {
    ASSERT_SUCCESS(right_table.Insert(ctx.txn_, row).GetStatus());
  }
  ASSERT_EQ(rs_->CreateIndex(
                ctx, "DateTarget",
                IndexSchema("DateAD", {0, 1}, {}, IndexMode::kNonUnique)),
            Status::kSuccess);
  const auto reloaded_status = ctx.GetTable("DateTarget");
  ASSERT_EQ(reloaded_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& reloaded = reloaded_status.Value();

  // a = 1 AND d < 20184 (2024-06-23) -> the two DATE rows before the bound.
  LookupJoin join(
      ctx.txn_, RangeProbeInput({Row({Value(1), Value::DateFromDays(20184)})}),
      {0, 1}, *reloaded, reloaded->GetIndex(0), {0, 1}, 1,
      BinaryOperation::kLessThan);
  const std::vector<Row> rows = Drain(join);
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(1), Value::DateFromDays(20184), Value(1),
                          Value::DateFromDays(20000), Value(1)}));
  EXPECT_EQ(rows[1], Row({Value(1), Value::DateFromDays(20184), Value(1),
                          Value::DateFromDays(20112), Value(2)}));

  // An INT64 bound on a DATE key column encodes under a different type tag, so
  // it must not reach the seek: the probe falls back to the (a) prefix and the
  // cross-typed re-check rejects every candidate.
  LookupJoin cross_typed(
      ctx.txn_, RangeProbeInput({Row({Value(1), Value(20184)})}), {0, 1},
      *reloaded, reloaded->GetIndex(0), {0, 1}, 1, BinaryOperation::kLessThan);
  EXPECT_TRUE(Drain(cross_typed).empty());
}

// A single-value unique index holds at most one position per key, so a point
// probe on the complete key short-circuits: the operator advances the outer
// cursor after the single candidate instead of walking the index. A missing
// key must not leave the short circuit stuck.
TEST_F(ExecutorTest, LookupJoinUniqueIndexShortCircuitsTheProbe) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_table,
      rs_->CreateTable(ctx, Schema{"UniqueTarget",
                                   {Column("k", ValueType::kInt64),
                                    Column("v", ValueType::kInt64)}}));
  for (const auto& row :
       {Row({Value(1), Value(10)}), Row({Value(3), Value(30)})}) {
    ASSERT_SUCCESS(right_table.Insert(ctx.txn_, row).GetStatus());
  }
  ASSERT_EQ(
      rs_->CreateIndex(ctx, "UniqueTarget",
                       IndexSchema("UniqueK", {0}, {}, IndexMode::kUnique)),
      Status::kSuccess);
  const auto reloaded_status = ctx.GetTable("UniqueTarget");
  ASSERT_EQ(reloaded_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& reloaded = reloaded_status.Value();

  // Probe order hits a missing key first, then both present keys.
  LookupJoin join(ctx.txn_,
                  RangeProbeInput({Row({Value(2)}), Row({Value(1)}),
                                   Row({Value(99)}), Row({Value(3)})}),
                  {0}, *reloaded, reloaded->GetIndex(0), {0});
  const std::vector<Row> rows = Drain(join);
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(1), Value(10)}));
  EXPECT_EQ(rows[1], Row({Value(3), Value(3), Value(30)}));
}

}  // namespace tinylamb
