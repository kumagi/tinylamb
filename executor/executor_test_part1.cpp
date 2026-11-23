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
  find_context.Abort();
  scan_context.Abort();
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
      std::getenv("TINYLAMB_DISABLE_INDEX_SCAN_PREFETCH") != nullptr;
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

// An empty right side makes every probe miss: the cursor must report a clean
// exhaustion, not a failure, and must not lose the left side's error either.
TEST_F(ExecutorTest, LookupJoinEmptyRightSideExhaustsCleanly) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_table,
      rs_->CreateTable(ctx, Schema{"EmptyTarget",
                                   {Column("a", ValueType::kInt64),
                                    Column("b", ValueType::kInt64)}}));
  ASSERT_EQ(rs_->CreateIndex(
                ctx, "EmptyTarget",
                IndexSchema("EmptyAB", {0, 1}, {}, IndexMode::kNonUnique)),
            Status::kSuccess);
  const auto reloaded_status = ctx.GetTable("EmptyTarget");
  ASSERT_EQ(reloaded_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& reloaded = reloaded_status.Value();

  LookupJoin join(
      ctx.txn_,
      RangeProbeInput({Row({Value(1), Value(2)}), Row({Value(3), Value(4)})}),
      {0, 1}, *reloaded, reloaded->GetIndex(0), {0, 1}, 1,
      BinaryOperation::kLessThan);
  EXPECT_TRUE(Drain(join).empty());

  // An empty left side never probes at all.
  LookupJoin empty_left(ctx.txn_, RangeProbeInput({}), {0, 1}, *reloaded,
                        reloaded->GetIndex(0), {0, 1}, 1,
                        BinaryOperation::kLessThan);
  EXPECT_TRUE(Drain(empty_left).empty());
}

// The residual predicate carries what the probe cannot decide. Driven here
// through the plan/factory pair: an equality on a NON-key column of a composite
// index leaves the seek on the leading key only, and the extra equality must
// still filter the probe's over-fetch. The old executor sought with the join's
// own key order here, landing between unrelated stored keys and silently
// dropping every match.
TEST_F(ExecutorTest, LookupJoinRechecksEqualityOnANonKeyColumn) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, left_table,
      rs_->CreateTable(ctx, Schema{"ProbeSide",
                                   {Column("x", ValueType::kInt64),
                                    Column("y", ValueType::kInt64)}}));
  ASSERT_SUCCESS(
      left_table.Insert(ctx.txn_, Row({Value(1), Value(5)})).GetStatus());
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_table,
      rs_->CreateTable(ctx, Schema{"CompositeSide",
                                   {Column("a", ValueType::kInt64),
                                    Column("b", ValueType::kInt64),
                                    Column("c", ValueType::kInt64)}}));
  for (const auto& row : {Row({Value(1), Value(10), Value(5)}),
                          Row({Value(1), Value(20), Value(5)}),
                          Row({Value(1), Value(30), Value(6)}),
                          Row({Value(2), Value(40), Value(5)})}) {
    ASSERT_SUCCESS(right_table.Insert(ctx.txn_, row).GetStatus());
  }
  ASSERT_EQ(rs_->CreateIndex(
                ctx, "CompositeSide",
                IndexSchema("CompositeAB", {0, 1}, {}, IndexMode::kNonUnique)),
            Status::kSuccess);
  const auto right = ctx.GetTable("CompositeSide");
  const auto right_stats = ctx.GetStats("CompositeSide");
  ASSERT_TRUE(right.HasValue());
  ASSERT_TRUE(right_stats.HasValue());
  // NOTE: FullScanPlan keeps its statistics by reference, so the scan's stats
  // must outlive the plan -- bind them to a named local, not a temporary.
  const TableStatistics left_stats(left_table.GetSchema());
  Plan plan(new ProductPlan(
      std::make_shared<FullScanPlan>(left_table, left_stats),
      {ColumnName("ProbeSide", "x"), ColumnName("ProbeSide", "y")},
      *right.Value(), right.Value()->GetIndex(0),
      {ColumnName("CompositeSide", "a"), ColumnName("CompositeSide", "c")},
      *right_stats.Value()));
  // The probe derives the (a) prefix in the index's own key order; `c` is not
  // an index key column, so `x = c` is not part of the seek. The join emits
  // the probe's over-fetch and the residual predicate above it decides.
  auto& product = static_cast<ProductPlan&>(*plan);  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
  const std::optional<LookupSeek>& probe = product.Lookup();
  ASSERT_TRUE(probe.has_value());
  ASSERT_EQ(SeekOf(probe).right_columns.size(), 1U);
  EXPECT_EQ(SeekOf(probe).right_columns[0], ColumnName("CompositeSide", "a"));
  EXPECT_EQ(SeekOf(probe).left_columns[0], ColumnName("ProbeSide", "x"));
  ASSERT_EQ(SeekOf(probe).range_position, std::nullopt);
  // Without the residual the probe over-fetches every row under the (a)
  // prefix: 1, 2 and 3 rows of the composite side.
  {
    Executor raw = plan->EmitExecutor(ctx);
    const std::vector<Row> over_fetched = Drain(*raw);
    ASSERT_EQ(over_fetched.size(), 3U);
  }

  const Expression residual = BinaryExpressionExp(
      ColumnValueExp(ColumnName("ProbeSide", "y")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("CompositeSide", "c")));
  Plan filtered = std::make_shared<SelectionPlan>(
      plan, residual, TableStatistics(right.Value()->GetSchema()));
  Executor executor = filtered->EmitExecutor(ctx);
  std::ostringstream dump;
  executor->Dump(dump, 0);
  EXPECT_NE(dump.str().find("LookupJoin"), std::string::npos) << dump.str();
  const std::vector<Row> rows = Drain(*executor);
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(5), Value(1), Value(10), Value(5)}));
  EXPECT_EQ(rows[1], Row({Value(1), Value(5), Value(1), Value(20), Value(5)}));
}

// The range allowlist is INT64/DATE only. A FLOAT64 range column stays a
// residual predicate, so the probe must still return the right rows through the
// wider equality-prefix scan.
TEST_F(ExecutorTest, LookupJoinRejectsNonAllowlistedRangeTypes) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, probe_table,
      rs_->CreateTable(ctx, Schema{"FloatProbe",
                                   {Column("a", ValueType::kInt64),
                                    Column("f", ValueType::kDouble)}}));
  ASSERT_SUCCESS(
      probe_table.Insert(ctx.txn_, Row({Value(1), Value(1.5)})).GetStatus());
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_table,
      rs_->CreateTable(ctx, Schema{"FloatTarget",
                                   {Column("a", ValueType::kInt64),
                                    Column("f", ValueType::kDouble),
                                    Column("v", ValueType::kInt64)}}));
  for (const auto& row : {Row({Value(1), Value(1.5), Value(10)}),
                          Row({Value(1), Value(9.5), Value(90)}),
                          Row({Value(1), Value(2.5), Value(20)})}) {
    ASSERT_SUCCESS(right_table.Insert(ctx.txn_, row).GetStatus());
  }
  ASSERT_EQ(rs_->CreateIndex(
                ctx, "FloatTarget",
                IndexSchema("FloatAV", {0, 2}, {}, IndexMode::kNonUnique)),
            Status::kSuccess);
  const auto right = ctx.GetTable("FloatTarget");
  const auto right_stats = ctx.GetStats("FloatTarget");
  ASSERT_TRUE(right.HasValue());
  ASSERT_TRUE(right_stats.HasValue());
  // NOTE: FullScanPlan keeps its statistics by reference, so the scan's stats
  // must outlive the plan -- bind them to a named local, not a temporary.
  const TableStatistics left_stats(probe_table.GetSchema());
  Plan left_plan = std::make_shared<FullScanPlan>(probe_table, left_stats);
  auto product = std::make_shared<ProductPlan>(
      left_plan,
      std::vector<ColumnName>{ColumnName("FloatProbe", "a"),
                              ColumnName("FloatProbe", "f")},
      *right.Value(), right.Value()->GetIndex(0),
      std::vector<ColumnName>{ColumnName("FloatTarget", "a"),
                              ColumnName("FloatTarget", "f")},
      *right_stats.Value());
  EXPECT_FALSE(product->SetLookupRange(1, ColumnName("FloatProbe", "f"),
                                       ColumnName("FloatTarget", "f"),
                                       BinaryOperation::kLessThan));
  ASSERT_TRUE(product->Lookup().has_value());
  EXPECT_EQ(SeekOf(product->Lookup()).range_position, std::nullopt);
  EXPECT_EQ(SeekOf(product->Lookup()).right_columns.size(), 1U);
  // A mismatched operand type is rejected as well: the per-row re-check could
  // not decide a cross-typed comparison.
  EXPECT_FALSE(product->SetLookupRange(1, ColumnName("FloatProbe", "a"),
                                       ColumnName("FloatTarget", "f"),
                                       BinaryOperation::kLessThan));
  // The range column is not the first unpinned index key column, so a bound
  // there cannot narrow the seek at all.
  EXPECT_FALSE(product->SetLookupRange(2, ColumnName("FloatProbe", "f"),
                                       ColumnName("FloatTarget", "f"),
                                       BinaryOperation::kLessThan));
}

TEST_F(ExecutorTest, Insert) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  Schema src_schema{
      "SrcTable",
      {Column("key2", ValueType::kInt64), Column("name2", ValueType::kVarChar),
       Column("score2", ValueType::kDouble)}};
  rs_->CreateTable(ctx, src_schema);
  const StatusOr<std::shared_ptr<Table>> right_tbl_status =
      ctx.GetTable("SrcTable");
  ASSERT_EQ(right_tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& right_tbl = right_tbl_status.Value();
  BulkInsert(ctx.txn_, *right_tbl,
             {{Row({Value(9), Value("troop"), Value(1.2)})},
              {Row({Value(7), Value("arise"), Value(3.9)})},
              {Row({Value(1), Value("probe"), Value(4.9)})},
              {Row({Value(3), Value("ought"), Value(12.4)})},
              {Row({Value(3), Value("extra"), Value(99.9)})},
              {Row({Value(232), Value("out"), Value(40.9)})},
              {Row({Value(0), Value("arise"), Value(9.2)})}});
  auto insert = std::make_shared<Insert>(
      ctx.txn_, &*tbl, std::make_shared<FullScan>(ctx.txn_, *right_tbl));
  LOG(INFO) << *insert;
  Row result;

  // Act 1 -- iterate Insert executor to produce rows from SrcTable into
  // SampleTable
  ASSERT_TRUE(insert->Next(&result, nullptr));
  ASSERT_EQ(result[1], Value(7));
  ASSERT_FALSE(insert->Next(&result, nullptr));

  // Assert 1 -- Insert executor produced the expected row and then exhausted
  // (implicit above)

  // Arrange 2 -- prepare verification FullScan over the populated SampleTable
  std::unordered_set rows({Row({Value(0), Value("hello"), Value(1.2)}),
                           Row({Value(3), Value("piyo"), Value(12.2)}),
                           Row({Value(1), Value("world"), Value(4.9)}),
                           Row({Value(2), Value("arise"), Value(4.14)}),
                           Row({Value(9), Value("troop"), Value(1.2)}),
                           Row({Value(7), Value("arise"), Value(3.9)}),
                           Row({Value(1), Value("probe"), Value(4.9)}),
                           Row({Value(3), Value("ought"), Value(12.4)}),
                           Row({Value(3), Value("extra"), Value(99.9)}),
                           Row({Value(232), Value("out"), Value(40.9)}),
                           Row({Value(0), Value("arise"), Value(9.2)})});

  // Act 2 -- iterate FullScan to verify all 12 rows are present in SampleTable
  FullScan fs(ctx.txn_, *tbl);
  while (!rows.empty()) {
    Row got;
    RowPosition pos;
    ASSERT_TRUE(fs.Next(&got, &pos));
    ASSERT_NE(rows.find(got), rows.end());
    rows.erase(got);
  }

  // Assert 2 -- FullScan consumed all expected rows and cursor exhausted
  ASSERT_TRUE(rows.empty());
}

TEST_F(ExecutorTest, Update) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  Schema src_schema{
      "SrcTable",
      {Column("key2", ValueType::kInt64), Column("name2", ValueType::kVarChar),
       Column("score2", ValueType::kDouble)}};
  rs_->CreateTable(ctx, src_schema);
  const StatusOr<std::shared_ptr<Table>> right_tbl_status =
      ctx.GetTable("SrcTable");
  ASSERT_EQ(right_tbl_status.GetStatus(), Status::kSuccess);

  std::vector<NamedExpression> update_rule = {
      NamedExpression("key", ColumnValueExp("key")),
      NamedExpression("name", ConstantValueExp(Value("****"))),
      NamedExpression("score",
                      BinaryExpressionExp(ColumnValueExp("score"),
                                          BinaryOperation::kMultiply,
                                          ConstantValueExp(Value(2.0))))};
  auto update = std::make_shared<Update>(
      ctx.txn_, &*tbl,
      std::make_shared<Projection>(update_rule, tbl->GetSchema(),
                                   std::make_shared<FullScan>(ctx.txn_, *tbl)));
  LOG(INFO) << *update;
  Row result;

  // Act 1 -- iterate Update executor to apply updates to SampleTable rows
  ASSERT_TRUE(update->Next(&result, nullptr));
  ASSERT_EQ(result[1], Value(4));
  ASSERT_FALSE(update->Next(&result, nullptr));

  // Assert 1 -- Update executor produced the expected updated row and exhausted
  // (implicit above)

  // Arrange 2 -- prepare verification FullScan over the updated SampleTable
  std::unordered_set<Row> rows({{Row({Value(0), Value("****"), Value(2.4)})},
                                {Row({Value(3), Value("****"), Value(24.4)})},
                                {Row({Value(1), Value("****"), Value(9.8)})},
                                {Row({Value(2), Value("****"), Value(8.28)})}});

  // Act 2 -- iterate FullScan to verify all 4 updated rows are present
  FullScan fs(ctx.txn_, *tbl);
  while (!rows.empty()) {
    Row got;
    RowPosition pos;
    ASSERT_TRUE(fs.Next(&got, &pos));
    ASSERT_NE(rows.find(got), rows.end());
    rows.erase(got);
  }

  // Assert 2 -- FullScan consumed all expected updated rows and cursor
  // exhausted
  ASSERT_TRUE(rows.empty());
}

TEST_F(ExecutorTest, UpdatePreservesConstraintFailureStatusAndMessage) {
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        ctx.GetTable(kTableName));
  // Deliberately provide an incomplete replacement image at valid positions.
  auto source = std::make_shared<Projection>(
      std::vector<NamedExpression>{
          NamedExpression("key", ColumnValueExp("key"))},
      table->GetSchema(), std::make_shared<FullScan>(ctx.txn_, *table));
  Update update(ctx.txn_, table.get(), std::move(source));
  Row result;
  EXPECT_FALSE(update.Next(&result, nullptr));
  EXPECT_EQ(update.GetStatus(), Status::kInvalidArgument);
  EXPECT_NE(update.GetStatus().GetMessage().find("update failed on table"),
            std::string::npos);
  EXPECT_NE(update.GetStatus().GetMessage().find(
                "row column count does not match table schema"),
            std::string::npos);
  size_t count = 0;
  for (auto scan = table->BeginFullScan(ctx.txn_); scan.IsValid(); ++scan) {
    EXPECT_EQ((*scan).values_.size(), 3);
    ++count;
  }
  EXPECT_EQ(count, 4);
}

TEST_F(ExecutorTest, Aggregation) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  auto fs = std::make_shared<FullScan>(ctx.txn_, *tbl);
  std::vector<NamedExpression> aggregates = {
      NamedExpression("count", AggregateExpressionExp(AggregationType::kCount,
                                                      ColumnValueExp("key"))),
      NamedExpression("sum", AggregateExpressionExp(AggregationType::kSum,
                                                    ColumnValueExp("score"))),
      NamedExpression("avg", AggregateExpressionExp(AggregationType::kAvg,
                                                    ColumnValueExp("score"))),
      NamedExpression("min", AggregateExpressionExp(AggregationType::kMin,
                                                    ColumnValueExp("score"))),
      NamedExpression("max", AggregateExpressionExp(AggregationType::kMax,
                                                    ColumnValueExp("score")))};
  AggregationExecutor agg(std::move(fs), tbl->GetSchema(),
                          std::move(aggregates));
  Row result;

  // Act -- advance Aggregation cursor to compute aggregates over all rows
  ASSERT_TRUE(agg.Next(&result, nullptr));

  // Assert -- aggregated values match expected statistics
  ASSERT_EQ(result[0], Value(4));
  // Accumulating doubles loses low-order bits, so a strict equality against
  // the decimal literal is not a stable contract; allow rounding slack.
  EXPECT_NEAR(result[1].value.double_value, 22.44, 1e-9);
  EXPECT_NEAR(result[2].value.double_value, 5.61, 1e-9);
  ASSERT_EQ(result[3], Value(1.2));
  ASSERT_EQ(result[4], Value(12.2));
  ASSERT_FALSE(agg.Next(&result, nullptr));
}

TEST_F(ExecutorTest, MixedAggregateKeepsBitAndResult) {
  // PRODUCTION BUG (fixed): when a typed aggregate (BIT_AND) shared the row
  // loop with a generic one (STRING_AGG), the typed aggregate owned no
  // accumulator and the row-wise switch had no BIT_* cases, so it silently
  // produced NULL.
  const Schema schema("bits", {Column("x", ValueType::kInt64),
                               Column("y", ValueType::kVarChar)});
  auto src = std::make_shared<ConstantExecutor>(std::vector<Row>{
      Row({Value(int64_t{5}), Value("a")}),
      Row({Value(int64_t{1}), Value("b")}),
      Row({Value(int64_t{3}), Value("c")}),
  });
  std::vector<NamedExpression> aggregates = {
      NamedExpression("bit_and",
                      AggregateExpressionExp(AggregationType::kBitAnd,
                                             ColumnValueExp("x"))),
      NamedExpression("str", AggregateExpressionExp(AggregationType::kStringAgg,
                                                    ColumnValueExp("y")))};
  AggregationExecutor agg(src, schema, aggregates);
  Row result;
  ASSERT_TRUE(agg.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(int64_t{1}));  // 5 & 1 & 3
  EXPECT_EQ(result[1], Value("a,b,c"));
}

TEST_F(ExecutorTest, MinMaxWithNaNIsNaNAcrossPaths) {
  // PRODUCTION BUG (fixed): the typed fast paths skipped NaN (IEEE compares
  // false) while the ground truth accumulator returns NaN for any group
  // containing NaN, so the result depended on the execution path.
  const Schema schema("nans", {Column("v", ValueType::kDouble)});
  auto make_src = [] {
    return std::make_shared<ConstantExecutor>(std::vector<Row>{
        Row({Value(std::numeric_limits<double>::quiet_NaN())}),
        Row({Value(3.0)}),
    });
  };
  auto make_aggregates = [] {
    return std::vector<NamedExpression>{
        NamedExpression("min", AggregateExpressionExp(AggregationType::kMin,
                                                      ColumnValueExp("v"))),
        NamedExpression("max", AggregateExpressionExp(AggregationType::kMax,
                                                      ColumnValueExp("v")))};
  };

  AggregationExecutor serial(make_src(), schema, make_aggregates());
  Row serial_result;
  ASSERT_TRUE(serial.Next(&serial_result, nullptr));
  EXPECT_TRUE(std::isnan(serial_result[0].value.double_value));
  EXPECT_TRUE(std::isnan(serial_result[1].value.double_value));

  ParallelAggregationExecutor parallel(make_src(), schema, make_aggregates(),
                                       2);
  Row parallel_result;
  ASSERT_TRUE(parallel.Next(&parallel_result, nullptr));
  EXPECT_TRUE(std::isnan(parallel_result[0].value.double_value));
  EXPECT_TRUE(std::isnan(parallel_result[1].value.double_value));
}

TEST_F(ExecutorTest, VectorizedScanFilterProjectAggregatePipeline) {
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> table_status =
      ctx.GetTable(kTableName);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();
  const Schema input_schema = table->GetSchema();
  auto scan = std::make_shared<FullScan>(ctx.txn_, *table);
  auto filter = std::make_shared<Selection>(
      BinaryExpressionExp(ColumnValueExp("score"),
                          BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(4.0))),
      input_schema, scan);
  std::vector<NamedExpression> projections = {NamedExpression(
      "doubled",
      BinaryExpressionExp(ColumnValueExp("score"), BinaryOperation::kMultiply,
                          ConstantValueExp(Value(2.0))))};
  auto project = std::make_shared<Projection>(projections, input_schema,
                                              std::move(filter));
  const Schema projected_schema("projected",
                                {Column("doubled", ValueType::kDouble)});
  std::vector<NamedExpression> aggregates = {
      NamedExpression("count",
                      AggregateExpressionExp(AggregationType::kCount,
                                             ColumnValueExp("doubled"))),
      NamedExpression("sum",
                      AggregateExpressionExp(AggregationType::kSum,
                                             ColumnValueExp("doubled")))};
  AggregationExecutor aggregate(std::move(project), projected_schema,
                                std::move(aggregates));

  DataChunk output;
  ASSERT_EQ(aggregate.NextBatch(&output, 8), 1);
  ASSERT_EQ(output.Size(), 1);
  const Row result = output.RowAt(0);
  EXPECT_EQ(result[0], Value(3));
  // Accumulated double sum: allow rounding slack against the literal.
  EXPECT_NEAR(result[1].value.double_value, 42.48, 1e-9);
  EXPECT_EQ(aggregate.NextBatch(&output, 8), 0);
}

TEST_F(ExecutorTest, MorselDrivenParallelScanReadsEveryPageOnce) {
  constexpr std::string_view kParallelTable = "ParallelScanTable";
  constexpr int64_t kRows = 2000;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kParallelTable,
                  {Column("key", ValueType::kInt64),
                   Column("payload", ValueType::kVarChar)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      std::string payload = "row-" + std::to_string(key);
      payload.resize(100, 'x');
      ASSERT_SUCCESS(
          table
              .Insert(writer.txn_, Row({Value(key), Value(std::move(payload))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }

  TransactionContext reader = rs_->BeginReadOnlyContext();
  const StatusOr<std::shared_ptr<Table>> table_status =
      reader.GetTable(kParallelTable);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();
  ParallelScan scan(reader.txn_, *table, 4, 1);
  ASSERT_GT(scan.MorselCount(), 4U);
  EXPECT_EQ(scan.WorkerCount(), 4U);

  std::unordered_set<int64_t> keys;
  keys.reserve(kRows);
  DataChunk chunk;
  while (scan.NextBatch(&chunk, 37) != 0) {
    ASSERT_LE(chunk.Size(), 37U);
    for (size_t row = 0; row < chunk.Size(); ++row) {
      EXPECT_TRUE(
          keys.insert(chunk.ColumnAt(0).ValueAt(row).value.int_value).second);
    }
  }
  EXPECT_EQ(keys.size(), static_cast<size_t>(kRows));
  EXPECT_TRUE(keys.contains(0));
  EXPECT_TRUE(keys.contains(kRows - 1));
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(ExecutorTest, FusedTypedScanAggregatesNullableOldSnapshot) {
  constexpr std::string_view name = "FoldSnapshot";
  std::vector<RowPosition> positions;
  positions.reserve(2000);
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, table,
        rs_->CreateTable(
            writer, Schema(name, {Column("value", ValueType::kInt64),
                                  Column("padding", ValueType::kVarChar)})));
    for (int64_t i = 0; i < 2000; ++i) {
      ASSIGN_OR_ASSERT_FAIL(
          RowPosition, position,
          table.Insert(writer.txn_, Row({i % 5 == 0 ? Value() : Value(i),
                                         Value(std::string(100, 'x'))})));
      positions.push_back(position);
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  auto old = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, old.GetTable(name));
  auto old_scan = std::make_shared<ParallelScan>(old.txn_, *table, 4, 1,
                                                 std::vector<slot_t>{0});
  ASSERT_GT(old_scan->MorselCount(), 4U);
  {
    auto writer = rs_->BeginContext();
    ASSERT_SUCCESS(table
                       ->Update(writer.txn_, positions[0],
                                Row({Value(99), Value(std::string(100, 'x'))}))
                       .GetStatus());
    ASSERT_SUCCESS(table->Delete(writer.txn_, positions[1]));
    ASSERT_SUCCESS(writer.PreCommit());
  }
  const Schema schema(name, {Column("value", ValueType::kInt64)});
  const std::vector<NamedExpression> aggregates{
      NamedExpression("rows",
                      AggregateExpressionExp(AggregationType::kCount, nullptr)),
      NamedExpression("count", AggregateExpressionExp(AggregationType::kCount,
                                                      ColumnValueExp("value"))),
      NamedExpression("sum", AggregateExpressionExp(AggregationType::kSum,
                                                    ColumnValueExp("value"))),
      NamedExpression("min", AggregateExpressionExp(AggregationType::kMin,
                                                    ColumnValueExp("value"))),
      NamedExpression("max", AggregateExpressionExp(AggregationType::kMax,
                                                    ColumnValueExp("value")))};
  AggregationExecutor before(old_scan, schema, aggregates);
  Row row;
  ASSERT_TRUE(before.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(2000), Value(1600), Value(1600000), Value(1),
                      Value(1999)}));
  EXPECT_TRUE(old_scan->WasTypedFolded());
  EXPECT_FALSE(before.Next(&row, nullptr));
  DataChunk exhausted;
  EXPECT_EQ(old_scan->NextBatch(&exhausted), 0U);
  ASSERT_SUCCESS(before.GetStatus());
  ASSERT_SUCCESS(old.PreCommit());
  auto current = rs_->BeginReadOnlyContext();
  auto current_scan = std::make_shared<ParallelScan>(current.txn_, *table, 4, 1,
                                                     std::vector<slot_t>{0});
  AggregationExecutor after(current_scan, schema, aggregates);
  ASSERT_TRUE(after.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(1999), Value(1600), Value(1600098), Value(2),
                      Value(1999)}));
  EXPECT_TRUE(current_scan->WasTypedFolded());
  ASSERT_SUCCESS(after.GetStatus());
  ASSERT_SUCCESS(current.PreCommit());
}

TEST_F(ExecutorTest, FusedTypedScanPreservesIntermediateSumOverflow) {
  const int64_t hi = std::numeric_limits<int64_t>::max();
  const int64_t lo = std::numeric_limits<int64_t>::min();
  const std::vector<std::vector<int64_t>> cases{
      {hi, 0, 1, -1}, {lo, 0, -1, 1}, {-hi, 0, hi, hi}, {hi, 0, -hi, -hi}};
  for (size_t i = 0; i < cases.size(); ++i) {
    const std::string name = "FoldOverflow" + std::to_string(i);
    {
      auto writer = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(
          Table, table,
          rs_->CreateTable(
              writer, Schema(name, {Column("value", ValueType::kInt64),
                                    Column("padding", ValueType::kVarChar)})));
      for (int64_t value : cases[i]) {
        ASSERT_SUCCESS(
            table
                .Insert(writer.txn_,
                        Row({Value(value), Value(std::string(12000, 'x'))}))
                .GetStatus());
      }
      ASSERT_SUCCESS(writer.PreCommit());
    }
    auto reader = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, reader.GetTable(name));
    const std::vector<NamedExpression> aggregates{NamedExpression(
        "sum", AggregateExpressionExp(AggregationType::kSum,
                                      ColumnValueExp("value")))};
    Row expected;
    std::vector<Row> reference_rows;
    reference_rows.reserve(cases[i].size());
    for (int64_t value : cases[i]) {
      reference_rows.emplace_back(std::vector<Value>{Value(value)});
    }
    AggregationExecutor serial(
        std::make_shared<ConstantExecutor>(std::move(reference_rows)),
        Schema(name, {Column("value", ValueType::kInt64)}), aggregates);
    const bool serial_success = serial.Next(&expected, nullptr);
    EXPECT_EQ(serial_success, i >= 2);
    auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 4, 1,
                                               std::vector<slot_t>{0});
    ASSERT_GE(scan->MorselCount(), 2U);
    AggregationExecutor fused(
        scan, Schema(name, {Column("value", ValueType::kInt64)}), aggregates);
    Row actual;
    EXPECT_EQ(fused.Next(&actual, nullptr), serial_success);
    EXPECT_TRUE(scan->WasTypedFolded());
    if (serial_success) {
      EXPECT_EQ(actual, expected);
      ASSERT_SUCCESS(fused.GetStatus());
    } else {
      EXPECT_EQ(fused.GetStatus(), Status::kIsInfinity);
      EXPECT_EQ(serial.GetStatus(), Status::kIsInfinity);
    }
    ASSERT_SUCCESS(reader.PreCommit());
  }
}

TEST_F(ExecutorTest, FusedTypedScanEmptyAndAllNullAggregates) {
  for (int count : {0, 20}) {
    const std::string name = "FoldNull" + std::to_string(count);
    {
      auto writer = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(
          Table, table,
          rs_->CreateTable(writer,
                           Schema(name, {Column("value", ValueType::kInt64)})));
      for (int i = 0; i < count; ++i) {
        ASSERT_SUCCESS(table.Insert(writer.txn_, Row({Value()})).GetStatus());
      }
      ASSERT_SUCCESS(writer.PreCommit());
    }
    auto reader = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, reader.GetTable(name));
    auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 4, 1);
    AggregationExecutor aggregate(
        scan, table->GetSchema(),
        {NamedExpression(
             "rows", AggregateExpressionExp(AggregationType::kCount, nullptr)),
         NamedExpression("count",
                         AggregateExpressionExp(AggregationType::kCount,
                                                ColumnValueExp("value"))),
         NamedExpression("sum",
                         AggregateExpressionExp(AggregationType::kSum,
                                                ColumnValueExp("value")))});
    Row row;
    ASSERT_TRUE(aggregate.Next(&row, nullptr));
    EXPECT_TRUE(scan->WasTypedFolded());
    EXPECT_EQ(row, Row({Value(count), Value(0), Value()}));
    ASSERT_SUCCESS(aggregate.GetStatus());
    ASSERT_SUCCESS(reader.PreCommit());
  }
}

TEST_F(ExecutorTest, FusedTypedScanSurfacesConsumerFailure) {
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable(kTableName));
  ParallelScan scan(reader.txn_, *table, 4, 1, std::vector<slot_t>{0});
  StatusOr<bool> result = scan.TryFoldTypedMorsels(
      [](size_t, const TypedCellBatch&) { return Status::kConflicts; });
  EXPECT_FALSE(result.HasValue());
  EXPECT_EQ(result.GetStatus(), Status::kConflicts);
  EXPECT_EQ(scan.GetStatus(), Status::kConflicts);
  DataChunk chunk;
  EXPECT_EQ(scan.NextBatch(&chunk), 0U);
  ASSERT_SUCCESS(reader.PreCommit());
}

// The worker-side row filter must decide rows inside the decode workers and
// hand chunks out exactly as a serial Selection over the same conjuncts
// would: survivors only, once each, no rows lost across morsel boundaries.
TEST_F(ExecutorTest, MorselDrivenParallelScanWorkerFilterDecidesRows) {
  constexpr std::string_view kFilteredTable = "ParallelFilteredTable";
  constexpr int64_t kRows = 2000;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kFilteredTable,
                  {Column("key", ValueType::kInt64),
                   Column("payload", ValueType::kVarChar)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      std::string payload = "row-" + std::to_string(key);
      payload.resize(100, 'x');
      ASSERT_SUCCESS(
          table
              .Insert(writer.txn_, Row({Value(key), Value(std::move(payload))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }

  TransactionContext reader = rs_->BeginReadOnlyContext();
  const StatusOr<std::shared_ptr<Table>> table_status =
      reader.GetTable(kFilteredTable);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();

  // One simple compare (worker fast path) plus one residual (bytecode):
  // `key >= 10 AND key % 2 = 0`.
  const std::vector<Expression> conjuncts{
      BinaryExpressionExp(ColumnValueExp("key"),
                          BinaryOperation::kGreaterThanEquals,
                          ConstantValueExp(Value(int64_t{10}))),
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kModulo,
                              ConstantValueExp(Value(int64_t{2}))),
          BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{0})))};
  const Schema& schema = table->GetSchema();
  const relational_detail::CompiledScanFilter filter =
      relational_detail::CompileScanFilter(conjuncts, schema);
  relational_detail::WorkerScanFilterPrograms programs =
      relational_detail::CompileWorkerFilter(filter, schema);
  ASSERT_TRUE(programs.complete)
      << "residual bytecode must compile for the worker filter test";

  ParallelScan scan(reader.txn_, *table, 4, 1);
  ASSERT_TRUE(scan.CanTakeRowFilter());
  scan.SetRowFilter(conjuncts, filter, std::move(programs));

  std::unordered_set<int64_t> keys;
  keys.reserve(kRows);
  DataChunk chunk;
  while (scan.NextBatch(&chunk, 53) != 0) {
    for (size_t row = 0; row < chunk.Size(); ++row) {
      EXPECT_TRUE(
          keys.insert(chunk.ColumnAt(0).ValueAt(row).value.int_value).second);
    }
  }
  ASSERT_SUCCESS(scan.GetStatus());
  for (int64_t key = 0; key < kRows; ++key) {
    EXPECT_EQ(keys.contains(key), key >= 10 && key % 2 == 0) << key;
  }
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

// Rows whose worker program raises are parked for the contextual re-check on
// the consumer thread, and the first such error surfaces as the scan's
// sticky status: a raising row must not be silently dropped.
TEST_F(ExecutorTest, MorselDrivenParallelScanSurfacesDeferredRowErrors) {
  constexpr std::string_view kDeferredTable = "ParallelDeferredTable";
  constexpr int64_t kRows = 2000;
  constexpr int64_t kBoomKey = 500;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kDeferredTable,
                  {Column("key", ValueType::kInt64),
                   Column("payload", ValueType::kVarChar)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      std::string payload = "row-" + std::to_string(key);
      payload.resize(100, 'x');
      ASSERT_SUCCESS(
          table
              .Insert(writer.txn_, Row({Value(key), Value(std::move(payload))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }

  TransactionContext reader = rs_->BeginReadOnlyContext();
  const StatusOr<std::shared_ptr<Table>> table_status =
      reader.GetTable(kDeferredTable);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();

  // `1000 / (500 - key) = 1`: INT64 division promotes to FLOAT64, so within
  // the key domain [0, 2000) no row satisfies it (the exact quotient 1.0
  // would need key == -500) -- every row is a clean rejection except
  // key == 500, where the division by zero raises inside the worker program
  // and parks the row for the consumer-side re-check.
  const std::vector<Expression> conjuncts{BinaryExpressionExp(
      BinaryExpressionExp(ConstantValueExp(Value(int64_t{1000})),
                          BinaryOperation::kDivide,
                          BinaryExpressionExp(ConstantValueExp(Value(kBoomKey)),
                                              BinaryOperation::kSubtract,
                                              ColumnValueExp("key"))),
      BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{1})))};
  const Schema& schema = table->GetSchema();
  const relational_detail::CompiledScanFilter filter =
      relational_detail::CompileScanFilter(conjuncts, schema);
  relational_detail::WorkerScanFilterPrograms programs =
      relational_detail::CompileWorkerFilter(filter, schema);
  ASSERT_TRUE(programs.complete);

  ParallelScan scan(reader.txn_, *table, 4, 1);
  scan.SetRowFilter(conjuncts, filter, std::move(programs));

  std::unordered_set<int64_t> keys;
  keys.reserve(kRows);
  DataChunk chunk;
  while (scan.NextBatch(&chunk, 53) != 0) {
    for (size_t row = 0; row < chunk.Size(); ++row) {
      EXPECT_TRUE(
          keys.insert(chunk.ColumnAt(0).ValueAt(row).value.int_value).second);
    }
  }
  EXPECT_NE(scan.GetStatus(), Status::kSuccess)
      << "the deferred raising row must surface as a sticky error";
  // The raising row must not be silently dropped into the result as a
  // match or a non-match: the scan ends in an error, never a row.
  EXPECT_TRUE(keys.empty());
  EXPECT_FALSE(keys.contains(kBoomKey));
  reader.txn_.PreCommit();
}

TEST_F(ExecutorTest, ThreadLocalAggregationMergesPartialAndDistinctStates) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<SyntheticBatchExecutor>(10000);
  std::vector<NamedExpression> aggregates = {
      NamedExpression("count", AggregateExpressionExp(AggregationType::kCount,
                                                      ColumnValueExp("value"))),
      NamedExpression("sum", AggregateExpressionExp(AggregationType::kSum,
                                                    ColumnValueExp("value"))),
      NamedExpression("avg", AggregateExpressionExp(AggregationType::kAvg,
                                                    ColumnValueExp("value"))),
      NamedExpression("min", AggregateExpressionExp(AggregationType::kMin,
                                                    ColumnValueExp("value"))),
      NamedExpression("max", AggregateExpressionExp(AggregationType::kMax,
                                                    ColumnValueExp("value"))),
      NamedExpression("distinct_count",
                      AggregateExpressionExp(AggregationType::kCount,
                                             ColumnValueExp("value"), true))};
  ParallelAggregationExecutor aggregate(input, schema, std::move(aggregates),
                                        4);

  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(10000));
  EXPECT_EQ(result[1], Value(495000));
  EXPECT_EQ(result[2], Value(49.5));
  EXPECT_EQ(result[3], Value(0));
  EXPECT_EQ(result[4], Value(99));
  EXPECT_EQ(result[5], Value(100));
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

TEST_F(ExecutorTest, ParallelSortPreservesOrderAndStableTies) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<SyntheticBatchExecutor>(10000);
  SortExecutor sort(input, schema,
                    {{ColumnValueExp("value"), true, std::nullopt}}, 4);

  int64_t previous_value = -1;
  slot_t previous_position = 0;
  size_t count = 0;
  Row row;
  RowPosition position;
  while (sort.Next(&row, &position)) {
    const int64_t value = row[0].value.int_value;
    EXPECT_GE(value, previous_value);
    if (value == previous_value) {
      EXPECT_GT(position.slot, previous_position);
    }
    previous_value = value;
    previous_position = position.slot;
    ++count;
  }
  EXPECT_EQ(count, 10000U);
  EXPECT_EQ(previous_value, 99);
}

// ===== DistinctExecutor =====
TEST_F(ExecutorTest, DistinctEmptyInput) {
  DistinctExecutor distinct(
      std::make_shared<ConstantExecutor>(std::vector<Row>{}));
  Row row;
  RowPosition pos;
  ASSERT_FALSE(distinct.Next(&row, &pos));
  ASSERT_FALSE(distinct.Next(&row, nullptr));
}

TEST_F(ExecutorTest, DistinctAllDuplicates) {
  std::vector<Row> rows;
  rows.reserve(6);
  for (int i = 0; i < 6; ++i) {
    rows.emplace_back(std::vector<Value>{Value(7)});
  }
  DistinctExecutor distinct(
      std::make_shared<ConstantExecutor>(std::move(rows)));
  Row row;
  RowPosition pos;
  ASSERT_TRUE(distinct.Next(&row, &pos));
  EXPECT_EQ(row, Row({Value(7)}));
  ASSERT_FALSE(distinct.Next(&row, nullptr));
}

TEST_F(ExecutorTest, DistinctAlreadySortedInput) {
  std::vector<Row> rows{Row({Value(1)}), Row({Value(2)}), Row({Value(2)}),
                        Row({Value(3)}), Row({Value(4)}), Row({Value(4)})};
  DistinctExecutor distinct(
      std::make_shared<ConstantExecutor>(std::move(rows)));
  Row row;
  RowPosition pos;
  int64_t expected = 1;
  while (distinct.Next(&row, &pos)) {
    EXPECT_EQ(row, Row({Value(expected)}));
    ++expected;
  }
  EXPECT_EQ(expected, 5);
}

TEST_F(ExecutorTest, DistinctUnsortedInterleavedDuplicates) {
  std::vector<Row> rows{Row({Value(3)}), Row({Value(1)}), Row({Value(3)}),
                        Row({Value(2)}), Row({Value(1)}), Row({Value(4)})};
  DistinctExecutor distinct(
      std::make_shared<ConstantExecutor>(std::move(rows)));
  Row row;
  RowPosition pos;
  std::unordered_set<Value> seen;
  size_t count = 0;
  while (distinct.Next(&row, &pos)) {
    ASSERT_TRUE(seen.insert(row[0]).second);
    ++count;
  }
  EXPECT_EQ(count, 4U);
  EXPECT_EQ(seen, std::unordered_set<Value>(
                      {Value(1), Value(2), Value(3), Value(4)}));
}

TEST_F(ExecutorTest, DistinctMultipleColumns) {
  std::vector<Row> rows{
      Row({Value(1), Value("a")}), Row({Value(1), Value("a")}),
      Row({Value(1), Value("b")}), Row({Value(2), Value("a")}),
      Row({Value(2), Value("a")}), Row({Value(2), Value("c")})};
  DistinctExecutor distinct(
      std::make_shared<ConstantExecutor>(std::move(rows)));
  Row row;
  RowPosition pos;
  std::unordered_set<Row> seen;
  size_t count = 0;
  while (distinct.Next(&row, &pos)) {
    ASSERT_TRUE(seen.insert(row).second);
    ++count;
  }
  EXPECT_EQ(count, 4U);
  EXPECT_TRUE(seen.contains(Row({Value(1), Value("b")})));
  EXPECT_TRUE(seen.contains(Row({Value(2), Value("c")})));
}

TEST_F(ExecutorTest, DistinctNullValueTreatedAsValue) {
  // std::hash<Value> maps kNull to a constant (it no longer throws), so
  // DistinctExecutor deduplicates NULL-containing rows like any other value.
  std::vector<Row> rows{Row({Value()}), Row({Value()}), Row({Value(5)})};
  DistinctExecutor distinct(
      std::make_shared<ConstantExecutor>(std::move(rows)));
  Row row;
  RowPosition pos;
  size_t count = 0;
  bool saw_null = false;
  while (distinct.Next(&row, &pos)) {
    if (row[0].IsNull()) {
      saw_null = true;
    }
    ++count;
  }
  EXPECT_EQ(count, 2U);
  EXPECT_TRUE(saw_null);
}

TEST_F(ExecutorTest, DistinctOverFullScanPreservesRowsAndPositions) {
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  DistinctExecutor distinct(std::make_shared<FullScan>(ctx.txn_, *tbl));
  LOG(INFO) << distinct;
  std::unordered_set<Row> expected(
      {Row({Value(0), Value("hello"), Value(1.2)}),
       Row({Value(3), Value("piyo"), Value(12.2)}),
       Row({Value(1), Value("world"), Value(4.9)}),
       Row({Value(2), Value("arise"), Value(4.14)})});
  Row row;
  RowPosition pos;
  size_t count = 0;
  while (distinct.Next(&row, &pos)) {
    ASSERT_NE(expected.find(row), expected.end());
    ASSERT_TRUE(pos.IsValid());
    ++count;
  }
  EXPECT_EQ(count, 4U);
  std::stringstream ss;
  distinct.Dump(ss, 0);
  EXPECT_NE(ss.str().find("Distinct"), std::string::npos);
}

// ===== CrossJoin =====
TEST_F(ExecutorTest, CrossJoinProducesCartesianProduct) {
  auto left = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)})});
  auto right = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value("a")}), Row({Value("b")})});
  CrossJoin join(left, right);
  std::unordered_set<Row> expected(
      {Row({Value(1), Value("a")}), Row({Value(1), Value("b")}),
       Row({Value(2), Value("a")}), Row({Value(2), Value("b")})});
  Row got;
  RowPosition pos;
  size_t count = 0;
  while (join.Next(&got, &pos)) {
    ASSERT_NE(expected.find(got), expected.end());
    ++count;
  }
  EXPECT_EQ(count, 4U);
  std::stringstream ss;
  join.Dump(ss, 0);
  EXPECT_NE(ss.str().find("CrossJoin"), std::string::npos);
}

TEST_F(ExecutorTest, CrossJoinEmptyRightSideYieldsNoRows) {
  auto left = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)})});
  auto right = std::make_shared<ConstantExecutor>(std::vector<Row>{});
  CrossJoin join(left, right);
  Row got;
  RowPosition pos;
  ASSERT_FALSE(join.Next(&got, &pos));
}

TEST_F(ExecutorTest, CrossJoinEmptyLeftSideYieldsNoRows) {
  auto left = std::make_shared<ConstantExecutor>(std::vector<Row>{});
  auto right = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value("a")}), Row({Value("b")})});
  CrossJoin join(left, right);
  Row got;
  ASSERT_FALSE(join.Next(&got, nullptr));
}

TEST_F(ExecutorTest, CrossJoinBothSidesEmptyYieldsNoRows) {
  auto left = std::make_shared<ConstantExecutor>(std::vector<Row>{});
  auto right = std::make_shared<ConstantExecutor>(std::vector<Row>{});
  CrossJoin join(left, right);
  Row got;
  DataChunk chunk;
  ASSERT_FALSE(join.Next(&got, nullptr));
  EXPECT_EQ(join.NextBatch(&chunk, 8), 0U);
}

// ===== DeleteExecutor =====
TEST_F(ExecutorTest, DeleteRemovesAllRows) {
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  auto deleter = std::make_shared<DeleteExecutor>(
      ctx.txn_, *tbl, std::make_shared<FullScan>(ctx.txn_, *tbl));
  LOG(INFO) << *deleter;
  Row result;
  RowPosition rp;
  ASSERT_TRUE(deleter->Next(&result, &rp));
  EXPECT_EQ(result, Row({Value("Delete Rows"), Value(4)}));
  ASSERT_FALSE(deleter->Next(&result, &rp));
  Row got;
  FullScan fs(ctx.txn_, *tbl);
  ASSERT_FALSE(fs.Next(&got, nullptr));
  std::stringstream ss;
  deleter->Dump(ss, 0);
  EXPECT_NE(ss.str().find("Delete"), std::string::npos);
}

TEST_F(ExecutorTest, DeleteEmptySourceCountsZero) {
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  auto deleter = std::make_shared<DeleteExecutor>(
      ctx.txn_, *tbl, std::make_shared<ConstantExecutor>(std::vector<Row>{}));
  Row result;
  ASSERT_TRUE(deleter->Next(&result, nullptr));
  EXPECT_EQ(result, Row({Value("Delete Rows"), Value(0)}));
  ASSERT_FALSE(deleter->Next(&result, nullptr));
}

// ===== ConstantExecutor =====
TEST_F(ExecutorTest, ConstantExecutorNextBatchSplitsRows) {
  std::vector<Row> rows{Row({Value(1)}), Row({Value(2)}), Row({Value(3)})};
  ConstantExecutor ce(std::move(rows));
  DataChunk chunk;
  EXPECT_EQ(ce.NextBatch(&chunk, 2), 2U);
  EXPECT_EQ(chunk.RowAt(0), Row({Value(1)}));
  EXPECT_EQ(ce.NextBatch(&chunk, 2), 1U);
  EXPECT_EQ(ce.NextBatch(&chunk, 2), 0U);
  ConstantExecutor single(Row({Value(9)}));
  Row got;
  ASSERT_TRUE(single.Next(&got, nullptr));
  EXPECT_EQ(got, Row({Value(9)}));
  ASSERT_FALSE(single.Next(&got, nullptr));
  std::stringstream ss;
  ce.Dump(ss, 0);
  EXPECT_NE(ss.str().find("ConstantExecutor"), std::string::npos);
}

// ===== ZoneMap =====
TEST_F(ExecutorTest, ZoneMapTracksStats) {
  ZoneMap map;
  EXPECT_FALSE(map.Initialized());
  EXPECT_EQ(map.NullCount(), 0U);
  EXPECT_EQ(map.ValueCount(), 0U);
  EXPECT_FALSE(map.Minimum().has_value());
  EXPECT_FALSE(map.Maximum().has_value());
  map.Add(Value(10));
  map.Add(Value(3));
  map.Add(Value(7));
  map.Add(Value());
  EXPECT_TRUE(map.Initialized());
  EXPECT_EQ(map.NullCount(), 1U);
  EXPECT_EQ(map.ValueCount(), 3U);
  const std::optional<Value> minimum = map.Minimum();
  const std::optional<Value> maximum = map.Maximum();
  if (!minimum || !maximum) {
    GTEST_FAIL() << "failed";
    return;
  }
  EXPECT_EQ(*minimum, Value(3));
  EXPECT_EQ(*maximum, Value(10));
  map.Reset();
  EXPECT_FALSE(map.Initialized());
  EXPECT_EQ(map.NullCount(), 0U);
  EXPECT_EQ(map.ValueCount(), 0U);
  EXPECT_FALSE(map.Minimum().has_value());
  EXPECT_FALSE(map.Maximum().has_value());
}

TEST_F(ExecutorTest, ZoneMapMayMatchComparisons) {
  ZoneMap map;
  map.Add(Value(5));
  map.Add(Value(10));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kEquals, Value(5)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kEquals, Value(7)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kEquals, Value(4)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kEquals, Value(11)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kNotEquals, Value(5)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kNotEquals, Value(4)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kLessThan, Value(11)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kLessThan, Value(5)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kLessThanEquals, Value(5)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kLessThanEquals, Value(4)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kGreaterThan, Value(4)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kGreaterThan, Value(10)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kGreaterThanEquals, Value(10)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kGreaterThanEquals, Value(11)));
}

TEST_F(ExecutorTest, ZoneMapMayMatchDegenerateCases) {
  ZoneMap single;
  single.Add(Value(7));
  EXPECT_FALSE(single.MayMatch(BinaryOperation::kNotEquals, Value(7)));
  EXPECT_TRUE(single.MayMatch(BinaryOperation::kNotEquals, Value(6)));
  EXPECT_TRUE(single.MayMatch(BinaryOperation::kAdd, Value(1)));
  EXPECT_FALSE(single.MayMatch(BinaryOperation::kEquals, Value()));
  ZoneMap empty;
  // An uninitialized zone map proves nothing: the producer may not have
  // maintained it, so MayMatch must stay conservative (§6.8).
  EXPECT_FALSE(empty.Initialized());
  EXPECT_TRUE(empty.MayMatch(BinaryOperation::kEquals, Value(1)));
  ZoneMap null_only;
  null_only.AddNull();
  // An initialized zone holding only NULLs can never satisfy a comparison.
  EXPECT_TRUE(null_only.Initialized());
  EXPECT_FALSE(null_only.MayMatch(BinaryOperation::kEquals, Value(1)));
  ZoneMap doubles;
  doubles.Add(Value(1.0));
  // Type-mismatched constants must be conservative: the comparison may match
  // after implicit conversion, so pruning is not allowed.
  EXPECT_TRUE(doubles.MayMatch(BinaryOperation::kEquals, Value(1)));
  EXPECT_TRUE(doubles.MayMatch(BinaryOperation::kLessThan, Value(2)));
}

// ===== Selection edge cases =====
TEST_F(ExecutorTest, SelectionConstantOnLeftIsFlipped) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  auto collect_keys = [&](const Expression& exp) {
    Selection sel(exp, tbl->GetSchema(),
                  std::make_shared<FullScan>(ctx.txn_, *tbl));
    std::vector<int64_t> keys;
    Row got;
    while (sel.Next(&got, nullptr)) {
      keys.push_back(got[0].value.int_value);
    }
    std::ranges::sort(keys);
    return keys;
  };
  EXPECT_EQ(collect_keys(BinaryExpressionExp(ConstantValueExp(Value(2)),
                                             BinaryOperation::kLessThan,
                                             ColumnValueExp("key"))),
            std::vector<int64_t>({3}));
  EXPECT_EQ(collect_keys(BinaryExpressionExp(ConstantValueExp(Value(2)),
                                             BinaryOperation::kLessThanEquals,
                                             ColumnValueExp("key"))),
            std::vector<int64_t>({2, 3}));
  EXPECT_EQ(collect_keys(BinaryExpressionExp(ConstantValueExp(Value(3)),
                                             BinaryOperation::kGreaterThan,
                                             ColumnValueExp("key"))),
            std::vector<int64_t>({0, 1, 2}));
  EXPECT_EQ(collect_keys(BinaryExpressionExp(
                ConstantValueExp(Value(3)), BinaryOperation::kGreaterThanEquals,
                ColumnValueExp("key"))),
            std::vector<int64_t>({0, 1, 2, 3}));
  EXPECT_EQ(collect_keys(BinaryExpressionExp(ConstantValueExp(Value(3)),
                                             BinaryOperation::kEquals,
                                             ColumnValueExp("key"))),
            std::vector<int64_t>({3}));
  EXPECT_EQ(collect_keys(BinaryExpressionExp(ConstantValueExp(Value(3)),
                                             BinaryOperation::kNotEquals,
                                             ColumnValueExp("key"))),
            std::vector<int64_t>({0, 1, 2}));
}

TEST_F(ExecutorTest, SelectionSupportsOrPredicate) {
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  Expression key_is_0 =
      BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(0)));
  Expression key_is_3 =
      BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(3)));
  Selection sel(BinaryExpressionExp(key_is_0, BinaryOperation::kOr, key_is_3),
                tbl->GetSchema(), std::make_shared<FullScan>(ctx.txn_, *tbl));
  Row got;
  std::unordered_set<Value> keys;
  while (sel.Next(&got, nullptr)) {
    keys.insert(got[0]);
  }
  EXPECT_EQ(keys, std::unordered_set<Value>({Value(0), Value(3)}));
}

// ===== ParallelScan =====
TEST_F(ExecutorTest, ParallelScanEmptyTableReturnsNoRows) {
  constexpr std::string_view kTable = "EmptyScanTable";
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kTable, {Column("key", ValueType::kInt64)}};
    const StatusOr<Table> table_status = rs_->CreateTable(writer, schema);
    ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  TransactionContext reader = rs_->BeginReadOnlyContext();
  const StatusOr<std::shared_ptr<Table>> table_status = reader.GetTable(kTable);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();
  ParallelScan scan(reader.txn_, *table, 2, 1);
  DataChunk chunk;
  EXPECT_EQ(scan.NextBatch(&chunk), 0U);
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(ExecutorTest, ParallelScanNextScalarPath) {
  constexpr std::string_view kTable = "ParallelScalarTable";
  constexpr int64_t kRows = 500;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kTable, {Column("key", ValueType::kInt64)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      ASSERT_SUCCESS(table.Insert(writer.txn_, Row({Value(key)})).GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  TransactionContext reader = rs_->BeginReadOnlyContext();
  const StatusOr<std::shared_ptr<Table>> table_status = reader.GetTable(kTable);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();
  ParallelScan scan(reader.txn_, *table, 2, 1);
  LOG(INFO) << scan;
  std::unordered_set<int64_t> keys;
  keys.reserve(static_cast<size_t>(kRows));
  Row row;
  RowPosition pos;
  size_t count = 0;
  while (scan.Next(&row, &pos)) {
    ASSERT_TRUE(keys.insert(row[0].value.int_value).second);
    ASSERT_TRUE(pos.IsValid());
    ++count;
  }
  EXPECT_EQ(count, static_cast<size_t>(kRows));
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(ExecutorTest, ParallelScanProjectedColumns) {
  constexpr std::string_view kTable = "ParallelProjectionTable";
  constexpr int64_t kRows = 800;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kTable,
                  {Column("key", ValueType::kInt64),
                   Column("name", ValueType::kVarChar)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      ASSERT_SUCCESS(
          table
              .Insert(writer.txn_,
                      Row({Value(key), Value("n" + std::to_string(key))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  TransactionContext reader = rs_->BeginReadOnlyContext();
  const StatusOr<std::shared_ptr<Table>> table_status = reader.GetTable(kTable);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();
  ParallelScan scan(reader.txn_, *table, 2, 1, std::vector<slot_t>{0, 1});
  DataChunk chunk;
  size_t total = 0;
  std::unordered_set<std::string> names;
  names.reserve(static_cast<size_t>(kRows));
  while (scan.NextBatch(&chunk, 64) != 0) {
    ASSERT_EQ(chunk.ColumnCount(), 2U);
    for (size_t row = 0; row < chunk.Size(); ++row) {
      names.insert(chunk.ColumnAt(0).ValueAt(row).AsString());
    }
    total += chunk.Size();
  }
  EXPECT_EQ(total, static_cast<size_t>(kRows));
  EXPECT_EQ(names.size(), static_cast<size_t>(kRows));
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(ExecutorTest, ParallelScanRejectsReorderedProjection) {
  constexpr std::string_view kTable = "ParallelProjectionTable";
  constexpr int64_t kRows = 800;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kTable,
                  {Column("key", ValueType::kInt64),
                   Column("name", ValueType::kVarChar)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      ASSERT_SUCCESS(
          table
              .Insert(writer.txn_,
                      Row({Value(key), Value("n" + std::to_string(key))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  TransactionContext reader = rs_->BeginReadOnlyContext();
  const StatusOr<std::shared_ptr<Table>> table_status = reader.GetTable(kTable);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();
  ParallelScan scan(reader.txn_, *table, 2, 1, std::vector<slot_t>{1, 0});
  DataChunk chunk;
  EXPECT_EQ(scan.NextBatch(&chunk, 64), 0U);
  EXPECT_EQ(scan.GetStatus().GetCode(), StatusCode::kInvalidArgument);
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(ExecutorTest, ParallelScanSplitsLargeChunksWhenAskedSmallBatches) {
  constexpr std::string_view kTable = "ParallelSplitTable";
  constexpr int64_t kRows = 2000;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kTable, {Column("key", ValueType::kInt64)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      ASSERT_SUCCESS(table.Insert(writer.txn_, Row({Value(key)})).GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  TransactionContext reader = rs_->BeginReadOnlyContext();
  const StatusOr<std::shared_ptr<Table>> table_status = reader.GetTable(kTable);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();
  ParallelScan scan(reader.txn_, *table, 4, 1);
  DataChunk chunk;
  ASSERT_GT(scan.NextBatch(&chunk, 1000), 0U);
  size_t total = chunk.Size();
  size_t batches = 0;
  while (true) {
    const size_t got = scan.NextBatch(&chunk, 1);
    if (got == 0) {
      break;
    }
    EXPECT_EQ(got, 1U);
    total += got;
    ++batches;
  }
  EXPECT_EQ(total, static_cast<size_t>(kRows));
  EXPECT_GT(batches, 10U);
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

// ===== ParallelAggregationExecutor =====
TEST_F(ExecutorTest, ParallelAggregationCountStarAndExpressionChild) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<SyntheticBatchExecutor>(100);
  std::vector<NamedExpression> aggregates = {
      NamedExpression(
          "count_star",
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"))),
      NamedExpression("sum_plus_one",
                      AggregateExpressionExp(
                          AggregationType::kSum,
                          BinaryExpressionExp(ColumnValueExp("value"),
                                              BinaryOperation::kAdd,
                                              ConstantValueExp(Value(1)))))};
  ParallelAggregationExecutor aggregate(input, schema, std::move(aggregates),
                                        2);
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(100));
  EXPECT_EQ(result[1], Value(5050));
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

TEST_F(ExecutorTest, ParallelAggregationRespectsWhereFilter) {
  // PRODUCTION BUG (fixed): the typed fast paths (row count / column scan)
  // ignored an aggregate's FILTER(WHERE) predicate, so a pushed-down filter
  // silently over-counted. The generic path must apply it.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<SyntheticBatchExecutor>(100);  // values 0..99
  auto sum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                   ColumnValueExp("value"));
  sum->SetWhereFilter(BinaryExpressionExp(ColumnValueExp("value"),
                                          BinaryOperation::kGreaterThan,
                                          ConstantValueExp(Value(50))));
  std::vector<NamedExpression> aggregates = {
      NamedExpression("sum_gt_50", std::move(sum))};
  ParallelAggregationExecutor aggregate(input, schema, std::move(aggregates),
                                        2);
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  // SyntheticBatchExecutor yields 0..99; value > 50 keeps 51..99 = 3675
  // (the unfiltered sum would be 4950).
  EXPECT_EQ(result[0], Value(3675));
}

TEST_F(ExecutorTest, ParallelAggregationAverageOverEmptyInput) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<ConstantExecutor>(std::vector<Row>{});
  std::vector<NamedExpression> aggregates = {NamedExpression(
      "avg",
      AggregateExpressionExp(AggregationType::kAvg, ColumnValueExp("value")))};
  ParallelAggregationExecutor aggregate(input, schema, std::move(aggregates),
                                        2);
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_TRUE(result[0].IsNull());
}

TEST_F(ExecutorTest, ParallelAggregationNextBatchAndDump) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<SyntheticBatchExecutor>(50);
  std::vector<NamedExpression> aggregates = {NamedExpression(
      "count", AggregateExpressionExp(AggregationType::kCount,
                                      ColumnValueExp("value")))};
  ParallelAggregationExecutor aggregate(input, schema, std::move(aggregates),
                                        3);
  LOG(INFO) << aggregate;
  DataChunk chunk;
  EXPECT_EQ(aggregate.NextBatch(&chunk, 8), 1U);
  EXPECT_EQ(chunk.RowAt(0)[0], Value(50));
  EXPECT_EQ(aggregate.NextBatch(&chunk, 8), 0U);
}

TEST_F(ExecutorTest, ParallelAggregationMissingColumnThrows) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input =
      std::make_shared<ConstantExecutor>(std::vector<Row>{Row({Value(2)})});
  std::vector<NamedExpression> aggregates = {NamedExpression(
      "sum_missing", AggregateExpressionExp(AggregationType::kSum,
                                            ColumnValueExp("missing")))};
  ParallelAggregationExecutor aggregate(input, schema, std::move(aggregates),
                                        2);
  Row result;
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
  EXPECT_NE(aggregate.GetStatus(), Status::kSuccess);
}

}  // namespace tinylamb
