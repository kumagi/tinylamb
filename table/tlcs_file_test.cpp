/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#include "table/tlcs_file.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "common/crc32c.hpp"
#include "common/memory_budget.hpp"
#include "common/random_string.hpp"
#include "common/runtime_filter.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "index/index_schema.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

std::vector<Row> ScanAllRows(Transaction& txn, const Table& table) {
  std::vector<Row> out;
  Iterator iter = table.BeginFullScan(txn);
  while (iter.IsValid()) {
    out.push_back(*iter);
    ++iter;
  }
  return out;
}

TEST(TlcsFileTest, MaterializeIntSliceAcceptsUnalignedOffsets) {
  const auto check = []<typename Offset>(const std::array<Offset, 3>& offsets) {
    alignas(Offset) std::array<uint8_t, 1 + (3 * sizeof(Offset))> bytes{};
    std::memcpy(bytes.data() + 1, offsets.data(), 3 * sizeof(Offset));
    LSTableColumnBlock block;
    block.for_width = sizeof(Offset);
    block.for_base = -42;
    block.for_bytes = std::span(bytes).subspan(1);
    std::array<int64_t, 2> result{};
    block.MaterializeIntSlice(1, result.size(), result.data());
    EXPECT_EQ(result[0], block.IntAt(1));
    EXPECT_EQ(result[1], block.IntAt(2));
  };
  check(std::array<uint16_t, 3>{0, 257, 65535});
  check(std::array<uint32_t, 3>{0, 65537, 4294967295U});
}

TEST(TlcsFileTest, DictionaryAccessChecksCodesAndStringOffsets) {
  LSTableColumnDictionary dictionary;
  dictionary.int_entries = {7, 42};
  EXPECT_EQ(dictionary.IntEntryAt(1), 42);
  EXPECT_EQ(dictionary.IntEntryAt(2), 0);

  const std::array<uint64_t, 5> offsets{0, 2, 1, 5, 9};
  const std::string bytes = "abc";
  dictionary.flat_str_offsets = offsets;
  dictionary.flat_str_bytes = bytes;
  EXPECT_EQ(dictionary.StringEntryAt(0), "ab");
  for (size_t code : {size_t{1}, size_t{2}, size_t{3}, size_t{4}, SIZE_MAX}) {
    EXPECT_TRUE(dictionary.StringEntryAt(code).empty()) << code;
  }
}

TEST(TlcsFileTest, MultiBlockResidentCompactionZoneMapsAndProjection) {
  const std::string prefix = "tlcs_resident_" + RandomString();
  Schema schema(
      "items",
      {Column("id", ValueType::kInt64), Column("tag", ValueType::kVarChar),
       Column("ratio", ValueType::kDouble),
       Column("shipdate", ValueType::kDate), Column("big", ValueType::kInt64)});

  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  page_id_t first_pid = 0;
  constexpr int64_t kRows = 35000;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    first_pid = tbl.FirstPageId();
    for (int64_t i = 0; i < kRows; ++i) {
      Value v_id(i);
      Value v_tag = (i % 11 == 0)
                        ? Value()
                        : Value(std::string("tag_") + std::to_string(i % 7));
      Value v_ratio(static_cast<double>(i) * 0.25);
      Value v_date = Value::DateFromDays(19000 + (i / 100));
      Value v_big((i * 17) + 5);
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_, Row({v_id, v_tag, v_ratio, v_date, v_big}))
              .HasValue());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "items"));
    // A second compaction on the unchanged table must leave the final
    // state identical (block reuse/I/O counts are not asserted here).
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "items"));

    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "items"));
    std::shared_ptr<const TlcsFile> tlcs = tbl.GetBackingTlcsStore();
    ASSERT_NE(tlcs, nullptr);
    EXPECT_FALSE(tlcs->IsPaged());
    EXPECT_EQ(tlcs->TotalRows(), static_cast<uint64_t>(kRows));
    EXPECT_EQ(tlcs->BlockCount(), 2U);
    EXPECT_NE(tbl.GetLSTableStore(), nullptr);

    const std::string tlcs_path =
        prefix + ".db.tlcs." + std::to_string(first_pid);
    EXPECT_TRUE(std::filesystem::exists(tlcs_path));

    // Zone map pruning distinguishes block 0 ([0, 32767]) from block 1 ([32768,
    // 34999]).
    const std::vector<IntegerPeekCompare> hi_pred = {
        {.column = 0,
         .op = BinaryOperation::kGreaterThanEquals,
         .constant = 33000}};
    EXPECT_FALSE(
        tlcs->BlockMatchesZoneMaps(0, &hi_pred, nullptr, std::nullopt));
    EXPECT_TRUE(tlcs->BlockMatchesZoneMaps(1, &hi_pred, nullptr, std::nullopt));

    // Projected block decode returns only requested columns matching source
    // data.
    const std::array<slot_t, 2> proj = {0, 1};
    TlcsFile::DecodedBlock decoded;
    ASSERT_SUCCESS(tlcs->DecodeBlockColumns(1, proj, schema, &decoded));
    ASSERT_EQ(decoded.columns.size(), 2U);
    EXPECT_EQ(decoded.row_count, static_cast<uint32_t>(kRows - 32768));
    EXPECT_EQ(decoded.columns[0].int_cells[0], 32768);
    EXPECT_FALSE(decoded.columns[1].IsNull(0));  // 32768 % 11 == 5
    EXPECT_EQ(decoded.columns[1].VarcharAt(0), "tag_1");
    EXPECT_TRUE(decoded.columns[1].IsNull(1));  // 32769 % 11 == 0

    // Statistics should load from the compacted file's serialized ColumnStats.
    TableStatistics stats(schema);
    ASSERT_SUCCESS(stats.Update(ctx.txn_, tbl));
    EXPECT_EQ(stats.Rows(), static_cast<size_t>(kRows));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  db->DeleteAll();
}

TEST(TlcsFileTest, ForcedPagedModeCheckpointReopenAndMvccMutations) {
  ScopedEnvVar force_paged("TINYLAMB_TLCS_MMAP_LIMIT_BYTES", "1");
  const std::string prefix = "tlcs_paged_" + RandomString();
  Schema schema("orders", {Column("oid", ValueType::kInt64),
                           Column("status", ValueType::kVarChar),
                           Column("price", ValueType::kDouble)});

  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  page_id_t first_pid = 0;
  constexpr int64_t kInitialRows = 500;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    first_pid = tbl.FirstPageId();
    for (int64_t i = 0; i < kInitialRows; ++i) {
      std::string status_str =
          "st_" +
          std::to_string(i);  // >254 distinct entries -> fallback varchar
      Value v_status = (i % 25 == 0) ? Value() : Value(std::move(status_str));
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_, Row({Value(i), v_status,
                                    Value(static_cast<double>(i) + 0.5)}))
              .HasValue());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "orders"));
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "orders"));
    std::shared_ptr<const TlcsFile> tlcs = tbl.GetBackingTlcsStore();
    ASSERT_NE(tlcs, nullptr);
    EXPECT_TRUE(tlcs->IsPaged());
    EXPECT_EQ(tbl.GetLSTableStore(), nullptr);
    EXPECT_EQ(tlcs->TotalRows(), static_cast<uint64_t>(kInitialRows));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  ASSERT_TRUE(db->WriteCheckpoint().HasValue());
  db.reset();

  // Reopen the database cleanly and verify lazy paged TlcsFile attachment.
  db = Database::Create(prefix).MoveValue();
  RowPosition second_pos;
  RowPosition third_pos;
  {
    TransactionContext ctx = db->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "orders"));
    std::shared_ptr<const TlcsFile> tlcs = tbl.GetBackingTlcsStore();
    ASSERT_NE(tlcs, nullptr);
    EXPECT_TRUE(tlcs->IsPaged());
    EXPECT_EQ(tbl.GetLSTableStore(), nullptr);

    // The non-projected VARCHAR column lies between these two file streams.
    const std::array<slot_t, 2> projection = {2, 0};
    TlcsFile::DecodedBlock coalesced;
    TlcsFile::DecodedBlock projected;
    ASSERT_SUCCESS(tlcs->DecodeBlockColumns(0, projection, schema, &coalesced));
    ASSERT_SUCCESS(tlcs->DecodeBlockColumnsWithOptions(
        0, projection, schema, &projected, true, nullptr, {},
        /*read_projected_streams_directly=*/true));
    ASSERT_EQ(projected.row_count, coalesced.row_count);
    ASSERT_EQ(projected.column_ids, coalesced.column_ids);
    for (size_t col = 0; col < projection.size(); ++col) {
      EXPECT_EQ(projected.columns[col].int_cells,
                coalesced.columns[col].int_cells);
    }

    std::vector<Row> rows = ScanAllRows(ctx.txn_, tbl);
    ASSERT_EQ(rows.size(), static_cast<size_t>(kInitialRows));
    for (int64_t i = 0; i < kInitialRows; ++i) {
      const auto r = static_cast<size_t>(i);
      EXPECT_EQ(rows[r][0].value.int_value, i);
      if (i % 25 == 0) {
        EXPECT_TRUE(rows[r][1].IsNull());
      } else {
        EXPECT_EQ(rows[r][1].value.varchar_value, "st_" + std::to_string(i));
      }
      EXPECT_DOUBLE_EQ(rows[r][2].value.double_value,
                       static_cast<double>(i) + 0.5);
    }

    Iterator it = tbl.BeginFullScan(ctx.txn_);
    ASSERT_TRUE(it.IsValid());
    ++it;
    second_pos = it.Position();
    ++it;
    third_pos = it.Position();
  }

  // Exercise Update, Delete, and Insert across stub pages (triggers lazy page
  // hydration).
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "orders"));
    ASSERT_TRUE(tbl.Update(ctx.txn_, second_pos,
                           Row({Value(int64_t{1}),
                                Value(std::string("updated_1")), Value(99.25)}))
                    .HasValue());
    ASSERT_SUCCESS(tbl.Delete(ctx.txn_, third_pos));
    ASSERT_TRUE(
        tbl.Insert(ctx.txn_, Row({Value(int64_t{999}),
                                  Value(std::string("tail_999")), Value(5.5)}))
            .HasValue());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "orders"));
    std::vector<Row> after = ScanAllRows(ctx.txn_, tbl);
    ASSERT_EQ(after.size(),
              static_cast<size_t>(kInitialRows));  // -1 deleted, +1 inserted
    bool saw_updated = false;
    bool saw_deleted = false;
    bool saw_inserted = false;
    for (const Row& r : after) {
      const int64_t oid = r[0].value.int_value;
      if (oid == 1) {
        saw_updated = true;
        EXPECT_EQ(r[1].value.varchar_value, "updated_1");
        EXPECT_DOUBLE_EQ(r[2].value.double_value, 99.25);
      } else if (oid == 2) {
        saw_deleted = true;
      } else if (oid == 999) {
        saw_inserted = true;
        EXPECT_EQ(r[1].value.varchar_value, "tail_999");
      }
    }
    EXPECT_TRUE(saw_updated);
    EXPECT_FALSE(saw_deleted);
    EXPECT_TRUE(saw_inserted);

    // Re-compacting hydrated + appended pages succeeds and produces a clean
    // stub chain.
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "orders"));
    ASSIGN_OR_ASSERT_FAIL(TableStatistics, refreshed,
                          db->GetStatistics(ctx, "orders"));
    EXPECT_EQ(refreshed.Rows(), static_cast<size_t>(kInitialRows));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // DropTable removes companion `.tlcs.<pid>` on commit.
  const std::string tlcs_path =
      prefix + ".db.tlcs." + std::to_string(first_pid);
  EXPECT_TRUE(std::filesystem::exists(tlcs_path));
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->DropTable(ctx, "orders"));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  EXPECT_FALSE(std::filesystem::exists(tlcs_path));

  db->DeleteAll();
}

// #1111: the encoded-image read paths (DecodeBlockEncodedRowsByPid /
// ReadEncodedRowByPid) must decode a block's columns once and reuse the
// serialized images across calls, including across reclaim eviction (which
// must stay correct, just cold again).
TEST(TlcsFileTest, EncodedImageCacheServesRepeatStubReads) {
  ScopedEnvVar force_paged("TINYLAMB_TLCS_MMAP_LIMIT_BYTES", "1");
  const std::string prefix = "tlcs_encoded_cache_" + RandomString();
  Schema schema("cache_probe", {Column("id", ValueType::kInt64),
                                Column("tag", ValueType::kVarChar)});
  constexpr int64_t kRows = 1000;

  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    for (int64_t i = 0; i < kRows; ++i) {
      Value tag = (i % 17 == 0) ? Value() : Value("tag_" + std::to_string(i));
      ASSERT_TRUE(tbl.Insert(ctx.txn_, Row({Value(i), tag})).HasValue());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  std::shared_ptr<const TlcsFile> tlcs;
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "cache_probe"));
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "cache_probe"));
    tlcs = tbl.GetBackingTlcsStore();
    ASSERT_NE(tlcs, nullptr);
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  page_id_t pid = 0;
  ASSERT_SUCCESS(
      tlcs->ForEachBlockRow(schema, [&](page_id_t p, slot_t, const Row&) {
        pid = p;
        return Status::kSuccess;
      }));
  ASSERT_NE(pid, 0U);

  TlcsFile::ResetDecodeCallsForTest();
  std::vector<std::string> first;
  ASSERT_SUCCESS(tlcs->DecodeBlockEncodedRowsByPid(pid, schema, &first));
  EXPECT_EQ(first.size(), static_cast<size_t>(kRows));
  const uint64_t after_cold = TlcsFile::DecodeCallsForTest();
  EXPECT_GE(after_cold, 1U);

  std::vector<std::string> second;
  ASSERT_SUCCESS(tlcs->DecodeBlockEncodedRowsByPid(pid, schema, &second));
  EXPECT_EQ(second, first);
  EXPECT_EQ(TlcsFile::DecodeCallsForTest(), after_cold);
  EXPECT_EQ(tlcs->CachedEncodedBlocksForTest(), 1U);

  std::string single;
  ASSERT_SUCCESS(tlcs->ReadEncodedRowByPid(pid, 42, schema, &single));
  EXPECT_EQ(single, first[42]);
  EXPECT_EQ(TlcsFile::DecodeCallsForTest(), after_cold);

  // The cached image is wire-valid: it deserializes to the original values,
  // including the NULL marker on rows divisible by 17.
  Row probe;
  ASSERT_TRUE(probe.TryDeserialize(first[42].data(), schema).HasValue());
  EXPECT_EQ(probe[0], Value(int64_t{42}));
  EXPECT_EQ(probe[1], Value("tag_42"));
  Row null_probe;
  ASSERT_TRUE(null_probe.TryDeserialize(first[51].data(), schema).HasValue());
  EXPECT_TRUE(null_probe[1].IsNull());

  // Reclaim eviction must clear both caches and release budget; the next
  // read rebuilds (cold) with byte-identical results.
  EXPECT_GT(tlcs->EvictDecodedCacheForTest(size_t{1} << 40), 0U);
  EXPECT_EQ(tlcs->CachedEncodedBlocksForTest(), 0U);
  std::vector<std::string> third;
  ASSERT_SUCCESS(tlcs->DecodeBlockEncodedRowsByPid(pid, schema, &third));
  EXPECT_EQ(third, first);
  EXPECT_GT(TlcsFile::DecodeCallsForTest(), after_cold);

  tlcs.reset();
  db->DeleteAll();
}

// Re-compacting a table whose chain still holds columnar stub pages must not
// rebuild every stub row's serialized image: WriteTlcsTempFile feeds its
// staging columns straight from each decoded block (ForEachBlockRowByPid), so
// the backing file's encoded-image cache stays empty. Before the fix every
// stub block was materialized through GetOrLoadCachedEncodedRows — a
// Serialize -> assign -> TryDeserialize round-trip per row.
TEST(TlcsFileTest, StubRecompactionSkipsEncodedImageCache) {
  ScopedEnvVar force_paged("TINYLAMB_TLCS_MMAP_LIMIT_BYTES", "1");
  const std::string prefix = "tlcs_recompact_" + RandomString();
  Schema schema("recompact_items", {Column("id", ValueType::kInt64),
                                    Column("tag", ValueType::kVarChar)});
  constexpr int64_t kCompactedRows = 40000;  // two 32,768-row stub blocks
  constexpr int64_t kTailRows = 7;

  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    for (int64_t i = 0; i < kCompactedRows; ++i) {
      Value tag = (i % 9 == 0) ? Value() : Value("t_" + std::to_string(i));
      ASSERT_TRUE(tbl.Insert(ctx.txn_, Row({Value(i), tag})).HasValue());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  std::shared_ptr<const TlcsFile> first_tlcs;
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "recompact_items"));
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "recompact_items"));
    first_tlcs = tbl.GetBackingTlcsStore();
    ASSERT_NE(first_tlcs, nullptr);
    ASSERT_EQ(first_tlcs->BlockCount(), 2U);
    // A small real-page tail keeps the next compaction from early-outing on
    // an all-stub chain, so it must decode the stub blocks again.
    for (int64_t i = kCompactedRows; i < kCompactedRows + kTailRows; ++i) {
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_,
                     Row({Value(i), Value("tail_" + std::to_string(i))}))
              .HasValue());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "recompact_items"));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // The re-compaction consumed first_tlcs's stub rows without populating its
  // encoded-image cache — the per-row serialize round-trip is gone.
  EXPECT_EQ(first_tlcs->CachedEncodedBlocksForTest(), 0U);

  // And the rewritten file carries every row, tail included.
  {
    TransactionContext ctx = db->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "recompact_items"));
    std::shared_ptr<const TlcsFile> tlcs = tbl.GetBackingTlcsStore();
    ASSERT_NE(tlcs, nullptr);
    int64_t seen = 0;
    ASSERT_SUCCESS(
        tlcs->ForEachBlockRow(schema, [&](page_id_t, slot_t, const Row& row) {
          EXPECT_EQ(row[0].value.int_value, seen);
          if (seen % 9 == 0 && seen < kCompactedRows) {
            EXPECT_TRUE(row[1].IsNull());
          } else {
            const std::string expected =
                (seen < kCompactedRows ? "t_" : "tail_") + std::to_string(seen);
            EXPECT_EQ(row[1].value.varchar_value, expected);
          }
          ++seen;
          return Status::kSuccess;
        }));
    EXPECT_EQ(seen, kCompactedRows + kTailRows);
  }

  first_tlcs.reset();
  db->DeleteAll();
}

TEST(TlcsFileTest, ResidentBatchDecodesShortDictionaryStrings) {
  const std::string prefix = "tlcs_resident_tags_" + RandomString();
  const Schema schema("tags", {Column("id", ValueType::kInt64),
                               Column("tag", ValueType::kVarChar)});
  auto db = Database::Create(prefix).MoveValue();
  {
    auto context = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, db->CreateTable(context, schema));
    for (int64_t id = 0; id < 300; ++id) {
      ASSERT_TRUE(
          table
              .Insert(context.txn_, Row({Value(id), Value(id % 2 ? "Y" : "N")}))
              .HasValue());
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  {
    auto context = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(context, "tags"));
    ASSERT_SUCCESS(context.PreCommit());
  }
  auto context = db->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db->GetTable(context, "tags"));
  const auto tlcs = table.GetBackingTlcsStore();
  ASSERT_NE(tlcs, nullptr);
  const auto resident = tlcs->ResidentStore();
  ASSERT_NE(resident, nullptr);
  ASSERT_FALSE(resident->blocks.empty());
  const auto& tags = resident->blocks[0].columns[1];
  ASSERT_TRUE(tags.all_single_byte_varchar);
  ASSERT_NE(tags.dict, nullptr);
  EXPECT_TRUE(tags.VarcharBytes().empty());

  const std::vector<IntegerPeekCompare> filtered{
      {.column = 0, .op = BinaryOperation::kGreaterThanEquals, .constant = 3}};
  for (const bool use_filter : {false, true}) {
    TableScanOptions options;
    options.peek_compares = use_filter ? &filtered : nullptr;
    auto scan = table.BeginFullScan(context.txn_, options);
    TypedCellBatch batch;
    batch.Configure(2, {ValueType::kInt64, ValueType::kVarChar},
                    {false, false});
    int64_t id = use_filter ? 3 : 0;
    for (size_t rows = scan.DecodeNextTypedBatch(&batch, 17); rows != 0;
         rows = scan.DecodeNextTypedBatch(&batch, 17)) {
      for (size_t row = 0; row < rows; ++row, ++id) {
        EXPECT_EQ(batch.cells[row * 2], id);
        EXPECT_EQ(batch.Varchar(row, 0), id % 2 ? "Y" : "N");
      }
    }
    ASSERT_SUCCESS(scan.GetStatus());
    EXPECT_EQ(id, 300);
  }
  ASSERT_SUCCESS(context.PreCommit());
}

TEST(TlcsFileTest, PagedStubBatchDecodeZoneMapsProjectionRuntimeFilterAndTail) {
  ScopedEnvVar force_paged("TINYLAMB_TLCS_MMAP_LIMIT_BYTES", "1");
  const std::string prefix = "tlcs_paged_batch_" + RandomString();
  Schema schema("batch_items", {Column("id", ValueType::kInt64),
                                Column("tag", ValueType::kVarChar),
                                Column("ratio", ValueType::kDouble),
                                Column("shipdate", ValueType::kDate)});

  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  constexpr int64_t kCompactedRows = 33500;
  constexpr int64_t kTailRows = 12;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    for (int64_t i = 0; i < kCompactedRows; ++i) {
      Value v_tag = (i % 11 == 0)
                        ? Value()
                        : Value(std::string("t_") + std::to_string(i));
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_,
                     Row({Value(i), v_tag, Value(static_cast<double>(i) * 0.5),
                          Value::DateFromDays(19000 + (i % 500))}))
              .HasValue());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "batch_items"));
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "batch_items"));
    for (int64_t i = kCompactedRows; i < kCompactedRows + kTailRows; ++i) {
      Value v_tag = (i % 11 == 0)
                        ? Value()
                        : Value(std::string("tail_") + std::to_string(i));
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_,
                     Row({Value(i), v_tag, Value(static_cast<double>(i) * 0.5),
                          Value::DateFromDays(19000 + (i % 500))}))
              .HasValue());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  {
    TransactionContext ctx = db->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "batch_items"));
    std::shared_ptr<const TlcsFile> tlcs = tbl.GetBackingTlcsStore();
    ASSERT_NE(tlcs, nullptr);
    EXPECT_TRUE(tlcs->IsPaged());
    EXPECT_EQ(tbl.GetLSTableStore(), nullptr);
    EXPECT_EQ(tlcs->BlockCount(), 2U);

    // 1. DecodeNextTypedBatch across stub block 0, block 1, and tail RowPage
    // with projection {0, 1}, peek_compares (id >= 32700), and RuntimeFilter.
    const std::vector<slot_t> proj = {0, 1};
    const std::vector<IntegerPeekCompare> peek_32700 = {
        {.column = 0,
         .op = BinaryOperation::kGreaterThanEquals,
         .constant = 32700}};
    const std::vector<int64_t> rf_keys = {32750, 32767, 32768, 33000,
                                          33011, 33499, 33502, 33509};
    std::shared_ptr<const RuntimeFilterGroup> rf =
        RuntimeFilterBuilder::FromKeys(rf_keys);
    ASSERT_NE(rf, nullptr);

    TableScanOptions opts;
    opts.projection = proj;
    opts.peek_compares = &peek_32700;
    Iterator batch_it = tbl.BeginFullScan(ctx.txn_, opts);
    batch_it.SetRuntimeFilter(rf.get(), /*column=*/0, ValueType::kInt64);

    TypedCellBatch batch;
    batch.Configure(2, {ValueType::kInt64, ValueType::kVarChar},
                    {false, false});
    std::vector<int64_t> seen_ids;
    while (true) {
      batch.Reset();
      const size_t n = batch_it.DecodeNextTypedBatch(&batch, 3);
      if (n == 0) {
        ASSERT_SUCCESS(batch_it.GetStatus());
        break;
      }
      EXPECT_LE(n, 3U);
      for (size_t r = 0; r < n; ++r) {
        ASSERT_EQ(batch.nulls[(r * 2) + 0], 0U);
        const int64_t id = batch.cells[(r * 2) + 0];
        seen_ids.push_back(id);
        if (id % 11 == 0) {
          EXPECT_EQ(batch.nulls[(r * 2) + 1], 1U);
        } else {
          EXPECT_EQ(batch.nulls[(r * 2) + 1], 0U);
          const std::string expected =
              (id < kCompactedRows ? "t_" : "tail_") + std::to_string(id);
          EXPECT_EQ(batch.Varchar(r, 0), expected);
        }
      }
    }
    EXPECT_EQ(seen_ids, rf_keys);

    const std::vector<int64_t> date_keys = {19009, 19011, 19268};
    const auto dates = RuntimeFilterBuilder::FromKeys(date_keys);
    ASSERT_NE(dates, nullptr);
    for (const bool capture_positions : {false, true}) {
      Iterator extra = tbl.BeginFullScan(ctx.txn_, opts);
      extra.SetRuntimeFilter(rf.get(), 0, ValueType::kInt64);
      ASSERT_TRUE(extra.IsValid());
      ASSERT_EQ((*extra)[0].value.int_value, 32750);
      ++extra;
      ASSERT_TRUE(extra.IsValid());
      ASSERT_EQ((*extra)[0].value.int_value, 32767);

      // The date column should participate in filtering even though it is
      // absent from the projection and the iterator has already read rows.
      extra.AddExtraRuntimeFilter(dates.get(), 3, ValueType::kDate);
      ASSERT_TRUE(extra.IsValid());
      ASSERT_EQ((*extra)[0].value.int_value, 32768);
      batch.capture_positions = capture_positions;
      std::vector<int64_t> matching_ids;
      while (const size_t n = extra.DecodeNextTypedBatch(&batch, 2)) {
        for (size_t row = 0; row < n; ++row) {
          matching_ids.push_back(batch.cells[2 * row]);
        }
      }
      ASSERT_SUCCESS(extra.GetStatus());
      EXPECT_EQ(matching_ids, (std::vector<int64_t>{32768, 33011, 33509}));
    }

    // 2. Row-by-row operator* scan with zone map skipping block 0 completely.
    const std::vector<IntegerPeekCompare> peek_hi = {
        {.column = 0,
         .op = BinaryOperation::kGreaterThanEquals,
         .constant = 33000}};
    TableScanOptions star_opts;
    star_opts.projection = std::vector<slot_t>{0, 1};
    star_opts.peek_compares = &peek_hi;
    Iterator row_it = tbl.BeginFullScan(ctx.txn_, star_opts);
    row_it.SetRuntimeFilter(rf.get(), /*column=*/0, ValueType::kInt64);
    std::vector<int64_t> star_ids;
    while (row_it.IsValid()) {
      const Row& r = *row_it;
      ASSERT_EQ(r.Count(), 2U);
      const int64_t id = r[0].value.int_value;
      star_ids.push_back(id);
      if (id % 11 == 0) {
        EXPECT_TRUE(r[1].IsNull());
      } else {
        const std::string expected =
            (id < kCompactedRows ? "t_" : "tail_") + std::to_string(id);
        EXPECT_EQ(r[1].value.varchar_value, expected);
      }
      ++row_it;
    }
    ASSERT_SUCCESS(row_it.GetStatus());
    const std::vector<int64_t> expected_hi = {33000, 33011, 33499, 33502,
                                              33509};
    EXPECT_EQ(star_ids, expected_hi);
  }

  db->DeleteAll();
}

namespace {

struct TlcsBudgetResetGuard {
  size_t prev_limit{ProcessMemoryBudget::kDefaultLimitBytes};
  size_t prev_storage{size_t{4} << 30};
  size_t prev_columnar{size_t{2} << 30};
  TlcsBudgetResetGuard()
      : prev_limit(ProcessMemoryBudget::Global().Limit()),
        prev_storage(ProcessMemoryBudget::Global().StoragePoolLimit()),
        prev_columnar(ProcessMemoryBudget::Global().ColumnarPoolLimit()) {}
  TlcsBudgetResetGuard(const TlcsBudgetResetGuard&) = delete;
  TlcsBudgetResetGuard& operator=(const TlcsBudgetResetGuard&) = delete;
  TlcsBudgetResetGuard(TlcsBudgetResetGuard&&) = delete;
  TlcsBudgetResetGuard& operator=(TlcsBudgetResetGuard&&) = delete;
  ~TlcsBudgetResetGuard() {
    auto& budget = ProcessMemoryBudget::Global();
    budget.ResetTestKnobs();
    budget.ResetForTest(prev_limit, prev_storage, prev_columnar, true);
  }
};

void WriteLe32(uint8_t* dst, uint32_t v) {
  dst[0] = static_cast<uint8_t>(v & 0xFFU);
  dst[1] = static_cast<uint8_t>((v >> 8) & 0xFFU);
  dst[2] = static_cast<uint8_t>((v >> 16) & 0xFFU);
  dst[3] = static_cast<uint8_t>((v >> 24) & 0xFFU);
}

void WriteLe64(uint8_t* dst, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    dst[i] = static_cast<uint8_t>((v >> (i * 8)) & 0xFFU);
  }
}

}  // namespace

TEST(TlcsFileTest, SingleRowPointReadMatchesEveryEncodingAndBlockBoundary) {
  const std::string prefix = "tlcs_point_" + RandomString();
  Schema schema(
      "point_tbl",
      {Column("id", ValueType::kInt64), Column("tag", ValueType::kVarChar),
       Column("ratio", ValueType::kDouble), Column("day", ValueType::kDate),
       Column("note", ValueType::kVarChar)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  page_id_t first_pid = 0;
  constexpr int64_t kRows = 35000;
  auto expected_row = [](int64_t i) {
    return Row({Value((i * 3) + 1),
                (i % 11 == 0)
                    ? Value()
                    : Value(std::string("tag_") + std::to_string(i % 7)),
                Value(static_cast<double>(i) * 0.25),
                Value::DateFromDays(19000 + (i / 100)),
                Value(std::string("note_") + std::to_string(i * 7))});
  };
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    first_pid = tbl.FirstPageId();
    for (int64_t i = 0; i < kRows; ++i) {
      ASSERT_TRUE(tbl.Insert(ctx.txn_, expected_row(i)).HasValue());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "point_tbl"));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  const std::string tlcs_path =
      prefix + ".db.tlcs." + std::to_string(first_pid);
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TlcsFile>, tlcs,
                        TlcsFile::Open(tlcs_path, schema));
  ASSERT_EQ(tlcs->BlockCount(), 2U);

  // First/last rows of both blocks plus scattered rows in between, covering
  // FOR, narrow-dict, raw-double and varchar column segments.
  const std::vector<int64_t> samples = {
      0, 1, 7, 1234, 16383, 32766, 32767, 32768, 32769, 33333, 34998, 34999};
  for (const int64_t i : samples) {
    const auto block = static_cast<uint32_t>(i / 32768);
    const auto slot = static_cast<uint32_t>(i % 32768);
    Row row;
    ASSERT_SUCCESS(tlcs->ReadRowByBlockIndex(block, slot, schema, &row));
    EXPECT_EQ(row, expected_row(i)) << "row " << i;
  }

  // Repeated point reads must agree with the one-shot decode (no stale cache).
  for (int rep = 0; rep < 3; ++rep) {
    for (const int64_t i : samples) {
      const auto block = static_cast<uint32_t>(i / 32768);
      const auto slot = static_cast<uint32_t>(i % 32768);
      Row row;
      ASSERT_SUCCESS(tlcs->ReadRowByBlockIndex(block, slot, schema, &row));
      EXPECT_EQ(row, expected_row(i)) << "row " << i << " rep " << rep;
    }
  }
  db->DeleteAll();
}

TEST(TlcsFileTest, RejectsVersionOneHeaderPerAdr0011) {
  const std::string prefix = "tlcs_v1_reject_" + RandomString();
  Schema schema("v1_tbl", {Column("id", ValueType::kInt64)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  page_id_t first_pid = 0;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    first_pid = tbl.FirstPageId();
    for (int64_t i = 0; i < 64; ++i) {
      ASSERT_TRUE(tbl.Insert(ctx.txn_, Row({Value(i)})).HasValue());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "v1_tbl"));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  const std::string tlcs_path =
      prefix + ".db.tlcs." + std::to_string(first_pid);
  const std::string v1_path = tlcs_path + ".v1copy";
  std::filesystem::copy_file(tlcs_path, v1_path,
                             std::filesystem::copy_options::overwrite_existing);
  {
    std::fstream f(v1_path, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(f.is_open());
    std::array<uint8_t, 72> hdr{};
    f.read(reinterpret_cast<char*>(hdr.data()), hdr.size());
    ASSERT_TRUE(f.good());
    WriteLe32(hdr.data() + 8, 1U);
    const uint32_t new_crc = Crc32C(hdr.data(), 68);
    WriteLe32(hdr.data() + 68, new_crc);
    f.seekp(0);
    f.write(reinterpret_cast<const char*>(hdr.data()), hdr.size());
    ASSERT_TRUE(f.good());
  }

  StatusOr<std::shared_ptr<TlcsFile>> v1_open = TlcsFile::Open(v1_path, schema);
  ASSERT_FALSE(v1_open.HasValue());
  EXPECT_EQ(v1_open.GetStatus(), Status::kCorrupt);

  std::error_code ec;
  std::filesystem::remove(v1_path, ec);
  db->DeleteAll();
}

TEST(TlcsFileTest, RejectsHeaderRowCountBelowDirectoryBlockRowSum) {
  const std::string prefix = "tlcs_rowcnt_" + RandomString();
  Schema schema("rowcnt_tbl", {Column("id", ValueType::kInt64)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  page_id_t first_pid = 0;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    first_pid = tbl.FirstPageId();
    for (int64_t i = 0; i < 100; ++i) {
      ASSERT_TRUE(tbl.Insert(ctx.txn_, Row({Value(i)})).HasValue());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "rowcnt_tbl"));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  const std::string tlcs_path =
      prefix + ".db.tlcs." + std::to_string(first_pid);
  const std::string bad_path = tlcs_path + ".badrowcnt";
  std::filesystem::copy_file(tlcs_path, bad_path,
                             std::filesystem::copy_options::overwrite_existing);
  {
    std::fstream f(bad_path, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(f.is_open());
    std::array<uint8_t, 72> hdr{};
    f.read(reinterpret_cast<char*>(hdr.data()), hdr.size());
    ASSERT_TRUE(f.good());
    // total_rows_ lives at offset 32 (8-byte magic + version + flags +
    // layout generation + first pid). Under-report it so the resident-store
    // builder would size its buffers too small for the directory row counts.
    WriteLe64(hdr.data() + 32, 1U);
    WriteLe32(hdr.data() + 68, Crc32C(hdr.data(), 68));
    f.seekp(0);
    f.write(reinterpret_cast<const char*>(hdr.data()), hdr.size());
    ASSERT_TRUE(f.good());
  }

  StatusOr<std::shared_ptr<TlcsFile>> opened = TlcsFile::Open(bad_path, schema);
  ASSERT_FALSE(opened.HasValue());
  EXPECT_EQ(opened.GetStatus(), Status::kCorrupt);

  std::error_code ec;
  std::filesystem::remove(bad_path, ec);
  db->DeleteAll();
}

// Builds a minimal single-block TLCS file with one INT64 column stored as a
// FOR-8 payload. With `payload_offset == nullptr` the payload points inside the
// file (valid); otherwise it is an out-of-bounds value which the reader must
// reject before building a resident span / paged read buffer.
void WriteMinimalTlcsFile(const std::string& path, bool out_of_bounds,
                          uint8_t dict_kind = 0) {
  std::vector<uint8_t> f;
  const auto u8 = [&](uint8_t v) { f.push_back(v); };
  const auto u32 = [&](uint32_t v) {
    for (int i = 0; i < 4; ++i) {
      u8(static_cast<uint8_t>(v >> (i * 8)));
    }
  };
  const auto u64 = [&](uint64_t v) {
    for (int i = 0; i < 8; ++i) {
      u8(static_cast<uint8_t>(v >> (i * 8)));
    }
  };
  const auto append = [&](const std::vector<uint8_t>& s) {
    f.insert(f.end(), s.begin(), s.end());
  };

  constexpr uint64_t kGroupOffset = 72;  // right after the header
  constexpr uint32_t kRowCount = 1;

  // --- directory group body (pid span + body span) ---
  std::vector<uint8_t> body;
  const auto bu8 = [&](uint8_t v) { body.push_back(v); };
  const auto bu32 = [&](uint32_t v) {
    for (int i = 0; i < 4; ++i) {
      bu8(static_cast<uint8_t>(v >> (i * 8)));
    }
  };
  const auto bu64 = [&](uint64_t v) {
    for (int i = 0; i < 8; ++i) {
      bu8(static_cast<uint8_t>(v >> (i * 8)));
    }
  };
  bu32(kRowCount);  // row_count
  bu8(0);           // any_null
  bu8(2);           // encoding = kFor
  bu8(8);           // for_width = 8
  bu8(0);           // flags
  bu64(0);          // for_base
  bu64(0);          // min_int
  bu64(0);          // max_int
  for (int i = 0; i < 2; ++i) {
    bu64(0);  // min_double / max_double
  }
  bu32(UINT32_MAX);  // const_code
  bu32(UINT32_MAX);  // min_code
  bu32(0);           // max_code
  for (int i = 0; i < 4; ++i) {
    bu64(0);  // dict_mask
  }
  bu64(0);                                            // nulls_offset
  bu64(0);                                            // nulls_size
  bu64(out_of_bounds ? 1'000'000ULL : kGroupOffset);  // data_offset
  bu64(8);                                            // data_size
  bu64(0);                                            // aux_offset
  bu64(0);                                            // aux_size

  std::vector<uint8_t> pids;
  pids.reserve(8);
  for (int i = 0; i < 8; ++i) {
    pids.push_back(static_cast<uint8_t>(1ULL >> (i * 8)));
  }
  const uint32_t pid_crc = Crc32C(pids.data(), pids.size());
  const uint32_t body_crc = Crc32C(body.data(), body.size());

  // --- directory blob ---
  std::vector<uint8_t> dir;
  const auto du8 = [&](uint8_t v) { dir.push_back(v); };
  const auto du32 = [&](uint32_t v) {
    for (int i = 0; i < 4; ++i) {
      du8(static_cast<uint8_t>(v >> (i * 8)));
    }
  };
  const auto du64 = [&](uint64_t v) {
    for (int i = 0; i < 8; ++i) {
      du8(static_cast<uint8_t>(v >> (i * 8)));
    }
  };
  du8(static_cast<uint8_t>(ValueType::kInt64));  // col type
  du8(0);                                        // unsigned
  du8(dict_kind);                                // dict kind
  du8(static_cast<uint8_t>(ValueType::kInt64));  // dict value type
  du8(0);                                        // bytewise
  du8(0);                                        // order
  du8(0);                                        // single-byte
  du32(0);                                       // dict entries
  du32(128);                                     // dir group size
  du32(1);                                       // group count
  du64(kGroupOffset);                            // group offset
  du32(1);                                       // block count
  du32(pid_crc);
  du64(body.size());  // body_size
  du32(body_crc);
  du64(1);  // first_pid
  du64(1);  // last_pid
  du64(1);  // min_pid
  du64(1);  // max_pid
  du8(1);   // pids_monotonic
  du32(0);  // stats size
  const uint32_t dir_crc = Crc32C(dir.data(), dir.size());

  // --- header ---
  for (const char* p = "TLCS0001"; *p != '\0'; ++p) {
    u8(static_cast<uint8_t>(*p));
  }
  u32(2);                                         // version
  u32(0);                                         // flags
  u64(1);                                         // layout generation
  u64(1);                                         // first pid
  u64(1);                                         // total rows
  u32(1);                                         // block count
  u32(1);                                         // column count
  u64(kGroupOffset + pids.size() + body.size());  // dir offset
  u64(dir.size());                                // dir length
  u32(dir_crc);
  const uint32_t hdr_crc = Crc32C(f.data(), f.size());
  u32(hdr_crc);

  append(pids);
  append(body);
  append(dir);

  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(out.is_open());
  out.write(reinterpret_cast<const char*>(f.data()), f.size());
  out.close();
}

TEST(TlcsFileTest, RejectsColumnPayloadOffsetOutsideFile) {
  const Schema schema("colspan_tbl", {Column("id", ValueType::kInt64)});
  const std::string good_path = "tlcs_colspan_ok_" + RandomString() + ".tlcs";
  const std::string bad_path = "tlcs_colspan_bad_" + RandomString() + ".tlcs";
  WriteMinimalTlcsFile(good_path, /*out_of_bounds=*/false);
  WriteMinimalTlcsFile(bad_path, /*out_of_bounds=*/true);

  // The well-formed twin opens; the out-of-bounds column payload is rejected
  // before any span / read buffer can be built from it.
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TlcsFile>, ok,
                        TlcsFile::Open(good_path, schema));
  EXPECT_NE(ok, nullptr);

  StatusOr<std::shared_ptr<TlcsFile>> bad = TlcsFile::Open(bad_path, schema);
  ASSERT_FALSE(bad.HasValue());
  EXPECT_EQ(bad.GetStatus(), Status::kCorrupt);

  std::error_code ec;
  std::filesystem::remove(good_path, ec);
  std::filesystem::remove(bad_path, ec);
}

TEST(TlcsFileTest, RejectsUnknownDictKind) {
  const Schema schema("dkind_tbl", {Column("id", ValueType::kInt64)});
  const std::string good_path = "tlcs_dkind_ok_" + RandomString() + ".tlcs";
  const std::string bad_path = "tlcs_dkind_bad_" + RandomString() + ".tlcs";
  WriteMinimalTlcsFile(good_path, /*out_of_bounds=*/false, /*dict_kind=*/0);
  WriteMinimalTlcsFile(bad_path, /*out_of_bounds=*/false, /*dict_kind=*/0x7f);

  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TlcsFile>, ok,
                        TlcsFile::Open(good_path, schema));
  EXPECT_NE(ok, nullptr);

  // An unrecognized dict kind must fail validation at open rather than fall
  // past every dict branch and leave columns undecoded.
  StatusOr<std::shared_ptr<TlcsFile>> bad = TlcsFile::Open(bad_path, schema);
  ASSERT_FALSE(bad.HasValue());
  EXPECT_EQ(bad.GetStatus(), Status::kCorrupt);

  std::error_code ec;
  std::filesystem::remove(good_path, ec);
  std::filesystem::remove(bad_path, ec);
}

// Two single-block dir groups whose min/max pid ranges both cover the pid
// space, so the `FindBlockIndex` fallback cannot prune either group. The
// groups are deliberately ordered non-monotonically (pid 50 000 then 10 000).
void WriteOverlappingPidTlcsFile(const std::string& path) {
  std::vector<uint8_t> body;
  const auto bu8 = [&](uint8_t v) { body.push_back(v); };
  const auto bu32 = [&](uint32_t v) {
    for (int i = 0; i < 4; ++i) {
      bu8(static_cast<uint8_t>(v >> (i * 8)));
    }
  };
  const auto bu64 = [&](uint64_t v) {
    for (int i = 0; i < 8; ++i) {
      bu8(static_cast<uint8_t>(v >> (i * 8)));
    }
  };
  bu32(1);  // row_count
  bu8(0);   // any_null
  bu8(2);   // encoding = kFor
  bu8(8);   // for_width = 8
  bu8(0);   // flags
  for (int i = 0; i < 2; ++i) {
    bu64(0);  // for_base / min_int
  }
  bu64(0);  // max_int
  for (int i = 0; i < 2; ++i) {
    bu64(0);  // min_double / max_double
  }
  bu32(UINT32_MAX);  // const_code
  bu32(UINT32_MAX);  // min_code
  bu32(0);           // max_code
  for (int i = 0; i < 4; ++i) {
    bu64(0);  // dict_mask
  }
  bu64(0);  // nulls_offset
  bu64(0);  // nulls_size
  // Out-of-bounds payload: a full group load (the old fallback) fails
  // validation while the pid-only path still resolves the block.
  bu64(1000000);  // data_offset
  bu64(8);        // data_size
  bu64(0);        // aux_offset
  bu64(0);        // aux_size
  const uint32_t body_crc = Crc32C(body.data(), body.size());

  // NOLINTNEXTLINE(modernize-avoid-c-arrays)  // runtime-sized buffer,
  // std::array not applicable
  const uint64_t group_offsets[2] = {72, 72 + 8 + body.size()};
  // NOLINTNEXTLINE(modernize-avoid-c-arrays)  // runtime-sized buffer,
  // std::array not applicable
  const uint64_t pids[2] = {50000, 10000};
  std::vector<uint8_t> dir;
  const auto du8 = [&](uint8_t v) { dir.push_back(v); };
  const auto du32 = [&](uint32_t v) {
    for (int i = 0; i < 4; ++i) {
      du8(static_cast<uint8_t>(v >> (i * 8)));
    }
  };
  const auto du64 = [&](uint64_t v) {
    for (int i = 0; i < 8; ++i) {
      du8(static_cast<uint8_t>(v >> (i * 8)));
    }
  };
  du8(static_cast<uint8_t>(ValueType::kInt64));
  du8(0);                                        // unsigned
  du8(0);                                        // dict kind = kNone
  du8(static_cast<uint8_t>(ValueType::kInt64));  // dict value type
  du8(0);                                        // bytewise
  du8(0);                                        // order
  du8(0);                                        // single-byte
  du32(0);                                       // dict entries
  du32(1);                                       // dir group size
  du32(2);                                       // group count
  for (uint32_t g = 0; g < 2; ++g) {
    std::vector<uint8_t> pid_span;
    pid_span.reserve(8);
    for (int i = 0; i < 8; ++i) {
      pid_span.push_back(static_cast<uint8_t>(pids[g] >> (i * 8)));
    }
    const uint32_t pid_crc = Crc32C(pid_span.data(), pid_span.size());
    du64(group_offsets[g]);
    du32(1);  // block count
    du32(pid_crc);
    du64(body.size());
    du32(body_crc);
    du64(pids[g]);  // first_pid
    du64(pids[g]);  // last_pid
    du64(1);        // min_pid (overlapping)
    du64(200000);   // max_pid (overlapping)
    du8(0);         // pids_monotonic = false
  }
  du32(0);  // stats size
  const uint32_t dir_crc = Crc32C(dir.data(), dir.size());

  std::vector<uint8_t> f;
  const auto u8 = [&](uint8_t v) { f.push_back(v); };
  const auto u32 = [&](uint32_t v) {
    for (int i = 0; i < 4; ++i) {
      u8(static_cast<uint8_t>(v >> (i * 8)));
    }
  };
  const auto u64 = [&](uint64_t v) {
    for (int i = 0; i < 8; ++i) {
      u8(static_cast<uint8_t>(v >> (i * 8)));
    }
  };
  for (const char* p = "TLCS0001"; *p != '\0'; ++p) {
    u8(static_cast<uint8_t>(*p));
  }
  u32(2);                                   // version
  u32(0);                                   // flags
  u64(1);                                   // layout generation
  u64(1);                                   // first pid
  u64(2);                                   // total rows
  u32(2);                                   // block count
  u32(1);                                   // column count
  u64(group_offsets[1] + 8 + body.size());  // dir offset
  u64(dir.size());                          // dir length
  u32(dir_crc);
  u32(Crc32C(f.data(), f.size()));  // header crc
  for (unsigned long pid : pids) {
    for (int i = 0; i < 8; ++i) {
      u8(static_cast<uint8_t>(pid >> (i * 8)));
    }
    f.insert(f.end(), body.begin(), body.end());
  }
  f.insert(f.end(), dir.begin(), dir.end());

  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(out.is_open());
  out.write(reinterpret_cast<const char*>(f.data()), f.size());
  out.close();
  ASSERT_TRUE(out.good());
}

TEST(TlcsFileTest, StoredSchemaRejectsColumnCountExceedingDirectory) {
  const std::string path = "tlcs_bad_columns_" + RandomString() + ".tlcs";
  WriteOverlappingPidTlcsFile(path);
  {
    std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file.is_open());
    std::array<uint8_t, 72> header{};
    file.read(reinterpret_cast<char*>(header.data()), header.size());
    ASSERT_EQ(file.gcount(), static_cast<std::streamsize>(header.size()));
    WriteLe32(header.data() + 44, UINT32_MAX);
    WriteLe32(header.data() + 68, Crc32C(header.data(), 68));
    file.seekp(0);
    file.write(reinterpret_cast<const char*>(header.data()), header.size());
  }
  Schema schema;
  const auto opened = TlcsFile::OpenWithStoredSchema(path, &schema);
  ASSERT_FALSE(opened.HasValue());
  EXPECT_EQ(opened.GetStatus(), Status::kCorrupt);
  EXPECT_TRUE(std::filesystem::remove(path));
}

TEST(TlcsFileTest, FindBlockIndexWithOverlappingPidRanges) {
  const Schema schema("overlap_tbl", {Column("id", ValueType::kInt64)});
  const std::string path = "tlcs_overlap_" + RandomString() + ".tlcs";
  WriteOverlappingPidTlcsFile(path);

  // Both the lazy compact index (default budget) and the tiny-budget
  // pid-only fallback must resolve the same blocks.
  for (int pass = 0; pass < 2; ++pass) {
    TlcsBudgetResetGuard guard;
    if (pass == 1) {
      // Force the pid index over the size cap: max_pid ~= 50k -> ~200 KiB,
      // while columnar cap / 32 = 8 MiB / 32 = 256 KiB ... use 2 MiB so the
      // index does not fit and the per-group pid-scan path is exercised.
      ProcessMemoryBudget::Global().ResetForTest(
          ProcessMemoryBudget::kDefaultLimitBytes, size_t{4} << 30,
          size_t{2} << 20, /*check_rss_pressure=*/false);
    }
    ProcessMemoryBudget::Global().SetTestAlwaysSpill(true);
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TlcsFile>, tlcs,
                          TlcsFile::Open(path, schema));
    EXPECT_TRUE(tlcs->IsPagedDirectory());
    EXPECT_EQ(tlcs->FindBlockIndex(50000), 0U);
    EXPECT_EQ(tlcs->FindBlockIndex(10000), 1U);
    EXPECT_EQ(tlcs->FindBlockIndex(12345), LSTableStore::kInvalidBlockIndex);
    EXPECT_EQ(tlcs->FindBlockIndex(50000), 0U);  // stable after index build
  }

  std::error_code ec;
  std::filesystem::remove(path, ec);
}

TEST(TlcsFileTest, PagedDirectoryModeAndReclaimDropResidentAndEagerCaches) {
  TlcsBudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(size_t{256} << 20, size_t{64} << 20, size_t{64} << 20,
                      /*check_rss_pressure=*/false);
  const std::string prefix = "tlcs_p4_reclaim_" + RandomString();
  Schema schema("reclaim_tbl", {Column("id", ValueType::kInt64),
                                Column("tag", ValueType::kVarChar)});

  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  page_id_t first_pid = 0;
  constexpr int64_t kRows = 33500;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    first_pid = tbl.FirstPageId();
    for (int64_t i = 0; i < kRows; ++i) {
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_,
                     Row({Value(i), Value("row_" + std::to_string(i % 13))}))
              .HasValue());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  std::string tlcs_path = prefix + ".db.tlcs." + std::to_string(first_pid);
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "reclaim_tbl"));
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "reclaim_tbl"));
    std::shared_ptr<const TlcsFile> tlcs = tbl.GetBackingTlcsStore();
    ASSERT_NE(tlcs, nullptr);
    EXPECT_FALSE(tlcs->IsPaged());
    EXPECT_FALSE(tlcs->IsPagedDirectory());
    std::shared_ptr<const LSTableStore> active_res = tlcs->ResidentStore();
    ASSERT_NE(active_res, nullptr);
    const size_t before_used = budget.ColumnarUsed();
    EXPECT_GT(before_used, 0U);

    // Push query pressure near Limit() so TryReserveWithReclaim triggers
    // ReclaimStage::kColumnar on both TableIncarnation and TlcsFile.
    const size_t avail = budget.Available();
    ASSERT_GT(avail, size_t{256} << 10);
    const size_t fill = avail - (size_t{64} << 10);
    ASSERT_TRUE(budget.TryReserve(MemoryPoolKind::kQuery, fill, false));
    const bool ok =
        budget.TryReserveWithReclaim(MemoryPoolKind::kQuery, size_t{4} << 20);
    budget.Release(MemoryPoolKind::kQuery, fill);
    if (ok) {
      budget.Release(MemoryPoolKind::kQuery, size_t{4} << 20);
    }

    EXPECT_TRUE(tlcs->IsPaged());
    EXPECT_EQ(tlcs->ResidentStore(), nullptr);
    EXPECT_TRUE(tlcs->IsPagedDirectory());
    // Active scan reference stays intact and readable while cached copy fell
    // back.
    EXPECT_EQ(active_res->total_rows, static_cast<uint64_t>(kRows));
    active_res.reset();
    tbl.SetLSTableStore(nullptr);
    EXPECT_LT(budget.ColumnarUsed(), before_used);
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Reopen with AlwaysSpill enabled to force paged-directory lookup path
  // directly and validate zone maps, block row counts, and full scan.
  {
    budget.SetTestAlwaysSpill(true);
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TlcsFile>, paged_tlcs,
                          TlcsFile::Open(tlcs_path, schema));
    EXPECT_TRUE(paged_tlcs->IsPaged());
    EXPECT_TRUE(paged_tlcs->IsPagedDirectory());
    EXPECT_EQ(paged_tlcs->BlockCount(), 2U);

    ASSIGN_OR_ASSERT_FAIL(uint32_t, rows0, paged_tlcs->BlockRowCount(0));
    ASSIGN_OR_ASSERT_FAIL(uint32_t, rows1, paged_tlcs->BlockRowCount(1));
    EXPECT_EQ(rows0, 32768U);
    EXPECT_EQ(rows1, static_cast<uint32_t>(kRows - 32768));

    const std::vector<IntegerPeekCompare> hi_pred = {
        {.column = 0,
         .op = BinaryOperation::kGreaterThanEquals,
         .constant = 33000}};
    EXPECT_FALSE(
        paged_tlcs->BlockMatchesZoneMaps(0, &hi_pred, nullptr, std::nullopt));
    EXPECT_TRUE(
        paged_tlcs->BlockMatchesZoneMaps(1, &hi_pred, nullptr, std::nullopt));

    const std::array<slot_t, 2> proj = {0, 1};
    TlcsFile::DecodedBlock blk;
    ASSERT_SUCCESS(paged_tlcs->DecodeBlockColumns(1, proj, schema, &blk));
    EXPECT_EQ(blk.columns[0].int_cells[0], 32768);
    EXPECT_EQ(paged_tlcs->FindBlockIndex(blk.pid), 1U);
  }

  db->DeleteAll();
}

TEST(TlcsFileTest, BuildLSTableStoreRespectsEffectiveColumnarPoolCap) {
  TlcsBudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  // The storage, query and transaction floors leave ~1.6 MiB for columnar
  // data while allowing the database to reserve its 8 MiB WAL buffer.
  budget.ResetForTest(size_t{32} << 20, size_t{6} << 20, size_t{5} << 20,
                      /*check_rss_pressure=*/false);
  budget.SetPoolFloorsForTest(size_t{6} << 20, 0, size_t{10} << 20,
                              size_t{8} << 20);

  const std::string prefix = "tlcs_lstable_cap_" + RandomString();
  Schema schema("cap_tbl", {Column("k", ValueType::kInt64),
                            Column("v", ValueType::kVarChar)});
  ASSIGN_OR_ASSERT_FAIL(std::unique_ptr<Database>, db,
                        Database::Create(prefix));
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    const std::string payload(80, 'a');
    for (int64_t i = 0; i < 40000; ++i) {
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_,
                     Row({Value(i), Value(payload + std::to_string(i))}))
              .HasValue());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  {
    TransactionContext ctx = db->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "cap_tbl"));
    std::shared_ptr<const LSTableStore> store =
        BuildLSTableStore(ctx.txn_, tbl);
    EXPECT_EQ(store, nullptr);
    EXPECT_LE(budget.ColumnarUsed(),
              budget.EffectivePoolMaxBytes(MemoryPoolKind::kColumnar));
  }

  db->DeleteAll();
}

TEST(TlcsFileTest, NarrowDictMetadataAndLengthsOnlyDecode) {
  TlcsBudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(size_t{256} << 20, size_t{64} << 20, size_t{64} << 20,
                      /*check_rss_pressure=*/false);

  const std::string prefix = "tlcs_narrow_dict_" + RandomString();
  Schema schema("dict_tbl", {Column("narrow_tag", ValueType::kVarChar),
                             Column("wide_text", ValueType::kVarChar)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  page_id_t first_pid = 0;
  constexpr int64_t kRows = 400;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    first_pid = tbl.FirstPageId();
    for (int64_t i = 0; i < kRows; ++i) {
      Value narrow = (i % 10 == 0)   ? Value()
                     : (i % 10 == 1) ? Value("")
                                     : Value("cat_" + std::to_string(i % 5));
      Value wide = Value("wide_payload_" + std::to_string(i) + "_extra");
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_, Row({std::move(narrow), std::move(wide)}))
              .HasValue());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "dict_tbl"));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  const std::string tlcs_path =
      prefix + ".db.tlcs." + std::to_string(first_pid);
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TlcsFile>, tlcs,
                        TlcsFile::Open(tlcs_path, schema));
  ASSERT_EQ(tlcs->DictKinds().size(), 2U);
  EXPECT_EQ(tlcs->DictKinds()[0], TlcsDictKind::kCompleteNarrow);
  EXPECT_NE(tlcs->DictKinds()[1], TlcsDictKind::kCompleteNarrow);

  const std::array<slot_t, 2> proj = {0, 1};
  const std::array<uint8_t, 2> lengths_only = {1, 1};
  TlcsFile::DecodedBlock blk;
  std::vector<uint8_t> io;
  ASSERT_SUCCESS(tlcs->DecodeBlockColumnsWithOptions(
      0, proj, schema, &blk, /*materialize_dict_ints=*/true, &io,
      lengths_only));
  ASSERT_EQ(blk.row_count, static_cast<uint32_t>(kRows));
  EXPECT_TRUE(blk.columns[1].varchar_bytes.empty());
  for (int64_t i = 0; i < kRows; ++i) {
    const auto r = static_cast<size_t>(i);
    if (i % 10 == 0) {
      EXPECT_TRUE(blk.columns[0].IsNull(r));
    } else if (i % 10 == 1) {
      EXPECT_FALSE(blk.columns[0].IsNull(r));
      EXPECT_EQ(blk.columns[0].VarcharLength(r), 0U);
    } else {
      EXPECT_EQ(blk.columns[0].VarcharLength(r),
                std::string("cat_" + std::to_string(i % 5)).size());
    }
    const std::string expected_wide =
        "wide_payload_" + std::to_string(i) + "_extra";
    EXPECT_EQ(blk.columns[1].VarcharLength(r), expected_wide.size());
  }

  const std::array<uint32_t, 2> selected = {219, 220};
  ASSERT_SUCCESS(tlcs->DecodeBlockColumnsWithOptions(
      0, proj, schema, &blk, /*materialize_dict_ints=*/true, &io, {}, false,
      selected));
  EXPECT_EQ(blk.columns[1].VarcharAt(219), "wide_payload_219_extra");
  EXPECT_EQ(blk.columns[1].VarcharAt(220), "wide_payload_220_extra");
  EXPECT_EQ(blk.columns[1].varchar_bytes.size(), 44U);
  EXPECT_TRUE(blk.columns[0].IsNull(220));

  db->DeleteAll();
}

TEST(TlcsFileTest, VarcharBlockBoundsCachingAndEviction) {
  TlcsBudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(size_t{256} << 20, size_t{64} << 20, size_t{64} << 20,
                      /*check_rss_pressure=*/false);

  const std::string prefix = "tlcs_varchar_bounds_" + RandomString();
  Schema schema("vbounds_tbl", {Column("id", ValueType::kInt64),
                                Column("url", ValueType::kVarChar)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  page_id_t first_pid = 0;
  constexpr int64_t kRows = 33500;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    first_pid = tbl.FirstPageId();
    for (int64_t i = 0; i < kRows; ++i) {
      Value u = (i == 0)
                    ? Value()
                    : (i < 32768 ? Value("aaa_prefix_" + std::to_string(i))
                                 : Value("zzz_prefix_" + std::to_string(i)));
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_, Row({Value(i), std::move(u)})).HasValue());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "vbounds_tbl"));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  const std::string tlcs_path =
      prefix + ".db.tlcs." + std::to_string(first_pid);
  {
    budget.SetTestAlwaysSpill(true);
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TlcsFile>, tlcs,
                          TlcsFile::Open(tlcs_path, schema));
    ASSERT_EQ(tlcs->BlockCount(), 2U);

    using BoundsPtr =
        std::shared_ptr<const std::vector<TlcsFile::VarcharBlockBound>>;
    const size_t before_used = budget.ColumnarUsed();
    ASSIGN_OR_ASSERT_FAIL(
        BoundsPtr, bounds,
        tlcs->GetOrComputeVarcharBlockBounds(/*col_idx=*/1, schema));
    ASSERT_EQ(bounds->size(), 2U);
    EXPECT_TRUE((*bounds)[0].has_null);
    EXPECT_TRUE((*bounds)[0].has_min_max);
    EXPECT_EQ((*bounds)[0].min_prefix[0], 'a');
    EXPECT_FALSE((*bounds)[1].has_null);
    EXPECT_TRUE((*bounds)[1].has_min_max);
    EXPECT_EQ((*bounds)[1].min_prefix[0], 'z');
    EXPECT_GT(budget.ColumnarUsed(), before_used);

    // Calling again hits the cached vector cleanly without extra charge.
    const size_t cached_used = budget.ColumnarUsed();
    ASSIGN_OR_ASSERT_FAIL(
        BoundsPtr, bounds_again,
        tlcs->GetOrComputeVarcharBlockBounds(/*col_idx=*/1, schema));
    EXPECT_EQ(bounds_again.get(), bounds.get());
    EXPECT_EQ(budget.ColumnarUsed(), cached_used);

    bounds.reset();
    bounds_again.reset();
    tlcs.reset();
    EXPECT_LE(budget.ColumnarUsed(), before_used);
  }

  db->DeleteAll();
}

TEST(TlcsFileTest, VarcharBlockBoundsFromOrderedDictionary) {
  TlcsBudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(size_t{256} << 20, size_t{64} << 20, size_t{64} << 20,
                      /*check_rss_pressure=*/false);

  const std::string prefix = "tlcs_vbounds_dict_" + RandomString();
  Schema schema("vbdict_tbl", {Column("id", ValueType::kInt64),
                               Column("tag", ValueType::kVarChar)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  page_id_t first_pid = 0;
  constexpr int64_t kRows = 35000;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    first_pid = tbl.FirstPageId();
    for (int64_t i = 0; i < kRows; ++i) {
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_, Row({Value(i), Value(std::string("tag_") +
                                                    std::to_string(i % 5))}))
              .HasValue());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "vbdict_tbl"));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  const std::string tlcs_path =
      prefix + ".db.tlcs." + std::to_string(first_pid);
  budget.SetTestAlwaysSpill(true);
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TlcsFile>, tlcs,
                        TlcsFile::Open(tlcs_path, schema));
  ASSERT_EQ(tlcs->BlockCount(), 2U);
  EXPECT_EQ(tlcs->DictionaryKind(1), TlcsDictKind::kCompleteNarrow);
  EXPECT_EQ(tlcs->DictionaryKind(2), TlcsDictKind::kNone);
  EXPECT_FALSE(tlcs->Dictionary(2).active);
  EXPECT_TRUE(tlcs->Dicts()[1].codes_follow_string_order);

  ASSIGN_OR_ASSERT_FAIL(
      std::shared_ptr<const std::vector<TlcsFile::VarcharBlockBound>>, bounds,
      tlcs->GetOrComputeVarcharBlockBounds(/*col_idx=*/1, schema));
  ASSERT_EQ(bounds->size(), 2U);
  for (const auto& b : *bounds) {
    EXPECT_TRUE(b.has_min_max);
    EXPECT_EQ(std::string_view(b.min_prefix.data(), b.min_len), "tag_0");
    EXPECT_EQ(std::string_view(b.max_prefix.data(), b.max_len), "tag_4");
  }
  db->DeleteAll();
}

TEST(TlcsFileTest,
     CompactWithSecondaryIndexesRebuildsAndSurvivesCrashAndReopen) {
  const std::string prefix = "tlcs_idx_reopen_" + RandomString();
  Schema schema("orders_idx", {Column("id", ValueType::kInt64),
                               Column("grp", ValueType::kInt64),
                               Column("pad", ValueType::kVarChar)});

  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  constexpr int64_t kRows = 240;
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_TRUE(db->CreateTable(ctx, schema).HasValue());
    ASSERT_SUCCESS(db->CreateIndex(
        ctx, "orders_idx", IndexSchema("idx_id", {0}, {}, IndexMode::kUnique)));
    ASSERT_SUCCESS(db->CreateIndex(
        ctx, "orders_idx",
        IndexSchema("idx_grp", {1}, {2}, IndexMode::kNonUnique)));
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "orders_idx"));
    for (int64_t i = 0; i < kRows; ++i) {
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_, Row({Value(i), Value(i % 7),
                                    Value("p_" + std::to_string(i) + "_" +
                                          std::string(110, 'a'))}))
              .HasValue());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "orders_idx"));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Verify unique and non-unique index scans reflect new stub RowPositions.
  RowPosition pos_50;
  RowPosition pos_55;
  {
    TransactionContext ctx = db->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "orders_idx"));
    ASSERT_NE(tbl.GetBackingTlcsStore(), nullptr);
    Iterator pk_it = tbl.BeginIndexScan(ctx.txn_, tbl.GetIndex(0),
                                        Value(int64_t{50}), Value(int64_t{60}));
    std::vector<int64_t> seen_ids;
    while (pk_it.IsValid()) {
      ASSERT_EQ(pk_it.GetStatus(), Status::kSuccess);
      const Row r = *pk_it;
      ASSERT_TRUE(r.IsValid());
      const int64_t id = r[0].value.int_value;
      if (id == 50) {
        pos_50 = pk_it.Position();
      } else if (id == 55) {
        pos_55 = pk_it.Position();
      }
      seen_ids.push_back(id);
      ++pk_it;
    }
    ASSERT_SUCCESS(pk_it.GetStatus());
    EXPECT_EQ(seen_ids.size(), 11U);
    EXPECT_EQ(seen_ids.front(), 50);
    EXPECT_EQ(seen_ids.back(), 60);

    Iterator grp_it = tbl.BeginIndexScan(ctx.txn_, tbl.GetIndex(1),
                                         Value(int64_t{3}), Value(int64_t{3}));
    size_t grp3_count = 0;
    while (grp_it.IsValid()) {
      const Row r = *grp_it;
      ASSERT_TRUE(r.IsValid());
      EXPECT_EQ(r[1].value.int_value, 3);
      ++grp3_count;
      ++grp_it;
    }
    ASSERT_SUCCESS(grp_it.GetStatus());
    EXPECT_EQ(grp3_count, static_cast<size_t>((kRows - 3 + 6) / 7));
  }

  // Update via relocation & delete across stub pages, then cleanly reopen
  // WITHOUT any manual WriteCheckpoint call.
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "orders_idx"));
    ASSERT_TRUE(tbl.Update(ctx.txn_, pos_50,
                           Row({Value(int64_t{50}), Value(int64_t{99}),
                                Value(std::string(500, 'u'))}))
                    .HasValue());
    ASSERT_SUCCESS(tbl.Delete(ctx.txn_, pos_55));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  db.reset();
  db = Database::Create(prefix).MoveValue();

  {
    TransactionContext ctx = db->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "orders_idx"));
    std::vector<Row> rows = ScanAllRows(ctx.txn_, tbl);
    ASSERT_EQ(rows.size(), static_cast<size_t>(kRows - 1));
    Iterator check_55 = tbl.BeginIndexScan(
        ctx.txn_, tbl.GetIndex(0), Value(int64_t{55}), Value(int64_t{55}));
    EXPECT_FALSE(check_55.IsValid());
    ASSERT_SUCCESS(check_55.GetStatus());

    Iterator check_50 = tbl.BeginIndexScan(
        ctx.txn_, tbl.GetIndex(0), Value(int64_t{50}), Value(int64_t{50}));
    ASSERT_TRUE(check_50.IsValid());
    EXPECT_EQ((*check_50)[1].value.int_value, 99);
    EXPECT_EQ((*check_50)[2].value.varchar_value, std::string(500, 'u'));
  }

  // Re-compact, mutate via stub hydration in a fresh transaction, commit, and
  // crash without shutdown writeback: crash recovery must hydrate the stub and
  // preserve every unmodified row plus the post-compaction mutations.
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "orders_idx"));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  RowPosition pos_10;
  {
    TransactionContext ctx = db->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "orders_idx"));
    Iterator pk_10 = tbl.BeginIndexScan(ctx.txn_, tbl.GetIndex(0),
                                        Value(int64_t{10}), Value(int64_t{10}));
    ASSERT_TRUE(pk_10.IsValid());
    pos_10 = pk_10.Position();
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "orders_idx"));
    ASSERT_SUCCESS(tbl.Delete(ctx.txn_, pos_10));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  db->EmulateCrash();
  db.reset();
  db = Database::Create(prefix).MoveValue();
  {
    TransactionContext ctx = db->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "orders_idx"));
    std::vector<Row> rows = ScanAllRows(ctx.txn_, tbl);
    EXPECT_EQ(rows.size(), static_cast<size_t>(kRows - 2));
    Iterator check_10 = tbl.BeginIndexScan(
        ctx.txn_, tbl.GetIndex(0), Value(int64_t{10}), Value(int64_t{10}));
    EXPECT_FALSE(check_10.IsValid());
  }

  db->DeleteAll();
}

// P356/426: TLCS compaction must not flatten MVCC version chains a pinned
// snapshot still needs.  Old behavior: ReplaceTableChainWithColumnarStubs
// demoted the chain regardless of registered version chains (an idle
// multi-statement reader holds no lease and does not block quiescence), so
// the pinned reader's next read served the CURRENT TLCS image -- rows
// "became newer" behind the snapshot.  New behavior: compaction declines
// with kConflicts while any chain is registered, and the pinned reader keeps
// answering from the heap.
TEST(TlcsFileTest, CompactionYieldsToPinnedVersionChains) {
  const std::string prefix = "tlcs_pin_" + RandomString();
  Schema schema("pinned", {Column("k", ValueType::kInt64),
                           Column("v", ValueType::kInt64)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  {
    TransactionContext setup = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(setup, schema));
    for (int64_t i = 0; i < 200; ++i) {
      ASSERT_TRUE(
          tbl.Insert(setup.txn_, Row({Value(i), Value(i * 10)})).HasValue());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }

  TransactionContext reader = db->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, rtbl, db->GetTable(reader, "pinned"));
  RowPosition first_pos;
  {
    Iterator it = rtbl.BeginFullScan(reader.txn_);
    ASSERT_TRUE(it.IsValid());
    first_pos = it.Position();
    ASSERT_EQ((*it)[1], Value(int64_t{0}));
  }
  {
    TransactionContext writer = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, wtbl, db->GetTable(writer, "pinned"));
    ASSIGN_OR_ASSERT_FAIL(
        RowPosition, np,
        wtbl.Update(writer.txn_, first_pos,
                    Row({Value(int64_t{0}), Value(int64_t{999})})));
    (void)np;
    ASSERT_SUCCESS(writer.PreCommit());
  }

  TransactionContext probe = db->BeginContext();
  ASSERT_TRUE(probe.txn_.HasPageVersionChains(first_pos.page_id))
      << "writer committed while a reader snapshot pins the old version";

  // Pre-compaction the pinned snapshot already answers the old image.
  ASSIGN_OR_ASSERT_FAIL_CONST(Row, old_image,
                              rtbl.Read(reader.txn_, first_pos));
  ASSERT_EQ(old_image, (Row{Value(int64_t{0}), Value(int64_t{0})}));

  // The compaction attempt declines instead of flattening that history.
  {
    TransactionContext compactor = db->BeginContext();
    EXPECT_EQ(db->CompactTableToTlcs(compactor, "pinned"), Status::kConflicts);
  }

  // And the pinned reader still sees the old image (old code: 999 after the
  // compaction flattened the page into a stub).
  ASSIGN_OR_ASSERT_FAIL_CONST(Row, still_old,
                              rtbl.Read(reader.txn_, first_pos));
  ASSERT_EQ(still_old, (Row{Value(int64_t{0}), Value(int64_t{0})}));

  db->DeleteAll();
}

// Issue 356/426 read-side branch: an index scan whose transaction is not
// entitled to a physical read must hydrate stub-resolved rows and answer
// them -- not serve the flattened current image.  (During b35 development
// this branch self-deadlocked: ResolveRow kept its shared PageRef latched
// while EnsurePageHydratedIfStub took the exclusive latch on the same page;
// SqlOocFuzzer.SeededSessionsShard10 hung deterministically.  This pin
// guards the latch order.)
TEST(TlcsFileTest, IndexScanOnStubHydratesForNonReadTransaction) {
  const std::string prefix = "tlcs_idx_hydrate_" + RandomString();
  Schema schema("stub_idx", {Column("k", ValueType::kInt64),
                             Column("v", ValueType::kInt64)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  {
    TransactionContext setup = db->BeginContext();
    ASSERT_TRUE(db->CreateTable(setup, schema).HasValue());
    ASSERT_SUCCESS(db->CreateIndex(
        setup, "stub_idx", IndexSchema("idx_k", {0}, {}, IndexMode::kUnique)));
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(setup, "stub_idx"));
    for (int64_t i = 0; i < 200; ++i) {
      ASSERT_TRUE(
          tbl.Insert(setup.txn_, Row({Value(i), Value(i * 10)})).HasValue());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "stub_idx"));
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  {
    // A plain (non-read-only) transaction is not entitled to physical stub
    // reads, so every resolved row walks the hydrate branch.
    TransactionContext rw = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(rw, "stub_idx"));
    ASSERT_NE(tbl.GetBackingTlcsStore(), nullptr);
    Iterator it = tbl.BeginIndexScan(rw.txn_, tbl.GetIndex(0),
                                     Value(int64_t{50}), Value(int64_t{60}));
    std::vector<int64_t> seen;
    while (it.IsValid()) {
      ASSERT_SUCCESS(it.GetStatus());
      const Row r = *it;
      ASSERT_TRUE(r.IsValid());
      seen.push_back(r[0].value.int_value);
      ASSERT_EQ(r[1].value.int_value, r[0].value.int_value * 10);
      ++it;
    }
    ASSERT_SUCCESS(it.GetStatus());
    EXPECT_EQ(seen.size(), 11U);
    EXPECT_EQ(seen.front(), 50);
    EXPECT_EQ(seen.back(), 60);
  }
  db->DeleteAll();
}

TEST(TlcsFileTest, EvictReclaimDropsResidentMmapAndSkipsOversizedMap) {
  TlcsBudgetResetGuard budget_guard;
  auto& budget = ProcessMemoryBudget::Global();
  const std::string prefix = "tlcs_mmap_evict_" + RandomString();
  Schema schema("mmap_tbl", {Column("k", ValueType::kInt64),
                             Column("v", ValueType::kInt64)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  page_id_t first_pid = 0;
  constexpr int64_t kRows = 10000;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    first_pid = tbl.FirstPageId();
    for (int64_t i = 0; i < kRows; ++i) {
      ASSERT_TRUE(tbl.Insert(ctx.txn_, Row({Value(i), Value((i * 7) ^ 0x5555)}))
                      .HasValue());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "mmap_tbl"));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  db.reset();

  const std::string tlcs_path =
      prefix + ".db.tlcs." + std::to_string(first_pid);
  const auto fsize = static_cast<size_t>(std::filesystem::file_size(tlcs_path));
  ASSERT_GT(fsize, 0U);

  const std::array<slot_t, 2> proj = {0, 1};
  const std::array<uint32_t, 3> sample_rows = {0, 17, 500};

  // When fsize <= budget.Limit() / 2, Open opens resident_mmap_; Evict clears
  // both resident_store_ and resident_mmap_, leaving Pread intact.
  budget.ResetForTest(std::max<size_t>(fsize * 4, size_t{32} << 20),
                      size_t{8} << 20, size_t{16} << 20,
                      /*check_rss_pressure=*/false);
  {
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TlcsFile>, tlcs,
                          TlcsFile::Open(tlcs_path, schema));
    TlcsFile::DecodedBlock gather_blk;
    EXPECT_TRUE(tlcs->TryGatherMmapFixedColumns(0, proj, sample_rows, schema,
                                                &gather_blk));
    EXPECT_EQ(gather_blk.columns[0].int_cells[1], 17);
    EXPECT_EQ(gather_blk.columns[1].int_cells[1], (17 * 7) ^ 0x5555);

    EXPECT_GT(tlcs->EvictDecodedCacheForTest(size_t{1} << 40), 0U);
    EXPECT_FALSE(tlcs->TryGatherMmapFixedColumns(0, proj, sample_rows, schema,
                                                 &gather_blk));

    TlcsFile::DecodedBlock pread_blk;
    ASSERT_SUCCESS(tlcs->DecodeBlockColumns(0, proj, schema, &pread_blk));
    EXPECT_EQ(pread_blk.columns[0].int_cells[17], 17);
    EXPECT_EQ(pread_blk.columns[1].int_cells[500], (500 * 7) ^ 0x5555);
  }

  // When fsize exceeds budget.Limit() / 2, Open skips resident_mmap_ and reads
  // via bounded Pread.
  const size_t small_limit = std::max<size_t>(fsize / 2, 1024);
  ASSERT_GT(fsize, small_limit / 2);
  budget.ResetForTest(small_limit, std::max<size_t>(small_limit / 4, 256),
                      std::max<size_t>(small_limit / 2, 512),
                      /*check_rss_pressure=*/false);
  {
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TlcsFile>, tlcs,
                          TlcsFile::Open(tlcs_path, schema));
    TlcsFile::DecodedBlock gather_blk;
    EXPECT_FALSE(tlcs->TryGatherMmapFixedColumns(0, proj, sample_rows, schema,
                                                 &gather_blk));
    TlcsFile::DecodedBlock pread_blk;
    ASSERT_SUCCESS(tlcs->DecodeBlockColumns(0, proj, schema, &pread_blk));
    EXPECT_EQ(pread_blk.columns[0].int_cells[500], 500);
    EXPECT_EQ(pread_blk.columns[1].int_cells[500], (500 * 7) ^ 0x5555);
  }

  // Both spellings of a zero limit skip resident_mmap_.
  for (const char* limit : {"0", "+0"}) {
    ScopedEnvVar no_mmap("TINYLAMB_TLCS_MMAP_LIMIT_BYTES", limit);
    budget.ResetForTest(size_t{1} << 30, size_t{256} << 20, size_t{512} << 20,
                        /*check_rss_pressure=*/false);
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TlcsFile>, tlcs,
                          TlcsFile::Open(tlcs_path, schema));
    TlcsFile::DecodedBlock gather_blk;
    EXPECT_FALSE(tlcs->TryGatherMmapFixedColumns(0, proj, sample_rows, schema,
                                                 &gather_blk));
    TlcsFile::DecodedBlock pread_blk;
    ASSERT_SUCCESS(tlcs->DecodeBlockColumns(0, proj, schema, &pread_blk));
    EXPECT_EQ(pread_blk.columns[0].int_cells[17], 17);
  }

  std::unique_ptr<Database> cleanup_db = Database::Create(prefix).MoveValue();
  cleanup_db->DeleteAll();
}

TEST(TlcsFileTest, TlcsV3PersistsColumnSynopsesAndLoadsInO1) {
  const std::string prefix = "tlcs_v3_syn_" + RandomString();
  Schema schema("v3_syn", {Column("id", ValueType::kInt64),
                           Column("unsorted", ValueType::kInt64),
                           Column("nullable_k", ValueType::kInt64)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  page_id_t first_pid = 0;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    first_pid = tbl.FirstPageId();
    for (int64_t i = 0; i < 200; ++i) {
      const Value nullable = (i % 10 == 0) ? Value() : Value(i * 2);
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_, Row({Value(i + 1), Value(200 - i), nullable}))
              .HasValue());
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "v3_syn"));
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  db->EmulateCrash();
  db.reset();

  const std::string tlcs_path =
      prefix + ".db.tlcs." + std::to_string(first_pid);
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TlcsFile>, tlcs,
                        TlcsFile::Open(tlcs_path, schema));
  EXPECT_EQ(tlcs->FormatVersion(), 3U);

  const std::vector<slot_t> cols = {0, 1, 2};
  auto entries = tlcs->BuildColumnCacheEntries(
      schema, tlcs->LayoutGeneration(), cols, LSTableStore::kBuildByteLimit);
  ASSERT_EQ(entries.size(), 3U);
  ASSERT_NE(entries[0], nullptr);
  ASSERT_NE(entries[1], nullptr);
  ASSERT_NE(entries[2], nullptr);

  EXPECT_FALSE(entries[0]->has_null);
  EXPECT_TRUE(entries[0]->is_monotonic);
  EXPECT_TRUE(entries[0]->is_unique_nonnull);
  EXPECT_TRUE(entries[0]->is_identity_1_to_n);
  EXPECT_EQ(entries[0]->min_int, 1);
  EXPECT_EQ(entries[0]->max_int, 200);

  EXPECT_FALSE(entries[1]->has_null);
  EXPECT_FALSE(entries[1]->is_monotonic);
  EXPECT_FALSE(entries[1]->is_unique_nonnull);
  EXPECT_FALSE(entries[1]->is_identity_1_to_n);
  EXPECT_EQ(entries[1]->min_int, 1);
  EXPECT_EQ(entries[1]->max_int, 200);

  EXPECT_TRUE(entries[2]->has_null);
  EXPECT_FALSE(entries[2]->is_monotonic);
  EXPECT_FALSE(entries[2]->is_unique_nonnull);
  EXPECT_EQ(entries[2]->min_int, 2);
  EXPECT_EQ(entries[2]->max_int, 398);

  tlcs.reset();
  std::unique_ptr<Database> cleanup_db = Database::Create(prefix).MoveValue();
  cleanup_db->DeleteAll();
}

TEST(TlcsFileTest, WriteCheckpointAutoCompactsUnlessDisabled) {
  const std::string prefix = "tlcs_autockpt_" + RandomString();
  Schema non_empty_schema("non_empty_tbl", {Column("k", ValueType::kInt64)});
  Schema empty_schema("empty_tbl", {Column("k", ValueType::kInt64)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  page_id_t pid_non_empty = 0;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, t1, db->CreateTable(ctx, non_empty_schema));
    ASSIGN_OR_ASSERT_FAIL(Table, t2, db->CreateTable(ctx, empty_schema));
    (void)t2;
    pid_non_empty = t1.FirstPageId();
    for (int64_t i = 0; i < 64; ++i) {
      ASSERT_TRUE(t1.Insert(ctx.txn_, Row({Value(i)})).HasValue());
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }

  {
    ScopedEnvVar disabled("TINYLAMB_AUTO_TLCS", "0");
    ASSERT_SUCCESS(db->WriteCheckpoint().GetStatus());
    auto reader = db->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, t1, db->GetTable(reader, "non_empty_tbl"));
    EXPECT_EQ(t1.GetBackingTlcsStore(), nullptr);
    ASSERT_SUCCESS(reader.PreCommit());
  }

  ASSERT_SUCCESS(db->WriteCheckpoint().GetStatus());
  {
    auto reader = db->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, t1, db->GetTable(reader, "non_empty_tbl"));
    ASSIGN_OR_ASSERT_FAIL(Table, t2, db->GetTable(reader, "empty_tbl"));
    EXPECT_NE(t1.GetBackingTlcsStore(), nullptr);
    EXPECT_EQ(t2.GetBackingTlcsStore(), nullptr);
    ASSERT_SUCCESS(reader.PreCommit());
  }
  const std::string tlcs_path =
      prefix + ".db.tlcs." + std::to_string(pid_non_empty);
  EXPECT_TRUE(std::filesystem::exists(tlcs_path));

  db->EmulateCrash();
  db.reset();
  std::unique_ptr<Database> reopened = Database::Create(prefix).MoveValue();
  {
    auto reader = reopened->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, t1,
                          reopened->GetTable(reader, "non_empty_tbl"));
    EXPECT_NE(t1.GetBackingTlcsStore(), nullptr);
    ASSERT_SUCCESS(reader.PreCommit());
  }
  reopened->DeleteAll();
}

TEST(TlcsFileTest,
     StubDecodeNextTypedBatchWithPeekDoesNotDropRowsAcrossBatches) {
  ScopedEnvVar force_paged("TINYLAMB_TLCS_MMAP_LIMIT_BYTES", "1");
  const std::string prefix = "tlcs_stub_peek_batch_" + RandomString();
  Schema schema("peek_stub", {Column("id", ValueType::kInt64),
                              Column("k", ValueType::kInt64)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  constexpr int64_t kRows = 200;
  std::vector<int64_t> expected;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    for (int64_t i = 0; i < kRows; ++i) {
      const int64_t k = i % 10;
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_, Row({Value(i), Value(k)})).GetStatus());
      if (k >= 3 && k < 7) {
        expected.push_back(i);
      }
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "peek_stub"));
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  ASSERT_SUCCESS(db->WriteCheckpoint().GetStatus());
  db.reset();

  // Reopen cleanly so BeginFullScan reads TLCS stub blocks without any resident
  // LSTableStore shortcut.
  std::unique_ptr<Database> reopened = Database::Create(prefix).MoveValue();
  {
    TransactionContext ctx = reopened->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, reopened->GetTable(ctx, "peek_stub"));
    ASSERT_NE(tbl.GetBackingTlcsStore(), nullptr);
    ASSERT_EQ(tbl.GetLSTableStore(), nullptr);

    const std::vector<IntegerPeekCompare> preds{
        {.column = 1, .op = BinaryOperation::kGreaterThanEquals, .constant = 3},
        {.column = 1, .op = BinaryOperation::kLessThan, .constant = 7}};
    TableScanOptions options;
    options.peek_compares = &preds;

    std::vector<int64_t> batched_ids;
    {
      auto scan = tbl.BeginFullScan(ctx.txn_, options);
      TypedCellBatch batch;
      batch.Configure(2, {ValueType::kInt64, ValueType::kInt64},
                      {false, false});
      for (size_t rows = scan.DecodeNextTypedBatch(&batch, 7); rows != 0;
           rows = scan.DecodeNextTypedBatch(&batch, 7)) {
        for (size_t r = 0; r < rows; ++r) {
          batched_ids.push_back(batch.cells[r * 2]);
          EXPECT_GE(batch.cells[(r * 2) + 1], 3);
          EXPECT_LT(batch.cells[(r * 2) + 1], 7);
        }
      }
      ASSERT_SUCCESS(scan.GetStatus());
    }
    EXPECT_EQ(batched_ids, expected);

    std::vector<int64_t> iter_ids;
    {
      auto scan = tbl.BeginFullScan(ctx.txn_, options);
      for (; scan.IsValid(); ++scan) {
        iter_ids.push_back((*scan)[0].value.int_value);
      }
      ASSERT_SUCCESS(scan.GetStatus());
    }
    EXPECT_EQ(iter_ids, expected);
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  reopened->DeleteAll();
}

TEST(TlcsFileTest,
     BuildColumnCacheEntriesMaterializesIntVecForRawDoubleWithMmap) {
  const std::string prefix = "tlcs_rawdbl_intvec_" + RandomString();
  Schema schema("rawdbl_tbl", {Column("id", ValueType::kInt64),
                               Column("dval", ValueType::kDouble)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  constexpr int64_t kRows = 512;
  page_id_t first_pid = 0;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    first_pid = tbl.FirstPageId();
    for (int64_t i = 0; i < kRows; ++i) {
      const double dv = (static_cast<double>(i) * 1.125) + 0.03125;
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_, Row({Value(i), Value(dv)})).GetStatus());
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "rawdbl_tbl"));
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  db->EmulateCrash();
  db.reset();

  const std::string tlcs_path =
      prefix + ".db.tlcs." + std::to_string(first_pid);
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TlcsFile>, tlcs,
                        TlcsFile::Open(tlcs_path, schema));
  const std::vector<slot_t> cols = {0, 1};
  auto entries = tlcs->BuildColumnCacheEntries(
      schema, tlcs->LayoutGeneration(), cols, LSTableStore::kBuildByteLimit);
  ASSERT_EQ(entries.size(), 2U);
  ASSERT_NE(entries[0], nullptr);
  ASSERT_NE(entries[1], nullptr);
  EXPECT_EQ(entries[0]->integer_column.size(), static_cast<size_t>(kRows));
  EXPECT_EQ(entries[1]->integer_column.size(), static_cast<size_t>(kRows));
  for (int64_t i = 0; i < kRows; ++i) {
    EXPECT_EQ(entries[0]->integer_column[static_cast<size_t>(i)], i);
    double got = 0.0;
    const int64_t bits = entries[1]->integer_column[static_cast<size_t>(i)];
    std::memcpy(&got, &bits, sizeof(got));
    EXPECT_DOUBLE_EQ(got, (static_cast<double>(i) * 1.125) + 0.03125);
  }
  tlcs.reset();
  std::unique_ptr<Database> cleanup_db = Database::Create(prefix).MoveValue();
  cleanup_db->DeleteAll();
}

TEST(TlcsFileTest, PreadColumnCachePlanDoesNotDoubleChargeAndSynopsesSurvive) {
  const std::string prefix = "tlcs_pread_syn_" + RandomString();
  Schema schema("syn_plan_tbl", {Column("id", ValueType::kInt64),
                                 Column("dval", ValueType::kDouble)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  constexpr int64_t kRows = 512;
  page_id_t first_pid = 0;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    first_pid = tbl.FirstPageId();
    for (int64_t i = 0; i < kRows; ++i) {
      const double dv = (static_cast<double>(i) * 1.125) + 0.03125;
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_, Row({Value(i + 1), Value(dv)})).GetStatus());
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "syn_plan_tbl"));
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  db->EmulateCrash();
  db.reset();

  const std::string tlcs_path =
      prefix + ".db.tlcs." + std::to_string(first_pid);
  {
    ScopedEnvVar no_mmap("TINYLAMB_TLCS_MMAP_LIMIT_BYTES", "0");
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TlcsFile>, tlcs,
                          TlcsFile::Open(tlcs_path, schema));
    EXPECT_TRUE(tlcs->IsPaged());
    const size_t dval_est = tlcs->EstimateColumnsCacheBytes(schema, {1});
    EXPECT_GE(dval_est, static_cast<size_t>(kRows) * sizeof(int64_t));
    EXPECT_LT(dval_est, (static_cast<size_t>(kRows) * sizeof(int64_t) * 3) / 2);

    EXPECT_TRUE(tlcs->HasColumnSynopses());
    ASSERT_EQ(tlcs->ColumnSynopses().size(), 2U);
    EXPECT_TRUE(tlcs->ColumnSynopses()[0].is_unique_nonnull);
    EXPECT_TRUE(tlcs->ColumnSynopses()[0].is_identity_1_to_n);
    EXPECT_EQ(tlcs->ColumnSynopses()[0].min_int, 1);
    EXPECT_EQ(tlcs->ColumnSynopses()[0].max_int, kRows);

    (void)tlcs->EvictDecodedCacheForTest(size_t{1} << 40);
    EXPECT_TRUE(tlcs->HasColumnSynopses());
    EXPECT_TRUE(tlcs->ColumnSynopses()[0].is_unique_nonnull);
  }
  std::unique_ptr<Database> cleanup_db = Database::Create(prefix).MoveValue();
  cleanup_db->DeleteAll();
}

TEST(
    TlcsFileTest,
    BuildLazyColumnCacheEntriesPopulatesZoneMapsAndDecodesOnlyRequestedBlocks) {
  const std::string prefix = "tlcs_lazy_blocks_" + RandomString();
  Schema schema(
      "lazy_tbl",
      {Column("id", ValueType::kInt64), Column("tag", ValueType::kVarChar),
       Column("txt", ValueType::kVarChar), Column("dbl", ValueType::kDouble)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  constexpr int64_t kRows = 100000;
  page_id_t first_pid = 0;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    first_pid = tbl.FirstPageId();
    for (int64_t i = 0; i < kRows; ++i) {
      Value tag = (i % 7 == 0) ? Value() : Value("t_" + std::to_string(i % 5));
      Value txt = Value("payload_" + std::to_string(i));
      Value dbl =
          (i % 13 == 0) ? Value() : Value(static_cast<double>(i) * 0.25);
      ASSERT_SUCCESS(tbl.Insert(ctx.txn_, Row({Value(i), std::move(tag),
                                               std::move(txt), std::move(dbl)}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "lazy_tbl"));
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  db.reset();
  db = Database::Create(prefix).MoveValue();

  {
    TransactionContext ctx = db->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "lazy_tbl"));
    tbl.SetLSTableStore(nullptr);
    ASSERT_NE(tbl.GetBackingTlcsStore(), nullptr);
    (void)tbl.GetBackingTlcsStore()->EvictDecodedCacheForTest(size_t{1} << 40);
    (void)tbl.EvictIdleColumnarHeadroom(size_t{1} << 40);

    std::shared_ptr<const LSTableStore> store = GetOrBuildLSTableStore(
        ctx.txn_, tbl, /*allow_lazy_build=*/true, /*projection=*/nullptr,
        /*allow_block_lazy=*/true);
    ASSERT_NE(store, nullptr);
    EXPECT_TRUE(store->is_block_lazy);
    ASSERT_EQ(store->blocks.size(), 4U);
    ASSERT_EQ(first_pid, tbl.FirstPageId());

    for (slot_t c = 0; c < 4; ++c) {
      ASSERT_NE(store->column_owners[c], nullptr);
      EXPECT_TRUE(store->column_owners[c]->is_block_lazy);
      EXPECT_EQ(store->column_owners[c]->LazyDecodedBlockCount(), 0U);
      EXPECT_TRUE(store->blocks[3].columns[c].int_cells.empty());
    }
    EXPECT_TRUE(store->blocks[3].columns[0].has_min_max);
    EXPECT_EQ(store->blocks[3].columns[0].min_int, 98304);
    EXPECT_EQ(store->blocks[3].columns[0].max_int, kRows - 1);
    EXPECT_TRUE(store->blocks[0].columns[1].has_null);

    LSTableStore::LazyBlockScratch scratch;
    const LSTablePageBlock* mat3 = store->MaterializeBlockView(3, &scratch);
    ASSERT_NE(mat3, nullptr);
    for (slot_t c = 0; c < 4; ++c) {
      EXPECT_EQ(store->column_owners[c]->LazyDecodedBlockCount(), 1U);
    }
    const size_t row_base3 = 98304;
    ASSERT_EQ(mat3->row_count, static_cast<uint32_t>(kRows - row_base3));
    for (uint32_t r = 0; r < mat3->row_count; ++r) {
      const auto g = static_cast<int64_t>(row_base3 + r);
      EXPECT_FALSE(mat3->columns[0].IsNull(r));
      EXPECT_EQ(mat3->columns[0].IntAt(r), g);
      if (g % 7 == 0) {
        EXPECT_TRUE(mat3->columns[1].IsNull(r));
      } else {
        EXPECT_FALSE(mat3->columns[1].IsNull(r));
        EXPECT_EQ(mat3->columns[1].VarcharAt(r), "t_" + std::to_string(g % 5));
      }
      EXPECT_FALSE(mat3->columns[2].IsNull(r));
      EXPECT_EQ(mat3->columns[2].VarcharAt(r), "payload_" + std::to_string(g));
      if (g % 13 == 0) {
        EXPECT_TRUE(mat3->columns[3].IsNull(r));
      } else {
        EXPECT_FALSE(mat3->columns[3].IsNull(r));
        EXPECT_DOUBLE_EQ(std::bit_cast<double>(mat3->columns[3].int_cells[r]),
                         static_cast<double>(g) * 0.25);
      }
    }

    const std::array<slot_t, 2> only_02{0, 2};
    const LSTablePageBlock* mat1 =
        store->MaterializeBlockView(1, &scratch, only_02);
    ASSERT_NE(mat1, nullptr);
    EXPECT_EQ(store->column_owners[0]->LazyDecodedBlockCount(), 2U);
    EXPECT_EQ(store->column_owners[1]->LazyDecodedBlockCount(), 1U);
    EXPECT_EQ(store->column_owners[2]->LazyDecodedBlockCount(), 2U);
    EXPECT_EQ(store->column_owners[3]->LazyDecodedBlockCount(), 1U);
    EXPECT_EQ(mat1->columns[0].IntAt(5), 32768 + 5);
    EXPECT_EQ(mat1->columns[2].VarcharAt(5),
              "payload_" + std::to_string(32768 + 5));
  }
  db->DeleteAll();
}

TEST(TlcsFileTest, DenseGetOrBuildUpgradesLazyCachedColumnsAndReusesBlocks) {
  const std::string prefix = "tlcs_lazy_upgrade_" + RandomString();
  Schema schema(
      "up_tbl",
      {Column("id", ValueType::kInt64), Column("dt", ValueType::kDate),
       Column("tag", ValueType::kVarChar), Column("txt", ValueType::kVarChar)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  constexpr int64_t kRows = 70000;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    for (int64_t i = 0; i < kRows; ++i) {
      Value dt =
          (i % 11 == 0) ? Value() : Value::DateFromDays(18000 + (i % 500));
      Value tag = (i % 5 == 0) ? Value() : Value("d_" + std::to_string(i % 4));
      Value txt = Value("s_" + std::to_string(i));
      ASSERT_SUCCESS(tbl.Insert(ctx.txn_, Row({Value(i), std::move(dt),
                                               std::move(tag), std::move(txt)}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "up_tbl"));
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  db.reset();
  db = Database::Create(prefix).MoveValue();

  {
    TransactionContext ctx = db->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "up_tbl"));
    tbl.SetLSTableStore(nullptr);
    if (tbl.GetBackingTlcsStore() != nullptr) {
      (void)tbl.GetBackingTlcsStore()->EvictDecodedCacheForTest(size_t{1}
                                                                << 40);
    }
    (void)tbl.EvictIdleColumnarHeadroom(size_t{1} << 40);

    std::shared_ptr<const LSTableStore> lazy_store = GetOrBuildLSTableStore(
        ctx.txn_, tbl, /*allow_lazy_build=*/true, /*projection=*/nullptr,
        /*allow_block_lazy=*/true);
    ASSERT_NE(lazy_store, nullptr);
    EXPECT_TRUE(lazy_store->is_block_lazy);
    ASSERT_EQ(lazy_store->blocks.size(), 3U);

    LSTableStore::LazyBlockScratch scratch;
    ASSERT_NE(lazy_store->MaterializeBlockView(1, &scratch), nullptr);
    for (slot_t c = 0; c < 4; ++c) {
      EXPECT_EQ(lazy_store->column_owners[c]->LazyDecodedBlockCount(), 1U);
    }

    std::shared_ptr<const LSTableStore> dense_store = GetOrBuildLSTableStore(
        ctx.txn_, tbl, /*allow_lazy_build=*/true, /*projection=*/nullptr,
        /*allow_block_lazy=*/false);
    ASSERT_NE(dense_store, nullptr);
    EXPECT_FALSE(dense_store->is_block_lazy);
    EXPECT_EQ(dense_store->IntegerColumn(0).size(), static_cast<size_t>(kRows));
    EXPECT_EQ(dense_store->IntegerColumn(1).size(), static_cast<size_t>(kRows));

    size_t row_base = 0;
    for (const auto & blk : dense_store->blocks) {
      for (uint32_t r = 0; r < blk.row_count; r += 137) {
        const auto g = static_cast<int64_t>(row_base + r);
        EXPECT_EQ(blk.columns[0].int_cells[r], g);
        if (g % 11 == 0) {
          EXPECT_TRUE(blk.columns[1].IsNull(r));
        } else {
          EXPECT_FALSE(blk.columns[1].IsNull(r));
          EXPECT_EQ(blk.columns[1].int_cells[r], 18000 + (g % 500));
        }
        if (g % 5 == 0) {
          EXPECT_TRUE(blk.columns[2].IsNull(r));
        } else {
          EXPECT_FALSE(blk.columns[2].IsNull(r));
          EXPECT_EQ(blk.columns[2].VarcharAt(r), "d_" + std::to_string(g % 4));
        }
        EXPECT_EQ(blk.columns[3].VarcharAt(r), "s_" + std::to_string(g));
      }
      row_base += blk.row_count;
    }
    EXPECT_EQ(row_base, static_cast<size_t>(kRows));
  }
  db->DeleteAll();
}

TEST(TlcsFileTest,
     LazyBlockDecodeFallsBackToTransientUnderTightColumnarBudget) {
  TlcsBudgetResetGuard budget_guard;
  const std::string prefix = "tlcs_lazy_transient_" + RandomString();
  Schema schema("trans_tbl", {Column("id", ValueType::kInt64),
                              Column("txt", ValueType::kVarChar)});
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  constexpr int64_t kRows = 70000;
  {
    TransactionContext ctx = db->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->CreateTable(ctx, schema));
    for (int64_t i = 0; i < kRows; ++i) {
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_,
                     Row({Value(i), Value("row_" + std::to_string(i))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_SUCCESS(db->CompactTableToTlcs(ctx, "trans_tbl"));
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  db.reset();

  ScopedEnvVar no_mmap("TINYLAMB_TLCS_MMAP_LIMIT_BYTES", "0");
  db = Database::Create(prefix).MoveValue();
  {
    TransactionContext ctx = db->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db->GetTable(ctx, "trans_tbl"));
    tbl.SetLSTableStore(nullptr);
    (void)tbl.EvictIdleColumnarHeadroom(size_t{1} << 40);

    std::shared_ptr<const LSTableStore> lazy_store = GetOrBuildLSTableStore(
        ctx.txn_, tbl, /*allow_lazy_build=*/true, /*projection=*/nullptr,
        /*allow_block_lazy=*/true);
    ASSERT_NE(lazy_store, nullptr);
    EXPECT_TRUE(lazy_store->is_block_lazy);

    auto& budget = ProcessMemoryBudget::Global();
    budget.SetColumnarPoolLimitForTest(budget.ColumnarUsed() + 64);

    LSTableStore::LazyBlockScratch scratch;
    const LSTablePageBlock* mat0 =
        lazy_store->MaterializeBlockView(0, &scratch);
    ASSERT_NE(mat0, nullptr);
    EXPECT_FALSE(scratch.transient.empty());
    EXPECT_EQ(lazy_store->column_owners[0]->LazyDecodedBlockCount(), 0U);
    EXPECT_EQ(lazy_store->column_owners[1]->LazyDecodedBlockCount(), 0U);
    ASSERT_EQ(mat0->row_count, 32768U);
    EXPECT_EQ(mat0->columns[0].IntAt(0), 0);
    EXPECT_EQ(mat0->columns[0].IntAt(12345), 12345);
    EXPECT_EQ(mat0->columns[1].VarcharAt(12345), "row_12345");
    scratch.transient.clear();
  }
  db->DeleteAll();
}

}  // namespace
}  // namespace tinylamb
