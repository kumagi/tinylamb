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

#include "full_scan_iterator.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/platform/file_io.hpp"
#include "common/platform/process.hpp"
#include "common/random_string.hpp"
#include "common/runtime_filter.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "gtest/gtest.h"
#include "iterator.hpp"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

void ForceScanWalFailure(Logger* logger) {
  const int full_fd = platform::OpenWriteFailingDescriptor();
  ASSERT_GE(full_fd, 0);
  ASSERT_EQ(platform::Dup2(full_fd, logger->Fd()), logger->Fd());
  ASSERT_EQ(platform::CloseFile(full_fd), 0);
  std::ignore = logger->AddLog("dead");
  ASSERT_NE(logger->WaitForDurable(logger->BufferedLSN()), Status::kSuccess);
  ASSERT_TRUE(logger->Failed());
}

}  // namespace

class FullScanIteratorTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "full_scan_iterator_test-" + RandomString();
    Recover();
    Schema sc("SampleTable", {Column("col1", ValueType::kInt64,
                                     Constraint(Constraint::kIndex)),
                              Column("col2", ValueType::kVarChar),
                              Column("col3", ValueType::kDouble)});
    TransactionContext ctx = db_->BeginContext();
    ASSERT_SUCCESS(db_->CreateTable(ctx, sc).GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  void Recover() {
    if (db_) {
      db_->EmulateCrash();
      db_.reset();
    }
    db_ = Database::Create(prefix_).MoveValue();
  }

  void TearDown() override { db_->DeleteAll(); }

  std::string prefix_;
  std::unique_ptr<Database> db_;
};

TEST_F(FullScanIteratorTest, Construct_Default_Succeeds) {}

TEST_F(FullScanIteratorTest,
       ExactUniquenessSurvivesRejectedDictionaryEncoding) {
  auto context = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      db_->CreateTable(
          context,
          Schema("UniqueProof", {Column("k", ValueType::kInt64),
                                 Column("duplicate", ValueType::kInt64),
                                 Column("nullable", ValueType::kInt64)})));
  for (int64_t i = 0; i < 8192; ++i) {
    // Exceed the separate first-column dense-domain uniqueness bitmap.
    const int64_t k = ((i * 4093) % 8192) * 100003;
    ASSERT_SUCCESS(table
                       .Insert(context.txn_, Row({Value(k), Value(i % 4096),
                                                  i < 2 ? Value() : Value(k)}))
                       .GetStatus());
  }
  const auto store = BuildLSTableStore(context.txn_, table);
  ASSERT_NE(store, nullptr);
  EXPECT_EQ(store->column_is_monotonic[0], 0);
  EXPECT_EQ(store->dictionaries[0].wide_code_bytes, 0);
  EXPECT_EQ(store->column_is_unique_nonnull[0], 1);
  EXPECT_EQ(store->column_is_unique_nonnull[1], 0);
  EXPECT_EQ(store->column_is_unique_nonnull[2], 0);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(FullScanIteratorTest,
       AuditRejectedRuntimeFilterPreservesLegacyKeyFilter) {
  auto context = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(context, "SampleTable"));
  for (int64_t key : {1, 2, 3}) {
    ASSERT_SUCCESS(
        table.Insert(context.txn_, Row({Value(key), Value("s"), Value(1.0)}))
            .GetStatus());
  }
  std::unordered_set<int64_t> keys{1, 3};
  TableScanOptions options;
  options.key_filter = &keys;
  options.key_column = 0;
  RuntimeFilterBuilder builder;
  builder.Observe(3);
  const auto group = builder.Build();
  for (bool absent : {false, true}) {
    auto iterator = table.BeginFullScan(context.txn_, options);
    iterator.SetRuntimeFilter(absent ? nullptr : group.get(), 1,
                              ValueType::kInt64);
    std::vector<int64_t> actual;
    while (iterator.IsValid()) {
      actual.push_back((*iterator)[0].value.int_value);
      ++iterator;
    }
    EXPECT_EQ(actual, (std::vector<int64_t>{1, 3}));
    EXPECT_EQ(iterator.GetStatus(), Status::kSuccess);
  }
}

TEST_F(FullScanIteratorTest, AuditRuntimeFilterRebuildsPeekForNewColumn) {
  auto context = db_->BeginContext();
  const Schema schema("two_ints", {Column("a", ValueType::kInt64),
                                   Column("b", ValueType::kInt64)});
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->CreateTable(context, schema));
  for (int64_t key : {1, 2, 3}) {
    ASSERT_SUCCESS(table.Insert(context.txn_, Row({Value(key), Value(key + 1)}))
                       .GetStatus());
  }
  const std::unordered_set<int64_t> keys{1, 2, 3};
  TableScanOptions options;
  options.key_filter = &keys;
  options.key_column = 0;
  auto iterator = table.BeginFullScan(context.txn_, options);
  RuntimeFilterBuilder builder;
  builder.Observe(3);
  const auto group = builder.Build();
  iterator.SetRuntimeFilter(group.get(), 1, ValueType::kInt64);
  ASSERT_TRUE(iterator.IsValid());
  EXPECT_EQ((*iterator)[0], Value(2));
  ++iterator;
  EXPECT_FALSE(iterator.IsValid());
  EXPECT_EQ(iterator.GetStatus(), Status::kSuccess);
}

TEST_F(FullScanIteratorTest, NumericStreamsOwnPageSlicesAndPreserveBits) {
  constexpr int64_t kRows = 4097;
  const auto make_row = [](int64_t row) {
    constexpr std::array<uint64_t, 6> kDoubleBits{0,
                                                  0x8000000000000000ULL,
                                                  0x7ff0000000000000ULL,
                                                  0xfff0000000000000ULL,
                                                  0x7ff8000000000042ULL,
                                                  1};
    const int64_t integer = row == 1   ? std::numeric_limits<int64_t>::min()
                            : row == 2 ? std::numeric_limits<int64_t>::max()
                                       : -row;
    return Row({row % 11 == 0 ? Value() : Value(integer),
                row % 13 == 0 ? Value() : Value::DateFromDays(row - 2000),
                row % 17 == 0
                    ? Value()
                    : Value(std::bit_cast<double>(kDoubleBits[row % 6])),
                Value(), Value(std::string(80, 'p'))});
  };
  {
    auto context = db_->BeginContext();
    const Schema schema(
        "NumericStreams",
        {Column("n", ValueType::kInt64), Column("d", ValueType::kDate),
         Column("f", ValueType::kDouble), Column("z", ValueType::kInt64),
         Column("s", ValueType::kVarChar)});
    ASSERT_SUCCESS(db_->CreateTable(context, schema).GetStatus());
    ASSIGN_OR_ASSERT_FAIL(Table, table,
                          db_->GetTable(context, "NumericStreams"));
    for (int64_t i = 0; i < kRows; ++i) {
      ASSERT_SUCCESS(table.Insert(context.txn_, make_row(i)).GetStatus());
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  auto reader = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(reader, "NumericStreams"));
  const auto store = BuildLSTableStore(reader.txn_, table);
  ASSERT_NE(store, nullptr);
  ASSERT_GT(store->blocks.size(), 1U);
  ASSERT_EQ(store->integer_columns.size(), 5U);
  EXPECT_TRUE(store->integer_columns[4].empty());
  size_t base = 0;
  for (const auto& block : store->blocks) {
    EXPECT_TRUE(block.columns[4].int_cells.empty());
    for (size_t c = 0; c < 4; ++c) {
      const auto& cells = store->integer_columns[c];
      const auto& column = block.columns[c];
      ASSERT_EQ(cells.size(), kRows);
      EXPECT_EQ(column.int_cells.data(), cells.data() + base);
      ASSERT_EQ(column.int_cells.size(), block.row_count);
      for (size_t r = 0; r < block.row_count; ++r) {
        const auto expected = make_row(static_cast<int64_t>(base + r));
        EXPECT_EQ(column.IsNull(r), expected[c].IsNull());
        const int64_t bits =
            expected[c].IsNull() ? 0
            : c == 2 ? std::bit_cast<int64_t>(expected[c].value.double_value)
                     : expected[c].value.int_value;
        EXPECT_EQ(column.int_cells[r], bits);
        EXPECT_EQ(cells[base + r], bits);
      }
    }
    base += block.row_count;
  }
  EXPECT_EQ(base, kRows);
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(FullScanIteratorTest, NarrowDictionaryStreamsOwnAllPageSlices) {
  constexpr int64_t kRows = 1200;
  const auto text = [](int64_t row) {
    if (row % 17 == 0) {
      return Value();
    }
    if (row % 19 == 0) {
      return Value("");
    }
    std::string value = "東京" + std::string(80, 'p');
    value.push_back('\0');
    value += std::to_string(row % 37);
    return Value(std::move(value));
  };
  {
    auto context = db_->BeginContext();
    const Schema schema(
        "NarrowStreams",
        {Column("n", ValueType::kInt64), Column("d", ValueType::kDouble),
         Column("b", ValueType::kVarChar), Column("s", ValueType::kVarChar)});
    ASSERT_SUCCESS(db_->CreateTable(context, schema).GetStatus());
    ASSIGN_OR_ASSERT_FAIL(Table, table,
                          db_->GetTable(context, "NarrowStreams"));
    for (int64_t i = 0; i < kRows; ++i) {
      ASSERT_SUCCESS(
          table
              .Insert(
                  context.txn_,
                  Row({Value(i % 7), Value(static_cast<double>(i % 11) * 0.25),
                       Value(std::string(1, static_cast<char>('a' + (i % 7)))),
                       text(i)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  auto reader = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(reader, "NarrowStreams"));
  const auto store = BuildLSTableStore(reader.txn_, table);
  ASSERT_NE(store, nullptr);
  ASSERT_GT(store->blocks.size(), 1U);
  for (size_t c = 0; c < 4; ++c) {
    const auto& dictionary = store->dictionaries[c];
    ASSERT_TRUE(dictionary.active);
    ASSERT_EQ(dictionary.narrow_codes.size(), kRows);
    EXPECT_TRUE(dictionary.wide_codes.empty());
    size_t base = 0;
    for (const auto& block : store->blocks) {
      const auto& column = block.columns[c];
      EXPECT_EQ(column.dict_codes.data(),
                dictionary.narrow_codes.data() + base);
      EXPECT_EQ(column.dict_codes.size(), block.row_count);
      EXPECT_EQ(column.DictionaryCodeAt(block.row_count), UINT32_MAX);
      for (size_t r = 0; r < block.row_count; ++r) {
        const size_t row = base + r;
        const uint32_t code = column.DictionaryCodeAt(r);
        EXPECT_EQ(code, dictionary.DictionaryCodeAt(row));
        if (c == 0) {
          ASSERT_LT(code, dictionary.int_entries.size());
          EXPECT_EQ(dictionary.int_entries[code], row % 7);
        } else if (c == 1) {
          ASSERT_LT(code, dictionary.double_entries.size());
          EXPECT_EQ(dictionary.double_entries[code],
                    static_cast<double>(row % 11) * 0.25);
        } else if (c == 2) {
          EXPECT_EQ(column.VarcharAt(r),
                    std::string(1, static_cast<char>('a' + (row % 7))));
        } else {
          const auto expected = text(static_cast<int64_t>(row));
          EXPECT_EQ(column.IsNull(r), expected.IsNull());
          if (expected.IsNull()) {
            EXPECT_EQ(code, LSTableColumnDictionary::kNullCode);
          } else {
            EXPECT_EQ(column.VarcharAt(r), expected.value.varchar_value);
          }
        }
      }
      base += block.row_count;
    }
    EXPECT_EQ(base, kRows);
    EXPECT_EQ(dictionary.DictionaryCodeAt(base), UINT32_MAX);
  }
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(FullScanIteratorTest, PackedIntegerDictionaryKeepsNullsAndBoundaries) {
  for (const int64_t cardinality : {int64_t{1}, int64_t{300}, int64_t{65536}}) {
    const std::string name = "PackedIntegers" + std::to_string(cardinality);
    const int64_t rows = std::max(int64_t{8193}, (cardinality * 3) + 1);
    const auto value_at = [cardinality](int64_t i) {
      if (i % 17 == 0) {
        return Value();
      }
      const int64_t code = i % cardinality;
      return Value(cardinality == 1 ? 42
                   : code == 0      ? INT64_MIN
                   : code == 1      ? INT64_MAX
                                    : code - 150);
    };
    {
      auto setup = db_->BeginContext();
      const Schema schema(
          name, {Column("n", ValueType::kInt64), Column("k", ValueType::kInt64),
                 Column("d", ValueType::kDate)});
      ASSERT_SUCCESS(db_->CreateTable(setup, schema).GetStatus());
      ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(setup, name));
      for (int64_t i = 0; i < rows; ++i) {
        ASSERT_SUCCESS(
            table
                .Insert(
                    setup.txn_,
                    Row({Value(i), value_at(i),
                         i % 23 == 0 ? Value() : Value::DateFromDays(i % 31)}))
                .GetStatus());
      }
      ASSERT_SUCCESS(setup.PreCommit());
    }
    auto reader = db_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(reader, name));
    const auto store = BuildLSTableStore(reader.txn_, table);
    ASSERT_NE(store, nullptr);
    for (const size_t c : {size_t{1}, size_t{2}}) {
      const auto& dictionary = store->dictionaries[c];
      ASSERT_FALSE(dictionary.active);
      ASSERT_EQ(dictionary.packed_code_bits,
                std::bit_width(dictionary.int_entries.size()));
      ASSERT_GE(dictionary.packed_code_bits, 1U);
      EXPECT_EQ(dictionary.DictionaryCodeCount(), static_cast<size_t>(rows));
      EXPECT_EQ(
          dictionary.wide_codes.size(),
          (((static_cast<size_t>(rows) * dictionary.packed_code_bits) + 7) /
           8) +
              7);
      EXPECT_LT(dictionary.wide_codes.size(),
                static_cast<size_t>(rows) * dictionary.wide_code_bytes);
      size_t base = 0;
      for (const auto& block : store->blocks) {
        const auto& column = block.columns[c];
        EXPECT_EQ(column.DictionaryCodeAt(block.row_count), UINT32_MAX);
        for (size_t row = 0; row < block.row_count; ++row) {
          const int64_t n = block.columns[0].int_cells[row];
          const auto expected = c == 1        ? value_at(n)
                                : n % 23 == 0 ? Value()
                                              : Value::DateFromDays(n % 31);
          const uint32_t code = dictionary.DictionaryCodeAt(base + row);
          EXPECT_EQ(code, column.DictionaryCodeAt(row));
          if (expected.IsNull()) {
            EXPECT_EQ(code, dictionary.int_entries.size());
          } else {
            ASSERT_LT(code, dictionary.int_entries.size());
            EXPECT_EQ(dictionary.int_entries[code], expected.value.int_value);
          }
        }
        base += block.row_count;
      }
      EXPECT_EQ(base, dictionary.DictionaryCodeCount());
      EXPECT_EQ(dictionary.DictionaryCodeAt(base), UINT32_MAX);
      EXPECT_EQ(dictionary.DictionaryCodeAt(SIZE_MAX), UINT32_MAX);
    }
    ASSERT_SUCCESS(reader.PreCommit());
  }
}

TEST_F(FullScanIteratorTest, WideStringDictionaryKeepsBytesAndSnapshot) {
  for (const int64_t cardinality : {int64_t{300}, int64_t{65536}}) {
    const std::string name = "WideStrings" + std::to_string(cardinality);
    const int64_t rows = cardinality == 300 ? 3000 : cardinality * 3;
    const auto text = [cardinality](int64_t i) {
      // Some complete page blocks contain only one-byte values; their old
      // raw-byte fast path must be disabled after installing wider codes.
      if (cardinality == 300 && i < 1024) {
        return Value(std::string(1, static_cast<char>('a' + (i % 26))));
      }
      if (i % 17 == 0) {
        return Value();
      }
      if (i % 19 == 0) {
        return Value("");
      }
      std::string value = "é東京" + std::string(80, 'p');
      value.push_back('\0');
      value += std::to_string(i % cardinality);
      return Value(std::move(value));
    };
    {
      auto setup = db_->BeginContext();
      const Schema schema(name, {Column("k", ValueType::kInt64),
                                 Column("s", ValueType::kVarChar),
                                 Column("v", ValueType::kDouble)});
      ASSERT_SUCCESS(db_->CreateTable(setup, schema).GetStatus());
      ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(setup, name));
      for (int64_t i = 0; i < rows; ++i) {
        ASSERT_SUCCESS(
            table.Insert(setup.txn_, Row({Value(i), text(i), Value(1.0)}))
                .GetStatus());
      }
      ASSERT_SUCCESS(setup.PreCommit());
    }
    auto reader = db_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(reader, name));
    const auto store = BuildLSTableStore(reader.txn_, table);
    ASSERT_NE(store, nullptr);
    const size_t width = cardinality < 65535 ? 2 : 4;
    ASSERT_EQ(store->dictionaries[1].wide_code_bytes, width);
    ASSERT_FALSE(store->dictionaries[1].active);
    size_t row_offset = 0;
    const auto& dictionary = store->dictionaries[1];
    EXPECT_EQ(dictionary.wide_codes.size(), static_cast<size_t>(rows) * width);
    for (const auto& block : store->blocks) {
      const auto& strings = block.columns[1];
      EXPECT_TRUE(strings.varchar_ends.empty());
      EXPECT_TRUE(strings.varchar_bytes.empty());
      EXPECT_EQ(strings.wide_code_begin, row_offset);
      EXPECT_EQ(strings.wide_code_count, block.row_count);
      EXPECT_EQ(strings.DictionaryCodeAt(block.row_count), UINT32_MAX);
      for (uint32_t r = 0; r < block.row_count; ++r) {
        EXPECT_TRUE(store->MayContainInteger(0, block.columns[0].int_cells[r]));
        const auto expected = text(block.columns[0].int_cells[r]);
        EXPECT_EQ(strings.DictionaryCodeAt(r),
                  dictionary.DictionaryCodeAt(row_offset + r));
        EXPECT_EQ(strings.IsNull(r), expected.IsNull());
        if (!expected.IsNull()) {
          EXPECT_EQ(strings.VarcharAt(r), expected.value.varchar_value);
        }
      }
      row_offset += block.row_count;
    }
    EXPECT_EQ(dictionary.DictionaryCodeAt(static_cast<size_t>(rows)),
              UINT32_MAX);
    const auto check_scan = [&] {
      auto scan = table.BeginFullScan(reader.txn_, {0, 1});
      TypedCellBatch batch;
      size_t count = 0;
      while (const size_t n = scan.DecodeNextTypedBatch(&batch, 137)) {
        for (size_t r = 0; r < n; ++r) {
          const auto expected = text(batch.cells[r * batch.stride]);
          EXPECT_EQ(batch.nulls[(r * batch.stride) + 1] != 0,
                    expected.IsNull());
          if (!expected.IsNull()) {
            EXPECT_EQ(batch.Varchar(r, 0), expected.value.varchar_value);
          }
        }
        count += n;
      }
      EXPECT_EQ(count, static_cast<size_t>(rows));
      EXPECT_EQ(scan.GetStatus(), Status::kSuccess);
    };
    check_scan();
    {
      auto writer = db_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(Table, current, db_->GetTable(writer, name));
      auto scan = current.BeginFullScan(writer.txn_);
      const auto position = scan.Position();
      scan.DropPageLatch();
      ASSERT_SUCCESS(current
                         .Update(writer.txn_, position,
                                 Row({Value(0), Value("updated"), Value(1.0)}))
                         .GetStatus());
      ASSERT_SUCCESS(writer.PreCommit());
    }
    check_scan();
    ASSERT_SUCCESS(reader.PreCommit());
  }
}

TEST_F(FullScanIteratorTest, BeginFullScan_AllRows_ScansSuccessfully) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(ctx, "SampleTable"));

  for (int i = 0; i < 130; ++i) {
    ASSERT_SUCCESS(
        table
            .Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                   Value(0.1 + i)}))
            .GetStatus());
  }
  Iterator it = table.BeginFullScan(ctx.txn_);
  while (it.IsValid()) {
    LOG(TRACE) << *it;
    ++it;
  }
}

TEST_F(FullScanIteratorTest,
       BeginFullScan_WithProjection_ReturnsRequestedColumnsInOrder) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(ctx, "SampleTable"));
  ASSERT_SUCCESS(
      table.Insert(ctx.txn_, Row({Value(42), Value("not copied"), Value(3.5)}))
          .GetStatus());

  Iterator projected = table.BeginFullScan(ctx.txn_, {0, 2});
  ASSERT_TRUE(projected.IsValid());
  ASSERT_EQ((*projected).values_.size(), 2);
  EXPECT_EQ((*projected)[0], Value(42));
  EXPECT_EQ((*projected)[1], Value(3.5));

  Iterator no_columns = table.BeginFullScan(ctx.txn_, std::vector<slot_t>{});
  ASSERT_TRUE(no_columns.IsValid());
  EXPECT_TRUE((*no_columns).values_.empty());
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanIteratorTest,
       BeginFullScan_AfterDeletingFirstSlot_ScansRemainingRows) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(ctx, "SampleTable"));
  RowPosition first;
  for (int i = 0; i < 6; ++i) {
    ASSIGN_OR_ASSERT_FAIL(
        RowPosition, position,
        table.Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                    Value(0.1 + i)})));
    if (i == 0) {
      first = position;
    }
  }
  ASSERT_SUCCESS(table.Delete(ctx.txn_, first));

  int count = 0;
  int64_t maximum = 0;
  for (Iterator it = table.BeginFullScan(ctx.txn_); it.IsValid(); ++it) {
    ++count;
    maximum = std::max(maximum, (*it)[0].value.int_value);
  }

  EXPECT_EQ(count, 5);
  EXPECT_EQ(maximum, 5);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanIteratorTest,
       BeginFullScan_OldSnapshotAfterDelete_ScansPhysicallyDeletedTailRow) {
  TransactionContext seed = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(seed, "SampleTable"));
  RowPosition tail;
  for (int i = 0; i < 2; ++i) {
    ASSIGN_OR_ASSERT_FAIL(
        RowPosition, inserted,
        table.Insert(seed.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                     Value(0.1 + i)})));
    tail = inserted;
  }
  ASSERT_SUCCESS(seed.PreCommit());

  TransactionContext old_reader = db_->BeginContext();
  TransactionContext writer = db_->BeginContext();
  ASSERT_SUCCESS(table.Delete(writer.txn_, tail));
  ASSERT_SUCCESS(writer.PreCommit());

  size_t old_count = 0;
  for (Iterator it = table.BeginFullScan(old_reader.txn_); it.IsValid(); ++it) {
    ++old_count;
  }
  EXPECT_EQ(old_count, 2);
  ASSERT_SUCCESS(old_reader.PreCommit());

  TransactionContext fresh_reader = db_->BeginContext();
  size_t fresh_count = 0;
  for (Iterator it = table.BeginFullScan(fresh_reader.txn_); it.IsValid();
       ++it) {
    ++fresh_count;
  }
  EXPECT_EQ(fresh_count, 1);
  ASSERT_SUCCESS(fresh_reader.PreCommit());
}

TEST_F(FullScanIteratorTest,
       BeginMorselScan_WithMorsels_IteratesOnlyRequestedPages) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(ctx, "SampleTable"));
  for (int i = 0; i < 130; ++i) {
    ASSERT_SUCCESS(
        table
            .Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                   Value(0.1 + i)}))
            .GetStatus());
  }
  const std::vector<Table::ScanMorsel> morsels =
      table.BuildScanMorsels(ctx.txn_, 2);
  ASSERT_FALSE(morsels.empty());

  int64_t seen = 0;
  for (const Table::ScanMorsel& morsel : morsels) {
    Iterator it = table.BeginMorselScan(ctx.txn_, morsel);
    while (it.IsValid()) {
      ASSERT_EQ((*it).values_.size(), 3U);
      ++seen;
      ++it;
    }
  }
  EXPECT_EQ(seen, 130);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanIteratorTest, BeginFullScan_EmptyTable_YieldsNoRows) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(Table, table, db_->GetTable(ctx, "SampleTable"));
  Iterator it = table.BeginFullScan(ctx.txn_);

  EXPECT_FALSE(it.IsValid());
  EXPECT_FALSE(it.Position().IsValid());
  ++it;
  EXPECT_FALSE(it.IsValid());
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanIteratorTest, OutputOperator_WhenCalled_PrintsScanName) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(ctx, "SampleTable"));
  ASSERT_SUCCESS(
      table.Insert(ctx.txn_, Row({Value(7), Value("seven"), Value(0.7)}))
          .GetStatus());
  Iterator it = table.BeginFullScan(ctx.txn_);
  ASSERT_TRUE(it.IsValid());

  std::ostringstream output;
  output << it;

  EXPECT_NE(output.str().find("FullScan"), std::string::npos);
  EXPECT_NE(output.str().find("SampleTable"), std::string::npos);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanIteratorTest, ReadVersionFailureTerminatesScanSticky) {
  const std::string db_name = "full_scan_failure-" + RandomString() + ".db";
  const std::string log_name = "full_scan_failure-" + RandomString() + ".log";
  {
    auto page_manager = PageManager::Create(db_name, 10).MoveValue();
    auto logger = Logger::Create(log_name).MoveValue();
    RecoveryManager recovery(log_name, page_manager->GetPool());
    TransactionManager transactions(page_manager.get(), logger.get(),
                                    &recovery);

    Transaction pin = transactions.Begin(true);
    Transaction seed = transactions.Begin();
    PageRef allocated =
        page_manager->AllocateNewPage(seed, PageType::kRowPage).MoveValue();
    const page_id_t page_id = allocated->PageID();
    allocated.PageUnlock();
    Table table(Schema("failure_table", {Column("value", ValueType::kInt64)}),
                page_id);
    ASSERT_SUCCESS(table.Insert(seed, Row({Value(7)})).GetStatus());
    ASSERT_SUCCESS(seed.PreCommit());

    Transaction reader = transactions.Begin(true);
    Transaction broken = transactions.Begin();
    ASSERT_TRUE(broken.AddWriteSet(RowPosition(page_id + 1000, 0)));
    ForceScanWalFailure(logger.get());
    EXPECT_NE(broken.PreCommit(), Status::kSuccess);
    ASSERT_TRUE(transactions.FailStopped());

    Iterator scan = table.BeginFullScan(reader);
    EXPECT_FALSE(scan.IsValid());
    EXPECT_EQ(scan.GetStatus(), Status::kIOError);
  }
  std::ignore = std::remove(db_name.c_str());
  tinylamb::RemoveLogAndSegments(log_name);
}

}  // namespace tinylamb
