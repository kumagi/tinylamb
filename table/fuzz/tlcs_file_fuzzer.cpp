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

#include <fuzzer/FuzzedDataProvider.h>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "table/iterator.hpp"
#include "table/scan_options.hpp"
#include "table/table.hpp"
#include "table/tlcs_file.hpp"
#include "type/column.hpp"
#include "type/date.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

struct ScopedTempDir {
  std::filesystem::path dir;
  ScopedTempDir() {
    dir = std::filesystem::temp_directory_path() /
          ("tlcs_file_fuzz_" + RandomString(10));
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
  }
  ~ScopedTempDir() {
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
  }
};

void FuzzCorruptTlcsBytes(const uint8_t* data, size_t size) {
  ScopedTempDir tmp;
  const std::string path = (tmp.dir / "corrupt.tlcs").string();
  {
    std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
    ofs.write(reinterpret_cast<const char*>(data),
              static_cast<std::streamsize>(size));
  }
  Schema stored_schema;
  auto res = TlcsFile::OpenWithStoredSchema(path, &stored_schema);
  if (res.HasValue()) {
    auto& file = *res.Value();
    const uint32_t blocks = std::min<uint32_t>(file.BlockCount(), 4U);
    std::vector<slot_t> cols;
    for (slot_t c = 0; c < std::min<slot_t>(file.ColumnCount(), 8); ++c) {
      cols.push_back(c);
    }
    TlcsFile::DecodedBlock decoded;
    for (uint32_t b = 0; b < blocks; ++b) {
      std::ignore = file.DecodeBlockColumns(b, cols, stored_schema, &decoded);
    }
  }
  Schema dummy("t", {Column("a", ValueType::kInt64),
                     Column("b", ValueType::kVarChar)});
  auto res2 = TlcsFile::Open(path, dummy);
  if (res2.HasValue()) {
    TlcsFile::DecodedBlock decoded;
    const std::array<slot_t, 2> cols = {0, 1};
    if (res2.Value()->BlockCount() > 0) {
      std::ignore = res2.Value()->DecodeBlockColumns(0, cols, dummy, &decoded);
    }
  }
}

bool SameValue(const Value& a, const Value& b) {
  if (a.IsNull() && b.IsNull()) {
    return true;
  }
  if (a.IsNull() != b.IsNull() || a.type != b.type) {
    return false;
  }
  if (a.type == ValueType::kDouble) {
    if (std::isnan(a.value.double_value) && std::isnan(b.value.double_value)) {
      return true;
    }
    return a.value.double_value == b.value.double_value;
  }
  return a == b;
}

bool EvalPeek(const IntegerPeekCompare& cmp, const Value& v) {
  if (v.IsNull()) {
    return false;
  }
  return MatchIntegerCompare(v.value.int_value, cmp.op, cmp.constant);
}

Value MakeDateVal(int64_t days) {
  Value v(days);
  v.type = ValueType::kDate;
  return v;
}

void FuzzTlcsRoundTrip(FuzzedDataProvider& fdp) {
  ScopedTempDir tmp;
  const std::string base = (tmp.dir / "db").string();
  auto db_res = Database::Create(base, /*wal_sync_ms=*/1);
  if (!db_res.HasValue()) {
    return;
  }
  auto db = db_res.MoveValue();
  db->SetSynchronousCommit(true);

  Schema schema("t", {Column("i", ValueType::kInt64),
                      Column("d", ValueType::kDouble),
                      Column("s", ValueType::kVarChar),
                      Column("dt", ValueType::kDate)});
  {
    TransactionContext ctx = db->BeginContext();
    if (!db->CreateTable(ctx, schema).HasValue()) {
      return;
    }
    assert(ctx.PreCommit() == Status::kSuccess);
  }

  const int nrows = fdp.ConsumeIntegralInRange<int>(1, 48);
  std::vector<Row> expected;
  expected.reserve(static_cast<size_t>(nrows));
  {
    TransactionContext ctx = db->BeginContext();
    auto tbl_res = ctx.GetTable("t");
    assert(tbl_res.HasValue());
    for (int r = 0; r < nrows && fdp.remaining_bytes() > 0; ++r) {
      Value vi;
      if (fdp.ConsumeIntegralInRange<int>(0, 5) != 0) {
        const int ikind = fdp.ConsumeIntegralInRange<int>(0, 3);
        if (ikind == 0) {
          vi = Value(fdp.ConsumeIntegralInRange<int64_t>(-10, 10));
        } else if (ikind == 1) {
          vi = Value(fdp.ConsumeIntegral<int64_t>());
        } else if (ikind == 2) {
          vi = Value(std::numeric_limits<int64_t>::min());
        } else {
          vi = Value(std::numeric_limits<int64_t>::max());
        }
      }

      Value vd;
      if (fdp.ConsumeIntegralInRange<int>(0, 5) != 0) {
        const int dkind = fdp.ConsumeIntegralInRange<int>(0, 4);
        if (dkind == 0) {
          vd = Value(fdp.ConsumeFloatingPointInRange<double>(-100.0, 100.0));
        } else if (dkind == 1) {
          vd = Value(std::numeric_limits<double>::quiet_NaN());
        } else if (dkind == 2) {
          vd = Value(std::numeric_limits<double>::infinity());
        } else if (dkind == 3) {
          vd = Value(-0.0);
        } else {
          vd = Value(fdp.ConsumeFloatingPoint<double>());
        }
      }

      Value vs;
      if (fdp.ConsumeIntegralInRange<int>(0, 5) != 0) {
        const int skind = fdp.ConsumeIntegralInRange<int>(0, 3);
        if (skind == 0) {
          vs = Value(std::string("s") +
                     std::to_string(fdp.ConsumeIntegralInRange<int>(0, 4)));
        } else if (skind == 1) {
          vs = Value(std::string("2026-01-02 03:04:05"));
        } else if (skind == 2) {
          vs = Value(std::string(""));
        } else {
          vs = Value(fdp.ConsumeRandomLengthString(40));
        }
      }

      Value vdt;
      if (fdp.ConsumeIntegralInRange<int>(0, 5) != 0) {
        vdt = MakeDateVal(fdp.ConsumeIntegralInRange<int64_t>(-50000, 50000));
      }

      Row row({vi, vd, vs, vdt});
      assert(tbl_res.Value()->Insert(ctx.txn_, row).HasValue());
      expected.push_back(std::move(row));
    }
    assert(ctx.PreCommit() == Status::kSuccess);
  }

  if (expected.empty()) {
    return;
  }

  // Compact to TLCS!
  {
    TransactionContext ctx = db->BeginContext();
    Status st = db->CompactTableToTlcs(ctx, "t", /*write_checkpoint=*/true,
                                       /*refresh_statistics=*/false);
    if (st == Status::kConflicts) {
      std::ignore = ctx.Abort();
      return;
    }
    assert(st == Status::kSuccess);
    assert(ctx.PreCommit() == Status::kSuccess);
  }

  // Verify full scan after TLCS compaction matches pre-compaction rows.
  std::shared_ptr<TlcsFile> tlcs;
  {
    TransactionContext ctx = db->BeginReadOnlyContext();
    auto tbl_res = ctx.GetTable("t");
    assert(tbl_res.HasValue());
    std::vector<Row> got;
    Iterator it = tbl_res.Value()->BeginFullScan(ctx.txn_);
    while (it.IsValid()) {
      got.push_back(*it);
      ++it;
    }
    assert(it.GetStatus() == Status::kSuccess);
    assert(got.size() == expected.size());
    for (size_t i = 0; i < got.size(); ++i) {
      assert(got[i].values_.size() == expected[i].values_.size());
      for (size_t c = 0; c < got[i].values_.size(); ++c) {
        assert(SameValue(got[i][c], expected[i][c]));
      }
    }
    assert(ctx.PreCommit() == Status::kSuccess);
  }

  // Find the written .tlcs file and test direct TlcsFile APIs (zone maps,
  // projected decode, sparse varchar_rows).
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(tmp.dir, ec)) {
    if (entry.path().filename().string().find(".tlcs.") != std::string::npos) {
      auto open_res = TlcsFile::Open(entry.path().string(), schema);
      assert(open_res.HasValue());
      tlcs = open_res.MoveValue();
      break;
    }
  }
  if (!tlcs || tlcs->BlockCount() == 0) {
    return;
  }

  // Zone-map no-false-negative invariant check on column 0 (INT64) and 3 (DATE)
  const BinaryOperation kOps[] = {
      BinaryOperation::kEquals,         BinaryOperation::kNotEquals,
      BinaryOperation::kLessThan,       BinaryOperation::kLessThanEquals,
      BinaryOperation::kGreaterThan,    BinaryOperation::kGreaterThanEquals};
  std::vector<IntegerPeekCompare> peeks;
  const int npeeks = fdp.ConsumeIntegralInRange<int>(1, 3);
  for (int p = 0; p < npeeks; ++p) {
    IntegerPeekCompare cmp;
    cmp.column = fdp.ConsumeBool() ? 0 : 3;
    cmp.op = kOps[fdp.ConsumeIntegralInRange<size_t>(0, 5)];
    cmp.constant = fdp.ConsumeIntegralInRange<int64_t>(-20, 20);
    peeks.push_back(cmp);
  }
  bool any_row_matches = false;
  for (const Row& r : expected) {
    bool row_ok = true;
    for (const auto& cmp : peeks) {
      if (!EvalPeek(cmp, r[cmp.column])) {
        row_ok = false;
        break;
      }
    }
    if (row_ok) {
      any_row_matches = true;
      break;
    }
  }
  bool any_block_matches = false;
  for (uint32_t b = 0; b < tlcs->BlockCount(); ++b) {
    if (tlcs->BlockMatchesZoneMaps(b, &peeks, nullptr, std::nullopt)) {
      any_block_matches = true;
    }
  }
  if (any_row_matches) {
    assert(any_block_matches);
  }

  // Projected decode + sparse varchar_rows check
  const std::array<slot_t, 4> all_cols = {0, 1, 2, 3};
  TlcsFile::DecodedBlock decoded;
  std::vector<uint8_t> scratch;
  size_t total_decoded = 0;
  for (uint32_t b = 0; b < tlcs->BlockCount(); ++b) {
    assert(tlcs->DecodeBlockColumns(b, all_cols, schema, &decoded, true,
                                    &scratch) == Status::kSuccess);
    total_decoded += decoded.row_count;
  }
  assert(total_decoded == expected.size());
}

}  // namespace
}  // namespace tinylamb

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size < 2) {
    return 0;
  }
  if ((data[0] & 1U) == 0) {
    tinylamb::FuzzCorruptTlcsBytes(data + 1, size - 1);
  } else {
    FuzzedDataProvider fdp(data + 1, size - 1);
    tinylamb::FuzzTlcsRoundTrip(fdp);
  }
  return 0;
}
