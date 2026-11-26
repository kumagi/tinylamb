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
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <system_error>
#include <tuple>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "index/index_schema.hpp"
#include "table/iterator.hpp"
#include "table/table.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

struct ScopedDbDir {
  std::filesystem::path dir;
  std::string base;

  ScopedDbDir() {
    dir = std::filesystem::temp_directory_path() /
          ("recov_fuzz_" + RandomString(10));
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    base = (dir / "db").string();
  }

  ~ScopedDbDir() {
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
  }
};

using RowTuple = std::tuple<int64_t, int64_t, std::string>;

Row MakeRow(int64_t id, int64_t val, const std::string& tag) {
  return Row({Value(id), Value(val), Value(std::string(tag))});
}

std::vector<RowTuple> ScanSorted(Database& db) {
  TransactionContext ctx = db.BeginReadOnlyContext();
  auto tbl_res = ctx.GetTable("t");
  assert(tbl_res.HasValue());
  std::vector<RowTuple> out;
  Iterator it = tbl_res.Value()->BeginFullScan(ctx.txn_);
  while (it.IsValid()) {
    const Row& r = *it;
    out.emplace_back(r[0].value.int_value, r[1].value.int_value,
                     r[2].value.varchar_value);
    ++it;
  }
  assert(it.GetStatus() == Status::kSuccess);
  assert(ctx.PreCommit() == Status::kSuccess);
  std::sort(out.begin(), out.end());
  return out;
}

std::vector<RowTuple> ModelSorted(const std::map<int64_t, RowTuple>& model) {
  std::vector<RowTuple> out;
  out.reserve(model.size());
  for (const auto& [k, v] : model) {
    out.push_back(v);
  }
  std::sort(out.begin(), out.end());
  return out;
}

void AssertModelMatch(Database& db, const std::map<int64_t, RowTuple>& model,
                      const char* where) {
  auto got = ScanSorted(db);
  auto want = ModelSorted(model);
  if (got != want) {
    std::fprintf(stderr, "[RECOVERY MISMATCH at %s] got=%zu want=%zu\n", where,
                 got.size(), want.size());
    for (const auto& r : got) {
      std::fprintf(stderr, "  GOT:  id=%lld v=%lld len=%zu\n",
                   static_cast<long long>(std::get<0>(r)),
                   static_cast<long long>(std::get<1>(r)),
                   std::get<2>(r).size());
    }
    for (const auto& r : want) {
      std::fprintf(stderr, "  WANT: id=%lld v=%lld len=%zu\n",
                   static_cast<long long>(std::get<0>(r)),
                   static_cast<long long>(std::get<1>(r)),
                   std::get<2>(r).size());
    }
    assert(false);
  }
}

void VerifyIndexPointLookups(Database& db,
                             const std::map<int64_t, RowTuple>& model) {
  TransactionContext ctx = db.BeginReadOnlyContext();
  auto tbl_res = ctx.GetTable("t");
  assert(tbl_res.HasValue());
  const auto& tbl = *tbl_res.Value();
  if (tbl.IndexCount() == 0) {
    assert(ctx.PreCommit() == Status::kSuccess);
    return;
  }
  for (const auto& [id, tup] : model) {
    Iterator it = tbl.BeginIndexScan(ctx.txn_, tbl.GetIndex(0),
                                     {Value(id)}, {Value(id)}, true);
    int matches = 0;
    while (it.IsValid()) {
      const Row& r = *it;
      if (r[0].value.int_value == id) {
        assert(r[1].value.int_value == std::get<1>(tup));
        assert(r[2].value.varchar_value == std::get<2>(tup));
        ++matches;
      }
      ++it;
    }
    assert(it.GetStatus() == Status::kSuccess);
    assert(matches == 1);
  }
  assert(ctx.PreCommit() == Status::kSuccess);
}

}  // namespace
}  // namespace tinylamb

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  using namespace tinylamb;
  if (size < 4) {
    return 0;
  }
  FuzzedDataProvider fdp(data, size);
  ScopedDbDir scoped;

  auto db_res = Database::Create(scoped.base, /*wal_sync_ms=*/1);
  if (!db_res.HasValue()) {
    return 0;
  }
  std::unique_ptr<Database> db = db_res.MoveValue();
  db->SetSynchronousCommit(true);

  Schema schema("t", {Column("id", ValueType::kInt64),
                      Column("v", ValueType::kInt64),
                      Column("tag", ValueType::kVarChar)});
  {
    TransactionContext ctx = db->BeginContext();
    auto t_res = db->CreateTable(ctx, schema);
    if (!t_res.HasValue()) {
      return 0;
    }
    if (fdp.ConsumeBool()) {
      IndexSchema idx("idx_id", {0}, {}, IndexMode::kNonUnique);
      if (db->CreateIndex(ctx, "t", idx) != Status::kSuccess) {
        return 0;
      }
    }
    assert(ctx.PreCommit() == Status::kSuccess);
  }

  std::map<int64_t, RowTuple> model;
  std::map<int64_t, RowPosition> positions;

  auto refresh_positions = [&]() {
    positions.clear();
    TransactionContext ctx = db->BeginReadOnlyContext();
    auto tbl_res = ctx.GetTable("t");
    assert(tbl_res.HasValue());
    Iterator it = tbl_res.Value()->BeginFullScan(ctx.txn_);
    while (it.IsValid()) {
      positions[(*it)[0].value.int_value] = it.Position();
      ++it;
    }
    assert(ctx.PreCommit() == Status::kSuccess);
  };

  const bool trace = std::getenv("TINYLAMB_FUZZ_TRACE") != nullptr;
  const int max_ops = fdp.ConsumeIntegralInRange<int>(1, 24);
  for (int i = 0; i < max_ops && fdp.remaining_bytes() > 0; ++i) {
    const int op = fdp.ConsumeIntegralInRange<int>(0, 6);
    if (trace) {
      std::fprintf(stderr, "step=%d op=%d model=%zu\n", i, op, model.size());
    }
    if (op == 0) {
      // Committed or aborted insert/update/delete batch
      const bool commit = fdp.ConsumeBool();
      const int batch = fdp.ConsumeIntegralInRange<int>(1, 4);
      auto staged_model = model;
      auto staged_pos = positions;
      TransactionContext ctx = db->BeginContext();
      auto tbl_res = ctx.GetTable("t");
      assert(tbl_res.HasValue());
      auto& tbl = *tbl_res.Value();

      for (int b = 0; b < batch && fdp.remaining_bytes() > 0; ++b) {
        const int64_t id = fdp.ConsumeIntegralInRange<int64_t>(0, 15);
        const int64_t v = fdp.ConsumeIntegralInRange<int64_t>(-1000, 1000);
        const std::string tag = fdp.ConsumeRandomLengthString(12);
        auto it = staged_model.find(id);
        if (it == staged_model.end()) {
          auto pos = tbl.Insert(ctx.txn_, MakeRow(id, v, tag));
          assert(pos.HasValue());
          staged_model[id] = {id, v, tag};
          staged_pos[id] = pos.Value();
        } else if (fdp.ConsumeBool()) {
          auto pos = tbl.Update(ctx.txn_, staged_pos[id], MakeRow(id, v, tag));
          assert(pos.HasValue());
          staged_model[id] = {id, v, tag};
          staged_pos[id] = pos.Value();
        } else {
          assert(tbl.Delete(ctx.txn_, staged_pos[id]) == Status::kSuccess);
          staged_model.erase(id);
          staged_pos.erase(id);
        }
      }
      if (commit) {
        assert(ctx.PreCommit() == Status::kSuccess);
        model = std::move(staged_model);
        positions = std::move(staged_pos);
      } else {
        assert(ctx.Abort() == Status::kSuccess);
      }
    } else if (op == 1) {
      // Checkpoint
      auto ckpt = db->WriteCheckpoint(/*allow_auto_compaction=*/fdp.ConsumeBool());
      assert(ckpt.HasValue());
    } else if (op == 2) {
      // Compact to TLCS
      TransactionContext ctx = db->BeginContext();
      Status st = db->CompactTableToTlcs(ctx, "t",
                                         /*write_checkpoint=*/fdp.ConsumeBool(),
                                         /*refresh_statistics=*/false);
      assert(st == Status::kSuccess);
      assert(ctx.PreCommit() == Status::kSuccess);
      refresh_positions();
    } else if (op == 3) {
      // EmulateCrash + reopen + verify exact model!
      db->EmulateCrash();
      db.reset();
      auto reopened = Database::Create(scoped.base, /*wal_sync_ms=*/1);
      assert(reopened.HasValue());
      db = reopened.MoveValue();
      db->SetSynchronousCommit(true);
      AssertModelMatch(*db, model, "after_crash");
      VerifyIndexPointLookups(*db, model);
      refresh_positions();
    } else if (op == 4) {
      // Uncommitted loser transaction left open across crash!
      {
        TransactionContext loser = db->BeginContext();
        auto tbl_res = loser.GetTable("t");
        assert(tbl_res.HasValue());
        const int64_t id = fdp.ConsumeIntegralInRange<int64_t>(100, 120);
        auto pos = tbl_res.Value()->Insert(
            loser.txn_, MakeRow(id, 9999, fdp.ConsumeRandomLengthString(8)));
        assert(pos.HasValue());
        // Crash without committing or aborting: loser must not survive.
        db->EmulateCrash();
      }
      db.reset();
      auto reopened = Database::Create(scoped.base, /*wal_sync_ms=*/1);
      assert(reopened.HasValue());
      db = reopened.MoveValue();
      db->SetSynchronousCommit(true);
      AssertModelMatch(*db, model, "after_crash");
      VerifyIndexPointLookups(*db, model);
      refresh_positions();
    } else if (op == 5) {
      // Verify live state matches model
      AssertModelMatch(*db, model, "after_crash");
      VerifyIndexPointLookups(*db, model);
    } else if (op == 6 && fdp.remaining_bytes() >= 4) {
      // WAL tail truncation / corruption robustness test (terminal op):
      // Reopening a truncated or bit-flipped WAL must never crash or UB.
      db->EmulateCrash();
      db.reset();
      std::error_code ec;
      for (const auto& entry :
           std::filesystem::directory_iterator(scoped.dir, ec)) {
        const std::string fname = entry.path().filename().string();
        if (fname.find(".log") != std::string::npos &&
            fname.find(".meta") == std::string::npos) {
          const auto sz = std::filesystem::file_size(entry.path(), ec);
          if (!ec && sz > 0) {
            if (fdp.ConsumeBool()) {
              const auto new_sz =
                  fdp.ConsumeIntegralInRange<uint64_t>(0, sz - 1);
              std::filesystem::resize_file(entry.path(), new_sz, ec);
            } else {
              std::fstream fs(entry.path(),
                              std::ios::in | std::ios::out | std::ios::binary);
              if (fs.is_open()) {
                const auto off =
                    fdp.ConsumeIntegralInRange<uint64_t>(0, sz - 1);
                fs.seekp(static_cast<std::streamoff>(off));
                const char byte = static_cast<char>(fdp.ConsumeIntegral<uint8_t>());
                fs.write(&byte, 1);
              }
            }
          }
          break;
        }
      }
      auto reopened = Database::Create(scoped.base, /*wal_sync_ms=*/1);
      if (reopened.HasValue()) {
        // If recovery succeeded (e.g. truncated tail after last complete
        // record), scanning must not crash.
        TransactionContext ctx = reopened.Value()->BeginReadOnlyContext();
        auto tbl_res = ctx.GetTable("t");
        if (tbl_res.HasValue()) {
          Iterator it = tbl_res.Value()->BeginFullScan(ctx.txn_);
          while (it.IsValid()) {
            ++it;
          }
        }
        std::ignore = ctx.PreCommit();
      }
      return 0;
    }
  }

  // Final verification
  assert(ScanSorted(*db) == ModelSorted(model));
  VerifyIndexPointLookups(*db, model);
  return 0;
}
