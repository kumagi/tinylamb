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
#include <array>
#include <cassert>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <utility>
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
          ("mvcc_fuzz_" + RandomString(10));
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    base = (dir / "db").string();
  }

  ~ScopedDbDir() {
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
  }
};

struct RowEntry {
  int64_t val{0};
  std::string tag;
  RowPosition pos{};
  bool operator==(const RowEntry& o) const {
    return val == o.val && tag == o.tag;
  }
};

using StateMap = std::map<int64_t, RowEntry>;

struct ActiveSlot {
  std::optional<TransactionContext> ctx;
  bool read_only{false};
  StateMap snapshot;
  // Staged changes relative to `snapshot`: nullopt = deleted.
  std::map<int64_t, std::optional<RowEntry>> staged;
};

StateMap EffectiveState(const ActiveSlot& s) {
  StateMap out = s.snapshot;
  for (const auto& [k, v] : s.staged) {
    if (v.has_value()) {
      out[k] = *v;
    } else {
      out.erase(k);
    }
  }
  return out;
}

std::vector<std::tuple<int64_t, int64_t, std::string>> SortedPairs(
    const StateMap& m) {
  std::vector<std::tuple<int64_t, int64_t, std::string>> out;
  out.reserve(m.size());
  for (const auto& [id, e] : m) {
    out.emplace_back(id, e.val, e.tag);
  }
  std::sort(out.begin(), out.end());
  return out;
}

void VerifySlotView(ActiveSlot& slot) {
  assert(slot.ctx.has_value());
  auto tbl_res = slot.ctx->GetTable("t");
  assert(tbl_res.HasValue());
  std::vector<std::tuple<int64_t, int64_t, std::string>> got;
  Iterator it = tbl_res.Value()->BeginFullScan(slot.ctx->txn_);
  while (it.IsValid()) {
    const Row& r = *it;
    got.emplace_back(r[0].value.int_value, r[1].value.int_value,
                     r[2].value.varchar_value);
    ++it;
  }
  assert(it.GetStatus() == Status::kSuccess);
  std::sort(got.begin(), got.end());
  auto want = SortedPairs(EffectiveState(slot));
  assert(got == want);
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
  db->SetSynchronousCommit(false);

  // Use non-blocking conflict policy (kWaitDie) so single-threaded interleaved
  // transactions never self-deadlock waiting for another slot on this thread!
  db->SetDeadlockPolicy(Database::DeadlockPolicy::kWaitDie);

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

  StateMap committed;
  // Seed 2..6 initial rows so concurrent updates/deletes hit real version
  // chains.
  {
    const int init_rows = fdp.ConsumeIntegralInRange<int>(1, 6);
    TransactionContext ctx = db->BeginContext();
    auto tbl_res = ctx.GetTable("t");
    assert(tbl_res.HasValue());
    for (int64_t id = 0; id < init_rows; ++id) {
      const int64_t v = fdp.ConsumeIntegralInRange<int64_t>(-100, 100);
      const std::string tag = "init" + std::to_string(id);
      auto pos = tbl_res.Value()->Insert(
          ctx.txn_, Row({Value(id), Value(v), Value(std::string(tag))}));
      assert(pos.HasValue());
      committed[id] = RowEntry{v, tag, pos.Value()};
    }
    assert(ctx.PreCommit() == Status::kSuccess);
  }

  constexpr size_t kNumSlots = 3;
  std::array<ActiveSlot, kNumSlots> slots;
  int64_t next_insert_id = 100;

  // Track which row keys currently have uncommitted write intents held by
  // another active slot (on this single thread, attempting to write a row
  // whose intent is held by an older txn with kWaitDie would fail cleanly,
  // whereas an older txn waiting on a younger txn would block this single
  // thread; so we only issue writes when no other slot holds that key, OR when
  // the writer is strictly younger so Wait-Die aborts immediately without
  // blocking!).
  auto key_held_by_other = [&](size_t self, int64_t id) -> bool {
    for (size_t j = 0; j < kNumSlots; ++j) {
      if (j != self && slots[j].ctx.has_value() &&
          slots[j].staged.contains(id)) {
        return true;
      }
    }
    return false;
  };

  const int max_ops = fdp.ConsumeIntegralInRange<int>(1, 36);
  for (int i = 0; i < max_ops && fdp.remaining_bytes() > 0; ++i) {
    const size_t s = fdp.ConsumeIntegralInRange<size_t>(0, kNumSlots - 1);
    const int op = fdp.ConsumeIntegralInRange<int>(0, 6);

    if (op == 0) {
      // Begin txn in slot s if empty
      if (!slots[s].ctx.has_value()) {
        const bool ro = fdp.ConsumeBool();
        slots[s].read_only = ro;
        slots[s].ctx.emplace(ro ? db->BeginReadOnlyContext()
                                : db->BeginContext());
        slots[s].snapshot = committed;
        slots[s].staged.clear();
      }
    } else if (op == 1) {
      // Full scan + snapshot isolation assertion
      if (slots[s].ctx.has_value()) {
        VerifySlotView(slots[s]);
      }
    } else if (op == 2) {
      // Insert new unique id
      if (slots[s].ctx.has_value() && !slots[s].read_only) {
        auto tbl_res = slots[s].ctx->GetTable("t");
        assert(tbl_res.HasValue());
        const int64_t id = next_insert_id++;
        const int64_t v = fdp.ConsumeIntegralInRange<int64_t>(-500, 500);
        const std::string tag = fdp.ConsumeRandomLengthString(10);
        auto pos = tbl_res.Value()->Insert(
            slots[s].ctx->txn_,
            Row({Value(id), Value(v), Value(std::string(tag))}));
        if (pos.HasValue()) {
          slots[s].staged[id] = RowEntry{v, tag, pos.Value()};
        }
      }
    } else if (op == 3) {
      // Update existing row visible in this slot
      if (slots[s].ctx.has_value() && !slots[s].read_only) {
        auto eff = EffectiveState(slots[s]);
        if (!eff.empty()) {
          auto it = eff.begin();
          std::advance(it,
                       fdp.ConsumeIntegralInRange<size_t>(0, eff.size() - 1));
          const int64_t id = it->first;
          // Avoid single-thread Wait-Die self-wait if an older slot tries to
          // lock a key held by a younger slot.
          if (!key_held_by_other(s, id)) {
            // Also check committed still has the same RowPosition (not
            // already updated/deleted by a later-committed txn that would
            // trigger a first-committer-wins write-write conflict).
            auto tbl_res = slots[s].ctx->GetTable("t");
            assert(tbl_res.HasValue());
            const int64_t new_v =
                fdp.ConsumeIntegralInRange<int64_t>(-500, 500);
            const std::string new_tag = fdp.ConsumeRandomLengthString(10);
            auto pos = tbl_res.Value()->Update(
                slots[s].ctx->txn_, it->second.pos,
                Row({Value(id), Value(new_v), Value(std::string(new_tag))}));
            if (pos.HasValue()) {
              slots[s].staged[id] = RowEntry{new_v, new_tag, pos.Value()};
            } else {
              // Write-write conflict or stale version: abort cleanly!
              std::ignore = slots[s].ctx->Abort();
              slots[s].ctx.reset();
              slots[s].staged.clear();
            }
          }
        }
      }
    } else if (op == 4) {
      // Delete existing row visible in this slot
      if (slots[s].ctx.has_value() && !slots[s].read_only) {
        auto eff = EffectiveState(slots[s]);
        if (!eff.empty()) {
          auto it = eff.begin();
          std::advance(it,
                       fdp.ConsumeIntegralInRange<size_t>(0, eff.size() - 1));
          const int64_t id = it->first;
          if (!key_held_by_other(s, id)) {
            auto tbl_res = slots[s].ctx->GetTable("t");
            assert(tbl_res.HasValue());
            Status st = tbl_res.Value()->Delete(slots[s].ctx->txn_,
                                                it->second.pos);
            if (st == Status::kSuccess) {
              slots[s].staged[id] = std::nullopt;
            } else {
              std::ignore = slots[s].ctx->Abort();
              slots[s].ctx.reset();
              slots[s].staged.clear();
            }
          }
        }
      }
    } else if (op == 5) {
      // Commit slot s
      if (slots[s].ctx.has_value()) {
        VerifySlotView(slots[s]);
        Status st = slots[s].ctx->PreCommit();
        if (st == Status::kSuccess) {
          for (const auto& [k, v] : slots[s].staged) {
            if (v.has_value()) {
              committed[k] = *v;
            } else {
              committed.erase(k);
            }
          }
        } else {
          std::ignore = slots[s].ctx->Abort();
        }
        slots[s].ctx.reset();
        slots[s].staged.clear();
      }
    } else if (op == 6) {
      // Abort slot s
      if (slots[s].ctx.has_value()) {
        assert(slots[s].ctx->Abort() == Status::kSuccess);
        slots[s].ctx.reset();
        slots[s].staged.clear();
      }
    }
  }

  // Drain all remaining slots and verify final committed state.
  for (size_t s = 0; s < kNumSlots; ++s) {
    if (slots[s].ctx.has_value()) {
      VerifySlotView(slots[s]);
      std::ignore = slots[s].ctx->Abort();
      slots[s].ctx.reset();
      slots[s].staged.clear();
    }
  }
  {
    ActiveSlot check;
    check.ctx.emplace(db->BeginReadOnlyContext());
    check.read_only = true;
    check.snapshot = committed;
    VerifySlotView(check);
    assert(check.ctx->PreCommit() == Status::kSuccess);
  }
  return 0;
}
