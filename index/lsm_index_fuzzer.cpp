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
#include <cstdio>
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
#include "index/lsm_index.hpp"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {
namespace {

struct ScopedLsmEnv {
  std::filesystem::path dir;
  std::string prefix;
  std::unique_ptr<PageManager> pm;
  std::unique_ptr<Logger> logger;
  std::unique_ptr<RecoveryManager> rm;
  std::unique_ptr<TransactionManager> tm;

  ScopedLsmEnv() {
    dir = std::filesystem::temp_directory_path() /
          ("lsm_fuzz_" + RandomString(10));
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    prefix = (dir / "db").string();
  }

  ~ScopedLsmEnv() {
    tm.reset();
    rm.reset();
    logger.reset();
    pm.reset();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
  }

  bool Reopen() {
    tm.reset();
    rm.reset();
    logger.reset();
    pm.reset();
    auto pm_res = PageManager::Create(prefix + ".db", 64, false);
    if (!pm_res.HasValue()) {
      return false;
    }
    pm = pm_res.MoveValue();
    auto log_res = Logger::Create(prefix + ".log");
    if (!log_res.HasValue()) {
      return false;
    }
    logger = log_res.MoveValue();
    rm = std::make_unique<RecoveryManager>(prefix + ".log", pm->GetPool(),
                                           logger->SegmentSize());
    rm->SetLogger(logger.get());
    tm = std::make_unique<TransactionManager>(pm.get(), logger.get(), rm.get());
    tm->SetSynchronousCommit(false);
    return rm->RecoverFrom(0, tm.get()) == Status::kSuccess;
  }
};

std::string MakeKey(FuzzedDataProvider& fdp) {
  // Bias toward short keys with collisions, boundary bytes (0x00, 0xFF), and
  // occasional longer keys.
  const int kind = fdp.ConsumeIntegralInRange<int>(0, 4);
  if (kind == 0) {
    return "k" + std::to_string(fdp.ConsumeIntegralInRange<int>(0, 24));
  }
  if (kind == 1) {
    return fdp.ConsumeRandomLengthString(4);
  }
  if (kind == 2) {
    std::string s = fdp.ConsumeRandomLengthString(12);
    if (s.empty()) {
      s.push_back('\0');
    }
    return s;
  }
  if (kind == 3) {
    return std::string(1, static_cast<char>(fdp.ConsumeIntegral<uint8_t>()));
  }
  return fdp.ConsumeRandomLengthString(32);
}

std::string MakeVal(FuzzedDataProvider& fdp) {
  const int kind = fdp.ConsumeIntegralInRange<int>(0, 2);
  if (kind == 0) {
    return "v" + std::to_string(fdp.ConsumeIntegralInRange<int>(0, 100));
  }
  if (kind == 1) {
    return fdp.ConsumeRandomLengthString(16);
  }
  return fdp.ConsumeRandomLengthString(64);
}

std::map<std::string, std::string> EffectiveMap(
    const std::map<std::string, std::string>& committed,
    const std::map<std::string, std::optional<std::string>>& staged) {
  std::map<std::string, std::string> out = committed;
  for (const auto& [k, v] : staged) {
    if (v.has_value()) {
      out[k] = *v;
    } else {
      out.erase(k);
    }
  }
  return out;
}

void VerifyScan(LsmIndex& idx, Transaction& txn,
                const std::map<std::string, std::string>& expected,
                const std::string& begin, const std::string& end,
                bool ascending, FuzzedDataProvider& fdp) {
  std::vector<std::pair<std::string, std::string>> want;
  for (const auto& [k, v] : expected) {
    if (!begin.empty() && k < begin) {
      continue;
    }
    if (!end.empty() && k > end) {
      continue;
    }
    want.emplace_back(k, v);
  }
  if (!ascending) {
    std::reverse(want.begin(), want.end());
  }

  LsmIndexIterator it = idx.Begin(txn, begin, end, ascending);
  std::vector<std::pair<std::string, std::string>> got;
  while (it.IsValid()) {
    got.emplace_back(it.Key(), it.Value());
    if (ascending) {
      ++it;
    } else {
      --it;
    }
  }
  assert(it.GetStatus() == Status::kSuccess);
  assert(got == want);

  // Also test backward step (--it on ascending, ++it on descending) and
  // SeekBefore on descending scans.
  if (!want.empty() && fdp.ConsumeBool()) {
    LsmIndexIterator it2 = idx.Begin(txn, begin, end, ascending);
    size_t steps =
        fdp.ConsumeIntegralInRange<size_t>(0, std::min<size_t>(want.size(), 5));
    for (size_t i = 0; i < steps && it2.IsValid(); ++i) {
      assert(it2.Key() == want[i].first);
      if (ascending) {
        ++it2;
      } else {
        --it2;
      }
    }
    if (steps > 0 && steps < want.size() && it2.IsValid()) {
      if (ascending) {
        --it2;
      } else {
        ++it2;
      }
      assert(it2.IsValid());
      assert(it2.Key() == want[steps - 1].first);
      assert(it2.Value() == want[steps - 1].second);
    }
  }
  if (!ascending && !want.empty() && fdp.ConsumeBool()) {
    const std::string seek_key = MakeKey(fdp);
    LsmIndexIterator it3 = idx.Begin(txn, begin, end, /*ascending=*/false);
    const bool found = it3.SeekBefore(seek_key);
    std::optional<std::pair<std::string, std::string>> expected_before;
    for (const auto& kv : want) {
      if (kv.first < seek_key) {
        expected_before = kv;
        break;
      }
    }
    assert(found == expected_before.has_value());
    if (found) {
      assert(it3.IsValid());
      assert(it3.Key() == expected_before->first);
      assert(it3.Value() == expected_before->second);
    }
  }
}

}  // namespace
}  // namespace tinylamb

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  using namespace tinylamb;
  if (size < 4) {
    return 0;
  }
  FuzzedDataProvider fdp(data, size);
  ScopedLsmEnv env;
  if (!env.Reopen()) {
    return 0;
  }

  page_id_t root = 0;
  {
    Transaction txn = env.tm->Begin();
    auto idx_res = LsmIndex::Open(txn);
    if (!idx_res.HasValue()) {
      return 0;
    }
    root = idx_res.Value().Root();
    if (txn.PreCommit() != Status::kSuccess) {
      return 0;
    }
  }

  std::map<std::string, std::string> committed;
  std::map<std::string, std::optional<std::string>> staged;
  std::optional<Transaction> active_txn;

  auto ensure_txn = [&]() -> Transaction& {
    if (!active_txn.has_value()) {
      active_txn.emplace(env.tm->Begin());
      staged.clear();
    }
    return *active_txn;
  };

  const int max_ops = fdp.ConsumeIntegralInRange<int>(1, 40);
  for (int op_idx = 0; op_idx < max_ops && fdp.remaining_bytes() > 0;
       ++op_idx) {
    const int op = fdp.ConsumeIntegralInRange<int>(0, 9);
    LsmIndex idx(root);

    if (op == 0) {
      // Insert
      Transaction& txn = ensure_txn();
      const std::string k = MakeKey(fdp);
      const std::string v = MakeVal(fdp);
      auto eff = EffectiveMap(committed, staged);
      const bool exists = eff.contains(k);
      const Status st = idx.Insert(txn, k, v);
      if (exists) {
        assert(st == Status::kDuplicates);
      } else {
        assert(st == Status::kSuccess);
        staged[k] = v;
      }
    } else if (op == 1) {
      // Update
      Transaction& txn = ensure_txn();
      const std::string k = MakeKey(fdp);
      const std::string v = MakeVal(fdp);
      auto eff = EffectiveMap(committed, staged);
      const bool exists = eff.contains(k);
      const Status st = idx.Update(txn, k, v);
      if (!exists) {
        assert(st == Status::kNotExists);
      } else {
        assert(st == Status::kSuccess);
        staged[k] = v;
      }
    } else if (op == 2) {
      // Delete
      Transaction& txn = ensure_txn();
      const std::string k = MakeKey(fdp);
      auto eff = EffectiveMap(committed, staged);
      const bool exists = eff.contains(k);
      const Status st = idx.Delete(txn, k);
      if (!exists) {
        assert(st == Status::kNotExists);
      } else {
        assert(st == Status::kSuccess);
        staged[k] = std::nullopt;
      }
    } else if (op == 3) {
      // Point ReadVisible
      Transaction& txn = ensure_txn();
      const std::string k = MakeKey(fdp);
      auto eff = EffectiveMap(committed, staged);
      auto res = idx.ReadVisible(txn, k);
      if (auto it = eff.find(k); it != eff.end()) {
        assert(res.HasValue());
        assert(res.Value() == it->second);
      } else {
        assert(!res.HasValue());
      }
    } else if (op == 4) {
      // Range scan (ascending or descending)
      Transaction& txn = ensure_txn();
      auto eff = EffectiveMap(committed, staged);
      const std::string b = fdp.ConsumeBool() ? MakeKey(fdp) : "";
      const std::string e = fdp.ConsumeBool() ? MakeKey(fdp) : "";
      const bool asc = fdp.ConsumeBool();
      VerifyScan(idx, txn, eff, b, e, asc, fdp);
    } else if (op == 5) {
      // Commit
      if (active_txn.has_value()) {
        assert(active_txn->PreCommit() == Status::kSuccess);
        committed = EffectiveMap(committed, staged);
        staged.clear();
        active_txn.reset();
      }
    } else if (op == 6) {
      // Abort
      if (active_txn.has_value()) {
        assert(active_txn->Abort() == Status::kSuccess);
        staged.clear();
        active_txn.reset();
      }
    } else if (op == 7) {
      // Flush LSM write buffers
      env.pm->FlushLsmBuffersForTest(root);
    } else if (op == 8) {
      // Checkpoint (only when no active txn holding latches)
      if (!active_txn.has_value()) {
        std::ignore = env.logger->WaitForDurable(env.logger->BufferedLSN());
        assert(env.pm->WriteFuzzyCheckpoint(0) == Status::kSuccess);
      }
    } else if (op == 9) {
      // Simulate crash + reopen! Uncommitted staged ops must vanish.
      if (active_txn.has_value()) {
        // Drop active txn without committing after flushing WAL of prior
        // commits.
        active_txn.reset();
        staged.clear();
      }
      std::ignore = env.logger->WaitForDurable(env.logger->BufferedLSN());
      if (!env.Reopen()) {
        return 0;
      }
      Transaction verify_txn = env.tm->Begin();
      VerifyScan(idx, verify_txn, committed, "", "", /*ascending=*/true, fdp);
      VerifyScan(idx, verify_txn, committed, "", "", /*ascending=*/false, fdp);
      assert(verify_txn.PreCommit() == Status::kSuccess);
    }
  }

  // Final full verification after closing any open txn.
  if (active_txn.has_value()) {
    if (fdp.ConsumeBool()) {
      assert(active_txn->PreCommit() == Status::kSuccess);
      committed = EffectiveMap(committed, staged);
    } else {
      assert(active_txn->Abort() == Status::kSuccess);
    }
    staged.clear();
    active_txn.reset();
  }
  {
    LsmIndex idx(root);
    Transaction txn = env.tm->Begin();
    VerifyScan(idx, txn, committed, "", "", /*ascending=*/true, fdp);
    VerifyScan(idx, txn, committed, "", "", /*ascending=*/false, fdp);
    assert(txn.PreCommit() == Status::kSuccess);
  }
  return 0;
}
