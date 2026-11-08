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

// Model-based memtable fuzzer: random Put/Delete/Lookup/Snapshot/Sync ops
// checked against a std::map reference.
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "table/fuzz/fuzz_helpers.hpp"
#include "table/memtable/memtable.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::fuzz {
namespace {

struct Lcg {
  uint64_t s;
  explicit Lcg(uint64_t seed) : s(seed | 1) {}
  uint64_t Next() {
    s = (s * 6364136223846793005ULL) + 1442695040888963407ULL;
    return s >> 17;
  }
  uint32_t Below(uint32_t n) { return static_cast<uint32_t>(Next() % n); }
};

constexpr int kKeyspace = 300;

Schema MakeSchema() {
  std::vector<Column> cols;
  cols.emplace_back(ColumnName{"", "id"}, ValueType::kInt64,
                    Constraint{Constraint::kPrimaryKey});
  cols.emplace_back(ColumnName{"", "v"}, ValueType::kInt64);
  return Schema{"mtmodel", std::move(cols), 0, 1};
}

#define FAIL(msg)                                    \
  do {                                               \
    std::cerr << "MT-MODEL FAIL: " << (msg) << "\n"; \
    std::abort();                                    \
  } while (0)

}  // namespace

int FuzzMemtableModelOneInput(const uint8_t* data, size_t size) {
  if (size < 32) {
    return 0;
  }
  Lcg rng([&] {
    uint64_t s = 0;
    std::memcpy(&s, data, 8);
    return s;
  }());

  const Schema sc = MakeSchema();
  MemTable mt(sc);

  std::map<int64_t, int64_t> model;

  auto key_of = [&](int64_t id) {
    return Row{Value(id), Value(0)}.EncodeKeyMemcomparableFormat(sc);
  };
  auto row_of = [&](int64_t id, int64_t seq) {
    return Row{Value(id), Value(seq)};
  };

  int sync_count = 0;
  const int ops = 100 + static_cast<int>(rng.Below(900));
  for (int op = 0; op < ops; ++op) {
    const uint32_t sel = rng.Below(100);
    const auto id = static_cast<int64_t>(rng.Below(kKeyspace));

    const bool dbg = std::getenv("MTDBG") != nullptr;
    if (dbg) {
      std::cerr << "op=" << op << " sel=" << sel << " id=" << id << "\n";
    }
    if (sel < 45) {
      const auto seq = static_cast<int64_t>(rng.Below(100000));
      if (dbg) {
        std::cerr << "  PUT id=" << id << " seq=" << seq << "\n";
      }
      if (mt.Put(row_of(id, seq)) != Status::kSuccess) {
        FAIL("put failed");
      }
      model[id] = seq;
    } else if (sel < 65) {
      if (mt.Delete(key_of(id)) != Status::kSuccess) {
        FAIL("delete failed");
      }
      model[id] = -1;
    } else if (sel < 90) {
      Row out;
      const Status st = mt.Lookup(key_of(id), &out);
      auto it = model.find(id);
      if (it == model.end()) {
        if (st != Status::kNotExists) {
          FAIL("absent key not kNotExists");
        }
      } else if (it->second == -1) {
        if (st != Status::kDeleted) {
          FAIL("tombstone not kDeleted");
        }
      } else {
        if (st != Status::kSuccess) {
          FAIL("live key not kSuccess");
        }
        if (out[1].AsInt64() != it->second) {
          FAIL("stale value returned");
        }
      }
    } else if (sel < 97) {
      auto snap = mt.Snapshot();
      std::vector<std::pair<std::string, int64_t>> want;
      for (const auto& [mid, mseq] : model) {
        if (mseq >= 0) {
          want.emplace_back(key_of(mid), mseq);
        }
      }
      size_t wi = 0;
      for (const auto& [k, e] : snap) {
        if (e.is_deleted) {
          continue;
        }
        if (wi >= want.size() || k != want[wi].first ||
            e.row[1].AsInt64() != want[wi].second) {
          FAIL("snapshot key order/content mismatch");
        }
        ++wi;
      }
      if (wi != want.size()) {
        FAIL("snapshot missing live rows");
      }
    } else {
      std::vector<char> blob;
      std::vector<uint32_t> kr, rr;
      size_t gen = 0;
      if (mt.Sync(&blob, &kr, &rr, &gen) != Status::kSuccess) {
        FAIL("sync failed");
      }
      blob.clear();
      kr.clear();
      rr.clear();
      if (mt.Size() != 0) {
        FAIL("Size() != 0 after flush");
      }
      if (!mt.Snapshot().empty()) {
        FAIL("Snapshot not empty after flush");
      }
      model.clear();
      ++sync_count;
    }
  }

  std::vector<char> blob;
  std::vector<uint32_t> kr, rr;
  size_t gen = 0;
  if (mt.Sync(&blob, &kr, &rr, &gen) != Status::kSuccess) {
    FAIL("final sync");
  }
  if (!blob.empty() && kr.empty()) {
    FAIL("blob without index");
  }
  (void)sync_count;
  return 0;
}

#undef FAIL

}  // namespace tinylamb::fuzz

#ifndef TINYLAMB_FUZZ_NO_MAIN_SYMBOL
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  return tinylamb::fuzz::FuzzMemtableModelOneInput(data, size);
}
#endif
