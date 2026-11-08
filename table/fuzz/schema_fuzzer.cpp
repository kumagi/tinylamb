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

// Schema-level fuzzer: derives adversarial schemas and datasets from the
// input bytes and drives the full storage pipeline (sorted run -> PAX chunk
// -> validation -> row round-trip -> point lookups -> multi-run merge ->
// memtable flush).
#include <algorithm>
#include <array>
#include <cfloat>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iostream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/platform/endian.hpp"
#include "table/fuzz/fuzz_chunk_common.hpp"
#include "table/fuzz/fuzz_helpers.hpp"
#include "table/memtable/memtable.hpp"
#include "table/pax_chunk/pax_chunk.hpp"
#include "table/sorted_run/sorted_run.hpp"
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
  explicit Lcg(uint64_t seed) : s(seed) {}
  uint64_t Next() {
    s = (s * 6364136223846793005ULL) + 1442695040888963407ULL;
    return s >> 17;
  }
  uint32_t Below(uint32_t n) { return static_cast<uint32_t>(Next() % n); }
};

constexpr int kMaxCols = 6;
constexpr int kMaxRows = 1200;
constexpr size_t kMaxStringLen = 2048;

struct ColSpec {
  ValueType type = ValueType::kNull;
  std::string name;
  int policy = 0;
};

std::string TrickyName(Lcg& rng, int idx) {
  switch (rng.Below(8)) {
    case 0:
      return "";
    case 1:
      return "c" + std::to_string(idx);
    case 2: {
      std::string s(300, 'L');
      return s;
    }
    case 3:
      return "\xE5\x88\x97" + std::to_string(idx);
    case 4:
      return "a.b";
    case 5:
      return "x";
    case 6:
      return {"nul\0inside", 10};
    default:
      return "col_" + std::to_string(idx);
  }
}

int64_t GenInt(Lcg& rng, int policy, int64_t i) {
  switch (policy) {
    case 0:
      return static_cast<int64_t>(rng.Next());
    case 1:
      return static_cast<int64_t>(i % 7);
    case 2:
      return static_cast<int64_t>(rng.Next()) % 251;
    case 3: {
      constexpr std::array<int64_t, 7> ext = {
          INT64_MAX, INT64_MIN, 0, -1, 1, INT64_MAX - 1, INT64_MIN + 1};
      return ext[rng.Below(7)];
    }
    case 4:
      return i * 1000000007LL;
    case 5:
      return 42;
    default: {
      const auto hi = static_cast<int64_t>(INT64_MAX - (i % 4));
      const auto lo = static_cast<int64_t>(INT64_MIN + (i % 4));
      return ((i & 1) != 0) ? hi : lo;
    }
  }
}

double GenDouble(Lcg& rng, int policy, int64_t i) {
  switch (policy) {
    case 0: {
      const uint64_t bits = rng.Next();
      double d = 0.0;
      std::memcpy(&d, &bits, 8);
      return d;
    }
    case 1:
      return static_cast<double>(i % 13);
    case 2: {
      constexpr std::array<double, 4> ext = {0.0, -0.0, 1.5, -1.5};
      return ext[rng.Below(4)];
    }
    case 3: {
      constexpr std::array<double, 7> ext = {static_cast<double>(NAN),
                                             static_cast<double>(INFINITY),
                                             -static_cast<double>(INFINITY),
                                             DBL_MIN,
                                             DBL_MAX,
                                             5e-324,
                                             123.456};
      return ext[rng.Below(7)];
    }
    default:
      return 42.0;
  }
}

std::string GenString(Lcg& rng, int policy, int64_t i, size_t max_len) {
  switch (policy) {
    case 0:
      return "";
    case 1:
      return "v" + std::to_string(i % 11);
    case 2: {
      const size_t span = max_len > 64 ? max_len - 64 : 0;
      const size_t n =
          64 + (span != 0 ? rng.Below(static_cast<uint32_t>(span)) : 0);
      std::string s(std::min(n, max_len), static_cast<char>('a' + (i % 26)));
      return s;
    }
    case 3: {
      std::string s = std::string("ab\0cd", 5) + "\x80\xFF" + "ef";
      s.resize(s.size() + rng.Below(16), '\x01');
      return s;
    }
    case 4: {
      size_t n = static_cast<size_t>(8) * (1U + rng.Below(40));
      n = std::min(n, max_len & ~static_cast<size_t>(7));
      std::string s(n == 0 ? std::min<size_t>(8, max_len) : n, 'm');
      return s;
    }
    default:
      return "s" + std::to_string(i);
  }
}

Value MakeValue(Lcg& rng, const ColSpec& c, int64_t i, size_t max_len) {
  if (c.type == ValueType::kInt64) {
    return Value(GenInt(rng, c.policy, i));
  }
  if (c.type == ValueType::kDouble) {
    return Value(GenDouble(rng, c.policy, i));
  }
  return Value(GenString(rng, c.policy, i, max_len));
}

bool SameField(const Value& a, const Value& b, ValueType t) {
  if (a.type != b.type) {
    return false;
  }
  switch (t) {
    case ValueType::kInt64:
      return a.AsInt64() == b.AsInt64();
    case ValueType::kDouble: {
      const double da = a.AsDouble();
      const double db = b.AsDouble();
      uint64_t ba = 0;
      uint64_t bb = 0;
      std::memcpy(&ba, &da, sizeof(ba));
      std::memcpy(&bb, &db, sizeof(bb));
      return ba == bb;
    }
    default:
      return a.AsStringView() == b.AsStringView();
  }
}

bool SameRow(const Row& a, const Row& b, const std::vector<ColSpec>& specs) {
  if (a.Count() != b.Count()) {
    return false;
  }
  for (size_t i = 0; i < specs.size(); ++i) {
    if (!SameField(a[i], b[i], specs[i].type)) {
      return false;
    }
  }
  return true;
}

SortedRun BuildRun(const std::vector<std::pair<std::string, Row>>& pairs,
                   const Schema& /*sc*/, size_t gen) {
  std::vector<char> blob;
  std::vector<SortedRun::Entry> index;
  for (const auto& [key, row] : pairs) {
    const auto key_off = static_cast<uint32_t>(blob.size());
    const uint32_t be_len = htobe32(static_cast<uint32_t>(key.size()));
    const char* p = reinterpret_cast<const char*>(&be_len);
    blob.insert(blob.end(), p, p + 4);
    blob.insert(blob.end(), key.begin(), key.end());
    const auto row_off = static_cast<uint32_t>(blob.size());
    std::vector<char> buf(row.Size());
    row.Serialize(buf.data());
    blob.insert(blob.end(), buf.begin(), buf.end());
    index.emplace_back(key_off, row_off, false);
  }
  return {std::move(blob), std::move(index), gen};
}

#define FAIL(msg)                                       \
  do {                                                  \
    std::cerr << "SCHEMA-FUZZ FAIL: " << (msg) << "\n"; \
    std::abort();                                       \
  } while (0)

}  // namespace

int FuzzSchemaOneInput(const uint8_t* data, size_t size) {
  if (size < 64) {
    return 0;
  }

  try {
    Lcg rng([&] {
      uint64_t s = 0;
      std::memcpy(&s, data, 8);
      return s | 1;
    }());

    const int ncols = 1 + static_cast<int>(rng.Below(kMaxCols));
    std::vector<ColSpec> specs;
    specs.reserve(static_cast<size_t>(ncols));
    for (int i = 0; i < ncols; ++i) {
      ColSpec c;
      const uint32_t t = rng.Below(100);
      if (t < 55) {
        c.type = ValueType::kInt64;
      } else if (t < 80) {
        c.type = ValueType::kDouble;
      } else {
        c.type = ValueType::kVarChar;
      }
      c.name = TrickyName(rng, i);
      c.policy = static_cast<int>(rng.Below(7));
      specs.push_back(c);
    }
    const uint32_t key_idx = rng.Below(static_cast<uint32_t>(ncols));

    std::vector<Column> cols;
    cols.reserve(specs.size());
    for (const auto& c : specs) {
      if (&c == &specs[key_idx]) {
        cols.emplace_back(ColumnName{"", c.name}, c.type,
                          Constraint{Constraint::kPrimaryKey});
      } else {
        cols.emplace_back(ColumnName{"", c.name}, c.type);
      }
    }
    const Schema sc("schema_fuzz", std::move(cols),
                    static_cast<uint16_t>(key_idx), 1);

    const int nrows = 8 + static_cast<int>(rng.Below(kMaxRows - 8));
    constexpr size_t kMaxValueBytes = size_t{1} << 20;
    const size_t value_slots =
        static_cast<size_t>(nrows) * static_cast<size_t>(ncols);
    const size_t max_len =
        std::min(kMaxStringLen,
                 std::max<size_t>(
                     64, kMaxValueBytes / std::max<size_t>(value_slots, 1)));
    std::vector<std::pair<std::string, Row>> pairs;
    std::vector<Row> oracle;
    oracle.reserve(static_cast<size_t>(nrows));
    int64_t min_key = INT64_MAX;
    int64_t max_key = INT64_MIN;
    const bool key_is_int64 = specs[key_idx].type == ValueType::kInt64;
    for (int i = 0; i < nrows; ++i) {
      std::vector<Value> vals;
      vals.reserve(specs.size());
      for (const auto& c : specs) {
        vals.push_back(MakeValue(rng, c, i, max_len));
      }
      Row r(vals);
      std::string key = r.EncodeKeyMemcomparableFormat(sc);
      if (key_is_int64) {
        const int64_t k = r[static_cast<size_t>(key_idx)].AsInt64();
        min_key = std::min(min_key, k);
        max_key = std::max(max_key, k);
      }
      oracle.push_back(r);
      pairs.emplace_back(std::move(key), std::move(r));
    }
    std::stable_sort(
        pairs.begin(), pairs.end(),
        [](const auto& a, const auto& b) { return a.first < b.first; });
    for (size_t i = 0; i < pairs.size(); ++i) {
      oracle[i] = pairs[i].second;
    }

    const SortedRun run = BuildRun(pairs, sc, 1);
    const PaxChunk chunk = PaxChunk::BuildFromSortedRun(run, sc);
    if (PaxChunk::ValidateChunkBytes(chunk.Data()) != Status::kSuccess) {
      FAIL("writer emitted structurally invalid bytes");
    }
    if (chunk.TotalRowCount() != static_cast<size_t>(nrows)) {
      FAIL("total row count mismatch after build");
    }

    for (int i = 0; i < nrows; ++i) {
      auto rr = chunk.ReadRow(static_cast<size_t>(i), sc);
      if (rr.GetStatus() != Status::kSuccess) {
        FAIL("ReadRow failed on valid index");
      }
      if (!SameRow(rr.Value(), oracle[static_cast<size_t>(i)], specs)) {
        FAIL("lossy round-trip");
      }
    }

    if (key_is_int64) {
      for (int t = 0; t < 40; ++t) {
        const int64_t q =
            oracle[static_cast<size_t>(rng.Below(static_cast<uint32_t>(nrows)))]
                  [static_cast<size_t>(key_idx)]
                      .AsInt64();
        size_t idx = 0;
        if (chunk.PointLookup(q, sc, &idx) != Status::kSuccess) {
          FAIL("PointLookup missed a present key");
        }
        auto got = chunk.ReadRowByKey(q, sc);
        if (got.GetStatus() != Status::kSuccess ||
            got.Value()[static_cast<size_t>(key_idx)].AsInt64() != q) {
          FAIL("ReadRowByKey returned wrong row");
        }
      }
      size_t dummy = 0;
      if (min_key != INT64_MIN) {
        const int64_t below = min_key - 1;
        if (chunk.PointLookup(below, sc, &dummy) == Status::kSuccess &&
            chunk.ReadRowByKey(below, sc).GetStatus() == Status::kSuccess) {
          FAIL("phantom key below range");
        }
      }
      if (max_key != INT64_MAX) {
        const int64_t above = max_key + 1;
        if (chunk.ReadRowByKey(above, sc).GetStatus() == Status::kSuccess) {
          FAIL("phantom key above range");
        }
      }
    }

    {
      std::vector<std::pair<std::string, Row>> a;
      std::vector<std::pair<std::string, Row>> b;
      for (size_t i = 0; i < pairs.size(); ++i) {
        ((i & 1) == 0 ? a : b).push_back(pairs[i]);
      }
      std::stable_sort(a.begin(), a.end(), [](const auto& x, const auto& y) {
        return x.first < y.first;
      });
      std::stable_sort(b.begin(), b.end(), [](const auto& x, const auto& y) {
        return x.first < y.first;
      });
      const SortedRun ra = BuildRun(a, sc, 2);
      const SortedRun rb = BuildRun(b, sc, 3);
      std::vector<const SortedRun*> inputs = {&ra, &rb};
      auto merged = MergeAllSortedRuns(inputs, sc);
      if (merged.GetStatus() != Status::kSuccess) {
        FAIL("MergeAllSortedRuns failed on legit input");
      }
      if (PaxChunk::ValidateChunkBytes(merged.Value().Data()) !=
          Status::kSuccess) {
        FAIL("merge output invalid");
      }
      size_t distinct = 0;
      {
        std::vector<std::string> ks;
        ks.reserve(pairs.size());
        for (auto& [k, r] : pairs) {
          ks.push_back(k);
        }
        std::sort(ks.begin(), ks.end());
        distinct =
            static_cast<size_t>(std::unique(ks.begin(), ks.end()) - ks.begin());
      }
      if (merged.Value().TotalRowCount() != distinct) {
        FAIL("merge row count != distinct keys");
      }
    }

    {
      MemTable mt(sc);
      std::unordered_set<std::string> unique;
      for (auto& [k, r] : pairs) {
        mt.Put(r);
        unique.insert(k);
      }
      std::vector<char> blob;
      std::vector<uint32_t> kr;
      std::vector<uint32_t> rr;
      std::vector<uint8_t> del;
      size_t gen = 0;
      if (mt.Sync(&blob, &kr, &rr, &gen, &del) != Status::kSuccess) {
        FAIL("memtable sync failed");
      }
      SortedRun flushed = BuildRun({}, sc, 0);
      {
        std::vector<char> b2 = blob;
        std::vector<SortedRun::Entry> idx;
        idx.reserve(kr.size());
        for (size_t i = 0; i < kr.size(); ++i) {
          idx.emplace_back(kr[i], rr[i], del[i] != 0);
        }
        flushed = SortedRun(std::move(b2), std::move(idx), gen);
      }
      std::vector<const SortedRun*> one = {&flushed};
      auto merged = MergeAllSortedRuns(one, sc);
      if (merged.GetStatus() != Status::kSuccess ||
          PaxChunk::ValidateChunkBytes(merged.Value().Data()) !=
              Status::kSuccess) {
        FAIL("flushed-run merge invalid");
      }
      if (merged.Value().TotalRowCount() != unique.size()) {
        FAIL("flushed row count != unique keys");
      }
    }
  } catch (const std::exception& e) {
    std::cerr << "SCHEMA-FUZZ exception: " << e.what() << "\n";
    std::abort();
  }
  return 0;
}

#undef FAIL

}  // namespace tinylamb::fuzz

#ifndef TINYLAMB_FUZZ_NO_MAIN_SYMBOL
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  return tinylamb::fuzz::FuzzSchemaOneInput(data, size);
}
#endif
