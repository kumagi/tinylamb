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

// Adversarial LSM merge: runs built from untrusted key sequences. Merge must
// terminate with ascending output and row count bounded by total input rows.
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/platform/endian.hpp"
#include "table/fuzz/fuzz_chunk_common.hpp"
#include "table/fuzz/fuzz_helpers.hpp"
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

Schema MakeSchema() {
  std::vector<Column> cols;
  cols.emplace_back(ColumnName{"", "id"}, ValueType::kInt64,
                    Constraint{Constraint::kPrimaryKey});
  cols.emplace_back(ColumnName{"", "v"}, ValueType::kVarChar);
  return Schema{"m", std::move(cols), 0, 1};
}

}  // namespace

int FuzzLsmMergeOneInput(const uint8_t* data, size_t size) {
  if (size < 8) {
    return 0;
  }
  const Schema sc = MakeSchema();

  const int n_runs = 1 + (data[0] & 0x3);
  size_t off = 1;
  std::vector<SortedRun> runs;
  const uint64_t base_gen = data[1];
  // Ground-truth map keyed by `id`: nullopt means the latest generation wrote
  // a tombstone; a string means the latest generation wrote that live payload.
  std::map<int64_t, std::optional<std::string>> expected;
  std::vector<int64_t> prev_ids;
  for (int r = 0; r < n_runs && off + 2 <= size; ++r) {
    const int n_keys = static_cast<int>(data[off++] % 17);
    std::vector<int64_t> ids;
    for (int k = 0; k < n_keys && off + sizeof(int64_t) <= size; ++k) {
      int64_t id = 0;
      std::memcpy(&id, data + off, sizeof(id));
      off += sizeof(int64_t);
      ids.push_back(id % 100000);
    }
    // Carry over up to 2 keys from the previous run so cross-generation
    // overwrites and tombstone shadowing are consistently exercised.
    for (size_t c = 0; c < prev_ids.size() && c < 2; ++c) {
      ids.push_back(prev_ids[c]);
    }
    if (ids.empty()) {
      continue;
    }
    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
    prev_ids = ids;
    std::vector<char> blob;
    std::vector<SortedRun::Entry> index;
    for (int64_t id : ids) {
      const std::string payload =
          "x" + std::to_string(id) + "_g" + std::to_string(r);
      Row live{Value(id), Value(std::string(payload))};
      const bool tombstone =
          ((id % 7 == 3) && (r % 2 == 1)) || ((id + r) % 11 == 0 && r > 0);
      expected[id] =
          tombstone ? std::nullopt : std::optional<std::string>(payload);
      const std::string key = live.EncodeKeyMemcomparableFormat(sc);
      const auto key_off = static_cast<uint32_t>(blob.size());
      const uint32_t be_len = htobe32(static_cast<uint32_t>(key.size()));
      const char* p = reinterpret_cast<const char*>(&be_len);
      blob.insert(blob.end(), p, p + sizeof(uint32_t));
      blob.insert(blob.end(), key.begin(), key.end());
      uint32_t row_off = key_off;
      if (!tombstone) {
        row_off = static_cast<uint32_t>(blob.size());
        std::vector<char> buf(live.Size());
        live.Serialize(buf.data());
        blob.insert(blob.end(), buf.begin(), buf.end());
      }
      index.emplace_back(key_off, row_off, tombstone);
    }
    runs.emplace_back(std::move(blob), std::move(index),
                      base_gen + static_cast<uint64_t>(r));
  }
  if (runs.empty()) {
    return 0;
  }

  std::vector<const SortedRun*> ptrs;
  ptrs.reserve(runs.size());
  for (const auto& r : runs) {
    ptrs.push_back(&r);
  }
  // Occasionally reverse input run pointer order to verify generation tags
  // rather than vector position determine the winning version.
  if ((data[0] & 0x10) != 0) {
    std::reverse(ptrs.begin(), ptrs.end());
  }
  auto chunk_or = MergeAllSortedRuns(ptrs, sc);
  if (!chunk_or.HasValue()) {
    return 0;
  }

  const PaxChunk chunk = chunk_or.MoveValue();
  size_t total_in = 0;
  for (const auto& r : runs) {
    total_in += r.Size();
  }
  if (chunk.TotalRowCount() > total_in) {
    std::abort();
  }

  std::vector<std::pair<int64_t, std::string>> expected_live;
  expected_live.reserve(expected.size());
  for (const auto& [k, v] : expected) {
    if (v.has_value()) {
      expected_live.emplace_back(k, *v);
    }
  }
  if (chunk.TotalRowCount() != expected_live.size()) {
    std::abort();
  }

  int64_t prev = 0;
  bool first = true;
  for (size_t i = 0; i < chunk.TotalRowCount(); ++i) {
    auto row_or = chunk.ReadRow(i, sc);
    if (!row_or.HasValue()) {
      std::abort();
    }
    const int64_t k = row_or.Value()[0].AsInt64();
    const std::string_view v = row_or.Value()[1].AsStringView();
    if (!first) {
      if (!(prev < k)) {
        std::abort();
      }
    }
    if (k != expected_live[i].first || v != expected_live[i].second) {
      std::abort();
    }
    prev = k;
    first = false;
  }

  // Verify PointLookup agrees with both live winners and tombstoned keys.
  for (const auto& [k, v] : expected) {
    size_t row_idx = 0;
    const Status st = chunk.PointLookup(k, sc, &row_idx);
    if (v.has_value()) {
      if (st != Status::kSuccess || row_idx >= chunk.TotalRowCount()) {
        std::abort();
      }
      auto row_or = chunk.ReadRow(row_idx, sc);
      if (!row_or.HasValue() || row_or.Value()[0].AsInt64() != k ||
          row_or.Value()[1].AsStringView() != *v) {
        std::abort();
      }
    } else {
      if (st != Status::kNotExists) {
        std::abort();
      }
    }
  }
  return 0;
}

}  // namespace tinylamb::fuzz

#ifndef TINYLAMB_FUZZ_NO_MAIN_SYMBOL
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  return tinylamb::fuzz::FuzzLsmMergeOneInput(data, size);
}
#endif
