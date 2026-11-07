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

// Corrupted-storage fuzz: build one VALID adaptive chunk, flip bytes
// according to the fuzz input, and hammer every read/seek path.
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/env_endian.hpp"
#include "common/status_or.hpp"
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
  cols.emplace_back(ColumnName{"", "qty"}, ValueType::kInt64);
  cols.emplace_back(ColumnName{"", "name"}, ValueType::kVarChar,
                    Constraint{Constraint::kNotNull});
  cols.emplace_back(ColumnName{"", "score"}, ValueType::kDouble);
  return Schema{"t", std::move(cols), 0, 1};
}

PaxChunk BuildValidChunk() {
  const Schema sc = MakeSchema();
  std::vector<char> blob;
  std::vector<SortedRun::Entry> index;
  constexpr int kRows = 300;
  for (int i = 0; i < kRows; ++i) {
    Row r{Value(i), Value(1 + ((i * 7919) % 97)),
          Value("n" + std::to_string(i % 40)), Value(50.0 + (i % 128) * 0.25)};
    const std::string key = r.EncodeKeyMemcomparableFormat(sc);
    const auto key_off = static_cast<uint32_t>(blob.size());
    const uint32_t be_len = htobe32(static_cast<uint32_t>(key.size()));
    const char* p = reinterpret_cast<const char*>(&be_len);
    blob.insert(blob.end(), p, p + sizeof(uint32_t));
    blob.insert(blob.end(), key.begin(), key.end());
    const auto row_off = static_cast<uint32_t>(blob.size());
    std::vector<char> buf(r.Size());
    r.Serialize(buf.data());
    blob.insert(blob.end(), buf.begin(), buf.end());
    index.emplace_back(key_off, row_off, false);
  }
  SortedRun run(std::move(blob), std::move(index), 1);
  return PaxChunk::BuildFromSortedRun(run, sc);
}

}  // namespace

int FuzzPaxMutateOneInput(const uint8_t* data, size_t size) {
  if (size < 16) return 0;
  const Schema sc = MakeSchema();
  static const PaxChunk kGood = BuildValidChunk();

  auto bytes = kGood.Data();
  const size_t mask_len = std::min(size - 8, bytes.size());
  for (size_t i = 0; i < mask_len; ++i) {
    bytes[i] ^= static_cast<char>(data[i]);
  }
  PaxChunk bad(bytes, kGood.GetFooter());

  const Status v1 = PaxChunk::ValidateChunkBytes(bytes);
  const Status v2 = PaxChunk::ValidateChunkBytes(bytes);
  if (v1 != v2) {
    std::fprintf(stderr, "nondeterministic ValidateChunkBytes verdict\n");
    std::abort();
  }
  if (v1 != Status::kSuccess) return 0;

  int64_t probe = 0;
  std::memcpy(&probe, data + size - 8, 8);
  const slot_t col = data[size - 9] % 4;

  for (size_t i = 0; i < bad.TotalRowCount(); i += 7) {
    auto r = bad.ReadRow(i, sc);
    if (r.HasValue()) {
      if (r.Value().Count() != sc.ColumnCount()) std::abort();
    }
  }
  const size_t row_i = static_cast<size_t>(probe % 64);
  auto cv = bad.ReadColumnValue(row_i, col, sc);
  (void)cv;
  size_t idx = 12345;
  (void)bad.PointLookup(probe, sc, &idx);
  (void)bad.LowerBoundIndex(probe ^ 0x5555, sc, &idx);
  return 0;
}

}  // namespace tinylamb::fuzz

#ifndef TINYLAMB_FUZZ_NO_MAIN_SYMBOL
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  return tinylamb::fuzz::FuzzPaxMutateOneInput(data, size);
}
#endif
