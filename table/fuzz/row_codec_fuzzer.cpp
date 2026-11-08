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

// Fuzz Row::DeserializeBounded + memcomparable roundtrip + differential
// verification of TryDeserialize vs TryDeserializeProjected vs
// TryDeserializePlanned (with slot reuse) vs TryPeekInteger.
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "table/fuzz/fuzz_helpers.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::fuzz {
namespace {

[[noreturn]] void CodecFail(const char* detail) {
  (void)std::fprintf(stderr, "[ROW-CODEC-FUZZ FAIL] %s\n", detail);
  (void)std::fflush(stderr);
  std::abort();
}

bool SameCell(const Value& a, const Value& b) {
  if (a.IsNull() != b.IsNull() || a.type != b.type ||
      a.IsUnsigned() != b.IsUnsigned()) {
    return false;
  }
  if (a.IsNull()) {
    return true;
  }
  if (a.type == ValueType::kDouble) {
    const double da = a.value.double_value;
    const double db = b.value.double_value;
    if (std::isnan(da) && std::isnan(db)) {
      return true;
    }
    uint64_t ba = 0;
    uint64_t bb = 0;
    std::memcpy(&ba, &da, sizeof(ba));
    std::memcpy(&bb, &db, sizeof(bb));
    return ba == bb;
  }
  return a == b;
}

RowDecodePlan BuildDecodePlan(const Schema& sc,
                              const std::vector<slot_t>& proj) {
  RowDecodePlan plan;
  plan.stored_count = sc.ColumnCount();
  plan.kept_count = proj.size();
  plan.stored_types.resize(sc.ColumnCount());
  plan.stored_slot.assign(sc.ColumnCount(), RowDecodePlan::kNoSlot);
  plan.slot_unsigned.assign(proj.size(), 0);
  plan.all_fixed = true;
  plan.has_unsigned = false;
  for (slot_t i = 0; i < sc.ColumnCount(); ++i) {
    const Column& col = sc.GetColumn(i);
    plan.stored_types[i] = col.Type();
    if (col.Type() != ValueType::kInt64 && col.Type() != ValueType::kDate &&
        col.Type() != ValueType::kDouble) {
      plan.all_fixed = false;
    }
  }
  for (size_t k = 0; k < proj.size(); ++k) {
    const slot_t col_idx = proj[k];
    plan.stored_slot[col_idx] = k;
    if (sc.GetColumn(col_idx).IsUnsigned()) {
      plan.slot_unsigned[k] = 1;
      plan.has_unsigned = true;
    }
  }
  return plan;
}

Value SynthesizeCell(const Column& col, const uint8_t* data, size_t size,
                     size_t* cursor) {
  auto next_u8 = [&]() -> uint8_t {
    return *cursor < size ? data[(*cursor)++] : 0U;
  };
  if (next_u8() % 4 == 0) {
    return {};
  }
  switch (col.Type()) {
    case ValueType::kInt64: {
      uint64_t v = 0;
      for (int i = 0; i < 8; ++i) {
        v = (v << 8) | next_u8();
      }
      Value out(static_cast<int64_t>(v));
      if (col.IsUnsigned()) {
        out.SetUnsigned();
      }
      return out;
    }
    case ValueType::kDate: {
      uint64_t v = 0;
      for (int i = 0; i < 4; ++i) {
        v = (v << 8) | next_u8();
      }
      return Value::DateFromDays(static_cast<int64_t>(v % 200000) - 100000);
    }
    case ValueType::kDouble: {
      uint64_t v = 0;
      for (int i = 0; i < 8; ++i) {
        v = (v << 8) | next_u8();
      }
      double d = 0.0;
      std::memcpy(&d, &v, sizeof(d));
      return Value(d);
    }
    case ValueType::kVarChar: {
      const size_t len = next_u8() % 24;
      std::string s;
      s.reserve(len);
      for (size_t i = 0; i < len; ++i) {
        s.push_back(static_cast<char>(next_u8()));
      }
      return Value(std::move(s));
    }
    default:
      return {};
  }
}

}  // namespace

int FuzzRowCodecOneInput(const uint8_t* data, size_t size) {
  if (size < 2) {
    return 0;
  }
  const Schema sc = MakeSchemaFromByte(data[0]);
  const uint8_t mode = data[1];

  int rejections = 0;

  // (1) Deserialize arbitrary bytes against the derived schema.
  if ((mode & 0x1) != 0) {
    try {
      Row out;
      out.DeserializeBounded(reinterpret_cast<const char*>(data + 2), size - 2,
                             sc);
      if (out.Count() != sc.ColumnCount()) {
        CodecFail("DeserializeBounded column count mismatch");
      }
    } catch (const std::runtime_error&) {
      ++rejections;
    } catch (const std::exception&) {
      ++rejections;
    }
  }

  // (2) Memcomparable decode of an arbitrary byte string must terminate and
  // stay in-bounds.
  if ((mode & 0x2) != 0) {
    try {
      const std::string_view src(reinterpret_cast<const char*>(data + 2),
                                 size - 2);
      Row out;
      out.DecodeMemcomparableFormatBounded(src);
    } catch (const std::runtime_error&) {
      ++rejections;
    } catch (const std::exception&) {
      ++rejections;
    }
  }

  // (3) Differential check across Serialize -> TryDeserialize,
  // TryDeserializeProjected, TryDeserializePlanned (with slot reuse across
  // two consecutive rows), DeserializeBounded, and TryPeekInteger.
  if (size >= 4 && sc.ColumnCount() > 0) {
    size_t pos = 2;
    std::vector<slot_t> proj;
    for (slot_t c = 0; c < sc.ColumnCount(); ++c) {
      if (((mode >> (c % 6)) & 1U) != 0) {
        proj.push_back(c);
      }
    }
    if (proj.empty()) {
      proj.push_back(0);
    }
    const RowDecodePlan plan = BuildDecodePlan(sc, proj);
    const RowDecodePlan full_plan = [&]() {
      std::vector<slot_t> all(sc.ColumnCount());
      for (slot_t c = 0; c < sc.ColumnCount(); ++c) {
        all[c] = c;
      }
      return BuildDecodePlan(sc, all);
    }();

    Row reused_projected;
    Row reused_full;
    for (int iter = 0; iter < 2; ++iter) {
      std::vector<Value> cells;
      cells.reserve(sc.ColumnCount());
      for (slot_t c = 0; c < sc.ColumnCount(); ++c) {
        cells.push_back(SynthesizeCell(sc.GetColumn(c), data, size, &pos));
      }
      const Row original(std::move(cells));
      if (!original.CheckSerializable().ok()) {
        continue;
      }
      std::vector<char> buf(original.Size());
      const size_t written = original.Serialize(buf.data());
      if (written != buf.size()) {
        CodecFail("Row::Serialize size mismatch");
      }

      Row full_decoded;
      const StatusOr<size_t> d1 = full_decoded.TryDeserialize(buf.data(), sc);
      if (!d1.HasValue() || d1.Value() != written ||
          full_decoded.Count() != sc.ColumnCount()) {
        CodecFail("Row::TryDeserialize failed on valid image");
      }

      Row bounded_decoded;
      const size_t d_bounded =
          bounded_decoded.DeserializeBounded(buf.data(), buf.size(), sc);
      if (d_bounded != written || bounded_decoded.Count() != sc.ColumnCount()) {
        CodecFail("Row::DeserializeBounded disagreed with TryDeserialize");
      }

      const StatusOr<size_t> d_full_plan =
          reused_full.TryDeserializePlanned(buf.data(), full_plan);
      if (!d_full_plan.HasValue() || d_full_plan.Value() != written ||
          reused_full.Count() != sc.ColumnCount()) {
        CodecFail("Row::TryDeserializePlanned(full) failed");
      }

      for (slot_t c = 0; c < sc.ColumnCount(); ++c) {
        if (!SameCell(full_decoded[c], original[c]) ||
            !SameCell(bounded_decoded[c], original[c]) ||
            !SameCell(reused_full[c], original[c])) {
          CodecFail("Decoded cell disagreed with original cell");
        }
        const std::optional<int64_t> peeked =
            Row::TryPeekInteger(buf.data(), sc, c);
        const ValueType vt = sc.GetColumn(c).Type();
        if (!original[c].IsNull() &&
            (vt == ValueType::kInt64 || vt == ValueType::kDate)) {
          if (!peeked.has_value() || *peeked != original[c].value.int_value) {
            CodecFail("Row::TryPeekInteger disagreed with original cell");
          }
        } else if (peeked.has_value()) {
          CodecFail("Row::TryPeekInteger returned value for NULL/non-int cell");
        }
      }

      Row proj_decoded;
      const StatusOr<size_t> d_proj =
          proj_decoded.TryDeserializeProjected(buf.data(), sc, proj);
      const StatusOr<size_t> d_plan =
          reused_projected.TryDeserializePlanned(buf.data(), plan);
      if (!d_proj.HasValue() || !d_plan.HasValue() ||
          d_proj.Value() != written || d_plan.Value() != written ||
          proj_decoded.Count() != proj.size() ||
          reused_projected.Count() != proj.size()) {
        CodecFail("Projected decode failed on valid image");
      }
      for (size_t k = 0; k < proj.size(); ++k) {
        if (!SameCell(proj_decoded[k], original[proj[k]]) ||
            !SameCell(reused_projected[k], original[proj[k]])) {
          CodecFail("Projected cell disagreed with original cell");
        }
      }
    }
  }

  static_cast<void>(rejections);
  return 0;
}

}  // namespace tinylamb::fuzz

#ifndef TINYLAMB_FUZZ_NO_MAIN_SYMBOL
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  return tinylamb::fuzz::FuzzRowCodecOneInput(data, size);
}
#endif
