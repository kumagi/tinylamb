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

// Comprehensive memcomparable format & composite index key / SortKeyOrder
// fuzzer:
//   Oracle 1: Multi-column composite Row memcomparable round-trip, canonical
//             re-encoding idempotence, EncodeMemcomparableFormatTo parity, and
//             DecodeMemcomparableFormatBounded chunk-boundary walking.
//   Oracle 2: Composite key total-order equivalence (memcmp of encoded keys vs
//             column-by-column SQL/index ordering), transitivity, and
//             std::hash<Row> consistency with Row::operator==.
//   Oracle 3: Index prefix range-scan upper-bound invariant
//             [EncodeParts(prefix), EncodeParts(prefix) + '\xff').
//   Oracle 4: Directional sort-key byte encoding (ASC/DESC, NULLS FIRST/LAST,
//             case-insensitive collation, NaN < -inf) vs SortKeyOrder::Compare
//             and EqualUnderCollation.
//   Oracle 5: Adversarial / truncated / bit-flipped memcomparable byte stream
//             rejection and canonical bijection check.
#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/platform/endian.hpp"
#include "common/status_or.hpp"
#include "table/fuzz/fuzz_helpers.hpp"
#include "type/collation.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/date.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::fuzz {
namespace {

struct ByteCursor {
  const uint8_t* data{};
  size_t size{};
  size_t pos{0};

  uint8_t NextU8() { return pos < size ? data[pos++] : 0U; }

  uint64_t NextU64() {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
      v = (v << 8) | NextU8();
    }
    return v;
  }

  int64_t NextI64() { return static_cast<int64_t>(NextU64()); }
};

enum class ColKind : uint8_t {
  kSignedInt64 = 0,
  kUnsignedInt64 = 1,
  kDouble = 2,
  kVarChar = 3,
  kDate = 4,
  kIntArray = 5,
};

[[noreturn]] void Fail(const char* oracle, const char* detail) {
  (void)std::fprintf(stderr, "[MEMCMP-FUZZ FAIL][%s] %s\n", oracle, detail);
  (void)std::fflush(stderr);
  std::abort();
}

Value MakeInt64Value(ByteCursor& c, bool is_unsigned) {
  const uint8_t mode = c.NextU8() % 10;
  int64_t raw = 0;
  switch (mode) {
    case 0:
      raw = 0;
      break;
    case 1:
      raw = 1;
      break;
    case 2:
      raw = -1;
      break;
    case 3:
      raw = std::numeric_limits<int64_t>::min();
      break;
    case 4:
      raw = std::numeric_limits<int64_t>::max();
      break;
    case 5:
      raw = std::numeric_limits<int64_t>::min() + 1;
      break;
    case 6:
      raw = std::numeric_limits<int64_t>::max() - 1;
      break;
    case 7:
      raw = static_cast<int64_t>(c.NextU8()) - 128;
      break;
    default:
      raw = c.NextI64();
      break;
  }
  Value v(raw);
  if (is_unsigned) {
    v.SetUnsigned();
  }
  return v;
}

Value MakeDoubleValue(ByteCursor& c) {
  const uint8_t mode = c.NextU8() % 12;
  switch (mode) {
    case 0:
      return Value(0.0);
    case 1:
      return Value(-0.0);
    case 2:
      return Value(1.0);
    case 3:
      return Value(-1.0);
    case 4:
      return Value(std::numeric_limits<double>::infinity());
    case 5:
      return Value(-std::numeric_limits<double>::infinity());
    case 6:
      return Value(std::numeric_limits<double>::lowest());
    case 7:
      return Value(std::numeric_limits<double>::max());
    case 8:
      return Value(std::numeric_limits<double>::denorm_min());
    case 9:
      return Value(-std::numeric_limits<double>::denorm_min());
    case 10:
      return Value(std::numeric_limits<double>::quiet_NaN());
    default: {
      const uint64_t raw = c.NextU64();
      double d = 0.0;
      std::memcpy(&d, &raw, sizeof(d));
      return Value(d);
    }
  }
}

// Adversarial strings hitting the 8-byte group & continuation-flag boundaries
// (0, 1, 7, 8, 9, 15, 16, 17, 24) and embedding NUL, 0x09, 0xFF, and ASCII
// case pairs, while avoiding spaces so VARCHAR never enters interval parsing.
Value MakeVarCharValue(ByteCursor& c) {
  static constexpr std::array<size_t, 12> kLengths = {0,  1,  2,  7,  8,  9,
                                                      15, 16, 17, 23, 24, 25};
  const size_t len = kLengths[c.NextU8() % kLengths.size()];
  std::string s;
  s.reserve(len);
  for (size_t i = 0; i < len; ++i) {
    const uint8_t b = c.NextU8();
    switch (b % 8) {
      case 0:
        s.push_back('\x00');
        break;
      case 1:
        s.push_back('\x01');
        break;
      case 2:
        s.push_back('\x08');
        break;
      case 3:
        s.push_back('\x09');
        break;
      case 4:
        s.push_back('\xff');
        break;
      case 5:
        s.push_back(static_cast<char>('a' + (b % 6)));
        break;
      case 6:
        s.push_back(static_cast<char>('A' + (b % 6)));
        break;
      default: {
        char ch = static_cast<char>(b);
        if (ch == ' ') {
          ch = '_';
        }
        s.push_back(ch);
        break;
      }
    }
  }
  return Value(std::move(s));
}

Value MakeDateValue(ByteCursor& c) {
  const int64_t days = (c.NextI64() % 400000) - 200000;
  return Value::DateFromDays(days);
}

Value MakeArrayValue(ByteCursor& c) {
  const size_t n = c.NextU8() % 4;
  std::vector<Value> elems;
  elems.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    if (c.NextU8() % 5 == 0) {
      elems.emplace_back();
    } else {
      elems.push_back(MakeInt64Value(c, false));
    }
  }
  return Value::Array(std::move(elems), "INT64");
}

Value MakeColumnValue(ByteCursor& c, ColKind kind, bool allow_null) {
  if (allow_null && (c.NextU8() % 5 == 0)) {
    return {};
  }
  switch (kind) {
    case ColKind::kSignedInt64:
      return MakeInt64Value(c, false);
    case ColKind::kUnsignedInt64:
      return MakeInt64Value(c, true);
    case ColKind::kDouble:
      return MakeDoubleValue(c);
    case ColKind::kVarChar:
      return MakeVarCharValue(c);
    case ColKind::kDate:
      return MakeDateValue(c);
    case ColKind::kIntArray:
      return MakeArrayValue(c);
  }
  return Value(int64_t{0});
}

bool SameDecodedValue(const Value& a, const Value& b) {
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
    return da == db;
  }
  return a == b;
}

// Reference comparator matching the memcomparable byte order of a single
// same-typed column (NULLS FIRST, IEEE total order with NaN above +inf and
// -0.0 == +0.0 for index keys).
int CompareIndexColumn(const Value& a, const Value& b) {
  if (a.IsNull() || b.IsNull()) {
    return (a.IsNull() && b.IsNull()) ? 0 : (a.IsNull() ? -1 : 1);
  }
  if (a.type == ValueType::kDouble) {
    const double da = a.value.double_value;
    const double db = b.value.double_value;
    const bool na = std::isnan(da);
    const bool nb = std::isnan(db);
    if (na || nb) {
      return (na && nb) ? 0 : (na ? 1 : -1);
    }
    return da < db ? -1 : (db < da ? 1 : 0);
  }
  return CompareForOrderBy(a, b);
}

int CompareIndexRows(const Row& a, const Row& b) {
  const size_t n = std::min(a.Count(), b.Count());
  for (size_t i = 0; i < n; ++i) {
    const int c = CompareIndexColumn(a[i], b[i]);
    if (c != 0) {
      return c;
    }
  }
  return a.Count() < b.Count() ? -1 : (b.Count() < a.Count() ? 1 : 0);
}

// Reference sort-key byte encoder mirroring SortExecutor (executor/sort.cpp)
// where NaN sorts below -inf (matching CompareForOrderBy and SortKeyOrder).
void AppendSortExecutableValue(const Value& v, std::string* out) {
  switch (v.type) {
    case ValueType::kInt64:
    case ValueType::kDate: {
      out->push_back(static_cast<char>(v.type));
      const uint64_t be = htobe64(static_cast<uint64_t>(v.value.int_value));
      std::array<char, 8> buf{};
      std::memcpy(buf.data(), &be, buf.size());
      if (!v.IsUnsigned()) {
        buf[0] ^= static_cast<char>(0x80);
      }
      out->append(buf.data(), buf.size());
      break;
    }
    case ValueType::kVarChar: {
      const StatusOr<std::string> enc = v.TryEncodeMemcomparableFormat();
      if (!enc.HasValue()) {
        Fail("SortEncode", "VARCHAR TryEncodeMemcomparableFormat failed");
      }
      out->append(enc.Value());
      break;
    }
    case ValueType::kDouble: {
      out->push_back(static_cast<char>(ValueType::kDouble));
      double d = v.value.double_value;
      if (std::isnan(d)) {
        out->append(8, '\0');
        break;
      }
      if (d == 0.0) {
        d = 0.0;
      }
      uint64_t bits = 0;
      std::memcpy(&bits, &d, sizeof(bits));
      const bool negative = (bits >> 63) != 0;
      const uint64_t code = negative ? ~bits : (bits | (uint64_t{1} << 63));
      const uint64_t be = htobe64(code);
      std::array<char, 8> buf{};
      std::memcpy(buf.data(), &be, buf.size());
      out->append(buf.data(), buf.size());
      break;
    }
    case ValueType::kArray: {
      out->push_back(static_cast<char>(ValueType::kArray));
      for (const Value& elem : v.ArrayElements()) {
        if (elem.IsNull()) {
          out->push_back('\x01');
          continue;
        }
        out->push_back('\x02');
        AppendSortExecutableValue(elem, out);
      }
      out->push_back('\x00');
      break;
    }
    case ValueType::kNull:
      Fail("SortEncode", "Unexpected NULL in AppendSortExecutableValue");
  }
}

std::string EncodeDirectionalSortKey(
    const Row& row, const std::vector<SortKeyOrder>& key_orders) {
  std::string out;
  for (size_t i = 0; i < key_orders.size(); ++i) {
    const SortKeyOrder& ko = key_orders[i];
    const Value& raw = row[i];
    if (raw.IsNull()) {
      out.push_back(ko.NullsFirst() ? '\x00' : '\xff');
      continue;
    }
    const Value v = ApplyKeyCollation(raw, ko.KeyCollation());
    out.push_back('\x01');
    const size_t begin = out.size();
    AppendSortExecutableValue(v, &out);
    if (!ko.Ascending()) {
      for (size_t b = begin; b < out.size(); ++b) {
        out[b] = static_cast<char>(~out[b]);
      }
    }
  }
  return out;
}

void CheckSingleValueEncodeTo(const Value& v) {
  if (v.IsNull() || v.type == ValueType::kArray) {
    return;
  }
  const StatusOr<std::string> expected = v.TryEncodeMemcomparableFormat();
  if (!expected.HasValue()) {
    Fail("EncodeTo", "TryEncodeMemcomparableFormat failed on scalar");
  }
  std::vector<char> buf(expected.Value().size() + 16, '\x5a');
  const size_t written = v.EncodeMemcomparableFormatTo(buf.data());
  if (written != expected.Value().size()) {
    Fail("EncodeTo", "EncodeMemcomparableFormatTo length mismatch");
  }
  if (std::memcmp(buf.data(), expected.Value().data(), written) != 0) {
    Fail("EncodeTo", "EncodeMemcomparableFormatTo bytes mismatch");
  }
}

void CheckCompositeRoundTrip(const Row& row, std::string* out_encoded) {
  const StatusOr<std::string> enc_or = row.TryEncodeMemcomparableFormat();
  if (!enc_or.HasValue()) {
    Fail("RoundTrip", "Row::TryEncodeMemcomparableFormat failed");
  }
  const std::string& enc = enc_or.Value();

  // Per-column concatenation and DecodeMemcomparableFormatBounded walk must
  // match the composite buffer byte-for-byte.
  std::string manual_concat;
  size_t bounded_offset = 0;
  for (size_t i = 0; i < row.Count(); ++i) {
    CheckSingleValueEncodeTo(row[i]);
    const StatusOr<std::string> part = row[i].TryEncodeMemcomparableFormat();
    if (!part.HasValue()) {
      Fail("RoundTrip", "Value::TryEncodeMemcomparableFormat failed");
    }
    manual_concat += part.Value();
    const size_t step = Value::DecodeMemcomparableFormatBounded(
        enc.data() + bounded_offset, enc.size() - bounded_offset);
    if (step != part.Value().size()) {
      Fail("BoundedWalk",
           "DecodeMemcomparableFormatBounded chunk size mismatch");
    }
    bounded_offset += step;
  }
  if (bounded_offset != enc.size() || manual_concat != enc) {
    Fail("BoundedWalk",
         "Composite encoding does not equal sum of column chunks");
  }

  Row decoded;
  const Status st = decoded.TryDecodeMemcomparableFormat(enc);
  if (!st.ok()) {
    Fail("RoundTrip", "Row::TryDecodeMemcomparableFormat failed on valid key");
  }
  if (decoded.Count() != row.Count()) {
    Fail("RoundTrip", "Decoded column count mismatch");
  }
  for (size_t i = 0; i < row.Count(); ++i) {
    if (!SameDecodedValue(decoded[i], row[i])) {
      Fail("RoundTrip", "Decoded column value mismatch");
    }
  }

  // Canonical idempotence: re-encoding the decoded Row must produce the exact
  // same byte sequence.
  const StatusOr<std::string> re_enc = decoded.TryEncodeMemcomparableFormat();
  if (!re_enc.HasValue() || re_enc.Value() != enc) {
    Fail("RoundTrip", "Re-encoding decoded Row changed memcomparable bytes");
  }
  *out_encoded = enc;
}

void CheckAdversarialBytes(const uint8_t* data, size_t size) {
  const std::string_view raw(reinterpret_cast<const char*>(data), size);
  Row decoded;
  const Status st = decoded.TryDecodeMemcomparableFormat(raw);
  bool bounded_ok = true;
  size_t pos = 0;
  try {
    while (pos < raw.size()) {
      const size_t step = Value::DecodeMemcomparableFormatBounded(
          raw.data() + pos, raw.size() - pos);
      if (step == 0 || pos + step > raw.size()) {
        bounded_ok = false;
        break;
      }
      pos += step;
    }
  } catch (const std::exception&) {
    bounded_ok = false;
  }
  if (st.ok() != bounded_ok || (st.ok() && pos != raw.size())) {
    Fail("AdversarialParity",
         "TryDecodeMemcomparableFormat and DecodeMemcomparableFormatBounded "
         "disagreed on validity");
  }
  if (st.ok()) {
    // Any accepted byte stream without non-canonical NaN/negative-zero floats
    // must re-encode to the same length.
    const StatusOr<std::string> re_enc = decoded.TryEncodeMemcomparableFormat();
    if (!re_enc.HasValue() || re_enc.Value().size() != raw.size()) {
      Fail("AdversarialCanonical", "Accepted key failed to re-encode cleanly");
    }
  }
}

}  // namespace

int FuzzMemcmpOneInput(const uint8_t* data, size_t size) {
  if (size < 4) {
    return 0;
  }
  // Always run the raw adversarial parser check on the input slice first.
  CheckAdversarialBytes(data, size);

  ByteCursor cursor{.data = data, .size = size};
  const size_t num_cols = (cursor.NextU8() % 5) + 1;
  const size_t num_rows = (cursor.NextU8() % 6) + 2;

  std::vector<ColKind> col_kinds(num_cols);
  std::vector<SortKeyOrder> sort_orders(num_cols);
  for (size_t c = 0; c < num_cols; ++c) {
    col_kinds[c] = static_cast<ColKind>(cursor.NextU8() % 6);
    const uint8_t sort_cfg = cursor.NextU8();
    const bool asc = (sort_cfg & 1U) != 0;
    const bool nf = (sort_cfg & 2U) != 0;
    const Collation coll = ((sort_cfg & 4U) != 0) ? Collation::kCaseInsensitive
                                                  : Collation::kBinary;
    sort_orders[c] = SortKeyOrder(asc, nf, coll);
  }

  std::vector<Row> rows;
  std::vector<std::string> encoded_keys;
  std::vector<std::string> sort_keys;
  rows.reserve(num_rows);
  encoded_keys.reserve(num_rows);
  sort_keys.reserve(num_rows);

  for (size_t r = 0; r < num_rows; ++r) {
    std::vector<Value> vals;
    vals.reserve(num_cols);
    for (size_t c = 0; c < num_cols; ++c) {
      vals.push_back(MakeColumnValue(cursor, col_kinds[c], true));
    }
    rows.emplace_back(std::move(vals));
    std::string enc;
    CheckCompositeRoundTrip(rows.back(), &enc);
    encoded_keys.push_back(std::move(enc));
    sort_keys.push_back(EncodeDirectionalSortKey(rows.back(), sort_orders));
  }

  // Oracle 2 & 4: Pairwise ordering, hash consistency, and SortKeyOrder parity.
  const std::hash<Row> row_hasher;
  for (size_t i = 0; i < num_rows; ++i) {
    for (size_t j = 0; j < num_rows; ++j) {
      // Equal rows must hash identically and encode identically.
      if (rows[i] == rows[j]) {
        if (row_hasher(rows[i]) != row_hasher(rows[j])) {
          Fail("RowHash", "Equal rows produced different std::hash<Row>");
        }
        if (encoded_keys[i] != encoded_keys[j]) {
          Fail("RowEqEncode",
               "Equal rows produced different memcomparable keys");
        }
      }

      // Composite memcomparable byte order vs CompareIndexRows (for rows
      // without arrays, since array memcomparable prefixes element count).
      const bool has_array = std::ranges::any_of(
          col_kinds, [](ColKind k) { return k == ColKind::kIntArray; });
      if (!has_array) {
        const int byte_cmp = encoded_keys[i] < encoded_keys[j]
                                 ? -1
                                 : (encoded_keys[j] < encoded_keys[i] ? 1 : 0);
        const int ref_cmp = CompareIndexRows(rows[i], rows[j]);
        if (byte_cmp != ref_cmp) {
          Fail("CompositeOrder",
               "Memcomparable byte order disagreed with CompareIndexRows");
        }
      }

      // Directional sort-key byte order vs SortKeyOrder::Compare +
      // EqualUnderCollation.
      int expected_sort_cmp = 0;
      bool all_peers = true;
      for (size_t c = 0; c < num_cols; ++c) {
        const int cmp = sort_orders[c].Compare(rows[i][c], rows[j][c]);
        if (cmp != 0) {
          expected_sort_cmp = cmp < 0 ? -1 : 1;
          all_peers = false;
          break;
        }
        const Value ci =
            ApplyKeyCollation(rows[i][c], sort_orders[c].KeyCollation());
        const Value cj =
            ApplyKeyCollation(rows[j][c], sort_orders[c].KeyCollation());
        if (!EqualUnderCollation(ci, cj)) {
          all_peers = false;
        }
      }
      const int sort_byte_cmp = sort_keys[i] < sort_keys[j]
                                    ? -1
                                    : (sort_keys[j] < sort_keys[i] ? 1 : 0);
      if (sort_byte_cmp != expected_sort_cmp) {
        Fail("SortKeyOrder",
             "Directional sort key bytes disagreed with SortKeyOrder::Compare");
      }
      if (all_peers && sort_keys[i] != sort_keys[j]) {
        Fail("SortPeerEq",
             "Peer rows under EqualUnderCollation encoded to distinct keys");
      }
    }
  }

  // Oracle 3: Prefix range scan bound [prefix, prefix + '\xff') invariant.
  if (num_cols >= 2) {
    for (size_t prefix_cols = 1; prefix_cols < num_cols; ++prefix_cols) {
      for (size_t target = 0; target < num_rows; ++target) {
        std::string lower;
        bool has_null_prefix = false;
        for (size_t c = 0; c < prefix_cols; ++c) {
          if (rows[target][c].IsNull()) {
            has_null_prefix = true;
            break;
          }
          lower += rows[target][c].EncodeMemcomparableFormat();
        }
        if (has_null_prefix) {
          continue;
        }
        std::string upper = lower;
        upper.push_back('\xff');

        for (size_t r = 0; r < num_rows; ++r) {
          bool prefix_matches = true;
          for (size_t c = 0; c < prefix_cols; ++c) {
            if (!SameDecodedValue(rows[r][c], rows[target][c])) {
              prefix_matches = false;
              break;
            }
          }
          const bool in_range =
              (lower <= encoded_keys[r]) && (encoded_keys[r] < upper);
          if (prefix_matches != in_range) {
            Fail("PrefixBound",
                 "Index prefix range [lower, lower+0xff) misclassified row");
          }
        }
      }
    }
  }

  // Oracle 5b: Every 1-byte truncation of a valid composite encoding must
  // either fail cleanly or stop at an exact earlier column boundary.
  if (!encoded_keys.empty() && !encoded_keys[0].empty()) {
    const std::string& full = encoded_keys[0];
    std::vector<size_t> valid_boundaries = {0};
    for (size_t c = 0; c < rows[0].Count(); ++c) {
      valid_boundaries.push_back(valid_boundaries.back() +
                                 rows[0][c].EncodeMemcomparableFormat().size());
    }
    for (size_t cut = 1; cut < full.size(); ++cut) {
      const bool is_col_boundary =
          std::ranges::find(valid_boundaries, cut) != valid_boundaries.end();
      Row partial;
      const Status st = partial.TryDecodeMemcomparableFormat(
          std::string_view(full.data(), cut));
      if (st.ok() != is_col_boundary) {
        Fail("TruncationBoundary",
             "Mid-column truncation was accepted or valid prefix rejected");
      }
    }
  }

  return 0;
}

}  // namespace tinylamb::fuzz

#ifndef TINYLAMB_FUZZ_NO_MAIN_SYMBOL
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  return tinylamb::fuzz::FuzzMemcmpOneInput(data, size);
}
#endif
