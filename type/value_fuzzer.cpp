/**
 * Copyright 2023 KUMAZAKI Hiroki
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

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "type/date.hpp"
#include "type/value.hpp"

using tinylamb::Value;
using tinylamb::ValueType;

namespace {

struct Stream {
  const uint8_t* data;
  size_t size;
  size_t offset{0};

  uint8_t Next() { return offset < size ? data[offset++] : 0; }
  bool Done() const { return offset >= size; }
};

int64_t NextI64(Stream& s) {
  uint64_t v = 0;
  for (size_t i = 0; i < 8 && !s.Done(); ++i) {
    v = (v << 8) | s.Next();
  }
  return static_cast<int64_t>(v);
}

// biased=true concentrates on the classic boundary payloads (0, ±1,
// INT64_MIN/MAX); biased=false uses the full byte range.
Value MakeInt(Stream& s, bool biased) {
  if (biased) {
    switch (s.Next() % 7) {
      case 0:
        return Value(int64_t{0});
      case 1:
        return Value(int64_t{1});
      case 2:
        return Value(int64_t{-1});
      case 3:
        return Value(std::numeric_limits<int64_t>::min());
      case 4:
        return Value(std::numeric_limits<int64_t>::max());
      case 5:
        return Value(int64_t{2});
      default:
        break;
    }
  }
  return Value(NextI64(s));
}

double NextDouble(Stream& s) {
  const int64_t raw = NextI64(s);
  double d = 0.0;
  std::memcpy(&d, &raw, sizeof(d));
  return d;
}

Value MakeDouble(Stream& s, bool biased) {
  if (biased) {
    switch (s.Next() % 8) {
      case 0:
        return Value(0.0);
      case 1:
        return Value(-0.0);
      case 2:
        return Value(1.0);
      case 3:
        return Value(std::numeric_limits<double>::infinity());
      case 4:
        return Value(-std::numeric_limits<double>::infinity());
      case 5:
        return Value(std::numeric_limits<double>::quiet_NaN());
      case 6:
        return Value(std::numeric_limits<double>::signaling_NaN());
      default:
        break;
    }
  }
  return Value(NextDouble(s));
}

Value MakeString(Stream& s, bool biased) {
  const size_t len = biased ? s.Next() % 4 : s.Next() % 33;
  std::string out;
  out.reserve(len);
  for (size_t i = 0; i < len; ++i) {
    // Include NUL and high bytes: memcomparable escaping must survive them.
    out.push_back(static_cast<char>(biased ? (s.Next() % 4) : s.Next()));
  }
  return Value(std::move(out));
}

Value MakeValue(Stream& s, bool biased) {
  const uint8_t pick = s.Next() % (biased ? 5 : 7);
  switch (pick) {
    case 0:
      return Value();  // NULL
    case 1:
    case 2:
      return MakeInt(s, biased);
    case 3:
      return MakeDouble(s, biased);
    case 4:
      return MakeString(s, biased);
    case 5:
      // DATE: Value::Date parses; invalid strings throw, so go through the
      // day-based constructor which is total.
      return Value::DateFromDays(static_cast<int64_t>(s.Next()) * 7 - 700);
    default: {
      const size_t n = s.Next() % 3;
      std::vector<Value> elems;
      for (size_t i = 0; i < n; ++i) {
        elems.push_back(MakeInt(s, biased));
      }
      return Value::Array(std::move(elems), "INT64");
    }
  }
}

bool SameValue(const Value& a, const Value& b) {
  if (a.IsNull() != b.IsNull() || a.type != b.type) {
    return false;
  }
  if (a.type == ValueType::kDouble) {
    if (std::isnan(a.value.double_value) || std::isnan(b.value.double_value)) {
      // NaN == NaN under Value::operator==?  Fall back to bitwise equality
      // so the round-trip oracle does not depend on SQL comparison rules.
      double x = a.value.double_value;
      double y = b.value.double_value;
      int64_t rx, ry;
      std::memcpy(&rx, &x, sizeof(rx));
      std::memcpy(&ry, &y, sizeof(ry));
      // Normalize only the NaN payload, keep +0/-0 distinct.
      if (std::isnan(x)) rx = std::numeric_limits<int64_t>::min();
      if (std::isnan(y)) ry = std::numeric_limits<int64_t>::min();
      return rx == ry;
    }
  }
  return a == b;
}

void CheckSerializeRoundTrip(const Value& v) {
  if (v.type == ValueType::kNull) {
    // A NULL Value is typeless: TryDeserialize documents "cannot parse
    // without type", NULLs travel through the Row-level null bitmap instead.
    return;
  }
  std::vector<char> buffer(v.Size());
  const size_t written = v.Serialize(buffer.data());
  assert(written == buffer.size());
  Value decoded;
  const auto des = decoded.TryDeserialize(buffer.data(), v.type);
  if (v.type != ValueType::kArray) {
    // Plain values must survive byte-for-byte.
    assert(des.HasValue());
    assert(SameValue(decoded, v));
  } else {
    // Arrays serialize their envelope; accept either full support or a
    // clean Status error, but never a crash or a wrong type.
    if (des.HasValue()) {
      assert(decoded.type == ValueType::kArray || decoded.IsNull());
    }
  }
  const auto skip = Value::TrySkipSerialized(buffer.data(), v.type);
  if (skip.HasValue()) {
    assert(skip.Value() == written);
  }
}

void CheckMemcomparable(const Value& v) {
  const auto enc = v.TryEncodeMemcomparableFormat();
  if (!enc.HasValue()) {
    return;  // Unsupported types must fail cleanly, which they did.
  }
  Value decoded;
  const auto dec = decoded.TryDecodeMemcomparableFormat(enc.Value());
  assert(dec.HasValue());
  assert(dec.Value() == enc.Value().size());
  assert(SameValue(decoded, v));
}

// Byte-order of the memcomparable encoding must follow the SQL < order for
// same-typed non-NULL values (the index key contract).
void CheckMemcomparableOrdering(const Value& a, const Value& b) {
  if (a.IsNull() || b.IsNull() || a.type != b.type ||
      a.type == ValueType::kArray) {
    return;
  }
  if (a.type == ValueType::kDouble &&
      (std::isnan(a.value.double_value) || std::isnan(b.value.double_value))) {
    return;
  }
  const auto ea = a.TryEncodeMemcomparableFormat();
  const auto eb = b.TryEncodeMemcomparableFormat();
  if (!ea.HasValue() || !eb.HasValue()) {
    return;
  }
  const auto less = a.TryLess(b);
  const auto eq = a == b;
  const int enc_cmp =
      ea.Value() < eb.Value() ? -1 : (eb.Value() < ea.Value() ? 1 : 0);
  if (less.HasValue()) {
    const auto greater = b.TryLess(a);
    const int sql_cmp =
        less.Value() ? -1 : (greater.HasValue() && greater.Value() ? 1 : 0);
    if (sql_cmp != 0) {
      assert(sql_cmp == enc_cmp);
    } else if (!eq) {
      // Distinct SQL-equal values (none today) would still need equal keys.
      assert(enc_cmp == 0);
    }
  }
}

void CheckTrichotomy(const Value& a, const Value& b) {
  if (a.IsNull() || b.IsNull() || a.type != b.type) {
    return;  // Cross-type ordering is SQL-undefined; NULL short-circuits.
  }
  const auto lt = a.TryLess(b);
  const auto gt = a.TryGreater(b);
  if (!lt.HasValue() || !gt.HasValue()) {
    return;  // Undefined orders (arrays) are allowed as errors.
  }
  const bool eq = (a == b);
  if (a.type == ValueType::kDouble &&
      (std::isnan(a.value.double_value) || std::isnan(b.value.double_value))) {
    // NaN compares false against everything (SQL and IEEE).
    assert(!lt.Value() && !gt.Value());
    return;
  }
  assert(static_cast<int>(lt.Value()) + static_cast<int>(gt.Value()) +
             static_cast<int>(eq) ==
         1);
  // Derived operators must agree with the primitives.
  assert(a <= b == (lt.Value() || eq));
  assert(a >= b == (gt.Value() || eq));
}

void CheckOrderByTotal(const Value& a, const Value& b, const Value& c) {
  const int ab = CompareForOrderBy(a, b);
  const int ba = CompareForOrderBy(b, a);
  assert((ab > 0) == (ba < 0));
  assert((ab == 0) == (ba == 0));
  const int bc = CompareForOrderBy(b, c);
  const int ac = CompareForOrderBy(a, c);
  if (ab < 0 && bc < 0) {
    assert(ac < 0);
  }
  if (ab > 0 && bc > 0) {
    assert(ac > 0);
  }
  if (ab == 0 && bc == 0) {
    assert(ac == 0);
  }
}

}  // namespace

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  if (size < 2) {
    return 0;
  }
  // Split the input at every position and check that the memcomparable
  // encoding preserves the VARCHAR total order.  The source strings must be
  // compared before they are moved into the Value objects; comparing the
  // moved-from strings would silently make every comparison false.
  const std::string_view input(reinterpret_cast<const char*>(data), size);
  for (size_t i = 1; i < size - 1; ++i) {
    const std::string_view left = input.substr(0, i);
    const std::string_view right = input.substr(i);
    const Value l{std::string(left)}, r{std::string(right)};
    const std::string encoded_left = l.EncodeMemcomparableFormat();
    const std::string encoded_right = r.EncodeMemcomparableFormat();
    const int cmp = left < right ? -1 : (right < left ? 1 : 0);
    const int encoded_cmp = encoded_left < encoded_right
                                ? -1
                                : (encoded_right < encoded_left ? 1 : 0);
    if (cmp != encoded_cmp) {
      __builtin_trap();
    }
  }

  // Typed value invariants over three derived values (two "biased" toward
  // boundary payloads, one fully random).
  Stream stream{data, size};
  const Value a = MakeValue(stream, true);
  const Value b = MakeValue(stream, true);
  const Value c = MakeValue(stream, false);

  CheckSerializeRoundTrip(a);
  CheckSerializeRoundTrip(b);
  CheckSerializeRoundTrip(c);
  CheckMemcomparable(a);
  CheckMemcomparable(b);
  CheckMemcomparable(c);
  CheckMemcomparableOrdering(a, b);
  CheckMemcomparableOrdering(b, c);
  CheckTrichotomy(a, b);
  CheckTrichotomy(b, c);
  CheckOrderByTotal(a, b, c);
  CheckOrderByTotal(c, b, a);

  // SQL arithmetic surface: TryArithmetic must return a Value or a Status,
  // never crash; int64 div-by-zero and overflow are errors, not exceptions.
  static const tinylamb::BinaryOperation ops[] = {
      tinylamb::BinaryOperation::kAdd,
      tinylamb::BinaryOperation::kSubtract,
      tinylamb::BinaryOperation::kMultiply,
      tinylamb::BinaryOperation::kDivide,
      tinylamb::BinaryOperation::kModulo,
  };
  for (const tinylamb::BinaryOperation op : ops) {
    (void)a.TryArithmetic(b, op);
    (void)b.TryArithmetic(c, op);
  }
  return 0;
}
