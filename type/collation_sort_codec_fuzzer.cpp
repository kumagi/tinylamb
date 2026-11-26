/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Pure in-process libFuzzer for type/collation.{hpp,cpp} and
// type/row_block_codec.{hpp,cpp}:
//   Oracle 1: Collation & SortKeyOrder algebraic laws (reflexivity,
//             antisymmetry, transitivity, EqualUnderCollation consistency,
//             HashFoldedForCollation == FNV-1a(FoldCaseForCollation),
//             CollationOfSpec/CollationFromName/CollationName round-trip).
//   Oracle 2: RowBlockCodec lossless round-trip for Values (including tags,
//             collations, unsigned, NaN/-0.0/+-Inf, arrays), Rows, and framed
//             RowBlocks.
//   Oracle 3: RowBlockCodec adversarial / truncated / bit-flipped input
//             resilience (never crashes, UBs, OOMs, or exceeds depth limits).

#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "type/collation.hpp"
#include "type/row.hpp"
#include "type/row_block_codec.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::fuzz {
namespace {

struct Cursor {
  const uint8_t* data{};
  size_t size{};
  size_t pos{0};

  uint8_t NextU8() noexcept { return pos < size ? data[pos++] : 0U; }

  int64_t NextI64() noexcept {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
      v = (v << 8U) | NextU8();
    }
    return static_cast<int64_t>(v);
  }

  std::string NextString(size_t max_len) {
    const uint8_t mode = NextU8() % 8U;
    if (mode == 0) {
      static constexpr std::array<const char*, 5> kEnums = {
          "TESTENUMNEGATIVE", "TESTENUM0", "TESTENUM1", "TESTENUM2",
          "TESTENUM2147483647"};
      return kEnums[NextU8() % kEnums.size()];
    }
    const size_t len = NextU8() % (max_len + 1);
    std::string s;
    s.reserve(len);
    for (size_t i = 0; i < len; ++i) {
      s.push_back(static_cast<char>(NextU8()));
    }
    return s;
  }

  Value NextValue(int depth) {
    const uint8_t kind = NextU8() % (depth > 0 ? 8U : 7U);
    switch (kind) {
      case 0:
        return Value();
      case 1:
        return Value(NextI64());
      case 2:
        return Value(NextI64()).WithUnsigned();
      case 3: {
        const uint8_t dmode = NextU8() % 6U;
        if (dmode == 0) {
          return Value(-0.0);
        }
        if (dmode == 1) {
          return Value(0.0);
        }
        if (dmode == 2) {
          return Value(std::numeric_limits<double>::quiet_NaN());
        }
        if (dmode == 3) {
          return Value(std::numeric_limits<double>::infinity());
        }
        if (dmode == 4) {
          return Value(-std::numeric_limits<double>::infinity());
        }
        double d = 0.0;
        const int64_t bits = NextI64();
        std::memcpy(&d, &bits, sizeof(d));
        return Value(d);
      }
      case 4:
        return Value::DateFromDays(NextI64() % 100000);
      case 5:
      case 6: {
        Value v(NextString(20));
        const uint8_t tag = NextU8() % 3U;
        if (tag == 1) {
          v = ApplyCollation(v, Collation::kBinary);
        } else if (tag == 2) {
          v = ApplyCollation(v, Collation::kCaseInsensitive);
        }
        return v;
      }
      default: {
        const size_t n = NextU8() % 4U;
        std::vector<Value> elems;
        elems.reserve(n);
        for (size_t i = 0; i < n; ++i) {
          elems.push_back(NextValue(depth - 1));
        }
        return Value::Array(std::move(elems), "INT64");
      }
    }
  }
};

[[noreturn]] void Fail(const char* oracle, const char* msg) {
  (void)std::fprintf(stderr, "[COLLATION-CODEC-FUZZ FAIL][%s] %s\n", oracle,
                     msg);
  (void)std::fflush(stderr);
  std::abort();
}

int Sign(int x) noexcept { return x < 0 ? -1 : (x > 0 ? 1 : 0); }

bool ValuesBitEqual(const Value& a, const Value& b) {
  if (a.type != b.type || a.Collation() != b.Collation() ||
      a.IsUnsigned() != b.IsUnsigned()) {
    return false;
  }
  switch (a.type) {
    case ValueType::kNull:
      return true;
    case ValueType::kInt64:
    case ValueType::kDate:
      return a.value.int_value == b.value.int_value;
    case ValueType::kDouble: {
      uint64_t ba = 0;
      uint64_t bb = 0;
      std::memcpy(&ba, &a.value.double_value, sizeof(ba));
      std::memcpy(&bb, &b.value.double_value, sizeof(bb));
      return ba == bb;
    }
    case ValueType::kVarChar:
      return a.value.varchar_value == b.value.varchar_value;
    case ValueType::kArray: {
      const auto& ea = a.ArrayElements();
      const auto& eb = b.ArrayElements();
      if (ea.size() != eb.size()) {
        return false;
      }
      for (size_t i = 0; i < ea.size(); ++i) {
        if (!ValuesBitEqual(ea[i], eb[i])) {
          return false;
        }
      }
      return true;
    }
  }
  return false;
}

void FuzzCollationAndSortOrder(Cursor& c) {
  // HashFoldedForCollation must match FNV-1a of FoldCaseForCollation
  const std::string s = c.NextString(48);
  const std::string folded = FoldCaseForCollation(s);
  uint64_t ref_hash = 14695981039346656037ULL;
  for (const char ch : folded) {
    ref_hash ^= static_cast<uint64_t>(static_cast<unsigned char>(ch));
    ref_hash *= 1099511628211ULL;
  }
  if (HashFoldedForCollation(s) != static_cast<size_t>(ref_hash)) {
    Fail("HashFolded",
         "HashFoldedForCollation disagreed with FoldCaseForCollation");
  }

  // CollationName / CollationFromName / CollationOfSpec sanity
  for (const Collation col :
       {Collation::kNone, Collation::kBinary, Collation::kCaseInsensitive}) {
    if (CollationFromName(CollationName(col)) != col) {
      Fail("CollationName", "CollationFromName(CollationName(c)) != c");
    }
  }
  (void)CollationOfSpec(s);

  // Algebraic laws of SortKeyOrder::Compare on triples
  const bool asc = (c.NextU8() & 1U) != 0;
  const std::optional<bool> nulls_first =
      (c.NextU8() & 1U) != 0 ? std::optional<bool>((c.NextU8() & 1U) != 0)
                             : std::nullopt;
  const Collation key_col = static_cast<Collation>(c.NextU8() % 3U);
  SortKeyOrder order(asc, nulls_first, key_col);

  const Value a = c.NextValue(1);
  const Value b = c.NextValue(1);
  const Value d = c.NextValue(1);

  (void)order.Observe(a);
  (void)order.Observe(b);
  (void)order.Observe(d);

  // Reflexivity
  if (order.Compare(a, a) != 0 || order.Compare(b, b) != 0 ||
      order.Compare(d, d) != 0) {
    Fail("SortKeyOrder", "Compare is not reflexive");
  }

  // Antisymmetry
  const int ab = Sign(order.Compare(a, b));
  const int ba = Sign(order.Compare(b, a));
  if (ab != -ba) {
    Fail("SortKeyOrder", "Compare is not antisymmetric");
  }
  const int bc = Sign(order.Compare(b, d));
  const int cb = Sign(order.Compare(d, b));
  if (bc != -cb) {
    Fail("SortKeyOrder", "Compare is not antisymmetric (b, c)");
  }
  const int ac = Sign(order.Compare(a, d));
  const int ca = Sign(order.Compare(d, a));
  if (ac != -ca) {
    Fail("SortKeyOrder", "Compare is not antisymmetric (a, c)");
  }

  // Transitivity holds when no conflicting explicit string collations exist
  // (ResolveCollation folds whenever either side is CI, which is non-transitive
  // if mixed with explicit kBinary on a third value).
  const bool uniform_collation =
      !HaveConflictingCollations(a, b) && !HaveConflictingCollations(b, d) &&
      !HaveConflictingCollations(a, d) &&
      (CollationOf(a) == CollationOf(b) && CollationOf(b) == CollationOf(d));
  if (uniform_collation) {
    if (ab <= 0 && bc <= 0 && ac > 0) {
      Fail("SortKeyOrder", "Compare violated transitivity (<=)");
    }
    if (ab >= 0 && bc >= 0 && ac < 0) {
      Fail("SortKeyOrder", "Compare violated transitivity (>=)");
    }
  }

  // EqualUnderCollation reflexivity and symmetry
  if (!EqualUnderCollation(a, a)) {
    Fail("EqualUnderCollation", "not reflexive");
  }
  if (EqualUnderCollation(a, b) != EqualUnderCollation(b, a)) {
    Fail("EqualUnderCollation", "not symmetric");
  }
}

void FuzzRowBlockCodecRoundTrip(Cursor& c) {
  const size_t num_rows = 1U + (c.NextU8() % 12U);
  const size_t num_cols = 1U + (c.NextU8() % 6U);
  std::vector<Row> rows;
  rows.reserve(num_rows);
  for (size_t r = 0; r < num_rows; ++r) {
    std::vector<Value> vals;
    vals.reserve(num_cols);
    for (size_t col = 0; col < num_cols; ++col) {
      vals.push_back(c.NextValue(2));
    }
    Row row(std::move(vals));
    (void)EstimateRowMemoryBytes(row);
    rows.push_back(std::move(row));
  }

  // Single-row round-trip
  std::string one_buf;
  if (EncodeSpillRow(rows[0], &one_buf) == Status::kSuccess) {
    std::string_view view(one_buf);
    StatusOr<Row> dec = DecodeSpillRow(&view);
    if (!dec.HasValue() || !view.empty() ||
        dec.Value().values_.size() != rows[0].values_.size()) {
      Fail("SpillRow", "EncodeSpillRow/DecodeSpillRow failed");
    }
    for (size_t i = 0; i < rows[0].values_.size(); ++i) {
      if (!ValuesBitEqual(rows[0][i], dec.Value()[i])) {
        Fail("SpillRow", "decoded Value mismatch");
      }
    }
  }

  // Framed RowBlock round-trip
  std::string block_buf;
  if (EncodeRowBlock(rows, &block_buf) == Status::kSuccess) {
    std::string_view view(block_buf);
    std::vector<Row> decoded;
    if (DecodeRowBlock(&view, &decoded) != Status::kSuccess || !view.empty() ||
        decoded.size() != rows.size()) {
      Fail("RowBlock", "EncodeRowBlock/DecodeRowBlock failed");
    }
    for (size_t r = 0; r < rows.size(); ++r) {
      if (decoded[r].values_.size() != rows[r].values_.size()) {
        Fail("RowBlock", "decoded row size mismatch");
      }
      for (size_t col = 0; col < rows[r].values_.size(); ++col) {
        if (!ValuesBitEqual(rows[r][col], decoded[r][col])) {
          Fail("RowBlock", "decoded cell mismatch");
        }
      }
    }

    // Truncation resilience
    if (block_buf.size() > 1) {
      const size_t cut = c.NextU8() % block_buf.size();
      std::string_view trunc(block_buf.data(), cut);
      std::vector<Row> out;
      if (DecodeRowBlock(&trunc, &out) == Status::kSuccess) {
        Fail("RowBlock", "truncated block decoded as success");
      }
    }
  }
}

void FuzzCorruptRowBlockDecode(const uint8_t* data, size_t size) {
  std::string_view sv1(reinterpret_cast<const char*>(data), size);
  (void)DecodeSpillValue(&sv1);
  std::string_view sv2(reinterpret_cast<const char*>(data), size);
  (void)DecodeSpillRow(&sv2);
  std::string_view sv3(reinterpret_cast<const char*>(data), size);
  std::vector<Row> rows;
  (void)DecodeRowBlock(&sv3, &rows);
}

}  // namespace
}  // namespace tinylamb::fuzz

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size < 2 || size > 4096) {
    return 0;
  }
  tinylamb::fuzz::Cursor c{data, size, 0};
  switch (c.NextU8() % 3U) {
    case 0:
      tinylamb::fuzz::FuzzCollationAndSortOrder(c);
      break;
    case 1:
      tinylamb::fuzz::FuzzRowBlockCodecRoundTrip(c);
      break;
    default:
      tinylamb::fuzz::FuzzCorruptRowBlockDecode(data + 1, size - 1);
      break;
  }
  return 0;
}
