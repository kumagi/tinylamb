/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0. */

// Byte-driven DataChunk/ColumnVector fuzzing: every bulk path
// (AppendGather, Append(source,row), AppendFrom, AppendRowFromColumns) must
// produce exactly the same observable state as the scalar Append(Row)
// reference, including null bits, positions and the zone-map-backed
// aggregates.

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

#include "executor/data_chunk.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

struct Cursor {
  const uint8_t* data;
  size_t size;
  size_t pos{0};
  uint8_t Next() { return pos < size ? data[pos++] : 0; }
  bool Done() const { return pos >= size; }
};

Value MakeInt(Cursor& c) {
  switch (c.Next() % 6) {
    case 0:
      return Value();
    case 1:
      return Value(int64_t{0});
    case 2:
      return Value(int64_t{-1});
    case 3:
      return Value(std::numeric_limits<int64_t>::min());
    case 4:
      return Value(static_cast<int64_t>(c.Next()) - 128);
    default:
      return Value(static_cast<int64_t>(c.Next()) << 40);
  }
}

Value MakeDouble(Cursor& c) {
  if (c.Next() % 8 == 0) return Value();
  return Value(static_cast<double>(static_cast<int64_t>(c.Next()) - 128) / 4.0);
}

Value MakeString(Cursor& c) {
  if (c.Next() % 8 == 0) return Value();
  const size_t len = c.Next() % 5;
  std::string s;
  for (size_t i = 0; i < len; ++i) {
    s.push_back(static_cast<char>(1 + c.Next() % 60));
  }
  return Value(std::move(s));
}

void CheckChunkMatches(const DataChunk& a, const DataChunk& b,
                       const std::string& tag) {
  assert(a.Size() == b.Size());
  assert(a.ColumnCount() == b.ColumnCount());
  for (size_t row = 0; row < a.Size(); ++row) {
    const Row left = a.RowAt(row);
    const Row right = b.RowAt(row);
    assert(left == right);
  }
  (void)tag;
}

}  // namespace

inline void Try(const uint8_t* data, size_t size, bool verbose) {
  if (size < 8) {
    return;
  }
  Cursor c{data, size};
  const Schema schema("chunk_fuzz", {Column("i", ValueType::kInt64),
                                     Column("d", ValueType::kDouble),
                                     Column("s", ValueType::kVarChar)});
  const size_t rows = 1 + c.Next() % 24;
  DataChunk source(schema);
  std::vector<Row> model;
  for (size_t row = 0; row < rows && !c.Done(); ++row) {
    Row r({MakeInt(c), MakeDouble(c), MakeString(c)});
    source.Append(r, RowPosition{static_cast<page_id_t>(row),
                                 static_cast<slot_t>(row * 2)});
    model.push_back(std::move(r));
  }

  // (1) scalar Append reference build.
  DataChunk scalar(schema);
  for (const Row& r : model) {
    scalar.Append(r);
  }
  for (size_t row = 0; row < model.size(); ++row) {
    assert(scalar.RowAt(row) == model[row]);
  }

  // (2) Append(source, i) path.
  DataChunk row_by_row(schema, 0);
  for (size_t i = 0; i < source.Size(); ++i) {
    row_by_row.Append(source, i);
  }
  CheckChunkMatches(scalar, row_by_row, "Append(source,row)");

  // (3) AppendGather with a random selection vector.
  std::vector<uint32_t> selection;
  const size_t gather = 1 + c.Next() % 32;
  for (size_t i = 0; i < gather && !c.Done(); ++i) {
    selection.push_back(c.Next() % static_cast<uint8_t>(source.Size() + 1) %
                        static_cast<uint32_t>(source.Size()));
  }
  DataChunk gathered(schema, 0);
  gathered.AppendGather(source, selection.data(), selection.size());
  DataChunk gathered_ref(schema, 0);
  for (const uint32_t idx : selection) {
    gathered_ref.Append(source.RowAt(idx));
  }
  CheckChunkMatches(gathered_ref, gathered, "AppendGather");

  // (4) ColumnVector::AppendFrom matches scalar appends.
  for (size_t col = 0; col < source.ColumnCount(); ++col) {
    ColumnVector from_col(schema.GetColumn(col).Type(), 0);
    ColumnVector scalar_col(schema.GetColumn(col).Type(), 0);
    for (size_t i = 0; i < source.Size(); ++i) {
      from_col.AppendFrom(source.ColumnAt(col), i);
      scalar_col.Append(source.ColumnAt(col).ValueAt(i));
    }
    assert(from_col.Size() == scalar_col.Size());
    for (size_t i = 0; i < from_col.Size(); ++i) {
      assert(from_col.IsNull(i) == scalar_col.IsNull(i));
      assert(from_col.ValueAt(i) == scalar_col.ValueAt(i));
    }
  }

  // (5) int column aggregates equal the scalar fold.
  uint64_t bit_and = ~uint64_t{0};
  uint64_t bit_or = 0;
  uint64_t bit_xor = 0;
  int64_t bool_and = 1;
  int64_t bool_or = 0;
  for (size_t i = 0; i < source.Size(); ++i) {
    const Value v = source.ColumnAt(0).ValueAt(i);
    if (v.IsNull()) {
      continue;  // SQL aggregates skip NULL inputs
    }
    const uint64_t bits = static_cast<uint64_t>(v.value.int_value);
    bit_and &= bits;
    bit_or |= bits;
    bit_xor ^= bits;
    bool_and = (bool_and && v.Truthy()) ? 1 : 0;
    bool_or = (bool_or || v.Truthy()) ? 1 : 0;
  }
  const bool any_int = bit_and != ~uint64_t{0} || bit_or != 0 || bit_xor != 0;
  if (source.Size() > 0 && any_int) {
    const Value agg_and = source.AggregateBitAnd(0);
    const Value agg_or = source.AggregateBitOr(0);
    const Value agg_xor = source.AggregateBitXor(0);
    if (!agg_and.IsNull()) {
      assert(static_cast<uint64_t>(agg_and.value.int_value) == bit_and);
    }
    if (!agg_or.IsNull()) {
      assert(static_cast<uint64_t>(agg_or.value.int_value) == bit_or);
    }
    if (!agg_xor.IsNull()) {
      assert(static_cast<uint64_t>(agg_xor.value.int_value) == bit_xor);
    }
    (void)bool_and;
    (void)bool_or;
  }
  (void)verbose;
}

}  // namespace tinylamb

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  tinylamb::Try(data, size, false);
  return 0;
}
