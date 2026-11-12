/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0. */

// Byte-driven DataChunk/ColumnVector fuzzing: every bulk path
// (AppendGather, Append(source,row), AppendFrom, AppendRowFromColumns) must
// produce exactly the same observable state as the scalar reference: row
// values (null bits included), row positions, per-column zone maps
// (initialized / null count / value count / min / max) and the column
// aggregates (BIT_AND/OR/XOR, LOGICAL_AND/OR) against a scalar fold.

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "executor/data_chunk.hpp"
#include "executor/zone_map.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

struct Cursor {
  const uint8_t* data{};
  size_t size{};
  size_t pos{0};
  uint8_t Next() { return pos < size ? data[pos++] : 0; }
  [[nodiscard]] bool Done() const { return pos >= size; }
};

Value MakeInt(Cursor& c) {
  switch (c.Next() % 6) {
    case 0:
      return {};  // NULL
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
  if (c.Next() % 8 == 0) {
    return {};  // NULL
  }
  return Value(static_cast<double>(static_cast<int64_t>(c.Next()) - 128) / 4.0);
}

Value MakeString(Cursor& c) {
  if (c.Next() % 8 == 0) {
    return {};  // NULL
  }
  const size_t len = c.Next() % 5;
  std::string s;
  for (size_t i = 0; i < len; ++i) {
    s.push_back(static_cast<char>(1 + (c.Next() % 60)));
  }
  return Value(std::move(s));
}

bool SameOptionalValue(const std::optional<Value>& a,
                       const std::optional<Value>& b) {
  return a.has_value() == b.has_value() && (!a.has_value() || *a == *b);
}

void CheckChunkMatches(const DataChunk& a, const DataChunk& b,
                       const std::string& tag) {
  assert(a.Size() == b.Size());
  assert(a.ColumnCount() == b.ColumnCount());
  for (size_t row = 0; row < a.Size(); ++row) {
    const Row left = a.RowAt(row);
    const Row right = b.RowAt(row);
    assert(left == right);
    assert(a.PositionAt(row) == b.PositionAt(row));
  }
  for (size_t col = 0; col < a.ColumnCount(); ++col) {
    const ZoneMap& za = a.ZoneMapAt(col);
    const ZoneMap& zb = b.ZoneMapAt(col);
    assert(za.Initialized() == zb.Initialized());
    assert(za.NullCount() == zb.NullCount());
    assert(za.ValueCount() == zb.ValueCount());
    assert(SameOptionalValue(za.Minimum(), zb.Minimum()));
    assert(SameOptionalValue(za.Maximum(), zb.Maximum()));
  }
  (void)tag;
}

}  // namespace

namespace {
inline void Try(const uint8_t* data, size_t size, bool verbose) {
  if (size < 8) {
    return;
  }
  Cursor c{.data = data, .size = size};
  const Schema schema("chunk_fuzz", {Column("i", ValueType::kInt64),
                                     Column("d", ValueType::kDouble),
                                     Column("s", ValueType::kVarChar)});
  const size_t rows = 1 + (c.Next() % 24);
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
  for (size_t row = 0; row < model.size(); ++row) {
    scalar.Append(model[row], source.PositionAt(row));
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
  const size_t gather = 1 + (c.Next() % 32);
  for (size_t i = 0; i < gather && !c.Done() && source.Size() > 0; ++i) {
    selection.push_back(c.Next() % static_cast<uint32_t>(source.Size()));
  }
  DataChunk gathered(schema, 0);
  gathered.AppendGather(source, selection.data(), selection.size());
  DataChunk gathered_ref(schema, 0);
  for (const uint32_t idx : selection) {
    gathered_ref.Append(source.RowAt(idx), source.PositionAt(idx));
  }
  CheckChunkMatches(gathered_ref, gathered, "AppendGather");

  // (3b) AppendRowFromColumns over the source's column vectors.
  std::vector<const ColumnVector*> column_sources;
  for (size_t col = 0; col < source.ColumnCount(); ++col) {
    column_sources.push_back(&source.ColumnAt(col));
  }
  DataChunk from_columns(schema, 0);
  for (size_t i = 0; i < source.Size(); ++i) {
    from_columns.AppendRowFromColumns(column_sources, i, source.PositionAt(i));
  }
  CheckChunkMatches(scalar, from_columns, "AppendRowFromColumns");

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

  // (5) int column aggregates equal the scalar fold; an all-NULL (or empty)
  // input must yield NULL, any non-NULL input a non-NULL result.
  uint64_t bit_and = ~uint64_t{0};
  uint64_t bit_or = 0;
  uint64_t bit_xor = 0;
  bool bool_and = true;
  bool bool_or = false;
  bool any_non_null = false;
  for (size_t i = 0; i < source.Size(); ++i) {
    const Value v = source.ColumnAt(0).ValueAt(i);
    if (v.IsNull()) {
      continue;  // SQL aggregates skip NULL inputs.
    }
    any_non_null = true;
    const auto bits = static_cast<uint64_t>(v.value.int_value);
    bit_and &= bits;
    bit_or |= bits;
    bit_xor ^= bits;
    bool_and = bool_and && v.Truthy();
    bool_or = bool_or || v.Truthy();
  }
  const Value agg_and = source.AggregateBitAnd(0);
  const Value agg_or = source.AggregateBitOr(0);
  const Value agg_xor = source.AggregateBitXor(0);
  const Value agg_land = source.AggregateLogicalAnd(0);
  const Value agg_lor = source.AggregateLogicalOr(0);
  assert(agg_and.IsNull() == !any_non_null);
  assert(agg_or.IsNull() == !any_non_null);
  assert(agg_xor.IsNull() == !any_non_null);
  assert(agg_land.IsNull() == !any_non_null);
  assert(agg_lor.IsNull() == !any_non_null);
  if (any_non_null) {
    assert(static_cast<uint64_t>(agg_and.value.int_value) == bit_and);
    assert(static_cast<uint64_t>(agg_or.value.int_value) == bit_or);
    assert(static_cast<uint64_t>(agg_xor.value.int_value) == bit_xor);
    assert(agg_land.Truthy() == bool_and);
    assert(agg_lor.Truthy() == bool_or);
  }
  (void)verbose;
}

}  // namespace
}  // namespace tinylamb

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  tinylamb::Try(data, size, false);
  return 0;
}
