/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0. */

#ifndef TINYLAMB_PAX_PAGE_FUZZER_HPP
#define TINYLAMB_PAX_PAGE_FUZZER_HPP

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "common/byte_stream.hpp"
#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "executor/data_chunk.hpp"
#include "page/page.hpp"
#include "page/pax_page.hpp"
#include "page_type.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

// Byte-driven PAX page codec stress test.  Mode 1 builds a typed DataChunk
// from the input, stores it into a fresh kPaxPage, and requires Load() to
// reproduce every value exactly (the dictionary / bit-packed / plain column
// block paths all live behind this codec).  Mode 2 treats the raw fuzz bytes
// as an on-disk PAX body: Load() must then fail cleanly or return a chunk it
// can safely materialise -- never overrun the page (checked by ASan/UBSan).
inline void Try(const uint8_t* data, size_t size, bool verbose) {
  if (size < 2) {
    return;
  }
  ByteStream stream(data, size);
  const Schema schema(
      "pax_fuzz",
      {Column("id", ValueType::kInt64), Column("price", ValueType::kDouble),
       Column("name", ValueType::kVarChar), Column("day", ValueType::kDate)});
  DataChunk chunk(schema);
  const size_t row_count = stream.Pick(48);
  for (size_t row = 0; row < row_count; ++row) {
    Value id;
    Value price;
    Value name;
    Value day;
    if (stream.Pick(8) != 0) {  // NULL-heavy mixes exercise the bitmap
      switch (stream.Pick(5)) {
        case 0:
          id = Value(int64_t{0});
          break;
        case 1:
          id = Value(int64_t{-1});
          break;
        case 2:
          id = Value(static_cast<int64_t>(stream.Varint()));
          break;
        case 3:
          // Negate in unsigned space: Varint can land on INT64_MIN, whose
          // signed negation is UB (found by the UBSan pax run).
          id = Value(static_cast<int64_t>(0ULL - stream.Varint()));
          break;
        default:
          id = Value(int64_t{4096});
          break;
      }
    }
    if (stream.Pick(8) != 0) {
      double d = 0.0;
      if (stream.Pick(4) == 0) {
        d = stream.Pick(2) == 0 ? -0.0 : 0.0;
      } else {
        d = static_cast<double>(stream.Varint()) / 4.0;
        if (stream.Pick(2) == 0) {
          d = -d;
        }
      }
      price = Value(d);
    }
    if (stream.Pick(8) != 0) {
      name = Value(std::string(stream.Bytes(stream.Pick(40))));
    }
    if (stream.Pick(8) != 0) {
      day = Value::DateFromDays(static_cast<int64_t>(stream.Pick(8192)) - 4096);
    }
    chunk.Append(Row({id, price, name, day}));
  }

  Page page(41, PageType::kPaxPage);
  const Status store = page.body.pax_page.Store(chunk);
  if (store == Status::kSuccess) {
    ASSIGN_OR_CRASH(DataChunk, restored, page.body.pax_page.Load());
    assert(restored.Size() == chunk.Size());
    for (size_t row = 0; row < chunk.Size(); ++row) {
      assert(restored.RowAt(row) == chunk.RowAt(row));
    }
  } else {
    assert(store == Status::kNoSpace);
  }

  // Mode 2: hostile header -- reinterpret the fuzz bytes as a stored body.
  Page raw(42, PageType::kPaxPage);
  const size_t copy =
      size < kPageBodySize ? size : static_cast<size_t>(kPageBodySize);
  std::memcpy(&raw.body.pax_page, data, copy);
  const StatusOr<DataChunk> hostile = raw.body.pax_page.Load();
  if (hostile.HasValue() && verbose) {
    LOG(TRACE) << "hostile body decoded " << hostile.Value().Size() << " rows";
  }
}

}  // namespace tinylamb

#endif  // TINYLAMB_PAX_PAGE_FUZZER_HPP
