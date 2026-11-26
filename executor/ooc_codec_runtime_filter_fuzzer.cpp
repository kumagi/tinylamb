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

#include <fuzzer/FuzzedDataProvider.h>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <vector>

#include "common/random_string.hpp"
#include "common/runtime_filter.hpp"
#include "common/sorted_distinct_runs.hpp"
#include "common/spill/spill_store.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/ooc/chunk_codec.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

bool SameValue(const Value& a, const Value& b) {
  if (a.IsNull() && b.IsNull()) {
    return true;
  }
  if (a.IsNull() != b.IsNull() || a.type != b.type) {
    return false;
  }
  if (a.type == ValueType::kDouble) {
    if (std::isnan(a.value.double_value) && std::isnan(b.value.double_value)) {
      return true;
    }
    return a.value.double_value == b.value.double_value;
  }
  return a == b;
}

void FuzzRuntimeFilter(FuzzedDataProvider& fdp) {
  RuntimeFilterBuilder builder;
  std::set<int64_t> observed;
  const int nkeys = fdp.ConsumeIntegralInRange<int>(0, 128);
  for (int i = 0; i < nkeys && fdp.remaining_bytes() > 0; ++i) {
    if (fdp.ConsumeIntegralInRange<int>(0, 7) == 0) {
      builder.ObserveNull();
      continue;
    }
    int64_t k = 0;
    const int kind = fdp.ConsumeIntegralInRange<int>(0, 3);
    if (kind == 0) {
      k = fdp.ConsumeIntegralInRange<int64_t>(-100, 100);
    } else if (kind == 1) {
      k = fdp.ConsumeIntegral<int64_t>();
    } else if (kind == 2) {
      k = std::numeric_limits<int64_t>::min();
    } else {
      k = std::numeric_limits<int64_t>::max();
    }
    builder.Observe(k);
    observed.insert(k);
  }

  // Test small max_exact_keys so both ExactSet and MinMax+Bloom paths trigger.
  const size_t max_exact = fdp.ConsumeIntegralInRange<size_t>(0, 32);
  const bool allow_reject_all = fdp.ConsumeBool();
  auto group = builder.Build(allow_reject_all, max_exact);

  if (observed.empty()) {
    if (allow_reject_all) {
      assert(group != nullptr);
      assert(group->RejectsAll());
      assert(group->Reject(0));
      assert(group->Reject(std::numeric_limits<int64_t>::min()));
      assert(group->Reject(std::numeric_limits<int64_t>::max()));
    }
  } else {
    assert(group != nullptr);
    // Soundness: NO false negatives on any observed key!
    for (int64_t k : observed) {
      assert(!group->Reject(k));
      assert(!group->RejectRange(k, k));
      if (k > std::numeric_limits<int64_t>::min()) {
        assert(!group->RejectRange(k - 1, k));
      }
      if (k < std::numeric_limits<int64_t>::max()) {
        assert(!group->RejectRange(k, k + 1));
      }
    }
    // If exact set was chosen, there must be NO false positives either.
    if (group->Has(RuntimeFilterKind::kExactSet) && group->PartCount() == 1) {
      for (int p = 0; p < 16 && fdp.remaining_bytes() > 0; ++p) {
        const int64_t probe = fdp.ConsumeIntegralInRange<int64_t>(-120, 120);
        assert(group->Reject(probe) == !observed.contains(probe));
      }
    }
    // Range rejection soundness over random intervals.
    for (int r = 0; r < 8 && fdp.remaining_bytes() > 0; ++r) {
      int64_t a = fdp.ConsumeIntegralInRange<int64_t>(-150, 150);
      int64_t b = fdp.ConsumeIntegralInRange<int64_t>(-150, 150);
      if (a > b) {
        std::swap(a, b);
      }
      auto it = observed.lower_bound(a);
      const bool has_key_in_range = (it != observed.end() && *it <= b);
      if (has_key_in_range) {
        assert(!group->RejectRange(a, b));
      }
    }
  }

  // Also fuzz malformed BloomRuntimeFilter bit counts / word counts.
  const size_t words = fdp.ConsumeIntegralInRange<size_t>(0, 8);
  const size_t bit_count = fdp.ConsumeIntegralInRange<size_t>(0, 1024);
  std::vector<uint64_t> bits(words, fdp.ConsumeIntegral<uint64_t>());
  BloomRuntimeFilter bloom(std::move(bits), bit_count, observed.size());
  if (bloom.disabled()) {
    assert(!bloom.Reject(0));
    assert(!bloom.Reject(42));
  } else {
    (void)bloom.Reject(fdp.ConsumeIntegral<int64_t>());
  }
}

void FuzzSortedDistinctRuns(FuzzedDataProvider& fdp) {
  SortedDistinctRuns runs_a;
  SortedDistinctRuns runs_b;
  std::set<uint64_t> model_a;
  std::set<uint64_t> model_b;

  const int batches = fdp.ConsumeIntegralInRange<int>(1, 12);
  for (int b = 0; b < batches && fdp.remaining_bytes() > 0; ++b) {
    const int n = fdp.ConsumeIntegralInRange<int>(0, 24);
    std::vector<uint64_t> vals;
    vals.reserve(static_cast<size_t>(n));
    for (int i = 0; i < n && fdp.remaining_bytes() > 0; ++i) {
      const int kind = fdp.ConsumeIntegralInRange<int>(0, 2);
      uint64_t v = 0;
      if (kind == 0) {
        v = fdp.ConsumeIntegralInRange<uint64_t>(0, 32);
      } else if (kind == 1) {
        v = fdp.ConsumeIntegral<uint64_t>();
      } else {
        v = std::numeric_limits<uint64_t>::max();
      }
      vals.push_back(v);
    }
    std::sort(vals.begin(), vals.end());
    vals.erase(std::unique(vals.begin(), vals.end()), vals.end());

    const bool use_a = fdp.ConsumeBool();
    SortedDistinctRuns& target = use_a ? runs_a : runs_b;
    std::set<uint64_t>& model = use_a ? model_a : model_b;

    const bool inject_fail = fdp.ConsumeIntegralInRange<int>(0, 7) == 0;
    if (inject_fail) {
      target.SetFailNextMergeForTest(true);
    }
    Status st = target.Add(vals);
    if (st == Status::kSuccess) {
      model.insert(vals.begin(), vals.end());
    }
    target.SetFailNextMergeForTest(false);
  }

  if (fdp.ConsumeBool()) {
    Status st = runs_a.Absorb(std::move(runs_b));
    assert(st == Status::kSuccess);
    model_a.insert(model_b.begin(), model_b.end());
    model_b.clear();
  }

  std::vector<uint64_t> got;
  assert(runs_a.ForEach([&](uint64_t v) { got.push_back(v); }) ==
         Status::kSuccess);
  std::vector<uint64_t> want(model_a.begin(), model_a.end());
  assert(got == want);
}

void FuzzChunkCodecCorrupt(const uint8_t* data, size_t size) {
  std::string_view sv(reinterpret_cast<const char*>(data), size);
  const std::string_view orig = sv;
  DataChunk out;
  Status st = DecodeDataChunk(&sv, &out);
  if (st != Status::kSuccess) {
    assert(sv.data() == orig.data() && sv.size() == orig.size());
    assert(out.Size() == 0);
  } else {
    assert(sv.size() <= orig.size());
    (void)EstimateChunkMemoryBytes(out);
    for (size_t r = 0; r < out.Size(); ++r) {
      Row row = out.RowAt(r);
      (void)row.Size();
    }
  }
}

void FuzzChunkCodecRoundTrip(FuzzedDataProvider& fdp) {
  const std::vector<ValueType> types = {ValueType::kInt64, ValueType::kDouble,
                                        ValueType::kVarChar, ValueType::kDate};
  const int nrows = fdp.ConsumeIntegralInRange<int>(0, 40);
  std::vector<Row> rows;
  std::vector<RowPosition> positions;
  rows.reserve(static_cast<size_t>(nrows));
  positions.reserve(static_cast<size_t>(nrows));

  for (int r = 0; r < nrows && fdp.remaining_bytes() > 0; ++r) {
    Value vi = fdp.ConsumeIntegralInRange<int>(0, 4) == 0
                   ? Value()
                   : Value(fdp.ConsumeIntegral<int64_t>());
    Value vd;
    if (fdp.ConsumeIntegralInRange<int>(0, 4) != 0) {
      const int dk = fdp.ConsumeIntegralInRange<int>(0, 3);
      if (dk == 0) {
        vd = Value(std::numeric_limits<double>::quiet_NaN());
      } else if (dk == 1) {
        vd = Value(-0.0);
      } else if (dk == 2) {
        vd = Value(std::numeric_limits<double>::infinity());
      } else {
        vd = Value(fdp.ConsumeFloatingPoint<double>());
      }
    }
    Value vs = fdp.ConsumeIntegralInRange<int>(0, 4) == 0
                   ? Value()
                   : Value(fdp.ConsumeRandomLengthString(64));
    Value vdt;
    if (fdp.ConsumeIntegralInRange<int>(0, 4) != 0) {
      vdt = Value(fdp.ConsumeIntegralInRange<int64_t>(-50000, 50000));
      vdt.type = ValueType::kDate;
    }
    rows.emplace_back(std::vector<Value>{vi, vd, vs, vdt});
    positions.emplace_back(
        static_cast<page_id_t>(fdp.ConsumeIntegralInRange<uint32_t>(1, 100)),
        static_cast<slot_t>(r));
  }

  DataChunk chunk;
  chunk.Initialize(types, rows.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    chunk.Append(rows[i], positions[i]);
  }

  // 1. In-memory EncodeDataChunk / DecodeDataChunk round-trip
  std::string buf;
  assert(EncodeDataChunk(chunk, &buf) == Status::kSuccess);
  std::string_view sv(buf);
  DataChunk decoded;
  assert(DecodeDataChunk(&sv, &decoded) == Status::kSuccess);
  assert(sv.empty());
  assert(decoded.Size() == rows.size());
  for (size_t r = 0; r < rows.size(); ++r) {
    Row got = decoded.RowAt(r);
    assert(got.values_.size() == rows[r].values_.size());
    for (size_t c = 0; c < rows[r].values_.size(); ++c) {
      if (!SameValue(got[c], rows[r][c])) {
        std::fprintf(stderr,
                     "CHUNK_CODEC MISMATCH r=%zu c=%zu got(type=%d,null=%d,int=%lld) want(type=%d,null=%d,int=%lld)\n",
                     r, c, static_cast<int>(got[c].type), got[c].IsNull() ? 1 : 0,
                     static_cast<long long>(got[c].value.int_value),
                     static_cast<int>(rows[r][c].type),
                     rows[r][c].IsNull() ? 1 : 0,
                     static_cast<long long>(rows[r][c].value.int_value));
        assert(false);
      }
    }
  }

  // 2. SpillStore WriteDataChunk / ReadDataChunk round-trip with small
  // max_frame_bytes to force frame splitting!
  const std::filesystem::path dir =
      std::filesystem::temp_directory_path() /
      ("chunk_codec_fuzz_" + RandomString(10));
  std::error_code ec;
  std::filesystem::create_directories(dir, ec);
  {
    SpillStoreOptions opts;
    opts.temp_dir = dir;
    opts.temp_dir_set = true;
    SpillStore store(opts);
    std::string scratch;
    const size_t max_frame = fdp.ConsumeIntegralInRange<size_t>(0, 256);
    SpillSegmentId seg_id = kInvalidSpillSegmentId;
    {
      SpillSegmentWriter writer(&store);
      assert(WriteDataChunk(&writer, chunk, &scratch, max_frame) ==
             Status::kSuccess);
      auto seg_res = writer.Finish();
      assert(seg_res.HasValue());
      seg_id = seg_res.Value();
    }

    SpillSegmentReader reader(&store, seg_id);
    std::vector<Row> roundtripped;
    while (true) {
      DataChunk part;
      auto next_res = ReadDataChunk(&reader, &part, &scratch);
      assert(next_res.HasValue());
      if (!next_res.Value()) {
        break;
      }
      for (size_t r = 0; r < part.Size(); ++r) {
        roundtripped.push_back(part.RowAt(r));
      }
    }
    assert(roundtripped.size() == rows.size());
    for (size_t r = 0; r < rows.size(); ++r) {
      assert(roundtripped[r].values_.size() == rows[r].values_.size());
      for (size_t c = 0; c < rows[r].values_.size(); ++c) {
        assert(SameValue(roundtripped[r][c], rows[r][c]));
      }
    }
    store.DropSegment(seg_id);
    assert(store.Stats().current_active_bytes == 0);
  }
  std::filesystem::remove_all(dir, ec);
}

}  // namespace
}  // namespace tinylamb

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size < 2) {
    return 0;
  }
  const uint8_t mode = data[0] % 4;
  if (mode == 0) {
    FuzzedDataProvider fdp(data + 1, size - 1);
    tinylamb::FuzzRuntimeFilter(fdp);
  } else if (mode == 1) {
    FuzzedDataProvider fdp(data + 1, size - 1);
    tinylamb::FuzzSortedDistinctRuns(fdp);
  } else if (mode == 2) {
    tinylamb::FuzzChunkCodecCorrupt(data + 1, size - 1);
  } else {
    FuzzedDataProvider fdp(data + 1, size - 1);
    tinylamb::FuzzChunkCodecRoundTrip(fdp);
  }
  return 0;
}
