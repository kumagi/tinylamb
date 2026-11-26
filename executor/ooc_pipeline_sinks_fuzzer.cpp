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
#if defined(__GLIBC__)
#include <malloc.h>
#endif

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <system_error>
#include <vector>

#include "common/memory_budget.hpp"
#include "common/query_grant.hpp"
#include "common/random_string.hpp"
#include "common/set_operation.hpp"
#include "common/spill/spill_store.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/pipeline/ooc_sinks.hpp"
#include "executor/detail/pipeline/pipeline_ir.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/column_value.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

class VectorSource final : public PipelineSource {
 public:
  VectorSource(std::vector<ValueType> types, std::vector<Row> rows)
      : types_(std::move(types)), rows_(std::move(rows)) {}

  Status Open(PipelineContext*) override {
    idx_ = 0;
    return Status::kSuccess;
  }

  StatusOr<bool> NextBatch(PipelineContext*, DataChunk* out,
                           size_t max_rows) override {
    if (idx_ >= rows_.size()) {
      return false;
    }
    const size_t take = std::min(max_rows, rows_.size() - idx_);
    out->Initialize(types_, take);
    for (size_t i = 0; i < take; ++i) {
      out->Append(rows_[idx_ + i]);
    }
    idx_ += take;
    return true;
  }

  [[nodiscard]] std::string Describe() const override { return "VectorSource"; }

 private:
  std::vector<ValueType> types_;
  std::vector<Row> rows_;
  size_t idx_{0};
};

std::string CanonicalRow(const Row& row) {
  std::ostringstream oss;
  for (size_t i = 0; i < row.values_.size(); ++i) {
    if (i > 0) {
      oss << '\x1f';
    }
    const Value& v = row[i];
    if (v.IsNull()) {
      oss << "NULL";
    } else if (v.type == ValueType::kDouble) {
      if (std::isnan(v.value.double_value)) {
        oss << "NaN";
      } else {
        double d = v.value.double_value == 0.0 ? 0.0 : v.value.double_value;
        oss << d;
      }
    } else {
      if (static_cast<int>(v.type) > static_cast<int>(ValueType::kArray)) {
        std::fprintf(stderr,
                     "BAD VALUE TYPE col=%zu/%zu type=%d int=%lld\n", i,
                     row.values_.size(), static_cast<int>(v.type),
                     static_cast<long long>(v.value.int_value));
        assert(false);
      }
      oss << static_cast<int>(v.type) << ':' << v.AsString();
    }
  }
  return oss.str();
}

std::vector<std::string> DrainSource(PipelineContext* ctx,
                                     const std::shared_ptr<PipelineSource>& src,
                                     bool sort_rows) {
  assert(src->Open(ctx) == Status::kSuccess);
  std::vector<std::string> out;
  DataChunk chunk;
  while (true) {
    auto res = src->NextBatch(ctx, &chunk, 64);
    assert(res.HasValue());
    if (!res.Value()) {
      break;
    }
    for (size_t r = 0; r < chunk.Size(); ++r) {
      out.push_back(CanonicalRow(chunk.RowAt(r)));
    }
  }
  if (sort_rows) {
    std::sort(out.begin(), out.end());
  }
  return out;
}

std::unique_ptr<PipelineContext> MakeCtx(const std::filesystem::path& spill_dir,
                                         size_t grant_bytes) {
  auto grant_res =
      QueryMemoryCoordinator::Global().AcquireGrant(grant_bytes, 0);
  assert(grant_res.HasValue());
  SpillStoreOptions opts;
  opts.temp_dir = spill_dir;
  opts.temp_dir_set = true;
  return std::make_unique<PipelineContext>(nullptr, grant_res.MoveValue(), 1,
                                           opts);
}

std::vector<Row> GenRows(FuzzedDataProvider& fdp, int count, int key_domain,
                         int payload_len) {
  std::vector<Row> rows;
  rows.reserve(static_cast<size_t>(count));
  for (int i = 0; i < count; ++i) {
    Value k = fdp.ConsumeIntegralInRange<int>(0, 7) == 0
                  ? Value()
                  : Value(fdp.ConsumeIntegralInRange<int64_t>(0, key_domain));
    Value v = fdp.ConsumeIntegralInRange<int>(0, 7) == 0
                  ? Value()
                  : Value(fdp.ConsumeIntegralInRange<int64_t>(-50, 50));
    Value s;
    if (fdp.ConsumeIntegralInRange<int>(0, 7) != 0) {
      std::string pad(static_cast<size_t>(payload_len),
                      static_cast<char>('a' + (i % 26)));
      pad += std::to_string(fdp.ConsumeIntegralInRange<int>(0, key_domain));
      s = Value(std::move(pad));
    }
    rows.emplace_back(std::vector<Value>{k, v, s});
  }
  return rows;
}

void FeedChunks(PipelineContext* ctx, PipelineSink* sink,
                const std::vector<ValueType>& types,
                const std::vector<Row>& rows, size_t batch_size) {
  for (size_t i = 0; i < rows.size(); i += batch_size) {
    const size_t n = std::min(batch_size, rows.size() - i);
    DataChunk chunk;
    chunk.Initialize(types, n);
    for (size_t j = 0; j < n; ++j) {
      chunk.Append(rows[i + j]);
    }
    assert(sink->Consume(ctx, 0, chunk) == Status::kSuccess);
  }
}

}  // namespace
}  // namespace tinylamb

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  using namespace tinylamb;
  if (size < 8) {
    return 0;
  }
  FuzzedDataProvider fdp(data, size);

  const std::filesystem::path dir =
      std::filesystem::temp_directory_path() /
      ("ooc_sinks_fuzz_" + RandomString(10));
  std::error_code ec;
  std::filesystem::create_directories(dir, ec);

  const Schema schema("t", {Column("k", ValueType::kInt64),
                            Column("v", ValueType::kInt64),
                            Column("s", ValueType::kVarChar)});
  const std::vector<ValueType> types = {ValueType::kInt64, ValueType::kInt64,
                                        ValueType::kVarChar};

  const int op = fdp.ConsumeIntegralInRange<int>(0, 4);
  const int nrows = fdp.ConsumeIntegralInRange<int>(1, 300);
  const int key_domain = fdp.ConsumeIntegralInRange<int>(1, 20);
  const int payload_len = fdp.ConsumeIntegralInRange<int>(8, 180);
  const size_t tight_budget =
      fdp.ConsumeIntegralInRange<size_t>(16 * 1024, 64 * 1024);
  const size_t batch_size = fdp.ConsumeIntegralInRange<size_t>(1, 32);

  std::vector<Row> rows = GenRows(fdp, nrows, key_domain, payload_len);

  const size_t query_used_before = ProcessMemoryBudget::Global().QueryUsed();
#if defined(__GLIBC__)
  const size_t heap_before = mallinfo2().uordblks;
#endif

  if (op == 0) {
    // OocSortSink: compare generous in-memory vs tight-budget spilled!
    const bool asc0 = fdp.ConsumeBool();
    const bool asc1 = fdp.ConsumeBool();
    const bool has_limit = fdp.ConsumeBool();
    const std::optional<size_t> limit =
        has_limit ? std::optional<size_t>(fdp.ConsumeIntegralInRange<size_t>(0, 40))
                  : std::nullopt;
    const size_t offset =
        has_limit ? fdp.ConsumeIntegralInRange<size_t>(0, 15) : 0;

    auto run_sort = [&](size_t grant_bytes) {
      auto ctx = MakeCtx(dir, grant_bytes);
      std::vector<OocSortSink::KeySpec> keys;
      OocSortSink::KeySpec k0;
      k0.expression = ColumnValueExp("k");
      k0.ascending = asc0;
      k0.column_slot = 0;
      OocSortSink::KeySpec k1;
      k1.expression = ColumnValueExp("s");
      k1.ascending = asc1;
      k1.column_slot = 2;
      OocSortSink::KeySpec k2;
      k2.expression = ColumnValueExp("v");
      k2.ascending = true;
      k2.column_slot = 1;
      keys.push_back(k0);
      keys.push_back(k1);
      keys.push_back(k2);

      OocSortSink sink(keys, schema, limit, offset);
      assert(sink.Init(ctx.get(), 1) == Status::kSuccess);
      FeedChunks(ctx.get(), &sink, types, rows, batch_size);
      auto src = sink.Finalize(ctx.get());
      assert(src.HasValue());
      auto out = DrainSource(ctx.get(), src.Value(), /*sort_rows=*/false);
      src.Value().reset();
      assert(ctx->spill_store.Stats().current_active_bytes == 0);
      return out;
    };

    auto expected = run_sort(64ULL * 1024 * 1024);
    auto actual = run_sort(tight_budget);
    assert(actual == expected);
  } else if (op == 1) {
    // OocDistinctSink: full-row or key-col DISTINCT
    const bool full_row = fdp.ConsumeBool();
    std::vector<slot_t> key_cols = full_row ? std::vector<slot_t>{}
                                            : std::vector<slot_t>{0, 1};
    auto run_distinct = [&](size_t grant_bytes) {
      auto ctx = MakeCtx(dir, grant_bytes);
      OocDistinctSink sink(schema, key_cols);
      assert(sink.Init(ctx.get(), 1) == Status::kSuccess);
      FeedChunks(ctx.get(), &sink, types, rows, batch_size);
      auto src = sink.Finalize(ctx.get());
      assert(src.HasValue());
      auto out = DrainSource(ctx.get(), src.Value(), /*sort_rows=*/true);
      src.Value().reset();
      assert(ctx->spill_store.Stats().current_active_bytes == 0);
      return out;
    };
    auto expected = run_distinct(64ULL * 1024 * 1024);
    auto actual = run_distinct(tight_budget);
    assert(actual == expected);
  } else if (op == 2) {
    // OocGroupAggSink: GROUP BY k, COUNT(*), SUM(v), MIN(v), MAX(v), COUNT(DISTINCT v)
    const bool with_distinct = fdp.ConsumeBool();
    auto run_agg = [&](size_t grant_bytes) {
      auto ctx = MakeCtx(dir, grant_bytes);
      std::vector<Expression> group_keys = {ColumnValueExp("k")};
      std::vector<Expression> aggs = {
          AggregateExpressionExp(AggregationType::kCount, Expression()),
          AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("v")),
          AggregateExpressionExp(AggregationType::kMin, ColumnValueExp("v")),
          AggregateExpressionExp(AggregationType::kMax, ColumnValueExp("v")),
      };
      std::vector<Column> out_cols = {
          Column("k", ValueType::kInt64),
          Column("c", ValueType::kInt64),
          Column("s", ValueType::kInt64),
          Column("mn", ValueType::kInt64),
          Column("mx", ValueType::kInt64),
      };
      if (with_distinct) {
        aggs.push_back(AggregateExpressionExp(AggregationType::kCount,
                                              ColumnValueExp("v"),
                                              /*distinct=*/true));
        out_cols.emplace_back("cd", ValueType::kInt64);
      }
      Schema out_schema("agg", std::move(out_cols));
      OocGroupAggSink sink(schema, std::move(group_keys), std::move(aggs),
                           out_schema);
      assert(sink.Init(ctx.get(), 1) == Status::kSuccess);
      FeedChunks(ctx.get(), &sink, types, rows, batch_size);
      auto src = sink.Finalize(ctx.get());
      assert(src.HasValue());
      auto out = DrainSource(ctx.get(), src.Value(), /*sort_rows=*/true);
      src.Value().reset();
      assert(ctx->spill_store.Stats().current_active_bytes == 0);
      return out;
    };
    auto expected = run_agg(64ULL * 1024 * 1024);
    auto actual = run_agg(tight_budget);
    assert(actual == expected);
  } else if (op == 3) {
    // OocHashBuildSink + OocAdaptiveHashProbeSource across JoinKinds
    const int nright = fdp.ConsumeIntegralInRange<int>(1, 200);
    std::vector<Row> right_rows = GenRows(fdp, nright, key_domain, payload_len);
    const JoinKind kinds[] = {JoinKind::kInner,      JoinKind::kLeftOuter,
                              JoinKind::kRightOuter, JoinKind::kFullOuter,
                              JoinKind::kSemi,       JoinKind::kAnti};
    const JoinKind kind = kinds[fdp.ConsumeIntegralInRange<size_t>(0, 5)];
    const bool null_safe = fdp.ConsumeBool();
    const bool build_left = fdp.ConsumeBool();

    auto run_join = [&](size_t grant_bytes) {
      auto ctx = MakeCtx(dir, grant_bytes);
      OocHashJoinSpec spec;
      spec.left_cols = {0};
      spec.right_cols = {0};
      spec.left_schema = schema;
      spec.right_schema = schema;
      spec.kind = kind;
      spec.key_null_safe = {null_safe};
      spec.build_left = build_left;
      auto jctx = std::make_shared<OocHashJoinContext>(std::move(spec));

      OocHashBuildSink build_sink(jctx);
      assert(build_sink.Init(ctx.get(), 1) == Status::kSuccess);
      const auto& build_rows = build_left ? rows : right_rows;
      const auto& probe_rows = build_left ? right_rows : rows;
      FeedChunks(ctx.get(), &build_sink, types, build_rows, batch_size);
      auto build_done = build_sink.Finalize(ctx.get());
      assert(build_done.HasValue());

      auto probe_src = std::make_shared<VectorSource>(types, probe_rows);
      auto join_src = std::make_shared<OocAdaptiveHashProbeSource>(
          jctx, probe_src, std::vector<std::shared_ptr<PipelineOperator>>{});
      auto out = DrainSource(ctx.get(), join_src, /*sort_rows=*/true);
      join_src.reset();
      jctx.reset();
      assert(ctx->spill_store.Stats().current_active_bytes == 0);
      return out;
    };

    auto expected = run_join(64ULL * 1024 * 1024);
    auto actual = run_join(tight_budget);
    assert(actual == expected);
  } else {
    // OocSetOpCoordinator: UNION, INTERSECT [ALL], EXCEPT [ALL]
    const int nright = fdp.ConsumeIntegralInRange<int>(1, 200);
    std::vector<Row> right_rows = GenRows(fdp, nright, key_domain, payload_len);
    const SetOperationKind kinds[] = {
        SetOperationKind::kUnion, SetOperationKind::kIntersect,
        SetOperationKind::kIntersectAll, SetOperationKind::kExcept,
        SetOperationKind::kExceptAll};
    const SetOperationKind kind =
        kinds[fdp.ConsumeIntegralInRange<size_t>(0, 4)];

    auto run_setop = [&](size_t grant_bytes) {
      auto ctx = MakeCtx(dir, grant_bytes);
      auto coord = std::make_shared<OocSetOpCoordinator>(
          kind, schema, std::vector<Schema>{schema, schema});
      OocSetOpInputSink s0(coord, 0, /*is_final=*/false, nullptr);
      OocSetOpInputSink s1(coord, 1, /*is_final=*/true, nullptr);
      assert(s0.Init(ctx.get(), 1) == Status::kSuccess);
      FeedChunks(ctx.get(), &s0, types, rows, batch_size);
      assert(s0.Finalize(ctx.get()).HasValue());
      assert(s1.Init(ctx.get(), 1) == Status::kSuccess);
      FeedChunks(ctx.get(), &s1, types, right_rows, batch_size);
      auto src = s1.Finalize(ctx.get());
      assert(src.HasValue());
      auto out = DrainSource(ctx.get(), src.Value(), /*sort_rows=*/true);
      src.Value().reset();
      coord.reset();
      assert(ctx->spill_store.Stats().current_active_bytes == 0);
      return out;
    };

    auto expected = run_setop(64ULL * 1024 * 1024);
    auto actual = run_setop(tight_budget);
    assert(actual == expected);
  }

  assert(ProcessMemoryBudget::Global().QueryUsed() == query_used_before);

#if defined(__GLIBC__)
  const size_t heap_after = mallinfo2().uordblks;
  if (heap_after > heap_before) {
    // Real heap delta after full cleanup must not leak megabytes.
    assert(heap_after - heap_before < 32ULL * 1024 * 1024);
  }
#endif

  std::filesystem::remove_all(dir, ec);
  return 0;
}
