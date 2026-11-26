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
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

#include "common/random_string.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/window_eval.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/window_function_expression.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
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
    const double diff = std::fabs(a.value.double_value - b.value.double_value);
    return diff <= 1e-9 * std::max({1.0, std::fabs(a.value.double_value),
                                    std::fabs(b.value.double_value)});
  }
  return a == b;
}

Expression MakeWindowCall(FuzzedDataProvider& fdp) {
  const char* kCols[] = {"p", "o", "x", "d", "s"};
  const char* kFuncs[] = {
      "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "LAG",  "LEAD",
      "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE", "COUNT", "SUM", "AVG",
      "MIN", "MAX", "PERCENT_RANK", "CUME_DIST"};

  const std::string fn = kFuncs[fdp.ConsumeIntegralInRange<size_t>(0, 15)];
  std::vector<Expression> args;
  if (fn == "ROW_NUMBER" || fn == "RANK" || fn == "DENSE_RANK" ||
      fn == "PERCENT_RANK" || fn == "CUME_DIST") {
    // 0 args
  } else if (fn == "NTILE") {
    args.push_back(
        ConstantValueExp(Value(fdp.ConsumeIntegralInRange<int64_t>(1, 5))));
  } else if (fn == "NTH_VALUE") {
    args.push_back(
        ColumnValueExp(kCols[fdp.ConsumeIntegralInRange<size_t>(0, 4)]));
    args.push_back(
        ConstantValueExp(Value(fdp.ConsumeIntegralInRange<int64_t>(1, 4))));
  } else if (fn == "LAG" || fn == "LEAD") {
    args.push_back(
        ColumnValueExp(kCols[fdp.ConsumeIntegralInRange<size_t>(0, 4)]));
    if (fdp.ConsumeBool()) {
      args.push_back(
          ConstantValueExp(Value(fdp.ConsumeIntegralInRange<int64_t>(0, 3))));
    }
  } else if (fn == "SUM" || fn == "AVG") {
    args.push_back(ColumnValueExp(fdp.ConsumeBool() ? "x" : "d"));
  } else if (fn == "COUNT") {
    if (fdp.ConsumeBool()) {
      args.push_back(
          ColumnValueExp(kCols[fdp.ConsumeIntegralInRange<size_t>(0, 4)]));
    }
  } else {
    args.push_back(
        ColumnValueExp(kCols[fdp.ConsumeIntegralInRange<size_t>(0, 4)]));
  }

  std::vector<Expression> part_by;
  const int npart = fdp.ConsumeIntegralInRange<int>(0, 2);
  for (int i = 0; i < npart; ++i) {
    part_by.push_back(
        ColumnValueExp(kCols[fdp.ConsumeIntegralInRange<size_t>(0, 4)]));
  }

  std::vector<WindowOrderTerm> order_by;
  const int norder = fdp.ConsumeIntegralInRange<int>(0, 2);
  for (int i = 0; i < norder; ++i) {
    WindowOrderTerm term;
    term.expression =
        ColumnValueExp(kCols[fdp.ConsumeIntegralInRange<size_t>(0, 4)]);
    term.ascending = fdp.ConsumeBool();
    if (fdp.ConsumeBool()) {
      term.nulls_first = fdp.ConsumeBool();
    }
    order_by.push_back(std::move(term));
  }

  Expression expr = WindowFunctionCallExp(fn, std::move(args),
                                          std::move(part_by),
                                          std::move(order_by));
  auto* w = dynamic_cast<WindowFunctionCallExpression*>(expr.get());
  assert(w != nullptr);

  if (fdp.ConsumeBool() && !w->order_by.empty()) {
    w->has_frame = true;
    const WindowFrameUnit units[] = {WindowFrameUnit::kRows,
                                     WindowFrameUnit::kRange,
                                     WindowFrameUnit::kGroups};
    w->frame_unit = units[fdp.ConsumeIntegralInRange<size_t>(0, 2)];
    const int s_kind = fdp.ConsumeIntegralInRange<int>(0, 2);
    if (s_kind == 0) {
      w->frame_start.type = WindowFrameBoundType::kUnboundedPreceding;
    } else if (s_kind == 1) {
      w->frame_start.type = WindowFrameBoundType::kCurrentRow;
    } else {
      w->frame_start.type = WindowFrameBoundType::kOffsetPreceding;
      w->frame_start.offset =
          ConstantValueExp(Value(fdp.ConsumeIntegralInRange<int64_t>(0, 3)));
    }
    const int e_kind = fdp.ConsumeIntegralInRange<int>(0, 2);
    if (e_kind == 0) {
      w->frame_end.type = WindowFrameBoundType::kCurrentRow;
    } else if (e_kind == 1) {
      w->frame_end.type = WindowFrameBoundType::kUnboundedFollowing;
    } else {
      w->frame_end.type = WindowFrameBoundType::kOffsetFollowing;
      w->frame_end.offset =
          ConstantValueExp(Value(fdp.ConsumeIntegralInRange<int64_t>(0, 3)));
    }
    // RANGE with offset requires single numeric ORDER BY; fall back to ROWS
    // otherwise so we test valid SQL frame specs.
    if (w->frame_unit == WindowFrameUnit::kRange &&
        (w->frame_start.type == WindowFrameBoundType::kOffsetPreceding ||
         w->frame_end.type == WindowFrameBoundType::kOffsetFollowing)) {
      w->frame_unit = WindowFrameUnit::kRows;
    }
  }
  return expr;
}

}  // namespace
}  // namespace tinylamb

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  using namespace tinylamb;
  using namespace tinylamb::relational_detail;
  if (size < 8) {
    return 0;
  }
  FuzzedDataProvider fdp(data, size);

  const std::filesystem::path dir =
      std::filesystem::temp_directory_path() /
      ("win_eval_fuzz_" + RandomString(10));
  std::error_code ec;
  std::filesystem::create_directories(dir, ec);
  auto db_res = Database::Create((dir / "db").string(), /*wal_sync_ms=*/1);
  if (!db_res.HasValue()) {
    std::filesystem::remove_all(dir, ec);
    return 0;
  }
  auto db = db_res.MoveValue();
  TransactionContext ctx = db->BeginReadOnlyContext();

  const Schema schema("t", {Column("p", ValueType::kInt64),
                            Column("o", ValueType::kInt64),
                            Column("x", ValueType::kInt64),
                            Column("d", ValueType::kDouble),
                            Column("s", ValueType::kVarChar)});

  const int nrows = fdp.ConsumeIntegralInRange<int>(1, 48);
  std::vector<Row> rows;
  rows.reserve(static_cast<size_t>(nrows));
  for (int i = 0; i < nrows && fdp.remaining_bytes() > 0; ++i) {
    Value vp = fdp.ConsumeIntegralInRange<int>(0, 5) == 0
                   ? Value()
                   : Value(fdp.ConsumeIntegralInRange<int64_t>(0, 4));
    Value vo = fdp.ConsumeIntegralInRange<int>(0, 5) == 0
                   ? Value()
                   : Value(fdp.ConsumeIntegralInRange<int64_t>(-3, 5));
    Value vx = fdp.ConsumeIntegralInRange<int>(0, 5) == 0
                   ? Value()
                   : Value(fdp.ConsumeIntegralInRange<int64_t>(-20, 20));
    Value vd;
    if (fdp.ConsumeIntegralInRange<int>(0, 5) != 0) {
      const int dk = fdp.ConsumeIntegralInRange<int>(0, 4);
      if (dk == 0) {
        vd = Value(std::numeric_limits<double>::quiet_NaN());
      } else if (dk == 1) {
        vd = Value(-0.0);
      } else {
        vd = Value(
            static_cast<double>(fdp.ConsumeIntegralInRange<int>(-10, 10)) *
            0.5);
      }
    }
    Value vs = fdp.ConsumeIntegralInRange<int>(0, 5) == 0
                   ? Value()
                   : Value(std::string("s") +
                           std::to_string(fdp.ConsumeIntegralInRange<int>(0, 4)));
    rows.emplace_back(std::vector<Value>{vp, vo, vx, vd, vs});
  }
  if (rows.empty()) {
    std::ignore = ctx.PreCommit();
    db.reset();
    std::filesystem::remove_all(dir, ec);
    return 0;
  }

  const int nwins = fdp.ConsumeIntegralInRange<int>(1, 3);
  std::vector<Expression> win_owners;
  std::vector<const WindowFunctionCallExpression*> win_ptrs;
  std::vector<std::string> win_names;
  for (int w = 0; w < nwins; ++w) {
    win_owners.push_back(MakeWindowCall(fdp));
    win_ptrs.push_back(
        dynamic_cast<const WindowFunctionCallExpression*>(win_owners.back().get()));
    win_names.push_back("$win" + std::to_string(w));
  }

  CteMap empty_ctes;

  // 1. Row-based reference evaluation
  std::vector<Row> row_copy = rows;
  std::vector<std::vector<Value>> row_outputs;
  Status row_st = ComputeWindowColumns(ctx, win_ptrs, row_copy, schema, nullptr,
                                       empty_ctes, &row_outputs);

  // 2. Columnar evaluation
  Relation col_rel;
  col_rel.schema = schema;
  auto col_tbl = BuildColumnarTableFromRows(schema, rows);
  assert(col_tbl.HasValue());
  col_rel.SetColumnarTable(col_tbl.MoveValue(), /*uniform_types=*/true);
  auto col_res = ExtendColumnarRelationWithWindows(
      ctx, std::move(col_rel), win_ptrs, win_names, nullptr, empty_ctes);

  if (row_st == Status::kSuccess) {
    assert(col_res.HasValue());
    Relation out_rel = col_res.MoveValue();
    assert(out_rel.EnsureRowsMaterialized() == Status::kSuccess);
    assert(out_rel.rows.size() == rows.size());
    for (size_t r = 0; r < rows.size(); ++r) {
      for (size_t w = 0; w < win_ptrs.size(); ++w) {
        const Value& want = row_outputs[w][r];
        const Value& got = out_rel.rows[r][schema.ColumnCount() + w];
        assert(SameValue(want, got));
      }
    }
  } else {
    // If row evaluation returned an error, columnar must also fail cleanly
    // (never crash or silently succeed with garbage).
    assert(!col_res.HasValue());
  }

  std::ignore = ctx.PreCommit();
  db.reset();
  std::filesystem::remove_all(dir, ec);
  return 0;
}
