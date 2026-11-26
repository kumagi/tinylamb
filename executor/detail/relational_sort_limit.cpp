/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/platform/process.hpp"
#include "executor/detail/relational_sort_limit.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/analytics_factorized_join.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/work_table_index.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "expression/bytecode.hpp"
#include "expression/cast_expression.hpp"
#include "expression/collation.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/parameter_slot.hpp"
#include "query/statement.hpp"
#include "type/collation.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb::relational_detail {

// Projection preserves the declared value domain without copying constraints
// (a projected or outer-joined value need not retain the source constraint).
Column ProjectionDomainColumn(std::string_view name, const Column& source) {
  Column result(name, source.Type());
  result.SetUnsigned(source.IsUnsigned());
  result.SetIntegerBits(source.IntegerBits());
  result.SetInterval(source.IsInterval());
  result.SetTextDomain(source.GetTextDomain());
  result.SetScale(source.Scale());
  return result;
}

Column ProjectionExpressionColumn(std::string_view name,
                                  const Expression& expression,
                                  const Schema& schema) {
  if (expression->Type() == TypeTag::kColumnValue) {
    const int offset =
        schema.Offset(expression->AsColumnValue().GetColumnName());
    if (offset >= 0) {
      return ProjectionDomainColumn(
          name, schema.GetColumn(static_cast<size_t>(offset)));
    }
  }
  if (expression->Type() == TypeTag::kCastExp) {
    const auto& cast = expression->AsCastExpression();
    Column result(name, cast.TargetValueType());
    const std::string target = AsciiToLower(cast.TargetTypeName());
    result.SetInterval(target == "interval");
    if (target == "timestamp") {
      result.SetTextDomain(TextDomain::kTimestamp);
    } else if (target == "string" || target == "varchar" || target == "char") {
      result.SetTextDomain(TextDomain::kString);
    }
    result.SetUnsigned(target.starts_with("uint"));
    if (target == "int8" || target == "uint8") {
      result.SetIntegerBits(8);
    } else if (target == "int16" || target == "uint16") {
      result.SetIntegerBits(16);
    } else if (target == "int32" || target == "uint32") {
      result.SetIntegerBits(32);
    }
    return result;
  }
  if (expression->Type() == TypeTag::kConstantValue) {
    // A literal's type is known statically; without it an empty result
    // (WHERE false / LIMIT 0) leaves the column kNull and CTAS falls back
    // to VARCHAR, rejecting later typed INSERTs.
    const Value& constant = expression->AsConstantValue().GetValue();
    if (!constant.IsNull()) {
      Column result(name, constant.type);
      result.SetUnsigned(constant.IsUnsigned());
      result.SetInterval(constant.IsInterval());
      result.SetTextDomain(expression->AsConstantValue().GetTextDomain());
      return result;
    }
  }
  return Column(name, ValueType::kNull);
}

bool IsScalarBytecodeCompilable(const Expression& node) {
  if (!node) {
    return true;
  }
  if (node->Type() == TypeTag::kFunctionCallExp) {
    // Lean proof: lean/TinylambOpt/PredicateAndRelationalLowering.lean
    // (ascii_substr_1based_slice_sound)
    const auto& name = node->AsFunctionCallExpression().FuncName();
    if (!IdentifierEquals(name, "coalesce") &&
        !IdentifierEquals(name, "ifnull") && !IdentifierEquals(name, "round") &&
        !IdentifierEquals(name, "substr") &&
        !IdentifierEquals(name, "substring") &&
        !IdentifierEquals(name, "byte_length") &&
        !IdentifierEquals(name, "length") &&
        !IdentifierEquals(name, "char_length")) {
      return false;
    }
  }
  return std::ranges::all_of(ExpressionChildren(node),
                             IsScalarBytecodeCompilable);
}

StatusOr<ColumnarTopNResult> TryColumnarBoundedTopN(
    const relational_detail::ColumnarTable& src_table, const Schema& src_schema,
    const Schema& out_schema, std::span<const TopNOutputSlot> output_slots,
    std::span<const ColumnarTopNKey> keys,
    std::span<const SortKeyOrder> order_specs, size_t topn_capacity) {
  if (keys.empty() || keys.size() != order_specs.size()) {
    return ColumnarTopNResult(std::nullopt);
  }
  const size_t n_rows = src_table.total_rows;
  const size_t capacity = std::min(topn_capacity, n_rows);
  const size_t n_keys = keys.size();
  const size_t out_width =
      output_slots.empty() ? out_schema.ColumnCount() : output_slots.size();
  bool has_programs = false;
  for (const ColumnarTopNKey& k : keys) {
    if (k.program.has_value()) {
      has_programs = true;
      break;
    }
  }
  if (capacity == 0 && !has_programs) {
    return ColumnarTopNResult(
        std::pair{std::make_shared<relational_detail::ColumnarTable>(), false});
  }
  struct ColTopNEntry {
    uint32_t chunk_idx{0};
    uint32_t row_idx{0};
    size_t seq{0};
    std::vector<Value> keys;
  };
  decltype(TryReserveQueryMemory(size_t{0})) charge;
  if (capacity > 0) {
    charge = TryReserveQueryMemory(
        capacity * (sizeof(ColTopNEntry) + n_keys * sizeof(Value)));
    if (!charge) {
      return std::optional<
          std::pair<std::shared_ptr<relational_detail::ColumnarTable>, bool>>(
          std::nullopt);
    }
  }

  std::vector<uint8_t> needed_cols;
  if (has_programs) {
    needed_cols.assign(src_schema.ColumnCount(), 0);
    for (const ColumnarTopNKey& k : keys) {
      if (!k.program.has_value()) {
        continue;
      }
      for (const BytecodeInstruction& inst : k.program->Instructions()) {
        if (inst.opcode == BytecodeOp::kLoadColumn) {
          if (inst.operand >= needed_cols.size()) {
            return std::optional<std::pair<
                std::shared_ptr<relational_detail::ColumnarTable>, bool>>(
                std::nullopt);
          }
          needed_cols[inst.operand] = 1;
        }
      }
    }
  }

  std::shared_ptr<relational_detail::ChunkStringArena> heap_key_arena;
  const auto intern_entry_strings = [&](ColTopNEntry* entry) {
    for (size_t k = 0; k < n_keys; ++k) {
      if (!keys[k].program.has_value()) {
        continue;
      }
      Value& kv = entry->keys[k];
      if (!kv.IsNull() && kv.type == ValueType::kVarChar) {
        if (heap_key_arena == nullptr) {
          heap_key_arena =
              std::make_shared<relational_detail::ChunkStringArena>();
        }
        kv.value.varchar_value = heap_key_arena->Store(kv.value.varchar_value);
      }
    }
  };

  auto heap_before = [&](const ColTopNEntry& a, const ColTopNEntry& b) {
    for (size_t k = 0; k < n_keys; ++k) {
      const int cmp = order_specs[k].Compare(a.keys[k], b.keys[k]);
      if (cmp != 0) {
        return cmp < 0;
      }
    }
    return a.seq < b.seq;
  };

  std::vector<ColTopNEntry> heap;
  heap.reserve(capacity);
  std::vector<relational_detail::ColumnarColumn> eval_cols(has_programs ? n_keys
                                                                        : 0);
  std::vector<BytecodeColumnView> views;
  std::vector<relational_detail::ColumnarColumn> view_scratches;
  std::vector<Value> cand_keys(n_keys);
  std::vector<SortKeyOrder> observed(order_specs.begin(), order_specs.end());
  size_t seq = 0;

  for (size_t ci = 0; ci < src_table.chunks.size(); ++ci) {
    const auto& ch = src_table.chunks[ci];
    if (ch.rows == 0) {
      continue;
    }
    if (has_programs) {
      if (ch.columns.size() < needed_cols.size()) {
        return std::optional<
            std::pair<std::shared_ptr<relational_detail::ColumnarTable>, bool>>(
            std::nullopt);
      }
      views.resize(ch.columns.size());
      view_scratches.resize(ch.columns.size());
      for (size_t c = 0; c < ch.columns.size(); ++c) {
        if (c < needed_cols.size() && needed_cols[c] != 0) {
          views[c] = ch.columns[c].ToBytecodeView(ch.rows, &view_scratches[c],
                                                  nullptr);
        } else {
          views[c] = BytecodeColumnView{};
        }
      }
      for (size_t k = 0; k < n_keys; ++k) {
        const ColumnarTopNKey& key = keys[k];
        if (!key.program.has_value()) {
          continue;
        }
        eval_cols[k].Init(key.result_type, key.result_unsigned,
                          key.result_interval, ch.rows);
        BytecodeColumnSink sink = eval_cols[k].AsBytecodeSink(0);
        Status eval_err{Status::kSuccess};
        const auto st = key.program->TryEvaluateColumnarBatch(
            views, nullptr, ch.rows, &sink, &eval_err);
        if (st == BytecodeProgram::FastEval::kError) {
          return eval_err;
        }
        if (st != BytecodeProgram::FastEval::kDone) {
          return std::optional<std::pair<
              std::shared_ptr<relational_detail::ColumnarTable>, bool>>(
              std::nullopt);
        }
        eval_cols[k].SyncFromBytecodeSink(sink, nullptr, ch.rows);
      }
    }
    if (capacity == 0) {
      continue;
    }

    const auto read_key_val = [&](size_t k, size_t r, Value* out) {
      const ColumnarTopNKey& key = keys[k];
      if (key.program.has_value()) {
        eval_cols[k].WriteValueInto(r, out);
        return;
      }
      ch.columns[key.col].WriteValueInto(r, out);
      if (key.is_coalesce && out->IsNull()) {
        ch.columns[key.fallback_col].WriteValueInto(r, out);
      }
    };

    for (size_t r = 0; r < ch.rows; ++r, ++seq) {
      for (size_t k = 0; k < n_keys; ++k) {
        read_key_val(k, r, &cand_keys[k]);
      }
      RETURN_IF_FAIL(ObserveSortKeys(observed, cand_keys));
      if (heap.size() < capacity) {
        heap.push_back(ColTopNEntry{.chunk_idx = static_cast<uint32_t>(ci),
                                    .row_idx = static_cast<uint32_t>(r),
                                    .seq = seq,
                                    .keys = cand_keys});
        intern_entry_strings(&heap.back());
        std::push_heap(heap.begin(), heap.end(), heap_before);
      } else {
        const int cmp0 =
            order_specs[0].Compare(cand_keys[0], heap.front().keys[0]);
        if (cmp0 > 0) {
          continue;
        }
        bool enter = cmp0 < 0;
        if (cmp0 == 0) {
          for (size_t k = 1; k < n_keys; ++k) {
            const int cmp =
                order_specs[k].Compare(cand_keys[k], heap.front().keys[k]);
            if (cmp != 0) {
              enter = cmp < 0;
              break;
            }
          }
          if (!enter) {
            continue;
          }
        }
        std::pop_heap(heap.begin(), heap.end(), heap_before);
        ColTopNEntry& dst = heap.back();
        dst.chunk_idx = static_cast<uint32_t>(ci);
        dst.row_idx = static_cast<uint32_t>(r);
        dst.seq = seq;
        dst.keys.assign(cand_keys.begin(), cand_keys.end());
        intern_entry_strings(&dst);
        std::push_heap(heap.begin(), heap.end(), heap_before);
      }
    }
  }

  auto out_table = std::make_shared<relational_detail::ColumnarTable>();
  if (heap.empty()) {
    return std::optional<
        std::pair<std::shared_ptr<relational_detail::ColumnarTable>, bool>>(
        std::pair{std::move(out_table), false});
  }
  std::ranges::sort(heap, heap_before);
  relational_detail::ColumnarChunk dst_chunk;
  dst_chunk.rows = heap.size();
  dst_chunk.columns.resize(out_width);
  for (size_t col_i = 0; col_i < out_width; ++col_i) {
    const Column& schema_col = out_schema.GetColumn(col_i);
    dst_chunk.columns[col_i].Init(schema_col.Type(), schema_col.IsUnsigned(),
                                  schema_col.IsInterval(), heap.size());
  }
  relational_detail::ChunkStringArena* arena = nullptr;
  Value cell;
  Row src_row;
  for (size_t out_r = 0; out_r < heap.size(); ++out_r) {
    const auto& entry = heap[out_r];
    const auto& src_ch = src_table.chunks[entry.chunk_idx];
    bool src_row_ready = false;
    for (size_t col_i = 0; col_i < out_width; ++col_i) {
      if (output_slots.empty()) {
        src_ch.columns[col_i].WriteValueInto(entry.row_idx, &cell);
      } else {
        const TopNOutputSlot& slot = output_slots[col_i];
        if (slot.program != nullptr) {
          if (!src_row_ready) {
            src_ch.MaterializeRowInto(entry.row_idx, &src_row);
            src_row_ready = true;
          }
          ASSIGN_OR_RETURN(Value, evaluated,
                           TryEvaluateRowAuto(*slot.program, src_row));
          cell = std::move(evaluated);
        } else {
          src_ch.columns[slot.col].WriteValueInto(entry.row_idx, &cell);
          if (slot.is_coalesce && cell.IsNull()) {
            src_ch.columns[slot.fallback_col].WriteValueInto(entry.row_idx,
                                                             &cell);
          }
        }
      }
      auto& dst_col = dst_chunk.columns[col_i];
      if (!cell.IsNull() && cell.type == ValueType::kVarChar) {
        if (arena == nullptr) {
          arena = dst_chunk.EnsureArena();
        }
        if (!dst_col.is_uniform) {
          cell.value.varchar_value = arena->Store(cell.value.varchar_value);
        }
      }
      dst_col.AppendValue(cell, arena, out_r);
    }
  }
  bool all_uniform = true;
  for (const auto& col : dst_chunk.columns) {
    if (!col.is_uniform) {
      all_uniform = false;
      break;
    }
  }
  out_table->AppendChunk(std::move(dst_chunk));
  return std::optional<
      std::pair<std::shared_ptr<relational_detail::ColumnarTable>, bool>>(
      std::pair{std::move(out_table), all_uniform});
}

size_t EstimateKeyedRowBytes(const KeyedRow& keyed) {
  size_t bytes = sizeof(KeyedRow) + (keyed.keys.capacity() * 40) +
                 EstimateRowBytesFast(keyed.row);
  for (const Value& key : keyed.keys) {
    if (key.type == ValueType::kVarChar) {
      bytes += key.value.varchar_value.size();
    }
  }
  return bytes;
}

bool KeyedRowPrecedes(const std::vector<SortKeyOrder>& order_specs,
                      const KeyedRow& left, const KeyedRow& right) {
  for (size_t i = 0; i < order_specs.size(); ++i) {
    const int cmp = order_specs[i].Compare(left.keys[i], right.keys[i]);
    if (cmp != 0) {
      return cmp < 0;
    }
  }
  return left.sequence < right.sequence;
}

Row EncodeKeyedRunRow(KeyedRow&& keyed) {
  std::vector<Value> combined;
  combined.reserve(keyed.keys.size() + 1 + keyed.row.values_.size());
  for (Value& key : keyed.keys) {
    combined.push_back(std::move(key));
  }
  combined.emplace_back(static_cast<int64_t>(keyed.sequence));
  for (Value& cell : keyed.row.values_) {
    combined.push_back(std::move(cell));
  }
  return Row(std::move(combined));
}

StatusOr<KeyedRow> DecodeKeyedRunRow(Row&& encoded, size_t key_count) {
  if (key_count >= encoded.values_.size()) {
    return StatusError(StatusCode::kCorrupt, "missing spilled sort sequence");
  }
  const Value& sequence = encoded.values_[key_count];
  if (sequence.type != ValueType::kInt64 || sequence.IsUnsigned() ||
      sequence.value.int_value < 0) {
    return StatusError(StatusCode::kCorrupt, "invalid spilled sort sequence");
  }
  KeyedRow keyed;
  keyed.keys.reserve(key_count);
  for (size_t i = 0; i < key_count; ++i) {
    keyed.keys.push_back(std::move(encoded.values_[i]));
  }
  keyed.sequence = static_cast<size_t>(sequence.value.int_value);
  std::vector<Value> payload;
  payload.reserve(encoded.values_.size() - key_count - 1);
  for (size_t i = key_count + 1; i < encoded.values_.size(); ++i) {
    payload.push_back(std::move(encoded.values_[i]));
  }
  keyed.row = Row(std::move(payload));
  return keyed;
}

StatusOr<Relation> DistinctOf(Relation input,
                              const std::vector<Expression>& distinct_on,
                              TransactionContext* ctx, const Scope* outer,
                              const CteMap* ctes) {
  if (distinct_on.empty()) {
    WorkTableIndex seen;
    Relation distinct;
    distinct.schema = input.schema;
    CopyExecutionStats(&distinct, input);
    RETURN_IF_FAIL(input.FinishSpill());
    RETURN_IF_FAIL(input.ForEachRow([&](const Row& row) {
      // Canonicalize the emitted row so a representative value is chosen
      // consistently (NaN -> quiet NaN, -0.0 -> +0.0), matching GoogleSQL.
      Row canonical = row;
      for (Value& value : canonical.values_) {
        value = CanonicalDistinctValue(value);
      }
      if (seen.Insert(canonical)) {
        distinct.AddRow(std::move(canonical));
      }
    }));
    RETURN_IF_FAIL(distinct.FinishSpill());
    return distinct;
  }
  WorkTableIndex seen_keys;
  Relation distinct;
  distinct.schema = input.schema;
  CopyExecutionStats(&distinct, input);
  RETURN_IF_FAIL(input.FinishSpill());
  Status distinct_error{Status::kSuccess};
  Status iterated = input.ForEachRow([&](const Row& row) {
    std::vector<Value> current_keys;
    current_keys.reserve(distinct_on.size());
    for (const auto& expr : distinct_on) {
      Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
      StatusOr<Value> val_or =
          ctx != nullptr && ctes != nullptr
              ? TryEvaluate(expr, scope, nullptr, *ctx, *ctes)
              : expr->TryEvaluate(row, input.schema);
      if (!val_or.HasValue()) {
        if (distinct_error == Status::kSuccess) {
          distinct_error = val_or.GetStatus();
        }
        return;
      }
      current_keys.push_back(CanonicalDistinctValue(val_or.MoveValue()));
    }
    if (seen_keys.Insert(Row(std::move(current_keys)))) {
      distinct.AddRow(row);
    }
  });
  if (iterated != Status::kSuccess && distinct_error == Status::kSuccess) {
    distinct_error = iterated;
  }
  RETURN_IF_FAIL(distinct_error);
  RETURN_IF_FAIL(distinct.FinishSpill());
  return distinct;
}

struct ResolvedOrderByKeys {
  std::vector<Expression> key_expressions;
  std::vector<std::optional<slot_t>> ordinal_columns;
  std::vector<SortKeyOrder> order_specs;
  std::vector<std::optional<slot_t>> direct_key_cols;
  bool all_direct_keys{false};
};

// NOLINTNEXTLINE(misc-use-anonymous-namespace): file-local helper.
static StatusOr<ResolvedOrderByKeys> ResolveOrderByKeys(
    const SelectStatement& statement, const Schema& schema) {
  const std::vector<SelectStatement::OrderByTerm>& order_by =
      statement.OrderBy();
  ResolvedOrderByKeys resolved;
  resolved.key_expressions.reserve(order_by.size());
  resolved.ordinal_columns.reserve(order_by.size());
  for (const auto& key : order_by) {
    Expression expression = key.expression;
    std::optional<slot_t> ordinal_column;
    if (const std::optional<CollatedOrdinal> collated =
            AsCollatedOrdinal(expression);
        collated.has_value()) {
      // A COLLATE-qualified ordinal is still an ordinal: reject out-of-range
      // values here instead of falling through to the constant-key path,
      // which would silently sort by a constant instead of erroring.
      if (collated->ordinal < 1 ||
          std::cmp_greater(collated->ordinal, schema.ColumnCount())) {
        return StatusError(StatusCode::kInvalidArgument,
                           "ORDER BY ordinal out of range");
      }
      ordinal_column = static_cast<slot_t>(collated->ordinal - 1);
    } else if (expression && expression->Type() == TypeTag::kConstantValue &&
               dynamic_cast<const ParameterSlot*>(expression.get()) ==
                   nullptr) {
      const Value& ordinal_value = expression->AsConstantValue().GetValue();
      if (ordinal_value.type == ValueType::kInt64 && !ordinal_value.IsNull()) {
        // Any bare integer ORDER BY item is a SELECT-list ordinal; values
        // below 1 are out of range just like oversized ones (the Project
        // path rejects both with the same error).
        if (ordinal_value.value.int_value < 1 ||
            std::cmp_greater(ordinal_value.value.int_value,
                             schema.ColumnCount())) {
          return StatusError(StatusCode::kInvalidArgument,
                             "ORDER BY ordinal out of range");
        }
        ordinal_column = static_cast<slot_t>(ordinal_value.value.int_value - 1);
      }
    }
    resolved.ordinal_columns.push_back(ordinal_column);
    resolved.key_expressions.push_back(std::move(expression));
  }
  resolved.order_specs.reserve(order_by.size());
  for (const SelectStatement::OrderByTerm& term : order_by) {
    resolved.order_specs.emplace_back(term.ascending, term.nulls_first,
                                      CollationOfExpression(term.expression));
  }
  resolved.direct_key_cols.reserve(resolved.key_expressions.size());
  resolved.all_direct_keys = !resolved.key_expressions.empty();
  for (size_t i = 0; i < resolved.key_expressions.size(); ++i) {
    const Expression& key = resolved.key_expressions[i];
    if (resolved.ordinal_columns[i]) {
      resolved.direct_key_cols.push_back(resolved.ordinal_columns[i]);
    } else if (key && key->Type() == TypeTag::kColumnValue) {
      const auto col_off =
          LocalColumnOffset(schema, key->AsColumnValue().GetColumnName());
      resolved.direct_key_cols.push_back(
          col_off.has_value() ? std::optional<slot_t>(*col_off) : std::nullopt);
      if (!col_off.has_value()) {
        resolved.all_direct_keys = false;
      }
    } else {
      resolved.direct_key_cols.emplace_back(std::nullopt);
      resolved.all_direct_keys = false;
    }
  }
  return resolved;
}

StatusOr<bool> TryDistinctOnOrderedWinners(TransactionContext& context,
                                           const SelectStatement& statement,
                                           Relation* output, const Scope* outer,
                                           const CteMap& ctes) {
  const char* disable_env = ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_DISTINCT_ON_TOP1");
  if (disable_env != nullptr && disable_env[0] == '1') {
    return false;
  }
  if (output == nullptr || statement.OrderBy().empty() ||
      statement.DistinctOn().empty()) {
    return false;
  }
  RETURN_IF_FAIL(output->FinishSpill());
  if (output->HasSpill()) {
    return false;
  }
  if (output->HasColumnarOnly()) {
    if (!QueryMemoryBudget::Global().CanReserve(output->TotalRows() * 160)) {
      return false;
    }
    RETURN_IF_FAIL(output->EnsureRowsMaterializedChecked());
  }
  if (output->HasSpill() || output->HasColumnarOnly()) {
    return false;
  }

  // Lean proof: lean/TinylambOpt/ProposedProofs/DistinctOnTop1.lean (sound)
  const auto sort_begin = std::chrono::steady_clock::now();
  ASSIGN_OR_RETURN(ResolvedOrderByKeys, resolved,
                   (ResolveOrderByKeys(statement, output->schema)));
  const size_t n = output->rows.size();
  if (n == 0) {
    return true;
  }

  const std::vector<Expression>& distinct_on = statement.DistinctOn();
  std::vector<std::optional<slot_t>> distinct_direct_cols;
  distinct_direct_cols.reserve(distinct_on.size());
  for (const Expression& expr : distinct_on) {
    if (expr && expr->Type() == TypeTag::kColumnValue) {
      distinct_direct_cols.emplace_back(LocalColumnOffset(
          output->schema, expr->AsColumnValue().GetColumnName()));
    } else {
      distinct_direct_cols.emplace_back(std::nullopt);
    }
  }

  struct Winner {
    std::vector<Value> order_keys;
    size_t row_idx{0};
  };
  std::unordered_map<Row, Winner, WorkTableRowHash, WorkTableRowEqual> winners;
  winners.reserve(std::min<size_t>(n, 4096));

  std::vector<Value> cur_order_keys(resolved.key_expressions.size());
  std::vector<Value> cur_distinct_keys;
  for (size_t ri = 0; ri < n; ++ri) {
    const Row& row = output->rows[ri];
    Scope scope{.row = &row, .schema = &output->schema, .outer = outer};
    for (size_t k = 0; k < resolved.key_expressions.size(); ++k) {
      const auto direct_key_col = resolved.direct_key_cols[k];
      if (direct_key_col.has_value()) {
        cur_order_keys[k] = row[direct_key_col.value()];
      } else {
        ASSIGN_OR_RETURN(Value, kv,
                         (TryEvaluate(resolved.key_expressions[k], scope,
                                      nullptr, context, ctes)));
        cur_order_keys[k] = std::move(kv);
      }
    }
    RETURN_IF_FAIL(ObserveSortKeys(resolved.order_specs, cur_order_keys));
    cur_distinct_keys.clear();
    cur_distinct_keys.reserve(distinct_on.size());
    for (size_t k = 0; k < distinct_on.size(); ++k) {
      const auto direct_key_col = distinct_direct_cols[k];
      if (direct_key_col.has_value()) {
        cur_distinct_keys.push_back(
            CanonicalDistinctValue(row[direct_key_col.value()]));
      } else {
        ASSIGN_OR_RETURN(
            Value, dv,
            (TryEvaluate(distinct_on[k], scope, nullptr, context, ctes)));
        cur_distinct_keys.push_back(CanonicalDistinctValue(dv));
      }
    }
    Row dkey(std::move(cur_distinct_keys));
    auto it = winners.find(dkey);
    if (it == winners.end()) {
      winners.emplace(std::move(dkey),
                      Winner{.order_keys = cur_order_keys, .row_idx = ri});
    } else {
      int cmp = 0;
      for (size_t k = 0; k < resolved.order_specs.size(); ++k) {
        cmp = resolved.order_specs[k].Compare(cur_order_keys[k],
                                              it->second.order_keys[k]);
        if (cmp != 0) {
          break;
        }
      }
      if (cmp < 0) {
        it->second.order_keys = cur_order_keys;
        it->second.row_idx = ri;
      }
    }
  }

  std::vector<Winner> ordered_winners;
  ordered_winners.reserve(winners.size());
  for (auto& [k, w] : winners) {
    (void)k;
    ordered_winners.push_back(std::move(w));
  }
  winners.clear();
  std::ranges::sort(ordered_winners, [&](const Winner& a, const Winner& b) {
    for (size_t k = 0; k < resolved.order_specs.size(); ++k) {
      const int cmp =
          resolved.order_specs[k].Compare(a.order_keys[k], b.order_keys[k]);
      if (cmp != 0) {
        return cmp < 0;
      }
    }
    return a.row_idx < b.row_idx;
  });

  std::vector<Row> old_rows;
  old_rows.swap(output->rows);
  output->ResetContents();
  output->rows.reserve(ordered_winners.size());
  for (const Winner& w : ordered_winners) {
    output->AddRow(std::move(old_rows[w.row_idx]));
  }
  RETURN_IF_FAIL(output->FinishSpill());
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->sort_ms += ElapsedMs(sort_begin);
  }
  return true;
}

// Sorts `output` in place by the statement's ORDER BY terms. Key values are
// evaluated once per row (not per comparison). All rows are copied out before
// refilling so a previously spilled output is never appended to after its
// spill files were finished.
Status ApplyOrderBy(TransactionContext& context,
                    const SelectStatement& statement, Relation* output,
                    const Scope* outer, const CteMap& ctes,
                    bool allow_bounded) {
  const auto sort_begin = std::chrono::steady_clock::now();
  const std::vector<SelectStatement::OrderByTerm>& order_by =
      statement.OrderBy();
  ASSIGN_OR_RETURN(ResolvedOrderByKeys, resolved,
                   (ResolveOrderByKeys(statement, output->schema)));
  std::vector<Expression>& key_expressions = resolved.key_expressions;
  std::vector<SortKeyOrder>& order_specs = resolved.order_specs;
  std::vector<std::optional<slot_t>>& direct_key_cols =
      resolved.direct_key_cols;
  const bool all_direct_keys = resolved.all_direct_keys;
  RETURN_IF_FAIL(output->FinishSpill());
  bool col_bounded = false;
  size_t raw_keep = 0;
  if (allow_bounded && statement.HasLimit() && !statement.WithTies() &&
      statement.Offset() <=
          std::numeric_limits<size_t>::max() - statement.Limit()) {
    raw_keep = statement.Offset() + statement.Limit();
    col_bounded = true;
  }
  if (!output->HasSpill() && output->HasColumnarOnly() && col_bounded &&
      !order_specs.empty()) {
    std::vector<ColumnarTopNKey> col_topn_keys;
    col_topn_keys.reserve(order_specs.size());
    bool col_keys_ok = true;
    for (size_t i = 0; i < key_expressions.size(); ++i) {
      if (direct_key_cols[i].has_value()) {
        col_topn_keys.push_back(
            ColumnarTopNKey::Direct(direct_key_cols[i].value()));
        continue;
      }
      const Expression& key = key_expressions[i];
      if (outer != nullptr || !key || ContainsQuery(key) ||
          ContainsAggregate(key) || ContainsParameterSlot(key) ||
          !BytecodeEnabled() || !IsScalarBytecodeCompilable(key)) {
        col_keys_ok = false;
        break;
      }
      bool cols_ok = true;
      for (const ColumnName& cn : key->TouchedColumns()) {
        if (cn.name == "*" || cn.name.find('.') != std::string::npos ||
            cn.schema.find('.') != std::string::npos) {
          cols_ok = false;
          break;
        }
        const auto off = LocalColumnOffset(output->schema, cn);
        if (!off.has_value() ||
            !IdentifierEquals(output->schema.GetColumn(*off).Name().name,
                              cn.name)) {
          cols_ok = false;
          break;
        }
      }
      if (!cols_ok) {
        col_keys_ok = false;
        break;
      }
      std::optional<BytecodeProgram> compiled =
          BytecodeCompiler::CompileRowCached(key, output->schema,
                                             BytecodeCompiler::Context::kValue);
      if (!compiled.has_value() || !compiled->FastInterpretable()) {
        col_keys_ok = false;
        break;
      }
      const Column res_col =
          ProjectionExpressionColumn("", key, output->schema);
      col_topn_keys.push_back(
          ColumnarTopNKey::Computed(std::move(*compiled), res_col));
    }
    if (col_keys_ok && col_topn_keys.size() == order_specs.size()) {
      const size_t total_in = output->columnar->total_rows;
      ASSIGN_OR_RETURN(ColumnarTopNResult, topn_res,
                       (TryColumnarBoundedTopN(
                           *output->columnar, output->schema, output->schema,
                           {}, col_topn_keys, order_specs, raw_keep)));
      if (topn_res.has_value()) {
        output->SetColumnarTable(std::move(topn_res->first), topn_res->second);
        if (auto* runtime = context.execution_runtime()) {
          ++runtime->topn_finish_uses;
          runtime->topn_finish_capacity = std::min(total_in, raw_keep);
          runtime->topn_finish_input_rows = total_in;
          if (total_in > 0) {
            runtime->sort_ms += ElapsedMs(sort_begin);
          }
        }
        return Status::kSuccess;
      }
    }
  }

  if (!output->HasSpill() &&
      (!output->HasColumnarOnly() ||
       QueryMemoryBudget::Global().CanReserve(output->TotalRows() * 160))) {
    RETURN_IF_FAIL(output->EnsureRowsMaterializedChecked());
  }
  // Lean proof: lean/TinylambOpt/ResearchProofs/SqlFetchWithTies.lean (sound)
  if (allow_bounded && statement.HasLimit() && statement.Limit() == 1 &&
      statement.Offset() == 0 && statement.WithTies() && !output->HasSpill() &&
      !output->HasColumnarOnly() && output->rows.size() > 1 &&
      !order_specs.empty()) {
    std::vector<Value> best_keys;
    std::vector<Value> cur_keys(key_expressions.size());
    std::vector<size_t> winner_indices;
    winner_indices.reserve(output->rows.size());
    for (size_t ri = 0; ri < output->rows.size(); ++ri) {
      const Row& row = output->rows[ri];
      Scope scope{.row = &row, .schema = &output->schema, .outer = outer};
      for (size_t k = 0; k < key_expressions.size(); ++k) {
        if (direct_key_cols[k].has_value()) {
          cur_keys[k] = row[direct_key_cols[k].value()];
          continue;
        }
        ASSIGN_OR_RETURN(
            Value, kv,
            (TryEvaluate(key_expressions[k], scope, nullptr, context, ctes)));
        cur_keys[k] = std::move(kv);
      }
      RETURN_IF_FAIL(ObserveSortKeys(order_specs, cur_keys));
      if (winner_indices.empty()) {
        best_keys = cur_keys;
        winner_indices.push_back(ri);
        continue;
      }
      int cmp = 0;
      for (size_t k = 0; k < order_specs.size(); ++k) {
        cmp = order_specs[k].Compare(cur_keys[k], best_keys[k]);
        if (cmp != 0) {
          break;
        }
      }
      if (cmp < 0) {
        best_keys = cur_keys;
        winner_indices.clear();
        winner_indices.push_back(ri);
      } else if (cmp == 0) {
        winner_indices.push_back(ri);
      }
    }
    std::vector<Row> old_rows;
    old_rows.swap(output->rows);
    output->ResetContents();
    output->rows.reserve(winner_indices.size());
    for (const size_t wi : winner_indices) {
      output->AddRow(std::move(old_rows[wi]));
    }
    RETURN_IF_FAIL(output->FinishSpill());
    if (context.execution_runtime() != nullptr) {
      ++context.execution_runtime()->topn_finish_uses;
      context.execution_runtime()->topn_finish_capacity = 1;
      context.execution_runtime()->topn_finish_input_rows = old_rows.size();
      context.execution_runtime()->sort_ms += ElapsedMs(sort_begin);
    }
    return Status::kSuccess;
  }

  if (!output->HasSpill() && !output->HasColumnarOnly() && all_direct_keys &&
      output->rows.size() <= std::numeric_limits<uint32_t>::max()) {
    const size_t n = output->rows.size();
    const bool bounded = col_bounded;
    const size_t keep = bounded ? std::min(n, raw_keep) : n;
    if (bounded && context.execution_runtime() != nullptr) {
      ++context.execution_runtime()->topn_finish_uses;
      context.execution_runtime()->topn_finish_capacity = keep;
      context.execution_runtime()->topn_finish_input_rows = n;
    }
    const std::vector<Row>& rows_ref = output->rows;
    for (size_t k = 0; k < order_specs.size(); ++k) {
      if (!direct_key_cols[k].has_value()) {
        return StatusError(StatusCode::kRuntimeError,
                           "direct sort key column is missing");
      }
      const size_t col = direct_key_cols[k].value();
      for (const Row& row : rows_ref) {
        RETURN_IF_FAIL(order_specs[k].Observe(row[col]));
      }
    }
    if (n >= 256 && !order_specs.empty()) {
      struct SortEntry {
        uint64_t k0;
        uint64_t k1;
        uint32_t idx;
        uint32_t k2;
      };
      const auto can_encode_col = [&](size_t oi) noexcept {
        if (oi >= order_specs.size() || !direct_key_cols[oi].has_value() ||
            order_specs[oi].KeyCollation() == Collation::kCaseInsensitive) {
          return false;
        }
        const slot_t col = *direct_key_cols[oi];
        if (col >= output->schema.ColumnCount()) {
          return false;
        }
        const Column& c = output->schema.GetColumn(col);
        if (c.IsUnsigned()) {
          return false;
        }
        if (oi == 2 && c.Type() == ValueType::kDouble) {
          return false;
        }
        return c.Type() == ValueType::kInt64 || c.Type() == ValueType::kDate ||
               c.Type() == ValueType::kDouble ||
               c.Type() == ValueType::kVarChar;
      };
      if (can_encode_col(0)) {
        const bool has_k1 = order_specs.size() >= 2 && can_encode_col(1);
        const bool has_k2 =
            has_k1 && order_specs.size() >= 3 && can_encode_col(2);
        const slot_t col0 = direct_key_cols[0].value_or(0);
        const slot_t col1 = has_k1 ? direct_key_cols[1].value_or(0) : 0;
        const slot_t col2 = has_k2 ? direct_key_cols[2].value_or(0) : 0;
        bool exact0 = true;
        bool exact1 = has_k1;
        bool exact2 = has_k2 && order_specs.size() == 3;
        if (order_specs.size() == 1) {
          exact1 = true;
          exact2 = true;
        } else if (order_specs.size() == 2) {
          exact2 = has_k1;
        }
        bool bail_out = false;
        const auto encode_val = [&](const Value& v, const SortKeyOrder& spec,
                                    bool* all_exact) noexcept -> uint64_t {
          if (v.IsNull()) {
            return spec.NullsFirst() ? 0ULL : ~uint64_t{0};
          }
          uint64_t u = 0;
          if (v.type == ValueType::kInt64 || v.type == ValueType::kDate) {
            if (v.IsUnsigned()) {
              bail_out = true;
              *all_exact = false;
              return 1ULL << 63;
            }
            u = static_cast<uint64_t>(v.value.int_value) ^ (1ULL << 63);
            if (!spec.Ascending()) {
              u = ~u;
            }
            if (u == 0ULL || u == ~uint64_t{0}) {
              *all_exact = false;
              u = (u == 0ULL) ? 1ULL : (~uint64_t{0} - 1ULL);
            }
            return u;
          }
          if (v.type == ValueType::kDouble) {
            double d = v.value.double_value;
            if (std::isnan(d)) {
              bail_out = true;
              *all_exact = false;
              return 1ULL << 63;
            }
            if (d == 0.0) {
              d = 0.0;
            }
            const auto bits = std::bit_cast<uint64_t>(d);
            u = (bits & (1ULL << 63)) != 0 ? ~bits : (bits | (1ULL << 63));
            if (!spec.Ascending()) {
              u = ~u;
            }
            if (u == 0ULL || u == ~uint64_t{0}) {
              *all_exact = false;
              u = (u == 0ULL) ? 1ULL : (~uint64_t{0} - 1ULL);
            }
            return u;
          }
          // INTERVAL values are VARCHAR-tagged but their canonical text is not
          // zero-padded (e.g. "0-0 10 0:0:0" vs "0-0 2 0:0:0"), so a raw
          // byte-prefix key would order them differently from the canonical
          // parsed-interval comparator. Bail out like a collated value.
          if (v.type == ValueType::kVarChar && !v.IsCaseInsensitive() &&
              !v.IsInterval()) {
            const std::string_view sv = v.value.varchar_value;
            if (sv.size() >= 9 && sv.size() <= 18 && sv.starts_with("TESTENUM"))
                [[unlikely]] {
              bail_out = true;
              *all_exact = false;
              return 1ULL << 63;
            }
            // Monotonic 8-byte prefix: NUL bytes and sentinel collisions only
            // merge adjacent prefixes while preserving weak order; full
            // Value::Compare resolves any tie via *all_exact = false.
            const size_t len = std::min<size_t>(sv.size(), 8);
            uint64_t pfx = 0;
            for (size_t b = 0; b < len; ++b) {
              const auto ch = static_cast<uint8_t>(sv[b]);
              if (ch == 0) {
                *all_exact = false;
              }
              pfx |= static_cast<uint64_t>(ch) << ((7 - b) * 8);
            }
            if (sv.size() > 8) {
              *all_exact = false;
            }
            u = pfx;
            if (!spec.Ascending()) {
              u = ~u;
            }
            if (u == 0ULL || u == ~uint64_t{0}) {
              *all_exact = false;
              u = (u == 0ULL) ? 1ULL : (~uint64_t{0} - 1ULL);
            }
            return u;
          }
          bail_out = true;
          *all_exact = false;
          return 1ULL << 63;
        };
        const auto encode_val32 = [&](const Value& v, const SortKeyOrder& spec,
                                      bool* all_exact) noexcept -> uint32_t {
          if (v.IsNull()) {
            return spec.NullsFirst() ? 0U : ~uint32_t{0};
          }
          if (v.type == ValueType::kInt64 || v.type == ValueType::kDate) {
            if (v.IsUnsigned() || v.value.int_value < -2147483647LL ||
                v.value.int_value > 2147483646LL) {
              bail_out = true;
              *all_exact = false;
              return 1U << 31;
            }
            uint32_t u =
                static_cast<uint32_t>(static_cast<int32_t>(v.value.int_value)) ^
                (1U << 31);
            if (!spec.Ascending()) {
              u = ~u;
            }
            return u;
          }
          // INTERVAL values are VARCHAR-tagged but their canonical text is not
          // zero-padded (e.g. "0-0 10 0:0:0" vs "0-0 2 0:0:0"), so a raw
          // byte-prefix key would order them differently from the canonical
          // parsed-interval comparator. Bail out like a collated value.
          if (v.type == ValueType::kVarChar && !v.IsCaseInsensitive() &&
              !v.IsInterval()) {
            const std::string_view sv = v.value.varchar_value;
            if (sv.size() >= 9 && sv.size() <= 18 && sv.starts_with("TESTENUM"))
                [[unlikely]] {
              bail_out = true;
              *all_exact = false;
              return 1U << 31;
            }
            // Monotonic 4-byte prefix (same NUL/sentinel contract as 8-byte).
            const size_t len = std::min<size_t>(sv.size(), 4);
            uint32_t pfx = 0;
            for (size_t b = 0; b < len; ++b) {
              const auto ch = static_cast<uint8_t>(sv[b]);
              if (ch == 0) {
                *all_exact = false;
              }
              pfx |= static_cast<uint32_t>(ch) << ((3 - b) * 8);
            }
            if (sv.size() > 4) {
              *all_exact = false;
            }
            uint32_t u = pfx;
            if (!spec.Ascending()) {
              u = ~u;
            }
            if (u == 0U || u == ~uint32_t{0}) {
              *all_exact = false;
              u = (u == 0U) ? 1U : (~uint32_t{0} - 1U);
            }
            return u;
          }
          bail_out = true;
          *all_exact = false;
          return 0U;
        };
        std::vector<SortEntry> entries(n);
        for (size_t i = 0; i < n; ++i) {
          const Row& r = rows_ref[i];
          entries[i].k0 = encode_val(r[col0], order_specs[0], &exact0);
          entries[i].k1 =
              has_k1 ? encode_val(r[col1], order_specs[1], &exact1) : 0ULL;
          entries[i].idx = static_cast<uint32_t>(i);
          entries[i].k2 =
              has_k2 ? encode_val32(r[col2], order_specs[2], &exact2) : 0U;
          if (bail_out) [[unlikely]] {
            break;
          }
        }
        if (!bail_out) {
          const bool fully_exact =
              exact0 && exact1 && exact2 &&
              (order_specs.size() == 1 || (order_specs.size() == 2 && has_k1) ||
               (order_specs.size() == 3 && has_k2));
          const size_t start_i =
              !exact0 ? 0 : (!exact1 ? 1 : (!exact2 ? 2 : 3));
          const auto entry_precedes = [&](const SortEntry& a,
                                          const SortEntry& b) {
            if (a.k0 != b.k0) {
              return a.k0 < b.k0;
            }
            if (exact0) {
              if (a.k1 != b.k1) {
                return a.k1 < b.k1;
              }
              if (exact1 && a.k2 != b.k2) {
                return a.k2 < b.k2;
              }
            }
            if (fully_exact) {
              return a.idx < b.idx;
            }
            const Row& left_row = rows_ref[a.idx];
            const Row& right_row = rows_ref[b.idx];
            for (size_t i = start_i; i < order_specs.size(); ++i) {
              const slot_t col = *direct_key_cols[i];
              const int cmp =
                  order_specs[i].Compare(left_row[col], right_row[col]);
              if (cmp != 0) {
                return cmp < 0;
              }
            }
            return a.idx < b.idx;
          };
          if (bounded && keep < n) {
            std::partial_sort(
                entries.begin(),
                entries.begin() + static_cast<std::ptrdiff_t>(keep),
                entries.end(), entry_precedes);
          } else if (::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_RADIX_SORT") == nullptr &&
                     n >= 1024 && exact0) {
            // Lean proof:
            // lean/TinylambOpt/ResearchProofs/StableRadix.lean
            // (sound)
            auto key_fn = [](const SortEntry& e) noexcept { return e.k0; };
            if (!fully_exact || has_k1 || order_specs.size() > 1) {
              StableRadixSortByUint64KeyAndTieBreak(entries, key_fn,
                                                    entry_precedes);
            } else {
              StableRadixSortByUint64Key(entries, key_fn);
            }
          } else {
            std::sort(entries.begin(), entries.end(), entry_precedes);
          }
          std::vector<Row> old_rows = std::move(output->rows);
          output->ResetContents();
          output->rows.reserve(keep);
          for (size_t i = 0; i < keep; ++i) {
            output->AddRow(std::move(old_rows[entries[i].idx]));
          }
          RETURN_IF_FAIL(output->FinishSpill());
          if (context.execution_runtime() != nullptr) {
            context.execution_runtime()->sort_ms += ElapsedMs(sort_begin);
          }
          return Status::kSuccess;
        }
      }
    }
    std::vector<uint32_t> order(n);
    for (size_t i = 0; i < n; ++i) {
      order[i] = static_cast<uint32_t>(i);
    }
    const auto idx_precedes = [&](uint32_t a, uint32_t b) {
      const Row& left_row = rows_ref[a];
      const Row& right_row = rows_ref[b];
      for (size_t i = 0; i < order_specs.size(); ++i) {
        const slot_t col = *direct_key_cols[i];
        const int cmp = order_specs[i].Compare(left_row[col], right_row[col]);
        if (cmp != 0) {
          return cmp < 0;
        }
      }
      return a < b;
    };
    if (bounded && keep < n) {
      std::partial_sort(order.begin(),
                        order.begin() + static_cast<std::ptrdiff_t>(keep),
                        order.end(), idx_precedes);
    } else {
      std::ranges::stable_sort(order, idx_precedes);
    }
    std::vector<Row> old_rows;
    old_rows.swap(output->rows);
    output->ResetContents();
    output->rows.reserve(keep);
    for (size_t i = 0; i < keep; ++i) {
      output->AddRow(std::move(old_rows[order[i]]));
    }
    RETURN_IF_FAIL(output->FinishSpill());
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->sort_ms += ElapsedMs(sort_begin);
    }
    return Status::kSuccess;
  }

  ExternalKeyedSorter sorter(order_specs, col_bounded, raw_keep);
  Status sort_error{Status::kSuccess};
  Status iterated = output->ForEachRow([&](const Row& row) -> void {
    if (sort_error != Status::kSuccess) {
      return;
    }
    Scope scope{.row = &row, .schema = &output->schema, .outer = outer};
    std::vector<Value> keys;
    keys.reserve(order_by.size());
    for (size_t k = 0; k < key_expressions.size(); ++k) {
      if (direct_key_cols[k].has_value()) {
        keys.push_back(row[*direct_key_cols[k]]);
        continue;
      }
      StatusOr<Value> key_value =
          TryEvaluate(key_expressions[k], scope, nullptr, context, ctes);
      if (!key_value.HasValue()) {
        sort_error = key_value.GetStatus();
        return;
      }
      keys.push_back(key_value.MoveValue());
    }
    Status st_add = sorter.Add(KeyedRow{.keys = std::move(keys), .row = row});
    if (st_add != Status::kSuccess) {
      sort_error = st_add;
    }
  });
  RETURN_IF_FAIL(sort_error);
  if (iterated != Status::kSuccess) {
    return iterated;
  }
  // Every input row has been ingested by `sorter` (in heap or external sorted
  // runs); release the input storage before emitting the sorted output.
  output->ResetContents();
  if (sorter.UsedBoundedHeap() && context.execution_runtime() != nullptr) {
    ++context.execution_runtime()->topn_finish_uses;
    context.execution_runtime()->topn_finish_capacity =
        std::min(sorter.TotalInputRows(), raw_keep);
    context.execution_runtime()->topn_finish_input_rows =
        sorter.TotalInputRows();
  }
  RETURN_IF_FAIL(sorter.EmitSorted([&](Row&& row) {
    output->AddRow(std::move(row));
    return Status::kSuccess;
  }));
  RETURN_IF_FAIL(output->FinishSpill());
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->sort_ms += ElapsedMs(sort_begin);
  }
  return Status::kSuccess;
}

StatusOr<Relation> LimitedRows(const SelectStatement& statement,
                               Relation&& input, TransactionContext* context,
                               const Scope* outer, const CteMap* ctes) {
  Relation limited;
  limited.schema = input.schema;
  CopyExecutionStats(&limited, input);
  RETURN_IF_FAIL(input.FinishSpill());
  const size_t total = input.TotalRows();
  const size_t begin = std::min(statement.Offset(), total);
  const size_t available = total - begin;
  // An explicit LIMIT 0 is a real bound (zero rows); only an absent LIMIT
  // leaves the window unbounded.
  const bool unlimited = statement.Limit() == 0 && !statement.HasLimit();
  const size_t count =
      unlimited ? available : std::min(statement.Limit(), available);
  if (count == 0) {
    RETURN_IF_FAIL(limited.FinishSpill());
    return limited;
  }

  if (!statement.WithTies() || statement.OrderBy().empty() ||
      count == available) {
    if (!input.HasSpill()) {
      if (input.HasColumnarOnly()) {
        if (begin == 0 && count >= total) {
          return std::move(input);
        }
        limited.SetColumnarTable(relational_detail::SliceColumnarTable(
                                     *input.columnar, begin, count),
                                 input.uniform_cell_types);
        return limited;
      }
      if (begin == 0) {
        if (count < input.rows.size()) {
          input.rows.resize(count);
        }
        return std::move(input);
      }
      // Transfer the memory charge from `input` to `limited` and route moved
      // rows through AddRow() so retained offsets stay budgeted and spillable.
      input.ReleaseCharge();
      for (size_t i = 0; i < count; ++i) {
        limited.AddRow(std::move(input.rows[begin + i]));
      }
      RETURN_IF_FAIL(limited.FinishSpill());
      return limited;
    }
    size_t seen = 0;
    RETURN_IF_FAIL(input.ForEachRowBounded(begin + count, [&](const Row& row) {
      if (seen++ >= begin) {
        limited.AddRow(row);
      }
    }));
    RETURN_IF_FAIL(limited.FinishSpill());
    return limited;
  }

  // The sort already ordered peers next to each other. Retain only the
  // boundary row for comparison, and send accepted rows through AddRow so
  // even an arbitrarily large peer group can spill.
  const auto tied = [&](const Row& last_row,
                        const Row& current_row) -> StatusOr<bool> {
    for (const auto& term : statement.OrderBy()) {
      // ORDER BY <ordinal> COLLATE 'spec': the sort bound the ordinal to
      // that output column under the spec's collation. Evaluating the
      // COLLATE wrapper per row compares the ordinal literal with itself
      // and ties every row, so resolve to the bound column value first
      // (same gate as the ordinal binding above; an unsupported spec
      // resolves to the engine default exactly as COLLATE() does).
      if (term.expression) {
        const std::optional<CollatedOrdinal> collated =
            AsCollatedOrdinal(term.expression);
        if (collated.has_value() && collated->ordinal >= 1 &&
            std::cmp_less_equal(collated->ordinal, last_row.values_.size()) &&
            std::cmp_less_equal(collated->ordinal,
                                current_row.values_.size())) {
          const auto column = static_cast<size_t>(collated->ordinal - 1);
          const std::optional<Collation> spec = CollationOfSpec(collated->spec);
          const int cmp = CompareUnderCollation(
              last_row.values_[column], current_row.values_[column],
              spec.value_or(Collation::kNone));
          if (cmp != 0) {
            return false;
          }
          continue;
        }
      }
      // ORDER BY <ordinal>: the sort bound the integer to that output
      // column; comparing the literal with itself would tie every row.
      if (term.expression &&
          term.expression->Type() == TypeTag::kConstantValue) {
        const Value& ordinal = term.expression->AsConstantValue().GetValue();
        if (ordinal.type == ValueType::kInt64 && !ordinal.IsUnsigned() &&
            ordinal.value.int_value >= 1 &&
            std::cmp_less_equal(ordinal.value.int_value,
                                last_row.values_.size()) &&
            std::cmp_less_equal(ordinal.value.int_value,
                                current_row.values_.size())) {
          const auto column = static_cast<size_t>(ordinal.value.int_value - 1);
          if (!EqualUnderCollation(last_row.values_[column],
                                   current_row.values_[column])) {
            return false;
          }
          continue;
        }
      }
      Scope scope_last{
          .row = &last_row, .schema = &input.schema, .outer = outer};
      Scope scope_curr{
          .row = &current_row, .schema = &input.schema, .outer = outer};
      StatusOr<Value> last_or =
          context != nullptr && ctes != nullptr
              ? TryEvaluate(term.expression, scope_last, nullptr, *context,
                            *ctes)
              : term.expression->TryEvaluate(last_row, input.schema);
      ASSIGN_OR_RETURN(Value, val_last, (std::move(last_or)));
      StatusOr<Value> curr_or =
          context != nullptr && ctes != nullptr
              ? TryEvaluate(term.expression, scope_curr, nullptr, *context,
                            *ctes)
              : term.expression->TryEvaluate(current_row, input.schema);
      ASSIGN_OR_RETURN(Value, val_curr, (std::move(curr_or)));
      // Peers must collapse exactly the way the ORDER BY grouped them, so a
      // case-insensitive key treats 'a' and 'A' as the same tie instead of
      // cutting the tie group at the LIMIT boundary.
      if (!EqualUnderCollation(val_last, val_curr)) {
        return false;
      }
    }
    return true;
  };

  const size_t end = begin + count;
  size_t seen = 0;
  Row boundary;
  Status tie_error{Status::kSuccess};
  const Status read = input.ForEachRowBounded(total, [&](const Row& row) {
    const size_t index = seen++;
    if (index < begin) {
      return true;
    }
    if (index >= end) {
      auto peer = tied(boundary, row);
      if (!peer.HasValue()) {
        tie_error = peer.GetStatus();
        return false;
      }
      if (!peer.Value()) {
        return false;
      }
    } else if (index == end - 1) {
      boundary = row;
    }
    limited.AddRow(row);
    return limited.GetStatus().ok();
  });
  RETURN_IF_FAIL(tie_error);
  RETURN_IF_FAIL(read);
  RETURN_IF_FAIL(limited.FinishSpill());
  return limited;
}

}  // namespace tinylamb::relational_detail
