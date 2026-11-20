/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/typed_stream_aggregate_internal.hpp"

namespace tinylamb::relational_detail {

namespace {

StatusOr<Relation> FinishCompactGroups(
    ExecutionRuntime* runtime, const SelectStatement& statement,
    const Relation& input,
    const std::vector<const AggregateExpression*>& aggregates,
    const TypedAggregationPlan& plan, TypedWorkerGroups& groups,
    const std::vector<PositionalHaving>* having = nullptr,
    size_t begin_group = 0, size_t end_group = SIZE_MAX,
    const std::vector<uint8_t>* keep = nullptr,
    std::vector<ColumnTypeTracker>* out_col_tracks = nullptr) {
  end_group = std::min(end_group, groups.GroupCount());
  Relation output(runtime);
  CopyExecutionStats(&output, input);
  std::vector<Column> columns;
  columns.reserve(statement.SelectList().size());
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    columns.emplace_back(ProjectionName(statement.SelectList()[i], i),
                         ValueType::kNull);
  }
  const size_t flush_granularity = Relation::ChargeFlushGranularity();
  if (end_group > begin_group && Relation::HasAmpleMemoryBudget()) {
    const size_t span = end_group - begin_group;
    output.rows.reserve(having == nullptr && keep == nullptr
                            ? span
                            : std::min<size_t>(span, 65536));
  }
  std::vector<ColumnTypeTracker> col_tracks(columns.size());
  const bool has_post_agg = std::ranges::any_of(
      plan.emit_items,
      [](const TypedEmitItem& item) { return item.post_agg != nullptr; });
  std::vector<Value> finished;
  finished.reserve(aggregates.size());
  size_t chunk_hint = 0;
  for (size_t group = begin_group; group < end_group; ++group) {
    if (keep != nullptr && (group >= keep->size() || (*keep)[group] == 0)) {
      continue;
    }
    ASSIGN_OR_RETURN(std::vector<Value>, dependent_keys,
                     EvaluateDependentKeys(plan, groups, group, &chunk_hint));
    finished.clear();
    for (size_t i = 0; i < aggregates.size(); ++i) {
      if (!plan.slim) {
        ASSIGN_OR_RETURN(Value, value, groups.State(group, i).TryFinish());
        finished.push_back(std::move(value));
        continue;
      }
      Value value;
      Status err = Status::kSuccess;
      if (!FinishSlimCell(groups, group, i, plan.slim_kinds[i], &value, &err)) {
        return err;
      }
      finished.push_back(std::move(value));
    }
    bool keep_group = true;
    if (having != nullptr) {
      Status having_err = Status::kSuccess;
      for (const auto& clause : *having) {
        const Value* right_ptr =
            clause.rhs_is_agg ? &finished[clause.rhs_agg_index] : nullptr;
        keep_group = EvalPositionalHavingValues(
            clause, finished[clause.agg_index], right_ptr, &having_err);
        if (!having_err.ok()) {
          return having_err;
        }
        if (!keep_group) {
          break;
        }
      }
    }
    if (!keep_group) {
      continue;
    }
    std::vector<Value> values;
    values.reserve(plan.emit_items.size());
    size_t row_bytes = 64;
    for (size_t ci = 0; ci < plan.emit_items.size(); ++ci) {
      const auto& item = plan.emit_items[ci];
      Value cell;
      if (const auto* post_agg_ptr = item.post_agg.get()) {
        const auto& post_agg = *post_agg_ptr;
        std::vector<Value> post_agg_cells;
        post_agg_cells.reserve(post_agg.slots.size());
        bool types_match_schema = post_agg_ptr->program.has_value();
        for (size_t si = 0; si < post_agg.slots.size(); ++si) {
          const auto& slot = post_agg.slots[si];
          Value arg;
          if (slot.is_key) {
            groups.ReadGroupKeyOwned(group, slot.index, &arg, &chunk_hint);
          } else {
            arg = finished[slot.index];
          }
          if (types_match_schema && !arg.IsNull()) {
            const ValueType expected =
                post_agg.eval_schema.GetColumn(si).Type();
            if (expected != ValueType::kNull && arg.type != expected) {
              types_match_schema = false;
            }
          }
          post_agg_cells.push_back(std::move(arg));
        }
        const Row eval_row(std::move(post_agg_cells));
        if (types_match_schema && post_agg_ptr->program.has_value()) {
          ASSIGN_OR_RETURN(
              Value, evaluated,
              TryEvaluateRowAuto(*post_agg_ptr->program, eval_row));
          cell = std::move(evaluated);
        } else {
          ASSIGN_OR_RETURN(
              Value, evaluated,
              post_agg.expr->TryEvaluate(eval_row, post_agg.eval_schema));
          cell = std::move(evaluated);
        }
      } else if (const std::optional<size_t> dep_key = item.dependent_key) {
        cell = dependent_keys[*dep_key];
      } else if (item.constant) {
        cell = *item.constant;
      } else if (item.is_key) {
        groups.ReadGroupKeyOwned(group, item.index, &cell, &chunk_hint);
      } else {
        cell = has_post_agg ? finished[item.index]
                            : std::move(finished[item.index]);
      }
      if (ci < col_tracks.size()) {
        col_tracks[ci].Observe(cell);
      }
      row_bytes += cell.type == ValueType::kArray
                       ? EstimateValueBytes(cell)
                       : EstimateScalarValueBytes(cell);
      values.push_back(std::move(cell));
    }
    output.AddRowWithKnownBytes(Row(std::move(values)), row_bytes,
                                flush_granularity);
  }
  if (out_col_tracks != nullptr) {
    *out_col_tracks = col_tracks;
  }
  const bool all_uniform = ColumnTypeTracker::FinalizeColumns(
      std::span<const std::vector<ColumnTypeTracker>>(&col_tracks, 1), columns);
  output.schema = Schema("", std::move(columns));
  RETURN_IF_FAIL(output.FinishSpill());
  RETURN_IF_FAIL(output.GetStatus());
  output.uniform_cell_types =
      !output.HasSpill() && all_uniform && !output.rows.empty();
  if (runtime != nullptr) {
    if (begin_group == 0) {
      runtime->aggregate_input_rows += input.TotalRows();
      runtime->aggregate_updates += input.TotalRows() * aggregates.size();
    }
    runtime->aggregate_groups += end_group - begin_group;
  }
  return output;
}

StatusOr<Value> CompactArgument(const TypedAggregateInput& item, const Row& row,
                                bool allow_typed_double = true) {
  if (item.column) {
    return row[*item.column];
  }
  if (item.conditional) {
    return EvaluateTypedConditionalOnRow(*item.conditional, row);
  }
  if (allow_typed_double && item.double_expr) {
    return EvaluateTypedDoubleExprOnRow(*item.double_expr, row);
  }
  if (allow_typed_double && item.coalesce_double) {
    return EvaluateTypedCoalesceDoubleOnRow(*item.coalesce_double, row);
  }
  if (item.int_expr) {
    return EvaluateTypedIntExprOnRow(*item.int_expr, row);
  }
  if (item.child) {
    return TryEvaluateRowAuto(*item.child, row);
  }
  if (item.double_expr) {
    return EvaluateTypedDoubleExprOnRow(*item.double_expr, row);
  }
  return Value(1);
}

inline uint64_t FastStringHash64(std::string_view sv) {
  const size_t n = sv.size();
  const char* p = sv.data();
  uint64_t h = 0x9e3779b97f4a7c15ULL ^
               (static_cast<uint64_t>(n) * 0xbf58476d1ce4e5b9ULL);
  size_t i = 0;
  for (; i + 8 <= n; i += 8) {
    uint64_t w = 0;
    std::memcpy(&w, p + i, 8);
    h = (h ^ w) * 0x9e3779b97f4a7c15ULL;
  }
  if (i < n) {
    uint64_t tail = 0;
    std::memcpy(&tail, p + i, n - i);
    h = (h ^ tail) * 0xbf58476d1ce4e5b9ULL;
  }
  h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9ULL;
  h = (h ^ (h >> 27)) * 0x94d049bb133111ebULL;
  return h ^ (h >> 31);
}

inline size_t FastCompactCellHash(const Value& value) {
  if (!value.IsNull()) {
    if ((value.type == ValueType::kInt64 || value.type == ValueType::kDate) &&
        !value.IsUnsigned()) {
      auto x = static_cast<uint64_t>(value.value.int_value);
      x += 0x9e3779b97f4a7c15ULL;
      x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
      x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
      return static_cast<size_t>(x ^ (x >> 31));
    }
    if (value.type == ValueType::kVarChar && !value.IsCaseInsensitive()) {
      return static_cast<size_t>(FastStringHash64(value.value.varchar_value));
    }
  }
  auto x = static_cast<uint64_t>(DistinctValueHash{}(value));
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  return static_cast<size_t>(x ^ (x >> 31));
}

inline bool FastCompactCellEqual(const Value& left, const Value& right) {
  if (!left.IsNull() && !right.IsNull()) {
    if (left.type != right.type) {
      return false;
    }
    if ((left.type == ValueType::kInt64 || left.type == ValueType::kDate) &&
        !left.IsUnsigned() && !right.IsUnsigned()) {
      return left.value.int_value == right.value.int_value;
    }
    if (left.type == ValueType::kVarChar && !left.IsCaseInsensitive() &&
        !right.IsCaseInsensitive()) {
      return left.value.varchar_value == right.value.varchar_value;
    }
  }
  return DistinctValueEqual{}(left, right);
}

inline uint64_t MixCompactInt64(uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

struct CompactColDict {
  struct Slot {
    size_t hash{0};
    union {
      int64_t int_val;
      const char* str_ptr;
      const Value* val_ptr;
    } payload{.val_ptr = nullptr};
    uint32_t code{0};
    uint32_t str_len{0};
    uint8_t kind{0};
    ValueType vtype{ValueType::kNull};
  };
  struct BorrowedDictMap {
    const LSTableColumnDictionary* dict{nullptr};
    std::vector<uint32_t> code_to_local;
    uint32_t null_local{UINT32_MAX};
  };
  std::vector<Slot> slots;
  std::vector<uint32_t> reps;
  std::vector<size_t> hashes;
  std::deque<Value> owned_vals;
  std::vector<BorrowedDictMap> borrowed_maps;
  size_t mask{0};
  Slot mru_slot;
  bool saw_ci{false};

  BorrowedDictMap& GetOrAddBorrowedMap(const LSTableColumnDictionary* src_dict,
                                       size_t entry_count) {
    for (auto& entry : borrowed_maps) {
      if (entry.dict == src_dict) {
        if (entry.code_to_local.size() < entry_count) {
          entry.code_to_local.resize(entry_count, UINT32_MAX);
        }
        return entry;
      }
    }
    borrowed_maps.push_back(BorrowedDictMap{
        .dict = src_dict,
        .code_to_local = std::vector<uint32_t>(entry_count, UINT32_MAX),
        .null_local = UINT32_MAX,
    });
    return borrowed_maps.back();
  }

  static const Value& NullCell() noexcept {
    static const Value kNull;
    return kNull;
  }

  static size_t HashSignedInt(int64_t val) noexcept {
    auto x = static_cast<uint64_t>(val);
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return static_cast<size_t>(x ^ (x >> 31));
  }

  static bool CellMatchesSlot(const Value& cell, const Slot& s) noexcept {
    if (s.kind == 1) {
      return !cell.IsNull() && cell.type == s.vtype && !cell.IsUnsigned() &&
             !cell.IsInterval() && cell.value.int_value == s.payload.int_val;
    }
    if (s.kind == 2) {
      return !cell.IsNull() && cell.type == ValueType::kVarChar &&
             !cell.IsCaseInsensitive() &&
             cell.value.varchar_value.size() == s.str_len &&
             (s.str_len == 0 ||
              std::memcmp(s.payload.str_ptr, cell.value.varchar_value.data(),
                          s.str_len) == 0);
    }
    return FastCompactCellEqual(cell, *s.payload.val_ptr);
  }

  static void FillSlotFromValue(Slot& s, size_t h, const Value& stable_cell,
                                uint32_t code) noexcept {
    s.hash = h;
    s.code = code;
    if (!stable_cell.IsNull() &&
        (stable_cell.type == ValueType::kInt64 ||
         stable_cell.type == ValueType::kDate) &&
        !stable_cell.IsUnsigned() && !stable_cell.IsInterval()) {
      s.kind = 1;
      s.vtype = stable_cell.type;
      s.str_len = 0;
      s.payload.int_val = stable_cell.value.int_value;
    } else if (!stable_cell.IsNull() &&
               stable_cell.type == ValueType::kVarChar &&
               !stable_cell.IsCaseInsensitive() &&
               stable_cell.value.varchar_value.size() <= UINT32_MAX) {
      s.kind = 2;
      s.vtype = ValueType::kVarChar;
      s.str_len = static_cast<uint32_t>(stable_cell.value.varchar_value.size());
      s.payload.str_ptr = stable_cell.value.varchar_value.data();
    } else {
      s.kind = 3;
      s.vtype = stable_cell.type;
      s.str_len = 0;
      s.payload.val_ptr = &stable_cell;
    }
  }

  static void FillSlot(Slot& s, size_t h, const std::vector<Row>& rows,
                       uint32_t r, slot_t col, uint32_t code) noexcept {
    FillSlotFromValue(s, h, rows[r][col], code);
  }

  void Init(size_t initial_cap = 64) {
    size_t cap = 64;
    while (cap < initial_cap) {
      cap *= 2;
    }
    slots.assign(cap, Slot{});
    mask = cap - 1;
  }

  void Grow() {
    const size_t next = slots.empty() ? 64 : slots.size() * 2;
    std::vector<Slot> replacement(next);
    const size_t new_mask = next - 1;
    for (const Slot& old : slots) {
      if (old.kind == 0) {
        continue;
      }
      size_t pos = old.hash & new_mask;
      while (replacement[pos].kind != 0) {
        pos = (pos + 1) & new_mask;
      }
      replacement[pos] = old;
    }
    slots = std::move(replacement);
    mask = new_mask;
  }

  uint32_t FindOrInsertCell(const std::vector<Row>& rows, uint32_t r,
                            slot_t col, const Value& cell) {
    if (cell.IsCaseInsensitive()) {
      saw_ci = true;
    }
    if (reps.size() <= 64 && mru_slot.kind != 0 &&
        CellMatchesSlot(cell, mru_slot)) {
      return mru_slot.code;
    }
    if (slots.empty()) {
      Init();
    }
    if ((reps.size() + 1) * 10 > slots.size() * 7) {
      Grow();
    }
    const size_t h = FastCompactCellHash(cell);
    size_t pos = h & mask;
    while (slots[pos].kind != 0) {
      if (slots[pos].hash == h && CellMatchesSlot(cell, slots[pos])) {
        if (reps.size() <= 64) {
          mru_slot = slots[pos];
        }
        return slots[pos].code;
      }
      pos = (pos + 1) & mask;
    }
    const auto code = static_cast<uint32_t>(reps.size());
    reps.push_back(r);
    hashes.push_back(h);
    FillSlot(slots[pos], h, rows, r, col, code);
    if (reps.size() <= 64) {
      mru_slot = slots[pos];
    }
    return code;
  }

  template <bool RecordHash>
  uint32_t FindOrInsertSignedIntImpl(uint32_t r, ValueType vt, int64_t val,
                                     size_t h) {
    if (reps.size() <= 64 && mru_slot.kind == 1 && mru_slot.vtype == vt &&
        mru_slot.payload.int_val == val) {
      return mru_slot.code;
    }
    if (slots.empty()) {
      Init();
    }
    if ((reps.size() + 1) * 10 > slots.size() * 7) {
      Grow();
    }
    size_t pos = h & mask;
    while (slots[pos].kind != 0) {
      const Slot& cur = slots[pos];
      if (cur.kind == 1 && cur.vtype == vt && cur.payload.int_val == val) {
        if (reps.size() <= 64) {
          mru_slot = cur;
        }
        return cur.code;
      }
      pos = (pos + 1) & mask;
    }
    const auto code = static_cast<uint32_t>(reps.size());
    reps.push_back(r);
    if constexpr (RecordHash) {
      hashes.push_back(h);
    }
    Slot& dst = slots[pos];
    dst.hash = h;
    dst.code = code;
    dst.kind = 1;
    dst.vtype = vt;
    dst.str_len = 0;
    dst.payload.int_val = val;
    if (reps.size() <= 64) {
      mru_slot = dst;
    }
    return code;
  }

  template <bool RecordHash>
  uint32_t FindOrInsertStringImpl(uint32_t r, std::string_view sv, size_t h) {
    if (sv.size() > UINT32_MAX) {
      return FindOrInsertStableValueImpl<RecordHash>(r, Value(std::string(sv)),
                                                     h);
    }
    if (reps.size() <= 64 && mru_slot.kind == 2 &&
        mru_slot.str_len == sv.size() &&
        (sv.empty() ||
         std::memcmp(mru_slot.payload.str_ptr, sv.data(), sv.size()) == 0)) {
      return mru_slot.code;
    }
    if (slots.empty()) {
      Init();
    }
    if ((reps.size() + 1) * 10 > slots.size() * 7) {
      Grow();
    }
    size_t pos = h & mask;
    while (slots[pos].kind != 0) {
      const Slot& cur = slots[pos];
      if (cur.kind == 2 && cur.str_len == sv.size() &&
          (sv.empty() ||
           std::memcmp(cur.payload.str_ptr, sv.data(), sv.size()) == 0)) {
        if (reps.size() <= 64) {
          mru_slot = cur;
        }
        return cur.code;
      }
      pos = (pos + 1) & mask;
    }
    const auto code = static_cast<uint32_t>(reps.size());
    reps.push_back(r);
    if constexpr (RecordHash) {
      hashes.push_back(h);
    }
    Slot& dst = slots[pos];
    dst.hash = h;
    dst.code = code;
    dst.kind = 2;
    dst.vtype = ValueType::kVarChar;
    dst.str_len = static_cast<uint32_t>(sv.size());
    dst.payload.str_ptr = sv.data();
    if (reps.size() <= 64) {
      mru_slot = dst;
    }
    return code;
  }

  template <bool RecordHash>
  uint32_t FindOrInsertStableValueImpl(uint32_t r, const Value& cell,
                                       size_t h) {
    if (cell.IsCaseInsensitive()) {
      saw_ci = true;
    }
    if (reps.size() <= 64 && mru_slot.kind != 0 &&
        CellMatchesSlot(cell, mru_slot)) {
      return mru_slot.code;
    }
    if (slots.empty()) {
      Init();
    }
    if ((reps.size() + 1) * 10 > slots.size() * 7) {
      Grow();
    }
    size_t pos = h & mask;
    while (slots[pos].kind != 0) {
      if (slots[pos].hash == h && CellMatchesSlot(cell, slots[pos])) {
        if (reps.size() <= 64) {
          mru_slot = slots[pos];
        }
        return slots[pos].code;
      }
      pos = (pos + 1) & mask;
    }
    const auto code = static_cast<uint32_t>(reps.size());
    reps.push_back(r);
    if constexpr (RecordHash) {
      hashes.push_back(h);
    }
    if (cell.IsNull()) {
      FillSlotFromValue(slots[pos], h, NullCell(), code);
    } else {
      FillSlotFromValue(slots[pos], h, cell, code);
      if (slots[pos].kind == 3) {
        owned_vals.push_back(cell);
        Value& stored = owned_vals.back();
        if (stored.type == ValueType::kVarChar && stored.owned_data.empty() &&
            !stored.value.varchar_value.empty()) {
          stored.owned_data.assign(stored.value.varchar_value);
          stored.value.varchar_value = stored.owned_data;
        }
        slots[pos].payload.val_ptr = &stored;
      }
    }
    if (reps.size() <= 64) {
      mru_slot = slots[pos];
    }
    return code;
  }

  uint32_t FindOrInsertNull(uint32_t r) {
    return FindOrInsertStableValueImpl<true>(r, NullCell(),
                                             FastCompactCellHash(NullCell()));
  }

  uint32_t FindOrInsertSignedInt(uint32_t r, ValueType vt, int64_t val) {
    return FindOrInsertSignedIntImpl<true>(r, vt, val, HashSignedInt(val));
  }

  uint32_t FindOrInsertString(uint32_t r, std::string_view sv) {
    return FindOrInsertStringImpl<true>(
        r, sv, static_cast<size_t>(FastStringHash64(sv)));
  }

  uint32_t FindOrInsertStableValue(uint32_t r, const Value& cell) {
    return FindOrInsertStableValueImpl<true>(r, cell,
                                             FastCompactCellHash(cell));
  }

  uint32_t FindOrInsertColumnarGlobal(const ColumnarColumn& col, size_t lr,
                                      uint32_t r, size_t prehash,
                                      bool has_prehash) {
    if (col.is_uniform) {
      if (col.type == ValueType::kNull || col.IsNull(lr)) {
        return FindOrInsertStableValueImpl<false>(
            r, NullCell(),
            has_prehash ? prehash : FastCompactCellHash(NullCell()));
      }
      if ((col.type == ValueType::kInt64 || col.type == ValueType::kDate) &&
          !col.is_unsigned && !col.is_interval) {
        const int64_t v = col.IntAt(lr);
        return FindOrInsertSignedIntImpl<false>(
            r, col.type, v, has_prehash ? prehash : HashSignedInt(v));
      }
      if (col.type == ValueType::kVarChar && !col.is_interval) {
        const std::string_view sv = col.StringAt(lr);
        if (sv.size() <= UINT32_MAX) {
          return FindOrInsertStringImpl<false>(
              r, sv,
              has_prehash ? prehash
                          : static_cast<size_t>(FastStringHash64(sv)));
        }
      }
    }
    Value tmp;
    col.WriteValueInto(lr, &tmp);
    const size_t h = has_prehash ? prehash : FastCompactCellHash(tmp);
    return FindOrInsertStableValueImpl<false>(r, tmp, h);
  }

  uint32_t FindOrInsertPrehashed(const std::vector<Row>& rows, uint32_t r,
                                 slot_t col, size_t h, const Value& cell) {
    if (slots.empty()) {
      Init();
    }
    if ((reps.size() + 1) * 10 > slots.size() * 7) {
      Grow();
    }
    size_t pos = h & mask;
    while (slots[pos].kind != 0) {
      if (slots[pos].hash == h && CellMatchesSlot(cell, slots[pos])) {
        return slots[pos].code;
      }
      pos = (pos + 1) & mask;
    }
    const auto code = static_cast<uint32_t>(reps.size());
    reps.push_back(r);
    FillSlot(slots[pos], h, rows, r, col, code);
    return code;
  }
};

struct GlobalSourceSlot {
  uint32_t worker{0};
  uint32_t local{0};
};

void PopulateLocalOrderedPks(std::vector<size_t>& active_w, size_t active_n,
                             const std::vector<std::vector<size_t>>& first_rows,
                             const std::vector<uint64_t>& packed_rows,
                             std::vector<std::vector<size_t>>* global_ids,
                             std::vector<const Row*>* borrowed_key_rows,
                             std::vector<uint32_t>* borrowed_key_row_ids,
                             const std::vector<Row>* input_rows,
                             const std::vector<const Row*>* parent_key_rows,
                             const std::vector<uint32_t>* parent_key_row_ids,
                             std::vector<GlobalSourceSlot>* out_src,
                             std::vector<uint64_t>& out_pks) {
  const size_t total = out_pks.size();
  if (borrowed_key_rows != nullptr) {
    borrowed_key_rows->resize(total);
  }
  if (borrowed_key_row_ids != nullptr) {
    borrowed_key_row_ids->resize(total);
  }
  if (out_src != nullptr) {
    out_src->resize(total);
  }
  if (total == 0 || active_n == 0) {
    return;
  }
  if (active_n <= 16) {
    std::array<const size_t*, 16> ptrs{};
    std::array<size_t, 16> rem{};
    std::array<size_t, 16> heads{};
    std::array<size_t, 16> cursors{};
    std::array<size_t*, 16> gid_ptrs{};
    std::array<uint32_t, 16> w_ids{};
    for (size_t i = 0; i < active_n; ++i) {
      const size_t w = active_w[i];
      ptrs[i] = first_rows[w].data();
      rem[i] = first_rows[w].size();
      heads[i] = ptrs[i][0];
      cursors[i] = 0;
      gid_ptrs[i] = global_ids != nullptr ? (*global_ids)[w].data() : nullptr;
      w_ids[i] = static_cast<uint32_t>(w);
    }
    size_t n = active_n;
    const uint64_t* packed_ptr = packed_rows.data();
    uint64_t* out_ptr = out_pks.data();
    const Row** borrowed_ptr =
        borrowed_key_rows != nullptr ? borrowed_key_rows->data() : nullptr;
    uint32_t* borrowed_id_ptr = borrowed_key_row_ids != nullptr
                                    ? borrowed_key_row_ids->data()
                                    : nullptr;
    GlobalSourceSlot* src_ptr = out_src != nullptr ? out_src->data() : nullptr;
    const Row* input_ptr = input_rows != nullptr ? input_rows->data() : nullptr;
    const Row* const* parent_ptr =
        parent_key_rows != nullptr ? parent_key_rows->data() : nullptr;
    const uint32_t* parent_id_ptr =
        parent_key_row_ids != nullptr ? parent_key_row_ids->data() : nullptr;
    size_t global = 0;
    while (n > 1 && global < total) {
      size_t best_idx = 0;
      size_t best_r = heads[0];
      for (size_t ai = 1; ai < n; ++ai) {
        const size_t r = heads[ai];
        if (r < best_r) {
          best_r = r;
          best_idx = ai;
        }
      }
      const size_t local = cursors[best_idx]++;
      if (gid_ptrs[best_idx] != nullptr) {
        gid_ptrs[best_idx][local] = global;
      }
      if (src_ptr != nullptr) {
        src_ptr[global] = GlobalSourceSlot{
            .worker = w_ids[best_idx], .local = static_cast<uint32_t>(local)};
      }
      if (borrowed_ptr != nullptr) {
        borrowed_ptr[global] =
            input_ptr != nullptr
                ? &input_ptr[best_r]
                : (parent_ptr != nullptr ? parent_ptr[best_r] : nullptr);
      }
      if (borrowed_id_ptr != nullptr) {
        borrowed_id_ptr[global] = parent_id_ptr != nullptr
                                      ? parent_id_ptr[best_r]
                                      : static_cast<uint32_t>(best_r);
      }
      out_ptr[global] = packed_ptr[best_r];
      ++global;
      if (cursors[best_idx] == rem[best_idx]) {
        --n;
        ptrs[best_idx] = ptrs[n];
        rem[best_idx] = rem[n];
        heads[best_idx] = heads[n];
        cursors[best_idx] = cursors[n];
        gid_ptrs[best_idx] = gid_ptrs[n];
        w_ids[best_idx] = w_ids[n];
      } else {
        heads[best_idx] = ptrs[best_idx][cursors[best_idx]];
      }
    }
    if (n == 1) {
      const size_t* p = ptrs[0];
      size_t local = cursors[0];
      size_t* gid = gid_ptrs[0];
      const uint32_t w0 = w_ids[0];
      for (; global < total; ++global, ++local) {
        const size_t r = p[local];
        if (gid != nullptr) {
          gid[local] = global;
        }
        if (src_ptr != nullptr) {
          src_ptr[global] = GlobalSourceSlot{
              .worker = w0, .local = static_cast<uint32_t>(local)};
        }
        if (borrowed_ptr != nullptr) {
          borrowed_ptr[global] =
              input_ptr != nullptr
                  ? &input_ptr[r]
                  : (parent_ptr != nullptr ? parent_ptr[r] : nullptr);
        }
        if (borrowed_id_ptr != nullptr) {
          borrowed_id_ptr[global] = parent_id_ptr != nullptr
                                        ? parent_id_ptr[r]
                                        : static_cast<uint32_t>(r);
        }
        out_ptr[global] = packed_ptr[r];
      }
    }
    return;
  }
  std::vector<size_t> cursors(first_rows.size(), 0);
  for (size_t global = 0; global < total; ++global) {
    size_t best_idx = 0;
    size_t best_r = first_rows[active_w[0]][cursors[active_w[0]]];
    for (size_t ai = 1; ai < active_n; ++ai) {
      const size_t w = active_w[ai];
      const size_t r = first_rows[w][cursors[w]];
      if (r < best_r) {
        best_r = r;
        best_idx = ai;
      }
    }
    const size_t best_w = active_w[best_idx];
    const size_t local = cursors[best_w]++;
    if (cursors[best_w] == first_rows[best_w].size()) {
      active_w[best_idx] = active_w[--active_n];
    }
    if (global_ids != nullptr) {
      (*global_ids)[best_w][local] = global;
    }
    if (out_src != nullptr) {
      (*out_src)[global] =
          GlobalSourceSlot{.worker = static_cast<uint32_t>(best_w),
                           .local = static_cast<uint32_t>(local)};
    }
    if (borrowed_key_rows != nullptr) {
      (*borrowed_key_rows)[global] = input_rows != nullptr
                                         ? &(*input_rows)[best_r]
                                         : (*parent_key_rows)[best_r];
    }
    if (borrowed_key_row_ids != nullptr) {
      (*borrowed_key_row_ids)[global] = parent_key_row_ids != nullptr
                                            ? (*parent_key_row_ids)[best_r]
                                            : static_cast<uint32_t>(best_r);
    }
    out_pks[global] = packed_rows[best_r];
  }
}

void EncodeColumnarSliceToDict(CompactColDict& dict, const ColumnarColumn& col,
                               size_t lr_start, size_t take,
                               uint32_t global_r_base, uint32_t* out_codes) {
  if (col.is_uniform) {
    if (col.type == ValueType::kNull ||
        (col.encoding == ColumnarColumn::Encoding::kConstant &&
         col.const_is_null)) {
      const uint32_t null_code = dict.FindOrInsertNull(global_r_base);
      std::fill_n(out_codes, take, null_code);
      return;
    }
    if (col.encoding == ColumnarColumn::Encoding::kConstant &&
        !col.is_unsigned && !col.is_interval) {
      if (col.type == ValueType::kInt64 || col.type == ValueType::kDate) {
        const uint32_t c =
            dict.FindOrInsertSignedInt(global_r_base, col.type, col.const_int);
        std::fill_n(out_codes, take, c);
        return;
      }
      if (col.type == ValueType::kVarChar) {
        const uint32_t c =
            dict.FindOrInsertString(global_r_base, col.const_string);
        std::fill_n(out_codes, take, c);
        return;
      }
    }
    if ((col.type == ValueType::kInt64 || col.type == ValueType::kDate) &&
        !col.is_unsigned && !col.is_interval) {
      const ValueType vt = col.type;
      if (col.IsFlat()) {
        const int64_t* ints = col.ints.data() + lr_start;
        if (!col.has_null) {
          for (size_t i = 0; i < take; ++i) {
            out_codes[i] = dict.FindOrInsertSignedInt(
                global_r_base + static_cast<uint32_t>(i), vt, ints[i]);
          }
        } else {
          const uint8_t* nulls = col.nulls.data() + lr_start;
          for (size_t i = 0; i < take; ++i) {
            const uint32_t r = global_r_base + static_cast<uint32_t>(i);
            out_codes[i] = nulls[i] != 0
                               ? dict.FindOrInsertNull(r)
                               : dict.FindOrInsertSignedInt(r, vt, ints[i]);
          }
        }
      } else if (!col.has_null) {
        for (size_t i = 0; i < take; ++i) {
          out_codes[i] = dict.FindOrInsertSignedInt(
              global_r_base + static_cast<uint32_t>(i), vt,
              col.IntAt(lr_start + i));
        }
      } else {
        for (size_t i = 0; i < take; ++i) {
          const uint32_t r = global_r_base + static_cast<uint32_t>(i);
          const size_t lr = lr_start + i;
          out_codes[i] = col.IsNull(lr)
                             ? dict.FindOrInsertNull(r)
                             : dict.FindOrInsertSignedInt(r, vt, col.IntAt(lr));
        }
      }
      return;
    }
    if (col.type == ValueType::kVarChar && !col.is_interval) {
      if (col.encoding == ColumnarColumn::Encoding::kDictionary &&
          col.dict != nullptr) {
        const size_t ec = col.dict->StringEntryCount();
        auto& bmap = dict.GetOrAddBorrowedMap(col.dict, ec);
        uint32_t* __restrict c2l = bmap.code_to_local.data();
        for (size_t i = 0; i < take; ++i) {
          const uint32_t src_code = col.DictCodeAt(lr_start + i);
          if (src_code == ColumnarColumn::kNullDictCode || src_code >= ec) {
            if (bmap.null_local == UINT32_MAX) {
              bmap.null_local = dict.FindOrInsertNull(global_r_base +
                                                      static_cast<uint32_t>(i));
            }
            out_codes[i] = bmap.null_local;
          } else {
            uint32_t mapped = c2l[src_code];
            if (mapped == UINT32_MAX) {
              mapped = dict.FindOrInsertString(
                  global_r_base + static_cast<uint32_t>(i),
                  col.dict->StringEntryAt(src_code));
              c2l[src_code] = mapped;
            }
            out_codes[i] = mapped;
          }
        }
        return;
      }
      if (col.encoding == ColumnarColumn::Encoding::kGathered) {
        const LSTableColumnDictionary* last_dict = nullptr;
        CompactColDict::BorrowedDictMap* last_bmap = nullptr;
        size_t last_ec = 0;
        for (size_t i = 0; i < take; ++i) {
          const uint32_t r = global_r_base + static_cast<uint32_t>(i);
          const ColumnarColumn* leaf = nullptr;
          size_t leaf_r = 0;
          if (!col.ResolveLeaf(lr_start + i, &leaf, &leaf_r)) {
            out_codes[i] = dict.FindOrInsertNull(r);
          } else if (leaf->encoding == ColumnarColumn::Encoding::kDictionary &&
                     leaf->dict != nullptr && !leaf->is_interval &&
                     leaf->type == ValueType::kVarChar) {
            if (leaf->dict != last_dict) {
              last_dict = leaf->dict;
              last_ec = last_dict->StringEntryCount();
              last_bmap = &dict.GetOrAddBorrowedMap(last_dict, last_ec);
            }
            const uint32_t src_code = leaf->DictCodeAt(leaf_r);
            if (src_code == ColumnarColumn::kNullDictCode ||
                src_code >= last_ec) {
              if (last_bmap->null_local == UINT32_MAX) {
                last_bmap->null_local = dict.FindOrInsertNull(r);
              }
              out_codes[i] = last_bmap->null_local;
            } else {
              uint32_t mapped = last_bmap->code_to_local[src_code];
              if (mapped == UINT32_MAX) {
                mapped = dict.FindOrInsertString(
                    r, last_dict->StringEntryAt(src_code));
                last_bmap->code_to_local[src_code] = mapped;
              }
              out_codes[i] = mapped;
            }
          } else {
            out_codes[i] = dict.FindOrInsertString(r, leaf->StringAt(leaf_r));
          }
        }
        return;
      }
      if (col.IsFlat()) {
        const std::string_view* strs = col.strings.data() + lr_start;
        if (!col.has_null) {
          for (size_t i = 0; i < take; ++i) {
            out_codes[i] = dict.FindOrInsertString(
                global_r_base + static_cast<uint32_t>(i), strs[i]);
          }
        } else {
          const uint8_t* nulls = col.nulls.data() + lr_start;
          for (size_t i = 0; i < take; ++i) {
            const uint32_t r = global_r_base + static_cast<uint32_t>(i);
            out_codes[i] = nulls[i] != 0 ? dict.FindOrInsertNull(r)
                                         : dict.FindOrInsertString(r, strs[i]);
          }
        }
      } else {
        for (size_t i = 0; i < take; ++i) {
          const uint32_t r = global_r_base + static_cast<uint32_t>(i);
          const size_t lr = lr_start + i;
          out_codes[i] = col.IsNull(lr)
                             ? dict.FindOrInsertNull(r)
                             : dict.FindOrInsertString(r, col.StringAt(lr));
        }
      }
      return;
    }
  }
  Value tmp;
  for (size_t i = 0; i < take; ++i) {
    col.WriteValueInto(lr_start + i, &tmp);
    out_codes[i] = dict.FindOrInsertStableValue(
        global_r_base + static_cast<uint32_t>(i), tmp);
  }
}

// Flat argument kinds used to pack numeric/COUNT intermediates cleanly:
// 0 = NULL, 1 = double value in flat_double_args, 2 = exact INT64 0,
// 3 = COUNT non-null 1.
inline Value FlatArgToValue(double val, uint8_t arg_kind) {
  switch (arg_kind) {
    case 1:
      return Value(val);
    case 2:
      return Value(int64_t{0});
    case 3:
      return Value(int64_t{1});
    default:
      return {};
  }
}

inline bool ApplyFlatArg(TypedWorkerGroups* local, size_t group_index, size_t a,
                         SlimAggKind agg_kind, double val, uint8_t arg_kind,
                         Status* error) {
  if (arg_kind == 0) {
    return true;
  }
  if (agg_kind == SlimAggKind::kCount) {
    ++local->SlimCount(group_index, a);
    return true;
  }
  if (agg_kind == SlimAggKind::kSumDouble) {
    if (!local->mixed_numeric_sums.empty() && local->mixed_numeric_sums[a]) {
      if (arg_kind == 2) {
        ++local->SlimCount(group_index, a);
        return true;
      }
      local->SlimFlags(group_index, a) |= 2;
    }
    if (std::isinf(val)) {
      local->SlimFlags(group_index, a) |= 1;
    }
    local->SlimDoubleTotal(group_index, a) += val;
    ++local->SlimCount(group_index, a);
    return true;
  }
  if (agg_kind == SlimAggKind::kAvgDouble) {
    local->SlimDoubleTotal(group_index, a) += val;
    ++local->SlimCount(group_index, a);
    return true;
  }
  *error = StatusError(StatusCode::kInvalidArgument,
                       "flat double argument mismatch");
  return false;
}

bool EvalDoubleExprChunkSlice(const TypedDoubleExprInput& expr,
                              const ColumnarChunk& ch, size_t lr_start,
                              size_t take, double* out_vals, uint8_t* out_valid,
                              Row* scratch_row) {
  bool all_uniform = true;
  bool any_all_null = false;
  bool all_flat = true;
  for (const TypedDoubleExprOp& op : expr.ops) {
    if (op.kind == TypedDoubleExprOp::Kind::kLoadDoubleCol) {
      const ColumnarColumn& c = ch.columns[op.column];
      if (!c.is_uniform) {
        all_uniform = false;
        break;
      }
      if (!c.IsFlat()) {
        all_flat = false;
      }
      if (c.type == ValueType::kNull ||
          (c.encoding == ColumnarColumn::Encoding::kConstant &&
           c.const_is_null)) {
        any_all_null = true;
      } else if (c.type != ValueType::kDouble) {
        all_uniform = false;
        break;
      }
    } else if (op.kind == TypedDoubleExprOp::Kind::kLoadIntColAsDouble) {
      const ColumnarColumn& c = ch.columns[op.column];
      if (!c.is_uniform) {
        all_uniform = false;
        break;
      }
      if (!c.IsFlat()) {
        all_flat = false;
      }
      if (c.type == ValueType::kNull ||
          (c.encoding == ColumnarColumn::Encoding::kConstant &&
           c.const_is_null)) {
        any_all_null = true;
      } else if (c.type != ValueType::kInt64 || c.is_unsigned) {
        all_uniform = false;
        break;
      }
    }
  }
  if (all_uniform) {
    if (any_all_null) {
      std::fill_n(out_valid, take, uint8_t{0});
      return true;
    }
    if (all_flat) {
      if (expr.shape == TypedDoubleExprInput::Shape::kSingleDoubleCol) {
        const ColumnarColumn& c0 = ch.columns[expr.col0];
        const double* d0 = c0.doubles.data() + lr_start;
        if (!c0.has_null) {
          std::memcpy(out_vals, d0, take * sizeof(double));
          std::fill_n(out_valid, take, uint8_t{1});
        } else {
          const uint8_t* n0 = c0.nulls.data() + lr_start;
          for (size_t i = 0; i < take; ++i) {
            out_vals[i] = d0[i];
            out_valid[i] = n0[i] == 0 ? uint8_t{1} : uint8_t{0};
          }
        }
        return true;
      }
      if (expr.shape == TypedDoubleExprInput::Shape::kMulTwoDoubleCols) {
        const ColumnarColumn& c0 = ch.columns[expr.col0];
        const ColumnarColumn& c1 = ch.columns[expr.col1];
        const double* d0 = c0.doubles.data() + lr_start;
        const double* d1 = c1.doubles.data() + lr_start;
        if (!c0.has_null && !c1.has_null) {
          for (size_t i = 0; i < take; ++i) {
            out_vals[i] = d0[i] * d1[i];
            out_valid[i] = 1;
          }
        } else {
          const uint8_t* n0 =
              c0.has_null ? c0.nulls.data() + lr_start : nullptr;
          const uint8_t* n1 =
              c1.has_null ? c1.nulls.data() + lr_start : nullptr;
          for (size_t i = 0; i < take; ++i) {
            const bool missing =
                (n0 != nullptr && n0[i] != 0) || (n1 != nullptr && n1[i] != 0);
            out_vals[i] = missing ? 0.0 : (d0[i] * d1[i]);
            out_valid[i] = missing ? uint8_t{0} : uint8_t{1};
          }
        }
        return true;
      }
      if (expr.shape == TypedDoubleExprInput::Shape::kMulConstMinusCol) {
        const ColumnarColumn& c0 = ch.columns[expr.col0];
        const ColumnarColumn& c1 = ch.columns[expr.col1];
        const double* d0 = c0.doubles.data() + lr_start;
        const double* d1 = c1.doubles.data() + lr_start;
        const double k0 = expr.k0;
        if (!c0.has_null && !c1.has_null) {
          for (size_t i = 0; i < take; ++i) {
            out_vals[i] = d0[i] * (k0 - d1[i]);
            out_valid[i] = 1;
          }
        } else {
          const uint8_t* n0 =
              c0.has_null ? c0.nulls.data() + lr_start : nullptr;
          const uint8_t* n1 =
              c1.has_null ? c1.nulls.data() + lr_start : nullptr;
          for (size_t i = 0; i < take; ++i) {
            const bool missing =
                (n0 != nullptr && n0[i] != 0) || (n1 != nullptr && n1[i] != 0);
            out_vals[i] = missing ? 0.0 : (d0[i] * (k0 - d1[i]));
            out_valid[i] = missing ? uint8_t{0} : uint8_t{1};
          }
        }
        return true;
      }
      if (expr.ops.size() == 3 &&
          expr.ops[0].kind == TypedDoubleExprOp::Kind::kLoadDoubleCol &&
          expr.ops[1].kind == TypedDoubleExprOp::Kind::kLoadIntColAsDouble &&
          expr.ops[2].kind == TypedDoubleExprOp::Kind::kMul) {
        const ColumnarColumn& c0 = ch.columns[expr.ops[0].column];
        const ColumnarColumn& c1 = ch.columns[expr.ops[1].column];
        const double* d0 = c0.doubles.data() + lr_start;
        const int64_t* i1 = c1.ints.data() + lr_start;
        if (!c0.has_null && !c1.has_null) {
          for (size_t i = 0; i < take; ++i) {
            out_vals[i] = d0[i] * static_cast<double>(i1[i]);
            out_valid[i] = 1;
          }
        } else {
          const uint8_t* n0 =
              c0.has_null ? c0.nulls.data() + lr_start : nullptr;
          const uint8_t* n1 =
              c1.has_null ? c1.nulls.data() + lr_start : nullptr;
          for (size_t i = 0; i < take; ++i) {
            const bool missing =
                (n0 != nullptr && n0[i] != 0) || (n1 != nullptr && n1[i] != 0);
            out_vals[i] = missing ? 0.0 : (d0[i] * static_cast<double>(i1[i]));
            out_valid[i] = missing ? uint8_t{0} : uint8_t{1};
          }
        }
        return true;
      }
    } else {
      if (expr.shape == TypedDoubleExprInput::Shape::kSingleDoubleCol) {
        const ColumnarColumn& c0 = ch.columns[expr.col0];
        if (!c0.has_null) {
          for (size_t i = 0; i < take; ++i) {
            out_vals[i] = c0.DoubleAt(lr_start + i);
            out_valid[i] = 1;
          }
        } else {
          for (size_t i = 0; i < take; ++i) {
            const size_t lr = lr_start + i;
            if (c0.IsNull(lr)) {
              out_vals[i] = 0.0;
              out_valid[i] = 0;
            } else {
              out_vals[i] = c0.DoubleAt(lr);
              out_valid[i] = 1;
            }
          }
        }
        return true;
      }
      if (expr.shape == TypedDoubleExprInput::Shape::kMulTwoDoubleCols) {
        const ColumnarColumn& c0 = ch.columns[expr.col0];
        const ColumnarColumn& c1 = ch.columns[expr.col1];
        for (size_t i = 0; i < take; ++i) {
          const size_t lr = lr_start + i;
          if ((c0.has_null && c0.IsNull(lr)) ||
              (c1.has_null && c1.IsNull(lr))) {
            out_vals[i] = 0.0;
            out_valid[i] = 0;
          } else {
            out_vals[i] = c0.DoubleAt(lr) * c1.DoubleAt(lr);
            out_valid[i] = 1;
          }
        }
        return true;
      }
      if (expr.shape == TypedDoubleExprInput::Shape::kMulConstMinusCol) {
        const ColumnarColumn& c0 = ch.columns[expr.col0];
        const ColumnarColumn& c1 = ch.columns[expr.col1];
        const double k0 = expr.k0;
        for (size_t i = 0; i < take; ++i) {
          const size_t lr = lr_start + i;
          if ((c0.has_null && c0.IsNull(lr)) ||
              (c1.has_null && c1.IsNull(lr))) {
            out_vals[i] = 0.0;
            out_valid[i] = 0;
          } else {
            out_vals[i] = c0.DoubleAt(lr) * (k0 - c1.DoubleAt(lr));
            out_valid[i] = 1;
          }
        }
        return true;
      }
      if (expr.ops.size() == 3 &&
          expr.ops[0].kind == TypedDoubleExprOp::Kind::kLoadDoubleCol &&
          expr.ops[1].kind == TypedDoubleExprOp::Kind::kLoadIntColAsDouble &&
          expr.ops[2].kind == TypedDoubleExprOp::Kind::kMul) {
        const ColumnarColumn& c0 = ch.columns[expr.ops[0].column];
        const ColumnarColumn& c1 = ch.columns[expr.ops[1].column];
        for (size_t i = 0; i < take; ++i) {
          const size_t lr = lr_start + i;
          if ((c0.has_null && c0.IsNull(lr)) ||
              (c1.has_null && c1.IsNull(lr))) {
            out_vals[i] = 0.0;
            out_valid[i] = 0;
          } else {
            out_vals[i] = c0.DoubleAt(lr) * static_cast<double>(c1.IntAt(lr));
            out_valid[i] = 1;
          }
        }
        return true;
      }
    }
    std::array<double, 16> stack{};
    // Producers cap ops at 16 entries so pushes can never exceed the stack;
    // keep that bound explicit here instead of relying on the upstream
    // compiler invariant.
    if (expr.ops.size() > stack.size()) {
      return false;
    }
    for (size_t i = 0; i < take; ++i) {
      const size_t lr = lr_start + i;
      size_t sp = 0;
      bool valid = true;
      for (const TypedDoubleExprOp& op : expr.ops) {
        switch (op.kind) {
          case TypedDoubleExprOp::Kind::kLoadDoubleCol: {
            const ColumnarColumn& c = ch.columns[op.column];
            if (c.has_null && c.IsNull(lr)) {
              valid = false;
            } else {
              stack[sp++] = c.DoubleAt(lr);
            }
            break;
          }
          case TypedDoubleExprOp::Kind::kLoadIntColAsDouble: {
            const ColumnarColumn& c = ch.columns[op.column];
            if (c.has_null && c.IsNull(lr)) {
              valid = false;
            } else {
              stack[sp++] = static_cast<double>(c.IntAt(lr));
            }
            break;
          }
          case TypedDoubleExprOp::Kind::kConstant:
            stack[sp++] = op.constant;
            break;
          case TypedDoubleExprOp::Kind::kAdd:
            stack[sp - 2] += stack[sp - 1];
            --sp;
            break;
          case TypedDoubleExprOp::Kind::kSub:
            stack[sp - 2] -= stack[sp - 1];
            --sp;
            break;
          case TypedDoubleExprOp::Kind::kMul:
            stack[sp - 2] *= stack[sp - 1];
            --sp;
            break;
        }
        if (!valid) {
          break;
        }
      }
      out_vals[i] = valid ? stack[0] : 0.0;
      out_valid[i] = valid ? uint8_t{1} : uint8_t{0};
    }
    return true;
  }
  for (size_t i = 0; i < take; ++i) {
    ch.MaterializeRowInto(lr_start + i, scratch_row);
    const Value v = EvaluateTypedDoubleExprOnRow(expr, *scratch_row);
    if (v.IsNull()) {
      out_vals[i] = 0.0;
      out_valid[i] = 0;
    } else if (v.type == ValueType::kDouble) {
      out_vals[i] = v.value.double_value;
      out_valid[i] = 1;
    } else {
      return false;
    }
  }
  return true;
}

StatusOr<bool> TryBuildPackedGroupingSets(
    const Relation& input, const std::vector<uint32_t>& chunk_prefix,
    const std::vector<TypedAggregationPlan>& plans,
    const std::vector<bool>& mixed_numeric_sums, size_t agg_workers,
    size_t arg_chunk, size_t argument_count, bool all_direct_args,
    const std::vector<std::vector<Value>>& worker_arguments,
    const std::vector<double>& flat_double_args,
    const std::vector<uint8_t>& flat_arg_kinds, bool exact_partial_merge,
    bool finite_sum, bool all_double, bool no_nulls, bool finest_keys_unique,
    std::vector<TypedWorkerGroups>& groups, std::vector<size_t>& row_groups,
    std::vector<std::vector<size_t>>& coarse_maps) {
  const bool is_columnar = input.HasColumnarOnly();
  const ColumnarTable* columnar = is_columnar ? input.columnar.get() : nullptr;
  const size_t nrows = input.TotalRows();
  const bool use_flat_args = !flat_double_args.empty();
  const bool need_row_groups = !row_groups.empty();
  const size_t count = plans.size();
  std::vector<slot_t> unique_cols;
  for (const auto& plan : plans) {
    for (const slot_t col : plan.key_offsets) {
      if (std::ranges::find(unique_cols, col) == unique_cols.end()) {
        unique_cols.push_back(col);
      }
    }
  }
  const size_t kcols = unique_cols.size();
  if (kcols == 0 || kcols > 32) {
    return false;
  }
  std::shared_ptr<QueryMemoryCharge> dict_charge;
  const size_t code_bytes =
      nrows * (kcols * sizeof(uint32_t) + sizeof(uint64_t));
  if (nrows <= SIZE_MAX / (kcols * sizeof(uint32_t) + sizeof(uint64_t))) {
    dict_charge = TryReserveQueryMemory(code_bytes);
  }
  if (!dict_charge) {
    return false;
  }
  const bool dbg_phases = std::getenv("TINYLAMB_GROUP_PHASES") != nullptr;
  const auto t_p0 = std::chrono::steady_clock::now();

  const size_t finest_workers =
      std::max<size_t>(1, std::min<size_t>(8, agg_workers));
  std::vector<std::vector<CompactColDict>> local_dicts(
      finest_workers, std::vector<CompactColDict>(kcols));
  std::vector<std::vector<std::vector<uint32_t>>> worker_codes(
      finest_workers, std::vector<std::vector<uint32_t>>(kcols));

  RETURN_IF_FAIL(RunParallelTasks(finest_workers, [&](size_t w) {
    const size_t w_chunk = nrows / finest_workers;
    const size_t begin = w * w_chunk;
    const size_t end = w + 1 == finest_workers ? nrows : begin + w_chunk;
    const size_t len = end - begin;
    for (size_t k = 0; k < kcols; ++k) {
      worker_codes[w][k].resize(len);
    }
    if (is_columnar) {
      if (len == 0) {
        return;
      }
      size_t ci_hint = 0;
      auto [ci, lr_start] = TypedWorkerGroups::LocateColumnarChunkRow(
          chunk_prefix, static_cast<uint32_t>(begin), &ci_hint);
      size_t rem = len;
      size_t out_off = 0;
      constexpr size_t kTile = 1024;
      while (rem > 0) {
        const ColumnarChunk& ch = columnar->chunks[ci];
        const size_t chunk_take = std::min(rem, ch.rows - lr_start);
        for (size_t tile = 0; tile < chunk_take; tile += kTile) {
          const size_t tile_take = std::min(kTile, chunk_take - tile);
          const auto r_base = static_cast<uint32_t>(begin + out_off + tile);
          for (size_t k = 0; k < kcols; ++k) {
            const slot_t col = unique_cols[k];
            EncodeColumnarSliceToDict(
                local_dicts[w][k], ch.columns[col], lr_start + tile, tile_take,
                r_base, worker_codes[w][k].data() + out_off + tile);
          }
        }
        rem -= chunk_take;
        out_off += chunk_take;
        ++ci;
        lr_start = 0;
      }
      return;
    }
    constexpr size_t kTile = 1024;
    for (size_t tile = 0; tile < len; tile += kTile) {
      const size_t tile_end = std::min(len, tile + kTile);
      for (size_t k = 0; k < kcols; ++k) {
        const slot_t col = unique_cols[k];
        CompactColDict& dict = local_dicts[w][k];
        uint32_t* const out_codes = worker_codes[w][k].data();
        for (size_t lr = tile; lr < tile_end; ++lr) {
          const auto r = static_cast<uint32_t>(begin + lr);
          out_codes[lr] =
              dict.FindOrInsertCell(input.rows, r, col, input.rows[r][col]);
        }
      }
    }
  }));

  // Early bit-budget bound: if even per-worker distinct counts already exceed
  // 64 bits combined, global dictionary merging cannot fit in uint64_t.
  size_t lower_bits = 0;
  for (size_t k = 0; k < kcols; ++k) {
    size_t max_local = 1;
    for (size_t w = 0; w < finest_workers; ++w) {
      max_local = std::max(max_local, local_dicts[w][k].reps.size());
    }
    if (max_local > 1) {
      lower_bits += static_cast<size_t>(std::bit_width(max_local - 1));
    }
  }
  if (lower_bits > 64) {
    return false;
  }
  const auto t_p1 = std::chrono::steady_clock::now();

  // Merge per-column local dictionaries into global dictionaries in parallel.
  std::vector<std::vector<std::vector<uint32_t>>> local_to_global(
      finest_workers, std::vector<std::vector<uint32_t>>(kcols));
  std::vector<size_t> col_cards(kcols, 1);
  auto merge_column = [&](size_t k) {
    const slot_t col = unique_cols[k];
    bool any_ci = false;
    size_t total_local = 0;
    for (size_t w = 0; w < finest_workers; ++w) {
      any_ci = any_ci || local_dicts[w][k].saw_ci;
      total_local += local_dicts[w][k].reps.size();
    }
    CompactColDict global;
    global.Init(std::min<size_t>(total_local, 65536));
    global.reps.reserve(local_dicts[0][k].reps.size());
    for (size_t w = 0; w < finest_workers; ++w) {
      const auto& ld = local_dicts[w][k];
      auto& remap = local_to_global[w][k];
      remap.resize(ld.reps.size());
      if (is_columnar) {
        size_t chunk_hint = 0;
        for (size_t i = 0; i < ld.reps.size(); ++i) {
          const uint32_t rep = ld.reps[i];
          const auto [ci, lr] = TypedWorkerGroups::LocateColumnarChunkRow(
              chunk_prefix, rep, &chunk_hint);
          remap[i] = global.FindOrInsertColumnarGlobal(
              columnar->chunks[ci].columns[col], lr, rep, ld.hashes[i],
              !any_ci);
        }
      } else {
        for (size_t i = 0; i < ld.reps.size(); ++i) {
          const uint32_t rep = ld.reps[i];
          const size_t h = ld.hashes[i];
          remap[i] = any_ci
                         ? global.FindOrInsertCell(input.rows, rep, col,
                                                   input.rows[rep][col])
                         : global.FindOrInsertPrehashed(input.rows, rep, col, h,
                                                        input.rows[rep][col]);
        }
      }
    }
    col_cards[k] = global.reps.size();
  };
  if (finest_workers > 1 && kcols > 1) {
    std::atomic<size_t> next_k{0};
    RETURN_IF_FAIL(
        RunParallelTasks(std::min(kcols, finest_workers), [&](size_t) {
          for (size_t k = next_k.fetch_add(1); k < kcols;
               k = next_k.fetch_add(1)) {
            merge_column(k);
          }
        }));
  } else {
    for (size_t k = 0; k < kcols; ++k) {
      merge_column(k);
    }
  }
  std::vector<std::vector<CompactColDict>>().swap(local_dicts);

  std::vector<size_t> col_bits(kcols, 0);
  std::vector<size_t> col_shifts(kcols, 0);
  size_t total_bits = 0;
  for (size_t k = 0; k < kcols; ++k) {
    if (col_cards[k] > 1) {
      col_bits[k] = static_cast<size_t>(std::bit_width(col_cards[k] - 1));
      if (col_bits[k] > 32) {
        return false;
      }
      col_shifts[k] = total_bits;
      total_bits += col_bits[k];
    }
  }
  if (total_bits > 64) {
    return false;
  }
  const auto t_p2 = std::chrono::steady_clock::now();

  if (is_columnar) {
    for (size_t s = 0; s < count; ++s) {
      groups[s].borrowed_columnar = columnar;
      groups[s].borrowed_chunk_prefix = &chunk_prefix;
    }
  }

  std::vector<uint64_t> set_masks(count, 0);
  for (size_t s = 0; s < count; ++s) {
    uint64_t set_mask = 0;
    for (const slot_t col : plans[s].key_offsets) {
      const auto it = std::ranges::find(unique_cols, col);
      const auto k =
          static_cast<size_t>(std::distance(unique_cols.begin(), it));
      if (col_bits[k] > 0) {
        set_mask |= ((uint64_t{1} << col_bits[k]) - 1ULL) << col_shifts[k];
      }
    }
    set_masks[s] = set_mask;
  }

  const auto direct_arg_at = [&](size_t r, size_t a,
                                 size_t* hint = nullptr) -> Value {
    const auto& item = plans[0].aggregates[a];
    Value cell;
    if (is_columnar) {
      size_t local_hint = hint != nullptr ? *hint : 0;
      const auto [ci, lr] = TypedWorkerGroups::LocateColumnarChunkRow(
          chunk_prefix, static_cast<uint32_t>(r), &local_hint);
      if (hint != nullptr) {
        *hint = local_hint;
      }
      columnar->chunks[ci].columns[*item.column].WriteValueInto(lr, &cell);
    } else {
      cell = input.rows[r][*item.column];
    }
    if (plans[0].slim_kinds[a] == SlimAggKind::kCount) {
      return cell.IsNull() ? Value() : Value(1);
    }
    return cell;
  };
  const auto add_finite_sum = [no_nulls](TypedWorkerGroups& local, size_t group,
                                         const Value& value) {
    if (!no_nulls && !value.IsNull()) {
      local.SlimCount(group, 0) = 1;
    }
    if (value.type == ValueType::kDouble) {
      local.SlimDoubleTotal(group, 0) += value.value.double_value;
      local.SlimFlags(group, 0) |= 2;
    }
  };
  const auto add_finite_sum_flat = [no_nulls](TypedWorkerGroups& local,
                                              size_t group, double val,
                                              uint8_t kind) {
    if (!no_nulls && kind != 0) {
      local.SlimCount(group, 0) = 1;
    }
    if (kind == 1) {
      local.SlimDoubleTotal(group, 0) += val;
      local.SlimFlags(group, 0) |= 2;
    }
  };

  // Trivial single-group path when every grouping column has cardinality 1.
  if (set_masks[0] == 0) {
    std::vector<std::vector<std::vector<uint32_t>>>().swap(worker_codes);
    std::vector<std::vector<std::vector<uint32_t>>>().swap(local_to_global);
    groups[0].ReserveGroups(1);
    groups[0].group_count = 1;
    if (is_columnar) {
      groups[0].borrowed_key_row_ids.push_back(0);
    } else {
      groups[0].borrowed_key_rows.push_back(input.rows.data());
    }
    if (exact_partial_merge) {
      std::vector<TypedWorkerGroups> partials(finest_workers);
      for (auto& partial : partials) {
        partial.Configure(true, plans[0].key_offsets.size(), argument_count);
        partial.ConfigureSlim(plans[0]);
        partial.mixed_numeric_sums = mixed_numeric_sums;
        partial.ReserveGroups(1);
        partial.group_count = 1;
      }
      std::vector<Status> errors(finest_workers, Status::kSuccess);
      std::vector<size_t> error_rows(finest_workers, SIZE_MAX);
      RETURN_IF_FAIL(RunParallelTasks(finest_workers, [&](size_t w) {
        const size_t w_chunk = nrows / finest_workers;
        const size_t begin = w * w_chunk;
        const size_t end = w + 1 == finest_workers ? nrows : begin + w_chunk;
        auto& partial = partials[w];
        size_t chunk_hint = 0;
        for (size_t r = begin; r < end; ++r) {
          for (size_t a = 0; a < argument_count; ++a) {
            if (use_flat_args) {
              const size_t idx = (r * argument_count) + a;
              if (!ApplyFlatArg(&partial, 0, a, plans[0].slim_kinds[a],
                                flat_double_args[idx], flat_arg_kinds[idx],
                                &errors[w])) {
                error_rows[w] = r;
                return;
              }
            } else {
              Value arg_val =
                  all_direct_args
                      ? direct_arg_at(r, a, &chunk_hint)
                      : worker_arguments[r / arg_chunk]
                                        [((r % arg_chunk) * argument_count) +
                                         a];
              if (!ApplySlimValue(&partial, 0, a, plans[0].slim_kinds[a],
                                  &arg_val, &errors[w])) {
                error_rows[w] = r;
                return;
              }
            }
          }
        }
      }));
      const auto first_error = std::ranges::min_element(error_rows);
      if (*first_error != SIZE_MAX) {
        return errors[static_cast<size_t>(first_error - error_rows.begin())];
      }
      Status merge_err = Status::kSuccess;
      for (size_t w = 0; w < finest_workers; ++w) {
        for (size_t a = 0; a < argument_count; ++a) {
          if (!MergeSlimCell(groups.data(), 0, partials[w], 0, a,
                             plans[0].slim_kinds[a], &merge_err)) {
            return merge_err;
          }
        }
      }
    } else if (finite_sum && all_double) {
      double total = 0.0;
      const std::optional<slot_t> agg_col = plans[0].aggregates[0].column;
      if (all_direct_args && agg_col.has_value()) {
        const slot_t col = *agg_col;
        if (is_columnar) {
          for (const ColumnarChunk& ch : columnar->chunks) {
            const ColumnarColumn& c = ch.columns[col];
            if (c.IsFlat() && c.is_uniform && c.type == ValueType::kDouble) {
              const double* d = c.doubles.data();
              for (size_t lr = 0; lr < ch.rows; ++lr) {
                total += d[lr];
              }
            } else if (c.is_uniform && c.type == ValueType::kDouble &&
                       !c.has_null) {
              for (size_t lr = 0; lr < ch.rows; ++lr) {
                total += c.DoubleAt(lr);
              }
            } else {
              Value tmp;
              for (size_t lr = 0; lr < ch.rows; ++lr) {
                c.WriteValueInto(lr, &tmp);
                total += tmp.value.double_value;
              }
            }
          }
        } else {
          for (size_t r = 0; r < nrows; ++r) {
            total += input.rows[r][col].value.double_value;
          }
        }
      } else if (use_flat_args) {
        for (size_t r = 0; r < nrows; ++r) {
          total += flat_double_args[r];
        }
      } else {
        for (const auto& w_chunk_vals : worker_arguments) {
          for (const Value& value : w_chunk_vals) {
            total += value.value.double_value;
          }
        }
      }
      groups[0].SlimDoubleTotal(0, 0) = total;
      groups[0].SlimFlags(0, 0) |= 2;
      if (need_row_groups) {
        row_groups.assign(nrows, 0);
      }
    } else {
      size_t chunk_hint = 0;
      for (size_t r = 0; r < nrows; ++r) {
        if (finite_sum) {
          if (use_flat_args) {
            add_finite_sum_flat(groups[0], 0, flat_double_args[r],
                                flat_arg_kinds[r]);
          } else {
            const Value arg_val =
                all_direct_args
                    ? direct_arg_at(r, 0, &chunk_hint)
                    : worker_arguments[r / arg_chunk]
                                      [(r % arg_chunk) * argument_count];
            add_finite_sum(groups[0], 0, arg_val);
          }
        } else {
          for (size_t a = 0; a < argument_count; ++a) {
            if (use_flat_args) {
              const size_t idx = (r * argument_count) + a;
              Status error{Status::kSuccess};
              if (!ApplyFlatArg(groups.data(), 0, a, plans[0].slim_kinds[a],
                                flat_double_args[idx], flat_arg_kinds[idx],
                                &error)) {
                return error;
              }
            } else {
              Value arg_val =
                  all_direct_args
                      ? direct_arg_at(r, a, &chunk_hint)
                      : worker_arguments[r / arg_chunk]
                                        [((r % arg_chunk) * argument_count) +
                                         a];
              Status error{Status::kSuccess};
              if (!ApplySlimValue(groups.data(), 0, a, plans[0].slim_kinds[a],
                                  &arg_val, &error)) {
                return error;
              }
            }
          }
        }
      }
      if (need_row_groups) {
        row_groups.assign(nrows, 0);
      }
    }
    for (size_t s = 1; s < count; ++s) {
      groups[s].ReserveGroups(1);
      groups[s].group_count = 1;
      if (is_columnar) {
        groups[s].borrowed_key_row_ids.push_back(0);
      } else {
        groups[s].borrowed_key_rows.push_back(input.rows.data());
      }
      coarse_maps[s].assign(1, 0);
    }
    return true;
  }

  const bool use_row_ids = finest_keys_unique && finite_sum && all_direct_args;
  // Pass 3: Pack the keys and distribute duplicate keys when they need a
  // finest-level aggregation.
  std::vector<uint64_t> packed_all(nrows);
  std::vector<uint8_t> row_buckets(need_row_groups && !use_row_ids ? nrows : 0);
  std::vector<std::vector<std::vector<uint32_t>>> chunk_buckets(
      use_row_ids ? 0 : finest_workers,
      std::vector<std::vector<uint32_t>>(finest_workers));
  RETURN_IF_FAIL(RunParallelTasks(finest_workers, [&](size_t w) {
    const size_t w_chunk = nrows / finest_workers;
    const size_t begin = w * w_chunk;
    const size_t end = w + 1 == finest_workers ? nrows : begin + w_chunk;
    const size_t len = end - begin;
    if (!use_row_ids) {
      for (auto& bucket : chunk_buckets[w]) {
        bucket.reserve((len / finest_workers) + 16);
      }
    }
    std::array<const uint32_t*, 32> col_ptr{};
    std::array<const uint32_t*, 32> remap_ptr{};
    std::array<size_t, 32> shifts{};
    size_t active_cols = 0;
    for (size_t k = 0; k < kcols; ++k) {
      if (col_bits[k] > 0) {
        col_ptr[active_cols] = worker_codes[w][k].data();
        remap_ptr[active_cols] = local_to_global[w][k].data();
        shifts[active_cols] = col_shifts[k];
        ++active_cols;
      }
    }
    const uint64_t finest_mask = set_masks[0];
    uint8_t* const rb_ptr = row_buckets.empty() ? nullptr : row_buckets.data();
    for (size_t lr = 0; lr < len; ++lr) {
      uint64_t pk = 0;
      for (size_t i = 0; i < active_cols; ++i) {
        pk |= static_cast<uint64_t>(remap_ptr[i][col_ptr[i][lr]]) << shifts[i];
      }
      const size_t r = begin + lr;
      packed_all[r] = pk;
      if (!use_row_ids) {
        const auto b = static_cast<uint8_t>(MixCompactInt64(pk & finest_mask) %
                                            finest_workers);
        if (rb_ptr != nullptr) {
          rb_ptr[r] = b;
        }
        chunk_buckets[w][b].push_back(static_cast<uint32_t>(r));
      }
    }
  }));
  std::vector<std::vector<std::vector<uint32_t>>>().swap(worker_codes);
  std::vector<std::vector<std::vector<uint32_t>>>().swap(local_to_global);

  auto& finest = groups[0];
  std::vector<std::vector<uint64_t>> group_pks(count);
  size_t total_groups = 0;
  const auto aggregate_finest = [&]() -> Status {
    std::vector<TypedWorkerGroups> partials(finest_workers);
    for (auto& partial : partials) {
      partial.Configure(true, plans[0].key_offsets.size(), argument_count);
      partial.ConfigureSlim(plans[0]);
      partial.int_key = true;
      partial.borrowed_key_offsets = &plans[0].key_offsets;
      if (is_columnar) {
        partial.borrowed_columnar = columnar;
        partial.borrowed_chunk_prefix = &chunk_prefix;
      }
      partial.mixed_numeric_sums = mixed_numeric_sums;
    }
    std::vector<uint32_t> local_groups(need_row_groups ? nrows : 0);
    std::vector<std::vector<size_t>> first_rows(finest_workers);
    std::vector<Status> errors(finest_workers, Status::kSuccess);
    std::vector<size_t> error_rows(finest_workers, SIZE_MAX);
    RETURN_IF_FAIL(RunParallelTasks(finest_workers, [&](size_t w) {
      auto& partial = partials[w];
      auto& first = first_rows[w];
      size_t bucket_rows = 0;
      for (size_t src = 0; src < finest_workers; ++src) {
        bucket_rows += chunk_buckets[src][w].size();
      }
      partial.int_index.Reserve(bucket_rows >= 4096 && kcols >= 3
                                    ? (bucket_rows * 3) / 4
                                    : std::min<size_t>(bucket_rows, 8192));
      first.reserve(std::min<size_t>(bucket_rows, 4096));
      const uint64_t finest_mask = set_masks[0];
      uint32_t* const lg_ptr = need_row_groups ? local_groups.data() : nullptr;
      const double* const flat_v =
          use_flat_args ? flat_double_args.data() : nullptr;
      const uint8_t* const flat_k =
          use_flat_args ? flat_arg_kinds.data() : nullptr;
      int64_t prev_pk = 0;
      size_t prev_group = 0;
      bool has_prev = false;
      for (size_t src = 0; src < finest_workers; ++src) {
        size_t chunk_hint = 0;
        for (const uint32_t r32 : chunk_buckets[src][w]) {
          const size_t r = r32;
          const auto pk = static_cast<int64_t>(packed_all[r] & finest_mask);
          size_t group = 0;
          if (has_prev && pk == prev_pk) {
            group = prev_group;
          } else {
            bool inserted = false;
            size_t* slot = partial.int_index.TryEmplace(pk, &inserted);
            if (inserted) {
              *slot = partial.GroupCount();
              partial.AppendSlimOnlyGroup();
              first.push_back(r);
            }
            group = *slot;
            prev_pk = pk;
            prev_group = group;
            has_prev = true;
          }
          if (lg_ptr != nullptr) {
            lg_ptr[r] = static_cast<uint32_t>(group);
          }
          if (finite_sum) {
            if (use_flat_args) {
              add_finite_sum_flat(partial, group, flat_v[r], flat_k[r]);
            } else {
              const Value arg_val =
                  all_direct_args
                      ? direct_arg_at(r, 0, &chunk_hint)
                      : worker_arguments[r / arg_chunk]
                                        [(r % arg_chunk) * argument_count];
              add_finite_sum(partial, group, arg_val);
            }
          } else {
            for (size_t a = 0; a < argument_count; ++a) {
              if (use_flat_args) {
                const size_t idx = (r * argument_count) + a;
                if (!ApplyFlatArg(&partial, group, a, plans[0].slim_kinds[a],
                                  flat_v[idx], flat_k[idx], &errors[w])) {
                  error_rows[w] = r;
                  return;
                }
              } else {
                Value arg_val =
                    all_direct_args
                        ? direct_arg_at(r, a, &chunk_hint)
                        : worker_arguments[r / arg_chunk]
                                          [((r % arg_chunk) * argument_count) +
                                           a];
                if (!ApplySlimValue(&partial, group, a, plans[0].slim_kinds[a],
                                    &arg_val, &errors[w])) {
                  error_rows[w] = r;
                  return;
                }
              }
            }
          }
        }
      }
    }));
    std::vector<std::vector<std::vector<uint32_t>>>().swap(chunk_buckets);
    const auto first_error = std::ranges::min_element(error_rows);
    if (*first_error != SIZE_MAX) {
      return errors[static_cast<size_t>(first_error - error_rows.begin())];
    }

    std::vector<std::vector<size_t>> global_ids(need_row_groups ? finest_workers
                                                                : 0);
    std::vector<size_t> active_w;
    active_w.reserve(finest_workers);
    for (size_t w = 0; w < finest_workers; ++w) {
      if (need_row_groups) {
        global_ids[w].resize(partials[w].GroupCount(), SIZE_MAX);
      }
      total_groups += partials[w].GroupCount();
      if (!first_rows[w].empty()) {
        active_w.push_back(w);
      }
    }
    finest.ReserveGroups(total_groups);
    finest.group_count = total_groups;
    group_pks[0].resize(total_groups);
    std::vector<GlobalSourceSlot> global_src;
    if (total_groups > 0) {
      PopulateLocalOrderedPks(
          active_w, active_w.size(), first_rows, packed_all,
          need_row_groups ? &global_ids : nullptr,
          is_columnar ? nullptr : &finest.borrowed_key_rows,
          is_columnar ? &finest.borrowed_key_row_ids : nullptr,
          is_columnar ? nullptr : &input.rows, nullptr, nullptr, &global_src,
          group_pks[0]);
    }
    std::vector<uint64_t>().swap(packed_all);
    if (total_groups > 0) {
      const size_t copy_workers =
          total_groups >= 4096 ? finest_workers : size_t{1};
      RETURN_IF_FAIL(RunParallelTasks(copy_workers, [&](size_t w) {
        const size_t raw_begin = (total_groups * w) / copy_workers;
        const size_t raw_end = (total_groups * (w + 1)) / copy_workers;
        const size_t g_begin = w == 0 ? 0 : (raw_begin & ~size_t{63});
        const size_t g_end =
            w + 1 == copy_workers ? total_groups : (raw_end & ~size_t{63});
        for (size_t global = g_begin; global < g_end; ++global) {
          const GlobalSourceSlot src_slot = global_src[global];
          const auto& partial = partials[src_slot.worker];
          const size_t local = src_slot.local;
          for (size_t a = 0; a < argument_count; ++a) {
            finest.SlimCount(global, a) = partial.SlimCount(local, a);
            finest.SlimIntTotal(global, a) = partial.SlimIntTotal(local, a);
            finest.SlimDoubleTotal(global, a) =
                partial.SlimDoubleTotal(local, a);
            finest.SlimFlags(global, a) = partial.SlimFlags(local, a);
          }
        }
      }));
    }
    std::vector<GlobalSourceSlot>().swap(global_src);
    if (need_row_groups) {
      RETURN_IF_FAIL(RunParallelTasks(finest_workers, [&](size_t w) {
        const size_t begin = std::min(nrows, w * arg_chunk);
        const size_t end = std::min(nrows, begin + arg_chunk);
        for (size_t r = begin; r < end; ++r) {
          row_groups[r] = global_ids[row_buckets[r]][local_groups[r]];
        }
      }));
    }
    return Status::kSuccess;
  };
  if (use_row_ids) {
    // Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
    //   (unique_group_key_fiber_is_singleton)
    // The fused partial aggregate already emitted one row per finest key.
    // Use its row index as the group ID and keep the original row order.
    total_groups = nrows;
    finest.ReserveGroups(nrows);
    finest.group_count = nrows;
    if (is_columnar) {
      finest.borrowed_key_row_ids.resize(nrows);
    } else {
      finest.borrowed_key_rows.resize(nrows);
    }
    group_pks[0] = std::move(packed_all);
    RETURN_IF_FAIL(RunParallelTasks(finest_workers, [&](size_t worker) {
      const size_t chunk = nrows / finest_workers;
      const size_t begin = chunk * worker;
      const size_t end = worker + 1 == finest_workers ? nrows : begin + chunk;
      size_t chunk_hint = 0;
      for (size_t r = begin; r < end; ++r) {
        if (is_columnar) {
          finest.borrowed_key_row_ids[r] = static_cast<uint32_t>(r);
        } else {
          finest.borrowed_key_rows[r] = &input.rows[r];
        }
        add_finite_sum(finest, r, direct_arg_at(r, 0, &chunk_hint));
        if (need_row_groups) {
          row_groups[r] = r;
        }
      }
    }));
  } else {
    RETURN_IF_FAIL(aggregate_finest());
  }

  // Pass 5: Build coarser grouping sets from packed uint64 group keys.
  for (size_t s = 1; s < count; ++s) {
    size_t parent = 0;
    for (size_t candidate = 1; candidate < s; ++candidate) {
      if (groups[candidate].GroupCount() < groups[parent].GroupCount() &&
          std::ranges::all_of(plans[s].key_offsets, [&](slot_t key) {
            return std::ranges::find(plans[candidate].key_offsets, key) !=
                   plans[candidate].key_offsets.end();
          })) {
        parent = candidate;
      }
    }
    const size_t pg_count = groups[parent].GroupCount();
    if (set_masks[s] == 0 && pg_count > 0) {
      groups[s].ReserveGroups(1);
      groups[s].group_count = 1;
      if (is_columnar) {
        groups[s].borrowed_key_row_ids.push_back(
            groups[0].borrowed_key_row_ids[0]);
      } else {
        groups[s].borrowed_key_rows.push_back(groups[0].borrowed_key_rows[0]);
      }
      coarse_maps[s].assign(total_groups, 0);
      group_pks[s] = {0};
      continue;
    }
    if (set_masks[s] == set_masks[parent]) {
      groups[s].ReserveGroups(pg_count);
      groups[s].group_count = pg_count;
      if (is_columnar) {
        groups[s].borrowed_key_row_ids = groups[parent].borrowed_key_row_ids;
      } else {
        groups[s].borrowed_key_rows = groups[parent].borrowed_key_rows;
      }
      group_pks[s] = group_pks[parent];
      if (parent == 0) {
        coarse_maps[s].resize(pg_count);
        std::iota(coarse_maps[s].begin(), coarse_maps[s].end(), size_t{0});
      } else {
        coarse_maps[s] = coarse_maps[parent];
      }
      continue;
    }
    const uint64_t mask_s = set_masks[s];
    const auto& parent_pks = group_pks[parent];
    if (pg_count >= 16384 && agg_workers > 1) {
      const size_t cw = std::min<size_t>(8, agg_workers);
      std::vector<uint64_t> c_pks(pg_count);
      std::vector<uint8_t> c_row_bucket(pg_count);
      std::vector<std::vector<std::vector<size_t>>> c_buckets(
          cw, std::vector<std::vector<size_t>>(cw));
      RETURN_IF_FAIL(RunParallelTasks(cw, [&](size_t w) {
        const size_t begin = (pg_count * w) / cw;
        const size_t end = (pg_count * (w + 1)) / cw;
        for (auto& bucket : c_buckets[w]) {
          bucket.reserve(((end - begin) / cw) + 16);
        }
        for (size_t g = begin; g < end; ++g) {
          const uint64_t cpk = parent_pks[g] & mask_s;
          c_pks[g] = cpk;
          const auto b = static_cast<uint8_t>(MixCompactInt64(cpk) % cw);
          c_row_bucket[g] = b;
          c_buckets[w][b].push_back(g);
        }
      }));
      std::vector<FlatIntKeyMap<size_t>> c_maps(cw);
      std::vector<size_t> c_counts(cw, 0);
      std::vector<uint32_t> c_local_groups(pg_count);
      std::vector<std::vector<size_t>> c_first(cw);
      RETURN_IF_FAIL(RunParallelTasks(cw, [&](size_t w) {
        auto& cmap = c_maps[w];
        auto& cf = c_first[w];
        size_t bucket_rows = 0;
        for (size_t src = 0; src < cw; ++src) {
          bucket_rows += c_buckets[src][w].size();
        }
        cmap.Reserve(std::min<size_t>((bucket_rows * 3) / 4, 8192));
        int64_t prev_pk = 0;
        size_t prev_local = 0;
        bool has_prev = false;
        size_t local_count = 0;
        for (size_t src = 0; src < cw; ++src) {
          for (const size_t g : c_buckets[src][w]) {
            const auto pk = static_cast<int64_t>(c_pks[g]);
            size_t local = 0;
            if (has_prev && pk == prev_pk) {
              local = prev_local;
            } else {
              bool inserted = false;
              size_t* slot = cmap.TryEmplace(pk, &inserted);
              if (inserted) {
                *slot = local_count++;
                cf.push_back(g);
              }
              local = *slot;
              prev_pk = pk;
              prev_local = local;
              has_prev = true;
            }
            c_local_groups[g] = static_cast<uint32_t>(local);
          }
        }
        c_counts[w] = local_count;
      }));
      std::vector<std::vector<std::vector<size_t>>>().swap(c_buckets);
      std::vector<FlatIntKeyMap<size_t>>().swap(c_maps);
      std::vector<std::vector<size_t>> c_global_ids(cw);
      std::vector<size_t> c_active_w;
      c_active_w.reserve(cw);
      size_t total_c = 0;
      for (size_t w = 0; w < cw; ++w) {
        c_global_ids[w].resize(c_counts[w], SIZE_MAX);
        total_c += c_counts[w];
        if (!c_first[w].empty()) {
          c_active_w.push_back(w);
        }
      }
      groups[s].ReserveGroups(total_c);
      groups[s].group_count = total_c;
      group_pks[s].resize(total_c);
      if (total_c > 0) {
        PopulateLocalOrderedPks(
            c_active_w, c_active_w.size(), c_first, c_pks, &c_global_ids,
            is_columnar ? nullptr : &groups[s].borrowed_key_rows,
            is_columnar ? &groups[s].borrowed_key_row_ids : nullptr, nullptr,
            is_columnar ? nullptr : &groups[parent].borrowed_key_rows,
            is_columnar ? &groups[parent].borrowed_key_row_ids : nullptr,
            nullptr, group_pks[s]);
      }
      if (parent == 0) {
        coarse_maps[s].resize(pg_count);
        RETURN_IF_FAIL(RunParallelTasks(cw, [&](size_t w) {
          const size_t begin = (pg_count * w) / cw;
          const size_t end = (pg_count * (w + 1)) / cw;
          for (size_t g = begin; g < end; ++g) {
            coarse_maps[s][g] =
                c_global_ids[c_row_bucket[g]][c_local_groups[g]];
          }
        }));
      } else {
        std::vector<size_t> parent_groups(pg_count);
        RETURN_IF_FAIL(RunParallelTasks(cw, [&](size_t w) {
          const size_t begin = (pg_count * w) / cw;
          const size_t end = (pg_count * (w + 1)) / cw;
          for (size_t g = begin; g < end; ++g) {
            parent_groups[g] = c_global_ids[c_row_bucket[g]][c_local_groups[g]];
          }
        }));
        coarse_maps[s].resize(total_groups);
        const auto& pmap = coarse_maps[parent];
        RETURN_IF_FAIL(RunParallelTasks(cw, [&](size_t w) {
          const size_t begin = (total_groups * w) / cw;
          const size_t end = (total_groups * (w + 1)) / cw;
          for (size_t fg = begin; fg < end; ++fg) {
            coarse_maps[s][fg] = parent_groups[pmap[fg]];
          }
        }));
      }
    } else {
      FlatIntKeyMap<size_t> cmap;
      cmap.Reserve(std::min<size_t>(pg_count, 1024));
      std::vector<size_t> parent_groups(pg_count);
      std::vector<uint64_t> c_out_pks;
      c_out_pks.reserve(std::min<size_t>(pg_count, 1024));
      if (is_columnar) {
        std::vector<uint32_t> c_row_ids;
        c_row_ids.reserve(std::min<size_t>(pg_count, 1024));
        const auto& parent_row_ids = groups[parent].borrowed_key_row_ids;
        for (size_t g = 0; g < pg_count; ++g) {
          const uint64_t cpk = parent_pks[g] & mask_s;
          bool inserted = false;
          size_t* slot = cmap.TryEmplace(static_cast<int64_t>(cpk), &inserted);
          if (inserted) {
            *slot = c_row_ids.size();
            c_row_ids.push_back(parent_row_ids[g]);
            c_out_pks.push_back(cpk);
          }
          parent_groups[g] = *slot;
        }
        groups[s].ReserveGroups(c_row_ids.size());
        groups[s].group_count = c_row_ids.size();
        groups[s].borrowed_key_row_ids = std::move(c_row_ids);
      } else {
        std::vector<const Row*> c_rows;
        c_rows.reserve(std::min<size_t>(pg_count, 1024));
        for (size_t g = 0; g < pg_count; ++g) {
          const uint64_t cpk = parent_pks[g] & mask_s;
          bool inserted = false;
          size_t* slot = cmap.TryEmplace(static_cast<int64_t>(cpk), &inserted);
          if (inserted) {
            *slot = c_rows.size();
            c_rows.push_back(groups[parent].borrowed_key_rows[g]);
            c_out_pks.push_back(cpk);
          }
          parent_groups[g] = *slot;
        }
        groups[s].ReserveGroups(c_rows.size());
        groups[s].group_count = c_rows.size();
        groups[s].borrowed_key_rows = std::move(c_rows);
      }
      group_pks[s] = std::move(c_out_pks);
      if (parent == 0) {
        coarse_maps[s] = std::move(parent_groups);
      } else {
        coarse_maps[s].resize(total_groups);
        for (size_t fg = 0; fg < total_groups; ++fg) {
          coarse_maps[s][fg] = parent_groups[coarse_maps[parent][fg]];
        }
      }
    }
  }
  if (dbg_phases) {
    const auto t_p5 = std::chrono::steady_clock::now();
    const auto ms_between = [](auto a, auto b) {
      return std::chrono::duration<double, std::milli>(b - a).count();
    };
    std::cerr << "[packed] p1=" << ms_between(t_p0, t_p1)
              << " p2=" << ms_between(t_p1, t_p2)
              << " total_groups=" << total_groups
              << " total_bits=" << total_bits
              << " rest=" << ms_between(t_p2, t_p5) << "\n";
  }
  return true;
}

constexpr size_t kColumnarNullHash = 0x9e3779b97f4a7c15ULL;
constexpr size_t kColumnarNanHash = 0xbf58476d1ce4e5b9ULL;

inline size_t HashCanonicalScalarValue(const Value& v) noexcept {
  if (v.IsNull()) {
    return kColumnarNullHash;
  }
  if ((v.type == ValueType::kInt64 || v.type == ValueType::kDate) &&
      !v.IsUnsigned() && !v.IsInterval()) {
    return static_cast<size_t>(
        MixCompactInt64(static_cast<uint64_t>(v.value.int_value)));
  }
  if (v.type == ValueType::kVarChar && !v.IsCaseInsensitive() &&
      !v.IsInterval()) {
    return static_cast<size_t>(FastStringHash64(v.value.varchar_value));
  }
  if (v.type == ValueType::kDouble) {
    double d = v.value.double_value;
    if (d == 0.0) {
      d = 0.0;
    }
    if (std::isnan(d)) {
      return kColumnarNanHash;
    }
    return static_cast<size_t>(MixCompactInt64(std::bit_cast<uint64_t>(d)));
  }
  return FastCompactCellHash(v);
}

inline size_t HashColumnarCell(const ColumnarColumn& col, size_t lr) {
  if (col.is_uniform && !col.is_unsigned && !col.is_interval) {
    if (col.type == ValueType::kNull || col.IsNull(lr)) {
      return kColumnarNullHash;
    }
    if (col.type == ValueType::kInt64 || col.type == ValueType::kDate) {
      return static_cast<size_t>(
          MixCompactInt64(static_cast<uint64_t>(col.IntAt(lr))));
    }
    if (col.type == ValueType::kVarChar) {
      return static_cast<size_t>(FastStringHash64(col.StringAt(lr)));
    }
    if (col.type == ValueType::kDouble) {
      double d = col.DoubleAt(lr);
      if (d == 0.0) {
        d = 0.0;
      }
      if (std::isnan(d)) {
        return kColumnarNanHash;
      }
      return static_cast<size_t>(MixCompactInt64(std::bit_cast<uint64_t>(d)));
    }
  }
  Value tmp;
  col.WriteValueInto(lr, &tmp);
  tmp = CanonicalDistinctValue(tmp);
  return HashCanonicalScalarValue(tmp);
}

inline bool ColumnarCellsEqual(const ColumnarColumn& col_a, size_t lr_a,
                               const ColumnarColumn& col_b, size_t lr_b) {
  if (col_a.is_uniform && col_b.is_uniform && !col_a.is_unsigned &&
      !col_b.is_unsigned && !col_a.is_interval && !col_b.is_interval) {
    const bool null_a = (col_a.type == ValueType::kNull) || col_a.IsNull(lr_a);
    const bool null_b = (col_b.type == ValueType::kNull) || col_b.IsNull(lr_b);
    if (null_a || null_b) {
      return null_a && null_b;
    }
    if (col_a.type == col_b.type) {
      if (col_a.encoding == ColumnarColumn::Encoding::kDictionary &&
          col_b.encoding == ColumnarColumn::Encoding::kDictionary &&
          col_a.dict != nullptr && col_a.dict == col_b.dict &&
          col_a.DictCodeAt(lr_a) == col_b.DictCodeAt(lr_b)) {
        return true;
      }
      switch (col_a.type) {
        case ValueType::kInt64:
        case ValueType::kDate:
          return col_a.IntAt(lr_a) == col_b.IntAt(lr_b);
        case ValueType::kDouble: {
          const double da = col_a.DoubleAt(lr_a);
          const double db = col_b.DoubleAt(lr_b);
          return da == db || (std::isnan(da) && std::isnan(db));
        }
        case ValueType::kVarChar:
          return col_a.StringAt(lr_a) == col_b.StringAt(lr_b);
        default:
          break;
      }
    }
  }
  Value va;
  Value vb;
  col_a.WriteValueInto(lr_a, &va);
  col_b.WriteValueInto(lr_b, &vb);
  return FastCompactCellEqual(CanonicalDistinctValue(va),
                              CanonicalDistinctValue(vb));
}

inline void ExtractCanonicalColumnarKey(const ColumnarColumn& col, size_t lr,
                                        Value* out) {
  col.WriteValueInto(lr, out);
  *out = CanonicalDistinctValue(*out);
}

bool TryEvaluateTypedDoubleChunkRow(const TypedDoubleExprInput& expr,
                                    const ColumnarChunk& ch, size_t lr,
                                    bool* out_valid, double* out_val) {
  auto load_val = [&](slot_t c, double* d) -> int {
    const ColumnarColumn& col = ch.columns[c];
    if (!col.is_uniform || col.is_unsigned) {
      return -1;
    }
    if (col.type == ValueType::kNull || col.IsNull(lr)) {
      return 0;
    }
    if (col.type == ValueType::kDouble) {
      *d = col.DoubleAt(lr);
      return 1;
    }
    if (col.type == ValueType::kInt64) {
      *d = static_cast<double>(col.IntAt(lr));
      return 1;
    }
    return -1;
  };
  double v0 = 0.0;
  double v1 = 0.0;
  double v2 = 0.0;
  switch (expr.shape) {
    case TypedDoubleExprInput::Shape::kSingleDoubleCol: {
      const int st = load_val(expr.col0, &v0);
      if (st < 0) {
        return false;
      }
      *out_valid = st > 0;
      *out_val = v0;
      return true;
    }
    case TypedDoubleExprInput::Shape::kMulTwoDoubleCols: {
      const int s0 = load_val(expr.col0, &v0);
      const int s1 = load_val(expr.col1, &v1);
      if (s0 < 0 || s1 < 0) {
        return false;
      }
      if (s0 == 0 || s1 == 0) {
        *out_valid = false;
        return true;
      }
      *out_valid = true;
      *out_val = v0 * v1;
      return true;
    }
    case TypedDoubleExprInput::Shape::kMulConstMinusCol: {
      const int s0 = load_val(expr.col0, &v0);
      const int s1 = load_val(expr.col1, &v1);
      if (s0 < 0 || s1 < 0) {
        return false;
      }
      if (s0 == 0 || s1 == 0) {
        *out_valid = false;
        return true;
      }
      *out_valid = true;
      *out_val = v0 * (expr.k0 - v1);
      return true;
    }
    case TypedDoubleExprInput::Shape::kMulConstMinusColMulConstPlusCol: {
      const int s0 = load_val(expr.col0, &v0);
      const int s1 = load_val(expr.col1, &v1);
      const int s2 = load_val(expr.col2, &v2);
      if (s0 < 0 || s1 < 0 || s2 < 0) {
        return false;
      }
      if (s0 == 0 || s1 == 0 || s2 == 0) {
        *out_valid = false;
        return true;
      }
      *out_valid = true;
      *out_val = v0 * (expr.k0 - v1) * (expr.k1 + v2);
      return true;
    }
    case TypedDoubleExprInput::Shape::kGeneric:
      break;
  }
  std::array<double, 16> stack{};
  size_t sp = 0;
  // Producers cap ops at 16 entries so pushes can never exceed the stack;
  // keep that bound explicit here instead of relying on the upstream
  // compiler invariant.
  if (expr.ops.size() > stack.size()) {
    return false;
  }
  for (const TypedDoubleExprOp& op : expr.ops) {
    switch (op.kind) {
      case TypedDoubleExprOp::Kind::kLoadDoubleCol:
      case TypedDoubleExprOp::Kind::kLoadIntColAsDouble: {
        const int st = load_val(op.column, &v0);
        if (st < 0) {
          return false;
        }
        if (st == 0) {
          *out_valid = false;
          return true;
        }
        stack[sp++] = v0;
        break;
      }
      case TypedDoubleExprOp::Kind::kConstant:
        stack[sp++] = op.constant;
        break;
      case TypedDoubleExprOp::Kind::kAdd:
        stack[sp - 2] += stack[sp - 1];
        --sp;
        break;
      case TypedDoubleExprOp::Kind::kSub:
        stack[sp - 2] -= stack[sp - 1];
        --sp;
        break;
      case TypedDoubleExprOp::Kind::kMul:
        stack[sp - 2] *= stack[sp - 1];
        --sp;
        break;
    }
  }
  *out_valid = true;
  *out_val = stack[0];
  return true;
}

bool TryEvaluateTypedFilterCompareChunkRow(const TypedFilterCompare& cmp,
                                           const ColumnarChunk& ch, size_t lr,
                                           bool* out_match) {
  const ColumnarColumn& lcol = ch.columns[cmp.left];
  if (!lcol.is_uniform || lcol.is_unsigned || lcol.is_interval) {
    return false;
  }
  if (lcol.type == ValueType::kNull || lcol.IsNull(lr)) {
    *out_match = false;
    return true;
  }
  if (cmp.string_payload) {
    if (lcol.type != ValueType::kVarChar) {
      return false;
    }
    *out_match = MatchTypedStringCompare(lcol.StringAt(lr), cmp);
    return true;
  }
  if (cmp.constant_payload) {
    if (cmp.is_double) {
      if (lcol.type != ValueType::kDouble) {
        return false;
      }
      const double a = lcol.DoubleAt(lr);
      switch (cmp.op) {
        case BinaryOperation::kEquals:
          *out_match = a == cmp.double_constant;
          return true;
        case BinaryOperation::kNotEquals:
          *out_match = a != cmp.double_constant;
          return true;
        case BinaryOperation::kLessThan:
          *out_match = a < cmp.double_constant;
          return true;
        case BinaryOperation::kLessThanEquals:
          *out_match = a <= cmp.double_constant;
          return true;
        case BinaryOperation::kGreaterThan:
          *out_match = a > cmp.double_constant;
          return true;
        case BinaryOperation::kGreaterThanEquals:
          *out_match = a >= cmp.double_constant;
          return true;
        default:
          return false;
      }
    }
    if (lcol.type != ValueType::kInt64 && lcol.type != ValueType::kDate) {
      return false;
    }
    const int64_t a = lcol.IntAt(lr);
    switch (cmp.op) {
      case BinaryOperation::kEquals:
        *out_match = a == cmp.int_constant;
        return true;
      case BinaryOperation::kNotEquals:
        *out_match = a != cmp.int_constant;
        return true;
      case BinaryOperation::kLessThan:
        *out_match = a < cmp.int_constant;
        return true;
      case BinaryOperation::kLessThanEquals:
        *out_match = a <= cmp.int_constant;
        return true;
      case BinaryOperation::kGreaterThan:
        *out_match = a > cmp.int_constant;
        return true;
      case BinaryOperation::kGreaterThanEquals:
        *out_match = a >= cmp.int_constant;
        return true;
      default:
        return false;
    }
  }
  const ColumnarColumn& rcol = ch.columns[cmp.right];
  if (!rcol.is_uniform || rcol.is_unsigned || rcol.is_interval) {
    return false;
  }
  if (rcol.type == ValueType::kNull || rcol.IsNull(lr)) {
    *out_match = false;
    return true;
  }
  if (cmp.is_double) {
    if (lcol.type != ValueType::kDouble || rcol.type != ValueType::kDouble) {
      return false;
    }
    const double a = lcol.DoubleAt(lr);
    const double b = rcol.DoubleAt(lr);
    switch (cmp.op) {
      case BinaryOperation::kEquals:
        *out_match = a == b;
        return true;
      case BinaryOperation::kNotEquals:
        *out_match = a != b;
        return true;
      case BinaryOperation::kLessThan:
        *out_match = a < b;
        return true;
      case BinaryOperation::kLessThanEquals:
        *out_match = a <= b;
        return true;
      case BinaryOperation::kGreaterThan:
        *out_match = a > b;
        return true;
      case BinaryOperation::kGreaterThanEquals:
        *out_match = a >= b;
        return true;
      default:
        return false;
    }
  }
  if ((lcol.type != ValueType::kInt64 && lcol.type != ValueType::kDate) ||
      rcol.type != lcol.type) {
    return false;
  }
  const int64_t a = lcol.IntAt(lr);
  const int64_t b = rcol.IntAt(lr);
  switch (cmp.op) {
    case BinaryOperation::kEquals:
      *out_match = a == b;
      return true;
    case BinaryOperation::kNotEquals:
      *out_match = a != b;
      return true;
    case BinaryOperation::kLessThan:
      *out_match = a < b;
      return true;
    case BinaryOperation::kLessThanEquals:
      *out_match = a <= b;
      return true;
    case BinaryOperation::kGreaterThan:
      *out_match = a > b;
      return true;
    case BinaryOperation::kGreaterThanEquals:
      *out_match = a >= b;
      return true;
    default:
      return false;
  }
}

bool TryEvaluateTypedConditionalChunkRow(const TypedConditionalInput& cond,
                                         const ColumnarChunk& ch, size_t lr,
                                         Value* out_val) {
  bool matched = false;
  if (!TryEvaluateTypedFilterCompareChunkRow(cond.condition, ch, lr,
                                             &matched)) {
    return false;
  }
  if (matched) {
    if (cond.then_column) {
      ch.columns[*cond.then_column].WriteValueInto(lr, out_val);
    } else if (cond.result_type == ValueType::kDouble) {
      *out_val = Value(static_cast<double>(cond.then_int_constant));
    } else {
      *out_val = Value(cond.then_int_constant);
    }
    return true;
  }
  if (cond.has_else_constant) {
    if (cond.result_type == ValueType::kDouble) {
      *out_val = Value(static_cast<double>(cond.else_int_constant));
    } else {
      *out_val = Value(cond.else_int_constant);
    }
  } else {
    *out_val = Value();
  }
  return true;
}

bool AccumulateCompactColumnarRow(
    const TypedAggregationPlan& plan,
    const std::vector<const AggregateExpression*>& /*aggregates*/,
    const ColumnarChunk& ch, size_t lr, TypedWorkerGroups* target, size_t group,
    Row* scratch_row, bool* scratch_ready, Status* error) {
  const bool is_slim = target->slim;
  const auto apply_val = [&](size_t a, Value&& val) -> bool {
    if (is_slim) {
      return ApplySlimValue(target, group, a, plan.slim_kinds[a], &val, error);
    }
    const Status st = target->State(group, a).Add(val);
    if (!st.ok()) {
      *error = st;
      return false;
    }
    return true;
  };
  const auto apply_double = [&](size_t a, double d) -> bool {
    const SlimAggKind kind = is_slim ? plan.slim_kinds[a] : SlimAggKind::kNone;
    if (is_slim && (kind == SlimAggKind::kAvgDouble ||
                    (kind == SlimAggKind::kSumDouble &&
                     target->mixed_numeric_sums.empty() && std::isfinite(d)))) {
      ++target->SlimCount(group, a);
      target->SlimDoubleTotal(group, a) += d;
      return true;
    }
    return apply_val(a, Value(d));
  };
  const auto ensure_row = [&]() {
    if (!*scratch_ready) {
      ch.MaterializeRowInto(lr, scratch_row);
      *scratch_ready = true;
    }
  };
  const size_t agg_count = plan.aggregates.size();
  for (size_t a = 0; a < agg_count; ++a) {
    const TypedAggregateInput& item = plan.aggregates[a];
    const SlimAggKind kind = is_slim ? plan.slim_kinds[a] : SlimAggKind::kNone;
    if (is_slim && kind == SlimAggKind::kCount && !item.column &&
        !item.conditional && !item.int_expr && !item.child &&
        !item.double_expr && !item.coalesce_double) {
      ++target->SlimCount(group, a);
      continue;
    }
    if (item.column) {
      const ColumnarColumn& col = ch.columns[*item.column];
      if (is_slim && kind == SlimAggKind::kCount) {
        if (col.is_uniform) {
          if (col.type != ValueType::kNull &&
              (!col.has_null || !col.IsNull(lr))) {
            ++target->SlimCount(group, a);
          }
          continue;
        }
      } else if (is_slim && col.is_uniform && !col.is_unsigned) {
        if (col.type == ValueType::kNull || col.IsNull(lr)) {
          continue;
        }
        if (col.type == ValueType::kDouble &&
            (kind == SlimAggKind::kAvgDouble ||
             (kind == SlimAggKind::kSumDouble &&
              target->mixed_numeric_sums.empty()))) {
          const double d = col.DoubleAt(lr);
          if (std::isfinite(d)) {
            ++target->SlimCount(group, a);
            target->SlimDoubleTotal(group, a) += d;
            continue;
          }
        } else if (col.type == ValueType::kInt64) {
          const int64_t v = col.IntAt(lr);
          if (kind == SlimAggKind::kAvgDouble) {
            ++target->SlimCount(group, a);
            target->SlimDoubleTotal(group, a) += static_cast<double>(v);
            continue;
          }
          if (kind == SlimAggKind::kSumInt64) {
            int64_t sum = target->SlimIntTotal(group, a);
            if (!__builtin_add_overflow(sum, v, &sum)) {
              target->SlimIntTotal(group, a) = sum;
              ++target->SlimCount(group, a);
              continue;
            }
          }
        }
      }
      Value v;
      col.WriteValueInto(lr, &v);
      if (!apply_val(a, std::move(v))) {
        return false;
      }
      continue;
    }
    if (item.double_expr || item.coalesce_double) {
      const TypedDoubleExprInput& dexpr =
          item.coalesce_double ? item.coalesce_double->expr : *item.double_expr;
      bool valid = false;
      double dv = 0.0;
      if (TryEvaluateTypedDoubleChunkRow(dexpr, ch, lr, &valid, &dv)) {
        if (valid) {
          if (!apply_double(a, dv)) {
            return false;
          }
        } else if (item.coalesce_double) {
          Value fb = item.coalesce_double->fallback;
          if (!fb.IsNull() && !apply_val(a, std::move(fb))) {
            return false;
          }
        }
        continue;
      }
    }
    if (item.conditional) {
      Value cval;
      if (TryEvaluateTypedConditionalChunkRow(*item.conditional, ch, lr,
                                              &cval)) {
        if (!cval.IsNull() && !apply_val(a, std::move(cval))) {
          return false;
        }
        continue;
      }
    }
    ensure_row();
    auto computed = CompactArgument(item, *scratch_row, is_slim);
    if (!computed.HasValue()) {
      *error = computed.GetStatus();
      return false;
    }
    if (!apply_val(a, computed.MoveValue())) {
      return false;
    }
  }
  return true;
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
// (columnar_null_indicator_popcount_sound)
struct NullIndicatorBoolOp {
  enum class Kind : uint8_t {
    kColIsNull = 0,
    kColIsNotNull = 1,
    kNot = 2,
    kAnd = 3,
    kOr = 4,
  } kind{Kind::kColIsNull};
  slot_t col{0};
};

struct NullIndicatorCaseSpec {
  bool valid{false};
  std::vector<NullIndicatorBoolOp> rpn;
  bool then_is_null{true};
  int64_t then_int{0};
  bool else_is_null{true};
  int64_t else_int{0};
};

bool CompileNullIndicatorBoolRpn(const Expression& expr, const Schema& schema,
                                 std::vector<NullIndicatorBoolOp>* out,
                                 size_t depth = 0) {
  if (!expr || depth > 32 || out->size() >= 32) {
    return false;
  }
  if (expr->Type() == TypeTag::kUnaryExp) {
    const UnaryExpression& ue = expr->AsUnaryExpression();
    if (ue.Op() == UnaryOperation::kIsNull ||
        ue.Op() == UnaryOperation::kIsNotNull) {
      if (!ue.Child() || ue.Child()->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const auto col = LocalColumnOffset(
          schema, ue.Child()->AsColumnValue().GetColumnName());
      if (!col || *col >= schema.ColumnCount()) {
        return false;
      }
      out->push_back(NullIndicatorBoolOp{
          .kind = ue.Op() == UnaryOperation::kIsNull
                      ? NullIndicatorBoolOp::Kind::kColIsNull
                      : NullIndicatorBoolOp::Kind::kColIsNotNull,
          .col = static_cast<slot_t>(*col),
      });
      return true;
    }
    if (ue.Op() == UnaryOperation::kNot) {
      if (!CompileNullIndicatorBoolRpn(ue.Child(), schema, out, depth + 1)) {
        return false;
      }
      out->push_back(
          NullIndicatorBoolOp{.kind = NullIndicatorBoolOp::Kind::kNot});
      return true;
    }
    return false;
  }
  if (expr->Type() == TypeTag::kBinaryExp) {
    const BinaryExpression& be = expr->AsBinaryExpression();
    if (be.Op() != BinaryOperation::kAnd && be.Op() != BinaryOperation::kOr) {
      return false;
    }
    if (!CompileNullIndicatorBoolRpn(be.Left(), schema, out, depth + 1) ||
        !CompileNullIndicatorBoolRpn(be.Right(), schema, out, depth + 1)) {
      return false;
    }
    out->push_back(NullIndicatorBoolOp{
        .kind = be.Op() == BinaryOperation::kAnd
                    ? NullIndicatorBoolOp::Kind::kAnd
                    : NullIndicatorBoolOp::Kind::kOr,
    });
    return true;
  }
  return false;
}

NullIndicatorCaseSpec TryBuildNullIndicatorCaseSpec(
    const AggregateExpression* agg, SlimAggKind kind, const Schema& schema) {
  NullIndicatorCaseSpec spec;
  if (agg == nullptr ||
      (kind != SlimAggKind::kCount && kind != SlimAggKind::kSumInt64) ||
      agg->Distinct() || agg->HasWhereFilter() ||
      agg->Having() != AggregateHavingModifier::kNone ||
      !agg->InnerOrderBy().empty() || agg->InnerLimit().has_value() ||
      !agg->Child() || agg->Child()->Type() != TypeTag::kCaseExp) {
    return spec;
  }
  const CaseExpression& ce = agg->Child()->AsCaseExpression();
  if (ce.when_clauses_.size() != 1) {
    return spec;
  }
  const auto parse_branch = [&](const Expression& branch, bool* is_null,
                                int64_t* int_val) -> bool {
    if (!branch) {
      *is_null = true;
      *int_val = 0;
      return true;
    }
    if (branch->Type() != TypeTag::kConstantValue) {
      return false;
    }
    const Value& v = branch->AsConstantValue().GetValue();
    if (v.IsNull()) {
      *is_null = true;
      *int_val = 0;
      return true;
    }
    if (kind == SlimAggKind::kCount) {
      *is_null = false;
      *int_val = 1;
      return true;
    }
    if (v.type == ValueType::kInt64 && !v.IsUnsigned()) {
      *is_null = false;
      *int_val = v.value.int_value;
      return true;
    }
    return false;
  };
  if (!parse_branch(ce.when_clauses_[0].second, &spec.then_is_null,
                    &spec.then_int) ||
      !parse_branch(ce.else_clause_, &spec.else_is_null, &spec.else_int)) {
    return spec;
  }
  if (!CompileNullIndicatorBoolRpn(ce.when_clauses_[0].first, schema,
                                   &spec.rpn) ||
      spec.rpn.empty()) {
    return spec;
  }
  spec.valid = true;
  return spec;
}

int64_t CountNullIndicatorTrueInSlice(const NullIndicatorCaseSpec& spec,
                                      const ColumnarChunk& ch, size_t lr_start,
                                      size_t take) noexcept {
  if (take == 0) {
    return 0;
  }
  // Check whether every referenced column has chunk-invariant nullness.
  bool all_const_nullness = true;
  for (const NullIndicatorBoolOp& op : spec.rpn) {
    if (op.kind != NullIndicatorBoolOp::Kind::kColIsNull &&
        op.kind != NullIndicatorBoolOp::Kind::kColIsNotNull) {
      continue;
    }
    const ColumnarColumn& c = ch.columns[op.col];
    if (c.encoding == ColumnarColumn::Encoding::kGathered || !c.is_uniform) {
      all_const_nullness = false;
      break;
    }
    const bool always_null =
        c.type == ValueType::kNull ||
        (c.encoding == ColumnarColumn::Encoding::kConstant && c.const_is_null);
    const bool never_null = !always_null && !c.has_null;
    if (!always_null && !never_null) {
      all_const_nullness = false;
      break;
    }
  }
  std::array<bool, 32> st{};
  if (all_const_nullness) {
    size_t sp = 0;
    for (const NullIndicatorBoolOp& op : spec.rpn) {
      switch (op.kind) {
        case NullIndicatorBoolOp::Kind::kColIsNull:
        case NullIndicatorBoolOp::Kind::kColIsNotNull: {
          const ColumnarColumn& c = ch.columns[op.col];
          const bool is_null =
              c.type == ValueType::kNull ||
              (c.encoding == ColumnarColumn::Encoding::kConstant &&
               c.const_is_null);
          st[sp++] = (op.kind == NullIndicatorBoolOp::Kind::kColIsNull)
                         ? is_null
                         : !is_null;
          break;
        }
        case NullIndicatorBoolOp::Kind::kNot:
          st[sp - 1] = !st[sp - 1];
          break;
        case NullIndicatorBoolOp::Kind::kAnd:
          st[sp - 2] = st[sp - 2] && st[sp - 1];
          --sp;
          break;
        case NullIndicatorBoolOp::Kind::kOr:
          st[sp - 2] = st[sp - 2] || st[sp - 1];
          --sp;
          break;
      }
    }
    return st[0] ? static_cast<int64_t>(take) : 0;
  }
  int64_t true_count = 0;
  for (size_t i = 0; i < take; ++i) {
    const size_t lr = lr_start + i;
    size_t sp = 0;
    for (const NullIndicatorBoolOp& op : spec.rpn) {
      switch (op.kind) {
        case NullIndicatorBoolOp::Kind::kColIsNull:
          st[sp++] = ch.columns[op.col].IsNull(lr);
          break;
        case NullIndicatorBoolOp::Kind::kColIsNotNull:
          st[sp++] = !ch.columns[op.col].IsNull(lr);
          break;
        case NullIndicatorBoolOp::Kind::kNot:
          st[sp - 1] = !st[sp - 1];
          break;
        case NullIndicatorBoolOp::Kind::kAnd:
          st[sp - 2] = st[sp - 2] && st[sp - 1];
          --sp;
          break;
        case NullIndicatorBoolOp::Kind::kOr:
          st[sp - 2] = st[sp - 2] || st[sp - 1];
          --sp;
          break;
      }
    }
    true_count += st[0] ? 1 : 0;
  }
  return true_count;
}

Status AccumulateScalarChunkSlice(
    const TypedAggregationPlan& plan,
    const std::vector<const AggregateExpression*>& aggregates,
    const std::vector<NullIndicatorCaseSpec>& null_indicator_specs,
    const ColumnarChunk& ch, size_t lr_start, size_t take,
    bool reject_non_finite_double, TypedWorkerGroups* target, bool* non_finite,
    Status* error) {
  constexpr size_t kTile = 1024;
  std::array<double, kTile> tile_vals{};
  std::array<uint8_t, kTile> tile_valid{};
  Row scratch_row;
  const size_t agg_count = plan.aggregates.size();
  const bool is_slim = target->slim;
  bool can_fast_tile = is_slim && target->mixed_numeric_sums.empty();
  if (can_fast_tile) {
    for (size_t a = 0; a < agg_count; ++a) {
      const auto& item = plan.aggregates[a];
      const SlimAggKind kind = plan.slim_kinds[a];
      if (!item.column && !item.conditional && !item.int_expr && !item.child &&
          !item.double_expr && !item.coalesce_double &&
          kind == SlimAggKind::kCount) {
        continue;
      }
      if (a < null_indicator_specs.size() && null_indicator_specs[a].valid) {
        continue;
      }
      if (item.column) {
        const ColumnarColumn& col = ch.columns[*item.column];
        if (kind == SlimAggKind::kCount) {
          continue;
        }
        if (col.is_uniform && !col.is_unsigned &&
            ((col.type == ValueType::kDouble &&
              (kind == SlimAggKind::kSumDouble ||
               kind == SlimAggKind::kAvgDouble)) ||
             (col.type == ValueType::kInt64 &&
              (kind == SlimAggKind::kAvgDouble ||
               kind == SlimAggKind::kSumInt64)))) {
          continue;
        }
        can_fast_tile = false;
        break;
      }
      if (item.double_expr || item.coalesce_double) {
        if (kind == SlimAggKind::kSumDouble ||
            kind == SlimAggKind::kAvgDouble) {
          continue;
        }
      }
      can_fast_tile = false;
      break;
    }
  }
  if (can_fast_tile) {
    for (size_t a = 0; a < agg_count; ++a) {
      const auto& item = plan.aggregates[a];
      const SlimAggKind kind = plan.slim_kinds[a];
      if (a < null_indicator_specs.size() && null_indicator_specs[a].valid) {
        const NullIndicatorCaseSpec& spec = null_indicator_specs[a];
        const int64_t m =
            CountNullIndicatorTrueInSlice(spec, ch, lr_start, take);
        const int64_t rem_false = static_cast<int64_t>(take) - m;
        if (kind == SlimAggKind::kCount) {
          target->SlimCount(0, a) +=
              (spec.then_is_null ? 0 : m) + (spec.else_is_null ? 0 : rem_false);
        } else {
          int64_t add_total = 0;
          int64_t add_cnt = 0;
          if (!spec.then_is_null && m > 0) {
            int64_t prod = 0;
            if (__builtin_mul_overflow(m, spec.then_int, &prod) ||
                __builtin_add_overflow(add_total, prod, &add_total)) {
              *error = StatusError(StatusCode::kIsInfinity,
                                   "integer overflow in SUM");
              return *error;
            }
            add_cnt += m;
          }
          if (!spec.else_is_null && rem_false > 0) {
            int64_t prod = 0;
            if (__builtin_mul_overflow(rem_false, spec.else_int, &prod) ||
                __builtin_add_overflow(add_total, prod, &add_total)) {
              *error = StatusError(StatusCode::kIsInfinity,
                                   "integer overflow in SUM");
              return *error;
            }
            add_cnt += rem_false;
          }
          int64_t next_total = 0;
          if (__builtin_add_overflow(target->SlimIntTotal(0, a), add_total,
                                     &next_total)) {
            *error =
                StatusError(StatusCode::kIsInfinity, "integer overflow in SUM");
            return *error;
          }
          target->SlimIntTotal(0, a) = next_total;
          target->SlimCount(0, a) += add_cnt;
        }
        continue;
      }
      if (!item.column && !item.double_expr && !item.coalesce_double) {
        target->SlimCount(0, a) += static_cast<int64_t>(take);
        continue;
      }
      if (item.column) {
        const ColumnarColumn& col = ch.columns[*item.column];
        if (kind == SlimAggKind::kCount) {
          if (col.is_uniform) {
            if (col.type == ValueType::kNull ||
                (col.encoding == ColumnarColumn::Encoding::kConstant &&
                 col.const_is_null)) {
              continue;
            }
            if (!col.has_null) {
              target->SlimCount(0, a) += static_cast<int64_t>(take);
              continue;
            }
            if (col.IsFlat()) {
              const uint8_t* __restrict nulls = col.nulls.data() + lr_start;
              int64_t nn = 0;
              for (size_t i = 0; i < take; ++i) {
                nn += nulls[i] == 0 ? 1 : 0;
              }
              target->SlimCount(0, a) += nn;
              continue;
            }
            int64_t nn = 0;
            for (size_t i = 0; i < take; ++i) {
              nn += col.IsNull(lr_start + i) ? 0 : 1;
            }
            target->SlimCount(0, a) += nn;
            continue;
          }
          int64_t nn = 0;
          Value tmp;
          for (size_t i = 0; i < take; ++i) {
            col.WriteValueInto(lr_start + i, &tmp);
            if (!tmp.IsNull()) {
              ++nn;
            }
          }
          target->SlimCount(0, a) += nn;
          continue;
        }
        if (col.type == ValueType::kNull ||
            (col.encoding == ColumnarColumn::Encoding::kConstant &&
             col.const_is_null)) {
          continue;
        }
        if (col.type == ValueType::kDouble) {
          double sum = 0.0;
          int64_t nn = 0;
          if (col.encoding == ColumnarColumn::Encoding::kConstant) {
            if (!std::isfinite(col.const_double)) {
              if (reject_non_finite_double) {
                *non_finite = true;
                return Status::kSuccess;
              }
              Value dv(col.const_double);
              for (size_t i = 0; i < take; ++i) {
                if (!ApplySlimValue(target, 0, a, kind, &dv, error)) {
                  return *error;
                }
              }
              continue;
            }
            sum = col.const_double * static_cast<double>(take);
            nn = static_cast<int64_t>(take);
          } else if (col.IsFlat() && !col.has_null) {
            const double* __restrict src = col.doubles.data() + lr_start;
            for (size_t i = 0; i < take; ++i) {
              sum += src[i];
            }
            nn = static_cast<int64_t>(take);
          } else if (col.IsFlat() && col.has_null) {
            const double* __restrict src = col.doubles.data() + lr_start;
            const uint8_t* __restrict nulls = col.nulls.data() + lr_start;
            for (size_t i = 0; i < take; ++i) {
              if (nulls[i] == 0) {
                sum += src[i];
                ++nn;
              }
            }
          } else if (!col.has_null) {
            for (size_t i = 0; i < take; ++i) {
              sum += col.DoubleAt(lr_start + i);
            }
            nn = static_cast<int64_t>(take);
          } else {
            for (size_t i = 0; i < take; ++i) {
              const size_t lr = lr_start + i;
              if (!col.IsNull(lr)) {
                sum += col.DoubleAt(lr);
                ++nn;
              }
            }
          }
          if (!std::isfinite(sum)) {
            if (reject_non_finite_double) {
              *non_finite = true;
              return Status::kSuccess;
            }
            for (size_t i = 0; i < take; ++i) {
              const size_t lr = lr_start + i;
              if (!col.IsNull(lr)) {
                Value dv(col.DoubleAt(lr));
                if (!ApplySlimValue(target, 0, a, kind, &dv, error)) {
                  return *error;
                }
              }
            }
            continue;
          }
          target->SlimDoubleTotal(0, a) += sum;
          target->SlimCount(0, a) += nn;
          continue;
        }
        // Uniform signed int64 column (SUM INT64 or AVG INT64).
        if (kind == SlimAggKind::kAvgDouble) {
          double sum = 0.0;
          int64_t nn = 0;
          if (!col.has_null) {
            for (size_t i = 0; i < take; ++i) {
              sum += static_cast<double>(col.IntAt(lr_start + i));
            }
            nn = static_cast<int64_t>(take);
          } else {
            for (size_t i = 0; i < take; ++i) {
              const size_t lr = lr_start + i;
              if (!col.IsNull(lr)) {
                sum += static_cast<double>(col.IntAt(lr));
                ++nn;
              }
            }
          }
          target->SlimDoubleTotal(0, a) += sum;
          target->SlimCount(0, a) += nn;
          continue;
        }
        // kSumInt64: preserve exact overflow error semantics.
        int64_t total = target->SlimIntTotal(0, a);
        int64_t nn = target->SlimCount(0, a);
        for (size_t i = 0; i < take; ++i) {
          const size_t lr = lr_start + i;
          if (col.has_null && col.IsNull(lr)) {
            continue;
          }
          const int64_t v = col.IntAt(lr);
          int64_t next = 0;
          if (__builtin_add_overflow(total, v, &next)) {
            // Match the canonical accumulator: raise at the first overflowing
            // partial sum, leaving no wrapped value in the slim state.
            // Re-adding via ApplySlimValue here would double-count `v` (and
            // can itself stay in range after the wrap), hiding the error.
            return StatusError(StatusCode::kIsInfinity,
                               "integer overflow in SUM");
          }
          total = next;
          ++nn;
        }
        target->SlimIntTotal(0, a) = total;
        target->SlimCount(0, a) = nn;
        continue;
      }
      // double_expr / coalesce_double via vectorized EvalDoubleExprChunkSlice.
      const TypedDoubleExprInput& dexpr =
          item.coalesce_double ? item.coalesce_double->expr : *item.double_expr;
      size_t offset = 0;
      bool expr_ok = true;
      while (offset < take) {
        const size_t sub = std::min(kTile, take - offset);
        if (!EvalDoubleExprChunkSlice(dexpr, ch, lr_start + offset, sub,
                                      tile_vals.data(), tile_valid.data(),
                                      &scratch_row)) {
          expr_ok = false;
          break;
        }
        double sum = 0.0;
        int64_t nn = 0;
        if (!item.coalesce_double) {
          for (size_t i = 0; i < sub; ++i) {
            if (tile_valid[i] != 0) {
              sum += tile_vals[i];
              ++nn;
            }
          }
          if (!std::isfinite(sum)) {
            if (reject_non_finite_double) {
              *non_finite = true;
              return Status::kSuccess;
            }
            for (size_t i = 0; i < sub; ++i) {
              if (tile_valid[i] != 0) {
                Value dv(tile_vals[i]);
                if (!ApplySlimValue(target, 0, a, kind, &dv, error)) {
                  return *error;
                }
              }
            }
          } else {
            target->SlimDoubleTotal(0, a) += sum;
            target->SlimCount(0, a) += nn;
          }
        } else {
          const Value& fb = item.coalesce_double->fallback;
          const bool fb_null = fb.IsNull();
          const double fb_d =
              !fb_null && fb.type == ValueType::kDouble
                  ? fb.value.double_value
                  : (!fb_null && fb.type == ValueType::kInt64
                         ? static_cast<double>(fb.value.int_value)
                         : 0.0);
          const bool fb_int_fallback = !fb_null &&
                                       fb.type == ValueType::kInt64 &&
                                       kind == SlimAggKind::kSumDouble;
          if (!fb_int_fallback) {
            for (size_t i = 0; i < sub; ++i) {
              if (tile_valid[i] != 0) {
                sum += tile_vals[i];
                ++nn;
              } else if (!fb_null) {
                sum += fb_d;
                ++nn;
              }
            }
            if (!std::isfinite(sum)) {
              if (reject_non_finite_double) {
                *non_finite = true;
                return Status::kSuccess;
              }
              for (size_t i = 0; i < sub; ++i) {
                if (tile_valid[i] != 0) {
                  Value dv(tile_vals[i]);
                  if (!ApplySlimValue(target, 0, a, kind, &dv, error)) {
                    return *error;
                  }
                } else if (!fb_null) {
                  Value fbv(fb);
                  if (!ApplySlimValue(target, 0, a, kind, &fbv, error)) {
                    return *error;
                  }
                }
              }
            } else {
              target->SlimDoubleTotal(0, a) += sum;
              target->SlimCount(0, a) += nn;
            }
          } else {
            for (size_t i = 0; i < sub; ++i) {
              Value arg = tile_valid[i] != 0 ? Value(tile_vals[i]) : Value(fb);
              if (arg.type == ValueType::kDouble &&
                  !std::isfinite(arg.value.double_value) &&
                  reject_non_finite_double) {
                *non_finite = true;
                return Status::kSuccess;
              }
              if (!ApplySlimValue(target, 0, a, kind, &arg, error)) {
                return *error;
              }
            }
          }
        }
        offset += sub;
      }
      if (!expr_ok) {
        if (reject_non_finite_double) {
          *non_finite = true;
          return Status::kSuccess;
        }
        for (size_t lr = lr_start + offset; lr < lr_start + take; ++lr) {
          ch.MaterializeRowInto(lr, &scratch_row);
          auto computed = CompactArgument(item, scratch_row, is_slim);
          if (!computed.HasValue()) {
            return computed.GetStatus();
          }
          Value v = computed.MoveValue();
          if (!ApplySlimValue(target, 0, a, kind, &v, error)) {
            return *error;
          }
        }
      }
    }
    return Status::kSuccess;
  }

  for (size_t i = 0; i < take; ++i) {
    bool scratch_ready = false;
    if (!AccumulateCompactColumnarRow(plan, aggregates, ch, lr_start + i,
                                      target, 0, &scratch_row, &scratch_ready,
                                      error)) {
      return *error;
    }
    if (reject_non_finite_double && is_slim) {
      for (size_t a = 0; a < agg_count; ++a) {
        if ((plan.slim_kinds[a] == SlimAggKind::kSumDouble ||
             plan.slim_kinds[a] == SlimAggKind::kAvgDouble) &&
            !std::isfinite(target->SlimDoubleTotal(0, a))) {
          *non_finite = true;
          return Status::kSuccess;
        }
      }
    }
  }
  return Status::kSuccess;
}

StatusOr<std::optional<Relation>> TryCompactColumnarScalarAggregate(
    TransactionContext& context, const SelectStatement& statement,
    const Relation& input,
    const std::vector<const AggregateExpression*>& aggregates,
    const TypedAggregationPlan& plan,
    const std::vector<PositionalHaving>& having,
    const std::vector<uint32_t>& chunk_prefix) {
  const ColumnarTable& columnar = *input.columnar;
  const size_t total_rows = columnar.total_rows;
  const size_t agg_count = aggregates.size();
  std::vector<AggregateAccumulator::Shape> shapes;
  if (!plan.slim) {
    shapes.reserve(agg_count);
    for (const AggregateExpression* agg : aggregates) {
      shapes.push_back(AggregateAccumulator::AnalyzeShape(agg, &input.schema));
    }
  }
  TypedWorkerGroups groups;
  groups.Configure(true, 0, agg_count);
  if (plan.slim) {
    groups.ConfigureSlim(plan);
    groups.AppendSlimOnlyGroup();
  } else {
    groups.shapes = &shapes;
    groups.AppendNonSlimGroup({}, agg_count, aggregates);
  }

  std::vector<NullIndicatorCaseSpec> null_indicator_specs;
  if (plan.slim && groups.mixed_numeric_sums.empty()) {
    null_indicator_specs.resize(agg_count);
    for (size_t a = 0; a < agg_count; ++a) {
      null_indicator_specs[a] = TryBuildNullIndicatorCaseSpec(
          aggregates[a], plan.slim_kinds[a], input.schema);
    }
  }

  const size_t max_workers =
      QueryParallelWorkerLimit(std::thread::hardware_concurrency());
  bool parallel_sum_int_safe = true;
  for (size_t a = 0; a < plan.slim_kinds.size(); ++a) {
    const SlimAggKind kind = plan.slim_kinds[a];
    if (IsSlimDistinctKind(kind)) {
      parallel_sum_int_safe = false;
      break;
    }
    if (kind == SlimAggKind::kSumInt64) {
      if (a >= null_indicator_specs.size() || !null_indicator_specs[a].valid ||
          null_indicator_specs[a].then_int < 0 ||
          null_indicator_specs[a].else_int < 0) {
        parallel_sum_int_safe = false;
        break;
      }
    }
  }
  const bool can_parallel_scalar = plan.slim && total_rows >= 32768 &&
                                   max_workers > 1 && parallel_sum_int_safe;
  bool parallel_done = false;
  if (can_parallel_scalar) {
    const size_t workers =
        std::min<size_t>(max_workers, (total_rows + 16383) / 16384);
    if (workers > 1) {
      std::vector<TypedWorkerGroups> partials(workers);
      for (auto& partial : partials) {
        partial.Configure(true, 0, agg_count);
        partial.ConfigureSlim(plan);
        partial.AppendSlimOnlyGroup();
      }
      std::vector<Status> errors(workers, Status::kSuccess);
      std::vector<uint8_t> non_finite(workers, 0);
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
        const size_t begin = (total_rows * w) / workers;
        const size_t end = (total_rows * (w + 1)) / workers;
        if (begin >= end) {
          return;
        }
        size_t ci_hint = 0;
        auto [ci, lr_start] = TypedWorkerGroups::LocateColumnarChunkRow(
            chunk_prefix, static_cast<uint32_t>(begin), &ci_hint);
        size_t rem = end - begin;
        while (rem > 0) {
          const ColumnarChunk& ch = columnar.chunks[ci];
          const size_t take = std::min(rem, ch.rows - lr_start);
          bool nf = false;
          Status st = AccumulateScalarChunkSlice(
              plan, aggregates, null_indicator_specs, ch, lr_start, take, true,
              &partials[w], &nf, &errors[w]);
          if (!st.ok()) {
            if (errors[w].ok()) {
              errors[w] = st;
            }
            return;
          }
          if (nf) {
            non_finite[w] = 1;
            return;
          }
          rem -= take;
          ++ci;
          lr_start = 0;
        }
      }));
      for (size_t w = 0; w < workers; ++w) {
        RETURN_IF_FAIL(errors[w]);
      }
      if (std::ranges::none_of(non_finite,
                               [](uint8_t nf) { return nf != 0; })) {
        bool overflowed_to_inf = false;
        Status merge_err = Status::kSuccess;
        for (size_t a = 0; a < agg_count; ++a) {
          for (size_t w = 0; w < workers; ++w) {
            if (!MergeSlimCell(&groups, 0, partials[w], 0, a,
                               plan.slim_kinds[a], &merge_err)) {
              return merge_err;
            }
          }
          if ((plan.slim_kinds[a] == SlimAggKind::kSumDouble ||
               plan.slim_kinds[a] == SlimAggKind::kAvgDouble) &&
              !std::isfinite(groups.SlimDoubleTotal(0, a))) {
            overflowed_to_inf = true;
            break;
          }
        }
        if (!overflowed_to_inf) {
          parallel_done = true;
        } else {
          std::ranges::fill(groups.slim_counts, int64_t{0});
          std::ranges::fill(groups.slim_int_totals, int64_t{0});
          std::ranges::fill(groups.slim_double_totals, 0.0);
          std::ranges::fill(groups.slim_flags, uint8_t{0});
        }
      }
    }
  }
  if (!parallel_done) {
    for (const ColumnarChunk& ch : columnar.chunks) {
      if (ch.rows == 0) {
        continue;
      }
      bool dummy_nf = false;
      Status err{Status::kSuccess};
      Status st = AccumulateScalarChunkSlice(
          plan, aggregates, null_indicator_specs, ch, 0, ch.rows, false,
          &groups, &dummy_nf, &err);
      RETURN_IF_FAIL(st);
      RETURN_IF_FAIL(err);
    }
  }
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->aggregate_input_rows += total_rows;
  }
  ASSIGN_OR_RETURN(
      Relation, out,
      (FinishCompactGroups(context.execution_runtime(), statement, input,
                           aggregates, plan, groups, &having)));
  return std::optional<Relation>(std::move(out));
}

struct ColumnarKeyLocation {
  const ColumnarTable* columnar{nullptr};
  const std::vector<uint32_t>* chunk_prefix{nullptr};
  const std::vector<slot_t>* key_offsets{nullptr};
  bool single_int_key{false};
};

class ColumnarGroupLookup {
 public:
  // Hook invoked with the fresh group id right after a new group is
  // allocated (see on_new_group).
  using NewGroupFn = void (*)(void* ctx, size_t group, uint32_t global_r,
                              size_t ci, size_t lr, size_t hash);

  void Configure(ColumnarKeyLocation loc, bool borrow_keys, bool slim,
                 const std::vector<const AggregateExpression*>* aggregates,
                 TypedWorkerGroups* storage, NewGroupFn hook = nullptr,
                 void* hook_ctx = nullptr) {
    loc_ = loc;
    borrow_keys_ = borrow_keys;
    slim_ = slim;
    aggregates_ = aggregates;
    storage_ = storage;
    on_new_group = hook;
    on_new_group_ctx = hook_ctx;
  }

  void Reserve(size_t expected) {
    if (loc_.single_int_key) {
      int_map_.Reserve(expected);
      return;
    }
    size_t cap = slots_.empty() ? 64 : slots_.size();
    while ((expected + 1) * 10 > cap * 7) {
      cap *= 2;
    }
    if (cap > slots_.size()) {
      Rehash(cap);
    }
  }

  size_t FindOrInsert(uint32_t global_r, size_t ci, size_t lr, size_t hash,
                      bool* inserted, NewGroupFn on_inserted = nullptr,
                      void* on_inserted_ctx = nullptr) {
    const ColumnarChunk& ch = loc_.columnar->chunks[ci];
    const NewGroupFn new_group =
        on_inserted != nullptr ? on_inserted : on_new_group;
    const void* new_group_ctx =
        on_inserted != nullptr ? on_inserted_ctx : on_new_group_ctx;
    if (loc_.single_int_key) {
      const ColumnarColumn& col = ch.columns[(*loc_.key_offsets)[0]];
      if (col.type == ValueType::kNull || col.IsNull(lr)) {
        if (null_group_ == SIZE_MAX) {
          null_group_ = AllocateNewGroup(global_r, ci, lr, hash, new_group,
                                         new_group_ctx);
          *inserted = true;
        } else {
          *inserted = false;
        }
        return null_group_;
      }
      const int64_t kv = col.IntAt(lr);
      bool map_inserted = false;
      size_t* slot = int_map_.TryEmplace(kv, &map_inserted);
      if (map_inserted) {
        *slot =
            AllocateNewGroup(global_r, ci, lr, hash, new_group, new_group_ctx);
      }
      *inserted = map_inserted;
      return *slot;
    }
    if (slots_.empty()) {
      Rehash(64);
    }
    if ((storage_->GroupCount() + 1) * 10 > slots_.size() * 7) {
      Rehash(slots_.size() * 2);
    }
    size_t pos = hash & mask_;
    while (slots_[pos].group != SIZE_MAX) {
      const Slot& s = slots_[pos];
      if (s.hash == hash && MatchesChunkRow(s, ch, lr)) {
        *inserted = false;
        return s.group;
      }
      pos = (pos + 1) & mask_;
    }
    const size_t g =
        AllocateNewGroup(global_r, ci, lr, hash, new_group, new_group_ctx);
    slots_[pos] = Slot{.hash = hash,
                       .ci = static_cast<uint32_t>(ci),
                       .lr = static_cast<uint32_t>(lr),
                       .group = g};
    *inserted = true;
    return g;
  }

  [[nodiscard]] bool SameGroupRows(size_t ci_a, size_t lr_a, size_t ci_b,
                                   size_t lr_b) const {
    const ColumnarChunk& ca = loc_.columnar->chunks[ci_a];
    const ColumnarChunk& cb = loc_.columnar->chunks[ci_b];
    return std::ranges::all_of(*loc_.key_offsets, [&](const slot_t col_idx) {
      return ColumnarCellsEqual(ca.columns[col_idx], lr_a, cb.columns[col_idx],
                                lr_b);
    });
  }

 private:
  struct Slot {
    size_t hash{0};
    uint32_t ci{0};
    uint32_t lr{0};
    size_t group{SIZE_MAX};
  };

  size_t AllocateNewGroup(uint32_t global_r, size_t ci, size_t lr, size_t hash,
                          NewGroupFn on_inserted, const void* ctx) {
    const ColumnarChunk& ch = loc_.columnar->chunks[ci];
    auto finish = [&](size_t g) {
      if (on_inserted != nullptr) {
        on_inserted(const_cast<void*>(ctx), g, global_r, ci, lr, hash);
      }
      return g;
    };
    if (borrow_keys_ && slim_) {
      const size_t g = storage_->GroupCount();
      storage_->AppendBorrowedSlimGroupRowId(global_r);
      return finish(g);
    }
    if (!slim_) {
      std::vector<Value> canonical_keys(loc_.key_offsets->size());
      for (size_t k = 0; k < loc_.key_offsets->size(); ++k) {
        ExtractCanonicalColumnarKey(ch.columns[(*loc_.key_offsets)[k]], lr,
                                    &canonical_keys[k]);
      }
      const size_t g = storage_->GroupCount();
      storage_->AppendNonSlimGroup(canonical_keys, aggregates_->size(),
                                   *aggregates_);
      return finish(g);
    }
    // slim_ && !borrow_keys_ (for example DOUBLE/unsigned/interval keys)
    const size_t g = storage_->GroupCount();
    if (storage_->flat_group_keys.empty()) {
      storage_->flat_group_keys.reserve(64 * loc_.key_offsets->size());
    }
    Value tmp;
    for (unsigned short key_offset : *loc_.key_offsets) {
      ExtractCanonicalColumnarKey(ch.columns[key_offset], lr, &tmp);
      storage_->flat_group_keys.push_back(std::move(tmp));
    }
    storage_->AppendSlimOnlyGroup();
    return finish(g);
  }

  [[nodiscard]] bool MatchesChunkRow(const Slot& s, const ColumnarChunk& ch,
                                     size_t lr) const {
    const ColumnarChunk& rep_ch = loc_.columnar->chunks[s.ci];
    return std::ranges::all_of(*loc_.key_offsets, [&](const slot_t col_idx) {
      return ColumnarCellsEqual(rep_ch.columns[col_idx], s.lr,
                                ch.columns[col_idx], lr);
    });
  }

  void Rehash(size_t cap) {
    std::vector<Slot> next(cap);
    const size_t new_mask = cap - 1;
    for (const Slot& old : slots_) {
      if (old.group == SIZE_MAX) {
        continue;
      }
      size_t pos = old.hash & new_mask;
      while (next[pos].group != SIZE_MAX) {
        pos = (pos + 1) & new_mask;
      }
      next[pos] = old;
    }
    slots_ = std::move(next);
    mask_ = new_mask;
  }

  ColumnarKeyLocation loc_{};
  bool borrow_keys_{false};
  bool slim_{true};
  const std::vector<const AggregateExpression*>* aggregates_{nullptr};
  TypedWorkerGroups* storage_{nullptr};
  NewGroupFn on_new_group{nullptr};
  void* on_new_group_ctx{nullptr};
  FlatIntKeyMap<size_t> int_map_;
  size_t null_group_{SIZE_MAX};
  std::vector<Slot> slots_;
  size_t mask_{0};
};

// A checked SUM(INT64) must not be skew-split into contiguous row ranges:
// every private accumulator starts at 0, so its checked per-add wraps at
// points the canonical row-order accumulation does not — overflow is missed
// when it happens across a range boundary and fabricated when a partition-
// local sum wraps while the row-order running sum is still legal. The skew
// path instead tracks each range's exact running-sum excursion in int128
// (range-local wrap never happens) and merges the ranges sequentially in
// row order: a carry crossing int64 reproduces the canonical per-add
// overflow exactly, in both directions.
std::vector<size_t> CheckedSlimSumMeasures(const TypedAggregationPlan& plan) {
  std::vector<size_t> out;
  if (!plan.slim) {
    return out;
  }
  for (size_t i = 0; i < plan.slim_kinds.size(); ++i) {
    if (plan.slim_kinds[i] == SlimAggKind::kSumInt64) {
      out.push_back(i);
    }
  }
  return out;
}

struct CheckedSumRangeStats {
  __int128 sum{0};
  __int128 prefix_min{0};
  __int128 prefix_max{0};
  int64_t count{0};
};

using CheckedSumCarry = std::vector<CheckedSumRangeStats>;

struct CheckedSumProbeCtx {
  TypedWorkerGroups* storage{nullptr};
  const std::vector<CheckedSumCarry>* stats{nullptr};
  __int128 carry{0};
};

// Seeds a freshly allocated merge accumulator so its SUM(INT64) cells equal
// the canonical running state over every row of this group that precedes
// the merge point (carry sum + non-NULL count). `stats` is indexed by
// measure slot; only checked-SUM slots are read.
inline void SeedCheckedSumGroup(void* ctx, size_t group, uint32_t /*unused*/,
                                size_t /*unused*/, size_t /*unused*/,
                                size_t /*unused*/) {
  const CheckedSumProbeCtx& probe =
      *static_cast<const CheckedSumProbeCtx*>(ctx);
  if (group >= probe.stats->size()) {
    return;
  }
  const CheckedSumCarry& stats = (*probe.stats)[group];
  for (size_t i = 0; i < stats.size(); ++i) {
    if (stats[i].count == 0 && stats[i].sum == 0) {
      continue;
    }
    probe.storage->SlimIntTotal(group, i) =
        static_cast<int64_t>(probe.carry + stats[i].sum);
    probe.storage->SlimCount(group, i) = stats[i].count;
  }
}

// on_new_group hook: records the row id that materialized each group.
inline void PushCarryFirstRow(void* ctx, size_t /*unused*/, uint32_t global_r,
                              size_t /*unused*/, size_t /*unused*/,
                              size_t /*unused*/) {
  static_cast<std::vector<uint32_t>*>(ctx)->push_back(global_r);
}

// Exact per-group running-sum excursion of one contiguous row range
// [begin, end) for every checked SUM(INT64) measure. Output is indexed by
// this range's local group id, then by measure slot. A SUM argument that is
// not a plain columnar INT64 clears `out` (the caller then runs the whole
// accumulation single-threaded, which is trivially canonical).
Status CollectCheckedSumRangeStats(
    const TypedAggregationPlan& plan,
    const std::vector<const AggregateExpression*>& aggregates,
    const ColumnarTable& columnar, const std::vector<uint32_t>& chunk_prefix,
    const std::vector<size_t>& hashes, size_t begin, size_t end,
    const ColumnarKeyLocation& key_loc, std::vector<CheckedSumCarry>* out,
    std::vector<uint32_t>* first_rows_out) {
  struct Range {
    ColumnarGroupLookup lookup;
    TypedWorkerGroups storage;
    std::vector<uint32_t> ids;
    std::vector<CheckedSumCarry> stats;
    std::vector<uint32_t> first_rows;
  };
  const size_t agg_count = plan.aggregates.size();
  Range range;
  range.storage.Configure(true, key_loc.key_offsets->size(), agg_count);
  range.storage.ConfigureSlim(plan);
  range.lookup.Configure(key_loc, /*borrow_keys=*/false, /*slim=*/true,
                         &aggregates, &range.storage, &PushCarryFirstRow,
                         &range.first_rows);
  size_t ci_hint = 0;
  auto [ci, lr_start] = TypedWorkerGroups::LocateColumnarChunkRow(
      chunk_prefix, static_cast<uint32_t>(begin), &ci_hint);
  size_t rem = end - begin;
  size_t cur_r = begin;
  while (rem > 0) {
    const ColumnarChunk& ch = columnar.chunks[ci];
    const size_t take = std::min(rem, ch.rows - lr_start);
    range.ids.reserve(range.ids.size() + take);
    for (size_t i = 0; i < take; ++i) {
      const auto r = static_cast<uint32_t>(cur_r + i);
      bool inserted = false;
      const size_t local =
          range.lookup.FindOrInsert(r, ci, lr_start + i, hashes[r], &inserted);
      while (range.stats.size() <= local) {
        range.stats.emplace_back(agg_count);
      }
      range.ids.push_back(static_cast<uint32_t>(local));
    }
    rem -= take;
    cur_r += take;
    ++ci;
    lr_start = 0;
  }
  for (size_t a = 0; a < agg_count; ++a) {
    if (plan.slim_kinds[a] != SlimAggKind::kSumInt64) {
      continue;
    }
    const TypedAggregateInput& item = plan.aggregates[a];
    if (!item.column) {
      out->clear();  // not a plain column; caller falls back to serial
      return Status::kSuccess;
    }
    size_t i = 0;
    size_t ci2 = 0;
    auto [lc, lr2] = TypedWorkerGroups::LocateColumnarChunkRow(
        chunk_prefix, static_cast<uint32_t>(begin), &ci2);
    size_t left = end - begin;
    while (left > 0) {
      const ColumnarChunk& ch = columnar.chunks[lc];
      const size_t take = std::min(left, ch.rows - lr2);
      const ColumnarColumn& col = ch.columns[*item.column];
      for (size_t t = 0; t < take; ++t, ++i) {
        CheckedSumRangeStats& stats = range.stats[range.ids[i]][a];
        if (col.IsNull(lr2 + t)) {
          continue;
        }
        if (!col.is_uniform || col.is_unsigned ||
            col.type != ValueType::kInt64) {
          out->clear();  // not a plain int64 column; fall back to serial
          return Status::kSuccess;
        }
        const __int128 next =
            stats.sum + static_cast<__int128>(col.IntAt(lr2 + t));
        stats.sum = next;
        stats.prefix_min = std::min(stats.prefix_min, next);
        stats.prefix_max = std::max(stats.prefix_max, next);
        ++stats.count;
      }
      left -= take;
      ++lc;
      lr2 = 0;
    }
  }
  *out = std::move(range.stats);
  *first_rows_out = std::move(range.first_rows);
  return Status::kSuccess;
}

// Sequential (row-order) carry merge of the per-range checked-SUM stats.
// `first_rows[w][g]` is the row that created range w's local group g, so
// every local group of every range can be mapped to one global group id
// (carry_gid). Walking the ranges in row order then verifies the running
// per-group carry never leaves int64 — exactly the condition the canonical
// per-add check enforces, in both directions — and stores the exact
// canonical state just before range w's first row into
// carry_out[w][g][a] for the split accumulation to seed itself with.
Status MergeCheckedSumRanges(
    size_t workers, const std::vector<std::vector<CheckedSumCarry>>& per_range,
    const std::vector<std::vector<size_t>>& carry_gid, size_t total_groups,
    const std::vector<size_t>& checked_idx,
    std::vector<std::vector<CheckedSumCarry>>* carry_out) {
  constexpr __int128 kMin = -static_cast<__int128>(1) << 63;
  constexpr __int128 kMax = (static_cast<__int128>(1) << 63) - 1;
  const size_t n = checked_idx.size();
  std::vector<__int128> carry(total_groups * n, 0);
  std::vector<int64_t> count(total_groups * n, 0);
  for (size_t w = 0; w < workers; ++w) {
    const auto& stats = per_range[w];
    for (size_t g = 0; g < stats.size(); ++g) {
      const size_t base_idx = carry_gid[w][g] * n;
      for (size_t i = 0; i < n; ++i) {
        const size_t a = checked_idx[i];
        const CheckedSumRangeStats& s = stats[g][a];
        const __int128 before = carry[base_idx + i];
        if (before + s.prefix_min < kMin || before + s.prefix_max > kMax) {
          return StatusError(StatusCode::kIsInfinity,
                             "integer overflow in SUM");
        }
        (*carry_out)[w][g][a].sum = before;
        (*carry_out)[w][g][a].count = count[base_idx + i];
        carry[base_idx + i] = before + s.sum;
        count[base_idx + i] += s.count;
      }
    }
  }
  return Status::kSuccess;
}

void HashChunkSliceKeys(
    const ColumnarChunk& ch, size_t lr_start, size_t take,
    const std::vector<slot_t>& key_offsets,
    std::unordered_map<const LSTableColumnDictionary*, std::vector<size_t>>*
        dict_hash_cache,
    size_t* out_hashes) {
  const size_t kcols = key_offsets.size();
  const size_t seed = kcols * 0x9e3779b97f4a7c15ULL;
  std::fill_n(out_hashes, take, seed);
  for (size_t k = 0; k < kcols; ++k) {
    const ColumnarColumn& col = ch.columns[key_offsets[k]];
    const auto mix_into = [&](size_t i, size_t chash) {
      out_hashes[i] = (out_hashes[i] * 0x9e3779b97f4a7c15ULL) ^ chash;
    };
    if (col.is_uniform && !col.is_unsigned && !col.is_interval) {
      if (col.type == ValueType::kNull ||
          (col.encoding == ColumnarColumn::Encoding::kConstant &&
           col.const_is_null)) {
        for (size_t i = 0; i < take; ++i) {
          mix_into(i, kColumnarNullHash);
        }
        continue;
      }
      if (col.encoding == ColumnarColumn::Encoding::kConstant) {
        const size_t chash = HashColumnarCell(col, lr_start);
        for (size_t i = 0; i < take; ++i) {
          mix_into(i, chash);
        }
        continue;
      }
      if (col.encoding == ColumnarColumn::Encoding::kDictionary &&
          col.dict != nullptr &&
          (col.type == ValueType::kVarChar || col.type == ValueType::kInt64 ||
           col.type == ValueType::kDate || col.type == ValueType::kDouble)) {
        const size_t ec = col.DictEntryCount();
        auto& dhashes = (*dict_hash_cache)[col.dict];
        if (dhashes.size() < ec) {
          dhashes.resize(ec);
          if (col.type == ValueType::kVarChar) {
            for (size_t c = 0; c < ec; ++c) {
              dhashes[c] = static_cast<size_t>(
                  FastStringHash64(col.dict->StringEntryAt(c)));
            }
          } else if (col.type == ValueType::kDouble) {
            const auto entries = col.dict->DoubleEntries();
            for (size_t c = 0; c < ec; ++c) {
              double d = entries[c];
              if (d == 0.0) {
                d = 0.0;
              }
              dhashes[c] = std::isnan(d) ? kColumnarNanHash
                                         : static_cast<size_t>(MixCompactInt64(
                                               std::bit_cast<uint64_t>(d)));
            }
          } else {
            for (size_t c = 0; c < ec; ++c) {
              dhashes[c] = static_cast<size_t>(MixCompactInt64(
                  static_cast<uint64_t>(col.dict->IntEntryAt(c))));
            }
          }
        }
        const size_t* __restrict dh = dhashes.data();
        if (!col.has_null) {
          for (size_t i = 0; i < take; ++i) {
            mix_into(i, dh[col.DictCodeAt(lr_start + i)]);
          }
        } else {
          for (size_t i = 0; i < take; ++i) {
            const uint32_t code = col.DictCodeAt(lr_start + i);
            mix_into(i, (code == ColumnarColumn::kNullDictCode || code >= ec)
                            ? kColumnarNullHash
                            : dh[code]);
          }
        }
        continue;
      }
      if (col.IsFlat()) {
        if (col.type == ValueType::kInt64 || col.type == ValueType::kDate) {
          const int64_t* __restrict ints = col.ints.data() + lr_start;
          const uint8_t* __restrict nulls =
              col.has_null ? col.nulls.data() + lr_start : nullptr;
          for (size_t i = 0; i < take; ++i) {
            const size_t chash = (nulls != nullptr && nulls[i] != 0)
                                     ? kColumnarNullHash
                                     : static_cast<size_t>(MixCompactInt64(
                                           static_cast<uint64_t>(ints[i])));
            mix_into(i, chash);
          }
          continue;
        }
        if (col.type == ValueType::kVarChar) {
          const std::string_view* __restrict strs =
              col.strings.data() + lr_start;
          const uint8_t* __restrict nulls =
              col.has_null ? col.nulls.data() + lr_start : nullptr;
          for (size_t i = 0; i < take; ++i) {
            const size_t chash =
                (nulls != nullptr && nulls[i] != 0)
                    ? kColumnarNullHash
                    : static_cast<size_t>(FastStringHash64(strs[i]));
            mix_into(i, chash);
          }
          continue;
        }
        if (col.type == ValueType::kDouble) {
          const double* __restrict dbls = col.doubles.data() + lr_start;
          const uint8_t* __restrict nulls =
              col.has_null ? col.nulls.data() + lr_start : nullptr;
          for (size_t i = 0; i < take; ++i) {
            size_t chash = kColumnarNullHash;
            if (nulls == nullptr || nulls[i] == 0) {
              double d = dbls[i];
              if (d == 0.0) {
                d = 0.0;
              }
              chash = std::isnan(d) ? kColumnarNanHash
                                    : static_cast<size_t>(MixCompactInt64(
                                          std::bit_cast<uint64_t>(d)));
            }
            mix_into(i, chash);
          }
          continue;
        }
      }
    }
    for (size_t i = 0; i < take; ++i) {
      mix_into(i, HashColumnarCell(col, lr_start + i));
    }
  }
}

StatusOr<std::optional<Relation>> TryCompactColumnarKeyedAggregate(
    TransactionContext& context, const SelectStatement& statement,
    const Relation& input,
    const std::vector<const AggregateExpression*>& aggregates,
    const TypedAggregationPlan& plan,
    const std::vector<PositionalHaving>& having,
    const std::vector<uint32_t>& chunk_prefix) {
  const ColumnarTable& columnar = *input.columnar;
  const size_t total_rows = columnar.total_rows;
  const size_t agg_count = aggregates.size();
  const size_t key_count = plan.key_offsets.size();

  bool borrow_keys = true;
  bool single_int_key = key_count == 1;
  ValueType first_non_null_type = ValueType::kNull;
  for (const ColumnarChunk& ch : columnar.chunks) {
    if (ch.rows == 0) {
      continue;
    }
    for (size_t k = 0; k < key_count; ++k) {
      const ColumnarColumn& c = ch.columns[plan.key_offsets[k]];
      if (!c.is_uniform || c.is_unsigned || c.is_interval) {
        borrow_keys = false;
        single_int_key = false;
        break;
      }
      if (c.type != ValueType::kNull && c.type != ValueType::kInt64 &&
          c.type != ValueType::kDate && c.type != ValueType::kVarChar) {
        borrow_keys = false;
      }
      if (single_int_key && k == 0 && c.type != ValueType::kNull) {
        if ((c.type != ValueType::kInt64 && c.type != ValueType::kDate) ||
            (first_non_null_type != ValueType::kNull &&
             first_non_null_type != c.type)) {
          single_int_key = false;
        } else {
          first_non_null_type = c.type;
        }
      }
    }
  }
  if (single_int_key && first_non_null_type == ValueType::kNull) {
    // All-null single key column still works cleanly with single_int_key.
    single_int_key = true;
  }

  std::vector<AggregateAccumulator::Shape> shapes;
  if (!plan.slim) {
    shapes.reserve(agg_count);
    for (const AggregateExpression* agg : aggregates) {
      shapes.push_back(AggregateAccumulator::AnalyzeShape(agg, &input.schema));
    }
  }

  TypedWorkerGroups groups;
  groups.Configure(true, key_count, agg_count);
  groups.int_key = false;
  if (borrow_keys && plan.slim) {
    groups.borrowed_key_offsets = &plan.key_offsets;
    groups.borrowed_columnar = &columnar;
    groups.borrowed_chunk_prefix = &chunk_prefix;
  }
  if (plan.slim) {
    groups.ConfigureSlim(plan);
  } else {
    groups.shapes = &shapes;
  }

  const ColumnarKeyLocation key_loc{
      .columnar = &columnar,
      .chunk_prefix = &chunk_prefix,
      .key_offsets = &plan.key_offsets,
      .single_int_key = single_int_key,
  };

  const size_t max_workers = std::min<size_t>(
      8, QueryParallelWorkerLimit(std::thread::hardware_concurrency()));
  std::shared_ptr<QueryMemoryCharge> parallel_charge;
  if (plan.slim && total_rows >= 32768 && max_workers > 1 &&
      total_rows <= SIZE_MAX / 16) {
    parallel_charge = TryReserveQueryMemory(total_rows * 16);
  }

  if (parallel_charge != nullptr) {
    const size_t workers = std::min<size_t>(max_workers, total_rows / 8192);
    if (workers > 1) {
      std::vector<size_t> hashes(total_rows);
      std::vector<std::vector<std::vector<uint32_t>>> chunk_buckets(
          workers, std::vector<std::vector<uint32_t>>(workers));
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
        const size_t begin = (total_rows * w) / workers;
        const size_t end = (total_rows * (w + 1)) / workers;
        for (auto& bucket : chunk_buckets[w]) {
          bucket.reserve(((end - begin) / workers) + 16);
        }
        std::unordered_map<const LSTableColumnDictionary*, std::vector<size_t>>
            dict_cache;
        size_t ci_hint = 0;
        auto [ci, lr_start] = TypedWorkerGroups::LocateColumnarChunkRow(
            chunk_prefix, static_cast<uint32_t>(begin), &ci_hint);
        size_t rem = end - begin;
        size_t cur_r = begin;
        while (rem > 0) {
          const ColumnarChunk& ch = columnar.chunks[ci];
          const size_t take = std::min(rem, ch.rows - lr_start);
          HashChunkSliceKeys(ch, lr_start, take, plan.key_offsets, &dict_cache,
                             hashes.data() + cur_r);
          for (size_t i = 0; i < take; ++i) {
            const auto r = static_cast<uint32_t>(cur_r + i);
            chunk_buckets[w][hashes[r] % workers].push_back(r);
          }
          rem -= take;
          cur_r += take;
          ++ci;
          lr_start = 0;
        }
      }));

      size_t max_dest_rows = 0;
      for (size_t dest = 0; dest < workers; ++dest) {
        size_t dest_rows = 0;
        for (size_t src = 0; src < workers; ++src) {
          dest_rows += chunk_buckets[src][dest].size();
        }
        max_dest_rows = std::max(max_dest_rows, dest_rows);
      }
      // A skewed group's rows may only be split into contiguous per-worker
      // ranges when every measure's accumulation is invariant under the
      // re-association that split implies. A checked SUM(INT64) is not:
      // private per-range accumulators starting at 0 fabricate overflow
      // when a range-local sum wraps and miss it when the overflow happens
      // across a range boundary. For those the ranges are summarized
      // exactly instead (CollectCheckedSumRangeStats) and a sequential
      // carry chain validates and seeds the split accumulation in row
      // order (MergeCheckedSumRanges + SeedCheckedSumGroup below); a range
      // that cannot be summarized falls through to the bucket path, where
      // each group stays owned by one worker and rows keep their order.
      const bool may_skew =
          max_dest_rows * 4 >= total_rows * 3 && total_rows >= workers * 1024 &&
          std::ranges::none_of(plan.slim_kinds, IsSlimDistinctKind);
      const std::vector<size_t> checked_idx = CheckedSlimSumMeasures(plan);
      bool skewed = may_skew && checked_idx.empty();
      bool skew_carry_ready = false;
      std::vector<std::vector<CheckedSumCarry>> skew_carry(workers);
      if (may_skew && !checked_idx.empty()) {
        std::vector<std::vector<CheckedSumCarry>> range_stats(workers);
        std::vector<std::vector<uint32_t>> range_first(workers);
        std::vector<Status> stats_errors(workers, Status::kSuccess);
        RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
          const size_t begin = (total_rows * w) / workers;
          const size_t end = (total_rows * (w + 1)) / workers;
          if (begin >= end) {
            return;
          }
          stats_errors[w] = CollectCheckedSumRangeStats(
              plan, aggregates, columnar, chunk_prefix, hashes, begin, end,
              key_loc, &range_stats[w], &range_first[w]);
        }));
        bool usable = true;
        for (size_t w = 0; usable && w < workers; ++w) {
          usable = stats_errors[w].ok() && !range_stats[w].empty() &&
                   range_stats[w].size() == range_first[w].size();
        }
        if (usable) {
          // One global id per distinct group across all ranges, so the
          // carry chain follows each group through every range in order.
          TypedWorkerGroups carry_groups;
          carry_groups.Configure(true, key_count, agg_count);
          carry_groups.ConfigureSlim(plan);
          ColumnarGroupLookup carry_lookup;
          carry_lookup.Configure(key_loc, /*borrow_keys=*/false,
                                 /*slim=*/true, &aggregates, &carry_groups);
          std::vector<std::vector<size_t>> carry_gid(workers);
          for (size_t w = 0; w < workers; ++w) {
            carry_gid[w].resize(range_first[w].size());
            for (size_t g = 0; g < range_first[w].size(); ++g) {
              const uint32_t r = range_first[w][g];
              size_t hint = 0;
              const auto [ci, lr] = TypedWorkerGroups::LocateColumnarChunkRow(
                  chunk_prefix, r, &hint);
              bool inserted = false;
              carry_gid[w][g] =
                  carry_lookup.FindOrInsert(r, ci, lr, hashes[r], &inserted);
            }
          }
          for (size_t w = 0; w < workers; ++w) {
            skew_carry[w].resize(range_stats[w].size(),
                                 std::vector<CheckedSumRangeStats>(agg_count));
          }
          RETURN_IF_FAIL(MergeCheckedSumRanges(workers, range_stats, carry_gid,
                                               carry_groups.GroupCount(),
                                               checked_idx, &skew_carry));
          skewed = true;
          skew_carry_ready = true;
        }
      }

      std::vector<TypedWorkerGroups> partials(workers);
      std::vector<ColumnarGroupLookup> lookups(workers);
      std::vector<std::vector<uint32_t>> first_rows(workers);
      std::vector<Status> errors(workers, Status::kSuccess);
      std::vector<size_t> error_rows(workers, SIZE_MAX);
      std::vector<CheckedSumProbeCtx> probe_ctxs(workers);

      for (size_t w = 0; w < workers; ++w) {
        partials[w].Configure(true, key_count, agg_count);
        partials[w].ConfigureSlim(plan);
        probe_ctxs[w].storage = &partials[w];
        probe_ctxs[w].stats = &skew_carry[w];
        lookups[w].Configure(key_loc, /*borrow_keys=*/false, /*slim=*/true,
                             &aggregates, &partials[w],
                             skew_carry_ready ? &SeedCheckedSumGroup : nullptr,
                             skew_carry_ready ? &probe_ctxs[w] : nullptr);
      }

      if (skewed) {
        std::vector<std::vector<std::vector<uint32_t>>>().swap(chunk_buckets);
        RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
          const size_t begin = (total_rows * w) / workers;
          const size_t end = (total_rows * (w + 1)) / workers;
          lookups[w].Reserve(std::min<size_t>(end - begin, 4096));
          size_t ci_hint = 0;
          auto [ci, lr_start] = TypedWorkerGroups::LocateColumnarChunkRow(
              chunk_prefix, static_cast<uint32_t>(begin), &ci_hint);
          size_t rem = end - begin;
          size_t cur_r = begin;
          Row scratch_row;
          while (rem > 0) {
            const ColumnarChunk& ch = columnar.chunks[ci];
            const size_t take = std::min(rem, ch.rows - lr_start);
            for (size_t i = 0; i < take; ++i) {
              const auto r = static_cast<uint32_t>(cur_r + i);
              const size_t lr = lr_start + i;
              bool inserted = false;
              const size_t local =
                  lookups[w].FindOrInsert(r, ci, lr, hashes[r], &inserted);
              if (inserted) {
                first_rows[w].push_back(r);
              }
              bool scratch_ready = false;
              if (!AccumulateCompactColumnarRow(
                      plan, aggregates, ch, lr, &partials[w], local,
                      &scratch_row, &scratch_ready, &errors[w])) {
                error_rows[w] = r;
                return;
              }
            }
            rem -= take;
            cur_r += take;
            ++ci;
            lr_start = 0;
          }
        }));
      } else {
        RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
          size_t bucket_rows = 0;
          for (size_t src = 0; src < workers; ++src) {
            bucket_rows += chunk_buckets[src][w].size();
          }
          lookups[w].Reserve(std::min<size_t>((bucket_rows * 3) / 4, 8192));
          first_rows[w].reserve(std::min<size_t>(bucket_rows, 4096));
          Row scratch_row;
          for (size_t src = 0; src < workers; ++src) {
            size_t ci_hint = 0;
            for (const uint32_t r : chunk_buckets[src][w]) {
              const auto [ci, lr] = TypedWorkerGroups::LocateColumnarChunkRow(
                  chunk_prefix, r, &ci_hint);
              const ColumnarChunk& ch = columnar.chunks[ci];
              bool inserted = false;
              const size_t local =
                  lookups[w].FindOrInsert(r, ci, lr, hashes[r], &inserted);
              if (inserted) {
                first_rows[w].push_back(r);
              }
              bool scratch_ready = false;
              if (!AccumulateCompactColumnarRow(
                      plan, aggregates, ch, lr, &partials[w], local,
                      &scratch_row, &scratch_ready, &errors[w])) {
                error_rows[w] = r;
                return;
              }
            }
          }
        }));
        std::vector<std::vector<std::vector<uint32_t>>>().swap(chunk_buckets);
      }

      const auto first_err = std::ranges::min_element(error_rows);
      if (*first_err != SIZE_MAX) {
        return errors[static_cast<size_t>(first_err - error_rows.begin())];
      }

      if (skewed) {
        ColumnarGroupLookup merged_lookup;
        merged_lookup.Configure(key_loc, borrow_keys, /*slim=*/true,
                                &aggregates, &groups);
        for (size_t w = 0; w < workers; ++w) {
          const size_t ng = partials[w].GroupCount();
          if (skew_carry_ready && ng != skew_carry[w].size()) {
            return StatusError(StatusCode::kRuntimeError,
                               "skew aggregation state mismatch");
          }
          for (size_t local = 0; local < ng; ++local) {
            const uint32_t r = first_rows[w][local];
            size_t hint = 0;
            const auto [ci, lr] = TypedWorkerGroups::LocateColumnarChunkRow(
                chunk_prefix, r, &hint);
            bool inserted = false;
            const size_t global =
                merged_lookup.FindOrInsert(r, ci, lr, hashes[r], &inserted);
            Status merge_err = Status::kSuccess;
            for (size_t a = 0; a < agg_count; ++a) {
              if (skew_carry_ready &&
                  plan.slim_kinds[a] == SlimAggKind::kSumInt64) {
                // Carried exactly: seeded prefix plus this range's rows is
                // the group's state after range w, so the last range that
                // contains the group writes the final value (both sum and
                // count stay inside int64 for every surviving state).
                const CheckedSumRangeStats& seed = skew_carry[w][local][a];
                groups.SlimIntTotal(global, a) =
                    static_cast<int64_t>(seed.sum) +
                    partials[w].SlimIntTotal(local, a);
                groups.SlimCount(global, a) =
                    seed.count + partials[w].SlimCount(local, a);
                continue;
              }
              if (!MergeSlimCell(&groups, global, partials[w], local, a,
                                 plan.slim_kinds[a], &merge_err)) {
                return merge_err;
              }
            }
          }
        }
      } else {
        size_t total_groups = 0;
        std::vector<size_t> cursors(workers, 0);
        std::array<size_t, 8> active_w{};
        size_t active_n = 0;
        for (size_t w = 0; w < workers; ++w) {
          total_groups += partials[w].GroupCount();
          if (!first_rows[w].empty()) {
            active_w[active_n++] = w;
          }
        }
        groups.ReserveGroups(total_groups);
        groups.group_count = total_groups;
        if (borrow_keys) {
          groups.borrowed_key_row_ids.resize(total_groups);
        } else {
          groups.flat_group_keys.resize(total_groups * key_count);
        }
        std::vector<GlobalSourceSlot> global_src(total_groups);
        size_t hint = 0;
        for (size_t global = 0; global < total_groups; ++global) {
          size_t best_idx = 0;
          uint32_t best_r = first_rows[active_w[0]][cursors[active_w[0]]];
          for (size_t ai = 1; ai < active_n; ++ai) {
            const size_t w = active_w[ai];
            const uint32_t r = first_rows[w][cursors[w]];
            if (r < best_r) {
              best_r = r;
              best_idx = ai;
            }
          }
          const size_t best_w = active_w[best_idx];
          const size_t local = cursors[best_w]++;
          if (cursors[best_w] == first_rows[best_w].size()) {
            active_w[best_idx] = active_w[--active_n];
          }
          global_src[global] = GlobalSourceSlot{
              .worker = static_cast<uint32_t>(best_w),
              .local = static_cast<uint32_t>(local),
          };
          if (borrow_keys) {
            groups.borrowed_key_row_ids[global] = best_r;
          } else {
            const auto [ci, lr] = TypedWorkerGroups::LocateColumnarChunkRow(
                chunk_prefix, best_r, &hint);
            const ColumnarChunk& ch = columnar.chunks[ci];
            for (size_t k = 0; k < key_count; ++k) {
              ExtractCanonicalColumnarKey(
                  ch.columns[plan.key_offsets[k]], lr,
                  &groups.flat_group_keys[(global * key_count) + k]);
            }
          }
        }
        const size_t copy_workers = total_groups >= 4096 ? workers : size_t{1};
        RETURN_IF_FAIL(RunParallelTasks(copy_workers, [&](size_t w) {
          const size_t raw_begin = (total_groups * w) / copy_workers;
          const size_t raw_end = (total_groups * (w + 1)) / copy_workers;
          const size_t g_begin = w == 0 ? 0 : (raw_begin & ~size_t{63});
          const size_t g_end =
              w + 1 == copy_workers ? total_groups : (raw_end & ~size_t{63});
          for (size_t global = g_begin; global < g_end; ++global) {
            const GlobalSourceSlot src_slot = global_src[global];
            const auto& partial = partials[src_slot.worker];
            const size_t local = src_slot.local;
            for (size_t a = 0; a < agg_count; ++a) {
              groups.SlimCount(global, a) = partial.SlimCount(local, a);
              groups.SlimIntTotal(global, a) = partial.SlimIntTotal(local, a);
              groups.SlimDoubleTotal(global, a) =
                  partial.SlimDoubleTotal(local, a);
              groups.SlimFlags(global, a) = partial.SlimFlags(local, a);
            }
          }
        }));
      }

      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->aggregate_input_rows += total_rows;
      }
      ASSIGN_OR_RETURN(
          Relation, out,
          (FinishCompactGroups(context.execution_runtime(), statement, input,
                               aggregates, plan, groups, &having)));
      return std::optional<Relation>(std::move(out));
    }
  }

  // Serial keyed columnar path (small inputs, non-slim MIN/MAX, or low memory).
  ColumnarGroupLookup serial_lookup;
  serial_lookup.Configure(key_loc, borrow_keys && plan.slim, plan.slim,
                          &aggregates, &groups);
  serial_lookup.Reserve(std::min<size_t>(total_rows, 4096));
  std::unordered_map<const LSTableColumnDictionary*, std::vector<size_t>>
      dict_cache;
  std::vector<size_t> chunk_hashes;
  Row scratch_row;
  Status err{Status::kSuccess};
  uint32_t global_r = 0;
  for (size_t ci = 0; ci < columnar.chunks.size(); ++ci) {
    const ColumnarChunk& ch = columnar.chunks[ci];
    if (ch.rows == 0) {
      continue;
    }
    if (!single_int_key) {
      chunk_hashes.resize(ch.rows);
      HashChunkSliceKeys(ch, 0, ch.rows, plan.key_offsets, &dict_cache,
                         chunk_hashes.data());
    }
    for (size_t lr = 0; lr < ch.rows; ++lr, ++global_r) {
      const size_t h = single_int_key ? 0 : chunk_hashes[lr];
      bool inserted = false;
      const size_t g =
          serial_lookup.FindOrInsert(global_r, ci, lr, h, &inserted);
      bool scratch_ready = false;
      if (!AccumulateCompactColumnarRow(plan, aggregates, ch, lr, &groups, g,
                                        &scratch_row, &scratch_ready, &err)) {
        return err;
      }
    }
  }
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->aggregate_input_rows += total_rows;
  }
  ASSIGN_OR_RETURN(
      Relation, out,
      (FinishCompactGroups(context.execution_runtime(), statement, input,
                           aggregates, plan, groups, &having)));
  return std::optional<Relation>(std::move(out));
}

StatusOr<std::optional<Relation>> TryCompactColumnarRelationAggregate(
    TransactionContext& context, const SelectStatement& statement,
    const Relation& input,
    const std::vector<const AggregateExpression*>& aggregates,
    const TypedAggregationPlan& plan) {
  if (!input.HasColumnarOnly() || input.columnar == nullptr ||
      input.columnar->total_rows == 0 ||
      input.columnar->total_rows > std::numeric_limits<uint32_t>::max()) {
    return std::optional<Relation>{};
  }
  std::vector<PositionalHaving> having;
  if (statement.Having() && !TryCompilePositionalHavingConjuncts(
                                statement.Having(), plan.aggregates, &having)) {
    return std::optional<Relation>{};
  }
  if (statement.Having() && !plan.dependent_key_programs.empty()) {
    return std::optional<Relation>{};
  }
  const size_t schema_cols = input.schema.ColumnCount();
  for (const ColumnarChunk& ch : input.columnar->chunks) {
    if (ch.columns.size() < schema_cols) {
      return std::optional<Relation>{};
    }
  }
  std::vector<uint32_t> chunk_prefix;
  chunk_prefix.reserve(input.columnar->chunks.size() + 1);
  chunk_prefix.push_back(0);
  size_t prefix = 0;
  for (const ColumnarChunk& ch : input.columnar->chunks) {
    prefix += ch.rows;
    chunk_prefix.push_back(static_cast<uint32_t>(prefix));
  }
  if (plan.key_offsets.empty()) {
    return TryCompactColumnarScalarAggregate(
        context, statement, input, aggregates, plan, having, chunk_prefix);
  }
  return TryCompactColumnarKeyedAggregate(context, statement, input, aggregates,
                                          plan, having, chunk_prefix);
}

}  // namespace

StatusOr<std::optional<Relation>> TryCompactStatisticalAggregate(
    TransactionContext& context, const SelectStatement& statement,
    const Relation& input,
    const std::vector<const AggregateExpression*>& aggregates,
    const CteMap& ctes) {
  if (input.HasSpill() || input.rows.empty() || aggregates.empty() ||
      input.rows.size() > static_cast<size_t>(INT64_MAX) ||
      !statement.OrderBy().empty() || statement.Distinct() ||
      HasWindowFunctions(statement) ||
      std::getenv("TINYLAMB_DISABLE_COMPACT_STATISTICS") != nullptr) {
    return std::optional<Relation>{};
  }
  std::vector<slot_t> keys;
  for (const auto& key : statement.GroupBy()) {
    if (!key || key->Type() != TypeTag::kColumnValue) {
      return std::optional<Relation>{};
    }
    const auto column =
        LocalColumnOffset(input.schema, key->AsColumnValue().GetColumnName());
    if (!column) {
      return std::optional<Relation>{};
    }
    keys.push_back(static_cast<slot_t>(*column));
  }
  struct Argument {
    AggregationType kind;
    size_t state;
  };
  struct Input {
    std::optional<slot_t> column;
    bool average{false};
    bool statistics{false};
  };
  std::vector<Argument> arguments;
  std::vector<Input> inputs;
  const auto add_argument = [&](AggregationType kind,
                                std::optional<slot_t> column, bool statistics) {
    auto existing = std::ranges::find(inputs, column, &Input::column);
    if (existing == inputs.end()) {
      inputs.push_back({column});
      existing = inputs.end() - 1;
    }
    existing->average = existing->average || kind == AggregationType::kAvg;
    existing->statistics = existing->statistics || statistics;
    arguments.push_back({kind, static_cast<size_t>(existing - inputs.begin())});
  };
  bool has_statistics = false;
  for (const auto* aggregate : aggregates) {
    const auto kind = aggregate->GetType();
    const bool statistical = kind == AggregationType::kVarPop ||
                             kind == AggregationType::kVarSamp ||
                             kind == AggregationType::kStddevPop ||
                             kind == AggregationType::kStddevSamp;
    if ((!statistical && kind != AggregationType::kAvg &&
         kind != AggregationType::kCount) ||
        aggregate->Distinct() || aggregate->NeedsGroupContext() ||
        aggregate->WhereFilter() ||
        aggregate->Having() != AggregateHavingModifier::kNone ||
        aggregate->HavingCondition() || !aggregate->InnerOrderBy().empty() ||
        aggregate->InnerLimit() || aggregate->SecondaryArg() ||
        !aggregate->ExtraArgs().empty() || !aggregate->TrailingArgs().empty()) {
      return std::optional<Relation>{};
    }
    has_statistics = has_statistics || statistical;
    if (IsCountStar(*aggregate)) {
      add_argument(kind, std::nullopt, false);
      continue;
    }
    const auto& child = aggregate->Child();
    if (!child || child->Type() != TypeTag::kColumnValue) {
      return std::optional<Relation>{};
    }
    const auto column =
        LocalColumnOffset(input.schema, child->AsColumnValue().GetColumnName());
    if (!column || input.schema.GetColumn(*column).IsUnsigned()) {
      return std::optional<Relation>{};
    }
    add_argument(kind, static_cast<slot_t>(*column), statistical);
  }
  if (!has_statistics || !std::ranges::all_of(input.rows, [&](const Row& row) {
        return std::ranges::all_of(keys,
                                   [&](slot_t key) {
                                     const auto& value = row[key];
                                     return !value.IsCaseInsensitive() &&
                                            !value.IsUnsigned() &&
                                            (value.IsNull() ||
                                             value.type == ValueType::kInt64 ||
                                             value.type == ValueType::kDate ||
                                             value.type == ValueType::kVarChar);
                                   }) &&
               std::ranges::all_of(inputs, [&](const Input& argument) {
                 if (!argument.column) {
                   return true;
                 }
                 const auto& value = row[*argument.column];
                 return value.IsNull() || (!value.IsUnsigned() &&
                                           (value.type == ValueType::kInt64 ||
                                            value.type == ValueType::kDouble));
               });
      })) {
    return std::optional<Relation>{};
  }
  // These are the same sequential moments as AggregateAccumulator, including
  // extended precision and its un-clamped variance. Never merge partial
  // moments: every group's owner consumes its original row sequence.
  struct Moments {
    platform::ExtendedFloat sx{0};
    platform::ExtendedFloat sxx{0};
    double total{0};
    int64_t count{0};
  };
  const size_t bytes_per_row = 192 + (inputs.size() * sizeof(Moments) * 2);
  if (input.rows.size() > SIZE_MAX / bytes_per_row) {
    return std::optional<Relation>{};
  }
  auto charge = TryReserveQueryMemory(input.rows.size() * bytes_per_row);
  if (!charge) {
    return std::optional<Relation>{};
  }
  const size_t workers =
      keys.empty() ? 1 : ParallelWorkerCount(input.rows.size() / 8192);
  std::vector<size_t> hashes(input.rows.size());
  std::vector<std::vector<size_t>> partitions(workers);
  if (workers == 1) {
    for (size_t r = 0; r < input.rows.size(); ++r) {
      hashes[r] = TypedWorkerGroups::HashRowKey(input.rows[r], keys);
    }
    partitions[0].resize(input.rows.size());
    std::iota(partitions[0].begin(), partitions[0].end(), size_t{0});
  } else {
    std::vector<std::vector<std::vector<size_t>>> chunk_buckets(
        workers, std::vector<std::vector<size_t>>(workers));
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
      const size_t begin = (input.rows.size() * w) / workers;
      const size_t end = (input.rows.size() * (w + 1)) / workers;
      for (auto& bucket : chunk_buckets[w]) {
        bucket.reserve(((end - begin) / workers) + 16);
      }
      for (size_t r = begin; r < end; ++r) {
        const size_t h = TypedWorkerGroups::HashRowKey(input.rows[r], keys);
        hashes[r] = h;
        chunk_buckets[w][h % workers].push_back(r);
      }
    }));
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
      size_t total = 0;
      for (size_t src = 0; src < workers; ++src) {
        total += chunk_buckets[src][w].size();
      }
      partitions[w].reserve(total);
      for (size_t src = 0; src < workers; ++src) {
        partitions[w].insert(partitions[w].end(), chunk_buckets[src][w].begin(),
                             chunk_buckets[src][w].end());
      }
    }));
  }
  std::vector<TypedWorkerGroups> groups(workers);
  std::vector<std::vector<Moments>> moments(workers);
  std::vector<size_t> first(input.rows.size(), SIZE_MAX);
  for (auto& group : groups) {
    group.Configure(true, keys.size(), 0);
    group.borrowed_key_offsets = &keys;
  }
  RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
    std::vector<Value> scratch;
    for (const size_t r : partitions[w]) {
      bool inserted = false;
      const auto& row = input.rows[r];
      const size_t group = groups[w].FindOrInsertRow(row, keys, {}, &scratch,
                                                     &inserted, hashes[r]);
      if (inserted) {
        first[r] = group;
        moments[w].resize((group + 1) * inputs.size());
      }
      for (size_t a = 0; a < inputs.size(); ++a) {
        const auto& argument = inputs[a];
        auto& state = moments[w][(group * inputs.size()) + a];
        if (!argument.column) {
          ++state.count;
          continue;
        }
        const auto& value = row[*argument.column];
        if (value.IsNull()) {
          continue;
        }
        ++state.count;
        if (argument.average) {
          state.total += value.type == ValueType::kDouble
                             ? value.value.double_value
                             : static_cast<double>(value.value.int_value);
        }
        if (argument.statistics) {
          const platform::ExtendedFloat x =
              value.type == ValueType::kDouble
                  ? static_cast<platform::ExtendedFloat>(
                        value.value.double_value)
                  : static_cast<platform::ExtendedFloat>(value.value.int_value);
          state.sx += x;
          state.sxx += x * x;
        }
      }
    }
  }));
  Relation output(context.execution_runtime());
  CopyExecutionStats(&output, input);
  std::vector<Column> columns;
  columns.reserve(statement.SelectList().size());
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    columns.emplace_back(ProjectionName(statement.SelectList()[i], i),
                         ValueType::kNull);
  }
  const auto finish_row_into = [&](size_t r, AggregateResultMap* finished,
                                   const std::vector<Value*>& agg_slots,
                                   Relation* target) -> Status {
    for (size_t a = 0; a < arguments.size(); ++a) {
      const auto& state =
          moments[hashes[r] % workers]
                 [(first[r] * inputs.size()) + arguments[a].state];
      const auto kind = arguments[a].kind;
      Value value;
      if (kind == AggregationType::kCount) {
        value = Value(state.count);
      } else if (kind == AggregationType::kAvg) {
        if (state.count != 0) {
          value = Value(state.total / static_cast<double>(state.count));
        }
      } else {
        const bool sample = kind == AggregationType::kVarSamp ||
                            kind == AggregationType::kStddevSamp;
        if (state.count >= (sample ? 2 : 1)) {
          const platform::ExtendedFloat mean = state.sx / state.count;
          const platform::ExtendedFloat ssd =
              state.sxx - (state.count * mean * mean);
          const platform::ExtendedFloat variance =
              ssd / (sample ? state.count - 1 : state.count);
          value = Value(
              static_cast<double>(kind == AggregationType::kStddevPop ||
                                          kind == AggregationType::kStddevSamp
                                      ? platform::Sqrt(variance)
                                      : variance));
        }
      }
      *agg_slots[a] = std::move(value);
    }
    Scope scope{.row = &input.rows[r], .schema = &input.schema};
    if (statement.Having()) {
      ASSIGN_OR_RETURN(
          Value, accepted,
          TryEvaluate(statement.Having(), scope, finished, context, ctes));
      if (!Truthy(accepted)) {
        return Status::kSuccess;
      }
    }
    Row row;
    row.values_.reserve(statement.SelectList().size());
    for (const auto& item : statement.SelectList()) {
      ASSIGN_OR_RETURN(
          Value, value,
          TryEvaluate(item.expression, scope, finished, context, ctes));
      row.values_.push_back(std::move(value));
    }
    target->AddRow(std::move(row));
    return Status::kSuccess;
  };
  const size_t emit_workers = ParallelWorkerCount(first.size() / 8192);
  const bool can_parallel_emit =
      emit_workers > 1 && first.size() >= 16384 &&
      !StatementContainsVolatileFunction(statement) &&
      !ContainsQuery(statement.Having()) &&
      std::ranges::none_of(statement.SelectList(), [](const auto& item) {
        return ContainsQuery(item.expression);
      });
  if (can_parallel_emit) {
    std::vector<Relation> pieces;
    pieces.reserve(emit_workers);
    for (size_t w = 0; w < emit_workers; ++w) {
      pieces.emplace_back(nullptr);
    }
    std::vector<Status> emit_errors(emit_workers, Status::kSuccess);
    RETURN_IF_FAIL(RunParallelTasks(emit_workers, [&](size_t w) {
      const size_t begin = (first.size() * w) / emit_workers;
      const size_t end = (first.size() * (w + 1)) / emit_workers;
      AggregateResultMap finished;
      finished.reserve(arguments.size());
      std::vector<Value*> agg_slots(arguments.size(), nullptr);
      for (size_t a = 0; a < arguments.size(); ++a) {
        agg_slots[a] = &finished.emplace(aggregates[a], Value()).first->second;
      }
      for (size_t r = begin; r < end; ++r) {
        if (first[r] == SIZE_MAX) {
          continue;
        }
        const Status st = finish_row_into(r, &finished, agg_slots, &pieces[w]);
        if (st != Status::kSuccess) {
          emit_errors[w] = st;
          return;
        }
      }
    }));
    for (const Status& st : emit_errors) {
      RETURN_IF_FAIL(st);
    }
    for (auto& piece : pieces) {
      RETURN_IF_FAIL(piece.GetStatus());
      for (auto& row : piece.rows) {
        output.AddRow(std::move(row));
      }
    }
  } else {
    AggregateResultMap finished;
    finished.reserve(arguments.size());
    std::vector<Value*> agg_slots(arguments.size(), nullptr);
    for (size_t a = 0; a < arguments.size(); ++a) {
      agg_slots[a] = &finished.emplace(aggregates[a], Value()).first->second;
    }
    for (size_t r = 0; r < first.size(); ++r) {
      if (first[r] == SIZE_MAX) {
        continue;
      }
      RETURN_IF_FAIL(finish_row_into(r, &finished, agg_slots, &output));
    }
  }
  if (!output.rows.empty()) {
    for (size_t i = 0; i < columns.size(); ++i) {
      columns[i] =
          Column(columns[i].Name(), ValueTypeOf(output.rows.front()[i]));
      columns[i].SetUnsigned(output.rows.front()[i].IsUnsigned());
    }
  }
  output.schema = Schema("", std::move(columns));
  RETURN_IF_FAIL(output.FinishSpill());
  RETURN_IF_FAIL(output.GetStatus());
  if (auto* runtime = context.execution_runtime()) {
    runtime->typed_parallel_aggregations += workers > 1 ? 1 : 0;
    runtime->aggregate_input_rows += input.rows.size();
    runtime->aggregate_groups += static_cast<size_t>(std::ranges::count_if(
        first, [](size_t group) { return group != SIZE_MAX; }));
    runtime->aggregate_updates += input.rows.size() * arguments.size();
  }
  return std::optional<Relation>(std::move(output));
}

StatusOr<std::optional<Relation>> TryCompactRelationAggregate(
    TransactionContext& context, const SelectStatement& statement,
    const Relation& input,
    const std::vector<const AggregateExpression*>& aggregates) {
  const bool columnar_only = input.HasColumnarOnly();
  if (input.HasSpill() ||
      (!columnar_only && input.rows.size() != input.TotalRows()) ||
      !statement.OrderBy().empty() || HasWindowFunctions(statement)) {
    return std::optional<Relation>{};
  }
  auto plan =
      PlanTypedAggregation(statement, nullptr, input.schema, aggregates, {});
  if (!plan.eligible ||
      std::ranges::any_of(aggregates, [](const auto* aggregate) {
        return aggregate->Distinct();
      })) {
    return std::optional<Relation>{};
  }
  if (columnar_only) {
    return TryCompactColumnarRelationAggregate(context, statement, input,
                                               aggregates, plan);
  }
  std::vector<PositionalHaving> having;
  if (statement.Having() && !TryCompilePositionalHavingConjuncts(
                                statement.Having(), plan.aggregates, &having)) {
    return std::optional<Relation>{};
  }
  // Reserve a conservative bound for keys, hash slots, and padded slim state.
  // Oversized inputs keep the existing partitioned aggregation path.
  const size_t group_bytes =
      128 + (plan.key_offsets.size() * sizeof(Value) * 2) +
      (aggregates.size() * (plan.slim ? 64 : sizeof(AggregateAccumulator)));
  if (input.TotalRows() > SIZE_MAX - 4096 ||
      input.TotalRows() + 4096 >
          (SIZE_MAX - input.charged_bytes_) / group_bytes) {
    return std::optional<Relation>{};
  }
  const size_t bytes =
      input.charged_bytes_ + ((input.TotalRows() + 4096) * group_bytes);
  if (!QueryMemoryBudget::Global().CanReserve(bytes)) {
    return std::optional<Relation>{};
  }
  QueryMemoryCharge charge(bytes);
  const auto started = std::chrono::steady_clock::now();
  TypedWorkerGroups groups;
  groups.int_key =
      plan.key_offsets.size() == 1 &&
      (input.schema.GetColumn(plan.key_offsets[0]).Type() ==
           ValueType::kInt64 ||
       input.schema.GetColumn(plan.key_offsets[0]).Type() == ValueType::kDate);
  groups.Configure(true, plan.key_offsets.size(), plan.aggregates.size());
  const bool borrow_keys =
      !input.rows.empty() &&
      std::ranges::all_of(input.rows, [&](const Row& row) {
        return std::ranges::all_of(plan.key_offsets, [&](slot_t slot) {
          const auto& value = row[slot];
          return !value.IsCaseInsensitive() && !value.IsUnsigned() &&
                 (value.IsNull() || value.type == ValueType::kInt64 ||
                  value.type == ValueType::kDate ||
                  value.type == ValueType::kVarChar);
        });
      });
  if (borrow_keys) {
    groups.borrowed_key_offsets = &plan.key_offsets;
  }
  std::vector<AggregateAccumulator::Shape> shapes;
  if (plan.slim) {
    groups.ConfigureSlim(plan);
  } else {
    for (const auto* aggregate : aggregates) {
      shapes.push_back(
          AggregateAccumulator::AnalyzeShape(aggregate, &input.schema));
    }
    groups.shapes = &shapes;
  }
  // A scalar aggregate has exactly one state. It does not need to hash the
  // empty key again for each row (notably an AVG over UNION ALL inputs).
  const bool scalar = plan.key_offsets.empty();
  if (scalar && !input.rows.empty()) {
    bool inserted = false;
    static_cast<void>(
        groups.FindOrInsert({}, 0, aggregates.size(), aggregates, &inserted));
  }
  const auto accumulate = [&](TypedWorkerGroups& target, size_t group,
                              const Row& row) -> Status {
    for (size_t i = 0; i < plan.aggregates.size(); ++i) {
      const auto& item = plan.aggregates[i];
      Value computed;
      const Value* value = nullptr;
      if (item.column) {
        value = &row[*item.column];
      } else if (item.conditional) {
        computed = EvaluateTypedConditionalOnRow(*item.conditional, row);
        value = &computed;
      } else if (item.double_expr) {
        computed = EvaluateTypedDoubleExprOnRow(*item.double_expr, row);
        value = &computed;
      } else if (item.int_expr) {
        ASSIGN_OR_RETURN(Value, evaluated,
                         EvaluateTypedIntExprOnRow(*item.int_expr, row));
        computed = std::move(evaluated);
        value = &computed;
      } else if (item.child) {
        ASSIGN_OR_RETURN(Value, evaluated,
                         TryEvaluateRowAuto(*item.child, row));
        computed = std::move(evaluated);
        value = &computed;
      }
      if (!plan.slim) {
        RETURN_IF_FAIL(target.State(group, i).Add(value ? *value : Value(1)));
        continue;
      }
      Status error{Status::kSuccess};
      if (!ApplySlimValue(&target, group, i, plan.slim_kinds[i], value,
                          &error)) {
        return error;
      }
    }
    return Status::kSuccess;
  };
  static const bool partition_disabled =
      std::getenv("TINYLAMB_DISABLE_COMPACT_PARTITION_AGG") != nullptr;
  const size_t partitions = ParallelWorkerCount(input.rows.size() / 8192);
  const bool allow_scalar_parallel =
      scalar && !partition_disabled && plan.slim && partitions > 1 &&
      input.rows.size() >= 32768 &&
      std::ranges::none_of(plan.slim_kinds, [](SlimAggKind kind) {
        return kind == SlimAggKind::kSumInt64 || IsSlimDistinctKind(kind);
      });
  bool scalar_parallel_done = false;
  if (allow_scalar_parallel) {
    std::vector<TypedWorkerGroups> local(partitions);
    for (auto& part : local) {
      part.Configure(true, 0, plan.aggregates.size());
      part.ConfigureSlim(plan);
      bool ins = false;
      static_cast<void>(
          part.FindOrInsert({}, 0, aggregates.size(), aggregates, &ins));
    }
    std::vector<Status> errors(partitions, Status::kSuccess);
    std::vector<size_t> error_rows(partitions, SIZE_MAX);
    RETURN_IF_FAIL(RunParallelTasks(partitions, [&](size_t p) {
      const size_t begin = (input.rows.size() * p) / partitions;
      const size_t end = (input.rows.size() * (p + 1)) / partitions;
      for (size_t r = begin; r < end; ++r) {
        const Status status = accumulate(local[p], 0, input.rows[r]);
        if (status != Status::kSuccess) {
          errors[p] = status;
          error_rows[p] = r;
          return;
        }
      }
    }));
    const auto first_err = std::ranges::min_element(error_rows);
    if (*first_err != SIZE_MAX) {
      return errors[static_cast<size_t>(first_err - error_rows.begin())];
    }
    bool nonfinite_sum_double = false;
    for (size_t p = 0; p < partitions && !nonfinite_sum_double; ++p) {
      for (size_t a = 0; a < aggregates.size(); ++a) {
        if (plan.slim_kinds[a] == SlimAggKind::kSumDouble &&
            ((local[p].SlimFlags(0, a) & 1) != 0 ||
             !std::isfinite(local[p].SlimDoubleTotal(0, a)))) {
          nonfinite_sum_double = true;
          break;
        }
      }
    }
    if (!nonfinite_sum_double) {
      Status merge_err = Status::kSuccess;
      for (size_t p = 0; p < partitions; ++p) {
        for (size_t a = 0; a < aggregates.size(); ++a) {
          if (!MergeSlimCell(&groups, 0, local[p], 0, a, plan.slim_kinds[a],
                             &merge_err)) {
            return merge_err;
          }
        }
      }
      scalar_parallel_done = true;
      if (auto* runtime = context.execution_runtime()) {
        ++runtime->typed_parallel_aggregations;
      }
    }
  }
  const size_t staging_per_row = 160 + (plan.aggregates.size() * 64);
  const size_t padding = partitions * 4096 * plan.aggregates.size() * 64;
  auto partition_charge =
      !scalar_parallel_done && !partition_disabled && borrow_keys && !scalar &&
              plan.slim && partitions > 1 && input.rows.size() >= 32768 &&
              input.rows.size() <= (SIZE_MAX - padding) / staging_per_row
          ? TryReserveQueryMemory((input.rows.size() * staging_per_row) +
                                  padding)
          : nullptr;
  if (partition_charge) {
    // Each key has one owner; its original row order is retained. This also
    // avoids reassociating floating sums or hiding intermediate overflows.
    std::vector<size_t> hashes(input.rows.size());
    RETURN_IF_FAIL(RunParallelTasks(partitions, [&](size_t p) {
      const size_t begin = input.rows.size() * p / partitions;
      const size_t end = input.rows.size() * (p + 1) / partitions;
      for (size_t r = begin; r < end; ++r) {
        hashes[r] =
            TypedWorkerGroups::HashRowKey(input.rows[r], plan.key_offsets);
      }
    }));
    std::vector<std::vector<size_t>> rows(partitions);
    for (auto& bucket : rows) {
      bucket.reserve(input.rows.size() / partitions);
    }
    for (size_t r = 0; r < input.rows.size(); ++r) {
      rows[hashes[r] % partitions].push_back(r);
    }
    std::vector<TypedWorkerGroups> local(partitions);
    for (auto& part : local) {
      part.Configure(true, plan.key_offsets.size(), plan.aggregates.size());
      part.ConfigureSlim(plan);
      part.int_key = groups.int_key;
      part.borrowed_key_offsets = &plan.key_offsets;
    }
    std::vector<size_t> first(input.rows.size(), SIZE_MAX);
    std::vector<Status> errors(partitions, Status::kSuccess);
    std::vector<size_t> error_rows(partitions, SIZE_MAX);
    RETURN_IF_FAIL(RunParallelTasks(partitions, [&](size_t p) {
      std::vector<Value> keys;
      for (const size_t r : rows[p]) {
        bool inserted = false;
        const size_t group =
            local[p].FindOrInsertRow(input.rows[r], plan.key_offsets,
                                     aggregates, &keys, &inserted, hashes[r]);
        if (inserted) {
          first[r] = group;
        }
        const Status status = accumulate(local[p], group, input.rows[r]);
        if (status != Status::kSuccess) {
          errors[p] = status;
          error_rows[p] = r;
          return;
        }
      }
    }));
    const auto error = std::ranges::min_element(error_rows);
    if (*error != SIZE_MAX) {
      return errors[static_cast<size_t>(error - error_rows.begin())];
    }
    size_t group_count = 0;
    for (const auto& part : local) {
      group_count += part.GroupCount();
    }
    groups.ReserveGroups(group_count);
    // No second hash build: emit only needs group IDs and their keys/states.
    // Encounter-order adoption keeps LIMIT/tie behavior deterministic.
    for (size_t r = 0; r < first.size(); ++r) {
      if (first[r] == SIZE_MAX) {
        continue;
      }
      auto& part = local[hashes[r] % partitions];
      const size_t source = first[r];
      const size_t group = groups.group_count++;
      groups.borrowed_key_rows.push_back(&input.rows[r]);
      for (size_t a = 0; a < aggregates.size(); ++a) {
        groups.SlimCount(group, a) = part.SlimCount(source, a);
        groups.SlimIntTotal(group, a) = part.SlimIntTotal(source, a);
        groups.SlimDoubleTotal(group, a) = part.SlimDoubleTotal(source, a);
        groups.SlimFlags(group, a) = part.SlimFlags(source, a);
      }
    }
    if (auto* runtime = context.execution_runtime()) {
      ++runtime->typed_parallel_aggregations;
    }
  } else if (!scalar_parallel_done) {
    std::vector<Value> keys;
    keys.reserve(plan.key_offsets.size());
    for (const auto& row : input.rows) {
      bool inserted = false;
      const size_t group =
          scalar ? 0
                 : groups.FindOrInsertRow(row, plan.key_offsets, aggregates,
                                          &keys, &inserted);
      RETURN_IF_FAIL(accumulate(groups, group, row));
    }
  }
  if (groups.GroupCount() == 0 &&
      (statement.GroupBy().empty() || statement.IsGroupAll())) {
    bool inserted = false;
    static_cast<void>(
        groups.FindOrInsert({}, 0, aggregates.size(), aggregates, &inserted));
  }
  ASSIGN_OR_RETURN(
      Relation, output,
      (FinishCompactGroups(context.execution_runtime(), statement, input,
                           aggregates, plan, groups, &having)));
  if (auto* runtime = context.execution_runtime()) {
    runtime->project_ms += ElapsedMs(started);
  }
  return std::optional<Relation>(std::move(output));
}

// Factor the finest grouping key once per input row. Every coarser set
// maps that key's group id to its own state, so repeated strings and computed
// aggregate arguments are not evaluated again for every ROLLUP branch.
// Accumulators still receive the original rows in original order.
StatusOr<std::optional<std::vector<Relation>>> TryCompactGroupingSets(
    TransactionContext& context,
    const std::vector<const SelectStatement*>& sets, Relation& input,
    bool finest_keys_unique) {
  if (sets.size() < 3 || sets.size() > 32 || input.HasSpill()) {
    return std::optional<std::vector<Relation>>{};
  }
  if (input.HasColumnarOnly() &&
      input.TotalRows() > std::numeric_limits<uint32_t>::max()) {
    // T4539: bail out of the compaction (not a wrong answer: the generic
    // path takes over) instead of compacting over an empty row set.
    if (!input.EnsureRowsMaterialized().ok()) {
      return std::optional<std::vector<Relation>>{};
    }
  }
  const bool is_columnar_input = input.HasColumnarOnly();
  const size_t total_rows = input.TotalRows();
  std::vector<uint32_t> chunk_prefix;
  if (is_columnar_input) {
    chunk_prefix.reserve(input.columnar->chunks.size() + 1);
    uint64_t prefix = 0;
    chunk_prefix.push_back(0);
    for (const ColumnarChunk& ch : input.columnar->chunks) {
      prefix += ch.rows;
      chunk_prefix.push_back(static_cast<uint32_t>(prefix));
    }
  }
  const size_t count = sets.size();
  std::vector<TypedAggregationPlan> plans;
  plans.reserve(count);
  std::vector<std::vector<const AggregateExpression*>> aggregates(count);
  std::vector<std::vector<AggregateAccumulator::Shape>> shapes(count);
  std::vector<std::vector<size_t>> key_maps(count);
  for (size_t s = 0; s < count; ++s) {
    const auto& statement = *sets[s];
    if (statement.Having() || statement.Qualify() ||
        !statement.OrderBy().empty() || HasWindowFunctions(statement) ||
        StatementContainsVolatileFunction(statement)) {
      return std::optional<std::vector<Relation>>{};
    }
    std::unordered_set<const AggregateExpression*> seen;
    for (const auto& item : statement.SelectList()) {
      CollectAggregates(item.expression, &aggregates[s], &seen);
    }
    auto plan = PlanTypedAggregation(statement, nullptr, input.schema,
                                     aggregates[s], {},
                                     /*allow_composite_select=*/true);
    if (!plan.eligible || !plan.dependent_key_programs.empty() ||
        aggregates[s].empty() ||
        std::ranges::any_of(
            aggregates[s],
            [](const auto* aggregate) { return aggregate->Distinct(); }) ||
        (s == 0 && plan.key_offsets.empty()) ||
        aggregates[s].size() != aggregates[0].size()) {
      return std::optional<std::vector<Relation>>{};
    }
    for (size_t a = 0; a < aggregates[s].size(); ++a) {
      if (aggregates[s][a]->ToString() != aggregates[0][a]->ToString()) {
        return std::optional<std::vector<Relation>>{};
      }
      shapes[s].push_back(
          AggregateAccumulator::AnalyzeShape(aggregates[s][a], &input.schema));
    }
    const auto& finest = s == 0 ? plan.key_offsets : plans[0].key_offsets;
    for (const auto slot : plan.key_offsets) {
      const auto found = std::ranges::find(finest, slot);
      if (found == finest.end()) {
        return std::optional<std::vector<Relation>>{};
      }
      key_maps[s].push_back(
          static_cast<size_t>(std::distance(finest.begin(), found)));
    }
    plans.push_back(std::move(plan));
  }
  std::vector<bool> mixed_numeric_sums(aggregates[0].size(), false);
  bool can_use_slim = !plans[0].slim;
  bool needs_dynamic_sum = false;
  std::vector<slot_t> numeric_columns;
  const std::function<bool(const Expression&)> numeric_expression =
      [&](const Expression& expression) {
        if (!expression) {
          return false;
        }
        if (expression->Type() == TypeTag::kConstantValue) {
          const auto& value = expression->AsConstantValue().GetValue();
          return value.IsNull() ||
                 (!value.IsUnsigned() && (value.type == ValueType::kInt64 ||
                                          value.type == ValueType::kDouble));
        }
        if (expression->Type() == TypeTag::kColumnValue) {
          const auto offset = LocalColumnOffset(
              input.schema, expression->AsColumnValue().GetColumnName());
          if (!offset) {
            return false;
          }
          const auto& column = input.schema.GetColumn(*offset);
          if (column.IsUnsigned() || (column.Type() != ValueType::kInt64 &&
                                      column.Type() != ValueType::kDouble)) {
            return false;
          }
          numeric_columns.push_back(static_cast<slot_t>(*offset));
          return true;
        }
        if (expression->Type() == TypeTag::kBinaryExp) {
          const auto& binary = expression->AsBinaryExpression();
          const auto op = binary.Op();
          return (op == BinaryOperation::kAdd ||
                  op == BinaryOperation::kSubtract ||
                  op == BinaryOperation::kMultiply ||
                  op == BinaryOperation::kDivide ||
                  op == BinaryOperation::kModulo) &&
                 numeric_expression(binary.Left()) &&
                 numeric_expression(binary.Right());
        }
        if (expression->Type() == TypeTag::kFunctionCallExp) {
          const auto& function = expression->AsFunctionCallExpression();
          return (IdentifierEquals(function.FuncName(), "coalesce") ||
                  IdentifierEquals(function.FuncName(), "ifnull")) &&
                 std::ranges::all_of(function.Args(), numeric_expression);
        }
        return false;
      };
  for (size_t a = 0; can_use_slim && a < aggregates[0].size(); ++a) {
    if (plans[0].slim_kinds[a] != SlimAggKind::kNone) {
      continue;
    }
    if (aggregates[0][a]->GetType() != AggregationType::kSum ||
        !plans[0].aggregates[a].child ||
        !numeric_expression(aggregates[0][a]->Child())) {
      can_use_slim = false;
      break;
    }
    mixed_numeric_sums[a] = true;
    needs_dynamic_sum = true;
  }
  std::ranges::sort(numeric_columns);
  numeric_columns.erase(
      std::unique(numeric_columns.begin(), numeric_columns.end()),
      numeric_columns.end());
  for (const slot_t column : numeric_columns) {
    if (!can_use_slim) {
      break;
    }
    const auto type = input.schema.GetColumn(column).Type();
    if (is_columnar_input) {
      for (const ColumnarChunk& ch : input.columnar->chunks) {
        const ColumnarColumn& c = ch.columns[column];
        if (c.is_uniform) {
          if (c.type != ValueType::kNull && (c.type != type || c.is_unsigned)) {
            can_use_slim = false;
            break;
          }
        } else {
          Value val;
          for (size_t r = 0; r < ch.rows; ++r) {
            c.WriteValueInto(r, &val);
            if (!val.IsNull() && (val.type != type || val.IsUnsigned())) {
              can_use_slim = false;
              break;
            }
          }
          if (!can_use_slim) {
            break;
          }
        }
      }
    } else {
      can_use_slim = std::ranges::all_of(input.rows, [&](const Row& row) {
        return row[column].IsNull() ||
               (row[column].type == type && !row[column].IsUnsigned());
      });
    }
  }
  if (can_use_slim && needs_dynamic_sum) {
    for (auto& plan : plans) {
      plan.slim = true;
      for (size_t a = 0; a < mixed_numeric_sums.size(); ++a) {
        if (mixed_numeric_sums[a]) {
          plan.slim_kinds[a] = SlimAggKind::kSumDouble;
        }
      }
    }
  } else {
    mixed_numeric_sums.clear();
  }
  // Dynamic numeric expressions may have just qualified for slim state.
  // Account for the selected representation, not the discarded generic
  // accumulator bound, before reserving optional parallel workspaces.
  size_t per_row = count * sizeof(size_t);
  for (const auto& plan : plans) {
    per_row += 128 + (plan.key_offsets.size() * sizeof(Value) * 2) +
               (plan.aggregates.size() *
                (plan.slim ? 64 : sizeof(AggregateAccumulator)));
  }
  if (per_row == 0 || total_rows > SIZE_MAX - 4096 ||
      total_rows + 4096 > (SIZE_MAX - input.charged_bytes_) / per_row) {
    return std::optional<std::vector<Relation>>{};
  }
  const size_t bytes = input.charged_bytes_ + ((total_rows + 4096) * per_row);
  if (!QueryMemoryBudget::Global().CanReserve(bytes)) {
    return std::optional<std::vector<Relation>>{};
  }
  QueryMemoryCharge charge(bytes);
  const auto started = std::chrono::steady_clock::now();
  const bool group_phases =
      std::getenv("TINYLAMB_GROUP_PHASES") != nullptr && total_rows >= 100000;
  const auto phase = [&](const char* name) {
    if (!group_phases) {
      return;
    }
    const double ms = std::chrono::duration<double, std::milli>(
                          std::chrono::steady_clock::now() - started)
                          .count();
    std::cerr << "[group] " << name << " " << ms << " rows=" << total_rows
              << " sets=" << count << "\n";
  };
  const size_t agg_workers = std::min<size_t>(
      8, QueryParallelWorkerLimit(std::thread::hardware_concurrency()));
  std::vector<TypedWorkerGroups> groups(count);
  bool borrow_keys = false;
  if (total_rows > 0) {
    if (is_columnar_input) {
      borrow_keys = true;
      for (const ColumnarChunk& ch : input.columnar->chunks) {
        for (const slot_t slot : plans[0].key_offsets) {
          const ColumnarColumn& c = ch.columns[slot];
          if (c.is_uniform) {
            if (c.type != ValueType::kNull && c.type != ValueType::kInt64 &&
                c.type != ValueType::kDate && c.type != ValueType::kVarChar) {
              borrow_keys = false;
              break;
            }
          } else {
            Value value;
            for (size_t r = 0; r < ch.rows; ++r) {
              c.WriteValueInto(r, &value);
              if (value.IsCaseInsensitive() ||
                  (value.type != ValueType::kNull &&
                   value.type != ValueType::kInt64 &&
                   value.type != ValueType::kDate &&
                   value.type != ValueType::kVarChar)) {
                borrow_keys = false;
                break;
              }
            }
            if (!borrow_keys) {
              break;
            }
          }
        }
        if (!borrow_keys) {
          break;
        }
      }
    } else {
      const size_t check_workers =
          total_rows >= 16384 ? std::min(agg_workers, total_rows) : size_t{1};
      std::vector<uint8_t> chunk_ok(check_workers, 1);
      RETURN_IF_FAIL(RunParallelTasks(check_workers, [&](size_t w) {
        const size_t begin = (total_rows * w) / check_workers;
        const size_t end = (total_rows * (w + 1)) / check_workers;
        for (size_t r = begin; r < end; ++r) {
          const Row& row = input.rows[r];
          for (const slot_t slot : plans[0].key_offsets) {
            const auto& value = row[slot];
            if (value.IsCaseInsensitive() ||
                (value.type != ValueType::kNull &&
                 value.type != ValueType::kInt64 &&
                 value.type != ValueType::kDate &&
                 value.type != ValueType::kVarChar)) {
              chunk_ok[w] = 0;
              return;
            }
          }
        }
      }));
      borrow_keys =
          std::ranges::all_of(chunk_ok, [](uint8_t ok) { return ok != 0; });
    }
  }
  for (size_t s = 0; s < count; ++s) {
    const auto& plan = plans[s];
    groups[s].Configure(true, plan.key_offsets.size(), plan.aggregates.size());
    if (borrow_keys) {
      groups[s].borrowed_key_offsets = &plan.key_offsets;
    }
    groups[s].int_key = plan.key_offsets.size() == 1 &&
                        (input.schema.GetColumn(plan.key_offsets[0]).Type() ==
                             ValueType::kInt64 ||
                         input.schema.GetColumn(plan.key_offsets[0]).Type() ==
                             ValueType::kDate);
    if (plan.slim) {
      groups[s].ConfigureSlim(plan);
      groups[s].mixed_numeric_sums = mixed_numeric_sums;
    } else {
      groups[s].shapes = &shapes[s];
    }
  }
  // Each grouping set receives rows in their original order. Build the
  // finest IDs once, then let independent workers fold coarser sets. This
  // avoids sharing accumulator state or reassociating integer/floating sums.
  const size_t set_workers = std::min<size_t>(
      count - 1, QueryParallelWorkerLimit(std::thread::hardware_concurrency()));
  const auto shareable_argument = [](const TypedAggregateInput& argument) {
    if (argument.conditional || argument.int_expr) {
      return false;
    }
    if (!argument.child || argument.coalesce_double) {
      return true;
    }
    const auto& program = *argument.child;
    return std::ranges::all_of(
        program.Instructions(), [&](const auto& instruction) {
          switch (instruction.opcode) {
            case BytecodeOp::kLoadColumn:
            case BytecodeOp::kJumpIfNotNull:
            case BytecodeOp::kPop:
              return true;
            case BytecodeOp::kLoadConstant: {
              const auto& value = program.Constants()[instruction.operand];
              return value.IsNull() || (!value.IsUnsigned() &&
                                        (value.type == ValueType::kInt64 ||
                                         value.type == ValueType::kDouble));
            }
            case BytecodeOp::kBinaryDouble:
              return instruction.binary == BinaryOperation::kAdd ||
                     instruction.binary == BinaryOperation::kSubtract ||
                     instruction.binary == BinaryOperation::kMultiply;
            default:
              return false;
          }
        });
  };
  bool parallel_sets =
      total_rows >= 4096 && (agg_workers > 1 || set_workers > 1) &&
      plans[0].slim &&
      std::ranges::all_of(plans[0].aggregates, shareable_argument);
  const size_t argument_count = plans[0].aggregates.size();
  const bool all_direct_args =
      parallel_sets &&
      std::ranges::all_of(plans[0].aggregates, [](const auto& item) {
        return item.column.has_value();
      });
  bool flat_double_eligible =
      parallel_sets && !all_direct_args && argument_count >= 1;
  if (flat_double_eligible) {
    for (size_t a = 0; a < argument_count; ++a) {
      const auto& item = plans[0].aggregates[a];
      const SlimAggKind sk = plans[0].slim_kinds[a];
      if (sk == SlimAggKind::kCount) {
        if (item.conditional || item.int_expr || item.child) {
          flat_double_eligible = false;
          break;
        }
        continue;
      }
      if (sk != SlimAggKind::kSumDouble && sk != SlimAggKind::kAvgDouble) {
        flat_double_eligible = false;
        break;
      }
      if (item.coalesce_double) {
        const Value& fb = item.coalesce_double->fallback;
        if (fb.IsNull() || fb.type == ValueType::kDouble) {
          continue;
        }
        if (fb.type == ValueType::kInt64 && !fb.IsUnsigned() &&
            (sk == SlimAggKind::kAvgDouble ||
             (fb.value.int_value == 0 && !mixed_numeric_sums.empty() &&
              mixed_numeric_sums[a]))) {
          continue;
        }
        flat_double_eligible = false;
        break;
      }
      if (item.double_expr) {
        continue;
      }
      if (item.column) {
        const auto col_type = input.schema.GetColumn(*item.column).Type();
        if (col_type == ValueType::kDouble) {
          continue;
        }
      }
      flat_double_eligible = false;
      break;
    }
  }
  std::shared_ptr<QueryMemoryCharge> argument_charge;
  if (parallel_sets) {
    const size_t arg_bytes =
        all_direct_args
            ? 0
            : (flat_double_eligible
                   ? (argument_count * (sizeof(double) + sizeof(uint8_t)))
                   : (argument_count * sizeof(Value)));
    const size_t bytes_per_row = sizeof(size_t) + arg_bytes;
    if (total_rows <= SIZE_MAX / bytes_per_row) {
      argument_charge = TryReserveQueryMemory(total_rows * bytes_per_row);
    }
    parallel_sets = argument_charge != nullptr;
  }
  const size_t arg_workers =
      std::max<size_t>(1, std::min(agg_workers, total_rows));
  const size_t arg_chunk =
      total_rows == 0 ? 1 : ((total_rows + arg_workers - 1) / arg_workers);
  std::vector<std::vector<Value>> worker_arguments;
  std::vector<double> flat_double_args;
  std::vector<uint8_t> flat_arg_kinds;
  const auto direct_arg_at = [&](size_t r, size_t a,
                                 size_t* hint = nullptr) -> Value {
    const auto& item = plans[0].aggregates[a];
    Value cell;
    if (input.HasColumnarOnly()) {
      size_t local_hint = hint != nullptr ? *hint : 0;
      const auto [ci, lr] = TypedWorkerGroups::LocateColumnarChunkRow(
          chunk_prefix, static_cast<uint32_t>(r), &local_hint);
      if (hint != nullptr) {
        *hint = local_hint;
      }
      input.columnar->chunks[ci].columns[*item.column].WriteValueInto(lr,
                                                                      &cell);
    } else {
      cell = input.rows[r][*item.column];
    }
    if (plans[0].slim_kinds[a] == SlimAggKind::kCount) {
      return cell.IsNull() ? Value() : Value(1);
    }
    return cell;
  };
  bool exact_partial_merge = false;
  bool finite_sum = false;
  bool all_double = false;
  bool all_non_negative = false;
  bool no_nulls = false;
  if (parallel_sets) {
    if (flat_double_eligible) {
      flat_double_args.resize(total_rows * argument_count);
      flat_arg_kinds.resize(total_rows * argument_count);
    } else if (!all_direct_args) {
      worker_arguments.resize(arg_workers);
    }
    std::vector<Status> arg_errors(arg_workers, Status::kSuccess);
    std::vector<size_t> arg_error_rows(arg_workers, SIZE_MAX);
    std::vector<size_t> arg_non_parallel_rows(arg_workers, SIZE_MAX);
    std::vector<uint8_t> worker_exact_ok(
        arg_workers, total_rows <= static_cast<size_t>(INT64_MAX) ? 1 : 0);
    std::vector<std::vector<uint64_t>> worker_mag_sums(
        arg_workers, std::vector<uint64_t>(argument_count, 0));
    const bool could_be_finite_sum =
        argument_count == 1 &&
        plans[0].slim_kinds[0] == SlimAggKind::kSumDouble;
    std::vector<uint8_t> worker_finite_sum(arg_workers,
                                           could_be_finite_sum ? 1 : 0);
    std::vector<uint8_t> worker_all_double(arg_workers,
                                           could_be_finite_sum ? 1 : 0);
    std::vector<uint8_t> worker_non_negative(arg_workers,
                                             could_be_finite_sum ? 1 : 0);
    std::vector<uint8_t> worker_no_nulls(arg_workers,
                                         could_be_finite_sum ? 1 : 0);
    RETURN_IF_FAIL(RunParallelTasks(arg_workers, [&](size_t w) {
      const size_t begin = std::min(total_rows, w * arg_chunk);
      const size_t end = std::min(total_rows, begin + arg_chunk);
      if (begin >= end) {
        return;
      }
      const auto observe_arg = [&](size_t a, const Value& value) {
        if (worker_exact_ok[w] != 0) {
          const auto kind = plans[0].slim_kinds[a];
          if (kind == SlimAggKind::kSumInt64 ||
              kind == SlimAggKind::kAvgDouble) {
            if (!value.IsNull()) {
              if (value.type != ValueType::kInt64 || value.IsUnsigned()) {
                worker_exact_ok[w] = 0;
              } else {
                const uint64_t limit = kind == SlimAggKind::kSumInt64
                                           ? static_cast<uint64_t>(INT64_MAX)
                                           : uint64_t{1} << 53;
                const auto raw = static_cast<uint64_t>(value.value.int_value);
                const uint64_t mag = value.value.int_value < 0 ? 0 - raw : raw;
                if (mag > limit - worker_mag_sums[w][a]) {
                  worker_exact_ok[w] = 0;
                } else {
                  worker_mag_sums[w][a] += mag;
                }
              }
            }
          } else if (kind != SlimAggKind::kCount) {
            worker_exact_ok[w] = 0;
          }
        }
        if (worker_finite_sum[w] != 0) {
          if (value.IsNull()) {
            worker_no_nulls[w] = 0;
            worker_all_double[w] = 0;
          } else if (value.type == ValueType::kDouble) {
            if (!std::isfinite(value.value.double_value)) {
              worker_finite_sum[w] = 0;
              worker_all_double[w] = 0;
              worker_non_negative[w] = 0;
            } else if (!(value.value.double_value >= 0.0)) {
              worker_non_negative[w] = 0;
            }
          } else if (!mixed_numeric_sums.empty() && mixed_numeric_sums[0] &&
                     value.type == ValueType::kInt64 && !value.IsUnsigned() &&
                     value.value.int_value == 0) {
            worker_all_double[w] = 0;
          } else {
            worker_finite_sum[w] = 0;
            worker_all_double[w] = 0;
            worker_non_negative[w] = 0;
          }
        }
      };

      if (flat_double_eligible) {
        for (size_t a = 0; a < argument_count; ++a) {
          if (plans[0].slim_kinds[a] != SlimAggKind::kCount) {
            worker_exact_ok[w] = 0;
            break;
          }
        }
        double* const dst_vals = flat_double_args.data();
        uint8_t* const dst_kinds = flat_arg_kinds.data();
        struct FlatArgMeta {
          bool is_count{false};
          bool is_coalesce{false};
          const TypedDoubleExprInput* expr{nullptr};
          double fb_val{0.0};
          uint8_t fb_kind{0};
          bool fb_finite{true};
          bool fb_all_dbl{true};
          bool fb_non_neg{true};
          bool fb_no_null{true};
        };
        std::vector<FlatArgMeta> metas(argument_count);
        for (size_t a = 0; a < argument_count; ++a) {
          const auto& item = plans[0].aggregates[a];
          FlatArgMeta& m = metas[a];
          m.is_count = plans[0].slim_kinds[a] == SlimAggKind::kCount;
          m.is_coalesce = item.coalesce_double.has_value();
          if (item.coalesce_double) {
            m.expr = &item.coalesce_double->expr;
            const Value& fb = item.coalesce_double->fallback;
            if (fb.IsNull()) {
              m.fb_kind = 0;
              m.fb_all_dbl = false;
              m.fb_no_null = false;
            } else if (fb.type == ValueType::kDouble) {
              m.fb_kind = 1;
              m.fb_val = fb.value.double_value;
              if (!std::isfinite(m.fb_val)) {
                m.fb_finite = false;
                m.fb_all_dbl = false;
                m.fb_non_neg = false;
              } else if (!(m.fb_val >= 0.0)) {
                m.fb_non_neg = false;
              }
            } else {
              if (plans[0].slim_kinds[a] == SlimAggKind::kAvgDouble) {
                m.fb_kind = 1;
                m.fb_val = static_cast<double>(fb.value.int_value);
              } else {
                m.fb_kind = 2;
                m.fb_val = 0.0;
                m.fb_all_dbl = false;
              }
            }
          } else if (item.double_expr) {
            m.expr = &*item.double_expr;
          }
        }
        if (is_columnar_input) {
          size_t ci_hint = 0;
          auto [ci, lr_start] = TypedWorkerGroups::LocateColumnarChunkRow(
              chunk_prefix, static_cast<uint32_t>(begin), &ci_hint);
          size_t rem = end - begin;
          size_t cur_r = begin;
          constexpr size_t kFlatTile = 1024;
          std::array<double, kFlatTile> tile_vals{};
          std::array<uint8_t, kFlatTile> tile_valid{};
          Row scratch_row;
          while (rem > 0) {
            const ColumnarChunk& ch = input.columnar->chunks[ci];
            const size_t chunk_take = std::min(rem, ch.rows - lr_start);
            for (size_t tile = 0; tile < chunk_take; tile += kFlatTile) {
              const size_t tile_take = std::min(kFlatTile, chunk_take - tile);
              const size_t r_base = cur_r + tile;
              const size_t tile_lr = lr_start + tile;
              for (size_t a = 0; a < argument_count; ++a) {
                const auto& item = plans[0].aggregates[a];
                const FlatArgMeta& m = metas[a];
                if (m.is_count) {
                  if (!item.column) {
                    for (size_t i = 0; i < tile_take; ++i) {
                      const size_t idx = ((r_base + i) * argument_count) + a;
                      dst_vals[idx] = 0.0;
                      dst_kinds[idx] = 3;
                    }
                  } else {
                    const ColumnarColumn& c = ch.columns[*item.column];
                    if (c.is_uniform && c.type == ValueType::kNull) {
                      for (size_t i = 0; i < tile_take; ++i) {
                        const size_t idx = ((r_base + i) * argument_count) + a;
                        dst_vals[idx] = 0.0;
                        dst_kinds[idx] = 0;
                      }
                    } else if (c.is_uniform && !c.has_null) {
                      for (size_t i = 0; i < tile_take; ++i) {
                        const size_t idx = ((r_base + i) * argument_count) + a;
                        dst_vals[idx] = 0.0;
                        dst_kinds[idx] = 3;
                      }
                    } else if (c.IsFlat() && c.is_uniform) {
                      const uint8_t* nulls = c.nulls.data() + tile_lr;
                      for (size_t i = 0; i < tile_take; ++i) {
                        const size_t idx = ((r_base + i) * argument_count) + a;
                        dst_vals[idx] = 0.0;
                        dst_kinds[idx] =
                            nulls[i] != 0 ? uint8_t{0} : uint8_t{3};
                      }
                    } else {
                      for (size_t i = 0; i < tile_take; ++i) {
                        const size_t idx = ((r_base + i) * argument_count) + a;
                        dst_vals[idx] = 0.0;
                        dst_kinds[idx] =
                            c.IsNull(tile_lr + i) ? uint8_t{0} : uint8_t{3};
                      }
                    }
                  }
                  continue;
                }
                if (m.expr != nullptr) {
                  if (!EvalDoubleExprChunkSlice(
                          *m.expr, ch, tile_lr, tile_take, tile_vals.data(),
                          tile_valid.data(), &scratch_row)) {
                    arg_non_parallel_rows[w] = r_base;
                    return;
                  }
                  if (argument_count == 1) {
                    const bool chk = worker_finite_sum[w] != 0;
                    uint8_t fs = worker_finite_sum[w];
                    uint8_t ad = worker_all_double[w];
                    uint8_t nn = worker_non_negative[w];
                    uint8_t non = worker_no_nulls[w];
                    double* out_v = dst_vals + r_base;
                    uint8_t* out_k = dst_kinds + r_base;
                    for (size_t i = 0; i < tile_take; ++i) {
                      if (tile_valid[i] != 0) {
                        const double dv = tile_vals[i];
                        if (chk) {
                          if (!std::isfinite(dv)) {
                            fs = 0;
                            ad = 0;
                            nn = 0;
                          } else if (!(dv >= 0.0)) {
                            nn = 0;
                          }
                        }
                        out_v[i] = dv;
                        out_k[i] = 1;
                      } else if (m.is_coalesce) {
                        out_v[i] = m.fb_val;
                        out_k[i] = m.fb_kind;
                        if (chk) {
                          if (!m.fb_finite) {
                            fs = 0;
                          }
                          if (!m.fb_all_dbl) {
                            ad = 0;
                          }
                          if (!m.fb_non_neg) {
                            nn = 0;
                          }
                          if (!m.fb_no_null) {
                            non = 0;
                          }
                        }
                      } else {
                        out_v[i] = 0.0;
                        out_k[i] = 0;
                        if (chk) {
                          non = 0;
                          ad = 0;
                        }
                      }
                    }
                    worker_finite_sum[w] = fs;
                    worker_all_double[w] = ad;
                    worker_non_negative[w] = nn;
                    worker_no_nulls[w] = non;
                  } else {
                    for (size_t i = 0; i < tile_take; ++i) {
                      const size_t idx = ((r_base + i) * argument_count) + a;
                      if (tile_valid[i] != 0) {
                        dst_vals[idx] = tile_vals[i];
                        dst_kinds[idx] = 1;
                      } else if (m.is_coalesce) {
                        dst_vals[idx] = m.fb_val;
                        dst_kinds[idx] = m.fb_kind;
                      } else {
                        dst_vals[idx] = 0.0;
                        dst_kinds[idx] = 0;
                      }
                    }
                  }
                  continue;
                }
                const ColumnarColumn& c = ch.columns[*item.column];
                if (c.is_uniform && c.type == ValueType::kNull) {
                  for (size_t i = 0; i < tile_take; ++i) {
                    const size_t idx = ((r_base + i) * argument_count) + a;
                    dst_vals[idx] = 0.0;
                    dst_kinds[idx] = 0;
                  }
                } else if (c.IsFlat() && c.is_uniform &&
                           c.type == ValueType::kDouble) {
                  const double* src_d = c.doubles.data() + tile_lr;
                  const uint8_t* nulls =
                      c.has_null ? c.nulls.data() + tile_lr : nullptr;
                  for (size_t i = 0; i < tile_take; ++i) {
                    const size_t idx = ((r_base + i) * argument_count) + a;
                    if (nulls != nullptr && nulls[i] != 0) {
                      dst_vals[idx] = 0.0;
                      dst_kinds[idx] = 0;
                    } else {
                      dst_vals[idx] = src_d[i];
                      dst_kinds[idx] = 1;
                    }
                  }
                } else if (c.is_uniform && c.type == ValueType::kDouble) {
                  for (size_t i = 0; i < tile_take; ++i) {
                    const size_t idx = ((r_base + i) * argument_count) + a;
                    if (c.has_null && c.IsNull(tile_lr + i)) {
                      dst_vals[idx] = 0.0;
                      dst_kinds[idx] = 0;
                    } else {
                      dst_vals[idx] = c.DoubleAt(tile_lr + i);
                      dst_kinds[idx] = 1;
                    }
                  }
                } else {
                  Value cell_tmp;
                  for (size_t i = 0; i < tile_take; ++i) {
                    c.WriteValueInto(tile_lr + i, &cell_tmp);
                    const size_t idx = ((r_base + i) * argument_count) + a;
                    if (cell_tmp.IsNull()) {
                      dst_vals[idx] = 0.0;
                      dst_kinds[idx] = 0;
                    } else if (cell_tmp.type == ValueType::kDouble) {
                      dst_vals[idx] = cell_tmp.value.double_value;
                      dst_kinds[idx] = 1;
                    } else {
                      arg_non_parallel_rows[w] = r_base + i;
                      return;
                    }
                  }
                }
              }
            }
            rem -= chunk_take;
            cur_r += chunk_take;
            ++ci;
            lr_start = 0;
          }
        } else {
          for (size_t r = begin; r < end; ++r) {
            const Row& row = input.rows[r];
            for (size_t a = 0; a < argument_count; ++a) {
              const auto& item = plans[0].aggregates[a];
              const FlatArgMeta& m = metas[a];
              const size_t idx = (r * argument_count) + a;
              if (m.is_count) {
                const bool non_null =
                    !item.column || !row[*item.column].IsNull();
                dst_vals[idx] = 0.0;
                dst_kinds[idx] = non_null ? uint8_t{3} : uint8_t{0};
                continue;
              }
              Value value;
              if (item.column) {
                value = row[*item.column];
              } else if (item.coalesce_double) {
                value = EvaluateTypedCoalesceDoubleOnRow(*item.coalesce_double,
                                                         row);
              } else {
                value = EvaluateTypedDoubleExprOnRow(*item.double_expr, row);
              }
              if (value.IsNull()) {
                dst_vals[idx] = 0.0;
                dst_kinds[idx] = 0;
                if (worker_finite_sum[w] != 0) {
                  worker_no_nulls[w] = 0;
                  worker_all_double[w] = 0;
                }
              } else if (value.type == ValueType::kDouble) {
                const double dv = value.value.double_value;
                dst_vals[idx] = dv;
                dst_kinds[idx] = 1;
                if (worker_finite_sum[w] != 0) {
                  if (!std::isfinite(dv)) {
                    worker_finite_sum[w] = 0;
                    worker_all_double[w] = 0;
                    worker_non_negative[w] = 0;
                  } else if (!(dv >= 0.0)) {
                    worker_non_negative[w] = 0;
                  }
                }
              } else if (!value.IsUnsigned() &&
                         value.type == ValueType::kInt64 &&
                         (plans[0].slim_kinds[a] == SlimAggKind::kAvgDouble ||
                          (value.value.int_value == 0 &&
                           !mixed_numeric_sums.empty() &&
                           mixed_numeric_sums[a]))) {
                if (plans[0].slim_kinds[a] == SlimAggKind::kAvgDouble) {
                  dst_vals[idx] = static_cast<double>(value.value.int_value);
                  dst_kinds[idx] = 1;
                } else {
                  dst_vals[idx] = 0.0;
                  dst_kinds[idx] = 2;
                  worker_all_double[w] = 0;
                }
              } else {
                arg_non_parallel_rows[w] = r;
                return;
              }
            }
          }
        }
      } else if (all_direct_args) {
        if (is_columnar_input) {
          size_t ci_hint = 0;
          auto [ci, lr_start] = TypedWorkerGroups::LocateColumnarChunkRow(
              chunk_prefix, static_cast<uint32_t>(begin), &ci_hint);
          size_t rem = end - begin;
          size_t cur_r = begin;
          Value cell_tmp;
          while (rem > 0) {
            const ColumnarChunk& ch = input.columnar->chunks[ci];
            const size_t chunk_take = std::min(rem, ch.rows - lr_start);
            for (size_t a = 0; a < argument_count; ++a) {
              const auto& item = plans[0].aggregates[a];
              const ColumnarColumn& c = ch.columns[*item.column];
              const bool is_count =
                  plans[0].slim_kinds[a] == SlimAggKind::kCount;
              if (c.is_uniform && is_count) {
                continue;
              }
              if (c.is_uniform && c.type == ValueType::kNull) {
                if (worker_finite_sum[w] != 0) {
                  worker_no_nulls[w] = 0;
                  worker_all_double[w] = 0;
                }
                continue;
              }
              if (c.is_uniform && !is_count &&
                  (c.is_unsigned || (c.type != ValueType::kInt64 &&
                                     c.type != ValueType::kDouble))) {
                arg_non_parallel_rows[w] = cur_r;
                return;
              }
              if (c.is_uniform && !is_count && c.type == ValueType::kDouble &&
                  !c.has_null && worker_exact_ok[w] == 0) {
                if (worker_finite_sum[w] != 0) {
                  if (c.IsFlat()) {
                    const double* src_d = c.doubles.data() + lr_start;
                    for (size_t i = 0; i < chunk_take; ++i) {
                      if (!std::isfinite(src_d[i])) {
                        worker_finite_sum[w] = 0;
                        worker_all_double[w] = 0;
                        worker_non_negative[w] = 0;
                        break;
                      }
                      if (!(src_d[i] >= 0.0)) {
                        worker_non_negative[w] = 0;
                      }
                    }
                  } else {
                    for (size_t i = 0; i < chunk_take; ++i) {
                      const double dv = c.DoubleAt(lr_start + i);
                      if (!std::isfinite(dv)) {
                        worker_finite_sum[w] = 0;
                        worker_all_double[w] = 0;
                        worker_non_negative[w] = 0;
                        break;
                      }
                      if (!(dv >= 0.0)) {
                        worker_non_negative[w] = 0;
                      }
                    }
                  }
                }
                continue;
              }
              for (size_t i = 0; i < chunk_take; ++i) {
                c.WriteValueInto(lr_start + i, &cell_tmp);
                if (is_count) {
                  observe_arg(a, cell_tmp.IsNull() ? Value() : Value(1));
                } else {
                  if (!cell_tmp.IsNull() &&
                      (cell_tmp.IsUnsigned() ||
                       (cell_tmp.type != ValueType::kInt64 &&
                        cell_tmp.type != ValueType::kDouble))) {
                    arg_non_parallel_rows[w] = cur_r + i;
                    return;
                  }
                  observe_arg(a, cell_tmp);
                }
              }
            }
            rem -= chunk_take;
            cur_r += chunk_take;
            ++ci;
            lr_start = 0;
          }
        } else {
          for (size_t r = begin; r < end; ++r) {
            const Row& row = input.rows[r];
            for (size_t a = 0; a < argument_count; ++a) {
              const auto& item = plans[0].aggregates[a];
              const Value& cell = row[*item.column];
              if (plans[0].slim_kinds[a] == SlimAggKind::kCount) {
                observe_arg(a, cell.IsNull() ? Value() : Value(1));
              } else {
                if (!cell.IsNull() &&
                    (cell.IsUnsigned() || (cell.type != ValueType::kInt64 &&
                                           cell.type != ValueType::kDouble))) {
                  arg_non_parallel_rows[w] = r;
                  return;
                }
                observe_arg(a, cell);
              }
            }
          }
        }
      } else {
        worker_arguments[w].reserve((end - begin) * argument_count);
        if (is_columnar_input) {
          size_t ci_hint = 0;
          auto [ci, lr_start] = TypedWorkerGroups::LocateColumnarChunkRow(
              chunk_prefix, static_cast<uint32_t>(begin), &ci_hint);
          size_t rem = end - begin;
          size_t cur_r = begin;
          Row scratch_row;
          while (rem > 0) {
            const ColumnarChunk& ch = input.columnar->chunks[ci];
            const size_t chunk_take = std::min(rem, ch.rows - lr_start);
            for (size_t i = 0; i < chunk_take; ++i) {
              ch.MaterializeRowInto(lr_start + i, &scratch_row);
              for (size_t a = 0; a < argument_count; ++a) {
                const auto& item = plans[0].aggregates[a];
                Value value;
                if (item.column) {
                  const Value& cell = scratch_row[*item.column];
                  if (plans[0].slim_kinds[a] == SlimAggKind::kCount) {
                    value = cell.IsNull() ? Value() : Value(1);
                  } else {
                    if (!cell.IsNull() && (cell.IsUnsigned() ||
                                           (cell.type != ValueType::kInt64 &&
                                            cell.type != ValueType::kDouble))) {
                      arg_non_parallel_rows[w] = cur_r + i;
                      return;
                    }
                    value = cell;
                  }
                } else {
                  auto computed = CompactArgument(item, scratch_row);
                  if (!computed.HasValue()) {
                    arg_errors[w] = computed.GetStatus();
                    arg_error_rows[w] = cur_r + i;
                    return;
                  }
                  value = computed.MoveValue();
                  if (!value.IsNull() && (value.IsUnsigned() ||
                                          (value.type != ValueType::kInt64 &&
                                           value.type != ValueType::kDouble))) {
                    arg_non_parallel_rows[w] = cur_r + i;
                    return;
                  }
                }
                observe_arg(a, value);
                worker_arguments[w].push_back(std::move(value));
              }
            }
            rem -= chunk_take;
            cur_r += chunk_take;
            ++ci;
            lr_start = 0;
          }
        } else {
          for (size_t r = begin; r < end; ++r) {
            const Row& row = input.rows[r];
            for (size_t a = 0; a < argument_count; ++a) {
              const auto& item = plans[0].aggregates[a];
              Value value;
              if (item.column) {
                const Value& cell = row[*item.column];
                if (plans[0].slim_kinds[a] == SlimAggKind::kCount) {
                  value = cell.IsNull() ? Value() : Value(1);
                } else {
                  if (!cell.IsNull() && (cell.IsUnsigned() ||
                                         (cell.type != ValueType::kInt64 &&
                                          cell.type != ValueType::kDouble))) {
                    arg_non_parallel_rows[w] = r;
                    return;
                  }
                  value = cell;
                }
              } else {
                auto computed = CompactArgument(item, row);
                if (!computed.HasValue()) {
                  arg_errors[w] = computed.GetStatus();
                  arg_error_rows[w] = r;
                  return;
                }
                value = computed.MoveValue();
                if (!value.IsNull() && (value.IsUnsigned() ||
                                        (value.type != ValueType::kInt64 &&
                                         value.type != ValueType::kDouble))) {
                  arg_non_parallel_rows[w] = r;
                  return;
                }
              }
              observe_arg(a, value);
              worker_arguments[w].push_back(std::move(value));
            }
          }
        }
      }
    }));
    const auto first_err = std::ranges::min_element(arg_error_rows);
    const size_t first_non_parallel =
        *std::ranges::min_element(arg_non_parallel_rows);
    if (*first_err != SIZE_MAX && *first_err < first_non_parallel) {
      return arg_errors[static_cast<size_t>(first_err -
                                            arg_error_rows.begin())];
    }
    if (first_non_parallel != SIZE_MAX) {
      parallel_sets = false;
    } else {
      exact_partial_merge = std::ranges::all_of(
          worker_exact_ok, [](uint8_t ok) { return ok != 0; });
      for (size_t a = 0; exact_partial_merge && a < argument_count; ++a) {
        const auto kind = plans[0].slim_kinds[a];
        if (kind == SlimAggKind::kCount) {
          continue;
        }
        const uint64_t limit = kind == SlimAggKind::kSumInt64
                                   ? static_cast<uint64_t>(INT64_MAX)
                                   : uint64_t{1} << 53;
        uint64_t total_mag = 0;
        for (size_t w = 0; w < arg_workers; ++w) {
          if (worker_mag_sums[w][a] > limit - total_mag) {
            exact_partial_merge = false;
            break;
          }
          total_mag += worker_mag_sums[w][a];
        }
      }
      finite_sum = could_be_finite_sum &&
                   std::ranges::all_of(worker_finite_sum,
                                       [](uint8_t ok) { return ok != 0; });
      all_double =
          finite_sum && std::ranges::all_of(worker_all_double,
                                            [](uint8_t ok) { return ok != 0; });
      all_non_negative =
          finite_sum && std::ranges::all_of(worker_non_negative,
                                            [](uint8_t ok) { return ok != 0; });
      no_nulls =
          finite_sum && std::ranges::all_of(worker_no_nulls,
                                            [](uint8_t ok) { return ok != 0; });
    }
  }
  phase("arguments");
  if (parallel_sets) {
    const bool can_fold_partials =
        exact_partial_merge || (finite_sum && all_non_negative);
    const auto add_finite_sum = [no_nulls](TypedWorkerGroups& local,
                                           size_t group, const Value& value) {
      if (!no_nulls && !value.IsNull()) {
        local.SlimCount(group, 0) = 1;
      }
      if (value.type == ValueType::kDouble) {
        local.SlimDoubleTotal(group, 0) += value.value.double_value;
        local.SlimFlags(group, 0) |= 2;
      }
    };
    std::vector<size_t> row_groups;
    if (!can_fold_partials) {
      row_groups.resize(total_rows);
    }
    std::vector<std::vector<size_t>> coarse_maps(count);
    const bool has_distinct_aggs =
        std::ranges::any_of(plans[0].slim_kinds, [](SlimAggKind kind) {
          return kind == SlimAggKind::kCountDistinctInt64 ||
                 kind == SlimAggKind::kCountDistinctDouble;
        });
    bool packed_done = false;
    if (borrow_keys && !has_distinct_aggs && !plans[0].key_offsets.empty() &&
        (plans[0].key_offsets.size() != 1 || !groups[0].int_key) &&
        total_rows <= UINT32_MAX &&
        std::getenv("TINYLAMB_DISABLE_PACKED_GROUPING_DICT") == nullptr) {
      auto packed_or = TryBuildPackedGroupingSets(
          input, chunk_prefix, plans, groups[0].mixed_numeric_sums, agg_workers,
          arg_chunk, argument_count, all_direct_args, worker_arguments,
          flat_double_args, flat_arg_kinds, exact_partial_merge, finite_sum,
          all_double, no_nulls, finest_keys_unique, groups, row_groups,
          coarse_maps);
      RETURN_IF_FAIL(packed_or.GetStatus());
      packed_done = packed_or.MoveValue();
    }
    const size_t finest_workers = std::min<size_t>(8, agg_workers);
    if (!packed_done) {
      if (input.HasColumnarOnly()) {
        RETURN_IF_FAIL(input.EnsureRowsMaterialized());
      }
      const bool use_flat = !flat_double_args.empty();
      const auto get_arg_val = [&](size_t r, size_t a) -> Value {
        if (all_direct_args && plans[0].aggregates[0].column.has_value()) {
          return direct_arg_at(r, a);
        }
        if (use_flat) {
          const size_t idx = (r * argument_count) + a;
          return FlatArgToValue(flat_double_args[idx], flat_arg_kinds[idx]);
        }
        return worker_arguments[r / arg_chunk]
                               [((r % arg_chunk) * argument_count) + a];
      };
      std::shared_ptr<QueryMemoryCharge> partition_charge;
      if (borrow_keys && plans[0].key_offsets.size() > 1 &&
          finest_workers > 1) {
        const size_t bytes_per_row = 256 + (argument_count * 64);
        const size_t padding = finest_workers * 4096 * argument_count * 64;
        if (input.rows.size() <= (SIZE_MAX - padding) / bytes_per_row) {
          partition_charge = TryReserveQueryMemory(
              (input.rows.size() * bytes_per_row) + padding);
        }
      }
      if (partition_charge) {
        // Assign every key to exactly one worker. Each worker visits its rows
        // in input order, so SUM retains the original per-group operation
        // sequence. Global IDs are rebuilt in first-encounter order afterward.
        std::vector<size_t> hashes(input.rows.size());
        std::vector<std::vector<std::vector<size_t>>> chunk_buckets(
            finest_workers, std::vector<std::vector<size_t>>(finest_workers));
        RETURN_IF_FAIL(RunParallelTasks(finest_workers, [&](size_t w) {
          const size_t chunk = input.rows.size() / finest_workers;
          const size_t begin = w * chunk;
          const size_t end =
              w + 1 == finest_workers ? input.rows.size() : begin + chunk;
          for (auto& bucket : chunk_buckets[w]) {
            bucket.reserve(((end - begin) / finest_workers) + 16);
          }
          for (size_t r = begin; r < end; ++r) {
            const size_t h = TypedWorkerGroups::HashRowKey(
                input.rows[r], plans[0].key_offsets);
            hashes[r] = h;
            chunk_buckets[w][h % finest_workers].push_back(r);
          }
        }));
        size_t max_dest_rows = 0;
        for (size_t dest = 0; dest < finest_workers; ++dest) {
          size_t dest_rows = 0;
          for (size_t src = 0; src < finest_workers; ++src) {
            dest_rows += chunk_buckets[src][dest].size();
          }
          max_dest_rows = std::max(max_dest_rows, dest_rows);
        }
        const bool skewed_finest = max_dest_rows * 4 >= input.rows.size() * 3 &&
                                   input.rows.size() >= finest_workers * 1024;
        std::vector<TypedWorkerGroups> partials(finest_workers);
        for (auto& partial : partials) {
          partial.Configure(true, plans[0].key_offsets.size(), argument_count);
          partial.ConfigureSlim(plans[0]);
          partial.int_key = groups[0].int_key;
          partial.borrowed_key_offsets = &plans[0].key_offsets;
          partial.mixed_numeric_sums = mixed_numeric_sums;
        }
        std::vector<Status> errors(finest_workers, Status::kSuccess);
        std::vector<size_t> error_rows(finest_workers, SIZE_MAX);
        std::vector<size_t> local_groups(input.rows.size());
        std::vector<std::vector<size_t>> first_rows(finest_workers);
        if (skewed_finest) {
          std::vector<std::vector<std::vector<size_t>>>().swap(chunk_buckets);
          RETURN_IF_FAIL(RunParallelTasks(finest_workers, [&](size_t w) {
            const size_t chunk = input.rows.size() / finest_workers;
            const size_t begin = w * chunk;
            const size_t end =
                w + 1 == finest_workers ? input.rows.size() : begin + chunk;
            auto& partial = partials[w];
            auto& first = first_rows[w];
            std::vector<Value> keys;
            for (size_t r = begin; r < end; ++r) {
              bool inserted = false;
              const size_t group = partial.FindOrInsertRow(
                  input.rows[r], plans[0].key_offsets, aggregates[0], &keys,
                  &inserted, hashes[r]);
              local_groups[r] = group;
              if (inserted) {
                first.push_back(r);
              }
              if (exact_partial_merge) {
                for (size_t a = 0; a < argument_count; ++a) {
                  if (use_flat) {
                    const size_t idx = (r * argument_count) + a;
                    if (!ApplyFlatArg(&partial, group, a,
                                      plans[0].slim_kinds[a],
                                      flat_double_args[idx],
                                      flat_arg_kinds[idx], &errors[w])) {
                      error_rows[w] = r;
                      return;
                    }
                  } else {
                    Value arg_val = get_arg_val(r, a);
                    if (!ApplySlimValue(&partial, group, a,
                                        plans[0].slim_kinds[a], &arg_val,
                                        &errors[w])) {
                      error_rows[w] = r;
                      return;
                    }
                  }
                }
              } else if (can_fold_partials && finite_sum) {
                add_finite_sum(partial, group, get_arg_val(r, 0));
              }
            }
          }));
          const auto first_error = std::ranges::min_element(error_rows);
          if (*first_error != SIZE_MAX) {
            return errors[static_cast<size_t>(first_error -
                                              error_rows.begin())];
          }
          std::vector<std::vector<size_t>> global_ids(finest_workers);
          std::vector<Value> fine_keys;
          auto& finest = groups[0];
          for (size_t w = 0; w < finest_workers; ++w) {
            global_ids[w].resize(partials[w].GroupCount(), SIZE_MAX);
            for (size_t local = 0; local < first_rows[w].size(); ++local) {
              const size_t r = first_rows[w][local];
              bool inserted = false;
              const size_t global = finest.FindOrInsertRow(
                  input.rows[r], plans[0].key_offsets, aggregates[0],
                  &fine_keys, &inserted, hashes[r]);
              global_ids[w][local] = global;
              if (can_fold_partials) {
                for (size_t a = 0; a < argument_count; ++a) {
                  finest.SlimCount(global, a) +=
                      partials[w].SlimCount(local, a);
                  finest.SlimIntTotal(global, a) +=
                      partials[w].SlimIntTotal(local, a);
                  finest.SlimDoubleTotal(global, a) +=
                      partials[w].SlimDoubleTotal(local, a);
                  finest.SlimFlags(global, a) =
                      std::max(finest.SlimFlags(global, a),
                               partials[w].SlimFlags(local, a));
                }
              }
            }
          }
          if (!can_fold_partials) {
            RETURN_IF_FAIL(RunParallelTasks(finest_workers, [&](size_t w) {
              const size_t chunk = input.rows.size() / finest_workers;
              const size_t begin = w * chunk;
              const size_t end =
                  w + 1 == finest_workers ? input.rows.size() : begin + chunk;
              for (size_t r = begin; r < end; ++r) {
                row_groups[r] = global_ids[w][local_groups[r]];
              }
            }));
            if (finite_sum && all_double) {
              std::ranges::fill(finest.slim_flags, uint8_t{2});
              double* __restrict totals = finest.slim_double_totals.data();
              const size_t* __restrict rgs = row_groups.data();
              if (all_direct_args &&
                  plans[0].aggregates[0].column.has_value()) {
                const slot_t col = *plans[0].aggregates[0].column;
                for (size_t r = 0; r < input.rows.size(); ++r) {
                  totals[rgs[r]] += input.rows[r][col].value.double_value;
                }
              } else if (use_flat) {
                for (size_t r = 0; r < input.rows.size(); ++r) {
                  totals[rgs[r]] += flat_double_args[r];
                }
              } else {
                size_t r = 0;
                for (const auto& w_args : worker_arguments) {
                  for (const Value& val : w_args) {
                    totals[rgs[r]] += val.value.double_value;
                    ++r;
                  }
                }
              }
            } else if (finite_sum) {
              if (all_direct_args &&
                  plans[0].aggregates[0].column.has_value()) {
                for (size_t r = 0; r < input.rows.size(); ++r) {
                  add_finite_sum(finest, row_groups[r], direct_arg_at(r, 0));
                }
              } else if (use_flat) {
                for (size_t r = 0; r < input.rows.size(); ++r) {
                  add_finite_sum(
                      finest, row_groups[r],
                      FlatArgToValue(flat_double_args[r], flat_arg_kinds[r]));
                }
              } else {
                size_t r = 0;
                for (const auto& w_args : worker_arguments) {
                  for (const Value& val : w_args) {
                    add_finite_sum(finest, row_groups[r], val);
                    ++r;
                  }
                }
              }
            } else {
              for (size_t r = 0; r < input.rows.size(); ++r) {
                for (size_t a = 0; a < argument_count; ++a) {
                  if (use_flat) {
                    const size_t idx = (r * argument_count) + a;
                    Status err{Status::kSuccess};
                    if (!ApplyFlatArg(
                            &finest, row_groups[r], a, plans[0].slim_kinds[a],
                            flat_double_args[idx], flat_arg_kinds[idx], &err)) {
                      return err;
                    }
                  } else {
                    Value arg_val = get_arg_val(r, a);
                    Status err{Status::kSuccess};
                    if (!ApplySlimValue(&finest, row_groups[r], a,
                                        plans[0].slim_kinds[a], &arg_val,
                                        &err)) {
                      return err;
                    }
                  }
                }
              }
            }
          }
        } else {
          RETURN_IF_FAIL(RunParallelTasks(finest_workers, [&](size_t w) {
            auto& partial = partials[w];
            auto& first = first_rows[w];
            size_t bucket_rows = 0;
            for (size_t src = 0; src < finest_workers; ++src) {
              bucket_rows += chunk_buckets[src][w].size();
            }
            partial.ReserveExpectedKeys(bucket_rows);
            std::vector<Value> keys;
            for (size_t src = 0; src < finest_workers; ++src) {
              for (const size_t r : chunk_buckets[src][w]) {
                bool inserted = false;
                const size_t group = partial.FindOrInsertRow(
                    input.rows[r], plans[0].key_offsets, aggregates[0], &keys,
                    &inserted, hashes[r]);
                local_groups[r] = group;
                if (inserted) {
                  first.push_back(r);
                }
                if (finite_sum) {
                  add_finite_sum(partial, group, get_arg_val(r, 0));
                } else {
                  for (size_t a = 0; a < argument_count; ++a) {
                    if (use_flat) {
                      const size_t idx = (r * argument_count) + a;
                      if (!ApplyFlatArg(&partial, group, a,
                                        plans[0].slim_kinds[a],
                                        flat_double_args[idx],
                                        flat_arg_kinds[idx], &errors[w])) {
                        error_rows[w] = r;
                        return;
                      }
                    } else {
                      Value arg_val = get_arg_val(r, a);
                      if (!ApplySlimValue(&partial, group, a,
                                          plans[0].slim_kinds[a], &arg_val,
                                          &errors[w])) {
                        error_rows[w] = r;
                        return;
                      }
                    }
                  }
                }
              }
            }
          }));
          const auto first_error = std::ranges::min_element(error_rows);
          if (*first_error != SIZE_MAX) {
            return errors[static_cast<size_t>(first_error -
                                              error_rows.begin())];
          }
          std::vector<std::vector<size_t>> global_ids(finest_workers);
          size_t total_groups = 0;
          for (size_t w = 0; w < finest_workers; ++w) {
            global_ids[w].resize(partials[w].GroupCount(), SIZE_MAX);
            total_groups += partials[w].GroupCount();
          }
          auto& finest = groups[0];
          finest.ReserveGroups(total_groups);
          finest.group_count = total_groups;
          finest.borrowed_key_rows.resize(total_groups);
          std::vector<size_t> cursors(finest_workers, 0);
          std::array<size_t, 8> active_w{};
          size_t active_n = 0;
          for (size_t w = 0; w < finest_workers; ++w) {
            if (!first_rows[w].empty()) {
              active_w[active_n++] = w;
            }
          }
          for (size_t global = 0; global < total_groups; ++global) {
            size_t best_idx = 0;
            size_t best_r = first_rows[active_w[0]][cursors[active_w[0]]];
            for (size_t ai = 1; ai < active_n; ++ai) {
              const size_t w = active_w[ai];
              const size_t r = first_rows[w][cursors[w]];
              if (r < best_r) {
                best_r = r;
                best_idx = ai;
              }
            }
            const size_t best_w = active_w[best_idx];
            const size_t local = cursors[best_w]++;
            if (cursors[best_w] == first_rows[best_w].size()) {
              active_w[best_idx] = active_w[--active_n];
            }
            global_ids[best_w][local] = global;
            finest.borrowed_key_rows[global] = &input.rows[best_r];
          }
          RETURN_IF_FAIL(RunParallelTasks(finest_workers, [&](size_t w) {
            auto& partial = partials[w];
            const auto& gids = global_ids[w];
            const size_t ng = partial.GroupCount();
            for (size_t local = 0; local < ng; ++local) {
              const size_t global = gids[local];
              for (size_t a = 0; a < argument_count; ++a) {
                finest.SlimCount(global, a) = partial.SlimCount(local, a);
                finest.SlimIntTotal(global, a) = partial.SlimIntTotal(local, a);
                finest.SlimDoubleTotal(global, a) =
                    partial.SlimDoubleTotal(local, a);
                finest.SlimFlags(global, a) = partial.SlimFlags(local, a);
              }
            }
          }));
          if (!can_fold_partials) {
            RETURN_IF_FAIL(RunParallelTasks(finest_workers, [&](size_t w) {
              const size_t begin = std::min(input.rows.size(), w * arg_chunk);
              const size_t end = std::min(input.rows.size(), begin + arg_chunk);
              for (size_t r = begin; r < end; ++r) {
                row_groups[r] =
                    global_ids[hashes[r] % finest_workers][local_groups[r]];
              }
            }));
          }
        }
      } else {
        std::vector<Value> fine_keys;
        const size_t nrows = input.rows.size();
        for (size_t r = 0; r < nrows; ++r) {
          bool inserted = false;
          const size_t group =
              groups[0].FindOrInsertRow(input.rows[r], plans[0].key_offsets,
                                        aggregates[0], &fine_keys, &inserted);
          if (!can_fold_partials) {
            row_groups[r] = group;
          }
          if (finite_sum) {
            add_finite_sum(groups[0], group, get_arg_val(r, 0));
          } else {
            for (size_t a = 0; a < argument_count; ++a) {
              if (use_flat) {
                const size_t idx = (r * argument_count) + a;
                Status error{Status::kSuccess};
                if (!ApplyFlatArg(groups.data(), group, a,
                                  plans[0].slim_kinds[a], flat_double_args[idx],
                                  flat_arg_kinds[idx], &error)) {
                  return error;
                }
              } else {
                Value arg_val = get_arg_val(r, a);
                Status error{Status::kSuccess};
                if (!ApplySlimValue(groups.data(), group, a,
                                    plans[0].slim_kinds[a], &arg_val, &error)) {
                  return error;
                }
              }
            }
          }
        }
      }
    }  // if (!packed_done)
    if (no_nulls) {
      // SUM only observes whether its count is zero. Every group was formed
      // from at least one non-NULL argument, so a presence count is sufficient.
      std::ranges::fill(groups[0].slim_counts, int64_t{1});
    }
    phase("inserted");
    // A coarser grouping set can project any already-built finer set.
    // ROLLUP therefore hashes each distinct parent tuple only once instead
    // of copying the same prefix from every finest-level group at every
    // level. The ID maps compose back to finest IDs, preserving first-seen
    // group order and the original row order of every accumulation.
    if (!packed_done) {
      for (size_t s = 1; s < count; ++s) {
        size_t parent = 0;
        for (size_t candidate = 1; candidate < s; ++candidate) {
          if (groups[candidate].GroupCount() < groups[parent].GroupCount() &&
              std::ranges::all_of(plans[s].key_offsets, [&](slot_t key) {
                return std::ranges::find(plans[candidate].key_offsets, key) !=
                       plans[candidate].key_offsets.end();
              })) {
            parent = candidate;
          }
        }
        const size_t pg_count = groups[parent].GroupCount();
        if (plans[s].key_offsets.empty() && pg_count > 0 && borrow_keys &&
            groups[s].slim) {
          groups[s].ReserveGroups(1);
          groups[s].group_count = 1;
          groups[s].borrowed_key_rows.push_back(
              groups[parent].borrowed_key_rows[0]);
          coarse_maps[s].assign(groups[0].GroupCount(), 0);
          continue;
        }
        if (borrow_keys && groups[s].slim && pg_count >= 8192 &&
            agg_workers > 1) {
          const size_t cw = std::min<size_t>(8, agg_workers);
          std::vector<size_t> c_hashes(pg_count);
          std::vector<std::vector<std::vector<size_t>>> c_buckets(
              cw, std::vector<std::vector<size_t>>(cw));
          RETURN_IF_FAIL(RunParallelTasks(cw, [&](size_t w) {
            const size_t begin = (pg_count * w) / cw;
            const size_t end = (pg_count * (w + 1)) / cw;
            for (auto& bucket : c_buckets[w]) {
              bucket.reserve(((end - begin) / cw) + 16);
            }
            for (size_t g = begin; g < end; ++g) {
              const size_t h = TypedWorkerGroups::HashRowKey(
                  *groups[parent].borrowed_key_rows[g], plans[s].key_offsets);
              c_hashes[g] = h;
              c_buckets[w][h % cw].push_back(g);
            }
          }));
          std::vector<TypedWorkerGroups> c_partials(cw);
          for (auto& cp : c_partials) {
            cp.Configure(true, plans[s].key_offsets.size(), 0);
            cp.int_key = groups[s].int_key;
            cp.borrowed_key_offsets = &plans[s].key_offsets;
          }
          std::vector<size_t> c_local_groups(pg_count);
          std::vector<std::vector<size_t>> c_first(cw);
          RETURN_IF_FAIL(RunParallelTasks(cw, [&](size_t w) {
            auto& cp = c_partials[w];
            auto& cf = c_first[w];
            size_t bucket_rows = 0;
            for (size_t src = 0; src < cw; ++src) {
              bucket_rows += c_buckets[src][w].size();
            }
            cp.ReserveExpectedKeys(bucket_rows);
            std::vector<Value> scratch;
            for (size_t src = 0; src < cw; ++src) {
              for (const size_t g : c_buckets[src][w]) {
                bool inserted = false;
                const size_t local = cp.FindOrInsertRow(
                    *groups[parent].borrowed_key_rows[g], plans[s].key_offsets,
                    {}, &scratch, &inserted, c_hashes[g]);
                c_local_groups[g] = local;
                if (inserted) {
                  cf.push_back(g);
                }
              }
            }
          }));
          std::vector<std::vector<size_t>> c_global_ids(cw);
          size_t total_c = 0;
          for (size_t w = 0; w < cw; ++w) {
            c_global_ids[w].resize(c_partials[w].GroupCount(), SIZE_MAX);
            total_c += c_partials[w].GroupCount();
          }
          groups[s].ReserveGroups(total_c);
          groups[s].group_count = total_c;
          groups[s].borrowed_key_rows.resize(total_c);
          std::vector<size_t> c_cursors(cw, 0);
          std::array<size_t, 8> c_active_w{};
          size_t c_active_n = 0;
          for (size_t w = 0; w < cw; ++w) {
            if (!c_first[w].empty()) {
              c_active_w[c_active_n++] = w;
            }
          }
          for (size_t global = 0; global < total_c; ++global) {
            size_t best_idx = 0;
            size_t best_g = c_first[c_active_w[0]][c_cursors[c_active_w[0]]];
            for (size_t ai = 1; ai < c_active_n; ++ai) {
              const size_t w = c_active_w[ai];
              const size_t g = c_first[w][c_cursors[w]];
              if (g < best_g) {
                best_g = g;
                best_idx = ai;
              }
            }
            const size_t best_w = c_active_w[best_idx];
            const size_t local = c_cursors[best_w]++;
            if (c_cursors[best_w] == c_first[best_w].size()) {
              c_active_w[best_idx] = c_active_w[--c_active_n];
            }
            c_global_ids[best_w][local] = global;
            groups[s].borrowed_key_rows[global] =
                groups[parent].borrowed_key_rows[best_g];
          }
          if (parent == 0) {
            coarse_maps[s].resize(pg_count);
            RETURN_IF_FAIL(RunParallelTasks(cw, [&](size_t w) {
              const size_t begin = (pg_count * w) / cw;
              const size_t end = (pg_count * (w + 1)) / cw;
              for (size_t g = begin; g < end; ++g) {
                coarse_maps[s][g] =
                    c_global_ids[c_hashes[g] % cw][c_local_groups[g]];
              }
            }));
          } else {
            std::vector<size_t> parent_groups(pg_count);
            RETURN_IF_FAIL(RunParallelTasks(cw, [&](size_t w) {
              const size_t begin = (pg_count * w) / cw;
              const size_t end = (pg_count * (w + 1)) / cw;
              for (size_t g = begin; g < end; ++g) {
                parent_groups[g] =
                    c_global_ids[c_hashes[g] % cw][c_local_groups[g]];
              }
            }));
            const size_t finest_count = groups[0].GroupCount();
            coarse_maps[s].resize(finest_count);
            const auto& pmap = coarse_maps[parent];
            RETURN_IF_FAIL(RunParallelTasks(cw, [&](size_t w) {
              const size_t begin = (finest_count * w) / cw;
              const size_t end = (finest_count * (w + 1)) / cw;
              for (size_t fg = begin; fg < end; ++fg) {
                coarse_maps[s][fg] = parent_groups[pmap[fg]];
              }
            }));
          }
          continue;
        }
        std::vector<size_t> parent_keys;
        for (const slot_t key : plans[s].key_offsets) {
          const auto found = std::ranges::find(plans[parent].key_offsets, key);
          parent_keys.push_back(static_cast<size_t>(
              std::distance(plans[parent].key_offsets.begin(), found)));
        }
        std::vector<size_t> parent_groups;
        parent_groups.reserve(pg_count);
        std::vector<Value> coarse_keys;
        coarse_keys.reserve(parent_keys.size());
        for (size_t group = 0; group < pg_count; ++group) {
          if (borrow_keys) {
            bool inserted = false;
            parent_groups.push_back(groups[s].FindOrInsertRow(
                *groups[parent].borrowed_key_rows[group], plans[s].key_offsets,
                aggregates[s], &coarse_keys, &inserted));
            continue;
          }
          coarse_keys.clear();
          for (const size_t key : parent_keys) {
            coarse_keys.push_back(groups[parent].GroupKey(group, key));
          }
          bool inserted = false;
          parent_groups.push_back(groups[s].FindOrInsert(
              coarse_keys,
              groups[s].int_key && !coarse_keys[0].IsNull()
                  ? coarse_keys[0].value.int_value
                  : 0,
              argument_count, aggregates[s], &inserted));
        }
        if (parent == 0) {
          coarse_maps[s] = std::move(parent_groups);
        } else {
          coarse_maps[s].reserve(groups[0].GroupCount());
          for (const size_t parent_group : coarse_maps[parent]) {
            coarse_maps[s].push_back(parent_groups[parent_group]);
          }
        }
      }
    }  // if (!packed_done)
    phase("finest");
    std::vector<Status> errors(count, Status::kSuccess);
    const auto fold_sets = [&](size_t worker) {
      for (size_t s = worker + 1; s < count; s += set_workers) {
        try {
          if (!has_distinct_aggs && groups[s].slim && groups[0].slim &&
              groups[0].GroupCount() > 0 &&
              groups[s].GroupCount() == groups[0].GroupCount()) {
            auto& local = groups[s];
            const auto& finest = groups[0];
            local.slim_counts = finest.slim_counts;
            local.slim_int_totals = finest.slim_int_totals;
            local.slim_double_totals = finest.slim_double_totals;
            local.slim_flags = finest.slim_flags;
            continue;
          }
          const auto& coarse_groups = coarse_maps[s];
          if (can_fold_partials) {
            auto& local = groups[s];
            auto& finest = groups[0];
            const size_t nfine = finest.GroupCount();
            if (!exact_partial_merge && finite_sum && argument_count == 1) {
              const size_t* __restrict cgs = coarse_groups.data();
              const int64_t* __restrict f_cnt = finest.slim_counts.data();
              const double* __restrict f_dbl = finest.slim_double_totals.data();
              const uint8_t* __restrict f_flg = finest.slim_flags.data();
              int64_t* __restrict l_cnt = local.slim_counts.data();
              double* __restrict l_dbl = local.slim_double_totals.data();
              uint8_t* __restrict l_flg = local.slim_flags.data();
              for (size_t fine = 0; fine < nfine; ++fine) {
                const size_t coarse = cgs[fine];
                l_cnt[coarse] += f_cnt[fine];
                l_dbl[coarse] += f_dbl[fine];
                l_flg[coarse] |= f_flg[fine];
              }
            } else {
              for (size_t fine = 0; fine < nfine; ++fine) {
                const size_t coarse = coarse_groups[fine];
                for (size_t a = 0; a < argument_count; ++a) {
                  local.SlimCount(coarse, a) += finest.SlimCount(fine, a);
                  local.SlimIntTotal(coarse, a) += finest.SlimIntTotal(fine, a);
                  local.SlimDoubleTotal(coarse, a) +=
                      finest.SlimDoubleTotal(fine, a);
                  local.SlimFlags(coarse, a) |= finest.SlimFlags(fine, a);
                }
              }
            }
            continue;
          }
          if (finite_sum) {
            auto& local = groups[s];
            if (no_nulls) {
              std::ranges::fill(local.slim_counts, int64_t{1});
            }
            if (all_double) {
              std::ranges::fill(local.slim_flags, uint8_t{2});
              double* __restrict totals = local.slim_double_totals.data();
              const size_t* __restrict cgs = coarse_groups.data();
              const size_t* __restrict rgs = row_groups.data();
              if (all_direct_args &&
                  plans[0].aggregates[0].column.has_value()) {
                const slot_t col = *plans[0].aggregates[0].column;
                if (input.HasColumnarOnly()) {
                  size_t r = 0;
                  Value tmp;
                  for (const ColumnarChunk& ch : input.columnar->chunks) {
                    const ColumnarColumn& c = ch.columns[col];
                    if (c.IsFlat() && c.is_uniform &&
                        c.type == ValueType::kDouble && !c.has_null) {
                      const double* __restrict src_d = c.doubles.data();
                      for (size_t lr = 0; lr < ch.rows; ++lr, ++r) {
                        totals[cgs[rgs[r]]] += src_d[lr];
                      }
                    } else if (c.is_uniform && c.type == ValueType::kDouble &&
                               !c.has_null) {
                      for (size_t lr = 0; lr < ch.rows; ++lr, ++r) {
                        totals[cgs[rgs[r]]] += c.DoubleAt(lr);
                      }
                    } else {
                      for (size_t lr = 0; lr < ch.rows; ++lr, ++r) {
                        c.WriteValueInto(lr, &tmp);
                        totals[cgs[rgs[r]]] += tmp.value.double_value;
                      }
                    }
                  }
                } else {
                  for (size_t r = 0; r < total_rows; ++r) {
                    totals[cgs[rgs[r]]] +=
                        input.rows[r][col].value.double_value;
                  }
                }
              } else if (!flat_double_args.empty()) {
                const double* __restrict f_v = flat_double_args.data();
                for (size_t r = 0; r < total_rows; ++r) {
                  totals[cgs[rgs[r]]] += f_v[r];
                }
              } else {
                size_t r = 0;
                for (auto& worker_argument : worker_arguments) {
                  for (const Value& value : worker_argument) {
                    totals[cgs[rgs[r]]] += value.value.double_value;
                    ++r;
                  }
                }
              }
            } else if (all_direct_args) {
              if (input.HasColumnarOnly()) {
                const slot_t col = *plans[0].aggregates[0].column;
                const bool is_cnt =
                    plans[0].slim_kinds[0] == SlimAggKind::kCount;
                size_t r = 0;
                Value tmp;
                for (const ColumnarChunk& ch : input.columnar->chunks) {
                  const ColumnarColumn& c = ch.columns[col];
                  for (size_t lr = 0; lr < ch.rows; ++lr, ++r) {
                    c.WriteValueInto(lr, &tmp);
                    add_finite_sum(
                        local, coarse_groups[row_groups[r]],
                        is_cnt ? (tmp.IsNull() ? Value() : Value(1)) : tmp);
                  }
                }
              } else {
                for (size_t r = 0; r < total_rows; ++r) {
                  add_finite_sum(local, coarse_groups[row_groups[r]],
                                 direct_arg_at(r, 0));
                }
              }
            } else if (!flat_double_args.empty()) {
              const double* __restrict f_v = flat_double_args.data();
              const uint8_t* __restrict f_k = flat_arg_kinds.data();
              for (size_t r = 0; r < total_rows; ++r) {
                const size_t group = coarse_groups[row_groups[r]];
                if (!no_nulls && f_k[r] != 0) {
                  local.SlimCount(group, 0) = 1;
                }
                if (f_k[r] == 1) {
                  local.SlimDoubleTotal(group, 0) += f_v[r];
                  local.SlimFlags(group, 0) |= 2;
                }
              }
            } else {
              for (size_t w = 0; w < worker_arguments.size(); ++w) {
                size_t r = w * arg_chunk;
                for (const Value& value : worker_arguments[w]) {
                  add_finite_sum(local, coarse_groups[row_groups[r]], value);
                  ++r;
                }
              }
            }
            continue;
          }

          if (all_direct_args && plans[0].aggregates[0].column.has_value()) {
            if (input.HasColumnarOnly()) {
              size_t r = 0;
              Value tmp;
              for (const ColumnarChunk& ch : input.columnar->chunks) {
                for (size_t lr = 0; lr < ch.rows && errors[s].ok(); ++lr, ++r) {
                  const size_t group = coarse_groups[row_groups[r]];
                  for (size_t a = 0; a < argument_count; ++a) {
                    const auto& item = plans[0].aggregates[a];
                    ch.columns[*item.column].WriteValueInto(lr, &tmp);
                    Value arg_val =
                        plans[0].slim_kinds[a] == SlimAggKind::kCount
                            ? (tmp.IsNull() ? Value() : Value(1))
                            : tmp;
                    if (!ApplySlimValue(&groups[s], group, a,
                                        plans[s].slim_kinds[a], &arg_val,
                                        &errors[s])) {
                      break;
                    }
                  }
                }
                if (!errors[s].ok()) {
                  break;
                }
              }
            } else {
              for (size_t r = 0; r < total_rows && errors[s].ok(); ++r) {
                const size_t group = coarse_groups[row_groups[r]];
                for (size_t a = 0; a < argument_count; ++a) {
                  Value arg_val = direct_arg_at(r, a);
                  if (!ApplySlimValue(&groups[s], group, a,
                                      plans[s].slim_kinds[a], &arg_val,
                                      &errors[s])) {
                    break;
                  }
                }
              }
            }
          } else if (!flat_double_args.empty()) {
            const double* __restrict f_v = flat_double_args.data();
            const uint8_t* __restrict f_k = flat_arg_kinds.data();
            for (size_t r = 0; r < total_rows && errors[s].ok(); ++r) {
              const size_t group = coarse_groups[row_groups[r]];
              const size_t base = r * argument_count;
              for (size_t a = 0; a < argument_count; ++a) {
                if (!ApplyFlatArg(&groups[s], group, a, plans[s].slim_kinds[a],
                                  f_v[base + a], f_k[base + a], &errors[s])) {
                  break;
                }
              }
            }
          } else {
            for (size_t w = 0; w < worker_arguments.size() && errors[s].ok();
                 ++w) {
              const auto& w_args = worker_arguments[w];
              const size_t begin = w * arg_chunk;
              const size_t w_rows = w_args.size() / argument_count;
              for (size_t lr = 0; lr < w_rows && errors[s].ok(); ++lr) {
                const size_t group = coarse_groups[row_groups[begin + lr]];
                const size_t base = lr * argument_count;
                for (size_t a = 0; a < argument_count; ++a) {
                  if (!ApplySlimValue(&groups[s], group, a,
                                      plans[s].slim_kinds[a], &w_args[base + a],
                                      &errors[s])) {
                    break;
                  }
                }
              }
            }
          }
        } catch (const std::exception& error) {
          errors[s] = StatusError(StatusCode::kRuntimeError, error.what());
        } catch (...) {
          errors[s] = StatusError(StatusCode::kRuntimeError,
                                  "parallel grouping set failed");
        }
      }
    };
    if (set_workers == 1) {
      fold_sets(0);
    } else {
      RETURN_IF_FAIL(RunParallelTasks(set_workers, fold_sets));
    }
    for (const auto& error : errors) {
      RETURN_IF_FAIL(error);
    }
    phase("fold");
    if (auto* runtime = context.execution_runtime()) {
      ++runtime->typed_parallel_aggregations;
    }
  } else {
    // Drop the optional argument cache before the original streaming route.
    std::vector<std::vector<Value>>().swap(worker_arguments);
    std::vector<double>().swap(flat_double_args);
    std::vector<uint8_t>().swap(flat_arg_kinds);
    argument_charge.reset();
    if (input.HasColumnarOnly()) {
      RETURN_IF_FAIL(input.EnsureRowsMaterialized());
    }
    std::vector<size_t> mapping;
    std::vector<Value> keys, coarse, values;
    keys.reserve(plans[0].key_offsets.size());
    coarse.reserve(plans[0].key_offsets.size());
    values.reserve(aggregates[0].size());
    for (const auto& row : input.rows) {
      bool inserted = false;
      const size_t finest = groups[0].FindOrInsertRow(
          row, plans[0].key_offsets, aggregates[0], &keys, &inserted);
      if (inserted) {
        mapping.push_back(finest);
        for (size_t s = 1; s < count; ++s) {
          if (borrow_keys) {
            bool added = false;
            mapping.push_back(groups[s].FindOrInsertRow(
                row, plans[s].key_offsets, aggregates[s], &coarse, &added));
            continue;
          }
          coarse.clear();
          for (const auto key : key_maps[s]) {
            coarse.push_back(keys[key]);
          }
          bool added = false;
          mapping.push_back(groups[s].FindOrInsert(
              coarse,
              groups[s].int_key && !coarse[0].IsNull()
                  ? coarse[0].value.int_value
                  : 0,
              aggregates[s].size(), aggregates[s], &added));
        }
      }
      values.clear();
      for (const auto& item : plans[0].aggregates) {
        ASSIGN_OR_RETURN(Value, value,
                         CompactArgument(item, row, plans[0].slim));
        values.push_back(std::move(value));
      }
      for (size_t s = 0; s < count; ++s) {
        const size_t group = mapping[(finest * count) + s];
        for (size_t a = 0; a < values.size(); ++a) {
          if (plans[s].slim) {
            Status error{Status::kSuccess};
            if (!ApplySlimValue(&groups[s], group, a, plans[s].slim_kinds[a],
                                &values[a], &error)) {
              return error;
            }
          } else {
            RETURN_IF_FAIL(groups[s].State(group, a).Add(values[a]));
          }
        }
      }
    }
  }
  std::vector<Relation> output(count);
  // A parent RANK() <= K only needs the groups that can still place. Keeping
  // boundary ties preserves the rank values of every surviving row.
  std::vector<std::vector<uint8_t>> rank_keep;
  const auto* rank_limit =
      context.execution_runtime() != nullptr
          ? context.execution_runtime()->grouping_rank_limit
                ? &*context.execution_runtime()->grouping_rank_limit
                : nullptr
          : nullptr;
  if (rank_limit != nullptr && rank_limit->limit > 0 &&
      std::ranges::all_of(plans, [](const auto& plan) { return plan.slim; })) {
    struct RankRef {
      size_t set{0};
      size_t group{0};
      double sum{0};
    };
    struct Category {
      std::string_view name;
      bool is_null{false};
      bool is_int{false};
      bool is_unsigned{false};  // signed -1 and UINT64_MAX are distinct keys
      int64_t int_key{0};
      std::vector<RankRef> rows;
    };
    struct CategoryTable {
      std::vector<Category> cats;
      std::optional<size_t> null_id;
      std::unordered_map<int64_t, size_t> int_ids;
      std::unordered_map<std::string_view, size_t> str_ids;
      std::deque<std::string> owned_strings;

      size_t FindOrAddNull() {
        if (null_id.has_value()) {
          return *null_id;
        }
        Category cat;
        cat.is_null = true;
        const size_t id = cats.size();
        null_id = id;
        cats.push_back(std::move(cat));
        return id;
      }

      size_t FindOrAddInt(int64_t key, bool is_unsigned = false) {
        if (cats.size() <= 8 && int_ids.empty()) {
          for (size_t i = 0; i < cats.size(); ++i) {
            if (cats[i].is_int && cats[i].is_unsigned == is_unsigned &&
                cats[i].int_key == key) {
              return i;
            }
          }
        } else {
          if (int_ids.empty()) {
            for (size_t i = 0; i < cats.size(); ++i) {
              if (cats[i].is_int && !cats[i].is_unsigned) {
                int_ids.emplace(cats[i].int_key, i);
              }
            }
          }
          if (auto it = int_ids.find(key); it != int_ids.end()) {
            return it->second;
          }
        }
        Category cat;
        cat.is_int = true;
        cat.is_unsigned = is_unsigned;
        cat.int_key = key;
        const size_t id = cats.size();
        if (!int_ids.empty() || cats.size() >= 8) {
          int_ids.emplace(cat.int_key, id);
        }
        cats.push_back(std::move(cat));
        return id;
      }

      size_t FindOrAddString(std::string_view name) {
        if (cats.size() <= 8 && str_ids.empty()) {
          for (size_t i = 0; i < cats.size(); ++i) {
            if (!cats[i].is_null && !cats[i].is_int && cats[i].name == name) {
              return i;
            }
          }
        } else {
          if (str_ids.empty()) {
            for (size_t i = 0; i < cats.size(); ++i) {
              if (!cats[i].is_null && !cats[i].is_int) {
                str_ids.emplace(cats[i].name, i);
              }
            }
          }
          if (auto it = str_ids.find(name); it != str_ids.end()) {
            return it->second;
          }
        }
        Category cat;
        cat.name = name;
        const size_t id = cats.size();
        if (!str_ids.empty() || cats.size() >= 8) {
          str_ids.emplace(cat.name, id);
        }
        cats.push_back(std::move(cat));
        return id;
      }

      size_t FindOrAddOwnedString(std::string name) {
        if (!str_ids.empty()) {
          if (auto it = str_ids.find(name); it != str_ids.end()) {
            return it->second;
          }
        } else if (cats.size() <= 8) {
          for (size_t i = 0; i < cats.size(); ++i) {
            if (!cats[i].is_null && !cats[i].is_int && cats[i].name == name) {
              return i;
            }
          }
        }
        owned_strings.push_back(std::move(name));
        return FindOrAddString(owned_strings.back());
      }

      size_t FindOrAdd(const Value& value) {
        if (value.IsNull()) {
          return FindOrAddNull();
        }
        if (value.type == ValueType::kInt64 || value.type == ValueType::kDate) {
          return FindOrAddInt(value.value.int_value, value.IsUnsigned());
        }
        if (value.type == ValueType::kVarChar) {
          return FindOrAddString(value.value.varchar_value);
        }
        return SIZE_MAX;
      }
    };
    CategoryTable merged_categories;
    bool shaped = true;
    std::vector<std::optional<size_t>> partition_item(count);
    std::vector<std::optional<size_t>> order_item(count);
    for (size_t s = 0; shaped && s < count; ++s) {
      const auto& list = sets[s]->SelectList();
      if (list.size() != plans[s].emit_items.size()) {
        shaped = false;
        break;
      }
      for (size_t i = 0; i < list.size(); ++i) {
        const std::string name = ProjectionName(list[i], i);
        if (name == rank_limit->partition_name) {
          partition_item[s] = i;
        } else if (name == rank_limit->order_name) {
          order_item[s] = i;
        }
      }
      if (!partition_item[s] || !order_item[s]) {
        shaped = false;
        break;
      }
      const auto& order = plans[s].emit_items[*order_item[s]];
      const auto& part = plans[s].emit_items[*partition_item[s]];
      if (order.post_agg != nullptr || part.post_agg != nullptr ||
          order.is_key || order.constant ||
          order.index >= plans[s].slim_kinds.size() ||
          plans[s].slim_kinds[order.index] != SlimAggKind::kSumDouble ||
          !(part.is_key || part.constant)) {
        shaped = false;
      }
    }
    size_t total_rank_groups = 0;
    if (shaped) {
      for (size_t s = 0; s < count; ++s) {
        total_rank_groups += groups[s].GroupCount();
      }
    }
    const size_t rank_workers =
        (shaped && agg_workers > 1 && total_rank_groups >= 8192)
            ? std::min<size_t>(8, agg_workers)
            : 1;
    if (shaped && rank_workers > 1) {
      struct RankSlice {
        size_t set;
        size_t begin;
        size_t end;
      };
      std::vector<RankSlice> slices;
      const size_t slice_size =
          std::max<size_t>(4096, total_rank_groups / (rank_workers * 4));
      for (size_t s = 0; s < count; ++s) {
        const size_t ng = groups[s].GroupCount();
        for (size_t begin = 0; begin < ng; begin += slice_size) {
          slices.push_back({s, begin, std::min(ng, begin + slice_size)});
        }
      }
      std::vector<CategoryTable> worker_tables(rank_workers);
      std::vector<uint8_t> worker_failed(rank_workers, 0);
      std::atomic<size_t> next_slice{0};
      RETURN_IF_FAIL(RunParallelTasks(rank_workers, [&](size_t w) {
        auto& tbl = worker_tables[w];
        for (size_t idx = next_slice.fetch_add(1); idx < slices.size();
             idx = next_slice.fetch_add(1)) {
          const auto [s, begin, end] = slices[idx];
          const auto& part = plans[s].emit_items[*partition_item[s]];
          const auto& order = plans[s].emit_items[*order_item[s]];
          const bool mixed = !groups[s].mixed_numeric_sums.empty() &&
                             groups[s].mixed_numeric_sums[order.index];
          size_t fixed_cat = SIZE_MAX;
          if (part.constant) {
            fixed_cat = tbl.FindOrAdd(*part.constant);
            if (fixed_cat == SIZE_MAX) {
              worker_failed[w] = 1;
              return;
            }
          }
          size_t chunk_hint = 0;
          Value key_buf;
          for (size_t group = begin; group < end; ++group) {
            size_t category = SIZE_MAX;
            if (part.constant) {
              category = fixed_cat;
            } else if (groups[s].borrowed_columnar != nullptr) {
              groups[s].ReadGroupKey(group, part.index, &key_buf, &chunk_hint);
              category = key_buf.type == ValueType::kVarChar &&
                                 !key_buf.owned_data.empty()
                             ? tbl.FindOrAddOwnedString(
                                   std::string(key_buf.value.varchar_value))
                             : tbl.FindOrAdd(key_buf);
            } else {
              category = tbl.FindOrAdd(groups[s].GroupKey(group, part.index));
            }
            if (category == SIZE_MAX) {
              worker_failed[w] = 1;
              return;
            }
            RankRef ref{.set = s, .group = group, .sum = 0};
            if (groups[s].SlimCount(group, order.index) != 0) {
              ref.sum = groups[s].SlimDoubleTotal(group, order.index);
              if (mixed) {
                ref.sum += static_cast<double>(
                    groups[s].SlimIntTotal(group, order.index));
              }
            } else {
              ref.sum = std::numeric_limits<double>::quiet_NaN();
            }
            tbl.cats[category].rows.push_back(ref);
          }
        }
      }));
      if (std::ranges::any_of(worker_failed,
                              [](uint8_t f) { return f != 0; })) {
        shaped = false;
      } else {
        std::vector<std::vector<size_t>> cat_map(rank_workers);
        for (size_t w = 0; w < rank_workers; ++w) {
          cat_map[w].resize(worker_tables[w].cats.size());
          for (size_t lc = 0; lc < worker_tables[w].cats.size(); ++lc) {
            const Category& src = worker_tables[w].cats[lc];
            if (src.is_null) {
              cat_map[w][lc] = merged_categories.FindOrAddNull();
            } else if (src.is_int) {
              cat_map[w][lc] =
                  merged_categories.FindOrAddInt(src.int_key, src.is_unsigned);
            } else {
              cat_map[w][lc] = merged_categories.FindOrAddString(src.name);
            }
          }
        }
        std::vector<size_t> total_per_cat(merged_categories.cats.size(), 0);
        for (size_t w = 0; w < rank_workers; ++w) {
          for (size_t lc = 0; lc < worker_tables[w].cats.size(); ++lc) {
            total_per_cat[cat_map[w][lc]] +=
                worker_tables[w].cats[lc].rows.size();
          }
        }
        for (size_t gc = 0; gc < merged_categories.cats.size(); ++gc) {
          merged_categories.cats[gc].rows.reserve(total_per_cat[gc]);
        }
        for (size_t w = 0; w < rank_workers; ++w) {
          for (size_t lc = 0; lc < worker_tables[w].cats.size(); ++lc) {
            auto& dst = merged_categories.cats[cat_map[w][lc]].rows;
            auto& src = worker_tables[w].cats[lc].rows;
            dst.insert(dst.end(), src.begin(), src.end());
          }
        }
      }
    } else if (shaped) {
      for (size_t s = 0; s < count; ++s) {
        const auto& part = plans[s].emit_items[*partition_item[s]];
        const auto& order = plans[s].emit_items[*order_item[s]];
        const bool mixed = !groups[s].mixed_numeric_sums.empty() &&
                           groups[s].mixed_numeric_sums[order.index];
        size_t chunk_hint = 0;
        Value key_buf;
        for (size_t group = 0; group < groups[s].GroupCount(); ++group) {
          size_t category = SIZE_MAX;
          if (part.constant) {
            category = merged_categories.FindOrAdd(*part.constant);
          } else if (groups[s].borrowed_columnar != nullptr) {
            groups[s].ReadGroupKey(group, part.index, &key_buf, &chunk_hint);
            category = key_buf.type == ValueType::kVarChar &&
                               !key_buf.owned_data.empty()
                           ? merged_categories.FindOrAddOwnedString(
                                 std::string(key_buf.value.varchar_value))
                           : merged_categories.FindOrAdd(key_buf);
          } else {
            category = merged_categories.FindOrAdd(
                groups[s].GroupKey(group, part.index));
          }
          if (category == SIZE_MAX) {
            shaped = false;
            break;
          }
          RankRef ref{.set = s, .group = group, .sum = 0};
          if (groups[s].SlimCount(group, order.index) != 0) {
            ref.sum = groups[s].SlimDoubleTotal(group, order.index);
            if (mixed) {
              ref.sum += static_cast<double>(
                  groups[s].SlimIntTotal(group, order.index));
            }
            merged_categories.cats[category].rows.push_back(ref);
          } else {
            ref.sum = std::numeric_limits<double>::quiet_NaN();
            merged_categories.cats[category].rows.push_back(ref);
          }
        }
        if (!shaped) {
          break;
        }
      }
    }
    // A ROLLUP/grouping-set "all" row is synthesized for empty sets below.
    // Materialize it BEFORE the rank_keep vector is sized, otherwise its
    // group index falls outside keep[] and emit silently drops the grand
    // total (especially with empty input and 3+ sets).
    for (size_t s = 0; s < count; ++s) {
      if (groups[s].GroupCount() == 0 && sets[s]->IsGroupAll()) {
        bool inserted = false;
        static_cast<void>(groups[s].FindOrInsert({}, 0, aggregates[s].size(),
                                                 aggregates[s], &inserted));
      }
    }
    if (shaped) {
      rank_keep.assign(count, {});
      for (size_t s = 0; s < count; ++s) {
        rank_keep[s].assign(groups[s].GroupCount(), 0);
        // A synthesized group-all row has no RankRef in the ranking pass;
        // keep it so the grand total survives QUALIFY rank <= n.
        if (sets[s]->IsGroupAll() && !rank_keep[s].empty()) {
          rank_keep[s][0] = 1;
        }
      }
      auto process_category = [&](Category& category) {
        auto nan_begin = std::partition(
            category.rows.begin(), category.rows.end(),
            [](const RankRef& ref) { return !std::isnan(ref.sum); });
        const size_t num_count =
            static_cast<size_t>(nan_begin - category.rows.begin());
        if (num_count >= rank_limit->limit) {
          const auto nth = category.rows.begin() +
                           static_cast<std::ptrdiff_t>(rank_limit->limit - 1);
          std::nth_element(
              category.rows.begin(), nth, nan_begin,
              [](const RankRef& a, const RankRef& b) { return a.sum > b.sum; });
          const double threshold = nth->sum;
          for (auto it = category.rows.begin(); it != nan_begin; ++it) {
            if (it->sum >= threshold) {
              rank_keep[it->set][it->group] = 1;
            }
          }
        } else {
          for (const RankRef& ref : category.rows) {
            rank_keep[ref.set][ref.group] = 1;
          }
        }
      };
      if (rank_workers > 1 && merged_categories.cats.size() > 1) {
        std::atomic<size_t> next_cat{0};
        RETURN_IF_FAIL(RunParallelTasks(
            std::min(rank_workers, merged_categories.cats.size()), [&](size_t) {
              for (size_t c = next_cat.fetch_add(1);
                   c < merged_categories.cats.size();
                   c = next_cat.fetch_add(1)) {
                process_category(merged_categories.cats[c]);
              }
            }));
      } else {
        for (Category& category : merged_categories.cats) {
          process_category(category);
        }
      }
    }
  }
  size_t output_groups = 0;
  for (size_t s = 0; s < count; ++s) {
    output_groups += groups[s].GroupCount();
  }
  // Building has finished: the per-input-row, per-set upper bound is no
  // longer live workspace. Retain the actual slim vector capacities and a
  // conservative bound for the integer hash tables instead. Keeping the old
  // bound while materializing results can spill a small output even though
  // the grouping tables themselves occupy only a fraction of that bound.
  // Borrowed keys have no separately owned string payload to account for.
  std::vector<std::vector<Value>>().swap(worker_arguments);
  std::vector<double>().swap(flat_double_args);
  std::vector<uint8_t>().swap(flat_arg_kinds);
  argument_charge.reset();
  if (borrow_keys && std::ranges::all_of(groups, [](const auto& group) {
        return group.slim;
      })) {
    size_t retained = groups.capacity() * sizeof(TypedWorkerGroups);
    for (const auto& group : groups) {
      retained +=
          group.key_index.capacity() * sizeof(TypedWorkerGroups::KeySlot);
      retained += group.borrowed_key_rows.capacity() * sizeof(const Row*);
      retained += group.borrowed_key_row_ids.capacity() * sizeof(uint32_t);
      retained += group.flat_group_keys.capacity() * sizeof(Value);
      retained +=
          (group.slim_counts.capacity() + group.slim_int_totals.capacity()) *
          sizeof(int64_t);
      retained += group.slim_double_totals.capacity() * sizeof(double);
      retained += group.slim_flags.capacity() * sizeof(uint8_t);
      retained += group.mixed_numeric_sums.capacity();
      // FlatIntKeyMap grows by at most four at a 70% load factor, with
      // 16-byte slots. This also covers its minimum allocation.
      retained += (group.GroupCount() + 2) * 128;
    }
    if (retained < charge.Bytes()) {
      charge = QueryMemoryCharge(retained);
    }
  }
  size_t kept_groups = output_groups;
  if (!rank_keep.empty()) {
    kept_groups = 0;
    for (const auto& rk : rank_keep) {
      for (const uint8_t k : rk) {
        kept_groups += k != 0 ? 1 : 0;
      }
    }
  }
  if (set_workers > 1 && output_groups >= 4096 && kept_groups >= 2048) {
    // A large finest set must not monopolize one worker while the small
    // rollup totals finish early. Schedule contiguous group ranges across
    // all sets, then concatenate each set in its original encounter order.
    static const bool split_disabled =
        std::getenv("TINYLAMB_DISABLE_GROUPING_SET_EMIT_SPLIT") != nullptr;
    const size_t workers = split_disabled
                               ? std::min<size_t>(8, set_workers)
                               : ParallelWorkerCount(output_groups / 8192);
    const size_t chunk = std::max<size_t>(
        4096, (output_groups + workers * 4 - 1) / (workers * 4));
    struct EmitRange {
      size_t set;
      size_t begin;
      size_t end;
    };
    std::vector<EmitRange> ranges;
    for (size_t s = 0; s < count; ++s) {
      const size_t size = groups[s].GroupCount();
      const size_t stride =
          split_disabled || !plans[s].slim ? std::max<size_t>(1, size) : chunk;
      for (size_t begin = 0; begin < std::max<size_t>(1, size);
           begin += stride) {
        const size_t range_end = std::min(size, begin + stride);
        if (!rank_keep.empty() && begin < range_end &&
            std::memchr(rank_keep[s].data() + begin, 1, range_end - begin) ==
                nullptr) {
          continue;
        }
        ranges.push_back({s, begin, range_end});
      }
    }
    ExecutionRuntime* const parent_runtime = context.execution_runtime();
    std::vector<ExecutionRuntime> runtimes(workers);
    std::vector<Relation> shards(ranges.size());
    std::vector<std::vector<ColumnTypeTracker>> range_tracks(ranges.size());
    std::vector<Status> errors(ranges.size(), Status::kSuccess);
    std::atomic<size_t> next{0};
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
      for (size_t i = next.fetch_add(1); i < ranges.size();
           i = next.fetch_add(1)) {
        const auto [s, begin, end] = ranges[i];
        const std::vector<uint8_t>* keep =
            rank_keep.empty() ? nullptr : &rank_keep[s];
        auto relation = FinishCompactGroups(
            &runtimes[worker], *sets[s], input, aggregates[s], plans[s],
            groups[s], nullptr, begin, end, keep, &range_tracks[i]);
        if (!relation.HasValue()) {
          errors[i] = relation.GetStatus();
          continue;
        }
        shards[i] = relation.MoveValue();
        shards[i].set_runtime(parent_runtime);
      }
    }));
    for (const auto& error : errors) {
      RETURN_IF_FAIL(error);
    }
    std::vector<std::vector<Relation>> set_shards(count);
    std::vector<std::vector<std::vector<ColumnTypeTracker>>> set_tracks(count);
    for (size_t i = 0; i < ranges.size(); ++i) {
      const size_t s = ranges[i].set;
      if (output[s].schema.ColumnCount() == 0 &&
          shards[i].schema.ColumnCount() > 0) {
        output[s].schema = shards[i].schema;
      }
      set_shards[s].push_back(std::move(shards[i]));
      set_tracks[s].push_back(std::move(range_tracks[i]));
    }
    for (size_t s = 0; s < count; ++s) {
      output[s].set_runtime(parent_runtime);
      CopyExecutionStats(&output[s], input);
      RETURN_IF_FAIL(Relation::MergeShardsInto(set_shards[s], &output[s]));
      std::vector<Column> set_columns;
      set_columns.reserve(sets[s]->SelectList().size());
      for (size_t ci = 0; ci < sets[s]->SelectList().size(); ++ci) {
        set_columns.emplace_back(ProjectionName(sets[s]->SelectList()[ci], ci),
                                 ValueType::kNull);
      }
      const bool all_uniform =
          ColumnTypeTracker::FinalizeColumns(set_tracks[s], set_columns);
      output[s].schema = Schema("", std::move(set_columns));
      RETURN_IF_FAIL(output[s].FinishSpill());
      RETURN_IF_FAIL(output[s].GetStatus());
      output[s].uniform_cell_types =
          !output[s].HasSpill() && all_uniform && !output[s].rows.empty();
    }
    if (auto* runtime = context.execution_runtime()) {
      for (const auto& local : runtimes) {
        runtime->aggregate_input_rows += local.aggregate_input_rows;
        runtime->aggregate_groups += local.aggregate_groups;
        runtime->aggregate_updates += local.aggregate_updates;
        runtime->relation_spills += local.relation_spills;
      }
    }
  } else {
    for (size_t s = 0; s < count; ++s) {
      ASSIGN_OR_RETURN(
          Relation, relation,
          (FinishCompactGroups(context.execution_runtime(), *sets[s], input,
                               aggregates[s], plans[s], groups[s], nullptr, 0,
                               SIZE_MAX,
                               rank_keep.empty() ? nullptr : &rank_keep[s])));
      output[s] = std::move(relation);
    }
  }
  phase("emit");
  if (auto* runtime = context.execution_runtime()) {
    runtime->project_ms += ElapsedMs(started);
  }
  return std::optional<std::vector<Relation>>(std::move(output));
}

}  // namespace tinylamb::relational_detail
