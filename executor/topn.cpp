/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/topn.hpp"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <ostream>
#include <queue>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "executor/parallel_scan.hpp"
#include "executor/query_memory.hpp"
#include "expression/column_value.hpp"
#include "page/row_position.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/lstable_bridge.hpp"
#include "type/collation.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
Status TopNExecutor::Materialize() {
  if (limit_ == 0) {
    output_end_ = 0;
    materialized_ = true;
    return Status::kSuccess;
  }
  const size_t capacity = offset_ > static_cast<size_t>(-1) - limit_
                              ? static_cast<size_t>(-1)
                              : offset_ + limit_;
  heap_capacity_ = capacity;
  // One comparator contract for the whole heap: NULL placement, direction and
  // collation all come from SortKeyOrder (type/collation.hpp), the same type
  // the sort operators and PdqSort use, so a LIMIT over a case-insensitive
  // ORDER BY keeps exactly the rows a full sort would have kept.
  std::vector<SortKeyOrder> orders;
  orders.reserve(keys_.size());
  for (const Key& key : keys_) {
    orders.push_back(MakeSortKeyOrder(key));
  }
  auto precedes = [&orders](const Candidate& left, const Candidate& right) {
    for (size_t i = 0; i < left.keys.size(); ++i) {
      const int cmp = orders[i].Compare(left.keys[i], right.keys[i]);
      if (cmp != 0) {
        return cmp < 0;
      }
    }
    return left.sequence < right.sequence;
  };
  auto worse_first = [&precedes](const Candidate& left,
                                 const Candidate& right) {
    return precedes(left, right);
  };
  size_t sequence = 0;
  if (with_ties_) {
    DataChunk chunk;
    while (source_->NextBatch(&chunk) != 0) {
      for (size_t i = 0; i < chunk.Size(); ++i) {
        Candidate candidate{.row = chunk.RowAt(i),
                            .position = chunk.PositionAt(i),
                            .keys = {},
                            .sequence = sequence++};
        candidate.keys.reserve(keys_.size());
        for (const Key& key : keys_) {
          StatusOr<Value> key_value =
              key.expression->TryEvaluate(candidate.row, schema_);
          if (!key_value.HasValue()) {
            const Status key_error = key_value.GetStatus();
            FailWith(key_error);
            return key_error;
          }
          candidate.keys.push_back(key_value.MoveValue());
        }
        rows_.push_back(std::move(candidate));
      }
    }
    FailWithChildOf(*source_);
    std::ranges::sort(rows_, precedes);
    output_index_ = std::min(offset_, rows_.size());
    output_end_ = output_index_;
    if (output_index_ < rows_.size()) {
      const size_t boundary = limit_ > rows_.size() - output_index_
                                  ? rows_.size()
                                  : output_index_ + limit_;
      output_end_ = boundary;
      if (boundary != 0 && boundary <= rows_.size()) {
        const Candidate& last = rows_[boundary - 1];
        while (output_end_ < rows_.size()) {
          bool tied = true;
          for (size_t i = 0; i < last.keys.size(); ++i) {
            // Peers must collapse exactly the way the ordering groups them,
            // so a case-insensitive key treats 'a' and 'A' as the same tie
            // instead of splitting the boundary row. A binary key keeps
            // Value's equality (NaN, -0.0 and interval equivalence).
            if (!EqualUnderCollation(last.keys[i],
                                     rows_[output_end_].keys[i])) {
              tied = false;
              break;
            }
          }
          if (!tied) {
            break;
          }
          ++output_end_;
        }
      }
    }
    materialized_ = true;
    return GetStatus();
  }

  std::priority_queue<Candidate, std::vector<Candidate>, decltype(worse_first)>
      heap(worse_first);

  std::vector<size_t> direct_columns;
  for (const Key& key : keys_) {
    if (key.expression->Type() != TypeTag::kColumnValue) {
      break;
    }
    const int offset =
        schema_.Offset(key.expression->AsColumnValue().GetColumnName());
    if (offset < 0) {
      break;
    }
    direct_columns.push_back(static_cast<size_t>(offset));
  }
  bool folded = false;
  if (direct_columns.size() == keys_.size()) {
    auto scan = std::dynamic_pointer_cast<ParallelScan>(source_);
    if (scan && scan->OutputColumnCount() != schema_.ColumnCount()) {
      scan.reset();
    }
    if (scan) {
      std::vector<slot_t> required;
      required.reserve(direct_columns.size());
      for (size_t column : direct_columns) {
        required.push_back(scan->TableColumnSlot(column));
      }
      auto prepared = scan->PrepareLSTableColumns(required);
      if (!prepared.HasValue()) {
        return prepared.GetStatus();
      }
      if (!prepared.Value()) {
        scan.reset();
      }
    }
    const auto* store = scan ? scan->GetLSTableStore() : nullptr;
    if (store != nullptr &&
        scan->OutputColumnCount() == schema_.ColumnCount()) {
      struct BlockRow {
        const LSTablePageBlock* block{nullptr};
        size_t row{0};
        size_t sequence{0};
      };
      // Column-store blocks expose raw cells, and a stored VARCHAR never
      // carries a value-level COLLATE tag (Value::Serialize keeps a tag only
      // inside an array), so these Values compare exactly like the tag-free
      // values the chunk path hands the non-fused branches below. That is why
      // the comparisons here may read the key's static collation alone:
      // SortKeyOrder::Compare resolves the operands' own tags on top of it,
      // and for an untagged pair that resolution is the key collation itself.
      const auto value_at = [](const LSTableColumnBlock& column, size_t row) {
        if (column.IsNull(row)) {
          return Value();
        }
        if (column.type == ValueType::kVarChar) {
          return Value(std::string(column.VarcharAt(row)));
        }
        if (column.type == ValueType::kDouble) {
          return Value(std::bit_cast<double>(column.int_cells[row]));
        }
        Value value(column.int_cells[row]);
        value.type = column.type;
        return column.is_unsigned ? value.WithUnsigned() : value;
      };
      const auto block_precedes = [&](const BlockRow& left,
                                      const BlockRow& right) {
        for (size_t k = 0; k < keys_.size(); ++k) {
          const auto slot = scan->TableColumnSlot(direct_columns[k]);
          const auto& l = left.block->columns[slot];
          const auto& r = right.block->columns[slot];
          int comparison = 0;
          if (l.type == ValueType::kVarChar && !l.IsNull(left.row) &&
              !r.IsNull(right.row)) {
            if (l.dict && l.dict == r.dict &&
                l.dict->codes_follow_string_order &&
                orders[k].KeyCollation() != Collation::kCaseInsensitive) {
              // codes_follow_string_order is proven against
              // CompareVarCharForOrderBy(..., case_insensitive=false), and no
              // stored VARCHAR can carry a tag that would fold the comparison,
              // so code order *is* the canonical ORDER BY order here. The fold
              // arm below is the only place a collation changes byte order.
              const auto a = l.DictionaryCodeAt(left.row);
              const auto b = r.DictionaryCodeAt(right.row);
              comparison = a < b ? -1 : (b < a ? 1 : 0);
            } else {
              comparison = CompareVarCharForOrderBy(
                  l.VarcharAt(left.row), r.VarcharAt(right.row),
                  orders[k].KeyCollation() == Collation::kCaseInsensitive);
            }
            if (!orders[k].Ascending()) {
              comparison = -comparison;
            }
          } else {
            comparison = orders[k].Compare(value_at(l, left.row),
                                           value_at(r, right.row));
          }
          if (comparison != 0) {
            return comparison < 0;
          }
        }
        return left.sequence < right.sequence;
      };
      size_t bytes = 0;
      const size_t workers = scan->WorkerCount();
      if (!__builtin_mul_overflow(capacity, sizeof(BlockRow) * 2, &bytes) &&
          !__builtin_mul_overflow(bytes, workers + 1, &bytes) &&
          QueryMemoryBudget::Global().CanReserve(bytes)) {
        QueryMemoryCharge charge(bytes);
        using BlockHeap = std::priority_queue<BlockRow, std::vector<BlockRow>,
                                              decltype(block_precedes)>;
        std::vector<BlockHeap> partials;
        partials.reserve(workers);
        for (size_t w = 0; w < workers; ++w) {
          partials.emplace_back(block_precedes);
        }
        std::vector<size_t> prefixes(store->blocks.size() + 1, 0);
        for (size_t b = 0; b < store->blocks.size(); ++b) {
          if (__builtin_add_overflow(prefixes[b], store->blocks[b].row_count,
                                     &prefixes[b + 1])) {
            return StatusError(StatusCode::kTooBigData,
                               "TopN row count overflow");
          }
        }
        std::vector<size_t> counts(workers, 0);
        auto fold = scan->TryFoldLSTableBlocks(
            workers,
            [&](size_t w, const LSTablePageBlock& block,
                const uint32_t* selected, size_t count) {
              auto& local = partials[w];
              counts[w] += count;
              const size_t prefix =
                  prefixes[static_cast<size_t>(&block - store->blocks.data())];
              for (size_t i = 0; i < count; ++i) {
                const size_t row = selected == nullptr ? i : selected[i];
                const BlockRow candidate{
                    .block = &block, .row = row, .sequence = prefix + row};
                if (local.size() < capacity) {
                  local.push(candidate);
                } else if (block_precedes(candidate, local.top())) {
                  local.pop();
                  local.push(candidate);
                }
              }
              return Status::kSuccess;
            },
            nullptr,
            [&](size_t w, const LSTablePageBlock& block) {
              const auto& local = partials[w];
              if (local.size() < capacity || keys_.empty()) {
                return true;
              }
              const auto slot = scan->TableColumnSlot(direct_columns.front());
              const auto& column = block.columns[slot];
              // A NULL might precede the current boundary; leave these blocks
              // to the normal comparator. Equality also stays for later keys
              // and stable encounter-order ties.
              if (column.has_null) {
                return true;
              }
              const auto& boundary = local.top();
              Value best;
              if (column.type == ValueType::kVarChar) {
                // A code bound is a bound under the canonical order only when
                // the codes follow CompareVarCharForOrderBy(..., false) and the
                // key does not fold; both are checked here, and stored strings
                // carry no tag of their own that could resurrect the fold. The
                // comparison below then runs in the very order the bound was
                // taken in, so a skipped block cannot hold a winner.
                if (!column.dict || !column.dict->codes_follow_string_order ||
                    !column.has_code_min_max ||
                    orders.front().KeyCollation() ==
                        Collation::kCaseInsensitive) {
                  return true;
                }
                const auto code = orders.front().Ascending() ? column.min_code
                                                             : column.max_code;
                best.type = ValueType::kVarChar;
                best.value.varchar_value = column.dict->StringEntryAt(code);
              } else if ((column.type == ValueType::kInt64 ||
                          column.type == ValueType::kDate) &&
                         !column.is_unsigned && column.has_min_max) {
                best = Value(orders.front().Ascending() ? column.min_int
                                                        : column.max_int);
                best.type = column.type;
              } else {
                return true;
              }
              return orders.front().Compare(
                         best, value_at(boundary.block->columns[slot],
                                        boundary.row)) <= 0;
            });
        if (!fold.HasValue()) {
          return fold.GetStatus();
        }
        folded = fold.Value();
        if (folded) {
          BlockHeap best(block_precedes);
          for (size_t w = 0; w < workers; ++w) {
            input_rows_ += counts[w];
            auto& local = partials[w];
            while (!local.empty()) {
              const BlockRow candidate = local.top();
              local.pop();
              if (best.size() < capacity) {
                best.push(candidate);
              } else if (block_precedes(candidate, best.top())) {
                best.pop();
                best.push(candidate);
              }
            }
          }
          // Only the winning prefix becomes owned rows. The scan keeps the
          // immutable store alive throughout comparisons and materialization.
          while (!best.empty()) {
            const BlockRow selected = best.top();
            best.pop();
            Candidate candidate;
            candidate.sequence = selected.sequence;
            candidate.position = RowPosition(
                selected.block->page_id, selected.block->SlotAt(selected.row));
            bool cached_payload = true;
            for (size_t c = 0; c < schema_.ColumnCount(); ++c) {
              cached_payload &= store->HasColumn(scan->TableColumnSlot(c));
            }
            if (cached_payload) {
              for (size_t c = 0; c < schema_.ColumnCount(); ++c) {
                candidate.row.values_.push_back(
                    value_at(selected.block->columns[scan->TableColumnSlot(c)],
                             selected.row));
              }
            } else {
              ASSIGN_OR_RETURN(Row, row,
                               scan->ReadFoldedRow(candidate.position));
              candidate.row = std::move(row);
            }
            for (size_t c : direct_columns) {
              candidate.keys.push_back(candidate.row[c]);
            }
            heap.push(std::move(candidate));
          }
          fused_scan_ = true;
        }
      }
    }
  }
  if (!folded && direct_columns.size() == keys_.size()) {
    if (auto scan = std::dynamic_pointer_cast<ParallelScan>(source_)) {
      // Each morsel keeps its best prefix. The union of these prefixes
      // contains the global best prefix, including stable equal-key ties.
      // Charge the geometric heap capacities and decoded position batches
      // before consuming anything; a large LIMIT retains the regular path.
      size_t candidate_bytes = 0, heap_bytes = 0, scan_bytes = 0, bytes = 0;
      const size_t payload =
          sizeof(Candidate) +
          ((schema_.ColumnCount() + keys_.size()) * sizeof(Value));
      if (!__builtin_mul_overflow(capacity, payload * 2, &candidate_bytes) &&
          !__builtin_mul_overflow(scan->MorselCount() + 1, candidate_bytes,
                                  &heap_bytes) &&
          !__builtin_mul_overflow(
              scan->WorkerCount() * kDefaultVectorSize,
              (static_cast<size_t>(schema_.ColumnCount()) * 9) +
                  sizeof(RowPosition),
              &scan_bytes) &&
          !__builtin_add_overflow(heap_bytes, scan_bytes, &bytes) &&
          QueryMemoryBudget::Global().CanReserve(bytes)) {
        QueryMemoryCharge charge(bytes);
        using Heap = decltype(heap);
        std::vector<Heap> partials;
        partials.reserve(scan->MorselCount());
        for (size_t m = 0; m < scan->MorselCount(); ++m) {
          partials.emplace_back(worse_first);
        }
        std::vector<size_t> counts(scan->MorselCount(), 0);
        auto fold = scan->TryFoldTypedMorsels(
            [&](size_t m, const TypedCellBatch& batch) -> Status {
              if (batch.stride != schema_.ColumnCount() ||
                  batch.positions.size() != batch.rows) {
                return StatusError(StatusCode::kRuntimeError,
                                   "typed TopN layout mismatch");
              }
              const auto value_at = [&](size_t batch_row, size_t column) {
                const size_t cell = (batch_row * batch.stride) + column;
                if (batch.nulls[cell]) {
                  return Value();
                }
                Value value;
                if (batch.slot_types[column] == ValueType::kDouble) {
                  value = Value(std::bit_cast<double>(batch.cells[cell]));
                } else {
                  value = Value(batch.cells[cell]);
                  value.type = batch.slot_types[column];
                  if (batch.slot_unsigned[column]) {
                    value = value.WithUnsigned();
                  }
                }
                return value;
              };
              Candidate candidate;
              candidate.keys.resize(keys_.size());
              auto& local = partials[m];
              for (size_t r = 0; r < batch.rows; ++r) {
                candidate.sequence = counts[m]++;
                candidate.keys.resize(keys_.size());
                for (size_t k = 0; k < keys_.size(); ++k) {
                  candidate.keys[k] = value_at(r, direct_columns[k]);
                }
                if (local.size() < capacity ||
                    precedes(candidate, local.top())) {
                  candidate.row.values_.clear();
                  candidate.row.values_.reserve(batch.stride);
                  for (size_t c = 0; c < batch.stride; ++c) {
                    candidate.row.values_.push_back(value_at(r, c));
                  }
                  candidate.position = batch.positions[r];
                  if (local.size() == capacity) {
                    local.pop();
                  }
                  local.push(std::move(candidate));
                  candidate = Candidate{};
                }
              }
              return Status::kSuccess;
            },
            true);
        if (!fold.HasValue()) {
          FailWith(fold.GetStatus());
          return fold.GetStatus();
        }
        folded = fold.Value();
        if (folded) {
          size_t prefix = 0;
          for (size_t m = 0; m < partials.size(); ++m) {
            auto& local = partials[m];
            while (!local.empty()) {
              Candidate candidate =
                  std::move(const_cast<Candidate&>(local.top()));
              local.pop();
              if (__builtin_add_overflow(candidate.sequence, prefix,
                                         &candidate.sequence)) {
                return StatusError(StatusCode::kTooBigData,
                                   "TopN input row count overflow");
              }
              if (heap.size() < capacity || precedes(candidate, heap.top())) {
                if (heap.size() == capacity) {
                  heap.pop();
                }
                heap.push(std::move(candidate));
              }
            }
            if (__builtin_add_overflow(prefix, counts[m], &prefix)) {
              return StatusError(StatusCode::kTooBigData,
                                 "TopN input row count overflow");
            }
          }
          input_rows_ = prefix;
          fused_scan_ = true;
        }
      }
    }
  }

  if (!folded && direct_columns.size() == keys_.size()) {
    DataChunk chunk;
    Candidate candidate;
    while (source_->NextBatch(&chunk) != 0) {
      for (size_t i = 0; i < chunk.Size(); ++i) {
        ++input_rows_;
        candidate.sequence = sequence++;
        candidate.keys.resize(keys_.size());
        for (size_t key = 0; key < keys_.size(); ++key) {
          candidate.keys[key] = chunk.ColumnAt(direct_columns[key]).ValueAt(i);
        }
        // Most rows lose to the boundary. Materialize their payload only
        // when admitted, and reuse key storage on every rejected row.
        if (heap.size() < capacity || precedes(candidate, heap.top())) {
          candidate.row = chunk.RowAt(i);
          candidate.position = chunk.PositionAt(i);
          if (heap.size() == capacity) {
            heap.pop();
          }
          heap.push(std::move(candidate));
          candidate = Candidate{};
        }
      }
    }
  } else if (!folded) {
    DataChunk chunk;
    while (source_->NextBatch(&chunk) != 0) {
      for (size_t i = 0; i < chunk.Size(); ++i) {
        ++input_rows_;
        Candidate candidate{.row = chunk.RowAt(i),
                            .position = chunk.PositionAt(i),
                            .keys = {},
                            .sequence = sequence++};
        candidate.keys.reserve(keys_.size());
        for (const Key& key : keys_) {
          StatusOr<Value> key_value =
              key.expression->TryEvaluate(candidate.row, schema_);
          if (!key_value.HasValue()) {
            const Status key_error = key_value.GetStatus();
            FailWith(key_error);
            return key_error;
          }
          candidate.keys.push_back(key_value.MoveValue());
        }
        if (heap.size() < capacity) {
          heap.push(std::move(candidate));
        } else if (capacity != 0 && precedes(candidate, heap.top())) {
          heap.pop();
          heap.push(std::move(candidate));
        }
      }
    }
  }
  FailWithChildOf(*source_);
  rows_.reserve(heap.size());
  while (!heap.empty()) {
    rows_.push_back(std::move(const_cast<Candidate&>(heap.top())));
    heap.pop();
  }
  std::ranges::sort(rows_, precedes);
  output_index_ = std::min(offset_, rows_.size());
  output_end_ = rows_.size();
  materialized_ = true;
  return GetStatus();
}

size_t TopNExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset(schema_, max_rows);
  if (!materialized_) {
    if (const Status status = Materialize(); status != Status::kSuccess) {
      FailWith(status);
      return 0;
    }
  }
  while (destination->Size() < max_rows && output_index_ < output_end_) {
    destination->Append(std::move(rows_[output_index_].row),
                        rows_[output_index_].position);
    ++output_index_;
  }
  return destination->Size();
}

void TopNExecutor::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent)) << "TopN (limit=" << limit_
         << ", offset=" << offset_ << (with_ties_ ? ", with ties" : "")
         << (fused_scan_ ? ", fused typed scan" : "") << ")\n"
         << Indent(static_cast<size_t>(indent) + 2);
  source_->Dump(output, indent + 2);
  if (materialized_) {
    output << "\n"
           << Indent(static_cast<size_t>(indent))
           << "TopN heap_capacity=" << heap_capacity_
           << " input_rows=" << input_rows_ << " output_rows="
           << (output_end_ - std::min(offset_, rows_.size()));
  }
}

}  // namespace tinylamb
