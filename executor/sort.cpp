/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/sort.hpp"

#include <algorithm>
#include <exception>
#include <mutex>
#include <ostream>
#include <queue>
#include <thread>
#include <utility>
#include <vector>

#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

using PositionedRow = std::pair<Row, RowPosition>;

void SortRange(std::vector<PositionedRow>* rows, size_t begin, size_t end,
               const auto& less) {
  std::stable_sort(rows->begin() + static_cast<std::ptrdiff_t>(begin),
                   rows->begin() + static_cast<std::ptrdiff_t>(end), less);
}

}  // namespace

void SortExecutor::Materialize() {
  auto less = [&](const PositionedRow& left, const PositionedRow& right) {
    for (const Key& key : keys_) {
      const Value a = key.expression->Evaluate(left.first, schema_);
      const Value b = key.expression->Evaluate(right.first, schema_);
      if (a == b) continue;
      if (a.IsNull()) return key.ascending;
      if (b.IsNull()) return !key.ascending;
      return key.ascending ? a < b : b < a;
    }
    return false;
  };

  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  QueryMemoryCharge charge;
  std::vector<SpillFile> runs;
  Row row;
  RowPosition position;
  while (source_->Next(&row, &position)) {
    const size_t bytes = EstimateRowBytes(row) + sizeof(RowPosition);
    if (!budget.CanReserve(bytes)) {
      if (!rows_.empty()) {
        std::stable_sort(rows_.begin(), rows_.end(), less);
        SpillFile run;
        for (const auto& item : rows_) {
          run.Append(item.first, item.second);
        }
        run.FinishWriting();
        runs.push_back(std::move(run));
        charge.ReleaseAll();
        rows_.clear();
        rows_.shrink_to_fit();
      }
      charge.Add(bytes);
      rows_.emplace_back(std::move(row), position);
      continue;
    }
    charge.Add(bytes);
    rows_.emplace_back(std::move(row), position);
  }

  if (runs.empty()) {
    const size_t workers =
        rows_.size() < 4096
            ? 1
            : std::min(worker_count_, std::max<size_t>(1, rows_.size()));
    const size_t run_size = (rows_.size() + workers - 1) / workers;
    std::exception_ptr error;
    std::mutex error_mutex;
    std::vector<std::jthread> threads;
    threads.reserve(workers);
    for (size_t worker = 0; worker < workers; ++worker) {
      const size_t begin = worker * run_size;
      const size_t end = std::min(rows_.size(), begin + run_size);
      if (begin == end) break;
      threads.emplace_back([&, begin, end] {
        try {
          SortRange(&rows_, begin, end, less);
        } catch (...) {
          std::scoped_lock lock(error_mutex);
          if (!error) error = std::current_exception();
        }
      });
    }
    threads.clear();
    if (error) std::rethrow_exception(error);
    for (size_t width = run_size; width < rows_.size(); width *= 2) {
      for (size_t begin = 0; begin < rows_.size(); begin += width * 2) {
        const size_t middle = std::min(rows_.size(), begin + width);
        const size_t end = std::min(rows_.size(), begin + width * 2);
        if (middle != end) {
          std::inplace_merge(
              rows_.begin() + static_cast<std::ptrdiff_t>(begin),
              rows_.begin() + static_cast<std::ptrdiff_t>(middle),
              rows_.begin() + static_cast<std::ptrdiff_t>(end), less);
        }
      }
      if (width > rows_.size() / 2) break;
    }
  } else {
    if (!rows_.empty()) {
      std::stable_sort(rows_.begin(), rows_.end(), less);
      SpillFile run;
      for (const auto& item : rows_) {
        run.Append(item.first, item.second);
      }
      run.FinishWriting();
      runs.push_back(std::move(run));
      charge.ReleaseAll();
      rows_.clear();
    }

    struct Cursor {
      std::vector<PositionedRow> data;
      size_t index{0};
      size_t run_id{0};
    };
    std::vector<Cursor> cursors(runs.size());
    for (size_t i = 0; i < runs.size(); ++i) {
      cursors[i].data = runs[i].ReadAllPositioned();
      cursors[i].run_id = i;
    }
    auto cursor_less = [&](size_t a, size_t b) {
      return less(cursors[b].data[cursors[b].index],
                  cursors[a].data[cursors[a].index]);
    };
    std::priority_queue<size_t, std::vector<size_t>, decltype(cursor_less)> heap(
        cursor_less);
    for (size_t i = 0; i < cursors.size(); ++i) {
      if (!cursors[i].data.empty()) heap.push(i);
    }
    while (!heap.empty()) {
      const size_t id = heap.top();
      heap.pop();
      rows_.push_back(std::move(cursors[id].data[cursors[id].index]));
      ++cursors[id].index;
      if (cursors[id].index < cursors[id].data.size()) {
        heap.push(id);
      }
    }
    size_t output_bytes = 0;
    for (const auto& item : rows_) {
      output_bytes += EstimateRowBytes(item.first);
    }
    charge = QueryMemoryCharge(output_bytes);
  }

  rows_charge_ = std::move(charge);
  materialized_ = true;
}

bool SortExecutor::Next(Row* dst, RowPosition* rp) {
  if (!materialized_) Materialize();
  if (offset_ >= rows_.size()) return false;
  *dst = rows_[offset_].first;
  if (rp != nullptr) *rp = rows_[offset_].second;
  ++offset_;
  return true;
}

void SortExecutor::Dump(std::ostream& output, int indent) const {
  output << "ParallelSort (" << worker_count_ << " workers)\n"
         << std::string(indent + 2, ' ');
  source_->Dump(output, indent + 2);
}

}  // namespace tinylamb
