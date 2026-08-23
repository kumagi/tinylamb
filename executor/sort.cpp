/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/sort.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <fstream>
#include <ios>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <queue>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "common/decoder.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "type/row.hpp"
#include "page/row_position.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

using PositionedRow = std::pair<Row, RowPosition>;

void SortRange(std::vector<PositionedRow>* rows, size_t begin, size_t end,
               const auto& less) {
  std::stable_sort(rows->begin() + static_cast<std::ptrdiff_t>(begin),
                   rows->begin() + static_cast<std::ptrdiff_t>(end), less);
}

// Rows held per spilled-run cursor during the merge.  Bounds merge memory to
// cursors x window instead of reloading every run in full.
constexpr size_t kMergeWindowRows = 128;

// Sequential positioned-row reader over a finished spill run.  Decodes rows
// on demand so the merge never buffers an entire run in memory.
class RunReader {
 public:
  explicit RunReader(const SpillFile& run)
      : stream_(run.Path(), std::ios::binary | std::ios::in),
        remaining_(run.Count()) {
    if (!stream_) {
      throw std::runtime_error("failed to open spill run: " +
                               run.Path().string());
    }
    uint64_t stored = 0;
    stream_.read(reinterpret_cast<char*>(&stored), sizeof(stored));
    if (stream_.gcount() != static_cast<std::streamsize>(sizeof(stored)) ||
        stored != remaining_) {
      throw std::runtime_error("spill run header mismatch: " +
                               run.Path().string());
    }
  }

  bool Next(PositionedRow* dst) {
    if (remaining_ == 0) { return false;
}
    Row row;
    RowPosition position;
    Decoder dec(stream_);
    dec >> row >> position;
    if (!stream_) {
      throw std::runtime_error("truncated spill run");
    }
    --remaining_;
    *dst = PositionedRow(std::move(row), position);
    return true;
  }

 private:
  std::ifstream stream_;
  uint64_t remaining_{0};
};

}  // namespace

void SortExecutor::Materialize() {
  auto less = [&](const PositionedRow& left, const PositionedRow& right) {
    for (const Key& key : keys_) {
      const Value a = key.expression->Evaluate(left.first, schema_);
      const Value b = key.expression->Evaluate(right.first, schema_);
      if (a == b) { continue;
}
      if (a.IsNull()) { return key.ascending;
}
      if (b.IsNull()) { return !key.ascending;
}
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
        std::ranges::stable_sort(rows_, less);
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
      if (begin == end) { break;
}
      threads.emplace_back([&, begin, end] {
        try {
          SortRange(&rows_, begin, end, less);
        } catch (...) {
          std::scoped_lock lock(error_mutex);
          if (!error) { error = std::current_exception();
}
        }
      });
    }
    threads.clear();
    if (error) { std::rethrow_exception(error);
}
    for (size_t width = run_size; width < rows_.size(); width *= 2) {
      for (size_t begin = 0; begin < rows_.size(); begin += width * 2) {
        const size_t middle = std::min(rows_.size(), begin + width);
        const size_t end = std::min(rows_.size(), begin + (width * 2));
        if (middle != end) {
          std::inplace_merge(
              rows_.begin() + static_cast<std::ptrdiff_t>(begin),
              rows_.begin() + static_cast<std::ptrdiff_t>(middle),
              rows_.begin() + static_cast<std::ptrdiff_t>(end), less);
        }
      }
      if (width > rows_.size() / 2) { break;
}
    }
    rows_charge_ = std::move(charge);
  } else {
    if (!rows_.empty()) {
      std::ranges::stable_sort(rows_, less);
      SpillFile run;
      for (const auto& item : rows_) {
        run.Append(item.first, item.second);
      }
      run.FinishWriting();
      runs.push_back(std::move(run));
      charge.ReleaseAll();
      rows_.clear();
    }

    // Streaming k-way merge over the spilled runs.  Each cursor holds only a
    // bounded window of decoded rows (charged against the query memory
    // budget and released as it drains), so peak merge memory no longer
    // scales with the full dataset.  Output growth into rows_ is charged as
    // it happens.
    struct MergeWindow {
      QueryMemoryCharge charge;
      std::vector<PositionedRow> rows;
      size_t index{0};
    };
    struct RunCursor {
      size_t run_id{0};
      std::unique_ptr<RunReader> reader;
      std::optional<MergeWindow> window;

      [[nodiscard]] const PositionedRow& Current() const {
        // Cursors enter the merge heap only with a filled window; the guard
        // keeps that contract explicit instead of dereferencing unchecked.
        if (!window || window->index >= window->rows.size()) {
          throw std::runtime_error("merge cursor has no current row");
        }
        return window->rows[window->index];
      }
      bool Fill() {
        window.reset();
        MergeWindow next;
        PositionedRow item;
        while (next.rows.size() < kMergeWindowRows && reader->Next(&item)) {
          next.charge.Add(EstimateRowBytes(item.first) +
                          sizeof(RowPosition));
          next.rows.push_back(std::move(item));
        }
        if (next.rows.empty()) { return false;
}
        window.emplace(std::move(next));
        return true;
      }
      bool Advance() {
        if (!window) { return false;
}
        ++window->index;
        if (window->index < window->rows.size()) { return true;
}
        return Fill();
      }
    };

    std::vector<RunCursor> cursors(runs.size());
    for (size_t i = 0; i < cursors.size(); ++i) {
      cursors[i].run_id = i;
      cursors[i].reader = std::make_unique<RunReader>(runs[i]);
      cursors[i].Fill();
    }
    auto cursor_less = [&](size_t a, size_t b) {
      const PositionedRow& ra = cursors[a].Current();
      const PositionedRow& rb = cursors[b].Current();
      if (less(ra, rb)) { return false;  // a sorts before b: keep a on top
}
      if (less(rb, ra)) { return true;
}
      // Equal keys: break ties by run id so the merged output keeps the same
      // stable order the in-memory path guarantees.
      return cursors[a].run_id > cursors[b].run_id;
    };
    std::priority_queue<size_t, std::vector<size_t>, decltype(cursor_less)> heap(
        cursor_less);
    for (size_t i = 0; i < cursors.size(); ++i) {
      if (cursors[i].window) { heap.push(i);
}
    }
    QueryMemoryCharge output_charge;
    while (!heap.empty()) {
      const size_t id = heap.top();
      heap.pop();
      RunCursor& cursor = cursors[id];
      if (!cursor.window) { continue; }
      rows_.push_back(std::move(cursor.window->rows[cursor.window->index]));
      output_charge.Add(EstimateRowBytes(rows_.back().first));
      if (cursor.Advance()) { heap.push(id);
}
    }
    charge.ReleaseAll();
    rows_charge_ = std::move(output_charge);
  }

  materialized_ = true;
}

bool SortExecutor::Next(Row* dst, RowPosition* rp) {
  if (!materialized_) { Materialize();
}
  if (offset_ >= rows_.size()) { return false;
}
  *dst = std::move(rows_[offset_].first);
  if (rp != nullptr) { *rp = rows_[offset_].second;
}
  ++offset_;
  return true;
}

void SortExecutor::Dump(std::ostream& output, int indent) const {
  output << "ParallelSort (" << worker_count_ << " workers)\n"
         << std::string(indent + 2, ' ');
  source_->Dump(output, indent + 2);
}

}  // namespace tinylamb
