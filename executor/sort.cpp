/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/sort.hpp"

#include <algorithm>
#include <exception>
#include <mutex>
#include <ostream>
#include <thread>

#include "type/value.hpp"

namespace tinylamb {
void SortExecutor::Materialize() {
  Row row;
  RowPosition position;
  while (source_->Next(&row, &position)) rows_.emplace_back(row, position);
  const auto less = [&](const auto& left, const auto& right) {
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
  const size_t workers =
      std::min(worker_count_, std::max<size_t>(1, rows_.size()));
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
        std::stable_sort(rows_.begin() + static_cast<std::ptrdiff_t>(begin),
                         rows_.begin() + static_cast<std::ptrdiff_t>(end),
                         less);
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
         << Indent(static_cast<size_t>(indent + 2));
  source_->Dump(output, indent + 2);
}
}  // namespace tinylamb
