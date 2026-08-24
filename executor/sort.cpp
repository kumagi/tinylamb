/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/sort.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <fstream>
#include <ios>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <queue>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/decoder.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

using PositionedRow = std::pair<Row, RowPosition>;

constexpr size_t kMergeWindowRows = 128;
constexpr size_t kParallelSortMinRows = 4096;

uint64_t BSwap64(uint64_t v) { return __builtin_bswap64(v); }

void AppendMemComparableValue(const Value& v, std::string* out) {
  switch (v.type) {
    case ValueType::kInt64:
    case ValueType::kDate: {
      out->push_back(static_cast<char>(v.type));
      const uint64_t be = BSwap64(static_cast<uint64_t>(v.value.int_value));
      char buf[8];
      std::memcpy(buf, &be, sizeof(buf));
      buf[0] ^= static_cast<char>(0x80);
      out->append(buf, sizeof(buf));
      break;
    }
    case ValueType::kVarChar: {
      const std::string_view s(v.value.varchar_value.data(),
                               v.value.varchar_value.size());
      out->push_back(static_cast<char>(ValueType::kVarChar));
      if (s.empty()) {
        out->append(9, '\0');
        break;
      }
      for (size_t i = 0;; i += 8) {
        if (9 <= s.size() - i) {
          out->append(s.substr(i, 8));
          out->push_back('\x09');
        } else {
          const size_t tail = s.size() - i;
          out->append(s.substr(i, tail));
          out->append(8 - tail, '\0');
          out->push_back(
              static_cast<char>((s.size() % 8) + (s.size() % 8 == 0 ? 8 : 0)));
          break;
        }
      }
      break;
    }
    case ValueType::kDouble: {
      out->push_back(static_cast<char>(ValueType::kDouble));
      uint64_t bits = 0;
      std::memcpy(&bits, &v.value.double_value, sizeof(bits));
      uint64_t be = BSwap64(bits);
      if (0 <= v.value.double_value) {
        be |= 0x80;
      } else {
        be = ~be;
      }
      char buf[8];
      std::memcpy(buf, &be, sizeof(buf));
      out->append(buf, sizeof(buf));
      break;
    }
    case ValueType::kNull:
      throw std::runtime_error("Cannot encode unknown type.");
  }
}

using Span = std::pair<uint32_t, uint32_t>;

int CompareSpans(std::string_view a, std::string_view b) {
  const size_t common = std::min(a.size(), b.size());
  const int c = std::memcmp(a.data(), b.data(), common);
  if (c != 0) { return c;
}
  return a.size() < b.size() ? -1 : (a.size() > b.size() ? 1 : 0);
}

class SortKeyEncoder {
 public:
  enum class Kind { kSingleUInt64, kEncoded };

  SortKeyEncoder(const std::vector<SortExecutor::Key>& keys,
                 const Schema& schema)
      : schema_(&schema) {
    specs_.reserve(keys.size());
    for (const SortExecutor::Key& key : keys) {
      Spec spec;
      spec.expr = key.expression;
      spec.ascending = key.ascending;
      if (key.expression->Type() == TypeTag::kColumnValue) {
        const int offset =
            schema.Offset(key.expression->AsColumnValue().GetColumnName());
        if (offset >= 0) {
          const ValueType type =
              schema.GetColumn(static_cast<size_t>(offset)).Type();
          if (type == ValueType::kInt64 || type == ValueType::kDate) {
            spec.column = offset;
          }
        }
      }
      specs_.push_back(spec);
    }
    single_int_ = specs_.size() == 1 && specs_[0].column >= 0;
    kind_ = single_int_ ? Kind::kSingleUInt64 : Kind::kEncoded;
  }

  [[nodiscard]] Kind GetKind() const { return kind_; }
  [[nodiscard]] bool SingleAscending() const { return specs_[0].ascending; }

  uint64_t SingleKey(const Row& row, bool* is_null) const {
    const Value& v = row.values_[static_cast<size_t>(specs_[0].column)];
    if (v.IsNull()) {
      *is_null = true;
      return 0;
    }
    *is_null = false;
    const uint64_t bits = static_cast<uint64_t>(v.value.int_value);
    return specs_[0].ascending ? bits ^ (uint64_t{1} << 63) : ~bits;
  }

  void AppendEncoded(const Row& row, std::string* out) const {
    for (const Spec& spec : specs_) {
      Value evaluated;
      const Value* v = nullptr;
      if (spec.column >= 0) {
        v = &row.values_[static_cast<size_t>(spec.column)];
      } else {
        evaluated = spec.expr->Evaluate(row, *schema_);
        v = &evaluated;
      }
      if (spec.ascending) {
        if (v->IsNull()) {
          out->push_back('\x00');
          continue;
        }
        out->push_back('\x01');
        AppendMemComparableValue(*v, out);
      } else {
        if (v->IsNull()) {
          out->push_back('\xff');
          continue;
        }
        out->push_back('\x00');
        const size_t begin = out->size();
        AppendMemComparableValue(*v, out);
        for (size_t i = begin; i < out->size(); ++i) {
          (*out)[i] = static_cast<char>(~(*out)[i]);
        }
      }
    }
  }

 private:
  struct Spec {
    Expression expr;
    bool ascending{true};
    int column{-1};
  };
  const Schema* schema_;
  std::vector<Spec> specs_;
  bool single_int_{false};
  Kind kind_{Kind::kEncoded};
};

void RadixSortIndices(size_t* begin, size_t* end,
                      const std::vector<uint64_t>& keys,
                      std::vector<size_t>* tmp) {
  const size_t n = static_cast<size_t>(end - begin);
  if (n < 2) { return;
}
  tmp->resize(n);
  size_t* src = begin;
  size_t* dst = tmp->data();
  for (int pass = 0; pass < 8; ++pass) {
    const int shift = pass * 8;
    size_t count[257]{};
    for (size_t i = 0; i < n; ++i) {
      ++count[((keys[src[i]] >> shift) & 0xFF) + 1];
    }
    for (int b = 0; b < 256; ++b) { count[b + 1] += count[b];
}
    for (size_t i = 0; i < n; ++i) {
      dst[count[(keys[src[i]] >> shift) & 0xFF]++] = src[i];
    }
    std::swap(src, dst);
  }
  if (src != begin) {
    std::copy(src, src + n, begin);
  }
}

class KeyOrdering {
 public:
  KeyOrdering(const std::vector<SortExecutor::Key>& keys, const Schema& schema)
      : encoder_(keys, schema) {}

  void AppendRow(const Row& row) {
    if (encoder_.GetKind() == SortKeyEncoder::Kind::kSingleUInt64) {
      bool is_null = false;
      const uint64_t key = encoder_.SingleKey(row, &is_null);
      raw_keys_.push_back(key);
      null_flags_.push_back(is_null);
      return;
    }
    const uint32_t begin = static_cast<uint32_t>(blob_.size());
    encoder_.AppendEncoded(row, &blob_);
    spans_.emplace_back(begin, static_cast<uint32_t>(blob_.size() - begin));
  }

  void AppendEncodedTo(const Row& row, std::string* out) const {
    encoder_.AppendEncoded(row, out);
  }

  void ResetForNewChunk() {
    blob_.clear();
    spans_.clear();
    raw_keys_.clear();
    null_flags_.clear();
  }

  [[nodiscard]] SortKeyEncoder::Kind GetKind() const {
    return encoder_.GetKind();
  }

  std::vector<size_t> BuildPermutation(size_t rows, size_t workers) const {
    std::vector<size_t> perm(rows);
    for (size_t i = 0; i < rows; ++i) { perm[i] = i;
}
    if (rows < 2) { return perm;
}

    size_t sortable_begin = 0;
    size_t sortable_end = rows;
    if (GetKind() == SortKeyEncoder::Kind::kSingleUInt64 &&
        !null_flags_.empty()) {
      std::vector<size_t> nulls;
      std::vector<size_t> non_nulls;
      for (size_t i = 0; i < rows; ++i) {
        (null_flags_[i] ? nulls : non_nulls).push_back(i);
      }
      if (!nulls.empty()) {
        if (encoder_.SingleAscending()) {
          std::copy(nulls.begin(), nulls.end(), perm.begin());
          std::copy(non_nulls.begin(), non_nulls.end(),
                    perm.begin() +
                        static_cast<std::ptrdiff_t>(nulls.size()));
          sortable_begin = nulls.size();
        } else {
          std::copy(non_nulls.begin(), non_nulls.end(), perm.begin());
          std::copy(nulls.begin(), nulls.end(),
                    perm.begin() +
                        static_cast<std::ptrdiff_t>(non_nulls.size()));
          sortable_end = non_nulls.size();
        }
      }
    }

    const size_t sortable_rows = sortable_end - sortable_begin;
    if (sortable_rows < 2) { return perm;
}
    size_t* base = perm.data() + sortable_begin;
    const auto index_less = [&](size_t a, size_t b) {
      if (GetKind() == SortKeyEncoder::Kind::kSingleUInt64) {
        return raw_keys_[a] < raw_keys_[b];
      }
      return CompareSpans(SpanAt(a), SpanAt(b)) < 0;
    };

    const bool parallel = workers > 1 && sortable_rows >= kParallelSortMinRows;
    if (!parallel) {
      if (GetKind() == SortKeyEncoder::Kind::kSingleUInt64) {
        std::vector<size_t> tmp;
        RadixSortIndices(base, base + sortable_rows, raw_keys_, &tmp);
      } else {
        std::stable_sort(base, base + sortable_rows, index_less);
      }
      return perm;
    }

    const size_t run_size = (sortable_rows + workers - 1) / workers;
    std::exception_ptr error;
    std::mutex error_mutex;
    std::vector<std::jthread> threads;
    threads.reserve(workers);
    for (size_t worker = 0; worker < workers; ++worker) {
      const size_t begin = worker * run_size;
      const size_t end = std::min(sortable_rows, begin + run_size);
      if (begin == end) { break;
}
      threads.emplace_back([&, begin, end] {
        try {
          if (GetKind() == SortKeyEncoder::Kind::kSingleUInt64) {
            std::vector<size_t> tmp;
            RadixSortIndices(base + begin, base + end, raw_keys_, &tmp);
          } else {
            std::stable_sort(base + begin, base + end, index_less);
          }
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
    for (size_t width = run_size; width < sortable_rows; width *= 2) {
      for (size_t begin = 0; begin < sortable_rows; begin += width * 2) {
        const size_t middle = std::min(sortable_rows, begin + width);
        const size_t end = std::min(sortable_rows, begin + (width * 2));
        if (middle != end) {
          std::inplace_merge(base + begin, base + middle, base + end,
                             index_less);
        }
      }
      if (width > sortable_rows / 2) { break;
}
    }
    return perm;
  }

 private:
  [[nodiscard]] std::string_view SpanAt(size_t index) const {
    const Span span = spans_[index];
    return std::string_view(blob_.data() + span.first, span.second);
  }

  SortKeyEncoder encoder_;
  std::string blob_;
  std::vector<Span> spans_;
  std::vector<uint64_t> raw_keys_;
  std::vector<uint8_t> null_flags_;
};

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

void ApplyPermutation(std::vector<PositionedRow>* rows,
                      const std::vector<size_t>& permutation) {
  if (permutation.size() != rows->size()) { return;
}
  std::vector<PositionedRow> sorted;
  sorted.reserve(permutation.size());
  for (const size_t index : permutation) {
    sorted.push_back(std::move((*rows)[index]));
  }
  *rows = std::move(sorted);
}

}  // namespace

void SortExecutor::Materialize() {
  KeyOrdering ordering(keys_, schema_);
  std::vector<SpillFile> runs;
  const auto write_sorted_run = [&]() {
    ApplyPermutation(&rows_,
                     ordering.BuildPermutation(rows_.size(), worker_count_));
    SpillFile run;
    for (const auto& item : rows_) {
      run.Append(item.first, item.second);
    }
    run.FinishWriting();
    runs.push_back(std::move(run));
    ordering.ResetForNewChunk();
    rows_.clear();
    rows_.shrink_to_fit();
  };

  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  QueryMemoryCharge charge;
  Row row;
  RowPosition position;
  while (source_->Next(&row, &position)) {
    const size_t bytes = EstimateRowBytes(row) + sizeof(RowPosition);
    const bool spill_now = !budget.CanReserve(bytes);
    if (spill_now && !rows_.empty()) {
      write_sorted_run();
      charge.ReleaseAll();
    }
    charge.Add(bytes);
    ordering.AppendRow(row);
    rows_.emplace_back(std::move(row), position);
  }

  if (runs.empty()) {
    const size_t workers =
        rows_.size() < kParallelSortMinRows
            ? 1
            : std::min(worker_count_, std::max<size_t>(1, rows_.size()));
    ApplyPermutation(&rows_,
                     ordering.BuildPermutation(rows_.size(), workers));
    rows_charge_ = std::move(charge);
  } else {
    if (!rows_.empty()) {
      write_sorted_run();
      charge.ReleaseAll();
    }

    struct MergeWindow {
      QueryMemoryCharge charge;
      std::vector<PositionedRow> rows;
      std::string keys;
      std::vector<std::pair<uint32_t, uint32_t>> spans;
      size_t index{0};

      bool Fill(const KeyOrdering& ord, RunReader* reader) {
        charge.ReleaseAll();
        rows.clear();
        keys.clear();
        spans.clear();
        index = 0;
        PositionedRow item;
        while (rows.size() < kMergeWindowRows && reader->Next(&item)) {
          charge.Add(EstimateRowBytes(item.first) + sizeof(RowPosition));
          const uint32_t begin = static_cast<uint32_t>(keys.size());
          ord.AppendEncodedTo(item.first, &keys);
          spans.emplace_back(begin,
                             static_cast<uint32_t>(keys.size() - begin));
          rows.push_back(std::move(item));
        }
        return !rows.empty();
      }
    };
    struct RunCursor {
      size_t run_id{0};
      std::unique_ptr<RunReader> reader;
      std::optional<MergeWindow> window;

      [[nodiscard]] const PositionedRow& Current() const {
        if (!window || window->index >= window->rows.size()) {
          throw std::runtime_error("merge cursor has no current row");
        }
        return window->rows[window->index];
      }
      bool FillInitial(const KeyOrdering& ord) {
        window.emplace();
        return window->Fill(ord, reader.get());
      }
      bool Advance(const KeyOrdering& ord) {
        if (!window) { return false;
}
        ++window->index;
        if (window->index < window->rows.size()) { return true;
}
        return window->Fill(ord, reader.get());
      }
    };

    std::vector<RunCursor> cursors(runs.size());
    for (size_t i = 0; i < cursors.size(); ++i) {
      cursors[i].run_id = i;
      cursors[i].reader = std::make_unique<RunReader>(runs[i]);
      cursors[i].FillInitial(ordering);
    }
    auto current_key = [&cursors](size_t id) -> std::string_view {
      const MergeWindow& w = *cursors[id].window;
      const auto span = w.spans[w.index];
      return std::string_view(w.keys.data() + span.first, span.second);
    };
    auto cursor_less = [&](size_t a, size_t b) {
      const int c = CompareSpans(current_key(a), current_key(b));
      if (c != 0) { return c > 0;
}
      return cursors[a].run_id > cursors[b].run_id;
    };
    std::priority_queue<size_t, std::vector<size_t>, decltype(cursor_less)>
        heap(cursor_less);
    for (size_t i = 0; i < cursors.size(); ++i) {
      if (cursors[i].window && !cursors[i].window->rows.empty()) {
        heap.push(i);
      }
    }
    QueryMemoryCharge output_charge;
    while (!heap.empty()) {
      const size_t id = heap.top();
      heap.pop();
      RunCursor& cursor = cursors[id];
      if (!cursor.window ||
          cursor.window->index >= cursor.window->rows.size()) {
        continue;
      }
      rows_.push_back(std::move(cursor.window->rows[cursor.window->index]));
      output_charge.Add(EstimateRowBytes(rows_.back().first));
      if (cursor.Advance(ordering)) { heap.push(id);
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
