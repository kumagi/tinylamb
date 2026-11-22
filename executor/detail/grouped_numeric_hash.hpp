/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_GROUPED_NUMERIC_HASH_HPP
#define TINYLAMB_EXECUTOR_DETAIL_GROUPED_NUMERIC_HASH_HPP

#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <vector>

#include "executor/detail/grouped_output_sink.hpp"
#include "executor/detail/relation.hpp"

namespace tinylamb::relational_detail {

// Retained numeric build tuples, populated directly by grouped-output pushes.
// Group indices are published incrementally as rows are pushed. Payloads own no
// Row/Value heap objects. Signed INT64 equality keys only; all other key
// domains decline the enclosing fast path rather than approximate SQL equality.
class GroupedNumericHash {
 public:
  struct Cell {
    uint64_t bits{0};
    ValueType type{ValueType::kNull};
    bool is_unsigned{false};

    [[nodiscard]] Value Get() const {
      if (type == ValueType::kNull) {
        return {};
      }
      if (type == ValueType::kDouble) {
        return Value(std::bit_cast<double>(bits));
      }
      if (type == ValueType::kDate) {
        return Value::DateFromDays(std::bit_cast<int64_t>(bits));
      }
      Value value(std::bit_cast<int64_t>(bits));
      if (is_unsigned) {
        value.SetUnsigned();
      }
      return value;
    }
  };
  static constexpr uint32_t kEnd = std::numeric_limits<uint32_t>::max();

  explicit GroupedNumericHash(std::vector<size_t> keys,
                              bool track_matches = false)
      : keys_(std::move(keys)), track_matches_(track_matches) {}

  GroupedOutputSink Sink() {
    return {.begin = [this](const Schema& schema, size_t rows,
                            size_t) -> StatusOr<bool> {
              try {
                return Begin(schema, rows);
              } catch (const std::bad_alloc&) {
                return Status::kNoSpace;
              }
            },
            .push =
                [this](size_t, size_t first, std::span<const Value> cells) {
                  return Push(first, cells);
                }};
  }

  [[nodiscard]] bool Supported() const {
    return supported_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] const Schema& OutputSchema() const { return schema_; }
  [[nodiscard]] size_t Width() const { return width_; }
  [[nodiscard]] size_t Size() const { return next_.size(); }
  [[nodiscard]] bool TrackMatches() const { return track_matches_; }
  [[nodiscard]] bool IsMatched(size_t row_index) const {
    return !matched_.empty() &&
           std::atomic_ref<const uint8_t>(matched_[row_index])
                   .load(std::memory_order_acquire) != 0;
  }
  [[nodiscard]] std::span<const Cell> RowCells(size_t row_index) const {
    return {cells_.data() + (row_index * width_), width_};
  }
  [[nodiscard]] const std::vector<size_t>& KeyColumns() const { return keys_; }

  // Called after all build writers finish; excludes keys that cannot match.
  template <typename Fn>
  void ForEachKey(Fn&& consume) const {
    if (!Supported() || keys_.size() > 4) {
      return;
    }
    std::array<int64_t, 4> key{};
    for (size_t r = 0; r < Size(); ++r) {
      bool valid = true;
      for (size_t k = 0; k < keys_.size(); ++k) {
        const Cell& cell = cells_[(r * width_) + keys_[k]];
        valid &= cell.type == ValueType::kInt64 && !cell.is_unsigned;
        key[k] = std::bit_cast<int64_t>(cell.bits);
      }
      if (valid) {
        consume(std::span<const int64_t>(key).first(keys_.size()));
      }
    }
  }

  template <typename Fn>
  void Matches(std::span<const Value> probe, std::span<const size_t> keys,
               Fn&& consume) const {
    if (!Supported() || keys.size() != keys_.size()) {
      return;
    }
    for (size_t key : keys) {
      if (key >= probe.size() || probe[key].IsNull() ||
          probe[key].type != ValueType::kInt64 || probe[key].IsUnsigned()) {
        return;
      }
    }
    size_t hash = Hash(probe, keys);
    uint32_t at = std::atomic_ref<const uint32_t>(heads_[hash & mask_])
                      .load(std::memory_order_relaxed);
    while (at != kEnd) {
      const Cell* row = cells_.data() + (static_cast<size_t>(at) * width_);
      bool equal = true;
      for (size_t k = 0; k < keys.size(); ++k) {
        equal &= std::bit_cast<int64_t>(row[keys_[k]].bits) ==
                 probe[keys[k]].value.int_value;
      }
      if (equal) {
        if (track_matches_) {
          // One atomic RMW: another matcher may set the same bit between our
          // load and store, and a plain overwrite would lose that update.
          // acq_rel also publishes the build-side writes to a reader that
          // observes the flag (IsMatched loads with acquire).
          std::atomic_ref<uint8_t> matched_ref(matched_[at]);
          matched_ref.fetch_or(1, std::memory_order_acq_rel);
        }
        consume(std::span<const Cell>(row, width_));
      }
      at = next_[at];
    }
  }

 private:
  static size_t Hash(std::span<const Value> cells,
                     std::span<const size_t> keys) {
    uint64_t hash = 0x9e3779b97f4a7c15ULL;
    for (size_t key : keys) {
      auto value = static_cast<uint64_t>(cells[key].value.int_value);
      value ^= value >> 30;
      value *= 0xbf58476d1ce4e5b9ULL;
      value ^= value >> 27;
      value *= 0x94d049bb133111ebULL;
      value ^= value >> 31;
      hash = (hash ^ value) * 0x100000001b3ULL;
    }
    return static_cast<size_t>(hash);
  }

  StatusOr<bool> Begin(const Schema& schema, size_t rows) {
    width_ = schema.ColumnCount();
    if (width_ == 0 || width_ > 32 || rows >= kEnd || keys_.empty()) {
      return false;
    }
    if (rows > (std::numeric_limits<size_t>::max() - 32) /
                   ((width_ * sizeof(Cell)) + 20)) {
      return false;
    }
    for (size_t key : keys_) {
      if (key >= width_) {
        return false;
      }
    }
    size_t buckets = 8;
    while (buckets < rows * 2) {
      buckets *= 2;
    }
    const size_t bytes = (rows * ((width_ * sizeof(Cell)) + sizeof(uint32_t) +
                                  (track_matches_ ? sizeof(uint8_t) : 0))) +
                         (buckets * sizeof(uint32_t));
    charge_ = TryReserveQueryMemory(bytes);
    if (!charge_) {
      return false;
    }
    schema_ = schema;
    cells_.resize(rows * width_);
    next_.resize(rows, kEnd);
    if (track_matches_) {
      matched_.assign(rows, 0);
    } else {
      matched_.clear();
    }
    heads_.assign(buckets, kEnd);
    mask_ = buckets - 1;
    return true;
  }

  Status Push(size_t first, std::span<const Value> cells) {
    if (cells.size() % width_ != 0 || first > Size() ||
        cells.size() / width_ > Size() - first) {
      return Status::kInvalidArgument;
    }
    for (size_t r = 0; r < cells.size() / width_; ++r) {
      const auto input = cells.subspan(r * width_, width_);
      bool valid = true;
      bool null_key = false;
      for (size_t k : keys_) {
        null_key |= input[k].IsNull();
        valid &= input[k].IsNull() ||
                 (input[k].type == ValueType::kInt64 && !input[k].IsUnsigned());
      }
      const size_t index = first + r;
      for (size_t c = 0; c < width_; ++c) {
        const Value& v = input[c];
        Cell& cell = cells_[(index * width_) + c];
        cell.type = v.type;
        cell.is_unsigned = v.IsUnsigned();
        if (v.IsNull()) {
          continue;
        }
        if (v.type == ValueType::kDouble) {
          cell.bits = std::bit_cast<uint64_t>(v.value.double_value);
        } else if (v.type == ValueType::kInt64) {
          cell.bits = static_cast<uint64_t>(v.value.int_value);
        } else {
          valid = false;
        }
      }
      if (!valid) {
        supported_.store(false, std::memory_order_relaxed);
        continue;
      }
      if (null_key) {
        continue;
      }
      std::atomic_ref<uint32_t> head(heads_[Hash(input, keys_) & mask_]);
      uint32_t previous = head.load(std::memory_order_relaxed);
      do {
        next_[index] = previous;
      } while (!head.compare_exchange_weak(
          previous, static_cast<uint32_t>(index), std::memory_order_relaxed));
    }
    return Status::kSuccess;
  }

  // Charge dies last, after the buffers it accounts for.
  std::shared_ptr<QueryMemoryCharge> charge_;
  Schema schema_;
  std::vector<size_t> keys_;
  size_t width_{0};
  size_t mask_{0};
  std::vector<Cell> cells_;
  std::vector<uint32_t> next_;
  std::vector<uint32_t> heads_;
  bool track_matches_{false};
  mutable std::vector<uint8_t> matched_;
  std::atomic<bool> supported_{true};
};

}  // namespace tinylamb::relational_detail
#endif
