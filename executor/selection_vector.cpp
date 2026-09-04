/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/selection_vector.hpp"

#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <stdexcept>
#include <utility>
#include <vector>

namespace tinylamb {

ValidityBitmap::ValidityBitmap(size_t count, bool default_val) : size_(count) {
  Reset(count, default_val);
}

ValidityBitmap::ValidityBitmap(std::vector<uint64_t> words, size_t count)
    : words_(std::move(words)), size_(count) {
  MaskTrailingBits();
}

void ValidityBitmap::Reset(size_t count, bool default_val) {
  size_ = count;
  const size_t word_count = (count + 63) / 64;
  words_.assign(word_count, default_val ? ~uint64_t{0} : uint64_t{0});
  MaskTrailingBits();
}

void ValidityBitmap::Resize(size_t count, bool default_val) {
  const size_t old_size = size_;
  const size_t old_word_count = words_.size();
  const size_t new_word_count = (count + 63) / 64;
  size_ = count;
  words_.resize(new_word_count, default_val ? ~uint64_t{0} : uint64_t{0});
  if (count > old_size && default_val && old_word_count > 0) {
    // If the last word of old_size had unused bits masked out, fill them.
    const size_t remainder = old_size % 64;
    if (remainder != 0) {
      words_[old_word_count - 1] |= (~uint64_t{0}) << remainder;
    }
  }
  MaskTrailingBits();
}

void ValidityBitmap::Clear() {
  words_.clear();
  size_ = 0;
}

void ValidityBitmap::MaskTrailingBits() {
  if (size_ == 0 || words_.empty()) {
    return;
  }
  const size_t remainder = size_ % 64;
  if (remainder != 0) {
    const uint64_t mask = (1ULL << remainder) - 1ULL;
    words_.back() &= mask;
  }
}

size_t ValidityBitmap::CountValid() const {
  size_t count = 0;
  for (uint64_t w : words_) {
    count += static_cast<size_t>(std::popcount(w));
  }
  return count;
}

ValidityBitmap ValidityBitmap::BitwiseAnd(const ValidityBitmap& other) const {
  const size_t min_size = std::min(size_, other.size_);
  const size_t word_count = (min_size + 63) / 64;
  std::vector<uint64_t> result(word_count);
  for (size_t i = 0; i < word_count; ++i) {
    result[i] = words_[i] & other.words_[i];
  }
  ValidityBitmap res(std::move(result), min_size);
  return res;
}

ValidityBitmap ValidityBitmap::BitwiseOr(const ValidityBitmap& other) const {
  const size_t max_size = std::max(size_, other.size_);
  const size_t word_count = (max_size + 63) / 64;
  std::vector<uint64_t> result(word_count, 0);
  for (size_t i = 0; i < word_count; ++i) {
    const uint64_t w1 = i < words_.size() ? words_[i] : 0;
    const uint64_t w2 = i < other.words_.size() ? other.words_[i] : 0;
    result[i] = w1 | w2;
  }
  ValidityBitmap res(std::move(result), max_size);
  return res;
}

ValidityBitmap ValidityBitmap::BitwiseNot() const {
  std::vector<uint64_t> result(words_.size());
  for (size_t i = 0; i < words_.size(); ++i) {
    result[i] = ~words_[i];
  }
  ValidityBitmap res(std::move(result), size_);
  return res;
}

ValidityBitmap& ValidityBitmap::operator&=(const ValidityBitmap& other) {
  *this = BitwiseAnd(other);
  return *this;
}

ValidityBitmap& ValidityBitmap::operator|=(const ValidityBitmap& other) {
  *this = BitwiseOr(other);
  return *this;
}

void ValidityBitmap::ToSelectionVector(SelectionVector* sel) const {
  assert(sel != nullptr);
  sel->Clear();
  sel->Reserve(CountValid());
  for (size_t word_idx = 0; word_idx < words_.size(); ++word_idx) {
    uint64_t w = words_[word_idx];
    size_t base = word_idx * 64;
    while (w != 0) {
      const int bit = std::countr_zero(w);
      const size_t index = base + static_cast<size_t>(bit);
      if (index < size_) {
        sel->PushBack(static_cast<uint32_t>(index));
      }
      w &= w - 1;  // Clear lowest set bit
    }
  }
}

SelectionVector::SelectionVector(size_t capacity) {
  indices_.reserve(capacity);
}

SelectionVector::SelectionVector(std::vector<uint32_t> indices)
    : indices_(std::move(indices)) {}

void SelectionVector::Initialize(size_t count) {
  indices_.resize(count);
  std::iota(indices_.begin(), indices_.end(), 0U);
}

void SelectionVector::Filter(const ValidityBitmap& validity,
                             SelectionVector* output) const {
  assert(output != nullptr);
  output->Clear();
  output->Reserve(indices_.size());
  for (uint32_t idx : indices_) {
    if (idx < validity.Size() && validity.Get(idx)) {
      output->PushBack(idx);
    }
  }
}

void SelectionVector::Filter(const std::function<bool(uint32_t)>& pred,
                             SelectionVector* output) const {
  assert(output != nullptr);
  output->Clear();
  output->Reserve(indices_.size());
  for (uint32_t idx : indices_) {
    if (pred(idx)) {
      output->PushBack(idx);
    }
  }
}

SelectionVector SelectionVector::Slice(size_t offset, size_t count) const {
  if (offset >= indices_.size()) {
    return SelectionVector{};
  }
  const size_t actual_count = std::min(count, indices_.size() - offset);
  std::vector<uint32_t> sliced(indices_.begin() + offset,
                               indices_.begin() + offset + actual_count);
  return SelectionVector(std::move(sliced));
}

}  // namespace tinylamb
