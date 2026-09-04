/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_SELECTION_VECTOR_HPP
#define TINYLAMB_EXECUTOR_SELECTION_VECTOR_HPP

#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

namespace tinylamb {

class SelectionVector;

// Validity / boolean bitmap packed into 64-bit words for high-performance
// vectorized operations.
class ValidityBitmap {
 public:
  ValidityBitmap() = default;
  explicit ValidityBitmap(size_t count, bool default_val = true);
  explicit ValidityBitmap(std::vector<uint64_t> words, size_t count);

  void Reset(size_t count, bool default_val = true);
  void Resize(size_t count, bool default_val = true);
  void Clear();

  [[nodiscard]] size_t Size() const { return size_; }
  [[nodiscard]] bool Empty() const { return size_ == 0; }
  [[nodiscard]] size_t WordCount() const { return words_.size(); }

  [[nodiscard]] bool Get(size_t index) const {
    assert(index < size_ && "ValidityBitmap index out of range");
    return (words_[index / 64] & (1ULL << (index % 64))) != 0;
  }

  [[nodiscard]] bool operator[](size_t index) const { return Get(index); }

  void Set(size_t index, bool val) {
    assert(index < size_ && "ValidityBitmap index out of range");
    if (val) {
      words_[index / 64] |= (1ULL << (index % 64));
    } else {
      words_[index / 64] &= ~(1ULL << (index % 64));
    }
  }

  void SetBit(size_t index) {
    assert(index < size_ && "ValidityBitmap index out of range");
    words_[index / 64] |= (1ULL << (index % 64));
  }

  void ClearBit(size_t index) {
    assert(index < size_ && "ValidityBitmap index out of range");
    words_[index / 64] &= ~(1ULL << (index % 64));
  }

  [[nodiscard]] size_t CountValid() const;
  [[nodiscard]] size_t CountTrue() const { return CountValid(); }
  [[nodiscard]] size_t CountFalse() const { return size_ - CountValid(); }
  [[nodiscard]] bool AllValid() const { return CountValid() == size_; }
  [[nodiscard]] bool NoneValid() const { return CountValid() == 0; }

  [[nodiscard]] const std::vector<uint64_t>& Words() const { return words_; }
  [[nodiscard]] std::vector<uint64_t>& Words() { return words_; }

  [[nodiscard]] ValidityBitmap BitwiseAnd(const ValidityBitmap& other) const;
  [[nodiscard]] ValidityBitmap BitwiseOr(const ValidityBitmap& other) const;
  [[nodiscard]] ValidityBitmap BitwiseNot() const;

  ValidityBitmap& operator&=(const ValidityBitmap& other);
  ValidityBitmap& operator|=(const ValidityBitmap& other);
  [[nodiscard]] ValidityBitmap operator~() const { return BitwiseNot(); }

  void ToSelectionVector(SelectionVector* sel) const;

 private:
  void MaskTrailingBits();

  std::vector<uint64_t> words_;
  size_t size_{0};
};

// Selection vector holding indices of active rows in a batch / DataChunk,
// avoiding row-by-row memory copying.
class SelectionVector {
 public:
  SelectionVector() = default;
  explicit SelectionVector(size_t capacity);
  explicit SelectionVector(std::vector<uint32_t> indices);

  void Initialize(size_t count);
  void Clear() { indices_.clear(); }
  void Reserve(size_t capacity) { indices_.reserve(capacity); }
  void Resize(size_t size) { indices_.resize(size); }
  void PushBack(uint32_t index) { indices_.push_back(index); }

  [[nodiscard]] size_t Size() const { return indices_.size(); }
  [[nodiscard]] bool Empty() const { return indices_.empty(); }
  [[nodiscard]] size_t Capacity() const { return indices_.capacity(); }

  [[nodiscard]] uint32_t operator[](size_t index) const {
    assert(index < indices_.size() && "SelectionVector index out of range");
    return indices_[index];
  }

  [[nodiscard]] uint32_t& operator[](size_t index) {
    assert(index < indices_.size() && "SelectionVector index out of range");
    return indices_[index];
  }

  [[nodiscard]] uint32_t Get(size_t index) const { return (*this)[index]; }
  void Set(size_t index, uint32_t row_idx) { (*this)[index] = row_idx; }

  [[nodiscard]] const uint32_t* Data() const { return indices_.data(); }
  [[nodiscard]] uint32_t* Data() { return indices_.data(); }
  [[nodiscard]] const std::vector<uint32_t>& Indices() const {
    return indices_;
  }

  void Filter(const ValidityBitmap& validity, SelectionVector* output) const;
  void Filter(const std::function<bool(uint32_t)>& pred,
              SelectionVector* output) const;

  [[nodiscard]] SelectionVector Slice(size_t offset, size_t count) const;

 private:
  std::vector<uint32_t> indices_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_SELECTION_VECTOR_HPP
