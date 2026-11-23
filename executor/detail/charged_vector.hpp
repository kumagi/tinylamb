/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_CHARGED_VECTOR_HPP
#define TINYLAMB_EXECUTOR_DETAIL_CHARGED_VECTOR_HPP

#include <iterator>
#include <limits>
#include <utility>
#include <vector>

namespace tinylamb::relational_detail {

// Reserve the new allocation while the old allocation is still live.  Retain
// the charge alongside the vector across shards, including after clear().
template <typename T, typename Charge>
bool ReserveChargedVector(std::vector<T>& values, Charge& charge,
                          size_t capacity) {
  if (capacity <= values.capacity()) {
    if (values.capacity() >
        std::numeric_limits<size_t>::max() / sizeof(T)) {
      return false;
    }
    const size_t allocated_bytes = values.capacity() * sizeof(T);
    const size_t charged_bytes = charge.Bytes();
    return allocated_bytes <= charged_bytes ||
           charge.TryAdd(allocated_bytes - charged_bytes);
  }
  if (capacity > values.max_size() ||
      capacity > std::numeric_limits<size_t>::max() / sizeof(T)) {
    return false;
  }
  Charge replacement_charge;
  const size_t requested_bytes = capacity * sizeof(T);
  if (!replacement_charge.TryAdd(requested_bytes)) {
    return false;
  }
  std::vector<T> replacement;
  replacement.reserve(capacity);
  const size_t allocated_capacity = replacement.capacity();
  if (allocated_capacity > std::numeric_limits<size_t>::max() / sizeof(T)) {
    return false;
  }
  const size_t allocated_bytes = allocated_capacity * sizeof(T);
  if (allocated_bytes > requested_bytes &&
      !replacement_charge.TryAdd(allocated_bytes - requested_bytes)) {
    return false;
  }
  try {
    replacement.insert(replacement.end(),
                       std::make_move_iterator(values.begin()),
                       std::make_move_iterator(values.end()));
  } catch (...) {
    // Move-constructing T can throw after a prefix of `values` was already
    // moved into `replacement`. Move that prefix back so `values` stays
    // whole — callers see a failed reserve, never silently moved-from data.
    const size_t moved = replacement.size();
    for (size_t i = 0; i < moved; ++i) {
      values[i] = std::move(replacement[i]);
    }
    throw;
  }
  values.swap(replacement);
  // Destroy the old allocation before refunding it.
  std::vector<T>().swap(replacement);
  charge = std::move(replacement_charge);
  return true;
}

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_CHARGED_VECTOR_HPP
