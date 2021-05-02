//
// Created by kumagi on 22/07/10.
//

#ifndef TINYLAMB_CONVERTER_HPP
#define TINYLAMB_CONVERTER_HPP

#include <unordered_set>
#include <vector>

namespace tinylamb {

template <typename T>
std::unordered_set<T> MakeSet(const std::vector<T>& from) {
  std::unordered_set<T> joined;
  for (const auto& f : from) {
    joined.insert(f);
  }
  return joined;
}

template <typename T>
std::unordered_set<T> Merge(const std::unordered_set<T>& a,  // NOLINT
                            const std::unordered_set<T>& b) {
  std::unordered_set<T> merged = a;
  for (const auto& f : b) {
    merged.insert(f);
  }
  return merged;
}

}  // namespace tinylamb

#endif  // TINYLAMB_CONVERTER_HPP
