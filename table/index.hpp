//
// Created by kumagi on 2022/02/06.
//

#ifndef TINYLAMB_INDEX_HPP
#define TINYLAMB_INDEX_HPP

#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/serdes.hpp"

namespace tinylamb {
class Row;
class Encoder;
class Decoder;

class Index {
 public:
  Index() = default;
  [[nodiscard]] std::string GenerateKey(const Row& row) const;
  Index(std::string_view name, std::vector<size_t> key, page_id_t pid)
      : name_(name), key_(std::move(key)), pid_(pid) {}
  friend Encoder& operator<<(Encoder& a, const Index& idx);
  friend Decoder& operator>>(Decoder& e, Index& idx);
  bool operator==(const Index& rhs) const;

  std::string name_;
  std::vector<size_t> key_;
  page_id_t pid_{};
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_HPP
