//
// Created by kumagi on 22/05/05.
//

#ifndef TINYLAMB_INDEX_SCHEMA_HPP
#define TINYLAMB_INDEX_SCHEMA_HPP

#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/serdes.hpp"

namespace tinylamb {
struct Row;
class Encoder;
class Decoder;

class IndexSchema {
 public:
  IndexSchema() = default;
  [[nodiscard]] std::string GenerateKey(const Row& row) const;
  IndexSchema(std::string_view name, std::vector<size_t> key)
      : name_(name), key_(std::move(key)) {}
  friend Encoder& operator<<(Encoder& a, const IndexSchema& idx);
  friend Decoder& operator>>(Decoder& e, IndexSchema& idx);
  bool operator==(const IndexSchema& rhs) const = default;

  std::string name_;
  std::vector<size_t> key_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_SCHEMA_HPP
