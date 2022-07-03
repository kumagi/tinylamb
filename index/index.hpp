//
// Created by kumagi on 2022/02/06.
//

#ifndef TINYLAMB_INDEX_HPP
#define TINYLAMB_INDEX_HPP

#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/serdes.hpp"
#include "index/index_schema.hpp"

namespace tinylamb {
struct Row;
class Encoder;
class Decoder;

class Index {
 public:
  Index() = default;
  [[nodiscard]] std::string GenerateKey(const Row& row) const;
  Index(std::string_view name, std::vector<slot_t> key, page_id_t pid,
        std::vector<slot_t> include = {}, IndexMode mode = IndexMode::kUnique)
      : sc_(name, std::move(key), std::move(include), mode), pid_(pid) {}
  [[nodiscard]] bool IsUnique() const { return sc_.IsUnique(); }
  [[nodiscard]] page_id_t Root() const { return pid_; }
  friend Encoder& operator<<(Encoder& a, const Index& idx);
  friend Decoder& operator>>(Decoder& e, Index& idx);
  bool operator==(const Index& rhs) const = default;
  friend std::ostream& operator<<(std::ostream& o, const Index& rhs);

  IndexSchema sc_;
  page_id_t pid_{};
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_HPP
