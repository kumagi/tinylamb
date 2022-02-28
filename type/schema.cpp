#include "schema.hpp"

#include <cstring>
#include <utility>
#include <vector>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/log_message.hpp"
#include "page/row_position.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

Schema::Schema(std::string_view schema_name, std::vector<Column> columns)
    : name_(schema_name), columns_(std::move(columns)) {}

Schema Schema::Extract(const std::vector<size_t>& elms) const {
  std::vector<Column> extracted;
  extracted.reserve(elms.size());
  for (size_t offset : elms) {
    extracted.push_back(columns_[offset]);
  }
  return {name_, std::move(extracted)};
}

Schema Schema::operator+(const Schema& rhs) const {
  std::vector<Column> merged = columns_;
  merged.reserve(columns_.size() + rhs.columns_.size());
  for (const auto& c : rhs.columns_) {
    merged.push_back(c);
  }
  return {"", merged};
}

bool Schema::operator==(const Schema& rhs) const {
  return name_ == rhs.name_ && columns_ == rhs.columns_;
}

std::ostream& operator<<(std::ostream& o, const Schema& s) {
  o << s.name_ << " [ ";
  for (size_t i = 0; i < s.columns_.size(); ++i) {
    if (0 < i) o << " | ";
    o << s.columns_[i];
  }
  o << " ]";
  return o;
}

Encoder& operator<<(Encoder& a, const Schema& sc) {
  a << sc.name_ << sc.columns_;
  return a;
}

Decoder& operator>>(Decoder& e, Schema& sc) {
  e >> sc.name_ >> sc.columns_;
  return e;
}

}  // namespace tinylamb
