#include "schema.hpp"

#include <cstring>
#include <utility>
#include <vector>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/log_message.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

Schema::Schema(std::string_view schema_name, std::vector<Column> columns)
    : name_(schema_name), columns_(std::move(columns)) {}

std::unordered_set<std::string> Schema::ColumnSet() const {
  std::unordered_set<std::string> ret;
  ret.reserve(columns_.size());
  for (const auto& c : columns_) {
    ret.emplace(c.Name());
  }
  return ret;
}

int Schema::Offset(std::string_view name) const {
  for (int i = 0; i < static_cast<int>(columns_.size()); ++i) {
    if (columns_[i].Name() == name) {
      return i;
    }
  }
  return -1;
}

Schema Schema::operator+(const Schema& rhs) const {
  std::vector<Column> merged = columns_;
  merged.reserve(columns_.size() + rhs.columns_.size());
  for (const auto& c : rhs.columns_) {
    merged.push_back(c);
  }
  return {"", merged};
}

std::ostream& operator<<(std::ostream& o, const Schema& s) {
  o << s.name_ << " [ ";
  for (size_t i = 0; i < s.columns_.size(); ++i) {
    if (0 < i) {
      o << " | ";
    }
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
