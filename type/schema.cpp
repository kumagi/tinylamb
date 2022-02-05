#include "schema.hpp"

#include <cstring>
#include <utility>
#include <vector>

#include "common/log_message.hpp"
#include "page/row_position.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

Schema::Schema(std::string_view schema_name, std::vector<Column> columns)
    : name_(schema_name), columns_(std::move(columns)) {}

size_t Schema::Serialize(char* dst) const {
  const char* const original_offset = dst;
  dst += SerializeStringView(dst, name_);
  dst += SerializeInteger(dst, static_cast<int64_t>(columns_.size()));
  for (const auto& c : columns_) {
    dst += c.Serialize(dst);
  }
  return dst - original_offset;
}

size_t Schema::Deserialize(const char* src) {
  const char* const original_offset = src;
  std::string_view sv;
  src += DeserializeStringView(src, &sv);
  name_ = sv;
  int64_t columns;
  src += DeserializeInteger(src, &columns);
  columns_.clear();
  columns_.reserve(columns);
  for (size_t i = 0; i < columns; ++i) {
    Column c;
    src += c.Deserialize(src);
    columns_.push_back(std::move(c));
  }
  return src - original_offset;
}

[[nodiscard]] size_t Schema::Size() const {
  size_t ret = 0;
  ret += SerializeSize(name_);
  ret += sizeof(int64_t);
  for (const auto& c : columns_) {
    ret += c.Size();
  }
  return ret;
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

}  // namespace tinylamb

uint64_t std::hash<tinylamb::Schema>::operator()(
    const tinylamb::Schema& sc) const {
  uint64_t result = std::hash<std::string_view>()(sc.Name());
  for (size_t i = 0; i < sc.ColumnCount(); ++i) {
    result += std::hash<tinylamb::Column>()(sc.GetColumn(i));
  }
  return result;
}
