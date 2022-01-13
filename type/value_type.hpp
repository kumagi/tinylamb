#ifndef TINYLAMB_VALUE_TYPE_HPP
#define TINYLAMB_VALUE_TYPE_HPP

namespace tinylamb {

enum class ValueType : uint8_t { kUnknown, kInt64, kVarChar };
enum class AttributeType : uint16_t { kPrimaryKey = 1, kUnique = 1 << 1 };

}  // namespace tinylamb
#endif  // TINYLAMB_VALUE_TYPE_HPP
