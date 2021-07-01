#ifndef TINYLAMB_CONSTANTS_HPP
#define TINYLAMB_CONSTANTS_HPP

#include <limits>

namespace tinylamb {

static constexpr size_t kPageSize = 1024 * 32;
static_assert(kPageSize <= std::numeric_limits<uint16_t>::max());

enum class Status : uint8_t {
  kUnknown,
  kSuccess,
  kNoSpace,
  kConflicts,
  kUnknownType,
  kNotImplemented,
};

inline std::string_view ToString(Status s) {
  switch (s) {
    case Status::kUnknown:
      return "Unknown";
    case Status::kSuccess:
      return "Success";
    case Status::kNoSpace:
      return "NoSpace";
    case Status::kConflicts:
      return "Conflicts";
    case Status::kUnknownType:
      return "UnknownType";
    case Status::kNotImplemented:
      return "NotImplemented";
    default:
      return "INVALID STATUS";
  }
}

}  // namespace tinylamb

#endif  // TINYLAMB_CONSTANTS_HPP
