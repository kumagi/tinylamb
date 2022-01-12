#ifndef TINYLAMB_CONSTANTS_HPP
#define TINYLAMB_CONSTANTS_HPP

#include <limits>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace tinylamb {

static constexpr size_t kPageSize = 1024 * 32;
static_assert(kPageSize <= std::numeric_limits<uint16_t>::max());
static constexpr size_t kPageHeaderSize =
    sizeof(uint64_t) +  // page_id
    sizeof(uint64_t) +  // page_lsn
    sizeof(uint64_t) +  // rec_lsn
    sizeof(uint64_t) +  // page_type
    sizeof(uint64_t);  // checksum
static constexpr size_t kPageBodySize = kPageSize - kPageHeaderSize;

enum class Status : uint8_t {
  kUnknown,
  kSuccess,
  kNoSpace,
  kConflicts,
  kUnknownType,
  kNotImplemented,
};

typedef uint64_t lsn_t;
typedef uint64_t txn_id_t;
typedef uint64_t page_id_t;

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
