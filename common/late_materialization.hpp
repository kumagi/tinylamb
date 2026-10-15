/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_COMMON_LATE_MATERIALIZATION_HPP
#define TINYLAMB_COMMON_LATE_MATERIALIZATION_HPP

#include <cstdint>
#include <string_view>

#include "common/constants.hpp"

namespace tinylamb {

enum class LateMaterializationIdentity : uint8_t {
  kEquiKey,
  kUniqueWinner,
  kRowId,
};

struct LateMaterializationRowIdBinding {
  slot_t page_column{0};
  slot_t slot_column{1};
};

[[nodiscard]] inline std::string_view ToString(
    LateMaterializationIdentity identity) {
  switch (identity) {
    case LateMaterializationIdentity::kEquiKey:
      return "equi-key";
    case LateMaterializationIdentity::kUniqueWinner:
      return "unique-winner";
    case LateMaterializationIdentity::kRowId:
      return "rowid";
  }
  return "unknown";
}

}  // namespace tinylamb

#endif  // TINYLAMB_COMMON_LATE_MATERIALIZATION_HPP
