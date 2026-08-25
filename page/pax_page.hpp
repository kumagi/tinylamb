/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PAGE_PAX_PAGE_HPP
#define TINYLAMB_PAGE_PAX_PAGE_HPP

#include <array>
#include <cstddef>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"

namespace tinylamb {

// Fixed-size persistent PAX body. Metadata and scalar values are encoded in
// big-endian order; offsets are relative to the first byte of this object.
class PaxPage {
 public:
  void Initialize();
  [[nodiscard]] Status Store(const DataChunk& chunk);
  [[nodiscard]] StatusOr<DataChunk> Load() const;

 private:
  std::array<char, kPageBodySize> bytes_{};
};

static_assert(sizeof(PaxPage) == kPageBodySize);
static_assert(std::is_standard_layout_v<PaxPage>);

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_PAX_PAGE_HPP
