/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PAGE_PAX_LAYOUT_HPP
#define TINYLAMB_PAGE_PAX_LAYOUT_HPP

#include <cstddef>
#include <cstdint>

namespace tinylamb {

inline constexpr uint16_t kPaxFormatVersion = 1;

// All offsets are relative to the beginning of Page::body. Fixed-width
// integers make the on-disk format independent of compiler pointer size.
struct PaxPageHeader {
  uint16_t format_version{kPaxFormatVersion};
  uint16_t column_count{0};
  uint16_t row_count{0};
  uint16_t row_capacity{0};
  uint32_t visibility_offset{0};
  uint32_t visibility_length{0};
  uint32_t directory_offset{0};
  uint32_t payload_begin{0};
  uint32_t payload_end{0};
  uint32_t reserved{0};
};

enum class PaxEncoding : uint8_t {
  kPlain = 0,
  kDictionary = 1,
  kBitPacked = 2,
};

// Each column owns an independent values region and NULL bitmap. Variable
// length columns store uint32 offsets followed by byte payload in data region.
struct PaxColumnDirectory {
  uint8_t value_type{0};
  PaxEncoding encoding{PaxEncoding::kPlain};
  uint16_t flags{0};
  uint32_t data_offset{0};
  uint32_t data_length{0};
  uint32_t null_bitmap_offset{0};
  uint32_t null_bitmap_length{0};
  uint32_t auxiliary_offset{0};
  uint32_t auxiliary_length{0};
};

[[nodiscard]] constexpr size_t PaxBitmapBytes(size_t row_capacity) {
  return (row_capacity + 7) / 8;
}

[[nodiscard]] constexpr size_t PaxDirectoryBytes(size_t column_count) {
  return column_count * sizeof(PaxColumnDirectory);
}

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_PAX_LAYOUT_HPP
