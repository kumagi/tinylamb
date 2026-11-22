/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_COLUMNAR_TABLE_HPP
#define TINYLAMB_EXECUTOR_DETAIL_COLUMNAR_TABLE_HPP

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "common/status_or.hpp"
#include "executor/query_memory.hpp"
#include "expression/bytecode.hpp"
#include "expression/expression.hpp"
#include "table/lstable_bridge.hpp"
#include "type/collation.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// Maximum target rows per Structure-of-Arrays chunk so column slices fit L1/L2
// cache and worker shards concatenate in O(chunks) without row copies.
inline constexpr size_t kColumnarChunkRows = 2048;

// Append-only slab arena owning computed/copied VARCHAR payloads inside a
// chunk. Using deque slabs ensures existing string_views remain stable across
// appends. A slab is raw storage: it is allocated uninitialized
// (make_unique_for_overwrite) because Store() immediately overwrites the bytes
// it hands out -- a value-initialized `std::vector<char>(cap)` slab wrote the
// whole 16 KiB block twice (memset, then memcpy).
struct ChunkStringArena {
  static constexpr size_t kDefaultSlabBytes = 16384;
  struct Slab {
    std::unique_ptr<char[]>
        data;  // NOLINT(modernize-avoid-c-arrays) dynamic slab size
    size_t capacity{0};
  };
  std::deque<Slab> slabs;
  size_t tail_used{0};
  size_t allocated_bytes{0};

  // Move-only (Slab owns a unique_ptr): without an explicit noexcept move,
  // std::vector<ChunkStringArena>::resize selects the copy path through
  // move_if_noexcept and fails inside deque's copy constructor.
  ChunkStringArena() = default;
  ChunkStringArena(ChunkStringArena&&) noexcept = default;
  ChunkStringArena& operator=(ChunkStringArena&&) noexcept = default;
  ChunkStringArena(const ChunkStringArena&) = delete;
  ChunkStringArena& operator=(const ChunkStringArena&) = delete;
  ~ChunkStringArena() = default;

  [[nodiscard]] std::string_view Store(std::string_view text) {
    if (text.empty()) {
      return {""};
    }
    if (!slabs.empty() && tail_used + text.size() <= slabs.back().capacity) {
      char* dst = slabs.back().data.get() + tail_used;
      std::memcpy(dst, text.data(), text.size());
      tail_used += text.size();
      return {dst, text.size()};
    }
    const size_t cap = std::max(kDefaultSlabBytes, text.size());
    // NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamically sized slab
    slabs.push_back(Slab{.data = std::make_unique_for_overwrite<char[]>(cap),
                         .capacity = cap});
    allocated_bytes += cap;
    char* dst = slabs.back().data.get();
    std::memcpy(dst, text.data(), text.size());
    tail_used = text.size();
    return {dst, text.size()};
  }
};

struct ColumnarTable;

// Shared per-chunk gather index for late-materialization pointer columns
// produced by joins and zero-copy filters over ColumnarTables. Keeps the
// referenced ColumnarTable alive and maps each chunk row r to
// (src_chunks[r], src_rows[r]), using kNullRow when outer-join padding yields
// SQL NULL.
struct ChunkGatherIndex {
  static constexpr uint32_t kNullRow = UINT32_MAX;
  std::shared_ptr<const ColumnarTable> source_table;
  std::vector<uint32_t> src_chunks;
  std::vector<uint32_t> src_rows;
  bool has_null{false};
};

// One contiguous column within a ColumnarChunk. Scalar uniform columns store
// either flat typed vectors (Encoding::kFlat) or zero-copy compressed /
// dictionary / Frame-of-Reference / constant / late-row-ID representations.
// Heterogeneous, collated, or array columns cleanly fall back to a per-chunk
// Value vector.
struct ColumnarColumn {
  enum class Encoding : uint8_t {
    kFlat = 0,
    kConstant = 1,
    kDictionary = 2,
    kFrameOfReference = 3,
    kGathered = 4,
  };

  static constexpr uint32_t kNullDictCode = UINT32_MAX;

  ValueType type{ValueType::kNull};
  bool is_unsigned{false};
  bool is_interval{false};
  bool is_uniform{true};
  bool has_null{false};
  Encoding encoding{Encoding::kFlat};

  // Flat representation (used when encoding == Encoding::kFlat).
  std::vector<uint8_t> nulls;
  std::vector<int64_t> ints;
  std::vector<double> doubles;
  std::vector<std::string_view> strings;
  std::vector<Value> fallback;

  // Keeps borrowed immutable buffers (LSTableStore, synthesized dictionaries)
  // and column-local arenas alive even when a ColumnarColumn is sliced or
  // moved.
  std::shared_ptr<const void> encoding_owner;
  std::vector<std::shared_ptr<const void>> extra_owners;
  std::shared_ptr<ChunkStringArena> local_arena;

  // Encoding::kConstant state.
  size_t const_rows{0};
  bool const_is_null{true};
  int64_t const_int{0};
  double const_double{0.0};
  std::string_view const_string;

  // Encoding::kDictionary state.
  const LSTableColumnDictionary* dict{nullptr};
  const uint8_t* borrowed_dict_u8{nullptr};
  const uint8_t* borrowed_nulls{nullptr};
  size_t borrowed_count{0};
  std::vector<uint32_t> dict_codes;

  // Encoding::kFrameOfReference / raw borrowed integer slice state.
  uint8_t for_width{0};
  int64_t for_base{0};
  const uint8_t* for_bytes{nullptr};

  // Encoding::kGathered late-materialization pointer state.
  std::shared_ptr<const ChunkGatherIndex> gather_index;
  size_t gather_col{0};

  void ResetEncodingState() noexcept {
    encoding = Encoding::kFlat;
    encoding_owner.reset();
    extra_owners.clear();
    local_arena.reset();
    const_rows = 0;
    const_is_null = true;
    const_int = 0;
    const_double = 0.0;
    const_string = {};
    dict = nullptr;
    borrowed_dict_u8 = nullptr;
    borrowed_nulls = nullptr;
    borrowed_count = 0;
    dict_codes.clear();
    for_width = 0;
    for_base = 0;
    for_bytes = nullptr;
    gather_index.reset();
    gather_col = 0;
  }

  void ClearFlatVectors() noexcept {
    nulls.clear();
    ints.clear();
    doubles.clear();
    strings.clear();
    fallback.clear();
  }

  [[nodiscard]] bool IsFlat() const noexcept {
    return encoding == Encoding::kFlat;
  }

  [[nodiscard]] bool IsEncoded() const noexcept {
    return encoding != Encoding::kFlat;
  }

  void Init(ValueType value_type, bool unsigned_flag = false,
            bool interval_flag = false, size_t reserve_rows = 0) {
    type = value_type;
    is_unsigned = unsigned_flag;
    is_interval = interval_flag;
    has_null = false;
    ResetEncodingState();
    ClearFlatVectors();
    if (value_type == ValueType::kArray) {
      is_uniform = false;
      fallback.reserve(reserve_rows);
      return;
    }
    is_uniform = true;
    if (reserve_rows == 0) {
      return;
    }
    switch (value_type) {
      case ValueType::kInt64:
      case ValueType::kDate:
        ints.reserve(reserve_rows);
        break;
      case ValueType::kDouble:
        doubles.reserve(reserve_rows);
        break;
      case ValueType::kVarChar:
        strings.reserve(reserve_rows);
        break;
      case ValueType::kNull:
      default:
        break;
    }
  }

  void InitEmptyLike(const ColumnarColumn& proto, size_t reserve_rows = 0) {
    const size_t flat_reserve = proto.IsFlat() ? reserve_rows : 0;
    Init(proto.type, proto.is_unsigned, proto.is_interval, flat_reserve);
    if (!proto.is_uniform) {
      is_uniform = false;
      ints.clear();
      doubles.clear();
      strings.clear();
      if (flat_reserve > 0) {
        fallback.reserve(flat_reserve);
      }
    }
  }

  [[nodiscard]] size_t DictEntryCount() const noexcept {
    if (dict == nullptr) {
      return 0;
    }
    switch (type) {
      case ValueType::kVarChar:
        return dict->StringEntryCount();
      case ValueType::kInt64:
      case ValueType::kDate:
        return dict->IntEntryCount();
      case ValueType::kDouble:
        return dict->DoubleEntryCount();
      default:
        return 0;
    }
  }

  [[nodiscard]] uint32_t DictCodeAt(size_t row_idx) const noexcept {
    if (borrowed_dict_u8 != nullptr) {
      if (has_null &&
          ((borrowed_nulls != nullptr && borrowed_nulls[row_idx] != 0) ||
           borrowed_dict_u8[row_idx] == LSTableColumnDictionary::kNullCode)) {
        return kNullDictCode;
      }
      return borrowed_dict_u8[row_idx];
    }
    return dict_codes[row_idx];
  }

  [[nodiscard]] uint32_t CheckedDictCodeAt(size_t row_idx) const noexcept {
    const size_t rows =
        borrowed_dict_u8 != nullptr ? borrowed_count : dict_codes.size();
    if (row_idx >= rows || dict == nullptr) {
      return kNullDictCode;
    }
    const uint32_t code = DictCodeAt(row_idx);
    return code < DictEntryCount() ? code : kNullDictCode;
  }

  [[nodiscard]] int64_t ForIntAt(size_t row_idx) const noexcept {
    switch (for_width) {
      case 0:
        return for_base;
      case 1:
        return static_cast<int64_t>(static_cast<uint64_t>(for_base) +
                                    for_bytes[row_idx]);
      case 2: {
        uint16_t v = 0;
        std::memcpy(&v, for_bytes + (row_idx * sizeof(v)), sizeof(v));
        return static_cast<int64_t>(static_cast<uint64_t>(for_base) + v);
      }
      case 4: {
        uint32_t v = 0;
        std::memcpy(&v, for_bytes + (row_idx * sizeof(v)), sizeof(v));
        return static_cast<int64_t>(static_cast<uint64_t>(for_base) + v);
      }
      case 8: {
        int64_t v = 0;
        std::memcpy(&v, for_bytes + (row_idx * sizeof(v)), sizeof(v));
        return v;
      }
      default:
        return 0;
    }
  }

  // Resolves kGathered indirection to the leaf ColumnarColumn and row index.
  [[nodiscard]] [[gnu::always_inline]] bool NonGatheredIsNull(
      size_t row_idx) const noexcept {
    switch (encoding) {
      case Encoding::kFlat:
        if (!is_uniform) {
          return fallback[row_idx].IsNull();
        }
        if (type == ValueType::kNull) {
          return true;
        }
        return has_null && nulls[row_idx] != 0;
      case Encoding::kConstant:
        return const_is_null || type == ValueType::kNull;
      case Encoding::kDictionary:
        return CheckedDictCodeAt(row_idx) == kNullDictCode;
      case Encoding::kFrameOfReference:
        return has_null && borrowed_nulls != nullptr &&
               borrowed_nulls[row_idx] != 0;
      case Encoding::kGathered:
        return true;
    }
    return true;
  }

  // Returns false if the resolved value is NULL or out of range.
  [[nodiscard]] inline bool ResolveLeaf(size_t row_idx,
                                        const ColumnarColumn** out_col,
                                        size_t* out_row) const noexcept;

  [[nodiscard]] [[gnu::always_inline]] bool TryReadNonNullInt(
      size_t row_idx, int64_t* out) const noexcept {
    const ColumnarColumn* cur = this;
    size_t r = row_idx;
    if (cur->encoding == Encoding::kGathered) [[unlikely]] {
      if (!ResolveLeaf(row_idx, &cur, &r)) {
        return false;
      }
    }
    if (cur->NonGatheredIsNull(r)) {
      return false;
    }
    *out = cur->IntAt(r);
    return true;
  }

  [[nodiscard]] [[gnu::always_inline]] bool TryReadNonNullDouble(
      size_t row_idx, double* out) const noexcept {
    const ColumnarColumn* cur = this;
    size_t r = row_idx;
    if (cur->encoding == Encoding::kGathered) [[unlikely]] {
      if (!ResolveLeaf(row_idx, &cur, &r)) {
        return false;
      }
    }
    if (cur->NonGatheredIsNull(r)) {
      return false;
    }
    *out = cur->DoubleAt(r);
    return true;
  }

  [[nodiscard]] [[gnu::always_inline]] bool TryReadNonNullString(
      size_t row_idx, std::string_view* out) const noexcept {
    const ColumnarColumn* cur = this;
    size_t r = row_idx;
    if (cur->encoding == Encoding::kGathered) [[unlikely]] {
      if (!ResolveLeaf(row_idx, &cur, &r)) {
        return false;
      }
    }
    if (cur->NonGatheredIsNull(r)) {
      return false;
    }
    *out = cur->StringAt(r);
    return true;
  }

  [[nodiscard]] bool IsNull(size_t row_idx) const noexcept {
    if (encoding == Encoding::kGathered) [[unlikely]] {
      const ColumnarColumn* leaf = nullptr;
      size_t lr = 0;
      return !ResolveLeaf(row_idx, &leaf, &lr);
    }
    return NonGatheredIsNull(row_idx);
  }

  [[nodiscard]] bool AnyNonNull(size_t row_count) const noexcept;

  [[nodiscard]] int64_t IntAt(size_t row_idx) const noexcept {
    const ColumnarColumn* cur = this;
    size_t r = row_idx;
    if (cur->encoding == Encoding::kGathered) {
      if (!ResolveLeaf(row_idx, &cur, &r)) {
        return 0;
      }
    }
    switch (cur->encoding) {
      case Encoding::kFlat:
        return cur->is_uniform ? cur->ints[r]
                               : cur->fallback[r].value.int_value;
      case Encoding::kConstant:
        return cur->const_int;
      case Encoding::kDictionary: {
        const uint32_t code = cur->CheckedDictCodeAt(r);
        return code == kNullDictCode ? 0 : cur->dict->IntEntryAt(code);
      }
      case Encoding::kFrameOfReference:
        return cur->ForIntAt(r);
      case Encoding::kGathered:
        return 0;
    }
    return 0;
  }

  [[nodiscard]] double DoubleAt(size_t row_idx) const noexcept {
    const ColumnarColumn* cur = this;
    size_t r = row_idx;
    if (cur->encoding == Encoding::kGathered) {
      if (!ResolveLeaf(row_idx, &cur, &r)) {
        return 0.0;
      }
    }
    switch (cur->encoding) {
      case Encoding::kFlat:
        return cur->is_uniform ? cur->doubles[r]
                               : cur->fallback[r].value.double_value;
      case Encoding::kConstant:
        return cur->const_double;
      case Encoding::kDictionary: {
        const uint32_t code = cur->CheckedDictCodeAt(r);
        return code == kNullDictCode ? 0.0 : cur->dict->DoubleEntries()[code];
      }
      case Encoding::kFrameOfReference:
        return static_cast<double>(cur->ForIntAt(r));
      case Encoding::kGathered:
        return 0.0;
    }
    return 0.0;
  }

  [[nodiscard]] std::string_view StringAt(size_t row_idx) const noexcept {
    const ColumnarColumn* cur = this;
    size_t r = row_idx;
    if (cur->encoding == Encoding::kGathered) {
      if (!ResolveLeaf(row_idx, &cur, &r)) {
        return {};
      }
    }
    switch (cur->encoding) {
      case Encoding::kFlat:
        return cur->is_uniform ? cur->strings[r]
                               : cur->fallback[r].AsStringView();
      case Encoding::kConstant:
        return cur->const_string;
      case Encoding::kDictionary: {
        if (cur->dict == nullptr) {
          return {};
        }
        const uint32_t code = cur->CheckedDictCodeAt(r);
        return code == kNullDictCode ? std::string_view{}
                                     : cur->dict->StringEntryAt(code);
      }
      case Encoding::kFrameOfReference:
      case Encoding::kGathered:
        return {};
    }
    return {};
  }

  void MaterializeDictCodes(size_t prev_rows);
  void MaterializeFlatInto(size_t row_count, ColumnarColumn* dst,
                           ChunkStringArena* arena) const;
  void MaterializeToFlat(size_t row_count, ChunkStringArena* arena = nullptr);

  void SetConstantNull(ValueType vt, bool unsigned_flag, bool interval_flag,
                       size_t count) {
    Init(vt, unsigned_flag, interval_flag, 0);
    encoding = Encoding::kConstant;
    has_null = (vt != ValueType::kNull);
    const_is_null = true;
    const_rows = count;
  }

  void SetConstantInt(ValueType vt, bool unsigned_flag, int64_t val,
                      size_t count) {
    Init(vt, unsigned_flag, false, 0);
    encoding = Encoding::kConstant;
    has_null = false;
    const_is_null = false;
    const_int = val;
    const_rows = count;
  }

  void SetConstantDouble(double val, size_t count) {
    Init(ValueType::kDouble, false, false, 0);
    encoding = Encoding::kConstant;
    has_null = false;
    const_is_null = false;
    const_double = val;
    const_rows = count;
  }

  void SetConstantString(std::string_view val, bool interval_flag, size_t count,
                         std::shared_ptr<const void> owner = nullptr) {
    Init(ValueType::kVarChar, false, interval_flag, 0);
    encoding = Encoding::kConstant;
    encoding_owner = std::move(owner);
    has_null = false;
    const_is_null = false;
    const_string = val;
    const_rows = count;
  }

  void SetBorrowedNarrowDict(const LSTableColumnDictionary* src_dict,
                             std::shared_ptr<const void> owner, ValueType vt,
                             bool unsigned_flag, bool interval_flag,
                             bool slice_has_null, const uint8_t* codes_u8,
                             const uint8_t* nulls_u8, size_t count) {
    Init(vt, unsigned_flag, interval_flag, 0);
    encoding = Encoding::kDictionary;
    encoding_owner = std::move(owner);
    dict = src_dict;
    has_null = slice_has_null;
    borrowed_dict_u8 = codes_u8;
    borrowed_nulls = nulls_u8;
    borrowed_count = count;
  }

  void SetBorrowedFrameOfReference(std::shared_ptr<const void> owner,
                                   ValueType vt, bool unsigned_flag,
                                   uint8_t width, int64_t base,
                                   const uint8_t* bytes, bool slice_has_null,
                                   const uint8_t* nulls_u8, size_t count) {
    Init(vt, unsigned_flag, false, 0);
    encoding = Encoding::kFrameOfReference;
    encoding_owner = std::move(owner);
    for_width = width;
    for_base = base;
    for_bytes = bytes;
    has_null = slice_has_null;
    borrowed_nulls = nulls_u8;
    borrowed_count = count;
  }

  static void EnsureFallbackValueOwned(Value& v) {
    if (v.type == ValueType::kVarChar && v.owned_data.empty() &&
        !v.value.varchar_value.empty()) {
      v.owned_data.assign(v.value.varchar_value);
      v.value.varchar_value = v.owned_data;
    }
  }

  ChunkStringArena* EnsureLocalArena() {
    if (local_arena == nullptr) {
      local_arena = std::make_shared<ChunkStringArena>();
    }
    return local_arena.get();
  }

  void RetainOwner(std::shared_ptr<const void> owner) {
    if (owner == nullptr) {
      return;
    }
    if (encoding_owner == nullptr) {
      encoding_owner = std::move(owner);
      return;
    }
    if (encoding_owner.get() == owner.get()) {
      return;
    }
    for (const auto& existing : extra_owners) {
      if (existing.get() == owner.get()) {
        return;
      }
    }
    extra_owners.push_back(std::move(owner));
  }

  void RetainOwnersFrom(const ColumnarColumn& src) {
    if (this == &src) {
      return;
    }
    if (src.encoding_owner != nullptr) {
      RetainOwner(src.encoding_owner);
    }
    for (const auto& owner : src.extra_owners) {
      RetainOwner(owner);
    }
    if (src.local_arena != nullptr) {
      RetainOwner(src.local_arena);
    }
    if (src.encoding == Encoding::kGathered && src.gather_index != nullptr) {
      RetainOwner(src.gather_index);
    }
  }

  void SetGathered(std::shared_ptr<const ChunkGatherIndex> idx, size_t col_idx,
                   ValueType vt, bool unsigned_flag, bool interval_flag,
                   bool uniform_flag, bool may_have_null) {
    Init(vt, unsigned_flag, interval_flag, 0);
    is_uniform = uniform_flag;
    has_null = may_have_null || (idx != nullptr && idx->has_null);
    encoding = Encoding::kGathered;
    gather_index = std::move(idx);
    gather_col = col_idx;
  }

  void EnsureNullBitmap(size_t prev_rows) {
    if (!has_null && is_uniform && type != ValueType::kNull) {
      has_null = true;
      nulls.assign(prev_rows, 0);
    }
  }

  bool UpgradeNullTo(ValueType value_type, bool unsigned_flag,
                     bool interval_flag, size_t prev_rows) {
    if (encoding == Encoding::kConstant && const_is_null &&
        type == ValueType::kNull && value_type != ValueType::kArray &&
        value_type != ValueType::kNull) {
      type = value_type;
      is_unsigned = unsigned_flag;
      is_interval = interval_flag;
      has_null = (prev_rows > 0);
      return true;
    }
    if (encoding != Encoding::kFlat) {
      return false;
    }
    if (!is_uniform || type != ValueType::kNull ||
        value_type == ValueType::kArray || value_type == ValueType::kNull) {
      return false;
    }
    Init(value_type, unsigned_flag, interval_flag, prev_rows + 1);
    if (prev_rows > 0) {
      has_null = true;
      nulls.assign(prev_rows, 1);
      switch (type) {
        case ValueType::kInt64:
        case ValueType::kDate:
          ints.assign(prev_rows, 0);
          break;
        case ValueType::kDouble:
          doubles.assign(prev_rows, 0.0);
          break;
        case ValueType::kVarChar:
          strings.assign(prev_rows, std::string_view{});
          break;
        default:
          break;
      }
    }
    return true;
  }

  void AppendNull(size_t prev_rows) {
    if (encoding == Encoding::kConstant) {
      if (const_is_null || type == ValueType::kNull) {
        const_rows = std::max(const_rows, prev_rows);
        ++const_rows;
        if (type != ValueType::kNull) {
          has_null = true;
        }
        return;
      }
      MaterializeToFlat(prev_rows);
    } else if (encoding == Encoding::kDictionary) {
      if (borrowed_dict_u8 != nullptr) {
        MaterializeDictCodes(prev_rows);
      }
      has_null = true;
      dict_codes.push_back(kNullDictCode);
      return;
    } else if (encoding != Encoding::kFlat) {
      MaterializeToFlat(prev_rows);
    }
    if (!is_uniform) {
      fallback.emplace_back();
      return;
    }
    if (type == ValueType::kNull) {
      return;
    }
    EnsureNullBitmap(prev_rows);
    nulls.push_back(1);
    switch (type) {
      case ValueType::kInt64:
      case ValueType::kDate:
        ints.push_back(0);
        break;
      case ValueType::kDouble:
        doubles.push_back(0.0);
        break;
      case ValueType::kVarChar:
        strings.emplace_back();
        break;
      default:
        break;
    }
  }

  void AppendInt(int64_t v) {
    if (has_null) {
      nulls.push_back(0);
    }
    ints.push_back(v);
  }

  void AppendDouble(double v) {
    if (has_null) {
      nulls.push_back(0);
    }
    doubles.push_back(v);
  }

  void AppendString(std::string_view sv) {
    if (has_null) {
      nulls.push_back(0);
    }
    strings.push_back(sv);
  }

  void DemoteToFallback(size_t prev_rows) {
    if (encoding != Encoding::kFlat) {
      MaterializeToFlat(prev_rows);
    }
    if (!is_uniform) {
      return;
    }
    std::vector<Value> out;
    out.reserve(prev_rows);
    for (size_t r = 0; r < prev_rows; ++r) {
      Value v = ValueAt(r);
      EnsureFallbackValueOwned(v);
      out.push_back(std::move(v));
    }
    is_uniform = false;
    has_null = false;
    std::vector<uint8_t>().swap(nulls);
    std::vector<int64_t>().swap(ints);
    std::vector<double>().swap(doubles);
    std::vector<std::string_view>().swap(strings);
    fallback = std::move(out);
  }

  void AppendValue(const Value& v, ChunkStringArena* arena, size_t prev_rows) {
    if (encoding != Encoding::kFlat) {
      if (encoding == Encoding::kConstant) {
        if (v.IsNull()) {
          if (const_is_null || type == ValueType::kNull) {
            const_rows = std::max(const_rows, prev_rows);
            ++const_rows;
            if (type != ValueType::kNull) {
              has_null = true;
            }
            return;
          }
        } else if (!const_is_null && v.type == type &&
                   v.IsUnsigned() == is_unsigned &&
                   v.IsInterval() == is_interval && v.Collation() == 0) {
          const bool matches =
              (type == ValueType::kInt64 || type == ValueType::kDate)
                  ? (v.value.int_value == const_int)
              : (type == ValueType::kDouble)
                  ? (std::bit_cast<uint64_t>(v.value.double_value) ==
                     std::bit_cast<uint64_t>(const_double))
              : (type == ValueType::kVarChar)
                  ? (v.value.varchar_value == const_string)
                  : false;
          if (matches) {
            const_rows = std::max(const_rows, prev_rows);
            ++const_rows;
            return;
          }
        }
      }
      MaterializeToFlat(prev_rows, arena);
    }
    if (!is_uniform) {
      fallback.push_back(v);
      return;
    }
    if (v.IsNull()) {
      AppendNull(prev_rows);
      return;
    }
    if (type == ValueType::kNull && v.type != ValueType::kArray &&
        !v.IsCaseInsensitive() &&
        (v.type == ValueType::kVarChar
             ? (v.Collation() == 0 || v.IsInterval())
             : (v.Collation() == 0 || v.IsUnsigned()))) {
      UpgradeNullTo(v.type, v.IsUnsigned(), v.IsInterval(), prev_rows);
    }
    if (v.type == ValueType::kArray || v.type != type ||
        (v.type == ValueType::kInt64 &&
         ((is_unsigned && !v.IsUnsigned()) ||
          (!is_unsigned && v.Collation() != 0))) ||
        (v.type == ValueType::kVarChar &&
         ((is_interval && !v.IsInterval()) ||
          (!is_interval && v.Collation() != 0))) ||
        ((v.type == ValueType::kDate || v.type == ValueType::kDouble) &&
         v.Collation() != 0)) {
      DemoteToFallback(prev_rows);
      fallback.push_back(v);
      return;
    }
    if (has_null) {
      nulls.push_back(0);
    }
    switch (type) {
      case ValueType::kInt64:
      case ValueType::kDate:
        ints.push_back(v.value.int_value);
        break;
      case ValueType::kDouble:
        doubles.push_back(v.value.double_value);
        break;
      case ValueType::kVarChar: {
        ChunkStringArena* target_arena =
            arena != nullptr ? arena : EnsureLocalArena();
        strings.push_back(target_arena->Store(v.value.varchar_value));
        break;
      }
      default:
        DemoteToFallback(prev_rows);
        fallback.push_back(v);
        break;
    }
  }

  [[gnu::always_inline]] void NonGatheredWriteValueInto(size_t r,
                                                        Value* dst) const {
    if (encoding == Encoding::kConstant) {
      if (type == ValueType::kNull || const_is_null) {
        dst->SetNull();
        return;
      }
      switch (type) {
        case ValueType::kInt64:
          dst->SetInt64(const_int);
          if (is_unsigned) {
            dst->SetUnsigned();
          }
          break;
        case ValueType::kDate:
          dst->SetInt64(const_int);
          dst->type = ValueType::kDate;
          break;
        case ValueType::kDouble:
          dst->SetDouble(const_double);
          break;
        case ValueType::kVarChar:
          dst->CopyFromStringView(const_string);
          if (is_interval) {
            dst->SetInterval();
          }
          break;
        default:
          dst->SetNull();
          break;
      }
      return;
    }
    if (encoding == Encoding::kDictionary) {
      const uint32_t code = CheckedDictCodeAt(r);
      if (code == kNullDictCode) {
        dst->SetNull();
        return;
      }
      switch (type) {
        case ValueType::kVarChar:
          dst->CopyFromStringView(dict->StringEntryAt(code));
          if (is_interval) {
            dst->SetInterval();
          }
          break;
        case ValueType::kInt64:
          dst->SetInt64(dict->IntEntryAt(code));
          if (is_unsigned) {
            dst->SetUnsigned();
          }
          break;
        case ValueType::kDate:
          dst->SetInt64(dict->IntEntryAt(code));
          dst->type = ValueType::kDate;
          break;
        case ValueType::kDouble:
          dst->SetDouble(dict->DoubleEntries()[code]);
          break;
        default:
          dst->SetNull();
          break;
      }
      return;
    }
    if (encoding == Encoding::kFrameOfReference) {
      if (has_null && borrowed_nulls != nullptr && borrowed_nulls[r] != 0) {
        dst->SetNull();
        return;
      }
      dst->SetInt64(ForIntAt(r));
      if (type == ValueType::kDate) {
        dst->type = ValueType::kDate;
      } else if (is_unsigned) {
        dst->SetUnsigned();
      }
      return;
    }
    if (!is_uniform) {
      *dst = fallback[r];
      if (dst->type == ValueType::kVarChar && dst->owned_data.empty() &&
          !dst->value.varchar_value.empty()) {
        dst->owned_data.assign(dst->value.varchar_value);
        dst->value.varchar_value = dst->owned_data;
      }
      return;
    }
    if (NonGatheredIsNull(r)) {
      dst->SetNull();
      return;
    }
    switch (type) {
      case ValueType::kInt64:
        dst->SetInt64(ints[r]);
        if (is_unsigned) {
          dst->SetUnsigned();
        }
        break;
      case ValueType::kDate:
        dst->SetInt64(ints[r]);
        dst->type = ValueType::kDate;
        break;
      case ValueType::kDouble:
        dst->SetDouble(doubles[r]);
        break;
      case ValueType::kVarChar: {
        dst->CopyFromStringView(strings[r]);
        if (is_interval) {
          dst->SetInterval();
        }
        break;
      }
      default:
        dst->SetNull();
        break;
    }
  }

  void WriteValueInto(size_t r, Value* dst) const {
    if (encoding == Encoding::kGathered) [[unlikely]] {
      const ColumnarColumn* leaf = nullptr;
      size_t lr = 0;
      if (!ResolveLeaf(r, &leaf, &lr)) {
        dst->SetNull();
        return;
      }
      leaf->NonGatheredWriteValueInto(lr, dst);
      return;
    }
    NonGatheredWriteValueInto(r, dst);
  }

  [[nodiscard]] BytecodeColumnView AsBytecodeView() const noexcept {
    return BytecodeColumnView{
        .type = type,
        .is_unsigned = is_unsigned,
        .is_interval = is_interval,
        .is_uniform = is_uniform,
        .has_null = has_null,
        .nulls = nulls.empty() ? nullptr : nulls.data(),
        .ints = ints.empty() ? nullptr : ints.data(),
        .doubles = doubles.empty() ? nullptr : doubles.data(),
        .raw_double_bits = nullptr,
        .strings = strings.empty() ? nullptr : strings.data(),
        .fallback = fallback.empty() ? nullptr : fallback.data(),
    };
  }

  [[nodiscard]] BytecodeColumnView ToBytecodeView(
      size_t row_count, ColumnarColumn* flat_scratch,
      ChunkStringArena* scratch_arena) const;

  BytecodeColumnSink AsBytecodeSink(size_t prev_rows) noexcept {
    if (encoding != Encoding::kFlat) {
      MaterializeToFlat(prev_rows);
    }
    return BytecodeColumnSink{
        .initial_rows = prev_rows,
        .type = type,
        .is_uniform = is_uniform,
        .is_unsigned = is_unsigned,
        .is_interval = is_interval,
        .has_null = has_null,
        .may_borrow_constant_strings = false,
        .nulls = &nulls,
        .ints = &ints,
        .doubles = &doubles,
        .strings = &strings,
        .fallback = &fallback,
    };
  }

  void SyncFromBytecodeSink(const BytecodeColumnSink& sink,
                            ChunkStringArena* arena, size_t added_rows) {
    type = sink.type;
    is_uniform = sink.is_uniform;
    has_null = sink.has_null;
    if (sink.may_borrow_constant_strings && is_uniform &&
        type == ValueType::kVarChar && arena != nullptr &&
        added_rows <= strings.size()) {
      const size_t start = strings.size() - added_rows;
      for (size_t i = start; i < strings.size(); ++i) {
        if (!has_null || nulls[i] == 0) {
          strings[i] = arena->Store(strings[i]);
        }
      }
    }
  }

  [[nodiscard]] Value ValueAt(size_t r) const {
    Value v;
    WriteValueInto(r, &v);
    return v;
  }

  void AppendConstant(const Value& v, size_t count, ChunkStringArena* arena,
                      size_t prev_rows);
  void AppendSliceFrom(const ColumnarColumn& src, const uint32_t* sel,
                       size_t count, ChunkStringArena* arena, size_t prev_rows);
  [[nodiscard]] bool AppendSubstringFrom(const ColumnarColumn& src,
                                         size_t src_offset, const uint32_t* sel,
                                         size_t count, int64_t start,
                                         int64_t length, bool has_length,
                                         size_t prev_rows);

  [[nodiscard]] size_t EstimatedBytes() const noexcept {
    return (nulls.size() * sizeof(uint8_t)) + (ints.size() * sizeof(int64_t)) +
           (doubles.size() * sizeof(double)) +
           (strings.size() * sizeof(std::string_view)) +
           (fallback.size() * sizeof(Value)) +
           (dict_codes.size() * sizeof(uint32_t)) +
           (local_arena != nullptr ? local_arena->allocated_bytes : 0);
  }
};

// One horizontal batch of up to kColumnarChunkRows stored in
// Structure-of-Arrays layout, carrying shared ownership of any borrowed backing
// slabs.
struct ColumnarChunk {
  size_t rows{0};
  std::vector<ColumnarColumn> columns;
  std::shared_ptr<ChunkStringArena> string_arena;
  std::vector<std::shared_ptr<const void>> keepalives;

  ChunkStringArena* EnsureArena() {
    if (string_arena == nullptr) {
      string_arena = std::make_shared<ChunkStringArena>();
    }
    return string_arena.get();
  }

  void AppendKeepaliveUnique(std::shared_ptr<const void> owner) {
    if (owner == nullptr) {
      return;
    }
    for (const auto& existing : keepalives) {
      if (existing.get() == owner.get()) {
        return;
      }
    }
    keepalives.push_back(std::move(owner));
  }

  void RetainColumnOwners(const ColumnarColumn& sc) {
    if (sc.encoding_owner != nullptr) {
      AppendKeepaliveUnique(sc.encoding_owner);
    }
    for (const auto& owner : sc.extra_owners) {
      AppendKeepaliveUnique(owner);
    }
    if (sc.local_arena != nullptr) {
      AppendKeepaliveUnique(sc.local_arena);
    }
    if (sc.gather_index != nullptr) {
      AppendKeepaliveUnique(sc.gather_index);
    }
  }

  void InitFromSchema(const Schema& schema, size_t reserve_rows = 0) {
    rows = 0;
    const size_t cols = schema.ColumnCount();
    columns.resize(cols);
    for (size_t c = 0; c < cols; ++c) {
      const Column& col = schema.GetColumn(c);
      columns[c].Init(col.Type(), col.IsUnsigned(), col.IsInterval(),
                      reserve_rows);
    }
  }

  void AppendRow(const Row& row) {
    const size_t cols = columns.size();
    ChunkStringArena* arena = string_arena.get();
    for (size_t c = 0; c < cols; ++c) {
      if (c < row.Count()) {
        const Value& v = row[c];
        if (!v.IsNull() && v.type == ValueType::kVarChar &&
            columns[c].is_uniform &&
            (columns[c].type == ValueType::kVarChar ||
             columns[c].type == ValueType::kNull) &&
            arena == nullptr) {
          arena = EnsureArena();
        }
        columns[c].AppendValue(v, arena, rows);
      } else {
        columns[c].AppendNull(rows);
      }
    }
    ++rows;
  }

  void MaterializeRowInto(size_t r, Row* dst) const {
    const size_t cols = columns.size();
    dst->values_.resize(cols);
    for (size_t c = 0; c < cols; ++c) {
      columns[c].WriteValueInto(r, &dst->values_[c]);
    }
  }

  [[nodiscard]] Row MaterializeRow(size_t r) const {
    Row dst;
    MaterializeRowInto(r, &dst);
    return dst;
  }

  [[nodiscard]] size_t EstimatedBytes() const noexcept {
    size_t bytes = string_arena ? string_arena->allocated_bytes : 0;
    std::array<const ChunkGatherIndex*, 8> inline_gi{};
    size_t inline_count = 0;
    std::vector<const ChunkGatherIndex*> overflow_gi;
    for (const ColumnarColumn& col : columns) {
      bytes += col.EstimatedBytes();
      if (col.gather_index != nullptr) {
        const ChunkGatherIndex* gi = col.gather_index.get();
        bool found = false;
        for (size_t i = 0; i < inline_count; ++i) {
          if (inline_gi[i] == gi) {
            found = true;
            break;
          }
        }
        if (!found && !overflow_gi.empty()) {
          for (const ChunkGatherIndex* prev : overflow_gi) {
            if (prev == gi) {
              found = true;
              break;
            }
          }
        }
        if (!found) {
          if (inline_count < inline_gi.size()) {
            inline_gi[inline_count++] = gi;
          } else {
            overflow_gi.push_back(gi);
          }
          bytes +=
              (gi->src_chunks.size() + gi->src_rows.size()) * sizeof(uint32_t);
        }
      }
    }
    return bytes;
  }
};

// Whole-relation Structure-of-Arrays container composed of L2-friendly
// ColumnarChunks plus shared immutable backing owners (e.g. LSTableStore).
struct ColumnarTable {
  size_t total_rows{0};
  std::vector<ColumnarChunk> chunks;
  std::vector<std::shared_ptr<const void>> keepalives;
  std::shared_ptr<QueryMemoryCharge> memory_charge;

  void AppendChunk(ColumnarChunk&& chunk) {
    if (chunk.rows == 0) {
      return;
    }
    const size_t added_bytes = chunk.EstimatedBytes();
    total_rows += chunk.rows;
    chunks.push_back(std::move(chunk));
    if (added_bytes > 0) {
      if (memory_charge == nullptr) {
        memory_charge = std::make_shared<QueryMemoryCharge>();
      }
      memory_charge->Add(added_bytes);
    }
  }

  void AppendTable(ColumnarTable&& other) {
    if (other.total_rows == 0 && other.chunks.empty()) {
      other.memory_charge.reset();
      return;
    }
    for (auto& owner : other.keepalives) {
      if (owner != nullptr) {
        keepalives.push_back(std::move(owner));
      }
    }
    const size_t fallback_bytes = other.EstimatedBytes();
    total_rows += other.total_rows;
    chunks.reserve(chunks.size() + other.chunks.size());
    for (ColumnarChunk& chunk : other.chunks) {
      if (chunk.rows > 0) {
        chunks.push_back(std::move(chunk));
      }
    }
    if (other.memory_charge != nullptr) {
      if (other.memory_charge.use_count() == 1) {
        if (memory_charge == nullptr) {
          memory_charge = std::move(other.memory_charge);
        } else {
          memory_charge->Adopt(other.memory_charge->DetachBytes());
          other.memory_charge.reset();
        }
      } else {
        other.memory_charge.reset();
        if (fallback_bytes > 0) {
          if (memory_charge == nullptr) {
            memory_charge = std::make_shared<QueryMemoryCharge>();
          }
          memory_charge->Add(fallback_bytes);
        }
      }
    } else if (fallback_bytes > 0) {
      if (memory_charge == nullptr) {
        memory_charge = std::make_shared<QueryMemoryCharge>();
      }
      memory_charge->Add(fallback_bytes);
    }
    other.total_rows = 0;
    other.chunks.clear();
  }

  void AppendKeepalive(std::shared_ptr<const void> owner) {
    if (owner != nullptr) {
      keepalives.push_back(std::move(owner));
    }
  }

  [[nodiscard]] size_t EstimatedBytes() const noexcept {
    size_t bytes = 0;
    for (const ColumnarChunk& chunk : chunks) {
      bytes += chunk.EstimatedBytes();
    }
    return bytes;
  }
};

inline bool ColumnarColumn::ResolveLeaf(size_t row_idx,
                                        const ColumnarColumn** out_col,
                                        size_t* out_row) const noexcept {
  const ColumnarColumn* cur = this;
  size_t r = row_idx;
  while (cur->encoding == Encoding::kGathered) {
    const ChunkGatherIndex* gi = cur->gather_index.get();
    if (gi == nullptr || r >= gi->src_rows.size() ||
        r >= gi->src_chunks.size() || gi->source_table == nullptr)
        [[unlikely]] {
      return false;
    }
    const uint32_t sr = gi->src_rows[r];
    if (sr == ChunkGatherIndex::kNullRow) {
      return false;
    }
    const uint32_t sc = gi->src_chunks[r];
    if (sc >= gi->source_table->chunks.size() ||
        sr >= gi->source_table->chunks[sc].rows ||
        cur->gather_col >= gi->source_table->chunks[sc].columns.size())
        [[unlikely]] {
      return false;
    }
    cur = &gi->source_table->chunks[sc].columns[cur->gather_col];
    r = sr;
  }
  *out_col = cur;
  *out_row = r;
  return !cur->NonGatheredIsNull(r);
}

// Case-insensitive SQL column resolution in schema.
[[nodiscard]] std::optional<size_t> LocalColumnOffset(const Schema& schema,
                                                      const ColumnName& name);

// Scalar-only SQL column resolution (excludes whole-row alias matches).
[[nodiscard]] std::optional<size_t> ResolveScalarColumnOffset(
    const Schema& schema, const ColumnName& name);

// Specification for pushing SELECT projection into scan/producer SoA sink.
struct PushedScanProjection {
  std::vector<Expression> expressions;
  Schema output_schema;
};

// Computes zero-copy UTF-8 substring slice matching AST SUBSTR/SUBSTRING rules
// (`start > 0`: 1-based, `start < 0`: from end, `start == 0`: from start;
// caller must ensure `length >= 0` when `has_length`).
[[nodiscard]] std::string_view SliceUtf8SubstringView(std::string_view s,
                                                      int64_t start,
                                                      int64_t length,
                                                      bool has_length) noexcept;

// Precompiled projection over an SoA input schema (columnar chunks or
// LSTablePageBlock views).
struct CompiledColumnarProjection {
  enum class Kind : uint8_t {
    kPassThrough,
    kConstant,
    kNullCoalesce,
    kBytecode,
    kSubstring,
    kFallback
  };
  struct Item {
    Kind kind{Kind::kFallback};
    slot_t src_col{0};
    slot_t alt_col{0};
    Value constant;
    std::optional<BytecodeProgram> bytecode;
    int64_t substr_start{1};
    int64_t substr_length{0};
    bool substr_has_length{false};
  };
  std::vector<Item> items;
  Schema output_schema;
  bool all_fast{false};

  [[nodiscard]] static CompiledColumnarProjection Compile(
      const std::vector<Expression>& expressions, const Schema& input_schema,
      Schema out_schema);
};

// Evaluates a context-free CompiledColumnarProjection over `input`, returning
// std::nullopt if any chunk requires contextual AST fallback (`kUnsupported`)
// or setting `*error` on SQL evaluation error.
[[nodiscard]] StatusOr<std::optional<std::shared_ptr<ColumnarTable>>>
TryProjectColumnarTable(
    const CompiledColumnarProjection& plan, const ColumnarTable& input,
    std::shared_ptr<const ColumnarTable>* mutable_input = nullptr);

// Slices `[begin, begin + count)` from `input` preserving zero-copy chunk
// references and string lifetimes.
[[nodiscard]] std::shared_ptr<ColumnarTable> SliceColumnarTable(
    const ColumnarTable& input, size_t begin, size_t count);

// Materializes a ColumnarTable into row-oriented storage (parallel when large).
[[nodiscard]] Status MaterializeColumnarTableToRows(const ColumnarTable& table,
                                                    const Schema& schema,
                                                    std::vector<Row>* out_rows,
                                                    bool* uniform_ok);

// Converts row-oriented storage into SoA ColumnarChunks when interoperating
// with row-emitting producers (parallel when large).
std::shared_ptr<ColumnarTable> ProjectColumnarTableBySlots(
    const ColumnarTable& input, std::span<const slot_t> slots,
    bool* all_uniform_out = nullptr);

[[nodiscard]] StatusOr<std::shared_ptr<ColumnarTable>>
BuildColumnarTableFromRows(const Schema& schema, std::span<const Row> rows);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_COLUMNAR_TABLE_HPP
