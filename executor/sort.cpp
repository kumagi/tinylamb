/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/sort.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <fstream>
#include <ios>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <queue>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/decoder.hpp"
#include "common/exc_shim.hpp"
#include "common/exception_status.hpp"
#include "common/status_or.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/collation.hpp"
#include "type/row.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

using PositionedRow = std::pair<Row, RowPosition>;

constexpr size_t kParallelSortMinRows = 4096;

uint64_t BSwap64(uint64_t v) { return __builtin_bswap64(v); }

void AppendMemComparableValue(const Value& v, std::string* out) {
  if (v.IsInterval()) {
    // ENUM values are interval-tagged VARCHAR.  They must sort by EnumRank
    // ("TESTENUM"+rank byte), not raw bytes, otherwise ORDER BY over an ENUM
    // column yields byte order instead of member order (P373).
    const int rank = EnumRank(v.value.varchar_value);
    if (rank >= 0) {
      out->push_back(static_cast<char>(ValueType::kVarChar));
      out->append("TESTENUM", 8);
      out->push_back(static_cast<char>(rank));
      return;
    }
    out->append(v.EncodeMemcomparableFormat());
    return;
  }
  switch (v.type) {
    case ValueType::kInt64:
    case ValueType::kDate: {
      // Unsigned INT64 values >= 2^63 sort after all signed and smaller
      // unsigned values (matching type/value.cpp's TryLess for mixed
      // signed/unsigned INT64), so they get their own tag band above every
      // other value-type tag.
      const bool unsigned_high =
          v.type == ValueType::kInt64 && v.IsUnsigned() &&
          static_cast<uint64_t>(v.value.int_value) >= (uint64_t{1} << 63);
      if (unsigned_high) {
        out->push_back(static_cast<char>(ValueType::kArray) + 1);
        const uint64_t be = BSwap64(static_cast<uint64_t>(v.value.int_value));
        out->append(reinterpret_cast<const char*>(&be), 8);
        break;
      }
      out->push_back(static_cast<char>(v.type));
      const uint64_t be = BSwap64(static_cast<uint64_t>(v.value.int_value));
      std::array<char, 8> buf{};
      std::memcpy(buf.data(), &be, buf.size());
      // Flip the sign bit so that negative signed sorts before non-negative,
      // and unsigned < 2^63 interleaves with non-negative signed by uint
      // value — matching Value::TryLess for mixed signed/unsigned INT64.
      buf[0] ^= static_cast<char>(0x80);
      out->append(buf.data(), buf.size());
      break;
    }
    case ValueType::kVarChar: {
      const std::string_view s(v.value.varchar_value.data(),
                               v.value.varchar_value.size());
      out->push_back(static_cast<char>(ValueType::kVarChar));
      if (s.empty()) {
        out->append(9, '\0');
        break;
      }
      for (size_t i = 0;; i += 8) {
        if (9 <= s.size() - i) {
          out->append(s.substr(i, 8));
          out->push_back('\x09');
        } else {
          const size_t tail = s.size() - i;
          out->append(s.substr(i, tail));
          out->append(8 - tail, '\0');
          out->push_back(
              static_cast<char>((s.size() % 8) + (s.size() % 8 == 0 ? 8 : 0)));
          break;
        }
      }
      break;
    }
    case ValueType::kDouble: {
      out->push_back(static_cast<char>(ValueType::kDouble));
      double d = v.value.double_value;
      // GoogleSQL ordering: NULL < NaN < -inf < finite < +inf (matching
      // CompareForOrderBy in type/value.cpp). Canonicalize every NaN to all-0
      // bytes (0x0000000000000000 < -inf's 0x000fffffffffffff) and fold -0.0
      // into +0.0 so SortExecutor and TopNExecutor / SortKeyOrder agree.
      if (std::isnan(d)) {
        out->append(8, '\0');
        break;
      }
      if (d == 0.0) {
        d = 0.0;
      }
      uint64_t bits = 0;
      std::memcpy(&bits, &d, sizeof(bits));
      const bool negative = (bits >> 63) != 0;
      const uint64_t code = negative ? ~bits : (bits | (uint64_t{1} << 63));
      const uint64_t be = BSwap64(code);
      std::array<char, 8> buf{};
      std::memcpy(buf.data(), &be, buf.size());
      out->append(buf.data(), buf.size());
      break;
    }
    case ValueType::kArray:
      out->push_back(static_cast<char>(ValueType::kArray));
      for (const Value& elem : v.ArrayElements()) {
        if (elem.IsNull()) {
          out->push_back('\x01');
          continue;
        }
        out->push_back('\x02');
        AppendMemComparableValue(elem, out);
      }
      out->push_back('\x00');
      break;
    case ValueType::kNull:
      CHECK_MSG(false, "Cannot encode unknown type.");
  }
}

// 64-bit offsets: the in-memory key blob can exceed 4 GiB before the soft
// memory limit forces a spill; 32-bit offsets wrapped and compared the
// wrong windows (silently mis-ordered output).
using Span = std::pair<size_t, size_t>;

int CompareSpans(std::string_view a, std::string_view b) {
  const size_t common = std::min(a.size(), b.size());
  const int c = std::memcmp(a.data(), b.data(), common);
  if (c != 0) {
    return c;
  }
  return a.size() < b.size() ? -1 : (a.size() > b.size() ? 1 : 0);
}

class SortKeyEncoder {
 public:
  enum class Kind { kSingleUInt64, kPackedTwoInt64, kEncoded };

  SortKeyEncoder(const std::vector<SortExecutor::Key>& keys,
                 const Schema& schema, std::vector<SortKeyOrder>* domains)
      : schema_(&schema) {
    specs_.reserve(keys.size());
    for (const SortExecutor::Key& key : keys) {
      Spec spec;
      spec.expr = key.expression;
      spec.ascending = key.ascending;
      // PRODUCTION FIX: an explicit NULLS FIRST/LAST was dropped here, so
      // SortExecutor always placed NULLs first for ASC (last for DESC) and
      // disagreed with TopNExecutor on the same ORDER BY.
      spec.nulls_first = key.nulls_first.value_or(key.ascending);
      spec.collation = key.collation;
      spec.order = MakeSortKeyOrder(key);
      if (domains != nullptr) { spec.shared_order = &domains->at(specs_.size());
}
      if (key.expression->Type() == TypeTag::kColumnValue) {
        const int offset =
            schema.Offset(key.expression->AsColumnValue().GetColumnName());
        if (offset >= 0) {
          const auto& col = schema.GetColumn(static_cast<size_t>(offset));
          const ValueType type = col.Type();
          if (type == ValueType::kInt64 || type == ValueType::kDate) {
            spec.column = offset;
            spec.is_unsigned = col.IsUnsigned();
          }
        }
      }
      specs_.push_back(spec);
    }
    single_int_ = specs_.size() == 1 && specs_[0].column >= 0;
    const char* disable_packed =
        std::getenv("TINYLAMB_DISABLE_PACKED_ORDER_KEY");
    const bool allow_packed =
        disable_packed == nullptr || disable_packed[0] != '1';
    if (!single_int_ && allow_packed && specs_.size() == 2 &&
        specs_[0].column >= 0 && specs_[1].column >= 0 &&
        !specs_[0].is_unsigned && !specs_[1].is_unsigned) {
      kind_ = Kind::kPackedTwoInt64;
    } else {
      kind_ = single_int_ ? Kind::kSingleUInt64 : Kind::kEncoded;
    }
  }

  [[nodiscard]] Kind GetKind() const { return kind_; }
  [[nodiscard]] bool SingleNullsFirst() const { return specs_[0].nulls_first; }

  // Packs two nullable signed INT64/DATE keys in [-2^30, 2^30 - 1] with
  // independent ASC/DESC and NULLS FIRST/LAST into a single unsigned uint64
  // whose natural < order equals lexicographic SQL ORDER BY order.
  // Lean proof: lean/TinylambOpt/ResearchProofs/PackedOrderKey.lean (sound)
  bool TryPackTwoKey(const Row& row, uint64_t* out) const noexcept {
    static constexpr int64_t kMin31 = -(int64_t{1} << 30);
    static constexpr int64_t kMax31 = (int64_t{1} << 30) - 1;
    static constexpr uint64_t kMask31 = (uint64_t{1} << 31) - 1;
    uint64_t encoded = 0;
    for (size_t i = 0; i < 2; ++i) {
      const Value& v = row.values_[static_cast<size_t>(specs_[i].column)];
      uint64_t tag = 0;
      uint64_t norm = 0;
      if (v.IsNull()) {
        tag = specs_[i].nulls_first ? 0ULL : 1ULL;
        norm = 0;
      } else {
        if (v.IsUnsigned() ||
            (v.type != ValueType::kInt64 && v.type != ValueType::kDate)) {
          return false;
        }
        const int64_t x = v.value.int_value;
        if (x < kMin31 || x > kMax31) {
          return false;
        }
        tag = specs_[i].nulls_first ? 1ULL : 0ULL;
        const auto pos = static_cast<uint64_t>(x - kMin31);
        norm = specs_[i].ascending ? pos : (kMask31 - pos);
      }
      const uint64_t half = (tag << 31) | norm;
      encoded = (i == 0) ? (half << 32) : (encoded | half);
    }
    *out = encoded;
    return true;
  }

  // A single flipped-sign-bit integer cannot order mixed signedness: signed
  // 1 and unsigned 2^63+1 both map to 0x8000000000000001, and signed 0 and
  // unsigned 2^63 both map to 0x8000000000000000.  Value::TryLess orders
  // three bands (signed negatives < non-negative signed/unsigned < 2^63 <
  // unsigned >= 2^63), which needs one more bit than the 64-bit key holds.
  // Return the band separately; the mixed case is sorted by (band, key).
  uint64_t SingleKey(const Row& row, bool* is_null, uint8_t* band) const {
    const Value& v = row.values_[static_cast<size_t>(specs_[0].column)];
    if (v.IsNull()) {
      *is_null = true;
      *band = 1;
      return 0;
    }
    *is_null = false;
    const auto bits = static_cast<uint64_t>(v.value.int_value);
    const bool is_unsigned = specs_[0].is_unsigned || v.IsUnsigned();
    const bool high = bits >= (uint64_t{1} << 63);
    // Canonical (ascending) band: 0 = signed negative, 1 = non-negative
    // signed or unsigned below 2^63, 2 = unsigned >= 2^63.
    uint8_t canonical_band = 1;
    if (is_unsigned && high) {
      canonical_band = 2;
    } else if (!is_unsigned && high) {
      canonical_band = 0;
    }
    // Within a band the flipped (or, for band 2, raw) bits are monotonic in
    // the canonical value, so (band, mapped) is the full order.
    const uint64_t mapped =
        (canonical_band == 2) ? bits : (bits ^ (uint64_t{1} << 63));
    *band = specs_[0].ascending ? canonical_band
                                : static_cast<uint8_t>(2 - canonical_band);
    return specs_[0].ascending ? mapped : ~mapped;
  }

  void AppendEncoded(const Row& row, std::string* out,
                     Status* error = nullptr) const {
    for (const Spec& spec : specs_) {
      Value evaluated;
      Value folded;
      const Value* v = nullptr;
      if (spec.column >= 0) {
        v = &row.values_[static_cast<size_t>(spec.column)];
      } else {
        StatusOr<Value> res = spec.expr->TryEvaluate(row, *schema_);
        if (!res.HasValue()) {
          if (error != nullptr && error->ok()) {
            *error = res.GetStatus();
          }
          evaluated = Value();
        } else {
          evaluated = res.MoveValue();
        }
        v = &evaluated;
      }
      if (error != nullptr && error->ok()) {
        *error = ((spec.shared_order != nullptr) ? *spec.shared_order : spec.order).Observe(*v);
      }
      // Collation-aware encoding: a case-insensitive key folds its text
      // before the memcomparable form is written, so byte order *is* the
      // collation order. Doing it here (not in the comparator) keeps the
      // in-memory permutation, the spilled runs and the external merge
      // windows on one encoding — the merge re-evaluates the same key
      // expression on the row it read back, and a COLLATE tag on the value
      // survives that round trip even though Row serialization drops it.
      if (ResolveCollation(spec.collation, CollationOf(*v)) ==
          Collation::kCaseInsensitive) {
        folded = ApplyKeyCollation(*v, spec.collation);
        v = &folded;
      }
      const bool nulls_first = spec.nulls_first;
      if (spec.ascending) {
        if (v->IsNull()) {
          // Must sort above every non-null encoding ('\x01' + type byte
          // 1..5).  '\x02' would collide with a VARCHAR value's type tag and
          // sort below DOUBLE/DATE/ARRAY keys, silently flipping NULLS LAST
          // into NULLS FIRST for those types.
          out->push_back(nulls_first ? '\x00' : '\xff');
          continue;
        }
        out->push_back('\x01');
        AppendMemComparableValue(*v, out);
      } else {
        if (v->IsNull()) {
          out->push_back(nulls_first ? '\x00' : '\xff');
          continue;
        }
        out->push_back('\x01');
        const size_t begin = out->size();
        AppendMemComparableValue(*v, out);
        for (size_t i = begin; i < out->size(); ++i) {
          (*out)[i] = static_cast<char>(~(*out)[i]);
        }
      }
    }
  }

 private:
  struct Spec {
    Expression expr;
    bool ascending{true};
    bool nulls_first{true};
    int column{-1};
    bool is_unsigned{false};
    Collation collation{Collation::kNone};
    mutable SortKeyOrder order;
    SortKeyOrder* shared_order{};
  };
  const Schema* schema_;
  std::vector<Spec> specs_;
  bool single_int_{false};
  Kind kind_{Kind::kEncoded};
};

// Lean proof: lean/TinylambOpt/ResearchProofs/StableRadix.lean (sound)
void RadixSortIndices(size_t* begin, const size_t* end,
                      const std::vector<uint64_t>& keys) {
  const auto n = static_cast<size_t>(end - begin);
  if (n < 2) {
    return;
  }
  // Every pass below scatters all n slots of the scratch buffer before any of
  // them is read, so the buffer needs no zero-initialization.
  std::unique_ptr<size_t[]> scratch(
      new size_t[n]);  // NOLINT(modernize-avoid-c-arrays)
  size_t* src = begin;
  size_t* dst = scratch.get();
  for (int pass = 0; pass < 8; ++pass) {
    const int shift = pass * 8;
    std::array<size_t, 257> count{};
    for (size_t i = 0; i < n; ++i) {
      ++count[((keys[src[i]] >> shift) & 0xFF) + 1];
    }
    for (size_t b = 0; b < 256; ++b) {
      count[b + 1] += count[b];
    }
    for (size_t i = 0; i < n; ++i) {
      dst[count[(keys[src[i]] >> shift) & 0xFF]++] = src[i];
    }
    std::swap(src, dst);
  }
  if (src != begin) {
    std::copy(src, src + n, begin);
  }
}

class KeyOrdering {
 public:
  KeyOrdering(const std::vector<SortExecutor::Key>& keys, const Schema& schema,
              std::vector<SortKeyOrder>* domains)
      : encoder_(keys, schema, domains) {}

  void AppendRow(const Row& row, Status* error = nullptr) {
    if (encoder_.GetKind() == SortKeyEncoder::Kind::kSingleUInt64) {
      bool is_null = false;
      uint8_t band = 1;
      const uint64_t key = encoder_.SingleKey(row, &is_null, &band);
      if (!bands_.empty() && band != bands_.front()) {
        mixed_bands_ = true;
      }
      bands_.push_back(band);
      raw_keys_.push_back(key);
      null_flags_.push_back(static_cast<uint8_t>(is_null));
      return;
    }
    if (encoder_.GetKind() == SortKeyEncoder::Kind::kPackedTwoInt64) {
      if (!packed_overflow_) {
        uint64_t packed = 0;
        if (encoder_.TryPackTwoKey(row, &packed)) {
          raw_keys_.push_back(packed);
          return;
        }
        packed_overflow_ = true;
        std::vector<uint64_t>().swap(raw_keys_);
      }
      return;
    }
    const size_t begin = blob_.size();
    encoder_.AppendEncoded(row, &blob_, error);
    spans_.emplace_back(begin, blob_.size() - begin);
  }

  void AppendEncodedTo(const Row& row, std::string* out,
                       Status* error = nullptr) const {
    encoder_.AppendEncoded(row, out, error);
  }

  void ResetForNewChunk() {
    blob_.clear();
    spans_.clear();
    raw_keys_.clear();
    null_flags_.clear();
    bands_.clear();
    mixed_bands_ = false;
    packed_overflow_ = false;
  }

  [[nodiscard]] SortKeyEncoder::Kind GetKind() const {
    return encoder_.GetKind();
  }

  [[nodiscard]] StatusOr<std::vector<size_t>> BuildPermutation(
      size_t rows, size_t workers,
      const std::vector<PositionedRow>* source_rows = nullptr) {
    std::vector<size_t> perm(rows);
    for (size_t i = 0; i < rows; ++i) {
      perm[i] = i;
    }
    if (rows < 2) {
      return perm;
    }

    const bool packed_active =
        GetKind() == SortKeyEncoder::Kind::kPackedTwoInt64 && !packed_overflow_;
    if (GetKind() == SortKeyEncoder::Kind::kPackedTwoInt64 &&
        packed_overflow_ && source_rows != nullptr && spans_.empty()) {
      Status encode_status{Status::kSuccess};
      for (size_t i = 0; i < rows; ++i) {
        const size_t begin = blob_.size();
        encoder_.AppendEncoded((*source_rows)[i].first, &blob_, &encode_status);
        RETURN_IF_FAIL(encode_status);
        spans_.emplace_back(begin, blob_.size() - begin);
      }
    }

    size_t sortable_begin = 0;
    size_t sortable_end = rows;
    if (GetKind() == SortKeyEncoder::Kind::kSingleUInt64 &&
        !null_flags_.empty()) {
      std::vector<size_t> nulls;
      std::vector<size_t> non_nulls;
      for (size_t i = 0; i < rows; ++i) {
        ((null_flags_[i] != 0U) ? nulls : non_nulls).push_back(i);
      }
      if (!nulls.empty()) {
        // Honor the explicit NULLS FIRST/LAST request (the encoder already
        // defaulted it to "ascending -> first").
        if (encoder_.SingleNullsFirst()) {
          std::copy(nulls.begin(), nulls.end(), perm.begin());
          std::copy(non_nulls.begin(), non_nulls.end(),
                    perm.begin() + static_cast<std::ptrdiff_t>(nulls.size()));
          sortable_begin = nulls.size();
        } else {
          std::copy(non_nulls.begin(), non_nulls.end(), perm.begin());
          std::copy(
              nulls.begin(), nulls.end(),
              perm.begin() + static_cast<std::ptrdiff_t>(non_nulls.size()));
          sortable_end = non_nulls.size();
        }
      }
    }

    const size_t sortable_rows = sortable_end - sortable_begin;
    if (sortable_rows < 2) {
      return perm;
    }
    size_t* base = perm.data() + sortable_begin;
    const auto index_less = [&](size_t a, size_t b) {
      if (GetKind() == SortKeyEncoder::Kind::kSingleUInt64) {
        if (bands_[a] != bands_[b]) {
          return bands_[a] < bands_[b];
        }
        return raw_keys_[a] < raw_keys_[b];
      }
      if (packed_active) {
        return raw_keys_[a] < raw_keys_[b];
      }
      return CompareSpans(SpanAt(a), SpanAt(b)) < 0;
    };

    const bool use_radix =
        (GetKind() == SortKeyEncoder::Kind::kSingleUInt64 && !mixed_bands_) ||
        packed_active;
    const bool parallel = workers > 1 && sortable_rows >= kParallelSortMinRows;
    if (!parallel) {
      if (use_radix) {
        RadixSortIndices(base, base + sortable_rows, raw_keys_);
      } else {
        std::stable_sort(base, base + sortable_rows, index_less);
      }
      return perm;
    }

    const size_t run_size = (sortable_rows + workers - 1) / workers;
    Status error{Status::kSuccess};
    std::mutex error_mutex;
    std::vector<std::jthread> threads;
    threads.reserve(workers);
    for (size_t worker = 0; worker < workers; ++worker) {
      const size_t begin = worker * run_size;
      const size_t end = std::min(sortable_rows, begin + run_size);
      if (begin == end) {
        break;
      }
      try {
        threads.emplace_back([&, begin, end] {
          try {
            if (use_radix) {
              RadixSortIndices(base + begin, base + end, raw_keys_);
            } else {
              std::stable_sort(base + begin, base + end, index_less);
            }
          } catch (...) {
            std::scoped_lock lock(error_mutex);
            if (error == Status::kSuccess) {
              error = StatusFromCurrentException("parallel sort worker failed");
            }
          }
        });
      } catch (...) {
        std::scoped_lock lock(error_mutex);
        if (error == Status::kSuccess) {
          error = StatusFromCurrentException("parallel sort worker failed");
        }
        break;
      }
    }
    threads.clear();
    RETURN_IF_FAIL(error);
    for (size_t width = run_size; width < sortable_rows; width *= 2) {
      for (size_t begin = 0; begin < sortable_rows; begin += width * 2) {
        const size_t middle = std::min(sortable_rows, begin + width);
        const size_t end = std::min(sortable_rows, begin + (width * 2));
        if (middle != end) {
          std::inplace_merge(base + begin, base + middle, base + end,
                             index_less);
        }
      }
      if (width > sortable_rows / 2) {
        break;
      }
    }
    return perm;
  }

 private:
  [[nodiscard]] std::string_view SpanAt(size_t index) const {
    const Span span = spans_[index];
    return {blob_.data() + span.first, span.second};
  }

  SortKeyEncoder encoder_;
  std::string blob_;
  std::vector<Span> spans_;
  std::vector<uint64_t> raw_keys_;
  std::vector<uint8_t> null_flags_;
  // Mixed signedness needs a band tie-break the 64-bit key cannot express.
  std::vector<uint8_t> bands_;
  bool mixed_bands_{false};
  bool packed_overflow_{false};
};

class RunReader {
 public:
  explicit RunReader(const SpillFile& run)
      : stream_(run.Path(), std::ios::binary | std::ios::in),
        remaining_(run.Count()) {
    if (!stream_) {
      status_ = StatusError(StatusCode::kIOError,
                            "failed to open spill run: " + run.Path().string());
      return;
    }
    uint64_t stored = 0;
    stream_.read(reinterpret_cast<char*>(&stored), sizeof(stored));
    if (stream_.gcount() != static_cast<std::streamsize>(sizeof(stored)) ||
        stored != remaining_) {
      status_ =
          StatusError(StatusCode::kCorrupt,
                      "spill run header mismatch: " + run.Path().string());
    }
  }

  [[nodiscard]] Status GetStatus() const { return status_; }

  bool Next(PositionedRow* dst) {
    if (remaining_ == 0 || status_ != Status::kSuccess) {
      return false;
    }
    Row row;
    RowPosition position;
    Decoder dec(stream_);
    dec >> row >> position;
    if (!stream_) {
      status_ = StatusError(StatusCode::kCorrupt, "truncated spill run");
      return false;
    }
    --remaining_;
    *dst = PositionedRow(std::move(row), position);
    return true;
  }

 private:
  std::ifstream stream_;
  uint64_t remaining_{0};
  Status status_{Status::kSuccess};
};

void ApplyPermutation(std::vector<PositionedRow>* rows,
                      const std::vector<size_t>& permutation) {
  if (permutation.size() != rows->size()) {
    return;
  }
  std::vector<PositionedRow> sorted;
  sorted.reserve(permutation.size());
  for (const size_t index : permutation) {
    sorted.push_back(std::move((*rows)[index]));
  }
  *rows = std::move(sorted);
}

}  // namespace

Status SortExecutor::Materialize() {
  // Re-initialize on every attempt so a partial build from a previous failed
  // attempt cannot leak into the next (stale rows_, charge, etc.).
  rows_.clear();
  rows_.shrink_to_fit();
  rows_charge_ = QueryMemoryCharge();
  offset_ = 0;
  materialized_ = false;
  KeyOrdering ordering(keys_, schema_, options_.collation_domains.get());
  std::vector<SpillFile> runs;
  const auto write_sorted_run = [&]() -> Status {
    ASSIGN_OR_RETURN(
        std::vector<size_t>, permutation,
        ordering.BuildPermutation(rows_.size(), worker_count_, &rows_));
    ApplyPermutation(&rows_, permutation);
    SpillFile run;
    for (const auto& item : rows_) {
      RETURN_IF_FAIL(run.Append(item.first, item.second));
    }
    RETURN_IF_FAIL(run.FinishWriting());
    runs.push_back(std::move(run));
    ordering.ResetForNewChunk();
    rows_.clear();
    // Keep the vector capacity for the next run: the ingest loop immediately
    // refills rows_, so shrinking here only adds a malloc/free per run. The
    // memory budget is released separately by charge.ReleaseAll().
    return Status::kSuccess;
  };

  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  // The shape's own bound, when the plan gave one. It is checked in addition to
  // the process budget, never instead of it: the buffered shape may not outgrow
  // the memory it was planned with, and nothing may outgrow the process.
  const size_t shape_budget = options_.memory_budget_bytes;
  size_t buffered_bytes{0};
  const auto can_hold = [&](size_t bytes) {
    if (shape_budget != 0 && buffered_bytes + bytes > shape_budget) {
      return false;
    }
    return budget.CanReserve(bytes);
  };
  // An external merge shape is expected to spill, so it also bounds its
  // buffered run by rows. Zero means the memory bound alone decides.
  const size_t run_rows = options_.spill_mode == SpillMode::kExternalMerge
                              ? options_.spill_threshold_rows
                              : 0;
  const bool always_spill = options_.spill_mode == SpillMode::kExternalMerge;
  QueryMemoryCharge charge;
  Status encode_error{Status::kSuccess};
  DataChunk batch;
  while (source_->NextBatch(&batch) != 0) {
    for (size_t i = 0; i < batch.Size(); ++i) {
      Row row = batch.RowAt(i);
      const RowPosition position = batch.PositionAt(i);
      const size_t bytes = EstimateRowBytes(row) + sizeof(RowPosition);
      const bool spill_now =
          !can_hold(bytes) || (run_rows != 0 && rows_.size() >= run_rows);
      if (spill_now && !rows_.empty()) {
        RETURN_IF_FAIL(write_sorted_run());
        charge.ReleaseAll();
        buffered_bytes = 0;
      }
      charge.Add(bytes);
      buffered_bytes += bytes;
      ordering.AppendRow(row, &encode_error);
      RETURN_IF_FAIL(encode_error);
      rows_.emplace_back(std::move(row), position);
    }
  }
  RETURN_IF_FAIL(encode_error);
  if (auto st = source_->GetStatus(); st != Status::kSuccess) {
    return st;
  }

  if (runs.empty() && !always_spill) {
    const size_t workers =
        rows_.size() < kParallelSortMinRows
            ? 1
            : std::min(worker_count_, std::max<size_t>(1, rows_.size()));
    ASSIGN_OR_RETURN(std::vector<size_t>, permutation,
                     ordering.BuildPermutation(rows_.size(), workers, &rows_));
    ApplyPermutation(&rows_, permutation);
    rows_charge_ = std::move(charge);
  } else {
    if (!rows_.empty()) {
      RETURN_IF_FAIL(write_sorted_run());
      charge.ReleaseAll();
    }
    // How many rows one run keeps decoded while the runs are merged back; the
    // plan's policy hands it down (see SortMemoryPolicy::merge_window_rows).
    const size_t window_rows = MergeWindowRows();

    struct MergeWindow {
      explicit MergeWindow(size_t rows_per_window)
          : window_rows(rows_per_window) {}
      QueryMemoryCharge charge;
      std::vector<PositionedRow> rows;
      std::string keys;
      std::vector<std::pair<size_t, size_t>> spans;
      size_t index{0};
      size_t window_rows{0};

      bool Fill(const KeyOrdering& ord, RunReader* reader,
                Status* error = nullptr) {
        charge.ReleaseAll();
        rows.clear();
        keys.clear();
        spans.clear();
        index = 0;
        PositionedRow item;
        while (rows.size() < window_rows && reader->Next(&item)) {
          charge.Add(EstimateRowBytes(item.first) + sizeof(RowPosition));
          const size_t begin = keys.size();
          ord.AppendEncodedTo(item.first, &keys, error);
          spans.emplace_back(begin, keys.size() - begin);
          rows.push_back(std::move(item));
        }
        return !rows.empty();
      }
    };
    struct RunCursor {
      size_t run_id{0};
      std::unique_ptr<RunReader> reader;
      std::optional<MergeWindow> window;

      bool FillInitial(const KeyOrdering& ord, size_t window_rows,
                       Status* error) {
        window.emplace(window_rows);
        return window->Fill(ord, reader.get(), error);
      }
      bool Advance(const KeyOrdering& ord, Status* error) {
        if (!window) {
          return false;
        }
        ++window->index;
        if (window->index < window->rows.size()) {
          return true;
        }
        return window->Fill(ord, reader.get(), error);
      }
    };

    std::vector<RunCursor> cursors(runs.size());
    for (size_t i = 0; i < cursors.size(); ++i) {
      cursors[i].run_id = i;
      cursors[i].reader = std::make_unique<RunReader>(runs[i]);
      if (auto st = cursors[i].reader->GetStatus(); st != Status::kSuccess) {
        return st;
      }
      cursors[i].FillInitial(ordering, window_rows, &encode_error);
    }
    auto current_key = [&cursors](size_t id) -> std::string_view {
      const MergeWindow& w = *cursors[id].window;
      const auto span = w.spans[w.index];
      return {w.keys.data() + span.first, span.second};
    };
    auto cursor_less = [&](size_t a, size_t b) {
      const int c = CompareSpans(current_key(a), current_key(b));
      if (c != 0) {
        return c > 0;
      }
      // Tie-break: with this inverted (min-heap) comparator, "less" means
      // the larger run id, so the heap pops the smaller run id first.  Rows
      // from earlier runs appeared earlier in the pre-spill total order,
      // keeping the external merge stable like std::stable_sort.
      return cursors[a].run_id > cursors[b].run_id;
    };
    std::priority_queue<size_t, std::vector<size_t>, decltype(cursor_less)>
        heap(cursor_less);
    for (size_t i = 0; i < cursors.size(); ++i) {
      const std::optional<MergeWindow>& window = cursors[i].window;
      if (window && !window->rows.empty()) {
        heap.push(i);
      }
    }
    QueryMemoryCharge output_charge;
    while (!heap.empty()) {
      const size_t id = heap.top();
      heap.pop();
      RunCursor& cursor = cursors[id];
      if (!cursor.window ||
          cursor.window->index >= cursor.window->rows.size()) {
        continue;
      }
      rows_.push_back(std::move(cursor.window->rows[cursor.window->index]));
      output_charge.Add(EstimateRowBytes(rows_.back().first));
      if (cursor.Advance(ordering, &encode_error)) {
        heap.push(id);
      }
    }
    RETURN_IF_FAIL(encode_error);
    charge.ReleaseAll();
    rows_charge_ = std::move(output_charge);
    for (const RunCursor& cursor : cursors) {
      if (auto st = cursor.reader->GetStatus(); st != Status::kSuccess) {
        return st;
      }
    }
  }

  materialized_ = true;
  return Status::kSuccess;
}

bool SortExecutor::MaterializeOrThrow() {
  if (materialize_failed_) {
    return FailWith(materialize_error_);
  }
  const Status st = Materialize();
  if (st != Status::kSuccess) {
    materialize_failed_ = true;
    materialize_error_ = st;
    return FailWith(st);
  }
  return true;
}

size_t SortExecutor::NextBatch(DataChunk* destination, size_t max_rows) try {
  destination->Reset(schema_, max_rows);
  if (materialize_failed_) {
    FailWith(materialize_error_);
    return 0;
  }
  if (!materialized_) {
    if (!MaterializeOrThrow()) {
      return 0;
    }
  }
  while (destination->Size() < max_rows && offset_ < rows_.size()) {
    destination->Append(std::move(rows_[offset_].first), rows_[offset_].second);
    ++offset_;
  }
  return destination->Size();
} catch (const std::exception& error) {
  destination->Reset();
  materialize_failed_ = true;
  materialize_error_ = StatusError(StatusCode::kRuntimeError, error.what());
  FailWith(materialize_error_);
  return 0;
} catch (...) {
  destination->Reset();
  materialize_failed_ = true;
  materialize_error_ =
      StatusError(StatusCode::kRuntimeError, "sort worker failed");
  FailWith(materialize_error_);
  return 0;
}

void SortExecutor::Dump(std::ostream& output, int indent) const {
  output << (worker_count_ > 1 ? "ParallelSort (" : "Sort (") << worker_count_
         << " workers, spill=" << SpillModeLabel(options_.spill_mode);
  if (options_.memory_budget_bytes != 0) {
    output << ", budget=" << options_.memory_budget_bytes << "B";
  }
  if (options_.spill_threshold_rows != 0) {
    output << ", run_rows=" << options_.spill_threshold_rows;
  }
  output << ")\n" << std::string(static_cast<size_t>(indent) + 2, ' ');
  source_->Dump(output, indent + 2);
}

}  // namespace tinylamb
