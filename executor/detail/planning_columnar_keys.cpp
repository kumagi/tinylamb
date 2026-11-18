/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/planning_columnar_keys.hpp"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstring>
#include <limits>
#include <span>
#include <vector>

#include "common/parallel_work.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb::relational_detail {

CompositeIntKey EncodeCompositeIntKey(const Row& row,
                                      const std::vector<slot_t>& columns,
                                      const std::vector<int64_t>* offsets) {
  CompositeIntKey key;
  for (size_t index = 0; index < columns.size(); ++index) {
    const slot_t column = columns[index];
    key.parts[key.count++] =
        AddJoinKeyBias(row[column].value.int_value,
                       offsets != nullptr ? (*offsets)[index] : 0);
  }
  return key;
}

int64_t PackedJoinRanges::Encode(const Row& row,
                                 const std::vector<slot_t>& columns,
                                 const std::vector<int64_t>& offsets) const {
  uint64_t packed = 0;
  bool valid = true;
  for (size_t k = 0; k < columns.size(); ++k) {
    const int64_t value =
        AddJoinKeyBias(row[columns[k]].value.int_value, offsets[k]);
    const uint64_t relative =
        static_cast<uint64_t>(value) - static_cast<uint64_t>(minima[k]);
    valid &= value >= minima[k] && relative <= spans[k];
    packed |= relative << shifts[k];
  }
  return valid ? static_cast<int64_t>(packed)
               : std::numeric_limits<int64_t>::min();
}

std::optional<PackedJoinRanges> BuildPackedJoinRanges(
    const Relation& build, const std::vector<slot_t>& columns,
    const std::vector<int64_t>& offsets) {
  if (build.rows.size() < 1024) {
    return std::nullopt;
  }
  PackedJoinRanges ranges;
  ranges.minima.fill(std::numeric_limits<int64_t>::max());
  std::array<int64_t, CompositeIntKey::kMaxParts> maxima{};
  maxima.fill(std::numeric_limits<int64_t>::min());
  bool any = false;
  const size_t n = build.rows.size();
  const size_t workers = n >= 16384 ? ParallelWorkerCount(n / 8192) : 1;
  if (workers > 1) {
    struct State {
      std::array<int64_t, CompositeIntKey::kMaxParts> minima{};
      std::array<int64_t, CompositeIntKey::kMaxParts> maxima{};
      bool any{false};
      bool ok{true};
    };
    std::vector<State> states(workers);
    for (State& s : states) {
      s.minima.fill(std::numeric_limits<int64_t>::max());
      s.maxima.fill(std::numeric_limits<int64_t>::min());
    }
    if (!RunParallelTasks(workers, [&](size_t w) {
           const size_t begin = (n * w) / workers;
           const size_t end = (n * (w + 1)) / workers;
           State& s = states[w];
           for (size_t i = begin; i < end; ++i) {
             const Row& row = build.rows[i];
             if (HasNullKey(row, columns)) {
               continue;
             }
             s.any = true;
             for (size_t k = 0; k < columns.size(); ++k) {
               const Value& cell = row[columns[k]];
               if ((cell.type != ValueType::kInt64 &&
                    cell.type != ValueType::kDate) ||
                   cell.IsUnsigned()) {
                 s.ok = false;
                 return;
               }
               const int64_t value =
                   AddJoinKeyBias(cell.value.int_value, offsets[k]);
               s.minima[k] = std::min(s.minima[k], value);
               s.maxima[k] = std::max(s.maxima[k], value);
             }
           }
         }).ok()) {
      return std::nullopt;
    }
    for (const State& s : states) {
      if (!s.ok) {
        return std::nullopt;
      }
      if (s.any) {
        any = true;
        for (size_t k = 0; k < columns.size(); ++k) {
          ranges.minima[k] = std::min(ranges.minima[k], s.minima[k]);
          maxima[k] = std::max(maxima[k], s.maxima[k]);
        }
      }
    }
  } else {
    for (const Row& row : build.rows) {
      if (HasNullKey(row, columns)) {
        continue;
      }
      any = true;
      for (size_t k = 0; k < columns.size(); ++k) {
        const Value& cell = row[columns[k]];
        if ((cell.type != ValueType::kInt64 && cell.type != ValueType::kDate) ||
            cell.IsUnsigned()) {
          return std::nullopt;
        }
        const int64_t value = AddJoinKeyBias(cell.value.int_value, offsets[k]);
        ranges.minima[k] = std::min(ranges.minima[k], value);
        maxima[k] = std::max(maxima[k], value);
      }
    }
  }
  if (!any) {
    return std::nullopt;
  }
  unsigned bits = 0;
  for (size_t k = 0; k < columns.size(); ++k) {
    ranges.spans[k] = static_cast<uint64_t>(maxima[k]) -
                      static_cast<uint64_t>(ranges.minima[k]);
    ranges.shifts[k] = static_cast<uint8_t>(bits);
    bits += static_cast<unsigned>(std::bit_width(ranges.spans[k]));
    if (bits > 63) {
      return std::nullopt;
    }
  }
  return ranges;
}

void AppendBigEndian(std::string* output, uint64_t value, size_t width) {
  const size_t offset = output->size();
  output->resize(offset + width);
  for (size_t i = 0; i < width; ++i) {
    (*output)[offset + i] = static_cast<char>(value >> ((width - i - 1) * 8));
  }
}

void AppendMemcomparableInteger(int64_t input, uint8_t prefix, bool flip_sign,
                                std::string* output) {
  output->push_back(static_cast<char>(prefix));
  const size_t offset = output->size();
  AppendBigEndian(output, static_cast<uint64_t>(input), sizeof(uint64_t));
  if (flip_sign) {
    (*output)[offset] ^= static_cast<char>(0x80);
  }
}

bool BorrowedJoinKey::operator==(const BorrowedJoinKey& other) const {
  if (columns->size() != other.columns->size()) {
    return false;
  }
  for (size_t k = 0; k < columns->size(); ++k) {
    const Value& a = (*row)[(*columns)[k]];
    const Value& b = (*other.row)[(*other.columns)[k]];
    if (a.type != b.type) {
      return false;
    }
    if (a.IsNull()) {
      continue;
    }
    if (a.type == ValueType::kVarChar) {
      if (a.value.varchar_value != b.value.varchar_value) {
        return false;
      }
    } else if (AddJoinKeyBias(a.value.int_value, (*biases)[k]) !=
               AddJoinKeyBias(b.value.int_value, (*other.biases)[k])) {
      return false;
    }
  }
  return true;
}

size_t BorrowedJoinKeyHash::operator()(const BorrowedJoinKey& key) const {
  size_t hash = key.columns->size();
  for (size_t k = 0; k < key.columns->size(); ++k) {
    const Value& value = (*key.row)[(*key.columns)[k]];
    const size_t part =
        value.IsNull() ? 0
        : value.type == ValueType::kVarChar
            ? (!value.IsInterval() && !value.IsCaseInsensitive()
                   ? std::hash<std::string_view>{}(value.value.varchar_value)
                   : std::hash<Value>{}(value))
            : static_cast<size_t>(MixEstimateHash64(static_cast<uint64_t>(
                  AddJoinKeyBias(value.value.int_value, (*key.biases)[k]))));
    hash ^= part + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
  }
  return hash;
}

bool CanBorrowJoinKeys(const Relation& a, const Relation& b,
                       const std::vector<slot_t>& a_columns,
                       const std::vector<slot_t>& b_columns,
                       const std::vector<int64_t>& a_biases,
                       const std::vector<int64_t>& b_biases) {
  if (a.HasSpill() || b.HasSpill() || a_columns.empty() ||
      a_columns.size() != b_columns.size()) {
    return false;
  }
  for (size_t k = 0; k < a_columns.size(); ++k) {
    const auto& ac = a.schema.GetColumn(a_columns[k]);
    const auto& bc = b.schema.GetColumn(b_columns[k]);
    const auto type = ac.Type();
    if (type != bc.Type() || ac.IsUnsigned() || bc.IsUnsigned() ||
        (type != ValueType::kInt64 && type != ValueType::kDate &&
         type != ValueType::kVarChar) ||
        (type != ValueType::kInt64 && (a_biases[k] != 0 || b_biases[k] != 0))) {
      return false;
    }
  }
  const auto uniform = [](const Relation& input,
                          const std::vector<slot_t>& columns,
                          const std::vector<int64_t>& biases) {
    std::vector<size_t> check_indices;
    if (input.uniform_cell_types) {
      for (size_t k = 0; k < columns.size(); ++k) {
        const ValueType t = input.schema.GetColumn(columns[k]).Type();
        // VARCHAR cells can carry a case-insensitive collation tag, so they
        // need the per-row value check too even when cell types are uniform.
        if (t == ValueType::kInt64 || t == ValueType::kVarChar) {
          check_indices.push_back(k);
        }
      }
      if (check_indices.empty()) {
        return true;
      }
    } else {
      check_indices.reserve(columns.size());
      for (size_t k = 0; k < columns.size(); ++k) {
        check_indices.push_back(k);
      }
    }
    const auto check_row = [&](const Row& row) {
      for (size_t k : check_indices) {
        const auto& value = row[columns[k]];
        if (value.IsNull()) {
          continue;
        }
        if (value.type != input.schema.GetColumn(columns[k]).Type() ||
            value.IsUnsigned() || value.IsCaseInsensitive()) {
          return false;
        }
        int64_t biased = 0;
        if (biases[k] != 0 &&
            __builtin_add_overflow(value.value.int_value, biases[k], &biased)) {
          return false;
        }
      }
      return true;
    };
    const size_t n = input.rows.size();
    const size_t workers = n >= 16384 ? ParallelWorkerCount(n / 8192) : 1;
    if (workers <= 1) {
      return std::ranges::all_of(input.rows, check_row);
    }
    std::atomic<bool> ok{true};
    if (!RunParallelTasks(workers, [&](size_t w) {
           const size_t begin = (n * w) / workers;
           const size_t end = (n * (w + 1)) / workers;
           for (size_t i = begin; i < end; ++i) {
             if ((i & 63) == 0 && !ok.load(std::memory_order_relaxed)) {
               return;
             }
             if (!check_row(input.rows[i])) {
               ok.store(false, std::memory_order_relaxed);
               return;
             }
           }
         }).ok()) {
      return false;
    }
    return ok.load(std::memory_order_relaxed);
  };
  return uniform(a, a_columns, a_biases) && uniform(b, b_columns, b_biases);
}

void AppendMemcomparableVarChar(std::string_view input, std::string* output) {
  output->push_back(static_cast<char>(ValueType::kVarChar));
  if (input.empty()) {
    output->append(9, '\0');
    return;
  }
  size_t consumed = 0;
  while (consumed < input.size()) {
    const size_t remaining = input.size() - consumed;
    const size_t chunk = std::min<size_t>(8, remaining);
    const size_t offset = output->size();
    output->resize(offset + 9, '\0');
    std::memcpy(output->data() + offset, input.data() + consumed, chunk);
    consumed += chunk;
    (*output)[offset + 8] =
        static_cast<char>(consumed == input.size() ? chunk : 9);
  }
}

void AppendMemcomparableDouble(double d, std::string* output) {
  output->push_back(static_cast<char>(ValueType::kDouble));
  uint64_t bits = 0;
  std::memcpy(&bits, &d, sizeof(bits));
  if (std::isnan(d)) {
    // Every NaN is one peer: drop the sign/payload-dependent bits.
    bits = 0x7ff8000000000000ULL;
  }
  if (d < 0.0) {
    bits = ~bits;
  }
  const size_t offset = output->size();
  AppendBigEndian(output, bits, sizeof(bits));
  if (!std::isnan(d) && !(d < 0.0)) {
    (*output)[offset] ^= static_cast<char>(0x80);
  }
}

void AppendMemcomparableValue(const Value& value, std::string* output) {
  switch (value.type) {
    case ValueType::kNull:
      output->push_back(static_cast<char>(ValueType::kNull));
      return;
    case ValueType::kInt64:
      if (value.IsUnsigned()) {
        AppendMemcomparableInteger(value.value.int_value, uint8_t{6}, false,
                                   output);
      } else {
        AppendMemcomparableInteger(value.value.int_value,
                                   static_cast<uint8_t>(ValueType::kInt64),
                                   true, output);
      }
      return;
    case ValueType::kDate:
      AppendMemcomparableInteger(value.value.int_value,
                                 static_cast<uint8_t>(ValueType::kDate), true,
                                 output);
      return;
    case ValueType::kVarChar: {
      AppendMemcomparableVarChar(value.value.varchar_value, output);
      return;
    }
    case ValueType::kDouble: {
      // SQL equality folds signed zero; -0.0 must hash/encode as +0.0, as the
      // canonical hash_join.cpp encoder and Value's memcomparable format do.
      const double d =
          value.value.double_value == 0.0 ? 0.0 : value.value.double_value;
      AppendMemcomparableDouble(d, output);
      return;
    }
    case ValueType::kArray: {
      output->push_back(static_cast<char>(ValueType::kArray));
      const auto& elements = value.ArrayElements();
      const auto count = static_cast<uint32_t>(elements.size());
      AppendBigEndian(output, count, sizeof(count));
      output->append(value.ArrayElementSqlType());
      output->push_back('\0');
      for (const Value& element : elements) {
        if (element.IsNull()) {
          output->push_back('\0');
          continue;
        }
        output->push_back('\1');
        AppendMemcomparableValue(element, output);
      }
      return;
    }
  }
}

static void EncodeJoinKeyIntoBuffer(const Row& row,
                                    const std::vector<slot_t>& columns,
                                    bool null_safe, bool fold_zero,
                                    const std::vector<int64_t>* offsets,
                                    std::string* out) {
  out->clear();
  out->reserve(columns.size() * (null_safe ? 10 : 9));
  for (size_t index = 0; index < columns.size(); ++index) {
    const slot_t column = columns[index];
    const Value& value = row[column];
    if (value.IsNull()) {
      out->push_back(null_safe ? '\0' : static_cast<char>(ValueType::kNull));
      continue;
    }
    if (null_safe) {
      out->push_back('\1');
    }
    const int64_t bias = offsets != nullptr ? (*offsets)[index] : 0;
    if (bias != 0 &&
        (value.type == ValueType::kInt64 || value.type == ValueType::kDate)) {
      Value biased = value;
      biased.value.int_value = AddJoinKeyBias(biased.value.int_value, bias);
      AppendMemcomparableValue(biased, out);
    } else if (fold_zero && value.type == ValueType::kDouble &&
               value.value.double_value == 0.0) {
      Value positive_zero(0.0);
      AppendMemcomparableValue(positive_zero, out);
    } else {
      AppendMemcomparableValue(value, out);
    }
  }
}

std::string EncodeJoinKeyInternal(const Row& row,
                                  const std::vector<slot_t>& columns,
                                  bool null_safe, bool fold_zero,
                                  const std::vector<int64_t>* offsets) {
  std::string key;
  EncodeJoinKeyIntoBuffer(row, columns, null_safe, fold_zero, offsets, &key);
  return key;
}

std::string EncodeJoinKey(const Row& row, const std::vector<slot_t>& columns,
                          const std::vector<int64_t>* offsets) {
  // Join keys canonicalize doubles the way SQL equality demands: -0 folds to
  // +0 so both zeros share a bucket, while NaN bit patterns stay distinct
  // (NaN never joins with anything).
  return EncodeJoinKeyInternal(row, columns, false, true, offsets);
}

std::string EncodeNullSafeJoinKey(const Row& row,
                                  const std::vector<slot_t>& columns) {
  return EncodeJoinKeyInternal(row, columns, true, true, nullptr);
}

void EncodeNullSafeJoinKeyInto(const Row& row,
                               const std::vector<slot_t>& columns,
                               std::string* out) {
  EncodeJoinKeyIntoBuffer(row, columns, true, true, nullptr, out);
}

bool SameSingleIntegerJoinKey(const Schema& left_schema,
                              const std::vector<slot_t>& left_columns,
                              const Schema& right_schema,
                              const std::vector<slot_t>& right_columns) {
  if (left_columns.size() != 1 || right_columns.size() != 1) {
    return false;
  }
  const Column& left_col = left_schema.GetColumn(left_columns[0]);
  const Column& right_col = right_schema.GetColumn(right_columns[0]);
  const ValueType left_type = left_col.Type();
  // Raw-bit integer matching is only valid when both sides share the
  // signedness domain: unsigned 2^64-1 and signed -1 have the same bits but
  // are different SQL values.
  return left_type == right_col.Type() &&
         left_col.IsUnsigned() == right_col.IsUnsigned() &&
         (left_type == ValueType::kInt64 || left_type == ValueType::kDate);
}

bool EligibleHashedEstimateSchema(const Schema& left_schema,
                                  const std::vector<slot_t>& left_columns,
                                  const Schema& right_schema,
                                  const std::vector<slot_t>& right_columns) {
  if (left_columns.empty() || left_columns.size() != right_columns.size()) {
    return false;
  }
  for (size_t k = 0; k < right_columns.size(); ++k) {
    const auto& lc = left_schema.GetColumn(left_columns[k]);
    const auto& rc = right_schema.GetColumn(right_columns[k]);
    const ValueType t = rc.Type();
    if (lc.Type() != t || lc.IsUnsigned() || rc.IsUnsigned() ||
        (t != ValueType::kInt64 && t != ValueType::kDate &&
         t != ValueType::kVarChar)) {
      return false;
    }
  }
  return true;
}

std::optional<int64_t> HashEstimateKeyTuple(
    const Row& row, const std::vector<slot_t>& columns) noexcept {
  uint64_t hash = 0x9e3779b97f4a7c15ULL ^ columns.size();
  for (const slot_t col : columns) {
    const Value& value = row[col];
    uint64_t part = 0;
    if (value.type == ValueType::kInt64) {
      if (value.IsUnsigned()) {
        return std::nullopt;
      }
      part = MixEstimateHash64(static_cast<uint64_t>(value.value.int_value));
    } else if (value.type == ValueType::kDate) {
      part = MixEstimateHash64(static_cast<uint64_t>(value.value.int_value) ^
                               0x517cc1b727220a95ULL);
    } else if (value.type == ValueType::kVarChar) {
      if (value.IsInterval() || value.IsCaseInsensitive()) {
        return std::nullopt;
      }
      part = MixEstimateHash64(
          static_cast<uint64_t>(
              std::hash<std::string_view>{}(value.value.varchar_value)) ^
          static_cast<uint64_t>(value.value.varchar_value.size()));
    } else {
      return std::nullopt;
    }
    hash = std::rotl(hash, 13) ^ part;
    hash *= 0xbf58476d1ce4e5b9ULL;
  }
  return static_cast<int64_t>(hash ^ (hash >> 31));
}

bool ColumnarHasNullOrNaN(const ColumnarChunk& chunk,
                          std::span<const slot_t> columns, size_t r,
                          Value* scratch) {
  return std::ranges::any_of(columns, [&](slot_t col_idx) {
    const ColumnarColumn& col = chunk.columns[col_idx];
    if (col.IsNull(r)) {
      return true;
    }
    if (!col.is_uniform) {
      col.WriteValueInto(r, scratch);
      return scratch->type == ValueType::kDouble &&
             std::isnan(scratch->value.double_value);
    }
    return col.type == ValueType::kDouble && std::isnan(col.DoubleAt(r));
  });
}

std::optional<int64_t> ColumnarHashEstimateKeyTuple(
    const ColumnarChunk& chunk, std::span<const slot_t> columns, size_t r,
    Value* scratch) noexcept {
  uint64_t hash = 0x9e3779b97f4a7c15ULL ^ columns.size();
  for (const slot_t col_idx : columns) {
    const ColumnarColumn& col = chunk.columns[col_idx];
    uint64_t part = 0;
    if (col.is_uniform && !col.is_unsigned && !col.is_interval) {
      if (col.type == ValueType::kInt64) {
        part = MixEstimateHash64(static_cast<uint64_t>(col.IntAt(r)));
      } else if (col.type == ValueType::kDate) {
        part = MixEstimateHash64(static_cast<uint64_t>(col.IntAt(r)) ^
                                 0x517cc1b727220a95ULL);
      } else if (col.type == ValueType::kVarChar) {
        const std::string_view sv = col.StringAt(r);
        part = MixEstimateHash64(
            static_cast<uint64_t>(std::hash<std::string_view>{}(sv)) ^
            static_cast<uint64_t>(sv.size()));
      } else {
        return std::nullopt;
      }
    } else {
      col.WriteValueInto(r, scratch);
      if (scratch->type == ValueType::kInt64) {
        if (scratch->IsUnsigned()) {
          return std::nullopt;
        }
        part =
            MixEstimateHash64(static_cast<uint64_t>(scratch->value.int_value));
      } else if (scratch->type == ValueType::kDate) {
        part =
            MixEstimateHash64(static_cast<uint64_t>(scratch->value.int_value) ^
                              0x517cc1b727220a95ULL);
      } else if (scratch->type == ValueType::kVarChar) {
        if (scratch->IsInterval() || scratch->IsCaseInsensitive()) {
          return std::nullopt;
        }
        part = MixEstimateHash64(
            static_cast<uint64_t>(
                std::hash<std::string_view>{}(scratch->value.varchar_value)) ^
            static_cast<uint64_t>(scratch->value.varchar_value.size()));
      } else {
        return std::nullopt;
      }
    }
    hash = std::rotl(hash, 13) ^ part;
    hash *= 0xbf58476d1ce4e5b9ULL;
  }
  return static_cast<int64_t>(hash ^ (hash >> 31));
}

CompositeIntKey ColumnarEncodeCompositeIntKey(
    const ColumnarChunk& chunk, std::span<const slot_t> columns, size_t r,
    const std::vector<int64_t>* offsets) {
  CompositeIntKey key;
  for (size_t index = 0; index < columns.size(); ++index) {
    const int64_t raw = chunk.columns[columns[index]].IntAt(r);
    key.parts[key.count++] =
        AddJoinKeyBias(raw, offsets != nullptr ? (*offsets)[index] : 0);
  }
  return key;
}

void ColumnarEncodeJoinKeyInternal(const ColumnarChunk& chunk,
                                   std::span<const slot_t> columns, size_t r,
                                   bool fold_zero,
                                   const std::vector<int64_t>* offsets,
                                   Value* scratch, std::string* output) {
  output->clear();
  output->reserve(columns.size() * 9);
  for (size_t index = 0; index < columns.size(); ++index) {
    const ColumnarColumn& col = chunk.columns[columns[index]];
    if (col.IsNull(r)) {
      output->push_back(static_cast<char>(ValueType::kNull));
      continue;
    }
    const int64_t bias = offsets != nullptr ? (*offsets)[index] : 0;
    if (col.is_uniform && !col.is_unsigned && !col.is_interval) {
      if (col.type == ValueType::kInt64 || col.type == ValueType::kDate) {
        const int64_t biased =
            bias != 0 ? AddJoinKeyBias(col.IntAt(r), bias) : col.IntAt(r);
        AppendMemcomparableInteger(biased, static_cast<uint8_t>(col.type), true,
                                   output);
        continue;
      }
      if (col.type == ValueType::kVarChar) {
        AppendMemcomparableVarChar(col.StringAt(r), output);
        continue;
      }
      if (col.type == ValueType::kDouble) {
        double dv = col.DoubleAt(r);
        if (fold_zero && dv == 0.0) {
          dv = 0.0;
        }
        AppendMemcomparableDouble(dv, output);
        continue;
      }
    }
    col.WriteValueInto(r, scratch);
    if (scratch->IsNull()) {
      output->push_back(static_cast<char>(ValueType::kNull));
      continue;
    }
    if (bias != 0 && (scratch->type == ValueType::kInt64 ||
                      scratch->type == ValueType::kDate)) {
      Value biased = *scratch;
      biased.value.int_value = AddJoinKeyBias(biased.value.int_value, bias);
      AppendMemcomparableValue(biased, output);
    } else if (fold_zero && scratch->type == ValueType::kDouble &&
               scratch->value.double_value == 0.0) {
      AppendMemcomparableDouble(0.0, output);
    } else {
      AppendMemcomparableValue(*scratch, output);
    }
  }
}

bool TryColumnarEncodeJoinKeyNonNull(const ColumnarChunk& chunk,
                                     std::span<const slot_t> columns, size_t r,
                                     bool fold_zero,
                                     const std::vector<int64_t>* offsets,
                                     Value* scratch, std::string* output) {
  output->clear();
  output->reserve(columns.size() * 9);
  for (size_t index = 0; index < columns.size(); ++index) {
    const ColumnarColumn& col = chunk.columns[columns[index]];
    if (col.IsNull(r)) {
      return false;
    }
    const int64_t bias = offsets != nullptr ? (*offsets)[index] : 0;
    if (col.is_uniform && !col.is_unsigned && !col.is_interval) {
      if (col.type == ValueType::kInt64 || col.type == ValueType::kDate) {
        const int64_t biased =
            bias != 0 ? AddJoinKeyBias(col.IntAt(r), bias) : col.IntAt(r);
        AppendMemcomparableInteger(biased, static_cast<uint8_t>(col.type), true,
                                   output);
        continue;
      }
      if (col.type == ValueType::kVarChar) {
        AppendMemcomparableVarChar(col.StringAt(r), output);
        continue;
      }
      if (col.type == ValueType::kDouble) {
        double dv = col.DoubleAt(r);
        if (std::isnan(dv)) {
          return false;
        }
        if (fold_zero && dv == 0.0) {
          dv = 0.0;
        }
        AppendMemcomparableDouble(dv, output);
        continue;
      }
    }
    col.WriteValueInto(r, scratch);
    if (scratch->IsNull() || (scratch->type == ValueType::kDouble &&
                              std::isnan(scratch->value.double_value))) {
      return false;
    }
    if (bias != 0 && (scratch->type == ValueType::kInt64 ||
                      scratch->type == ValueType::kDate)) {
      Value biased = *scratch;
      biased.value.int_value = AddJoinKeyBias(biased.value.int_value, bias);
      AppendMemcomparableValue(biased, output);
    } else if (fold_zero && scratch->type == ValueType::kDouble &&
               scratch->value.double_value == 0.0) {
      AppendMemcomparableDouble(0.0, output);
    } else {
      AppendMemcomparableValue(*scratch, output);
    }
  }
  return true;
}

}  // namespace tinylamb::relational_detail
