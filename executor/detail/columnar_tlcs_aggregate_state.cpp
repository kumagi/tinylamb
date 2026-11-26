/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "executor/detail/columnar_tlcs_internal.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {
namespace {

Status DistinctBudgetExceeded() {
  return StatusError(StatusCode::kMemoryLimitExceeded,
                     "columnar COUNT(DISTINCT) exceeded budget");
}

template <typename DistinctValues>
StatusOr<std::unique_ptr<DistinctValues>> MakeDistinctValues() {
  QueryMemoryCharge charge;
  if (!charge.TryAdd(sizeof(DistinctValues))) {
    return DistinctBudgetExceeded();
  }
  auto values = std::make_unique<DistinctValues>();
  values->charge = std::move(charge);
  return values;
}

Status InsertDistinctString(DistinctStringValues& set, std::string_view value,
                            PagedStringArena& arena) {
  if (set.values.contains(value)) {
    return Status::kSuccess;
  }
  // Estimate the unordered_set node and its share of the bucket array.
  // The string bytes themselves are reserved by the arena.
  constexpr size_t kEntryBytes = 64;
  constexpr size_t kInitialBucketBytes = 128;
  const size_t bytes =
      kEntryBytes + (set.values.empty() ? kInitialBucketBytes : 0);
  if (!set.charge.TryAdd(bytes)) {
    return DistinctBudgetExceeded();
  }
  auto interned = arena.Intern(value);
  if (!interned.HasValue()) {
    set.charge.Release(bytes);
    return interned.GetStatus();
  }
  set.values.insert(interned.Value());
  return Status::kSuccess;
}

}  // namespace
Status FoldRowIntoAggState(
    const ColAggSpec& spec,
    const std::vector<const TlcsFile::DecodedColumnChunk*>& col_ptrs,
    uint32_t row, RuntimeAggState& st, PagedStringArena& arena) {
  if (spec.kind == ColAggKind::kCountStar) {
    ++st.count;
    st.has_value = true;
    return Status::kSuccess;
  }
  if (spec.double_arith != nullptr) {
    for (const slot_t fc : spec.double_arith->referenced_cols) {
      if (col_ptrs[fc]->IsNull(row)) {
        return Status::kSuccess;
      }
    }
    std::array<double, 32> stk{};
    size_t sp = 0;
    for (const auto& ins : spec.double_arith->rpn) {
      switch (ins.op) {
        case ColDoubleArithExpr::Op::kColDouble:
          stk[sp++] =
              std::bit_cast<double>(col_ptrs[ins.full_col]->int_cells[row]);
          break;
        case ColDoubleArithExpr::Op::kColInt64:
          stk[sp++] =
              static_cast<double>(col_ptrs[ins.full_col]->int_cells[row]);
          break;
        case ColDoubleArithExpr::Op::kConst:
          stk[sp++] = ins.constant;
          break;
        case ColDoubleArithExpr::Op::kAdd:
          stk[sp - 2] += stk[sp - 1];
          --sp;
          break;
        case ColDoubleArithExpr::Op::kSub:
          stk[sp - 2] -= stk[sp - 1];
          --sp;
          break;
        case ColDoubleArithExpr::Op::kMul:
          stk[sp - 2] *= stk[sp - 1];
          --sp;
          break;
        case ColDoubleArithExpr::Op::kNeg:
          stk[sp - 1] = -stk[sp - 1];
          break;
      }
    }
    if (sp == 1) {
      if (spec.kind == ColAggKind::kSumDouble && std::isinf(stk[0])) {
        st.saw_inf = true;  // P603 infinity provenance
      }
      st.double_val += stk[0];
      if (spec.kind == ColAggKind::kAvgDouble) {
        ++st.count;
      }
      st.has_value = true;
    }
    return Status::kSuccess;
  }
  const TlcsFile::DecodedColumnChunk& chunk = *col_ptrs[spec.full_col];
  if (chunk.IsNull(row)) {
    return Status::kSuccess;
  }
  switch (spec.kind) {
    case ColAggKind::kCountStar:
      break;
    case ColAggKind::kCountCol:
    case ColAggKind::kCountByteLen:
      ++st.count;
      st.has_value = true;
      break;
    case ColAggKind::kSumInt64: {
      int64_t next = 0;
      if (__builtin_add_overflow(st.int_val, chunk.int_cells[row], &next)) {
        return StatusError(StatusCode::kIsInfinity, "integer overflow on SUM");
      }
      st.int_val = next;
      st.has_value = true;
      break;
    }
    case ColAggKind::kSumByteLen:
      st.int_val += static_cast<int64_t>(chunk.VarcharLength(row));
      st.has_value = true;
      break;
    case ColAggKind::kSumDouble: {
      const auto v = std::bit_cast<double>(chunk.int_cells[row]);
      if (std::isinf(v)) {
        st.saw_inf = true;  // P603 infinity provenance
      }
      st.double_val += v;
      st.has_value = true;
      break;
    }
    case ColAggKind::kAvgDouble:
      st.double_val += chunk.type == ValueType::kDouble
                           ? std::bit_cast<double>(chunk.int_cells[row])
                           : static_cast<double>(chunk.int_cells[row]);
      ++st.count;
      st.has_value = true;
      break;
    case ColAggKind::kAvgByteLen:
      st.double_val += static_cast<double>(chunk.VarcharLength(row));
      ++st.count;
      st.has_value = true;
      break;
    case ColAggKind::kMinInt64:
    case ColAggKind::kMinDate: {
      const int64_t v = chunk.int_cells[row];
      if (!st.has_value || v < st.int_val) {
        st.int_val = v;
        st.has_value = true;
      }
      break;
    }
    case ColAggKind::kMaxInt64:
    case ColAggKind::kMaxDate: {
      const int64_t v = chunk.int_cells[row];
      if (!st.has_value || v > st.int_val) {
        st.int_val = v;
        st.has_value = true;
      }
      break;
    }
    case ColAggKind::kMinDouble: {
      const auto v = std::bit_cast<double>(chunk.int_cells[row]);
      if (std::isnan(v)) {
        st.saw_nan = true;
      }
      if (!st.has_value ||
          CompareForOrderBy(Value(v), Value(st.double_val)) < 0) {
        st.double_val = v;
        st.has_value = true;
      }
      break;
    }
    case ColAggKind::kMaxDouble: {
      const auto v = std::bit_cast<double>(chunk.int_cells[row]);
      if (std::isnan(v)) {
        st.saw_nan = true;
      }
      if (!st.has_value ||
          CompareForOrderBy(Value(v), Value(st.double_val)) > 0) {
        st.double_val = v;
        st.has_value = true;
      }
      break;
    }
    case ColAggKind::kMinVarChar: {
      // Lean proof: lean/TinylambOpt/GroupKeyAndDictionaryRewrites.lean
      // (order_preserving_dict_min_code_sound, unsorted_dict_min_code_bug)
      if (chunk.dict_encoded && chunk.dict != nullptr &&
          row < chunk.dict_codes.size()) {
        const uint8_t code = chunk.dict_codes[row];
        const size_t nd = chunk.dict->StringEntryCount();
        if (code < nd) {
          if (!st.has_value) {
            bool byte_sorted = chunk.dict->codes_follow_string_order;
            if (byte_sorted) {
              for (size_t k = 1; k < nd; ++k) {
                if (!(chunk.dict->StringEntryAt(k - 1) <
                      chunk.dict->StringEntryAt(k))) {
                  byte_sorted = false;
                  break;
                }
              }
            }
            const std::string_view sv = chunk.dict->StringEntryAt(code);
            st.dict_ptr = chunk.dict;
            st.dict_code = code;
            st.dict_byte_sorted = byte_sorted;
            st.str_ptr = sv.data();
            st.str_len = static_cast<uint32_t>(sv.size());
            st.has_value = true;
            break;
          }
          if (st.dict_ptr == chunk.dict) {
            if (code == st.dict_code) {
              break;
            }
            if (st.dict_byte_sorted) {
              if (code < st.dict_code) {
                const std::string_view sv = chunk.dict->StringEntryAt(code);
                st.dict_code = code;
                st.str_ptr = sv.data();
                st.str_len = static_cast<uint32_t>(sv.size());
              }
              break;
            }
            const std::string_view sv = chunk.dict->StringEntryAt(code);
            if (sv < std::string_view(st.str_ptr, st.str_len)) {
              st.dict_code = code;
              st.str_ptr = sv.data();
              st.str_len = static_cast<uint32_t>(sv.size());
            }
            break;
          }
        }
      }
      const std::string_view sv = chunk.VarcharAt(row);
      if (!st.has_value || sv < std::string_view(st.str_ptr, st.str_len)) {
        ASSIGN_OR_RETURN(std::string_view, interned, arena.Intern(sv));
        st.dict_ptr = nullptr;
        st.str_ptr = interned.data();
        st.str_len = static_cast<uint32_t>(interned.size());
        st.has_value = true;
      }
      break;
    }
    case ColAggKind::kMaxVarChar: {
      if (chunk.dict_encoded && chunk.dict != nullptr &&
          row < chunk.dict_codes.size()) {
        const uint8_t code = chunk.dict_codes[row];
        const size_t nd = chunk.dict->StringEntryCount();
        if (code < nd) {
          if (!st.has_value) {
            bool byte_sorted = chunk.dict->codes_follow_string_order;
            if (byte_sorted) {
              for (size_t k = 1; k < nd; ++k) {
                if (!(chunk.dict->StringEntryAt(k - 1) <
                      chunk.dict->StringEntryAt(k))) {
                  byte_sorted = false;
                  break;
                }
              }
            }
            const std::string_view sv = chunk.dict->StringEntryAt(code);
            st.dict_ptr = chunk.dict;
            st.dict_code = code;
            st.dict_byte_sorted = byte_sorted;
            st.str_ptr = sv.data();
            st.str_len = static_cast<uint32_t>(sv.size());
            st.has_value = true;
            break;
          }
          if (st.dict_ptr == chunk.dict) {
            if (code == st.dict_code) {
              break;
            }
            if (st.dict_byte_sorted) {
              if (code > st.dict_code) {
                const std::string_view sv = chunk.dict->StringEntryAt(code);
                st.dict_code = code;
                st.str_ptr = sv.data();
                st.str_len = static_cast<uint32_t>(sv.size());
              }
              break;
            }
            const std::string_view sv = chunk.dict->StringEntryAt(code);
            if (sv > std::string_view(st.str_ptr, st.str_len)) {
              st.dict_code = code;
              st.str_ptr = sv.data();
              st.str_len = static_cast<uint32_t>(sv.size());
            }
            break;
          }
        }
      }
      const std::string_view sv = chunk.VarcharAt(row);
      if (!st.has_value || sv > std::string_view(st.str_ptr, st.str_len)) {
        ASSIGN_OR_RETURN(std::string_view, interned, arena.Intern(sv));
        st.dict_ptr = nullptr;
        st.str_ptr = interned.data();
        st.str_len = static_cast<uint32_t>(interned.size());
        st.has_value = true;
      }
      break;
    }
    case ColAggKind::kMinByteLen: {
      const auto v = static_cast<int64_t>(chunk.VarcharLength(row));
      if (!st.has_value || v < st.int_val) {
        st.int_val = v;
        st.has_value = true;
      }
      break;
    }
    case ColAggKind::kMaxByteLen: {
      const auto v = static_cast<int64_t>(chunk.VarcharLength(row));
      if (!st.has_value || v > st.int_val) {
        st.int_val = v;
        st.has_value = true;
      }
      break;
    }
    case ColAggKind::kCountDistinctInt64:
    case ColAggKind::kCountDistinctDate:
    case ColAggKind::kCountDistinctDouble: {
      int64_t raw = chunk.int_cells[row];
      if (spec.kind == ColAggKind::kCountDistinctDouble) {
        const auto d = std::bit_cast<double>(raw);
        if (d == 0.0) {
          raw = 0;
        } else if (std::isnan(d)) {
          raw = int64_t{0x7ff8000000000000LL};
        }
      }
      if (!st.distinct_ints) {
        ASSIGN_OR_RETURN(std::unique_ptr<DistinctIntegerValues>, values,
                         (MakeDistinctValues<DistinctIntegerValues>()));
        st.distinct_ints = std::move(values);
      }
      if (!st.distinct_ints->values.TryInsertCharged(
              raw, st.distinct_ints->charge)) {
        return DistinctBudgetExceeded();
      }
      st.has_value = true;
      break;
    }
    case ColAggKind::kCountDistinctVarChar: {
      if (!st.distinct_strs) {
        ASSIGN_OR_RETURN(std::unique_ptr<DistinctStringValues>, values,
                         (MakeDistinctValues<DistinctStringValues>()));
        st.distinct_strs = std::move(values);
      }
      RETURN_IF_FAIL(
          InsertDistinctString(*st.distinct_strs, chunk.VarcharAt(row), arena));
      st.has_value = true;
      break;
    }
  }
  return Status::kSuccess;
}

Status MergeAggState(const ColAggSpec& spec, RuntimeAggState& dst,
                     RuntimeAggState& src, PagedStringArena& arena) {
  if (!src.has_value) {
    return Status::kSuccess;
  }
  switch (spec.kind) {
    case ColAggKind::kCountStar:
    case ColAggKind::kCountCol:
    case ColAggKind::kCountByteLen:
      dst.count += src.count;
      dst.has_value = true;
      break;
    case ColAggKind::kSumInt64: {
      int64_t next = 0;
      if (__builtin_add_overflow(dst.int_val, src.int_val, &next)) {
        return StatusError(StatusCode::kIsInfinity, "integer overflow on SUM");
      }
      dst.int_val = next;
      dst.has_value = true;
      break;
    }
    case ColAggKind::kSumByteLen:
      dst.int_val += src.int_val;
      dst.has_value = true;
      break;
    case ColAggKind::kSumDouble:
      // P603: propagate input provenance only.  A worker partial that
      // already overflowed to inf keeps the flag clear so the merged
      // finite-input infinity still raises at finish.
      dst.saw_inf = dst.saw_inf || src.saw_inf;
      dst.double_val += src.double_val;
      dst.has_value = true;
      break;
    case ColAggKind::kAvgDouble:
    case ColAggKind::kAvgByteLen:
      dst.double_val += src.double_val;
      dst.count += src.count;
      dst.has_value = true;
      break;
    case ColAggKind::kMinInt64:
    case ColAggKind::kMinDate:
    case ColAggKind::kMinByteLen:
      if (!dst.has_value || src.int_val < dst.int_val) {
        dst.int_val = src.int_val;
        dst.has_value = true;
      }
      break;
    case ColAggKind::kMaxInt64:
    case ColAggKind::kMaxDate:
    case ColAggKind::kMaxByteLen:
      if (!dst.has_value || src.int_val > dst.int_val) {
        dst.int_val = src.int_val;
        dst.has_value = true;
      }
      break;
    case ColAggKind::kMinDouble:
      dst.saw_nan = dst.saw_nan || src.saw_nan;
      if (!dst.has_value ||
          CompareForOrderBy(Value(src.double_val), Value(dst.double_val)) < 0) {
        dst.double_val = src.double_val;
        dst.has_value = true;
      }
      break;
    case ColAggKind::kMaxDouble:
      dst.saw_nan = dst.saw_nan || src.saw_nan;
      if (!dst.has_value ||
          CompareForOrderBy(Value(src.double_val), Value(dst.double_val)) > 0) {
        dst.double_val = src.double_val;
        dst.has_value = true;
      }
      break;
    case ColAggKind::kMinVarChar: {
      if (dst.has_value && dst.dict_ptr != nullptr &&
          dst.dict_ptr == src.dict_ptr) {
        if (src.dict_code == dst.dict_code) {
          break;
        }
        if (dst.dict_byte_sorted) {
          if (src.dict_code < dst.dict_code) {
            dst.dict_code = src.dict_code;
            dst.str_ptr = src.str_ptr;
            dst.str_len = src.str_len;
          }
          break;
        }
      }
      const std::string_view sv(src.str_ptr, src.str_len);
      if (!dst.has_value || sv < std::string_view(dst.str_ptr, dst.str_len)) {
        if (src.dict_ptr != nullptr) {
          dst.dict_ptr = src.dict_ptr;
          dst.dict_code = src.dict_code;
          dst.dict_byte_sorted = src.dict_byte_sorted;
          dst.str_ptr = src.str_ptr;
          dst.str_len = src.str_len;
        } else {
          ASSIGN_OR_RETURN(std::string_view, interned, arena.Intern(sv));
          dst.dict_ptr = nullptr;
          dst.str_ptr = interned.data();
          dst.str_len = static_cast<uint32_t>(interned.size());
        }
        dst.has_value = true;
      }
      break;
    }
    case ColAggKind::kMaxVarChar: {
      if (dst.has_value && dst.dict_ptr != nullptr &&
          dst.dict_ptr == src.dict_ptr) {
        if (src.dict_code == dst.dict_code) {
          break;
        }
        if (dst.dict_byte_sorted) {
          if (src.dict_code > dst.dict_code) {
            dst.dict_code = src.dict_code;
            dst.str_ptr = src.str_ptr;
            dst.str_len = src.str_len;
          }
          break;
        }
      }
      const std::string_view sv(src.str_ptr, src.str_len);
      if (!dst.has_value || sv > std::string_view(dst.str_ptr, dst.str_len)) {
        if (src.dict_ptr != nullptr) {
          dst.dict_ptr = src.dict_ptr;
          dst.dict_code = src.dict_code;
          dst.dict_byte_sorted = src.dict_byte_sorted;
          dst.str_ptr = src.str_ptr;
          dst.str_len = src.str_len;
        } else {
          ASSIGN_OR_RETURN(std::string_view, interned, arena.Intern(sv));
          dst.dict_ptr = nullptr;
          dst.str_ptr = interned.data();
          dst.str_len = static_cast<uint32_t>(interned.size());
        }
        dst.has_value = true;
      }
      break;
    }
    case ColAggKind::kCountDistinctInt64:
    case ColAggKind::kCountDistinctDate:
    case ColAggKind::kCountDistinctDouble:
      if (src.distinct_ints != nullptr) {
        if (!dst.distinct_ints) {
          dst.distinct_ints = std::move(src.distinct_ints);
        } else {
          bool inserted = true;
          src.distinct_ints->values.ForEach([&](int64_t key, uint8_t&) {
            if (inserted) {
              inserted = dst.distinct_ints->values.TryInsertCharged(
                  key, dst.distinct_ints->charge);
            }
          });
          if (!inserted) {
            return DistinctBudgetExceeded();
          }
        }
        dst.has_value = true;
      }
      break;
    case ColAggKind::kCountDistinctVarChar:
      if (src.distinct_strs != nullptr) {
        if (!dst.distinct_strs) {
          dst.distinct_strs = std::move(src.distinct_strs);
        } else {
          for (const std::string_view s : src.distinct_strs->values) {
            RETURN_IF_FAIL(InsertDistinctString(*dst.distinct_strs, s, arena));
          }
        }
        dst.has_value = true;
      }
      break;
  }
  return Status::kSuccess;
}

Status WorkerGroupTable::Reserve(size_t expected_groups) {
  const size_t need_slots =
      std::max<size_t>(1024, std::bit_ceil(expected_groups * 2));
  if (slots_.size() >= need_slots) {
    return Status::kSuccess;
  }
  return Rehash(need_slots);
}

Status WorkerGroupTable::ReserveGroupStorage(size_t groups) {
  const size_t previous = group_hashes_.capacity();
  if (groups <= previous) {
    return Status::kSuccess;
  }
  const size_t capacity = std::max(groups, previous * 2);
  const size_t nkeys = key_specs_->size();
  const size_t naggs = agg_specs_->size();
  size_t key_capacity = 0;
  size_t agg_capacity = 0;
  size_t bytes = 0;
  const size_t bytes_per_group = (nkeys * sizeof(RuntimeKeyCell)) +
                                 (naggs * sizeof(RuntimeAggState)) +
                                 sizeof(uint64_t);
  const auto limit = [] {
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "columnar group storage exceeded budget");
  };
  if (__builtin_mul_overflow(capacity, nkeys, &key_capacity) ||
      __builtin_mul_overflow(capacity, naggs, &agg_capacity) ||
      __builtin_mul_overflow(capacity, bytes_per_group, &bytes)) {
    return limit();
  }
  QueryMemoryCharge replacement_charge;
  if (charge_ != nullptr && !replacement_charge.TryAdd(bytes)) {
    return limit();
  }
  // Allocate all three arrays before moving the live groups.
  std::vector<RuntimeKeyCell> keys;
  keys.reserve(key_capacity);
  std::vector<RuntimeAggState> aggs;
  aggs.reserve(agg_capacity);
  std::vector<uint64_t> hashes;
  hashes.reserve(capacity);
  keys.insert(keys.end(), keys_.begin(), keys_.end());
  aggs.insert(aggs.end(), std::make_move_iterator(aggs_.begin()),
              std::make_move_iterator(aggs_.end()));
  hashes.insert(hashes.end(), group_hashes_.begin(), group_hashes_.end());
  keys_.swap(keys);
  aggs_.swap(aggs);
  group_hashes_.swap(hashes);
  if (charge_ != nullptr) {
    const size_t old_bytes = (keys.capacity() * sizeof(RuntimeKeyCell)) +
                             (aggs.capacity() * sizeof(RuntimeAggState)) +
                             (hashes.capacity() * sizeof(uint64_t));
    std::vector<RuntimeKeyCell>().swap(keys);
    std::vector<RuntimeAggState>().swap(aggs);
    std::vector<uint64_t>().swap(hashes);
    charge_->Release(old_bytes);
    charge_->Adopt(replacement_charge.DetachBytes());
  }
  return Status::kSuccess;
}

StatusOr<uint32_t> WorkerGroupTable::FindOrInsert(
    const RuntimeKeyCell* probe_keys, uint64_t hash, bool borrow_strings) {
  const size_t nkeys = key_specs_->size();
  if ((num_groups_ + 1) * 10 >= slots_.size() * 7) {
    const size_t next_cap = slots_.empty() ? size_t{1024} : slots_.size() * 2;
    RETURN_IF_FAIL(Rehash(next_cap));
  }
  const size_t mask = slots_.size() - 1;
  size_t pos = static_cast<size_t>(hash) & mask;
  while (true) {
    Slot& s = slots_[pos];
    if (s.group_plus_one == 0) {
      RETURN_IF_FAIL(ReserveGroupStorage(num_groups_ + 1));
      const auto gid = static_cast<uint32_t>(num_groups_);
      const size_t kbase = keys_.size();
      keys_.resize(kbase + nkeys);
      for (size_t i = 0; i < nkeys; ++i) {
        keys_[kbase + i] = probe_keys[i];
        const auto kk = (*key_specs_)[i].kind;
        if (!borrow_strings && !probe_keys[i].is_null &&
            (kk == ColKeyKind::kColVarChar ||
             kk == ColKeyKind::kCaseWhenVarChar ||
             kk == ColKeyKind::kRegexReplaceVarChar)) {
          auto interned = arena_.Intern(probe_keys[i].StrView());
          if (!interned.HasValue()) {
            keys_.resize(kbase);
            return interned.GetStatus();
          }
          keys_[kbase + i].str_ptr = interned.Value().data();
          keys_[kbase + i].str_len =
              static_cast<uint32_t>(interned.Value().size());
        }
      }
      aggs_.resize(aggs_.size() + agg_specs_->size());
      group_hashes_.push_back(hash);
      s.hash = hash;
      s.group_plus_one = gid + 1;
      ++num_groups_;
      return gid;
    }
    if (s.hash == hash) {
      const uint32_t existing = s.group_plus_one - 1;
      if (GroupKeySliceHasher::Equals(
              &keys_[static_cast<size_t>(existing) * nkeys], probe_keys,
              *key_specs_)) {
        return existing;
      }
    }
    pos = (pos + 1) & mask;
  }
}

Status WorkerGroupTable::MergeFromGroup(const RuntimeKeyCell* src_keys,
                                        uint64_t hash,
                                        std::span<RuntimeAggState> src_aggs,
                                        bool borrow_strings) {
  if (src_aggs.size() != agg_specs_->size()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "columnar group aggregate state width mismatch");
  }
  ASSIGN_OR_RETURN(uint32_t, gid, FindOrInsert(src_keys, hash, borrow_strings));
  const size_t abase = static_cast<size_t>(gid) * src_aggs.size();
  for (size_t a = 0; a < src_aggs.size(); ++a) {
    RETURN_IF_FAIL(
        MergeAggState((*agg_specs_)[a], aggs_[abase + a], src_aggs[a], arena_));
  }
  return Status::kSuccess;
}

Status WorkerGroupTable::Rehash(size_t new_cap) {
  const size_t new_bytes = new_cap * sizeof(Slot);
  const size_t old_bytes = slots_.size() * sizeof(Slot);
  if (charge_ != nullptr && new_bytes > old_bytes &&
      !charge_->TryAdd(new_bytes - old_bytes)) {
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "columnar hash table exceeded budget");
  }
  std::vector<Slot> next(new_cap);
  const size_t mask = new_cap - 1;
  for (const Slot& old : slots_) {
    if (old.group_plus_one == 0) {
      continue;
    }
    size_t pos = static_cast<size_t>(old.hash) & mask;
    while (next[pos].group_plus_one != 0) {
      pos = (pos + 1) & mask;
    }
    next[pos] = old;
  }
  slots_.swap(next);
  return Status::kSuccess;
}

StatusOr<Value> FinishAggState(const ColAggSpec& spec,
                               const RuntimeAggState& st) {
  switch (spec.kind) {
    case ColAggKind::kCountStar:
    case ColAggKind::kCountCol:
    case ColAggKind::kCountByteLen:
      return Value(st.count);
    case ColAggKind::kSumInt64:
    case ColAggKind::kSumByteLen:
      return st.has_value ? Value(st.int_val) : Value();
    case ColAggKind::kSumDouble:
      if (!st.has_value) {
        return Value();
      }
      if (!st.saw_inf && std::isinf(st.double_val)) {
        // P603: finite inputs whose running sum left the double range
        // raise, matching the canonical accumulator (aggregate_accumulator
        // "SUM overflow: result is infinite"); an actual infinity input
        // legitimately propagates as inf.
        return StatusError(StatusCode::kIsInfinity,
                           "SUM overflow: result is infinite");
      }
      return Value(st.double_val);
    case ColAggKind::kAvgDouble:
    case ColAggKind::kAvgByteLen:
      return st.count == 0
                 ? Value()
                 : Value(st.double_val / static_cast<double>(st.count));
    case ColAggKind::kMinInt64:
    case ColAggKind::kMaxInt64:
    case ColAggKind::kMinByteLen:
    case ColAggKind::kMaxByteLen:
      return st.has_value ? Value(st.int_val) : Value();
    case ColAggKind::kMinDate:
    case ColAggKind::kMaxDate:
      return st.has_value ? Value::DateFromDays(st.int_val) : Value();
    case ColAggKind::kMinDouble:
    case ColAggKind::kMaxDouble:
      // GoogleSQL: any NaN in the group makes MIN/MAX NaN.
      if (!st.has_value) {
        return Value();
      }
      return Value(st.saw_nan ? std::numeric_limits<double>::quiet_NaN()
                              : st.double_val);
    case ColAggKind::kMinVarChar:
    case ColAggKind::kMaxVarChar:
      if (!st.has_value) {
        return Value();
      }
      return st.str_len == 0 ? Value("")
                             : Value(std::string(st.str_ptr, st.str_len));
    case ColAggKind::kCountDistinctInt64:
    case ColAggKind::kCountDistinctDate:
    case ColAggKind::kCountDistinctDouble:
      if (st.distinct_count_final) {
        return Value(st.count);
      }
      return Value(static_cast<int64_t>(
          st.distinct_ints != nullptr ? st.distinct_ints->values.Size() : 0));
    case ColAggKind::kCountDistinctVarChar:
      if (st.distinct_count_final) {
        return Value(st.count);
      }
      return Value(static_cast<int64_t>(
          st.distinct_strs != nullptr ? st.distinct_strs->values.size() : 0));
  }
  return Value();
}

}  // namespace tinylamb::relational_detail
