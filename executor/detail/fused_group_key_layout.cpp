/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/fused_group_key_layout.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_group_dictionary.hpp"
#include "executor/detail/fused_grouped_join.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/jit.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/unary_expression.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {
Status FusedGroupKeyLayout::Prepare(const FusedKeyInputs& input) {
  use_split_key_gid = false;
  split_has_left_probe = false;
  split_uses_right_subgid = false;
  split_uses_pk_subgid = false;
  split_right_shift = 32;
  split_left_k = 1;
  split_right_subgid_count = 0;

  split_right_subgid_ptr = nullptr;

  split_right_rep_ptr = nullptr;
  split_right_rep_count = 0;

  const auto clear_split_right_dict = [&]() noexcept {
    split_right_subgid.clear();
    split_right_rep_idx.clear();
    shared_right_dict.reset();
    split_right_subgid_ptr = nullptr;
    split_right_rep_ptr = nullptr;
    split_right_rep_count = 0;
    split_right_subgid_count = 0;
  };
  const auto splitmix_i64 = [](int64_t iv) noexcept -> uint64_t {
    uint64_t x = static_cast<uint64_t>(iv) + 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
  };
  const auto build_flat_subgid_dict =
      [](size_t row_count, size_t max_unique, auto&& fill_hashes,
         auto&& eq_rows, std::vector<uint32_t>* out_subgid, uint32_t* out_count,
         const std::vector<uint8_t>* row_active = nullptr,
         std::vector<uint32_t>* out_rep_rows = nullptr) -> bool {
    return BuildGroupDictionary(row_count, max_unique, std::ref(fill_hashes),
                                std::ref(eq_rows), out_subgid, out_count,
                                row_active, out_rep_rows);
  };
  right_row_count = input.right_col_only ? input.total_right_rows
                                         : input.right_ptr->rows.size();
  const auto build_or_reuse_split_right_dict =
      [&](const std::vector<size_t>& col_indices, uint32_t* out_card) -> bool {
    *out_card = 0;
    clear_split_right_dict();
    struct RightDictSlotGuard {
      SharedBaseRelationCache* cache{nullptr};
      std::string key;
      bool active{false};
      RightDictSlotGuard() = default;
      RightDictSlotGuard(SharedBaseRelationCache* c, std::string k, bool a)
          : cache(c), key(std::move(k)), active(a) {}
      RightDictSlotGuard(const RightDictSlotGuard&) = delete;
      RightDictSlotGuard& operator=(const RightDictSlotGuard&) = delete;
      RightDictSlotGuard(RightDictSlotGuard&&) = delete;
      RightDictSlotGuard& operator=(RightDictSlotGuard&&) = delete;
      ~RightDictSlotGuard() {
        if (active && cache != nullptr) {
          cache->AbandonSplitRightDict(key);
        }
      }
    } right_dict_guard(input.shared_pk_cache, {}, false);
    std::string right_dict_key;
    if (input.shared_pk_cache != nullptr && input.right_col_only &&
        input.right_col_owner != nullptr) {
      right_dict_key = "R:" +
                       std::to_string(reinterpret_cast<uintptr_t>(
                           input.right_col_owner.get())) +
                       ":" + std::to_string(right_row_count) + ":" +
                       std::to_string(input.right_arg.columnar->chunks.size());
      for (const size_t col : col_indices) {
        right_dict_key += ":";
        right_dict_key += std::to_string(col);
      }
      std::shared_ptr<const SharedBaseRelationCache::CachedSplitRightDict> hit;
      if (!input.shared_pk_cache->AcquireSplitRightDictSlot(right_dict_key,
                                                            &hit)) {
        if (hit != nullptr) {
          shared_right_dict = std::move(hit);
          split_right_subgid_ptr = shared_right_dict->subgid.data();
          split_right_rep_ptr = shared_right_dict->rep_idx.data();
          split_right_rep_count = shared_right_dict->rep_idx.size();
          *out_card = shared_right_dict->subgid_count;
          return true;
        }
      } else {
        right_dict_guard.key = right_dict_key;
        right_dict_guard.active = true;
      }
    }
    // Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
    //   (unique_col_identity_subgid_sound)
    // If any dimension key column in col_indices is unique and non-null on the
    // right relation (PK or promoted unique-nonnull column), every right row
    // has a distinct dimension key tuple, so subgid(ri) = ri is an exact
    // bijection without hashing wide VARCHAR dimension attributes.
    if (right_row_count > 0 &&
        right_row_count <= std::numeric_limits<uint32_t>::max()) {
      bool any_unique_nonnull = false;
      for (const size_t col : col_indices) {
        if (col >= input.right.schema.ColumnCount()) {
          continue;
        }
        const auto ct = input.right.schema.GetColumn(col).GetConstraint().ctype;
        if (ct == Constraint::kPrimaryKey) {
          any_unique_nonnull = true;
          break;
        }
        if (ct == Constraint::kUnique && input.right_col_only &&
            input.right_arg.columnar != nullptr) {
          bool col_has_null = false;
          for (const ColumnarChunk& ch : input.right_arg.columnar->chunks) {
            if (ch.rows == 0) {
              continue;
            }
            if (col >= ch.columns.size() || ch.columns[col].has_null) {
              col_has_null = true;
              break;
            }
          }
          if (!col_has_null) {
            any_unique_nonnull = true;
            break;
          }
        }
      }
      if (any_unique_nonnull) {
        std::vector<uint32_t> id_vec(right_row_count);
        for (size_t ri = 0; ri < right_row_count; ++ri) {
          id_vec[ri] = static_cast<uint32_t>(ri);
        }
        const auto card = static_cast<uint32_t>(right_row_count);
        if (right_dict_guard.active && input.shared_pk_cache != nullptr) {
          auto cached =
              std::make_shared<SharedBaseRelationCache::CachedSplitRightDict>();
          cached->subgid_count = card;
          cached->subgid = id_vec;
          cached->rep_idx = std::move(id_vec);
          std::shared_ptr<const SharedBaseRelationCache::CachedSplitRightDict>
              shared_rd = std::move(cached);
          input.shared_pk_cache->PublishSplitRightDict(right_dict_key,
                                                       shared_rd);
          right_dict_guard.active = false;
          shared_right_dict = std::move(shared_rd);
          split_right_subgid_ptr = shared_right_dict->subgid.data();
          split_right_rep_ptr = shared_right_dict->rep_idx.data();
          split_right_rep_count = shared_right_dict->rep_idx.size();
        } else {
          split_right_subgid = id_vec;
          split_right_rep_idx = std::move(id_vec);
          split_right_subgid_ptr = split_right_subgid.data();
          split_right_rep_ptr = split_right_rep_idx.data();
          split_right_rep_count = split_right_rep_idx.size();
        }
        *out_card = card;
        return true;
      }
    }
    const auto fill_right_hashes =
        [&](std::vector<uint64_t>& hashes) -> Status {
      const auto mix_step = [](uint64_t& h, uint64_t v) noexcept {
        h = std::rotl(h, 7);
        h ^= v;
        h *= 0x100000001b3ULL;
      };
      if (input.right_col_only && input.right_arg.columnar != nullptr) {
        const auto& chunks = input.right_arg.columnar->chunks;
        struct HashSlice {
          size_t ci;
          size_t rb;
          size_t re;
          size_t out_base;
        };
        std::vector<HashSlice> slices;
        slices.reserve(chunks.size());
        size_t base = 0;
        for (size_t ci = 0; ci < chunks.size(); ++ci) {
          const size_t rows = chunks[ci].rows;
          if (rows == 0) {
            continue;
          }
          constexpr size_t kSliceRows = 4096;
          for (size_t rb = 0; rb < rows; rb += kSliceRows) {
            const size_t re = std::min(rows, rb + kSliceRows);
            slices.push_back(
                HashSlice{.ci = ci, .rb = rb, .re = re, .out_base = base + rb});
          }
          base += rows;
        }
        auto run_slice_worker = [&](size_t s_begin, size_t s_end) {
          std::vector<uint64_t> dict_hash;
          std::vector<uint8_t> dict_valid;
          Value v_tmp;
          for (size_t si = s_begin; si < s_end; ++si) {
            const HashSlice& slice = slices[si];
            const size_t n = slice.re - slice.rb;
            uint64_t* dst = hashes.data() + slice.out_base;
            std::fill_n(dst, n, uint64_t{0x9e3779b97f4a7c15ULL});
            const auto& cols = chunks[slice.ci].columns;
            for (const size_t column : col_indices) {
              const ColumnarColumn& sc = cols[column];
              if (sc.encoding == ColumnarColumn::Encoding::kConstant &&
                  sc.is_uniform) {
                uint64_t ch_hash = 0;
                if (!sc.const_is_null && sc.type != ValueType::kNull) {
                  if (sc.type == ValueType::kVarChar && !sc.is_interval) {
                    ch_hash = std::hash<std::string_view>{}(sc.const_string);
                  } else if ((sc.type == ValueType::kInt64 ||
                              sc.type == ValueType::kDate) &&
                             !sc.is_unsigned) {
                    ch_hash = splitmix_i64(sc.const_int);
                  } else {
                    sc.WriteValueInto(slice.rb, &v_tmp);
                    ch_hash = HashFusedKeyValue(v_tmp);
                  }
                }
                for (size_t i = 0; i < n; ++i) {
                  mix_step(dst[i], ch_hash);
                }
              } else if (sc.encoding == ColumnarColumn::Encoding::kDictionary &&
                         sc.is_uniform && !sc.is_interval &&
                         sc.type == ValueType::kVarChar && sc.dict != nullptr) {
                const size_t entry_count = sc.DictEntryCount();
                const bool cache_dict = entry_count <= 8192;
                if (cache_dict) {
                  dict_valid.assign(entry_count, 0);
                  if (dict_hash.size() < entry_count) {
                    dict_hash.resize(entry_count);
                  }
                }
                if (!sc.has_null && sc.borrowed_dict_u8 != nullptr) {
                  const uint8_t* codes = sc.borrowed_dict_u8 + slice.rb;
                  for (size_t i = 0; i < n; ++i) {
                    const uint32_t code = codes[i];
                    uint64_t hv = 0;
                    if (code < entry_count) [[likely]] {
                      if (cache_dict) {
                        if (dict_valid[code] != 0) [[likely]] {
                          hv = dict_hash[code];
                        } else {
                          hv = std::hash<std::string_view>{}(
                              sc.dict->StringEntryAt(code));
                          dict_hash[code] = hv;
                          dict_valid[code] = 1;
                        }
                      } else {
                        hv = std::hash<std::string_view>{}(
                            sc.dict->StringEntryAt(code));
                      }
                    }
                    mix_step(dst[i], hv);
                  }
                } else {
                  for (size_t i = 0; i < n; ++i) {
                    const uint32_t code = sc.DictCodeAt(slice.rb + i);
                    uint64_t hv = 0;
                    if ((!sc.has_null ||
                         code != ColumnarColumn::kNullDictCode) &&
                        code < entry_count) [[likely]] {
                      if (cache_dict) {
                        if (dict_valid[code] != 0) [[likely]] {
                          hv = dict_hash[code];
                        } else {
                          hv = std::hash<std::string_view>{}(
                              sc.dict->StringEntryAt(code));
                          dict_hash[code] = hv;
                          dict_valid[code] = 1;
                        }
                      } else {
                        hv = std::hash<std::string_view>{}(
                            sc.dict->StringEntryAt(code));
                      }
                    }
                    mix_step(dst[i], hv);
                  }
                }
              } else if (sc.encoding == ColumnarColumn::Encoding::kDictionary &&
                         sc.is_uniform && !sc.is_unsigned &&
                         (sc.type == ValueType::kInt64 ||
                          sc.type == ValueType::kDate) &&
                         sc.dict != nullptr) {
                const size_t entry_count = sc.DictEntryCount();
                const bool cache_dict = entry_count <= 8192;
                if (cache_dict) {
                  dict_valid.assign(entry_count, 0);
                  if (dict_hash.size() < entry_count) {
                    dict_hash.resize(entry_count);
                  }
                }
                for (size_t i = 0; i < n; ++i) {
                  const uint32_t code = sc.DictCodeAt(slice.rb + i);
                  uint64_t hv = 0;
                  if ((!sc.has_null || code != ColumnarColumn::kNullDictCode) &&
                      code < entry_count) [[likely]] {
                    if (cache_dict) {
                      if (dict_valid[code] != 0) [[likely]] {
                        hv = dict_hash[code];
                      } else {
                        hv = splitmix_i64(sc.dict->IntEntryAt(code));
                        dict_hash[code] = hv;
                        dict_valid[code] = 1;
                      }
                    } else {
                      hv = splitmix_i64(sc.dict->IntEntryAt(code));
                    }
                  }
                  mix_step(dst[i], hv);
                }
              } else if (sc.encoding == ColumnarColumn::Encoding::kFlat &&
                         sc.is_uniform && sc.type == ValueType::kVarChar &&
                         !sc.is_interval && !sc.strings.empty()) {
                const std::string_view* strs = sc.strings.data() + slice.rb;
                if (!sc.has_null || sc.nulls.empty()) {
                  for (size_t i = 0; i < n; ++i) {
                    mix_step(dst[i], std::hash<std::string_view>{}(strs[i]));
                  }
                } else {
                  const uint8_t* nul = sc.nulls.data() + slice.rb;
                  for (size_t i = 0; i < n; ++i) {
                    const uint64_t hv =
                        nul[i] != 0 ? 0ULL
                                    : std::hash<std::string_view>{}(strs[i]);
                    mix_step(dst[i], hv);
                  }
                }
              } else if (sc.is_uniform && !sc.is_unsigned &&
                         (sc.type == ValueType::kInt64 ||
                          sc.type == ValueType::kDate) &&
                         ((sc.encoding == ColumnarColumn::Encoding::kFlat &&
                           !sc.ints.empty()) ||
                          sc.encoding ==
                              ColumnarColumn::Encoding::kFrameOfReference)) {
                if (sc.encoding == ColumnarColumn::Encoding::kFlat &&
                    (!sc.has_null || sc.nulls.empty())) {
                  const int64_t* ivs = sc.ints.data() + slice.rb;
                  for (size_t i = 0; i < n; ++i) {
                    mix_step(dst[i], splitmix_i64(ivs[i]));
                  }
                } else {
                  for (size_t i = 0; i < n; ++i) {
                    const uint64_t hv =
                        sc.NonGatheredIsNull(slice.rb + i)
                            ? 0ULL
                            : splitmix_i64(
                                  sc.encoding == ColumnarColumn::Encoding::kFlat
                                      ? sc.ints[slice.rb + i]
                                      : sc.ForIntAt(slice.rb + i));
                    mix_step(dst[i], hv);
                  }
                }
              } else {
                for (size_t i = 0; i < n; ++i) {
                  sc.WriteValueInto(slice.rb + i, &v_tmp);
                  mix_step(dst[i], HashFusedKeyValue(v_tmp));
                }
              }
            }
          }
        };
        if (right_row_count >= 16384 && slices.size() > 1) {
          const size_t hw = std::min(
              slices.size(), ParallelWorkerCount(right_row_count / 4096));
          if (hw > 1) {
            std::atomic<size_t> next_slice{0};
            return RunParallelTasks(hw, [&](size_t) {
              while (true) {
                const size_t si =
                    next_slice.fetch_add(1, std::memory_order_relaxed);
                if (si >= slices.size()) {
                  break;
                }
                run_slice_worker(si, si + 1);
              }
            });
          }
        }
        run_slice_worker(0, slices.size());
        return Status::kSuccess;
      }
      const auto hash_right_row = [&](size_t r) {
        Value v_tmp;
        uint64_t hash = 0x9e3779b97f4a7c15ULL;
        for (const size_t column : col_indices) {
          const auto& value =
              input.read_right_cell(static_cast<uint32_t>(r), column, &v_tmp);
          mix_step(hash, HashFusedKeyValue(value));
        }
        return hash;
      };
      if (right_row_count >= 16384) {
        const size_t hw = ParallelWorkerCount(right_row_count / 4096);
        return RunParallelTasks(hw, [&](size_t worker) {
          const size_t b = (right_row_count * worker) / hw;
          const size_t e = (right_row_count * (worker + 1)) / hw;
          for (size_t r = b; r < e; ++r) {
            hashes[r] = hash_right_row(r);
          }
        });
      }
      for (size_t r = 0; r < right_row_count; ++r) {
        hashes[r] = hash_right_row(r);
      }
      return Status::kSuccess;
    };
    const auto eq_right = [&](size_t a, size_t b) {
      if (input.right_col_only && input.right_arg.columnar != nullptr) {
        const auto [ca, ra] =
            input.right_locator.Locate(static_cast<uint32_t>(a));
        const auto [cb, rb] =
            input.right_locator.Locate(static_cast<uint32_t>(b));
        const auto& cols_a = input.right_arg.columnar->chunks[ca].columns;
        const auto& cols_b = input.right_arg.columnar->chunks[cb].columns;
        Value x_tmp;
        Value y_tmp;
        for (const size_t column : col_indices) {
          const ColumnarColumn& sca = cols_a[column];
          const ColumnarColumn& scb = cols_b[column];
          if (sca.is_uniform && scb.is_uniform &&
              sca.encoding != ColumnarColumn::Encoding::kGathered &&
              scb.encoding != ColumnarColumn::Encoding::kGathered) {
            const bool na = sca.NonGatheredIsNull(ra);
            const bool nb = scb.NonGatheredIsNull(rb);
            if (na != nb) {
              return false;
            }
            if (na) {
              continue;
            }
            if (sca.type == ValueType::kVarChar &&
                scb.type == ValueType::kVarChar && !sca.is_interval &&
                !scb.is_interval) {
              if (sca.StringAt(ra) != scb.StringAt(rb)) {
                return false;
              }
              continue;
            }
            if ((sca.type == ValueType::kInt64 ||
                 sca.type == ValueType::kDate) &&
                sca.type == scb.type && sca.is_unsigned == scb.is_unsigned) {
              if (sca.IntAt(ra) != scb.IntAt(rb)) {
                return false;
              }
              continue;
            }
          }
          sca.WriteValueInto(ra, &x_tmp);
          scb.WriteValueInto(rb, &y_tmp);
          const bool eq =
              x_tmp.IsCaseInsensitive() || y_tmp.IsCaseInsensitive()
                  ? DistinctValueEqual{}(CanonicalDistinctValue(x_tmp),
                                         CanonicalDistinctValue(y_tmp))
                  : DistinctValueEqual{}(x_tmp, y_tmp);
          if (!eq) {
            return false;
          }
        }
        return true;
      }
      Value x_tmp;
      Value y_tmp;
      return std::ranges::all_of(col_indices, [&](size_t column) {
        const auto& x =
            input.read_right_cell(static_cast<uint32_t>(a), column, &x_tmp);
        const auto& y =
            input.read_right_cell(static_cast<uint32_t>(b), column, &y_tmp);
        return x.IsCaseInsensitive() || y.IsCaseInsensitive()
                   ? DistinctValueEqual{}(CanonicalDistinctValue(x),
                                          CanonicalDistinctValue(y))
                   : DistinctValueEqual{}(x, y);
      });
    };
    std::vector<uint32_t> rep_idxs;
    if (!build_flat_subgid_dict(
            right_row_count, std::numeric_limits<size_t>::max(),
            fill_right_hashes, eq_right, &split_right_subgid, out_card, nullptr,
            &rep_idxs)) {
      clear_split_right_dict();
      return false;
    }
    if (right_dict_guard.active && input.shared_pk_cache != nullptr) {
      auto cached =
          std::make_shared<SharedBaseRelationCache::CachedSplitRightDict>();
      cached->subgid_count = *out_card;
      cached->subgid = std::move(split_right_subgid);
      cached->rep_idx = std::move(rep_idxs);
      std::shared_ptr<const SharedBaseRelationCache::CachedSplitRightDict>
          shared_rd = std::move(cached);
      input.shared_pk_cache->PublishSplitRightDict(right_dict_key, shared_rd);
      right_dict_guard.active = false;
      shared_right_dict = std::move(shared_rd);
      split_right_subgid_ptr = shared_right_dict->subgid.data();
      split_right_rep_ptr = shared_right_dict->rep_idx.data();
      split_right_rep_count = shared_right_dict->rep_idx.size();
    } else {
      split_right_rep_idx = std::move(rep_idxs);
      split_right_subgid_ptr = split_right_subgid.data();
      split_right_rep_ptr = split_right_rep_idx.data();
      split_right_rep_count = split_right_rep_idx.size();
    }
    return true;
  };
  if (!input.int_group_key && !input.left.HasSpill() &&
      !input.right_ptr->HasSpill() && input.key_programs.size() == 2 &&
      right_row_count > 0 && right_row_count <= 262144) {
    for (size_t rk = 0; rk < 2; ++rk) {
      const size_t lk = 1 - rk;
      if (std::cmp_less(input.key_shapes[rk].direct_column, input.left_width)) {
        continue;
      }
      const bool left_is_extract =
          input.key_shapes[lk].extract_date_col >= 0 &&
          std::cmp_less(input.key_shapes[lk].extract_date_col,
                        input.left_width);
      bool left_is_int_col = false;
      if (input.key_shapes[lk].direct_column >= 0 &&
          std::cmp_less(input.key_shapes[lk].direct_column, input.left_width)) {
        const Column& lcol = input.left.schema.GetColumn(
            static_cast<size_t>(input.key_shapes[lk].direct_column));
        left_is_int_col = (lcol.Type() == ValueType::kInt64 ||
                           lcol.Type() == ValueType::kDate) &&
                          !lcol.IsUnsigned();
      }
      if (!left_is_extract && !left_is_int_col) {
        continue;
      }
      const auto rcol = static_cast<slot_t>(
          static_cast<size_t>(input.key_shapes[rk].direct_column) -
          input.left_width);
      if (rcol >= input.right.schema.ColumnCount()) {
        continue;
      }
      const Column& rcol_def = input.right.schema.GetColumn(rcol);
      Value rv_tmp;
      if (rcol_def.Type() == ValueType::kVarChar) {
        split_right_subgid.assign(right_row_count,
                                  std::numeric_limits<uint32_t>::max());
        // Columnar reads may use rv_tmp's owned short-string buffer.
        // Keep dictionary keys and the consecutive-key shortcut independent
        // of that scratch value.
        std::unordered_map<std::string, uint32_t> sv_to_gid;
        sv_to_gid.reserve(64);
        std::vector<uint32_t> local_rep_idxs;
        local_rep_idxs.reserve(64);
        std::string_view last_sv;
        uint32_t last_gid = std::numeric_limits<uint32_t>::max();
        bool all_ok = true;
        for (size_t ri = 0; ri < right_row_count; ++ri) {
          const Value& rv =
              input.read_right_cell(static_cast<uint32_t>(ri), rcol, &rv_tmp);
          if (rv.IsNull() || rv.type != ValueType::kVarChar ||
              rv.IsCaseInsensitive()) {
            all_ok = false;
            break;
          }
          const std::string_view sv = rv.value.varchar_value;
          if (last_gid != std::numeric_limits<uint32_t>::max() &&
              sv == last_sv) {
            split_right_subgid[ri] = last_gid;
            continue;
          }
          if (!sv.empty() && ((sv[0] >= '0' && sv[0] <= '9') || sv[0] == '-' ||
                              sv[0] == '+' || sv[0] == ' ')) {
            all_ok = false;
            break;
          }
          const auto next_id = static_cast<uint32_t>(sv_to_gid.size());
          const auto [it, ins] = sv_to_gid.emplace(std::string(sv), next_id);
          if (sv_to_gid.size() > 65535) {
            all_ok = false;
            break;
          }
          if (ins) {
            local_rep_idxs.push_back(static_cast<uint32_t>(ri));
          }
          last_sv = it->first;
          last_gid = it->second;
          split_right_subgid[ri] = last_gid;
        }
        if (all_ok) {
          use_split_key_gid = true;
          split_has_left_probe = true;
          split_uses_right_subgid = true;
          split_right_shift = 32;
          split_left_k = lk;
          split_right_subgid_count = sv_to_gid.size();
          split_right_rep_idx = std::move(local_rep_idxs);
          split_right_subgid_ptr = split_right_subgid.data();
          split_right_rep_ptr = split_right_rep_idx.data();
          split_right_rep_count = split_right_rep_idx.size();
          break;
        }
        clear_split_right_dict();
      } else if ((rcol_def.Type() == ValueType::kInt64 ||
                  rcol_def.Type() == ValueType::kDate) &&
                 !rcol_def.IsUnsigned()) {
        split_right_subgid.assign(right_row_count,
                                  std::numeric_limits<uint32_t>::max());
        FlatIntKeyMap<uint32_t> int_to_gid;
        int_to_gid.Reserve(256);
        std::vector<uint32_t> local_rep_idxs;
        local_rep_idxs.reserve(256);
        uint32_t next_id = 0;
        bool all_ok = true;
        for (size_t ri = 0; ri < right_row_count; ++ri) {
          const Value& rv =
              input.read_right_cell(static_cast<uint32_t>(ri), rcol, &rv_tmp);
          if (rv.IsNull() || rv.IsUnsigned()) {
            all_ok = false;
            break;
          }
          bool ins = false;
          uint32_t* slot = int_to_gid.TryEmplace(rv.value.int_value, &ins);
          if (ins) {
            if (next_id >= 65535) {
              all_ok = false;
              break;
            }
            *slot = next_id++;
            local_rep_idxs.push_back(static_cast<uint32_t>(ri));
          }
          split_right_subgid[ri] = *slot;
        }
        if (all_ok) {
          use_split_key_gid = true;
          split_has_left_probe = true;
          split_uses_right_subgid = true;
          split_right_shift = 32;
          split_left_k = lk;
          split_right_subgid_count = next_id;
          split_right_rep_idx = std::move(local_rep_idxs);
          split_right_subgid_ptr = split_right_subgid.data();
          split_right_rep_ptr = split_right_rep_idx.data();
          split_right_rep_count = split_right_rep_idx.size();
          break;
        }
        clear_split_right_dict();
      }
    }
  }

  if (!use_split_key_gid && !input.left.HasSpill() &&
      !input.right_ptr->HasSpill() && !input.int_group_key &&
      !input.key_programs.empty() && input.key_programs.size() <= 16) {
    // Factor finite right and deferred-PK dimension tuples once per query so
    // every surviving probe row probes one exact packed 64-bit integer key.
    std::vector<size_t> dimension_columns;
    dimension_columns.reserve(input.key_shapes.size());
    std::vector<std::vector<size_t>> pk_group_key_lookups(
        input.def_pk_groups.size());
    std::optional<size_t> probe_key;
    size_t left_key_count = 0;
    for (const auto& shape : input.key_shapes) {
      if ((shape.direct_column >= 0 &&
           std::cmp_less(shape.direct_column, input.left_width)) ||
          (shape.extract_date_col >= 0 &&
           std::cmp_less(shape.extract_date_col, input.left_width))) {
        ++left_key_count;
      }
    }
    const bool right_available =
        right_row_count > 0 && right_row_count <= 1048576;
    bool has_any_right_key = false;
    bool supported = true;
    for (size_t k = 0; k < input.key_shapes.size(); ++k) {
      const auto& shape = input.key_shapes[k];
      if (shape.direct_column >= 0 &&
          std::cmp_greater_equal(shape.direct_column, input.left_width)) {
        if (!right_available) {
          supported = false;
          break;
        }
        dimension_columns.push_back(static_cast<size_t>(shape.direct_column) -
                                    input.left_width);
        has_any_right_key = true;
        continue;
      }
      if (shape.direct_column >= 0 &&
          std::cmp_less(shape.direct_column, input.left_width)) {
        const auto lcol = static_cast<size_t>(shape.direct_column);
        const Column& column = input.left.schema.GetColumn(lcol);
        const bool integer_probe =
            !column.IsUnsigned() && (column.Type() == ValueType::kInt64 ||
                                     column.Type() == ValueType::kDate);
        const int li = input.deferred_left_probe != nullptr && lcol < 16
                           ? static_cast<int>(input.def_col_to_pk_lookup[lcol])
                           : -1;
        const size_t gi =
            li >= 0
                ? input.prepared_pk_lookups[static_cast<size_t>(li)].group_idx
                : 0;
        const bool pk_eligible =
            li >= 0 && input.def_pk_groups[gi].total_rows <= 262144;
        bool bypass_prefer_probe = false;
        if (pk_eligible && input.deferred_left_probe != nullptr &&
            input.def_pk_groups.size() == 1 && input.right_col_only &&
            right_row_count > 0 && right_row_count <= 2097152) {
          if (input.def_pk_groups[gi].total_rows <= 4096) {
            bypass_prefer_probe = true;
          } else {
            const slot_t pkc = input.def_pk_groups[gi].pk_col;
            const ScanRuntimeFilter& prf =
                input.deferred_left_probe->runtime_filter;
            if (prf.enabled() && prf.column == pkc) {
              bypass_prefer_probe = true;
            } else {
              for (const ScanRuntimeFilter& ext : prf.extra) {
                if (ext.enabled() && ext.column == pkc) {
                  bypass_prefer_probe = true;
                  break;
                }
              }
            }
          }
        }
        // Keep a sole INT64/DATE left key paired with right keys in the low 32
        // bits so split_key_unpack_only still avoids cloning left rep rows,
        // unless encoding the PK key enables dense/JIT split direct GIDs.
        const bool prefer_probe_slot =
            integer_probe && !probe_key && left_key_count == 1 &&
            has_any_right_key && !bypass_prefer_probe;
        if (pk_eligible && !prefer_probe_slot) {
          const auto uli = static_cast<size_t>(li);
          pk_group_key_lookups[input.prepared_pk_lookups[uli].group_idx]
              .push_back(uli);
          continue;
        }
        if (li < 0 && right_available && input.join_type == JoinType::kInner &&
            integer_probe) {
          const auto eq_it = std::ranges::find_if(
              input.equality_keys, [&](const EqualityKey& eq) {
                if (eq.left != lcol || eq.left_offset != 0 ||
                    eq.right_offset != 0 || eq.null_safe ||
                    eq.right >= input.right.schema.ColumnCount()) {
                  return false;
                }
                const Column& rc = input.right.schema.GetColumn(eq.right);
                return rc.Type() == column.Type() && !rc.IsUnsigned();
              });
          if (eq_it != input.equality_keys.end()) {
            if (std::ranges::find(dimension_columns, eq_it->right) ==
                dimension_columns.end()) {
              dimension_columns.push_back(eq_it->right);
            }
            has_any_right_key = true;
            continue;
          }
        }
        if (!probe_key && integer_probe) {
          probe_key = k;
          continue;
        }
        supported = false;
        break;
      }
      if (shape.extract_date_col >= 0 &&
          std::cmp_less(shape.extract_date_col, input.left_width) &&
          !probe_key) {
        probe_key = k;
        continue;
      }
      supported = false;
      break;
    }
    const bool any_pk_dict = std::ranges::any_of(
        pk_group_key_lookups,
        [](const std::vector<size_t>& v) { return !v.empty(); });
    if (supported && (!dimension_columns.empty() || any_pk_dict)) {
      size_t charge_bytes = 0;
      if (!dimension_columns.empty()) {
        charge_bytes +=
            (input.right_col_only ? 0 : input.right_ptr->charged_bytes_) +
            (right_row_count * (64 + dimension_columns.size() * sizeof(Value)));
      }
      for (size_t gi = 0; gi < input.def_pk_groups.size(); ++gi) {
        if (!pk_group_key_lookups[gi].empty()) {
          charge_bytes +=
              static_cast<size_t>(input.def_pk_groups[gi].total_rows) *
              (32 + pk_group_key_lookups[gi].size() * 8);
        }
      }
      split_dictionary_charge = TryReserveQueryMemory(charge_bytes);
      if (split_dictionary_charge) {
        bool dict_ok = true;
        uint32_t right_card = 0;
        if (!dimension_columns.empty()) {
          dict_ok =
              build_or_reuse_split_right_dict(dimension_columns, &right_card);
        }
        std::vector<std::vector<std::pair<uint32_t, uint32_t>>> pk_locs(
            input.def_pk_groups.size());
        for (size_t gi = 0; dict_ok && gi < input.def_pk_groups.size(); ++gi) {
          if (pk_group_key_lookups[gi].empty()) {
            continue;
          }
          PreparedDeferredPkGroup& grp = input.def_pk_groups[gi];
          const auto& lkp_idxs = pk_group_key_lookups[gi];
          const size_t nb_count = grp.blocks.size();
          std::vector<const RuntimeFilterGroup*> pk_rf_groups;
          if (input.deferred_left_probe != nullptr &&
              grp.pk_col < input.left.schema.ColumnCount() &&
              !input.left.schema.GetColumn(grp.pk_col).IsUnsigned()) {
            const ValueType col_type =
                input.left.schema.GetColumn(grp.pk_col).Type();
            const ScanRuntimeFilter& prf =
                input.deferred_left_probe->runtime_filter;
            if (prf.enabled() && prf.column == grp.pk_col &&
                prf.type == col_type) {
              pk_rf_groups.push_back(prf.group.get());
            }
            for (const ScanRuntimeFilter& ext : prf.extra) {
              if (ext.enabled() && ext.column == grp.pk_col &&
                  ext.type == col_type) {
                pk_rf_groups.push_back(ext.group.get());
              }
            }
          }
          std::string pk_dict_key;
          struct PkDictSlotGuard {
            SharedBaseRelationCache* cache{nullptr};
            std::string key;
            bool active{false};
            PkDictSlotGuard() = default;
            PkDictSlotGuard(SharedBaseRelationCache* c, std::string k, bool a)
                : cache(c), key(std::move(k)), active(a) {}
            PkDictSlotGuard(const PkDictSlotGuard&) = delete;
            PkDictSlotGuard& operator=(const PkDictSlotGuard&) = delete;
            PkDictSlotGuard(PkDictSlotGuard&&) = delete;
            PkDictSlotGuard& operator=(PkDictSlotGuard&&) = delete;
            ~PkDictSlotGuard() {
              if (active && cache != nullptr) {
                cache->AbandonPkDict(key);
              }
            }
          } pk_dict_guard(input.shared_pk_cache, {}, false);
          if (pk_rf_groups.empty() && input.shared_pk_cache != nullptr &&
              grp.store != nullptr) {
            pk_dict_key =
                "P:" + std::to_string(reinterpret_cast<uintptr_t>(grp.store)) +
                ":" + std::to_string(grp.store->layout_generation) + ":" +
                std::to_string(grp.store_pk_col) + ":" +
                std::to_string(grp.total_rows);
            for (const size_t li : lkp_idxs) {
              const auto& prep = input.prepared_pk_lookups[li];
              pk_dict_key +=
                  ":" + std::to_string(prep.store_payload_col) + "," +
                  std::to_string(static_cast<uint32_t>(prep.payload_type)) +
                  "," + (prep.payload_unsigned ? "1" : "0");
            }
            const bool want_dense = grp.dense_ptr != nullptr;
            pk_dict_key += ":" + std::to_string(want_dense ? 1 : 0);
            if (want_dense) {
              pk_dict_key += ":" + std::to_string(grp.min_k) + ":" +
                             std::to_string(grp.max_k) + ":" +
                             std::to_string(grp.dense_size);
            }
            std::shared_ptr<const SharedBaseRelationCache::CachedPkDict> hit;
            if (!input.shared_pk_cache->AcquirePkDictSlot(pk_dict_key, &hit)) {
              if (hit != nullptr) {
                grp.shared_pk_dict = std::move(hit);
                grp.use_subgid = true;
                grp.subgid_count = grp.shared_pk_dict->subgid_count;
                grp.subgid_ptr = grp.shared_pk_dict->subgid.data();
                grp.subgid_rep_ptr = grp.shared_pk_dict->subgid_rep_loc.data();
                if (grp.shared_pk_dict->has_dense) {
                  grp.dense_ptr = grp.shared_pk_dict->remapped_dense.data();
                  grp.dense_size = grp.shared_pk_dict->remapped_dense.size();
                }
                continue;
              }
            } else {
              pk_dict_guard.key = pk_dict_key;
              pk_dict_guard.active = true;
            }
          }
          pk_locs[gi].resize(grp.total_rows);
          std::vector<uint8_t> pk_active;
          if (!pk_rf_groups.empty()) {
            pk_active.assign(grp.total_rows, 1);
          }
          const auto fill_pk_hashes =
              [&](std::vector<uint64_t>& hashes) -> Status {
            const auto fill_blk = [&](uint32_t nb, Value& tmp) {
              const auto& gb = grp.blocks[nb];
              const uint32_t base = gb.row_base;
              for (uint32_t rin = 0; rin < gb.row_count; ++rin) {
                pk_locs[gi][base + rin] = {nb, rin};
                if (!pk_active.empty()) {
                  const int64_t pk_key = gb.pk_cells[rin];
                  bool rejected = false;
                  for (const RuntimeFilterGroup* rf_g : pk_rf_groups) {
                    if (rf_g->Reject(pk_key)) {
                      rejected = true;
                      break;
                    }
                  }
                  if (rejected) {
                    pk_active[base + rin] = 0;
                    continue;
                  }
                }
                uint64_t hash = 0x9e3779b97f4a7c15ULL;
                for (const size_t li : lkp_idxs) {
                  input.read_pk_cell(li, nb, rin, &tmp);
                  hash = std::rotl(hash, 7);
                  hash ^= HashFusedKeyValue(tmp);
                  hash *= 0x100000001b3ULL;
                }
                hashes[base + rin] = hash;
              }
            };
            const size_t pk_workers = std::min<size_t>(
                ParallelWorkerCount(grp.total_rows / 16384), nb_count);
            if (pk_workers > 1 && grp.total_rows >= 16384 && nb_count >= 2) {
              std::atomic<uint32_t> next_nb{0};
              const auto total_nb = static_cast<uint32_t>(nb_count);
              return RunParallelTasks(pk_workers, [&](size_t) {
                Value tmp;
                uint32_t nb = 0;
                while ((nb = next_nb.fetch_add(1, std::memory_order_relaxed)) <
                       total_nb) {
                  fill_blk(nb, tmp);
                }
              });
            }
            Value tmp;
            for (uint32_t nb = 0; nb < nb_count; ++nb) {
              fill_blk(nb, tmp);
            }
            return Status::kSuccess;
          };
          const auto eq_pk = [&](size_t a, size_t b) {
            const auto [nba, ra] = pk_locs[gi][a];
            const auto [nbb, rb] = pk_locs[gi][b];
            Value va;
            Value vb;
            for (const size_t li : lkp_idxs) {
              input.read_pk_cell(li, nba, ra, &va);
              input.read_pk_cell(li, nbb, rb, &vb);
              if (!DistinctValueEqual{}(va, vb)) {
                return false;
              }
            }
            return true;
          };
          std::vector<uint32_t> pk_rep_rows;
          dict_ok = build_flat_subgid_dict(
              grp.total_rows, std::numeric_limits<size_t>::max(),
              fill_pk_hashes, eq_pk, &grp.subgid, &grp.subgid_count,
              pk_active.empty() ? nullptr : &pk_active, &pk_rep_rows);
          if (dict_ok) {
            grp.use_subgid = true;
            grp.subgid_rep_loc.resize(pk_rep_rows.size());
            for (size_t id = 0; id < pk_rep_rows.size(); ++id) {
              grp.subgid_rep_loc[id] = pk_locs[gi][pk_rep_rows[id]];
            }
            if (pk_dict_guard.active && input.shared_pk_cache != nullptr) {
              auto cached =
                  std::make_shared<SharedBaseRelationCache::CachedPkDict>();
              cached->store_owner = grp.store_owner;
              cached->subgid_count = grp.subgid_count;
              cached->subgid = std::move(grp.subgid);
              cached->subgid_rep_loc = std::move(grp.subgid_rep_loc);
              if (grp.dense_ptr != nullptr && grp.dense_size > 0) {
                cached->has_dense = true;
                cached->min_k = grp.min_k;
                cached->max_k = grp.max_k;
                cached->span = grp.span;
                cached->remapped_dense.assign(grp.dense_ptr,
                                              grp.dense_ptr + grp.dense_size);
                for (auto& entry : cached->remapped_dense) {
                  if (entry.nb != std::numeric_limits<uint32_t>::max()) {
                    entry.subgid = cached->subgid[entry.subgid];
                  }
                }
              }
              std::shared_ptr<const SharedBaseRelationCache::CachedPkDict>
                  shared_pd = std::move(cached);
              input.shared_pk_cache->PublishPkDict(pk_dict_key, shared_pd);
              pk_dict_guard.active = false;
              grp.shared_pk_dict = std::move(shared_pd);
              grp.subgid_ptr = grp.shared_pk_dict->subgid.data();
              grp.subgid_rep_ptr = grp.shared_pk_dict->subgid_rep_loc.data();
              if (grp.shared_pk_dict->has_dense) {
                grp.dense_ptr = grp.shared_pk_dict->remapped_dense.data();
                grp.dense_size = grp.shared_pk_dict->remapped_dense.size();
              }
            } else {
              grp.subgid_ptr = grp.subgid.data();
              grp.subgid_rep_ptr = grp.subgid_rep_loc.data();
            }
          }
        }
        uint32_t bit_cursor = probe_key.has_value() ? 32U : 0U;
        if (dict_ok && !dimension_columns.empty()) {
          const uint32_t r_bits =
              right_card <= 1
                  ? 0U
                  : static_cast<uint32_t>(std::bit_width(right_card - 1));
          if (bit_cursor + r_bits <= 64U) {
            split_uses_right_subgid = true;
            split_right_shift = static_cast<uint8_t>(bit_cursor);
            split_right_subgid_count = right_card;
            bit_cursor += r_bits;
          } else {
            dict_ok = false;
          }
        }
        for (size_t gi = 0; dict_ok && gi < input.def_pk_groups.size(); ++gi) {
          if (!input.def_pk_groups[gi].use_subgid) {
            continue;
          }
          const uint32_t p_card = input.def_pk_groups[gi].subgid_count;
          const uint32_t p_bits =
              p_card <= 1 ? 0U
                          : static_cast<uint32_t>(std::bit_width(p_card - 1));
          // A zero-width field at offset 64 has no representable shift; a
          // shift of 64 on uint64 is UB, so fall back to the generic path.
          if (bit_cursor < 64U && bit_cursor + p_bits <= 64U) {
            input.def_pk_groups[gi].subgid_shift =
                static_cast<uint8_t>(bit_cursor);
            split_uses_pk_subgid = true;
            bit_cursor += p_bits;
          } else {
            dict_ok = false;
          }
        }
        if (dict_ok && bit_cursor <= 64U) {
          use_split_key_gid = true;
          split_has_left_probe = probe_key.has_value();
          split_left_k = probe_key.value_or(input.key_shapes.size());
          for (PreparedDeferredPkGroup& grp : input.def_pk_groups) {
            if (!grp.use_subgid || grp.dense_ptr == nullptr ||
                grp.shared_pk_dict != nullptr) {
              continue;
            }
            if (grp.shared_dense != nullptr) {
              grp.dense_index = *grp.shared_dense;
              grp.shared_dense.reset();
              grp.dense_ptr = grp.dense_index.data();
            }
            for (auto& entry : grp.dense_index) {
              if (entry.nb != std::numeric_limits<uint32_t>::max()) {
                entry.subgid = grp.subgid_ptr[entry.subgid];
              }
            }
          }
        } else {
          clear_split_right_dict();
          split_uses_right_subgid = false;
          split_uses_pk_subgid = false;
          for (auto& grp : input.def_pk_groups) {
            grp.subgid.clear();
            grp.subgid_rep_loc.clear();
            grp.shared_pk_dict.reset();
            grp.subgid_ptr = nullptr;
            grp.subgid_rep_ptr = nullptr;
            grp.subgid_count = 0;
            grp.use_subgid = false;
            if (grp.shared_dense != nullptr) {
              grp.dense_ptr = grp.shared_dense->data();
              grp.dense_size = grp.shared_dense->size();
            } else {
              grp.dense_ptr =
                  grp.dense_index.empty() ? nullptr : grp.dense_index.data();
              grp.dense_size = grp.dense_index.size();
            }
          }
          split_dictionary_charge.reset();
        }
      }
    }
  }
  use_fast_2key_year_lut =
      use_split_key_gid && split_has_left_probe && !split_uses_pk_subgid &&
      split_uses_right_subgid && split_right_subgid_count <= 64 &&
      input.key_shapes[split_left_k].extract_date_col >= 0 &&
      input.key_shapes[split_left_k].extract_date_part == 0;
  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/PackedRadix2x32BatchAgg.lean (sound)
  packed_2x32_group_key = false;
  packed_2x32_slot0 = 0;
  packed_2x32_slot1 = 0;
  packed_2x32_type0 = ValueType::kNull;
  packed_2x32_type1 = ValueType::kNull;
  if (!input.int_group_key && !use_split_key_gid && !input.left.HasSpill() &&
      !input.right_ptr->HasSpill() && input.key_programs.size() == 2 &&
      input.key_shapes[0].direct_column >= 0 &&
      input.key_shapes[1].direct_column >= 0) {
    const auto s0 = static_cast<slot_t>(input.key_shapes[0].direct_column);
    const auto s1 = static_cast<slot_t>(input.key_shapes[1].direct_column);
    const Column& c0 = input.result.schema.GetColumn(s0);
    const Column& c1 = input.result.schema.GetColumn(s1);
    const auto is_packable_type = [](const Column& col) {
      return !col.IsUnsigned() && (col.Type() == ValueType::kInt64 ||
                                   col.Type() == ValueType::kDate);
    };
    if (is_packable_type(c0) && is_packable_type(c1)) {
      packed_2x32_group_key = true;
      packed_2x32_slot0 = s0;
      packed_2x32_slot1 = s1;
      packed_2x32_type0 = c0.Type();
      packed_2x32_type1 = c1.Type();
    }
  }
  packed_2x32_unpack_only =
      packed_2x32_group_key && input.result.using_columns == nullptr &&
      !input.fusion->having && !input.fusion->post_having_active &&
      input.fusion->topn_limit == 0 && !input.fusion->rollup_count_distribution;
  if (packed_2x32_unpack_only) {
    for (const Expression& item : input.fusion->select_items) {
      if (!item) {
        packed_2x32_unpack_only = false;
        break;
      }
      if (item->Type() == TypeTag::kConstantValue &&
          !ContainsParameterSlot(item)) {
        continue;
      }
      if (item->Type() == TypeTag::kAggregateExp) {
        if (std::ranges::find(input.fusion->aggregates,
                              &item->AsAggregateExpression()) ==
            input.fusion->aggregates.end()) {
          packed_2x32_unpack_only = false;
          break;
        }
        continue;
      }
      if (item->Type() == TypeTag::kColumnValue) {
        const auto col_off = LocalColumnOffset(
            input.result.schema, item->AsColumnValue().GetColumnName());
        if (col_off.has_value() &&
            (*col_off == packed_2x32_slot0 || *col_off == packed_2x32_slot1)) {
          continue;
        }
      }
      packed_2x32_unpack_only = false;
      break;
    }
  }

  split_left_type = ValueType::kNull;

  if (input.fusion->having &&
      input.fusion->having->Type() == TypeTag::kBinaryExp) {
    const auto& predicate = input.fusion->having->AsBinaryExpression();
    if (predicate.Op() == BinaryOperation::kGreaterThan && predicate.Left() &&
        predicate.Left()->Type() == TypeTag::kAggregateExp &&
        predicate.Right() &&
        predicate.Right()->Type() == TypeTag::kConstantValue &&
        !ContainsParameterSlot(predicate.Right()) &&
        std::ranges::find(input.fusion->aggregates,
                          &predicate.Left()->AsAggregateExpression()) !=
            input.fusion->aggregates.end()) {
      const auto& threshold = predicate.Right()->AsConstantValue().GetValue();
      if (threshold.type == ValueType::kInt64 && !threshold.IsUnsigned() &&
          threshold.value.int_value >= 1) {
        compact_integer_threshold = threshold.value.int_value;
      }
    }
  }
  const bool compact_having_count_gt =
      input.raw_count_fusion && input.count_star_only &&
      (!input.fusion->having || compact_integer_threshold.has_value()) &&
      (!input.fusion->post_having_active ||
       (!input.fusion->post_having_is_self_sum_factor &&
        input.fusion->post_having_agg_slot == 0 &&
        input.fusion->post_having_op == BinaryOperation::kGreaterThan &&
        input.fusion->post_having_constant_or_factor >= 1.0)) &&
      (compact_integer_threshold.has_value() ||
       input.fusion->post_having_active);
  split_key_unpack_only =
      use_split_key_gid && (split_uses_right_subgid || split_uses_pk_subgid) &&
      (split_has_left_probe || input.join_type == JoinType::kInner) &&
      input.result.using_columns == nullptr &&
      ((!input.fusion->having && !input.fusion->post_having_active) ||
       compact_having_count_gt) &&
      input.fusion->topn_limit == 0 &&
      !input.fusion->rollup_count_distribution &&
      (input.raw_sum_fusion || input.raw_count_fusion ||
       input.fusion->aggregates.empty());
  if (split_key_unpack_only && split_has_left_probe) {
    if (input.key_shapes[split_left_k].direct_column >= 0 &&
        std::cmp_less(input.key_shapes[split_left_k].direct_column,
                      input.left_width)) {
      const auto slot =
          static_cast<slot_t>(input.key_shapes[split_left_k].direct_column);
      const Column& lcol = input.left.schema.GetColumn(slot);
      if (!lcol.IsUnsigned() && (lcol.Type() == ValueType::kInt64 ||
                                 lcol.Type() == ValueType::kDate)) {
        split_left_slot = slot;
        split_left_type = lcol.Type();
      } else {
        split_key_unpack_only = false;
      }
    } else if (input.key_shapes[split_left_k].extract_date_col < 0 ||
               std::cmp_greater_equal(
                   input.key_shapes[split_left_k].extract_date_col,
                   input.left_width)) {
      split_key_unpack_only = false;
    }
  }
  if (split_key_unpack_only) {
    auto col_unpackable = [&](size_t col_off) -> bool {
      if ((col_off >= input.left_width &&
           (split_uses_right_subgid || input.singleton_right_relation)) ||
          (split_has_left_probe && split_left_slot.has_value() &&
           col_off == *split_left_slot)) {
        return true;
      }
      if (col_off < 16 && input.def_col_to_pk_lookup[col_off] >= 0 &&
          input
              .def_pk_groups[input
                                 .prepared_pk_lookups[static_cast<size_t>(
                                     input.def_col_to_pk_lookup[col_off])]
                                 .group_idx]
              .use_subgid) {
        return true;
      }
      if (input.join_type == JoinType::kInner &&
          std::ranges::any_of(input.equality_keys, [&](const EqualityKey& eq) {
            if (eq.left != col_off || eq.left_offset != 0 ||
                eq.right_offset != 0 || eq.null_safe ||
                eq.right >= input.right.schema.ColumnCount()) {
              return false;
            }
            const Column& lc = input.left.schema.GetColumn(eq.left);
            const Column& rc = input.right.schema.GetColumn(eq.right);
            return lc.Type() == rc.Type() &&
                   lc.IsUnsigned() == rc.IsUnsigned() &&
                   lc.Type() != ValueType::kVarChar;
          })) {
        return true;
      }
      return false;
    };
    const std::function<bool(const Expression&)> expr_unpackable =
        [&](const Expression& expr) -> bool {
      if (!expr || ContainsParameterSlot(expr)) {
        return false;
      }
      if (expr->Type() == TypeTag::kConstantValue) {
        return true;
      }
      if (expr->Type() == TypeTag::kAggregateExp) {
        return std::ranges::find(input.fusion->aggregates,
                                 &expr->AsAggregateExpression()) !=
               input.fusion->aggregates.end();
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const auto col_off = LocalColumnOffset(
            input.result.schema, expr->AsColumnValue().GetColumnName());
        return col_off.has_value() && col_unpackable(*col_off);
      }
      if (expr->Type() == TypeTag::kUnaryExp) {
        return expr_unpackable(expr->AsUnaryExpression().Child());
      }
      if (expr->Type() == TypeTag::kBinaryExp) {
        const auto& bin = expr->AsBinaryExpression();
        return expr_unpackable(bin.Left()) && expr_unpackable(bin.Right());
      }
      return false;
    };
    if (!std::ranges::all_of(input.fusion->select_items, expr_unpackable)) {
      split_key_unpack_only = false;
    }
  }
  use_left_packed_key = false;
  left_pack_unpack_only = false;
  left_pack_uses_right = false;
  left_pack_uses_dict = false;
  left_pack_right_mask = 0;
  left_pack_dict_mask = 0;
  left_pack_dict_shift = 0;

  std::vector<size_t> left_pack_right_cols;

  left_col_only = input.left.HasColumnarOnly();
  const size_t left_total_rows = input.left.TotalRows();
  if (!use_split_key_gid && !input.int_group_key && !packed_2x32_group_key &&
      input.deferred_left_probe == nullptr && !input.left.HasSpill() &&
      !input.right_ptr->HasSpill() && left_total_rows >= 8192 &&
      left_total_rows <= 8'000'000 && input.all_direct_group_keys &&
      input.key_programs.size() >= 2 && input.key_programs.size() <= 8 &&
      !(input.raw_count_fusion && input.count_star_only &&
        (input.fusion->having || input.fusion->post_having_active))) {
    std::vector<size_t> lp_right_cols;
    lp_right_cols.reserve(input.key_shapes.size());
    std::vector<size_t> lp_dict_cols;
    std::vector<LeftPackedIntCol> lp_ints;
    const bool right_ok = right_row_count > 0 && right_row_count <= 262144;
    bool ok = true;
    for (const auto& shape : input.key_shapes) {
      if (shape.direct_column < 0) {
        ok = false;
        break;
      }
      const auto col = static_cast<size_t>(shape.direct_column);
      if (col >= input.left_width) {
        if (!right_ok) {
          ok = false;
          break;
        }
        lp_right_cols.push_back(col - input.left_width);
      } else {
        const Column& cdef = input.left.schema.GetColumn(col);
        if (!cdef.IsUnsigned() && (cdef.Type() == ValueType::kInt64 ||
                                   cdef.Type() == ValueType::kDate)) {
          lp_ints.push_back(LeftPackedIntCol{.col = static_cast<slot_t>(col),
                                             .type = cdef.Type()});
        } else if (cdef.Type() == ValueType::kVarChar) {
          lp_dict_cols.push_back(col);
        } else {
          ok = false;
          break;
        }
      }
    }
    std::vector<size_t> lp_col_offsets;
    std::vector<uint32_t> lp_ne_chunks;
    std::vector<size_t> lp_ne_bases;
    const auto locate_left_col = [&](size_t li) -> std::pair<size_t, size_t> {
      const auto it =
          std::upper_bound(lp_ne_bases.begin(), lp_ne_bases.end(), li);
      const auto idx = static_cast<size_t>((it - lp_ne_bases.begin()) - 1);
      return {lp_ne_chunks[idx], li - lp_ne_bases[idx]};
    };
    if (ok && left_col_only) {
      const auto& chunks = input.left.columnar->chunks;
      lp_col_offsets.resize(chunks.size(), 0);
      lp_ne_chunks.reserve(chunks.size());
      lp_ne_bases.reserve(chunks.size());
      size_t base = 0;
      for (size_t ci = 0; ci < chunks.size(); ++ci) {
        lp_col_offsets[ci] = base;
        if (chunks[ci].rows > 0) {
          lp_ne_chunks.push_back(static_cast<uint32_t>(ci));
          lp_ne_bases.push_back(base);
          base += chunks[ci].rows;
        }
      }
    }
    bool fast_dict_eligible = false;
    uint32_t fast_dict_card = 0;
    size_t fast_dict_col = 0;
    std::vector<uint32_t> fast_src_row_to_subgid;
    std::vector<size_t> fast_src_chunk_bases;
    std::optional<uint32_t> fast_null_subgid;
    if (ok && left_col_only && lp_dict_cols.size() == 1) {
      const size_t dc = lp_dict_cols[0];
      const ColumnarTable* src_table = nullptr;
      slot_t gcol = 0;
      bool can_src_gather = true;
      bool any_gather_null = false;
      for (const ColumnarChunk& ch : input.left.columnar->chunks) {
        if (ch.rows == 0) {
          continue;
        }
        const ColumnarColumn& cc = ch.columns[dc];
        if (cc.encoding != ColumnarColumn::Encoding::kGathered ||
            cc.gather_index == nullptr ||
            cc.gather_index->source_table == nullptr ||
            cc.gather_index->src_chunks.size() < ch.rows ||
            cc.gather_index->src_rows.size() < ch.rows) {
          can_src_gather = false;
          break;
        }
        if (src_table == nullptr) {
          src_table = cc.gather_index->source_table.get();
          gcol = cc.gather_col;
        } else if (src_table != cc.gather_index->source_table.get() ||
                   gcol != cc.gather_col) {
          can_src_gather = false;
          break;
        }
        any_gather_null |= cc.gather_index->has_null;
      }
      if (can_src_gather && src_table != nullptr && src_table->total_rows > 0 &&
          src_table->total_rows <= 4096) {
        for (const ColumnarChunk& sch : src_table->chunks) {
          if (sch.rows > 0 && gcol >= sch.columns.size()) {
            can_src_gather = false;
            break;
          }
        }
        if (can_src_gather) {
          fast_src_chunk_bases.resize(src_table->chunks.size(), 0);
          size_t sbase = 0;
          for (size_t sci = 0; sci < src_table->chunks.size(); ++sci) {
            fast_src_chunk_bases[sci] = sbase;
            sbase += src_table->chunks[sci].rows;
          }
          fast_src_row_to_subgid.assign(sbase, 0);
          std::unordered_map<Value, uint32_t, DistinctValueHash,
                             DistinctValueEqual>
              src_map;
          src_map.reserve(std::min<size_t>((sbase * 2) + 16, 4096));
          uint32_t next_id = 0;
          bool seen_ci = false;
          Value tmp;
          for (size_t sci = 0; sci < src_table->chunks.size() && !seen_ci;
               ++sci) {
            const ColumnarChunk& sch = src_table->chunks[sci];
            if (sch.rows == 0) {
              continue;
            }
            const ColumnarColumn& scc = sch.columns[gcol];
            const size_t base_r = fast_src_chunk_bases[sci];
            for (size_t sr = 0; sr < sch.rows; ++sr) {
              scc.WriteValueInto(sr, &tmp);
              if (tmp.IsCaseInsensitive()) {
                seen_ci = true;
                break;
              }
              Value kv = CanonicalDistinctValue(tmp);
              if (kv.type == ValueType::kVarChar && !kv.IsNull() &&
                  kv.owned_data.empty()) {
                kv = Value(std::string(kv.value.varchar_value));
              }
              auto [it, inserted] = src_map.try_emplace(std::move(kv), next_id);
              if (inserted) {
                ++next_id;
              }
              if (it->first.IsNull() && !fast_null_subgid.has_value()) {
                fast_null_subgid = it->second;
              }
              fast_src_row_to_subgid[base_r + sr] = it->second;
            }
          }
          if (!seen_ci) {
            if (any_gather_null && !fast_null_subgid.has_value()) {
              fast_null_subgid = next_id++;
            }
            if (next_id <= 1024) {
              fast_dict_eligible = true;
              fast_dict_card = next_id;
              fast_dict_col = dc;
            }
          }
        }
      }
    }
    if (ok && !lp_dict_cols.empty() && !fast_dict_eligible) {
      constexpr size_t kSample = 256;
      std::unordered_set<uint64_t> seen_hashes;
      seen_hashes.reserve(256);
      Value sample_tmp;
      for (size_t s = 0; s < kSample; ++s) {
        const size_t ri = (s * left_total_rows) / kSample;
        uint64_t h = 0x9e3779b97f4a7c15ULL;
        if (left_col_only) {
          const auto [ci, r] = locate_left_col(ri);
          const ColumnarChunk& ch = input.left.columnar->chunks[ci];
          for (const size_t dc : lp_dict_cols) {
            ch.columns[dc].WriteValueInto(r, &sample_tmp);
            h = std::rotl(h, 7) ^ HashFusedKeyValue(sample_tmp);
            h *= 0x100000001b3ULL;
          }
        } else {
          for (const size_t dc : lp_dict_cols) {
            const Value& v = input.left.rows[ri].values_[dc];
            h = std::rotl(h, 7) ^ std::hash<Value>{}(v);
            h *= 0x100000001b3ULL;
          }
        }
        seen_hashes.insert(h);
        if (seen_hashes.size() > 128) {
          ok = false;
          break;
        }
      }
    }
    if (ok && (!lp_dict_cols.empty() || !lp_right_cols.empty() ||
               lp_ints.size() > 2)) {
      const size_t charge_bytes =
          (left_total_rows *
           (lp_dict_cols.empty() ? 1 : (fast_dict_eligible ? 4 : 20))) +
          (lp_right_cols.empty() ? 0 : right_row_count * 16);
      left_pack_charge = TryReserveQueryMemory(charge_bytes);
      if (left_pack_charge) {
        const size_t n_ints = lp_ints.size();
        std::array<int64_t, 8> g_min{};
        std::array<int64_t, 8> g_max{};
        std::array<uint8_t, 8> g_non_null{};
        std::array<uint8_t, 8> g_null{};
        std::array<uint8_t, 8> int_range_done{};
        g_min.fill(std::numeric_limits<int64_t>::max());
        g_max.fill(std::numeric_limits<int64_t>::min());
        if (left_col_only) {
          for (size_t ji = 0; ji < n_ints; ++ji) {
            const ColumnarTable* src_table = nullptr;
            slot_t gcol = 0;
            bool can_src_gather = true;
            bool any_gather_null = false;
            for (const ColumnarChunk& ch : input.left.columnar->chunks) {
              if (ch.rows == 0) {
                continue;
              }
              const ColumnarColumn& cc = ch.columns[lp_ints[ji].col];
              if (cc.encoding != ColumnarColumn::Encoding::kGathered ||
                  cc.gather_index == nullptr ||
                  cc.gather_index->source_table == nullptr ||
                  cc.gather_index->src_chunks.size() < ch.rows ||
                  cc.gather_index->src_rows.size() < ch.rows) {
                can_src_gather = false;
                break;
              }
              if (src_table == nullptr) {
                src_table = cc.gather_index->source_table.get();
                gcol = cc.gather_col;
              } else if (src_table != cc.gather_index->source_table.get() ||
                         gcol != cc.gather_col) {
                can_src_gather = false;
                break;
              }
              any_gather_null |= cc.gather_index->has_null;
            }
            if (can_src_gather && src_table != nullptr &&
                src_table->total_rows > 0 && src_table->total_rows <= 4096) {
              bool type_ok = true;
              for (const ColumnarChunk& sch : src_table->chunks) {
                if (sch.rows > 0 && gcol >= sch.columns.size()) {
                  type_ok = false;
                  break;
                }
              }
              int64_t smin = std::numeric_limits<int64_t>::max();
              int64_t smax = std::numeric_limits<int64_t>::min();
              uint8_t snon_null = 0;
              uint8_t snull = any_gather_null ? uint8_t{1} : uint8_t{0};
              Value tmp;
              for (size_t sci = 0; sci < src_table->chunks.size() && type_ok;
                   ++sci) {
                const ColumnarChunk& sch = src_table->chunks[sci];
                if (sch.rows == 0) {
                  continue;
                }
                const ColumnarColumn& scc = sch.columns[gcol];
                for (size_t sr = 0; sr < sch.rows; ++sr) {
                  if (scc.is_uniform) {
                    if (scc.IsNull(sr)) {
                      snull = 1;
                    } else if (scc.type == lp_ints[ji].type &&
                               !scc.is_unsigned) {
                      const int64_t iv = scc.IntAt(sr);
                      snon_null = 1;
                      smin = std::min(smin, iv);
                      smax = std::max(smax, iv);
                    } else {
                      type_ok = false;
                      break;
                    }
                  } else {
                    scc.WriteValueInto(sr, &tmp);
                    if (tmp.IsNull()) {
                      snull = 1;
                    } else if (tmp.type == lp_ints[ji].type &&
                               !tmp.IsUnsigned()) {
                      const int64_t iv = tmp.value.int_value;
                      snon_null = 1;
                      smin = std::min(smin, iv);
                      smax = std::max(smax, iv);
                    } else {
                      type_ok = false;
                      break;
                    }
                  }
                }
              }
              if (type_ok) {
                g_min[ji] = smin;
                g_max[ji] = smax;
                g_non_null[ji] = snon_null;
                g_null[ji] = snull;
                int_range_done[ji] = 1;
              }
            }
          }
        }
        std::vector<uint64_t> left_hashes;
        if (!lp_dict_cols.empty() && !fast_dict_eligible) {
          left_hashes.resize(left_total_rows);
        }
        if (fast_dict_eligible) {
          left_pack_dict_subgid.resize(left_total_rows);
        }
        const size_t scan_workers =
            ParallelWorkerCount(left_total_rows / 32768);
        std::vector<std::vector<std::pair<uint32_t, uint32_t>>>
            worker_dict_reps;
        if (fast_dict_eligible) {
          worker_dict_reps.assign(scan_workers,
                                  std::vector<std::pair<uint32_t, uint32_t>>(
                                      fast_dict_card, {UINT32_MAX, 0U}));
        }
        struct WorkerRange {
          std::array<int64_t, 8> min_v{};
          std::array<int64_t, 8> max_v{};
          std::array<uint8_t, 8> has_non_null{};
          std::array<uint8_t, 8> has_null{};
          bool valid{true};
        };
        std::vector<WorkerRange> wranges(scan_workers);
        for (auto& wr : wranges) {
          wr.min_v.fill(std::numeric_limits<int64_t>::max());
          wr.max_v.fill(std::numeric_limits<int64_t>::min());
        }
        bool all_ints_precomputed = true;
        for (size_t ji = 0; ji < n_ints; ++ji) {
          if (int_range_done[ji] == 0) {
            all_ints_precomputed = false;
            break;
          }
        }
        const bool need_scan =
            !left_col_only || !lp_dict_cols.empty() || !all_ints_precomputed;
        if (need_scan) {
          RETURN_IF_FAIL(RunParallelTasks(scan_workers, [&](size_t worker) {
            WorkerRange& wr = wranges[worker];
            if (left_col_only) {
              const auto& chunks = input.left.columnar->chunks;
              const size_t cb = (chunks.size() * worker) / scan_workers;
              const size_t ce = (chunks.size() * (worker + 1)) / scan_workers;
              Value tmp;
              for (size_t ci = cb; ci < ce; ++ci) {
                const ColumnarChunk& ch = chunks[ci];
                if (ch.rows == 0) {
                  continue;
                }
                for (size_t ji = 0; ji < n_ints; ++ji) {
                  if (int_range_done[ji] != 0) {
                    continue;
                  }
                  const ColumnarColumn& cc = ch.columns[lp_ints[ji].col];
                  if (cc.encoding == ColumnarColumn::Encoding::kConstant) {
                    if (cc.IsNull(0)) {
                      wr.has_null[ji] = 1;
                    } else if (cc.is_uniform && !cc.is_unsigned &&
                               cc.type == lp_ints[ji].type) {
                      const int64_t iv = cc.IntAt(0);
                      wr.has_non_null[ji] = 1;
                      wr.min_v[ji] = std::min(wr.min_v[ji], iv);
                      wr.max_v[ji] = std::max(wr.max_v[ji], iv);
                    } else {
                      cc.WriteValueInto(0, &tmp);
                      if (tmp.IsNull()) {
                        wr.has_null[ji] = 1;
                      } else if (tmp.type == lp_ints[ji].type &&
                                 !tmp.IsUnsigned()) {
                        const int64_t iv = tmp.value.int_value;
                        wr.has_non_null[ji] = 1;
                        wr.min_v[ji] = std::min(wr.min_v[ji], iv);
                        wr.max_v[ji] = std::max(wr.max_v[ji], iv);
                      } else {
                        wr.valid = false;
                        return;
                      }
                    }
                  } else if (cc.is_uniform && !cc.is_unsigned &&
                             cc.type == lp_ints[ji].type) {
                    if (cc.encoding == ColumnarColumn::Encoding::kFlat &&
                        cc.ints.size() >= ch.rows &&
                        (!cc.has_null || cc.nulls.empty() ||
                         cc.nulls.size() >= ch.rows)) {
                      const int64_t* ptr = cc.ints.data();
                      if (!cc.has_null || cc.nulls.empty()) {
                        int64_t lmin = wr.min_v[ji];
                        int64_t lmax = wr.max_v[ji];
                        for (size_t r = 0; r < ch.rows; ++r) {
                          const int64_t iv = ptr[r];
                          lmin = std::min(lmin, iv);
                          lmax = std::max(lmax, iv);
                        }
                        wr.has_non_null[ji] = 1;
                        wr.min_v[ji] = lmin;
                        wr.max_v[ji] = lmax;
                      } else {
                        const uint8_t* nul = cc.nulls.data();
                        int64_t lmin = wr.min_v[ji];
                        int64_t lmax = wr.max_v[ji];
                        bool seen_nn = false;
                        bool seen_nl = false;
                        for (size_t r = 0; r < ch.rows; ++r) {
                          if (nul[r] != 0) {
                            seen_nl = true;
                          } else {
                            const int64_t iv = ptr[r];
                            lmin = std::min(lmin, iv);
                            lmax = std::max(lmax, iv);
                            seen_nn = true;
                          }
                        }
                        if (seen_nl) {
                          wr.has_null[ji] = 1;
                        }
                        if (seen_nn) {
                          wr.has_non_null[ji] = 1;
                          wr.min_v[ji] = lmin;
                          wr.max_v[ji] = lmax;
                        }
                      }
                    } else if (cc.encoding ==
                               ColumnarColumn::Encoding::kGathered) {
                      for (size_t r = 0; r < ch.rows; ++r) {
                        const ColumnarColumn* leaf = nullptr;
                        size_t lr = 0;
                        if (!cc.ResolveLeaf(r, &leaf, &lr)) {
                          wr.has_null[ji] = 1;
                        } else {
                          const int64_t iv = leaf->is_uniform
                                                 ? leaf->IntAt(lr)
                                                 : ColumnarIntValue(*leaf, lr);
                          wr.has_non_null[ji] = 1;
                          wr.min_v[ji] = std::min(wr.min_v[ji], iv);
                          wr.max_v[ji] = std::max(wr.max_v[ji], iv);
                        }
                      }
                    } else {
                      for (size_t r = 0; r < ch.rows; ++r) {
                        if (cc.IsNull(r)) {
                          wr.has_null[ji] = 1;
                        } else {
                          const int64_t iv = cc.IntAt(r);
                          wr.has_non_null[ji] = 1;
                          wr.min_v[ji] = std::min(wr.min_v[ji], iv);
                          wr.max_v[ji] = std::max(wr.max_v[ji], iv);
                        }
                      }
                    }
                  } else if (cc.is_uniform && cc.type == ValueType::kNull) {
                    wr.has_null[ji] = 1;
                  } else {
                    for (size_t r = 0; r < ch.rows; ++r) {
                      cc.WriteValueInto(r, &tmp);
                      if (tmp.IsNull()) {
                        wr.has_null[ji] = 1;
                      } else if (tmp.type == lp_ints[ji].type &&
                                 !tmp.IsUnsigned()) {
                        const int64_t iv = tmp.value.int_value;
                        wr.has_non_null[ji] = 1;
                        wr.min_v[ji] = std::min(wr.min_v[ji], iv);
                        wr.max_v[ji] = std::max(wr.max_v[ji], iv);
                      } else {
                        wr.valid = false;
                        return;
                      }
                    }
                  }
                }
                if (!lp_dict_cols.empty()) {
                  const size_t base_li = lp_col_offsets[ci];
                  if (fast_dict_eligible) {
                    const ChunkGatherIndex* gi =
                        ch.columns[fast_dict_col].gather_index.get();
                    const uint32_t* sc_ptr = gi->src_chunks.data();
                    const uint32_t* sr_ptr = gi->src_rows.data();
                    uint32_t* dst = left_pack_dict_subgid.data() + base_li;
                    auto* reps = worker_dict_reps[worker].data();
                    const uint32_t* row_to_sg = fast_src_row_to_subgid.data();
                    const size_t* cbases = fast_src_chunk_bases.data();
                    const uint32_t null_sg = fast_null_subgid.value_or(0U);
                    for (size_t r = 0; r < ch.rows; ++r) {
                      const uint32_t sr = sr_ptr[r];
                      const uint32_t sg =
                          sr == ChunkGatherIndex::kNullRow
                              ? null_sg
                              : row_to_sg[cbases[sc_ptr[r]] + sr];
                      dst[r] = sg;
                      if (reps[sg].first == UINT32_MAX) {
                        reps[sg] = {static_cast<uint32_t>(ci),
                                    static_cast<uint32_t>(r)};
                      }
                    }
                  } else {
                    for (size_t r = 0; r < ch.rows; ++r) {
                      uint64_t h = 0x9e3779b97f4a7c15ULL;
                      for (const size_t dc : lp_dict_cols) {
                        ch.columns[dc].WriteValueInto(r, &tmp);
                        if (tmp.IsCaseInsensitive()) {
                          wr.valid = false;
                          return;
                        }
                        h = std::rotl(h, 7) ^ HashFusedKeyValue(tmp);
                        h *= 0x100000001b3ULL;
                      }
                      left_hashes[base_li + r] = h;
                    }
                  }
                }
              }
            } else {
              const size_t b = (input.left.rows.size() * worker) / scan_workers;
              const size_t e =
                  (input.left.rows.size() * (worker + 1)) / scan_workers;
              for (size_t ri = b; ri < e; ++ri) {
                const Row& row = input.left.rows[ri];
                for (size_t ji = 0; ji < n_ints; ++ji) {
                  const Value& v = row.values_[lp_ints[ji].col];
                  if (v.IsNull()) {
                    wr.has_null[ji] = 1;
                  } else if (v.type == lp_ints[ji].type && !v.IsUnsigned()) {
                    const int64_t iv = v.value.int_value;
                    wr.has_non_null[ji] = 1;
                    wr.min_v[ji] = std::min(wr.min_v[ji], iv);
                    wr.max_v[ji] = std::max(wr.max_v[ji], iv);
                  } else {
                    wr.valid = false;
                    return;
                  }
                }
                if (!lp_dict_cols.empty()) {
                  uint64_t h = 0x9e3779b97f4a7c15ULL;
                  for (const size_t dc : lp_dict_cols) {
                    const Value& v = row.values_[dc];
                    if (v.IsCaseInsensitive()) {
                      wr.valid = false;
                      return;
                    }
                    h = std::rotl(h, 7) ^ std::hash<Value>{}(v);
                    h *= 0x100000001b3ULL;
                  }
                  left_hashes[ri] = h;
                }
              }
            }
          }));
        }
        for (const WorkerRange& wr : wranges) {
          if (!wr.valid) {
            ok = false;
            break;
          }
        }
        if (ok) {
          for (const WorkerRange& wr : wranges) {
            for (size_t ji = 0; ji < n_ints; ++ji) {
              if (int_range_done[ji] != 0) {
                continue;
              }
              if (wr.has_non_null[ji] != 0) {
                g_non_null[ji] = 1;
                g_min[ji] = std::min(g_min[ji], wr.min_v[ji]);
                g_max[ji] = std::max(g_max[ji], wr.max_v[ji]);
              }
              g_null[ji] |= wr.has_null[ji];
            }
          }
        }
        uint32_t dict_card = 0;
        std::vector<uint32_t> lp_dict_rep_rows;
        std::vector<std::pair<uint32_t, uint32_t>> fast_dict_rep_col_loc;
        if (ok && !lp_dict_cols.empty()) {
          if (fast_dict_eligible) {
            dict_card = fast_dict_card;
            fast_dict_rep_col_loc.assign(fast_dict_card, {UINT32_MAX, 0U});
            for (const auto& wreps : worker_dict_reps) {
              for (uint32_t sg = 0; sg < fast_dict_card; ++sg) {
                if (fast_dict_rep_col_loc[sg].first == UINT32_MAX &&
                    wreps[sg].first != UINT32_MAX) {
                  fast_dict_rep_col_loc[sg] = wreps[sg];
                }
              }
            }
            left_pack_chunk_offsets = std::move(lp_col_offsets);
          } else {
            ok = build_flat_subgid_dict(
                left_total_rows, 1024,
                [&](std::vector<uint64_t>& hashes) -> Status {
                  hashes = std::move(left_hashes);
                  return Status::kSuccess;
                },
                [&](size_t a, size_t b) {
                  if (left_col_only) {
                    const auto [cia, ra] = locate_left_col(a);
                    const auto [cib, rb] = locate_left_col(b);
                    const ColumnarChunk& ca = input.left.columnar->chunks[cia];
                    const ColumnarChunk& cb = input.left.columnar->chunks[cib];
                    Value va;
                    Value vb;
                    for (const size_t dc : lp_dict_cols) {
                      ca.columns[dc].WriteValueInto(ra, &va);
                      cb.columns[dc].WriteValueInto(rb, &vb);
                      if (!DistinctValueEqual{}(va, vb)) {
                        return false;
                      }
                    }
                    return true;
                  }
                  const Row& ra = input.left.rows[a];
                  const Row& rb = input.left.rows[b];
                  return std::ranges::all_of(lp_dict_cols, [&](size_t dc) {
                    return DistinctValueEqual{}(ra.values_[dc], rb.values_[dc]);
                  });
                },
                &left_pack_dict_subgid, &dict_card, nullptr, &lp_dict_rep_rows);
            if (ok && left_col_only) {
              left_pack_chunk_offsets = std::move(lp_col_offsets);
            }
          }
        }
        uint32_t right_card = 0;
        if (ok && !lp_right_cols.empty()) {
          ok = build_or_reuse_split_right_dict(lp_right_cols, &right_card);
        }
        uint32_t bit_cursor = 0;
        if (ok && !lp_right_cols.empty()) {
          const uint32_t r_bits =
              right_card <= 1
                  ? 0U
                  : static_cast<uint32_t>(std::bit_width(right_card - 1));
          if (bit_cursor + r_bits <= 64U) {
            left_pack_uses_right = true;
            split_right_shift = static_cast<uint8_t>(bit_cursor);
            split_right_subgid_count = right_card;
            left_pack_right_mask =
                r_bits == 0 ? 0ULL : ((uint64_t{1} << r_bits) - 1ULL);
            bit_cursor += r_bits;
          } else {
            ok = false;
          }
        }
        if (ok && !lp_dict_cols.empty()) {
          const uint32_t d_bits =
              dict_card <= 1
                  ? 0U
                  : static_cast<uint32_t>(std::bit_width(dict_card - 1));
          if (bit_cursor + d_bits <= 64U) {
            left_pack_uses_dict = true;
            left_pack_dict_shift = static_cast<uint8_t>(bit_cursor);
            left_pack_dict_mask =
                d_bits == 0 ? 0ULL : ((uint64_t{1} << d_bits) - 1ULL);
            bit_cursor += d_bits;
          } else {
            ok = false;
          }
        }
        for (size_t ji = 0; ok && ji < n_ints; ++ji) {
          if (g_non_null[ji] == 0) {
            if (bit_cursor >= 64U) {
              // Zero bits left: a shift of 64 on uint64 is UB, so drop the
              // packed-key fast path and let the generic path handle it.
              ok = false;
              break;
            }
            lp_ints[ji].min_val = 0;
            lp_ints[ji].null_code = 0;
            lp_ints[ji].mask = 0;
            lp_ints[ji].shift = static_cast<uint8_t>(bit_cursor);
            continue;
          }
          const uint64_t span = static_cast<uint64_t>(g_max[ji]) -
                                static_cast<uint64_t>(g_min[ji]);
          if (span >= std::numeric_limits<uint64_t>::max() - 2ULL) {
            ok = false;
            break;
          }
          const uint64_t domain = span + 1ULL + (g_null[ji] != 0 ? 1ULL : 0ULL);
          const uint32_t bits =
              domain <= 1ULL
                  ? 0U
                  : static_cast<uint32_t>(std::bit_width(domain - 1ULL));
          // `bits == 0` at offset 64 would store an unrepresentable shift.
          if (bit_cursor >= 64U || bit_cursor + bits > 64U) {
            ok = false;
            break;
          }
          lp_ints[ji].min_val = g_min[ji];
          lp_ints[ji].null_code = span + 1ULL;
          lp_ints[ji].mask = bits == 0 ? 0ULL : ((uint64_t{1} << bits) - 1ULL);
          lp_ints[ji].shift = static_cast<uint8_t>(bit_cursor);
          bit_cursor += bits;
        }
        if (ok && bit_cursor <= 64U) {
          use_left_packed_key = true;
          left_pack_ints = std::move(lp_ints);
          left_pack_dict_cols = std::move(lp_dict_cols);
          left_pack_right_cols = std::move(lp_right_cols);
          if (left_pack_uses_dict) {
            if (left_col_only) {
              if (!fast_dict_rep_col_loc.empty()) {
                left_pack_dict_rep_col_loc = std::move(fast_dict_rep_col_loc);
              } else {
                left_pack_dict_rep_col_loc.reserve(lp_dict_rep_rows.size());
                for (const uint32_t rr : lp_dict_rep_rows) {
                  const auto [ci, r] = locate_left_col(rr);
                  left_pack_dict_rep_col_loc.emplace_back(
                      static_cast<uint32_t>(ci), static_cast<uint32_t>(r));
                }
              }
            } else {
              left_pack_dict_rep_rows = std::move(lp_dict_rep_rows);
            }
          }
          auto col_unpackable = [&](size_t col_off) -> bool {
            if (std::ranges::any_of(left_pack_ints, [&](const auto& lpi) {
                  return lpi.col == col_off;
                })) {
              return true;
            }
            if (std::ranges::find(left_pack_dict_cols, col_off) !=
                left_pack_dict_cols.end()) {
              return true;
            }
            if (col_off >= input.left_width) {
              return std::ranges::find(left_pack_right_cols,
                                       col_off - input.left_width) !=
                     left_pack_right_cols.end();
            }
            if (input.join_type == JoinType::kInner) {
              return std::ranges::any_of(
                  input.equality_keys, [&](const EqualityKey& eq) {
                    if (eq.left != col_off || eq.left_offset != 0 ||
                        eq.right_offset != 0 || eq.null_safe ||
                        eq.right >= input.right.schema.ColumnCount()) {
                      return false;
                    }
                    const Column& lc = input.left.schema.GetColumn(eq.left);
                    const Column& rc = input.right.schema.GetColumn(eq.right);
                    return lc.Type() == rc.Type() &&
                           lc.IsUnsigned() == rc.IsUnsigned() &&
                           lc.Type() != ValueType::kVarChar &&
                           std::ranges::find(left_pack_right_cols, eq.right) !=
                               left_pack_right_cols.end();
                  });
            }
            return false;
          };
          const std::function<bool(const Expression&)> expr_unpackable =
              [&](const Expression& expr) -> bool {
            if (!expr || ContainsParameterSlot(expr)) {
              return false;
            }
            if (expr->Type() == TypeTag::kConstantValue) {
              return true;
            }
            if (expr->Type() == TypeTag::kAggregateExp) {
              return std::ranges::find(input.fusion->aggregates,
                                       &expr->AsAggregateExpression()) !=
                     input.fusion->aggregates.end();
            }
            if (expr->Type() == TypeTag::kColumnValue) {
              const auto off = LocalColumnOffset(
                  input.result.schema, expr->AsColumnValue().GetColumnName());
              return off.has_value() && col_unpackable(*off);
            }
            if (expr->Type() == TypeTag::kUnaryExp) {
              return expr_unpackable(expr->AsUnaryExpression().Child());
            }
            if (expr->Type() == TypeTag::kBinaryExp) {
              const auto& bin = expr->AsBinaryExpression();
              return expr_unpackable(bin.Left()) &&
                     expr_unpackable(bin.Right());
            }
            return false;
          };
          left_pack_unpack_only =
              !input.left.HasSpill() && !input.right_ptr->HasSpill() &&
              input.result.using_columns == nullptr && !input.fusion->having &&
              !input.fusion->post_having_active &&
              input.fusion->topn_limit == 0 &&
              !input.fusion->rollup_count_distribution &&
              (input.raw_sum_fusion || input.raw_count_fusion ||
               input.fusion->aggregates.empty()) &&
              std::ranges::all_of(input.fusion->select_items, expr_unpackable);
        } else {
          left_pack_uses_right = false;
          left_pack_uses_dict = false;
          left_pack_dict_subgid.clear();
          clear_split_right_dict();
          left_pack_charge.reset();
        }
      }
    }
  }
  return Status::kSuccess;
}
}  // namespace tinylamb::relational_detail
