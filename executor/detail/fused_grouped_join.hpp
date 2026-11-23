/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_FUSED_GROUPED_JOIN_HPP
#define TINYLAMB_EXECUTOR_DETAIL_FUSED_GROUPED_JOIN_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/bytecode.hpp"
#include "expression/expression.hpp"
#include "query/statement.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
struct LSTableStore;
}  // namespace tinylamb

namespace tinylamb::relational_detail {

struct DeferredLSTablePkLookup {
  std::shared_ptr<const LSTableStore> store;
  slot_t pk_col{0};
  slot_t store_pk_col{0};
  slot_t payload_col{0};
  slot_t target_proj_col{0};
  ValueType payload_type{ValueType::kNull};
  bool payload_unsigned{false};
};

struct DeferredLSTableProbe {
  std::shared_ptr<const LSTableStore> store;
  std::vector<slot_t> projection;
  ScanRuntimeFilter runtime_filter;
  std::vector<Expression> local_preds;
  bool local_preds_covered_by_peeks{false};
  std::vector<IntegerPeekCompare> integer_peeks;
  std::vector<NullPeekCompare> null_peeks;
  std::vector<DeferredLSTablePkLookup> pk_lookups;
  bool columnar_output{false};
};

template <typename Key, typename Mapped, typename Hash = std::hash<Key>,
          typename Equal = std::equal_to<Key>>
class FlatJoinValueBuckets {
  struct Entry;
  static constexpr uint32_t kNoEntry = std::numeric_limits<uint32_t>::max();

 public:
  class const_iterator {
   public:
    const_iterator() = default;
    const_iterator(const FlatJoinValueBuckets* buckets, uint32_t index)
        : buckets_(buckets), index_(index) {}

    const Entry& operator*() const { return buckets_->entries_[index_]; }
    const Entry* operator->() const { return &buckets_->entries_[index_]; }
    const_iterator& operator++() {
      index_ = buckets_->entries_[index_].duplicate_next;
      return *this;
    }
    bool operator==(const const_iterator& other) const = default;

   private:
    const FlatJoinValueBuckets* buckets_{nullptr};
    uint32_t index_{kNoEntry};
  };

  void reserve(size_t count) {
    if (count >= kNoEntry) {
      throw std::length_error("join bucket capacity overflow");
    }
    if constexpr (sizeof(size_t) <= sizeof(uint32_t)) {
      if (count > (std::numeric_limits<size_t>::max() - 1) / 2) {
        throw std::length_error("join bucket capacity overflow");
      }
    }
    const size_t target = count + ((count / 2) + 1);
    size_t bucket_count = 8;
    while (bucket_count < target) {
      if (bucket_count > std::numeric_limits<size_t>::max() / 2) {
        throw std::length_error("join bucket capacity overflow");
      }
      bucket_count *= 2;
    }
    entries_.reserve(count);
    buckets_.assign(bucket_count, kNoEntry);
  }

  template <typename KeyLike>
  bool emplace(KeyLike&& key, Mapped value) {
    EnsureBuckets();
    return emplace_bucket(std::forward<KeyLike>(key), value, BucketIndex(key));
  }

  template <typename KeyLike>
  bool emplace_hashed(KeyLike&& key, Mapped value, uint64_t hash) {
    EnsureBuckets();
    return emplace_bucket(std::forward<KeyLike>(key), value,
                          static_cast<size_t>(hash) & (buckets_.size() - 1));
  }

  [[nodiscard]] std::pair<const_iterator, const_iterator> equal_range(
      const Key& key) const {
    if (buckets_.empty()) {
      return {const_iterator(this, kNoEntry), const_iterator(this, kNoEntry)};
    }
    const size_t bucket = BucketIndex(key);
    return {const_iterator(this, FindHead(key, bucket)),
            const_iterator(this, kNoEntry)};
  }

  [[nodiscard]] std::pair<const_iterator, const_iterator> equal_range_hashed(
      const Key& key, uint64_t hash) const {
    if (buckets_.empty()) {
      return {const_iterator(this, kNoEntry), const_iterator(this, kNoEntry)};
    }
    const size_t bucket = static_cast<size_t>(hash) & (buckets_.size() - 1);
    return {const_iterator(this, FindHead(key, bucket)),
            const_iterator(this, kNoEntry)};
  }

  void PrefetchBucket(const Key& key) const {
    if (buckets_.empty()) {
      return;
    }
    __builtin_prefetch(
        static_cast<const uint32_t*>(&buckets_[BucketIndex(key)]));
  }

  void PrefetchBucketHashed(uint64_t hash) const {
    if (buckets_.empty()) {
      return;
    }
    __builtin_prefetch(static_cast<const uint32_t*>(
        &buckets_[static_cast<size_t>(hash) & (buckets_.size() - 1)]));
  }

 private:
  struct Entry {
    Key first;
    Mapped second;
    uint32_t duplicate_next;
    uint32_t collision_next;
  };

  void EnsureBuckets() {
    if (buckets_.empty()) {
      buckets_.assign(8, kNoEntry);
    }
  }

  template <typename KeyLike>
  bool emplace_bucket(KeyLike&& key, Mapped value, size_t bucket) {
    const uint32_t head = FindHead(key, bucket);
    const auto index = static_cast<uint32_t>(entries_.size());
    entries_.push_back(
        Entry{std::forward<KeyLike>(key), value, head, buckets_[bucket]});
    buckets_[bucket] = index;
    return head == kNoEntry;
  }

  [[nodiscard]] size_t BucketIndex(const Key& key) const {
    if (buckets_.empty()) {
      return 0;  // callers guard empty tables before dereferencing
    }
    if constexpr (std::is_same_v<Key, int64_t>) {
      uint64_t x = static_cast<uint64_t>(key) * 0x9e3779b97f4a7c15ULL;
      x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
      return static_cast<size_t>(x ^ (x >> 31)) & (buckets_.size() - 1);
    } else {
      return hash_(key) & (buckets_.size() - 1);
    }
  }

  [[nodiscard]] uint32_t FindHead(const Key& key, size_t bucket) const {
    for (uint32_t index = buckets_[bucket]; index != kNoEntry;
         index = entries_[index].collision_next) {
      if (equal_(entries_[index].first, key)) {
        return index;
      }
    }
    return kNoEntry;
  }

  Hash hash_{};
  Equal equal_{};
  std::vector<uint32_t> buckets_;
  std::vector<Entry> entries_;
};

template <typename Key, typename Hash = std::hash<Key>,
          typename Equal = std::equal_to<Key>>
using FlatJoinBuckets = FlatJoinValueBuckets<Key, const Row*, Hash, Equal>;

template <typename Key, typename Hash = std::hash<Key>,
          typename Equal = std::equal_to<Key>>
using FlatJoinIdxBuckets = FlatJoinValueBuckets<Key, uint32_t, Hash, Equal>;

template <typename Key, typename Mapped, typename Hash = std::hash<Key>,
          typename Equal = std::equal_to<Key>>
class ShardedFlatJoinValueBuckets {
 public:
  static constexpr size_t kShards = 64;
  using BucketType = FlatJoinValueBuckets<Key, Mapped, Hash, Equal>;
  using const_iterator = typename BucketType::const_iterator;

  template <typename KeyOfRow, typename SkipRow>
  Status BuildParallel(const std::vector<Row>& rows, KeyOfRow key_of_row,
                       SkipRow skip_row, size_t workers) {
    return BuildParallelRange(
        rows.size(),
        [&](size_t i, Key* out_key, Mapped* out_val) -> bool {
          const Row& row = rows[i];
          if (skip_row(row)) {
            return false;
          }
          *out_key = key_of_row(row);
          if constexpr (std::is_same_v<Mapped, const Row*>) {
            *out_val = &row;
          } else {
            *out_val = static_cast<Mapped>(i);
          }
          return true;
        },
        workers);
  }

  template <typename FillFn>
  Status BuildParallelRange(size_t total, FillFn&& fill_fn, size_t workers) {
    struct Item {
      Key key;
      Mapped value;
      uint64_t hash;
    };
    workers = std::max<size_t>(1, std::min(workers, total));
    std::vector<std::array<std::vector<Item>, kShards>> worker_parts(workers);
    const size_t approx_per_shard =
        std::max<size_t>(4, ((total / workers) / kShards) + 4);
    for (auto& parts : worker_parts) {
      for (auto& vec : parts) {
        vec.reserve(approx_per_shard);
      }
    }
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
      const size_t begin = (total * w) / workers;
      const size_t end = (total * (w + 1)) / workers;
      auto& local = worker_parts[w];
      for (size_t i = begin; i < end; ++i) {
        Key k;
        Mapped val;
        if (!fill_fn(i, &k, &val)) continue;
        const uint64_t h = HashKey(k);
        local[static_cast<size_t>(h >> 58)].push_back(
            Item{std::move(k), val, h});
      }
    }));
    const size_t merge_workers = std::min(workers, kShards);
    std::mutex error_mutex;
    Status error = Status::kSuccess;
    RETURN_IF_FAIL(RunParallelTasks(merge_workers, [&](size_t w) {
      try {
        for (size_t s = w; s < kShards; s += merge_workers) {
          size_t count = 0;
          for (size_t t = 0; t < workers; ++t) {
            count += worker_parts[t][s].size();
          }
          shards_[s].reserve(count);
          for (size_t t = 0; t < workers; ++t) {
            for (Item& item : worker_parts[t][s]) {
              shards_[s].emplace_hashed(std::move(item.key), item.value,
                                        item.hash);
            }
            std::vector<Item>().swap(worker_parts[t][s]);
          }
        }
      } catch (const std::exception& e) {
        std::scoped_lock lock(error_mutex);
        if (error.ok()) {
          error = StatusError(StatusCode::kRuntimeError, e.what());
        }
      }
    }));
    return error;
  }

  [[nodiscard]] uint64_t HashKey(const Key& key) const {
    uint64_t h = 0;
    if constexpr (std::is_same_v<Key, int64_t>) {
      h = static_cast<uint64_t>(key) * 0x94d049bb133111ebULL;
    } else {
      h = static_cast<uint64_t>(hash_(key));
    }
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    return h ^ (h >> 33);
  }

  [[nodiscard]] std::pair<const_iterator, const_iterator> equal_range(
      const Key& key) const {
    return equal_range_hashed(key, HashKey(key));
  }

  [[nodiscard]] std::pair<const_iterator, const_iterator> equal_range_hashed(
      const Key& key, uint64_t hash) const {
    return shards_[static_cast<size_t>(hash >> 58)].equal_range_hashed(key,
                                                                       hash);
  }

  void PrefetchBucket(const Key& key) const {
    PrefetchBucketHashed(HashKey(key));
  }

  void PrefetchBucketHashed(uint64_t hash) const {
    shards_[static_cast<size_t>(hash >> 58)].PrefetchBucketHashed(hash);
  }

 private:
  Hash hash_{};
  std::array<BucketType, kShards> shards_;
};

template <typename Key, typename Hash = std::hash<Key>,
          typename Equal = std::equal_to<Key>>
using ShardedFlatJoinBuckets =
    ShardedFlatJoinValueBuckets<Key, const Row*, Hash, Equal>;

template <typename Key, typename Hash = std::hash<Key>,
          typename Equal = std::equal_to<Key>>
using ShardedFlatJoinIdxBuckets =
    ShardedFlatJoinValueBuckets<Key, uint32_t, Hash, Equal>;

struct ProgramShape {
  int32_t direct_column{-1};
  bool fast{false};
  int32_t extract_date_col{-1};
  uint16_t extract_date_part{0};
  int32_t mul_col0{-1};
  int32_t mul_col1{-1};
  int32_t coalesce_mul_col0{-1};
  int32_t coalesce_mul_col1{-1};
  int32_t mul_one_minus_col0{-1};
  int32_t mul_one_minus_col1{-1};
  double mul_one_minus_const{1.0};
  int32_t profit_col0{-1};
  int32_t profit_col1{-1};
  int32_t profit_col2{-1};
  int32_t profit_col3{-1};
  double profit_const{1.0};
  int32_t like_column{-1};
  std::string_view like_pattern;
  bool like_negated{false};
  enum class CaseCondKind : uint8_t {
    kNone = 0,
    kColEqInt,
    kColEqStr,
    kColEq2StrOr,
    kColNe2StrAnd,
    kColLikePrefix,
    kIntComparisons,
  };
  enum class CaseValKind : uint8_t {
    kNone = 0,
    kIntConst1,
    kDirectInt64Col,
    kDirectDoubleCol,
    kMulOneMinus,
    kDoubleArith,
  };
  CaseCondKind case_cond{CaseCondKind::kNone};
  CaseValKind case_val{CaseValKind::kNone};
  bool case_else_null{false};
  int32_t case_cond_col{-1};
  int64_t case_cond_int{0};
  std::string_view case_cond_str0;
  std::string_view case_cond_str1;
  int32_t case_val_col0{-1};
  int32_t case_val_col1{-1};
  double case_val_const{1.0};
  std::shared_ptr<const BytecodeProgram> case_val_program;
  struct IntCaseComparison {
    int32_t left{-1};
    int32_t subtract{-1};
    BinaryOperation op{BinaryOperation::kEquals};
    int64_t constant{0};
    bool is_date{false};
  };
  std::vector<IntCaseComparison> case_int_comparisons;

  [[nodiscard]] bool HasPairDirect() const noexcept {
    return direct_column >= 0 || extract_date_col >= 0 || mul_col0 >= 0 ||
           coalesce_mul_col0 >= 0 || mul_one_minus_col0 >= 0 ||
           profit_col0 >= 0 ||
           (case_cond != CaseCondKind::kNone &&
            case_val != CaseValKind::kNone &&
            case_val != CaseValKind::kDoubleArith);
  }
};

ProgramShape ClassifyProgram(const BytecodeProgram& program,
                             const Expression& expr = Expression(),
                             const Schema* schema = nullptr);

void CollectLoadCells(const BytecodeProgram& program,
                      std::vector<slot_t>* cells);

StatusOr<Value> EvaluateProgramValue(const BytecodeProgram& program,
                                     const ProgramShape& shape,
                                     const Row& scratch);

StatusOr<std::optional<Relation>> TryFusedGroupedJoin(
    TransactionContext& context, const Relation& left, const Relation& right,
    const std::vector<Expression>& predicates, JoinType join_type,
    const Scope* outer, const CteMap& ctes, GroupedJoinFusionSpec* fusion,
    const DeferredLSTableProbe* deferred_left_probe = nullptr);

StatusOr<std::optional<Relation>> TryStreamGroupedBaseJoin(
    TransactionContext& context, const SelectStatement& statement,
    const Scope* outer, const CteMap& ctes,
    const std::vector<std::vector<slot_t>>& projections,
    const std::vector<Relation>& schemas, GroupedJoinFusionSpec* fusion);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_FUSED_GROUPED_JOIN_HPP
