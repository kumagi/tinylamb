/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_FUSED_GROUPED_JOIN_HPP
#define TINYLAMB_EXECUTOR_DETAIL_FUSED_GROUPED_JOIN_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/constants.hpp"
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
  std::vector<IntegerPeekCompare> integer_peeks;
  std::vector<NullPeekCompare> null_peeks;
  std::vector<DeferredLSTablePkLookup> pk_lookups;
};

template <typename Key, typename Hash = std::hash<Key>,
          typename Equal = std::equal_to<Key>>
class FlatJoinBuckets {
  struct Entry;
  static constexpr uint32_t kNoEntry = std::numeric_limits<uint32_t>::max();

 public:
  class const_iterator {
   public:
    const_iterator() = default;
    const_iterator(const FlatJoinBuckets* buckets, uint32_t index)
        : buckets_(buckets), index_(index) {}

    const Entry& operator*() const { return buckets_->entries_[index_]; }
    const Entry* operator->() const { return &buckets_->entries_[index_]; }
    const_iterator& operator++() {
      index_ = buckets_->entries_[index_].duplicate_next;
      return *this;
    }
    bool operator==(const const_iterator& other) const = default;

   private:
    const FlatJoinBuckets* buckets_{nullptr};
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
  bool emplace(KeyLike&& key, const Row* row) {
    const size_t bucket = BucketIndex(key);
    const uint32_t head = FindHead(key, bucket);
    const auto index = static_cast<uint32_t>(entries_.size());
    entries_.push_back(
        Entry{std::forward<KeyLike>(key), row, head, buckets_[bucket]});
    buckets_[bucket] = index;
    return head == kNoEntry;
  }

  [[nodiscard]] std::pair<const_iterator, const_iterator> equal_range(
      const Key& key) const {
    const size_t bucket = BucketIndex(key);
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

 private:
  struct Entry {
    Key first;
    const Row* second;
    uint32_t duplicate_next;
    uint32_t collision_next;
  };

  [[nodiscard]] size_t BucketIndex(const Key& key) const {
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

struct ProgramShape {
  int32_t direct_column{-1};
  bool fast{false};
  int32_t extract_date_col{-1};
  uint16_t extract_date_part{0};
  int32_t mul_col0{-1};
  int32_t mul_col1{-1};
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
    kDirectDoubleCol,
    kMulOneMinus,
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
  struct IntCaseComparison {
    int32_t left{-1};
    int32_t subtract{-1};
    BinaryOperation op{BinaryOperation::kEquals};
    int64_t constant{0};
  };
  std::vector<IntCaseComparison> case_int_comparisons;

  [[nodiscard]] bool HasPairDirect() const noexcept {
    return direct_column >= 0 || extract_date_col >= 0 || mul_col0 >= 0 ||
           mul_one_minus_col0 >= 0 || profit_col0 >= 0 ||
           (case_cond != CaseCondKind::kNone && case_val != CaseValKind::kNone);
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
