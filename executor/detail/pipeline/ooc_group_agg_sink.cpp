/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/memory_budget.hpp"
#include "executor/detail/ooc/external_sorter.hpp"
#include "executor/detail/pipeline/ooc_sinks.hpp"
#include "executor/detail/pipeline/pipeline_ops.hpp"
#include "type/row_block_codec.hpp"

namespace tinylamb {
namespace {

uint64_t HashGroupKeyRow(const Row& key) { return std::hash<Row>{}(key); }

// Returns the aggregate when `aggregates` is exactly one plain DISTINCT
// aggregate whose DISTINCT semantics the sorted skew fallback can reproduce
// by feeding each distinct value once to a DISTINCT-stripped accumulator.
// Every modifier the sorted path cannot replay disqualifies it: WHERE
// filters, HAVING modifiers, inner ORDER BY/LIMIT, secondary/trailing args
// and inner GROUP BY (NeedsGroupContext/HasInnerGroupBy cover them all).
// Saturating add for per-group byte accounting: an extreme cardinality must
// not wrap accounted_bytes below the budget so that kTooBigData still fires.
constexpr size_t SaturatingAddBytes(size_t a, size_t b) noexcept {
  return b > std::numeric_limits<size_t>::max() - a
             ? std::numeric_limits<size_t>::max()
             : a + b;
}

// Only COUNT/SUM/AVG qualify: they are the non-idempotent-insensitive
// aggregates whose distinct set is the only growing state, and a plain
// non-distinct clone over deduplicated input yields the DISTINCT result.
[[nodiscard]] const AggregateExpression* SingleStrippableDistinctAggregate(
    const std::vector<const AggregateExpression*>& aggregates) noexcept {
  if (aggregates.size() != 1 || aggregates[0] == nullptr) {
    return nullptr;
  }
  const AggregateExpression* agg = aggregates[0];
  const AggregationType t = agg->GetType();
  if (!agg->Distinct() || agg->HasWhereFilter() || agg->NeedsGroupContext() ||
      agg->HasInnerGroupBy() || agg->Child() == nullptr ||
      (t != AggregationType::kCount && t != AggregationType::kSum &&
       t != AggregationType::kAvg)) {
    return nullptr;
  }
  return agg;
}

// Streams finished aggregate rows lazily from PartitionLeafSet one leaf at a
// time so working memory stays <= B_op throughout Finalize -> NextBatch.
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is
// non-copyable by design
class GroupAggLeafSource final
    : public PipelineSource {  // NOLINT(cppcoreguidelines-special-member-functions)
 public:
  GroupAggLeafSource(
      SpillStore* store, PartitionLeafSet leaves, Schema input_schema,
      std::vector<Expression> group_keys,
      std::vector<std::optional<slot_t>> group_key_slots,
      std::vector<Expression> aggregate_owners,
      std::vector<const AggregateExpression*> aggregates, bool partial_mode,
      bool has_collection_agg, std::optional<slot_t> single_distinct_child_slot,
      std::shared_ptr<AggregateExpression> skew_strip_aggregate,
      size_t budget_bytes, QueryMemoryGrant* grant,
      std::optional<QueryMemoryGrant::ActiveConsumerGuard> consumer_guard,
      std::string label, std::shared_ptr<std::atomic<size_t>> output_groups)
      : store_(store),
        leaves_(std::move(leaves)),
        input_schema_(std::move(input_schema)),
        group_keys_(std::move(group_keys)),
        group_key_slots_(std::move(group_key_slots)),
        aggregate_owners_(std::move(aggregate_owners)),
        aggregates_(std::move(aggregates)),
        partial_mode_(partial_mode),
        has_collection_agg_(has_collection_agg),
        single_distinct_child_slot_(single_distinct_child_slot),
        skew_strip_aggregate_(std::move(skew_strip_aggregate)),
        budget_bytes_(budget_bytes),
        grant_(grant),
        consumer_guard_(std::move(consumer_guard)),
        label_(std::move(label)),
        output_groups_(std::move(output_groups)) {
    shapes_.resize(aggregates_.size());
    for (size_t a = 0; a < aggregates_.size(); ++a) {
      if (aggregates_[a] != nullptr) {
        shapes_[a] = relational_detail::AggregateAccumulator::AnalyzeShape(
            aggregates_[a], &input_schema_);
        if (shapes_[a].distinct_mode) {
          has_distinct_state_agg_ = true;
        }
      }
    }
    if (skew_strip_aggregate_ != nullptr) {
      // The skew table accumulates the DISTINCT-stripped clone instead of
      // the original aggregate: the sorter already feeds each distinct
      // value once, so the clone's plain state stays O(1) per group.
      skew_table_aggregates_.push_back(skew_strip_aggregate_.get());
    }
    if (grant_ != nullptr) {
      const size_t refill =
          budget_bytes_ > 0
              ? std::min<size_t>(
                    QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes,
                    std::max<size_t>(65536, budget_bytes_ / 4))
              : QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes;
      leaf_grant_cache_.emplace(grant_, refill);
    }
  }

  ~GroupAggLeafSource() override { ReleaseResources(); }

  StatusOr<bool> NextBatch(PipelineContext* /*ctx*/, DataChunk* out,
                           size_t max_rows) override {
    out->Reset();
    if (max_rows == 0) {
      return false;
    }
    while (out->Empty()) {
      if (active_table_ != nullptr) {
        const size_t start = active_table_cursor_;
        const size_t end = std::min(active_table_->size(), start + max_rows);
        out->Reserve(end - start);
        for (size_t g = start; g < end; ++g) {
          ASSIGN_OR_RETURN(Row, finished, active_table_->Finish(g));
          out->Append(std::move(finished));
        }
        if (output_groups_ != nullptr) {
          output_groups_->fetch_add(end - start, std::memory_order_relaxed);
        }
        active_table_cursor_ = end;
        if (active_table_cursor_ >= active_table_->size()) {
          active_table_.reset();
          active_table_cursor_ = 0;
          if (leaf_grant_cache_.has_value()) {
            leaf_grant_cache_->ReleaseAll();
          }
        }
        return true;
      }

      if (skew_sorter_ != nullptr) {
        RETURN_IF_FAIL(PumpSkewSorter(out, max_rows));
        if (!out->Empty()) {
          if (output_groups_ != nullptr) {
            output_groups_->fetch_add(out->Size(), std::memory_order_relaxed);
          }
          return true;
        }
        continue;
      }

      if (leaf_idx_ >= leaves_.leaves.size()) {
        ReleaseResources();
        return false;
      }

      const size_t current_leaf = leaf_idx_++;
      const PartitionLeaf& leaf = leaves_.leaves[current_leaf];
      const size_t op_budget = EffectiveLeafBudgetBytes();
      if (leaf.skew_fallback ||
          (op_budget > 0 && leaf.estimated_bytes > op_budget)) {
        RETURN_IF_FAIL(StartSkewLeaf(leaf));
        continue;
      }

      EnsureConsumerGuard();
      // Partition fits cleanly in memory: hash-aggregate/merge this single
      // leaf. If concurrent pressure exhausts grant mid-leaf, fall back to
      // external sorted aggregation without dropping the leaf prematurely.
      auto table =
          std::make_unique<AggregateGroupTable>(&input_schema_, &aggregates_);
      bool fallback_to_skew = false;
      auto account_new_group = [&](const Row& key_row) -> Status {
        if (!leaf_grant_cache_.has_value()) {
          return Status::kSuccess;
        }
        const size_t group_cost =
            EstimateRowMemoryBytes(key_row) +
            (aggregates_.size() *
             (sizeof(relational_detail::AggregateAccumulator) + 16)) +
            64;
        if (!leaf_grant_cache_->Reserve(group_cost)) {
          fallback_to_skew = true;
          return StatusError(
              StatusCode::kMemoryLimitExceeded,
              "OOC group-agg leaf grant exhausted: cannot reserve " +
                  std::to_string(group_cost) +
                  " bytes for a distinct group (pool=query)");
        }
        return Status::kSuccess;
      };
      Status read_st = Status::kSuccess;
      if (partial_mode_) {
        read_st = RecursivePartitioner::ReadLeafRows(
            store_, leaf, [&](Row&& packed) -> Status {
              const size_t before = table->size();
              RETURN_IF_FAIL(MergePackedPartialRow(table.get(), packed));
              if (table->size() > before) {
                RETURN_IF_FAIL(
                    account_new_group(table->Key(table->size() - 1)));
              }
              return Status::kSuccess;
            });
      } else {
        read_st = RecursivePartitioner::ReadLeafRows(
            store_, leaf, [&](Row&& raw) -> Status {
              ASSIGN_OR_RETURN(Row, key, ExtractKeyFromRaw(raw));
              const size_t before = table->size();
              // FindOrCreate(Row) takes by value, so this lvalue is copied
              // into the table's index node. Keep the local `key` for group
              // accounting below; Accumulate uses the original raw row.
              ASSIGN_OR_RETURN(size_t, gid, table->FindOrCreate(key));
              if (table->size() > before) {
                RETURN_IF_FAIL(account_new_group(key));
              }
              return table->Accumulate(gid, raw);
            });
      }
      if (fallback_to_skew) {
        table.reset();
        if (leaf_grant_cache_.has_value()) {
          leaf_grant_cache_->ReleaseAll();
        }
        RETURN_IF_FAIL(StartSkewLeaf(leaf));
        continue;
      }
      RETURN_IF_FAIL(read_st);
      leaves_.DropLeaf(current_leaf);

      if (table->empty()) {
        if (leaf_grant_cache_.has_value()) {
          leaf_grant_cache_->ReleaseAll();
        }
        continue;
      }
      active_table_ = std::move(table);
      active_table_cursor_ = 0;
    }
    return false;
  }

  [[nodiscard]] std::string Describe() const override { return label_; }

 private:
  [[nodiscard]] StatusOr<Row> ExtractKeyFromRaw(const Row& raw) const {
    std::vector<Value> vals;
    vals.reserve(group_keys_.size());
    for (size_t i = 0; i < group_keys_.size(); ++i) {
      Value v;
      const std::optional<slot_t> key_slot = group_key_slots_[i];
      if (key_slot.has_value() && *key_slot < raw.Count()) {
        v = raw[*key_slot];
      } else {
        ASSIGN_OR_RETURN(Value, eval_val,
                         EvalRowExpression(group_keys_[i], input_schema_, raw));
        v = std::move(eval_val);
      }
      vals.push_back(relational_detail::CanonicalDistinctValue(v));
    }
    return Row(std::move(vals));
  }

  Status MergePackedPartialRow(AggregateGroupTable* table,
                               const Row& packed) const {
    const size_t gcnt = group_keys_.size();
    if (packed.Count() < gcnt || packed.Count() - gcnt != aggregates_.size()) {
      return StatusError(StatusCode::kCorrupt,
                         "corrupt packed partial aggregate row");
    }
    std::vector<Value> key_vals;
    key_vals.reserve(gcnt);
    for (size_t i = 0; i < gcnt; ++i) {
      key_vals.push_back(packed[i]);
    }
    ASSIGN_OR_RETURN(size_t, gid,
                     table->FindOrCreate(Row(std::move(key_vals))));
    return MergePackedPartialStates(table, gid, packed);
  }

  Status MergePackedPartialStates(AggregateGroupTable* table, size_t gid,
                                  const Row& packed) const {
    const size_t gcnt = group_keys_.size();
    for (size_t a = 0; a < aggregates_.size(); ++a) {
      if (aggregates_[a] == nullptr) {
        continue;
      }
      const Value& blob_val = packed[gcnt + a];
      if (blob_val.type != ValueType::kVarChar) {
        return StatusError(StatusCode::kCorrupt,
                           "invalid packed partial aggregate state");
      }
      std::string_view sv = blob_val.value.varchar_value;
      ASSIGN_OR_RETURN(relational_detail::AggregateAccumulator*, state, table->StateFor(gid, a));
      RETURN_IF_FAIL(state->MergeSerializedPartialState(&sv, shapes_[a]));
    }
    return Status::kSuccess;
  }

  [[nodiscard]] bool UseSingleCountDistinctSort() const noexcept {
    return single_distinct_child_slot_.has_value() ||
           (!partial_mode_ &&
            SingleStrippableDistinctAggregate(aggregates_) != nullptr);
  }

  void EnsureConsumerGuard() {
    if (grant_ != nullptr && !consumer_guard_.has_value()) {
      consumer_guard_.emplace(grant_);
    }
  }

  void ReleaseResources() noexcept {
    active_table_.reset();
    active_table_cursor_ = 0;
    skew_table_.reset();
    skew_sorter_.reset();
    if (leaf_grant_cache_.has_value()) {
      leaf_grant_cache_->ReleaseAll();
    }
    consumer_guard_.reset();
  }

  [[nodiscard]] size_t EffectiveLeafBudgetBytes() const noexcept {
    size_t b = budget_bytes_;
    if (grant_ != nullptr) {
      const size_t grant_op = grant_->OperatorBudgetBytes();
      if (b == 0 || grant_op < b) {
        b = grant_op;
      }
    }
    return b;
  }

  Status StartSkewLeaf(const PartitionLeaf& leaf) {
    if (leaf_grant_cache_.has_value()) {
      leaf_grant_cache_->ReleaseAll();
    }
    // Keep our ActiveConsumerGuard active for the skew sorter: ExternalSorter
    // does not register one of its own (it only consults the grant), and
    // dropping ours would undercount the active consumers.
    // consumer_guard_ stays alive through skew_sorter_ usage.
    const size_t gcnt = group_keys_.size();
    std::vector<OocSortKey> sort_keys;
    if (partial_mode_) {
      if (gcnt != 0 && gcnt - 1 > std::numeric_limits<slot_t>::max()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "partial aggregate key column index out of range");
      }
      sort_keys.reserve(gcnt);
      for (size_t i = 0; i < gcnt; ++i) {
        sort_keys.push_back(OocSortKey{.column_index = static_cast<slot_t>(i)});
      }
      ExternalSorter::Options opts;
      opts.spill_store = store_;
      opts.memory_budget_bytes = budget_bytes_;
      opts.grant = grant_;
      opts.keys = std::move(sort_keys);
      skew_sorter_ = std::make_unique<ExternalSorter>(std::move(opts));
      RETURN_IF_FAIL(RecursivePartitioner::ReadLeafRows(
          store_, leaf,
          [&](Row&& r) { return skew_sorter_->AddRow(std::move(r)); }));
    } else {
      // Raw mode: prefix sort keys as trailing columns [raw_cols...,
      // key_0..g-1]
      // (+ optional distinct_val column for the O(1) skewed
      // single-DISTINCT path).
      // The trailing key offset is the ACTUAL row width, not
      // input_schema_.ColumnCount(): a partial projection / unset schema may
      // be narrower than the rows a chunk carries, and schema-derived key
      // indices would then point at raw input columns instead of the appended
      // group-key / distinct_val columns (wrong group boundaries -> WA).
      const bool single_cd = UseSingleCountDistinctSort();
      auto build_sorter = [&](size_t raw_width) -> Status {
        // T4853: the raw suffix indexes land in OocSortKey::column_index
        // (uint16); reject wrap-around instead of silently aliasing a low
        // input column (same rule as the ooc_sort_distinct_sink sites).
        const size_t max_slot = std::numeric_limits<slot_t>::max();
        const size_t key_count = gcnt + static_cast<size_t>(single_cd);
        if (key_count != 0 &&
            (raw_width > max_slot || key_count - 1 > max_slot - raw_width)) {
          return StatusError(StatusCode::kInvalidArgument,
                             "sort key column index out of range");
        }
        std::vector<OocSortKey> keys;
        keys.reserve(key_count);
        for (size_t i = 0; i < gcnt; ++i) {
          keys.push_back(
              OocSortKey{.column_index = static_cast<slot_t>(raw_width + i)});
        }
        if (single_cd) {
          keys.push_back(OocSortKey{
              .column_index = static_cast<slot_t>(raw_width + gcnt)});
        }
        ExternalSorter::Options opts;
        opts.spill_store = store_;
        opts.memory_budget_bytes = budget_bytes_;
        opts.grant = grant_;
        opts.keys = std::move(keys);
        skew_sorter_ = std::make_unique<ExternalSorter>(std::move(opts));
        return Status::kSuccess;
      };
      skew_raw_width_ = input_schema_.ColumnCount();
      RETURN_IF_FAIL(build_sorter(skew_raw_width_));
      bool first_row = true;
      RETURN_IF_FAIL(RecursivePartitioner::ReadLeafRows(
          store_, leaf, [&](Row&& raw) -> Status {
            if (raw.Count() != skew_raw_width_) {
              if (!first_row) {
                return StatusError(StatusCode::kCorrupt,
                                   "inconsistent raw aggregate spill row width");
              }
              // First row reveals the real width; rebuild the still-empty
              // sorter so its keys land on the appended columns.
              skew_raw_width_ = raw.Count();
              RETURN_IF_FAIL(build_sorter(skew_raw_width_));
            }
            first_row = false;
            ASSIGN_OR_RETURN(Row, key, ExtractKeyFromRaw(raw));
            raw.values_.reserve(raw.Count() + gcnt + 1);
            for (const Value& kv : key.values_) {
              raw.values_.push_back(kv);
            }
            if (single_cd) {
              Value dv;
              if (single_distinct_child_slot_.has_value()) {
                const slot_t dslot = *single_distinct_child_slot_;
                dv = dslot < skew_raw_width_ ? raw[dslot] : Value();
              } else {
                ASSIGN_OR_RETURN(Value, ev,
                                 EvalRowExpression(aggregates_[0]->Child(),
                                                   input_schema_, raw));
                dv = std::move(ev);
              }
              raw.values_.push_back(
                  relational_detail::CanonicalDistinctValue(dv));
            }
            return skew_sorter_->AddRow(std::move(raw));
          }));
    }
    RETURN_IF_FAIL(skew_sorter_->Finish());
    skew_table_.reset();
    have_active_skew_key_ = false;
    have_prev_distinct_ = false;
    return Status::kSuccess;
  }

  Status PumpSkewSorter(DataChunk* out, size_t max_rows) {
    const size_t gcnt = group_keys_.size();
    const bool single_cd = !partial_mode_ && UseSingleCountDistinctSort();
    const size_t key_start = partial_mode_ ? 0 : skew_raw_width_;
    const size_t suffix_count = partial_mode_ ? aggregates_.size() : static_cast<size_type>(single_cd);
    Row sorted_row;
    while (out->Size() < max_rows) {
      ASSIGN_OR_RETURN(bool, has, skew_sorter_->NextRow(&sorted_row));
      if (!has) {
        if (skew_table_ != nullptr && !skew_table_->empty()) {
          ASSIGN_OR_RETURN(Row, fin, skew_table_->Finish(0));
          out->Append(std::move(fin));
        }
        skew_table_.reset();
        have_active_skew_key_ = false;
        skew_sorter_.reset();
        leaves_.DropLeaf(leaf_idx_ - 1);
        break;
      }

      const size_t total_columns = sorted_row.Count();
      if (key_start > total_columns || gcnt > total_columns - key_start ||
          total_columns - key_start - gcnt != suffix_count) {
        return StatusError(StatusCode::kCorrupt,
                           "skew aggregate spill row width mismatch");
      }
      Row cur_key;
      auto begin = sorted_row.values_.begin() + static_cast<ptrdiff_t>(key_start);
      cur_key.values_.assign(begin, begin + static_cast<ptrdiff_t>(gcnt));

      if (have_active_skew_key_ &&
          !relational_detail::GroupKeyEqual{}(active_skew_key_, cur_key)) {
        // Flush the finished group before starting the next -> at most one
        // group's state is resident regardless of partition skew. Per-group
        // bytes are O(1) on the stripped-distinct path and budget-guarded
        // (kTooBigData) for collection/distinct-state aggregates below.
        ASSIGN_OR_RETURN(Row, fin, skew_table_->Finish(0));
        out->Append(std::move(fin));
        skew_table_.reset();
        have_active_skew_key_ = false;
        have_prev_distinct_ = false;
        skew_group_bytes_ = 0;
      }

      if (!have_active_skew_key_) {
        active_skew_key_ = std::move(cur_key);
        skew_table_ = std::make_unique<AggregateGroupTable>(
            &input_schema_, skew_strip_aggregate_ != nullptr
                                ? &skew_table_aggregates_
                                : &aggregates_);
        ASSIGN_OR_RETURN(size_t, gid,
                         skew_table_->FindOrCreate(active_skew_key_));
        (void)gid;
        have_active_skew_key_ = true;
        skew_group_bytes_ = 0;
      }

      if (partial_mode_) {
        RETURN_IF_FAIL(MergePackedPartialStates(skew_table_.get(), 0, sorted_row));
      } else if (single_cd) {
        // Sorted by (group_key, distinct_val): deduplicate adjacent equal
        // distinct_vals and feed each once into the DISTINCT-stripped
        // skew-table clone, so the accumulator keeps O(1) state per group
        // instead of materializing a distinct-value set!
        const Value& dval = sorted_row[skew_raw_width_ + gcnt];
        if (!dval.IsNull()) {
          if (!have_prev_distinct_ || !relational_detail::DistinctValueEqual{}(
                                          prev_distinct_val_, dval)) {
            prev_distinct_val_ = dval;
            have_prev_distinct_ = true;
            // Only one distinct value is resident here, but it can still be a
            // huge VARCHAR. Charge that value (replacing the previous one's
            // charge) so a single oversized value trips the budget guard.
            skew_group_bytes_ = EstimateValueMemoryBytes(prev_distinct_val_);
            if (budget_bytes_ > 0 && skew_group_bytes_ > budget_bytes_) {
              return StatusError(
                  StatusCode::kTooBigData,
                  "Single aggregate group exceeds operator memory budget");
            }
            ASSIGN_OR_RETURN(relational_detail::AggregateAccumulator*, state, skew_table_->StateFor(0, 0));
            RETURN_IF_FAIL(state->Add(dval));
          }
        }
      } else {
        sorted_row.values_.resize(skew_raw_width_);
        RETURN_IF_FAIL(skew_table_->Accumulate(0, sorted_row));
        size_t accounted_bytes = 0;
        if (has_collection_agg_) {
          // Collection aggregates (ARRAY_AGG/STRING_AGG) buffer every input
          // row, so the raw-row byte estimate is the retained-state size.
          skew_group_bytes_ =
              SaturatingAddBytes(skew_group_bytes_, EstimateRowMemoryBytes(sorted_row));
          accounted_bytes = SaturatingAddBytes(accounted_bytes, skew_group_bytes_);
        }
        if (has_distinct_state_agg_) {
          // DISTINCT accumulators retain one set entry per distinct value,
          // not per row: charging raw row bytes would reject
          // duplicate-heavy inputs whose real footprint is tiny. Charge the
          // live sets' real retained bytes (VARCHAR payloads included) via
          // DistinctRetainedBytes, saturating on add so an extreme cardinality
          // cannot wrap past the budget check (a stripped single DISTINCT
          // aggregate never reaches this branch -- its clone is not
          // distinct_mode).
          size_t distinct_bytes = 0;
          for (size_t a = 0; a < aggregates_.size(); ++a) {
            if (aggregates_[a] == nullptr || !shapes_[a].distinct_mode) {
              continue;
            }
            ASSIGN_OR_RETURN(relational_detail::AggregateAccumulator*, st, skew_table_->StateFor(0, a));
            distinct_bytes =
                SaturatingAddBytes(distinct_bytes, st->DistinctRetainedBytes());
          }
          accounted_bytes = SaturatingAddBytes(accounted_bytes, distinct_bytes);
        }
        if ((has_collection_agg_ || has_distinct_state_agg_) &&
            budget_bytes_ > 0 && accounted_bytes > budget_bytes_) {
          return StatusError(
              StatusCode::kTooBigData,
              "Single aggregate group exceeds operator memory budget");
        }
      }
    }
    return Status::kSuccess;
  }

  SpillStore* store_{nullptr};
  PartitionLeafSet leaves_;
  Schema input_schema_;
  std::vector<Expression> group_keys_;
  std::vector<std::optional<slot_t>> group_key_slots_;
  std::vector<Expression> aggregate_owners_;
  std::vector<const AggregateExpression*> aggregates_;
  // AnalyzeShape scans strings; compute once per aggregate rather than on
  // every spilled partial-state merge below.
  std::vector<relational_detail::AggregateAccumulator::Shape> shapes_;
  bool partial_mode_{false};
  bool has_collection_agg_{false};
  std::optional<slot_t> single_distinct_child_slot_;
  // Owned DISTINCT-stripped clone used by the skew table when the whole
  // aggregate list is one plain DISTINCT aggregate (see
  // SingleStrippableDistinctAggregate). Non-null implies the skew path is
  // the sorted single-distinct path, so its per-group state stays O(1).
  std::shared_ptr<AggregateExpression> skew_strip_aggregate_;
  std::vector<const AggregateExpression*> skew_table_aggregates_;
  bool has_distinct_state_agg_{false};
  size_t budget_bytes_{0};
  QueryMemoryGrant* grant_{nullptr};
  std::optional<QueryMemoryGrant::ActiveConsumerGuard> consumer_guard_;
  std::optional<QueryMemoryGrant::LocalBatchCache> leaf_grant_cache_;
  std::string label_;

  size_t leaf_idx_{0};
  std::unique_ptr<AggregateGroupTable> active_table_;
  size_t active_table_cursor_{0};

  std::unique_ptr<ExternalSorter> skew_sorter_;
  std::unique_ptr<AggregateGroupTable> skew_table_;
  Row active_skew_key_;
  bool have_active_skew_key_{false};
  size_t skew_group_bytes_{0};
  size_t skew_raw_width_{0};
  Value prev_distinct_val_;
  bool have_prev_distinct_{false};
  std::shared_ptr<std::atomic<size_t>> output_groups_;
};

}  // namespace

// --- OocGroupAggSink ---

OocGroupAggSink::OocGroupAggSink(
    Schema input_schema, std::vector<Expression> group_keys,
    std::vector<const AggregateExpression*> aggregates, Schema output_schema,
    std::string label)
    : input_schema_(std::move(input_schema)),
      group_keys_(std::move(group_keys)),
      aggregates_(std::move(aggregates)),
      output_schema_(std::move(output_schema)),
      label_(std::move(label)) {}

OocGroupAggSink::OocGroupAggSink(Schema input_schema,
                                 std::vector<Expression> group_keys,
                                 std::vector<Expression> agg_expressions,
                                 Schema output_schema, std::string label)
    : input_schema_(std::move(input_schema)),
      group_keys_(std::move(group_keys)),
      aggregate_owners_(std::move(agg_expressions)),
      output_schema_(std::move(output_schema)),
      label_(std::move(label)) {
  aggregates_.reserve(aggregate_owners_.size());
  for (const Expression& expr : aggregate_owners_) {
    if (expr && expr->Type() == TypeTag::kAggregateExp) {
      aggregates_.push_back(&expr->AsAggregateExpression());
    } else {
      aggregates_.push_back(nullptr);
    }
  }
}

OocGroupAggSink::~OocGroupAggSink() {
  partitioner_.reset();
  agg_table_.reset();
  raw_rows_.clear();
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
}

Status OocGroupAggSink::Init(PipelineContext* ctx, size_t /*worker_count*/) {
  consumer_guard_.emplace(&ctx->grant);
  budget_bytes_ = ctx->OperatorBudgetBytes();
  partition_key_status_ = Status::kSuccess;
  const size_t refill =
      budget_bytes_ > 0
          ? std::min<size_t>(
                QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes,
                std::max<size_t>(65536, budget_bytes_ / 4))
          : QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes;
  grant_cache_.emplace(&ctx->grant, refill);

  group_key_slots_.assign(group_keys_.size(), std::nullopt);
  for (size_t i = 0; i < group_keys_.size(); ++i) {
    if (group_keys_[i] && group_keys_[i]->Type() == TypeTag::kColumnValue) {
      const int offset =
          input_schema_.Offset(group_keys_[i]->AsColumnValue().GetColumnName());
      // T4853: schema offsets also feed slot_t; keep a too-wide schema
      // column out of the fast-path slot list rather than wrapping it.
      if (offset >= 0 &&
          std::cmp_less_equal(offset,
              std::numeric_limits<slot_t>::
                  max())) {  // NOLINT(modernize-use-integer-sign-comparison)
        group_key_slots_[i] = static_cast<slot_t>(offset);
      }
    }
  }

  all_combinable_ = true;
  has_collection_agg_ = false;
  shapes_.resize(aggregates_.size());
  for (size_t ai = 0; ai < aggregates_.size(); ++ai) {
    const AggregateExpression* agg = aggregates_[ai];
    if (agg == nullptr) {
      continue;
    }
    // AnalyzeShape performs string scans; compute it once per aggregate and
    // reuse it for every group and every spilled partial-state merge.
    shapes_[ai] = relational_detail::AggregateAccumulator::AnalyzeShape(
        agg, &input_schema_);
    relational_detail::AggregateAccumulator probe(agg, shapes_[ai]);
    if (!probe.CanSerializePartial()) {
      all_combinable_ = false;
    }
    const AggregationType t = agg->GetType();
    if (t == AggregationType::kArrayAgg || t == AggregationType::kStringAgg ||
        t == AggregationType::kArrayConcatAgg ||
        t == AggregationType::kPercentileCont ||
        t == AggregationType::kApproxQuantiles ||
        t == AggregationType::kApproxTopCount ||
        t == AggregationType::kApproxTopSum || !agg->InnerOrderBy().empty()) {
      has_collection_agg_ = true;
    }
  }

  // Detect a single plain COUNT/SUM/AVG(DISTINCT expr) for the O(1) sorted
  // skew fallback: the sorter feeds each distinct value once, so the skew
  // table accumulates a DISTINCT-stripped clone whose state stays O(1).
  single_distinct_child_slot_.reset();
  skew_strip_aggregate_.reset();
  const AggregateExpression* strip_agg =
      SingleStrippableDistinctAggregate(aggregates_);
  if (strip_agg != nullptr) {
    auto stripped = std::make_shared<AggregateExpression>(
        strip_agg->GetType(), strip_agg->Child(), false);
    stripped->SetIgnoreNulls(strip_agg->IgnoreNulls());
    skew_strip_aggregate_ = std::move(stripped);
    if (strip_agg->Child()->Type() == TypeTag::kColumnValue) {
      const int off = input_schema_.Offset(
          strip_agg->Child()->AsColumnValue().GetColumnName());
      // T4853: same uint16 bound as the group-key slots above.
      if (off >= 0 &&
          std::cmp_less_equal(off,
              std::numeric_limits<slot_t>::
                  max())) {  // NOLINT(modernize-use-integer-sign-comparison)
        single_distinct_child_slot_ = static_cast<slot_t>(off);
      }
    }
  }

  agg_table_ =
      std::make_unique<AggregateGroupTable>(&input_schema_, &aggregates_);
  mem_bytes_ = 0;
  total_input_rows_ = 0;
  spilled_ = false;
  return Status::kSuccess;
}

Status OocGroupAggSink::ExtractCanonicalGroupKeys(const Row& input_row,
                                                  Row* canonical_out) const {
  canonical_out->values_.clear();
  canonical_out->values_.reserve(group_keys_.size());
  for (size_t i = 0; i < group_keys_.size(); ++i) {
    Value v;
    const std::optional<slot_t> key_slot = group_key_slots_[i];
    if (key_slot.has_value() && *key_slot < input_row.Count()) {
      v = input_row[*key_slot];
    } else {
      ASSIGN_OR_RETURN(
          Value, eval_val,
          EvalRowExpression(group_keys_[i], input_schema_, input_row));
      v = std::move(eval_val);
    }
    canonical_out->values_.push_back(
        relational_detail::CanonicalDistinctValue(v));
  }
  return Status::kSuccess;
}

Status OocGroupAggSink::FlushPartialTableToPartitioner() {
  if (agg_table_ == nullptr || agg_table_->empty()) {
    return Status::kSuccess;
  }
  const size_t gcnt = group_keys_.size();
  for (size_t g = 0; g < agg_table_->size(); ++g) {
    const Row& key = agg_table_->Key(g);
    std::vector<Value> packed;
    packed.reserve(gcnt + aggregates_.size());
    for (const Value& kv : key.values_) {
      packed.push_back(kv);
    }
    std::string blob;
    for (size_t a = 0; a < aggregates_.size(); ++a) {
      if (aggregates_[a] == nullptr) {
        packed.emplace_back();
        continue;
      }
      blob.clear();
      ASSIGN_OR_RETURN(relational_detail::AggregateAccumulator*, state, agg_table_->StateFor(g, a));
      RETURN_IF_FAIL(state->SerializePartialState(&blob));
      packed.emplace_back(std::move(blob));
    }
    RETURN_IF_FAIL(partitioner_->AddRow(Row(std::move(packed))));
  }
  agg_table_ =
      std::make_unique<AggregateGroupTable>(&input_schema_, &aggregates_);
  mem_bytes_ = 0;
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
  return Status::kSuccess;
}

Status OocGroupAggSink::TransitionNonCombinableToSpill(PipelineContext* ctx) {
  if (spilled_) {
    return Status::kSuccess;
  }
  RecursivePartitioner::Options opts;
  opts.spill_store = &ctx->spill_store;
  opts.memory_budget_bytes = budget_bytes_;
  opts.grant = &ctx->grant;
  auto candidate = std::make_unique<RecursivePartitioner>(
      opts, [this](const Row& raw) -> uint64_t {
        Row key;
        Status st = ExtractCanonicalGroupKeys(raw, &key);
        if (st != Status::kSuccess) {
          // The hash functor cannot return Status. Remember the first failure
          // so Finalize surfaces it instead of producing a wrong grouping.
          if (partition_key_status_.ok()) {
            partition_key_status_ = st;
          }
          return 0;
        }
        return HashGroupKeyRow(key);
      });
  for (const Row& r : raw_rows_) {
    // The partitioner's hash functor cannot return Status; it records an
    // ExtractCanonicalGroupKeys failure in partition_key_status_ and routes
    // the row to bucket 0. Stop immediately so subsequent rows are not
    // needlessly spilled into the wrong bucket and the error surfaces here.
    RETURN_IF_FAIL(partition_key_status_);
    RETURN_IF_FAIL(candidate->AddRow(r));
  }
  RETURN_IF_FAIL(partition_key_status_);
  partitioner_ = std::move(candidate);
  // T3462: shrink_to_fit can throw bad_alloc across the Status boundary;
  // swap-with-empty frees the capacity without allocating (T2060 idiom).
  std::vector<Row>().swap(raw_rows_);
  mem_bytes_ = 0;
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
  spilled_ = true;
  return Status::kSuccess;
}

Status OocGroupAggSink::Consume(PipelineContext* ctx, size_t /*worker_id*/,
                                const DataChunk& input) {
  if (input.Empty()) {
    return Status::kSuccess;
  }
  total_input_rows_ += input.Size();
  const bool always_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();

  if (group_keys_.empty() && all_combinable_) {
    // Ungrouped combinable scalar aggregation (`SELECT SUM(x), COUNT(*) FROM
    // t`): holds 1 accumulator tuple cleanly in O(1) memory.
    if (agg_table_->empty()) {
      RETURN_IF_FAIL(agg_table_->FindOrCreate(Row{}).GetStatus());
    }
    for (size_t r = 0; r < input.Size(); ++r) {
      RETURN_IF_FAIL(agg_table_->Accumulate(0, input.RowAt(r)));
    }
    return Status::kSuccess;
  }

  if (all_combinable_) {
    if (!spilled_ && always_spill) {
      RecursivePartitioner::Options opts;
      opts.spill_store = &ctx->spill_store;
      opts.memory_budget_bytes = budget_bytes_;
      opts.grant = &ctx->grant;
      const size_t gcnt = group_keys_.size();
      partitioner_ = std::make_unique<RecursivePartitioner>(
          opts, [gcnt](const Row& packed) -> uint64_t {
            uint64_t h = 0x9e3779b97f4a7c15ULL;
            for (size_t i = 0; i < gcnt && i < packed.Count(); ++i) {
              h = (h << 7) | (h >> 57);
              h ^= std::hash<Value>{}(packed[i]);
              h *= 0x100000001b3ULL;
            }
            return h;
          });
      RETURN_IF_FAIL(FlushPartialTableToPartitioner());
      spilled_ = true;
    }

    const size_t pre_agg_cap =
        spilled_
            ? (budget_bytes_ > 0 ? std::max<size_t>(16384, budget_bytes_ / 4)
                                 : (2ULL << 20))
            : budget_bytes_;

    Row key;
    for (size_t r = 0; r < input.Size(); ++r) {
      const Row row = input.RowAt(r);
      RETURN_IF_FAIL(ExtractCanonicalGroupKeys(row, &key));
      const size_t old_groups = agg_table_->size();
      ASSIGN_OR_RETURN(size_t, gid, agg_table_->FindOrCreate(key));
      const bool inserted = (agg_table_->size() > old_groups);
      bool grant_exhausted = false;
      if (inserted) {
        const size_t group_cost =
            EstimateRowMemoryBytes(key) +
            (aggregates_.size() *
             (sizeof(relational_detail::AggregateAccumulator) + 16)) +
            64;
        mem_bytes_ += group_cost;
        if (!spilled_) {
          ProcessMemoryBudget::Global().RecordOperatorPeakBytes(mem_bytes_);
        }
        if (grant_cache_.has_value()) {
          if (!grant_cache_->Reserve(group_cost)) {
            grant_exhausted = true;
          }
        }
      }
      RETURN_IF_FAIL(agg_table_->Accumulate(gid, row));

      const bool cap_exceeded = (pre_agg_cap > 0 && mem_bytes_ > pre_agg_cap &&
                                 agg_table_->size() >= 2);
      const bool grant_flush =
          grant_exhausted &&
          (spilled_ ? !agg_table_->empty() : agg_table_->size() >= 2);
      if (cap_exceeded || grant_flush) {
        if (!spilled_) {
          RecursivePartitioner::Options opts;
          opts.spill_store = &ctx->spill_store;
          opts.memory_budget_bytes = budget_bytes_;
          opts.grant = &ctx->grant;
          const size_t gcnt = group_keys_.size();
          partitioner_ = std::make_unique<RecursivePartitioner>(
              opts, [gcnt](const Row& packed) -> uint64_t {
                uint64_t h = 0x9e3779b97f4a7c15ULL;
                for (size_t i = 0; i < gcnt && i < packed.Count(); ++i) {
                  h = (h << 7) | (h >> 57);
                  h ^= std::hash<Value>{}(packed[i]);
                  h *= 0x100000001b3ULL;
                }
                return h;
              });
          spilled_ = true;
        }
        RETURN_IF_FAIL(FlushPartialTableToPartitioner());
      }
    }
    return Status::kSuccess;
  }

  // Non-combinable grouped aggregates (e.g. COUNT(DISTINCT), STRING_AGG,
  // ARRAY_AGG, STDDEV): buffer raw rows up to budget, spilling into
  // RecursivePartitioner once budget is exceeded.
  if (!spilled_ && always_spill) {
    RETURN_IF_FAIL(TransitionNonCombinableToSpill(ctx));
  }
  for (size_t r = 0; r < input.Size(); ++r) {
    Row row = input.RowAt(r);
    if (spilled_) {
      RETURN_IF_FAIL(partition_key_status_);
      RETURN_IF_FAIL(partitioner_->AddRow(std::move(row)));
      continue;
    }
    const size_t rb = EstimateRowMemoryBytes(row);
    if ((budget_bytes_ > 0 && !raw_rows_.empty() &&
         mem_bytes_ + rb > budget_bytes_) ||
        (grant_cache_.has_value() && !grant_cache_->Reserve(rb))) {
      RETURN_IF_FAIL(TransitionNonCombinableToSpill(ctx));
      RETURN_IF_FAIL(partition_key_status_);
      RETURN_IF_FAIL(partitioner_->AddRow(std::move(row)));
      continue;
    }
    mem_bytes_ += rb;
    raw_rows_.push_back(std::move(row));
    ProcessMemoryBudget::Global().RecordOperatorPeakBytes(mem_bytes_);
  }
  return Status::kSuccess;
}

StatusOr<std::shared_ptr<PipelineSource>> OocGroupAggSink::Finalize(
    PipelineContext* ctx) {
  if (!spilled_) {
    if (!all_combinable_) {
      // P689: release each buffered row as its fold consumes it.  The
      // accumulators own every value they keep (DISTINCT sets, collection
      // buffers, concatenated strings are copied in), so holding all raw
      // rows until the whole fold finished double-charged the peak: raw
      // bytes (up to budget_bytes_) plus the fully grown agg_table_.
      for (Row& row : raw_rows_) {
        Row key;
        RETURN_IF_FAIL(ExtractCanonicalGroupKeys(row, &key));
        ASSIGN_OR_RETURN(size_t, gid, agg_table_->FindOrCreate(std::move(key)));
        RETURN_IF_FAIL(agg_table_->Accumulate(gid, row));
        row = Row();
      }
      raw_rows_.clear();
      std::vector<Row>().swap(raw_rows_);
    }
    if (group_keys_.empty() && agg_table_->empty()) {
      RETURN_IF_FAIL(agg_table_->FindOrCreate(Row{}).GetStatus());
    }
    std::vector<Row> out_rows;
    out_rows.reserve(agg_table_->size());
    for (size_t g = 0; g < agg_table_->size(); ++g) {
      ASSIGN_OR_RETURN(Row, finished, agg_table_->Finish(g));
      out_rows.push_back(std::move(finished));
    }
    output_groups_ = std::make_shared<std::atomic<size_t>>(out_rows.size());
    finalized_ = true;
    agg_table_.reset();
    mem_bytes_ = 0;
    if (grant_cache_.has_value()) {
      grant_cache_->ReleaseAll();
    }
    consumer_guard_.reset();
    return std::make_shared<RowVectorSource>(std::move(out_rows),
                                             label_ + "Output");
  }

  if (all_combinable_) {
    // Flush the last partial table. An empty table is a no-op; groups flushed
    // by earlier Consume calls are already in the partitioner, so an empty
    // LeafSet here only happens for genuinely empty grouped input (which
    // correctly yields zero groups).
    RETURN_IF_FAIL(FlushPartialTableToPartitioner());
  }
  PartitionLeafSet leaves;
  RETURN_IF_FAIL(partitioner_->Finalize(&leaves));
  RETURN_IF_FAIL(partition_key_status_);
  partitioner_.reset();
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
  auto leaf_guard = std::move(consumer_guard_);
  consumer_guard_.reset();
  output_groups_ = std::make_shared<std::atomic<size_t>>(0);
  finalized_ = true;

  return std::make_shared<GroupAggLeafSource>(
      &ctx->spill_store, std::move(leaves), input_schema_, group_keys_,
      group_key_slots_, aggregate_owners_, aggregates_, all_combinable_,
      has_collection_agg_, single_distinct_child_slot_, skew_strip_aggregate_,
      budget_bytes_, &ctx->grant, std::move(leaf_guard), label_ + "Output",
      output_groups_);
}

StatusOr<std::shared_ptr<PipelineSource>>
OocGroupAggSink::TryConsumeExactRowCount(PipelineContext* /*ctx*/,
                                         uint64_t row_count) {
  if (finalized_ || !group_keys_.empty() || aggregates_.empty() ||
      row_count > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    return std::shared_ptr<PipelineSource>{};
  }
  for (const AggregateExpression* agg : aggregates_) {
    if (agg == nullptr || agg->GetType() != AggregationType::kCount ||
        agg->Distinct() || agg->WhereFilter() || agg->HasWhereFilter() ||
        agg->NeedsGroupContext()) {
      return std::shared_ptr<PipelineSource>{};
    }
    if (agg->Child() != nullptr &&
        (agg->Child()->Type() != TypeTag::kColumnValue ||
         agg->Child()->AsColumnValue().GetColumnName().name != "*")) {
      return std::shared_ptr<PipelineSource>{};
    }
  }
  std::vector<Value> row_vals(aggregates_.size(),
                              Value(static_cast<int64_t>(row_count)));
  std::vector<Row> out_rows;
  out_rows.emplace_back(std::move(row_vals));
  total_input_rows_ = row_count;
  output_groups_ = std::make_shared<std::atomic<size_t>>(1);
  finalized_ = true;
  agg_table_.reset();
  raw_rows_.clear();
  std::vector<Row>().swap(raw_rows_);
  mem_bytes_ = 0;
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
  consumer_guard_.reset();
  return std::make_shared<RowVectorSource>(std::move(out_rows),
                                           label_ + "Output");
}

void OocGroupAggSink::Explain(std::ostream& out, int indent) const {
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  // After a spill the real path is GroupAggLeafSource over sorted partition
  // streams (merging partial states when all_combinable_, re-folding raw rows
  // otherwise), not the in-memory hash table.
  // A spill switches to the sorted leaf path even with zero group keys: the
  // keyless in-memory route is only "scalar" while it stays resident.
  const char* strategy = spilled_
                             ? (all_combinable_ ? "sort-combine" : "sort")
                             : group_keys_.empty() ? "scalar"
                                                   : "hash";
  out << pad << (explain_header_.empty() ? label_ : explain_header_)
      << " Aggregate strategy=" << strategy << "\n";
}

void OocGroupAggSink::DumpExplain(std::ostream& out, int indent) const {
  Explain(out, indent);
  if (finalized_) {
    const size_t groups = output_groups_ != nullptr
                              ? output_groups_->load(std::memory_order_relaxed)
                              : 0;
    out << std::string(static_cast<size_t>(std::max(0, indent)), ' ') << label_
        << " (groups=" << groups << ", input_rows=" << total_input_rows_
        << ")\n";
  }
}

}  // namespace tinylamb
