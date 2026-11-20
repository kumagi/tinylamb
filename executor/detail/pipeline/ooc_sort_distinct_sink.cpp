/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "common/memory_budget.hpp"
#include "executor/detail/ooc/chunk_codec.hpp"
#include "executor/detail/pipeline/ooc_sinks.hpp"
#include "executor/detail/pipeline/pipeline_ops.hpp"
#include "type/row_block_codec.hpp"

namespace tinylamb {

StatusOr<Value> EvalRowExpression(const Expression& expr, const Schema& schema,
                                  const Row& row) {
  if (!expr) {
    return Value();
  }
  if (expr->Type() == TypeTag::kColumnValue) {
    const ColumnName& col = expr->AsColumnValue().GetColumnName();
    const int offset = schema.Offset(col);
    if (offset >= 0 && static_cast<size_t>(offset) < row.Count()) {
      return row[static_cast<size_t>(offset)];
    }
    return StatusError(
        StatusCode::kNotExists,
        "column not found in EvalRowExpression: " + col.ToString());
  }
  return expr->TryEvaluate(row, schema);
}

namespace {

class ExternalSorterOutputSource final : public PipelineSource {
 public:
  ExternalSorterOutputSource(
      std::unique_ptr<ExternalSorter> sorter, size_t payload_cols,
      size_t offset, std::optional<size_t> limit,
      std::optional<QueryMemoryGrant::ActiveConsumerGuard> guard = std::nullopt,
      std::string label = "SortOutput")
      : sorter_(std::move(sorter)),
        payload_cols_(payload_cols),
        offset_(offset),
        limit_(limit),
        guard_(std::move(guard)),
        label_(std::move(label)) {
    // Release writer consumer guard before streaming output so downstream joins
    // / pipelines gain full memory share if any.
    guard_.reset();
  }

  StatusOr<bool> NextBatch(PipelineContext* /*ctx*/, DataChunk* out,
                           size_t max_rows) override {
    out->Reset();
    if (!sorter_ || (limit_.has_value() && emitted_ >= *limit_)) {
      return false;
    }
    Row row;
    while (out->Size() < max_rows) {
      if (limit_.has_value() && emitted_ >= *limit_) {
        break;
      }
      ASSIGN_OR_RETURN(bool, has_row, sorter_->NextRow(&row));
      if (!has_row) {
        sorter_.reset();
        break;
      }
      if (skipped_ < offset_) {
        ++skipped_;
        continue;
      }
      if (row.Count() > payload_cols_) {
        row.values_.resize(payload_cols_);
      }
      out->Append(std::move(row));
      ++emitted_;
    }
    if (out->Empty()) {
      sorter_.reset();
      return false;
    }
    return true;
  }

  [[nodiscard]] std::string Describe() const override { return label_; }

 private:
  std::unique_ptr<ExternalSorter> sorter_;
  size_t payload_cols_{0};
  size_t offset_{0};
  std::optional<size_t> limit_;
  std::optional<QueryMemoryGrant::ActiveConsumerGuard> guard_;
  size_t skipped_{0};
  size_t emitted_{0};
  std::string label_;
};

uint64_t HashCanonicalRow(const Row& canonical_row) {
  return std::hash<Row>{}(canonical_row);
}

class DistinctOutputSource final : public PipelineSource {
 public:
  DistinctOutputSource(SpillStore* store, PartitionLeafSet leaves,
                       Schema input_schema, std::vector<slot_t> key_cols,
                       std::vector<Expression> distinct_on_exprs,
                       bool has_seq_suffix, size_t budget_bytes,
                       std::string label, QueryMemoryGrant* grant = nullptr)
      : store_(store),
        leaves_(std::move(leaves)),
        input_schema_(std::move(input_schema)),
        key_cols_(std::move(key_cols)),
        distinct_on_exprs_(std::move(distinct_on_exprs)),
        has_seq_suffix_(has_seq_suffix),
        payload_width_(input_schema_.ColumnCount()),
        budget_bytes_(budget_bytes),
        label_(std::move(label)),
        grant_(grant) {
    // This source streams spilled leaves through ExternalSorter, which charges
    // the grant through `grant_`. Keep an active-consumer share for the whole
    // source lifetime so downstream stages cannot over-admit against it.
    if (grant_ != nullptr) {
      guard_.emplace(grant_);
    }
  }

  Status Open(PipelineContext* ctx) override {
    if (!has_seq_suffix_) {
      return Status::kSuccess;
    }
    // Preserve input order across partitions by deduplicating all leaves into a
    // single ExternalSorter sorted by the original sequence tag.
    ExternalSorter::Options opts;
    opts.spill_store = store_;
    opts.memory_budget_bytes = budget_bytes_;
    opts.grant = ctx != nullptr ? &ctx->grant : nullptr;
    opts.keys = {OocSortKey{.column_index = static_cast<slot_t>(payload_width_),
                            .ascending = true,
                            .nulls_first = true,
                            .collation = Collation::kNone}};
    auto order_sorter = std::make_unique<ExternalSorter>(std::move(opts));
    for (size_t li = 0; li < leaves_.leaves.size(); ++li) {
      const PartitionLeaf& leaf = leaves_.leaves[li];
      if (leaf.skew_fallback) {
        ASSIGN_OR_RETURN(std::unique_ptr<ExternalSorter>, sk,
                         BuildSkewSorter(leaf));
        Row row;
        Row prev_key;
        bool have_prev = false;
        while (true) {
          ASSIGN_OR_RETURN(bool, has, sk->NextRow(&row));
          if (!has) {
            break;
          }
          ASSIGN_OR_RETURN(Row, key, ExtractKey(row));
          if (!have_prev ||
              !relational_detail::GroupKeyEqual{}(prev_key, key)) {
            prev_key = std::move(key);
            have_prev = true;
            row.values_.resize(payload_width_ + 1);
            RETURN_IF_FAIL(order_sorter->AddRow(std::move(row)));
          }
        }
      } else {
        relational_detail::GroupKeyMap seen;
        RETURN_IF_FAIL(RecursivePartitioner::ReadLeafRows(
            store_, leaf, [&](Row&& r) -> Status {
              ASSIGN_OR_RETURN(Row, key, ExtractKey(r));
              if (seen.emplace(std::move(key), 0).second) {
                r.values_.resize(payload_width_ + 1);
                return order_sorter->AddRow(std::move(r));
              }
              return Status::kSuccess;
            }));
      }
      leaves_.DropLeaf(li);
    }
    RETURN_IF_FAIL(order_sorter->Finish());
    restored_order_sorter_ = std::move(order_sorter);
    return Status::kSuccess;
  }

  StatusOr<bool> NextBatch(PipelineContext* /*ctx*/, DataChunk* out,
                           size_t max_rows) override {
    out->Reset();
    if (restored_order_sorter_ != nullptr) {
      Row row;
      while (out->Size() < max_rows) {
        ASSIGN_OR_RETURN(bool, has, restored_order_sorter_->NextRow(&row));
        if (!has) {
          restored_order_sorter_.reset();
          break;
        }
        if (row.Count() > payload_width_) {
          row.values_.resize(payload_width_);
        }
        out->Append(std::move(row));
      }
      if (out->Empty()) {
        guard_.reset();
      }
      return !out->Empty();
    }
    while (out->Empty()) {
      if (emit_cursor_ < active_rows_.size()) {
        const size_t end =
            std::min(active_rows_.size(), emit_cursor_ + max_rows);
        out->Reserve(end - emit_cursor_);
        for (size_t i = emit_cursor_; i < end; ++i) {
          out->Append(std::move(active_rows_[i]));
        }
        emit_cursor_ = end;
        if (emit_cursor_ >= active_rows_.size()) {
          active_rows_.clear();
          emit_cursor_ = 0;
        }
        return true;
      }
      if (skew_sorter_ != nullptr) {
        Row row;
        while (out->Size() < max_rows) {
          ASSIGN_OR_RETURN(bool, has, skew_sorter_->NextRow(&row));
          if (!has) {
            skew_sorter_.reset();
            leaves_.DropLeaf(leaf_idx_ - 1);
            break;
          }
          ASSIGN_OR_RETURN(Row, key, ExtractKey(row));
          if (!have_prev_key_ ||
              !relational_detail::GroupKeyEqual{}(prev_key_, key)) {
            prev_key_ = std::move(key);
            have_prev_key_ = true;
            StripTrailingKeyCols(&row);
            out->Append(std::move(row));
          }
        }
        if (!out->Empty()) {
          return true;
        }
        continue;
      }
      if (leaf_idx_ >= leaves_.leaves.size()) {
        guard_.reset();
        return false;
      }
      const PartitionLeaf& leaf = leaves_.leaves[leaf_idx_++];
      // Dedup EVERY spilled leaf through ExternalSorter, which streams under
      // this source's query grant and removes consecutive equal keys during
      // emission, so no leaf-sized resident containers are needed here.
      ASSIGN_OR_RETURN(std::unique_ptr<ExternalSorter>, sk,
                       BuildSkewSorter(leaf));
      skew_sorter_ = std::move(sk);
      have_prev_key_ = false;
    }
    guard_.reset();
    return false;
  }

  [[nodiscard]] std::string Describe() const override { return label_; }

 private:
  StatusOr<std::unique_ptr<ExternalSorter>> BuildSkewSorter(
      const PartitionLeaf& leaf) {
    if (!distinct_on_exprs_.empty()) {
      const size_t key_base = payload_width_ + (has_seq_suffix_ ? 1U : 0U);
      std::vector<OocSortKey> keys;
      keys.reserve(distinct_on_exprs_.size() + (has_seq_suffix_ ? 1U : 0U));
      for (size_t i = 0; i < distinct_on_exprs_.size(); ++i) {
        keys.push_back(
            OocSortKey{.column_index = static_cast<slot_t>(key_base + i)});
      }
      if (has_seq_suffix_) {
        keys.push_back(
            OocSortKey{.column_index = static_cast<slot_t>(payload_width_)});
      }
      ExternalSorter::Options opts;
      opts.spill_store = store_;
      opts.memory_budget_bytes = budget_bytes_;
      opts.grant = grant_;
      opts.keys = std::move(keys);
      auto sorter = std::make_unique<ExternalSorter>(std::move(opts));
      RETURN_IF_FAIL(RecursivePartitioner::ReadLeafRows(
          store_, leaf, [&](Row&& r) -> Status {
            ASSIGN_OR_RETURN(Row, k, ExtractKey(r));
            if (has_seq_suffix_) {
              r.values_.resize(payload_width_ + 1);
            } else {
              r.values_.resize(payload_width_);
            }
            for (Value& kv : k.values_) {
              r.values_.push_back(std::move(kv));
            }
            return sorter->AddRow(std::move(r));
          }));
      RETURN_IF_FAIL(sorter->Finish());
      return sorter;
    }

    std::vector<OocSortKey> sort_keys;
    if (!key_cols_.empty()) {
      for (slot_t c : key_cols_) {
        sort_keys.push_back(OocSortKey{.column_index = c});
      }
    }
    ExternalSorter::Options opts;
    opts.spill_store = store_;
    opts.memory_budget_bytes = budget_bytes_;
    opts.grant = grant_;
    opts.keys = sort_keys;
    auto sorter = std::make_unique<ExternalSorter>(std::move(opts));
    bool rebuilt_keys = sort_keys.empty();
    RETURN_IF_FAIL(RecursivePartitioner::ReadLeafRows(
        store_, leaf, [&](Row&& r) -> Status {
          if (rebuilt_keys) {
            rebuilt_keys = false;
            const size_t arity = has_seq_suffix_ ? payload_width_ : r.Count();
            std::vector<OocSortKey> keys;
            keys.reserve(arity + (has_seq_suffix_ ? 1U : 0U));
            for (size_t c = 0; c < arity; ++c) {
              keys.push_back(
                  OocSortKey{.column_index = static_cast<slot_t>(c)});
            }
            if (has_seq_suffix_) {
              keys.push_back(OocSortKey{
                  .column_index = static_cast<slot_t>(payload_width_)});
            }
            ExternalSorter::Options o2;
            o2.spill_store = store_;
            o2.memory_budget_bytes = budget_bytes_;
            o2.grant = grant_;
            o2.keys = std::move(keys);
            sorter = std::make_unique<ExternalSorter>(std::move(o2));
          }
          return sorter->AddRow(std::move(r));
        }));
    RETURN_IF_FAIL(sorter->Finish());
    return sorter;
  }

  void StripTrailingKeyCols(Row* r) const {
    if ((!distinct_on_exprs_.empty() || has_seq_suffix_) &&
        r->Count() > payload_width_) {
      r->values_.resize(payload_width_);
    }
  }

  [[nodiscard]] StatusOr<Row> ExtractKey(const Row& r) const {
    std::vector<Value> vals;
    if (!distinct_on_exprs_.empty()) {
      vals.reserve(distinct_on_exprs_.size());
      for (const Expression& expr : distinct_on_exprs_) {
        ASSIGN_OR_RETURN(Value, ev, EvalRowExpression(expr, input_schema_, r));
        vals.push_back(relational_detail::CanonicalDistinctValue(ev));
      }
      return Row(std::move(vals));
    }
    if (key_cols_.empty()) {
      const size_t limit =
          has_seq_suffix_ ? std::min(r.Count(), payload_width_) : r.Count();
      vals.reserve(limit);
      for (size_t i = 0; i < limit; ++i) {
        vals.push_back(relational_detail::CanonicalDistinctValue(r[i]));
      }
    } else {
      vals.reserve(key_cols_.size());
      for (slot_t c : key_cols_) {
        vals.push_back(c < r.Count()
                           ? relational_detail::CanonicalDistinctValue(r[c])
                           : Value());
      }
    }
    return Row(std::move(vals));
  }

  SpillStore* store_{nullptr};
  PartitionLeafSet leaves_;
  Schema input_schema_;
  std::vector<slot_t> key_cols_;
  std::vector<Expression> distinct_on_exprs_;
  bool has_seq_suffix_{false};
  size_t payload_width_{0};
  size_t budget_bytes_{0};
  std::string label_;
  QueryMemoryGrant* grant_{nullptr};
  std::optional<QueryMemoryGrant::ActiveConsumerGuard> guard_;

  size_t leaf_idx_{0};
  std::vector<Row> active_rows_;
  size_t emit_cursor_{0};
  std::unique_ptr<ExternalSorter> skew_sorter_;
  std::unique_ptr<ExternalSorter> restored_order_sorter_;
  Row prev_key_;
  bool have_prev_key_{false};
};

}  // namespace

// --- OocSortSink ---

OocSortSink::OocSortSink(std::vector<KeySpec> keys, Schema input_schema,
                         std::optional<size_t> limit, size_t offset,
                         std::string label)
    : keys_(std::move(keys)),
      input_schema_(std::move(input_schema)),
      limit_(limit),
      offset_(offset),
      label_(std::move(label)),
      payload_cols_(input_schema_.ColumnCount()) {}

Status OocSortSink::Init(PipelineContext* ctx, size_t /*worker_count*/) {
  consumer_guard_.emplace(&ctx->grant);
  direct_slots_.assign(keys_.size(), std::nullopt);
  computed_key_indices_.clear();
  seq_ = 0;
  std::vector<OocSortKey> sorter_keys;
  sorter_keys.reserve(keys_.size() + (limit_.has_value() ? 1U : 0U));

  size_t extra_idx = 0;
  for (size_t i = 0; i < keys_.size(); ++i) {
    slot_t sort_slot = 0;
    const auto column_slot = keys_[i].column_slot;
    if (column_slot.has_value() &&
        *column_slot < input_schema_.ColumnCount()) {
      const auto slot = static_cast<slot_t>(*column_slot);
      direct_slots_[i] = slot;
      sort_slot = slot;
    } else if (keys_[i].expression &&
               keys_[i].expression->Type() == TypeTag::kColumnValue) {
      const int offset = input_schema_.Offset(
          keys_[i].expression->AsColumnValue().GetColumnName());
      if (offset >= 0) {
        direct_slots_[i] = static_cast<slot_t>(offset);
        sort_slot = static_cast<slot_t>(offset);
      } else {
        computed_key_indices_.push_back(i);
        sort_slot = static_cast<slot_t>(payload_cols_ + extra_idx++);
      }
    } else {
      computed_key_indices_.push_back(i);
      sort_slot = static_cast<slot_t>(payload_cols_ + extra_idx++);
    }
    sorter_keys.push_back(OocSortKey{
        .column_index = sort_slot,
        .ascending = keys_[i].ascending,
        .nulls_first = keys_[i].nulls_first,
        .collation = keys_[i].collation,
    });
  }
  if (limit_.has_value()) {
    // Deterministic FIFO tie-breaker for bounded Top-N heaps / spilled runs.
    sorter_keys.push_back(OocSortKey{
        .column_index = static_cast<slot_t>(payload_cols_ + extra_idx++),
        .ascending = true,
        .nulls_first = false,
        .collation = Collation::kNone,
    });
  }

  std::optional<size_t> effective_limit = limit_;
  if (effective_limit.has_value()) {
    const size_t lim = *effective_limit;
    if (offset_ > std::numeric_limits<size_t>::max() - lim) {
      effective_limit = std::numeric_limits<size_t>::max();
    } else {
      effective_limit = lim + offset_;
    }
  }

  ExternalSorter::Options opts;
  opts.spill_store = &ctx->spill_store;
  opts.memory_budget_bytes = ctx->OperatorBudgetBytes();
  opts.grant = &ctx->grant;
  opts.keys = std::move(sorter_keys);
  opts.limit = effective_limit;
  sorter_ = std::make_unique<ExternalSorter>(std::move(opts));
  return Status::kSuccess;
}

Status OocSortSink::Consume(PipelineContext* /*ctx*/, size_t /*worker_id*/,
                            const DataChunk& input) {
  if (input.Empty()) {
    return Status::kSuccess;
  }
  input_rows_ += input.Size();
  if (payload_cols_ == 0) {
    payload_cols_ = input.ColumnCount();
  }
  if (computed_key_indices_.empty() && !limit_.has_value()) {
    return sorter_->AddChunk(input);
  }
  const bool can_fast_skip_topk =
      limit_.has_value() && computed_key_indices_.empty();
  const size_t sql_key_count = keys_.size();
  for (size_t r = 0; r < input.Size(); ++r) {
    if (can_fast_skip_topk &&
        sorter_->ShouldSkipTopKChunkRow(input, r, sql_key_count)) {
      ++seq_;
      sorter_->RecordSkippedRow();
      continue;
    }
    Row row = input.RowAt(r);
    // RowAt already reserves exactly Count(); reserve once more only when
    // trailing computed-key / seq columns will actually be appended.
    if (!computed_key_indices_.empty() || limit_.has_value()) {
      row.values_.reserve(row.Count() + computed_key_indices_.size() +
                          (limit_.has_value() ? 1U : 0U));
    }
    for (const size_t kidx : computed_key_indices_) {
      ASSIGN_OR_RETURN(
          Value, kv,
          EvalRowExpression(keys_[kidx].expression, input_schema_, row));
      row.values_.push_back(std::move(kv));
    }
    if (limit_.has_value()) {
      row.values_.emplace_back(seq_++);
    }
    RETURN_IF_FAIL(sorter_->AddRow(std::move(row)));
  }
  return Status::kSuccess;
}

StatusOr<std::shared_ptr<PipelineSource>> OocSortSink::Finalize(
    PipelineContext* /*ctx*/) {
  RETURN_IF_FAIL(sorter_->Finish());
  finalized_ = true;
  const size_t after_offset =
      input_rows_ > offset_ ? (input_rows_ - offset_) : 0;
  output_rows_ =
      limit_.has_value() ? std::min(after_offset, *limit_) : after_offset;
  std::shared_ptr<PipelineSource> src =
      std::make_shared<ExternalSorterOutputSource>(
          std::move(sorter_), payload_cols_, offset_, limit_,
          std::move(consumer_guard_), label_ + "Output");
  consumer_guard_.reset();
  return src;
}

std::string OocSortSink::Describe() const {
  return limit_.has_value()
             ? (label_ + "(limit=" + std::to_string(*limit_) + ")")
             : label_;
}

void OocSortSink::Explain(std::ostream& out, int indent) const {
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  if (limit_.has_value()) {
    out << pad << "TopN (limit=" << *limit_ << ", offset=" << offset_ << ")\n";
  } else {
    out << pad << "ParallelSort (" << label_ << ")\n";
  }
}

void OocSortSink::DumpExplain(std::ostream& out, int indent) const {
  Explain(out, indent);
  if (finalized_ && limit_.has_value()) {
    const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
    out << pad << "TopN heap_capacity=" << *limit_
        << " input_rows=" << input_rows_ << " output_rows=" << output_rows_
        << "\n";
  }
}

// --- OocDistinctSink ---

OocDistinctSink::OocDistinctSink(Schema input_schema,
                                 std::vector<slot_t> key_cols,
                                 std::string label,
                                 std::vector<Expression> distinct_on_exprs,
                                 bool preserve_input_order)
    : input_schema_(std::move(input_schema)),
      key_cols_(std::move(key_cols)),
      label_(std::move(label)),
      distinct_on_exprs_(std::move(distinct_on_exprs)),
      preserve_input_order_(preserve_input_order) {}

OocDistinctSink::~OocDistinctSink() {
  partitioner_.reset();
  unique_rows_.clear();
  index_map_.clear();
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
}

StatusOr<Row> OocDistinctSink::ExtractKey(const Row& row) const {
  std::vector<Value> vals;
  if (!distinct_on_exprs_.empty()) {
    vals.reserve(distinct_on_exprs_.size());
    for (const Expression& expr : distinct_on_exprs_) {
      ASSIGN_OR_RETURN(Value, ev, EvalRowExpression(expr, input_schema_, row));
      vals.push_back(relational_detail::CanonicalDistinctValue(ev));
    }
    return Row(std::move(vals));
  }
  if (key_cols_.empty()) {
    const size_t limit =
        preserve_input_order_
            ? std::min<size_t>(row.Count(), input_schema_.ColumnCount())
            : row.Count();
    vals.reserve(limit);
    for (size_t i = 0; i < limit; ++i) {
      vals.push_back(relational_detail::CanonicalDistinctValue(row[i]));
    }
  } else {
    vals.reserve(key_cols_.size());
    for (slot_t c : key_cols_) {
      vals.push_back(c < row.Count()
                         ? relational_detail::CanonicalDistinctValue(row[c])
                         : Value());
    }
  }
  return Row(std::move(vals));
}

Row OocDistinctSink::BuildTaggedRow(Row&& row, const Row& /*key*/,
                                    uint64_t seq) const {
  if (preserve_input_order_) {
    row.values_.resize(input_schema_.ColumnCount());
    row.values_.emplace_back(static_cast<int64_t>(seq));
  }
  return std::move(row);
}

Status OocDistinctSink::Init(PipelineContext* ctx, size_t /*worker_count*/) {
  consumer_guard_.emplace(&ctx->grant);
  budget_bytes_ = ctx->OperatorBudgetBytes();
  next_seq_ = 0;
  spilled_ = false;
  unique_rows_.clear();
  index_map_.clear();
  mem_bytes_ = 0;
  partitioner_.reset();
  const size_t refill =
      budget_bytes_ > 0
          ? std::min<size_t>(
                QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes,
                std::max<size_t>(65536, budget_bytes_ / 4))
          : QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes;
  grant_cache_.emplace(&ctx->grant, refill);
  return Status::kSuccess;
}

Status OocDistinctSink::TransitionToSpill(PipelineContext* ctx) {
  if (spilled_) {
    return Status::kSuccess;
  }
  RecursivePartitioner::Options opts;
  opts.spill_store = &ctx->spill_store;
  opts.memory_budget_bytes = budget_bytes_;
  opts.grant = &ctx->grant;
  partitioner_ = std::make_unique<RecursivePartitioner>(
      opts, [this](const Row& r) -> uint64_t {
        StatusOr<Row> k = ExtractKey(r);
        if (!k.HasValue()) {
          return 0;
        }
        return HashCanonicalRow(k.Value());
      });

  for (size_t i = 0; i < unique_rows_.size(); ++i) {
    Row& r = unique_rows_[i];
    if (preserve_input_order_) {
      r.values_.resize(input_schema_.ColumnCount());
      r.values_.emplace_back(static_cast<int64_t>(i));
    }
    RETURN_IF_FAIL(partitioner_->AddRow(std::move(r)));
  }
  next_seq_ = std::max<uint64_t>(next_seq_, unique_rows_.size());
  unique_rows_.clear();
  unique_rows_.shrink_to_fit();
  index_map_.clear();
  mem_bytes_ = 0;
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
  spilled_ = true;
  return Status::kSuccess;
}

Status OocDistinctSink::Consume(PipelineContext* ctx, size_t /*worker_id*/,
                                const DataChunk& input) {
  const bool always_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
  if (!spilled_ && always_spill) {
    RETURN_IF_FAIL(TransitionToSpill(ctx));
  }
  for (size_t r = 0; r < input.Size(); ++r) {
    Row row = input.RowAt(r);
    if (spilled_) {
      if (preserve_input_order_) {
        row.values_.resize(input_schema_.ColumnCount());
        row.values_.emplace_back(static_cast<int64_t>(next_seq_++));
      }
      RETURN_IF_FAIL(partitioner_->AddRow(std::move(row)));
      continue;
    }
    ASSIGN_OR_RETURN(Row, key, ExtractKey(row));
    if (index_map_.contains(key)) {
      continue;
    }
    const size_t row_bytes = EstimateRowMemoryBytes(row);
    // ADR-0004 rule 9 applies to the first row too, before any retention.
    if (budget_bytes_ > 0 && row_bytes > budget_bytes_ / 4) {
      return StatusError(StatusCode::kTooBigData,
                         "row exceeds operator budget quarter");
    }
    const size_t add_bytes = row_bytes + EstimateRowMemoryBytes(key) + 64;
    if ((budget_bytes_ > 0 && !unique_rows_.empty() &&
         mem_bytes_ + add_bytes > budget_bytes_) ||
        (grant_cache_.has_value() && !grant_cache_->Reserve(add_bytes))) {
      RETURN_IF_FAIL(TransitionToSpill(ctx));
      if (preserve_input_order_) {
        row.values_.resize(input_schema_.ColumnCount());
        row.values_.emplace_back(static_cast<int64_t>(next_seq_++));
      }
      RETURN_IF_FAIL(partitioner_->AddRow(std::move(row)));
      continue;
    }
    index_map_.emplace(std::move(key), unique_rows_.size());
    unique_rows_.push_back(std::move(row));
    ++next_seq_;
    mem_bytes_ += add_bytes;
    ProcessMemoryBudget::Global().RecordOperatorPeakBytes(mem_bytes_);
  }
  return Status::kSuccess;
}

StatusOr<std::shared_ptr<PipelineSource>> OocDistinctSink::Finalize(
    PipelineContext* ctx) {
  if (!spilled_) {
    std::vector<Row> rows = std::move(unique_rows_);
    unique_rows_.clear();
    index_map_.clear();
    mem_bytes_ = 0;
    if (grant_cache_.has_value()) {
      grant_cache_->ReleaseAll();
    }
    consumer_guard_.reset();
    return std::make_shared<RowVectorSource>(std::move(rows),
                                             label_ + "Output");
  }
  PartitionLeafSet leaves;
  RETURN_IF_FAIL(partitioner_->Finalize(&leaves));
  partitioner_.reset();
  consumer_guard_.reset();
  return std::make_shared<DistinctOutputSource>(
      &ctx->spill_store, std::move(leaves), input_schema_, key_cols_,
      distinct_on_exprs_, preserve_input_order_, budget_bytes_, label_,
      &ctx->grant);
}

void OocDistinctSink::DumpExplain(std::ostream& out, int indent) const {
  out << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
      << "HashDistinct\n";
}

}  // namespace tinylamb
