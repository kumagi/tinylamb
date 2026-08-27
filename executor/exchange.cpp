/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/exchange.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/query_memory.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

uint64_t HashBytesKey(std::string_view bytes) {
  uint64_t hash = 14695981039346656037ULL;
  for (unsigned char c : bytes) {
    hash ^= static_cast<uint64_t>(c);
    hash *= 1099511628211ULL;
  }
  return hash;
}

class PartitionScanExecutor : public ExecutorBase {
 public:
  PartitionScanExecutor(std::shared_ptr<ExchangeExecutor> exchange,
                        size_t partition_idx)
      : exchange_(std::move(exchange)), partition_idx_(partition_idx) {}

  bool Next(Row* dst, RowPosition* rp) override {
    assert(dst != nullptr);
    if (!exchange_->IsMaterialized()) {
      exchange_->MaterializePipeline();
    }
    const auto& rows = exchange_->GetPartitionRows(partition_idx_);
    if (offset_ >= rows.size()) {
      return false;
    }
    *dst = rows[offset_].first;
    if (rp != nullptr) {
      *rp = rows[offset_].second;
    }
    ++offset_;
    return true;
  }

  void Dump(std::ostream& o, int indent) const override {
    o << "PartitionScan(partition=" << partition_idx_ << ")";
  }

 private:
  std::shared_ptr<ExchangeExecutor> exchange_;
  size_t partition_idx_{0};
  size_t offset_{0};
};

}  // namespace

ExchangeExecutor::ExchangeExecutor(
    Executor child, ExchangeType type, size_t partition_count,
    std::vector<slot_t> key_cols, std::vector<Value> range_bounds)
    : child_(std::move(child)),
      type_(type),
      partition_count_(std::max<size_t>(1, partition_count)),
      key_cols_(std::move(key_cols)),
      range_bounds_(std::move(range_bounds)) {
  partitions_.resize(partition_count_);
}

size_t ExchangeExecutor::RouteRow(const Row& row) const {
  switch (type_) {
    case ExchangeType::kHash: {
      std::string key;
      for (slot_t col : key_cols_) {
        if (col < row.Size()) {
          key += row[col].EncodeMemcomparableFormat();
        }
      }
      return HashBytesKey(key) % partition_count_;
    }
    case ExchangeType::kRange: {
      if (range_bounds_.empty() || key_cols_.empty()) {
        return 0;
      }
      const Value& key = row[key_cols_[0]];
      for (size_t i = 0; i < range_bounds_.size(); ++i) {
        if (key < range_bounds_[i]) {
          return i;
        }
      }
      return range_bounds_.size();
    }
    case ExchangeType::kGather:
    case ExchangeType::kBroadcast:
    default:
      return 0;
  }
}

void ExchangeExecutor::DistributeRows() {
  for (auto& p : partitions_) {
    p.clear();
  }

  Row row;
  RowPosition rp;
  size_t total_bytes = 0;

  while (child_ && child_->Next(&row, &rp)) {
    total_bytes += EstimateRowBytes(row) + sizeof(RowPosition);
    if (type_ == ExchangeType::kBroadcast) {
      for (size_t p = 0; p < partition_count_; ++p) {
        partitions_[p].emplace_back(row, rp);
      }
    } else {
      const size_t target = RouteRow(row);
      assert(target < partitions_.size());
      partitions_[target].emplace_back(std::move(row), rp);
    }
  }

  charge_.Add(total_bytes);
}

void ExchangeExecutor::EnsureMaterialized() {
  if (materialized_) {
    return;
  }
  materialized_ = true;
  DistributeRows();
  current_gather_part_ = 0;
  current_gather_offset_ = 0;
}

void ExchangeExecutor::MaterializePipeline() {
  EnsureMaterialized();
}

size_t ExchangeExecutor::MaterializedRowCount() const {
  size_t total = 0;
  for (const auto& p : partitions_) {
    total += p.size();
  }
  return total;
}

const std::vector<std::pair<Row, RowPosition>>&
ExchangeExecutor::GetPartitionRows(size_t partition_idx) const {
  assert(partition_idx < partitions_.size());
  return partitions_[partition_idx];
}

bool ExchangeExecutor::Next(Row* dst, RowPosition* rp) {
  assert(dst != nullptr);
  EnsureMaterialized();

  while (current_gather_part_ < partitions_.size()) {
    const auto& part = partitions_[current_gather_part_];
    if (current_gather_offset_ < part.size()) {
      *dst = part[current_gather_offset_].first;
      if (rp != nullptr) {
        *rp = part[current_gather_offset_].second;
      }
      ++current_gather_offset_;
      return true;
    }
    ++current_gather_part_;
    current_gather_offset_ = 0;
  }
  return false;
}

size_t ExchangeExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  EnsureMaterialized();

  size_t count = 0;
  Row row;
  RowPosition rp;
  while (count < max_rows && Next(&row, &rp)) {
    destination->Append(row, rp);
    ++count;
  }
  return count;
}

void ExchangeExecutor::Dump(std::ostream& o, int /*indent*/) const {
  o << "ExchangeExecutor(type=" << static_cast<int>(type_)
    << ", partitions=" << partition_count_ << ")";
}

void ExchangeExecutor::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
}

Executor ExchangeExecutor::GetPartitionExecutor(size_t partition_idx) {
  assert(partition_idx < partition_count_);
  return std::make_shared<PartitionScanExecutor>(
      std::shared_ptr<ExchangeExecutor>(this, [](ExchangeExecutor*){}),
      partition_idx);
}

}  // namespace tinylamb
