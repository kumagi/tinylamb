/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/analyze_scan.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/reservoir_sample.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/full_scan.hpp"
#include "table/hyper_log_log.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

AnalyzeScan::AnalyzeScan(TransactionContext& ctx, std::string_view schema_name,
                         const Table& table, size_t sample_size,
                         size_t bucket_count, Database* db)
    : ctx_(&ctx),
      schema_name_(schema_name),
      table_(&table),
      schema_(table.GetSchema()),
      sample_size_(sample_size),
      bucket_count_(bucket_count),
      db_(db),
      result_statistics_(table.GetSchema()) {}

AnalyzeScan::AnalyzeScan(TransactionContext& ctx, std::string_view schema_name,
                         Executor child, const Schema& schema,
                         size_t sample_size, size_t bucket_count, Database* db)
    : ctx_(&ctx),
      schema_name_(schema_name),
      child_(std::move(child)),
      schema_(schema),
      sample_size_(sample_size),
      bucket_count_(bucket_count),
      db_(db),
      result_statistics_(schema) {}

bool AnalyzeScan::Next(Row* dst, RowPosition* rp) {
  if (executed_) {
    return false;
  }
  executed_ = true;

  std::vector<Row> rows;
  if (child_) {
    Row r;
    RowPosition pos;
    while (child_->Next(&r, &pos)) {
      rows.push_back(r);
    }
  } else if (table_) {
    FullScan scan(ctx_->txn_, *table_);
    Row r;
    RowPosition pos;
    while (scan.Next(&r, &pos)) {
      rows.push_back(r);
    }
  }

  const size_t total_rows = rows.size();
  rows_analyzed_ = total_rows;

  const size_t col_count = schema_.ColumnCount();
  std::vector<ColumnStats> columns;
  columns.reserve(col_count);

  for (size_t col_idx = 0; col_idx < col_count; ++col_idx) {
    const Column& col = schema_.GetColumn(col_idx);
    const ValueType val_type = col.Type();

    size_t null_count = 0;
    size_t non_null_count = 0;
    HyperLogLog hll(10);
    ReservoirSample<Value> reservoir(sample_size_);
    std::unordered_map<Value, size_t> value_counts;

    for (const auto& row : rows) {
      if (col_idx < row.values_.size()) {
        const Value& val = row.values_[col_idx];
        if (val.IsNull()) {
          ++null_count;
        } else {
          ++non_null_count;
          hll.Add(val);
          reservoir.Add(val);
          ++value_counts[val];
        }
      } else {
        ++null_count;
      }
    }

    // Compute Most Common Values (top 5)
    std::vector<ValueFrequency> mcvs;
    mcvs.reserve(value_counts.size());
    for (const auto& [val, count] : value_counts) {
      mcvs.push_back(ValueFrequency{.value = val, .count = count});
    }
    std::sort(mcvs.begin(), mcvs.end(),
              [](const ValueFrequency& a, const ValueFrequency& b) {
                if (a.count != b.count) return a.count > b.count;
                return a.value < b.value;
              });
    if (mcvs.size() > kMostCommonValueCount) {
      mcvs.resize(kMostCommonValueCount);
    }

    // Sample-based histogram, lowest, and highest
    std::vector<Value> sample = reservoir.GetSample();
    std::sort(sample.begin(), sample.end());

    std::vector<ValueFrequency> lowest;
    std::vector<ValueFrequency> highest;
    std::vector<HistogramBucket> histogram;

    if (!sample.empty()) {
      // Lowest values (up to 5 unique)
      for (const auto& v : sample) {
        if (lowest.empty() || lowest.back().value != v) {
          lowest.push_back(
              ValueFrequency{.value = v, .count = value_counts[v]});
          if (lowest.size() == kBoundaryValueCount) break;
        }
      }

      // Highest values (up to 5 unique)
      for (auto it = sample.rbegin(); it != sample.rend(); ++it) {
        if (highest.empty() || highest.back().value != *it) {
          highest.push_back(
              ValueFrequency{.value = *it, .count = value_counts[*it]});
          if (highest.size() == kBoundaryValueCount) break;
        }
      }

      // Build histogram buckets
      const size_t num_buckets = std::min(bucket_count_, sample.size());
      if (num_buckets > 0) {
        const double bucket_step = static_cast<double>(sample.size()) /
                                   static_cast<double>(num_buckets);
        for (size_t b = 0; b < num_buckets; ++b) {
          const size_t start_idx = static_cast<size_t>(
              std::floor(static_cast<double>(b) * bucket_step));
          const size_t end_idx = static_cast<size_t>(
              std::floor(static_cast<double>(b + 1) * bucket_step));
          if (start_idx >= end_idx || start_idx >= sample.size()) continue;
          const size_t clamped_end = std::min(end_idx, sample.size());
          const Value& lower = sample[start_idx];
          const Value& upper = sample[clamped_end - 1];

          size_t distinct_in_bucket = 0;
          for (size_t i = start_idx; i < clamped_end; ++i) {
            if (i == start_idx || sample[i] != sample[i - 1]) {
              ++distinct_in_bucket;
            }
          }
          const size_t count =
              sample.size() > 0
                  ? (non_null_count * (clamped_end - start_idx)) / sample.size()
                  : (clamped_end - start_idx);
          histogram.push_back(HistogramBucket{
              .lower = lower,
              .upper = upper,
              .count = count,
              .distinct = distinct_in_bucket,
          });
        }
      }
    }

    ColumnStats cs(val_type);
    cs.non_null_count_ = non_null_count;
    cs.null_count_ = null_count;
    cs.distinct_count_ =
        non_null_count > 0
            ? std::max(size_t{1}, static_cast<size_t>(hll.Estimate()))
            : 0;
    cs.histogram_ = std::move(histogram);
    cs.most_common_values_ = std::move(mcvs);
    cs.lowest_values_ = std::move(lowest);
    cs.highest_values_ = std::move(highest);

    columns.push_back(std::move(cs));
  }

  result_statistics_.Assign(total_rows, std::move(columns));

  if (db_) {
    db_->UpdateStatistics(*ctx_, schema_name_, result_statistics_);
  }

  if (dst) {
    *dst = Row({Value(static_cast<int64_t>(total_rows))});
  }
  if (rp) {
    *rp = RowPosition();
  }
  return true;
}

void AnalyzeScan::Dump(std::ostream& o, int indent) const {
  o << std::string(indent, ' ') << "AnalyzeScan (table: " << schema_name_
    << ", sample_size: " << sample_size_ << ", buckets: " << bucket_count_
    << ")\n";
}

}  // namespace tinylamb
