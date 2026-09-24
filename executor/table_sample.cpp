#include "executor/table_sample.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <ostream>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

StatusOr<double> NumericValue(const Value& value) {
  if (value.IsNull()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "TABLESAMPLE requires non-null size");
  }
  if (value.type == ValueType::kInt64) {
    return static_cast<double>(value.value.int_value);
  }
  if (value.type == ValueType::kDouble) {
    return value.value.double_value;
  }
  return StatusError(StatusCode::kInvalidArgument,
                     "TABLESAMPLE size must be numeric");
}

StatusOr<uint64_t> NonnegativeInteger(const Value& value,
                                      std::string_view name) {
  if (value.IsNull() || value.type != ValueType::kInt64) {
    return StatusError(StatusCode::kInvalidArgument,
                       std::string(name) + " must be a non-null integer");
  }
  if (!value.IsUnsigned() && value.value.int_value < 0) {
    return StatusError(StatusCode::kInvalidArgument,
                       std::string(name) + " must be non-negative");
  }
  return static_cast<uint64_t>(value.value.int_value);
}

}  // namespace

TableSampleExecutor::TableSampleExecutor(TransactionContext& context,
                                         Executor child, Schema child_schema,
                                         TableSampleSpec spec)
    : context_(context),
      child_(std::move(child)),
      child_schema_(std::move(child_schema)),
      spec_(std::move(spec)),
      random_(std::random_device{}()) {
  output_schema_ = child_schema_;
  if (spec_.with_weight) {
    std::vector<Column> columns;
    columns.reserve(static_cast<size_t>(child_schema_.ColumnCount()) + 1U);
    for (size_t i = 0; i < child_schema_.ColumnCount(); ++i) {
      columns.push_back(child_schema_.GetColumn(i));
    }
    std::string qualifier;
    if (child_schema_.ColumnCount() != 0) {
      qualifier = child_schema_.GetColumn(0).Name().schema;
    }
    columns.emplace_back(ColumnName(qualifier, spec_.weight_alias),
                         ValueType::kDouble);
    output_schema_ = Schema("", std::move(columns));
  }
}

size_t TableSampleExecutor::PartitionHash::operator()(const Row& row) const {
  uint64_t hash = 1469598103934665603ULL;
  for (const Value& value : row.values_) {
    hash ^=
        std::hash<Value>{}(relational_detail::CanonicalDistinctValue(value));
    hash *= 1099511628211ULL;
  }
  return static_cast<size_t>(hash);
}

bool TableSampleExecutor::PartitionEqual::operator()(const Row& left,
                                                     const Row& right) const {
  if (left.values_.size() != right.values_.size()) {
    return false;
  }
  for (size_t i = 0; i < left.values_.size(); ++i) {
    if (!relational_detail::DistinctValueEqual{}(left.values_[i],
                                                 right.values_[i])) {
      return false;
    }
  }
  return true;
}

uint64_t TableSampleExecutor::Mix(uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

double TableSampleExecutor::NextUnit() {
  return static_cast<double>(random_() >> 11) * 0x1.0p-53;
}

uint64_t TableSampleExecutor::NextPriority(const RowPosition& position,
                                           uint64_t ordinal) {
  if (!seed_.has_value()) {
    return random_();
  }
  uint64_t identity = ordinal;
  if (position.IsValid()) {
    identity = (position.page_id << 16) ^ position.slot;
  }
  return Mix(*seed_ ^ Mix(identity));
}

Status TableSampleExecutor::Initialize() {
  if (initialized_) {
    return initialization_status_;
  }
  initialization_status_ = [&]() -> Status {
    if (!spec_.size) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TABLESAMPLE requires a size expression");
    }
    if (spec_.method == TableSampleMethod::kSystem) {
      return StatusError(StatusCode::kNotImplemented,
                         "SYSTEM TABLESAMPLE is not implemented");
    }
    if (spec_.method == TableSampleMethod::kSystem) {
      return StatusError(StatusCode::kNotImplemented,
                         "SYSTEM TABLESAMPLE is not implemented");
    }
    const bool reservoir = spec_.method == TableSampleMethod::kReservoir;
    if (reservoir != (spec_.unit == TableSampleUnit::kRows)) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TABLESAMPLE method and unit are incompatible");
    }
    ASSIGN_OR_RETURN(Value, size_value,
                     (spec_.size->TryEvaluate(Row(), child_schema_)));
    if (reservoir) {
      ASSIGN_OR_RETURN(uint64_t, rows,
                       (NonnegativeInteger(size_value, "RESERVOIR size")));
      if (rows > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        rows = std::numeric_limits<size_t>::max();
      }
      rows_ = static_cast<size_t>(rows);
    } else {
      ASSIGN_OR_RETURN(double, percent, (NumericValue(size_value)));
      if (!std::isfinite(percent) || percent < 0.0 || percent > 100.0) {
        return StatusError(StatusCode::kInvalidArgument,
                           "PERCENT value must be in the range [0, 100]");
      }
      probability_ = percent / 100.0;
    }
    if (spec_.repeatable_seed) {
      ASSIGN_OR_RETURN(
          Value, seed_value,
          ((*spec_.repeatable_seed)->TryEvaluate(Row(), child_schema_)));
      ASSIGN_OR_RETURN(uint64_t, parsed,
                       (NonnegativeInteger(seed_value, "REPEATABLE")));
      seed_ = parsed;
      random_.seed(*seed_);
    }
    if (spec_.partition_by.empty()) {
      partitions_.emplace(Row{}, 0);
      partition_states_.emplace_back();
    }
    return Status::kSuccess;
  }();
  if (initialization_status_ == Status::kSuccess) {
    initialized_ = true;
  }
  return initialization_status_;
}

StatusOr<size_t> TableSampleExecutor::PartitionFor(const Row& row) {
  if (spec_.partition_by.empty()) {
    return 0;
  }
  Row key;
  key.values_.reserve(spec_.partition_by.size());
  relational_detail::Scope scope{
      .row = &row, .schema = &child_schema_, .outer = nullptr};
  for (const Expression& expression : spec_.partition_by) {
    ASSIGN_OR_RETURN(Value, value,
                     (relational_detail::TryEvaluate(expression, scope, nullptr,
                                                     context_, {})));
    key.values_.push_back(relational_detail::CanonicalDistinctValue(value));
  }
  const size_t next_id = partition_states_.size();
  auto [iterator, inserted] = partitions_.try_emplace(std::move(key), next_id);
  if (inserted) {
    partition_states_.emplace_back();
  }
  return iterator->second;
}

Status TableSampleExecutor::MaterializeReservoir() {
  if (reservoir_ready_) {
    return Status::kSuccess;
  }
  if (reservoir_status_ != Status::kSuccess) {
    return reservoir_status_;
  }
  reservoir_status_ = [&]() -> Status {
    if (rows_ == 0) {
      return Status::kSuccess;
    }
    DataChunk input;
    while (true) {
      const size_t count = child_->NextBatch(&input, kDefaultVectorSize);
      if (count == 0) {
        if (child_->GetStatus() != Status::kSuccess) {
          return child_->GetStatus();
        }
        break;
      }
      for (size_t i = 0; i < count; ++i) {
        const Row row = input.RowAt(i);
        ASSIGN_OR_RETURN(size_t, partition, (PartitionFor(row)));
        const RowPosition position = input.PositionAt(i);
        const uint64_t ordinal = ordinal_++;
        Candidate candidate{.row = row,
                            .position = position,
                            .priority = NextPriority(position, ordinal),
                            .ordinal = ordinal,
                            .partition = partition};
        PartitionState& state = partition_states_[partition];
        ++state.seen;
        if (state.candidates.size() < rows_) {
          state.candidates.push(std::move(candidate));
        } else if (!state.candidates.empty() &&
                   candidate.priority < state.candidates.top().priority) {
          state.candidates.pop();
          state.candidates.push(std::move(candidate));
        }
      }
    }
    for (auto& [key, partition] : partitions_) {
      (void)key;
      PartitionState& state = partition_states_[partition];
      state.selected = state.candidates.size();
      while (!state.candidates.empty()) {
        output_rows_.push_back(state.candidates.top());
        state.candidates.pop();
      }
    }
    bool all_positions_valid = true;
    for (const Candidate& candidate : output_rows_) {
      all_positions_valid = all_positions_valid && candidate.position.IsValid();
    }
    if (all_positions_valid) {
      std::ranges::stable_sort(
          output_rows_, [](const Candidate& left, const Candidate& right) {
            if (left.position.page_id != right.position.page_id) {
              return left.position.page_id < right.position.page_id;
            }
            return left.position.slot < right.position.slot;
          });
    } else {
      std::ranges::stable_sort(
          output_rows_, [](const Candidate& left, const Candidate& right) {
            return left.ordinal < right.ordinal;
          });
    }
    return Status::kSuccess;
  }();
  if (reservoir_status_ == Status::kSuccess) {
    reservoir_ready_ = true;
  }
  return reservoir_status_;
}

bool TableSampleExecutor::Next(Row* destination, RowPosition* position) {
  if (output_offset_ >= output_batch_.Size()) {
    if (NextBatch(&output_batch_) == 0) {
      return false;
    }
    output_offset_ = 0;
  }
  *destination = output_batch_.RowAt(output_offset_);
  if (position != nullptr) {
    *position = output_batch_.PositionAt(output_offset_);
  }
  ++output_offset_;
  return true;
}

size_t TableSampleExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  if (max_rows == 0) {
    return 0;
  }
  const Status initialized = Initialize();
  if (initialized != Status::kSuccess) {
    FailWith(initialized);
    return 0;
  }
  if (spec_.method == TableSampleMethod::kReservoir) {
    const Status materialized = MaterializeReservoir();
    if (materialized != Status::kSuccess) {
      FailWith(materialized);
      return 0;
    }
    destination->Reset(output_schema_, max_rows);
    while (output_offset_ < output_rows_.size() &&
           destination->Size() < max_rows) {
      const Candidate& candidate = output_rows_[output_offset_++];
      Row row = candidate.row;
      if (spec_.with_weight) {
        const PartitionState& state = partition_states_[candidate.partition];
        const size_t selected = state.selected;
        const double weight = selected == 0 ? 1.0
                                            : static_cast<double>(state.seen) /
                                                  static_cast<double>(selected);
        row.values_.push_back(Value(weight));
      }
      destination->Append(std::move(row), candidate.position);
    }
    return destination->Size();
  }

  destination->Reset(output_schema_, max_rows);
  if (probability_ <= 0.0) {
    return 0;
  }
  DataChunk input;
  while (destination->Size() < max_rows) {
    const size_t requested = max_rows - destination->Size();
    const size_t count = child_->NextBatch(&input, requested);
    if (count == 0) {
      if (child_->GetStatus() != Status::kSuccess) {
        FailWith(child_->GetStatus());
      }
      break;
    }
    for (size_t i = 0; i < count; ++i) {
      double unit = 0.0;
      if (probability_ < 1.0) {
        if (seed_.has_value()) {
          unit = static_cast<double>(
                     NextPriority(input.PositionAt(i), ordinal_++) >> 11) *
                 0x1.0p-53;
        } else {
          unit = NextUnit();
          ++ordinal_;
        }
      }
      if (probability_ >= 1.0 || unit < probability_) {
        Row row = input.RowAt(i);
        if (spec_.with_weight) {
          row.values_.push_back(Value(1.0 / probability_));
        }
        destination->Append(std::move(row), input.PositionAt(i));
      }
    }
  }
  return destination->Size();
}

void TableSampleExecutor::Dump(std::ostream& o, int indent) const {
  o << "Sample: ";
  spec_.Dump(o);
  o << "\n" << Indent(static_cast<size_t>(indent) + 2);
  child_->Dump(o, indent + 2);
}

}  // namespace tinylamb
