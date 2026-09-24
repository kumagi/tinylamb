#ifndef TINYLAMB_EXECUTOR_TABLE_SAMPLE_HPP
#define TINYLAMB_EXECUTOR_TABLE_SAMPLE_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <queue>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status_or.hpp"
#include "executor/executor_base.hpp"
#include "expression/table_sample_spec.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class TransactionContext;

class TableSampleExecutor final : public ExecutorBase {
 public:
  TableSampleExecutor(TransactionContext& context, Executor child,
                      Schema child_schema, TableSampleSpec spec);
  TableSampleExecutor(const TableSampleExecutor&) = delete;
  TableSampleExecutor(TableSampleExecutor&&) = delete;
  TableSampleExecutor& operator=(const TableSampleExecutor&) = delete;
  TableSampleExecutor& operator=(TableSampleExecutor&&) = delete;
  ~TableSampleExecutor() override = default;

  bool Next(Row* destination, RowPosition* position) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  [[nodiscard]] const Schema& OutputSchema() const { return output_schema_; }
  void Dump(std::ostream& o, int indent) const override;

 private:
  struct PartitionHash {
    size_t operator()(const Row& row) const;
  };
  struct PartitionEqual {
    bool operator()(const Row& left, const Row& right) const;
  };
  struct Candidate {
    Row row;
    RowPosition position;
    uint64_t priority{0};
    uint64_t ordinal{0};
    size_t partition{0};

    bool operator<(const Candidate& other) const {
      if (priority != other.priority) {
        return priority < other.priority;
      }
      return ordinal > other.ordinal;
    }
  };
  struct PartitionState {
    size_t id{0};
    size_t seen{0};
    size_t selected{0};
    std::priority_queue<Candidate> candidates;
  };

  Status Initialize();
  Status MaterializeReservoir();
  StatusOr<size_t> PartitionFor(const Row& row);
  uint64_t NextPriority(const RowPosition& position, uint64_t ordinal);
  double NextUnit();
  static uint64_t Mix(uint64_t value);

  TransactionContext& context_;
  Executor child_;
  Schema child_schema_;
  Schema output_schema_;
  TableSampleSpec spec_;
  std::optional<uint64_t> seed_;
  std::mt19937_64 random_;
  double probability_{1.0};
  size_t rows_{0};
  uint64_t ordinal_{0};
  bool initialized_{false};
  bool reservoir_ready_{false};
  Status initialization_status_{Status::kSuccess};
  Status reservoir_status_{Status::kSuccess};
  std::unordered_map<Row, size_t, PartitionHash, PartitionEqual> partitions_;
  std::vector<PartitionState> partition_states_;
  std::vector<Candidate> output_rows_;
  size_t output_offset_{0};
  DataChunk output_batch_;
};

#endif
}
