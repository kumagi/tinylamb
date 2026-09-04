/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DICTIONARY_BATCH_AGGREGATION_HPP
#define TINYLAMB_EXECUTOR_DICTIONARY_BATCH_AGGREGATION_HPP

#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "executor/data_chunk.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

// Dictionary-encoded fast-path batch aggregation engine.
// When group keys have low cardinality, group values are mapped to dense
// integer dictionary codes (0 <= code < K). Aggregations accumulate directly
// into flat code-indexed arrays without hash table hashing overhead during
// batch iteration.
class DictionaryBatchAggregation {
 public:
  enum class AggOp : uint8_t {
    kCount,
    kSum,
    kAvg,
    kMin,
    kMax,
  };

  struct Accumulator {
    int64_t count{0};
    double sum{0.0};
    Value min_val;
    Value max_val;
    bool has_val{false};

    void Accumulate(const Value& val, AggOp op);
    [[nodiscard]] Value Finalize(AggOp op) const;
  };

  DictionaryBatchAggregation(Schema schema, slot_t group_slot, slot_t agg_slot,
                             AggOp op);

  // Ingests a batch of rows from a DataChunk
  void AccumulateChunk(const DataChunk& chunk);

  // Emits aggregated result as a DataChunk
  [[nodiscard]] DataChunk EmitResult(const Schema& output_schema) const;

  [[nodiscard]] size_t GroupCount() const { return dictionary_.size(); }
  [[nodiscard]] const std::vector<Value>& Dictionary() const {
    return dictionary_;
  }

 private:
  uint32_t GetOrCreateCode(const Value& group_val);

  Schema schema_;
  slot_t group_slot_{0};
  slot_t agg_slot_{1};
  AggOp op_{AggOp::kSum};

  std::vector<Value> dictionary_;
  std::unordered_map<Value, uint32_t> code_map_;
  std::vector<Accumulator> accumulators_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_DICTIONARY_BATCH_AGGREGATION_HPP
