/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_LATE_MATERIALIZATION_JOIN_HPP
#define TINYLAMB_EXECUTOR_LATE_MATERIALIZATION_JOIN_HPP

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

#include "common/constants.hpp"
#include "common/late_materialization.hpp"
#include "common/status_or.hpp"
#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class Index;
class IndexScanIterator;
class Table;
class Transaction;

class LateMaterializationJoin final : public ExecutorBase {
 public:
  LateMaterializationJoin(
      Transaction& txn, Executor left, std::vector<slot_t> left_columns,
      const Table& right_table, const Index& right_index,
      std::vector<slot_t> right_columns, Schema output_schema,
      Expression residual = nullptr, bool unique_winner = false,
      std::vector<slot_t> right_projection = {},
      TransactionManager::IndexReadGuard index_read_guard = {});
  LateMaterializationJoin(Transaction& txn, Executor left,
                          const Table& right_table, Schema output_schema,
                          LateMaterializationRowIdBinding rowid,
                          std::vector<slot_t> right_projection = {},
                          Expression residual = nullptr);
  LateMaterializationJoin(const LateMaterializationJoin&) = delete;
  LateMaterializationJoin(LateMaterializationJoin&&) = delete;
  LateMaterializationJoin& operator=(const LateMaterializationJoin&) = delete;
  LateMaterializationJoin& operator=(LateMaterializationJoin&&) = delete;
  ~LateMaterializationJoin() override;

  bool Next(Row* destination, RowPosition* position) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& output, int indent) const override;

  [[nodiscard]] static bool SupportsKeyType(ValueType type);
  [[nodiscard]] LateMaterializationIdentity Identity() const {
    return identity_;
  }
  [[nodiscard]] const Schema& OutputSchema() const { return output_schema_; }
  [[nodiscard]] size_t CandidateCount() const { return candidate_count_; }
  [[nodiscard]] size_t PayloadFetchCount() const {
    return payload_fetch_count_;
  }
  [[nodiscard]] const std::vector<slot_t>& RightProjection() const {
    return right_projection_;
  }

 private:
  struct Candidate {
    RowPosition position;
    Row key;
  };

  bool Validate();
  bool LoadNextIdentity();
  bool LoadEquiCandidates(const Row& left_row);
  bool LoadRowIdCandidate(const Row& left_row);
  [[nodiscard]] bool KeyMatches(const Row& left_key,
                                const Row& index_key) const;
  [[nodiscard]] StatusOr<Row> MaterializePayload(const Candidate& candidate,
                                                 const Row& left_row) const;
  [[nodiscard]] std::optional<Row> ExtractLeftKey(const Row& left_row) const;
  [[nodiscard]] std::optional<RowPosition> ExtractRowId(
      const Row& left_row) const;
  [[nodiscard]] bool IsLeftRowValid(const Row& left_row) const;

  Transaction& txn_;
  Executor left_;
  const Table* right_table_{nullptr};
  const Index* right_index_{nullptr};
  std::vector<slot_t> left_columns_;
  std::vector<slot_t> right_columns_;
  std::vector<slot_t> right_projection_;
  Schema output_schema_;
  Expression residual_;
  std::vector<Expression> residual_conjuncts_;
  LateMaterializationIdentity identity_{LateMaterializationIdentity::kEquiKey};
  LateMaterializationRowIdBinding rowid_{};
  TransactionManager::IndexReadGuard index_read_guard_;
  std::unique_ptr<IndexScanIterator> right_iterator_;
  Row hold_left_;
  RowPosition hold_left_position_;
  std::vector<Candidate> candidates_;
  size_t candidate_offset_{0};
  size_t candidate_count_{0};
  mutable size_t payload_fetch_count_{0};
  bool unique_winner_{false};
  bool have_left_{false};
  Status initial_status_{Status::kSuccess};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_LATE_MATERIALIZATION_JOIN_HPP
