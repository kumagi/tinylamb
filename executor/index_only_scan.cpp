/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//
// Created by kumagi on 22/06/30.
//

#include "index_only_scan.hpp"

#include <ostream>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "executor/detail/scan_filter.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/rewrite.hpp"
#include "index/index.hpp"
#include "index/index_scan_iterator.hpp"
#include "index/index_schema.hpp"
#include "page/row_position.hpp"
#include "transaction/transaction.hpp"
#include "type/column.hpp"

namespace tinylamb {

namespace {

TransactionManager::IndexReadGuard AcquireOnlyScanIndexReadGuard(
    Transaction& txn, const Index& index,
    TransactionManager::IndexReadGuard index_read_guard) {
  if (index_read_guard.HoldsLock()) {
    return index_read_guard;
  }
  return txn.AcquireIndexReadGuard(index.Root());
}

}  // namespace

IndexOnlyScan::IndexOnlyScan(
    Transaction& txn, const Table& table, const Index& index,
    const Value& begin, const Value& end, bool ascending, Expression where,
    const Schema& sc, TransactionManager::IndexReadGuard index_read_guard)
    : IndexOnlyScan(
          txn, table, index,
          begin.IsNull() ? std::vector<Value>{} : std::vector<Value>{begin},
          end.IsNull() ? std::vector<Value>{} : std::vector<Value>{end},
          ascending, std::move(where), sc, std::move(index_read_guard)) {}

IndexOnlyScan::IndexOnlyScan(
    Transaction& txn, const Table& table, const Index& index,
    const std::vector<Value>& begin_key, const std::vector<Value>& end_key,
    bool ascending, Expression where, const Schema& sc,
    TransactionManager::IndexReadGuard index_read_guard)
    : index_read_guard_(AcquireOnlyScanIndexReadGuard(
          txn, index, std::move(index_read_guard))),
      iter_(table, index, txn, begin_key, end_key, ascending, false,
            &index_read_guard_),
      cond_(std::move(where)),
      cond_conjuncts_(SplitConjuncts(cond_)),
      key_schema_(KeySchema(index, sc)),
      value_schema_(ValueSchema(index, sc)),
      output_schema_(OutputSchema(index, sc)),
      include_offsets_(index.sc_.include_),
      ascending_(ascending),
      nonunique_keys_(index.sc_.mode_ == IndexMode::kNonUnique) {
  constant_true_ = cond_ && cond_->Type() == TypeTag::kConstantValue &&
                   cond_->AsConstantValue().GetValue().Truthy();
  key_only_condition_ = nonunique_keys_ && !cond_conjuncts_.empty();
  for (const auto& conjunct : cond_conjuncts_) {
    if (!conjunct) {
      continue;
    }
    auto program = BytecodeEnabled() ? BytecodeCompiler::CompileRowCached(
                                           conjunct, output_schema_)
                                     : std::nullopt;
    // A context-free program is deterministic over its input cells. Reuse
    // its predicate verdict within one duplicate-key value list only when
    // every loaded cell belongs to the key, never to an INCLUDE payload.
    if (!program) {
      key_only_condition_ = false;
    } else {
      for (const auto& instruction : program->Instructions()) {
        if (instruction.opcode == BytecodeOp::kLoadColumn &&
            instruction.operand >= key_schema_.ColumnCount()) {
          key_only_condition_ = false;
        }
      }
    }
    cond_programs_.push_back(std::move(program));
  }
}

Schema IndexOnlyScan::KeySchema(const Index& idx, const Schema& input_schema) {
  const IndexSchema& is = idx.sc_;
  std::vector<Column> cols;
  cols.reserve(is.key_.size());
  for (const auto& key : is.key_) {
    cols.push_back(input_schema.GetColumn(key));
  }
  return {"", cols};
}

Schema IndexOnlyScan::ValueSchema(const Index& idx,
                                  const Schema& input_schema) {
  const IndexSchema& is = idx.sc_;
  std::vector<Column> cols;
  cols.reserve(is.include_.size());
  for (const auto& key : is.include_) {
    cols.push_back(input_schema.GetColumn(key));
  }
  return {"", cols};
}

Schema IndexOnlyScan::OutputSchema(const Index& idx,
                                   const Schema& input_schema) {
  const IndexSchema& is = idx.sc_;
  std::vector<Column> cols;
  cols.reserve(is.key_.size() + is.include_.size());
  for (const auto& key : is.key_) {
    cols.push_back(input_schema.GetColumn(key));
  }
  for (const auto& included : is.include_) {
    cols.push_back(input_schema.GetColumn(included));
  }
  return {"", cols};
}

bool IndexOnlyScan::TryEnableKeyDeduplication() {
  // A residual or INCLUDE payload can distinguish rows sharing a key. Keep
  // those scans on the ordinary row-wise path rather than picking one row.
  if (!nonunique_keys_ || key_schema_.ColumnCount() != 1 ||
      value_schema_.ColumnCount() != 0 || (cond_ && !constant_true_)) {
    return false;
  }
  distinct_keys_ = true;
  iter_.SetDistinctKeysMode(true);
  return true;
}

size_t IndexOnlyScan::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset(output_schema_, max_rows);
  Row dst;
  while (destination->Size() < max_rows && iter_.IsValid()) {
    // Advancing below can load the next key. Capture this entry's identity
    // first; the iterator increments it whenever it decodes a value list.
    const size_t key_generation = iter_.ValueListsDecoded();
    if (!iter_.TryReadIndexOnly(&dst)) {
      const Row& heap_row = *iter_;
      if (!heap_row.IsValid()) {
        ++iter_;
        continue;
      }
      if (include_offsets_.empty()) {
        dst = iter_.GetKey();
      } else {
        Row inc;
        inc.values_.reserve(include_offsets_.size());
        for (slot_t offset : include_offsets_) {
          inc.values_.push_back(heap_row[offset]);
        }
        dst = iter_.GetKey() + inc;
      }
    }
    if (distinct_keys_ && dst.IsValid()) {
      iter_.AdvanceToNextKey();
    } else {
      ++iter_;
    }
    if (!dst.IsValid()) {
      continue;
    }
    if (cond_ && !constant_true_) {
      if (key_only_condition_ && condition_key_generation_ == key_generation) {
        if (!condition_key_passed_) {
          continue;
        }
        destination->Append(std::move(dst));
        continue;
      }
      size_t predicate_index = 0;
      StatusOr<bool> pass = relational_detail::EvaluateConjunctsTolerant(
          cond_conjuncts_, [&](const Expression& conjunct) {
            const auto& program = cond_programs_[predicate_index++];
            if (program) {
              return TryEvaluateRowAuto(*program, dst);
            }
            return conjunct->TryEvaluate(dst, output_schema_);
          });
      if (!pass.HasValue()) {
        FailWith(pass.GetStatus());
        return destination->Size();
      }
      if (key_only_condition_) {
        condition_key_generation_ = key_generation;
        condition_key_passed_ = pass.Value();
      }
      if (!pass.Value()) {
        continue;
      }
    }
    destination->Append(std::move(dst));
  }
  if (!iter_.IsValid() && iter_.GetStatus() != Status::kSuccess) {
    FailWith(iter_.GetStatus());
  }
  return destination->Size();
}

void IndexOnlyScan::Dump(std::ostream& o, int /*indent*/) const {
  o << "IndexOnlyScan: " << iter_;
  if (!ascending_) {
    o << " reverse";
  }
  if (distinct_keys_) {
    o << " distinct-keys";
  }
  if (cond_) {
    o << " WHERE " << *cond_;
  }
}

}  // namespace tinylamb
