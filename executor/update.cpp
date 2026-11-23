/**
 * Copyright 2023 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "update.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/memory_budget.hpp"
#include "common/status_or.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/row_block_codec.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

std::string UpdateKeyString(const Value& value) {
  if (value.IsNull()) {
    return "\x01NULL";
  }
  return value.AsString();
}

// P652/689: budget-bounded cache of before-images keyed by RowPosition.  A
// full-table UPDATE used to decode each target row two or three times: the
// FK/PK verification passes re-read the row by position through
// pending.ForEach, and Table::Update then read the before-image once more
// for its index diff / failure-restore image.  The first reader stores the
// row here; later verification passes and the apply loop consume that copy
// (the apply loop hands it to the before-image-taking Table::Update
// overload).  When the query-pool budget refuses an entry, Get falls back to
// the caller's scratch row and the apply loop transparently lets
// Table::Update read the image itself -- exactly the old behavior.
class UpdatePreimageCache {
 public:
  UpdatePreimageCache() = default;
  UpdatePreimageCache(const UpdatePreimageCache&) = delete;
  UpdatePreimageCache& operator=(const UpdatePreimageCache&) = delete;
  UpdatePreimageCache(UpdatePreimageCache&&) = delete;
  UpdatePreimageCache& operator=(UpdatePreimageCache&&) = delete;
  ~UpdatePreimageCache() = default;

  // Returns a pointer to the before-image at `pos`.  A cache hit points into
  // entries_ and stays valid until Take() erases it.  On an over-budget miss
  // the pointee is the caller-owned `scratch`, so its lifetime is the caller's
  // scope (a callback-local Row must not be passed here); the apply loop never
  // relies on that case because Take() misses make Table::Update re-read the
  // before-image itself.
  StatusOr<const Row*> Get(const Table& target, Transaction& txn,
                           const RowPosition& pos, Row* scratch) {
    const auto found = entries_.find(pos);
    if (found != entries_.end()) {
      return &found->second;
    }
    StatusOr<Row> read = target.Read(txn, pos);
    if (!read.HasValue()) {
      return read.GetStatus();
    }
    Row image = read.MoveValue();
    if (!charge_.TryAdd(EstimateRowMemoryBytes(image))) {
      *scratch = std::move(image);
      return scratch;
    }
    return &entries_.emplace(pos, std::move(image)).first->second;
  }

  // Moves the cached image into `*out` and evicts the entry (each position
  // is updated at most once per statement).  Returns false on a cache miss;
  // the apply loop then lets Table::Update fetch the before-image itself.
  bool Take(const RowPosition& pos, Row* out) {
    const auto found = entries_.find(pos);
    if (found == entries_.end()) {
      return false;
    }
    *out = std::move(found->second);
    charge_.Release(EstimateRowMemoryBytes(*out));
    entries_.erase(found);
    return true;
  }

 private:
  std::unordered_map<RowPosition, Row> entries_;
  ProcessMemoryCharge charge_{MemoryPoolKind::kQuery, 0, false};
};

}  // namespace

bool Update::Execute(Row* dst, RowPosition* rp) {
  if (finished_) {
    return false;
  }
  uint64_t pull_steps = 0;
  SpillablePendingUpdateList pending;
  UpdatePreimageCache preimages;
  DataChunk batch;
  while (src_->NextBatch(&batch) != 0) {
    for (size_t i = 0; i < batch.Size(); ++i) {
      if ((++pull_steps & 255U) == 0 && txn_->HasStatementTimeout()) {
        const Status ts = txn_->CheckStatementTimeout();
        if (ts != Status::kSuccess) {
          return FailWith(ts);
        }
      }
      const RowPosition position = batch.PositionAt(i);
      // assert() disappears under NDEBUG; report an invalid source position
      // through Status so a malformed batch cannot feed a bogus row id into
      // the pending update list.
      if (!position.IsValid()) {
        return FailWith(StatusError(
            StatusCode::kInvalidArgument,
            "UPDATE source produced an invalid row position"));
      }
      const Status push_st = pending.PushBack(batch.RowAt(i), position);
      if (push_st != Status::kSuccess) {
        return FailWith(push_st);
      }
    }
  }
  if (src_->GetStatus() != Status::kSuccess) {
    return FailWith(src_->GetStatus());
  }
  // Primary-key emulation: every updated row vacates its current first-column
  // key and claims a new one; claiming a key still held by another row (or
  // claimed twice within this statement) is a duplicate-key error, and NULL
  // participates like any other key value.
  std::unordered_map<std::string, int64_t> live_keys;
  if (enforce_primary_key_) {
    auto it = target_->BeginFullScan(*txn_);
    size_t pk_scan_steps = 0;
    for (; it.IsValid(); ++it) {
      if ((++pk_scan_steps & 255U) == 0 && txn_->HasStatementTimeout()) {
        const Status ts = txn_->CheckStatementTimeout();
        if (ts != Status::kSuccess) {
          return FailWith(ts);
        }
      }
      const Row& current = *it;
      if (current.values_.empty()) {
        continue;
      }
      ++live_keys[UpdateKeyString(current[0])];
    }
    if (it.GetStatus() != Status::kSuccess) {
      return FailWith(it.GetStatus());
    }
    size_t pk_action_steps = 0;
    const Status vacate_st =
        pending.ForEach([&](const Row&, const RowPosition& pos) -> Status {
          if ((++pk_action_steps & 255U) == 0 && txn_->HasStatementTimeout()) {
            RETURN_IF_FAIL(txn_->CheckStatementTimeout());
          }
          Row scratch;
          StatusOr<const Row*> current =
              preimages.Get(*target_, *txn_, pos, &scratch);
          if (!current.HasValue()) {
            return current.GetStatus();
          }
          if (current.Value()->values_.empty()) {
            return StatusError(StatusCode::kCorrupt,
                               "primary-key update read an empty row");
          }
          const std::string old_key = UpdateKeyString((*current.Value())[0]);
          auto found = live_keys.find(old_key);
          if (found != live_keys.end()) {
            if (--found->second == 0) {
              live_keys.erase(found);
            }
          }
          return Status::kSuccess;
        });
    if (vacate_st != Status::kSuccess) {
      return FailWith(vacate_st);
    }
    std::unordered_set<std::string> claimed;
    const Status claim_st = pending.ForEach([&](const Row& updated_row,
                                                const RowPosition&) -> Status {
      if ((++pk_action_steps & 255U) == 0 && txn_->HasStatementTimeout()) {
        RETURN_IF_FAIL(txn_->CheckStatementTimeout());
      }
      if (updated_row.values_.empty()) {
        return Status::kSuccess;
      }
      const std::string new_key = UpdateKeyString(updated_row[0]);
      if (!claimed.insert(new_key).second || live_keys.contains(new_key)) {
        return StatusError(
            StatusCode::kDuplicates,
            "Modification resulted in duplicate primary key (" + new_key + ")");
      }
      return Status::kSuccess;
    });
    if (claim_st != Status::kSuccess) {
      return FailWith(claim_st);
    }
  }
  // FOREIGN KEY enforcement.  The parent multiset is seeded once; changed
  // child keys probe it, and changed parent keys probe the referencing
  // tables in a single pass via FindReferencedParentKey (NO ACTION: a
  // referenced old key rejects the statement).
  std::vector<ForeignKeyCounts> fk_counts;
  if (!fk_checks_.empty() || !ref_checks_.empty()) {
    fk_counts.reserve(fk_checks_.size());
    for (const ForeignKeyCheck& check : fk_checks_) {
      StatusOr<ForeignKeyCounts> counts =
          BuildForeignKeyCounts(*check.parent, *txn_, check.parent_offset);
      if (!counts.HasValue()) {
        return FailWith(counts.GetStatus());
      }
      fk_counts.push_back(counts.MoveValue());
    }
    // For self-references, validate children against the statement's final
    // parent multiset, including keys supplied by other updated rows.
    for (size_t c = 0; c < fk_checks_.size(); ++c) {
      const ForeignKeyCheck& check = fk_checks_[c];
      if (check.parent != target_) {
        continue;
      }
      size_t adjust_steps = 0;
      const Status adjust_st = pending.ForEach(
          [&](const Row& updated, const RowPosition& pos) -> Status {
            if ((++adjust_steps & 255U) == 0 && txn_->HasStatementTimeout()) {
              RETURN_IF_FAIL(txn_->CheckStatementTimeout());
            }
            Row scratch;
            ASSIGN_OR_RETURN(const Row*, old,
                             preimages.Get(*target_, *txn_, pos, &scratch));
            if (check.parent_offset >= old->Count() ||
                check.parent_offset >= updated.Count()) {
              return Status::kSuccess;
            }
            RETURN_IF_FAIL(
                fk_counts[c].DecrementKey((*old)[check.parent_offset]));
            return fk_counts[c].IncrementKey(updated[check.parent_offset]);
          });
      if (!adjust_st.ok()) {
        return FailWith(adjust_st);
      }
    }
    if (!fk_checks_.empty()) {
      size_t fk_action_steps = 0;
      const Status child_fk_st =
          pending.ForEach([&](const Row& updated_row,
                              const RowPosition& row_position) -> Status {
            if ((++fk_action_steps & 255U) == 0 &&
                txn_->HasStatementTimeout()) {
              RETURN_IF_FAIL(txn_->CheckStatementTimeout());
            }
            Row scratch;
            StatusOr<const Row*> old =
                preimages.Get(*target_, *txn_, row_position, &scratch);
            if (!old.HasValue()) {
              return old.GetStatus();
            }
            const Row& old_row = *old.Value();
            for (size_t c = 0; c < fk_checks_.size(); ++c) {
              const ForeignKeyCheck& check = fk_checks_[c];
              if (check.child_offset >= updated_row.values_.size() ||
                  check.child_offset >= old_row.values_.size()) {
                continue;
              }
              const Value& new_key = updated_row[check.child_offset];
              const Value& old_key = old_row[check.child_offset];
              if (new_key == old_key) {
                continue;
              }
              StatusOr<bool> viol =
                  IsForeignKeyViolation(fk_counts[c], new_key);
              if (!viol.HasValue()) {
                return viol.GetStatus();
              }
              if (viol.Value()) {
                return StatusError(
                    StatusCode::kConflicts,
                    "update on table " +
                        std::string(target_->GetSchema().Name()) +
                        " violates foreign key constraint (references " +
                        std::string(check.parent->GetSchema().Name()) + ")");
              }
            }
            return Status::kSuccess;
          });
      if (child_fk_st != Status::kSuccess) {
        return FailWith(child_fk_st);
      }
    }
    if (!ref_checks_.empty() && !pending.Empty()) {
      for (const ReferencedByCheck& check : ref_checks_) {
        // A key the statement itself re-supplies is not vacated: a swap
        // (row1 A->B, row2 B->A) and a row that keeps its key both leave
        // the value present at statement end, so children referencing it
        // must not reject the statement. Count the changed rows' old keys
        // and remove keys supplied by the post-images.
        std::unordered_set<std::string> supplied_keys;
        std::unordered_map<std::string, size_t> vacated_keys;
        const Status scan_st = pending.ForEach(
            [&](const Row& updated_row, const RowPosition& pos) -> Status {
              if (check.parent_offset < updated_row.values_.size() &&
                  !updated_row[check.parent_offset].IsNull()) {
                supplied_keys.insert(
                    ForeignKeyString(updated_row[check.parent_offset]));
              }
              Row scratch;
              StatusOr<const Row*> old =
                  preimages.Get(*target_, *txn_, pos, &scratch);
              if (!old.HasValue()) {
                return old.GetStatus();
              }
              const Row& old_row = *old.Value();
              if (check.parent_offset >= updated_row.values_.size() ||
                  check.parent_offset >= old_row.values_.size()) {
                return Status::kSuccess;
              }
              const Value& new_key = updated_row[check.parent_offset];
              const Value& old_key = old_row[check.parent_offset];
              if (new_key == old_key || old_key.IsNull()) {
                return Status::kSuccess;
              }
              ++vacated_keys[ForeignKeyString(old_key)];
              return Status::kSuccess;
            });
        if (scan_st != Status::kSuccess) {
          return FailWith(scan_st);
        }
        for (auto it = vacated_keys.begin(); it != vacated_keys.end();) {
          if (supplied_keys.contains(it->first)) {
            it = vacated_keys.erase(it);
          } else {
            ++it;
          }
        }
        // A non-unique referenced column can have untouched rows for the
        // same key. A snapshot occurrence beyond the number being changed
        // proves the key still exists after the update.
        if (!vacated_keys.empty() &&
            check.parent_offset >= target_->GetSchema().ColumnCount()) {
          return FailWith(StatusError(
              StatusCode::kCorrupt,
              "referenced key offset exceeds target schema width"));
        }
        if (!vacated_keys.empty() && !target_->GetSchema()
                                          .GetColumn(check.parent_offset)
                                          .GetConstraint()
                                          .IsUnique()) {
          const std::vector<slot_t> projection{
              static_cast<slot_t>(check.parent_offset)};
          auto current = target_->BeginFullScan(*txn_, projection);
          size_t steps = 0;
          for (; current.IsValid(); ++current) {
            if ((++steps & 255U) == 0 && txn_->HasStatementTimeout()) {
              const Status timeout = txn_->CheckStatementTimeout();
              if (timeout != Status::kSuccess) {
                return FailWith(timeout);
              }
            }
            const Row& parent = *current;
            if (parent.values_.empty() || parent[0].IsNull()) {
              continue;
            }
            auto found = vacated_keys.find(ForeignKeyString(parent[0]));
            if (found == vacated_keys.end()) {
              continue;
            }
            if (found->second > 0) {
              --found->second;
              continue;
            }
            vacated_keys.erase(found);
            if (vacated_keys.empty()) {
              break;
            }
          }
          if (current.GetStatus() != Status::kSuccess) {
            return FailWith(current.GetStatus());
          }
        }
        if (vacated_keys.empty()) {
          continue;
        }
        StatusOr<std::optional<std::string>> referenced =
            FindReferencedParentKey(
                *check.child, *txn_, check.child_offset,
                [&](const std::function<Status(const Value&)>& emit) -> Status {
                  return pending.ForEach(
                      [&](const Row& updated_row,
                          const RowPosition& row_position) -> Status {
                        Row scratch;
                        StatusOr<const Row*> old = preimages.Get(
                            *target_, *txn_, row_position, &scratch);
                        if (!old.HasValue()) {
                          return old.GetStatus();
                        }
                        const Row& old_row = *old.Value();
                        if (check.parent_offset >= updated_row.values_.size() ||
                            check.parent_offset >= old_row.values_.size()) {
                          return Status::kSuccess;
                        }
                        const Value& old_key = old_row[check.parent_offset];
                        if (old_key.IsNull() ||
                            !vacated_keys.contains(ForeignKeyString(old_key))) {
                          return Status::kSuccess;
                        }
                        return emit(old_key);
                      });
                },
                [&](const std::function<Status(const RowPosition&)>& emit)
                    -> Status {
                  // Self-referencing edge only: the ignore positions are
                  // target-row positions, meaningless against another child
                  // table's row space. The probe must not count this
                  // statement's own pre-images; the post-image references
                  // are checked explicitly below.
                  if (check.child != target_) {
                    return Status::kSuccess;
                  }
                  return pending.ForEach(
                      [&](const Row&, const RowPosition& row_position) {
                        return emit(row_position);
                      });
                });
        if (!referenced.HasValue()) {
          return FailWith(referenced.GetStatus());
        }
        if (referenced.Value().has_value()) {
          return FailWith(StatusError(
              StatusCode::kConflicts,
              "update on table " + std::string(target_->GetSchema().Name()) +
                  " violates foreign key constraint (referenced by " +
                  std::string(check.child->GetSchema().Name()) + ")"));
        }
        if (check.child == target_) {
          // The probe skipped this statement's own rows (pre-images); a
          // rewritten row whose post-image still references a vacated key
          // is a violation the scan cannot see.
          const Status post_st = pending.ForEach(
              [&](const Row& updated_row, const RowPosition&) -> Status {
                if (check.child_offset >= updated_row.values_.size()) {
                  return Status::kSuccess;
                }
                const Value& child_key = updated_row[check.child_offset];
                if (!child_key.IsNull() &&
                    vacated_keys.contains(ForeignKeyString(child_key))) {
                  return StatusError(
                      StatusCode::kConflicts,
                      "update on table " +
                          std::string(target_->GetSchema().Name()) +
                          " violates foreign key constraint (referenced by " +
                          std::string(check.child->GetSchema().Name()) + ")");
                }
                return Status::kSuccess;
              });
          if (post_st != Status::kSuccess) {
            return FailWith(post_st);
          }
        }
      }
    }
  }
  // The verification phases above (PK emulation, FK counts) may have spent
  // the budget without a timeout probe. Catch an already-expired deadline
  // before the first row is written.
  if (txn_->HasStatementTimeout()) {
    const Status ts = txn_->CheckStatementTimeout();
    if (ts != Status::kSuccess) {
      return FailWith(ts);
    }
  }
  int64_t update_count = 0;
  uint64_t apply_steps = 0;
  const std::string target_name = std::string(target_->GetSchema().Name());
  const Status apply_st = pending.ForEach(
      [&](const Row& row, const RowPosition& row_position) -> Status {
        // The whole DML operator fits in one NextBatch, so the executor's
        // pre/post-batch timeout wrapper cannot see the apply phase. Check
        // it here or a bulk UPDATE writes every row after the deadline and
        // still reports success. Batched like the pull loop to keep the
        // clock read off the per-row hot path.
        if ((++apply_steps & 255U) == 0 && txn_->HasStatementTimeout()) {
          const Status ts = txn_->CheckStatementTimeout();
          if (ts != Status::kSuccess) {
            return ts;
          }
        }
        // P652/689: consume a before-image cached by a verification pass
        // (saving Table::Update's own decode); without one, Update reads the
        // image exactly as before.
        Row preimage;
        StatusOr<RowPosition> p =
            preimages.Take(row_position, &preimage)
                ? target_->Update(*txn_, row_position, row, std::move(preimage))
                : target_->Update(*txn_, row_position, row);
        if (p.GetStatus() != Status::kSuccess) {
          // Preserve the cause: constraint, storage and missing-row failures
          // are not all retryable writer conflicts. Do not report a partial
          // update as successful or discard the lower layer's diagnostic.
          const Status failure = p.GetStatus();
          return StatusError(failure.GetCode(), "update failed on table " +
                                                    target_name + ": " +
                                                    ToString(failure));
        }
        ++update_count;
        return Status::kSuccess;
      });
  if (apply_st != Status::kSuccess) {
    return FailWith(apply_st);
  }
  if (assert_rows_modified_ >= 0 && update_count != assert_rows_modified_) {
    return FailWith(
        StatusError(StatusCode::kInvalidArgument,
                    "ASSERT_ROWS_MODIFIED was specified with " +
                        std::to_string(assert_rows_modified_) + " rows, but " +
                        std::to_string(update_count) + " rows were modified"));
  }
  *dst = Row({Value("Update Rows"), Value(update_count)});
  if (rp != nullptr) {
    *rp = RowPosition();
  }
  finished_ = true;
  return true;
}

size_t Update::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  if (max_rows == 0) {
    return 0;
  }
  Row row;
  RowPosition rp;
  if (!Execute(&row, &rp)) {
    return 0;
  }
  destination->Append(std::move(row), rp);
  return 1;
}

void Update::Dump(std::ostream& o, int indent) const {
  o << "Update: " << target_->GetSchema().Name() << "\n"
    << Indent(static_cast<size_t>(indent) + 2);
  src_->Dump(o, indent + 2);
}

}  // namespace tinylamb
