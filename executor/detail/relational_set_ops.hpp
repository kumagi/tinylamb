/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_RELATIONAL_SET_OPS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_RELATIONAL_SET_OPS_HPP

#include <optional>
#include <span>

#include "common/set_operation.hpp"
#include "executor/detail/relation.hpp"

namespace tinylamb::relational_detail {

bool NormalizeMovableSetRelations(std::span<Relation*> inputs);
bool NormalizeMovableSetInputs(Relation& left, Relation& right);
std::optional<QueryMemoryCharge> ResidentSetCharge(const Relation& left,
                                                   const Relation& right);
Status DistinctResidentRows(Relation& relation);
Status ReduceResidentRows(Relation& left, const Relation& right,
                          SetOperationKind operation);
bool TryConcatenateColumnarUnionAll(Relation& left, Relation& right);
bool TryColumnarSetOperation(Relation& left, Relation& right,
                             SetOperationKind operation);
bool TryColumnarMultiUnion(std::span<Relation*> inputs);

inline bool IsPresenceOnlySetOperation(SetOperationKind kind) {
  return kind == SetOperationKind::kIntersect ||
         kind == SetOperationKind::kExcept || kind == SetOperationKind::kUnion;
}

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_RELATIONAL_SET_OPS_HPP
