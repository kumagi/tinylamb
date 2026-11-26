/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#pragma once

#include <array>
#include <charconv>

#include "query/select_rewrites.hpp"

namespace tinylamb::select_rewrite_detail {

template <typename T>
std::string IntegerToName(T value) {
  std::array<char, 32> buffer{};
  const auto [ptr, ec] =
      std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
  if (ec == std::errc{}) {
    return std::string(buffer.data(), ptr);
  }
  return std::to_string(value);
}

bool ContainsQueryExpression(const Expression& expression);

void CountCteReferencesInStatement(  // NOLINT(misc-no-recursion)
    const SelectStatement& statement,
    const std::unordered_set<std::string>& names,
    std::unordered_map<std::string, size_t>* counts);

bool StatementQualifiesName(  // NOLINT(misc-no-recursion)
    const SelectStatement& statement, const std::string& name);

void CountNestedCteReferencesInExpression(  // NOLINT(misc-no-recursion)
    const Expression& expression, const std::string& name, size_t* count);

size_t CountNestedCteReferences(  // NOLINT(misc-no-recursion)
    const SelectStatement& statement, const std::string& name,
    bool count_own_sources, bool walk_with_queries);

struct SharedCteSite {
  SelectStatement* consumer{nullptr};
  std::string alias;
};

bool DerivedExpressionIsImmutable(  // NOLINT(misc-no-recursion)
    const Expression& expression);

std::string LoweredStarName(std::string_view text);

bool FlattenGroupedDerivedJoinCore(SelectStatement* outer);

struct GroupingRelation {
  std::string name;
  const Schema* schema{nullptr};
};

bool ResolveGroupingColumn(const std::vector<GroupingRelation>& relations,
                           const ColumnName& column,
                           GroupingRelation* relation);

bool ColumnIsPrimaryKey(const GroupingRelation& relation,
                        std::string_view name);

struct RuntimeUniqueColInfo {
  bool unique_non_null{false};
  int64_t min_int{0};
  int64_t max_int{-1};
  size_t row_count{0};
};

RuntimeUniqueColInfo InspectRuntimeUniqueNonNullColumn(
    TransactionContext& ctx, const std::string& table_name,
    std::string_view col_name);

bool TpcdsExprHasParam(const Expression& expr);

bool TpcdsStmtHasParam(  // NOLINT(misc-no-recursion)
    const SelectStatement& stmt);

bool IsSafeDisjointTagValue(const Value& val) noexcept;

std::optional<Value> TpcdsEvalClosedConstant(const Expression& expr);

bool SelectListHasStarOrModifiers(const SelectStatement& stmt);

std::string ExplicitOrColumnOutputName(const NamedExpression& item);

std::string InferSelectItemOutputName(const NamedExpression& item,
                                      size_t index);

bool ExtractPureUnionAllBranches(  // NOLINT(misc-no-recursion)
    const SelectStatement& union_stmt,
    std::vector<std::shared_ptr<SelectStatement>>* out_branches);

void SyncSetOpHeadFields(
    SelectStatement* container, const SelectStatement& head,
    const std::vector<const SelectStatement*>* other_leaves = nullptr);

template <typename LeafFn>
bool VisitSetOpLeavesAndSync(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<SelectStatement*>* visited,
    LeafFn&& leaf_fn) {
  if (stmt == nullptr || !visited->insert(stmt).second) {
    return false;
  }
  if (stmt->UnionAll().empty() && stmt->GetSetOperationTree() == nullptr) {
    return leaf_fn(stmt);
  }
  bool changed = false;
  const auto& tree = stmt->GetSetOperationTree();
  if (tree != nullptr) {
    if (tree->first != nullptr) {
      changed |= VisitSetOpLeavesAndSync(tree->first.get(), visited, leaf_fn);
    }
    std::vector<const SelectStatement*> other_leaves;
    other_leaves.reserve(tree->branches.size());
    for (const auto& br : tree->branches) {
      changed |= VisitSetOpLeavesAndSync(br.get(), visited, leaf_fn);
      other_leaves.push_back(br.get());
    }
    if (changed && !tree->grouped && tree->first != nullptr &&
        tree->first->UnionAll().empty() &&
        tree->first->GetSetOperationTree() == nullptr) {
      // Flattening promotes branch-local WITH declarations into a common
      // scope. Preserve distinct lexical bindings by executing the original
      // operands separately when two branches declare the same name.
      auto bindings = tree->first->WithQueries();
      bool separate_scopes = false;
      for (const SelectStatement* leaf : other_leaves) {
        if (leaf == nullptr) {
          continue;
        }
        for (const auto& [name, body] : leaf->WithQueries()) {
          auto [at, inserted] = bindings.emplace(name, body);
          if (!inserted && at->second != body) {
            separate_scopes = true;
          }
        }
      }
      if (separate_scopes) {
        auto scoped = std::make_shared<SetOperationTree>(*tree);
        scoped->grouped = true;
        stmt->SetSetOperationTree(std::move(scoped));
      } else {
        SyncSetOpHeadFields(stmt, *tree->first, &other_leaves);
      }
    }
  } else {
    SelectStatement head = *stmt;
    head.ClearUnionAll();
    head.SetOrderBy({});
    head.SetLimit(std::nullopt);
    head.SetOffset(0);
    if (leaf_fn(&head)) {
      SyncSetOpHeadFields(stmt, head);
      changed = true;
    }
  }
  for (const auto& br : stmt->UnionAll()) {
    changed |= VisitSetOpLeavesAndSync(br.get(), visited, leaf_fn);
  }
  return changed;
}

bool ValidateGroupedCteTarget(
    SelectStatement* grouped_stmt,
    std::unordered_map<std::string, ColumnName>* out_to_group_col);

}  // namespace tinylamb::select_rewrite_detail
