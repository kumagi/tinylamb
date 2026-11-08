/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <bit>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/string_util.hpp"
#include "executor/detail/scan_filter.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "plan/cascades.hpp"
#include "query/query_data.hpp"
#include "type/collation.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::cascades {

bool LogicalProperties::AreEqual(const std::string& col_a,
                                 const std::string& col_b) const {
  if (col_a == col_b) {
    return true;
  }
  auto match_names = [](std::string_view a, std::string_view b) {
    if (a == b) {
      return true;
    }
    auto dot_a = a.find('.');
    auto dot_b = b.find('.');
    if (dot_a == std::string_view::npos && dot_b != std::string_view::npos) {
      return a == b.substr(dot_b + 1);
    }
    if (dot_b == std::string_view::npos && dot_a != std::string_view::npos) {
      return b == a.substr(dot_a + 1);
    }
    return false;
  };
  if (match_names(col_a, col_b)) {
    return true;
  }
  for (const auto& ec : equivalence_classes) {
    bool has_a = false;
    bool has_b = false;
    for (const auto& c : ec) {
      if (match_names(c, col_a)) {
        has_a = true;
      }
      if (match_names(c, col_b)) {
        has_b = true;
      }
    }
    if (has_a && has_b) {
      return true;
    }
  }
  return false;
}

bool LogicalProperties::IsNotNull(const std::string& column) const {
  auto match_names = [](std::string_view a, std::string_view b) {
    if (a == b) {
      return true;
    }
    auto dot_a = a.find('.');
    auto dot_b = b.find('.');
    if (dot_a == std::string_view::npos && dot_b != std::string_view::npos) {
      return a == b.substr(dot_b + 1);
    }
    if (dot_b == std::string_view::npos && dot_a != std::string_view::npos) {
      return b == a.substr(dot_a + 1);
    }
    return false;
  };
  for (const auto& nn : not_null_columns) {
    if (match_names(nn, column)) {
      return true;
    }
  }
  for (const auto& ec : equivalence_classes) {
    bool has_col = false;
    bool has_nn = false;
    for (const auto& c : ec) {
      if (match_names(c, column)) {
        has_col = true;
      }
      for (const auto& nn : not_null_columns) {
        if (match_names(c, nn)) {
          has_nn = true;
        }
      }
    }
    if (has_col && has_nn) {
      return true;
    }
  }
  return false;
}

bool LogicalProperties::HasKey(const std::string& column) const {
  return IsUniqueOn({column});
}

bool LogicalProperties::IsUniqueOn(
    const std::unordered_set<std::string>& columns) const {
  if (max_1_row) {
    return true;
  }
  if (columns.empty()) {
    return false;
  }
  for (const auto& cand_key : candidate_keys) {
    if (cand_key.empty()) {
      continue;
    }
    bool key_satisfied = true;
    for (const auto& k : cand_key) {
      bool found = false;
      for (const auto& col : columns) {
        if (AreEqual(k, col)) {
          found = true;
          break;
        }
      }
      if (!found) {
        key_satisfied = false;
        break;
      }
    }
    if (key_satisfied) {
      return true;
    }
  }
  return false;
}

void Memo::RefreshLogicalProperties() {
  for (Group& group : groups_) {
    group.logical_properties_expression_count.reset();
    group.logical_properties_stale = true;
  }
  std::vector<uint8_t> state(groups_.size(), 0);
  std::function<void(GroupId)> refresh = [&](GroupId group) {
    if (group >= groups_.size() || state[group] != 0) {
      return;
    }
    state[group] = 1;
    for (const LogicalExpression& expression : groups_[group].expressions) {
      for (const GroupId child : expression.children) {
        refresh(child);
      }
    }
    DeriveLogicalProperties(group);
    state[group] = 2;
  };
  for (size_t group = 0; group < groups_.size(); ++group) {
    refresh(group);
  }
}

void Memo::InvalidateLogicalPropertyDependents(GroupId group) {
  if (group >= groups_.size()) {
    return;
  }
  for (const GroupId dependent : groups_[group].logical_property_dependents) {
    if (dependent >= groups_.size() ||
        groups_[dependent].logical_properties_stale) {
      continue;
    }
    groups_[dependent].logical_properties_stale = true;
    InvalidateLogicalPropertyDependents(dependent);
  }
}

namespace {

// Appends `ec` to `props.equivalence_classes` unless a class with the same
// member set was already pushed through `seen_keys`. Join groups derive
// properties once per expression, and every join expression re-reports its
// children's classes plus its own equalities, so the same pairs arrive once
// per alternative; without dedup a large join's root group accumulates the
// pair set once per expression and every class scan pays for the duplicates.
void PushEquivalenceClass(LogicalProperties& props,
                          std::unordered_set<std::string>* seen_keys,
                          std::unordered_set<std::string> ec) {
  if (ec.size() < 2) {
    return;
  }
  std::vector<std::string> members(ec.begin(), ec.end());
  std::ranges::sort(members);
  std::string key;
  for (const std::string& member : members) {
    key += member;
    key += '\x1f';
  }
  if (!seen_keys->insert(std::move(key)).second) {
    return;
  }
  props.equivalence_classes.push_back(std::move(ec));
}

}  // namespace

void Memo::DeriveLogicalProperties(GroupId group_id) {
  if (group_id >= groups_.size()) {
    return;
  }
  Group& group = groups_[group_id];
  LogicalProperties& props = group.logical_properties;
  const bool uninitialized =
      !group.logical_properties_expression_count.has_value();
  const bool shrank =
      group.logical_properties_expression_count &&
      *group.logical_properties_expression_count > group.expressions.size();
  const bool rebuild =
      uninitialized || shrank || group.logical_properties_stale;
  if (!rebuild &&
      group.logical_properties_expression_count == group.expressions.size()) {
    return;
  }
  if (rebuild) {
    props = LogicalProperties{};
  }
  std::unordered_set<std::string> seen_ec_keys;
  if (!rebuild) {
    for (const auto& ec : props.equivalence_classes) {
      std::vector<std::string> members(ec.begin(), ec.end());
      std::ranges::sort(members);
      std::string key;
      for (const std::string& member : members) {
        key += member;
        key += '\x1f';
      }
      seen_ec_keys.insert(std::move(key));
    }
  }
  const auto push_ec = [&](std::unordered_set<std::string> ec) {
    PushEquivalenceClass(props, &seen_ec_keys, std::move(ec));
  };

  if (group.expressions.empty()) {
    group.logical_properties_expression_count = 0;
    group.logical_properties_stale = false;
    InvalidateLogicalPropertyDependents(group_id);
    return;
  }
  const size_t first_expression =
      rebuild ? 0 : *group.logical_properties_expression_count;
  for (size_t expression_index = first_expression;
       expression_index < group.expressions.size(); ++expression_index) {
    const auto& expr = group.expressions[expression_index];
    switch (expr.operation) {
      case LogicalOperator::kScan: {
        // Catalog constraints describe the scan, not every derived group
        // whose relation set happens to contain this table. Projection,
        // aggregation and NULL extension can invalidate those guarantees.
        const auto found = table_schemas_.find(expr.table);
        const Schema* schema = expr.output_schema.ColumnCount() != 0
                                   ? &expr.output_schema
                               : found != table_schemas_.end() ? &found->second
                                                               : nullptr;
        if (schema == nullptr) {
          break;
        }
        std::unordered_set<std::string> primary_key;
        for (size_t i = 0; i < schema->ColumnCount(); ++i) {
          const Column& column = schema->GetColumn(i);
          const std::string name = column.Name().schema.empty()
                                       ? expr.table + "." + column.Name().name
                                       : column.Name().ToString();
          const auto constraint = column.GetConstraint().ctype;
          if (constraint == Constraint::kPrimaryKey) {
            primary_key.insert(name);
          }
          if (constraint == Constraint::kPrimaryKey ||
              constraint == Constraint::kNotNull) {
            props.not_null_columns.insert(name);
          }
        }
        if (!primary_key.empty()) {
          props.candidate_keys.push_back(std::move(primary_key));
        }
        break;
      }
      case LogicalOperator::kProjection: {
        if (expr.children.size() != 1 || expr.children[0] >= groups_.size()) {
          break;
        }
        const auto& child = groups_[expr.children[0]].logical_properties;
        props.max_1_row = props.max_1_row || child.max_1_row;
        std::vector<std::pair<std::string, std::string>> columns;
        for (size_t i = 0; i < expr.target_list.size(); ++i) {
          const auto& target = expr.target_list[i];
          if (!target.expression ||
              target.expression->Type() != TypeTag::kColumnValue) {
            continue;
          }
          const std::string source =
              target.expression->AsColumnValue().GetColumnName().ToString();
          const std::string output =
              i < expr.output_schema.ColumnCount()
                  ? expr.output_schema.GetColumn(i).Name().ToString()
              : target.name.empty() ? source
                                    : target.name;
          columns.emplace_back(source, output);
          if (child.IsNotNull(source)) {
            props.not_null_columns.insert(output);
          }
        }
        for (const auto& key : child.candidate_keys) {
          std::unordered_set<std::string> mapped;
          for (const auto& member : key) {
            const auto found =
                std::ranges::find_if(columns, [&](const auto& column) {
                  return child.AreEqual(member, column.first);
                });
            if (found == columns.end()) {
              mapped.clear();
              break;
            }
            mapped.insert(found->second);
          }
          if (!mapped.empty()) {
            props.candidate_keys.push_back(std::move(mapped));
          }
        }
        break;
      }
      case LogicalOperator::kAggregation: {
        std::unordered_set<std::string> group_keys;
        bool all_columns = true;
        for (const auto& g : expr.grouping_sets) {
          if (g && g->Type() == TypeTag::kColumnValue) {
            group_keys.insert(g->AsColumnValue().GetColumnName().ToString());
          } else {
            all_columns = false;
          }
        }
        // Computed keys still form many groups. Also, the plain-column
        // subset of a mixed key list is not by itself a uniqueness proof.
        if (all_columns && !group_keys.empty()) {
          props.candidate_keys.push_back(group_keys);
        } else if (expr.grouping_sets.empty()) {
          props.max_1_row = true;
        }
        break;
      }
      case LogicalOperator::kLimit: {
        if (expr.limit_count == 1) {
          props.max_1_row = true;
        }
        if (!expr.children.empty() && expr.children[0] < groups_.size()) {
          const auto& child_props =
              groups_[expr.children[0]].logical_properties;
          for (const auto& ck : child_props.candidate_keys) {
            props.candidate_keys.push_back(ck);
          }
          for (const auto& nn : child_props.not_null_columns) {
            props.not_null_columns.insert(nn);
          }
          for (const auto& ec : child_props.equivalence_classes) {
            push_ec(ec);
          }
          if (child_props.max_1_row) {
            props.max_1_row = true;
          }
        }
        break;
      }
      case LogicalOperator::kValues: {
        if (expr.values.size() <= 1) {
          props.max_1_row = true;
        }
        break;
      }
      case LogicalOperator::kSelection: {
        if (!expr.children.empty() && expr.children[0] < groups_.size()) {
          const auto& child_props =
              groups_[expr.children[0]].logical_properties;
          for (const auto& ck : child_props.candidate_keys) {
            props.candidate_keys.push_back(ck);
          }
          for (const auto& nn : child_props.not_null_columns) {
            props.not_null_columns.insert(nn);
          }
          for (const auto& ec : child_props.equivalence_classes) {
            push_ec(ec);
          }
          if (child_props.max_1_row) {
            props.max_1_row = true;
          }
        }
        if (expr.predicate && *expr.predicate) {
          for (const auto& c : SplitConjuncts(*expr.predicate)) {
            if (!c) {
              continue;
            }
            if (c->Type() == TypeTag::kBinaryExp) {
              const auto& bin = c->AsBinaryExpression();
              if (bin.Op() == BinaryOperation::kEquals &&
                  bin.Left()->Type() == TypeTag::kColumnValue &&
                  bin.Right()->Type() == TypeTag::kColumnValue) {
                push_ec(
                    {bin.Left()->AsColumnValue().GetColumnName().ToString(),
                     bin.Right()->AsColumnValue().GetColumnName().ToString()});
              }
            } else if (c->Type() == TypeTag::kUnaryExp) {
              const auto& unary = c->AsUnaryExpression();
              if (unary.Op() == UnaryOperation::kIsNotNull &&
                  unary.Child()->Type() == TypeTag::kColumnValue) {
                props.not_null_columns.insert(
                    unary.Child()->AsColumnValue().GetColumnName().ToString());
              }
            }
          }
        }
        break;
      }
      case LogicalOperator::kJoin: {
        if (expr.children.size() == 2 && expr.children[0] < groups_.size() &&
            expr.children[1] < groups_.size()) {
          const auto& left_props = groups_[expr.children[0]].logical_properties;
          const auto& right_props =
              groups_[expr.children[1]].logical_properties;
          for (const auto& ec : left_props.equivalence_classes) {
            push_ec(ec);
          }
          for (const auto& ec : right_props.equivalence_classes) {
            push_ec(ec);
          }
          for (const auto& nn : left_props.not_null_columns) {
            props.not_null_columns.insert(nn);
          }
          for (const auto& nn : right_props.not_null_columns) {
            props.not_null_columns.insert(nn);
          }
          if (left_props.max_1_row && right_props.max_1_row) {
            props.max_1_row = true;
          }

          std::unordered_set<std::string> left_join_cols, right_join_cols;
          if (expr.predicate && *expr.predicate) {
            const auto& left_rels = groups_[expr.children[0]].relations;
            const auto& right_rels = groups_[expr.children[1]].relations;
            for (const auto& c : SplitConjuncts(*expr.predicate)) {
              if (!c || c->Type() != TypeTag::kBinaryExp) {
                continue;
              }
              const auto& bin = c->AsBinaryExpression();
              if (bin.Op() != BinaryOperation::kEquals) {
                continue;
              }
              if (bin.Left()->Type() == TypeTag::kColumnValue &&
                  bin.Right()->Type() == TypeTag::kColumnValue) {
                const auto& c1 = bin.Left()->AsColumnValue().GetColumnName();
                const auto& c2 = bin.Right()->AsColumnValue().GetColumnName();
                push_ec({c1.ToString(), c2.ToString()});
                if (std::ranges::find(left_rels, c1.schema) !=
                        left_rels.end() &&
                    std::ranges::find(right_rels, c2.schema) !=
                        right_rels.end()) {
                  left_join_cols.insert(c1.ToString());
                  right_join_cols.insert(c2.ToString());
                } else if (std::ranges::find(right_rels, c1.schema) !=
                               right_rels.end() &&
                           std::ranges::find(left_rels, c2.schema) !=
                               left_rels.end()) {
                  right_join_cols.insert(c1.ToString());
                  left_join_cols.insert(c2.ToString());
                }
              }
            }
          }
          if (!right_join_cols.empty() &&
              right_props.IsUniqueOn(right_join_cols)) {
            for (const auto& ck : left_props.candidate_keys) {
              props.candidate_keys.push_back(ck);
            }
          }
          if (!left_join_cols.empty() &&
              left_props.IsUniqueOn(left_join_cols)) {
            for (const auto& ck : right_props.candidate_keys) {
              props.candidate_keys.push_back(ck);
            }
          }
        }
        break;
      }
      case LogicalOperator::kOuterJoin: {
        if (expr.children.size() == 2 && expr.children[0] < groups_.size() &&
            expr.children[1] < groups_.size()) {
          const auto& left_props = groups_[expr.children[0]].logical_properties;
          const auto& right_props =
              groups_[expr.children[1]].logical_properties;
          if (expr.join_type == 0) {  // LeftOuter
            for (const auto& nn : left_props.not_null_columns) {
              props.not_null_columns.insert(nn);
            }
            std::unordered_set<std::string> right_join_cols;
            if (expr.predicate && *expr.predicate) {
              const auto& right_rels = groups_[expr.children[1]].relations;
              for (const auto& c : SplitConjuncts(*expr.predicate)) {
                if (!c || c->Type() != TypeTag::kBinaryExp) {
                  continue;
                }
                const auto& bin = c->AsBinaryExpression();
                if (bin.Op() != BinaryOperation::kEquals) {
                  continue;
                }
                if (bin.Left()->Type() == TypeTag::kColumnValue) {
                  const auto& col = bin.Left()->AsColumnValue().GetColumnName();
                  if (std::ranges::find(right_rels, col.schema) !=
                      right_rels.end()) {
                    right_join_cols.insert(col.ToString());
                  }
                }
                if (bin.Right()->Type() == TypeTag::kColumnValue) {
                  const auto& col =
                      bin.Right()->AsColumnValue().GetColumnName();
                  if (std::ranges::find(right_rels, col.schema) !=
                      right_rels.end()) {
                    right_join_cols.insert(col.ToString());
                  }
                }
              }
            }
            if (!right_join_cols.empty() &&
                right_props.IsUniqueOn(right_join_cols)) {
              for (const auto& ck : left_props.candidate_keys) {
                props.candidate_keys.push_back(ck);
              }
            }
          } else if (expr.join_type == 1) {  // RightOuter
            for (const auto& nn : right_props.not_null_columns) {
              props.not_null_columns.insert(nn);
            }
          }
        }
        break;
      }
      case LogicalOperator::kSemiJoin:
      case LogicalOperator::kAntiJoin: {
        if (!expr.children.empty() && expr.children[0] < groups_.size()) {
          const auto& left_props = groups_[expr.children[0]].logical_properties;
          for (const auto& ck : left_props.candidate_keys) {
            props.candidate_keys.push_back(ck);
          }
          for (const auto& nn : left_props.not_null_columns) {
            props.not_null_columns.insert(nn);
          }
          for (const auto& ec : left_props.equivalence_classes) {
            push_ec(ec);
          }
          if (left_props.max_1_row) {
            props.max_1_row = true;
          }
        }
        break;
      }
      case LogicalOperator::kDistinct: {
        if (!expr.children.empty() && expr.children[0] < groups_.size()) {
          const auto& child_props =
              groups_[expr.children[0]].logical_properties;
          for (const auto& ck : child_props.candidate_keys) {
            props.candidate_keys.push_back(ck);
          }
          for (const auto& nn : child_props.not_null_columns) {
            props.not_null_columns.insert(nn);
          }
          for (const auto& ec : child_props.equivalence_classes) {
            push_ec(ec);
          }
          if (child_props.max_1_row) {
            props.max_1_row = true;
          }
        }
        break;
      }
      default:
        break;
    }
  }
  group.logical_properties_expression_count = group.expressions.size();
  group.logical_properties_stale = false;
  InvalidateLogicalPropertyDependents(group_id);
}

Pattern Pattern::Any(std::string capture) {
  Pattern pattern;
  pattern.capture_ = std::move(capture);
  return pattern;
}

Pattern Pattern::Op(LogicalOperator operation, std::vector<Pattern> children,
                    std::string capture) {
  Pattern pattern;
  pattern.operation_ = operation;
  pattern.children_ = std::move(children);
  pattern.capture_ = std::move(capture);
  return pattern;
}

Pattern Pattern::Op(LogicalOperator operation, std::vector<Pattern> children,
                    std::string capture, PayloadConstraint payload) {
  Pattern pattern = Op(operation, std::move(children), std::move(capture));
  pattern.payload_ = payload;
  return pattern;
}

bool Pattern::MatchGroup(
    const Memo& memo,
    GroupId group,  // NOLINT(misc-no-recursion) // Cascades pattern matching
                    // recurses over the expression tree by design; depth is
                    // bounded by the memo built from a finite query.
    Bindings* bindings) const {
  if (!capture_.empty()) {
    auto [iter, inserted] = bindings->emplace(capture_, group);
    if (!inserted && iter->second != group) {
      return false;
    }
  }
  if (!operation_) {
    return true;
  }
  return std::ranges::any_of(
      memo.Get(group).expressions,
      [&](const LogicalExpression&
              expression) {  // NOLINT(misc-no-recursion) // Part of
                             // Pattern::MatchGroup recursion; bounded by the
                             // finite memo.
        Bindings local = *bindings;
        if (Match(memo, group, expression, &local)) {
          *bindings = std::move(local);
          return true;
        }
        return false;
      });
}

bool Pattern::MatchPayload(const Memo& memo,
                           const LogicalExpression& expression,
                           [[maybe_unused]] const Bindings& bindings) const {
  if (payload_.requires_predicate && !expression.predicate) {
    return false;
  }
  if (payload_.outer_join_type.has_value()) {
    if (expression.operation != LogicalOperator::kOuterJoin ||
        expression.join_type != *payload_.outer_join_type) {
      return false;
    }
  }
  if (!payload_.predicate_within_child) {
    return true;
  }
  const size_t child_index = *payload_.predicate_within_child;
  if (!expression.predicate || child_index >= expression.children.size()) {
    return false;
  }
  const Group& child = memo.Get(expression.children[child_index]);
  return std::ranges::all_of((*expression.predicate)->TouchedColumns(),
                             [&child](const ColumnName& column) {
                               // Unqualified names cannot be proven to belong
                               // to the child, so they fail the constraint
                               // (strict interpretation).
                               return !column.schema.empty() &&
                                      std::ranges::find(child.relations,
                                                        column.schema) !=
                                          child.relations.end();
                             });
}

bool Pattern::Match(
    const Memo& memo,
    GroupId group,  // NOLINT(misc-no-recursion) // Cascades pattern matching
                    // recurses over the expression tree by design; depth is
                    // bounded by the memo built from a finite query.
    const LogicalExpression& expression, Bindings* bindings) const {
  if (operation_ && expression.operation != *operation_) {
    return false;
  }
  if (!children_.empty() && children_.size() != expression.children.size()) {
    return false;
  }
  Bindings local = *bindings;
  if (!capture_.empty()) {
    auto [iter, inserted] = local.emplace(capture_, group);
    if (!inserted && iter->second != group) {
      return false;
    }
  }
  for (size_t i = 0; i < children_.size(); ++i) {
    if (!children_[i].MatchGroup(memo, expression.children[i], &local)) {
      return false;
    }
  }
  if (!MatchPayload(memo, expression, local)) {
    return false;
  }
  *bindings = std::move(local);
  return true;
}

bool Rule::Apply(Memo& memo, GroupId group,
                 const LogicalExpression& expression) const {
  if (!MayApply(expression.operation)) {
    return false;
  }
  Bindings bindings;
  if (!pattern_.Match(memo, group, expression, &bindings)) {
    return false;
  }
  const size_t before_groups = memo.GroupCount();
  const size_t before_expressions = memo.ExpressionCount(group);
  const size_t before_revision = memo.Revision();
  transform_(bindings, memo, group, expression);
  return memo.GroupCount() != before_groups ||
         memo.ExpressionCount(group) != before_expressions ||
         memo.Revision() != before_revision;
}

RuleSet& RuleSet::Add(Rule rule) {
  Remove(rule.Name());
  rules_.push_back(std::move(rule));
  return *this;
}

bool RuleSet::Remove(std::string_view name) {
  const size_t old_size = rules_.size();
  std::erase_if(rules_, [&](const Rule& rule) { return rule.Name() == name; });
  return old_size != rules_.size();
}

bool RuleSet::Contains(std::string_view name) const {
  return std::ranges::any_of(
      rules_, [&](const Rule& rule) { return rule.Name() == name; });
}

std::vector<std::string> RuleSet::Names() const {
  std::vector<std::string> names;
  names.reserve(rules_.size());
  for (const Rule& rule : rules_) {
    names.push_back(rule.Name());
  }
  return names;
}

std::string PhysicalProperties::Key() const {
  std::string key = require_row_position ? "rowpos:1" : "rowpos:0";
  key.append(wait_for_write_intent ? "|wait:1" : "|wait:0");
  for (size_t i = 0; i < ordering.size(); ++i) {
    key.push_back('|');
    key.append(ordering[i].ToString());
    key.push_back(':');
    key.push_back(i < sort_ascending.size() && sort_ascending[i] ? 'a' : 'd');
    key.push_back(i < sort_nulls_first.size() ? ':' : '=');
    const std::optional<bool>& nulls_first =
        i < sort_nulls_first.size() ? sort_nulls_first[i] : std::nullopt;
    if (nulls_first.has_value()) {
      key.push_back(*nulls_first ? 'f' : 'l');
    } else {
      key.push_back('?');
    }
  }
  if (!partition_by.empty()) {
    key.append("|p:");
    for (size_t i = 0; i < partition_by.size(); ++i) {
      if (i > 0) {
        key.push_back(',');
      }
      key.append(partition_by[i].ToString());
    }
  }
  if (!collation.empty()) {
    key.append("|col:");
    key.append(collation);
  }
  if (!bloom_filter_keys.empty()) {
    key.append("|bf:");
    for (size_t i = 0; i < bloom_filter_keys.size(); ++i) {
      if (i > 0) {
        key.push_back(',');
      }
      key.append(bloom_filter_keys[i].ToString());
    }
  }
  if (join_multiplicity != JoinMultiplicity::kUnknown) {
    key.append("|mult:");
    key.append(std::to_string(static_cast<int>(join_multiplicity)));
  }
  if (is_unique) {
    key.append("|uniq:1");
  }
  key.append("|lim:");
  key.append(std::to_string(limit_hint));
  key.append("|am:");
  key.append(std::to_string(static_cast<int>(access_method)));
  key.append("|dist:");
  key.append(std::to_string(static_cast<int>(distribution)));
  key.append("|distinct:");
  key.push_back(distinct ? '1' : '0');
  return key;
}

double EstimateEquijoinSelectivity(
    const std::vector<double>& key_selectivities) {
  if (key_selectivities.empty()) {
    return 1.0;
  }
  std::vector<double> ordered = key_selectivities;
  std::sort(ordered.begin(), ordered.end());
  double selectivity = 1.0;
  double weight = 1.0;
  for (double key_selectivity : ordered) {
    key_selectivity = std::clamp(key_selectivity, 0.0, 1.0);
    selectivity *= std::pow(key_selectivity, weight);
    weight *= 0.5;
  }
  return std::clamp(selectivity, 0.0, 1.0);
}

double EstimateMemorySpillCost(OperatorCostKind kind, double input_rows,
                               const MemoryBudget& budget) {
  const double data_bytes = input_rows * budget.row_size_bytes;
  if (data_bytes <= budget.max_memory_bytes) {
    return 0.0;
  }
  const double excess_ratio = data_bytes / budget.max_memory_bytes;
  double multiplier = budget.io_spill_cost_multiplier;
  if (kind == OperatorCostKind::kSort) {
    multiplier *= std::log2(std::max(2.0, excess_ratio));
  } else if (kind == OperatorCostKind::kHashJoin) {
    multiplier *= 1.5;
  }
  return input_rows * (excess_ratio - 1.0) * multiplier;
}

const RuleContext& RuleContext::Empty() {
  static const RuleContext empty;
  return empty;
}

std::vector<PlanAlternative> ImplementationRule::Apply(
    const Memo& memo, GroupId group, const LogicalExpression& expression,
    const std::vector<BestPlan>& children, const PhysicalProperties& properties,
    const RuleContext& context) const {
  if (!MayApply(expression.operation)) {
    return {};
  }
  Bindings bindings;
  if (!pattern_.Match(memo, group, expression, &bindings)) {
    return {};
  }
  return implement_(group, memo, bindings, expression, children, properties,
                    context);
}

ImplementationRuleSet& ImplementationRuleSet::Add(ImplementationRule rule) {
  Remove(rule.Name());
  rules_.push_back(std::move(rule));
  return *this;
}

bool ImplementationRuleSet::Remove(std::string_view name) {
  const size_t old_size = rules_.size();
  std::erase_if(rules_, [&](const ImplementationRule& rule) {
    return rule.Name() == name;
  });
  return old_size != rules_.size();
}

bool ImplementationRuleSet::Contains(std::string_view name) const {
  return std::ranges::any_of(rules_, [&](const ImplementationRule& rule) {
    return rule.Name() == name;
  });
}

constexpr size_t kMaxExploreRuleSteps = 50000;

namespace {
// Temporary plan-time profiling helper (TINYLAMB_PLAN_PROFILE=1).
double PlanProfileMillisSince(std::chrono::steady_clock::time_point begin) {
  return std::chrono::duration<double, std::milli>(
             std::chrono::steady_clock::now() - begin)
      .count();
}
}  // namespace

namespace {

std::string FormatPhysicalProperties(const PhysicalProperties& properties) {
  std::ostringstream out;
  out << "row_position=" << (properties.require_row_position ? "true" : "false")
      << ", wait_for_write_intent="
      << (properties.wait_for_write_intent ? "true" : "false")
      << ", ordering=[";
  for (size_t i = 0; i < properties.ordering.size(); ++i) {
    if (i > 0) {
      out << ',';
    }
    out << properties.ordering[i].ToString();
    if (i < properties.sort_ascending.size()) {
      out << (properties.sort_ascending[i] ? " ASC" : " DESC");
    }
    const std::optional<bool>& nulls_first =
        i < properties.sort_nulls_first.size() ? properties.sort_nulls_first[i]
                                               : std::nullopt;
    if (nulls_first.has_value()) {
      out << (*nulls_first ? " NULLS FIRST" : " NULLS LAST");
    }
  }
  out << "]"
      << ", limit_hint="
      << (properties.limit_hint == std::numeric_limits<size_t>::max()
              ? "unlimited"
              : std::to_string(properties.limit_hint))
      << ", access_method=" << static_cast<int>(properties.access_method)
      << ", distribution=" << static_cast<int>(properties.distribution)
      << ", distinct=" << (properties.distinct ? "true" : "false")
      << ", unique=" << (properties.is_unique ? "true" : "false");
  if (!properties.collation.empty()) {
    out << ", collation=" << properties.collation;
  }
  if (properties.join_multiplicity != JoinMultiplicity::kUnknown) {
    out << ", join_multiplicity="
        << static_cast<int>(properties.join_multiplicity);
  }
  if (!properties.partition_by.empty()) {
    out << ", partition_by=[";
    for (size_t i = 0; i < properties.partition_by.size(); ++i) {
      if (i > 0) {
        out << ',';
      }
      out << properties.partition_by[i].ToString();
    }
    out << ']';
  }
  if (!properties.bloom_filter_keys.empty()) {
    out << ", bloom_filter_keys=[";
    for (size_t i = 0; i < properties.bloom_filter_keys.size(); ++i) {
      if (i > 0) {
        out << ',';
      }
      out << properties.bloom_filter_keys[i].ToString();
    }
    out << ']';
  }
  return out.str();
}

}  // namespace

namespace {

std::string PhysicalOperatorName(const Plan& plan) {
  if (plan == nullptr) {
    return "unknown";
  }
  const std::string text = plan->ToString();
  size_t begin = text.find_first_not_of(" \t\r\n");
  if (begin == std::string::npos) {
    return "unknown";
  }
  const size_t colon = text.find(':', begin);
  if (colon == std::string::npos) {
    const size_t end = text.find_first_of(" \t\r\n", begin);
    return text.substr(begin, end == std::string::npos ? end : end - begin);
  }
  return text.substr(begin, colon - begin);
}

}  // namespace

bool SatisfiesOrderingCollation(const Plan& plan,
                                const PhysicalProperties& required) {
  if (plan == nullptr) {
    return false;
  }
  // The requirement names the collation its consumer compares under; an empty
  // name is the engine default, which is the same byte order an explicit
  // 'binary' asks for. The two orders agree exactly when they agree on whether
  // they fold: byte order against byte order, or case-insensitive against
  // case-insensitive. A case-insensitive order is a *different* order and may
  // not stand in for a byte order (or the other way round), whatever produced
  // it.
  const bool plan_folds = plan->OrderCollation() == Collation::kCaseInsensitive;
  const bool required_folds =
      CollationFromName(required.collation) == Collation::kCaseInsensitive;
  return plan_folds == required_folds;
}

namespace {

// The query's own ORDER BY contract (expressions, direction, NULL placement)
// must be delivered verbatim before a plan may keep the required ordering.
bool IsOrderedByQueryContract(const Plan& plan, const QueryData& query) {
  if (plan == nullptr) {
    return false;
  }
  const bool order_contract_complete =
      query.order_expressions_.size() == query.order_ascending_.size() &&
      (query.order_nulls_first_.empty() ||
       query.order_nulls_first_.size() == query.order_expressions_.size());
  return order_contract_complete &&
         plan->IsOrderedBy(query.order_expressions_, query.order_ascending_,
                           query.order_nulls_first_);
}

PhysicalProperties InferProvidedProperties(const Plan& plan,
                                           const PhysicalProperties& required,
                                           const RuleContext& context) {
  PhysicalProperties provided;
  provided.require_row_position = required.require_row_position;
  provided.wait_for_write_intent = required.wait_for_write_intent;
  provided.access_method = required.access_method;
  provided.distribution = required.distribution;
  provided.limit_hint = required.limit_hint;
  provided.distinct = plan != nullptr && plan->EnforcesDistinct();
  provided.ordering = required.ordering;
  provided.sort_ascending = required.sort_ascending;
  provided.sort_nulls_first = required.sort_nulls_first;
  // The ordering is only ever provided under the collation it was requested
  // in, and a plan that produced its order under a different collation drops
  // the claim entirely (the property key must not advertise an order the
  // consumer cannot use).
  provided.collation = required.collation;
  if (!provided.ordering.empty()) {
    if (!SatisfiesOrderingCollation(plan, required) ||
        (context.query != nullptr &&
         !IsOrderedByQueryContract(plan, *context.query))) {
      provided.ordering.clear();
      provided.sort_ascending.clear();
      provided.sort_nulls_first.clear();
      provided.collation.clear();
    }
  }
  return provided;
}

}  // namespace

void AttachPlanExplainInfo(PlanAlternative& alternative,
                           const PhysicalProperties& required,
                           const RuleContext& context) {
  if (alternative.plan == nullptr) {
    return;
  }
  alternative.required_properties = required;
  alternative.provided_properties =
      InferProvidedProperties(alternative.plan, required, context);
  PlanExplainInfo info;
  info.rule_name =
      alternative.rule_name.empty() ? "implementation" : alternative.rule_name;
  info.physical_operator = PhysicalOperatorName(alternative.plan);
  info.required_properties =
      FormatPhysicalProperties(alternative.required_properties);
  info.provided_properties =
      FormatPhysicalProperties(alternative.provided_properties);
  alternative.plan->SetExplainInfo(std::move(info));
}

void SearchEngine::Explore(GroupId root) {
  best_.clear();
  optimizing_.clear();
  next_expression_.clear();
  applied_rules_.clear();
  steps_ = 0;
  budget_exhausted_ = false;
  explore_limit_reached_ = false;
  std::deque<GroupId> queue;
  std::unordered_set<GroupId> queued;
  const std::function<void(GroupId)> enqueue = [&](GroupId group) {
    if (group == kInvalidGroup) {
      return;
    }
    if (queued.insert(group).second) {
      queue.push_back(group);
    }
  };
  // Explore descendants first, then reconsider parents when descendants gain
  // alternatives. A cursor alone misses patterns enabled after the parent ran.
  std::unordered_set<GroupId> visited;
  const std::function<void(GroupId)> seed = [&](GroupId group) {
    if (!visited.insert(group).second) {
      return;
    }
    for (const auto& expression : memo_.Get(group).expressions) {
      for (GroupId child : expression.children) {
        seed(child);
      }
    }
    enqueue(group);
  };
  seed(root);
  (void)memo_.DrainTouchedGroups();
  size_t steps = 0;
  rule_stats_ = std::getenv("TINYLAMB_RULE_STATS") != nullptr;
  rule_attempt_counts_.clear();
  rule_attempt_nanos_.clear();
  constexpr size_t kMaxExploreSteps = 20000;
  while (!queue.empty() && steps < kMaxExploreSteps && !budget_exhausted_ &&
         !explore_limit_reached_) {
    ++steps;
    const GroupId group = queue.front();
    queue.pop_front();
    queued.erase(group);
    ExploreGroup(group, enqueue);
    for (GroupId touched : memo_.DrainTouchedGroups()) {
      enqueue(touched);
      for (GroupId parent : memo_.Get(touched).logical_property_dependents) {
        if (next_expression_[parent] != 0) {
          next_expression_[parent] = 0;
          enqueue(parent);
        }
      }
    }
  }
  explore_limit_reached_ =
      explore_limit_reached_ || (!budget_exhausted_ && !queue.empty());
  if (std::getenv("TINYLAMB_DEBUG_DUMP_MEMO") != nullptr) {
    memo_.Dump(std::cerr);
  }
  if (rule_stats_) {
    std::ostringstream stats;
    stats << "rule stats (steps=" << steps_ << ", pops=" << steps << "):\n";
    std::vector<std::pair<std::string, std::pair<size_t, uint64_t>>> entries;
    entries.reserve(rule_attempt_counts_.size());
    for (const auto& [name, count] : rule_attempt_counts_) {
      entries.emplace_back(name,
                           std::make_pair(count, rule_attempt_nanos_[name]));
    }
    std::ranges::sort(entries, [](const auto& a, const auto& b) {
      return a.second.second > b.second.second;
    });
    for (const auto& [name, timing] : entries) {
      stats << "  " << name << ' ' << timing.first << " attempts, "
            << static_cast<double>(timing.second) / 1e6 << "ms\n";
    }
    LOG(INFO) << stats.str();
  }
}

// Phase 7 worklist exploration: each group keeps a cursor over its
// expressions; rules appending expressions (here or in other groups) extend
// the work instead of rescanning settled state. No pass cap or convergence
// exception is needed because the memo is append-only and fingerprints
// deduplicate.
void SearchEngine::ExploreGroup(GroupId group,
                                const std::function<void(GroupId)>& enqueue) {
  size_t& next = next_expression_[group];
  while (next < memo_.Get(group).expressions.size()) {
    const LogicalExpression expression = memo_.Get(group).expressions[next];
    ++next;
    for (const Rule& rule : rules_->Rules()) {
      if (!rule.MayApply(expression.operation)) {
        continue;
      }
      if (step_budget_ != 0 && steps_ >= step_budget_) {
        budget_exhausted_ = true;
        return;
      }
      if (steps_ >= kMaxExploreRuleSteps) {
        explore_limit_reached_ = true;
        return;
      }
      ++steps_;
      if (rule_stats_) {
        ++rule_attempt_counts_[rule.Name()];
      }
      const auto rule_begin = rule_stats_
                                  ? std::chrono::steady_clock::now()
                                  : std::chrono::steady_clock::time_point{};
      // Transformation rules may legitimately decline; the return value is
      // only advisory for callers that track memo growth.
      // PRODUCTION FIX: a buggy rule used to throw std::invalid_argument
      // straight out of Explore, failing the whole query instead of just
      // skipping one bad alternative. Contain the failure per rule.
      try {
        if (rule.Apply(memo_, group, expression)) {
          applied_rules_.insert(rule.Name());
        }
      } catch (const std::invalid_argument&) {
        // Memo invariant violation from a transformation rule: keep the
        // alternative absent and continue exploring the remaining rules.
        continue;
      }
      if (rule_stats_) {
        rule_attempt_nanos_[rule.Name()] += static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - rule_begin)
                .count());
      }
    }
    for (GroupId child : expression.children) {
      enqueue(child);
    }
  }
}

std::optional<BestPlan> SearchEngine::Optimize(
    GroupId root, const PhysicalProperties& properties,
    const Implement& implement, const RuleContext& context) {
  const bool profile = std::getenv("TINYLAMB_PLAN_PROFILE") != nullptr;
  const auto explore_begin = std::chrono::steady_clock::now();
  Explore(root);
  const auto explore_ms = PlanProfileMillisSince(explore_begin);
  const auto lower_begin = std::chrono::steady_clock::now();
  std::optional<BestPlan> result =
      OptimizeGroup(root, properties, implement, context);
  if (profile) {
    LOG(INFO) << "plan-profile: explore=" << explore_ms
              << "ms lower=" << PlanProfileMillisSince(lower_begin)
              << "ms groups=" << memo_.GroupCount();
  }
  return result;
}

std::optional<BestPlan> SearchEngine::Optimize(
    GroupId root, const PhysicalProperties& properties,
    const ImplementationRuleSet& implementation_rules,
    const RuleContext& context) {
  return Optimize(
      root, properties,
      [&](GroupId group, const Memo&, const LogicalExpression& expression,
          const std::vector<BestPlan>& children,
          const PhysicalProperties& required, const RuleContext& ctx) {
        std::vector<PlanAlternative> alternatives;
        for (const ImplementationRule& rule : implementation_rules.Rules()) {
          if (!rule.MayApply(expression.operation)) {
            continue;
          }
          std::vector<PlanAlternative> generated =
              rule.Apply(memo_, group, expression, children, required, ctx);
          for (PlanAlternative& alternative : generated) {
            alternative.rule_name = rule.Name();
            AttachPlanExplainInfo(alternative, required, ctx);
          }
          alternatives.insert(alternatives.end(),
                              std::make_move_iterator(generated.begin()),
                              std::make_move_iterator(generated.end()));
        }
        return alternatives;
      },
      context);
}

std::optional<BestPlan> SearchEngine::Optimize(
    GroupId root, const PhysicalProperties& properties,
    const ImplementationRuleSet& implementation_rules) {
  return Optimize(root, properties, implementation_rules, RuleContext::Empty());
}

std::vector<PhysicalProperties> SearchEngine::RequiredChildProperties(
    const LogicalExpression& expression, const PhysicalProperties& required) {
  switch (expression.operation) {
    case LogicalOperator::kScan:
    case LogicalOperator::kValues:
    case LogicalOperator::kDummyScan:
    case LogicalOperator::kRelational:
      return {};
    case LogicalOperator::kJoin:
    case LogicalOperator::kOuterJoin:
    case LogicalOperator::kCrossJoin:
    case LogicalOperator::kSemiJoin:
    case LogicalOperator::kAntiJoin:
    case LogicalOperator::kApply:
      if (expression.children.size() == 1) {
        return {required};
      }
      return {PhysicalProperties{}, PhysicalProperties{}};
    case LogicalOperator::kRecursiveCte:
      if (expression.children.empty()) {
        return {};
      }
      if (expression.children.size() == 1) {
        return {required};
      }
      return {PhysicalProperties{}, PhysicalProperties{}};
    case LogicalOperator::kAggregation:
      // Aggregation collapses the input; ordering and row positions die here.
      return {PhysicalProperties{}};
    case LogicalOperator::kSelection:
    case LogicalOperator::kLimit:
    case LogicalOperator::kDistinct:
    case LogicalOperator::kEmpty:
    case LogicalOperator::kWindow:
    case LogicalOperator::kUnnest:
      // Row-filtering and row-shaping operators pass rows through in order.
      return {required};
    case LogicalOperator::kSample: {
      PhysicalProperties child = required;
      if (expression.sample_method == TableSampleMethod::kReservoir) {
        child.limit_hint = std::numeric_limits<size_t>::max();
      }
      return {child};
    }
    case LogicalOperator::kProjection: {
      // A projection renames columns: the parent's ordering is expressed in
      // output-name space, and pushing it through unchanged asks the child
      // for columns it does not expose (e.g. sorting below a projection by a
      // SELECT-list alias).  Translate each key to the projected input
      // column it derives from; a key that does not project from a simple
      // input column makes the ordering undeliverable below, so drop it and
      // let the parent enforce it above the projection.
      PhysicalProperties child = required;
      if (!child.ordering.empty()) {
        std::vector<ColumnName> translated;
        translated.reserve(child.ordering.size());
        for (const ColumnName& column : child.ordering) {
          bool mapped = false;
          for (const NamedExpression& output : expression.target_list) {
            if (!output.expression ||
                output.expression->Type() != TypeTag::kColumnValue) {
              continue;
            }
            const ColumnName& source =
                output.expression->AsColumnValue().GetColumnName();
            const bool name_matches =
                IdentifierEquals(output.name, column.name) ||
                IdentifierEquals(source.name, column.name);
            const bool qualified_matches =
                !column.schema.empty() && source == column;
            if (name_matches || qualified_matches) {
              translated.push_back(source);
              mapped = true;
              break;
            }
          }
          if (!mapped) {
            translated.clear();
            child.ordering.clear();
            break;
          }
        }
        if (!translated.empty()) {
          child.ordering = std::move(translated);
        }
      }
      return {child};
    }
    case LogicalOperator::kUnion:
    case LogicalOperator::kUnionAll:
    case LogicalOperator::kIntersect:
    case LogicalOperator::kIntersectAll:
    case LogicalOperator::kExcept:
    case LogicalOperator::kExceptAll:
      // Set operations do not preserve child row positions or a global
      // ordering, so no parent physical property can be pushed into them.
      return std::vector<PhysicalProperties>(expression.children.size(),
                                             PhysicalProperties{});
    case LogicalOperator::kSort: {
      // Sorting establishes the parent's ordering itself. The child still
      // has to provide row positions when requested, but an ordering or
      // Top-K hint below the sort is not semantically required.
      PhysicalProperties child = required;
      child.ordering.clear();
      child.limit_hint = std::numeric_limits<size_t>::max();
      return {child};
    }
    case LogicalOperator::kTopN: {
      // TopN establishes the parent ordering itself, but its own keys are
      // offered to the child as an optional ordering: a scan that delivers
      // them natively wins on cost and the implementation rule then elides
      // the engine-side heap.  When the ordering is delivered the child never
      // needs more than OFFSET + LIMIT rows.
      PhysicalProperties child;
      child.require_row_position = required.require_row_position;
      child.wait_for_write_intent = required.wait_for_write_intent;
      child.access_method = required.access_method;
      if (!expression.target_list.empty() &&
          expression.target_list.size() == expression.sort_ascending.size()) {
        bool keys_are_columns = true;
        for (const NamedExpression& key : expression.target_list) {
          if (key.expression &&
              key.expression->Type() == TypeTag::kColumnValue) {
            child.ordering.push_back(
                key.expression->AsColumnValue().GetColumnName());
            continue;
          }
          keys_are_columns = false;
          break;
        }
        if (keys_are_columns) {
          child.sort_ascending = expression.sort_ascending;
          child.sort_nulls_first = expression.sort_nulls_first;
        } else {
          child.ordering.clear();
        }
      }
      const size_t needed =
          expression.limit_offset >
                  std::numeric_limits<size_t>::max() - expression.limit_count
              ? std::numeric_limits<size_t>::max()
              : expression.limit_count + expression.limit_offset;
      child.limit_hint = std::min(required.limit_hint, needed);
      return {child};
    }
  }
  return {};
}

std::optional<BestPlan>
SearchEngine::OptimizeGroup(  // NOLINT(misc-no-recursion) // Cascades
                              // branch-and-bound search recurses over memo
                              // groups by design; memo depth is bounded by the
                              // finite query.
    GroupId group, const PhysicalProperties& properties,
    const Implement& implement, const RuleContext& context) {
  const std::string cache_key = std::to_string(group) + '/' + properties.Key();
  if (const auto found = best_.find(cache_key); found != best_.end()) {
    return found->second;
  }

  // Equivalence rewrites can form cycles between groups. An alternative
  // needing its own unfinished winner is not a finite executable tree.
  if (!optimizing_.insert(cache_key).second) {
    return std::nullopt;
  }

  const bool needs_ordering = !properties.ordering.empty();
  std::vector<Expression> required_order;
  required_order.reserve(properties.ordering.size());
  for (const auto& column : properties.ordering) {
    required_order.push_back(ColumnValueExp(column));
  }
  const std::vector<bool> required_ascending =
      properties.sort_ascending.empty()
          ? std::vector<bool>(required_order.size(), true)
          : properties.sort_ascending;
  std::optional<BestPlan> best;
  double best_score = std::numeric_limits<double>::infinity();
  const Group& memo_group = memo_.Get(group);
  for (size_t index = 0; index < memo_group.expressions.size(); ++index) {
    const LogicalExpression& expression = memo_group.expressions[index];
    const std::vector<PhysicalProperties> child_properties =
        RequiredChildProperties(expression, properties);
    if (child_properties.size() != expression.children.size()) {
      continue;
    }
    std::vector<std::vector<PhysicalProperties>> property_sets{
        child_properties};
    if (expression.operation == LogicalOperator::kTopN &&
        child_properties.size() == 1) {
      // A mixed-direction order may have a useful leading index order even
      // when no child can deliver the entire TopN order. Keep those winners
      // available to the incremental-sort alternative.
      for (size_t prefix = child_properties[0].ordering.size(); prefix > 1;) {
        --prefix;
        auto partial = child_properties;
        partial[0].ordering.resize(prefix);
        if (!partial[0].sort_ascending.empty()) {
          partial[0].sort_ascending.resize(prefix);
        }
        if (!partial[0].sort_nulls_first.empty()) {
          partial[0].sort_nulls_first.resize(prefix);
        }
        property_sets.push_back(std::move(partial));
      }
    }
    if (expression.children.size() == 1 &&
        (expression.operation == LogicalOperator::kAggregation ||
         expression.operation == LogicalOperator::kSort)) {
      std::vector<Expression> keys;
      if (expression.operation == LogicalOperator::kAggregation) {
        keys = expression.grouping_sets.empty() ? expression.partition_by
                                                : expression.grouping_sets;
      } else {
        for (const auto& target : expression.target_list) {
          keys.push_back(target.expression);
        }
      }
      auto ordered = child_properties;
      ordered[0].ordering.clear();
      for (const auto& key : keys) {
        if (!key || key->Type() != TypeTag::kColumnValue) {
          ordered[0].ordering.clear();
          break;
        }
        ordered[0].ordering.push_back(key->AsColumnValue().GetColumnName());
      }
      if (!ordered[0].ordering.empty()) {
        ordered[0].sort_ascending =
            expression.operation == LogicalOperator::kSort
                ? expression.sort_ascending
                : std::vector<bool>(keys.size(), true);
        ordered[0].sort_nulls_first =
            expression.operation == LogicalOperator::kSort
                ? expression.sort_nulls_first
                : std::vector<std::optional<bool>>(keys.size(), true);
        if (ordered != child_properties) {
          property_sets.push_back(std::move(ordered));
        }
      }
    }
    for (const auto& candidate_properties : property_sets) {
      std::vector<BestPlan> children;
      double child_cost = 0;
      double child_rows = 0;
      bool valid = true;
      for (size_t child = 0; child < expression.children.size(); ++child) {
        std::optional<BestPlan> child_plan =
            OptimizeGroup(expression.children[child],
                          candidate_properties[child], implement, context);
        if (!child_plan) {
          valid = false;
          break;
        }
        child_cost += child_plan->cost;
        child_rows += child_plan->estimated_rows;
        children.push_back(std::move(*child_plan));
      }
      if (!valid) {
        continue;
      }
      // Empty keeps its child only for schema; execution never reads it.
      if (expression.operation == LogicalOperator::kEmpty) {
        child_cost = 0;
        child_rows = 0;
      }
      // Branch-and-bound: every alternative adds a non-negative local cost
      // (plus a non-negative sort penalty below), so an expression whose
      // children alone already reach the incumbent cannot improve it.
      if (best && child_cost > best_score) {
        continue;
      }

      for (PlanAlternative alternative :
           implement(group, memo_, expression, children, properties, context)) {
        if (!alternative.plan) {
          continue;
        }
        AttachPlanExplainInfo(alternative, properties, context);
        const double cost = child_cost + alternative.local_cost;
        double score = cost;
        // Property-driven ordering decision (D6): alternatives that cannot
        // deliver the required ordering carry the sort the engine would have
        // to insert; the engine-side SortExecutor remains the safety net.
        if (needs_ordering &&
            (!alternative.plan->IsOrderedBy(required_order, required_ascending,
                                            properties.sort_nulls_first) ||
             !SatisfiesOrderingCollation(alternative.plan, properties))) {
          const double rows = std::max(alternative.estimated_rows, child_rows);
          score += rows * std::log2(std::max(2.0, rows));
        }
        if (!best || score < best_score ||
            (score == best_score &&
             expression.operation == LogicalOperator::kEmpty)) {
          best_score = score;
          best = BestPlan{.plan = std::move(alternative.plan),
                          .cost = cost,
                          .estimated_rows = alternative.estimated_rows,
                          .group = group,
                          .expression_index = index};
        }
      }
    }
  }
  optimizing_.erase(cache_key);
  best_.emplace(cache_key, best);
  return best;
}

}  // namespace tinylamb::cascades
