/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <optional>
#include <ranges>
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
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "plan/cascades_rules_internal.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::cascades::rule_detail {
// constant_unnest_to_values: elements materialized as memo literals beyond
// this count keep the runtime unnest path (memo blow-up and plan-cache
// bloat risk; the executor expands them lazily anyway).
constexpr size_t kConstantUnnestValuesThreshold = 1024;

void RegisterPredicateAndJoinRules(RuleSet& built) {
  using namespace dsl;
  // push_not_through_expression: Push NOT operators through comparisons,
  // double negations, and De Morgan's laws (AND / OR) into canonical form.
  // Lean proof: lean/TinylambOpt/ThreeValued.lean (double_negation_sound,
  //   de_morgan_and_sound, de_morgan_or_sound, not_comparison_unsound_on_nan)
  built.Add(Rule(
      "push_not_through_expression", Selection(Any("input")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        (void)bindings;
        if (expression.operation != LogicalOperator::kSelection ||
            expression.children.size() != 1 || !expression.predicate ||
            !*expression.predicate) {
          return;
        }
        const auto normalize_not = [](auto& self,
                                      const Expression& expr) -> Expression {
          if (!expr) {
            return expr;
          }
          if (expr->Type() == TypeTag::kUnaryExp) {
            const auto& un = expr->AsUnaryExpression();
            if (un.Op() == UnaryOperation::kNot && un.Child()) {
              const auto& child = un.Child();
              if (child->Type() == TypeTag::kUnaryExp) {
                const auto& inner_un = child->AsUnaryExpression();
                if (inner_un.Op() == UnaryOperation::kNot && inner_un.Child()) {
                  return self(self, inner_un.Child());
                }
              } else if (child->Type() == TypeTag::kBinaryExp) {
                const auto& bin = child->AsBinaryExpression();
                const auto op = bin.Op();
                const auto& l = bin.Left();
                const auto& r = bin.Right();
                switch (op) {
                  case BinaryOperation::kEquals:
                    return BinaryExpressionExp(self(self, l),
                                               BinaryOperation::kNotEquals,
                                               self(self, r));
                  case BinaryOperation::kNotEquals:
                    return BinaryExpressionExp(
                        self(self, l), BinaryOperation::kEquals, self(self, r));
                  case BinaryOperation::kAnd:
                    return BinaryExpressionExp(
                        self(self, UnaryExpressionExp(l, UnaryOperation::kNot)),
                        BinaryOperation::kOr,
                        self(self,
                             UnaryExpressionExp(r, UnaryOperation::kNot)));
                  case BinaryOperation::kOr:
                    return BinaryExpressionExp(
                        self(self, UnaryExpressionExp(l, UnaryOperation::kNot)),
                        BinaryOperation::kAnd,
                        self(self,
                             UnaryExpressionExp(r, UnaryOperation::kNot)));
                  default:
                    break;
                }
              }
            }
          } else if (expr->Type() == TypeTag::kBinaryExp) {
            const auto& bin = expr->AsBinaryExpression();
            auto l_norm = self(self, bin.Left());
            auto r_norm = self(self, bin.Right());
            if (l_norm->ToString() != bin.Left()->ToString() ||
                r_norm->ToString() != bin.Right()->ToString()) {
              return BinaryExpressionExp(l_norm, bin.Op(), r_norm);
            }
          }
          return expr;
        };

        Expression normalized =
            normalize_not(normalize_not, *expression.predicate);
        if (normalized &&
            normalized->ToString() != (*expression.predicate)->ToString()) {
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kSelection,
                                       .children = expression.children,
                                       .predicate = normalized});
        }
      },
      LogicalOperator::kSelection));

  // no_op_window_elimination: Remove Window operator when the outer
  // expressions do not reference any window function results. Both
  // bypasses below are gated on exactly that: dropping a Window node
  // whose `$win` outputs (or raw calls) are still referenced would
  // orphan columns (or evaluate calls row-at-a-time, which the
  // evaluator rejects).
  // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
  //   (no_op_window_elimination_sound,
  //    cpp_no_op_window_star_and_qualified_bug)
  built.Add(Rule(
      "no_op_window_elimination", Pattern::Any(),
      [](const Bindings&, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (expression.operation == LogicalOperator::kWindow &&
            expression.children.size() == 1) {
          const GroupId child = expression.children[0];
          if (child != group) {
            if (expression.target_list.empty()) {
              // Same filter contract as eliminate_true_selection: the
              // copied alternatives implement against this group's filter,
              // so a scan filter living only on the child group must move
              // with them (kWindow is unary and preserves the relation
              // set, so a non-null child filter implies single-relation).
              const Group& child_group = memo.Get(child);
              if (child_group.filter && memo.Get(group).relations.size() == 1) {
                // The child's filter can be branch-local; the copied
                // alternatives must keep that mark or a later rule will
                // push it into a scan shared by another set-op branch.
                InheritBranchLocal(memo, group, child);
                memo.MergeScanFilter(group, child_group.filter);
              }
              for (const LogicalExpression& child_expr :
                   child_group.expressions) {
                bool refs_group = false;
                for (GroupId c : child_expr.children) {
                  if (c == group) {
                    refs_group = true;
                    break;
                  }
                }
                if (refs_group) {
                  continue;
                }
                memo.AddExpression(group, child_expr);
              }
            } else if (!std::ranges::any_of(expression.target_list,
                                            [](const NamedExpression& target) {
                                              return target.expression &&
                                                     TreeContainsWindowCall(
                                                         target.expression);
                                            })) {
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = {child},
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema});
            }
          }
        } else if (expression.operation == LogicalOperator::kProjection &&
                   expression.children.size() == 1) {
          const GroupId in_id = expression.children[0];
          if (in_id != group) {
            // The outer targets must not reference the window outputs
            // (whether bare `$winN`, qualified `sub.w`, or `*` wildcard),
            // nor contain raw window calls.
            const auto outer_uses_window = [](const LogicalExpression& outer,
                                              const LogicalExpression& win) {
              if (outer.target_list.empty()) {
                // Unbound target list (e.g. SELECT * before expansion):
                // it may expose the window outputs even though nothing is
                // named yet, so treat it as a reference conservatively.
                return true;
              }
              std::unordered_set<std::string> outputs;
              for (const NamedExpression& target : win.target_list) {
                if (!target.name.empty()) {
                  outputs.insert(target.name);
                }
              }
              for (const NamedExpression& target : outer.target_list) {
                if (!target.expression) {
                  continue;
                }
                if (TreeContainsWindowCall(target.expression)) {
                  return true;
                }
                for (const ColumnName& column :
                     target.expression->TouchedColumns()) {
                  if (column.name == "*" || outputs.contains(column.name)) {
                    return true;
                  }
                }
              }
              return false;
            };
            const Group& input_group = memo.Get(in_id);
            for (const auto& win : input_group.expressions) {
              if (win.operation != LogicalOperator::kWindow ||
                  win.children.size() != 1 || win.children[0] == group) {
                continue;
              }
              if (outer_uses_window(expression, win)) {
                continue;
              }
              const GroupId child = win.children[0];
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = {child},
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema});
            }
          }
        }
      },
      {LogicalOperator::kWindow, LogicalOperator::kProjection}));

  // Lean proof: lean/TinylambOpt/RollupAndJoinFilterRewrites.lean
  // (filter_transfer_preserves_matches,
  // filter_transfer_two_hop_preserves_matches,
  //  transfer_filter_through_dimension_sound)
  // dynamic_filter_pushdown_join: annotate the probe-side join keys a
  // dynamic ("shared information passing") filter can be built over
  // (Phase 2 "Dynamic Filter Join / SIP / Bloom").
  //
  // The annotation is purely logical: it names the columns, never the filter
  // shape. Which side actually builds is a cost decision, and
  // join_commutativity supplies the mirrored expression, whose children[1]
  // is this join's children[0] -- so this one child position already yields
  // BOTH build/probe orientations to the implementation rules.
  //
  // The derived group keeps the original child's selection (D5: "残余述語を
  // 保持し、フィルタ消失を許さない") and its tag fingerprints the key set, so
  // two different key sets can never collide into one derived group.
  // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
  //   (dynamic_filter_and_semi_reduction_sound,
  //    cpp_dynamic_filter_erases_derived_probe_bug)
  built.Add(Rule(
      "dynamic_filter_pushdown_join", Join(Any("build"), Any("probe")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (memo.Get(group).tag.find("bloom") != std::string::npos ||
            memo.Get(group).relations.size() != 2) {
          return;
        }
        if (!expression.predicate || !*expression.predicate) {
          return;
        }
        const GroupId probe_id = bindings.at("probe");
        if (probe_id == group) {
          return;
        }
        // Fixpoint guard: the probe already is a dynamic-filter probe
        // group (join commutativity re-presents the filtered join).
        if (memo.Get(probe_id).tag.starts_with("bloom_probe:")) {
          return;
        }
        const std::vector<std::string> probe_rels =
            memo.Get(probe_id).relations;
        if (probe_rels.size() != 1 || memo.Get(probe_id).sample_barrier ||
            !std::ranges::any_of(memo.Get(probe_id).expressions,
                                 [&](const LogicalExpression& candidate) {
                                   return candidate.operation ==
                                              LogicalOperator::kScan &&
                                          candidate.table == probe_rels.front();
                                 })) {
          return;
        }
        std::vector<ColumnName> bloom_keys;
        for (const Expression& conjunct :
             SplitConjuncts(*expression.predicate)) {
          if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
            continue;
          }
          const auto& binary = conjunct->AsBinaryExpression();
          if (binary.Op() != BinaryOperation::kEquals ||
              binary.Left()->Type() != TypeTag::kColumnValue ||
              binary.Right()->Type() != TypeTag::kColumnValue) {
            continue;
          }
          const ColumnName left =
              binary.Left()->AsColumnValue().GetColumnName();
          const ColumnName right =
              binary.Right()->AsColumnValue().GetColumnName();
          const bool left_here =
              std::ranges::find(probe_rels, left.schema) != probe_rels.end();
          const bool right_here =
              std::ranges::find(probe_rels, right.schema) != probe_rels.end();
          if (left_here == right_here) {
            continue;  // both sides here (self-join) or neither side here
          }
          const ColumnName& key = left_here ? left : right;
          if (std::ranges::find(bloom_keys, key) == bloom_keys.end()) {
            bloom_keys.push_back(key);
          }
        }
        if (bloom_keys.empty()) {
          return;
        }
        // D5 gate: the derived probe group must keep the original probe
        // group's selection ("残余述語を保持し、フィルタ消失を許さない").
        // EnsureDerivedGroup creates an unfiltered group; carrying the base
        // filter over makes the alternative predicate-equivalent to the
        // original probe child.
        const Expression probe_filter = memo.Get(probe_id).filter;
        // Length-prefix each name: a quoted identifier may itself contain
        // the separator, and two different key sets must never collapse to
        // the same derived-group tag.
        std::string key_fingerprint;
        for (const ColumnName& key : bloom_keys) {
          const std::string name = key.ToString();
          key_fingerprint += std::to_string(name.size());
          key_fingerprint.push_back(':');
          key_fingerprint += name;
        }
        const GroupId filtered_probe = memo.EnsureDerivedGroup(
            probe_rels,
            "bloom_probe:" + GroupIdsTag({probe_id}) + key_fingerprint);
        if (filtered_probe != probe_id && filtered_probe != group) {
          memo.Get(filtered_probe).filter = probe_filter;
          // A branch-local probe must stay branch-local: other derived rules
          // inherit the mark, otherwise this shared group could leak the
          // probe filter into another UNION branch.
          InheritBranchLocal(memo, filtered_probe, group);
          // The logical half of the dynamic filter contract: these columns
          // hold the join key a build side publishes a sound membership /
          // range filter over. The executor picks the shape (exact set,
          // min/max, Bloom) from the build domain it observes at run time.
          memo.Get(filtered_probe).dynamic_filter_keys = bloom_keys;
          memo.AddExpression(
              filtered_probe,
              LogicalExpression{.operation = LogicalOperator::kScan,
                                .table = probe_rels.front()});
          LogicalExpression rewritten = expression;
          rewritten.children[1] = filtered_probe;
          memo.AddExpression(group, std::move(rewritten));
        }
      },
      LogicalOperator::kJoin));

  // semi_reduction_pushdown_semi_join / semi_reduction_pushdown_anti_join:
  // annotate the probe-side join keys whose distinct values bound the build
  // side of a semi/anti join (Phase 2 "Hash Semi Reduction").
  //
  // A semi/anti join emits only the probe side, so its hash build is a pure
  // membership test over the join key. That makes two build-side reductions
  // sound: one row per distinct key, and only the keys the probe side
  // actually carries. These rules record the probe side's key columns -- the
  // input the domain is built from -- and nothing about the reduction
  // itself; the implementation rule decides whether the shape pays, and the
  // executor owns the domain, the dedup, and the fallback.
  //
  // The annotation lives on a derived group whose tag fingerprints the key
  // set, so a probe group that is the probe of two joins with DIFFERENT key
  // sets never has to merge two annotations into one vector. The derived
  // group keeps the probe group's selection (D5: "残余述語を保持し、フィルタ
  // 消失を許さない"), so the alternative is predicate-equivalent to the
  // original probe child.
  const auto semi_reduction_annotate = [](const Bindings& bindings, Memo& memo,
                                          GroupId group,
                                          const LogicalExpression& expression) {
    if (expression.operation != LogicalOperator::kSemiJoin &&
        expression.operation != LogicalOperator::kAntiJoin) {
      return;
    }
    if (memo.Get(group).tag.find("semi_reduce") != std::string::npos ||
        memo.Get(group).relations.size() != 2) {
      return;
    }
    if (!expression.predicate || !*expression.predicate) {
      return;
    }
    const GroupId probe_id = bindings.at("probe");
    if (probe_id == group) {
      return;
    }
    // Fixpoint guard: the probe already is a semi-reduction probe.
    if (memo.Get(probe_id).tag.starts_with("semi_reduce_probe:")) {
      return;
    }
    const std::vector<std::string> probe_rels = memo.Get(probe_id).relations;
    if (probe_rels.size() != 1 || memo.Get(probe_id).sample_barrier ||
        !std::ranges::any_of(memo.Get(probe_id).expressions,
                             [&](const LogicalExpression& candidate) {
                               return candidate.operation ==
                                          LogicalOperator::kScan &&
                                      candidate.table == probe_rels.front();
                             })) {
      return;
    }
    const std::vector<Expression> probe_conjuncts =
        SplitConjuncts(*expression.predicate);
    std::vector<ColumnName> probe_keys;
    probe_keys.reserve(probe_conjuncts.size());
    for (const Expression& conjunct : probe_conjuncts) {
      if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
        continue;
      }
      const auto& binary = conjunct->AsBinaryExpression();
      if (binary.Op() != BinaryOperation::kEquals ||
          binary.Left()->Type() != TypeTag::kColumnValue ||
          binary.Right()->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName left = binary.Left()->AsColumnValue().GetColumnName();
      const ColumnName right = binary.Right()->AsColumnValue().GetColumnName();
      const bool left_here =
          std::ranges::find(probe_rels, left.schema) != probe_rels.end();
      const bool right_here =
          std::ranges::find(probe_rels, right.schema) != probe_rels.end();
      if (left_here == right_here) {
        continue;  // both sides here (self-join) or neither side here
      }
      const ColumnName& key = left_here ? left : right;
      if (std::ranges::find(probe_keys, key) == probe_keys.end()) {
        probe_keys.push_back(key);
      }
    }
    if (probe_keys.empty()) {
      return;
    }
    const Expression probe_filter = memo.Get(probe_id).filter;
    // Length-prefix each key: a plain comma-separated concatenation
    // collides for e.g. ["a", "b"] and ["a,b"], which would reuse a
    // derived group annotated with the wrong probe key set.
    std::string key_fingerprint = std::to_string(probe_keys.size()) + ":";
    for (const ColumnName& key : probe_keys) {
      const std::string text = key.ToString();
      key_fingerprint += std::to_string(text.size()) + "/" + text + ";";
    }
    const GroupId keyed_probe = memo.EnsureDerivedGroup(
        probe_rels,
        "semi_reduce_probe:" + GroupIdsTag({probe_id}) + key_fingerprint);
    if (keyed_probe != probe_id && keyed_probe != group) {
      memo.Get(keyed_probe).filter = probe_filter;
      // Keep the probe's branch locality: without this the derived
      // group may be shared by another UNION branch as well.
      InheritBranchLocal(memo, keyed_probe, group);
      // The logical half of the semi-reduction contract: the executor
      // may build a distinct key domain over these columns and drop
      // build rows the probe side can never match.
      memo.Get(keyed_probe).semi_reduction_probe_keys = probe_keys;
      if (probe_rels.size() == 1) {
        memo.AddExpression(
            keyed_probe, LogicalExpression{.operation = LogicalOperator::kScan,
                                           .table = probe_rels.front()});
      }
      LogicalExpression rewritten = expression;
      rewritten.children[0] = keyed_probe;
      memo.AddExpression(group, std::move(rewritten));
    }
  };
  // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
  //   (dynamic_filter_and_semi_reduction_sound,
  //    cpp_dynamic_filter_erases_derived_probe_bug)
  built.Add(Rule("semi_reduction_pushdown_semi_join",
                 SemiJoin(Any("probe"), Any("build")), semi_reduction_annotate,
                 LogicalOperator::kSemiJoin));
  // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
  //   (dynamic_filter_and_semi_reduction_sound,
  //    cpp_dynamic_filter_erases_derived_probe_bug)
  built.Add(Rule("semi_reduction_pushdown_anti_join",
                 AntiJoin(Any("probe"), Any("build")), semi_reduction_annotate,
                 LogicalOperator::kAntiJoin));

  // join_predicate_transitivity: Infer non-constant equality transitive
  // chains across multi-way joins (t1.a = t2.b and t2.b = t3.c -> t1.a =
  // t3.c).
  // Lean proof: lean/TinylambOpt/SetJoinAndScalarRewrites.lean
  //   (eq_transitivity_and_redundancy_sound)
  built.Add(Rule(
      "join_predicate_transitivity", Join(),
      [](const Bindings&, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (!expression.predicate || !*expression.predicate) {
          return;
        }
        const auto conjuncts = SplitConjuncts(*expression.predicate);
        std::vector<std::pair<ColumnName, ColumnName>> equalities;
        equalities.reserve(conjuncts.size());
        for (const auto& conj : conjuncts) {
          if (conj && conj->Type() == TypeTag::kBinaryExp) {
            const auto& bin = conj->AsBinaryExpression();
            if (bin.Op() == BinaryOperation::kEquals &&
                bin.Left()->Type() == TypeTag::kColumnValue &&
                bin.Right()->Type() == TypeTag::kColumnValue) {
              const ColumnName lhs =
                  bin.Left()->AsColumnValue().GetColumnName();
              const ColumnName rhs =
                  bin.Right()->AsColumnValue().GetColumnName();
              if (!ColumnsProvenDifferentType(memo, lhs, rhs)) {
                equalities.emplace_back(lhs, rhs);
              }
            }
          }
        }
        if (equalities.size() < 2) {
          return;
        }
        std::vector<Expression> new_conjuncts = conjuncts;
        bool added = false;
        for (size_t i = 0; i < equalities.size(); ++i) {
          for (size_t j = 0; j < equalities.size(); ++j) {
            if (i == j) {
              continue;
            }
            const auto& [a1, b1] = equalities[i];
            const auto& [a2, b2] = equalities[j];
            std::optional<std::pair<ColumnName, ColumnName>> inferred;
            if (b1 == a2 && a1 != b2) {
              inferred = {a1, b2};
            } else if (b1 == b2 && a1 != a2) {
              inferred = {a1, a2};
            } else if (a1 == a2 && b1 != b2) {
              inferred = {b1, b2};
            } else if (a1 == b2 && b1 != a2) {
              inferred = {b1, a2};
            }

            if (inferred && inferred->first.schema != inferred->second.schema) {
              const Expression candidate = BinaryExpressionExp(
                  ColumnValueExp(inferred->first), BinaryOperation::kEquals,
                  ColumnValueExp(inferred->second));
              const std::string cand_str = candidate->ToString();
              const std::string rev_str =
                  BinaryExpressionExp(ColumnValueExp(inferred->second),
                                      BinaryOperation::kEquals,
                                      ColumnValueExp(inferred->first))
                      ->ToString();
              bool exists = false;
              for (const auto& c : new_conjuncts) {
                if (c->ToString() == cand_str || c->ToString() == rev_str) {
                  exists = true;
                  break;
                }
              }
              if (!exists) {
                new_conjuncts.push_back(candidate);
                added = true;
              }
            }
          }
        }
        if (added) {
          LogicalExpression rewritten = expression;
          rewritten.predicate =
              CanonicalizeConjuncts(CombineConjuncts(new_conjuncts));
          memo.AddExpression(group, std::move(rewritten));
        }
      },
      LogicalOperator::kJoin));

  // inferred_inequality_pushdown: Infer inequality predicates across
  // equalities (t1.x > 10 and t1.x = t2.y -> t2.y > 10).
  // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
  //   (same_domain_infer_join_constants_and_inequalities_sound,
  //    cpp_cross_type_int64_double_nontransitive_bug)
  built.Add(Rule(
      "inferred_inequality_pushdown", Join(),
      [](const Bindings&, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (SelectionMayStrengthenSharedScans(memo, group) &&
            expression.predicate) {
          InferJoinInequalities(memo, *expression.predicate);
        }
      },
      LogicalOperator::kJoin));

  // redundant_join_predicate_elimination: Eliminate redundant join conditions
  // already satisfied by transitivity.
  // Lean proof: lean/TinylambOpt/SetJoinAndScalarRewrites.lean
  //   (eq_transitivity_and_redundancy_sound,
  //    cpp_redundant_join_eq_drops_self_eq_null_bug)
  built.Add(Rule(
      "redundant_join_predicate_elimination", Join(),
      [](const Bindings&, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (!expression.predicate || !*expression.predicate) {
          return;
        }
        const auto conjuncts = SplitConjuncts(*expression.predicate);
        if (conjuncts.size() < 3) {
          return;
        }
        std::unordered_map<std::string, std::string> parent;
        std::unordered_set<std::string> constrained;
        const auto find_root = [&](auto& self,
                                   const std::string& x) -> std::string {
          auto it = parent.find(x);
          if (it == parent.end() || it->second == x) {
            return x;
          }
          return it->second = self(self, it->second);
        };
        const auto unite = [&](const std::string& a,
                               const std::string& b) -> bool {
          // SQL `=` is NOT reflexive on NULL or NaN (`NULL = NULL` is
          // UNKNOWN, `NaN = NaN` is FALSE), whereas union-find `root(u) ==
          // root(u)` is always true. A self-equality `u = u` is redundant
          // ONLY when an earlier kept equality already constrained `u` to be
          // non-NULL and non-NaN.
          if (a == b) {
            if (constrained.contains(a)) {
              return false;
            }
            constrained.insert(a);
            return true;
          }
          std::string root_a = find_root(find_root, a);
          std::string root_b = find_root(find_root, b);
          constrained.insert(a);
          constrained.insert(b);
          if (root_a == root_b) {
            return false;
          }
          parent[root_a] = root_b;
          return true;
        };

        std::vector<Expression> kept;
        kept.reserve(conjuncts.size());
        bool removed = false;
        for (const auto& conj : conjuncts) {
          if (conj && conj->Type() == TypeTag::kBinaryExp) {
            const auto& bin = conj->AsBinaryExpression();
            if (bin.Op() == BinaryOperation::kEquals && bin.Left() &&
                bin.Right() && bin.Left()->Type() == TypeTag::kColumnValue &&
                bin.Right()->Type() == TypeTag::kColumnValue) {
              const ColumnName lhs_col =
                  bin.Left()->AsColumnValue().GetColumnName();
              const ColumnName rhs_col =
                  bin.Right()->AsColumnValue().GetColumnName();
              if (!ColumnsProvenDifferentType(memo, lhs_col, rhs_col)) {
                const std::string u = lhs_col.ToString();
                const std::string v = rhs_col.ToString();
                if (unite(u, v)) {
                  kept.push_back(conj);
                } else {
                  removed = true;
                }
                continue;
              }
            }
          }
          kept.push_back(conj);
        }

        if (removed && !kept.empty()) {
          LogicalExpression rewritten = expression;
          rewritten.predicate = CanonicalizeConjuncts(CombineConjuncts(kept));
          memo.AddExpression(group, std::move(rewritten));
        }
      },
      LogicalOperator::kJoin));

  // unnest_filter_pushdown: Push selection filter down into the child input
  // of Unnest.
  // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
  //   (unnest_and_values_rules_sound,
  //    cpp_unnest_alias_and_empty_array_throw_bugs)
  built.Add(Rule(
      "unnest_filter_pushdown", Selection(Unnest(Any(), "unnest")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        auto free_of_data_dependent_errors =
            [](const auto& self, const Expression& expr) -> bool {
          if (!expr) {
            return false;
          }
          switch (expr->Type()) {
            case TypeTag::kConstantValue:
            case TypeTag::kColumnValue:
              return true;
            case TypeTag::kUnaryExp: {
              const auto& u = expr->AsUnaryExpression();
              switch (u.Op()) {
                case UnaryOperation::kIsNull:
                case UnaryOperation::kIsNotNull:
                case UnaryOperation::kNot:
                case UnaryOperation::kIsTrue:
                case UnaryOperation::kIsNotTrue:
                case UnaryOperation::kIsFalse:
                case UnaryOperation::kIsNotFalse:
                  return self(self, u.Child());
                default:
                  return false;
              }
            }
            case TypeTag::kBinaryExp: {
              const auto& b = expr->AsBinaryExpression();
              switch (b.Op()) {
                case BinaryOperation::kEquals:
                case BinaryOperation::kNotEquals:
                case BinaryOperation::kLessThan:
                case BinaryOperation::kLessThanEquals:
                case BinaryOperation::kGreaterThan:
                case BinaryOperation::kGreaterThanEquals:
                case BinaryOperation::kIsDistinctFrom:
                case BinaryOperation::kIsNotDistinctFrom:
                case BinaryOperation::kAnd:
                case BinaryOperation::kOr:
                case BinaryOperation::kXor:
                  return self(self, b.Left()) && self(self, b.Right());
                default:
                  return false;
              }
            }
            case TypeTag::kInExp: {
              const auto& in = expr->AsInExpression();
              return self(self, in.child_) &&
                     std::ranges::all_of(in.list_, [&](const Expression& e) {
                       return self(self, e);
                     });
            }
            default:
              return false;
          }
        };
        if (!expression.predicate || !*expression.predicate ||
            !free_of_data_dependent_errors(free_of_data_dependent_errors,
                                           *expression.predicate) ||
            !SafeToReduceEvaluationCount(*expression.predicate)) {
          return;
        }
        const GroupId unnest_group_id = bindings.at("unnest");
        if (unnest_group_id == group) {
          return;
        }
        const Group& unnest_group = memo.Get(unnest_group_id);
        for (const LogicalExpression& unnest : unnest_group.expressions) {
          if (unnest.operation != LogicalOperator::kUnnest ||
              unnest.children.size() != 1 || unnest.children[0] == group) {
            continue;
          }
          const std::string eff_alias =
              unnest.unnest_alias.empty() ? "unnest" : unnest.unnest_alias;
          const std::unordered_set<ColumnName> touched =
              (*expression.predicate)->TouchedColumns();
          bool touches_unnest = false;
          for (const auto& col : touched) {
            if (col.name == "*" || IdentifierEquals(col.name, eff_alias) ||
                (!col.schema.empty() &&
                 IdentifierEquals(col.schema, eff_alias)) ||
                (!unnest.offset_alias.empty() &&
                 IdentifierEquals(col.name, unnest.offset_alias))) {
              touches_unnest = true;
              break;
            }
          }
          if (touches_unnest) {
            continue;
          }
          const GroupId unnest_child = unnest.children[0];
          const GroupId sel_below = memo.EnsureDerivedGroup(
              memo.Get(unnest_child).relations,
              "sel_unnest_child:" + GroupIdsTag({unnest_child}) +
                  PredicateTag(expression.predicate));
          InheritBranchLocal(memo, sel_below, group);
          if (sel_below != unnest_child && sel_below != group) {
            memo.AddExpression(
                sel_below,
                LogicalExpression{
                    .operation = LogicalOperator::kSelection,
                    .children = {unnest_child},
                    .predicate = expression.predicate,
                    .output_schema = memo.Get(unnest_child).expressions.empty()
                                         ? Schema()
                                         : memo.Get(unnest_child)
                                               .expressions.front()
                                               .output_schema});
            LogicalExpression new_unnest = unnest;
            new_unnest.children = {sel_below};
            memo.AddExpression(group, std::move(new_unnest));
          }
          return;
        }
      },
      LogicalOperator::kSelection));

  // constant_unnest_to_values: A stand-alone UNNEST over a one-row leaf
  // (the dummy that a FROM-level UNNEST gets when no other source exists)
  // whose array argument folds to a constant IS the expanded element list,
  // with each row = [element, offset?].  Fold it into a kValues node.
  // Refusals keep the executor path: nested-array element layouts,
  // non-constant shapes the pure evaluator cannot fold, struct/PROTO
  // widening layouts the rule cannot mirror, driving inputs wider than
  // the dummy leaf, and beyond-threshold element counts
  // (kConstantUnnestValuesThreshold).
  // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
  //   (unnest_and_values_rules_sound, cpp_parameter_slot_freeze_bug)
  built.Add(Rule(
      "constant_unnest_to_values", Unnest(Any("input"), "unnest"),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (!expression.predicate || !*expression.predicate ||
            ContainsParameterSlot(*expression.predicate)) {
          return;
        }
        const Expression& predicate = *expression.predicate;
        // Only shapes the runtime evaluator treats as deterministic
        // constants: a literal (possibly folded earlier by constant
        // folding), an ARRAY literal of constants, or a GENERATE_SERIES
        // over constants.  A generative builtin like CURRENT_DATE must
        // not freeze into a plan-cached literal.
        if (predicate->Type() == TypeTag::kFunctionCallExp) {
          if (!IdentifierEquals(
                  "GENERATE_SERIES",
                  predicate->AsFunctionCallExpression().FuncName())) {
            return;
          }
        } else if (predicate->Type() == TypeTag::kArrayExp) {
          bool constant_children = true;
          for (const Expression& child : ExpressionChildren(predicate)) {
            if (!child || child->Type() != TypeTag::kConstantValue) {
              constant_children = false;
              break;
            }
          }
          if (!constant_children) {
            return;
          }
        } else if (predicate->Type() != TypeTag::kConstantValue) {
          return;
        }
        const StatusOr<Value> evaluated =
            predicate->TryEvaluate(Row(), Schema());
        if (!evaluated.HasValue() || !evaluated.Value().IsArray()) {
          return;
        }
        const auto& elements = evaluated.Value().ArrayElements();
        if (elements.size() > kConstantUnnestValuesThreshold) {
          return;
        }
        for (const Value& element : elements) {
          if (element.IsArray()) {
            return;
          }
        }
        // The fold reproduces the executor's scalar layout (elem column
        // then the 0-based counter) exactly; a section schema that
        // disagrees comes from a struct/PROTO element layout.
        const bool with_offset = !expression.offset_alias.empty();
        const size_t expected_width = with_offset ? 2U : 1U;
        const std::string rel_name = expression.unnest_alias.empty()
                                         ? "unnest"
                                         : expression.unnest_alias;
        Schema section = expression.output_schema;
        if (section.ColumnCount() == expected_width) {
          for (size_t i = 0; i < section.ColumnCount(); ++i) {
            if (!IdentifierEquals(section.GetColumn(i).Name().schema,
                                  rel_name)) {
              return;
            }
          }
        } else if (section.ColumnCount() != 0) {
          return;
        } else {
          ValueType element_type = ValueType::kNull;
          for (const Value& element : elements) {
            if (!element.IsNull()) {
              element_type = element.type;
              break;
            }
          }
          std::vector<Column> derived;
          derived.emplace_back(ColumnName(rel_name, rel_name), element_type);
          if (with_offset) {
            derived.emplace_back(ColumnName(rel_name, expression.offset_alias),
                                 ValueType::kInt64);
          }
          section = Schema(rel_name, std::move(derived));
        }
        // Only a one-row driving leaf folds to plain Values.  The dummy
        // group owns the FROM-level route; VALUES leaves never reach the
        // unnest builder today, so treat anything else as unsupported.
        bool dummy_leaf = false;
        if (bindings.at("input") != group &&
            !memo.Get(bindings.at("input")).expressions.empty()) {
          dummy_leaf = std::ranges::all_of(
              memo.Get(bindings.at("input")).expressions,
              [](const LogicalExpression& leaf) {
                return leaf.operation == LogicalOperator::kDummyScan;
              });
        }
        if (!dummy_leaf) {
          return;
        }
        for (const LogicalExpression& existing : memo.Get(group).expressions) {
          if (existing.operation == LogicalOperator::kValues) {
            return;
          }
        }
        std::vector<Row> folded;
        folded.reserve(elements.size());
        for (size_t index = 0; index < elements.size(); ++index) {
          folded.emplace_back(
              with_offset
                  ? Row({elements[index], Value(static_cast<int64_t>(index))})
                  : Row({elements[index]}));
        }
        LogicalExpression values_expression{};
        values_expression.operation = LogicalOperator::kValues;
        values_expression.values = std::move(folded);
        values_expression.output_schema = std::move(section);
        memo.AddExpression(group, std::move(values_expression));
      },
      LogicalOperator::kUnnest));

  // recursive_termination_predicate_pushdown: narrow a recursive CTE's
  // depth window from a Selection directly above it. The opaque leaf
  // (childless kRecursiveCte with a depth column, the production shape
  // emitted by LiftRecursiveCtes) emits exactly the fixpoint rows whose
  // depth lies in [lower, upper], and every row at depth <= N is fully
  // produced by iteration N -- so filtering `depth <= N` post-hoc equals
  // capping the driver at depth N. Only comparisons of the depth column
  // against integer constants are sound here: any other predicate would
  // also drop rows the recursion needed as working set (a non-monotone
  // filter changes the fixpoint itself), so those keep the Selection.
  // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
  //   (recursive_termination_predicate_pushdown_sound,
  //    cpp_parameter_slot_freeze_bug)
  built.Add(Rule(
      "recursive_termination_predicate_pushdown", Selection(Any("cte")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kSelection ||
            expression.children.size() != 1 || !expression.predicate ||
            !*expression.predicate ||
            ContainsParameterSlot(*expression.predicate)) {
          return;
        }
        const GroupId cte_group_id = bindings.at("cte");
        if (cte_group_id == group) {
          return;
        }
        const Group& cte_group = memo.Get(cte_group_id);
        for (const LogicalExpression& cte : cte_group.expressions) {
          if (cte.operation != LogicalOperator::kRecursiveCte ||
              !cte.children.empty() || !cte.depth_spec.has_value()) {
            continue;
          }
          const RecursiveDepthSpec& spec = *cte.depth_spec;
          int64_t lower = spec.lower;
          int64_t upper = spec.upper;
          bool narrowed = false;
          bool recognized = true;
          for (const Expression& conjunct :
               SplitConjuncts(*expression.predicate)) {
            if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
              recognized = false;
              break;
            }
            const auto& bin = conjunct->AsBinaryExpression();
            // The depth test is `depth_col <op> int_const` either way
            // around; anything else keeps the Selection.
            const bool left_is_depth =
                bin.Left()->Type() == TypeTag::kColumnValue &&
                IdentifierEquals(
                    bin.Left()->AsColumnValue().GetColumnName().name,
                    spec.column) &&
                (bin.Left()->AsColumnValue().GetColumnName().schema.empty() ||
                 IdentifierEquals(
                     bin.Left()->AsColumnValue().GetColumnName().schema,
                     cte.cte_name));
            const bool right_is_depth =
                bin.Right()->Type() == TypeTag::kColumnValue &&
                IdentifierEquals(
                    bin.Right()->AsColumnValue().GetColumnName().name,
                    spec.column) &&
                (bin.Right()->AsColumnValue().GetColumnName().schema.empty() ||
                 IdentifierEquals(
                     bin.Right()->AsColumnValue().GetColumnName().schema,
                     cte.cte_name));
            const ExpressionBase* constant = nullptr;
            bool depth_on_left = false;
            if (left_is_depth &&
                bin.Right()->Type() == TypeTag::kConstantValue) {
              constant = bin.Right().get();
              depth_on_left = true;
            } else if (right_is_depth &&
                       bin.Left()->Type() == TypeTag::kConstantValue) {
              constant = bin.Left().get();
            } else {
              recognized = false;
              break;
            }
            const Value& bound = constant->AsConstantValue().GetValue();
            if (bound.IsNull() || bound.type != ValueType::kInt64) {
              recognized = false;
              break;
            }
            const int64_t n = bound.value.int_value;
            BinaryOperation op = bin.Op();
            if (!depth_on_left) {
              // `const <op> depth` mirrors to `depth <op'> const`.
              switch (op) {
                case BinaryOperation::kLessThan:
                  op = BinaryOperation::kGreaterThan;
                  break;
                case BinaryOperation::kLessThanEquals:
                  op = BinaryOperation::kGreaterThanEquals;
                  break;
                case BinaryOperation::kGreaterThan:
                  op = BinaryOperation::kLessThan;
                  break;
                case BinaryOperation::kGreaterThanEquals:
                  op = BinaryOperation::kLessThanEquals;
                  break;
                default:
                  break;
              }
            }
            switch (op) {
              case BinaryOperation::kLessThanEquals:
                if (n < upper) {
                  upper = n;
                  narrowed = true;
                }
                break;
              case BinaryOperation::kLessThan:
                if (n == std::numeric_limits<int64_t>::min()) {
                  recognized = false;
                  break;
                }
                if (n - 1 < upper) {
                  upper = n - 1;
                  narrowed = true;
                }
                break;
              case BinaryOperation::kGreaterThanEquals:
                if (n > lower) {
                  lower = n;
                  narrowed = true;
                }
                break;
              case BinaryOperation::kGreaterThan:
                if (n == std::numeric_limits<int64_t>::max()) {
                  recognized = false;
                  break;
                }
                if (n + 1 > lower) {
                  lower = n + 1;
                  narrowed = true;
                }
                break;
              default:
                recognized = false;
                break;
            }
            if (!recognized) {
              break;
            }
          }
          if (!recognized || !narrowed || lower > upper) {
            continue;
          }
          LogicalExpression narrowed_cte = cte;
          narrowed_cte.depth_spec = RecursiveDepthSpec{
              .column = spec.column, .lower = lower, .upper = upper};
          memo.AddExpression(group, std::move(narrowed_cte));
          return;
        }
      },
      LogicalOperator::kSelection));

  // in_list_to_semi_join: Transform large IN list predicates into SemiJoin
  // with a Values group.
  // Lean proof: lean/TinylambOpt/SetJoinAndScalarRewrites.lean
  //   (in_list_to_semi_join_sound, cpp_in_list_fingerprint_collision_bug)
  built.Add(Rule(
      "in_list_to_semi_join", Selection(Any("child")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (!expression.predicate || !*expression.predicate ||
            ContainsParameterSlot(*expression.predicate)) {
          return;
        }
        const GroupId child_id = bindings.at("child");
        if (child_id == group) {
          return;
        }
        std::vector<Value> in_values;
        std::optional<ColumnName> target_col;
        // D5 gate ("IN-list semi join: 収集できなかったOR枝を捨てず、全枝を
        // 保持する"): if ANY branch of the OR tree is not a col=const
        // equality on the same column, the constant table would silently
        // drop that branch's rows.  Track completeness and refuse to fire
        // instead of rewriting to a smaller row set.
        bool collected_all_branches = true;
        const auto collect_in_values = [&](auto& self,
                                           const Expression& exp) -> void {
          if (!exp) {
            collected_all_branches = false;
            return;
          }
          if (exp->Type() == TypeTag::kBinaryExp) {
            const auto& bin = exp->AsBinaryExpression();
            if (bin.Op() == BinaryOperation::kOr) {
              self(self, bin.Left());
              self(self, bin.Right());
              return;
            }
            if (bin.Op() == BinaryOperation::kEquals) {
              const bool left_col =
                  bin.Left()->Type() == TypeTag::kColumnValue &&
                  bin.Right()->Type() == TypeTag::kConstantValue;
              const bool right_col =
                  bin.Right()->Type() == TypeTag::kColumnValue &&
                  bin.Left()->Type() == TypeTag::kConstantValue;
              if (left_col || right_col) {
                const ColumnName col = (left_col ? bin.Left() : bin.Right())
                                           ->AsColumnValue()
                                           .GetColumnName();
                const Value constant = (left_col ? bin.Right() : bin.Left())
                                           ->AsConstantValue()
                                           .GetValue();
                if (constant.IsNull() ||
                    (!in_values.empty() &&
                     constant.type != in_values.front().type)) {
                  collected_all_branches = false;
                  return;
                }
                if (!target_col) {
                  target_col = col;
                }
                if (*target_col == col) {
                  in_values.push_back(constant);
                  return;
                }
              }
            }
          }
          collected_all_branches = false;
        };
        collect_in_values(collect_in_values, *expression.predicate);

        if (!collected_all_branches || !target_col || in_values.size() < 3) {
          return;
        }
        const std::string const_table_name = "in_const_" + target_col->name;
        // D5 gate ("derived group: 意味内容を含むfingerprintで識別する"):
        // type- and length-prefix every constant so `['a;b','c']` never
        // collides with `['a','b;c']` (Lean:
        // `cpp_in_list_fingerprint_collision_bug`).
        std::string value_fingerprint = target_col->ToString() + ":";
        for (const Value& v : in_values) {
          const std::string text = v.AsString();
          AppendInteger(value_fingerprint, static_cast<int>(v.type));
          value_fingerprint.push_back(':');
          AppendInteger(value_fingerprint, text.size());
          value_fingerprint.push_back(':');
          value_fingerprint += text;
          value_fingerprint.push_back(';');
        }
        const GroupId const_group =
            memo.EnsureDerivedGroup({}, "in_list_table:" + value_fingerprint);
        if (const_group != group) {
          std::vector<Row> rows;
          rows.reserve(in_values.size());
          for (const auto& v : in_values) {
            rows.push_back(Row({v}));
          }
          LogicalExpression values_expression;
          values_expression.operation = LogicalOperator::kValues;
          values_expression.target_list = {NamedExpression(
              target_col->name,
              ColumnValueExp(ColumnName(const_table_name, target_col->name)))};
          values_expression.values = std::move(rows);
          values_expression.output_schema =
              Schema(const_table_name,
                     {Column(target_col->name, in_values.front().type)});
          memo.AddExpression(const_group, std::move(values_expression));

          const Expression join_cond = BinaryExpressionExp(
              ColumnValueExp(*target_col), BinaryOperation::kEquals,
              ColumnValueExp(ColumnName(const_table_name, target_col->name)));

          memo.AddExpression(
              group,
              LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                                .children = {child_id, const_group},
                                .predicate = join_cond,
                                .target_list = expression.target_list,
                                .output_schema = expression.output_schema});
        }
      },
      LogicalOperator::kSelection));

  // functional_dependency_filter_reduction: Simplify filters using known
  // functional dependencies and equality keys.
  // Lean proof: lean/TinylambOpt/SetJoinAndScalarRewrites.lean
  //   (eq_transitivity_and_redundancy_sound)
  built.Add(Rule(
      "functional_dependency_filter_reduction", Selection(Any("child")),
      [](const Bindings&, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (!expression.predicate || !*expression.predicate) {
          return;
        }
        const auto conjuncts = SplitConjuncts(*expression.predicate);
        if (conjuncts.size() < 2) {
          return;
        }
        std::unordered_map<std::string, Value> eq_constants;
        for (const auto& conj : conjuncts) {
          if (conj && conj->Type() == TypeTag::kBinaryExp &&
              !ContainsParameterSlot(conj)) {
            const auto& bin = conj->AsBinaryExpression();
            if (bin.Op() == BinaryOperation::kEquals) {
              if (bin.Left()->Type() == TypeTag::kColumnValue &&
                  bin.Right()->Type() == TypeTag::kConstantValue) {
                const Value& val = bin.Right()->AsConstantValue().GetValue();
                if (!val.IsNull()) {
                  eq_constants.try_emplace(
                      bin.Left()->AsColumnValue().GetColumnName().ToString(),
                      val);
                }
              } else if (bin.Right()->Type() == TypeTag::kColumnValue &&
                         bin.Left()->Type() == TypeTag::kConstantValue) {
                const Value& val = bin.Left()->AsConstantValue().GetValue();
                if (!val.IsNull()) {
                  eq_constants.try_emplace(
                      bin.Right()->AsColumnValue().GetColumnName().ToString(),
                      val);
                }
              }
            }
          }
        }
        if (eq_constants.empty()) {
          return;
        }
        std::vector<Expression> kept;
        bool reduced = false;
        for (const auto& conj : conjuncts) {
          if (conj && conj->Type() == TypeTag::kBinaryExp &&
              !ContainsParameterSlot(conj)) {
            const auto& bin = conj->AsBinaryExpression();
            if (bin.Op() == BinaryOperation::kLessThanEquals ||
                bin.Op() == BinaryOperation::kGreaterThanEquals ||
                bin.Op() == BinaryOperation::kLessThan ||
                bin.Op() == BinaryOperation::kGreaterThan) {
              const Expression* column_expr = nullptr;
              const Expression* bound_expr = nullptr;
              BinaryOperation op = bin.Op();
              if (bin.Left()->Type() == TypeTag::kColumnValue &&
                  bin.Right()->Type() == TypeTag::kConstantValue) {
                column_expr = &bin.Left();
                bound_expr = &bin.Right();
              } else if (bin.Left()->Type() == TypeTag::kConstantValue &&
                         bin.Right()->Type() == TypeTag::kColumnValue) {
                column_expr = &bin.Right();
                bound_expr = &bin.Left();
                switch (op) {
                  case BinaryOperation::kLessThan:
                    op = BinaryOperation::kGreaterThan;
                    break;
                  case BinaryOperation::kGreaterThan:
                    op = BinaryOperation::kLessThan;
                    break;
                  case BinaryOperation::kLessThanEquals:
                    op = BinaryOperation::kGreaterThanEquals;
                    break;
                  case BinaryOperation::kGreaterThanEquals:
                    op = BinaryOperation::kLessThanEquals;
                    break;
                  default:
                    break;
                }
              }
              if (column_expr != nullptr) {
                const std::string col_str =
                    (*column_expr)->AsColumnValue().GetColumnName().ToString();
                auto it = eq_constants.find(col_str);
                if (it != eq_constants.end()) {
                  const Value& k = it->second;
                  const Value& bound =
                      (*bound_expr)->AsConstantValue().GetValue();
                  if (!k.IsNull() && !bound.IsNull() && k.type == bound.type) {
                    const StatusOr<Value> eval =
                        TryEvaluateBinary(op, k, bound);
                    if (eval.HasValue() && !eval.Value().IsNull() &&
                        eval.Value().Truthy()) {
                      reduced = true;
                      continue;
                    }
                  }
                }
              }
            }
          }
          kept.push_back(conj);
        }
        if (reduced && !kept.empty()) {
          memo.AddExpression(
              group,
              LogicalExpression{
                  .operation = LogicalOperator::kSelection,
                  .children = expression.children,
                  .predicate = CanonicalizeConjuncts(CombineConjuncts(kept)),
                  .target_list = expression.target_list,
                  .output_schema = expression.output_schema});
        }
      },
      LogicalOperator::kSelection));
  // filter_aggregate_pushdown: Push filtering conditions directly into
  // aggregate FILTER (WHERE ...) clauses where advantageous.
  // Lean proof: lean/TinylambOpt/NullRejectionAndGroupRewrites.lean
  //   (repaired_filter_aggregate_pushdown_sound,
  //    cpp_filter_agg_pushdown_compound_target_bug)
  built.Add(Rule(
      "filter_aggregate_pushdown", Aggregation(Selection(Any("input"), "sel")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kAggregation ||
            expression.target_list.empty()) {
          return;
        }
        const GroupId sel_id = bindings.at("sel");
        if (sel_id == group) {
          return;
        }
        const Group& sel_group = memo.Get(sel_id);
        for (const auto& sel_expr : sel_group.expressions) {
          if (sel_expr.operation != LogicalOperator::kSelection ||
              sel_expr.children.empty() || !sel_expr.predicate ||
              !*sel_expr.predicate) {
            continue;
          }
          const GroupId input_id = sel_expr.children[0];
          if (input_id == group || input_id == sel_id) {
            continue;
          }
          // Only scalar aggregation may move the selection into FILTER
          // clauses: with GROUP BY, groups whose every row fails the
          // predicate vanish from the original plan but would survive the
          // rewrite as COUNT=0 phantom groups.
          if (!expression.grouping_sets.empty()) {
            continue;
          }
          // The predicate is replicated into every aggregate's FILTER
          // clause, multiplying its evaluation count by the number of
          // aggregates. Volatile functions or subqueries would change
          // results (or cost), so such selections stay in place.
          if (!SafeToReduceEvaluationCount(*sel_expr.predicate)) {
            continue;
          }
          std::vector<NamedExpression> new_targets;
          new_targets.reserve(expression.target_list.size());
          bool transformed = false;
          bool all_valid = true;
          for (const auto& target : expression.target_list) {
            if (!target.expression) {
              all_valid = false;
              break;
            }
            if (target.expression->Type() == TypeTag::kAggregateExp) {
              const auto& orig = dynamic_cast<const AggregateExpression&>(
                  *target
                       .expression);  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
              if (orig.NeedsGroupContext() || !orig.InnerGroupBy().empty()) {
                all_valid = false;
                break;
              }
              auto agg_copy = CloneAggregate(orig, orig.Distinct());
              // Conjoin with any existing FILTER instead of silently
              // overwriting it.
              if (orig.HasWhereFilter()) {
                agg_copy->SetWhereFilter(BinaryExpressionExp(
                    orig.WhereFilter(), BinaryOperation::kAnd,
                    *sel_expr.predicate));
              } else {
                agg_copy->SetWhereFilter(*sel_expr.predicate);
              }
              new_targets.emplace_back(target.name, agg_copy);
              transformed = true;
            } else if (!ContainsAggregate(target.expression) &&
                       target.expression->TouchedColumns().empty()) {
              new_targets.push_back(target);
            } else {
              // Compound expressions containing aggregates (`SUM(x) + 1`) or
              // bare column references would lose the Selection filter if
              // passed through untouched.
              all_valid = false;
              break;
            }
          }
          if (all_valid && transformed) {
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kAggregation,
                                  .children = {input_id},
                                  .predicate = expression.predicate,
                                  .target_list = std::move(new_targets),
                                  .output_schema = expression.output_schema,
                                  .partition_by = expression.partition_by,
                                  .grouping_sets = expression.grouping_sets});
          }
          break;
        }
      },
      LogicalOperator::kAggregation));

  // aggregate_union_transpose: Push Aggregation below UnionAll when
  // aggregates are distributable (SUM, COUNT, MIN, MAX).
  // Lean proof: lean/TinylambOpt/NullRejectionAndGroupRewrites.lean
  //   (repaired_agg_union_sum_and_count_sound,
  //    cpp_agg_union_compound_sum_plus_one_bug,
  //    cpp_agg_union_having_pushed_to_branch_bug) &
  //   lean/TinylambOpt/AggBase.lean (aggFold_append)
  built.Add(Rule(
      "aggregate_union_transpose", Aggregation(Any("input")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kAggregation ||
            expression.children.size() != 1 || expression.target_list.empty()) {
          return;
        }
        const GroupId input_id = bindings.at("input");
        if (input_id == group) {
          return;
        }
        // Fixpoint guard: the re-aggregation this rule emits sits over a
        // union of already-pushed aggregates; pushing it again would nest
        // aggregate-of-aggregate groups without bound.
        if (memo.Get(input_id).tag.starts_with("union_all_pushed_aggs:")) {
          return;
        }
        const Group& input_group = memo.Get(input_id);
        for (const auto& union_expr : input_group.expressions) {
          if (union_expr.operation != LogicalOperator::kUnionAll ||
              union_expr.children.empty()) {
            continue;
          }

          std::vector<ColumnName> group_key_cols;
          bool keys_ok = true;
          for (const Expression& g : expression.grouping_sets) {
            if (!g || g->Type() != TypeTag::kColumnValue) {
              keys_ok = false;
              break;
            }
            group_key_cols.push_back(g->AsColumnValue().GetColumnName());
          }
          if (!keys_ok) {
            continue;
          }

          bool distributable = true;
          bool has_agg = false;
          std::unordered_set<std::string> seen_agg_names;
          for (const auto& target : expression.target_list) {
            if (!target.expression) {
              distributable = false;
              break;
            }
            if (target.expression->Type() == TypeTag::kAggregateExp) {
              const auto& agg = dynamic_cast<const AggregateExpression&>(
                  *target
                       .expression);  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
              if (target.name.empty() ||
                  !seen_agg_names.insert(target.name).second ||
                  agg.Distinct() || agg.NeedsGroupContext() ||
                  agg.HasWhereFilter() || !agg.InnerGroupBy().empty()) {
                distributable = false;
                break;
              }
              if (agg.GetType() != AggregationType::kSum &&
                  agg.GetType() != AggregationType::kCount &&
                  agg.GetType() != AggregationType::kMin &&
                  agg.GetType() != AggregationType::kMax) {
                distributable = false;
                break;
              }
              has_agg = true;
            } else {
              // Compound aggregate targets (`SUM(x) + 1`, `AVG(x) + 1`) are
              // NOT homomorphisms over UNION ALL (`(s1 + 1) + (s2 + 1) !=
              // (s1 + s2) + 1`). Non-aggregate targets may only reference
              // plain grouping columns preserved by the per-branch schema.
              if (ContainsAggregate(target.expression)) {
                distributable = false;
                break;
              }
              for (const ColumnName& touched :
                   target.expression->TouchedColumns()) {
                const bool in_group_keys = std::ranges::any_of(
                    group_key_cols, [&](const ColumnName& gk) {
                      return gk == touched ||
                             ((gk.schema.empty() || touched.schema.empty()) &&
                              gk.name == touched.name);
                    });
                if (!in_group_keys) {
                  distributable = false;
                  break;
                }
              }
              if (!distributable) {
                break;
              }
            }
          }
          if (!distributable || !has_agg) {
            continue;
          }

          std::vector<GroupId> pushed_aggs;
          pushed_aggs.reserve(union_expr.children.size());
          const std::string target_fingerprint =
              TargetListFingerprint(expression.target_list);
          for (size_t i = 0; i < union_expr.children.size(); ++i) {
            const GroupId branch_id = union_expr.children[i];
            // Keyed by the branch group itself (D1): two unions over the
            // same relation set must never share a per-branch aggregate.
            // Per-branch aggregates NEVER inherit HAVING (`predicate`);
            // HAVING applies only after cross-branch re-aggregation.
            const GroupId branch_agg = memo.EnsureDerivedGroup(
                memo.Get(branch_id).relations,
                "agg_union_branch_" + std::to_string(i) + ":" +
                    std::to_string(branch_id) + ":" + target_fingerprint);
            InheritBranchLocal(memo, branch_agg, group);
            if (branch_agg != branch_id && branch_agg != group) {
              memo.AddExpression(
                  branch_agg,
                  LogicalExpression{.operation = LogicalOperator::kAggregation,
                                    .children = {branch_id},
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema,
                                    .partition_by = expression.partition_by,
                                    .grouping_sets = expression.grouping_sets});
              pushed_aggs.push_back(branch_agg);
            }
          }
          if (pushed_aggs.size() != union_expr.children.size()) {
            continue;
          }

          // A constant tag made every aggregation over this relation set
          // share ONE union group; fingerprint the pushed branches.
          std::string union_tag = "union_all_pushed_aggs:";
          for (const GroupId pushed : pushed_aggs) {
            AppendInteger(union_tag, pushed);
            union_tag += ',';
          }
          const GroupId union_of_aggs =
              memo.EnsureDerivedGroup(memo.Get(group).relations, union_tag);
          InheritBranchLocal(memo, union_of_aggs, group);
          if (union_of_aggs != group && union_of_aggs != input_id) {
            memo.AddExpression(
                union_of_aggs,
                LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                  .children = pushed_aggs,
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema});

            std::vector<NamedExpression> final_targets;
            for (const auto& target : expression.target_list) {
              if (target.expression &&
                  target.expression->Type() == TypeTag::kAggregateExp) {
                const auto& agg = dynamic_cast<
                    const AggregateExpression&>(  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
                    *target.expression);
                if (agg.GetType() == AggregationType::kCount) {
                  final_targets.emplace_back(
                      target.name,
                      AggregateExpressionExp(
                          AggregationType::kSum,
                          ColumnValueExp(ColumnName(target.name))));
                } else {
                  final_targets.emplace_back(
                      target.name,
                      AggregateExpressionExp(
                          agg.GetType(),
                          ColumnValueExp(ColumnName(target.name))));
                }
              } else {
                final_targets.push_back(target);
              }
            }

            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kAggregation,
                                  .children = {union_of_aggs},
                                  .predicate = expression.predicate,
                                  .target_list = std::move(final_targets),
                                  .output_schema = expression.output_schema,
                                  .partition_by = expression.partition_by,
                                  .grouping_sets = expression.grouping_sets});
          }
        }
      },
      LogicalOperator::kAggregation));

  // pk_unique_distinct_elimination: Eliminate DISTINCT when input columns
  // form a primary key or unique key.
  // Lean proof: lean/TinylambOpt/SetJoinAndScalarRewrites.lean
  //   (pk_unique_distinct_elim_sound)
  built.Add(Rule(
      "pk_unique_distinct_elimination", Distinct(Any("input")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kDistinct ||
            expression.children.size() != 1) {
          return;
        }
        const GroupId input_id = bindings.at("input");
        if (input_id == group) {
          return;
        }
        const Group& input_group = memo.Get(input_id);
        // SQL's Distinct carries no target list. Prove uniqueness of the
        // actual child's output, mapping a projection back to its source
        // keys, and reuse that equivalent expression without another pass.
        // Copy candidates because AddExpression may invalidate storage.
        const auto candidates = input_group.expressions;
        for (const auto& child : candidates) {
          const LogicalProperties* properties = &input_group.logical_properties;
          std::unordered_set<std::string> columns;
          if (child.operation == LogicalOperator::kProjection &&
              child.children.size() == 1 && child.children[0] != group) {
            properties = &memo.Get(child.children[0]).logical_properties;
            columns.reserve(child.target_list.size());
            for (const auto& target : child.target_list) {
              if (target.expression &&
                  target.expression->Type() == TypeTag::kColumnValue) {
                columns.insert(target.expression->AsColumnValue()
                                   .GetColumnName()
                                   .ToString());
              }
            }
          } else {
            const Schema* schema = &child.output_schema;
            if (child.operation == LogicalOperator::kScan &&
                schema->ColumnCount() == 0) {
              const auto found = memo.GetTableSchemas().find(child.table);
              if (found != memo.GetTableSchemas().end()) {
                schema = &found->second;
              }
            }
            columns.reserve(schema->ColumnCount());
            for (size_t i = 0; i < schema->ColumnCount(); ++i) {
              columns.insert(schema->GetColumn(i).Name().ToString());
            }
          }
          if (!columns.empty() && properties->IsUniqueOn(columns)) {
            if (child.operation == LogicalOperator::kScan &&
                input_group.filter) {
              memo.MergeScanFilter(group, input_group.filter);
            }
            memo.AddExpression(group, child);
          }
        }
      },
      LogicalOperator::kDistinct));

  // not_null_is_not_null_elimination: Rewrite col IS NOT NULL to TRUE when
  // col has a NOT NULL constraint.
  // Lean proof: lean/TinylambOpt/SetJoinAndScalarRewrites.lean
  //   (not_null_and_trivial_selection_sound)
  built.Add(Rule(
      "not_null_is_not_null_elimination", Selection(Any("input")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kSelection ||
            expression.children.size() != 1 || !expression.predicate ||
            !*expression.predicate) {
          return;
        }
        const GroupId input_id = bindings.at("input");
        if (input_id == group) {
          return;
        }
        const Group& input_group = memo.Get(input_id);

        const auto conjuncts = SplitConjuncts(*expression.predicate);
        std::vector<Expression> kept;
        bool changed = false;
        for (const auto& conj : conjuncts) {
          if (conj && conj->Type() == TypeTag::kUnaryExp &&
              conj->AsUnaryExpression().Op() == UnaryOperation::kIsNotNull) {
            const auto& child = conj->AsUnaryExpression().Child();
            if (child && child->Type() == TypeTag::kColumnValue) {
              const ColumnName& col_name =
                  child->AsColumnValue().GetColumnName();
              if (input_group.logical_properties.IsNotNull(
                      col_name.ToString())) {
                changed = true;
                continue;
              }
            }
          }
          kept.push_back(conj);
        }

        if (changed) {
          if (kept.empty()) {
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = {input_id},
                                  .predicate = ConstantValueExp(Value(true)),
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema});
          } else {
            memo.AddExpression(
                group,
                LogicalExpression{
                    .operation = LogicalOperator::kSelection,
                    .children = {input_id},
                    .predicate = CanonicalizeConjuncts(CombineConjuncts(kept)),
                    .target_list = expression.target_list,
                    .output_schema = expression.output_schema});
          }
        }
      },
      LogicalOperator::kSelection));

  // join_identity_dummy: CrossJoin(X, DummyScan) -> X. DummyScan yields
  // exactly one row with no columns, so the cross product preserves X
  // row-for-row (and symmetrically for a leading DummyScan). Inner joins
  // without a predicate reach this shape through
  // join_to_cross_if_no_predicate, so only the CrossJoin form is needed.
  // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
  //   (core_join_identity_and_empty_sound)
  built.Add(Rule(
      "join_identity_dummy", CrossJoin(Any("left"), Any("right")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kCrossJoin ||
            expression.children.size() != 2) {
          return;
        }
        const GroupId left_id = bindings.at("left");
        const GroupId right_id = bindings.at("right");
        const auto is_dummy = [&](GroupId id) {
          return std::ranges::any_of(
              memo.Get(id).expressions, [](const LogicalExpression& candidate) {
                return candidate.operation == LogicalOperator::kDummyScan;
              });
        };
        const bool left_dummy = is_dummy(left_id);
        const bool right_dummy = is_dummy(right_id);
        if (left_dummy == right_dummy) {
          return;
        }
        const GroupId survivor = left_dummy ? right_id : left_id;
        const Group& survivor_group = memo.Get(survivor);
        if (survivor_group.relations != memo.Get(group).relations) {
          return;
        }
        // The copied alternatives implement against this group's filter,
        // so a scan filter living only on the survivor must move with them
        // (same contract as eliminate_true_selection). A non-null filter
        // implies a single-relation survivor and this unary reduction
        // preserves the relation set, so the destination is single too.
        if (survivor != group && survivor_group.filter &&
            memo.Get(group).relations.size() == 1) {
          // Same contract as eliminate_true_selection: carry the survivor's
          // branch-local mark with the filter so it is never later merged
          // into a scan shared by another set-op branch.
          InheritBranchLocal(memo, group, survivor);
          memo.MergeScanFilter(group, survivor_group.filter);
        }
        for (const LogicalExpression& alternative :
             survivor_group.expressions) {
          bool cycle = false;
          for (GroupId child : alternative.children) {
            if (child == group) {
              cycle = true;
              break;
            }
          }
          if (!cycle) {
            memo.AddExpression(group, alternative);
          }
        }
      },
      LogicalOperator::kCrossJoin));

  // count_star_rewrite_on_not_null: COUNT(col) -> COUNT(*) when col is
  // provably NOT NULL. COUNT skips NULLs, so on a non-nullable column it
  // counts exactly the input rows and saves per-row null checks.
  // The input group must retain a derived not-null proof: a catalog
  // constraint alone does not survive NULL extension or projection.
  // Aggregates with DISTINCT,
  // FILTER, HAVING modifiers or inner ORDER BY / LIMIT keep their shape.
  // Lean proof: lean/TinylambOpt/NullRejectionAndGroupRewrites.lean
  //   (count_star_rewrite_on_not_null_sound)
  built.Add(Rule(
      "count_star_rewrite_on_not_null", Aggregation(Any("input")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kAggregation ||
            expression.children.size() != 1 || expression.target_list.empty()) {
          return;
        }
        const GroupId input_id = bindings.at("input");
        if (input_id == group) {
          return;
        }
        const Group& input_group = memo.Get(input_id);
        bool changed = false;
        std::vector<NamedExpression> rewritten_targets;
        rewritten_targets.reserve(expression.target_list.size());
        for (const NamedExpression& target : expression.target_list) {
          if (!target.expression ||
              target.expression->Type() != TypeTag::kAggregateExp) {
            rewritten_targets.push_back(target);
            continue;
          }
          const auto& agg = target.expression->AsAggregateExpression();
          if (agg.GetType() != AggregationType::kCount || agg.Distinct() ||
              agg.WhereFilter() ||
              agg.Having() != AggregateHavingModifier::kNone ||
              !agg.InnerOrderBy().empty() || agg.InnerLimit().has_value() ||
              !agg.TrailingArgs().empty() || agg.SecondaryArg() ||
              !agg.Child() || agg.Child()->Type() != TypeTag::kColumnValue) {
            rewritten_targets.push_back(target);
            continue;
          }
          const ColumnName& column =
              agg.Child()->AsColumnValue().GetColumnName();
          if (column.schema.empty()) {
            rewritten_targets.push_back(target);
            continue;
          }
          // The input group is the proof boundary: a base declaration
          // does not survive a NULL-supplying outer-join side.
          if (!input_group.logical_properties.IsNotNull(column.ToString())) {
            rewritten_targets.push_back(target);
            continue;
          }
          NamedExpression star = target;
          star.expression =
              AggregateExpressionExp(AggregationType::kCount, nullptr, false);
          rewritten_targets.push_back(std::move(star));
          changed = true;
        }
        if (!changed) {
          return;
        }
        LogicalExpression rewritten = expression;
        rewritten.target_list = std::move(rewritten_targets);
        memo.AddExpression(group, std::move(rewritten));
      },
      LogicalOperator::kAggregation));

  // topn_push_through_inner_join is DISABLED (see the disabled-rules table
  // in docs/cascades_optimizer.md): pushing TopN below an inner join
  // requires proof that every left row finds a match, but uniqueness on
  // the right join keys only bounds the match count at one. The unmatched
  // left row inside the top-(limit+offset) prefix is that DISABLED rule's
  // counterexample, not a gate of the enabled rule below. Proving
  // ">= 1 match" needs referential-integrity metadata the memo does not
  // model.

  // topn_push_through_proven_one_to_one_join:
  // TopN(keys, limit, offset) over an inner Join(L, R) gains an alternative
  // Join(TopN(keys, limit+offset), R) when the join is proven 1:1 from the
  // pushed side S (every S row matches exactly one other-side row) and the
  // TopN keys are evaluable on S alone. Then the global top-(limit+offset)
  // joined rows correspond 1:1 to S's top-(limit+offset) rows, and the
  // parent TopN(limit, offset) re-establishes the exact output.
  // The 1:1 proof is either a declared FOREIGN KEY from S to the other
  // side (scan-free catalog proof)
  // or a snapshot proof recorded by the optimizer (Memo::IsProvenOneToOne).
  // Uniqueness alone never suffices (see the disabled rule above); WITH
  // TIES is excluded because tied peers can overflow the pushed cap.
  // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
  //   (topn_push_through_proven_one_to_one_join_sound,
  //    topn_push_inner_join_without_fk_unsound)
  built.Add(Rule(
      "topn_push_through_proven_one_to_one_join", TopN(Any("input")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kTopN ||
            expression.children.size() != 1 || expression.with_ties ||
            expression.prior_limit_offset != 0) {
          return;
        }
        if (expression.limit_offset >
            std::numeric_limits<size_t>::max() - expression.limit_count) {
          return;
        }
        const size_t total_limit =
            expression.limit_count + expression.limit_offset;
        if (total_limit == 0 || expression.target_list.empty() ||
            expression.target_list.size() != expression.sort_ascending.size()) {
          return;
        }
        // Snapshot everything the rewrite needs before mutating the memo:
        // AddExpression/EnsureDerivedGroup may reallocate group storage,
        // so no Group/expression reference may survive past them.
        const std::vector<NamedExpression> topn_keys = expression.target_list;
        const std::vector<bool> topn_ascending = expression.sort_ascending;
        const std::vector<std::optional<bool>> topn_nulls_first =
            expression.sort_nulls_first;
        const size_t topn_limit = expression.limit_count;
        const size_t topn_offset = expression.limit_offset;
        const GroupId input_id = bindings.at("input");
        const std::vector<LogicalExpression> join_candidates =
            memo.Get(input_id).expressions;
        const std::vector<std::string> input_relations =
            memo.Get(input_id).relations;
        for (const LogicalExpression& join_expr : join_candidates) {
          if (join_expr.operation != LogicalOperator::kJoin ||
              join_expr.children.size() != 2 ||
              !join_expr.predicate.has_value() || !*join_expr.predicate) {
            continue;
          }
          const GroupId left_id = join_expr.children[0];
          const GroupId right_id = join_expr.children[1];
          const std::vector<std::string> left_relations =
              memo.Get(left_id).relations;
          const std::vector<std::string> right_relations =
              memo.Get(right_id).relations;
          // v1 scope: two single-relation sides (the optimizer only marks
          // proofs for two-relation queries).
          if (left_relations.size() != 1 || right_relations.size() != 1) {
            continue;
          }
          const Expression join_predicate = *join_expr.predicate;
          for (int side = 0; side < 2; ++side) {
            const GroupId pushed_id = side == 0 ? left_id : right_id;
            const GroupId other_id = side == 0 ? right_id : left_id;
            const std::string pushed_rel =
                side == 0 ? left_relations.front() : right_relations.front();
            const std::string other_rel =
                side == 0 ? right_relations.front() : left_relations.front();
            // TopN keys must resolve to the pushed side alone; unqualified
            // names cannot be proven local (strict interpretation, same as
            // PayloadConstraint::predicate_within_child).
            bool keys_local = true;
            for (const NamedExpression& key : topn_keys) {
              if (!key.expression) {
                keys_local = false;
                break;
              }
              for (const ColumnName& column :
                   key.expression->TouchedColumns()) {
                if (column.schema.empty() || column.schema != pushed_rel) {
                  keys_local = false;
                  break;
                }
              }
              if (!keys_local) {
                break;
              }
            }
            if (!keys_local) {
              continue;
            }
            // Collect equi-join columns per side.
            std::unordered_set<std::string> pushed_join_cols;
            std::unordered_set<std::string> other_join_cols;
            size_t equi_join_count = 0;
            const std::vector<Expression> join_conjuncts =
                SplitConjuncts(join_predicate);
            pushed_join_cols.reserve(join_conjuncts.size() * 2);
            other_join_cols.reserve(join_conjuncts.size() * 2);
            for (const Expression& conjunct : join_conjuncts) {
              if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
                continue;
              }
              const auto& binary = conjunct->AsBinaryExpression();
              if (binary.Op() != BinaryOperation::kEquals ||
                  binary.Left()->Type() != TypeTag::kColumnValue ||
                  binary.Right()->Type() != TypeTag::kColumnValue) {
                continue;
              }
              const ColumnName& left_col =
                  binary.Left()->AsColumnValue().GetColumnName();
              const ColumnName& right_col =
                  binary.Right()->AsColumnValue().GetColumnName();
              const ColumnName* pushed_col = nullptr;
              const ColumnName* other_col = nullptr;
              if (left_col.schema == pushed_rel &&
                  right_col.schema == other_rel) {
                pushed_col = &left_col;
                other_col = &right_col;
              } else if (left_col.schema == other_rel &&
                         right_col.schema == pushed_rel) {
                pushed_col = &right_col;
                other_col = &left_col;
              } else {
                continue;
              }
              InsertColumnMatcherNames(pushed_join_cols, *pushed_col);
              InsertColumnMatcherNames(other_join_cols, *other_col);
              ++equi_join_count;
            }
            if (pushed_join_cols.empty() || other_join_cols.empty() ||
                equi_join_count != join_conjuncts.size()) {
              continue;
            }
            // At most one match per pushed row: the other-side join keys
            // are unique and NOT NULL (NULL keys never match, and a
            // nullable UNIQUE admits duplicate NULLs, so uniqueness alone
            // is not a proof; mirrors DeriveLogicalProperties).
            if (!memo.Get(other_id).logical_properties.IsUniqueOn(
                    other_join_cols)) {
              continue;
            }
            bool other_not_null = true;
            for (const std::string& column : other_join_cols) {
              if (!memo.Get(other_id).logical_properties.IsNotNull(column)) {
                other_not_null = false;
                break;
              }
            }
            if (!other_not_null) {
              continue;
            }
            // The declared-FK fallback below is scan-free: it trusts the
            // catalog, so it cannot look at the rows that actually survive
            // on the other side. Any single-relation predicate of the query
            // already lives in the other-side group's scan filter
            // (push_selection_through_join merges it there), and such a
            // filter can remove exactly the row the FOREIGN KEY
            // certificate guarantees -- `WHERE b.v > 5` on b drops the
            // guaranteed parent, so a fact row inside the pushed
            // top-(limit+offset) prefix would emit a row the plain join
            // filters away. The other side must therefore be
            // filter-free for the FK fallback; the snapshot proof
            // (MarkProvenOneToOneJoins) is the filter-aware alternative,
            // and its own WHERE gate keeps it to predicates that cannot
            // rescue a filtered other side.
            if (memo.Get(other_id).filter) {
              continue;
            }
            // At least one match per pushed row: FOREIGN KEY or snapshot.
            bool proven = memo.IsProvenOneToOne(pushed_rel, other_rel);
            if (!proven) {
              const std::vector<LogicalExpression> pushed_exprs =
                  memo.Get(pushed_id).expressions;
              for (const auto& candidate : pushed_exprs) {
                for (size_t i = 0; i < candidate.output_schema.ColumnCount();
                     ++i) {
                  const auto& column = candidate.output_schema.GetColumn(i);
                  if (!pushed_join_cols.contains(column.Name().ToString()) &&
                      !pushed_join_cols.contains(column.Name().name)) {
                    continue;
                  }
                  if (column.GetConstraint().ctype != Constraint::kForeign) {
                    continue;
                  }
                  std::string ref_table;
                  std::vector<std::string> ref_columns;
                  if (!ParseForeignKeyReference(column.GetConstraint(),
                                                &ref_table, &ref_columns)) {
                    continue;
                  }
                  bool references_join_keys =
                      ref_columns.size() == join_conjuncts.size();
                  for (const std::string& ref_column : ref_columns) {
                    references_join_keys = references_join_keys &&
                                           other_join_cols.contains(ref_column);
                  }
                  if (ref_table == other_rel && references_join_keys) {
                    // The FK certificate covers only non-NULL values
                    // (NULL keys match nothing), so without a NOT NULL
                    // proof on the pushed side an unmatched row inside
                    // the top-(limit+offset) prefix drops rows the
                    // unpushed plan keeps -- the same counterexample
                    // that keeps push_down_limit_through_join disabled.
                    if (memo.Get(pushed_id).logical_properties.IsNotNull(
                            column.Name().ToString()) ||
                        memo.Get(pushed_id).logical_properties.IsNotNull(
                            column.Name().name)) {
                      proven = true;
                    }
                    break;
                  }
                }
                if (proven) {
                  break;
                }
              }
            }
            if (!proven) {
              continue;
            }
            // All reads done; memo mutations below use owned copies only.
            // Tags carry the keys and the join predicate: two TopNs with
            // different keys, or two joins with different predicates over
            // the same relations, must never share a derived group (D1:
            // one group, one meaning).
            std::string keys_sig;
            for (const NamedExpression& key : topn_keys) {
              AppendInteger(keys_sig, key.name.size());
              keys_sig.push_back(':');
              keys_sig.append(key.name);
              keys_sig.push_back('=');
              keys_sig.append(key.expression ? key.expression->ToString()
                                             : std::string("<null>"));
              keys_sig.push_back('|');
            }
            AppendInteger(keys_sig, topn_ascending.size());
            keys_sig.push_back(':');
            for (const bool ascending : topn_ascending) {
              keys_sig.push_back(ascending ? '1' : '0');
            }
            keys_sig.push_back('/');
            AppendInteger(keys_sig, topn_nulls_first.size());
            keys_sig.push_back(':');
            for (const std::optional<bool>& nulls_first : topn_nulls_first) {
              keys_sig.push_back(nulls_first.has_value()
                                     ? (nulls_first.value() ? '1' : '0')
                                     : '?');
            }
            const std::vector<std::string> pushed_relations =
                memo.Get(pushed_id).relations;
            std::string tag = "topn_proven_one_to_one:";
            AppendInteger(tag, total_limit);
            tag += ":";
            tag += pushed_rel;
            tag += ":";
            tag += other_rel;
            tag += ":";
            tag += keys_sig;
            const GroupId pushed_topn_group =
                memo.EnsureDerivedGroup(pushed_relations, tag);
            if (pushed_topn_group == pushed_id || pushed_topn_group == group) {
              continue;
            }
            memo.AddExpression(
                pushed_topn_group,
                LogicalExpression{.operation = LogicalOperator::kTopN,
                                  .children = {pushed_id},
                                  .target_list = topn_keys,
                                  .sort_ascending = topn_ascending,
                                  .sort_nulls_first = topn_nulls_first,
                                  .limit_count = total_limit,
                                  .limit_offset = 0});
            LogicalExpression pushed_join = join_expr;
            pushed_join.children =
                side == 0 ? std::vector<GroupId>{pushed_topn_group, other_id}
                          : std::vector<GroupId>{other_id, pushed_topn_group};
            std::string pushed_join_tag;
            pushed_join_tag += "topn_proven_join:";
            AppendInteger(pushed_join_tag, total_limit);
            pushed_join_tag += ':';
            pushed_join_tag += pushed_rel;
            pushed_join_tag += ':';
            pushed_join_tag += other_rel;
            pushed_join_tag += ':';
            pushed_join_tag += join_predicate->ToString();
            pushed_join_tag += ':';
            pushed_join_tag += keys_sig;
            const GroupId pushed_join_group =
                memo.EnsureDerivedGroup(input_relations, pushed_join_tag);
            if (pushed_join_group == group || pushed_join_group == input_id) {
              continue;
            }
            memo.AddExpression(pushed_join_group, std::move(pushed_join));
            memo.AddExpression(
                group, LogicalExpression{.operation = LogicalOperator::kTopN,
                                         .children = {pushed_join_group},
                                         .target_list = topn_keys,
                                         .sort_ascending = topn_ascending,
                                         .sort_nulls_first = topn_nulls_first,
                                         .limit_count = topn_limit,
                                         .limit_offset = topn_offset});
          }
        }
      },
      LogicalOperator::kTopN));

  // or_to_union (TODO.md item 4a): Selection(OR(d0, d1)) over a
  // relation set gains a UNION ALL alternative: UnionAll(Selection(d0),
  // Selection(d1)). Each branch can then use its own index range while
  // IS TRUE partition predicates preserve the OR multiset even when the
  // branches overlap. Gates: 2..8
  // disjuncts, stable branch predicates (no subquery/aggregate/window/
  // volatile duplication across the branches), and not the same-column-
  // equality shape that or_to_in already covers.
  // Branch tags are content-addressed so different ORs never share a
  // branch group (D1: one group, one meaning).
  // Lean proof: lean/TinylambOpt/ThreeValued.lean (or_branches_disjoint,
  //   or_branches_exact_count, naive_or_split_loses_null_row) &
  //   lean/TinylambOpt/BagRel.lean (or_to_union_preserves_bag)
  built.Add(Rule(
      "or_to_union", Selection(Any("input")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kSelection ||
            expression.children.size() != 1 || !expression.predicate ||
            !*expression.predicate) {
          return;
        }
        const Expression predicate = *expression.predicate;
        const std::vector<Expression> disjuncts =
            relational_detail::SplitDisjuncts(predicate);
        if (disjuncts.size() < 2 || disjuncts.size() > 8 ||
            std::ranges::any_of(
                disjuncts, [](const Expression& branch) { return !branch; })) {
          return;
        }
        const GroupId input_id = bindings.at("input");
        const std::vector<std::string> input_relations =
            memo.Get(input_id).relations;
        if (input_relations.empty()) {
          return;
        }
        auto stable_branch = [&](const Expression& branch) {
          if (!branch || !ExpressionCannotThrowForPlanner(branch)) {
            return false;
          }
          std::vector<Expression> stack{branch};
          while (!stack.empty()) {
            Expression current = std::move(stack.back());
            stack.pop_back();
            if (!current) {
              continue;
            }
            switch (current->Type()) {
              case TypeTag::kQueryExp:
              case TypeTag::kAggregateExp:
              case TypeTag::kWindowFunctionExp:
                return false;
              case TypeTag::kFunctionCallExp:
                if (GetFunctionVolatility(
                        current->AsFunctionCallExpression().FuncName()) !=
                    Volatility::kImmutable) {
                  return false;
                }
                break;
              default:
                break;
            }
            const auto& schemas = memo.GetTableSchemas();
            for (const ColumnName& column : current->TouchedColumns()) {
              if (!column.schema.empty()) {
                if (std::ranges::find(input_relations, column.schema) ==
                    input_relations.end()) {
                  return false;
                }
                continue;
              }
              // An unqualified column must also resolve to an input
              // relation: an outer reference would be re-bound per UNION
              // branch and could evaluate differently in each. Keep the
              // permissive behaviour when schemas are unavailable (e.g.
              // synthesized memos in unit tests).
              if (schemas.empty()) {
                continue;
              }
              size_t matches = 0;
              for (const auto& rel : input_relations) {
                const auto found = schemas.find(rel);
                if (found != schemas.end() &&
                    found->second.Offset(ColumnName("", column.name)) >= 0) {
                  ++matches;
                }
              }
              if (matches != 1) {
                return false;
              }
            }
            for (const Expression& child : ExpressionChildren(current)) {
              if (child) {
                stack.push_back(child);
              }
            }
          }
          return true;
        };
        if (std::ranges::any_of(disjuncts, [&](const Expression& branch) {
              return !stable_branch(branch);
            })) {
          return;
        }
        // Same-column constant equalities already fold to IN (or_to_in);
        // a union alternative would only add dedup overhead there.
        auto same_column_equality = [&](const Expression& branch,
                                        std::string* column) {
          if (!branch || branch->Type() != TypeTag::kBinaryExp) {
            return false;
          }
          const auto& binary = branch->AsBinaryExpression();
          if (binary.Op() != BinaryOperation::kEquals) {
            return false;
          }
          const Expression* column_side = nullptr;
          if (binary.Left()->Type() == TypeTag::kColumnValue &&
              binary.Right()->Type() == TypeTag::kConstantValue) {
            column_side = &binary.Left();
          } else if (binary.Right()->Type() == TypeTag::kColumnValue &&
                     binary.Left()->Type() == TypeTag::kConstantValue) {
            column_side = &binary.Right();
          } else {
            return false;
          }
          *column = (*column_side)->AsColumnValue().GetColumnName().ToString();
          return true;
        };
        std::string common_column;
        bool all_same_column = true;
        for (const Expression& branch : disjuncts) {
          std::string column;
          if (!same_column_equality(branch, &column)) {
            all_same_column = false;
            break;
          }
          if (common_column.empty()) {
            common_column = std::move(column);
          } else if (common_column != column) {
            all_same_column = false;
            break;
          }
        }
        if (all_same_column) {
          return;
        }
        std::vector<GroupId> branches;
        branches.reserve(disjuncts.size());
        for (size_t i = 0; i < disjuncts.size(); ++i) {
          Expression branch_predicate =
              UnaryExpressionExp(disjuncts[i], UnaryOperation::kIsTrue);
          for (size_t j = 0; j < i; ++j) {
            Expression prior_not_true = UnaryExpressionExp(
                UnaryExpressionExp(disjuncts[j], UnaryOperation::kIsTrue),
                UnaryOperation::kIsNotTrue);
            branch_predicate = BinaryExpressionExp(std::move(branch_predicate),
                                                   BinaryOperation::kAnd,
                                                   std::move(prior_not_true));
          }
          std::string branch_tag = "or_union_branch:";
          AppendInteger(branch_tag, input_id);
          branch_tag += ':';
          AppendInteger(branch_tag, i);
          branch_tag += ':';
          branch_tag += branch_predicate->ToString();
          const GroupId branch_group =
              memo.EnsureDerivedGroup(input_relations, branch_tag);
          memo.Get(branch_group).branch_local = true;
          if (branch_group == input_id || branch_group == group) {
            return;
          }
          memo.AddExpression(
              branch_group,
              LogicalExpression{.operation = LogicalOperator::kSelection,
                                .children = {input_id},
                                .predicate = std::move(branch_predicate)});
          branches.push_back(branch_group);
        }
        memo.AddExpression(
            group, LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                     .children = std::move(branches)});
      },
      LogicalOperator::kSelection));

  // distinct_eager_over_join : Distinct(Join(L, R)) gains
  // a Join(Distinct(L), R) alternative when the other side's join keys are
  // unique. Each pushed-side row then matches at most one other-side row,
  // so deduplicating before the join yields the same rows while shrinking
  // the join input. No containment proof is needed (unlike TopN/limit
  // pushes): unmatched rows simply fail to join in both shapes, and for
  // LEFT joins they null-pad identically after dedup. The outer-join
  // side loop is restricted to the preserved (left) side: deduplicating
  // the null-supplying side would collapse fan-out matches. v1 scope:
  // full-row DISTINCT directly above the join (a Projection in between
  // keeps the existing path).
  // Lean proof: lean/TinylambOpt/SetJoinAndScalarRewrites.lean
  //   (distinct_eager_over_join_sound,
  //    cpp_distinct_eager_self_eq_uniqueness_bug)
  built.Add(Rule(
      "distinct_eager_over_join", Distinct(Any("input")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kDistinct ||
            expression.children.size() != 1) {
          return;
        }
        const GroupId input_id = bindings.at("input");
        const std::vector<LogicalExpression> join_candidates =
            memo.Get(input_id).expressions;
        const std::vector<std::string> input_relations =
            memo.Get(input_id).relations;
        for (const LogicalExpression& join_expr : join_candidates) {
          const bool is_inner = join_expr.operation == LogicalOperator::kJoin;
          const bool is_left_outer =
              join_expr.operation == LogicalOperator::kOuterJoin &&
              join_expr.join_type == 0;
          if ((!is_inner && !is_left_outer) || join_expr.children.size() != 2 ||
              !join_expr.predicate.has_value() || !*join_expr.predicate) {
            continue;
          }
          const GroupId left_id = join_expr.children[0];
          const GroupId right_id = join_expr.children[1];
          const std::vector<std::string> left_relations =
              memo.Get(left_id).relations;
          const std::vector<std::string> right_relations =
              memo.Get(right_id).relations;
          if (left_relations.size() != 1 || right_relations.size() != 1) {
            continue;
          }
          const Expression join_predicate = *join_expr.predicate;
          const int side_limit = is_inner ? 2 : 1;
          const std::vector<Expression> join_conjuncts =
              SplitConjuncts(join_predicate);
          for (int side = 0; side < side_limit; ++side) {
            const GroupId pushed_id = side == 0 ? left_id : right_id;
            const GroupId other_id = side == 0 ? right_id : left_id;
            const std::string pushed_rel =
                side == 0 ? left_relations.front() : right_relations.front();
            const std::string other_rel =
                side == 0 ? right_relations.front() : left_relations.front();
            std::unordered_set<std::string> other_join_cols;
            other_join_cols.reserve(join_conjuncts.size() * 2);
            for (const Expression& conjunct : join_conjuncts) {
              if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
                continue;
              }
              const auto& binary = conjunct->AsBinaryExpression();
              if (binary.Op() != BinaryOperation::kEquals ||
                  binary.Left()->Type() != TypeTag::kColumnValue ||
                  binary.Right()->Type() != TypeTag::kColumnValue) {
                continue;
              }
              const ColumnName& lhs_col =
                  binary.Left()->AsColumnValue().GetColumnName();
              const ColumnName& rhs_col =
                  binary.Right()->AsColumnValue().GetColumnName();
              if (lhs_col.schema == pushed_rel && rhs_col.schema == other_rel) {
                InsertColumnMatcherNames(other_join_cols, rhs_col);
              } else if (lhs_col.schema == other_rel &&
                         rhs_col.schema == pushed_rel) {
                InsertColumnMatcherNames(other_join_cols, lhs_col);
              }
            }
            if (other_join_cols.empty() ||
                !memo.Get(other_id).logical_properties.IsUniqueOn(
                    other_join_cols)) {
              continue;
            }
            // Tags omit the predicate for the pushed Distinct (its meaning
            // depends only on the pushed side) but carry it for the pushed
            // join: different predicates over the same relations must
            // never share a join group (D1).
            const std::vector<std::string> pushed_relations =
                memo.Get(pushed_id).relations;
            std::string distinct_tag = "distinct_eager:";
            distinct_tag += pushed_rel;
            distinct_tag.push_back(':');
            AppendInteger(distinct_tag, pushed_id);
            const GroupId pushed_distinct =
                memo.EnsureDerivedGroup(pushed_relations, distinct_tag);
            if (pushed_distinct == pushed_id || pushed_distinct == group) {
              continue;
            }
            memo.AddExpression(
                pushed_distinct,
                LogicalExpression{.operation = LogicalOperator::kDistinct,
                                  .children = {pushed_id}});
            LogicalExpression pushed_join = join_expr;
            pushed_join.children =
                side == 0 ? std::vector<GroupId>{pushed_distinct, other_id}
                          : std::vector<GroupId>{other_id, pushed_distinct};
            // The tag must identify the CHILDREN as well: with only the
            // relation names and predicate, two joins with different child
            // groups (e.g. one carrying an extra filter) share one derived
            // group and mix each other's alternatives.
            const GroupId pushed_join_group = memo.EnsureDerivedGroup(
                input_relations, "distinct_eager_join:" + pushed_rel + ":" +
                                     join_predicate->ToString() + ":" +
                                     GroupIdsTag(pushed_join.children));
            if (pushed_join_group == group || pushed_join_group == input_id) {
              continue;
            }
            memo.AddExpression(pushed_join_group, std::move(pushed_join));
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kDistinct,
                                  .children = {pushed_join_group}});
          }
        }
      },
      LogicalOperator::kDistinct));

  // topn_push_through_union_all (the TopN pushdown contract): TopN(keys,
  // limit, offset) over UnionAll gains a UnionAll(TopN(keys,
  // limit+offset)...) alternative keeping the parent TopN. Every branch's
  // top-(limit+offset) contains all its rows that can appear in the global
  // top, so the parent re-establishes the exact output (D6: the parent TopN
  // keeps establishing the order). WITH TIES is excluded (tied peers overflow
  // the pushed cap). Branch tags carry the branch GroupId, ordering
  // signature, and union fingerprint so different UNION ALLs never share a
  // capped branch group (D1).
  // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
  //   (topn_push_through_union_all_sound)
  built.Add(Rule(
      "topn_push_through_union_all", TopN(Any("input")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kTopN ||
            expression.children.size() != 1 || expression.with_ties ||
            expression.prior_limit_offset != 0) {
          return;
        }
        if (expression.limit_offset >
            std::numeric_limits<size_t>::max() - expression.limit_count) {
          return;
        }
        const size_t total_limit =
            expression.limit_count + expression.limit_offset;
        if (total_limit == 0 || expression.target_list.empty() ||
            expression.target_list.size() != expression.sort_ascending.size()) {
          return;
        }
        const std::vector<NamedExpression> topn_keys = expression.target_list;
        const std::vector<bool> topn_ascending = expression.sort_ascending;
        const std::vector<std::optional<bool>> topn_nulls_first =
            expression.sort_nulls_first;
        std::string topn_key_signature;
        for (const NamedExpression& key : topn_keys) {
          AppendInteger(topn_key_signature, key.name.size());
          topn_key_signature.push_back(':');
          topn_key_signature.append(key.name);
          topn_key_signature.push_back('=');
          topn_key_signature.append(key.expression ? key.expression->ToString()
                                                   : "<null>");
          topn_key_signature.push_back('|');
        }
        AppendInteger(topn_key_signature, topn_ascending.size());
        topn_key_signature.push_back(':');
        for (const bool ascending : topn_ascending) {
          topn_key_signature.push_back(ascending ? '1' : '0');
        }
        topn_key_signature.push_back('/');
        AppendInteger(topn_key_signature, topn_nulls_first.size());
        topn_key_signature.push_back(':');
        for (const std::optional<bool>& nulls_first : topn_nulls_first) {
          topn_key_signature.push_back(nulls_first.has_value()
                                           ? (nulls_first.value() ? '1' : '0')
                                           : '?');
        }
        const size_t topn_limit = expression.limit_count;
        const size_t topn_offset = expression.limit_offset;
        auto keys_match = [&](const LogicalExpression& candidate) {
          if (candidate.target_list.size() != topn_keys.size()) {
            return false;
          }
          for (size_t i = 0; i < topn_keys.size(); ++i) {
            if (candidate.target_list[i].name != topn_keys[i].name) {
              return false;
            }
            const std::string left = topn_keys[i].expression
                                         ? topn_keys[i].expression->ToString()
                                         : std::string("<null>");
            const std::string right =
                candidate.target_list[i].expression
                    ? candidate.target_list[i].expression->ToString()
                    : std::string("<null>");
            if (left != right) {
              return false;
            }
          }
          return candidate.sort_ascending == topn_ascending &&
                 candidate.sort_nulls_first == topn_nulls_first;
        };
        const GroupId input_id = bindings.at("input");
        const std::vector<LogicalExpression> union_candidates =
            memo.Get(input_id).expressions;
        const std::vector<std::string> input_relations =
            memo.Get(input_id).relations;
        for (const LogicalExpression& union_expr : union_candidates) {
          if (union_expr.operation != LogicalOperator::kUnionAll ||
              union_expr.children.size() < 2) {
            continue;
          }
          std::vector<GroupId> capped_children;
          capped_children.reserve(union_expr.children.size());
          bool skip = false;
          for (const GroupId child_id : union_expr.children) {
            for (const LogicalExpression& existing :
                 memo.Get(child_id).expressions) {
              if (existing.operation == LogicalOperator::kTopN &&
                  existing.limit_offset == 0 &&
                  existing.limit_count <= total_limit && keys_match(existing)) {
                skip = true;
                break;
              }
            }
            if (skip) {
              break;
            }
            std::string child_tag = "topn_union_child:";
            AppendInteger(child_tag, child_id);
            child_tag += ':';
            AppendInteger(child_tag, total_limit);
            child_tag += ':';
            child_tag += topn_key_signature;
            const GroupId capped = memo.EnsureDerivedGroup(
                memo.Get(child_id).relations, child_tag);
            if (capped == child_id || capped == group) {
              skip = true;
              break;
            }
            memo.AddExpression(
                capped, LogicalExpression{.operation = LogicalOperator::kTopN,
                                          .children = {child_id},
                                          .target_list = topn_keys,
                                          .sort_ascending = topn_ascending,
                                          .sort_nulls_first = topn_nulls_first,
                                          .limit_count = total_limit,
                                          .limit_offset = 0});
            capped_children.push_back(capped);
          }
          if (skip) {
            continue;
          }
          LogicalExpression capped_union = union_expr;
          capped_union.children = std::move(capped_children);
          std::string pushed_tag = "topn_union_pushed:";
          AppendInteger(pushed_tag, total_limit);
          pushed_tag.push_back(':');
          pushed_tag += topn_key_signature;
          pushed_tag.push_back(':');
          pushed_tag += union_expr.Fingerprint();
          const GroupId capped_union_group =
              memo.EnsureDerivedGroup(input_relations, pushed_tag);
          if (capped_union_group == group || capped_union_group == input_id) {
            continue;
          }
          memo.AddExpression(capped_union_group, std::move(capped_union));
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kTopN,
                                       .children = {capped_union_group},
                                       .target_list = topn_keys,
                                       .sort_ascending = topn_ascending,
                                       .sort_nulls_first = topn_nulls_first,
                                       .limit_count = topn_limit,
                                       .limit_offset = topn_offset});
        }
      },
      LogicalOperator::kTopN));

  // split_window: Window computing functions over incompatible PARTITION BY
  // specs -> stacked homogeneous Windows. Each Window node carries a single
  // partition_by payload, so mixed specs are split into maximal CONTIGUOUS
  // runs of identical partition specs: non-contiguous grouping (e.g.
  // [w0(p), w1(q), w2(p)] -> [w0, w2] + [w1]) would permute output columns
  // inside the same Memo group.
  // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
  //   (window_stack_and_merge_sound,
  //    cpp_split_window_noncontiguous_swaps_columns_bug)
  built.Add(Rule(
      "split_window", Window(Any("input")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kWindow ||
            expression.children.size() != 1 ||
            expression.target_list.size() < 2) {
          return;
        }
        const GroupId input_id = bindings.at("input");
        if (input_id == group) {
          return;
        }
        std::vector<std::pair<std::string, std::vector<size_t>>> runs;
        runs.reserve(expression.target_list.size());
        for (size_t i = 0; i < expression.target_list.size(); ++i) {
          const NamedExpression& target = expression.target_list[i];
          if (!target.expression ||
              target.expression->Type() != TypeTag::kWindowFunctionExp) {
            return;
          }
          const auto& call = dynamic_cast<
              const WindowFunctionCallExpression&>(  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
              *target.expression);
          std::string spec;
          AppendInteger(spec, call.partition_by.size());
          spec.push_back(':');
          for (const Expression& part : call.partition_by) {
            const std::string text = part ? part->ToString() : "<null>";
            AppendInteger(spec, text.size());
            spec.push_back('/');
            spec.append(text);
            spec.push_back(';');
          }
          if (runs.empty() || runs.back().first != spec) {
            runs.emplace_back(std::move(spec), std::vector<size_t>{i});
          } else {
            runs.back().second.push_back(i);
          }
        }
        if (runs.size() < 2) {
          return;
        }
        // Copy by value: EnsureDerivedGroup below may reallocate
        // memo.groups_.
        const std::vector<std::string> relations = memo.Get(input_id).relations;
        GroupId current = input_id;
        for (size_t s = 0; s < runs.size(); ++s) {
          const bool outermost = (s + 1 == runs.size());
          std::vector<NamedExpression> level_targets;
          level_targets.reserve(runs[s].second.size());
          std::vector<Expression> level_partition;
          for (size_t index : runs[s].second) {
            level_targets.push_back(expression.target_list[index]);
            if (level_partition.empty()) {
              const auto& call = dynamic_cast<
                  const WindowFunctionCallExpression&>(  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
                  *expression.target_list[index].expression);
              level_partition = call.partition_by;
            }
          }
          if (outermost) {
            LogicalExpression outer = expression;
            outer.children = {current};
            outer.target_list = std::move(level_targets);
            outer.partition_by = std::move(level_partition);
            memo.AddExpression(group, std::move(outer));
          } else {
            std::string signature = "window-split:" + GroupIdsTag({current});
            for (const NamedExpression& target : level_targets) {
              signature.append(target.name);
              signature.push_back('=');
              signature.append(target.expression->ToString());
              signature.push_back('|');
            }
            const GroupId level = memo.EnsureDerivedGroup(relations, signature);
            if (level == group || level == current) {
              return;
            }
            memo.AddExpression(
                level,
                LogicalExpression{.operation = LogicalOperator::kWindow,
                                  .children = {current},
                                  .target_list = std::move(level_targets),
                                  .partition_by = std::move(level_partition)});
            current = level;
          }
        }
      },
      LogicalOperator::kWindow));

  // values_fold_into_union: UnionAll over single-row Values branches -> a
  // single Values node concatenating every branch row. UNION ALL is bag
  // concatenation, and equal-width branches share the set operation's
  // positional column contract, so the fold preserves the multiset -- but
  // a real UNION ALL also coerces every branch to the set-op result type,
  // while ValuesExecutor emits rows verbatim. Every branch column type
  // must therefore match the first branch's declared type, or the folded
  // node would claim INT64 while carrying a DOUBLE value
  // (`SELECT 1 UNION ALL SELECT 2.5`). Only single-row branches fold:
  // collapsing multi-row branches would destroy the merge/parallelism
  // alternatives (MergeAppend over individually ordered branches) that
  // costing otherwise exploits.
  // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
  //   (unnest_and_values_rules_sound)
  built.Add(Rule(
      "values_fold_into_union", UnionAll(),
      [](const Bindings&, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kUnionAll ||
            expression.children.size() < 2) {
          return;
        }
        std::vector<Row> folded;
        size_t width = 0;
        Schema folded_schema;
        bool first = true;
        for (GroupId child : expression.children) {
          bool found_values = false;
          for (const LogicalExpression& branch : memo.Get(child).expressions) {
            if (branch.operation != LogicalOperator::kValues) {
              continue;
            }
            if (first) {
              width = branch.output_schema.ColumnCount();
              folded_schema = branch.output_schema;
              first = false;
            }
            if (branch.output_schema.ColumnCount() != width ||
                branch.values.size() != 1) {
              return;
            }
            for (size_t col = 0; col < width; ++col) {
              if (branch.output_schema.GetColumn(col).Type() !=
                  folded_schema.GetColumn(col).Type()) {
                // Set-op type promotion would coerce this branch; folding
                // it verbatim under the first branch's schema would not.
                return;
              }
            }
            found_values = true;
            for (const Row& row : branch.values) {
              folded.push_back(row);
            }
            break;
          }
          if (!found_values) {
            return;
          }
        }
        LogicalExpression merged = expression;
        merged.operation = LogicalOperator::kValues;
        merged.children = {};
        merged.values = std::move(folded);
        merged.output_schema = std::move(folded_schema);
        memo.AddExpression(group, std::move(merged));
      },
      LogicalOperator::kUnionAll));

  // cast_pushdown_on_comparison: CAST(col AS T) <op> const ->
  // col <op> const' when the cast is a provably injective, order-preserving,
  // total map over col's catalog domain. The rewritten column side is
  // sargable (index range extraction sees a bare column), while the
  // original is not. Soundness rests on three gates: (1) an allowlist of
  // domain pairs (same-domain redundancy, DATE -> TIMESTAMP), so lossy maps
  // such as INT64 -> FLOAT64 never rewrite; (2) a constant round-trip
  // proof the back/round_trip expressions reproduce const, so
  // the boundary value survives the domain change exactly; (3) totality of
  // the allowlisted maps, so no row can throw in the original and vanish
  // in the rewrite. Only single-relation selections are rewritten; joins
  // keep their conjuncts for NewJoin canonicalization.
  // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
  //   (cast_pushdown_on_comparison_sound, cpp_parameter_slot_freeze_bug)
  built.Add(Rule(
      "cast_pushdown_on_comparison", Selection(Any("input")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kSelection ||
            expression.children.size() != 1 || !expression.predicate ||
            !*expression.predicate) {
          return;
        }
        const GroupId input_id = bindings.at("input");
        if (input_id == group) {
          return;
        }
        const Group& input_group = memo.Get(input_id);
        if (input_group.relations.size() != 1) {
          return;
        }
        const std::string& relation = input_group.relations.front();
        const auto& schemas = memo.GetTableSchemas();
        const auto schema_found = schemas.find(relation);
        if (schema_found == schemas.end()) {
          return;
        }
        const Schema& schema = schema_found->second;
        bool changed = false;
        std::vector<Expression> rewritten_conjuncts;
        for (const Expression& conjunct :
             SplitConjuncts(*expression.predicate)) {
          Expression rewritten = conjunct;
          if (conjunct && conjunct->Type() == TypeTag::kBinaryExp) {
            const auto& binary = conjunct->AsBinaryExpression();
            const BinaryOperation op = binary.Op();
            const bool comparable = op == BinaryOperation::kEquals ||
                                    op == BinaryOperation::kNotEquals ||
                                    op == BinaryOperation::kLessThan ||
                                    op == BinaryOperation::kLessThanEquals ||
                                    op == BinaryOperation::kGreaterThan ||
                                    op == BinaryOperation::kGreaterThanEquals;
            const Expression* cast_side = nullptr;
            const Expression* const_side = nullptr;
            bool cast_on_left = false;
            if (binary.Left()->Type() == TypeTag::kCastExp &&
                binary.Right()->Type() == TypeTag::kConstantValue) {
              cast_side = &binary.Left();
              const_side = &binary.Right();
              cast_on_left = true;
            } else if (binary.Right()->Type() == TypeTag::kCastExp &&
                       binary.Left()->Type() == TypeTag::kConstantValue) {
              cast_side = &binary.Right();
              const_side = &binary.Left();
              cast_on_left = false;
            }
            if (comparable && cast_side != nullptr && *cast_side &&
                *const_side && !ContainsParameterSlot(*const_side)) {
              const auto& cast = (*cast_side)->AsCastExpression();
              const Value constant =
                  (*const_side)->AsConstantValue().GetValue();
              if (!constant.IsNull() && cast.Child() &&
                  cast.Child()->Type() == TypeTag::kColumnValue) {
                const ColumnName& column =
                    cast.Child()->AsColumnValue().GetColumnName();
                if (!column.schema.empty() &&
                    (column.schema == relation ||
                     IsSameTable(column.schema, relation))) {
                  const int offset = schema.Offset(ColumnName("", column.name));
                  if (offset >= 0) {
                    const ValueType source =
                        schema.GetColumn(static_cast<size_t>(offset)).Type();
                    std::string upper;
                    for (char c : cast.TargetTypeName()) {
                      upper.push_back(static_cast<char>(
                          std::toupper(static_cast<unsigned char>(c))));
                    }
                    const bool redundant =
                        (source == ValueType::kInt64 &&
                         (upper == "INT64" || upper == "INT" ||
                          upper == "INTEGER")) ||
                        (source == ValueType::kVarChar &&
                         (upper == "STRING" || upper == "VARCHAR" ||
                          upper == "TEXT")) ||
                        (source == ValueType::kDouble &&
                         (upper == "FLOAT64" || upper == "DOUBLE" ||
                          upper == "FLOAT")) ||
                        (source == ValueType::kDate && upper == "DATE");
                    const bool date_to_timestamp =
                        source == ValueType::kDate &&
                        (upper == "TIMESTAMP" || upper == "DATETIME");
                    if (redundant || date_to_timestamp) {
                      std::string_view back_name =
                          source == ValueType::kInt64     ? "INT64"
                          : source == ValueType::kVarChar ? "STRING"
                          : source == ValueType::kDouble  ? "FLOAT64"
                                                          : "DATE";
                      const Expression back =
                          CastExpressionExp(*const_side, std::string(back_name),
                                            cast.ReturnNullOnError());
                      const StatusOr<Value> folded =
                          back->TryEvaluate(Row(), Schema());
                      if (folded.HasValue() && !folded.Value().IsNull()) {
                        const Expression round_trip = CastExpressionExp(
                            ConstantValueExp(folded.Value()),
                            cast.TargetTypeName(), cast.ReturnNullOnError());
                        const StatusOr<Value> round_folded =
                            round_trip->TryEvaluate(Row(), Schema());
                        if (round_folded.HasValue() &&
                            round_folded.Value() == constant) {
                          const Expression narrowed =
                              ConstantValueExp(folded.Value());
                          rewritten = cast_on_left
                                          ? BinaryExpressionExp(cast.Child(),
                                                                op, narrowed)
                                          : BinaryExpressionExp(narrowed, op,
                                                                cast.Child());
                          changed = true;
                        }
                      }
                    }
                  }
                }
              }
            }
          }
          rewritten_conjuncts.push_back(std::move(rewritten));
        }
        if (!changed) {
          return;
        }
        memo.AddExpression(
            group,
            LogicalExpression{.operation = LogicalOperator::kSelection,
                              .children = {input_id},
                              .predicate = CanonicalizeConjuncts(
                                  CombineConjuncts(rewritten_conjuncts))});
      },
      LogicalOperator::kSelection));

  // extract_year_sargable: EXTRACT(YEAR FROM date_col) <op> year ->
  // half-open date range on date_col, added to the scan filter while the
  // original predicate stays as the residual.  The residual re-check alone
  // does NOT make this safe: for single-relation groups the scan filter is
  // the only predicate evaluated (there is no root SelectionPlan wrap), so
  // the bounds must be semantically exact in both operand orders (see the
  // bound table in the switch below). DATE columns only: timestamps carry
  // time-of-day and time-zone behavior the range cannot reproduce. The
  // sargable range unlocks index range scans for year-filtered queries.
  // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
  //   (extract_year_sargable_sound, cpp_parameter_slot_freeze_bug)
  built.Add(Rule(
      "extract_year_sargable", Selection(Any("input")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (!SelectionMayStrengthenSharedScans(memo, group)) {
          return;
        }
        if (expression.operation != LogicalOperator::kSelection ||
            expression.children.size() != 1 || !expression.predicate ||
            !*expression.predicate) {
          return;
        }
        const GroupId input_id = bindings.at("input");
        if (input_id == group) {
          return;
        }
        const auto& schemas = memo.GetTableSchemas();
        if (schemas.empty()) {
          return;
        }
        for (const Expression& conjunct :
             SplitConjuncts(*expression.predicate)) {
          if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
            continue;
          }
          const auto& binary = conjunct->AsBinaryExpression();
          const Expression* call_side = nullptr;
          const Expression* const_side = nullptr;
          bool call_on_left = false;
          if (binary.Left()->Type() == TypeTag::kFunctionCallExp &&
              binary.Right()->Type() == TypeTag::kConstantValue) {
            call_side = &binary.Left();
            const_side = &binary.Right();
            call_on_left = true;
          } else if (binary.Right()->Type() == TypeTag::kFunctionCallExp &&
                     binary.Left()->Type() == TypeTag::kConstantValue) {
            call_side = &binary.Right();
            const_side = &binary.Left();
            call_on_left = false;
          } else {
            continue;
          }
          const auto& call = (*call_side)->AsFunctionCallExpression();
          if (call.FuncName() != "extract_year" || call.Args().size() != 1 ||
              !call.Args()[0] ||
              call.Args()[0]->Type() != TypeTag::kColumnValue ||
              ContainsParameterSlot(*const_side)) {
            continue;
          }
          const Value year_value = (*const_side)->AsConstantValue().GetValue();
          if (year_value.IsNull() || year_value.type != ValueType::kInt64) {
            continue;
          }
          const int64_t year = year_value.value.int_value;
          if (year < 1 || year > 9998) {
            continue;
          }
          const ColumnName& column =
              call.Args()[0]->AsColumnValue().GetColumnName();
          if (column.schema.empty()) {
            continue;
          }
          const auto schema_found = schemas.find(column.schema);
          if (schema_found == schemas.end()) {
            continue;
          }
          const int offset =
              schema_found->second.Offset(ColumnName("", column.name));
          if (offset < 0 ||
              schema_found->second.GetColumn(static_cast<size_t>(offset))
                      .Type() != ValueType::kDate) {
            continue;
          }
          const StatusOr<Value> lower_status =
              Value::TryDate(std::to_string(year) + "-01-01");
          const StatusOr<Value> upper_status =
              Value::TryDate(std::to_string(year + 1) + "-01-01");
          if (!lower_status.HasValue() || !upper_status.HasValue()) {
            continue;
          }
          const Expression col = ColumnValueExp(column);
          const Expression lower = ConstantValueExp(lower_status.Value());
          const Expression upper = ConstantValueExp(upper_status.Value());
          std::vector<Expression> bounds;
          // Bound direction table (lower = year-01-01, upper =
          // (year+1)-01-01):
          //   extract <  year: col <  lower   | year <  extract: col >= upper
          //   extract <= year: col <  upper   | year <= extract: col >= lower
          //   extract >  year: col >= upper   | year >  extract: col <  lower
          //   extract >= year: col >= lower   | year >= extract: col <  upper
          // The mirrored forms swap the operands AND flip the bound; merely
          // mirroring the expression kept the old (inverted) comparison.
          switch (binary.Op()) {
            case BinaryOperation::kEquals:
              bounds.push_back(BinaryExpressionExp(
                  col, BinaryOperation::kGreaterThanEquals, lower));
              bounds.push_back(
                  BinaryExpressionExp(col, BinaryOperation::kLessThan, upper));
              break;
            case BinaryOperation::kLessThan:
              bounds.push_back(
                  call_on_left
                      ? BinaryExpressionExp(col, BinaryOperation::kLessThan,
                                            lower)
                      : BinaryExpressionExp(
                            col, BinaryOperation::kGreaterThanEquals, upper));
              break;
            case BinaryOperation::kLessThanEquals:
              bounds.push_back(
                  call_on_left
                      ? BinaryExpressionExp(col, BinaryOperation::kLessThan,
                                            upper)
                      : BinaryExpressionExp(
                            col, BinaryOperation::kGreaterThanEquals, lower));
              break;
            case BinaryOperation::kGreaterThan:
              bounds.push_back(
                  call_on_left
                      ? BinaryExpressionExp(
                            col, BinaryOperation::kGreaterThanEquals, upper)
                      : BinaryExpressionExp(col, BinaryOperation::kLessThan,
                                            lower));
              break;
            case BinaryOperation::kGreaterThanEquals:
              bounds.push_back(
                  call_on_left
                      ? BinaryExpressionExp(
                            col, BinaryOperation::kGreaterThanEquals, lower)
                      : BinaryExpressionExp(col, BinaryOperation::kLessThan,
                                            upper));
              break;
            default:
              continue;
          }
          // Strengthen the group that owns this Selection, not the shared
          // base scan group: with the gate above, a branch-local Selection
          // must not remove rows another UNION/outer-join branch needs.
          // push_selection_into_scan copies the scan alternative into this
          // group, so the range still reaches the access path.
          if (memo.Get(group).relations.size() == 1) {
            memo.MergeScanFilter(group, CombineConjuncts(bounds));
          }
        }
      },
      LogicalOperator::kSelection));

  // comparison_self_predicates: x = x -> x IS NOT NULL; x < x / x > x ->
  // NULL-preserving FALSE; x <= x / x >= x -> NULL-preserving TRUE.
  // Equality is reflexive exactly for non-null values, so x = x is
  // equivalent to the null test; strict self-inequalities can never hold
  // but must stay NULL on null input (a bare FALSE would differ), hence
  // the CASE form. NaN breaks reflexivity, so floating-point columns are
  // excluded via the catalog domain gate (integers, dates and strings
  // only).
  // Lean proof: lean/TinylambOpt/ThreeValued.lean
  //   (self_eq_unsound_without_not_null, self_eq_sound_when_not_null,
  //    not_comparison_unsound_on_nan)
  built.Add(Rule(
      "comparison_self_predicates", Selection(Any("input")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kSelection ||
            expression.children.size() != 1 || !expression.predicate ||
            !*expression.predicate) {
          return;
        }
        const GroupId input_id = bindings.at("input");
        if (input_id == group) {
          return;
        }
        const auto& schemas = memo.GetTableSchemas();
        if (schemas.empty()) {
          return;
        }
        bool changed = false;
        std::vector<Expression> rewritten_conjuncts;
        for (const Expression& conjunct :
             SplitConjuncts(*expression.predicate)) {
          Expression rewritten = conjunct;
          if (conjunct && conjunct->Type() == TypeTag::kBinaryExp) {
            const auto& binary = conjunct->AsBinaryExpression();
            if ((binary.Op() == BinaryOperation::kEquals ||
                 binary.Op() == BinaryOperation::kLessThan ||
                 binary.Op() == BinaryOperation::kGreaterThan ||
                 binary.Op() == BinaryOperation::kLessThanEquals ||
                 binary.Op() == BinaryOperation::kGreaterThanEquals) &&
                binary.Left()->Type() == TypeTag::kColumnValue &&
                binary.Right()->Type() == TypeTag::kColumnValue &&
                binary.Left()->AsColumnValue().GetColumnName() ==
                    binary.Right()->AsColumnValue().GetColumnName()) {
              const ColumnName& column =
                  binary.Left()->AsColumnValue().GetColumnName();
              if (!column.schema.empty()) {
                const auto schema_found = schemas.find(column.schema);
                if (schema_found != schemas.end()) {
                  const int offset =
                      schema_found->second.Offset(ColumnName("", column.name));
                  if (offset >= 0) {
                    const ValueType domain =
                        schema_found->second
                            .GetColumn(static_cast<size_t>(offset))
                            .Type();
                    const bool reflexive = domain == ValueType::kInt64 ||
                                           domain == ValueType::kDate ||
                                           domain == ValueType::kVarChar;
                    if (reflexive) {
                      const Expression col = ColumnValueExp(column);
                      if (binary.Op() == BinaryOperation::kEquals) {
                        rewritten =
                            UnaryExpressionExp(col, UnaryOperation::kIsNotNull);
                      } else {
                        const bool always_true =
                            binary.Op() == BinaryOperation::kLessThanEquals ||
                            binary.Op() == BinaryOperation::kGreaterThanEquals;
                        rewritten = CaseExpressionExp(
                            {{UnaryExpressionExp(col, UnaryOperation::kIsNull),
                              ConstantValueExp(Value())}},
                            ConstantValueExp(Value(always_true)));
                      }
                      changed = true;
                    }
                  }
                }
              }
            }
          }
          rewritten_conjuncts.push_back(std::move(rewritten));
        }
        if (!changed) {
          return;
        }
        memo.AddExpression(
            group,
            LogicalExpression{.operation = LogicalOperator::kSelection,
                              .children = {input_id},
                              .predicate = CanonicalizeConjuncts(
                                  CombineConjuncts(rewritten_conjuncts))});
      },
      LogicalOperator::kSelection));
}
}  // namespace tinylamb::cascades::rule_detail
