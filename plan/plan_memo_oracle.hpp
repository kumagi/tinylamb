/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_MEMO_ORACLE_HPP
#define TINYLAMB_PLAN_MEMO_ORACLE_HPP

#include <cstddef>
#include <functional>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "plan/cascades.hpp"

namespace tinylamb {

// Seeded random join-graph generator + memo-equivalence oracle for the
// Cascades optimizer (plan layer PBT entry point).
//
// Pipeline: GenerateJoinGraph(rng) builds a small relation set together with
// single-relation filter conjuncts (`tN.c0 = <const>` / `tN.c1 = <const>`,
// alternating by conjunct index) and two-relation join
// conjuncts (`tA.c0 = tB.c0`). CheckExploreEquivalence(graph) then verifies
// the memo properties every transformation rule must preserve:
//
//   1. structural invariants before and after SearchEngine::Explore
//      (relation masks partition, fingerprints unique, scan filters and join
//      conditions match the stored conjuncts);
//   2. determinism (rebuild + re-explore yields identical group/expression
//      counts);
//   3. deterministic re-exploration (rebuilding and exploring again yields
//      the same group and root-expression counts);
//   4. reduced-rule-set sanity (removing associativity must not empty or
//      collapse a multi-alternative root to one alternative).
//
// Any non-empty return is a logic bug. ShrinkJoinGraph reduces a failing
// graph while the mismatch (as reported by CheckExploreEquivalence) is
// preserved. The generator is deterministic in the RNG stream: the same seed
// always yields the same graph, so a `.test`-style replay only needs the
// seed. This header lives in the plan layer, so it only depends on
// common/type/expression/plan; query-engine execution belongs to tests above
// the layer DAG.

struct PlanMemoGenConfig {
  int min_relations = 2;
  int max_relations = 4;
  int max_conjuncts = 4;
};

struct GeneratedJoinGraph {
  std::vector<std::string> relations;
  std::vector<cascades::ConjunctInfo> conjuncts;
};

// Deterministic in the RNG stream: the same seed always yields the same
// graph. Only uses the RNG (no map iteration, no I/O).
GeneratedJoinGraph GenerateJoinGraph(std::mt19937& rng,
                                     const PlanMemoGenConfig& config = {});

// Verifies structural memo invariants. When `check_conjunct_attachment` is
// true, single-relation scan filters and join-condition payloads must match
// Memo::ScanFilterFor/JoinConditionFor exactly (holds for a freshly built
// memo; selection-pushdown rules may legitimately extend scan filters during
// exploration, so callers pass false after Explore). Returns "" when the memo
// is sound, otherwise a diagnostic.
std::string CheckMemoInvariants(const cascades::Memo& memo,
                                bool check_conjunct_attachment);

// Full oracle for one generated graph: fresh-build invariants, Explore with
// the default rule set, post-explore structural invariants, determinism, and
// a reduced-rule-set sanity check (removing the two associativity rules must
// still leave the root non-empty). Returns "" when all properties hold.
std::string CheckExploreEquivalence(const GeneratedJoinGraph& graph);

// Bounded delta-shrinker: greedily drops conjuncts one by one, then relations
// (with their touching conjuncts) while the mismatch is preserved. Always
// terminates; returns a locally reduced mismatch-preserving graph (not a
// proven minimum — the result depends on removal order), or the input when
// nothing shrinks.
GeneratedJoinGraph ShrinkJoinGraph(const GeneratedJoinGraph& graph);

// Same delta-shrinking algorithm with an injectable mismatch predicate.
// The default overload uses CheckExploreEquivalence(); tests exercise the
// shrink loop with a synthetic predicate because a sound engine leaves no
// naturally failing graph to shrink.
using GraphMismatchPredicate = std::function<bool(const GeneratedJoinGraph&)>;
GeneratedJoinGraph ShrinkJoinGraph(const GeneratedJoinGraph& graph,
                                   const GraphMismatchPredicate& is_mismatch);

// ---------------------------------------------------------------------------
// Complex Multi-Operator Memo Oracle
//
// Synthesizes rich Cascades Memos with per-relation schemas and column
// constraints (PRIMARY KEY / UNIQUE / NOT NULL / none; no foreign keys), plus
// multi-operator pipelines (Selection, Projection, Aggregation, Distinct, Sort,
// Limit, Window, TopN, Unnest, and a conditional OuterJoin root). SemiJoin /
// AntiJoin are not generated. This exercises the applicable default
// transformation rules during exploration; generator coverage is not proof that
// every rule fires.
// ---------------------------------------------------------------------------

struct ComplexMemoGenConfig {
  int min_relations = 2;
  int max_relations = 4;
  int max_conjuncts = 6;
  int max_operator_depth = 4;
};

struct GeneratedComplexMemo {
  std::vector<std::string> relations;
  std::unordered_map<std::string, Schema> schemas;
  std::vector<cascades::ConjunctInfo> conjuncts;
  std::function<cascades::GroupId(cascades::Memo&)> builder;
  std::string description;
};

GeneratedComplexMemo GenerateComplexMemo(
    std::mt19937& rng, const ComplexMemoGenConfig& config = {});

std::string CheckComplexMemoEquivalence(const GeneratedComplexMemo& gen);

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_MEMO_ORACLE_HPP
