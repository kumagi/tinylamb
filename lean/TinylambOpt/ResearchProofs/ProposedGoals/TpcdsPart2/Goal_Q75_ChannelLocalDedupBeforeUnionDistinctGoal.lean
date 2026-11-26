import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Local deduplication before UNION DISTINCT (TPC-DS Q75) — refuted

The goal quantifies over an arbitrary `[BEq α]` without lawfulness. With a non-symmetric
comparison, deduplicating each channel first can retain a row that a single global
deduplication removes, so the two lengths differ. The rewrite is sound only under a
lawful (reflexive, symmetric, transitive) equality instance, as supplied by SQL key types.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q75_ChannelLocalDedupBeforeUnionDistinctGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

/-- A deliberately non-lawful equality: `B2 == B1` and `B1 == A`, but not conversely. -/
inductive K where
  | A | B1 | B2 deriving DecidableEq

instance : BEq K where
  beq x y := match x, y with
    | K.B2, K.B1 => true
    | K.B1, K.A => true
    | _, _ => false

theorem refute : ¬ Q75_ChannelLocalDedupBeforeUnionDistinctGoal := by
  intro h
  have hx := h (α := K) [K.A] [K.B1, K.B2]
  have hneq :
      (([K.A].eraseDups ++ [K.B1, K.B2].eraseDups).eraseDups).length ≠
        (([K.A] ++ [K.B1, K.B2]).eraseDups).length := by
    native_decide
  exact hneq hx

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q75_ChannelLocalDedupBeforeUnionDistinctGoal
