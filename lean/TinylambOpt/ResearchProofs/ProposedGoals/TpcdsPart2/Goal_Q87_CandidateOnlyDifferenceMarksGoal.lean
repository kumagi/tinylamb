import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Candidate-only EXCEPT membership marks (TPC-DS Q87) — refuted

The goal quantifies over an arbitrary `[BEq α]`. With a non-symmetric comparison, a
candidate can be found in `c` even though filtering `c` by `sDedup.contains` discards the
matching row, so the two candidate counts differ. The rewrite requires a lawful equality
instance, which SQL key types do provide.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q87_CandidateOnlyDifferenceMarksGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

/-- A deliberately non-lawful equality: `X == Z` but neither `Z == X` nor `X == X`. -/
inductive K where
  | X | Z deriving DecidableEq

instance : BEq K where
  beq x y := match x, y with
    | K.X, K.Z => true
    | _, _ => false

theorem refute : ¬ Q87_CandidateOnlyDifferenceMarksGoal := by
  intro h
  have hx := h (α := K) [K.X] [K.Z] []
  have hneq :
      ([K.X].eraseDups.filter (fun x => ![K.Z].contains x && !([] : List K).contains x)).length ≠
        ([K.X].eraseDups.filter (fun x =>
          !([K.Z].filter (fun y => [K.X].eraseDups.contains y)).contains x &&
          !(([] : List K).filter (fun y => [K.X].eraseDups.contains y)).contains x)).length := by
    native_decide
  exact hneq hx

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q87_CandidateOnlyDifferenceMarksGoal
