import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false


namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q85
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpcds

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q85.Goal := by
  intro a b code hinj
  cases a with
  | none => cases b <;> simp [hit]
  | some pa =>
    cases b with
    | none => simp [hit]
    | some pb =>
      simp only [hit, Option.map_some, Option.isSome_some, Bool.true_and]
      by_cases h : pa = pb
      · subst pb
        simp
      · have hne : code pa ≠ code pb := by
          intro heq
          exact h (hinj pa pb heq)
        simp [beq_false_of_ne hne, beq_false_of_ne h]
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q85
