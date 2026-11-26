import TinylambOpt.ResearchGoals.November26.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.November26.Click.Goal_Q28
open TinylambOpt.ResearchGoals.November26.Click
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem forKey_length (rows : List (Nat × Nat)) (k : Nat) :
    (forKey rows k).length = (rows.map Prod.fst).count k := by
  dsimp [forKey]
  rw [List.count_eq_countP, List.countP_eq_length_filter]
  induction rows with
  | nil => rfl
  | cons r rs ih => by_cases hr : r.1 == k <;> simp [hr, ih]

theorem sound : TinylambOpt.ResearchGoals.November26.Click.Q28.Goal := by
  intro rows k lengths h
  have hle : (lengths.filter (fun n => n > 0)).length ≤ lengths.length := by
    exact List.length_filter_le _ _
  have hc : lengths.length = (rows.map Prod.fst).count k := forKey_length rows k
  omega
end TinylambOpt.ResearchProofs.November26.Click.Goal_Q28
