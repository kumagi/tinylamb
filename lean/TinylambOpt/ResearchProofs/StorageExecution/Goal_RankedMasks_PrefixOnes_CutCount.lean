import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `PrefixOnes.CutCount` — `∀ n, count model n 0 = n+1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PrefixOnes_CutCount

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PrefixOnes

-- Truth check on small inputs before attempting the proof.
private def check : Bool :=
  (List.range 12).all fun n => count model n 0 == n + 1

#eval check

private theorem step_false (q : Nat) : model.step q false = some 1 := by
  simp [model]

private theorem step_true (q : Nat) : model.step q true = if q == 1 then none else some 0 := by
  simp [model]

private theorem count_formula : ∀ n q : Nat, count model n q = if q == 1 then 1 else n + 1 := by
  intro n
  induction n with
  | zero =>
      intro q
      simp [count, model]
  | succ n ih =>
      intro q
      change ((model.step q false).map (count model n)).getD 0 +
          ((model.step q true).map (count model n)).getD 0 =
        (if q == 1 then 1 else (n + 1) + 1)
      rw [step_false, step_true]
      by_cases hq : q == 1 <;> simp [hq, ih] <;> omega

theorem sound_CutCount : PrefixOnes.CutCount.Goal := by
  intro n
  rw [count_formula]
  simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PrefixOnes_CutCount
