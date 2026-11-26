import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `RunLimit.RejectSlack` — Reject unused bit patterns in the final code word.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_RunLimit_RejectSlack

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.RunLimit

-- Truth check on small inputs (all ranks, including ranks past the code space).
private def check : Bool :=
  (List.range 7).all fun n =>
  (List.range 10).all fun q =>
  (List.range 30).all fun r =>
    decide (((unrank model n q r).isNone) = decide (count model n q ≤ r))

#eval check

private theorem step_false (q : Nat) : model.step q false = some 0 := by
  simp [model]

private theorem step_true (q : Nat) : model.step q true = if q < 2 then some (q + 1) else none := by
  simp [model]

private theorem lowCount_eq (n q : Nat) : lowCount model n q = count model n 0 := by
  simp [lowCount, step_false]

private theorem bind_cons_isNone (o : Option (List Bool)) (b : Bool) :
    (o.bind fun bs => some (b :: bs)).isNone = o.isNone := by
  cases o <;> simp

private theorem count_succ_eq (n q : Nat) :
    count model (n + 1) q =
      count model n 0 + (if q < 2 then count model n (q + 1) else 0) := by
  change ((model.step q false).map (count model n)).getD 0 +
    ((model.step q true).map (count model n)).getD 0 =
    count model n 0 + (if q < 2 then count model n (q + 1) else 0)
  rw [step_false, step_true]
  by_cases hq : q < 2 <;> simp [hq]

theorem sound_RejectSlack : RunLimit.RejectSlack.Goal := by
  intro n
  induction n with
  | zero =>
      intro q r
      by_cases hr : r = 0
      · subst r
        simp [unrank, count, model]
      · have hrb : (r == 0) = false := by simp [hr]
        have hpos : 1 ≤ r := by omega
        simp [unrank, count, model, hrb, hpos]
  | succ n ih =>
      intro q r
      by_cases hr : count model n 0 ≤ r
      · by_cases hq : q < 2
        · have hstep :
            (unrank model (n + 1) q r).isNone =
              (unrank model n (q + 1) (r - count model n 0)).isNone := by
            simp [unrank, lowCount_eq, step_true, hr, hq, bind_cons_isNone]
          rw [hstep, ih (q + 1) (r - count model n 0)]
          rw [count_succ_eq]
          simp [hq]
          by_cases htail : count model n (q + 1) ≤ r - count model n 0
          · have htot : count model n 0 + count model n (q + 1) ≤ r := by omega
            simp [htail, htot]
          · have htot : ¬ count model n 0 + count model n (q + 1) ≤ r := by omega
            simp [htail, htot]
        · have hstep : (unrank model (n + 1) q r).isNone = true := by
            simp [unrank, lowCount_eq, step_true, hr, hq]
          rw [hstep, count_succ_eq]
          simp [hq, hr]
      · have hstep :
            (unrank model (n + 1) q r).isNone = (unrank model n 0 r).isNone := by
          simp [unrank, lowCount_eq, step_false, hr, bind_cons_isNone]
        rw [hstep, ih 0 r]
        have hnot : ¬ count model (n + 1) q ≤ r := by
          rw [count_succ_eq]
          by_cases hq : q < 2 <;> simp [hq] <;> omega
        simp [hr, hnot]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_RunLimit_RejectSlack
