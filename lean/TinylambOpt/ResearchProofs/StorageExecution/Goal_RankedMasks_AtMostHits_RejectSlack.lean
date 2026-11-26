import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `AtMostHits.RejectSlack` — Reject unused bit patterns in the final code word.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_AtMostHits_RejectSlack

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.AtMostHits

-- Truth check on small inputs (all ranks, including ranks past the code space).
private def check : Bool :=
  (List.range 7).all fun n =>
  (List.range 10).all fun q =>
  (List.range 30).all fun r =>
    decide (((unrank model n q r).isNone) = decide (count model n q ≤ r))

#eval check

private theorem step_false (q : Nat) : model.step q false = some q := by
  simp [model]

private theorem step_true (q : Nat) : model.step q true = if q = 0 then none else some (q - 1) := by
  simp [model]

private theorem lowCount_eq (n q : Nat) : lowCount model n q = count model n q := by
  simp [lowCount, step_false]

private theorem bind_cons_isNone (o : Option (List Bool)) (b : Bool) :
    (o.bind fun bs => some (b :: bs)).isNone = o.isNone := by
  cases o <;> simp

private theorem count_zero (n : Nat) : count model n 0 = 1 := by
  induction n with
  | zero => rfl
  | succ n ih =>
      change ((model.step 0 false).map (count model n)).getD 0 +
        ((model.step 0 true).map (count model n)).getD 0 = 1
      simp [step_false, step_true, ih]

private theorem count_succ_eq (n q : Nat) :
    count model (n + 1) q = count model n q + (if q = 0 then 0 else count model n (q - 1)) := by
  change ((model.step q false).map (count model n)).getD 0 +
    ((model.step q true).map (count model n)).getD 0 =
    count model n q + (if q = 0 then 0 else count model n (q - 1))
  rw [step_false, step_true]
  by_cases hq : q = 0 <;> simp [hq]

theorem sound_RejectSlack : AtMostHits.RejectSlack.Goal := by
  intro n
  induction n with
  | zero =>
      intro q r
      by_cases hr : r = 0
      · subst r
        simp [unrank, count, model]
      · have hrb : (r == 0) = false := by simp [hr]
        simp [unrank, count, model, hrb]
        omega
  | succ n ih =>
      intro q r
      by_cases hr : count model n q ≤ r
      · by_cases hq : q = 0
        · subst q
          have hcnt : count model (n + 1) 0 = 1 := count_zero (n + 1)
          have hr1 : 1 ≤ r := by
            rw [count_zero n] at hr
            exact hr
          have hstep : (unrank model (n + 1) 0 r).isNone = true := by
            simp [unrank, lowCount_eq, step_true, hr]
          rw [hstep, hcnt]
          simp [hr1]
        · have hstep :
              (unrank model (n + 1) q r).isNone =
                (unrank model n (q - 1) (r - count model n q)).isNone := by
            simp [unrank, lowCount_eq, step_true, hr, hq, bind_cons_isNone]
          rw [hstep, ih (q - 1) (r - count model n q)]
          rw [count_succ_eq]
          simp [hq]
          by_cases htail : count model n (q - 1) ≤ r - count model n q
          · have htot : count model n q + count model n (q - 1) ≤ r := by omega
            simp [htail, htot]
          · have htot : ¬ count model n q + count model n (q - 1) ≤ r := by omega
            simp [htail, htot]
      · have hstep :
            (unrank model (n + 1) q r).isNone = (unrank model n q r).isNone := by
          simp [unrank, lowCount_eq, step_false, hr, bind_cons_isNone]
        rw [hstep, ih q r]
        have hnot : ¬ count model (n + 1) q ≤ r := by
          rw [count_succ_eq]
          by_cases hq : q = 0
          · subst q
            rw [count_zero n] at hr
            simp [count_zero]
            omega
          · simp [hq]
            omega
        simp [hr, hnot]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_AtMostHits_RejectSlack
