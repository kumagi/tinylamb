import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `BalancedNesting.RejectSlack` — Reject unused bit patterns in the final code word.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_BalancedNesting_RejectSlack

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.BalancedNesting

-- Truth check on small inputs, including ranks past the end of the code space.
private def check : Bool :=
  (List.range 9).all fun n =>
  (List.range 8).all fun q =>
  (List.range 40).all fun r =>
    decide (((unrank model n q r).isNone) = decide (count model n q ≤ r))

#eval check

private theorem step_false (q : Nat) : model.step q false = if q = 0 then none else some (q - 1) := by
  simp [model]

private theorem step_true (q : Nat) : model.step q true = some (q + 1) := by
  simp [model]

private theorem lowCount_eq (n q : Nat) :
    lowCount model n q = if q = 0 then 0 else count model n (q - 1) := by
  by_cases hq : q = 0
  · subst q
    simp [lowCount, model]
  · simp [lowCount, step_false, hq]

private theorem count_succ (n q : Nat) :
    count model (n + 1) q = (if q = 0 then 0 else count model n (q - 1)) + count model n (q + 1) := by
  by_cases hq : q = 0
  · subst q
    simp [count, step_false, step_true]
  · simp [count, step_false, step_true, hq]

private theorem bind_cons_isNone (o : Option (List Bool)) (b : Bool) :
    (o.bind fun bs => some (b :: bs)).isNone = o.isNone := by
  cases o <;> simp

theorem sound_RejectSlack : BalancedNesting.RejectSlack.Goal := by
  intro n
  induction n with
  | zero =>
      intro q r
      by_cases hq : q = 0
      · subst q
        by_cases hr : r = 0
        · subst r
          simp [unrank, count, model]
        · have hrb : (r == 0) = false := by simp [hr]
          simp [unrank, count, model, hrb]
          omega
      · have hbeq : (q == 0) = false := (beq_eq_false_iff_ne).2 hq
        simp [unrank, count, model, hbeq]
  | succ n ih =>
      intro q r
      by_cases hq : q = 0
      · subst q
        have hstep :
            (unrank model (n + 1) 0 r).isNone = (unrank model n 1 r).isNone := by
          simp [unrank, lowCount_eq, step_true, bind_cons_isNone]
        rw [hstep, ih 1 r]
        rw [count_succ]
        simp
      · have hlow : lowCount model n q = count model n (q - 1) := by
          rw [lowCount_eq]
          simp [hq]
        by_cases hr : count model n (q - 1) ≤ r
        · have hdec : decide (lowCount model n q ≤ r) = true := by simp [hlow, hr]
          have hstep :
              (unrank model (n + 1) q r).isNone =
                (unrank model n (q + 1) (r - lowCount model n q)).isNone := by
            simp [unrank, hdec, step_true, bind_cons_isNone]
          rw [hstep, ih (q + 1) (r - lowCount model n q)]
          rw [count_succ]
          simp [hq, hlow]
          by_cases ht : count model n (q + 1) ≤ r - count model n (q - 1)
          · have htot : count model n (q - 1) + count model n (q + 1) ≤ r := by omega
            simp [ht, htot]
          · have htot : ¬ count model n (q - 1) + count model n (q + 1) ≤ r := by omega
            simp [ht, htot]
        · have hdec : decide (lowCount model n q ≤ r) = false := by simp [hlow, hr]
          have hstep :
              (unrank model (n + 1) q r).isNone = (unrank model n (q - 1) r).isNone := by
            simp [unrank, hdec, step_false, hq, bind_cons_isNone]
          rw [hstep, ih (q - 1) r]
          have hnot : ¬ count model (n + 1) q ≤ r := by
            rw [count_succ]
            simp [hq]
            omega
          simp [hr, hnot]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_BalancedNesting_RejectSlack
