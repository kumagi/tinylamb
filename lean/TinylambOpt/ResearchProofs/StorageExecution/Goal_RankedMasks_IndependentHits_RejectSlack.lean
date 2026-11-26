import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `IndependentHits.RejectSlack` — Reject unused bit patterns in the final code word.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_IndependentHits_RejectSlack

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits

-- Truth check on small inputs, including ranks past the end of the code space.
private def check : Bool :=
  (List.range 9).all fun n =>
  (List.range 5).all fun q =>
  (List.range 30).all fun r =>
    decide (((unrank model n q r).isNone) = decide (count model n q ≤ r))

#eval check

private theorem step_false (q : Nat) : model.step q false = some 0 := by
  simp [model]

private theorem step_true (q : Nat) :
    model.step q true = if q = 1 then none else some 1 := by
  by_cases hq : q = 1
  · subst q
    simp [model]
  · simp [model, hq]

private theorem count_succ (n q : Nat) :
    count model (n + 1) q = count model n 0 + (if q = 1 then 0 else count model n 1) := by
  by_cases hq : q = 1
  · subst q
    simp [count, step_false, step_true]
  · simp [count, step_false, step_true, hq]

private theorem isNone_bind_some {α β : Type} (o : Option α) (f : α → β) :
    (o.bind (fun x => some (f x))).isNone = o.isNone := by
  cases o <;> simp

theorem sound_RejectSlack : IndependentHits.RejectSlack.Goal := by
  intro n
  induction n with
  | zero =>
      intro q r
      cases r <;> simp [unrank, count, model]
  | succ n ih =>
      intro q r
      by_cases hr : count model n 0 ≤ r
      · have hd : decide (count model n 0 ≤ r) = true := by simp [hr]
        by_cases hq : q = 1
        · subst q
          rw [count_succ]
          simp only [ite_true, Nat.add_zero]
          simp [unrank, lowCount, step_false, step_true, hd]
        · have hstep :
              (unrank model (n + 1) q r).isNone =
                (unrank model n 1 (r - count model n 0)).isNone := by
            simp [unrank, lowCount, step_false, step_true, hd, hq, isNone_bind_some]
          rw [hstep, ih 1 (r - count model n 0)]
          rw [count_succ]
          simp [hq]
          by_cases ht : count model n 1 ≤ r - count model n 0
          · have htot : count model n 0 + count model n 1 ≤ r := by omega
            simp [ht, htot]
          · have htot : ¬ count model n 0 + count model n 1 ≤ r := by omega
            simp [ht, htot]
      · have hd : decide (count model n 0 ≤ r) = false := by simp [hr]
        have hstep :
            (unrank model (n + 1) q r).isNone =
              (unrank model n 0 r).isNone := by
          simp [unrank, lowCount, step_false, hd, isNone_bind_some]
        rw [hstep, ih 0 r]
        simp only [hr, decide_false]
        rw [count_succ]
        by_cases hq : q = 1
        · simp [hq, hr]
        · simp [hq]
          omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_IndependentHits_RejectSlack
