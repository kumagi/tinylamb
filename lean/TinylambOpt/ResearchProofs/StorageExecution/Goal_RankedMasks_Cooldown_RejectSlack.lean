import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_Cooldown_RejectSlack

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.Cooldown

-- Truth check on small inputs, including ranks past the end of the code space.
private def check : Bool :=
  (List.range 9).all fun n =>
  (List.range 6).all fun q =>
  (List.range 30).all fun r =>
    decide (((unrank model n q r).isNone) = decide (count model n q ≤ r))

#eval check

private theorem step_false (q : Nat) : model.step q false = some (q - 1) := by
  simp [model]

private theorem step_true (q : Nat) :
    model.step q true = if q = 0 then some 2 else none := by
  by_cases hq : q = 0
  · subst q
    simp [model]
  · simp [model, hq]

private theorem lowCount_eq (n q : Nat) : lowCount model n q = count model n (q - 1) := by
  simp [lowCount, step_false]

private theorem count_succ (n q : Nat) :
    count model (n + 1) q = count model n (q - 1) + (if q = 0 then count model n 2 else 0) := by
  by_cases hq : q = 0
  · subst q
    simp [count, step_false, step_true]
  · simp [count, step_false, step_true, hq]

private theorem isNone_bind_some {α β : Type} (o : Option α) (f : α → β) :
    (o.bind (fun x => some (f x))).isNone = o.isNone := by
  cases o <;> simp

theorem sound_RejectSlack : Cooldown.RejectSlack.Goal := by
  intro n
  induction n with
  | zero =>
      intro q r
      cases r <;> simp [unrank, count, model]
  | succ n ih =>
      intro q r
      by_cases hr : count model n (q - 1) ≤ r
      · have hd : decide (count model n (q - 1) ≤ r) = true := by simp [hr]
        by_cases hq : q = 0
        · subst q
          simp at hr
          have hstep :
              (unrank model (n + 1) 0 r).isNone =
                (unrank model n 2 (r - count model n 0)).isNone := by
            simp [unrank, lowCount_eq, step_true, hd, isNone_bind_some]
          rw [hstep, ih 2 (r - count model n 0)]
          rw [count_succ]
          simp only [Nat.zero_sub, ite_true]
          by_cases ht : count model n 2 ≤ r - count model n 0
          · have htot : count model n 0 + count model n 2 ≤ r := by omega
            simp [ht, htot]
          · have htot : ¬ count model n 0 + count model n 2 ≤ r := by omega
            simp [ht, htot]
        · have hstep : (unrank model (n + 1) q r).isNone = true := by
            simp [unrank, lowCount_eq, step_true, hd, hq]
          rw [hstep]
          have htot : count model (n + 1) q ≤ r := by
            rw [count_succ]
            simp [hq, hr]
          simp [htot]
      · have hd : decide (count model n (q - 1) ≤ r) = false := by simp [hr]
        have hstep :
            (unrank model (n + 1) q r).isNone = (unrank model n (q - 1) r).isNone := by
          simp [unrank, lowCount_eq, step_false, hd, isNone_bind_some]
        rw [hstep, ih (q - 1) r]
        simp only [hr, decide_false]
        have hnot : ¬ count model (n + 1) q ≤ r := by
          rw [count_succ]
          by_cases hq : q = 0
          · have hr' : ¬ count model n 0 ≤ r := by simpa [hq] using hr
            simp [hq]
            omega
          · simp [hq]
            omega
        simp [hnot]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_Cooldown_RejectSlack
