import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_EvenParity_RejectSlack

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.EvenParity

-- Truth check on small inputs (all ranks, including ranks past the code space).
private def check : Bool :=
  (List.range 8).all fun n =>
  (List.range 12).all fun q =>
  (List.range 40).all fun r =>
    decide (((unrank model n q r).isNone) = decide (count model n q ≤ r))

#eval check

-- The parity machine always accepts a transition, so unranking fails only when
-- the rank is outside the code space.  Work with the equivalent "isSome" form.
private theorem step_false (q : Nat) : model.step q false = some (q % 2) := by
  simp [model]

private theorem step_true (q : Nat) : model.step q true = some ((q + 1) % 2) := by
  simp [model]

private theorem lowCount_eq (n q : Nat) : lowCount model n q = count model n (q % 2) := by
  simp [lowCount, step_false]

private theorem count_succ (n q : Nat) :
    count model (n + 1) q =
      count model n (q % 2) + count model n ((q + 1) % 2) := by
  simp [count, step_false, step_true]

private theorem bind_cons_isSome (o : Option (List Bool)) (b : Bool) :
    (o.bind fun bs => some (b :: bs)).isSome = o.isSome := by
  cases o <;> simp

private theorem not_isSome_eq_isNone {α : Type} (o : Option α) : (!o.isSome) = o.isNone := by
  cases o <;> simp

private theorem unrank_isSome (n q r : Nat) :
    (unrank model n q r).isSome = decide (r < count model n q) := by
  induction n generalizing q r with
  | zero =>
      by_cases hq : q = 0
      · subst q
        by_cases hr : r = 0
        · subst r
          simp [unrank, count, model]
        · simp [unrank, count, model, hr]
      · have hqb : (q == 0) = false := by simp [hq]
        simp [unrank, count, model, hqb]
  | succ n ih =>
      by_cases hlow : count model n (q % 2) ≤ r
      · have hunfold :
            (unrank model (n + 1) q r).isSome =
              (unrank model n ((q + 1) % 2) (r - count model n (q % 2))).isSome := by
          simp [unrank, lowCount_eq, step_true, hlow, bind_cons_isSome]
        rw [hunfold, ih ((q + 1) % 2) (r - count model n (q % 2)), count_succ]
        by_cases h : r < count model n (q % 2) + count model n ((q + 1) % 2)
        · have htail : r - count model n (q % 2) < count model n ((q + 1) % 2) := by
            omega
          simp [h, htail]
        · have htail : ¬ r - count model n (q % 2) < count model n ((q + 1) % 2) := by
            omega
          simp [h, htail]
      · have hunfold :
            (unrank model (n + 1) q r).isSome =
              (unrank model n (q % 2) r).isSome := by
          simp [unrank, lowCount_eq, step_false, hlow, bind_cons_isSome]
        rw [hunfold, ih (q % 2) r, count_succ]
        by_cases h : r < count model n (q % 2) + count model n ((q + 1) % 2)
        · have hhead : r < count model n (q % 2) := by omega
          simp [h, hhead]
        · have hhead : ¬ r < count model n (q % 2) := by omega
          simp [h, hhead]

theorem sound_RejectSlack : EvenParity.RejectSlack.Goal := by
  intro n q r
  have hs := congrArg Bool.not (unrank_isSome n q r)
  have hdecide : decide (count model n q ≤ r) = !decide (r < count model n q) := by
    by_cases h : count model n q ≤ r
    · have hnot : ¬ r < count model n q := by omega
      simp [h, hnot]
    · have hlt : r < count model n q := by omega
      simp [h, hlt]
  rw [hdecide]
  simpa [not_isSome_eq_isNone] using hs

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_EvenParity_RejectSlack
