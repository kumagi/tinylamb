import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PrefixOnes_RejectSlack

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PrefixOnes

-- Truth check on small inputs (all ranks, including ranks past the code space).
private def check : Bool :=
  (List.range 7).all fun n =>
  (List.range 10).all fun q =>
  (List.range 30).all fun r =>
    decide (((unrank model n q r).isNone) = decide (count model n q ≤ r))

#eval check

private theorem step_false (q : Nat) : model.step q false = some 1 := by
  simp [model]

private theorem step_true (q : Nat) : model.step q true = if q == 1 then none else some 0 := by
  by_cases hq : q = 1
  · subst q
    simp [model]
  · simp [model, hq]

private theorem bind_cons_isNone (o : Option (List Bool)) (b : Bool) :
    (o.bind fun bs => some (b :: bs)).isNone = o.isNone := by
  cases o <;> simp

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

private theorem count_one (n : Nat) : count model n 1 = 1 := by
  rw [count_formula]
  simp

private theorem count_zero (n : Nat) : count model n 0 = n + 1 := by
  rw [count_formula]
  simp

private theorem lowCount_eq (n q : Nat) : lowCount model n q = 1 := by
  simp [lowCount, step_false, count_one]

theorem sound_RejectSlack : PrefixOnes.RejectSlack.Goal := by
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
      by_cases hr0 : r = 0
      · subst r
        have hstep : (unrank model (n + 1) q 0).isNone = (unrank model n 1 0).isNone := by
          simp [unrank, lowCount_eq, step_false, bind_cons_isNone]
        rw [hstep, ih 1 0]
        have hpos : 0 < count model (n + 1) q := by
          rw [count_formula]
          by_cases hq : q == 1 <;> simp [hq] <;> omega
        simp [count_one, Nat.not_le_of_gt hpos]
      · have hr1 : 1 ≤ r := by omega
        by_cases hq : q == 1
        · have hstep : (unrank model (n + 1) q r).isNone = true := by
            simp [unrank, lowCount_eq, step_true, hq, hr1]
          rw [hstep]
          rw [count_formula]
          simp [hq, hr1]
        · have hstep : (unrank model (n + 1) q r).isNone = (unrank model n 0 (r - 1)).isNone := by
            simp [unrank, lowCount_eq, step_true, hq, hr1, bind_cons_isNone]
          rw [hstep, ih 0 (r - 1)]
          rw [count_zero]
          rw [count_formula]
          simp [hq]
          by_cases hle : (n + 1) + 1 ≤ r
          · have hle' : n + 1 ≤ r - 1 := by omega
            simp [hle, hle']
          · have hle' : ¬ n + 1 ≤ r - 1 := by omega
            simp [hle, hle']

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PrefixOnes_RejectSlack
