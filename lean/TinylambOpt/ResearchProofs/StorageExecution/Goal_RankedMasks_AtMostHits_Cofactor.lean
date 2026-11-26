import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_AtMostHits_Cofactor

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.AtMostHits

private abbrev hiCount (m : Machine) (n q : Nat) : Nat :=
  ((m.step q true).map (count m n)).getD 0

private theorem count_succ (m : Machine) (n q : Nat) :
    count m (n + 1) q = lowCount m n q + hiCount m n q := by
  simp [count, lowCount, hiCount]

-- AtMostHits でも false は残りヒット数を変えない。
private theorem run_false (q : Nat) (bs : List Bool) : run model q (false :: bs) = run model q bs := by
  simp [run, model]

-- true は残りヒット数を 1 減らす。残数 0 なら不能。
private theorem run_true (q : Nat) (bs : List Bool) :
    run model q (true :: bs) = if q = 0 then none else run model (q - 1) bs := by
  by_cases hq : q = 0
  · subst q
    simp [run, model]
  · simp [hq, run, model]

private theorem term_false (q b' : Nat) (bs : List Bool) :
    ((run model q (false :: bs)).map (count model b')).getD 0 =
      ((run model q bs).map (count model b')).getD 0 := by
  rw [run_false]

private theorem term_true_zero (b' : Nat) (bs : List Bool) :
    ((run model 0 (true :: bs)).map (count model b')).getD 0 = 0 := by
  rw [run_true]
  simp

private theorem term_true_pos (q b' : Nat) (bs : List Bool) (hq : q ≠ 0) :
    ((run model q (true :: bs)).map (count model b')).getD 0 =
      ((run model (q - 1) bs).map (count model b')).getD 0 := by
  rw [run_true]
  simp [hq]

private theorem sum_map_zero {α : Type} (xs : List α) : (xs.map (fun _ => (0 : Nat))).sum = 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]

-- `words (a+1)` を false 極 / true 極に分け、それぞれを帰納仮説で数え上げる。
private theorem cofactor_model : cofactor model := by
  intro q a
  induction a generalizing q with
  | zero =>
      intro b
      simp [words, run]
  | succ n ih =>
      intro b
      rw [show (n + 1) + b = (n + b) + 1 by omega]
      rw [count_succ]
      have hlow : lowCount model (n + b) q = count model (n + b) q := by
        simp [lowCount, model]
      rw [hlow]
      rw [words]
      simp only [List.map_append, List.sum_append, List.map_map]
      change count model (n + b) q + hiCount model (n + b) q =
        ((words n).map (fun pre => ((run model q (false :: pre)).map (count model b)).getD 0)).sum +
        ((words n).map (fun pre => ((run model q (true :: pre)).map (count model b)).getD 0)).sum
      by_cases hq : q = 0
      · subst q
        have hhi : hiCount model (n + b) 0 = 0 := by simp [hiCount, model]
        rw [hhi]
        simp [term_false, term_true_zero, sum_map_zero, ih]
      · have hhi : hiCount model (n + b) q = count model (n + b) (q - 1) := by
          simp [hiCount, model, hq]
        rw [hhi]
        simp [term_false, term_true_pos, hq, ih]

theorem sound_Cofactor : AtMostHits.Cofactor.Goal := by
  exact cofactor_model

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_AtMostHits_Cofactor
