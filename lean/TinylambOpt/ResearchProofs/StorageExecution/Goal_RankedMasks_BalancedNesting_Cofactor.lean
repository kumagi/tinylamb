import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_BalancedNesting_Cofactor

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.BalancedNesting

-- Truth check on small inputs (including states above the usual depth range).
private def check : Bool :=
  (List.range 9).all fun a =>
  (List.range 4).all fun b =>
  (List.range 4).all fun q =>
    decide (count model (a + b) q =
      ((words a).map (fun pre => ((run model q pre).map (count model b)).getD 0)).sum)

#eval check

-- The cofactor decomposition holds for every machine, not just BalancedNesting.
private theorem cofactor_all (m : Machine) : ∀ a b q, count m (a + b) q =
    ((words a).map (fun pre => ((run m q pre).map (count m b)).getD 0)).sum := by
  intro a
  induction a with
  | zero =>
      intro b q
      simp [words, run]
  | succ a ih =>
      have run_cons_getD (q : Nat) (bit : Bool) (pre : List Bool) (c : Nat) :
          ((run m q (bit :: pre)).map (count m c)).getD 0 =
            ((m.step q bit).map (fun q' => ((run m q' pre).map (count m c)).getD 0)).getD 0 := by
        cases hstep : m.step q bit with
        | none => simp [run, hstep]
        | some q' => simp [run, hstep]
      have sum_cons_getD (ps : List (List Bool)) (q : Nat) (bit : Bool) (c : Nat) :
          (ps.map ((fun pre => ((run m q pre).map (count m c)).getD 0) ∘
              (fun pre => bit :: pre))).sum =
            ((m.step q bit).map
              (fun q' => (ps.map (fun pre => ((run m q' pre).map (count m c)).getD 0)).sum)).getD 0 := by
        induction ps with
        | nil =>
            cases m.step q bit <;> simp
        | cons pre ps ih =>
            simp only [List.map_cons, List.sum_cons, Function.comp_apply]
            cases hstep : m.step q bit with
            | none => simp [run_cons_getD, hstep, ih]
            | some q' => simp [run_cons_getD, hstep, ih]
      have sum_run_cons (q : Nat) (bit : Bool) (c : Nat) :
          (((words a).map (fun pre => bit :: pre)).map
              (fun pre => ((run m q pre).map (count m c)).getD 0)).sum =
            ((m.step q bit).map (fun q' => count m (a + c) q')).getD 0 := by
        rw [List.map_map]
        rw [sum_cons_getD (words a) q bit c]
        cases hstep : m.step q bit with
        | none => simp
        | some q' => exact (ih c q').symm
      intro b q
      rw [Nat.succ_add]
      simp only [count, words, List.map_append, List.sum_append]
      rw [sum_run_cons q false b, sum_run_cons q true b]

theorem sound_Cofactor : BalancedNesting.Cofactor.Goal := by
  intro q a b
  exact cofactor_all model a b q

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_BalancedNesting_Cofactor
