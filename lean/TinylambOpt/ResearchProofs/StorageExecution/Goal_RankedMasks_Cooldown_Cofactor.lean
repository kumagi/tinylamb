import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `Cooldown.Cofactor` — Compute a parallel shard count by conditioning on its prefix
  state.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_Cooldown_Cofactor

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.Cooldown

-- Truth check on small inputs, including states the machine rarely reaches.
private def check : Bool :=
  (List.range 10).all fun a =>
  (List.range 5).all fun b =>
  (List.range 5).all fun q =>
    decide (count model (a + b) q =
      ((words a).map (fun pre => ((run model q pre).map (count model b)).getD 0)).sum)

#eval check

private theorem sum_map_zero {α : Type} (xs : List α) : (xs.map fun _ => (0 : Nat)).sum = 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]

-- Splitting a count at every length-`a` prefix is machine-independent:
-- `run` is deterministic, so a prefix either dies (contributing 0) or lands in a
-- unique state whose continuation count is given by the induction hypothesis.
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
        cases hs : m.step q bit with
        | none => simp [run, List.foldlM_cons, hs]
        | some q' => simp [run, List.foldlM_cons, hs]
      have sum_run_cons (q : Nat) (bit : Bool) (c : Nat) :
          (((words a).map (fun pre => bit :: pre)).map
              (fun pre => ((run m q pre).map (count m c)).getD 0)).sum =
            ((m.step q bit).map (fun q' => count m (a + c) q')).getD 0 := by
        simp only [List.map_map]
        change ((words a).map (fun pre => ((run m q (bit :: pre)).map (count m c)).getD 0)).sum =
          ((m.step q bit).map (fun q' => count m (a + c) q')).getD 0
        cases hstep : m.step q bit with
        | none =>
            rw [show ((words a).map (fun pre => ((run m q (bit :: pre)).map (count m c)).getD 0)) =
                (words a).map (fun _ => (0 : Nat)) by
                  apply List.map_congr_left
                  intro pre hp
                  simp [run_cons_getD, hstep]]
            simp
            exact sum_map_zero (words a)
        | some q' =>
            rw [show ((words a).map (fun pre => ((run m q (bit :: pre)).map (count m c)).getD 0)) =
                (words a).map (fun pre => ((run m q' pre).map (count m c)).getD 0) by
                  apply List.map_congr_left
                  intro pre hp
                  simp [run_cons_getD, hstep]]
            exact (ih c q').symm
      intro b q
      rw [Nat.succ_add]
      simp only [count, words, List.map_append, List.sum_append]
      rw [sum_run_cons q false b, sum_run_cons q true b]

theorem sound_Cofactor : Cooldown.Cofactor.Goal := by
  intro q a b
  exact cofactor_all model a b q

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_Cooldown_Cofactor
