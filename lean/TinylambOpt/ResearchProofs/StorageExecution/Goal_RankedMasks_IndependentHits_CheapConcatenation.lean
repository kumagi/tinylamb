import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_IndependentHits_CheapConcatenation

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits

/-! Concatenating a separator (an unselected row) between two legal pages is
legal: the separator resets the no-adjacent-hit state to `0`. -/

private theorem run_append (m : Machine) (q : Nat) (xs ys : List Bool) :
    run m q (xs ++ ys) = (run m q xs).bind (fun q' => run m q' ys) := by
  induction xs generalizing q with
  | nil => simp [run]
  | cons b bs ih =>
    simp only [List.cons_append, run, List.foldlM]
    cases hstep : m.step q b with
    | none => simp
    | some q' => simp

theorem sound_CheapConcatenation : IndependentHits.CheapConcatenation.Goal := by
  intro q a b ha hb
  cases hrun : run model q a with
  | none =>
    simp [legal, hrun] at ha
  | some qa =>
    have hconcat : run model q (a ++ [false] ++ b) = run model 0 b := by
      rw [List.append_assoc, run_append, hrun]
      simp [run, model]
    rw [show legal model q (a ++ [false] ++ b) = legal model 0 b by
      simp only [legal, hconcat]]
    exact hb

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_IndependentHits_CheapConcatenation
