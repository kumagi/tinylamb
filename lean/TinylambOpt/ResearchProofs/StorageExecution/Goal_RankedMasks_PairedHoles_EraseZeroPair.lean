import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PairedHoles_EraseZeroPair

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PairedHoles

private theorem run_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    run m q (b :: bs) = (m.step q b).bind (fun q' => run m q' bs) := by
  simp [run, List.foldlM]

private theorem run_append (m : Machine) (q : Nat) (a b : List Bool) :
    run m q (a ++ b) = (run m q a).bind (fun q' => run m q' b) := by
  induction a generalizing q with
  | nil => simp [run]
  | cons x xs ih => simp [run_cons, ih, Option.bind_assoc]

/-- Every state reachable from a state below 2 is still below 2. -/
private theorem ph_run_state : ∀ (xs : List Bool) (q : Nat), q < 2 →
    run model q xs = none ∨ ∃ q', q' < 2 ∧ run model q xs = some q'
  | [], q, hq => Or.inr ⟨q, hq, by simp [run]⟩
  | b :: bs, q, hq => by
    cases b with
    | false =>
      have hs : model.step q false = some (1 - q) := by
        simp [model]
      rw [run_cons, hs]
      simp only [Option.bind_some]
      exact ph_run_state bs (1 - q) (by omega)
    | true =>
      cases q with
      | zero =>
        have hs : model.step 0 true = some 0 := by
          simp [model]
        rw [run_cons, hs]
        simp only [Option.bind_some]
        exact ph_run_state bs 0 (by decide)
      | succ qn =>
        have hqn : qn = 0 := by omega
        subst qn
        simp [run_cons, model]

/-- On the reachable states, two consecutive zero bits are the identity. -/
private theorem ph_run_ff (q : Nat) (hq : q < 2) :
    run model q [false, false] = some q := by
  cases q with
  | zero => simp [run, List.foldlM, model]
  | succ qn =>
    have hqn : qn = 0 := by omega
    subst qn
    simp [run, List.foldlM, model]

theorem sound_EraseZeroPair : PairedHoles.EraseZeroPair.Goal := by
  intro a b
  have hrun : run model 0 (a ++ [false, false] ++ b) = run model 0 (a ++ b) := by
    simp only [run_append]
    cases ha : run model 0 a with
    | none => simp
    | some qa =>
      rcases ph_run_state a 0 (by decide) with hnone | ⟨q', hq', hrun'⟩
      · simp [hnone] at ha
      · have hqa : qa = q' := by simpa using ha.symm.trans hrun'
        subst q'
        simp [ph_run_ff qa hq']
  simp only [legal, hrun]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PairedHoles_EraseZeroPair
