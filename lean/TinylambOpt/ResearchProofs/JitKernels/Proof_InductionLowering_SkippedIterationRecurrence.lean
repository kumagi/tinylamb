import TinylambOpt.ResearchGoals.JitKernels.InductionLowering

/-! Independent proof record for the InductionLowering.SkippedIterationRecurrence research goal: the proposed rewrite is unsound and this file exhibits a counterexample. -/

/-!
A selection vector may leave gaps in the input ordinals. Running the
strength-reduced recurrence once per surviving row therefore produces values
at `0, 1, ...` while the selected rows may be `4, 7, ...`; the iteration
coordinate cannot be replaced by the surviving-row count. A single surviving
row at ordinal 1 is a concrete counterexample.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace Nov26.Proof_InductionLowering_SkippedIterationRecurrence
open TinylambOpt.ResearchGoals.JitKernels.InductionLowering
open TinylambOpt.ResearchGoals.JitKernels.InductionLowering.SkippedIterationRecurrence
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.JitKernels.InductionLowering

theorem refute : ¬ Goal := by
  intro h
  have t := h 1 0 0 2 (fun i => i == 1)
  have hbad : ¬ (let sel := (List.range 2).filter (fun i => i == 1);
      differences 1 0 0 0 1 sel.length = sel.map (fun (i : Nat) => poly 1 0 0 i)) := by
    decide
  exact hbad t

end Nov26.Proof_InductionLowering_SkippedIterationRecurrence
