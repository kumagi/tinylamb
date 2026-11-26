import TinylambOpt.ResearchGoals.JitKernels.InductionLowering
set_option linter.unusedSimpArgs false

/-!
A selection vector may leave gaps in the input ordinals. Running the
strength-reduced recurrence once per surviving row therefore produces values
at `0, 1, ...` while the selected rows may be `4, 7, ...`; the iteration
coordinate cannot be replaced by the surviving-row count. A single surviving
row at ordinal 1 is a concrete counterexample.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_InductionLowering_SkippedIterationRecurrence

open TinylambOpt.ResearchGoals.JitKernels.InductionLowering

theorem refute : ¬ SkippedIterationRecurrence.Goal := by
  intro h
  have t := h 1 0 0 2 (fun i => i == 1)
  have hbad : ¬ (let sel := (List.range 2).filter (fun i => i == 1);
      differences 1 0 0 0 1 sel.length = sel.map (fun (i : Nat) => poly 1 0 0 i)) := by
    native_decide
  exact hbad t

end TinylambOpt.ResearchProofs.JitKernels.Goal_InductionLowering_SkippedIterationRecurrence
