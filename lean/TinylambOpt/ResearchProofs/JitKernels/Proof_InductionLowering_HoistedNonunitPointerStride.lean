import TinylambOpt.ResearchGoals.JitKernels.InductionLowering

/-! Independent proof record for the InductionLowering.HoistedNonunitPointerStride research goal: the proposed rewrite is unsound and this file exhibits a counterexample. -/

/-!
An odd step is not necessarily coprime with an even slot count. With six slots
and step three the ring visit repeats after two steps, so duplicate detection
cannot be dropped from the non-unit stride specialization.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace Nov26.Proof_InductionLowering_HoistedNonunitPointerStride
open TinylambOpt.ResearchGoals.JitKernels.InductionLowering
open TinylambOpt.ResearchGoals.JitKernels.InductionLowering.HoistedNonunitPointerStride
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.JitKernels.InductionLowering

theorem refute : ¬ Goal := by
  intro h
  have t := h 6 3 0 (by omega) (by omega) (by omega) (by omega)
  have hbad : ¬ ((List.range 6).map (fun i => (0 + i * 3) % 6)).Nodup := by
    decide
  exact hbad t

end Nov26.Proof_InductionLowering_HoistedNonunitPointerStride
