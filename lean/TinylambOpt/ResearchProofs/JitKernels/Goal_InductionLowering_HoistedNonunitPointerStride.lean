import TinylambOpt.ResearchGoals.JitKernels.InductionLowering

/-!
An odd step is not necessarily coprime with an even slot count. With six slots
and step three the ring visit repeats after two steps, so duplicate detection
cannot be dropped from the non-unit stride specialization.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.InductionLowering.Goal_HoistedNonunitPointerStride

open TinylambOpt.ResearchGoals.JitKernels.InductionLowering

theorem refute : ¬ HoistedNonunitPointerStride.Goal := by
  intro h
  have t := h 6 3 0 (by omega) (by omega) (by omega) (by omega)
  have hbad : ¬ ((List.range 6).map (fun i => (0 + i * 3) % 6)).Nodup := by
    decide
  exact hbad t

end TinylambOpt.ResearchProofs.JitKernels.InductionLowering.Goal_HoistedNonunitPointerStride
