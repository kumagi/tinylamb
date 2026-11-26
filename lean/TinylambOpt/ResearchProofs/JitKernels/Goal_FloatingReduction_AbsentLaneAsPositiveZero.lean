import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-! Materializing an absent lane as `+0.0` changes the sign of a `-0.0`
accumulator: `-0.0 + +0.0` is `+0.0`. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_AbsentLaneAsPositiveZero

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

theorem refute : ¬ AbsentLaneAsPositiveZero.Goal := by
  intro h
  have h' := h (Float.ofBits 0x8000000000000000) ([none] : List (Option Float))
    (by native_decide)
  have hbad : ¬ same
      (total (Float.ofBits 0x8000000000000000) (([none] : List (Option Float)).map (fun v => v.getD 0)))
      (total (Float.ofBits 0x8000000000000000) (live [none])) := by
    unfold same bits total live
    native_decide
  exact hbad h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_AbsentLaneAsPositiveZero
