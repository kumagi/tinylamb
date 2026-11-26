import TinylambOpt.ResearchGoals.JitKernels.InductionLowering

/-!
An odd stride in an even-sized ring need not be coprime with the capacity.
With six slots and stride three, the visit sequence repeats after two steps.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.Goal_InductionLowering_HoistedNonunitPointerStride

open TinylambOpt.ResearchGoals.JitKernels.InductionLowering

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.InductionLowering.HoistedNonunitPointerStride.Goal := by
  intro h
  have h' := h 6 3 0 (by omega) (by omega) (by omega) (by omega)
  have hne : ¬ ((List.range 6).map (fun i => (0 + i * 3) % 6)).Nodup := by
    native_decide
  exact hne h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_InductionLowering_HoistedNonunitPointerStride
