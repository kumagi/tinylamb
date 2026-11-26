import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

/-! Two homes in the same aligned block need not have the same probe result:
    the first home may terminate at a vacant slot before the second home. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ControlByteProbe_AlignedProbeStartReuse

open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

theorem refute :
    ¬ TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe.AlignedProbeStartReuse.Goal := by
  intro h
  let xs : List Slot := [.empty, .live 1 9 7, .live 1 9 8]
  have hh := h xs 0 1 2 9 (by omega) (by native_decide) (by native_decide) (by omega)
  have hne : (probe (ring xs 0) 9 ≠ probe (ring xs 1) 9) := by native_decide
  exact hne hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_ControlByteProbe_AlignedProbeStartReuse
