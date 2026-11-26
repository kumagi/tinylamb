import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

/-! Two homes in the same alignment block need not see the same lanes: starting
at the second slot puts the vacancy ahead of the key, so the result differs. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe.Goal_AlignedProbeStartReuse

open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

theorem refute : ¬ AlignedProbeStartReuse.Goal := by
  intro h
  have hw := h [.live 1 9 7, .empty] 0 1 2 9 (by decide) (by decide) (by decide) (by decide)
  have hdiff : probe (ring [.live 1 9 7, .empty] 0) 9 ≠ probe (ring [.live 1 9 7, .empty] 1) 9 := by
    native_decide
  exact hdiff hw

end TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe.Goal_AlignedProbeStartReuse
