import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

/-! A vacancy anywhere in the group is not a terminator: an earlier live lane
may already hold the key. The counterexample keeps a match before the empty
byte, so SQL lookup must still return that payload. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe.Goal_WholeControlGroupTermination

open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

theorem refute : ¬ WholeControlGroupTermination.Goal := by
  intro h
  have hw := h [.live 1 9 7, .empty] 9
  simp [probe, untilEmpty, value] at hw

end TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe.Goal_WholeControlGroupTermination
