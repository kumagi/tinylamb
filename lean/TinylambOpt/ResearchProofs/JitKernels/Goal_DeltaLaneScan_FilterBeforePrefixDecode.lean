import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-! Removing unselected deltas before the prefix scan also removes their
contribution from every later row. Keeping only row one of deltas `[1,2]`
gives decoded value two, while the decoded row one of the original frame is
three, so filtering first is not equivalent. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_FilterBeforePrefixDecode

open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

theorem refute : ¬ FilterBeforePrefixDecode.Goal := by
  intro h
  have hw := h 0 [1, 2] (fun i => i == 1)
  simp [values] at hw

end TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_FilterBeforePrefixDecode
