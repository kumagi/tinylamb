import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets
import TinylambOpt.ResearchProofs.JitKernels.BailoutPacketsLemmas

/-! An error ballot over a selection vector is nonempty exactly when some
    gathered lane faults. SQL may therefore intersect the error mask with the
    demanded ordinals before taking the cold path. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BailoutPackets_ErrorBitsSelectedOnly

open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets
open Nov26.BailoutPacketsLemmas

theorem sound : TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.ErrorBitsSelectedOnly.Goal := by
  intro xs sel hb
  calc
    (first (gather xs sel)).isSome = (gather xs sel).any bad := by
      change (firstAt (gather xs sel) 0).isSome = (gather xs sel).any bad
      exact first_any_bad (gather xs sel) 0
    _ = sel.any (fun i => xs[i]?.any bad) := gather_any xs sel hb

end TinylambOpt.ResearchProofs.JitKernels.Goal_BailoutPackets_ErrorBitsSelectedOnly
