import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets
import TinylambOpt.ResearchProofs.JitKernels.BailoutPacketsLemmas

/-! An arbitrary error ballot can be restricted to demanded ordinals before
taking the cold path: a selected fault exists exactly when some gathered lane
faults. Execution may intersect the error mask with the selection mask. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_BailoutPackets_ErrorBitsSelectedOnly

open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets
open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.ErrorBitsSelectedOnly
open Nov26.BailoutPacketsLemmas

theorem sound : Goal := by
  intro xs sel hb
  calc
    (first (gather xs sel)).isSome = (gather xs sel).any bad := by
      change (firstAt (gather xs sel) 0).isSome = (gather xs sel).any bad
      exact first_any_bad (gather xs sel) 0
    _ = sel.any (fun i => xs[i]?.any bad) := gather_any xs sel hb

end Nov26.Proof_BailoutPackets_ErrorBitsSelectedOnly
