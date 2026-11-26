import TinylambOpt.ResearchGoals.JitKernels.CodeReachability
import TinylambOpt.ResearchProofs.JitKernels.Common_CodeReachability

/-! Replacing a direct branch by a generic stub that has at least the same
outgoing targets cannot enlarge the reachable set: every growth step of the
patched graph is contained in the corresponding step of the original graph. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_PatchOneJumpUpdatesMarkSet

open TinylambOpt.ResearchGoals.JitKernels.CodeReachability
open TinylambOpt.ResearchProofs.JitKernels.Common_CodeReachability

theorem sound : PatchOneJumpUpdatesMarkSet.Goal := by
  intro a b roots hedges n k hk
  exact mark_edges_mono a b roots hedges n k hk

end TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_PatchOneJumpUpdatesMarkSet
