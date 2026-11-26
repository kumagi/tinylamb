import TinylambOpt.ResearchGoals.JitKernels.CodeReachability
import TinylambOpt.ResearchProofs.JitKernels.Common_CodeReachability

/-! Filtering the heap down to marked objects keeps every direct-call table
that is reachable from the roots: the first object with a live key survives
the filter at the same position. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_EvictionPreservesDirectTargets

open TinylambOpt.ResearchGoals.JitKernels.CodeReachability
open TinylambOpt.ResearchProofs.JitKernels.Common_CodeReachability

theorem sound : EvictionPreservesDirectTargets.Goal := by
  intro heap roots
  dsimp
  intro k hk
  exact edges_retain heap (mark heap roots heap.length) k hk

end TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_EvictionPreservesDirectTargets
