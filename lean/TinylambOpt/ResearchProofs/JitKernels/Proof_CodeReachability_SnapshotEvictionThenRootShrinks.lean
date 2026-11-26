import TinylambOpt.ResearchGoals.JitKernels.CodeReachability
import TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_SnapshotEvictionThenRootShrinks

/-! A second collection after active-frame roots shrink may reuse the cache
already trimmed by the first collection. The bodies reachable from the smaller
root set inside that trimmed heap are exactly the bodies reachable from the same
roots in the original heap, so the retained direct-call targets are unchanged. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace Nov26.Proof_CodeReachability_SnapshotEvictionThenRootShrinks

open TinylambOpt.ResearchGoals.JitKernels.CodeReachability
open TinylambOpt.ResearchGoals.JitKernels.CodeReachability.SnapshotEvictionThenRootShrinks

theorem sound : Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_SnapshotEvictionThenRootShrinks.sound

end Nov26.Proof_CodeReachability_SnapshotEvictionThenRootShrinks
