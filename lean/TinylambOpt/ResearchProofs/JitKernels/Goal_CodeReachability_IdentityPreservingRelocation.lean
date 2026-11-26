import TinylambOpt.ResearchGoals.JitKernels.CodeReachability
import TinylambOpt.ResearchProofs.JitKernels.Common_CodeReachability

/-! An injective relocation of code identities transports the whole reachability
graph: original keys are in the marked set exactly when their images are in the
marked set of the relocated heap. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_IdentityPreservingRelocation

open TinylambOpt.ResearchGoals.JitKernels.CodeReachability
open TinylambOpt.ResearchProofs.JitKernels.Common_CodeReachability

theorem sound : TinylambOpt.ResearchGoals.JitKernels.CodeReachability.IdentityPreservingRelocation.Goal := by
  intro heap roots f hf n k
  exact mark_map_of_injective heap roots f hf n k

end TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_IdentityPreservingRelocation
