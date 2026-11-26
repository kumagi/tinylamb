import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
import TinylambOpt.ResearchProofs.Compression.Wire
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory.PatchedFrameSplitOverhead.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_RestartDirectory_PatchedFrameSplitOverhead

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory


/-!
Splitting a patched frame at the width chosen for the whole frame can add at
most one byte of rounding per integer header (lane bits and exception count).

The row values and exception bitmap are merely partitioned; no frame content
changes, so scans observe the same values before and after the split.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_PatchedFrameSplitOverhead

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchProofs.Compression.Wire

theorem sound : PatchedFrameSplitOverhead.Goal := by
  intro w a b
  have h1 := sound_SplitPenalty (w * a.length) (w * b.length)
  have h2 := sound_SplitPenalty a.length b.length
  unfold patchedBytes streamBytes
  rw [List.length_append, Nat.mul_add, List.filter_append, List.map_append, List.sum_append]
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_PatchedFrameSplitOverhead
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_RestartDirectory_PatchedFrameSplitOverhead
