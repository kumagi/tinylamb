import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory.SingleEditDirectoryGrowth.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_RestartDirectory_SingleEditDirectoryGrowth

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
open TinylambOpt.ResearchGoals.Compression.Wire

theorem refute : ¬ SingleEditDirectoryGrowth.Goal := by
  intro h
  have := h [] [0, 0] 127 1 (by decide)
  revert this
  simp [directory, offset, streamBytes]
  decide

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_RestartDirectory_SingleEditDirectoryGrowth
