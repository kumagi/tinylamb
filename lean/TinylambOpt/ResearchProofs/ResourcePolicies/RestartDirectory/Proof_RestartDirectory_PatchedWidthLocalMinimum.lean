import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_RestartDirectory_PatchedWidthLocalMinimum
open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
theorem refute : ¬ PatchedWidthLocalMinimum.Goal := by
  intro h
  let xs : List Nat := [0, 0, 129]
  have h' := h xs 8 (by decide) (by decide) (by decide) 0
  revert h'
  decide
end Nov26.Proof_RestartDirectory_PatchedWidthLocalMinimum
