import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_RestartDirectory_ExceptionCountWidthChoice
open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
theorem refute : ¬ ExceptionCountWidthChoice.Goal := by
  intro h
  let a : List Nat := [8, 0, 0, 0, 0, 0, 0, 0]
  let b : List Nat := [127, 0, 0, 0, 0, 0, 0, 0]
  have h' := h 3 a b (by decide) (by decide)
  revert h'
  decide
end Nov26.Proof_RestartDirectory_ExceptionCountWidthChoice
