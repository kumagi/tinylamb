import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! Refutation: dropping all bits on which equal-bodied guards disagree admits
rows matching each guard on a different subset of those bits; neither original
version accepts such a row. -/
namespace Nov26.Proof_VersionDispatch_CommonRequirementsCoverUnion
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CommonRequirementsCoverUnion
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CommonRequirementsCoverUnion
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CommonRequirementsCoverUnion
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CommonRequirementsCoverUnion

theorem refute : ¬ Goal := by
  intro h
  let a : Entry := {mask := 3, value := 0, code := 7}
  let b : Entry := {mask := 3, value := 3, code := 7}
  have hbad := h a b 1 (by decide) (by decide)
  have hne : dispatch [a, b] 1 ≠
      dispatch [{a with mask := a.mask &&& ~~~(a.value ^^^ b.value)}] 1 := by decide
  exact hne hbad

end Nov26.Proof_VersionDispatch_CommonRequirementsCoverUnion