import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_PatchedWidthLocalMinimum

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory


/-!
A width that beats both immediate neighbours is not necessarily the global
optimum of the patched-frame byte charge.

Refuted by `xs = [0,0,129]`, `w = 8`, `v = 0`: the charges are 6, 4, 5 for
widths 7, 8, 9, but width 0 charges only 3 bytes.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_PatchedWidthLocalMinimum

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

theorem refute : ¬ PatchedWidthLocalMinimum.Goal := by
  intro h
  let xs : List Nat := [0, 0, 129]
  have hleft : patchedBytes 8 xs < patchedBytes (8 - 1) xs := by native_decide
  have hright : patchedBytes 8 xs < patchedBytes (8 + 1) xs := by native_decide
  have hbad := h xs 8 (by decide) hleft hright 0
  have hcontra : ¬ patchedBytes 8 xs ≤ patchedBytes 0 xs := by native_decide
  exact hcontra hbad

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_PatchedWidthLocalMinimum
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_PatchedWidthLocalMinimum
