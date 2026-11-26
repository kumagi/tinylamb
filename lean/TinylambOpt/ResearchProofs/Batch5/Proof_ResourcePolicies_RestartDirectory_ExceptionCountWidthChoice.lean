import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_ExceptionCountWidthChoice

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory


/-!
The number of exceptions alone does not decide whether the patched frame beats
the fixed-width packed frame, because exception payloads have their own varint
width.

Refuted by `w = 0`, `a = [0,1]`, `b = [0,16]`: both frames have one exception,
but the patched charge exceeds the packed charge for `a` and not for `b`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_ExceptionCountWidthChoice

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
open TinylambOpt.ResearchGoals.Compression.Wire

theorem refute : ¬ ExceptionCountWidthChoice.Goal := by
  intro h
  let a : List Nat := [0, 1]
  let b : List Nat := [0, 16]
  have hlen : a.length = b.length := by native_decide
  have hcnt : (a.filter (flagged 0)).length = (b.filter (flagged 0)).length := by
    native_decide
  have hbad := h 0 a b hlen hcnt
  have hleft : ¬ patchedBytes 0 a ≤ packedSize a := by native_decide
  have hright : patchedBytes 0 b ≤ packedSize b := by native_decide
  exact hleft (hbad.mpr hright)

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_ExceptionCountWidthChoice
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_ExceptionCountWidthChoice
