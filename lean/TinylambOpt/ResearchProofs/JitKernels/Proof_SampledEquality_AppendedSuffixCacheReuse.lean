import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutation of "appending a common suffix preserves the sample-equality class". Two
13-byte keys can share a signature while differing in an unsampled position: at length 13
the offsets are `[0, 4, 9]`, so byte 8 is outside every window. Taking `a = replicate 13 1`
and `b` equal to `a` except `b[8] = 2`, the signatures agree, but after appending the
single byte `[9]` the string length becomes 14 and the sampled windows shift to
`0, 5, 10` — now byte 8 is inside the middle window, so the signatures of `a ++ [9]` and
`b ++ [9]` differ. A cache may not reuse the old equality class across appends.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.SampledEquality_AppendedSuffixCacheReuse
open TinylambOpt.ResearchGoals.JitKernels.SampledEquality

private def a : Bytes := List.replicate 13 1
private def b : Bytes := [1,1,1,1,1,1,1,1,2,1,1,1,1]

theorem refute : ¬ AppendedSuffixCacheReuse.Goal := by
  intro h
  have hs : sig a = sig b := by decide
  have hd : sig (a ++ [9]) ≠ sig (b ++ [9]) := by decide
  exact absurd (h a b [9] hs) hd

end TinylambOpt.ResearchProofs.JitKernels.SampledEquality_AppendedSuffixCacheReuse
