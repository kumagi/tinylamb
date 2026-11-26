import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutation of "unique build signatures certify every probe". Signature uniqueness in the
build pool says nothing about strings outside the pool. For `n = 13` the offsets are
`[0, 4, 9]`, so the sampled windows are bytes 0-3, 4-7 and 9-12: byte position 8 is never
observed. With `a = replicate 13 1` and `b = a.set 8 2` the single-key pool `[a]` has
distinct signatures and its key at index 0 equals `sig b`, yet `pool[0]? = some a ≠ some b`
because bytes 8 differ. A join kernel must still verify the full probe bytes.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.SampledEquality_UniqueBuildSamplesCertifyProbe
open TinylambOpt.ResearchGoals.JitKernels.SampledEquality

theorem refute : ¬ UniqueBuildSamplesCertifyProbe.Goal := by
  intro h
  have h1 : (keys [List.replicate 13 1]).Nodup := by
    simp only [keys, List.map_cons, List.nodup_cons]
    simp
  have h2 : (keys [List.replicate 13 1])[0]? = some (sig ((List.replicate 13 1).set 8 2)) := by
    decide
  have h3 := h [List.replicate 13 1] ((List.replicate 13 1).set 8 2) 0 h1 h2
  exact absurd h3 (by decide)

end TinylambOpt.ResearchProofs.JitKernels.SampledEquality_UniqueBuildSamplesCertifyProbe
