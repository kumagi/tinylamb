import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

/-!
Unique fingerprints in the build dictionary do not make the fingerprint map
injective on probes outside it. Two longer keys that differ only in an unsampled
byte share a signature, so a sampled-hit test alone cannot identify the row.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_UniqueBuildSamplesCertifyProbe

open TinylambOpt.ResearchGoals.JitKernels.SampledEquality
open TinylambOpt.SimdGroupHash (byteSamples)

theorem refute : ¬ UniqueBuildSamplesCertifyProbe.Goal := by
  intro h
  have got := h [List.replicate 13 (0 : UInt8)]
    (List.replicate 8 (0 : UInt8) ++ [1] ++ List.replicate 4 (0 : UInt8)) 0
  simp [keys, sig, byteSamples] at got

end TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_UniqueBuildSamplesCertifyProbe
