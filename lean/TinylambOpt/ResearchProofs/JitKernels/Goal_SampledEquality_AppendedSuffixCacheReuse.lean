import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

/-!
Appending a common suffix shifts the middle sample window. A byte that was
unsampled in the original keys can enter that window, so the old equality class
must not be reused for the extended keys.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_AppendedSuffixCacheReuse

open TinylambOpt.ResearchGoals.JitKernels.SampledEquality
open TinylambOpt.SimdGroupHash (byteSamples)

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.SampledEquality.AppendedSuffixCacheReuse.Goal := by
  intro h
  have got := h (List.replicate 13 (0 : UInt8))
    (List.replicate 8 (0 : UInt8) ++ [1] ++ List.replicate 4 (0 : UInt8))
    [0]
  simp [sig, byteSamples] at got

end TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_AppendedSuffixCacheReuse
