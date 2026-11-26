import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

/-!
Sample signatures are not injective for strings longer than twelve bytes: a
middle byte outside all three windows can change without changing the
signature. A singleton build dictionary therefore cannot certify such a probe.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_UniqueBuildSamplesCertifyProbe

open TinylambOpt.ResearchGoals.JitKernels.SampledEquality

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.SampledEquality.UniqueBuildSamplesCertifyProbe.Goal := by
  intro h
  let s13 : Bytes := List.replicate 13 5
  let probe : Bytes := s13.set 8 7
  have hnodup : (keys [s13]).Nodup := by simp [keys]
  have hkey : (keys [s13])[0]? = some (sig probe) := by native_decide
  have h' := h [s13] probe 0 hnodup hkey
  have hne : ¬ ([s13] : List Bytes)[0]? = some probe := by native_decide
  exact hne h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_UniqueBuildSamplesCertifyProbe
