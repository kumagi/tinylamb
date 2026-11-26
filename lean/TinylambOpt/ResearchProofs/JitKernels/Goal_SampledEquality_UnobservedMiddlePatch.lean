import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

/-!
Patching a byte outside all three sample windows leaves the generated grouping
signature unchanged. This allows a SQL kernel to reuse cached bucket metadata
for an unsampled payload update, while still requiring verification of cached
hits when the patched byte matters.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_UnobservedMiddlePatch

open TinylambOpt.ResearchGoals.JitKernels.SampledEquality
open TinylambOpt.SimdGroupHash (byteSamples)

theorem window_set_eq (l : List UInt8) (i o : Nat) (c : UInt8)
    (hout : ¬ (o ≤ i && i < o + 4)) :
    ((l.set i c).drop o).take 4 = (l.drop o).take 4 := by
  apply List.ext_getElem?
  intro j
  rw [List.getElem?_take, List.getElem?_take, List.getElem?_drop, List.getElem?_drop]
  rw [List.getElem?_set]
  by_cases hj : j < 4
  · simp [hj]
    by_cases hei : i = o + j
    · subst i
      simp at hout
      omega
    · simp [hei]
  · simp [hj]

theorem sound : TinylambOpt.ResearchGoals.JitKernels.SampledEquality.UnobservedMiddlePatch.Goal := by
  intro s i c hi hcov
  simp [covered, offsets] at hcov
  have h0 : ¬ (0 ≤ i && i < 0 + 4) := by
    simp
    omega
  have h1 : ¬ ((s.length - 4) / 2 ≤ i && i < (s.length - 4) / 2 + 4) := by
    intro h
    simp at h
    have := hcov.2.1 h.1
    omega
  have h2 : ¬ (s.length - 4 ≤ i && i < s.length - 4 + 4) := by
    intro h
    simp at h
    have hle : s.length ≤ i + 4 := by omega
    have := hcov.2.2 hle
    omega
  unfold sig byteSamples
  simp only [List.length_set, Prod.mk.injEq, true_and]
  exact ⟨window_set_eq s i 0 c h0, window_set_eq s i ((s.length - 4) / 2) c h1,
    window_set_eq s i (s.length - 4) c h2⟩

end TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_UnobservedMiddlePatch
