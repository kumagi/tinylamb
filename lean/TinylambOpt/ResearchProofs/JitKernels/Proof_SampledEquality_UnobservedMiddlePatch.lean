import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Soundness of "patching an unsampled byte leaves the signature unchanged": writing byte
`c` at index `i` of a string keeps `sig` fixed whenever `covered s.length i = false`, i.e.
whenever the JIT'd sample filter never reads that position. Then `i ≥ 4` (outside the head
window), the write falls outside the middle window (either before the drop point, so the
drop is unaffected, or at least 4 past it, so `take 4` never sees the patch), and
`i < s.length - 4` (before the tail window). Cached equality classes must be rechecked
only when a sampled byte moves; here no sampled byte moves.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.SampledEquality_UnobservedMiddlePatch
open TinylambOpt.ResearchGoals.JitKernels.SampledEquality

theorem sound : UnobservedMiddlePatch.Goal := by
  intro s i c hlt hcov
  unfold covered at hcov
  have hn : ¬ ((offsets s.length).any (fun o => o ≤ i && i < o + 4)) = true := by
    rw [hcov]
    exact Bool.false_ne_true
  have any_of : ∀ o ∈ offsets s.length, (o ≤ i && i < o + 4) = true →
      ((offsets s.length).any (fun o => o ≤ i && i < o + 4)) = true := by
    intro o ho hp
    rw [List.any_eq_true]
    exact ⟨o, ho, hp⟩
  have hge4 : 4 ≤ i := by
    by_cases hij : i < 4
    · exact absurd (any_of 0 (by simp [offsets]) (by simp [hij])) hn
    · omega
  have htail : i < s.length - 4 := by
    rcases Nat.lt_or_ge i (s.length - 4) with h | h
    · exact h
    · exact absurd (any_of (s.length - 4) (by simp [offsets])
        (by simp [h, (by omega : i < s.length - 4 + 4)])) hn
  have hmid : 4 ≤ i - (s.length - 4) / 2 ∨ i < (s.length - 4) / 2 := by
    by_cases hij : i < (s.length - 4) / 2
    · exact Or.inr hij
    · refine Or.inl (by
        have hnot : ¬ (i < (s.length - 4) / 2 + 4) := by
          intro h2
          exact absurd (any_of ((s.length - 4) / 2) (by simp [offsets])
            (by simp [(by omega : (s.length - 4) / 2 ≤ i), h2])) hn
        omega)
  unfold sig
  rw [Prod.mk.injEq]
  refine ⟨List.length_set, ?_⟩
  dsimp only [TinylambOpt.SimdGroupHash.byteSamples]
  rw [List.length_set, Prod.mk.injEq, Prod.mk.injEq]
  refine ⟨?_, ?_, ?_⟩
  · -- head window: the patch index is beyond `take 4`
    rw [List.take_set, List.set_eq_of_length_le (a := c)]
    rw [List.length_take]
    omega
  · -- middle window
    rcases hmid with hsub | hij
    · rw [List.drop_set, if_neg (by omega : ¬ (i < (s.length - 4) / 2)), List.take_set,
          List.set_eq_of_length_le (a := c)]
      rw [List.length_take]
      omega
    · rw [List.drop_set, if_pos hij]
  · -- tail window
    rw [List.drop_set, if_pos htail]

end TinylambOpt.ResearchProofs.JitKernels.SampledEquality_UnobservedMiddlePatch
