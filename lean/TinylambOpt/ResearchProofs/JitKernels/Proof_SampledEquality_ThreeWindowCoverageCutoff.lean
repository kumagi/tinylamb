import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Soundness of "omit exact byte verification through length twelve": for strings of
length `n ≤ 12` the three sampled windows `0`, `(n-4)/2`, `n-4` cover every byte position
(`(n-4)/2 ≤ 4` and `n-4 ≤ (n-4)/2 + 4` both hold), so equal length plus equal raw sample
windows already forces full string equality — a JIT'd grouping kernel may skip the
byte-exact recheck for keys up to twelve bytes.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.SampledEquality_ThreeWindowCoverageCutoff
open TinylambOpt.ResearchGoals.JitKernels.SampledEquality

private theorem covered_of_lt (n i : Nat) (hn : n ≤ 12) (hi : i < n) : covered n i = true := by
  have hd : i < 4 ∨ ((n-4)/2 ≤ i ∧ i < (n-4)/2 + 4) ∨ n-4 ≤ i := by omega
  rw [covered, offsets, List.any_eq_true]
  rcases hd with h | ⟨h1, h2⟩ | h
  · exact ⟨0, by simp, by simp [h]⟩
  · exact ⟨(n-4)/2, by simp, by simp [h1, h2]⟩
  · exact ⟨n-4, by simp, by simp [h, (by omega : i < n - 4 + 4)]⟩

private theorem agreeWin (a b : Bytes) (hl : a.length = b.length)
    (he1 : a.take 4 = b.take 4)
    (he2 : List.take 4 (List.drop ((a.length - 4) / 2) a) =
      List.take 4 (List.drop ((b.length - 4) / 2) b))
    (he3 : (a.drop (a.length - 4)).take 4 = (b.drop (b.length - 4)).take 4) :
    ∀ i, covered a.length i = true → a[i]? = b[i]? := by
  intro i hcov
  rw [covered, offsets, List.any_eq_true] at hcov
  obtain ⟨o, ho, hp⟩ := hcov
  simp at hp ho
  obtain ⟨hle, hlt⟩ := hp
  rcases ho with rfl | rfl | rfl
  · have h := congrArg (fun l => l[i]?) he1
    simp only [List.getElem?_take] at h
    rw [if_pos hlt] at h
    rw [if_pos hlt] at h
    exact h
  · have h := congrArg (fun l => l[i - (a.length - 4) / 2]?) he2
    simp only [List.getElem?_take] at h
    rw [if_pos (by omega : i - (a.length - 4) / 2 < 4)] at h
    rw [if_pos (by omega : i - (a.length - 4) / 2 < 4)] at h
    simp only [List.getElem?_drop] at h
    rw [← hl] at h
    rw [Nat.add_sub_of_le hle] at h
    exact h
  · have h := congrArg (fun l => l[i - (a.length - 4)]?) he3
    simp only [List.getElem?_take] at h
    rw [if_pos (by omega : i - (a.length - 4) < 4)] at h
    rw [if_pos (by omega : i - (a.length - 4) < 4)] at h
    simp only [List.getElem?_drop] at h
    rw [← hl] at h
    rw [Nat.add_sub_of_le hle] at h
    exact h

theorem sound : ThreeWindowCoverageCutoff.Goal := by
  intro a b hlen hs
  rw [sig, sig, Prod.mk.injEq] at hs
  obtain ⟨hl, hw⟩ := hs
  dsimp only [TinylambOpt.SimdGroupHash.byteSamples] at hw
  rw [Prod.mk.injEq, Prod.mk.injEq] at hw
  obtain ⟨he1, he2, he3⟩ := hw
  refine List.ext_getElem? (fun i => ?_)
  by_cases hi : i < a.length
  · exact agreeWin a b hl he1 he2 he3 i (covered_of_lt a.length i hlen hi)
  · rw [List.getElem?_eq_none (by omega : a.length ≤ i),
        List.getElem?_eq_none (by omega : b.length ≤ i)]

end TinylambOpt.ResearchProofs.JitKernels.SampledEquality_ThreeWindowCoverageCutoff
