import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MorselRetirement.Goal_DuplicateConsistentRetirement

open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement


/-!
Reordering completion messages cannot change ordered output: with consistent duplicate reports, find? yields the same rows for each id under any permutation of the report list.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

private theorem lookup_eq_some_of_mem (rs : List Report) (hc : consistent rs)
    (r : Report) (hr : r ∈ rs) (hri : r.1 = i) : lookup rs i = some r.2 := by
  unfold lookup
  have hsome : (rs.find? (fun q => q.1 == i)).isSome = true := by
    rw [List.find?_isSome]
    exact ⟨r, hr, by simp [hri]⟩
  obtain ⟨f, hf⟩ := Option.isSome_iff_exists.mp hsome
  have hfspec := List.find?_eq_some_iff_append.mp hf
  have hfi : f.1 = i := by simpa using hfspec.1
  have hfmem : f ∈ rs := List.mem_of_find?_eq_some hf
  have hrow : f.2 = r.2 := hc f hfmem r hr (by omega)
  simp [hf, hrow]

private theorem lookup_eq_of_perm_of_consistent (a b : List Report) (hc : consistent a)
    (hp : a.Perm b) (i : Nat) : lookup a i = lookup b i := by
  by_cases h : ∃ r ∈ a, r.1 = i
  · rcases h with ⟨r, hr, hri⟩
    have hla := lookup_eq_some_of_mem a hc r hr hri
    have hcb : consistent b := by
      intro x hx y hy hxy
      exact hc x (hp.symm.mem_iff.mp hx) y (hp.symm.mem_iff.mp hy) hxy
    have hrb : r ∈ b := hp.mem_iff.mp hr
    have hlb := lookup_eq_some_of_mem b hcb r hrb hri
    rw [hla, hlb]
  · have hna : lookup a i = none := by
      unfold lookup
      have hf : a.find? (fun q => q.1 == i) = none := by
        rw [List.find?_eq_none]
        intro x hx hxi
        exact h ⟨x, hx, by simpa using hxi⟩
      simp [hf]
    have hnb : lookup b i = none := by
      unfold lookup
      have hf : b.find? (fun q => q.1 == i) = none := by
        rw [List.find?_eq_none]
        intro x hx hxi
        exact h ⟨x, hp.symm.mem_iff.mp hx, by simpa using hxi⟩
      simp [hf]
    rw [hna, hnb]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.DuplicateConsistentRetirement.Goal := by
  intro a b start count hc hp
  have hl := lookup_eq_of_perm_of_consistent a b hc hp
  unfold emit readyIds
  simp [hl]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MorselRetirement.Goal_DuplicateConsistentRetirement
