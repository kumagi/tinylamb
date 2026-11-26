import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention
/-!
Registers the checked result for `WindowRetention.CountedWindowMembership.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_CountedWindowMembership
open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention
open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.CountedWindowMembership

theorem sound : Goal := by
  intro x y k middle
  rw [List.mem_eraseDups]
  constructor
  · intro hk
    rw [List.mem_append] at hk
    rcases hk with hk | hk
    · have hcm : (middle.count k) ≥ 1 := List.count_pos_iff.mpr hk
      have hcx : (x :: middle).count k = middle.count k + if x == k then 1 else 0 := by
        simp [List.count_cons]
      have _hcy : (middle ++ [y]).count k = middle.count k + if y == k then 1 else 0 := by
        rw [List.count_append]; simp [List.count_cons]
      omega
    · simp only [List.mem_singleton] at hk
      subst hk
      simp [List.count_cons, List.count_append]
  · intro h
    by_cases hmem : k ∈ middle ++ [y]
    · exact hmem
    · exfalso
      rw [List.mem_append, List.mem_singleton, not_or] at hmem
      rcases hmem with ⟨hm1, hm2⟩
      have hm : middle.count k = 0 := by
        rwa [List.count_eq_zero]
      have hy : (y == k) = false := by
        have : y ≠ k := fun he => hm2 he.symm
        simp [this]
      have hx : (x :: middle).count k = if x == k then 1 else 0 := by
        simp [List.count_cons, hm]
      simp [hx, hy] at h
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_CountedWindowMembership
