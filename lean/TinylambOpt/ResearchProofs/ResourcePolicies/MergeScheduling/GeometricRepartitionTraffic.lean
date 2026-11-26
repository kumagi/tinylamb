import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.GeometricRepartitionTraffic

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem shrinking_sum_le_five_mul_head : ∀ (xs : List Nat),
    shrinking xs → ∀ a, xs = a :: t → xs.sum ≤ 5 * a := by
  intro xs hs
  match xs, hs with
  | [], hs => intro a h; cases h
  | [a], hs =>
      intro a' h
      simp at h
      rcases h with ⟨rfl, rfl⟩
      simp [List.sum_cons]
      omega
  | a :: b :: rest, hs =>
      simp [shrinking] at hs
      rcases hs with ⟨hab, htail⟩
      intro a' h
      simp at h
      rcases h with ⟨rfl, rfl⟩
      have htail_sum : (b :: rest).sum ≤ 5 * b :=
        shrinking_sum_le_five_mul_head (b :: rest) htail b rfl
      simp [List.sum_cons] at htail_sum ⊢
      omega

theorem goal : GeometricRepartitionTraffic.Goal := by
  intro a rest h
  have hs : shrinking (a :: rest) := h
  have := shrinking_sum_le_five_mul_head (a :: rest) hs a rfl
  simpa [GeometricRepartitionTraffic.Goal] using this

end TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.GeometricRepartitionTraffic
