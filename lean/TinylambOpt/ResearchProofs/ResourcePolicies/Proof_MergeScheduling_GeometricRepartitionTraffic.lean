import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.Goal_GeometricRepartitionTraffic

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling


/-! Soundness of `GeometricRepartitionTraffic`: a geometrically shrinking list
    of partition byte sizes has total traffic at most five times its head. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

private theorem shrinking_sum_le_five_mul_head : ∀ (xs : List Nat),
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

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.GeometricRepartitionTraffic.Goal := by
  intro a rest h
  have hs : shrinking (a :: rest) := h
  have hsum : (a :: rest).sum ≤ 5 * a := shrinking_sum_le_five_mul_head (a :: rest) hs a rfl
  simpa [TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.GeometricRepartitionTraffic.Goal] using hsum
end TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.Goal_GeometricRepartitionTraffic
