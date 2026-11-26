import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_PredicateScheduling_FallibleFilterCommutation
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.FallibleFilterCommutation

theorem sound : Goal := by
  intro a b rest rows h
  apply List.map_congr_left
  intro r hr
  simp only [evaluate]
  cases ha : a r <;> cases hb : b r <;> simp [ha, hb] at h ⊢
  all_goals try rfl
  all_goals try (have := h r hr (by simp [ha]) (by simp [hb]); simp_all)
end Nov26.Proof_PredicateScheduling_FallibleFilterCommutation
