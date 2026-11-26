import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

namespace TinylambOpt.ResearchProofs.ResourcePolicies.WindowRetention.IgnoreNullsLagState

open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

theorem sound : IgnoreNullsLagState.Goal := by
  intro a b k
  simp only [tailRows, List.filterMap_append, List.length_append]

  -- same as ConsecutiveHaloReplacement on filtered lists
  rw [List.drop_append, List.drop_append]
  simp only [List.length_drop]
  congr 1
  · rw [List.drop_drop]; congr 1; omega
  · congr 1; omega

end TinylambOpt.ResearchProofs.ResourcePolicies.WindowRetention.IgnoreNullsLagState
