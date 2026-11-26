import TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum

namespace TinylambOpt.ResearchProofs.ResourcePolicies.VersionVacuum.ShardWatermarkExactConflict

open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum

theorem sound : Not ShardWatermarkExactConflict.Goal := by
  intro h
  have := h [(1, 10)] 5 2 (by decide)
  rcases this with ⟨t, ht, hlt⟩
  simp at ht

end TinylambOpt.ResearchProofs.ResourcePolicies.VersionVacuum.ShardWatermarkExactConflict
