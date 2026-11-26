import TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ClusteredPages_ReclusterPaybackEpoch
open TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages ReclusterPaybackEpoch

theorem sound : Goal := by
  intro a b qs rewrite n hle
  have hmul : n * score b qs ≤ n * score a qs := Nat.mul_le_mul_left n hle
  have hmulsub : n * (score a qs - score b qs) = n * score a qs - n * score b qs := by
    rw [Nat.mul_sub]
  constructor
  · intro h
    rw [hmulsub, Nat.le_sub_iff_add_le hmul]
    exact h
  · intro h
    rw [hmulsub, Nat.le_sub_iff_add_le hmul] at h
    exact h
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ClusteredPages_ReclusterPaybackEpoch
