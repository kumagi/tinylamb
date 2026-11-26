import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

namespace TinylambOpt.ResearchProofs.ResourcePolicies.WindowRetention.StrictKeyClosesPeerGroup

open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

theorem sound : StrictKeyClosesPeerGroup.Goal := by
  intro a b k next hk hpair
  rw [List.count_append, List.count_cons]
  have hne : (next == k) = false := by
    have : next ≠ k := by omega
    simp [this]
  have hpair_b : (next :: b).Pairwise (· ≤ ·) := (List.pairwise_append.mp hpair).2.1
  have hcb : b.count k = 0 := by
    rw [List.count_eq_zero]
    intro hmem
    have hle : next ≤ k := (List.pairwise_cons.mp hpair_b).1 k hmem
    omega
  simp [hne, hcb]

end TinylambOpt.ResearchProofs.ResourcePolicies.WindowRetention.StrictKeyClosesPeerGroup
