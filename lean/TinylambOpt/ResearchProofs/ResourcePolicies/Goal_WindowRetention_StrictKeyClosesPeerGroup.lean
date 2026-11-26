import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

/-!
`StrictKeyClosesPeerGroup`: in a non-decreasing partition, a following key strictly
greater than `k` proves that no later row still belongs to the peer group of `k`.
SQL execution can close and emit that peer group at the boundary row without
buffering the rest of the partition.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_StrictKeyClosesPeerGroup

open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

theorem sound : StrictKeyClosesPeerGroup.Goal := by
  intro a b k next hk hpair
  have hb : b.count k = 0 := by
    rw [List.count_eq_zero]
    intro hx
    have hnb : (next :: b).Pairwise (fun x y => x ≤ y) := (List.pairwise_append.mp hpair).2.1
    have hle : next ≤ k := (List.pairwise_cons.mp hnb).1 k hx
    omega
  rw [List.count_append]
  rw [List.count_cons]
  rw [hb]
  by_cases hkn : k == next
  · rw [beq_iff_eq] at hkn
    omega
  · have hne : next ≠ k := by omega
    simp [hne]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_StrictKeyClosesPeerGroup
