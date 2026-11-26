import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WindowRetention.Goal_StrictKeyClosesPeerGroup

open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention


/-!
In a sorted non-NULL partition, a following key strictly larger than `k` closes
the peer group of `k`: no later row can equal `k`. A peer buffer may therefore
be flushed as soon as such a key is observed, without scanning the rest of the
partition.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.StrictKeyClosesPeerGroup.Goal := by
  intro a b k next hkn hpair
  induction a with
  | nil =>
      simp only [List.nil_append, List.count_cons, List.count_nil]
      have hnextEq : next ≠ k := by omega
      have hb : ∀ z ∈ b, z ≠ k := by
        intro z hz
        have hzle : next ≤ z := by
          exact hpair.rel_of_mem_take_of_mem_drop (i := 1) (by simp) (by simp [hz])
        omega
      have hbzero : b.count k = 0 := List.count_eq_zero.mpr (by
        intro hk
        exact (hb k hk) rfl)
      have hnext : (next == k) = false := by simpa [beq_iff_eq] using hnextEq
      simp [hnext, hbzero]
  | cons x xs ih =>
      simp only [List.cons_append, List.count_cons]
      rw [ih (List.Pairwise.of_cons hpair)]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WindowRetention.Goal_StrictKeyClosesPeerGroup
