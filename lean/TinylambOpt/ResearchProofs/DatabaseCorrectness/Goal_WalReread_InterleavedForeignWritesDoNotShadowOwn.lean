import TinylambOpt.ResearchGoals.DatabaseCorrectness.WalReread

/-! Foreign WAL records do not shadow a transaction's own writes.

The owner predicate removes every foreign record from the window before the
edit chain is built, so inserting other transactions' records between a and b
cannot change read-your-writes ordering. SQL execution may interleave commits
freely without altering what a transaction observes from its own staged writes. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.WalReread_InterleavedForeignWritesDoNotShadowOwn

open TinylambOpt.ResearchGoals.DatabaseCorrectness.WalReread

theorem sound : InterleavedForeignWritesDoNotShadowOwn.Goal := by
  intro a foreign b owner lo hi root key hforeign
  simp only [ownEdits]
  have hf : foreign.filter (fun r => r.owner == owner && lo ≤ r.start && r.start < hi) = [] := by
    rw [List.eq_nil_iff_forall_not_mem]
    intro r hr
    rw [List.mem_filter] at hr
    simp only [Bool.and_eq_true] at hr
    have hne := hforeign r hr.1
    have heq : r.owner = owner := beq_iff_eq.mp hr.2.1.1
    exact hne heq
  rw [List.filter_append, List.filter_append, List.filterMap_append, hf]
  simp

end TinylambOpt.ResearchProofs.DatabaseCorrectness.WalReread_InterleavedForeignWritesDoNotShadowOwn
