import TinylambOpt.ResearchGoals.DatabaseCorrectness.CatalogDrop

/-! A reader that acquired the same table lease twice still holds one lease after
    a single release, so the closing gate cannot finish the drop. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.CatalogDrop.Goal_RepeatedTableLeaseBlocksReclaim

open TinylambOpt.ResearchGoals.DatabaseCorrectness.CatalogDrop

theorem sound : RepeatedTableLeaseBlocksReclaim.Goal := by
  intro c closing owner hbegin hcount
  rcases c with ⟨mode, gen, leases, children⟩
  cases mode <;> simp [beginDrop] at hbegin
  rcases hbegin with ⟨_, rfl⟩
  simp at hcount
  have hcnt : (leases.erase owner).count owner = leases.count owner - 1 := by
    rw [List.count_erase]
    simp
  have hnonempty : (leases.erase owner) ≠ [] := by
    intro hempty
    have hz : (leases.erase owner).count owner = 0 := by simp [hempty]
    omega
  have hfalse : (leases.erase owner).isEmpty = false := by
    cases hb : (leases.erase owner).isEmpty
    · rfl
    · exfalso
      exact hnonempty (by simpa using hb)
  simp [finishDrop, release, hfalse]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.CatalogDrop.Goal_RepeatedTableLeaseBlocksReclaim
