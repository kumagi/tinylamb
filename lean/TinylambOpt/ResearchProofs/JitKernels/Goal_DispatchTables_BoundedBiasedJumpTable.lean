import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_BoundedBiasedJumpTable

open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

theorem sound : TinylambOpt.ResearchGoals.JitKernels.DispatchTables.BoundedBiasedJumpTable.Goal := by
  intro as base n x hlo hhi
  have hnonneg : 0 ≤ x - base := by omega
  have hidx : (x - base).toNat < n := by
    rw [Int.toNat_lt hnonneg]
    omega
  have hbase : base + ((x - base).toNat : Int) = x := by
    rw [Int.toNat_of_nonneg hnonneg]
    omega
  simp [tab, hidx, Int.max_eq_left hnonneg, show base + (x - base) = x by omega]

end TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_BoundedBiasedJumpTable
