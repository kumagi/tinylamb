import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! A dense jump table indexed by an in-domain key returns exactly the CASE arm
that direct first-match dispatch selects. This licenses replacing a range ladder
with a biased table lookup for keys inside the profiled domain, preserving SQL
NULL results and the no-match case. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_BoundedBiasedJumpTable

open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

theorem sound : BoundedBiasedJumpTable.Goal := by
  intro as base n x hlo hhi
  have hx0 : 0 ≤ x - base := by omega
  have hi : (x - base).toNat < n := by omega
  have hidx : base + max (x - base) 0 = x := by omega
  simp [tab, hi, hidx]

end TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_BoundedBiasedJumpTable
