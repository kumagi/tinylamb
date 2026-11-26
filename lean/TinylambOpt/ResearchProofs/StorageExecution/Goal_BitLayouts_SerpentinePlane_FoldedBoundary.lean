import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentinePlane_FoldedBoundary

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentinePlane

/-- On an even plane `b`, the last row sits at `b*n + (n-1)`; on the following
    odd plane `b+1`, the same ordinal maps to the first row `(b+1)*n`.  Since
    `0 < n`, these two positions are consecutive. -/
theorem sound_FoldedBoundary : SerpentinePlane.FoldedBoundary.Goal := by
  intro n w b hn hb
  simp only [layout]
  have hodd : (b + 1) % 2 ≠ 0 := by omega
  simp only [hb, hodd, ↓reduceIte, Nat.sub_self]
  rw [Nat.succ_mul]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentinePlane_FoldedBoundary
