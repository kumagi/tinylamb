import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_QuadraticDecode_MergeNeighbors

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.QuadraticDecode

/-- Merging two adjacent extents pays off exactly when the fixed fee covers
    the cross term `2 * (left span) * (right span)` of the quadratic run cost.

    Write `L = m+1-a` and `R = b-m`.  Since `a ≤ m < b`, the spans are exact
    (`span a m = L`, `span (m+1) b = R`) and additive (`span a b = L + R`).
    Expanding `(L+R)^2 = L^2 + 2*L*R + R^2`, the inequality
    `f + (L+R)^2 ≤ (f+L^2) + (f+R^2)` is equivalent to `2*L*R ≤ f`. -/
theorem sound_MergeNeighbors : QuadraticDecode.MergeNeighbors.Goal := by
  intro f a m b ham hmb
  simp only [QuadraticDecode.price, span]
  have hsab : b + 1 - a = (m + 1 - a) + (b - m) := by omega
  have hsmb : b + 1 - (m + 1) = b - m := by omega
  rw [hsab, hsmb]
  grind

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_QuadraticDecode_MergeNeighbors
