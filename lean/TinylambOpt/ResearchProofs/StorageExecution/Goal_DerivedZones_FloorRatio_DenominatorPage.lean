import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `FloorRatio.DenominatorPage` — `∀ t a b k, k≤(kernel.bounds t a b).lo ↔
  k*(b.hi+1)≤a.lo*t`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_FloorRatio_DenominatorPage

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio

theorem sound_DenominatorPage : FloorRatio.DenominatorPage.Goal := by
  intro t a b k
  -- (kernel.bounds t a b).lo = (a.lo * t) / (b.hi + 1), and the
  -- denominator b.hi + 1 is strictly positive, so the standard
  -- `Nat.le_div_iff_mul_le` equivalence applies.
  change k ≤ (a.lo * t) / (b.hi + 1) ↔ k * (b.hi + 1) ≤ a.lo * t
  exact Nat.le_div_iff_mul_le (by omega : 0 < b.hi + 1)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_FloorRatio_DenominatorPage
