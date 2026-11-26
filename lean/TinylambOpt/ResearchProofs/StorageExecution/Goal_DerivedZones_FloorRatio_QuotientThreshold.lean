import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `FloorRatio.QuotientThreshold` — `∀ t a b k, (kernel.bounds t a b).hi<k ↔
  a.hi*t<k*(b.lo+1)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_FloorRatio_QuotientThreshold

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio

-- The upper certificate is a floor division by a strictly positive denominator,
-- so the standard Nat division threshold law applies directly.
theorem sound_QuotientThreshold : FloorRatio.QuotientThreshold.Goal := by
  intro t a b k
  dsimp [kernel]
  exact Nat.div_lt_iff_lt_mul (Nat.succ_pos b.lo)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_FloorRatio_QuotientThreshold
