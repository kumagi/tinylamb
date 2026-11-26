import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `SaturatingLoss.ThresholdInverse` — `∀ t x y k, 0<k → (k≤kernel.score t (x,y) ↔
  y+t+k≤x)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_SaturatingLoss_ThresholdInverse

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss

/-- For a positive threshold, `k ≤ x - (y+t)` holds exactly when
    `y+t+k ≤ x`. The positivity of `k` rules out the degenerate `k = 0`
    case, where truncated subtraction would make the left side trivially
    true even when `y+t > x`. -/
theorem sound_ThresholdInverse : SaturatingLoss.ThresholdInverse.Goal := by
  intro t x y k hk
  unfold kernel
  simp only
  constructor
  · intro h
    omega
  · intro h
    omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_SaturatingLoss_ThresholdInverse
