import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `WrappedSum.TranslateCycle` — `∀ t n x y, kernel.score t (x+n*(t+1),y) = kernel.score
  t (x,y)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_WrappedSum_TranslateCycle

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum

-- 真偽確認: t, n, x, y ∈ [0,8) および [0,12) を全網羅し反例 0 件（真）。
-- 周期 (t+1) の整数倍を加えても剰余は変わらない。

theorem sound_TranslateCycle : WrappedSum.TranslateCycle.Goal := by
  intro t n x y
  show ((x + n * (t + 1)) + y) % (t + 1) = (x + y) % (t + 1)
  have h : (x + n * (t + 1)) + y = (x + y) + n * (t + 1) := by omega
  rw [h, Nat.add_mul_mod_self_right]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_WrappedSum_TranslateCycle
