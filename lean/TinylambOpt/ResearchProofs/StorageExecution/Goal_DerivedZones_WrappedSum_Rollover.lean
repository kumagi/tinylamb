import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `WrappedSum.Rollover` — `∀ t a b, (a.lo+b.lo)/(t+1)<(a.hi+b.hi)/(t+1) → kernel.bounds
  t a b=⟨0,t⟩`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_WrappedSum_Rollover

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum

-- 真偽確認: t, al, ah, bl, bh < 6 の全組み合わせを native_decide で網羅し true を確認済み。
-- 証明の要旨: kernel.bounds は両端の商が等しいときだけ部分区間を返し、
-- それ以外は常に ⟨0,t⟩ を返す。仮説は商のstrictな大小なので条件は偽。

theorem sound_Rollover : WrappedSum.Rollover.Goal := by
  intro t a b h
  have hne : (a.lo + b.lo) / (t + 1) ≠ (a.hi + b.hi) / (t + 1) := by omega
  simp [kernel, hne]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_WrappedSum_Rollover
