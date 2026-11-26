import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `BucketSum.SoundInterval` — Bound a computed group or predicate key from independent
  column min/max.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BucketSum_SoundInterval

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum

-- 真偽確認: t, 各端点, x, y < 4 の全組み合わせを native_decide で網羅し true を確認済み。
-- 証明の要旨: Nat の除法は単調なので a.lo + b.lo ≤ x + y ≤ a.hi + b.hi をそのまま引き継ぐ。

theorem sound_SoundInterval : BucketSum.SoundInterval.Goal := by
  intro t a b x y hx hy
  obtain ⟨ha_lo, ha_hi⟩ := hx
  obtain ⟨hb_lo, hb_hi⟩ := hy
  constructor
  · exact Nat.div_le_div_right (by omega)
  · exact Nat.div_le_div_right (by omega)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BucketSum_SoundInterval
