import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `CappedPair.SoundInterval` — Bound a computed group or predicate key from independent
  column min/max.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_CappedPair_SoundInterval

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair

-- 真偽確認: t, a.lo, a.hi, b.lo, b.hi, x, y ∈ {0,1,2,3} の全組み合わせを
-- native_decide で網羅し true を確認済み。
-- 証明の要旨: min は各引数について単調なので、a.lo≤x≤a.hi と b.lo≤y≤b.hi から
-- min a.lo b.lo ≤ min x y ≤ min a.hi b.hi が成り立ち、外側の min t もこれを保つ。

theorem sound_SoundInterval : CappedPair.SoundInterval.Goal := by
  intro t a b x y hx hy
  obtain ⟨ha_lo, ha_hi⟩ := hx
  obtain ⟨hb_lo, hb_hi⟩ := hy
  change min t (min a.lo b.lo) ≤ min t (min x y) ∧
    min t (min x y) ≤ min t (min a.hi b.hi)
  constructor <;> omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_CappedPair_SoundInterval
