import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_CappedPair_SaturatedPage

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair

-- 真偽確認: t, a.lo, a.hi, b.lo, b.hi ∈ {0,1,2,3,4} の全組み合わせを
-- native_decide で網羅し、反例が無い（false = 反例なし）ことを確認済み。
-- 証明の要旨: bounds の下界は min t (min a.lo b.lo)。t ≤ a.lo と t ≤ b.lo から
-- t ≤ min a.lo b.lo なので、外側の min は t を選ぶ。

theorem sound_SaturatedPage : CappedPair.SaturatedPage.Goal := by
  intro t a b hta htb
  change min t (min a.lo b.lo) = t
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_CappedPair_SaturatedPage
