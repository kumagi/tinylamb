import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_StrictOverlap_EndpointSweep

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.StrictOverlap

/-- StrictOverlap の端点走査型ゴールは `StartMinusEnd.Goal` という名前で登録
されている（ContainsTick の `EndpointSweep.Goal` に対応する主張）。 -/
theorem sound_StrictOverlap_EndpointSweep : StartMinusEnd.Goal := by
  intro p rs hpxy hxy
  induction rs with
  | nil => simp [scan]
  | cons r rs ih =>
      have hrx : r.1.x < r.1.y := hxy r (by simp)
      have ih' : scan shape p rs =
          ((rs.filter (fun r => r.1.x < p.y)).map Prod.snd).sum -
            ((rs.filter (fun r => r.1.y ≤ p.x)).map Prod.snd).sum :=
        ih (fun r hr' => hxy r (by simp [hr']))
      by_cases hx : r.1.x < p.y
      · by_cases hy : r.1.y ≤ p.x
        · have hnlt : ¬ p.x < r.1.y := by omega
          simp [scan, shape, Int.max_lt, Int.lt_min, hpxy, hrx, hx, hy, hnlt] at ih' ⊢
          omega
        · have hlt : p.x < r.1.y := by omega
          simp [scan, shape, Int.max_lt, Int.lt_min, hpxy, hrx, hx, hy, hlt] at ih' ⊢
          omega
      · have hny : ¬ r.1.y ≤ p.x := by omega
        simp [scan, shape, Int.max_lt, Int.lt_min, hpxy, hrx, hx, hny] at ih' ⊢
        omega

end TinylambOpt.ResearchProofs.Atlas.Goal_StrictOverlap_EndpointSweep
