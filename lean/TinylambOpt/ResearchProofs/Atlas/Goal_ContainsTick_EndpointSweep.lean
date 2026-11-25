import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ContainsTick_EndpointSweep

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.ContainsTick

theorem sound_ContainsTick_EndpointSweep : EndpointSweep.Goal := by
  intro p rs h
  induction rs with
  | nil => simp [scan]
  | cons r rs ih =>
      have hrx : r.1.x ≤ r.1.y := h r (by simp)
      have ih' : scan shape p rs =
          ((rs.filter (fun r => r.1.x ≤ p.x)).map Prod.snd).sum -
            ((rs.filter (fun r => r.1.y ≤ p.x)).map Prod.snd).sum :=
        ih (fun r hr' => h r (by simp [hr']))
      by_cases hx : r.1.x ≤ p.x
      · by_cases hy : r.1.y ≤ p.x
        · have hnlt : ¬ p.x < r.1.y := by omega
          simp [scan, shape, hx, hy, hnlt] at ih' ⊢
          omega
        · have hlt : p.x < r.1.y := by omega
          simp [scan, shape, hx, hy, hlt] at ih' ⊢
          omega
      · have hny : ¬ r.1.y ≤ p.x := by omega
        simp [scan, shape, hx, hny] at ih' ⊢
        omega

end TinylambOpt.ResearchProofs.Atlas.Goal_ContainsTick_EndpointSweep
