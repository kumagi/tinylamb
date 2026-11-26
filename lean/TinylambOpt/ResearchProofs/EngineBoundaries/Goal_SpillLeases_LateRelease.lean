import TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_LateRelease

open TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

theorem sound : LateRelease.Goal := by
  intro s l hlt
  cases s with
  | mk e live pay pins =>
    cases l with
    | mk le lt =>
        change le < e at hlt
        have hne : le ≠ e := by omega
        have hbe : (e == le) = false := beq_eq_false_iff_ne.mpr hne.symm
        simp [release, hbe]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_LateRelease
