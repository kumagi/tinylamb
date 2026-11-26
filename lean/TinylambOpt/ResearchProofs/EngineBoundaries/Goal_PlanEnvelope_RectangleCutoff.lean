import TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

/-!
A cached or specialized plan needs the modeled validity conditions before it can serve a
SQL invocation.

Proved

- `RectangleCutoff` — A lower bound at the smallest row count and largest grant prunes
  a whole rectangle.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_RectangleCutoff

open TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

private theorem cost_add (p : Plan) (lo n g : Nat) (hlo : lo ≤ n) :
    cost p n g = cost p lo g + p.perRow * (n - lo) := by
  unfold cost
  have hmul : p.perRow * lo + p.perRow * (n - lo) = p.perRow * n := by
    rw [← Nat.mul_add, Nat.add_sub_of_le hlo]
  omega

theorem sound : RectangleCutoff.Goal := by
  intro p n lo hi g cutoff hlo hg hcut
  unfold cost at hcut ⊢
  have h1 : p.startup + p.perRow * lo ≤ p.startup + p.perRow * n := by
    exact Nat.add_le_add_left (Nat.mul_le_mul_left p.perRow hlo) _
  have h2 : (p.workingSet - hi) * p.spillRate ≤ (p.workingSet - g) * p.spillRate := by
    exact Nat.mul_le_mul_right p.spillRate (Nat.sub_le_sub_left hg p.workingSet)
  omega

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_RectangleCutoff
