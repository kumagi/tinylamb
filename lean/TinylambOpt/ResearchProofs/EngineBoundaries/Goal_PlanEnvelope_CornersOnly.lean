import TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_CornersOnly

open TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

private theorem cost_add (p : Plan) (lo n g : Nat) (hlo : lo ≤ n) :
    cost p n g = cost p lo g + p.perRow * (n - lo) := by
  unfold cost
  have hmul : p.perRow * lo + p.perRow * (n - lo) = p.perRow * n := by
    rw [← Nat.mul_add, Nat.add_sub_of_le hlo]
  omega

theorem refute : ¬ CornersOnly.Goal := by
  intro h
  let p : Plan := ⟨0,0,8,1⟩
  let q : Plan := ⟨2,0,4,2⟩
  have hcorners : [0,1].all (fun r => [0,8].all (cheaper p q r)) := by native_decide
  have hh := h p q 0 1 0 8 1 4 (by decide) (by decide) (by decide) (by decide) hcorners
  have hbad : ¬ cheaper p q 1 4 := by native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_CornersOnly
