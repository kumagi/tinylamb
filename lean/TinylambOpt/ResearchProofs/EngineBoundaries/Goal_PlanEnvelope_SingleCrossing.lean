import TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

/-!
A cached or specialized plan needs the modeled validity conditions before it can serve a
SQL invocation.

Proved

- `SingleCrossing` — A crossing between a high-startup and a high-per-row plan is
  one-directional.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_SingleCrossing

open TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

private theorem cost_add (p : Plan) (lo n g : Nat) (hlo : lo ≤ n) :
    cost p n g = cost p lo g + p.perRow * (n - lo) := by
  unfold cost
  have hmul : p.perRow * lo + p.perRow * (n - lo) = p.perRow * n := by
    rw [← Nat.mul_add, Nat.add_sub_of_le hlo]
  omega

theorem sound : SingleCrossing.Goal := by
  intro p q n m g hslope hnm hcheap
  unfold cheaper at hcheap ⊢
  have hpn := cost_add p n m g hnm
  have hqn := cost_add q n m g hnm
  have hmul : p.perRow * (m - n) ≤ q.perRow * (m - n) :=
    Nat.mul_le_mul_right (m - n) hslope
  have hcheap' : cost p n g ≤ cost q n g := by simpa [decide_eq_true_eq] using hcheap
  rw [hpn, hqn]
  exact decide_eq_true_eq.mpr (Nat.add_le_add hcheap' hmul)

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_SingleCrossing
