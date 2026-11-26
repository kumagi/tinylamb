import TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

/-!
A cached or specialized plan needs the modeled validity conditions before it can serve a
SQL invocation.

Proved

- `ReestimateRegret` — Winner mismatch under cardinality error is bounded by the two
  plans' slope difference.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_ReestimateRegret

open TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

private theorem cost_add (p : Plan) (lo n g : Nat) (hlo : lo ≤ n) :
    cost p n g = cost p lo g + p.perRow * (n - lo) := by
  unfold cost
  have hmul : p.perRow * lo + p.perRow * (n - lo) = p.perRow * n := by
    rw [← Nat.mul_add, Nat.add_sub_of_le hlo]
  omega

theorem sound : ReestimateRegret.Goal := by
  intro p q n m g hcheap
  have hcheap' : cost p n g ≤ cost q n g := by simpa [cheaper] using hcheap
  let K := max p.perRow q.perRow - min p.perRow q.perRow
  have hpp : p.perRow ≤ q.perRow + K := by unfold K; omega
  have hqq : q.perRow ≤ p.perRow + K := by unfold K; omega
  by_cases hnm : n ≤ m
  · have hpm := cost_add p n m g hnm
    have hqm := cost_add q n m g hnm
    rw [hpm, hqm]
    have hmul := Nat.mul_le_mul_right (m - n) hpp
    rw [Nat.add_mul] at hmul
    have hgoal : cost p n g + p.perRow * (m - n) ≤
        (cost q n g + q.perRow * (m - n)) + K * (m - n) := by
      omega
    have heq : max n m - min n m = m - n := by omega
    rw [heq, Nat.mul_comm]
    unfold K at *
    rw [Nat.mul_comm (m - n) p.perRow]
    rw [Nat.mul_comm (m - n) (max p.perRow q.perRow - min p.perRow q.perRow)]
    rw [Nat.sub_le_iff_le_add]
    omega
  · have hmn : m ≤ n := by omega
    have hpn := cost_add p m n g hmn
    have hqn := cost_add q m n g hmn
    rw [hpn, hqn] at hcheap'
    have hmul := Nat.mul_le_mul_right (n - m) hqq
    rw [Nat.add_mul] at hmul
    have hgoal : cost p m g ≤ cost q m g + K * (n - m) := by
      omega
    have heq : max n m - min n m = n - m := by omega
    rw [heq, Nat.mul_comm]
    unfold K at *
    rw [Nat.sub_le_iff_le_add]
    omega

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_ReestimateRegret
