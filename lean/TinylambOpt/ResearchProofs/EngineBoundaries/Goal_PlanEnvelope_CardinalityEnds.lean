import TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_CardinalityEnds

open TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

private theorem cost_add (p : Plan) (lo n g : Nat) (hlo : lo ≤ n) :
    cost p n g = cost p lo g + p.perRow * (n - lo) := by
  unfold cost
  have hmul : p.perRow * lo + p.perRow * (n - lo) = p.perRow * n := by
    rw [← Nat.mul_add, Nat.add_sub_of_le hlo]
  omega

theorem sound : CardinalityEnds.Goal := by
  intro p q lo hi n g hl hhi hloCheap hhiCheap
  have hlo' : cost p lo g ≤ cost q lo g := by simpa [cheaper] using hloCheap
  have hhi' : cost p hi g ≤ cost q hi g := by simpa [cheaper] using hhiCheap
  have hpn := cost_add p lo n g hl
  have hqn := cost_add q lo n g hl
  have hph := cost_add p lo hi g (Nat.le_trans hl hhi)
  have hqh := cost_add q lo hi g (Nat.le_trans hl hhi)
  rw [hph, hqh] at hhi'
  change decide (cost p n g ≤ cost q n g) = true
  apply decide_eq_true_eq.mpr
  rw [hpn, hqn]
  by_cases hs : p.perRow ≤ q.perRow
  · exact Nat.add_le_add hlo' (Nat.mul_le_mul_right (n - lo) hs)
  · have hqp : q.perRow < p.perRow := Nat.lt_of_not_ge hs
    rcases Nat.exists_eq_add_of_le (Nat.le_of_lt hqp) with ⟨k, hk⟩
    have hkD : k * (n - lo) ≤ k * (hi - lo) := Nat.mul_le_mul_left k (by omega)
    have hi'' : cost p lo g + k * (hi - lo) ≤ cost q lo g := by
      rw [hk, Nat.add_mul] at hhi'
      omega
    have hgoal : cost p lo g + k * (n - lo) ≤ cost q lo g := by
      exact Nat.le_trans (Nat.add_le_add_left hkD _) hi''
    rw [hk, Nat.add_mul]
    omega

private theorem min_cons_of_le (a b : Nat) (xs : List Nat) (h : a ≤ b) :
    (a :: b :: xs).min? = (a :: xs).min? := by
  cases hx : xs.min? with
  | none =>
      have hnil : xs = [] := List.min?_eq_none_iff.mp hx
      subst xs
      simp [h]
  | some m =>
      rw [List.min?_cons]
      rw [List.min?_cons, hx]
      rw [List.min?_cons, hx]
      simp only [Option.some.injEq, Option.elim_some]
      omega

example : ParetoPrune.Goal := by
  intro p q rest n g hcheap
  unfold best
  have hle : cost p n g ≤ cost q n g := by simpa [cheaper] using hcheap
  simpa using min_cons_of_le (cost p n g) (cost q n g) (List.map (fun r => cost r n g) rest) hle

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_CardinalityEnds
