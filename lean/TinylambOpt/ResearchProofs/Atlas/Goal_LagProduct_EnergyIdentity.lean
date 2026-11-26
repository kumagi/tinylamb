import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `LagProduct.EnergyIdentity` — `∀ x, 2*measure x = 2*(x.map (·^2)).sum - x.head?.getD
  0 ^ 2 - x.getLast?.getD 0 ^ 2 - QuadraticVariation.measure x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LagProduct_EnergyIdentity

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

private def lp (x : Series) : Int := LagProduct.measure x
private def qv (x : Series) : Int := QuadraticVariation.measure x
private def sqsum (x : Series) : Int := (x.map (fun v => v ^ 2)).sum

private theorem adj_cons₂ (a b : Int) (xs : Series) :
    adj (a :: b :: xs) = (a, b) :: adj (b :: xs) := by
  simp [adj]

private theorem lp_cons₂ (a b : Int) (xs : Series) :
    lp (a :: b :: xs) = a * b + lp (b :: xs) := by
  simp [lp, LagProduct.measure, adj_cons₂]

private theorem qv_cons₂ (a b : Int) (xs : Series) :
    qv (a :: b :: xs) = (b - a) ^ 2 + qv (b :: xs) := by
  simp [qv, QuadraticVariation.measure, delta, adj_cons₂]

private theorem sqsum_cons (a : Int) (xs : Series) :
    sqsum (a :: xs) = a ^ 2 + sqsum xs := by
  simp [sqsum]

theorem sound_LagProduct_EnergyIdentity : LagProduct.EnergyIdentity.Goal := by
  intro x
  change 2 * lp x = 2 * sqsum x - x.head?.getD 0 ^ 2 - x.getLast?.getD 0 ^ 2 - qv x
  induction x with
  | nil =>
      simp [lp, qv, sqsum, LagProduct.measure, QuadraticVariation.measure, delta, adj]
  | cons a xs ih =>
      cases xs with
      | nil =>
          simp [lp, qv, sqsum, LagProduct.measure, QuadraticVariation.measure, delta, adj]
          grind
      | cons b ys =>
          rw [lp_cons₂, qv_cons₂, sqsum_cons]
          simp only [List.head?_cons, Option.getD_some]
          have hlast : (a :: b :: ys).getLast?.getD 0 = (b :: ys).getLast?.getD 0 := by
            simp [List.getLast?_cons]
          rw [hlast]
          have ih' : 2 * lp (b :: ys) =
              2 * sqsum (b :: ys) - b ^ 2 - (b :: ys).getLast?.getD 0 ^ 2 - qv (b :: ys) := by
            simpa using ih
          grind

end TinylambOpt.ResearchProofs.Atlas.Goal_LagProduct_EnergyIdentity
