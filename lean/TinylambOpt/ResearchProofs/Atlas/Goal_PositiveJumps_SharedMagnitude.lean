import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `PositiveJumps.SharedMagnitude` — `∀ x, 2*measure x = TotalVariation.measure x +
  x.getLast?.getD 0-x.head?.getD 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PositiveJumps_SharedMagnitude

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! ### Exhaustive sanity check on small inputs -/

private def rowsOf : Nat → List Int → List (List Int)
  | 0, _ => [[]]
  | n + 1, pool => pool.flatMap (fun v => (rowsOf n pool).map (v :: ·))

#eval
  let pool := [-3, -2, -1, 0, 1, 2, 3]
  let xs := (List.range 5).flatMap (fun n => rowsOf n pool)
  xs.all (fun x =>
    2 * PositiveJumps.measure x ==
      TotalVariation.measure x + x.getLast?.getD 0 - x.head?.getD 0)

/-! ### Elementary lemmas -/

private theorem max_zero_twice (d : Int) : 2 * max 0 d = (d.natAbs : Int) + d := by
  omega

private theorem delta_cons_cons (a b : Int) (xs : Series) :
    delta (a :: b :: xs) = (b - a) :: delta (b :: xs) := by
  simp [delta, adj]

/-! ### Main theorem -/

theorem sound_PositiveJumps_SharedMagnitude : PositiveJumps.SharedMagnitude.Goal := by
  intro x
  induction x with
  | nil => simp [PositiveJumps.measure, TotalVariation.measure, delta, adj]
  | cons a xs ih =>
      cases xs with
      | nil => simp [PositiveJumps.measure, TotalVariation.measure, delta, adj]
      | cons b bs =>
          have hpj :
              PositiveJumps.measure (a :: b :: bs) =
                max 0 (b - a) + PositiveJumps.measure (b :: bs) := by
            simp [PositiveJumps.measure, delta_cons_cons]
          have htv :
              TotalVariation.measure (a :: b :: bs) =
                (b - a).natAbs + TotalVariation.measure (b :: bs) := by
            simp [TotalVariation.measure, delta_cons_cons]
          rw [hpj, htv]
          have hdist :
              2 * (max 0 (b - a) + PositiveJumps.measure (b :: bs)) =
                2 * max 0 (b - a) + 2 * PositiveJumps.measure (b :: bs) := by
            omega
          rw [hdist, max_zero_twice (b - a), ih]
          have hlast : (a :: b :: bs).getLast?.getD 0 = (b :: bs).getLast?.getD 0 := by
            simp
          rw [hlast]
          simp only [List.head?_cons, Option.getD_some]
          omega

end TinylambOpt.ResearchProofs.Atlas.Goal_PositiveJumps_SharedMagnitude
