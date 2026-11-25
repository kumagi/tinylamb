import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
import TinylambOpt.ResearchProofs.Atlas.Goal_PositiveJumps_SharedMagnitude

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PositiveJumps_ReverseBalance

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps

/-! Brute-force sanity check: every series of length ≤ 5 over [-3, 3].
    The list printed is empty, so no counterexample was found. -/

private def rowsOf : Nat → List Int → List (List Int)
  | 0, _ => [[]]
  | n + 1, pool => (rowsOf n pool).flatMap (fun xs => pool.map (fun v => v :: xs))

#eval
  let pool := [-3, -2, -1, 0, 1, 2, 3]
  let xs := (List.range 6).flatMap (fun n => rowsOf n pool)
  xs.filter (fun x =>
    !decide (measure x - measure x.reverse =
      x.getLast?.getD 0 - x.head?.getD 0)) |>.take 5

/-! ### Elementary lemmas about `delta` and reversal -/

private theorem delta_cons₂ (a b : Int) (xs : Series) :
    delta (a :: b :: xs) = (b - a) :: delta (b :: xs) := by
  simp [delta, adj]

private theorem getLastD_reverse (x : Series) : x.reverse.getLast?.getD 0 = x.head?.getD 0 := by
  rw [List.getLast?_reverse]

private theorem delta_append_singleton (x : Series) (a : Int) (hx : x ≠ []) :
    delta (x ++ [a]) = delta x ++ [a - x.getLast?.getD 0] := by
  induction x with
  | nil => contradiction
  | cons b bs ih =>
      cases bs with
      | nil => simp [delta, adj]
      | cons c cs =>
          have hne : (c :: cs) ≠ [] := by simp
          have hi := ih hne
          change (c - b) :: delta ((c :: cs) ++ [a]) =
            (c - b) :: (delta (c :: cs) ++ [a - (b :: c :: cs).getLast?.getD 0])
          rw [hi]
          simp [List.getLast?_cons]

private theorem delta_reverse (x : Series) :
    delta x.reverse = (delta x).reverse.map (fun v => -v) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      cases xs with
      | nil => rfl
      | cons b bs =>
          rw [List.reverse_cons]
          rw [delta_append_singleton ((b :: bs).reverse) a (by simp)]
          rw [ih]
          rw [getLastD_reverse (b :: bs)]
          simp only [List.head?_cons, Option.getD_some]
          rw [delta_cons₂]
          simp only [List.map_cons, List.reverse_cons, List.map_append]
          have hneg : -(b - a) = a - b := by omega
          rw [hneg]
          simp

/-! Total variation is invariant under reversal: the signed jumps are negated
    and reversed, and the absolute value of a negated jump is unchanged. -/

private theorem tv_reverse (x : Series) :
    TotalVariation.measure x.reverse = TotalVariation.measure x := by
  unfold TotalVariation.measure
  rw [delta_reverse]
  simp only [List.map_map]
  change ((delta x).reverse.map (fun v => ((-v).natAbs : Int))).sum =
    ((delta x).map (fun v => (v.natAbs : Int))).sum
  have h : (delta x).reverse.map (fun v => ((-v).natAbs : Int)) =
      (delta x).reverse.map (fun v => (v.natAbs : Int)) := by
    apply List.map_congr_left
    intro v hv
    simp [Int.natAbs_neg]
  rw [h, List.map_reverse]
  exact List.sum_reverse ((delta x).map (fun v => (v.natAbs : Int)))

/-! ### Main theorem

    Apply the already-proved `SharedMagnitude` identity to `x` and to
    `x.reverse`: the total-variation parts coincide, while the endpoint
    difference flips sign.  Cancelling the factor 2 gives the claim. -/

theorem sound_PositiveJumps_ReverseBalance : ReverseBalance.Goal := by
  intro x
  have h1 :=
    TinylambOpt.ResearchProofs.Atlas.Goal_PositiveJumps_SharedMagnitude.sound_PositiveJumps_SharedMagnitude x
  have h2 :=
    TinylambOpt.ResearchProofs.Atlas.Goal_PositiveJumps_SharedMagnitude.sound_PositiveJumps_SharedMagnitude x.reverse
  rw [tv_reverse, List.getLast?_reverse, List.head?_reverse] at h2
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_PositiveJumps_ReverseBalance
