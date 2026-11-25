import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TotalVariation_EndpointFence

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TotalVariation

/-! Brute-force sanity check: all series of length ≤ 4 over [-4, 4].
    `true` means no counterexample was found. -/
#eval do
  let vals : List Int := [-4, -3, -2, -1, 0, 1, 2, 3, 4]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  let ls : List (List Int) := lists 4
  pure ((ls.filter (fun (x : List Int) =>
    decide (measure x < distance (x.head?.getD 0) (x.getLast?.getD 0)))).isEmpty)

/-! Triangle inequality for the absolute value of an integer sum. -/
private theorem abs_sum_le_sum_abs (xs : List Int) :
    ((xs.sum.natAbs : Int) ≤ (xs.map (fun z => (z.natAbs : Int))).sum) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have htri : ((x + xs.sum).natAbs : Int) ≤ (x.natAbs : Int) + (xs.sum.natAbs : Int) := by
        exact_mod_cast Int.natAbs_add_le x xs.sum
      omega

/-! The signed jumps telescope to the endpoint difference. -/
private theorem delta_sum (x : Series) :
    (delta x).sum = x.getLast?.getD 0 - x.head?.getD 0 := by
  induction x with
  | nil => simp [delta, adj]
  | cons a xs ih =>
      cases xs with
      | nil => simp [delta, adj]
      | cons b ys =>
          simp [delta, adj] at ih ⊢
          rw [ih]
          omega

theorem sound_TotalVariation_EndpointFence : TotalVariation.EndpointFence.Goal := by
  intro x
  have h := abs_sum_le_sum_abs (delta x)
  rw [delta_sum x] at h
  simpa [TotalVariation.measure, distance] using h

end TinylambOpt.ResearchProofs.Atlas.Goal_TotalVariation_EndpointFence
