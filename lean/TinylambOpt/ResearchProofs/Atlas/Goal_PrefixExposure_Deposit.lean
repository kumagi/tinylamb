import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `PrefixExposure.Deposit` — `∀ b x, measure (shifted b x) = measure x + b*choose2
  (x.length+1)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PrefixExposure_Deposit

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PrefixExposure
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! Brute-force sanity check: all series of length <= 4 over [-3, 3] and all shifts. -/
#eval do
  let vals : List Int := [-3, -2, -1, 0, 1, 2, 3]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  let ls := lists 4
  let bad := ls.flatMap (fun x => vals.map (fun b =>
    PrefixExposure.measure (shifted b x) - PrefixExposure.measure x -
      b * (choose2 (x.length + 1) : Int)))
  pure ((bad.filter (fun d => d != 0)).length)

private theorem prefs_cons (a : Int) (xs : Series) :
    prefs (a :: xs) = a :: (prefs xs).map (fun v => a + v) := by
  simp [prefs, List.range_succ_eq_map, List.take_succ_cons, List.map_map,
    Function.comp_def]

private theorem measure_cons (a : Int) (xs : Series) :
    PrefixExposure.measure (a :: xs) =
      ((xs.length + 1 : Nat) : Int) * a + PrefixExposure.measure xs := by
  unfold PrefixExposure.measure
  rw [prefs_cons]
  simp only [List.sum_cons]
  rw [show ((prefs xs).map (fun v => a + v)).sum =
      (prefs xs).sum + (prefs xs).length * a by
    induction prefs xs with
    | nil => simp
    | cons b bs ih =>
        simp only [List.map_cons, List.sum_cons, List.length_cons]
        have hcast : ((bs.length + 1 : Nat) : Int) = (bs.length : Int) + 1 := by omega
        rw [hcast]
        grind]
  have hlen : (prefs xs).length = xs.length := by simp [prefs]
  rw [hlen]
  grind

private theorem choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
  cases n with
  | zero => simp [choose2]
  | succ n => unfold choose2; grind

theorem sound_PrefixExposure_Deposit : PrefixExposure.Deposit.Goal := by
  intro b x
  induction x with
  | nil => simp [shifted, PrefixExposure.measure, prefs, choose2]
  | cons a xs ih =>
      simp only [shifted, List.map_cons] at ih ⊢
      rw [measure_cons, measure_cons]
      simp only [List.length_cons] at ih ⊢
      rw [ih]
      rw [choose2_succ (xs.length + 1)]
      grind

end TinylambOpt.ResearchProofs.Atlas.Goal_PrefixExposure_Deposit
