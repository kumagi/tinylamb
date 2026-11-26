import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `BalanceFloor.PrefixCheckpoint` — A saved prefix plus page states yields a scan
  checkpoint at each boundary.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_PrefixCheckpoint

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor

private def steps (xs : List Nat) : List Int :=
  xs.map (fun x => if x % 2 == 0 then ((x / 2 : Nat) : Int) else -((x / 2 + 1 : Nat) : Int))

private def minPref (ss : List Int) : Int :=
  (ss.scanl (· + ·) 0).min?.getD 0

private theorem min_add_zero (c y : Int) : min c (c + y) = c + min 0 y := by
  by_cases hc : c ≤ c + y
  · by_cases hy : 0 ≤ y
    · simp [Int.min_def, hc, hy]
    · simp [Int.min_def, hc, hy]
      omega
  · have hy : ¬ 0 ≤ y := by omega
    simp [Int.min_def, hc, hy]

private theorem add_min (c a b : Int) : c + min a b = min (c + a) (c + b) := by
  by_cases h : a ≤ b
  · have h' : c + a ≤ c + b := by omega
    simp [Int.min_def, h, h']
  · have h' : ¬ c + a ≤ c + b := by omega
    simp [Int.min_def, h, h']

private theorem min_assoc3 (a b c : Int) : min a (min b c) = min (min a b) c := by
  by_cases hab : a ≤ b <;> by_cases hac : a ≤ c <;> by_cases hbc : b ≤ c <;>
    simp [Int.min_def, hab, hac, hbc] <;> omega

private theorem scanl_min?_eq (xs : List Int) (c : Int) :
    (xs.scanl (· + ·) c).min? = some (c + (xs.scanl (· + ·) 0).min?.getD 0) := by
  induction xs generalizing c with
  | nil => simp
  | cons x xs ih =>
      simp only [List.scanl_cons, List.min?_cons]
      rw [ih (c + x)]
      simp only [Option.elim_some]
      simp only [Int.zero_add]
      rw [ih x]
      simp only [Option.elim_some, Option.getD_some]
      congr 1
      simpa only [Int.add_assoc] using min_add_zero c (x + (xs.scanl (· + ·) 0).min?.getD 0)

private theorem minPref_cons (x : Int) (xs : List Int) :
    minPref (x :: xs) = min 0 (x + minPref xs) := by
  unfold minPref
  rw [List.scanl_cons, List.min?_cons]
  simp only [Int.zero_add]
  rw [scanl_min?_eq xs x]
  simp only [Option.elim_some, Option.getD_some]

private theorem minPref_nonpos (ss : List Int) : minPref ss ≤ 0 := by
  cases ss with
  | nil => simp [minPref]
  | cons x xs =>
      rw [minPref_cons]
      rw [Int.min_def]
      split <;> omega

private theorem minPref_append (sa sb : List Int) :
    minPref (sa ++ sb) = min (minPref sa) (sa.sum + minPref sb) := by
  induction sa with
  | nil =>
      simp only [List.nil_append, List.sum_nil]
      have hnil : minPref ([] : List Int) = 0 := by simp [minPref]
      rw [hnil, Int.zero_add]
      rw [Int.min_def]
      split
      · have h := minPref_nonpos sb
        omega
      · rfl
  | cons x xs ih =>
      rw [List.cons_append, minPref_cons, minPref_cons, ih]
      simp only [List.sum_cons]
      rw [add_min]
      rw [min_assoc3]
      congr 1
      omega

private theorem stitchAgg : stitch agg := by
  intro a b
  change agg.join ((steps a).sum, minPref (steps a)) ((steps b).sum, minPref (steps b)) =
    ((steps (a ++ b)).sum, minPref (steps (a ++ b)))
  rw [show steps (a ++ b) = steps a ++ steps b by simp [steps]]
  change ((steps a).sum + (steps b).sum, min (minPref (steps a)) ((steps a).sum + minPref (steps b))) =
    ((steps a ++ steps b).sum, minPref (steps a ++ steps b))
  rw [List.sum_append, minPref_append]

theorem sound_PrefixCheckpoint : BalanceFloor.PrefixCheckpoint.Goal := by
  intro init ps
  induction ps generalizing init with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.scanl_cons]
      rw [stitchAgg init p]
      rw [ih (init ++ p)]
      simp [List.range_succ_eq_map, Function.comp_def, List.append_assoc]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_PrefixCheckpoint
