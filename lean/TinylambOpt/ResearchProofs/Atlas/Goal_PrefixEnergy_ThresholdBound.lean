import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `PrefixEnergy.ThresholdBound` — `∀ x, (PrefixExposure.measure x)^2 ≤ size x * measure
  x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PrefixEnergy_ThresholdBound

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PrefixEnergy

/-! Brute-force truth check: every series of length <= 5 with entries in a
    small range satisfies (sum of prefix balances)^2 <= length * (sum of
    squared prefix balances). -/
#eval
  let rec gen : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => (gen n).flatMap (fun xs => [-3, -1, 0, 2, 4].map (fun v => xs ++ [v]))
  (gen 5).all (fun xs =>
    (PrefixExposure.measure xs) ^ 2 ≤ size xs * PrefixEnergy.measure xs)

private theorem sq_nonneg (a : Int) : 0 ≤ a ^ 2 := by
  rw [show a ^ 2 = a * a by grind]
  by_cases h : 0 ≤ a
  · exact Int.mul_nonneg h h
  · have hn : 0 ≤ -a := by omega
    have hm := Int.mul_nonneg hn hn
    grind

private theorem sum_sq_cauchy (xs : List Int) :
    xs.sum ^ 2 ≤ (xs.length : Int) * (xs.map (fun x => x ^ 2)).sum := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
      simp only [List.sum_cons, List.length_cons, List.map_cons, List.sum_cons]
      have hcast : ((xs.length + 1 : Nat) : Int) = (xs.length : Int) + 1 := by simp
      rw [hcast]
      by_cases hk : xs.length = 0
      · simp [hk]
        grind
      · have hkposNat : 0 < xs.length := Nat.pos_of_ne_zero hk
        have hkpos : (0 : Int) < xs.length := by exact_mod_cast hkposNat
        let k : Int := xs.length
        let Q : Int := (xs.map fun x => x ^ 2).sum
        let S : Int := xs.sum
        have hih : S ^ 2 ≤ k * Q := by simpa [k, Q, S] using ih
        have hv : 0 ≤ k * Q - S ^ 2 := by omega
        have hs : 0 ≤ (k * a - S) ^ 2 := sq_nonneg _
        have hid : (k * a - S) ^ 2 + (k * Q - S ^ 2) =
            k * (k * a ^ 2 - 2 * a * S + Q) := by grind
        have hp : 0 ≤ k * (k * a ^ 2 - 2 * a * S + Q) := by
          rw [← hid]
          omega
        have hmain : 0 ≤ k * a ^ 2 - 2 * a * S + Q := by
          by_cases hnonneg : 0 ≤ k * a ^ 2 - 2 * a * S + Q
          · exact hnonneg
          · have hneg : k * a ^ 2 - 2 * a * S + Q < 0 := by omega
            have hmneg := Int.mul_neg_of_pos_of_neg hkpos hneg
            have hcontr : False := (Int.lt_irrefl 0) (Int.lt_of_le_of_lt hp hmneg)
            exact False.elim hcontr
        change (a + S) ^ 2 ≤ (k + 1) * (a ^ 2 + Q)
        rw [show (a + S) ^ 2 = a ^ 2 + 2 * a * S + S ^ 2 by grind]
        rw [show (k + 1) * (a ^ 2 + Q) = a ^ 2 + Q + k * a ^ 2 + k * Q by grind]
        omega

private theorem prefs_length (x : Series) : (prefs x).length = x.length := by
  simp [prefs]

theorem sound_PrefixEnergy_ThresholdBound : PrefixEnergy.ThresholdBound.Goal := by
  intro x
  unfold PrefixExposure.measure PrefixEnergy.measure size
  rw [← prefs_length x]
  exact sum_sq_cauchy (prefs x)

end TinylambOpt.ResearchProofs.Atlas.Goal_PrefixEnergy_ThresholdBound
