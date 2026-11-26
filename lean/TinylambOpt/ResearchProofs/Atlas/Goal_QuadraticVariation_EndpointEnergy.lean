import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `QuadraticVariation.EndpointEnergy` — `∀ x, (x.getLast?.getD 0-x.head?.getD 0)^2 ≤
  (x.length-1:Int)*measure x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_QuadraticVariation_EndpointEnergy

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! Brute-force sanity check: all series of length ≤ 4 over [-4, 4].
    `false` means no counterexample was found. -/
#eval do
  let vals : List Int := [-4, -3, -2, -1, 0, 1, 2, 3, 4]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  let ls : List (List Int) := lists 4
  pure ((ls.filter (fun (x : List Int) =>
    decide ((x.length - 1 : Int) * QuadraticVariation.measure x <
      (x.getLast?.getD 0 - x.head?.getD 0)^2))).isEmpty)

/-! Cauchy--Schwarz for a list of integers: the square of the sum is at most
    the length times the sum of squares. -/
private theorem sum_sq_le_length_mul_sum_sq (l : List Int) :
    l.sum^2 ≤ (l.length : Int) * (l.map (fun z => z^2)).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases hnil : l = []
      · subst l
        simp
      · have hlen_ne : l.length ≠ 0 := by simpa using hnil
        have hposNat : 0 < l.length := Nat.pos_of_ne_zero hlen_ne
        have hpos : 0 < (l.length : Int) := by omega
        have hsq : 0 ≤ ((l.length : Int) * a - l.sum)^2 := Int.sq_nonneg _
        have hdiff : 0 ≤ (l.length : Int) * (l.map (fun z => z^2)).sum - l.sum^2 := by
          omega
        have hnp1 : 0 ≤ (l.length : Int) + 1 := by omega
        have hnon : 0 ≤ ((l.length : Int) + 1) *
            ((l.length : Int) * (l.map (fun z => z^2)).sum - l.sum^2) :=
          Int.mul_nonneg hnp1 hdiff
        have hid :
            (l.length : Int) * (((l.length : Int) + 1) *
                (a^2 + (l.map (fun z => z^2)).sum) - (a + l.sum)^2) =
              ((l.length : Int) * a - l.sum)^2 +
                ((l.length : Int) + 1) *
                  ((l.length : Int) * (l.map (fun z => z^2)).sum - l.sum^2) := by
          grind
        have hmul : 0 ≤ (l.length : Int) * (((l.length : Int) + 1) *
            (a^2 + (l.map (fun z => z^2)).sum) - (a + l.sum)^2) := by
          rw [hid]
          exact Int.add_nonneg hsq hnon
        have hD : 0 ≤ ((l.length : Int) + 1) *
            (a^2 + (l.map (fun z => z^2)).sum) - (a + l.sum)^2 := by
          have hm : (l.length : Int) * 0 ≤ (l.length : Int) * (((l.length : Int) + 1) *
              (a^2 + (l.map (fun z => z^2)).sum) - (a + l.sum)^2) := by
            simpa using hmul
          exact (Int.mul_le_mul_left hpos).mp hm
        simpa using (show (a + l.sum)^2 ≤ ((l.length : Int) + 1) *
          (a^2 + (l.map (fun z => z^2)).sum) by omega)

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

private theorem delta_length (x : Series) :
    (delta x).length = x.length - 1 := by
  simp [delta, adj, List.length_zip]

theorem sound_QuadraticVariation_EndpointEnergy : QuadraticVariation.EndpointEnergy.Goal := by
  intro x
  cases x with
  | nil => simp [QuadraticVariation.measure, delta, adj]
  | cons a xs =>
      have h := sum_sq_le_length_mul_sum_sq (delta (a :: xs))
      rw [delta_length, delta_sum] at h
      have hcast : ((a :: xs).length : Int) - 1 =
          (((a :: xs).length - 1 : Nat) : Int) := by
        simp
      rw [hcast]
      simpa [QuadraticVariation.measure] using h

end TinylambOpt.ResearchProofs.Atlas.Goal_QuadraticVariation_EndpointEnergy
