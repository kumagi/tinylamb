import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_AlternatingBalance_ShiftResidue

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.AlternatingBalance

/-! Brute-force sanity check: all series of length <= 4 over [-2, 2],
    all shifts b in [-2, 2].  Zero mismatches confirms the statement. -/
#eval do
  let vals : List Int := [-2, -1, 0, 1, 2]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  let ls := lists 4
  pure ((ls.flatMap (fun x => vals.map (fun b =>
    if measure (shifted b x) != measure x + ((x.length % 2 : Nat) : Int) * b
    then 1 else 0))).sum)

/-! #### Alternating (parity-weighted) sum helpers -/

private def alt (k : Nat) (x : Series) : Int :=
  ((x.zipIdx k).map (fun (v, i) => (-1 : Int) ^ i * v)).sum

private theorem measure_alt (x : Series) : AlternatingBalance.measure x = alt 0 x := by
  rfl

private theorem alt_cons (k : Nat) (a : Int) (xs : Series) :
    alt k (a :: xs) = (-1 : Int) ^ k * a + alt (k + 1) xs := by
  simp [alt, List.zipIdx_cons]

private theorem neg_one_pow_succ (n : Nat) : (-1 : Int) ^ (n + 1) = -((-1 : Int) ^ n) := by
  rw [Int.pow_succ]
  omega

private theorem alt_shift (k : Nat) (x : Series) : alt k x = (-1 : Int) ^ k * alt 0 x := by
  induction x generalizing k with
  | nil => simp [alt]
  | cons a xs ih =>
      rw [alt_cons, ih (k + 1)]
      rw [alt_cons, ih 1]
      rw [neg_one_pow_succ]
      simp only [Int.pow_zero]
      grind

private theorem measure_cons (a : Int) (xs : Series) :
    AlternatingBalance.measure (a :: xs) = a - AlternatingBalance.measure xs := by
  rw [measure_alt, measure_alt, alt_cons, alt_shift 1]
  rw [neg_one_pow_succ]
  simp only [Int.pow_zero]
  grind

theorem sound_AlternatingBalance_ShiftResidue : AlternatingBalance.ShiftResidue.Goal := by
  intro b x
  induction x with
  | nil => simp [AlternatingBalance.measure, shifted]
  | cons a xs ih =>
      change AlternatingBalance.measure ((a + b) :: shifted b xs) =
        AlternatingBalance.measure (a :: xs) + (((xs.length + 1) % 2 : Nat) : Int) * b
      rw [measure_cons, measure_cons, ih]
      by_cases h : xs.length % 2 == 0
      · have hlt : xs.length % 2 < 2 := Nat.mod_lt _ (by decide)
        have h0 : xs.length % 2 = 0 := by simpa using h
        have h1 : (xs.length + 1) % 2 = 1 := by omega
        rw [h0, h1]
        grind
      · have hlt : xs.length % 2 < 2 := Nat.mod_lt _ (by decide)
        have hn0 : ¬ xs.length % 2 = 0 := by simpa using h
        have h0 : xs.length % 2 = 1 := by omega
        have h1 : (xs.length + 1) % 2 = 0 := by omega
        rw [h0, h1]
        grind

end TinylambOpt.ResearchProofs.Atlas.Goal_AlternatingBalance_ShiftResidue
