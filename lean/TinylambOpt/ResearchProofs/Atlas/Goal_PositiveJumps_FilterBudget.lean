import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `PositiveJumps.FilterBudget` — `∀ x (keep : Int → Bool), measure (x.filter keep) ≤
  measure x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PositiveJumps_FilterBudget

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! ### Exhaustive sanity check on small inputs -/

private def rowsOf : Nat → List Int → List (List Int)
  | 0, _ => [[]]
  | n + 1, pool => pool.flatMap (fun v => (rowsOf n pool).map (v :: ·))

private def boolMasks : Nat → List (List Bool)
  | 0 => [[]]
  | n + 1 => (boolMasks n).flatMap (fun bs => [true :: bs, false :: bs])

private def maskKeep : List (Int × Bool) → Int → Bool
  | [], _ => false
  | (v, b) :: rest, w => if v == w then b else maskKeep rest w

#eval
  let pool := [-3, -2, -1, 0, 1, 2, 3]
  let xs := (List.range 6).flatMap (fun n => rowsOf n pool)
  let masks := boolMasks pool.length
  xs.all (fun x => masks.all (fun m =>
    PositiveJumps.measure (x.filter (maskKeep (pool.zip m))) ≤ PositiveJumps.measure x))

/-! ### Positive variation measured from an arbitrary anchor -/

private def pv (a : Int) : Series → Int
  | [] => 0
  | b :: xs => max 0 (b - a) + pv b xs

private theorem pv_eq_measure (a : Int) (xs : Series) :
    pv a xs = PositiveJumps.measure (a :: xs) := by
  induction xs generalizing a with
  | nil =>
      simp [pv, PositiveJumps.measure, delta, adj]
  | cons b bs ih =>
      simp only [pv]
      rw [ih b]
      simp [PositiveJumps.measure, delta, adj]

private theorem pos_add (x y : Int) : max 0 (x + y) ≤ max 0 x + max 0 y := by
  omega

/-- Inserting a point in front of a path cannot reduce its positive variation. -/
private theorem pv_prepend (a b : Int) (xs : Series) : pv a xs ≤ pv a (b :: xs) := by
  induction xs generalizing a with
  | nil =>
      simp only [pv]
      omega
  | cons c cs ih =>
      simp only [pv]
      have h : max 0 (c - a) ≤ max 0 (b - a) + max 0 (c - b) := by
        have h' := pos_add (b - a) (c - b)
        omega
      omega

/-- Filtering keeps a subsequence, hence cannot increase positive variation. -/
private theorem pv_filter_le (a : Int) (xs : Series) (keep : Int → Bool) :
    pv a (xs.filter keep) ≤ pv a xs := by
  induction xs generalizing a with
  | nil =>
      simp only [pv, List.filter_nil]
      omega
  | cons b bs ih =>
      by_cases hb : keep b = true
      · have hfilter : (b :: bs).filter keep = b :: bs.filter keep := by simp [hb]
        rw [hfilter]
        simp [pv]
        have h := ih b
        omega
      · have hfilter : (b :: bs).filter keep = bs.filter keep := by simp [hb]
        rw [hfilter]
        have h1 := ih a
        have h2 := pv_prepend a b bs
        omega

private theorem measure_le_pv (a : Int) (l : Series) :
    PositiveJumps.measure l ≤ pv a l := by
  cases l with
  | nil =>
      simp [pv, PositiveJumps.measure, delta, adj]
  | cons b bs =>
      rw [← pv_eq_measure b bs]
      simp only [pv]
      omega

/-! ### Main theorem -/

theorem sound_PositiveJumps_FilterBudget : PositiveJumps.FilterBudget.Goal := by
  intro x keep
  cases x with
  | nil =>
      simp [PositiveJumps.measure]
  | cons a xs =>
      calc
        PositiveJumps.measure ((a :: xs).filter keep) ≤ pv a ((a :: xs).filter keep) :=
          measure_le_pv a ((a :: xs).filter keep)
        _ ≤ pv a (a :: xs) := pv_filter_le a (a :: xs) keep
        _ = pv a xs := by simp [pv]
        _ = PositiveJumps.measure (a :: xs) := pv_eq_measure a xs

end TinylambOpt.ResearchProofs.Atlas.Goal_PositiveJumps_FilterBudget
