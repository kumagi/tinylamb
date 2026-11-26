import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `ContainsTick.PrefixIndex` — `∀ b p rs, (∀ r ∈ rs, bounded b r.1) → indexed shape b p
  (prefixMass rs) = scan shape p rs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ContainsTick_PrefixIndex

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.ContainsTick

private def boxIn (q : Box) (v : Point) : Bool :=
  decide (q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1)

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool) :
    ((rs.filter f).map Prod.snd).sum =
      (rs.map (fun r => if f r then r.2 else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih]

private theorem prefixMass_cons (r : Point × Int) (rs : Rows) (x y : Int) :
    prefixMass (r :: rs) x y =
      (if r.1.x < x && r.1.y < y then r.2 else 0) + prefixMass rs x y := by
  by_cases h : r.1.x < x && r.1.y < y <;> simp [prefixMass, h]

/-- Two-dimensional inclusion-exclusion over a half-open rectangle. -/
private theorem prefixMass_box (rs : Rows) (x0 x1 y0 y1 : Int)
    (hx : x0 < x1) (hy : y0 < y1) :
    prefixMass rs x1 y1 - prefixMass rs x0 y1 - prefixMass rs x1 y0 + prefixMass rs x0 y0 =
      ((rs.filter (fun r => boxIn (box x0 x1 y0 y1) r.1)).map Prod.snd).sum := by
  rw [sum_filter_eq_sum_if]
  simp only [boxIn, box]
  induction rs with
  | nil => simp [prefixMass]
  | cons r rs ih =>
      rw [prefixMass_cons, prefixMass_cons, prefixMass_cons, prefixMass_cons]
      simp only [List.map_cons, List.sum_cons]
      by_cases hx1 : r.1.x < x1 <;> by_cases hx0 : r.1.x < x0 <;>
        by_cases hy1 : r.1.y < y1 <;> by_cases hy0 : r.1.y < y0 <;>
        by_cases hxloP : x0 ≤ r.1.x <;> by_cases hyloP : y0 ≤ r.1.y <;>
        simp [hx1, hx0, hy1, hy0, hxloP, hyloP] <;>
        first | exact ih | omega

private theorem corners_box_one (x0 x1 y0 y1 : Int) (rs : Rows)
    (hx : x0 < x1) (hy : y0 < y1) :
    corners (box x0 x1 y0 y1) (prefixMass rs) =
      ((rs.filter (fun r => boxIn (box x0 x1 y0 y1) r.1)).map Prod.snd).sum := by
  simp [corners, box, hx, hy, prefixMass_box rs x0 x1 y0 y1 hx hy]

private theorem corners_box_empty (x0 x1 y0 y1 : Int) (rs : Rows)
    (h : ¬ (x0 < x1 ∧ y0 < y1)) :
    corners (box x0 x1 y0 y1) (prefixMass rs) = 0 := by
  by_cases hx : x0 < x1
  · have hy : ¬ y0 < y1 := by omega
    simp [corners, box, hx, hy]
  · simp [corners, box, hx]

private theorem sum_map_zero_of_all_zero {α : Type} (xs : List α) (f : α → Int)
    (h : ∀ x ∈ xs, f x = 0) : (xs.map f).sum = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

/-- On a bounded row the tick-containment predicate is the same as membership
    in the (nonempty) stencil rectangle: `v.x < p.x+1` is `v.x ≤ p.x` and
    `p.x+1 ≤ v.y` is `p.x < v.y`. -/
private theorem box_iff_qualifies (b : Int) (p v : Point) (hv : bounded b v)
    (_hx : -b < p.x + 1) (_hy : p.x + 1 < b + 1) :
    boxIn (box (-b) (p.x + 1) (p.x + 1) (b + 1)) v = shape.qualifies p v := by
  unfold bounded at hv
  unfold boxIn box shape
  apply Bool.eq_iff_iff.mpr
  simp only [Bool.and_eq_true, decide_eq_true_eq]
  constructor <;> intro h <;> omega

theorem sound_ContainsTick_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  simp [indexed, scan, shape]
  by_cases hrect : (-b < p.x + 1 && p.x + 1 < b + 1) = true
  · have hconj : -b < p.x + 1 ∧ p.x + 1 < b + 1 := by
      simpa only [Bool.and_eq_true, decide_eq_true_eq] using hrect
    rw [corners_box_one (-b) (p.x + 1) (p.x + 1) (b + 1) rs hconj.1 hconj.2]
    rw [sum_filter_eq_sum_if, sum_filter_eq_sum_if]
    apply congrArg List.sum
    apply List.map_congr_left
    intro r hr
    rw [box_iff_qualifies b p r.1 (hb r hr) hconj.1 hconj.2]
    dsimp [shape]
    rfl
  · have hn : ¬(-b < p.x + 1 ∧ p.x + 1 < b + 1) := by
      intro h
      apply hrect
      simpa only [Bool.and_eq_true, decide_eq_true_eq] using h
    rw [corners_box_empty (-b) (p.x + 1) (p.x + 1) (b + 1) rs hn]
    symm
    rw [sum_filter_eq_sum_if]
    apply sum_map_zero_of_all_zero
    intro r hr
    have hqfalse : (decide (r.1.x ≤ p.x) && decide (p.x < r.1.y)) = false := by
      apply Bool.eq_false_of_ne_true
      intro ht
      have hv := hb r hr
      unfold bounded at hv
      simp only [Bool.and_eq_true, decide_eq_true_eq] at ht
      have hdisj : p.x + 1 ≤ -b ∨ b + 1 ≤ p.x + 1 := by omega
      rcases hdisj with h1 | h1 <;> omega
    simp [hqfalse]

end TinylambOpt.ResearchProofs.Atlas.Goal_ContainsTick_PrefixIndex
