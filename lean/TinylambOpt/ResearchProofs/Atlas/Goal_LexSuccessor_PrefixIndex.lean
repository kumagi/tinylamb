import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `LexSuccessor.PrefixIndex` — `∀ b p rs, (∀ r ∈ rs, bounded b r.1) → indexed shape b p
  (prefixMass rs) = scan shape p rs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LexSuccessor_PrefixIndex

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.LexSuccessor

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

private theorem corners_box_one (q : Box) (rs : Rows)
    (hx : q.x0 < q.x1) (hy : q.y0 < q.y1) (hc : q.coeff = 1) :
    corners q (prefixMass rs) =
      ((rs.filter (fun r => boxIn q r.1)).map Prod.snd).sum := by
  rcases q with ⟨x0, x1, y0, y1, coeff⟩
  simp only at hc hx hy
  subst coeff
  simp [corners, box, hx, hy, prefixMass_box rs x0 x1 y0 y1 hx hy]

private theorem corners_box_empty (q : Box) (rs : Rows)
    (h : ¬ (q.x0 < q.x1 ∧ q.y0 < q.y1)) :
    corners q (prefixMass rs) = 0 := by
  rcases q with ⟨x0, x1, y0, y1, coeff⟩
  simp only at h
  by_cases hx : x0 < x1
  · have hy : ¬ y0 < y1 := by omega
    simp [corners, hx, hy]
  · simp [corners, hx]

private theorem sum_map_zero_of_all_zero {α : Type} (xs : List α) (f : α → Int)
    (h : ∀ x ∈ xs, f x = 0) : (xs.map f).sum = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem empty_box_filter_zero (q : Box) (rs : Rows)
    (h : ¬ (q.x0 < q.x1 ∧ q.y0 < q.y1)) :
    ((rs.filter (fun r => boxIn q r.1)).map Prod.snd).sum = 0 := by
  rw [sum_filter_eq_sum_if]
  apply sum_map_zero_of_all_zero
  intro r hr
  have hfalse : boxIn q r.1 = false := by
    apply Bool.eq_false_of_ne_true
    intro ht
    have ht' : q.x0 ≤ r.1.x ∧ r.1.x < q.x1 ∧ q.y0 ≤ r.1.y ∧ r.1.y < q.y1 := by
      simpa [boxIn] using ht
    omega
  simp [hfalse]

private theorem corners_eq_filter (q : Box) (rs : Rows) (hc : q.coeff = 1) :
    corners q (prefixMass rs) =
      ((rs.filter (fun r => boxIn q r.1)).map Prod.snd).sum := by
  by_cases h : q.x0 < q.x1 ∧ q.y0 < q.y1
  · exact corners_box_one q rs h.1 h.2 hc
  · rw [corners_box_empty q rs h, empty_box_filter_zero q rs h]

/-- For a bounded row the two stencil boxes are exactly the two arms of the
    lexicographic successor predicate, and they are disjoint. -/
private theorem contribution (b : Int) (p : Point) (r : Point × Int) (hr : bounded b r.1) :
    (if boxIn (box (p.x + 1) (b + 1) (-b) (b + 1)) r.1 then r.2 else 0) +
    (if boxIn (box p.x (p.x + 1) (p.y + 1) (b + 1)) r.1 then r.2 else 0) =
      (if shape.qualifies p r.1 then r.2 else 0) := by
  unfold bounded at hr
  by_cases hxlt : p.x < r.1.x
  · have hi1 : p.x + 1 ≤ r.1.x ∧ r.1.x < b + 1 ∧ -b ≤ r.1.y ∧ r.1.y < b + 1 := by omega
    have hi2 : ¬ (p.x ≤ r.1.x ∧ r.1.x < p.x + 1 ∧ p.y + 1 ≤ r.1.y ∧ r.1.y < b + 1) := by omega
    have hb1 : boxIn (box (p.x + 1) (b + 1) (-b) (b + 1)) r.1 = true := by
      rw [boxIn, box, decide_eq_true_eq]
      exact hi1
    have hb2 : boxIn (box p.x (p.x + 1) (p.y + 1) (b + 1)) r.1 = false := by
      rw [boxIn, box, decide_eq_false_iff_not]
      exact hi2
    have hq : shape.qualifies p r.1 = true := by simp [shape, hxlt]
    rw [hb1, hb2, hq]
    simp
  · by_cases hxeq : p.x = r.1.x
    · by_cases hylt : p.y < r.1.y
      · have hi1 : ¬ (p.x + 1 ≤ r.1.x ∧ r.1.x < b + 1 ∧ -b ≤ r.1.y ∧ r.1.y < b + 1) := by omega
        have hi2 : p.x ≤ r.1.x ∧ r.1.x < p.x + 1 ∧ p.y + 1 ≤ r.1.y ∧ r.1.y < b + 1 := by omega
        have hb1 : boxIn (box (p.x + 1) (b + 1) (-b) (b + 1)) r.1 = false := by
          rw [boxIn, box, decide_eq_false_iff_not]
          exact hi1
        have hb2 : boxIn (box p.x (p.x + 1) (p.y + 1) (b + 1)) r.1 = true := by
          rw [boxIn, box, decide_eq_true_eq]
          exact hi2
        have hq : shape.qualifies p r.1 = true := by simp [shape, hxeq, hylt]
        rw [hb1, hb2, hq]
        simp
      · have hi1 : ¬ (p.x + 1 ≤ r.1.x ∧ r.1.x < b + 1 ∧ -b ≤ r.1.y ∧ r.1.y < b + 1) := by omega
        have hi2 : ¬ (p.x ≤ r.1.x ∧ r.1.x < p.x + 1 ∧ p.y + 1 ≤ r.1.y ∧ r.1.y < b + 1) := by omega
        have hb1 : boxIn (box (p.x + 1) (b + 1) (-b) (b + 1)) r.1 = false := by
          rw [boxIn, box, decide_eq_false_iff_not]
          exact hi1
        have hb2 : boxIn (box p.x (p.x + 1) (p.y + 1) (b + 1)) r.1 = false := by
          rw [boxIn, box, decide_eq_false_iff_not]
          exact hi2
        have hq : shape.qualifies p r.1 = false := by simp [shape, hxeq, hylt]
        rw [hb1, hb2, hq]
        simp
    · have hi1 : ¬ (p.x + 1 ≤ r.1.x ∧ r.1.x < b + 1 ∧ -b ≤ r.1.y ∧ r.1.y < b + 1) := by omega
      have hi2 : ¬ (p.x ≤ r.1.x ∧ r.1.x < p.x + 1 ∧ p.y + 1 ≤ r.1.y ∧ r.1.y < b + 1) := by omega
      have hb1 : boxIn (box (p.x + 1) (b + 1) (-b) (b + 1)) r.1 = false := by
        rw [boxIn, box, decide_eq_false_iff_not]
        exact hi1
      have hb2 : boxIn (box p.x (p.x + 1) (p.y + 1) (b + 1)) r.1 = false := by
        rw [boxIn, box, decide_eq_false_iff_not]
        exact hi2
      have hq : shape.qualifies p r.1 = false := by simp [shape, hxlt, hxeq]
      rw [hb1, hb2, hq]
      simp

private theorem sum_map_add_int {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.map_cons, List.sum_cons]; rw [ih]; omega

theorem sound_LexSuccessor_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  let q1 := box (p.x + 1) (b + 1) (-b) (b + 1)
  let q2 := box p.x (p.x + 1) (p.y + 1) (b + 1)
  simp [indexed, scan, shape]
  rw [corners_eq_filter q1 rs (by simp [q1, box]),
      corners_eq_filter q2 rs (by simp [q2, box])]
  rw [sum_filter_eq_sum_if, sum_filter_eq_sum_if, sum_filter_eq_sum_if]
  rw [← sum_map_add_int]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  exact contribution b p r (hb r hr)

end TinylambOpt.ResearchProofs.Atlas.Goal_LexSuccessor_PrefixIndex
