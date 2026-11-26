import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `PriorTradingDay.PrefixIndex` — `∀ b p rs, (∀ r ∈ rs, bounded b r.1) → indexed shape
  b p (prefixMass rs) = scan shape p rs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PriorTradingDay_PrefixIndex

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.PriorTradingDay

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

/-- Each signed rectangle contributes exactly the rows inside it, even when it
    is empty (then both sides vanish). -/
private theorem corners_eq_sum_boxIn (rs : Rows) (x0 x1 y0 y1 : Int) :
    corners (box x0 x1 y0 y1) (prefixMass rs) =
      (rs.map (fun r => if boxIn (box x0 x1 y0 y1) r.1 then r.2 else 0)).sum := by
  by_cases h : x0 < x1 ∧ y0 < y1
  · rw [corners_box_one x0 x1 y0 y1 rs h.1 h.2]
    rw [sum_filter_eq_sum_if]
  · rw [corners_box_empty x0 x1 y0 y1 rs h]
    symm
    apply sum_map_zero_of_all_zero
    intro r hr
    have hf : boxIn (box x0 x1 y0 y1) r.1 = false := by
      apply Bool.eq_false_of_ne_true
      intro ht
      have hh := ht
      unfold boxIn at hh
      have ht' : x0 ≤ r.1.x ∧ r.1.x < x1 ∧ y0 ≤ r.1.y ∧ r.1.y < y1 :=
        of_decide_eq_true hh
      omega
    simp [hf]

private theorem sum_map_add_pointwise {α : Type} (xs : List α) (f g h : α → Int)
    (hfg : ∀ x ∈ xs, f x + g x = h x) :
    (xs.map f).sum + (xs.map g).sum = (xs.map h).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx := hfg x (by simp)
      have hi := ih (fun y hy => hfg y (by simp [hy]))
      omega

theorem sound_PriorTradingDay_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  simp [indexed, shape, scan]
  rw [corners_eq_sum_boxIn rs (p.x - 3) p.x (-b) (b + 1)]
  rw [corners_eq_sum_boxIn rs p.x (p.x + 1) (-b) p.y]
  rw [sum_filter_eq_sum_if]
  apply sum_map_add_pointwise
  intro r hr
  have hv := hb r hr
  unfold bounded at hv
  let q1 : Box := box (p.x - 3) p.x (-b) (b + 1)
  let q2 : Box := box p.x (p.x + 1) (-b) p.y
  change (if boxIn q1 r.1 then r.2 else 0) + (if boxIn q2 r.1 then r.2 else 0) =
    (if shape.qualifies p r.1 then r.2 else 0)
  by_cases h1 : boxIn q1 r.1 = true
  · have htq1 : p.x - 3 ≤ r.1.x ∧ r.1.x < p.x := by
      have hh := h1
      unfold boxIn at hh
      have ht : q1.x0 ≤ r.1.x ∧ r.1.x < q1.x1 ∧ q1.y0 ≤ r.1.y ∧ r.1.y < q1.y1 :=
        of_decide_eq_true hh
      exact ⟨ht.1, ht.2.1⟩
    have h2f : boxIn q2 r.1 = false := by
      apply Bool.eq_false_of_ne_true
      intro ht
      have hh := ht
      unfold boxIn at hh
      have ht' : q2.x0 ≤ r.1.x ∧ r.1.x < q2.x1 ∧ q2.y0 ≤ r.1.y ∧ r.1.y < q2.y1 :=
        of_decide_eq_true hh
      simp only [q2, box] at ht'
      omega
    have hq : shape.qualifies p r.1 = true := by
      simp [shape, htq1.1, htq1.2]
    simp [h1, h2f, hq]
  · by_cases h2 : boxIn q2 r.1 = true
    · have hh := h2
      unfold boxIn at hh
      have htq2 : q2.x0 ≤ r.1.x ∧ r.1.x < q2.x1 ∧ q2.y0 ≤ r.1.y ∧ r.1.y < q2.y1 :=
        of_decide_eq_true hh
      have heq : r.1.x = p.x := by
        simp only [q2, box] at htq2
        omega
      have hylt : r.1.y < p.y := by
        simp only [q2, box] at htq2
        exact htq2.2.2.2
      have hq : shape.qualifies p r.1 = true := by
        simp [shape, heq, hylt]
      simp [h1, h2, hq]
    · have hq : shape.qualifies p r.1 = false := by
        apply Bool.eq_false_of_ne_true
        intro ht
        have ht' : (((p.x - 3 ≤ r.1.x && r.1.x < p.x) = true) ||
            ((r.1.x == p.x && r.1.y < p.y) = true)) := by
          simpa [shape] using ht
        simp only [Bool.and_eq_true, Bool.or_eq_true, decide_eq_true_eq] at ht'
        rcases ht' with hfirst | hsecond
        · have hyhi : r.1.y < b + 1 := by omega
          have hb1 : boxIn q1 r.1 = true := by
            simp [boxIn, q1, box, hfirst.1, hfirst.2, hv.2.2.1, hyhi]
          exact h1 hb1
        · have hxlow : p.x ≤ r.1.x := by
            have heq : r.1.x = p.x := by simpa using hsecond.1
            omega
          have hxhigh : r.1.x < p.x + 1 := by
            have heq : r.1.x = p.x := by simpa using hsecond.1
            omega
          have hb2 : boxIn q2 r.1 = true := by
            simp [boxIn, q2, box, hxlow, hxhigh, hv.2.2.1, hsecond.2]
          exact h2 hb2
      simp [h1, h2, hq]

end TinylambOpt.ResearchProofs.Atlas.Goal_PriorTradingDay_PrefixIndex
