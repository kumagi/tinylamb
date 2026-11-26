import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `AlignedWeek.PrefixIndex` — `∀ b p rs, (∀ r ∈ rs, bounded b r.1) → indexed shape b p
  (prefixMass rs) = scan shape p rs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_AlignedWeek_PrefixIndex

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.AlignedWeek

private def inBox (q : Box) (v : Point) : Bool :=
  q.x0 ≤ v.x && v.x < q.x1 && q.y0 ≤ v.y && v.y < q.y1

private def cornerMass (r : Point × Int) (x y : Int) : Int :=
  if r.1.x < x && r.1.y < y then r.2 else 0

private theorem sum_map_zero {α : Type} (xs : List α) :
    (xs.map (fun _ => (0 : Int))).sum = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem prefixMass_cons (r : Point × Int) (rs : Rows) (x y : Int) :
    prefixMass (r :: rs) x y = cornerMass r x y + prefixMass rs x y := by
  unfold prefixMass cornerMass
  by_cases h : r.1.x < x && r.1.y < y <;> simp [h]

private theorem cornerMass_box (r : Point × Int) (q : Box)
    (hq : q.x0 < q.x1 ∧ q.y0 < q.y1) :
    cornerMass r q.x1 q.y1 - cornerMass r q.x0 q.y1 -
      cornerMass r q.x1 q.y0 + cornerMass r q.x0 q.y0 =
        (if inBox q r.1 then r.2 else 0) := by
  unfold cornerMass inBox
  by_cases hx1 : r.1.x < q.x1 <;> by_cases hx0 : r.1.x < q.x0 <;>
    by_cases hy1 : r.1.y < q.y1 <;> by_cases hy0 : r.1.y < q.y0 <;>
    by_cases hxl : q.x0 ≤ r.1.x <;> by_cases hyl : q.y0 ≤ r.1.y <;>
    simp [hx1, hx0, hy1, hy0, hxl, hyl] <;> omega

private theorem corners_cons (q : Box) (r : Point × Int) (rs : Rows) :
    corners q (prefixMass (r :: rs)) =
      corners q (prefixMass rs) + (if inBox q r.1 then q.coeff * r.2 else 0) := by
  unfold corners
  by_cases h : (q.x0 < q.x1 && q.y0 < q.y1) = true
  · rw [ite_eq_left h, ite_eq_left h]
    have hq : q.x0 < q.x1 ∧ q.y0 < q.y1 := by simpa using h
    rw [prefixMass_cons, prefixMass_cons, prefixMass_cons, prefixMass_cons]
    have hcorner := cornerMass_box r q hq
    have hsplit :
        (cornerMass r q.x1 q.y1 + prefixMass rs q.x1 q.y1) -
            (cornerMass r q.x0 q.y1 + prefixMass rs q.x0 q.y1) -
            (cornerMass r q.x1 q.y0 + prefixMass rs q.x1 q.y0) +
            (cornerMass r q.x0 q.y0 + prefixMass rs q.x0 q.y0) =
          (prefixMass rs q.x1 q.y1 - prefixMass rs q.x0 q.y1 -
            prefixMass rs q.x1 q.y0 + prefixMass rs q.x0 q.y0) +
          (cornerMass r q.x1 q.y1 - cornerMass r q.x0 q.y1 -
            cornerMass r q.x1 q.y0 + cornerMass r q.x0 q.y0) := by
      omega
    rw [hsplit, Int.mul_add, hcorner]
    by_cases hi : inBox q r.1 <;> simp [hi]
  · rw [ite_eq_right h, ite_eq_right h]
    have hnot : ¬ (q.x0 < q.x1 ∧ q.y0 < q.y1) := by
      intro hh
      apply h
      simpa using hh
    have hn : inBox q r.1 = false := by
      apply Bool.eq_false_of_ne_true
      intro ht
      simp [inBox] at ht
      exact hnot ⟨by omega, by omega⟩
    simp [hn]

private def stencilWeight (s : Shape) (b : Int) (p v : Point) : Int :=
  ((s.stencil b p).map (fun q => if inBox q v then q.coeff else 0)).sum

private theorem indexed_cons (s : Shape) (b : Int) (p : Point)
    (r : Point × Int) (rs : Rows) :
    indexed s b p (prefixMass (r :: rs)) =
      indexed s b p (prefixMass rs) + stencilWeight s b p r.1 * r.2 := by
  unfold indexed stencilWeight
  induction s.stencil b p with
  | nil => simp
  | cons q qs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [corners_cons, ih]
      by_cases hi : inBox q r.1 <;> simp [hi, Int.add_mul] <;> omega

private theorem scan_cons (s : Shape) (p : Point) (r : Point × Int) (rs : Rows) :
    scan s p (r :: rs) = (if s.qualifies p r.1 then r.2 else 0) + scan s p rs := by
  by_cases h : s.qualifies p r.1 = true <;> simp [scan, h]

private theorem prefixIndex_of_weight (s : Shape) (b : Int) (p : Point) (rs : Rows)
    (h : ∀ r ∈ rs, stencilWeight s b p r.1 = if s.qualifies p r.1 then (1 : Int) else 0) :
    indexed s b p (prefixMass rs) = scan s p rs := by
  induction rs with
  | nil =>
      simp [indexed, scan, prefixMass, corners]
      exact sum_map_zero (s.stencil b p)
  | cons r rs ih =>
      rw [indexed_cons, ih (fun r' hr' => h r' (by simp [hr'])),
        h r List.mem_cons_self, scan_cons]
      by_cases hq : s.qualifies p r.1 <;> simp [hq] <;> omega

/-- The single aligned-week box covers exactly the rows with the same floor buckets. -/
private theorem inBox_aligned (p v : Point) :
    inBox (box (7*(p.x/7)) (7*(p.x/7)+7) (10*(p.y/10)) (10*(p.y/10)+10)) v =
      shape.qualifies p v := by
  apply Bool.eq_iff_iff.mpr
  simp only [inBox, box, shape]
  constructor
  · intro h
    simp only [Bool.and_eq_true] at h
    rcases h with ⟨⟨⟨hx1, hx2⟩, hy1⟩, hy2⟩
    have hx1' : 7 * (p.x / 7) ≤ v.x := of_decide_eq_true hx1
    have hx2' : v.x < 7 * (p.x / 7) + 7 := of_decide_eq_true hx2
    have hy1' : 10 * (p.y / 10) ≤ v.y := of_decide_eq_true hy1
    have hy2' : v.y < 10 * (p.y / 10) + 10 := of_decide_eq_true hy2
    simp
    constructor
    · rw [Int.ediv_eq_iff_of_pos (by decide : (0 : Int) < 7)]
      omega
    · rw [Int.ediv_eq_iff_of_pos (by decide : (0 : Int) < 10)]
      omega
  · intro h
    simp at h
    rcases h with ⟨hx, hy⟩
    rw [Int.ediv_eq_iff_of_pos (by decide : (0 : Int) < 7)] at hx
    rw [Int.ediv_eq_iff_of_pos (by decide : (0 : Int) < 10)] at hy
    have hx1 : 7 * (p.x / 7) ≤ v.x := by omega
    have hx2 : v.x < 7 * (p.x / 7) + 7 := by omega
    have hy1 : 10 * (p.y / 10) ≤ v.y := by omega
    have hy2 : v.y < 10 * (p.y / 10) + 10 := by omega
    simp [hx1, hx2, hy1, hy2]

private theorem stencil_id (b : Int) (p v : Point) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  unfold stencilWeight
  rw [show shape.stencil b p =
      [box (7*(p.x/7)) (7*(p.x/7)+7) (10*(p.y/10)) (10*(p.y/10)+10)] by rfl]
  simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
  rw [inBox_aligned]
  simp [box]

theorem sound_AlignedWeek_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_weight shape b p rs (fun r hr => stencil_id b p r.1)

end TinylambOpt.ResearchProofs.Atlas.Goal_AlignedWeek_PrefixIndex
