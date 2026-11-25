import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ParetoFront_PrefixIndex

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.ParetoFront

/-! The stencil is the quadrant `[p.x, b+1) x [p.y, b+1)` with the single
corner cell `[p.x,p.x+1) x [p.y,p.y+1)` removed.  For a row confined to
`[-b,b]^2` the first box counts it iff `p.x ≤ v.x` and `p.y ≤ v.y`, and the
second subtracts exactly the equality case, so the signed weight is the
indicator of strict Pareto dominance.  Corner sums are additive over rows,
which turns the prefix-index expression into the scan by induction. -/

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

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, inBox, stencilWeight, bounded] at hv ⊢
  omega

theorem sound_ParetoFront_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_weight shape b p rs (fun r hr => stencil_id b p r.1 (hb r hr))

end TinylambOpt.ResearchProofs.Atlas.Goal_ParetoFront_PrefixIndex
