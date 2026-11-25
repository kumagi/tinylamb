import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas_RangePlans_G3

open TinylambOpt.ResearchGoals.Atlas.RangePlans

private theorem prefixMass_singleton (r : Point × Int) (x y : Int) :
    prefixMass [r] x y = if r.1.x < x && r.1.y < y then r.2 else 0 := by
  by_cases hx : r.1.x < x <;> by_cases hy : r.1.y < y <;> simp [prefixMass, hx, hy]

private theorem prefixMass_cons (r : Point × Int) (rs : Rows) (x y : Int) :
    prefixMass (r::rs) x y = (if r.1.x < x && r.1.y < y then r.2 else 0) + prefixMass rs x y := by
  by_cases hx : r.1.x < x <;> by_cases hy : r.1.y < y <;> simp [prefixMass, hx, hy]

private theorem prefixMass_append (es₁ es₂ : Rows) (x y : Int) :
    prefixMass (es₁ ++ es₂) x y = prefixMass es₁ x y + prefixMass es₂ x y := by
  induction es₁ with
  | nil => simp [prefixMass]
  | cons e es ih =>
      rw [List.cons_append, prefixMass_cons, prefixMass_cons, ih]
      omega

private theorem scan_cons (s : Shape) (p : Point) (r : Point × Int) (rs : Rows) :
    scan s p (r::rs) = (if s.qualifies p r.1 then r.2 else 0) + scan s p rs := by
  cases hq : s.qualifies p r.1 <;> simp [scan, hq]

private theorem corners_add (q : Box) (f g : Int → Int → Int) :
    corners q (fun x y => f x y + g x y) = corners q f + corners q g := by
  unfold corners
  by_cases h : q.x0 < q.x1 && q.y0 < q.y1
  · simp [h, Int.mul_add, Int.mul_sub]
    omega
  · simp [h]

private theorem indexed_add (ss : List Box) (f g : Int → Int → Int) :
    ((ss.map (corners · (fun x y => f x y + g x y))).sum) =
      (ss.map (corners · f)).sum + (ss.map (corners · g)).sum := by
  induction ss with
  | nil => simp
  | cons q qs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [corners_add, ih]
      omega

private theorem indexed_prefixMass_cons (s : Shape) (b : Int) (p : Point)
    (r : Point × Int) (rs : Rows) :
    indexed s b p (prefixMass (r::rs)) =
      indexed s b p (prefixMass [r]) + indexed s b p (prefixMass rs) := by
  unfold indexed
  have hfun : prefixMass (r::rs) = (fun x y => prefixMass [r] x y + prefixMass rs x y) := by
    funext x y
    rw [prefixMass_cons, prefixMass_singleton]
  rw [hfun]
  exact indexed_add (s.stencil b p) _ _

private theorem prefixIndex_eq_scan_of_row
    (s : Shape) (b : Int) (p : Point) (rs : Rows)
    (hrow : ∀ r : Point × Int, bounded b r.1 →
        indexed s b p (prefixMass [r]) = (if s.qualifies p r.1 then r.2 else 0))
    (hb : ∀ r ∈ rs, bounded b r.1) :
    indexed s b p (prefixMass rs) = scan s p rs := by
  induction rs with
  | nil =>
      simp [indexed, corners, prefixMass, scan]
      induction (s.stencil b p) <;> simp [*]
  | cons r rs ih =>
      have hbr := hb r (by simp)
      have hrs : ∀ r ∈ rs, bounded b r.1 := by intro r hr; exact hb r (by simp [hr])
      rw [indexed_prefixMass_cons, scan_cons, hrow r hbr, ih hrs]

private def inBox (q : Box) (r : Point) : Prop :=
  q.x0 ≤ r.x ∧ r.x < q.x1 ∧ q.y0 ≤ r.y ∧ r.y < q.y1

private instance (q : Box) (r : Point) : Decidable (inBox q r) := by
  unfold inBox
  infer_instance

private theorem sum_map_mul_const (ss : List Box) (f : Box → Int) (w : Int) :
    (ss.map (fun q => w * f q)).sum = w * (ss.map f).sum := by
  induction ss with
  | nil => simp
  | cons q qs ih =>
      simp only [List.map_cons, List.sum_cons, ih, Int.mul_add]

private theorem sum_map_mul_const' {α : Type} (ss : List α) (f : α → Int) (w : Int) :
    (ss.map (fun q => f q * w)).sum = (ss.map f).sum * w := by
  induction ss with
  | nil => simp
  | cons q qs ih =>
      simp only [List.map_cons, List.sum_cons, ih, Int.add_mul]

private theorem atPoint_append (es₁ es₂ : Rows) (r : Point) :
    atPoint (es₁ ++ es₂) r = atPoint es₁ r + atPoint es₂ r := by
  simp [atPoint, prefixMass_append]

private theorem atPoint_events (q : Box) (w : Int) (r : Point) :
    atPoint (events q w) r = if inBox q r then w * q.coeff else 0 := by
  unfold events
  by_cases hb : q.x0 < q.x1 && q.y0 < q.y1
  · have hbprop : q.x0 < q.x1 ∧ q.y0 < q.y1 := by simpa using hb
    simp [hb, atPoint, prefixMass]
    by_cases hx0 : q.x0 < r.x + 1 <;> by_cases hx1 : q.x1 < r.x + 1 <;>
      by_cases hy0 : q.y0 < r.y + 1 <;> by_cases hy1 : q.y1 < r.y + 1 <;>
      simp [hx0, hx1, hy0, hy1]
    all_goals unfold inBox; omega
  · have hbprop : ¬ (q.x0 < q.x1 ∧ q.y0 < q.y1) := by simpa using hb
    simp [hb, atPoint, prefixMass]
    unfold inBox
    omega

private theorem atPoint_flatMap_events (ss : List Box) (w : Int) (r : Point) :
    atPoint (ss.flatMap (events · w)) r =
      (ss.map (fun q => if inBox q r then w * q.coeff else 0)).sum := by
  induction ss with
  | nil => simp [atPoint, prefixMass]
  | cons q qs ih =>
      simp only [List.flatMap_cons]
      rw [atPoint_append, atPoint_events, ih]
      simp

end TinylambOpt.ResearchProofs.Atlas_RangePlans_G3
