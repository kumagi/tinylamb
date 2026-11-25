import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas_RangePlans_G4

open TinylambOpt.ResearchGoals.Atlas.RangePlans

private def memBox (q : Box) (v : Point) : Bool :=
  decide (q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1)

private theorem prefixMass_singleton (v : Point) (w x y : Int) :
    prefixMass [(v, w)] x y = if v.x < x && v.y < y then w else 0 := by
  by_cases h : v.x < x && v.y < y <;> simp [prefixMass, h]

private theorem prefixMass_cons (r : Point × Int) (rs : Rows) (x y : Int) :
    prefixMass (r :: rs) x y =
      prefixMass rs x y + (if r.1.x < x && r.1.y < y then r.2 else 0) := by
  by_cases h : r.1.x < x && r.1.y < y <;> simp [prefixMass, h] <;> omega

private theorem prefixMass_append (rs ts : Rows) (x y : Int) :
    prefixMass (rs ++ ts) x y = prefixMass rs x y + prefixMass ts x y := by
  induction rs with
  | nil => simp [prefixMass]
  | cons r rs ih =>
      simp [prefixMass_cons, ih]
      omega

private theorem corners_add (q : Box) (f g : Int → Int → Int) :
    corners q (fun x y => f x y + g x y) = corners q f + corners q g := by
  unfold corners
  by_cases hx : q.x0 < q.x1
  · by_cases hy : q.y0 < q.y1
    · simp [hx, hy, Int.mul_add, Int.mul_sub] <;> omega
    · simp [hx, hy]
  · simp [hx]

private theorem corners_singleton (q : Box) (v : Point) (w : Int) :
    corners q (prefixMass [(v, w)]) = q.coeff * (if memBox q v then w else 0) := by
  by_cases hx1 : q.x0 < q.x1
  · by_cases hy1 : q.y0 < q.y1
    · simp [corners, prefixMass_singleton, hx1, hy1]
      by_cases hxlo : v.x < q.x0 <;> by_cases hxhi : v.x < q.x1 <;>
        by_cases hylo : v.y < q.y0 <;> by_cases hyhi : v.y < q.y1 <;>
        by_cases hxloP : q.x0 ≤ v.x <;> by_cases hyloP : q.y0 ≤ v.y <;>
        simp [hxlo, hxhi, hylo, hyhi, hxloP, hyloP, memBox] <;> omega
    · simp [corners, hy1]
      have hnot : memBox q v = false := by
        simp [memBox]
        omega
      simp [hnot]
  · simp [corners, hx1]
    have hnot : memBox q v = false := by
      simp [memBox]
      omega
    simp [hnot]

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih] <;> omega

private theorem indexed_add (s : Shape) (b : Int) (p : Point) (f g : Int → Int → Int) :
    indexed s b p (fun x y => f x y + g x y) = indexed s b p f + indexed s b p g := by
  unfold indexed
  rw [show (s.stencil b p).map (fun q => corners q (fun x y => f x y + g x y)) =
        (s.stencil b p).map (fun q => corners q f + corners q g) by
        apply List.map_congr_left
        intro q hq
        exact corners_add q f g]
  exact sum_map_add (s.stencil b p) (fun q => corners q f) (fun q => corners q g)

private theorem map_const_zero_sum {α : Type} (xs : List α) :
    (xs.map (fun _ => (0 : Int))).sum = 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem corners_congr (q : Box) (f g : Int → Int → Int)
    (h : ∀ x y, f x y = g x y) : corners q f = corners q g := by
  unfold corners
  by_cases hq : q.x0 < q.x1 && q.y0 < q.y1
  · simp [hq, h]
  · simp [hq]

private theorem indexed_congr (s : Shape) (b : Int) (p : Point) (f g : Int → Int → Int)
    (h : ∀ x y, f x y = g x y) : indexed s b p f = indexed s b p g := by
  unfold indexed
  apply congrArg List.sum
  apply List.map_congr_left
  intro q hq
  exact corners_congr q f g h

private theorem prefixMass_cons_fun (r : Point × Int) (rs : Rows) :
    prefixMass (r :: rs) =
      fun x y => prefixMass rs x y + (if r.1.x < x && r.1.y < y then r.2 else 0) := by
  funext x y
  exact prefixMass_cons r rs x y

private theorem indexed_prefixMass_eq (s : Shape) (b : Int) (p : Point) (rs : Rows) :
    indexed s b p (prefixMass rs) =
      (rs.map (fun r => indexed s b p (prefixMass [r]))).sum := by
  induction rs with
  | nil =>
      simpa [prefixMass, indexed, corners] using map_const_zero_sum (s.stencil b p)
  | cons r rs ih =>
      rw [prefixMass_cons_fun, indexed_add, ih]
      have hs : indexed s b p (fun x y => if r.1.x < x && r.1.y < y then r.2 else 0) =
          indexed s b p (prefixMass [r]) := by
        apply indexed_congr
        intro x y
        simpa using (prefixMass_singleton r.1 r.2 x y).symm
      rw [hs]
      simp
      omega

private theorem stencil_weight (s : Shape) (b : Int) (p v : Point) (w : Int) :
    ((s.stencil b p).map (fun q => q.coeff * (if memBox q v then w else 0))).sum =
      w * ((s.stencil b p).map (fun q => q.coeff * (if memBox q v then 1 else 0))).sum := by
  induction s.stencil b p with
  | nil => simp
  | cons q qs ih =>
      by_cases h : memBox q v
      · simp [h, Int.mul_add, ih]
        rw [Int.mul_comm q.coeff w]
      · simp [h, ih]

private theorem indexed_singleton (s : Shape) (b : Int) (p v : Point) (w : Int) :
    indexed s b p (prefixMass [(v, w)]) =
      w * ((s.stencil b p).map (fun q => q.coeff * (if memBox q v then 1 else 0))).sum := by
  unfold indexed
  rw [show (s.stencil b p).map (fun q => corners q (prefixMass [(v, w)])) =
        (s.stencil b p).map (fun q => q.coeff * (if memBox q v then w else 0)) by
        apply List.map_congr_left
        intro q hq
        exact corners_singleton q v w]
  exact stencil_weight s b p v w

private theorem prefixIndex_of_stencil (s : Shape) (b : Int) (p : Point)
    (hst : ∀ v : Point, bounded b v →
      ((s.stencil b p).map (fun q => q.coeff * (if memBox q v then 1 else 0))).sum =
        if s.qualifies p v then 1 else 0) :
    ∀ rs : Rows, (∀ r ∈ rs, bounded b r.1) →
      indexed s b p (prefixMass rs) = scan s p rs := by
  intro rs hb
  rw [indexed_prefixMass_eq]
  induction rs with
  | nil => simp [scan]
  | cons r rs ih =>
      have hrb : bounded b r.1 := hb r (by simp)
      have hrs : ∀ r' ∈ rs, bounded b r'.1 := by
        intro r' hm
        exact hb r' (by simp [hm])
      have ih' := ih hrs
      simp [scan] at ih' ⊢
      by_cases hq : s.qualifies p r.1
      · rw [indexed_singleton, hst r.1 hrb]
        simp [hq, ih']
      · rw [indexed_singleton, hst r.1 hrb]
        simp [hq, ih']

namespace VarianceBudget

open TinylambOpt.ResearchGoals.Atlas.RangePlans.VarianceBudget

private theorem stencil_id (b : Int) (p v : Point) (_hv : bounded b v) :
    ((shape.stencil b p).map (fun q => q.coeff * (if memBox q v then 1 else 0))).sum =
      if shape.qualifies p v then 1 else 0 := by
  simp [shape, box, memBox, bounded] at _hv ⊢
  omega

theorem PrefixIndex_sound : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_stencil shape b p (fun v hv => stencil_id b p v hv) rs hb

end VarianceBudget

end TinylambOpt.ResearchProofs.Atlas_RangePlans_G4
