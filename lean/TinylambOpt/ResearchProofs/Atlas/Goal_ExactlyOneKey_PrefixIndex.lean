import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `ExactlyOneKey.PrefixIndex` — `∀ b p rs, (∀ r ∈ rs, bounded b r.1) → indexed shape b
  p (prefixMass rs) = scan shape p rs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ExactlyOneKey_PrefixIndex

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.ExactlyOneKey

private def inBox (q : Box) (v : Point) : Bool :=
  decide (q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1)

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool)
    (g : Point × Int → Int) :
    ((rs.filter f).map g).sum = (rs.map (fun r => if f r then g r else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih]

private theorem prefixMass_cons (r : Point × Int) (rs : Rows) (x y : Int) :
    prefixMass (r :: rs) x y =
      (if (r.1.x < x && r.1.y < y) = true then r.2 else 0) + prefixMass rs x y := by
  unfold prefixMass
  simp only [List.filter_cons]
  by_cases h : (r.1.x < x && r.1.y < y) = true
  · rw [ite_eq_left h]
    simp [h]
  · rw [ite_eq_right h]
    simp [h]

private def cornerMass (r : Point × Int) (x y : Int) : Int :=
  if (r.1.x < x && r.1.y < y) = true then r.2 else 0

private theorem cornerMass_box (r : Point × Int) (x0 x1 y0 y1 : Int)
    (hx : x0 < x1) (hy : y0 < y1) :
    cornerMass r x1 y1 - cornerMass r x0 y1 - cornerMass r x1 y0 + cornerMass r x0 y0 =
      if inBox (box x0 x1 y0 y1) r.1 then r.2 else 0 := by
  unfold cornerMass inBox box
  simp only [Bool.and_eq_true, decide_eq_true_eq]
  by_cases hx1 : r.1.x < x1 <;> by_cases hx0 : r.1.x < x0 <;>
    by_cases hy1 : r.1.y < y1 <;> by_cases hy0 : r.1.y < y0 <;>
    by_cases hb : x0 ≤ r.1.x ∧ r.1.x < x1 ∧ y0 ≤ r.1.y ∧ r.1.y < y1 <;>
    simp [hx1, hx0, hy1, hy0, hb]
  all_goals
    first
    | omega
    | intro hx hy; omega

private theorem corner_add (a b c d u v w z t : Int)
    (h : u - v - w + z = t) :
    u + a - (v + b) - (w + c) + (z + d) = a - b - c + d + t := by
  omega

private theorem prefixMass_box_cons (r : Point × Int) (rs : Rows)
    (x0 x1 y0 y1 : Int) (hx : x0 < x1) (hy : y0 < y1) :
    prefixMass (r :: rs) x1 y1 - prefixMass (r :: rs) x0 y1 -
        prefixMass (r :: rs) x1 y0 + prefixMass (r :: rs) x0 y0 =
      (prefixMass rs x1 y1 - prefixMass rs x0 y1 -
        prefixMass rs x1 y0 + prefixMass rs x0 y0) +
        (if inBox (box x0 x1 y0 y1) r.1 then r.2 else 0) := by
  rw [prefixMass_cons, prefixMass_cons, prefixMass_cons, prefixMass_cons]
  change cornerMass r x1 y1 + prefixMass rs x1 y1 -
    (cornerMass r x0 y1 + prefixMass rs x0 y1) -
    (cornerMass r x1 y0 + prefixMass rs x1 y0) +
    (cornerMass r x0 y0 + prefixMass rs x0 y0) = _
  have hrow := cornerMass_box r x0 x1 y0 y1 hx hy
  calc
    cornerMass r x1 y1 + prefixMass rs x1 y1 -
        (cornerMass r x0 y1 + prefixMass rs x0 y1) -
        (cornerMass r x1 y0 + prefixMass rs x1 y0) +
        (cornerMass r x0 y0 + prefixMass rs x0 y0) =
      prefixMass rs x1 y1 - prefixMass rs x0 y1 -
        prefixMass rs x1 y0 + prefixMass rs x0 y0 +
        (if inBox (box x0 x1 y0 y1) r.1 then r.2 else 0) :=
      corner_add _ _ _ _ _ _ _ _ _ hrow

private theorem prefixMass_box (rs : Rows) (x0 x1 y0 y1 : Int)
    (hx : x0 < x1) (hy : y0 < y1) :
    prefixMass rs x1 y1 - prefixMass rs x0 y1 - prefixMass rs x1 y0 + prefixMass rs x0 y0 =
      ((rs.filter (fun r => inBox (box x0 x1 y0 y1) r.1)).map Prod.snd).sum := by
  rw [sum_filter_eq_sum_if]
  induction rs with
  | nil => simp [prefixMass]
  | cons r rs ih =>
      rw [prefixMass_box_cons r rs x0 x1 y0 y1 hx hy, ih]
      simp only [List.map_cons, List.sum_cons]
      omega

private theorem corners_box_sum (q : Box) (rs : Rows) :
    corners q (prefixMass rs) =
      q.coeff * ((rs.filter (fun r => inBox q r.1)).map Prod.snd).sum := by
  unfold corners
  by_cases h : (q.x0 < q.x1 && q.y0 < q.y1) = true
  · rw [ite_eq_left h]
    have hp : q.x0 < q.x1 ∧ q.y0 < q.y1 := by
      simpa only [Bool.and_eq_true, decide_eq_true_eq] using h
    exact congrArg (fun z : Int => q.coeff * z)
      (prefixMass_box rs q.x0 q.x1 q.y0 q.y1 hp.1 hp.2)
  · rw [ite_eq_right h]
    have hnot : ¬ (q.x0 < q.x1 ∧ q.y0 < q.y1) := by
      intro hh
      apply h
      simp only [Bool.and_eq_true, decide_eq_true_eq]
      exact hh
    have hn : (rs.filter (fun r => inBox q r.1)) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro r hr
      simp [inBox, decide_eq_true_eq]
      omega
    simp [hn]

private theorem sum_map_zero {β : Type} (bs : List β) :
    (bs.map (fun _ => (0 : Int))).sum = 0 := by
  induction bs with
  | nil => rfl
  | cons b bs ih => simp [ih]

private theorem sum_map_add {β : Type} (bs : List β) (f g : β → Int) :
    (bs.map (fun b => f b + g b)).sum = (bs.map f).sum + (bs.map g).sum := by
  induction bs with
  | nil => simp
  | cons b bs ih => simp [ih]; omega

private theorem sum_swap {α β : Type} (as : List α) (bs : List β) (f : α → β → Int) :
    (as.map (fun a => (bs.map (fun b => f a b)).sum)).sum =
      (bs.map (fun b => (as.map (fun a => f a b)).sum)).sum := by
  induction as with
  | nil => simp [sum_map_zero]
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih, ← sum_map_add]

private def stencilWeight (s : Shape) (b : Int) (p v : Point) : Int :=
  ((s.stencil b p).map (fun q => if inBox q v then q.coeff else 0)).sum

private theorem stencilWeight_mul (st : List Box) (r : Point × Int) :
    (st.map (fun q => if inBox q r.1 then q.coeff * r.2 else 0)).sum =
      (st.map (fun q => if inBox q r.1 then q.coeff else 0)).sum * r.2 := by
  induction st with
  | nil => simp
  | cons q qs ih =>
      by_cases h : inBox q r.1 = true <;> simp [h, ih, Int.add_mul] <;> omega

private theorem mul_sum_if (rs : Rows) (c : Int) (f : Point × Int → Bool) :
    c * (rs.map (fun r => if f r then r.2 else 0)).sum =
      (rs.map (fun r => if f r then c * r.2 else 0)).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih, Int.mul_add] <;> omega

private theorem if_one_mul (rs : Rows) (f : Point × Int → Bool) :
    (rs.map (fun r => (if f r then (1 : Int) else 0) * r.2)).sum =
      ((rs.filter f).map Prod.snd).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih] <;> omega

private theorem prefixIndex_of_weight (s : Shape) (b : Int) (p : Point) (rs : Rows)
    (h : ∀ r ∈ rs, stencilWeight s b p r.1 = if s.qualifies p r.1 then (1 : Int) else 0) :
    indexed s b p (prefixMass rs) = scan s p rs := by
  unfold indexed scan
  have h1 : ((s.stencil b p).map (corners · (prefixMass rs))).sum =
      ((s.stencil b p).map
        (fun q => (rs.map (fun r => if inBox q r.1 then q.coeff * r.2 else 0)).sum)).sum := by
    induction s.stencil b p with
    | nil => rfl
    | cons q qs ih =>
        rw [List.map_cons, List.sum_cons, List.map_cons, List.sum_cons]
        rw [corners_box_sum, sum_filter_eq_sum_if, mul_sum_if, ih]
  rw [h1, sum_swap]
  have h2 : ∀ (rs' : Rows),
      (∀ r ∈ rs', stencilWeight s b p r.1 = if s.qualifies p r.1 then (1 : Int) else 0) →
      (rs'.map (fun r =>
        ((s.stencil b p).map (fun q => if inBox q r.1 then q.coeff * r.2 else 0)).sum)).sum =
      (rs'.map (fun r => (if s.qualifies p r.1 then (1 : Int) else 0) * r.2)).sum := by
    intro rs' h'
    induction rs' with
    | nil => simp
    | cons r rs' ih =>
        simp only [List.map_cons, List.sum_cons]
        have hhead :
            ((s.stencil b p).map
              (fun q => if inBox q r.1 then q.coeff * r.2 else 0)).sum =
            (if s.qualifies p r.1 then (1 : Int) else 0) * r.2 := by
          rw [stencilWeight_mul]
          change stencilWeight s b p r.1 * r.2 = _
          rw [h' r List.mem_cons_self]
        rw [hhead, ih (fun r' hr' => h' r' (List.mem_cons_of_mem r hr'))]
  rw [h2 rs h]
  exact if_one_mul rs (fun r => s.qualifies p r.1)

/-- On a bounded row the three signed rectangles count the two key matches
    and subtract twice the full-key match, i.e. exactly the XOR predicate. -/
private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, stencilWeight, inBox, bounded] at hv ⊢
  by_cases hx : v.x = p.x <;> by_cases hy : v.y = p.y
  all_goals
    have hxb : (v.x == p.x) = decide (v.x = p.x) := by rfl
    have hyb : (v.y == p.y) = decide (v.y = p.y) := by rfl
    simp only [hxb, hyb, hx, hy, ite_true]
    repeat' split <;> simp_all <;> omega

theorem sound_ExactlyOneKey_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_weight shape b p rs (fun r hr => stencil_id b p r.1 (hb r hr))

end TinylambOpt.ResearchProofs.Atlas.Goal_ExactlyOneKey_PrefixIndex
