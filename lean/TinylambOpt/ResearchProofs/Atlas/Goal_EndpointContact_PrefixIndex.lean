import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_EndpointContact_PrefixIndex

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.EndpointContact

private def boxIn (q : Box) (v : Point) : Bool :=
  decide (q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1)

private def stencilWeight (s : Shape) (b : Int) (p v : Point) : Int :=
  ((s.stencil b p).map (fun q => if boxIn q v then q.coeff else 0)).sum

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

private theorem filter_nil_of_forall_not {α : Type} (l : List α) (f : α → Bool)
    (h : ∀ a ∈ l, ¬ f a = true) : l.filter f = [] := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      have ha : ¬ f a = true := h a List.mem_cons_self
      have has : ∀ b ∈ as, ¬ f b = true := fun b hb => h b (List.mem_cons_of_mem a hb)
      simp [ha, ih has]

private theorem corners_box_sum (q : Box) (rs : Rows) :
    corners q (prefixMass rs) =
      q.coeff * ((rs.filter (fun r => boxIn q r.1)).map Prod.snd).sum := by
  unfold corners
  by_cases h : (q.x0 < q.x1 && q.y0 < q.y1) = true
  · rw [ite_eq_left h]
    exact congrArg (fun z : Int => q.coeff * z)
      (prefixMass_box rs q.x0 q.x1 q.y0 q.y1
        (by
          have hh : q.x0 < q.x1 ∧ q.y0 < q.y1 := by
            simpa [Bool.and_eq_true, decide_eq_true_eq] using h
          exact hh.1)
        (by
          have hh : q.x0 < q.x1 ∧ q.y0 < q.y1 := by
            simpa [Bool.and_eq_true, decide_eq_true_eq] using h
          exact hh.2))
  · rw [ite_eq_right h]
    have hnot : ¬ (q.x0 < q.x1 ∧ q.y0 < q.y1) := by
      intro hh
      apply h
      simpa using hh
    have hn : (rs.filter (fun r => boxIn q r.1)) = [] := by
      apply filter_nil_of_forall_not
      intro r _
      simp [boxIn, decide_eq_true_eq]
      omega
    simp [hn]

private theorem mul_sum_if (rs : Rows) (c : Int) (f : Point × Int → Bool) :
    c * (rs.map (fun r => if f r then r.2 else 0)).sum =
      (rs.map (fun r => if f r then c * r.2 else 0)).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih, Int.mul_add]

private theorem sum_map_zero {β : Type} (bs : List β) :
    (bs.map (fun _ => (0 : Int))).sum = 0 := by
  induction bs with
  | nil => rfl
  | cons b bs ih => simp [ih]

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih] <;> omega

private theorem sum_congr {α : Type} (xs : List α) (f g : α → Int)
    (h : ∀ x ∈ xs, f x = g x) : (xs.map f).sum = (xs.map g).sum :=
  congrArg List.sum (List.map_congr_left h)

private theorem sum_swap {α β : Type} (as : List α) (bs : List β) (f : α → β → Int) :
    (as.map (fun a => (bs.map (fun b => f a b)).sum)).sum =
      (bs.map (fun b => (as.map (fun a => f a b)).sum)).sum := by
  induction as with
  | nil => simp [sum_map_zero]
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih, ← sum_map_add]

private theorem indexed_eq_sum_weight (s : Shape) (b : Int) (p : Point) (rs : Rows) :
    indexed s b p (prefixMass rs) =
      (rs.map (fun r => stencilWeight s b p r.1 * r.2)).sum := by
  unfold indexed
  have h1 : ((s.stencil b p).map (corners · (prefixMass rs))).sum =
      ((s.stencil b p).map
        (fun q => (rs.map (fun r => if boxIn q r.1 then q.coeff * r.2 else 0)).sum)).sum := by
    induction s.stencil b p with
    | nil => rfl
    | cons q qs ih =>
      rw [List.map_cons, List.sum_cons, List.map_cons, List.sum_cons]
      rw [corners_box_sum, sum_filter_eq_sum_if, mul_sum_if, ih]
  rw [h1, sum_swap]
  apply sum_congr
  intro r _
  unfold stencilWeight
  induction s.stencil b p with
  | nil => simp
  | cons q qs ih =>
      by_cases h : boxIn q r.1 = true <;> simp [h, ih, Int.add_mul]

private theorem prefixIndex_of_weight (s : Shape) (b : Int) (p : Point) (rs : Rows)
    (h : ∀ r ∈ rs, stencilWeight s b p r.1 = if s.qualifies p r.1 then (1 : Int) else 0) :
    indexed s b p (prefixMass rs) = scan s p rs := by
  rw [indexed_eq_sum_weight]
  have hmap : rs.map (fun r => stencilWeight s b p r.1 * r.2) =
      rs.map (fun r => if s.qualifies p r.1 then r.2 else 0) := by
    apply List.map_congr_left
    intro r hr
    rw [h r hr]
    by_cases hc : s.qualifies p r.1 <;> simp [hc]
  rw [hmap]
  exact (sum_filter_eq_sum_if rs (fun r => s.qualifies p r.1)).symm

/-- 縦線・横線・その交点の重複引き。有界な行では符号付きステンシルの重みが
    `qualifies` の 0/1 と一致する。 -/
private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, boxIn, stencilWeight, bounded] at hv ⊢
  omega

theorem sound_EndpointContact_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_weight shape b p rs (fun r hr => stencil_id b p r.1 (hb r hr))

end TinylambOpt.ResearchProofs.Atlas.Goal_EndpointContact_PrefixIndex
