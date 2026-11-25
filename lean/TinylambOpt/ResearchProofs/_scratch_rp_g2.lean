import TinylambOpt.ResearchGoals.Atlas.RangePlans
set_option maxHeartbeats 0
set_option maxRecDepth 10000
namespace TinylambOpt.ResearchProofs._scratch_rp_g2
open TinylambOpt.ResearchGoals.Atlas.RangePlans

private def inRect (x0 x1 y0 y1 : Int) (r : Point) : Bool :=
  x0 ≤ r.x && r.x < x1 && y0 ≤ r.y && r.y < y1
private def boxIn (q : Box) (r : Point) : Bool := inRect q.x0 q.x1 q.y0 q.y1 r

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool) (g : Point × Int → Int) :
    ((rs.filter f).map g).sum = (rs.map (fun r => if f r then g r else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih]

private theorem prefixMass_cons (r : Point × Int) (x y : Int) (rs : Rows) :
    prefixMass (r :: rs) x y = (if (r.1.x < x && r.1.y < y) = true then r.2 else 0) + prefixMass rs x y := by
  unfold prefixMass
  simp only [List.filter_cons]
  by_cases h : (r.1.x < x && r.1.y < y) = true
  · rw [if_pos h]
    simp [h]
  · rw [if_neg h]
    simp [h]

private theorem prefixMass_box (rs : Rows) (x0 x1 y0 y1 : Int) :
    prefixMass rs x1 y1 - prefixMass rs x0 y1 - prefixMass rs x1 y0 + prefixMass rs x0 y0 =
    ((rs.filter (fun r => inRect x0 x1 y0 y1 r.1)).map Prod.snd).sum := by
  rw [sum_filter_eq_sum_if]
  induction rs with
  | nil => simp [prefixMass]
  | cons r rs ih =>
      rw [prefixMass_cons, prefixMass_cons, prefixMass_cons, prefixMass_cons]
      simp only [List.map_cons, List.sum_cons]
      by_cases hin : inRect x0 x1 y0 y1 r.1 = true
      · have hc : x0 ≤ r.1.x ∧ r.1.x < x1 ∧ y0 ≤ r.1.y ∧ r.1.y < y1 := by
          simpa [inRect, decide_eq_true_eq] using hin
        by_cases hx1 : r.1.x < x1 <;> by_cases hx0 : r.1.x < x0 <;>
          by_cases hy1 : r.1.y < y1 <;> by_cases hy0 : r.1.y < y0 <;>
            simp [hin, hx1, hx0, hy1, hy0] <;> omega
      · have hc : ¬ (x0 ≤ r.1.x ∧ r.1.x < x1 ∧ y0 ≤ r.1.y ∧ r.1.y < y1) := by
          simpa [inRect, decide_eq_true_eq] using hin
        by_cases hx1 : r.1.x < x1 <;> by_cases hx0 : r.1.x < x0 <;>
          by_cases hy1 : r.1.y < y1 <;> by_cases hy0 : r.1.y < y0 <;>
            simp [hin, hx1, hx0, hy1, hy0] <;> omega

private theorem filter_nil_of_forall_not {α : Type} (l : List α) (f : α → Bool)
    (h : ∀ a ∈ l, ¬ f a = true) : l.filter f = [] := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      have ha : ¬ f a = true := h a List.mem_cons_self
      have has : ∀ b ∈ as, ¬ f b = true := fun b hb => h b (List.mem_cons_of_mem a hb)
      simp [ha, ih has]

private theorem corners_box_sum (q : Box) (rs : Rows) :
    corners q (prefixMass rs) = q.coeff * ((rs.filter (fun r => boxIn q r.1)).map Prod.snd).sum := by
  unfold corners
  by_cases h : (q.x0 < q.x1 && q.y0 < q.y1) = true
  · rw [if_pos h]
    exact congrArg (fun z : Int => q.coeff * z) (prefixMass_box rs q.x0 q.x1 q.y0 q.y1)
  · rw [if_neg h]
    have hnot : ¬ (q.x0 < q.x1 ∧ q.y0 < q.y1) := by
      intro hh
      apply h
      simp only [Bool.and_eq_true, decide_eq_true_eq]
      exact hh
    have hn : (rs.filter (fun r => boxIn q r.1)) = [] := by
      apply filter_nil_of_forall_not
      intro r hr
      simp [boxIn, inRect, decide_eq_true_eq] at hr
      omega
    simp [hn]

private theorem mul_sum_if (rs : Rows) (c : Int) (f : Point × Int → Bool) :
    c * (rs.map (fun r => if f r then r.2 else 0)).sum = (rs.map (fun r => if f r then c * r.2 else 0)).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih, Int.mul_add] <;> omega

private theorem sum_map_zero {β : Type} (bs : List β) : (bs.map (fun _ => (0 : Int))).sum = 0 := by
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
      rw [ih]
      rw [← sum_map_add]

private def stencilWeight (s : Shape) (b : Int) (p : Point) (r : Point) : Int :=
  ((s.stencil b p).map (fun q => if boxIn q r then q.coeff else 0)).sum

private theorem stencilWeight_mul (st : List Box) (r : Point × Int) :
    (st.map (fun q => if boxIn q r.1 then q.coeff * r.2 else 0)).sum =
      (st.map (fun q => if boxIn q r.1 then q.coeff else 0)).sum * r.2 := by
  induction st with
  | nil => simp
  | cons q qs ih =>
      by_cases h : boxIn q r.1 = true <;> simp [h, ih, Int.add_mul] <;> omega

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
      ((s.stencil b p).map (fun q => (rs.map (fun r => if boxIn q r.1 then q.coeff * r.2 else 0)).sum)).sum := by
    induction s.stencil b p with
    | nil => rfl
    | cons q qs ih =>
        rw [List.map_cons, List.sum_cons, List.map_cons, List.sum_cons]
        rw [corners_box_sum, sum_filter_eq_sum_if, mul_sum_if, ih]
  rw [h1]
  rw [sum_swap]
  have h2 : ∀ (rs' : Rows), (∀ r ∈ rs', stencilWeight s b p r.1 = if s.qualifies p r.1 then (1 : Int) else 0) →
      (rs'.map (fun r => ((s.stencil b p).map (fun q => if boxIn q r.1 then q.coeff * r.2 else 0)).sum)).sum =
        (rs'.map (fun r => (if s.qualifies p r.1 then (1 : Int) else 0) * r.2)).sum := by
    intro rs' h'
    induction rs' with
    | nil => simp
    | cons r rs' ih =>
        simp only [List.map_cons, List.sum_cons]
        congr 1
        · rw [stencilWeight_mul]
          change stencilWeight s b p r.1 * r.2 = _
          rw [h' r List.mem_cons_self]
        · exact ih (fun r' hr' => h' r' (List.mem_cons_of_mem r hr'))
  rw [h2 rs h]
  exact if_one_mul rs (fun r => s.qualifies p r.1)

end TinylambOpt.ResearchProofs._scratch_rp_g2
