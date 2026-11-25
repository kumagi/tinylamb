import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PriorTradingDay_PageVerdict

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.PriorTradingDay

/-! ### Small-input sanity check (the goal is true) -/

private def grid (n : Nat) : List Int :=
  (List.range (2*n+1)).map (fun i : Nat => (i : Int) - (n : Int))

private def smallRows : List (Point × Int) :=
  (grid 1).flatMap (fun x => (grid 1).flatMap (fun y =>
    [-1, 0, 1].map (fun w => (⟨x, y⟩, w))))

private def pagesAt (lx ly ux uy : Int) : List Page :=
  let rs := smallRows.filter (fun r => lx ≤ r.1.x ∧ r.1.x ≤ ux ∧ ly ≤ r.1.y ∧ r.1.y ≤ uy)
  let singles : List Rows := rs.map (fun r => [r])
  let pairs : List Rows := rs.flatMap (fun r => rs.map (fun s => [r, s]))
  let choices : List Rows := ([] : Rows) :: (singles ++ pairs)
  choices.map (fun rows => ⟨⟨lx, ly⟩, ⟨ux, uy⟩, rows⟩)

private def smallPages : List Page :=
  (grid 1).flatMap (fun lx => (grid 1).flatMap (fun ly =>
    (grid 1).flatMap (fun ux => (grid 1).flatMap (fun uy =>
      pagesAt lx ly ux uy))))

private def brute (b : Int) (p : Point) (g : Page) : Bool :=
  let prem := g.rows.all (fun r => decide (g.lower.x ≤ r.1.x ∧ r.1.x ≤ g.upper.x ∧
      g.lower.y ≤ r.1.y ∧ r.1.y ≤ g.upper.y)) &&
    g.rows.all (fun r => decide (-b ≤ r.1.x ∧ r.1.x ≤ b ∧ -b ≤ r.1.y ∧ r.1.y ≤ b))
  !prem || (pageScan shape b p g == scan shape p g.rows)

#eval ([-1, 0, 1, 2].all (fun b => (grid 3).all (fun px => (grid 3).all (fun py =>
  smallPages.all (brute b ⟨px, py⟩)))))

/-! ### Shared rectangle and list algebra -/

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
  · rw [if_pos h]
    simp [h]
  · rw [if_neg h]
    simp [h]

private theorem corner_add (a b c d u v w z t : Int)
    (h : u - v - w + z = t) :
    u + a - (v + b) - (w + c) + (z + d) = a - b - c + d + t := by
  omega

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
  · rw [if_pos h]
    have hp : q.x0 < q.x1 ∧ q.y0 < q.y1 := by
      simpa only [Bool.and_eq_true, decide_eq_true_eq] using h
    exact congrArg (fun z : Int => q.coeff * z)
      (prefixMass_box rs q.x0 q.x1 q.y0 q.y1 hp.1 hp.2)
  · rw [if_neg h]
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

private theorem sum_map_mul_left {β : Type} (bs : List β) (f : β → Int) (w : Int) :
    (bs.map (fun b => w * f b)).sum = w * (bs.map f).sum := by
  induction bs with
  | nil => simp
  | cons b bs ih => simp [ih, Int.mul_add]

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

/-! ### Page verdicts -/

private theorem sum_map_mul_const (rs : Rows) (w : Int) :
    (rs.map (fun r => w * r.2)).sum = w * (rs.map Prod.snd).sum := by
  exact sum_map_mul_left rs Prod.snd w

private theorem filter_nil_of_forall_not {α : Type} (l : List α) (f : α → Bool)
    (h : ∀ a ∈ l, ¬ f a = true) : l.filter f = [] := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      have ha : ¬ f a = true := h a List.mem_cons_self
      have has : ∀ b ∈ as, ¬ f b = true :=
        fun b hb => h b (List.mem_cons_of_mem a hb)
      simp [ha, ih has]

private theorem uniform_correct (q : Box) (g : Page) (hg : valid g) (w : Int)
    (hu : uniform g q = some w) :
    w * (g.rows.map Prod.snd).sum =
      (g.rows.map (fun r => if inBox q r.1 then q.coeff * r.2 else 0)).sum := by
  unfold uniform at hu
  by_cases hguard :
      (q.x1 ≤ q.x0 || q.y1 ≤ q.y0 || g.upper.x < q.x0 || q.x1 ≤ g.lower.x ||
        g.upper.y < q.y0 || q.y1 ≤ g.lower.y) = true
  · rw [if_pos hguard] at hu
    cases hu
    have hgp : q.x1 ≤ q.x0 ∨ q.y1 ≤ q.y0 ∨ g.upper.x < q.x0 ∨
        q.x1 ≤ g.lower.x ∨ g.upper.y < q.y0 ∨ q.y1 ≤ g.lower.y := by
      simpa only [Bool.or_eq_true, decide_eq_true_eq, or_assoc] using hguard
    have hn : ∀ r ∈ g.rows, inBox q r.1 = false := by
      intro r hr
      have hb := hg r hr
      rcases hgp with h | h | h | h | h | h <;>
        simp [inBox, decide_eq_true_eq] <;> omega
    have hf : g.rows.filter (fun r => inBox q r.1) = [] := by
      apply filter_nil_of_forall_not
      intro r hr htrue
      rw [hn r hr] at htrue
      cases htrue
    change (0 : Int) * (g.rows.map Prod.snd).sum = _
    rw [Int.zero_mul, ← sum_filter_eq_sum_if g.rows (fun r => inBox q r.1)
      (fun r => q.coeff * r.2), hf]
    simp
  · rw [if_neg hguard] at hu
    by_cases hfull :
        (q.x0 ≤ g.lower.x && g.upper.x < q.x1 &&
          q.y0 ≤ g.lower.y && g.upper.y < q.y1) = true
    · rw [if_pos hfull] at hu
      cases hu
      have hfp : q.x0 ≤ g.lower.x ∧ g.upper.x < q.x1 ∧
          q.y0 ≤ g.lower.y ∧ g.upper.y < q.y1 := by
        simpa only [Bool.and_eq_true, decide_eq_true_eq, and_assoc] using hfull
      have hn : ∀ r ∈ g.rows, inBox q r.1 = true := by
        intro r hr
        have hb := hg r hr
        simp [inBox, decide_eq_true_eq] at hfp ⊢
        omega
      have hf : g.rows.filter (fun r => inBox q r.1) = g.rows :=
        List.filter_eq_self.mpr hn
      rw [← sum_filter_eq_sum_if g.rows (fun r => inBox q r.1)
        (fun r => q.coeff * r.2), hf, sum_map_mul_const]
    · rw [if_neg hfull] at hu
      cases hu

private theorem mapM_uniform_sum (qs : List Box) (g : Page) (hg : valid g) :
    match qs.mapM (uniform g) with
    | some ws => ws.sum * (g.rows.map Prod.snd).sum =
        (g.rows.map (fun r =>
          (qs.map (fun q => if inBox q r.1 then q.coeff * r.2 else 0)).sum)).sum
    | none => True := by
  induction qs with
  | nil => simp [sum_map_zero]
  | cons q qs ih =>
      simp only [List.mapM_cons, List.map_cons, List.sum_cons]
      cases hu : uniform g q with
      | none => trivial
      | some w =>
          cases hr : qs.mapM (uniform g) with
          | none => trivial
          | some ws =>
              have hq' := uniform_correct q g hg w hu
              have ht := ih
              simp only [hr] at ht
              change (w :: ws).sum * (g.rows.map Prod.snd).sum = _
              rw [List.sum_cons, Int.add_mul, hq', ht, ← sum_map_add]

private theorem pageScan_of_weight (s : Shape) (b : Int) (p : Point) (g : Page)
    (hg : valid g)
    (h : ∀ r ∈ g.rows, stencilWeight s b p r.1 = if s.qualifies p r.1 then (1 : Int) else 0) :
    pageScan s b p g = scan s p g.rows := by
  unfold pageScan
  cases hm : (s.stencil b p).mapM (uniform g) with
  | none => rfl
  | some ws =>
      have hsum := mapM_uniform_sum (s.stencil b p) g hg
      simp only [hm] at hsum ⊢
      rw [hsum]
      have hrow : (g.rows.map (fun r =>
          ((s.stencil b p).map (fun q => if inBox q r.1 then q.coeff * r.2 else 0)).sum)).sum =
          (g.rows.map (fun r => (if s.qualifies p r.1 then (1 : Int) else 0) * r.2)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro r hr
        rw [stencilWeight_mul]
        change stencilWeight s b p r.1 * r.2 = _
        rw [h r hr]
      rw [hrow]
      exact if_one_mul g.rows (fun r => s.qualifies p r.1)

private theorem pageVerdict_of_stencil (s : Shape)
    (hst : ∀ b p v, bounded b v →
      stencilWeight s b p v = if s.qualifies p v then (1 : Int) else 0) :
    ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
      pageScan s b p g = scan s p g.rows := by
  intro b p g hg hb
  exact pageScan_of_weight s b p g hg (fun r hr => hst b p r.1 (hb r hr))

/-! ### The PriorTradingDay stencil is the exact indicator -/

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, stencilWeight, inBox, bounded] at hv ⊢
  omega

theorem sound_PriorTradingDay_PageVerdict : PriorTradingDay.PageVerdict.Goal := by
  exact pageVerdict_of_stencil shape stencil_id

end TinylambOpt.ResearchProofs.Atlas.Goal_PriorTradingDay_PageVerdict
