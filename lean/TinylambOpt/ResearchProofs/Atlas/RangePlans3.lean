import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.RangePlans3

open TinylambOpt.ResearchGoals.Atlas.RangePlans

/-! ### Shared rectangle and list algebra -/

private def inBox (q : Box) (v : Point) : Bool :=
  decide (q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1)

private theorem point_beq_eq (a b : Point) :
    (a == b) = decide (a.x = b.x ∧ a.y = b.y) := by
  cases a; cases b; rfl

private theorem point_beq_self (a : Point) : (a == a) = true := by
  rw [point_beq_eq]; simp

private theorem point_beq_true_iff (a b : Point) : (a == b) = true ↔ a = b := by
  rw [point_beq_eq, decide_eq_true_eq]
  cases a; cases b
  simp only [Point.mk.injEq]

private theorem point_beq_false {a b : Point} (h : a ≠ b) : (a == b) = false := by
  apply Bool.eq_false_of_ne_true
  intro hb
  exact h (point_beq_true_iff a b |>.mp hb)

private theorem beq_false_of_ne {a b : Int} (h : a ≠ b) : (a == b) = false := by
  apply Bool.eq_false_of_ne_true
  intro hb
  exact h (eq_of_beq hb)

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

private theorem prefixMass_append (es₁ es₂ : Rows) (x y : Int) :
    prefixMass (es₁ ++ es₂) x y = prefixMass es₁ x y + prefixMass es₂ x y := by
  induction es₁ with
  | nil => simp [prefixMass]
  | cons e es ih =>
      rw [List.cons_append, prefixMass_cons, prefixMass_cons, ih]
      omega

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

private theorem sum_map_sub {β : Type} (bs : List β) (f g : β → Int) :
    (bs.map (fun b => f b - g b)).sum = (bs.map f).sum - (bs.map g).sum := by
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

/-! ### Prefix indices -/

namespace ExactlyOneKeyProofs

open TinylambOpt.ResearchGoals.Atlas.RangePlans.ExactlyOneKey

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, stencilWeight, inBox, bounded] at hv ⊢
  by_cases hx : v.x = p.x <;> by_cases hy : v.y = p.y
  all_goals
    have hxb : (v.x == p.x) = decide (v.x = p.x) := by rfl
    have hyb : (v.y == p.y) = decide (v.y = p.y) := by rfl
    simp only [hxb, hyb, hx, hy, if_false, if_true]
    repeat' split <;> simp_all <;> omega

theorem sound_ExactlyOneKey_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_weight shape b p rs (fun r hr => stencil_id b p r.1 (hb r hr))

end ExactlyOneKeyProofs

namespace PriorTradingDayProofs

open TinylambOpt.ResearchGoals.Atlas.RangePlans.PriorTradingDay

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, stencilWeight, inBox, bounded] at hv ⊢
  omega

theorem sound_PriorTradingDay_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_weight shape b p rs (fun r hr => stencil_id b p r.1 (hb r hr))

end PriorTradingDayProofs

namespace LexSuccessorProofs

open TinylambOpt.ResearchGoals.Atlas.RangePlans.LexSuccessor

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, stencilWeight, inBox, bounded] at hv ⊢
  omega

theorem sound_LexSuccessor_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_weight shape b p rs (fun r hr => stencil_id b p r.1 (hb r hr))

end LexSuccessorProofs

namespace EitherEndpointInsideProofs

open TinylambOpt.ResearchGoals.Atlas.RangePlans.EitherEndpointInside

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, stencilWeight, inBox, bounded] at hv ⊢
  omega

theorem sound_EitherEndpointInside_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_weight shape b p rs (fun r hr => stencil_id b p r.1 (hb r hr))

end EitherEndpointInsideProofs

namespace CrossChannelProofs

open TinylambOpt.ResearchGoals.Atlas.RangePlans.CrossChannel

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, stencilWeight, inBox, bounded] at hv ⊢
  omega

theorem sound_CrossChannel_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_weight shape b p rs (fun r hr => stencil_id b p r.1 (hb r hr))

end CrossChannelProofs

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

theorem sound_ExactlyOneKey_PageVerdict : ExactlyOneKey.PageVerdict.Goal := by
  exact pageVerdict_of_stencil ExactlyOneKey.shape ExactlyOneKeyProofs.stencil_id

theorem sound_PriorTradingDay_PageVerdict : PriorTradingDay.PageVerdict.Goal := by
  exact pageVerdict_of_stencil PriorTradingDay.shape PriorTradingDayProofs.stencil_id

theorem sound_LexSuccessor_PageVerdict : LexSuccessor.PageVerdict.Goal := by
  exact pageVerdict_of_stencil LexSuccessor.shape LexSuccessorProofs.stencil_id

theorem sound_EitherEndpointInside_PageVerdict : EitherEndpointInside.PageVerdict.Goal := by
  exact pageVerdict_of_stencil EitherEndpointInside.shape EitherEndpointInsideProofs.stencil_id

theorem sound_CrossChannel_PageVerdict : CrossChannel.PageVerdict.Goal := by
  exact pageVerdict_of_stencil CrossChannel.shape CrossChannelProofs.stencil_id

/-! ### Boundary events, coverage, and transposition -/

private theorem atPoint_append (es₁ es₂ : Rows) (r : Point) :
    atPoint (es₁ ++ es₂) r = atPoint es₁ r + atPoint es₂ r := by
  simp [atPoint, prefixMass_append]

private theorem atPoint_events (q : Box) (w : Int) (r : Point) :
    atPoint (events q w) r = if inBox q r then w * q.coeff else 0 := by
  unfold events
  by_cases hb : (q.x0 < q.x1 && q.y0 < q.y1) = true
  · simp [hb, atPoint, prefixMass]
    by_cases hx0 : q.x0 < r.x + 1 <;> by_cases hx1 : q.x1 < r.x + 1 <;>
      by_cases hy0 : q.y0 < r.y + 1 <;> by_cases hy1 : q.y1 < r.y + 1 <;>
      by_cases hxl : q.x0 ≤ r.x <;> by_cases hyl : q.y0 ≤ r.y <;>
      simp [inBox, decide_eq_true_eq, decide_eq_false_iff_not, hx0, hx1, hy0, hy1, hxl, hyl] <;> grind
  · have hb' : (q.x0 < q.x1 && q.y0 < q.y1) = false :=
      Bool.eq_false_of_ne_true hb
    simp [hb', atPoint, prefixMass]
    have hn : inBox q r = false := by
      unfold inBox
      apply Bool.eq_false_of_ne_true
      intro hbin
      simp only [decide_eq_true_eq] at hbin
      rcases hbin with ⟨hxlo, hxhi, hylo, hyhi⟩
      have : q.x0 < q.x1 ∧ q.y0 < q.y1 := by omega
      exact hb (by simp [this])
    simp [hn]

private theorem atPoint_flatMap_events (ss : List Box) (w : Int) (r : Point) :
    atPoint (ss.flatMap (events · w)) r =
      (ss.map (fun q => if inBox q r then w * q.coeff else 0)).sum := by
  induction ss with
  | nil => simp [atPoint, prefixMass]
  | cons q qs ih =>
      simp only [List.flatMap_cons]
      rw [atPoint_append, atPoint_events, ih]
      simp

private theorem compile_cons (s : Shape) (b : Int) (p : Point) (w : Int)
    (ps : Rows) :
    compile s b ((p, w) :: ps) =
      (s.stencil b p).flatMap (events · w) ++ compile s b ps := by
  simp [compile]

private theorem stencilWeight_scale_list (ss : List Box) (v : Point) (w : Int) :
    (ss.map (fun q => if inBox q v then w * q.coeff else 0)).sum =
      w * (ss.map (fun q => if inBox q v then q.coeff else 0)).sum := by
  induction ss with
  | nil => simp
  | cons q qs ih =>
      by_cases h : inBox q v = true
      · simp only [List.map_cons, List.sum_cons, h, if_true, ih, Int.mul_add]
        try ac_rfl
      · simp [h, ih]

private theorem stencilWeight_scale (s : Shape) (b : Int) (p v : Point) (w : Int) :
    ((s.stencil b p).map (fun q => if inBox q v then w * q.coeff else 0)).sum =
      w * stencilWeight s b p v := by
  exact stencilWeight_scale_list (s.stencil b p) v w

private theorem atPoint_compile (s : Shape) (b : Int) (ps : Rows) (r : Point) :
    atPoint (compile s b ps) r =
      (ps.map (fun (q, w) => w * stencilWeight s b q r)).sum := by
  induction ps with
  | nil => simp [compile, atPoint, prefixMass]
  | cons pw ps ih =>
      rcases pw with ⟨p, w⟩
      rw [compile_cons, atPoint_append, atPoint_flatMap_events, ih]
      rw [stencilWeight_scale]
      simp

private theorem sum_indicator_eq_length {α : Type} (xs : List α) (f : α → Bool) :
    ((xs.map (fun x => if f x then (1 : Int) else 0)).sum : Int) =
      ((xs.filter f).length : Nat) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases h : f x = true <;> simp [h, ih] <;> omega

private theorem sum_indicator_any {α : Type} (xs : List α) (f : α → Bool) :
    ((xs.map (fun x => if f x then (1 : Int) else 0)).sum = 0) ↔ xs.any f = false := by
  rw [sum_indicator_eq_length, Int.ofNat_eq_zero, List.length_eq_zero_iff,
    List.filter_eq_nil_iff]
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases h : f x = true
      · simp [h]
      · simp [h, ih]

private theorem unionAnti_of_stencil (s : Shape)
    (hst : ∀ b p v, bounded b v →
      stencilWeight s b p v = if s.qualifies p v then (1 : Int) else 0) :
    ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
      let es := compile s b (ps.map (fun p => (p, 1)))
      rs.filter (fun r => !covered s ps r.1) = rs.filter (fun r => atPoint es r.1 == 0) := by
  intro b ps rs hb
  apply List.filter_congr
  intro r hr
  have hbr := hb r hr
  have hat : atPoint (compile s b (ps.map (fun p => (p, 1)))) r.1 =
      (ps.map (fun p => if s.qualifies p r.1 then (1 : Int) else 0)).sum := by
    rw [atPoint_compile, List.map_map]
    change (ps.map (fun p => (1 : Int) * stencilWeight s b p r.1)).sum = _
    apply congrArg List.sum
    apply List.map_congr_left
    intro p hp
    rw [hst b p r.1 hbr]
    simp
  have hzero : atPoint (compile s b (ps.map (fun p => (p, 1)))) r.1 = 0 ↔
      ps.any (fun p => s.qualifies p r.1) = false := by
    rw [hat]
    exact sum_indicator_any ps (fun p => s.qualifies p r.1)
  cases hc : covered s ps r.1
  · have hz : atPoint (compile s b (ps.map (fun p => (p, 1)))) r.1 = 0 :=
      hzero.mpr hc
    simp [hc, hz]
  · have hnz : atPoint (compile s b (ps.map (fun p => (p, 1)))) r.1 ≠ 0 := by
      intro hz
      have hf := hzero.mp hz
      simp only [covered] at hc
      rw [hc] at hf
      cases hf
    simp [hc, hnz]

theorem sound_ExactlyOneKey_UnionAnti : ExactlyOneKey.UnionAnti.Goal := by
  exact unionAnti_of_stencil ExactlyOneKey.shape ExactlyOneKeyProofs.stencil_id

theorem sound_PriorTradingDay_UnionAnti : PriorTradingDay.UnionAnti.Goal := by
  exact unionAnti_of_stencil PriorTradingDay.shape PriorTradingDayProofs.stencil_id

theorem sound_LexSuccessor_UnionAnti : LexSuccessor.UnionAnti.Goal := by
  exact unionAnti_of_stencil LexSuccessor.shape LexSuccessorProofs.stencil_id

theorem sound_EitherEndpointInside_UnionAnti : EitherEndpointInside.UnionAnti.Goal := by
  exact unionAnti_of_stencil EitherEndpointInside.shape EitherEndpointInsideProofs.stencil_id

theorem sound_CrossChannel_UnionAnti : CrossChannel.UnionAnti.Goal := by
  exact unionAnti_of_stencil CrossChannel.shape CrossChannelProofs.stencil_id

private theorem scan_eq_sum_indicators (s : Shape) (p : Point) (rs : Rows) :
    scan s p rs = (rs.map (fun r => r.2 * (if s.qualifies p r.1 then (1 : Int) else 0))).sum := by
  unfold scan
  rw [sum_filter_eq_sum_if]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  by_cases h : s.qualifies p r.1 = true <;> simp [h]

private theorem probeTransposition_of_stencil (s : Shape)
    (hst : ∀ b p v, bounded b v →
      stencilWeight s b p v = if s.qualifies p v then (1 : Int) else 0) :
    ∀ b (ps rs : Rows), (∀ r ∈ rs, bounded b r.1) →
      (ps.map (fun (p, w) => w * scan s p rs)).sum =
        (rs.map (fun (r, w) => w * atPoint (compile s b ps) r)).sum := by
  intro b ps rs hb
  induction ps with
  | nil =>
      simp [compile, atPoint, prefixMass, Int.mul_zero]
      exact (sum_map_zero rs).symm
  | cons pr ps ih =>
      rcases pr with ⟨p, w⟩
      have hsplit : ∀ (r : Point), atPoint (compile s b ((p, w) :: ps)) r =
          atPoint (compile s b [(p, w)]) r + atPoint (compile s b ps) r := by
        intro r
        rw [compile_cons, atPoint_append]
        congr 1 <;> simp [compile]
      change w * scan s p rs + (ps.map (fun (p, w) => w * scan s p rs)).sum =
        (rs.map (fun (r, v) => v * atPoint (compile s b ((p, w) :: ps)) r)).sum
      have hm : rs.map (fun (r, v) => v * atPoint (compile s b ((p, w) :: ps)) r) =
          rs.map (fun (r, v) => v * atPoint (compile s b [(p, w)]) r +
            v * atPoint (compile s b ps) r) := by
        apply List.map_congr_left
        intro rr hrr
        rcases rr with ⟨r, v⟩
        simp only [hsplit r, Int.mul_add]
      rw [hm, sum_map_add]
      rw [ih]
      have hone : w * scan s p rs =
          (rs.map (fun (r, v) => v * atPoint (compile s b [(p, w)]) r)).sum := by
        have hat : ∀ r ∈ rs, atPoint (compile s b [(p, w)]) r.1 =
            w * (if s.qualifies p r.1 then (1 : Int) else 0) := by
          intro r hr
          have hrp := hb r hr
          rw [atPoint_compile]
          simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
          rw [hst b p r.1 hrp]
        have hat2 : rs.map (fun (r, v) => v * atPoint (compile s b [(p, w)]) r) =
            rs.map (fun (r, v) => v * (w * (if s.qualifies p r then (1 : Int) else 0))) := by
          apply List.map_congr_left
          intro rr hrr
          have hrp := hb rr hrr
          rcases rr with ⟨r, v⟩
          change v * atPoint (compile s b [(p, w)]) r = v * (w * (if s.qualifies p r then (1 : Int) else 0))
          rw [atPoint_compile]
          simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
          rw [hst b p r hrp]
        rw [hat2]
        have hcomm : rs.map (fun (r, v) => v * (w * (if s.qualifies p r then (1 : Int) else 0))) =
            rs.map (fun row => w * (row.2 * (if s.qualifies p row.1 then (1 : Int) else 0))) := by
          apply List.map_congr_left
          intro rr hrr
          rcases rr with ⟨r, v⟩
          ac_rfl
        rw [hcomm]
        rw [scan_eq_sum_indicators]
        exact (sum_map_mul_left rs (fun r => r.2 * (if s.qualifies p r.1 then (1 : Int) else 0)) w).symm
      rw [hone]

theorem sound_CrossChannel_ProbeTransposition : CrossChannel.ProbeTransposition.Goal := by
  exact probeTransposition_of_stencil CrossChannel.shape CrossChannelProofs.stencil_id

/-! ### Model-specific grouping identities -/

private theorem sum_filters_swap {α : Type} (ds : List α) (rs : Rows)
    (f : α → Point × Int → Bool) :
    (ds.map (fun d => ((rs.filter (fun r => f d r)).map Prod.snd).sum)).sum =
      (rs.map (fun r => (ds.map (fun d => if f d r then r.2 else 0)).sum)).sum := by
  calc
    (ds.map (fun d => ((rs.filter (fun r => f d r)).map Prod.snd).sum)).sum =
        (ds.map (fun d =>
          (rs.map (fun r => if f d r then r.2 else 0)).sum)).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro d hd
      exact sum_filter_eq_sum_if rs (fun r => f d r) Prod.snd
    _ = _ := sum_swap ds rs (fun d r => if f d r then r.2 else 0)

theorem sound_ExactlyOneKey_SubtractiveGrouping : ExactlyOneKey.SubtractiveGrouping.Goal := by
  intro p rs
  rw [scan_eq_sum_indicators]
  rw [sum_filter_eq_sum_if rs (fun r => r.1.x == p.x) Prod.snd,
    sum_filter_eq_sum_if rs (fun r => r.1.y == p.y) Prod.snd,
    sum_filter_eq_sum_if rs (fun r => r.1 == p) Prod.snd,
    ← sum_map_mul_left rs (fun r => if r.1 == p then r.2 else 0) 2,
    ← sum_map_add, ← sum_map_sub]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  by_cases hx : r.1.x = p.x
  · by_cases hy : r.1.y = p.y
    · have hp : r.1 = p := by
        have h1 : r.1 = ⟨r.1.x, r.1.y⟩ := rfl
        have h2 : p = ⟨p.x, p.y⟩ := rfl
        rw [h1, h2]
        exact (Point.mk.injEq r.1.x r.1.y p.x p.y).mpr ⟨hx, hy⟩
      have hpp : (p == p) = true := point_beq_self p
      simp [ExactlyOneKey.shape, hp, hpp]
      omega
    · have hp : r.1 ≠ p := by
        intro heq
        exact hy (congrArg Point.y heq)
      have hpb : (r.1 == p) = false := point_beq_false hp
      have hyb : (r.1.y == p.y) = false := beq_false_of_ne hy
      simp [ExactlyOneKey.shape, hx, hpb, hyb]
  · by_cases hy : r.1.y = p.y
    · have hp : r.1 ≠ p := by
        intro heq
        exact hx (congrArg Point.x heq)
      have hpb : (r.1 == p) = false := point_beq_false hp
      have hxb : (r.1.x == p.x) = false := beq_false_of_ne hx
      simp [ExactlyOneKey.shape, hy, hpb, hxb]
    · have hp : r.1 ≠ p := by
        intro heq
        exact hx (congrArg Point.x heq)
      have hpb : (r.1 == p) = false := point_beq_false hp
      have hxb : (r.1.x == p.x) = false := beq_false_of_ne hx
      have hyb : (r.1.y == p.y) = false := beq_false_of_ne hy
      simp [ExactlyOneKey.shape, hpb, hxb, hyb]

theorem sound_PriorTradingDay_DailyState : PriorTradingDay.DailyState.Goal := by
  intro p rs
  rw [scan_eq_sum_indicators]
  rw [sum_filters_swap [1, 2, 3] rs (fun d r => r.1.x == p.x - d),
    sum_filter_eq_sum_if rs (fun r => r.1.x == p.x && r.1.y < p.y) Prod.snd,
    ← sum_map_add]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
  by_cases h1 : r.1.x = p.x - 1 <;> by_cases h2 : r.1.x = p.x - 2 <;>
    by_cases h3 : r.1.x = p.x - 3 <;> by_cases hsx : r.1.x = p.x <;>
    by_cases hsy : r.1.y < p.y <;> by_cases hlo : p.x - 3 ≤ r.1.x <;>
    by_cases hhi : r.1.x < p.x
  all_goals
    have h1b : (r.1.x == p.x - 1) = decide (r.1.x = p.x - 1) := rfl
    have h2b : (r.1.x == p.x - 2) = decide (r.1.x = p.x - 2) := rfl
    have h3b : (r.1.x == p.x - 3) = decide (r.1.x = p.x - 3) := rfl
    have hsxb : (r.1.x == p.x) = decide (r.1.x = p.x) := rfl
    rw [h1b, h2b, h3b, hsxb]
    all_goals first
      | rw [decide_eq_true h1]
      | rw [decide_eq_false h1]
    all_goals first
      | rw [decide_eq_true h2]
      | rw [decide_eq_false h2]
    all_goals first
      | rw [decide_eq_true h3]
      | rw [decide_eq_false h3]
    all_goals first
      | rw [decide_eq_true hsx]
      | rw [decide_eq_false hsx]
    all_goals simp only [PriorTradingDay.shape, hsxb]
    all_goals try rw [decide_eq_true hsx]
    all_goals try rw [decide_eq_false hsx]
    simp only [hsy, hlo, hhi, Bool.and_eq_true, decide_eq_true_eq,
      Bool.false_eq_true, false_and, and_false, and_true, if_false, if_true] <;> grind

theorem sound_EitherEndpointInside_SplitBoundaryDebt :
    EitherEndpointInside.SplitBoundaryDebt.Goal := by
  intro p m rs hxm hmyBound
  rw [scan_eq_sum_indicators, scan_eq_sum_indicators, scan_eq_sum_indicators,
    sum_filter_eq_sum_if rs (fun r =>
      EitherEndpointInside.shape.qualifies ⟨p.x, m⟩ r.1 &&
      EitherEndpointInside.shape.qualifies ⟨m, p.y⟩ r.1) Prod.snd,
    ← sum_map_add, ← sum_map_sub]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  by_cases ha : p.x ≤ r.1.x <;> by_cases hb : r.1.x < m <;>
    by_cases hc : m ≤ r.1.x <;> by_cases hd : r.1.x < p.y <;>
    by_cases he : p.x ≤ r.1.y <;> by_cases hf : r.1.y < m <;>
    by_cases hg : m ≤ r.1.y <;> by_cases hh : r.1.y < p.y <;>
    simp [EitherEndpointInside.shape, ha, hb, hc, hd, he, hf, hg, hh] <;> omega

end TinylambOpt.ResearchProofs.Atlas.RangePlans3
