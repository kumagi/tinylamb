import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.RangePlans2

open TinylambOpt.ResearchGoals.Atlas.RangePlans

/-! ### Shared range-plan algebra -/

private def inBox (q : Box) (r : Point) : Bool :=
  q.x0 ≤ r.x && r.x < q.x1 && q.y0 ≤ r.y && r.y < q.y1

private theorem inBox_iff (q : Box) (r : Point) :
    inBox q r = true ↔ q.x0 ≤ r.x ∧ r.x < q.x1 ∧ q.y0 ≤ r.y ∧ r.y < q.y1 := by
  unfold inBox
  simp only [Bool.and_eq_true]
  constructor
  · intro h
    exact ⟨of_decide_eq_true h.1.1.1, of_decide_eq_true h.1.1.2,
      of_decide_eq_true h.1.2, of_decide_eq_true h.2⟩
  · intro h
    exact And.intro (And.intro (And.intro (decide_eq_true h.1) (decide_eq_true h.2.1))
      (decide_eq_true h.2.2.1)) (decide_eq_true h.2.2.2)

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool) (g : Point × Int → Int) :
    ((rs.filter f).map g).sum = (rs.map (fun r => if f r then g r else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih]

private theorem sum_map_add {β : Type} (bs : List β) (f g : β → Int) :
    (bs.map (fun b => f b + g b)).sum = (bs.map f).sum + (bs.map g).sum := by
  induction bs with
  | nil => simp
  | cons b bs ih => simp [ih]; omega

private theorem sum_map_add_nat {β : Type} (bs : List β) (f g : β → Nat) :
    (bs.map (fun b => f b + g b)).sum = (bs.map f).sum + (bs.map g).sum := by
  induction bs with
  | nil => simp
  | cons b bs ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_map_sub {β : Type} (bs : List β) (f g : β → Int) :
    (bs.map (fun b => f b - g b)).sum = (bs.map f).sum - (bs.map g).sum := by
  induction bs with
  | nil => simp
  | cons b bs ih => simp [ih]; omega

private theorem sum_map_zero {β : Type} (bs : List β) : (bs.map (fun _ => (0 : Int))).sum = 0 := by
  induction bs with
  | nil => rfl
  | cons b bs ih => simp [ih]

private theorem sum_swap {α β : Type} (as : List α) (bs : List β) (f : α → β → Int) :
    (as.map (fun a => (bs.map (fun b => f a b)).sum)).sum =
      (bs.map (fun b => (as.map (fun a => f a b)).sum)).sum := by
  induction as with
  | nil => exact (sum_map_zero bs).symm
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih, ← sum_map_add]

private theorem sum_map_const_int {β : Type} (bs : List β) (c : Int) :
    (bs.map (fun _ => c)).sum = (bs.length : Int) * c := by
  induction bs with
  | nil => simp
  | cons b bs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons, ih]
      change c + (bs.length : Int) * c = ((bs.length : Int) + 1) * c
      rw [Int.add_mul]
      omega

private theorem sum_indicator_count {α : Type} (xs : List α) (f : α → Bool) :
    (xs.map (fun a => if f a then (1 : Int) else 0)).sum = (xs.countP f : Int) := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
      simp only [List.map_cons, List.sum_cons, List.countP_cons, ih]
      by_cases h : f a = true <;> simp [h] <;> omega

private theorem sum_indicator_count_nat {α : Type} (xs : List α) (f : α → Bool) :
    (xs.map (fun a => if f a then 1 else 0)).sum = xs.countP f := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
      simp only [List.map_cons, List.sum_cons, List.countP_cons, ih]
      by_cases h : f a = true <;> simp [h] <;> omega

private theorem prefixMass_cons (r : Point × Int) (x y : Int) (rs : Rows) :
    prefixMass (r :: rs) x y = (if (r.1.x < x && r.1.y < y) = true then r.2 else 0) + prefixMass rs x y := by
  unfold prefixMass
  simp only [List.filter_cons, List.map_cons, List.sum_cons]
  by_cases h : (r.1.x < x && r.1.y < y) = true
  · rw [if_pos h]; simp [h]
  · rw [if_neg h]; simp [h]

private theorem prefixMass_box (rs : Rows) (x0 x1 y0 y1 : Int) (hx : x0 < x1) (hy : y0 < y1) :
    prefixMass rs x1 y1 - prefixMass rs x0 y1 - prefixMass rs x1 y0 + prefixMass rs x0 y0 =
      ((rs.filter (fun r => inBox (box x0 x1 y0 y1) r.1)).map Prod.snd).sum := by
  rw [sum_filter_eq_sum_if]
  induction rs with
  | nil => simp [prefixMass]
  | cons r rs ih =>
      rw [prefixMass_cons, prefixMass_cons, prefixMass_cons, prefixMass_cons]
      simp only [List.map_cons, List.sum_cons]
      by_cases hin : inBox (box x0 x1 y0 y1) r.1 = true
      · have hc : x0 ≤ r.1.x ∧ r.1.x < x1 ∧ y0 ≤ r.1.y ∧ r.1.y < y1 := by
          simp only [inBox, box, Bool.and_eq_true] at hin
          exact ⟨of_decide_eq_true hin.1.1.1, of_decide_eq_true hin.1.1.2,
            of_decide_eq_true hin.1.2, of_decide_eq_true hin.2⟩
        by_cases hx1 : r.1.x < x1 <;> by_cases hx0 : r.1.x < x0 <;>
          by_cases hy1 : r.1.y < y1 <;> by_cases hy0 : r.1.y < y0 <;>
            simp [hin, hx1, hx0, hy1, hy0] <;> rw [← ih] <;> omega
      · have hc : ¬ (x0 ≤ r.1.x ∧ r.1.x < x1 ∧ y0 ≤ r.1.y ∧ r.1.y < y1) := by
          intro hh
          apply hin
          simp only [inBox, box, Bool.and_eq_true]
          exact And.intro (And.intro (And.intro (decide_eq_true hh.1) (decide_eq_true hh.2.1))
            (decide_eq_true hh.2.2.1)) (decide_eq_true hh.2.2.2)
        simp only [not_and] at hc
        by_cases hx1 : r.1.x < x1 <;> by_cases hx0 : r.1.x < x0 <;>
          by_cases hy1 : r.1.y < y1 <;> by_cases hy0 : r.1.y < y0 <;>
            simp [hin, hx1, hx0, hy1, hy0] <;> rw [← ih] <;> omega

private theorem filter_nil_of_forall_not {α : Type} (l : List α) (f : α → Bool)
    (h : ∀ a ∈ l, ¬ f a = true) : l.filter f = [] := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      have ha : ¬ f a = true := h a List.mem_cons_self
      have has : ∀ b ∈ as, ¬ f b = true := fun b hb => h b (List.mem_cons_of_mem a hb)
      simp [ha, ih has]

private theorem corners_box_sum (q : Box) (rs : Rows) :
    corners q (prefixMass rs) = q.coeff * ((rs.filter (fun r => inBox q r.1)).map Prod.snd).sum := by
  unfold corners
  by_cases hbox : (q.x0 < q.x1 && q.y0 < q.y1) = true
  · rw [if_pos hbox]
    have hp : q.x0 < q.x1 ∧ q.y0 < q.y1 := by
      simpa only [Bool.and_eq_true, decide_eq_true_eq] using hbox
    exact congrArg (fun z : Int => q.coeff * z)
      (prefixMass_box rs q.x0 q.x1 q.y0 q.y1 hp.1 hp.2)
  · rw [if_neg hbox]
    have hnot : ¬ (q.x0 < q.x1 ∧ q.y0 < q.y1) := by
      intro hh
      apply hbox
      simp only [Bool.and_eq_true, decide_eq_true_eq]
      exact hh
    have hn : (rs.filter (fun r => inBox q r.1)) = [] := by
      apply filter_nil_of_forall_not
      intro r hr
      have hb : inBox q r.1 = false := by
        apply Bool.eq_false_of_ne_true
        intro hi
        apply hnot
        simp only [inBox, Bool.and_eq_true] at hi
        have hxlo := of_decide_eq_true hi.1.1.1
        have hxhi := of_decide_eq_true hi.1.1.2
        have hylo := of_decide_eq_true hi.1.2
        have hyhi := of_decide_eq_true hi.2
        constructor <;> omega
      simpa only [Bool.not_eq_true] using hb
    simp [hn]

private theorem sum_map_left_mul (rs : Rows) (c : Int) :
    (rs.map (fun r => c * r.2)).sum = c * (rs.map Prod.snd).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih => simp [ih, Int.mul_add]

private theorem mul_sum_if (rs : Rows) (c : Int) (f : Point × Int → Bool) :
    c * (rs.map (fun r => if f r then r.2 else 0)).sum =
      (rs.map (fun r => if f r then c * r.2 else 0)).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih, Int.mul_add] <;> omega

private def stencilWeight (s : Shape) (b : Int) (p : Point) (r : Point) : Int :=
  ((s.stencil b p).map (fun q => if inBox q r then q.coeff else 0)).sum

private theorem stencilWeight_mul (st : List Box) (r : Point × Int) :
    (st.map (fun q => if inBox q r.1 then q.coeff * r.2 else 0)).sum =
      (st.map (fun q => if inBox q r.1 then q.coeff else 0)).sum * r.2 := by
  induction st with
  | nil => simp
  | cons q qs ih =>
      by_cases h : inBox q r.1 = true <;> simp [h, ih, Int.add_mul] <;> omega

private theorem if_one_mul (rs : Rows) (f : Point × Int → Bool) :
    (rs.map (fun r => (if f r then (1 : Int) else 0) * r.2)).sum =
      ((rs.filter f).map Prod.snd).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih] <;> omega

private theorem prefixIndex_of_stencil (s : Shape) (b : Int) (p : Point) (rs : Rows)
    (h : ∀ r ∈ rs, stencilWeight s b p r.1 = if s.qualifies p r.1 then (1 : Int) else 0) :
    indexed s b p (prefixMass rs) = scan s p rs := by
  unfold indexed scan
  have h1 : ((s.stencil b p).map (corners · (prefixMass rs))).sum =
      ((s.stencil b p).map (fun q => (rs.map (fun r => if inBox q r.1 then q.coeff * r.2 else 0)).sum)).sum := by
    induction s.stencil b p with
    | nil => rfl
    | cons q qs ih =>
        rw [List.map_cons, List.sum_cons, List.map_cons, List.sum_cons]
        rw [corners_box_sum, sum_filter_eq_sum_if, mul_sum_if, ih]
  rw [h1, sum_swap]
  have h2 : ∀ (rs' : Rows), (∀ r ∈ rs', stencilWeight s b p r.1 = if s.qualifies p r.1 then (1 : Int) else 0) →
      (rs'.map (fun r => ((s.stencil b p).map (fun q => if inBox q r.1 then q.coeff * r.2 else 0)).sum)).sum =
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

/-! ### Page summaries -/

private theorem uniform_weight (g : Page) (q : Box) (hg : valid g) :
    match uniform g q with
    | some c => ∀ r ∈ g.rows, c = if inBox q r.1 then q.coeff else 0
    | none => True := by
  cases hu : uniform g q with
  | none => trivial
  | some c =>
      unfold uniform at hu
      split at hu
      · rename_i hdisjoint
        simp only [Option.some.injEq] at hu
        subst c
        intro r hr
        have hv := hg r hr
        simp only [Bool.or_eq_true, decide_eq_true_eq] at hdisjoint
        by_cases hi : q.x0 ≤ r.1.x ∧ r.1.x < q.x1 ∧ q.y0 ≤ r.1.y ∧ r.1.y < q.y1
        · rw [if_pos (by simpa [inBox, and_assoc, decide_eq_true_eq] using hi)]
          omega
        · rw [if_neg (by simpa [inBox, and_assoc, decide_eq_true_eq] using hi)]
      · split at hu
        · rename_i hcover
          simp only [Option.some.injEq] at hu
          subst c
          intro r hr
          have hv := hg r hr
          simp only [Bool.and_eq_true, decide_eq_true_eq] at hcover
          have hi : q.x0 ≤ r.1.x ∧ r.1.x < q.x1 ∧ q.y0 ≤ r.1.y ∧ r.1.y < q.y1 := by
            omega
          rw [if_pos (by simpa [inBox, and_assoc, decide_eq_true_eq] using hi)]
        · simp at hu

private theorem page_mass (g : Page) (st : List Box) (hg : valid g) :
    ∀ ws, st.mapM (uniform g) = some ws →
      ws.sum * (g.rows.map Prod.snd).sum =
        (g.rows.map (fun r => ((st.map (fun q => if inBox q r.1 then q.coeff else 0)).sum * r.2))).sum := by
  induction st with
  | nil =>
      intro ws hws
      simp at hws
      subst ws
      simp [sum_map_zero]
  | cons q qs ih =>
      intro ws hws
      cases hu : uniform g q with
      | none =>
          simp [List.mapM_cons, hu] at hws
      | some c =>
          cases hm : qs.mapM (uniform g) with
          | none => simp [List.mapM_cons, hu, hm] at hws
          | some vs =>
              simp [List.mapM_cons, hu, hm] at hws
              subst ws
              have hw0 := uniform_weight g q hg
              rw [hu] at hw0
              have hw : ∀ r ∈ g.rows, c = if inBox q r.1 then q.coeff else 0 := hw0
              have hq : c * (g.rows.map Prod.snd).sum =
                  (g.rows.map (fun r => (if inBox q r.1 then q.coeff else 0) * r.2)).sum := by
                rw [← sum_map_left_mul g.rows c]
                apply congrArg List.sum
                apply List.map_congr_left
                intro r hr
                rw [hw r hr]
              have ht := ih vs hm
              simp only [List.map_cons, List.sum_cons]
              rw [Int.add_mul, hq, ht, ← sum_map_add]
              apply congrArg List.sum
              apply List.map_congr_left
              intro r hr
              rw [Int.add_mul]

private theorem pageScan_of_stencil (s : Shape) (b : Int) (p : Point) (g : Page)
    (hg : valid g) (hb : ∀ r ∈ g.rows, bounded b r.1)
    (hst : ∀ r ∈ g.rows, stencilWeight s b p r.1 = if s.qualifies p r.1 then (1 : Int) else 0) :
    pageScan s b p g = scan s p g.rows := by
  unfold pageScan
  cases hm : (s.stencil b p).mapM (uniform g) with
  | none => rfl
  | some ws =>
      change ws.sum * (g.rows.map Prod.snd).sum = scan s p g.rows
      rw [page_mass g (s.stencil b p) hg ws hm]
      rw [show (g.rows.map (fun r => ((s.stencil b p).map (fun q => if inBox q r.1 then q.coeff else 0)).sum * r.2)) =
          (g.rows.map (fun r => (if s.qualifies p r.1 then (1 : Int) else 0) * r.2)) by
        apply List.map_congr_left
        intro r hr
        rw [← stencilWeight, hst r hr]]
      exact if_one_mul g.rows (fun r => s.qualifies p r.1)

/-! ### Compiled probe events and anti-joins -/

private theorem prefixMass_append (es₁ es₂ : Rows) (x y : Int) :
    prefixMass (es₁ ++ es₂) x y = prefixMass es₁ x y + prefixMass es₂ x y := by
  induction es₁ with
  | nil => simp [prefixMass]
  | cons e es ih =>
      rw [List.cons_append, prefixMass_cons, prefixMass_cons, ih]
      omega

private theorem atPoint_append (es₁ es₂ : Rows) (r : Point) :
    atPoint (es₁ ++ es₂) r = atPoint es₁ r + atPoint es₂ r := by
  simp [atPoint, prefixMass_append]

private theorem atPoint_events (q : Box) (w : Int) (r : Point) :
    atPoint (events q w) r = if inBox q r then w * q.coeff else 0 := by
  unfold events
  generalize hz : w * q.coeff = z
  by_cases hb : q.x0 < q.x1 && q.y0 < q.y1
  · have hbprop : q.x0 < q.x1 ∧ q.y0 < q.y1 := by
      simpa only [Bool.and_eq_true, decide_eq_true_eq] using hb
    simp [hb, atPoint, prefixMass]
    by_cases hx0 : q.x0 < r.x + 1 <;> by_cases hx1 : q.x1 < r.x + 1 <;>
      by_cases hy0 : q.y0 < r.y + 1 <;> by_cases hy1 : q.y1 < r.y + 1 <;>
      simp [hx0, hx1, hy0, hy1]
    all_goals (try simp only [inBox_iff]) <;> (try intro hi) <;> (try split) <;>
      (try (rename_i hbox; rcases hbox with ⟨hxlo,hxhi,hylo,hyhi⟩)) <;>
      (try (rcases hi with ⟨hxlo,hxhi,hylo,hyhi⟩)) <;>
      (try (
        have hnp : ¬ (q.x0 ≤ r.x ∧ r.x < q.x1 ∧ q.y0 ≤ r.y ∧ r.y < q.y1) := by
          intro hp
          have ht : inBox q r = true := (inBox_iff q r).mpr hp
          rw [hi] at ht
          contradiction
        simp only [not_and] at hnp)) <;>
      (try rw [Int.add_neg_cancel_left]) <;> omega
  · have hbprop : ¬ (q.x0 < q.x1 ∧ q.y0 < q.y1) := by
      intro hh
      apply hb
      simp only [Bool.and_eq_true, decide_eq_true_eq]
      exact hh
    simp [hb, atPoint, prefixMass]
    (try simp only [inBox_iff])
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

private theorem stencilWeight_scale (st : List Box) (w : Int) (r : Point) :
    (st.map (fun q => if inBox q r then w * q.coeff else 0)).sum =
      w * ((st.map (fun q => if inBox q r then q.coeff else 0)).sum) := by
  induction st with
  | nil => simp
  | cons q qs ih =>
      by_cases h : inBox q r = true
      · simp only [h, if_true, List.map_cons, List.sum_cons, ih]
        rw [Int.mul_add]
      · simp [h, ih]

private theorem atPoint_compile (s : Shape) (b : Int) (ps : Rows) (r : Point) :
    atPoint (compile s b ps) r =
      (ps.map (fun (p,w) => w * stencilWeight s b p r)).sum := by
  induction ps with
  | nil => simp [compile, atPoint, prefixMass]
  | cons pw ps ih =>
      rcases pw with ⟨p,w⟩
      simp only [compile, List.flatMap_cons, List.map_cons, List.sum_cons] at ih ⊢
      rw [atPoint_append, atPoint_flatMap_events, stencilWeight_scale, ih]
      rfl

private theorem sum_indicator_nonneg (ps : List Point) (f : Point → Bool) :
    0 ≤ (ps.map (fun p => if f p then (1 : Int) else 0)).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons]
      split <;> omega

private theorem sum_indicator_any_zero (ps : List Point) (f : Point → Bool) :
    ((ps.map (fun p => if f p then (1 : Int) else 0)).sum == 0) = !ps.any f := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.any_cons] at ih ⊢
      by_cases hp : f p = true
      · rw [hp]
        simp only [Bool.true_or, Bool.not_true, if_true]
        apply Bool.eq_false_of_ne_true
        intro hz
        have heq : 1 + (ps.map (fun p => if f p then (1 : Int) else 0)).sum = 0 :=
          of_decide_eq_true hz
        have hn := sum_indicator_nonneg ps f
        omega
      · have hf : f p = false := Bool.eq_false_of_ne_true hp
        rw [hf]
        simp only [Bool.false_eq_true, if_false, Bool.false_or, Int.zero_add, ih]

private theorem unionAnti_sound (s : Shape) (b : Int) (ps : List Point) (rs : Rows)
    (hb : ∀ r ∈ rs, bounded b r.1)
    (hst : ∀ p v, bounded b v → stencilWeight s b p v = if s.qualifies p v then (1 : Int) else 0) :
    let es := compile s b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered s ps r.1) = rs.filter (fun r => atPoint es r.1 == 0) := by
  apply List.filter_congr
  intro r hr
  have hbr := hb r hr
  have hc : atPoint (compile s b (ps.map (fun p => (p,1)))) r.1 =
      (ps.map (fun p => stencilWeight s b p r.1)).sum := by
    rw [atPoint_compile, List.map_map]
    apply congrArg List.sum
    apply List.map_congr_left
    intro p hp
    simp
  rw [hc]
  have hw : (ps.map (fun p => stencilWeight s b p r.1)) =
      (ps.map (fun p => if s.qualifies p r.1 then (1 : Int) else 0)) := by
    apply List.map_congr_left
    intro p hp
    exact hst p r.1 hbr
  rw [hw, sum_indicator_any_zero]
  cases hcov : covered s ps r.1
  · have hfalse : ps.any (fun p => s.qualifies p r.1) = false := by
      simpa only [covered] using hcov
    simp [hfalse]
  · have htrue : ps.any (fun p => s.qualifies p r.1) = true := by
      simpa only [covered] using hcov
    simp [htrue]

/-! ### PriceCorridor -/

namespace PriceCorridor

open TinylambOpt.ResearchGoals.Atlas.RangePlans.PriceCorridor

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    ((shape.stencil b p).map (fun q => if inBox q v then q.coeff else 0)).sum =
      if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, stencilWeight, box, inBox, bounded] at hv ⊢
  omega

theorem sound_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_stencil shape b p rs (fun r hr => stencil_id b p r.1 (hb r hr))

theorem sound_PageVerdict : PageVerdict.Goal := by
  intro b p g hg hb
  exact pageScan_of_stencil shape b p g hg hb (fun r hr => stencil_id b p r.1 (hb r hr))

theorem sound_UnionAnti : UnionAnti.Goal := by
  intro b ps rs hb
  exact unionAnti_sound shape b ps rs hb (fun p v hv => stencil_id b p v hv)

theorem sound_SlidingStrip : SlidingStrip.Goal := by
  intro p rs
  dsimp only
  unfold scan
  rw [sum_filter_eq_sum_if, sum_filter_eq_sum_if, sum_filter_eq_sum_if,
    sum_filter_eq_sum_if,
    ← sum_map_sub rs (fun r => if shape.qualifies ⟨p.x+1,p.y⟩ r.1 then r.2 else 0)
      (fun r => if shape.qualifies p r.1 then r.2 else 0),
    ← sum_map_sub rs (fun r => if (r.1.x == p.x + 4 && p.y - 2 ≤ r.1.y && r.1.y ≤ p.y + 5) = true then r.2 else 0)
      (fun r => if (r.1.x == p.x - 3 && p.y - 2 ≤ r.1.y && r.1.y ≤ p.y + 5) = true then r.2 else 0)]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  by_cases hylo : p.y - 2 ≤ r.1.y <;> by_cases hyhi : r.1.y ≤ p.y + 5 <;>
    by_cases hx4 : r.1.x = p.x + 4 <;> by_cases hx3 : r.1.x = p.x - 3 <;>
      simp [shape, hylo, hyhi, hx4, hx3] <;> omega

end PriceCorridor

/-! ### ParetoFront -/

namespace ParetoFront

open TinylambOpt.ResearchGoals.Atlas.RangePlans.ParetoFront

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    ((shape.stencil b p).map (fun q => if inBox q v then q.coeff else 0)).sum =
      if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, stencilWeight, box, inBox, bounded] at hv ⊢
  omega

theorem sound_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_stencil shape b p rs (fun r hr => stencil_id b p r.1 (hb r hr))

theorem sound_PageVerdict : PageVerdict.Goal := by
  intro b p g hg hb
  exact pageScan_of_stencil shape b p g hg hb (fun r hr => stencil_id b p r.1 (hb r hr))

private theorem dominates_trans {a b c : Point}
    (hab : shape.qualifies a b = true) (hbc : shape.qualifies b c = true) :
    shape.qualifies a c = true := by
  simp [shape] at hab hbc ⊢
  omega

private theorem exists_max_score (ps : List Point) (hne : ps ≠ []) :
    ∃ r ∈ ps, ∀ s ∈ ps, s.x + s.y ≤ r.x + r.y := by
  induction ps with
  | nil => exact False.elim (hne rfl)
  | cons a as ih =>
      by_cases has : as = []
      · subst as
        exact ⟨a, by simp, by intro s hs; simp at hs; subst s; omega⟩
      · obtain ⟨r, hr, hmax⟩ := ih has
        by_cases hle : r.x + r.y ≤ a.x + a.y
        · refine ⟨a, by simp, ?_⟩
          intro s hs
          simp only [List.mem_cons] at hs
          rcases hs with rfl | hs
          · omega
          · exact Int.le_trans (hmax s hs) hle
        · refine ⟨r, by simp [hr], ?_⟩
          intro s hs
          simp only [List.mem_cons] at hs
          rcases hs with rfl | hs
          · omega
          · exact hmax s hs

theorem sound_SkylineWitness : SkylineWitness.Goal := by
  intro p points
  cases hleft : points.any (shape.qualifies p)
  · have hright : (points.filter (fun r => !points.any (shape.qualifies r))).any
        (shape.qualifies p) = false := by
      apply Bool.eq_false_of_ne_true
      intro ht
      obtain ⟨r, hr, hqual⟩ := List.any_eq_true.mp ht
      have hrpts := (List.mem_filter.mp hr).1
      have htleft : points.any (shape.qualifies p) = true :=
        List.any_eq_true.mpr ⟨r, hrpts, hqual⟩
      rw [hleft] at htleft
      contradiction
    rw [hright]
  · obtain ⟨q, hq, hqual⟩ := List.any_eq_true.mp hleft
    let ds := points.filter (shape.qualifies p)
    have hqds : q ∈ ds := List.mem_filter.mpr ⟨hq, hqual⟩
    have hne : ds ≠ [] := by
      intro he
      rw [he] at hqds
      simp at hqds
    obtain ⟨r, hr, hmax⟩ := exists_max_score ds hne
    have hrqual := (List.mem_filter.mp hr).2
    have hnot : points.any (shape.qualifies r) = false := by
      apply Bool.eq_false_of_ne_true
      intro ht
      obtain ⟨s, hs, hsr⟩ := List.any_eq_true.mp ht
      have hsp : shape.qualifies p s = true := dominates_trans hrqual hsr
      have hsds : s ∈ ds := List.mem_filter.mpr ⟨hs, hsp⟩
      have hle := hmax s hsds
      have hlt : r.x + r.y < s.x + s.y := by
        simp [shape] at hsr
        omega
      omega
    have hrsky : r ∈ points.filter (fun r => !points.any (shape.qualifies r)) :=
      List.mem_filter.mpr ⟨(List.mem_filter.mp hr).1, by simpa [hnot]⟩
    have hright : (points.filter (fun r => !points.any (shape.qualifies r))).any
        (shape.qualifies p) = true := List.any_eq_true.mpr ⟨r, hrsky, hrqual⟩
    rw [hright]

theorem sound_UnionAnti : UnionAnti.Goal := by
  intro b ps rs hb
  exact unionAnti_sound shape b ps rs hb (fun p v hv => stencil_id b p v hv)

end ParetoFront

/-! ### ReversedRank -/

namespace ReversedRank

open TinylambOpt.ResearchGoals.Atlas.RangePlans.ReversedRank

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    ((shape.stencil b p).map (fun q => if inBox q v then q.coeff else 0)).sum =
      if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, stencilWeight, box, inBox, bounded] at hv ⊢
  omega

theorem sound_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_stencil shape b p rs (fun r hr => stencil_id b p r.1 (hb r hr))

theorem sound_PageVerdict : PageVerdict.Goal := by
  intro b p g hg hb
  exact pageScan_of_stencil shape b p g hg hb (fun r hr => stencil_id b p r.1 (hb r hr))

private theorem zipIdx_drop_shift (p : Point) (ps : List Point) (n : Nat)
    (g : Point → List Point → Nat) :
    (ps.zipIdx (n+1)).map (fun (q,i) => g q ((p::ps).drop (i+1))) =
      (ps.zipIdx n).map (fun (q,i) => g q (ps.drop (i+1))) := by
  rw [List.zipIdx_succ, List.map_map]
  apply List.map_congr_left
  intro x hx
  rcases x with ⟨q,i⟩
  simp [Function.comp_def, List.drop_succ_cons]

private theorem tail_counts_cons (p : Point) (ps : List Point)
    (hx : ∀ q ∈ ps, p.x < q.x) :
    (ps.map (fun q => (p::ps).countP (shape.qualifies q))).sum =
      (ps.map (fun q => ps.countP (shape.qualifies q))).sum +
        ps.countP (fun q => q.y < p.y) := by
  have hmap : ps.map (fun q => (p::ps).countP (shape.qualifies q)) =
      ps.map (fun q => ps.countP (shape.qualifies q) +
        (if shape.qualifies q p then 1 else 0)) := by
    apply List.map_congr_left
    intro q hq
    simp [List.countP_cons]
  rw [hmap]
  rw [sum_map_add_nat]
  have hind : (ps.map (fun q => if shape.qualifies q p then 1 else 0)).sum =
      ps.countP (fun q => q.y < p.y) := by
    rw [← sum_indicator_count_nat]
    apply congrArg List.sum
    apply List.map_congr_left
    intro q hq
    have hxq : ¬ (q.x < p.x) := by have := hx q hq; omega
    simp [shape, hx q hq, hxq]
  rw [hind]

private theorem inversionPairs_aux (ps : List Point)
    (hpair : ps.Pairwise (fun a b => a.x < b.x)) :
    (ps.map (fun p => ps.countP (shape.qualifies p))).sum =
      2 * (ps.zipIdx.map (fun (p,i) => (ps.drop (i+1)).countP (fun q => q.y < p.y))).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      have hx : ∀ q ∈ ps, p.x < q.x := (List.pairwise_cons.mp hpair).1
      have htail := ih (List.pairwise_cons.mp hpair).2
      have hself : shape.qualifies p p = false := by
        simp [shape]
      have hcount : (p::ps).countP (shape.qualifies p) =
          ps.countP (fun q => q.y < p.y) := by
        rw [List.countP_cons, hself]
        simp only [Bool.false_eq_true, if_false, Nat.zero_add]
        apply List.countP_congr
        intro q hq
        have hxq : ¬ (q.x < p.x) := by
          have := hx q hq; omega
        have hxq2 : p.x < q.x := hx q hq
        simp [shape, hxq, hxq2]
      have htailsum := tail_counts_cons p ps hx
      have hz := zipIdx_drop_shift p ps 0 (fun q xs => xs.countP (fun r => r.y < q.y))
      simp only [List.map_cons, List.sum_cons, List.zipIdx_cons, Nat.zero_add]
      rw [hcount, htailsum, htail, hz]
      simp only [List.drop_succ_cons, List.drop_zero]
      omega
theorem sound_InversionPairs : InversionPairs.Goal := by
  intro ps hpair
  exact inversionPairs_aux ps hpair

theorem sound_UnionAnti : UnionAnti.Goal := by
  intro b ps rs hb
  exact unionAnti_sound shape b ps rs hb (fun p v hv => stencil_id b p v hv)

end ReversedRank

/-! ### OutsideBand -/

namespace OutsideBand

open TinylambOpt.ResearchGoals.Atlas.RangePlans.OutsideBand

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    ((shape.stencil b p).map (fun q => if inBox q v then q.coeff else 0)).sum =
      if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, stencilWeight, box, inBox, bounded] at hv ⊢
  omega

theorem sound_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_stencil shape b p rs (fun r hr => stencil_id b p r.1 (hb r hr))

theorem sound_PageVerdict : PageVerdict.Goal := by
  intro b p g hg hb
  exact pageScan_of_stencil shape b p g hg hb (fun r hr => stencil_id b p r.1 (hb r hr))

theorem sound_UnionAnti : UnionAnti.Goal := by
  intro b ps rs hb
  exact unionAnti_sound shape b ps rs hb (fun p v hv => stencil_id b p v hv)

theorem sound_ComplementSum : ComplementSum.Goal := by
  intro p rs
  unfold scan
  rw [sum_filter_eq_sum_if, sum_filter_eq_sum_if, ← sum_map_sub]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  by_cases hx : (r.1.x - p.x).natAbs ≤ 2 <;> by_cases hy : (r.1.y - p.y).natAbs ≤ 4 <;>
    simp [shape, hx, hy] <;> omega

end OutsideBand

private theorem sum_map_filters (ds : List Nat) (rs : Rows) (f : Nat → Point × Int → Bool) :
    List.sum (ds.map (fun d => List.sum ((rs.filter (f d)).map Prod.snd))) =
      List.sum (rs.map (fun r => List.sum (ds.map (fun d => if f d r then r.2 else 0)))) := by
  induction ds with
  | nil => simp [sum_map_zero]
  | cons d ds ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [sum_filter_eq_sum_if rs (f d) Prod.snd, ih, ← sum_map_add]

private theorem ring_row (d : Nat) (w : Int) :
    (if 1 < d && d ≤ 5 then w else 0) =
      ([2,3,4,5].map (fun e => if d == e then w else 0)).sum := by
  simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
  by_cases h2 : d = 2 <;> by_cases h3 : d = 3 <;> by_cases h4 : d = 4 <;>
    by_cases h5 : d = 5 <;> by_cases hq : 1 < d ∧ d ≤ 5 <;>
      simp [h2, h3, h4, h5, hq] <;> omega

/-! ### RingNeighborhood -/

namespace RingNeighborhood

open TinylambOpt.ResearchGoals.Atlas.RangePlans.RingNeighborhood

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    ((shape.stencil b p).map (fun q => if inBox q v then q.coeff else 0)).sum =
      if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, stencilWeight, box, inBox, bounded] at hv ⊢
  omega

theorem sound_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_stencil shape b p rs (fun r hr => stencil_id b p r.1 (hb r hr))

theorem sound_PageVerdict : PageVerdict.Goal := by
  intro b p g hg hb
  exact pageScan_of_stencil shape b p g hg hb (fun r hr => stencil_id b p r.1 (hb r hr))

theorem sound_UnionAnti : UnionAnti.Goal := by
  intro b ps rs hb
  exact unionAnti_sound shape b ps rs hb (fun p v hv => stencil_id b p v hv)

theorem sound_PerimeterHistogram : PerimeterHistogram.Goal := by
  intro p rs
  unfold scan
  rw [sum_filter_eq_sum_if]
  rw [sum_map_filters]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  exact ring_row (max (r.1.x - p.x).natAbs (r.1.y - p.y).natAbs) r.2

end RingNeighborhood

end TinylambOpt.ResearchProofs.Atlas.RangePlans2
