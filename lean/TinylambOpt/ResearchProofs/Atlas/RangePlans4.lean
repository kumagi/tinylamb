import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.RangePlans4

open TinylambOpt.ResearchGoals.Atlas.RangePlans

/-! ### Generic finite-range algebra -/

private def inBox (q : Box) (v : Point) : Prop :=
  q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1

private instance (q : Box) (v : Point) : Decidable (inBox q v) := by
  unfold inBox
  infer_instance

private def memBox (q : Box) (v : Point) : Bool :=
  decide (inBox q v)

private theorem memBox_eq (q : Box) (v : Point) :
    memBox q v = decide (q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1) := by
  rfl

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool)
    (g : Point × Int → Int) :
    ((rs.filter f).map g).sum = (rs.map (fun r => if f r then g r else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih]

private theorem scan_cons (s : Shape) (p : Point) (r : Point × Int) (rs : Rows) :
    scan s p (r :: rs) = (if s.qualifies p r.1 then r.2 else 0) + scan s p rs := by
  by_cases h : s.qualifies p r.1 = true <;> simp [scan, h]

private theorem sum_filter_map_cons (r : Point × Int) (rs : Rows)
    (f : Point × Int → Bool) (g : Point × Int → Int) :
    (((r :: rs).filter f).map g).sum =
      (if f r then g r else 0) + ((rs.filter f).map g).sum := by
  by_cases h : f r = true <;> simp [h]

private theorem prefixMass_cons (r : Point × Int) (rs : Rows) (x y : Int) :
    prefixMass (r :: rs) x y =
      (if r.1.x < x && r.1.y < y then r.2 else 0) + prefixMass rs x y := by
  by_cases h : r.1.x < x && r.1.y < y <;> simp [prefixMass, h]

private theorem prefixMass_box (q : Box) (rs : Rows)
    (hq : q.x0 < q.x1 ∧ q.y0 < q.y1) :
    prefixMass rs q.x1 q.y1 - prefixMass rs q.x0 q.y1 -
      prefixMass rs q.x1 q.y0 + prefixMass rs q.x0 q.y0 =
      ((rs.filter (fun r => memBox q r.1)).map Prod.snd).sum := by
  rw [sum_filter_eq_sum_if]
  induction rs with
  | nil => simp [prefixMass]
  | cons r rs ih =>
      rw [prefixMass_cons, prefixMass_cons, prefixMass_cons, prefixMass_cons]
      simp only [List.map_cons, List.sum_cons]
      have hhead :
          (if r.1.x < q.x1 && r.1.y < q.y1 then r.2 else 0) -
            (if r.1.x < q.x0 && r.1.y < q.y1 then r.2 else 0) -
            (if r.1.x < q.x1 && r.1.y < q.y0 then r.2 else 0) +
            (if r.1.x < q.x0 && r.1.y < q.y0 then r.2 else 0) =
          (if memBox q r.1 then r.2 else 0) := by
        by_cases hx1 : r.1.x < q.x1 <;> by_cases hx0 : r.1.x < q.x0 <;>
          by_cases hy1 : r.1.y < q.y1 <;> by_cases hy0 : r.1.y < q.y0 <;>
          by_cases hxl : q.x0 ≤ r.1.x <;> by_cases hyl : q.y0 ≤ r.1.y <;>
          simp [memBox_eq, hx1, hx0, hy1, hy0, hxl, hyl] <;> grind
      omega

private theorem filter_nil_of_forall_not {α : Type} (l : List α) (f : α → Bool)
    (h : ∀ a ∈ l, ¬ f a = true) : l.filter f = [] := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      have ha : ¬ f a = true := h a (by simp)
      have has : ∀ b ∈ as, ¬ f b = true := fun b hb => h b (by simp [hb])
      simp [ha, ih has]

private theorem corners_box_sum (q : Box) (rs : Rows) :
    corners q (prefixMass rs) =
      q.coeff * ((rs.filter (fun r => memBox q r.1)).map Prod.snd).sum := by
  unfold corners
  by_cases h : (q.x0 < q.x1 && q.y0 < q.y1) = true
  · rw [if_pos h]
    exact congrArg (fun z : Int => q.coeff * z)
      (prefixMass_box q rs (by simpa using h))
  · rw [if_neg h]
    have hp : ¬ (q.x0 < q.x1 ∧ q.y0 < q.y1) := by
      intro hh
      exact h (by simp [hh])
    have hn : (rs.filter (fun r => memBox q r.1)) = [] := by
      apply filter_nil_of_forall_not
      intro r hr hm
      simp only [memBox, decide_eq_true_eq, inBox] at hm
      omega
    simp [hn]

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih] <;> omega

private theorem sum_map_mul_const {α : Type} (xs : List α) (f : α → Int) (w : Int) :
    (xs.map (fun x => w * f x)).sum = w * (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih, Int.mul_add]

private theorem sum_map_mul_const' {α : Type} (xs : List α) (f : α → Int) (w : Int) :
    (xs.map (fun x => f x * w)).sum = (xs.map f).sum * w := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih, Int.add_mul]

private theorem sum_map_zero {β : Type} (bs : List β) : (bs.map (fun _ => (0 : Int))).sum = 0 := by
  induction bs with
  | nil => simp
  | cons b bs ih => simp [ih]

private theorem sum_swap {α β : Type} (as : List α) (bs : List β)
    (f : α → β → Int) :
    (as.map (fun a => (bs.map (fun b => f a b)).sum)).sum =
      (bs.map (fun b => (as.map (fun a => f a b)).sum)).sum := by
  induction as with
  | nil => simp [sum_map_zero]
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih, ← sum_map_add]

private def stencilWeight (st : List Box) (v : Point) : Int :=
  (st.map (fun q => q.coeff * (if memBox q v then 1 else 0))).sum

private theorem stencilWeight_mul (st : List Box) (r : Point × Int) :
    (st.map (fun q => q.coeff * (if memBox q r.1 then r.2 else 0))).sum =
      stencilWeight st r.1 * r.2 := by
  induction st with
  | nil => simp [stencilWeight]
  | cons q qs ih =>
      by_cases h : memBox q r.1
      · simp [h, ih, stencilWeight, Int.add_mul]
      · simp [h, ih, stencilWeight]

private theorem mul_sum_if (rs : Rows) (c : Int) (f : Point × Int → Bool) :
    c * (rs.map (fun r => if f r then r.2 else 0)).sum =
      (rs.map (fun r => c * (if f r then r.2 else 0))).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih, Int.mul_add]

private theorem boxsum_eq_map_if (q : Box) (rs : Rows) :
    ((rs.filter (fun r => memBox q r.1)).map Prod.snd).sum =
      (rs.map (fun r => if memBox q r.1 then r.2 else 0)).sum :=
  sum_filter_eq_sum_if rs _ _

private theorem if_one_mul (rs : Rows) (f : Point × Int → Bool) :
    (rs.map (fun r => (if f r then (1 : Int) else 0) * r.2)).sum =
      ((rs.filter f).map Prod.snd).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih] <;> omega

private theorem stencil_sum_eq_scan (s : Shape) (b : Int) (p : Point) (rs : Rows)
    (h : ∀ r ∈ rs, stencilWeight (s.stencil b p) r.1 =
      if s.qualifies p r.1 then 1 else 0) :
    ((s.stencil b p).map (fun q =>
      q.coeff * ((rs.filter (fun r => memBox q r.1)).map Prod.snd).sum)).sum =
        scan s p rs := by
  have hboxes : ((s.stencil b p).map (fun q =>
      q.coeff * ((rs.filter (fun r => memBox q r.1)).map Prod.snd).sum)).sum =
      ((s.stencil b p).map (fun q =>
        (rs.map (fun r => q.coeff * (if memBox q r.1 then r.2 else 0))).sum)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro q hq
    rw [boxsum_eq_map_if]
    exact mul_sum_if rs q.coeff (fun r => memBox q r.1)
  rw [hboxes, sum_swap]
  have hrows : (rs.map (fun r =>
      ((s.stencil b p).map (fun q =>
        q.coeff * (if memBox q r.1 then r.2 else 0))).sum)).sum =
      (rs.map (fun r => (if s.qualifies p r.1 then 1 else 0) * r.2)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro r hr
    rw [stencilWeight_mul, h r hr]
  rw [hrows]
  exact if_one_mul rs (fun r => s.qualifies p r.1)

private theorem prefixIndex_of_stencil (s : Shape) (b : Int) (p : Point)
    (hst : ∀ v : Point, bounded b v →
      stencilWeight (s.stencil b p) v = if s.qualifies p v then 1 else 0)
    (rs : Rows) (hb : ∀ r ∈ rs, bounded b r.1) :
    indexed s b p (prefixMass rs) = scan s p rs := by
  unfold indexed
  have hcorners : ((s.stencil b p).map (corners · (prefixMass rs))).sum =
      ((s.stencil b p).map (fun q =>
        q.coeff * ((rs.filter (fun r => memBox q r.1)).map Prod.snd).sum)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro q hq
    exact corners_box_sum q rs
  rw [hcorners]
  exact stencil_sum_eq_scan s b p rs (fun r hr => hst r.1 (hb r hr))

/-! ### Page verdicts -/

private theorem uniform_zero_boxsum (g : Page) (q : Box) (w : Int)
    (hg : valid g) (hu : uniform g q = some w) :
    q.coeff * ((g.rows.filter (fun r => memBox q r.1)).map Prod.snd).sum =
      w * (g.rows.map Prod.snd).sum := by
  unfold uniform at hu
  by_cases hz : (q.x1 ≤ q.x0 || q.y1 ≤ q.y0 || g.upper.x < q.x0 || q.x1 ≤ g.lower.x ||
      g.upper.y < q.y0 || q.y1 ≤ g.lower.y) = true
  · simp only [hz, if_true, Option.some.injEq] at hu
    subst w
    have hn : (g.rows.filter (fun r => memBox q r.1)) = [] := by
      apply filter_nil_of_forall_not
      intro r hr hm
      have hv := hg r hr
      simp only [Bool.or_eq_true, decide_eq_true_eq] at hz
      simp only [memBox, decide_eq_true_eq, inBox] at hm
      grind
    simp [hn]
  · simp only [hz, if_false] at hu
    by_cases hf : (q.x0 ≤ g.lower.x && g.upper.x < q.x1 &&
        q.y0 ≤ g.lower.y && g.upper.y < q.y1) = true
    · have hfp : q.x0 ≤ g.lower.x ∧ g.upper.x < q.x1 ∧
          q.y0 ≤ g.lower.y ∧ g.upper.y < q.y1 := by
        simpa only [Bool.and_eq_true, decide_eq_true_eq, and_assoc] using hf
      rcases hfp with ⟨hx0, hx1, hy0, hy1⟩
      simp only [hf, if_true, Bool.false_eq_true, if_false, Option.some.injEq] at hu
      subst w
      have hs : g.rows.filter (fun r => memBox q r.1) = g.rows := by
        apply List.filter_eq_self.mpr
        intro r hr
        have hv := hg r hr
        simp only [memBox, decide_eq_true_eq, inBox]
        grind
      simp [hs]
    · simp [uniform, hz, hf] at hu

private theorem mapM_uniform_sum (g : Page) (qs : List Box) (ws : List Int)
    (hg : valid g) (hm : qs.mapM (uniform g) = some ws) :
    ws.sum * (g.rows.map Prod.snd).sum =
      (qs.map (fun q =>
        q.coeff * ((g.rows.filter (fun r => memBox q r.1)).map Prod.snd).sum)).sum := by
  induction qs generalizing ws with
  | nil =>
      cases ws with
      | nil => simp [List.mapM_nil]
      | cons w ws => simp [List.mapM_nil] at hm
  | cons q qs ih =>
      cases hu : uniform g q with
      | none => simp [List.mapM_cons, hu] at hm
      | some w =>
          simp [List.mapM_cons, hu, Option.bind_eq_some_iff] at hm
          rcases hm with ⟨ws', hws, rfl⟩
          simp only [List.map_cons, List.sum_cons]
          rw [uniform_zero_boxsum g q w hg hu, Int.add_mul]
          have htail := ih ws' hws
          rw [htail]

private theorem pageScan_of_stencil (s : Shape) (b : Int) (p : Point) (g : Page)
    (hg : valid g) (hb : ∀ r ∈ g.rows, bounded b r.1)
    (hst : ∀ r ∈ g.rows, bounded b r.1 →
      stencilWeight (s.stencil b p) r.1 = if s.qualifies p r.1 then 1 else 0) :
    pageScan s b p g = scan s p g.rows := by
  cases hm : (s.stencil b p).mapM (uniform g) with
  | none => simp [pageScan, hm]
  | some ws =>
      simp only [pageScan, hm]
      rw [mapM_uniform_sum g _ ws hg hm]
      exact stencil_sum_eq_scan s b p g.rows (fun r hr => hst r hr (hb r hr))

/-! ### Boundary-event transposition and coverage -/

private theorem prefixMass_append (es₁ es₂ : Rows) (x y : Int) :
    prefixMass (es₁ ++ es₂) x y = prefixMass es₁ x y + prefixMass es₂ x y := by
  induction es₁ with
  | nil => simp [prefixMass]
  | cons e es ih => simp [prefixMass_cons, ih] <;> omega

private theorem atPoint_append (es₁ es₂ : Rows) (r : Point) :
    atPoint (es₁ ++ es₂) r = atPoint es₁ r + atPoint es₂ r := by
  simp [atPoint, prefixMass_append]

private theorem atPoint_events (q : Box) (w : Int) (r : Point) :
    atPoint (events q w) r = if memBox q r then w * q.coeff else 0 := by
  unfold events
  by_cases hb : (q.x0 < q.x1 && q.y0 < q.y1) = true
  · simp [hb, atPoint, prefixMass]
    by_cases hx0 : q.x0 < r.x + 1 <;> by_cases hx1 : q.x1 < r.x + 1 <;>
      by_cases hy0 : q.y0 < r.y + 1 <;> by_cases hy1 : q.y1 < r.y + 1 <;>
      by_cases hxl : q.x0 ≤ r.x <;> by_cases hyl : q.y0 ≤ r.y <;>
      simp [memBox_eq, hx0, hx1, hy0, hy1, hxl, hyl] <;> grind
  · have hb' : (q.x0 < q.x1 && q.y0 < q.y1) = false :=
      Bool.eq_false_of_ne_true hb
    have hp : ¬ (q.x0 < q.x1 ∧ q.y0 < q.y1) := by
      intro hh
      exact hb (by simp [hh])
    simp [hb', atPoint, prefixMass]
    have hn : memBox q r = false := by
      simp only [memBox, decide_eq_false_iff_not, inBox]
      omega
    simp [hn]

private theorem atPoint_flatMap_events (ss : List Box) (w : Int) (r : Point) :
    atPoint (ss.flatMap (events · w)) r =
      (ss.map (fun q => if memBox q r then w * q.coeff else 0)).sum := by
  induction ss with
  | nil => simp [atPoint, prefixMass]
  | cons q qs ih =>
      simp only [List.flatMap_cons]
      rw [atPoint_append, atPoint_events, ih]
      simp

private theorem corners_singleton (q : Box) (r : Point) :
    corners q (prefixMass [(r, (1 : Int))]) =
      q.coeff * (if memBox q r then (1 : Int) else 0) := by
  rw [corners_box_sum]
  simp only [List.filter_cons, List.filter_nil, List.map_cons, List.map_nil,
    List.sum_cons, List.sum_nil, Int.add_zero]
  by_cases h : memBox q r = true <;> simp [h]

private theorem atPoint_compile_one (s : Shape) (b : Int) (p : Point) (w : Int)
    (r : Point) :
    atPoint (compile s b [(p, w)]) r =
      indexed s b p (prefixMass [(r, (1 : Int))]) * w := by
  unfold compile indexed
  simp only [List.map_cons, List.map_nil, List.flatMap_cons, List.flatMap_nil,
    List.nil_append, List.append_nil]
  rw [atPoint_flatMap_events]
  have hterm : ∀ q : Box,
      (if memBox q r then w * q.coeff else 0) =
        q.coeff * (if memBox q r then (1 : Int) else 0) * w := by
    intro q
    by_cases h : memBox q r <;> simp [h, Int.mul_comm]
  have hmap : (s.stencil b p).map (fun q => if memBox q r then w * q.coeff else 0) =
      (s.stencil b p).map (fun q =>
        q.coeff * (if memBox q r then (1 : Int) else 0) * w) := by
    apply List.map_congr_left
    intro q hq
    exact hterm q
  rw [hmap, sum_map_mul_const']
  congr 1
  apply congrArg List.sum
  apply List.map_congr_left
  intro q hq
  exact (corners_singleton q r).symm

private theorem atPoint_compile_one_of_stencil (s : Shape) (b : Int) (p : Point)
    (w : Int) (r : Point)
    (hst : ∀ v : Point, bounded b v →
      stencilWeight (s.stencil b p) v = if s.qualifies p v then 1 else 0)
    (hr : bounded b r) :
    atPoint (compile s b [(p, w)]) r =
      w * (if s.qualifies p r then 1 else 0) := by
  rw [atPoint_compile_one, prefixIndex_of_stencil s b p hst [(r, (1 : Int))] (by
    intro r' hr'
    simp only [List.mem_singleton] at hr'
    subst r'
    exact hr)]
  by_cases hq : s.qualifies p r = true <;> simp [scan, hq] <;> omega

private theorem compile_cons (s : Shape) (b : Int) (pr : Point × Int) (ps : Rows) :
    compile s b (pr :: ps) = compile s b [pr] ++ compile s b ps := by
  rcases pr with ⟨p, w⟩
  simp [compile, List.flatMap_cons]

private theorem atPoint_compile_weighted (s : Shape) (b : Int) (ps : Rows)
    (r : Point)
    (hst : ∀ p v : Point, bounded b v →
      stencilWeight (s.stencil b p) v = if s.qualifies p v then 1 else 0)
    (hr : bounded b r) :
    atPoint (compile s b ps) r =
      (ps.map (fun (p, w) => w * (if s.qualifies p r then 1 else 0))).sum := by
  induction ps with
  | nil => simp [compile, atPoint, prefixMass]
  | cons pr ps ih =>
      rcases pr with ⟨p, w⟩
      rw [compile_cons]
      change atPoint (compile s b [(p, w)] ++ compile s b ps) r =
        (((p, w) :: ps).map (fun (p, w) => w * (if s.qualifies p r then 1 else 0))).sum
      rw [atPoint_append, atPoint_compile_one_of_stencil s b p w r (hst p) hr, ih]
      simp

private theorem atPoint_compile_probes (s : Shape) (b : Int) (ps : List Point)
    (r : Point)
    (hst : ∀ p v : Point, bounded b v →
      stencilWeight (s.stencil b p) v = if s.qualifies p v then 1 else 0)
    (hr : bounded b r) :
    atPoint (compile s b (ps.map (fun p => (p, (1 : Int))))) r =
      (ps.map (fun p => if s.qualifies p r then 1 else 0)).sum := by
  simpa only [List.map_map, Function.comp_def, Prod.fst, Prod.snd, Int.one_mul] using
    atPoint_compile_weighted s b (ps.map (fun p => (p, (1 : Int)))) r hst hr

private theorem probeTransposition_of_stencil (s : Shape) (b : Int) (ps rs : Rows)
    (hst : ∀ p v : Point, bounded b v →
      stencilWeight (s.stencil b p) v = if s.qualifies p v then 1 else 0)
    (hb : ∀ r ∈ rs, bounded b r.1) :
    (ps.map (fun (p, w) => w * scan s p rs)).sum =
      (rs.map (fun (r, w) => w * atPoint (compile s b ps) r)).sum := by
  have hone : ∀ (p : Point) (w : Int), w * scan s p rs =
      (rs.map (fun (r, v) => v * atPoint (compile s b [(p, w)]) r)).sum := by
    intro p w
    change w * ((rs.filter (fun r => s.qualifies p r.1)).map Prod.snd).sum = _
    rw [sum_filter_eq_sum_if rs (fun r => s.qualifies p r.1) Prod.snd]
    have hm : rs.map (fun (r, v) => v * atPoint (compile s b [(p, w)]) r) =
        rs.map (fun (r, v) => w * (if s.qualifies p r then v else 0)) := by
      apply List.map_congr_left
      intro rr hrr
      rcases rr with ⟨r, v⟩
      change v * atPoint (compile s b [(p, w)]) r =
        w * (if s.qualifies p r then v else 0)
      rw [atPoint_compile_one_of_stencil s b p w r (hst p) (hb (r, v) hrr)]
      by_cases hq : s.qualifies p r = true <;> simp [hq] <;> grind
    rw [hm]
    exact mul_sum_if rs w (fun r => s.qualifies p r.1)
  induction ps with
  | nil => simp [compile, atPoint, prefixMass, sum_map_zero]
  | cons pr ps ih =>
      rcases pr with ⟨p, w⟩
      rw [compile_cons]
      change w * scan s p rs + (ps.map (fun (p, w) => w * scan s p rs)).sum =
        (rs.map (fun (r, v) => v * atPoint
          (compile s b [(p, w)] ++ compile s b ps) r)).sum
      rw [hone p w, ih]
      have hm : rs.map (fun (r, v) => v * atPoint
            (compile s b [(p, w)] ++ compile s b ps) r) =
          rs.map (fun (r, v) => v * atPoint (compile s b [(p, w)]) r +
            v * atPoint (compile s b ps) r) := by
        apply List.map_congr_left
        intro rr hrr
        rcases rr with ⟨r, v⟩
        change v * atPoint (compile s b [(p, w)] ++ compile s b ps) r =
          v * atPoint (compile s b [(p, w)]) r + v * atPoint (compile s b ps) r
        rw [atPoint_append, Int.mul_add]
      rw [hm, sum_map_add]

private theorem indicator_sum_nonneg (ps : List Point) (f : Point → Bool) :
    0 ≤ (ps.map (fun p => if f p then (1 : Int) else 0)).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      by_cases h : f p = true <;> simp [h] <;> omega

private theorem indicator_sum_ne_zero_iff (ps : List Point) (f : Point → Bool) :
    (ps.map (fun p => if f p then (1 : Int) else 0)).sum ≠ 0 ↔ ps.any f := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      by_cases h : f p = true
      · have hn := indicator_sum_nonneg ps f
        simp only [h, if_true, List.map_cons, List.sum_cons, List.any_cons,
          Bool.true_or, iff_true]
        omega
      · simp only [h, Bool.false_eq_true, if_false, List.map_cons, List.sum_cons,
          Int.zero_add, List.any_cons, Bool.false_or]
        exact ih

private theorem unionAnti_of_stencil (s : Shape) (b : Int) (ps : List Point)
    (rs : Rows)
    (hst : ∀ p v : Point, bounded b v →
      stencilWeight (s.stencil b p) v = if s.qualifies p v then 1 else 0)
    (hb : ∀ r ∈ rs, bounded b r.1) :
    let es := compile s b (ps.map (fun p => (p, 1)))
    rs.filter (fun r => !covered s ps r.1) = rs.filter (fun r => atPoint es r.1 == 0) := by
  intro es
  apply List.filter_congr
  intro r hr
  have hrv := hb r hr
  have hp := atPoint_compile_probes s b ps r.1 (fun p v hv => hst p v hv) hrv
  have hiff : covered s ps r.1 = true ↔ atPoint es r.1 ≠ 0 := by
    change ps.any (fun p => s.qualifies p r.1) = true ↔ atPoint es r.1 ≠ 0
    rw [hp]
    exact (indicator_sum_ne_zero_iff ps (fun p => s.qualifies p r.1)).symm
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro hc
    have hcf : covered s ps r.1 = false := by simpa using hc
    have hz : atPoint es r.1 = 0 := by
      by_cases he : atPoint es r.1 = 0
      · exact he
      · have hct := hiff.mpr (by intro h; exact he h)
        simp [hcf] at hct
    simp [hcf, hz]
  · intro hz
    have hzero : atPoint es r.1 = 0 := by simpa using hz
    have hcv : covered s ps r.1 = false := by
      cases hc : covered s ps r.1 with
      | false => rfl
      | true => simp [hc, hzero] at hiff
    simp [hcv]

namespace VarianceBudget

open TinylambOpt.ResearchGoals.Atlas.RangePlans.VarianceBudget

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight (shape.stencil b p) v = if shape.qualifies p v then 1 else 0 := by
  simp [shape, box, memBox_eq, stencilWeight, bounded] at hv ⊢
  grind

theorem sound_VarianceBudget_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_stencil shape b p (fun v hv => stencil_id b p v hv) rs hb

theorem sound_VarianceBudget_PageVerdict : PageVerdict.Goal := by
  intro b p g hg hb
  exact pageScan_of_stencil shape b p g hg hb (fun r hr hbr => stencil_id b p r.1 hbr)

theorem sound_VarianceBudget_ProbeTransposition : ProbeTransposition.Goal := by
  intro b ps rs hb
  exact probeTransposition_of_stencil shape b ps rs
    (fun p v hv => stencil_id b p v hv) hb

theorem sound_VarianceBudget_UnionAnti : UnionAnti.Goal := by
  intro b ps rs hb
  exact unionAnti_of_stencil shape b ps rs (fun p v hv => stencil_id b p v hv) hb

end VarianceBudget

namespace AlignedWeek

open TinylambOpt.ResearchGoals.Atlas.RangePlans.AlignedWeek

private theorem pair_beq_true_iff (a b c d : Int) :
    ((a, b) == (c, d)) = true ↔ a = c ∧ b = d := by
  have h : ((a, b) == (c, d)) = decide (a = c ∧ b = d) := rfl
  rw [h, decide_eq_true_eq]

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight (shape.stencil b p) v = if shape.qualifies p v then 1 else 0 := by
  simp [shape, box, memBox_eq, stencilWeight, bounded] at hv ⊢
  grind

theorem sound_AlignedWeek_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_stencil shape b p (fun v hv => stencil_id b p v hv) rs hb

theorem sound_AlignedWeek_PageVerdict : PageVerdict.Goal := by
  intro b p g hg hb
  exact pageScan_of_stencil shape b p g hg hb (fun r hr hbr => stencil_id b p r.1 hbr)

private theorem bucket_filter_map (p : Point) (rs : Rows) :
    ((rs.map (fun r => ((r.1.x / 7, r.1.y / 10), r.2))).filter
      (fun e => e.1 == (p.x / 7, p.y / 10))).map Prod.snd =
      (rs.filter (fun r => (r.1.x / 7, r.1.y / 10) == (p.x / 7, p.y / 10))).map
        Prod.snd := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : (r.1.x / 7, r.1.y / 10) == (p.x / 7, p.y / 10) <;>
        simp [h, ih]

theorem sound_AlignedWeek_BucketEquality : BucketEquality.Goal := by
  intro p rs
  dsimp only
  have hf : rs.filter (fun r => shape.qualifies p r.1) =
      rs.filter (fun r => (r.1.x / 7, r.1.y / 10) == (p.x / 7, p.y / 10)) := by
    apply List.filter_congr
    intro r hr
    apply Bool.eq_iff_iff.mpr
    constructor
    · intro ht
      have hp : r.1.x / 7 = p.x / 7 ∧ r.1.y / 10 = p.y / 10 := by
        simpa [shape] using ht
      exact pair_beq_true_iff _ _ _ _ |>.mpr hp
    · intro ht
      rcases pair_beq_true_iff _ _ _ _ |>.mp ht with ⟨hx, hy⟩
      simp [shape, hx, hy]
  change ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).sum = _
  rw [hf, bucket_filter_map]

theorem sound_AlignedWeek_UnionAnti : UnionAnti.Goal := by
  intro b ps rs hb
  exact unionAnti_of_stencil shape b ps rs (fun p v hv => stencil_id b p v hv) hb

end AlignedWeek

namespace GapSession

open TinylambOpt.ResearchGoals.Atlas.RangePlans.GapSession

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight (shape.stencil b p) v = if shape.qualifies p v then 1 else 0 := by
  simp [shape, box, memBox_eq, stencilWeight, bounded] at hv ⊢
  grind

theorem sound_GapSession_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_stencil shape b p (fun v hv => stencil_id b p v hv) rs hb

theorem sound_GapSession_PageVerdict : PageVerdict.Goal := by
  intro b p g hg hb
  exact pageScan_of_stencil shape b p g hg hb (fun r hr hbr => stencil_id b p r.1 hbr)

theorem sound_GapSession_ProbeTransposition : ProbeTransposition.Goal := by
  intro b ps rs hb
  exact probeTransposition_of_stencil shape b ps rs
    (fun p v hv => stencil_id b p v hv) hb

theorem sound_GapSession_UnionAnti : UnionAnti.Goal := by
  intro b ps rs hb
  exact unionAnti_of_stencil shape b ps rs (fun p v hv => stencil_id b p v hv) hb

end GapSession

namespace OverdueOrLost

open TinylambOpt.ResearchGoals.Atlas.RangePlans.OverdueOrLost

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight (shape.stencil b p) v = if shape.qualifies p v then 1 else 0 := by
  simp [shape, box, memBox_eq, stencilWeight, bounded] at hv ⊢
  grind

theorem sound_OverdueOrLost_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_stencil shape b p (fun v hv => stencil_id b p v hv) rs hb

theorem sound_OverdueOrLost_PageVerdict : PageVerdict.Goal := by
  intro b p g hg hb
  exact pageScan_of_stencil shape b p g hg hb (fun r hr hbr => stencil_id b p r.1 hbr)

theorem sound_OverdueOrLost_ProbeTransposition : ProbeTransposition.Goal := by
  intro b ps rs hb
  exact probeTransposition_of_stencil shape b ps rs
    (fun p v hv => stencil_id b p v hv) hb

theorem sound_OverdueOrLost_UnionAnti : UnionAnti.Goal := by
  intro b ps rs hb
  exact unionAnti_of_stencil shape b ps rs (fun p v hv => stencil_id b p v hv) hb

end OverdueOrLost

namespace AdjacentTiles

open TinylambOpt.ResearchGoals.Atlas.RangePlans.AdjacentTiles

private theorem natAbs_add_one_iff (a b : Int) :
    a.natAbs + b.natAbs = 1 ↔
      (a = 0 ∧ b = 1) ∨ (a = 0 ∧ b = -1) ∨
        (a = 1 ∧ b = 0) ∨ (a = -1 ∧ b = 0) := by
  constructor
  · intro h
    have ha : a.natAbs = 0 ∨ a.natAbs = 1 := by omega
    have hb : b.natAbs = 0 ∨ b.natAbs = 1 := by omega
    rcases ha with ha | ha <;> rcases hb with hb | hb
    · simp [Int.natAbs_eq_zero.mp ha, Int.natAbs_eq_zero.mp hb] at h
    · have hb' : b = 1 ∨ b = -1 := by simpa using Int.natAbs_eq_iff.mp hb
      rcases hb' with rfl | rfl <;> simp [Int.natAbs_eq_zero.mp ha] <;> omega
    · have ha' : a = 1 ∨ a = -1 := by simpa using Int.natAbs_eq_iff.mp ha
      rcases ha' with rfl | rfl <;> simp [Int.natAbs_eq_zero.mp hb] <;> omega
    · have ha' : a = 1 ∨ a = -1 := by simpa using Int.natAbs_eq_iff.mp ha
      have hb' : b = 1 ∨ b = -1 := by simpa using Int.natAbs_eq_iff.mp hb
      omega
  · rintro (⟨rfl, rfl⟩ | ⟨rfl, rfl⟩ | ⟨rfl, rfl⟩ | ⟨rfl, rfl⟩) <;> decide

private theorem qualifies_iff (p v : Point) :
    shape.qualifies p v = true ↔
      v = ⟨p.x - 1, p.y⟩ ∨ v = ⟨p.x + 1, p.y⟩ ∨
        v = ⟨p.x, p.y - 1⟩ ∨ v = ⟨p.x, p.y + 1⟩ := by
  simp only [shape, beq_iff_eq, natAbs_add_one_iff]
  constructor
  · cases v with
    | mk vx vy =>
      rintro (⟨hx, hy⟩ | ⟨hx, hy⟩ | ⟨hx, hy⟩ | ⟨hx, hy⟩)
      all_goals dsimp at hx hy
      · right; right; right
        simp only [Point.mk.injEq]
        constructor <;> omega
      · right; right; left
        simp only [Point.mk.injEq]
        constructor <;> omega
      · right; left
        simp only [Point.mk.injEq]
        constructor <;> omega
      · left
        simp only [Point.mk.injEq]
        constructor <;> omega
  · rintro (rfl | rfl | rfl | rfl) <;> grind

private theorem point_beq_eq (a b : Point) :
    (a == b) = decide (a.x = b.x ∧ a.y = b.y) := by
  cases a
  cases b
  rfl

private theorem point_beq_self (a : Point) : (a == a) = true := by
  rw [point_beq_eq]
  simp

private theorem point_beq_true_iff (a b : Point) : (a == b) = true ↔ a = b := by
  rw [point_beq_eq, decide_eq_true_eq]
  cases a
  cases b
  simp only [Point.mk.injEq]

private theorem point_beq_false_of_ne {a b : Point} (h : a ≠ b) : (a == b) = false := by
  apply Bool.eq_false_of_ne_true
  intro hb
  exact h (point_beq_true_iff a b |>.mp hb)

private theorem neighbor_ne (p : Point) :
    (⟨p.x - 1, p.y⟩ : Point) ≠ ⟨p.x + 1, p.y⟩ ∧
    (⟨p.x - 1, p.y⟩ : Point) ≠ ⟨p.x, p.y - 1⟩ ∧
    (⟨p.x - 1, p.y⟩ : Point) ≠ ⟨p.x, p.y + 1⟩ ∧
    (⟨p.x + 1, p.y⟩ : Point) ≠ ⟨p.x, p.y - 1⟩ ∧
    (⟨p.x + 1, p.y⟩ : Point) ≠ ⟨p.x, p.y + 1⟩ ∧
    (⟨p.x, p.y - 1⟩ : Point) ≠ ⟨p.x, p.y + 1⟩ := by
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_⟩ <;> intro h <;>
    simp only [Point.mk.injEq] at h <;> omega

private theorem row_sparse (p : Point) (r : Point × Int) :
    (if shape.qualifies p r.1 then r.2 else 0) =
      (if r.1 == ⟨p.x - 1, p.y⟩ then r.2 else 0) +
      (if r.1 == ⟨p.x + 1, p.y⟩ then r.2 else 0) +
      (if r.1 == ⟨p.x, p.y - 1⟩ then r.2 else 0) +
      (if r.1 == ⟨p.x, p.y + 1⟩ then r.2 else 0) := by
  rcases r with ⟨v, w⟩
  rcases neighbor_ne p with ⟨n12, n13, n14, n23, n24, n34⟩
  by_cases h1 : v = ⟨p.x - 1, p.y⟩
  · subst v
    have hq : shape.qualifies p ⟨p.x - 1, p.y⟩ = true :=
      (qualifies_iff p _).mpr (Or.inl rfl)
    simp [point_beq_eq, hq, point_beq_self, point_beq_false_of_ne n12,
      point_beq_false_of_ne n13, point_beq_false_of_ne n14]
  · have h1f := point_beq_false_of_ne h1
    by_cases h2 : v = ⟨p.x + 1, p.y⟩
    · subst v
      have hq : shape.qualifies p ⟨p.x + 1, p.y⟩ = true :=
        (qualifies_iff p _).mpr (Or.inr (Or.inl rfl))
      simp [point_beq_eq, hq, point_beq_self, h1f, point_beq_false_of_ne n23,
        point_beq_false_of_ne n24]
    · have h2f := point_beq_false_of_ne h2
      by_cases h3 : v = ⟨p.x, p.y - 1⟩
      · subst v
        have hq : shape.qualifies p ⟨p.x, p.y - 1⟩ = true :=
          (qualifies_iff p _).mpr (Or.inr (Or.inr (Or.inl rfl)))
        simp [point_beq_eq, hq, point_beq_self, h1f, h2f, point_beq_false_of_ne n34]
      · have h3f := point_beq_false_of_ne h3
        by_cases h4 : v = ⟨p.x, p.y + 1⟩
        · subst v
          have hq : shape.qualifies p ⟨p.x, p.y + 1⟩ = true :=
            (qualifies_iff p _).mpr (Or.inr (Or.inr (Or.inr rfl)))
          simp [point_beq_eq, hq, point_beq_self, h1f, h2f, h3f]
        · have h4f := point_beq_false_of_ne h4
          have hq : shape.qualifies p v = false := by
            apply Bool.eq_false_of_ne_true
            intro ht
            rcases (qualifies_iff p v).mp ht with h | h | h | h
            · exact h1 h
            · exact h2 h
            · exact h3 h
            · exact h4 h
          simp [point_beq_eq, hq, h1f, h2f, h3f, h4f]

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight (shape.stencil b p) v = if shape.qualifies p v then 1 else 0 := by
  simp [shape, box, memBox_eq, stencilWeight, bounded] at hv ⊢
  grind

theorem sound_AdjacentTiles_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  exact prefixIndex_of_stencil shape b p (fun v hv => stencil_id b p v hv) rs hb

theorem sound_AdjacentTiles_PageVerdict : PageVerdict.Goal := by
  intro b p g hg hb
  exact pageScan_of_stencil shape b p g hg hb (fun r hr hbr => stencil_id b p r.1 hbr)

theorem sound_AdjacentTiles_SparseNeighbors : SparseNeighbors.Goal := by
  intro p rs
  dsimp only
  induction rs with
  | nil => simp [scan]
  | cons r rs ih =>
      simp only [scan] at ih ⊢
      rw [sum_filter_map_cons, sum_filter_map_cons, sum_filter_map_cons,
        sum_filter_map_cons, sum_filter_map_cons, row_sparse]
      omega

theorem sound_AdjacentTiles_UnionAnti : UnionAnti.Goal := by
  intro b ps rs hb
  exact unionAnti_of_stencil shape b ps rs (fun p v hv => stencil_id b p v hv) hb

end AdjacentTiles

/-! ### Rank selection -/

private theorem countP_le_countP (xs : List Int) (p q : Int → Bool)
    (h : ∀ x ∈ xs, p x = true → q x = true) :
    xs.countP p ≤ xs.countP q := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have ih' := ih (fun y hy => h y (by simp [hy]))
      simp only [List.countP_cons]
      by_cases hp : p x = true
      · have hqt := h x (by simp) hp
        simp [hp, hqt]
        omega
      · by_cases hq : q x = true
        · simp [hp, hq]
          omega
        · simp [hp, hq]
          exact ih'

private theorem countP_lt_eq_pred (xs : List Int) (x : Int) :
    xs.countP (fun y => y < x) = xs.countP (fun y => y ≤ x - 1) := by
  apply congrArg (fun p => xs.countP p)
  funext y
  apply Bool.eq_iff_iff.mpr
  constructor <;> intro h <;> simp only [decide_eq_true_eq] at h ⊢ <;> omega

private theorem sorted_lt_count_le (xs : List Int)
    (hs : xs.Pairwise (fun a b => a ≤ b)) :
    ∀ (k : Nat) (hk : k < xs.length) (a : Int), xs[k]'hk = a →
      (xs.filter (fun y => y < a)).length ≤ k := by
  induction xs with
  | nil => intro k hk; simp at hk
  | cons x xs ih =>
      intro k hk a0 hidx
      rcases List.pairwise_cons.mp hs with ⟨hhead, htail⟩
      cases k with
      | zero =>
          have ha0 : a0 = x := by
            simpa only [List.getElem_cons_zero] using hidx.symm
          subst a0
          have hn : (x :: xs).filter (fun y => y < x) = [] := by
            apply filter_nil_of_forall_not
            intro y hy hlt
            simp only [decide_eq_true_eq] at hlt
            rcases List.mem_cons.mp hy with rfl | hym
            · omega
            · have hle := hhead y hym
              omega
          simp [hn]
      | succ k =>
          have hk' : k < xs.length := by
            simpa only [List.length_cons, Nat.add_lt_add_iff_right] using hk
          have hidx' : xs[k] = a0 := by
            simpa only [List.getElem_cons_succ] using hidx
          have hle := ih htail k hk' a0 hidx'
          rw [List.filter_cons]
          by_cases ha : x < a0 <;> simp [ha] <;> omega

private theorem sorted_kth_le_count (xs : List Int)
    (hs : xs.Pairwise (fun a b => a ≤ b)) :
    ∀ (k : Nat) (hk : k < xs.length) (a : Int), xs[k]'hk = a →
      k < xs.countP (fun y => y ≤ a) := by
  induction xs with
  | nil => intro k hk; simp at hk
  | cons x xs ih =>
      intro k hk a0 hidx
      rcases List.pairwise_cons.mp hs with ⟨hhead, htail⟩
      cases k with
      | zero =>
          have ha0 : a0 = x := by
            simpa only [List.getElem_cons_zero] using hidx.symm
          subst a0
          simp
      | succ k =>
          have hk' : k < xs.length := by
            simpa only [List.length_cons, Nat.add_lt_add_iff_right] using hk
          have hidx' : xs[k] = a0 := by
            simpa only [List.getElem_cons_succ] using hidx
          have hle := ih htail k hk' a0 hidx'
          have ha0 : a0 = xs[k] := hidx'.symm
          subst a0
          have ha : x ≤ xs[k] := hhead xs[k] (List.getElem_mem hk')
          rw [List.countP_cons]
          have hle' : k < xs.countP (fun y => y ≤ xs[k]) := hle
          by_cases hax : x ≤ xs[k] <;> simp [hax] <;> omega

private theorem kth_eq_of_threshold (xs : List Int)
    (hs : xs.Pairwise (fun a b => a ≤ b)) (k : Nat) (hk : k < xs.length) (x : Int)
    (hlt : xs.countP (fun y => y < x) ≤ k)
    (hle : k < xs.countP (fun y => y ≤ x)) : x = xs[k] := by
  let a := xs[k]
  have ha_lt : xs.countP (fun y => y < a) ≤ k := by
    rw [List.countP_eq_length_filter]
    exact sorted_lt_count_le xs hs k hk a rfl
  have ha_le : k < xs.countP (fun y => y ≤ a) :=
    sorted_kth_le_count xs hs k hk a rfl
  change x = a
  by_cases hxa : x < a
  · have hsub : xs.countP (fun y => y ≤ x) ≤ xs.countP (fun y => y < a) := by
      apply countP_le_countP
      intro y hy hle
      simp only [decide_eq_true_eq] at hle ⊢
      omega
    omega
  · by_cases hax : a < x
    · have hsub : xs.countP (fun y => y ≤ a) ≤ xs.countP (fun y => y < x) := by
        apply countP_le_countP
        intro y hy hle
        simp only [decide_eq_true_eq] at hle ⊢
        omega
      omega
    · omega

private theorem pow_log2_le_self (n : Nat) (hn : n ≠ 0) : 2 ^ n.log2 ≤ n :=
  (Nat.le_log2 hn).mp (Nat.le_refl _)

private theorem log2_shrink {w n : Nat} (hn : 2 * n ≤ w) (hw : w ≠ 0)
    (hnpos : n ≠ 0) : n.log2 + 2 ≤ w.log2 + 1 := by
  have hpow : 2 ^ (n.log2 + 1) ≤ w := by
    rw [Nat.pow_succ]
    have hp := pow_log2_le_self n hnpos
    exact Nat.le_trans (Nat.mul_le_mul_right 2 hp) (by simpa [Nat.mul_comm] using hn)
  have hlog := (Nat.le_log2 hw).mpr hpow
  omega

end TinylambOpt.ResearchProofs.Atlas.RangePlans4
