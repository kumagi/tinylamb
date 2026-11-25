import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.RP_ContainsTick_StrictOverlap

open TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
# RP_ContainsTick_StrictOverlap

Goals in `ContainsTick` and `StrictOverlap` of
`TinylambOpt.ResearchGoals.Atlas.RangePlans`.

Infrastructure (verified-style lemmas adapted from
`Atlas_RangePlans_G3`): prefixMass/scan/corners linearity, per-singleton-row
indexed evaluation, and the event-transpose (`atPoint`) identities used by
`UnionAnti`.
-/

private def memBox (q : Box) (v : Point) : Bool :=
  decide (q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1)

private theorem memBox_iff (q : Box) (v : Point) :
    memBox q v = true ↔ q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1 := by
  simp [memBox]

private theorem prefixMass_singleton (r : Point × Int) (x y : Int) :
    prefixMass [r] x y = if r.1.x < x && r.1.y < y then r.2 else 0 := by
  by_cases hx : r.1.x < x <;> by_cases hy : r.1.y < y <;> simp [prefixMass, hx, hy]

private theorem prefixMass_cons (r : Point × Int) (rs : Rows) (x y : Int) :
    prefixMass (r :: rs) x y =
      (if r.1.x < x && r.1.y < y then r.2 else 0) + prefixMass rs x y := by
  by_cases hx : r.1.x < x <;> by_cases hy : r.1.y < y <;> simp [prefixMass, hx, hy]

private theorem prefixMass_append (es₁ es₂ : Rows) (x y : Int) :
    prefixMass (es₁ ++ es₂) x y = prefixMass es₁ x y + prefixMass es₂ x y := by
  induction es₁ with
  | nil => simp [prefixMass]
  | cons e es ih =>
      rw [List.cons_append, prefixMass_cons, prefixMass_cons, ih]
      omega

private theorem scan_cons (s : Shape) (p : Point) (r : Point × Int) (rs : Rows) :
    scan s p (r :: rs) = (if s.qualifies p r.1 then r.2 else 0) + scan s p rs := by
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
    indexed s b p (prefixMass (r :: rs)) =
      indexed s b p (prefixMass [r]) + indexed s b p (prefixMass rs) := by
  unfold indexed
  have hfun : prefixMass (r :: rs) =
      (fun x y => prefixMass [r] x y + prefixMass rs x y) := by
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

/-! ### Singleton corners -/

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

private theorem sum_map_mul_const (ss : List Box) (f : Box → Int) (w : Int) :
    (ss.map (fun q => w * f q)).sum = w * (ss.map f).sum := by
  induction ss with
  | nil => simp
  | cons q qs ih =>
      simp only [List.map_cons, List.sum_cons, ih, Int.mul_add]

private theorem stencil_weight (st : List Box) (r : Point) (w : Int) :
    ((st.map (fun q => q.coeff * (if memBox q r then w else 0))).sum) =
      w * ((st.map (fun q => q.coeff * (if memBox q r then (1 : Int) else 0))).sum) := by
  induction st with
  | nil => simp
  | cons q qs ih =>
      by_cases h : memBox q r = true
      · simp [h, Int.mul_add, ih]
        rw [Int.mul_comm q.coeff w]
      · simp [h, ih]

private theorem indexed_singleton
    (s : Shape) (b : Int) (p v : Point) (w : Int)
    (hst : ((s.stencil b p).map
        (fun q => q.coeff * (if memBox q v then (1 : Int) else 0))).sum =
      if s.qualifies p v then 1 else 0) :
    indexed s b p (prefixMass [(v, w)]) = if s.qualifies p v then w else 0 := by
  unfold indexed
  rw [show (s.stencil b p).map (fun q => corners q (prefixMass [(v, w)])) =
        (s.stencil b p).map (fun q => q.coeff * (if memBox q v then w else 0)) by
        apply List.map_congr_left
        intro q hq
        exact corners_singleton q v w]
  rw [stencil_weight, hst]
  cases hq : s.qualifies p v <;> simp [hq]

/-! ### atPoint / events lemmas (adapted from G3) -/

private def inBox (q : Box) (r : Point) : Prop :=
  q.x0 ≤ r.x ∧ r.x < q.x1 ∧ q.y0 ≤ r.y ∧ r.y < q.y1

private instance (q : Box) (r : Point) : Decidable (inBox q r) := by
  unfold inBox
  infer_instance

private theorem inBox_iff_memBox (q : Box) (r : Point) : inBox q r ↔ memBox q r = true := by
  simp [inBox, memBox_iff]

private theorem memBox_eq_inBox (q : Box) (r : Point) :
    memBox q r = (if inBox q r then true else false) := by
  by_cases h : inBox q r
  · rw [if_pos h]
    have : memBox q r = true := (inBox_iff_memBox q r).mp h
    rw [this]
  · rw [if_neg h]
    have : memBox q r ≠ true := fun hc => h ((inBox_iff_memBox q r).mpr hc)
    cases hm : memBox q r <;> simp [hm] at this ⊢

private theorem if_inBox_iff_memBox {α : Type} (q : Box) (r : Point) (a b : α) :
    (if inBox q r then a else b) = (if memBox q r = true then a else b) := by
  by_cases h : inBox q r
  · have hm : memBox q r = true := (inBox_iff_memBox q r).mp h
    rw [if_pos h, if_pos hm]
  · have hm : ¬ memBox q r = true := fun hc => h ((inBox_iff_memBox q r).mpr hc)
    rw [if_neg h, if_neg hm]

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

private theorem atPoint_compile (s : Shape) (b : Int) (ps : List Point) (r : Point) :
    atPoint (compile s b (ps.map (fun p => (p, 1)))) r =
      (ps.map (fun p =>
        ((s.stencil b p).map (fun q => if inBox q r then (1 : Int) * q.coeff else 0)).sum)).sum := by
  induction ps with
  | nil => simp [compile, atPoint, prefixMass]
  | cons p ps ih =>
      have hstep : compile s b ((p :: ps).map (fun q => (q, 1))) =
          ((s.stencil b p).flatMap (events · 1)) ++
            compile s b (ps.map (fun q => (q, 1))) := by
        simp [compile, List.map_cons, List.flatMap_cons]
      rw [hstep, atPoint_append, atPoint_flatMap_events, ih]
      simp [List.map_cons, List.sum_cons]

private theorem prefixIndex_eq_scan_of_row'
    (s : Shape) (b : Int) (p : Point) (rs : Rows)
    (hrow : ∀ r : Point × Int, bounded b r.1 → r.1.x < r.1.y →
        indexed s b p (prefixMass [r]) = (if s.qualifies p r.1 then r.2 else 0))
    (hb : ∀ r ∈ rs, bounded b r.1 ∧ r.1.x < r.1.y) :
    indexed s b p (prefixMass rs) = scan s p rs := by
  induction rs with
  | nil =>
      simp [indexed, corners, prefixMass, scan]
      induction (s.stencil b p) <;> simp [*]
  | cons r rs ih =>
      have hbr := (hb r (by simp)).1
      have hxr := (hb r (by simp)).2
      have hrs : ∀ r' ∈ rs, bounded b r'.1 ∧ r'.1.x < r'.1.y := by
        intro r' hr'; exact hb r' (by simp [hr'])
      rw [indexed_prefixMass_cons, scan_cons, hrow r hbr hxr, ih hrs]

/-! ### Per-shape stencil weight identities -/

private theorem ct_stencil_one (b : Int) (p v : Point) (hv : bounded b v) :
    ((ContainsTick.shape.stencil b p).map
      (fun q => q.coeff * (if memBox q v then (1 : Int) else 0))).sum =
    if ContainsTick.shape.qualifies p v then 1 else 0 := by
  simp [ContainsTick.shape, box, memBox, bounded] at hv ⊢
  omega

private theorem so_stencil_one (b : Int) (p v : Point) (hv : bounded b v)
    (hx : v.x < v.y) :
    ((StrictOverlap.shape.stencil b p).map
      (fun q => q.coeff * (if memBox q v then (1 : Int) else 0))).sum =
    if StrictOverlap.shape.qualifies p v then 1 else 0 := by
  simp [StrictOverlap.shape, box, memBox, bounded] at hv hx ⊢
  by_cases hp : p.x < p.y <;> simp [hp] <;> omega

private theorem ContainsTick_PrefixIndex_sound : ContainsTick.PrefixIndex.Goal := by
  intro b p rs hb
  refine prefixIndex_eq_scan_of_row _ _ _ _ ?_ hb
  intro r hr
  exact indexed_singleton _ _ _ _ _ (ct_stencil_one b p r.1 hr)

private theorem StrictOverlap_PrefixIndex_sound : StrictOverlap.PrefixIndex.Goal := by
  intro b p rs hb
  refine prefixIndex_eq_scan_of_row' _ _ _ _ ?_ hb
  intro r hbv hx
  exact indexed_singleton _ _ _ _ _
    (so_stencil_one b p r.1 hbv hx)

/-! ### UnionAnti helpers -/

private theorem probe_term_eq (q : Box) (v : Point) :
    (if inBox q v then (1 : Int) * q.coeff else 0) =
      q.coeff * (if memBox q v then (1 : Int) else 0) := by
  rw [if_inBox_iff_memBox]
  by_cases h : memBox q v = true <;> simp [h]

private theorem ct_probe_one (b : Int) (p v : Point) (hv : bounded b v) :
    ((ContainsTick.shape.stencil b p).map
      (fun q => if inBox q v then (1 : Int) * q.coeff else 0)).sum =
    if ContainsTick.shape.qualifies p v then 1 else 0 := by
  rw [show ((ContainsTick.shape.stencil b p).map
        (fun q => if inBox q v then (1 : Int) * q.coeff else 0)) =
      ((ContainsTick.shape.stencil b p).map
        (fun q => q.coeff * (if memBox q v then (1 : Int) else 0))) by
      apply List.map_congr_left
      intro q hq
      exact probe_term_eq q v]
  exact ct_stencil_one b p v hv

private theorem so_probe_one (b : Int) (p v : Point) (hv : bounded b v)
    (hx : v.x < v.y) :
    ((StrictOverlap.shape.stencil b p).map
      (fun q => if inBox q v then (1 : Int) * q.coeff else 0)).sum =
    if StrictOverlap.shape.qualifies p v then 1 else 0 := by
  rw [show ((StrictOverlap.shape.stencil b p).map
        (fun q => if inBox q v then (1 : Int) * q.coeff else 0)) =
      ((StrictOverlap.shape.stencil b p).map
        (fun q => q.coeff * (if memBox q v then (1 : Int) else 0))) by
      apply List.map_congr_left
      intro q hq
      exact probe_term_eq q v]
  exact so_stencil_one b p v hv hx

private theorem sum_map_ite_one {α : Type} (l : List α) (f : α → Bool) :
    (l.map (fun a => if f a then (1 : Int) else 0)).sum =
      ((l.filter f).length : Int) := by
  induction l with
  | nil => simp
  | cons a as ih =>
      by_cases h : f a = true
      · simp [h, ih] <;> omega
      · have h' : f a = false := Bool.of_not_eq_true h
        simp [h', ih] <;> omega

private theorem count_eq_zero_iff {α : Type} (l : List α) (f : α → Bool) :
    ((l.filter f).length : Int) = 0 ↔ ∀ a ∈ l, f a = false := by
  rw [Int.ofNat_eq_zero, List.length_eq_zero_iff, List.filter_eq_nil_iff]
  constructor
  · intro h a ha
    exact Bool.of_not_eq_true (h a ha)
  · intro h a ha
    rw [Bool.not_eq_true]
    exact h a ha

private theorem unionAnti_of_probe
    (s : Shape) (b : Int) (ps : List Point) (rs : Rows) (E : Point → Prop)
    (hprobe : ∀ p v, bounded b v → E v →
      ((s.stencil b p).map
        (fun q => if inBox q v then (1 : Int) * q.coeff else 0)).sum =
      if s.qualifies p v then 1 else 0)
    (hh : ∀ r ∈ rs, bounded b r.1 ∧ E r.1) :
    rs.filter (fun r => !covered s ps r.1) =
      rs.filter (fun r => atPoint (compile s b (ps.map (fun p => (p, 1)))) r.1 == 0) := by
  apply List.filter_congr
  intro r hr
  have hbr := (hh r hr).1
  have hE := (hh r hr).2
  have hcnt : atPoint (compile s b (ps.map (fun p => (p, 1)))) r.1 =
      ((ps.filter (fun p => s.qualifies p r.1)).length : Int) := by
    rw [atPoint_compile]
    rw [show (ps.map (fun p =>
          ((s.stencil b p).map (fun q => if inBox q r.1 then (1 : Int) * q.coeff else 0)).sum)) =
        (ps.map (fun p => if s.qualifies p r.1 then (1 : Int) else 0)) by
        apply List.map_congr_left
        intro p hp
        exact hprobe p r.1 hbr hE]
    exact sum_map_ite_one ps (fun p => s.qualifies p r.1)
  have hcov : covered s ps r.1 = ps.any (fun p => s.qualifies p r.1) := rfl
  have hiff : (atPoint (compile s b (ps.map (fun p => (p, 1)))) r.1 == 0) =
      (!covered s ps r.1) := by
    rw [hcnt, hcov]
    by_cases hlen : ((ps.filter (fun p => s.qualifies p r.1)).length : Int) = 0
    · have hfalse : ∀ p ∈ ps, s.qualifies p r.1 = false :=
        (count_eq_zero_iff ps (fun p => s.qualifies p r.1)).mp hlen
      have hnotany : ps.any (fun p => s.qualifies p r.1) = false := by
        rw [List.any_eq_false]
        intro p hp
        rw [Bool.not_eq_true]
        exact hfalse p hp
      rw [hlen, hnotany]
      simp
    · have hlenN : (ps.filter (fun p => s.qualifies p r.1)).length ≠ 0 := by
        intro h0
        apply hlen
        rw [h0]
        simp
      have hpos : 0 < (ps.filter (fun p => s.qualifies p r.1)).length :=
        Nat.pos_of_ne_zero hlenN
      obtain ⟨p, hp⟩ := List.length_pos_iff_exists_mem.mp hpos
      rw [List.mem_filter] at hp
      have hsome : ∃ p ∈ ps, s.qualifies p r.1 = true := ⟨p, hp.1, hp.2⟩
      obtain ⟨p, hp, hpt⟩ := hsome
      have hany : ps.any (fun p => s.qualifies p r.1) = true :=
        List.any_eq_true.mpr ⟨p, hp, hpt⟩
      rw [hany]
      simp [beq_eq_false_iff_ne.mpr hlen]
  exact hiff.symm

theorem ContainsTick_UnionAnti_sound : ContainsTick.UnionAnti.Goal := by
  intro b ps rs hb
  exact unionAnti_of_probe _ _ _ _ (fun _ => True)
    (fun p v hbv _ => ct_probe_one b p v hbv)
    (fun r hr => ⟨hb r hr, trivial⟩)

theorem StrictOverlap_UnionAnti_sound : StrictOverlap.UnionAnti.Goal := by
  intro b ps rs hb
  exact unionAnti_of_probe _ _ _ _ (fun v => v.x < v.y)
    (fun p v hbv hx => so_probe_one b p v hbv hx)
    (fun r hr => hb r hr)

/-! ### EndpointSweep / StartMinusEnd -/

private theorem ct_endpoint_aux (p : Point) :
    ∀ rs : Rows, (∀ r ∈ rs, r.1.x ≤ r.1.y) →
      ((rs.filter (fun r => ContainsTick.shape.qualifies p r.1)).map Prod.snd).sum =
        ((rs.filter (fun r => r.1.x ≤ p.x)).map Prod.snd).sum -
        ((rs.filter (fun r => r.1.y ≤ p.x)).map Prod.snd).sum := by
  intro rs
  induction rs with
  | nil => intro _; simp
  | cons r rs ih =>
      intro hh
      have hr : r.1.x ≤ r.1.y := hh r (by simp)
      have hrs : ∀ r' ∈ rs, r'.1.x ≤ r'.1.y := by
        intro r' h; exact hh r' (by simp [h])
      by_cases hA : r.1.x ≤ p.x <;> by_cases hB : p.x < r.1.y <;>
        by_cases hC : r.1.y ≤ p.x <;>
        simp [hA, hB, hC, hr, ContainsTick.shape, List.filter_cons, ih hrs] <;> omega

theorem ContainsTick_EndpointSweep_sound : ContainsTick.EndpointSweep.Goal := by
  intro p rs hh
  simpa [scan, ContainsTick.shape] using ct_endpoint_aux p rs hh

private theorem so_endpoint_aux (p : Point) (hp : p.x < p.y) :
    ∀ rs : Rows, (∀ r ∈ rs, r.1.x < r.1.y) →
      ((rs.filter (fun r => StrictOverlap.shape.qualifies p r.1)).map Prod.snd).sum =
        ((rs.filter (fun r => r.1.x < p.y)).map Prod.snd).sum -
        ((rs.filter (fun r => r.1.y ≤ p.x)).map Prod.snd).sum := by
  intro rs
  induction rs with
  | nil => intro _; simp
  | cons r rs ih =>
      intro hh
      have hr : r.1.x < r.1.y := hh r (by simp)
      have hrs : ∀ r' ∈ rs, r'.1.x < r'.1.y := by
        intro r' h; exact hh r' (by simp [h])
      by_cases hA : r.1.x < p.y <;> by_cases hB : r.1.y ≤ p.x <;>
        simp [hA, hB, hr, hp, StrictOverlap.shape, List.filter_cons, ih hrs,
              max_lt_iff, lt_min_iff] <;> omega

theorem StrictOverlap_StartMinusEnd_sound : StrictOverlap.StartMinusEnd.Goal := by
  intro p rs hp hh
  simpa [scan, StrictOverlap.shape] using so_endpoint_aux p hp rs hh

/-! ### StrictOverlap.PageVerdict counterexample (documented in issues/research/atlas.md) -/

private def cexPage : Page := ⟨⟨1, 1⟩, ⟨1, 1⟩, [((⟨1, 1⟩ : Point), 7)]⟩
private def cexProbe : Point := ⟨0, 2⟩

theorem StrictOverlap_PageVerdict_disproof :
    ¬ TinylambOpt.ResearchGoals.Atlas.RangePlans.StrictOverlap.PageVerdict.Goal := by
  intro h
  have hv : valid cexPage := by
    intro r hr
    simp [cexPage] at hr
    subst hr
    simp [valid, cexPage]
  have hb : ∀ r ∈ cexPage.rows, bounded 2 r.1 := by
    intro r hr
    simp [cexPage] at hr
    subst hr
    show bounded 2 (⟨1, 1⟩ : Point)
    unfold bounded
    decide
  have hh := h 2 cexProbe cexPage hv hb
  have hneq : ¬ (pageScan StrictOverlap.shape 2 cexProbe cexPage =
      scan StrictOverlap.shape cexProbe cexPage.rows) := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.DevinBatch.RP_ContainsTick_StrictOverlap
