import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Claude1125.RangeRankSeek

open TinylambOpt.ResearchGoals.Atlas.RangePlans

/-! Shared finite-list and two-dimensional prefix-sum algebra. -/

private def boxIn (q : Box) (v : Point) : Bool :=
  decide (q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1)

theorem point_beq_true_iff (a b : Point) :
    (a == b) = true ↔ a.x = b.x ∧ a.y = b.y := by
  change ((a.x == b.x) && (a.y == b.y)) = true ↔ _
  simp only [Bool.and_eq_true, beq_iff_eq]

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

private theorem prefixMass_append (rs ts : Rows) (x y : Int) :
    prefixMass (rs ++ ts) x y = prefixMass rs x y + prefixMass ts x y := by
  induction rs with
  | nil => simp [prefixMass]
  | cons r rs ih =>
    rw [List.cons_append, prefixMass_cons, prefixMass_cons, ih]
    omega

private theorem prefixMass_box (rs : Rows) (x0 x1 y0 y1 : Int) (hx : x0 < x1) (hy : y0 < y1) :
    prefixMass rs x1 y1 - prefixMass rs x0 y1 - prefixMass rs x1 y0 + prefixMass rs x0 y0 =
      ((rs.filter (fun r => boxIn (box x0 x1 y0 y1) r.1)).map Prod.snd).sum := by
  rw [sum_filter_eq_sum_if]
  simp only [boxIn, box, decide_eq_true_eq]
  induction rs with
  | nil => simp [prefixMass]
  | cons r rs ih =>
    rw [prefixMass_cons, prefixMass_cons, prefixMass_cons, prefixMass_cons]
    simp only [List.map_cons, List.sum_cons]
    by_cases hx1 : r.1.x < x1 <;> by_cases hx0 : r.1.x < x0 <;>
      by_cases hy1 : r.1.y < y1 <;> by_cases hy0 : r.1.y < y0 <;>
      by_cases hxloP : x0 ≤ r.1.x <;> by_cases hyloP : y0 ≤ r.1.y <;>
      simp [hx1, hx0, hy1, hy0, hxloP, hyloP, decide_eq_true_eq] <;> first | exact ih | omega
    all_goals trace_state

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
  · rw [if_pos h]
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
  · rw [if_neg h]
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

private theorem sum_map_mul_const {α : Type} (xs : List α) (f : α → Int) (w : Int) :
    (xs.map (fun x => w * f x)).sum = w * (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih, Int.mul_add]

private theorem sum_map_snd_const (rs : Rows) (c : Int) :
    (rs.map (fun r => c * r.2)).sum = c * (rs.map Prod.snd).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih => simp [ih, Int.mul_add]

private theorem sum_map_mul_const' {α : Type} (xs : List α) (f : α → Int) (w : Int) :
    (xs.map (fun x => f x * w)).sum = (xs.map f).sum * w := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih, Int.add_mul]

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih] <;> omega

private theorem sum_map_zero {β : Type} (bs : List β) :
    (bs.map (fun _ => (0 : Int))).sum = 0 := by
  induction bs with
  | nil => rfl
  | cons b bs ih => simp [ih]

private theorem mul_sum_if (rs : Rows) (c : Int) (f : Point × Int → Bool) :
    c * (rs.map (fun r => if f r then r.2 else 0)).sum =
      (rs.map (fun r => if f r then c * r.2 else 0)).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
    by_cases h : f r = true <;> simp [h, ih, Int.mul_add]

private theorem sum_congr {α : Type} (xs : List α) (f g : α → Int)
    (h : ∀ x ∈ xs, f x = g x) : (xs.map f).sum = (xs.map g).sum :=
  congrArg List.sum (List.map_congr_left h)

private theorem sum_map_le {α : Type} (xs : List α) (f g : α → Int)
    (h : ∀ x ∈ xs, f x ≤ g x) : (xs.map f).sum ≤ (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    simp only [List.map_cons, List.sum_cons]
    have hx := h x (by simp)
    have ht := ih (fun y hy => h y (by simp [hy]))
    omega

private theorem sum_map_sub {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x - g x)).sum = (xs.map f).sum - (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih] <;> omega

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

private theorem scan_eq_sum_weight_of_stencil (s : Shape) (b : Int) (p : Point) (rs : Rows)
    (hst : ∀ v, bounded b v →
      stencilWeight s b p v = if s.qualifies p v then (1 : Int) else 0)
    (hb : ∀ r ∈ rs, bounded b r.1) :
    scan s p rs = (rs.map (fun r => stencilWeight s b p r.1 * r.2)).sum := by
  rw [← prefixIndex_of_weight s b p rs (fun r hr => hst r.1 (hb r hr))]
  exact indexed_eq_sum_weight s b p rs

/-! Page headers are exact whenever every stencil rectangle has a uniform verdict. -/

private theorem uniform_cases (g : Page) (q : Box) (c : Int) (hvalid : valid g)
    (hg : uniform g q = some c) :
    (c = 0 ∧ ∀ r ∈ g.rows, boxIn q r.1 = false) ∨
      (q.x0 ≤ g.lower.x ∧ g.upper.x < q.x1 ∧
        q.y0 ≤ g.lower.y ∧ g.upper.y < q.y1 ∧ c = q.coeff ∧
        ∀ r ∈ g.rows, boxIn q r.1 = true) := by
  unfold uniform at hg
  by_cases hz : q.x1 ≤ q.x0 || q.y1 ≤ q.y0 || g.upper.x < q.x0 || q.x1 ≤ g.lower.x ||
      g.upper.y < q.y0 || q.y1 ≤ g.lower.y
  · rw [if_pos hz] at hg
    have hc : c = 0 := by simpa [eq_comm] using hg
    refine Or.inl ⟨hc, ?_⟩
    intro r hr
    have hv := hvalid r hr
    have hz' : q.x1 ≤ q.x0 ∨ q.y1 ≤ q.y0 ∨ g.upper.x < q.x0 ∨
        q.x1 ≤ g.lower.x ∨ g.upper.y < q.y0 ∨ q.y1 ≤ g.lower.y := by
      simp only [Bool.or_eq_true, decide_eq_true_eq, or_assoc] at hz
      exact hz
    rcases hz' with hz | hz | hz | hz | hz | hz <;>
      simp [boxIn, decide_eq_true_eq] <;> omega
  · rw [if_neg hz] at hg
    by_cases hf : q.x0 ≤ g.lower.x && g.upper.x < q.x1 &&
        q.y0 ≤ g.lower.y && g.upper.y < q.y1
    · rw [if_pos hf] at hg
      have hc : c = q.coeff := by simpa [eq_comm] using hg
      have hf' : q.x0 ≤ g.lower.x ∧ g.upper.x < q.x1 ∧
          q.y0 ≤ g.lower.y ∧ g.upper.y < q.y1 := by
        simp only [Bool.and_eq_true, decide_eq_true_eq, and_assoc] at hf
        exact hf
      rcases hf' with ⟨hx0, hx1, hy0, hy1⟩
      refine Or.inr ⟨hx0, hx1, hy0, hy1, hc, ?_⟩
      intro r hr
      have hv := hvalid r hr
      simp [boxIn, decide_eq_true_eq]
      omega
    · simp [hf] at hg

private theorem sum_if_one {α : Type} (xs : List α) (f : α → Bool) :
    (xs.map (fun x => if f x then (1 : Int) else 0)).sum =
      (xs.filter f).length := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
    by_cases h : f x = true <;> simp [h, ih] <;> omega

private theorem sum_map_one {α : Type} (xs : List α) :
    (xs.map (fun _ => (1 : Int))).sum = xs.length := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih] <;> omega

private theorem mapM_eq_some_map {α β : Type} (f : α → Option β) (g : α → β)
    (xs : List α) (h : ∀ a ∈ xs, f a = some (g a)) :
    xs.mapM f = some (xs.map g) := by
  induction xs with
  | nil => rfl
  | cons a xs ih =>
    rw [List.mapM_cons, h a (by simp), ih (fun b hb => h b (by simp [hb]))]
    rfl

private theorem all_some_of_mapM_eq_some {α β : Type} (f : α → Option β)
    (xs : List α) (ys : List β) (h : xs.mapM f = some ys) :
    ∀ a ∈ xs, ∃ b, f a = some b := by
  induction xs generalizing ys with
  | nil =>
    intro a ha
    simp at ha
  | cons a xs ih =>
    cases ha : f a with
    | none => simp [List.mapM_cons, ha] at h
    | some b =>
      cases ht : xs.mapM f with
      | none => simp [List.mapM_cons, ha, ht] at h
      | some zs =>
        have htail : xs.mapM f = some zs := ht
        intro a' ha'
        rcases List.mem_cons.mp ha' with rfl | ha'
        · exact ⟨b, ha⟩
        · exact ih zs htail a' ha'

private theorem pageScan_eq_scan (s : Shape) (b : Int) (p : Point) (g : Page)
    (hst : ∀ v, bounded b v →
      stencilWeight s b p v = if s.qualifies p v then (1 : Int) else 0)
    (hb : ∀ r ∈ g.rows, bounded b r.1) (hg : valid g) :
    pageScan s b p g = scan s p g.rows := by
  unfold pageScan
  split
  · rename_i ws hws
    have hs := all_some_of_mapM_eq_some (uniform g) (s.stencil b p) ws hws
    let cq : Box → Int := fun q => ((uniform g q).getD 0)
    have hcq : ∀ q ∈ s.stencil b p, uniform g q = some (cq q) := by
      intro q hq
      rcases hs q hq with ⟨c, hc⟩
      simp [cq, hc]
    have hm : (s.stencil b p).mapM (uniform g) =
        some ((s.stencil b p).map cq) :=
      mapM_eq_some_map (uniform g) cq (s.stencil b p) hcq
    have hws' : ws = (s.stencil b p).map cq := by
      have hsome : some ws = some ((s.stencil b p).map cq) := hws.symm.trans hm
      exact Option.some.inj hsome
    rw [hws']
    have hpoint : ∀ q ∈ s.stencil b p, ∀ r ∈ g.rows,
        (if boxIn q r.1 then q.coeff else 0) = cq q := by
      intro q hq r hr
      rcases uniform_cases g q (cq q) hg (hcq q hq) with h | h
      · rcases h with ⟨hc, hall⟩
        simp [hc, hall r hr]
      · rcases h with ⟨_, _, _, _, hc, hall⟩
        simp [hc, hall r hr]
    have hscan : scan s p g.rows =
        ((s.stencil b p).map cq).sum * (g.rows.map Prod.snd).sum := by
      rw [scan_eq_sum_weight_of_stencil s b p g.rows hst hb]
      have hmap : g.rows.map (fun r => stencilWeight s b p r.1 * r.2) =
          g.rows.map (fun r => ((s.stencil b p).map cq).sum * r.2) := by
        apply List.map_congr_left
        intro r hr
        congr 1
        unfold stencilWeight
        apply sum_congr
        intro q hq
        exact hpoint q hq r hr
      rw [hmap]
      exact sum_map_snd_const g.rows ((s.stencil b p).map cq).sum
    rw [hscan]
  · rfl

/-! Event transposition and unit-weight anti-join coverage. -/

private theorem atPoint_append (es₁ es₂ : Rows) (r : Point) :
    atPoint (es₁ ++ es₂) r = atPoint es₁ r + atPoint es₂ r := by
  simp [atPoint, prefixMass_append]

private theorem atPoint_events (q : Box) (w : Int) (r : Point) :
    atPoint (events q w) r = if boxIn q r then w * q.coeff else 0 := by
  unfold events
  by_cases hb : q.x0 < q.x1 && q.y0 < q.y1
  · have hbox : q.x0 < q.x1 ∧ q.y0 < q.y1 := by simpa using hb
    simp [hb, atPoint, prefixMass]
    by_cases hm : q.x0 ≤ r.x ∧ r.x < q.x1 ∧ q.y0 ≤ r.y ∧ r.y < q.y1
    · have hmd : decide (q.x0 ≤ r.x ∧ r.x < q.x1 ∧ q.y0 ≤ r.y ∧ r.y < q.y1) = true := by
        simp [hm]
      simp only [boxIn, hmd, if_true]
      by_cases hx0 : q.x0 < r.x + 1 <;> by_cases hx1 : q.x1 < r.x + 1 <;>
        by_cases hy0 : q.y0 < r.y + 1 <;> by_cases hy1 : q.y1 < r.y + 1 <;>
        simp [hx0, hx1, hy0, hy1] <;> omega
    · have hmd : decide (q.x0 ≤ r.x ∧ r.x < q.x1 ∧ q.y0 ≤ r.y ∧ r.y < q.y1) = false := by
        simp [hm]
      simp only [boxIn, hmd, if_false]
      have hout : r.x < q.x0 ∨ q.x1 ≤ r.x ∨ r.y < q.y0 ∨ q.y1 ≤ r.y := by omega
      rcases hout with hout | hout | hout | hout
      · by_cases hx0 : q.x0 < r.x + 1 <;> by_cases hx1 : q.x1 < r.x + 1 <;>
          by_cases hy0 : q.y0 < r.y + 1 <;> by_cases hy1 : q.y1 < r.y + 1 <;>
          simp [hx0, hx1, hy0, hy1] <;> omega
      · by_cases hx0 : q.x0 < r.x + 1 <;> by_cases hx1 : q.x1 < r.x + 1 <;>
          by_cases hy0 : q.y0 < r.y + 1 <;> by_cases hy1 : q.y1 < r.y + 1 <;>
          simp [hx0, hx1, hy0, hy1] <;> omega
      · by_cases hx0 : q.x0 < r.x + 1 <;> by_cases hx1 : q.x1 < r.x + 1 <;>
          by_cases hy0 : q.y0 < r.y + 1 <;> by_cases hy1 : q.y1 < r.y + 1 <;>
          simp [hx0, hx1, hy0, hy1] <;> omega
      · by_cases hx0 : q.x0 < r.x + 1 <;> by_cases hx1 : q.x1 < r.x + 1 <;>
          by_cases hy0 : q.y0 < r.y + 1 <;> by_cases hy1 : q.y1 < r.y + 1 <;>
          simp [hx0, hx1, hy0, hy1] <;> omega
  · have hb' : ¬ (q.x0 < q.x1 ∧ q.y0 < q.y1) := by simpa using hb
    simp [hb, atPoint, prefixMass]
    simp only [boxIn, decide_eq_true_eq]
    omega

private theorem atPoint_flatMap_events (ss : List Box) (w : Int) (r : Point) :
    atPoint (ss.flatMap (events · w)) r =
      (ss.map (fun q => if boxIn q r then w * q.coeff else 0)).sum := by
  induction ss with
  | nil => simp [atPoint, prefixMass]
  | cons q qs ih =>
    simp only [List.flatMap_cons]
    rw [atPoint_append, atPoint_events, ih]
    simp

private theorem atPoint_compile (s : Shape) (b : Int) (ps : Rows) (r : Point) :
    atPoint (compile s b ps) r =
      (ps.map (fun pr => pr.2 * stencilWeight s b pr.1 r)).sum := by
  unfold compile
  induction ps with
  | nil => simp [atPoint, prefixMass]
  | cons pr ps ih =>
    simp only [List.flatMap_cons]
    rw [atPoint_append, ih]
    simp only [List.map_cons, List.sum_cons]
    have hw : ((s.stencil b pr.1).map
        (fun q => if boxIn q r then pr.2 * q.coeff else 0)).sum =
        pr.2 * stencilWeight s b pr.1 r := by
      unfold stencilWeight
      induction s.stencil b pr.1 with
      | nil => simp
      | cons q qs ihq =>
        by_cases h : boxIn q r = true
        · simp [h, ihq, Int.mul_add]
        · simp [h, ihq]
    rw [atPoint_flatMap_events, hw]

private theorem probeTransposition (s : Shape) (b : Int) (ps rs : Rows)
    (hst : ∀ p v, bounded b v →
      stencilWeight s b p v = if s.qualifies p v then (1 : Int) else 0)
    (hb : ∀ r ∈ rs, bounded b r.1) :
    (ps.map (fun (p, w) => w * scan s p rs)).sum =
      (rs.map (fun (r, w) => w * atPoint (compile s b ps) r)).sum := by
  have hleft : ps.map (fun (p, w) => w * scan s p rs) =
      ps.map (fun pr => pr.2 * (rs.map (fun rr => stencilWeight s b pr.1 rr.1 * rr.2)).sum) := by
    apply List.map_congr_left
    rintro ⟨p, w⟩ _
    dsimp only
    rw [scan_eq_sum_weight_of_stencil s b p rs (hst p) hb]
  have hright : rs.map (fun (r, w) => w * atPoint (compile s b ps) r) =
      rs.map (fun rr => rr.2 * (ps.map (fun pr => pr.2 * stencilWeight s b pr.1 rr.1)).sum) := by
    apply List.map_congr_left
    rintro ⟨r, w⟩ _
    dsimp only
    rw [atPoint_compile]
  rw [hleft, hright]
  change (ps.map (fun pr => pr.2 *
      (rs.map (fun rr => stencilWeight s b pr.1 rr.1 * rr.2)).sum)).sum =
    (rs.map (fun rr => rr.2 *
      (ps.map (fun pr => pr.2 * stencilWeight s b pr.1 rr.1)).sum)).sum
  have hs1 : (ps.map (fun pr => pr.2 *
      (rs.map (fun rr => stencilWeight s b pr.1 rr.1 * rr.2)).sum)).sum =
      (ps.map (fun pr => (rs.map (fun rr => pr.2 * (stencilWeight s b pr.1 rr.1 * rr.2))).sum)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro pr _
    symm
    exact sum_map_mul_const rs (fun rr => stencilWeight s b pr.1 rr.1 * rr.2) pr.2
  have hs2 : (ps.map (fun pr => (rs.map (fun rr => pr.2 * (stencilWeight s b pr.1 rr.1 * rr.2))).sum)).sum =
      (rs.map (fun rr => (ps.map (fun pr => pr.2 * (stencilWeight s b pr.1 rr.1 * rr.2))).sum)).sum := by
    simpa only using sum_swap ps rs (fun pr rr => pr.2 * (stencilWeight s b pr.1 rr.1 * rr.2))
  rw [hs1, hs2]
  apply sum_congr
  intro rr _
  have hmul : (ps.map (fun pr => pr.2 * (stencilWeight s b pr.1 rr.1 * rr.2))).sum =
      (ps.map (fun pr => (pr.2 * stencilWeight s b pr.1 rr.1) * rr.2)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro pr _
    simp [Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
  rw [hmul, sum_map_mul_const']
  simp [Int.mul_comm]

private theorem atPoint_compile_units (s : Shape) (b : Int) (ps : List Point) (r : Point)
    (P : Point → Prop)
    (hst : ∀ p v, P v → bounded b v →
      stencilWeight s b p v = if s.qualifies p v then (1 : Int) else 0)
    (hP : P r) (hr : bounded b r) :
    atPoint (compile s b (ps.map (fun p => (p, 1)))) r =
      (ps.countP (fun p => s.qualifies p r) : Int) := by
  rw [atPoint_compile]
  have hmap : (ps.map (fun p => (p, (1 : Int)))).map
      (fun pr => pr.2 * stencilWeight s b pr.1 r) =
      ps.map (fun p => if s.qualifies p r then (1 : Int) else 0) := by
    rw [List.map_map]
    apply List.map_congr_left
    intro p _
    simp [hst p r hP hr]
  rw [hmap, List.countP_eq_length_filter]
  exact sum_if_one ps (fun p => s.qualifies p r)

private theorem unionAnti_of_stencil (s : Shape) (b : Int) (ps : List Point) (rs : Rows)
    (P : Point → Prop)
    (hst : ∀ p v, P v → bounded b v →
      stencilWeight s b p v = if s.qualifies p v then (1 : Int) else 0)
    (hP : ∀ r ∈ rs, P r.1) (hb : ∀ r ∈ rs, bounded b r.1) :
    let es := compile s b (ps.map (fun p => (p, 1)))
    rs.filter (fun r => !covered s ps r.1) = rs.filter (fun r => atPoint es r.1 == 0) := by
  apply List.filter_congr
  intro r hr
  rw [atPoint_compile_units s b ps r.1 P hst (hP r hr) (hb r hr)]
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro hnot
    have hcf : covered s ps r.1 = false := by simpa using hnot
    have hz : ps.countP (fun p => s.qualifies p r.1) = 0 := by
      apply List.countP_eq_zero.mpr
      intro p hp
      have hqual : ¬ (s.qualifies p r.1 = true) := by
        intro hq
        have hct : covered s ps r.1 = true :=
          List.any_eq_true.mpr ⟨p, hp, hq⟩
        exact Bool.noConfusion (hct.symm.trans hcf)
      exact hqual
    simp [hz]
  · intro hzero
    have hz : ps.countP (fun p => s.qualifies p r.1) = 0 := by
      exact_mod_cast (show (ps.countP (fun p => s.qualifies p r.1) : Int) = 0 by simpa using hzero)
    have hcf : covered s ps r.1 = false := by
      apply Bool.eq_false_of_ne_true
      intro hct
      have hp : 0 < ps.countP (fun p => s.qualifies p r.1) :=
        List.countP_pos_iff.mpr (by simpa [covered] using hct)
      omega
    simp [hcf]

/-! Rank seeking: a logarithmic-fuel search for the first integer whose rank
exceeds `k`.  The rank threshold lemma is proved for sorted lists, not by
computation. -/

private theorem lt_two_pow_log2_succ (n : Nat) : n < 2 ^ (n.log2 + 1) := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
    rw [Nat.log2_def]
    by_cases h2 : 2 ≤ n
    · rw [if_pos h2, Nat.pow_succ]
      have hhalf := ih (n / 2) (Nat.div_lt_self (Nat.le_of_lt h2) (by decide))
      have hmod := Nat.mod_lt n (by decide : 0 < 2)
      have hadd := Nat.mod_add_div n 2
      omega
    · have hn : n < 2 := by omega
      have hcases : n = 0 ∨ n = 1 := by omega
      rcases hcases with rfl | rfl <;> decide

private theorem seek_eq_target (rank : Int → Int) (k : Nat) (target : Int)
    (hrank : ∀ x, rank x ≤ (k : Int) ↔ x < target) :
    ∀ fuel lo hi, lo ≤ target → target ≤ hi → (hi - lo).toNat < 2 ^ fuel →
      seek rank k fuel lo hi = target := by
  intro fuel
  induction fuel with
  | zero =>
    intro lo hi hlo hhi hd
    have hzero : (hi - lo).toNat = 0 := by omega
    have heq : hi = lo := by omega
    simp only [seek]
    omega
  | succ fuel ih =>
    intro lo hi hlo hhi hd
    by_cases hstop : hi ≤ lo
    · simp [seek, hstop]
      omega
    · simp only [seek, hstop, if_false]
      let mid := (lo + hi) / 2
      have hd' : hi - lo < (2 : Int) * (2 ^ fuel : Nat) := by
        have hd2 : (hi - lo).toNat < 2 ^ fuel * 2 := by
          simpa [Nat.pow_succ, Nat.mul_comm] using hd
        have hd3 : ((hi - lo).toNat : Int) < ((2 ^ fuel * 2 : Nat) : Int) := by
          exact_mod_cast hd2
        have hnon : 0 ≤ hi - lo := by omega
        rw [Int.toNat_of_nonneg hnon] at hd3
        have htmp : hi - lo < (2 ^ fuel : Nat) * 2 := by
          simpa [Int.natCast_mul] using hd3
        omega
      have hm := Int.mul_ediv_add_emod (lo + hi) 2
      have hr0 := Int.emod_nonneg (lo + hi) (by decide : (2 : Int) ≠ 0)
      have hr2 := Int.emod_lt_of_pos (lo + hi) (by decide : (0 : Int) < 2)
      by_cases hk : (k : Int) < rank mid
      · rw [if_pos hk]
        apply ih lo mid hlo ?_ ?_
        · by_cases hlt : mid < target
          · have hle : rank mid ≤ (k : Int) := (hrank mid).mpr hlt
            omega
          · omega
        · change (mid - lo).toNat < 2 ^ fuel
          dsimp [mid] at *
          omega
      · rw [if_neg hk]
        have hle : rank mid ≤ (k : Int) := by omega
        have hmt : mid < target := (hrank mid).mp hle
        apply ih (mid + 1) hi ?_ hhi ?_
        · omega
        · change (hi - (mid + 1)).toNat < 2 ^ fuel
          dsimp [mid] at *
          omega

private theorem count_le_index_iff (xs : List Int) (hs : xs.Pairwise (· ≤ ·))
    (k : Nat) (hk : k < xs.length) (x : Int) :
    xs.countP (fun a => a ≤ x) ≤ k ↔ x < xs[k] := by
  induction xs generalizing k with
  | nil => simp at hk
  | cons a as ih =>
    rcases List.pairwise_cons.mp hs with ⟨ha, ht⟩
    cases k with
    | zero =>
      by_cases hax : a ≤ x
      · have hcount : (a :: as).countP (fun b => b ≤ x) =
          1 + as.countP (fun b => b ≤ x) := by
          simp [List.countP_cons, hax, Nat.add_comm]
        rw [hcount]
        change (1 + as.countP (fun b => b ≤ x) ≤ 0) ↔ x < a
        omega
      · have hnone : as.countP (fun b => b ≤ x) = 0 := by
          apply List.countP_eq_zero.mpr
          intro b hb
          have hab := ha b hb
          simp only [decide_eq_true_eq]
          omega
        have hcount : (a :: as).countP (fun b => b ≤ x) = 0 := by
          simp [List.countP_cons, hax, hnone]
        rw [hcount]
        change (0 ≤ 0) ↔ x < a
        omega
    | succ k =>
      have hk' : k < as.length := by simpa using hk
      by_cases hax : a ≤ x
      · have hcount : (a :: as).countP (fun b => b ≤ x) =
            1 + as.countP (fun b => b ≤ x) := by
          simp [List.countP_cons, hax, Nat.add_comm]
        rw [hcount]
        change (1 + as.countP (fun b => b ≤ x) ≤ k + 1) ↔ x < as[k]
        constructor
        · intro h
          exact (ih ht k hk').mp (by omega)
        · intro h
          have h' := (ih ht k hk').mpr h
          omega
      · have hnone : as.countP (fun b => b ≤ x) = 0 := by
          apply List.countP_eq_zero.mpr
          intro b hb
          have hab := ha b hb
          simp only [decide_eq_true_eq]
          omega
        have hgt : ¬ as[k] ≤ x := by
          have hz := List.countP_eq_zero.mp hnone as[k] (List.getElem_mem hk')
          simpa only [decide_eq_true_eq] using hz
        have hcount : (a :: as).countP (fun b => b ≤ x) = 0 := by
          simp [List.countP_cons, hax, hnone]
        rw [hcount]
        rw [List.getElem_cons_succ]
        change (0 ≤ k + 1) ↔ x < as[k]
        omega

private theorem beforeScore_eq_count (s : Shape) (b : Int) (p : Point) (rs : Rows) (x : Int)
    (P : Point → Prop)
    (hst : ∀ v, P v → bounded b v →
      stencilWeight s b p v = if s.qualifies p v then (1 : Int) else 0)
    (hP : ∀ r ∈ rs, P r.1) (hb : ∀ r ∈ rs, bounded b r.1) :
    beforeScore s b p rs x =
      (((rs.filter (fun r => s.qualifies p r.1)).filter (fun r => r.2 ≤ x)).length : Int) := by
  unfold beforeScore
  let rs' := (rs.filter (fun r => r.2 ≤ x)).map (fun r => (r.1, (1 : Int)))
  have hi := prefixIndex_of_weight s b p rs' (fun r hr => by
    rcases List.mem_map.mp hr with ⟨r0, hr0, heq0⟩
    subst r
    exact hst r0.1 (hP r0 (List.mem_filter.mp hr0).1)
      (hb r0 (List.mem_filter.mp hr0).1))
  rw [hi]
  have hscan : scan s p rs' =
      (((rs.filter (fun r => s.qualifies p r.1)).filter (fun r => r.2 ≤ x)).length : Int) := by
    unfold scan rs'
    simp [List.filter_map, List.map_map, List.filter_filter, Function.comp_def, sum_map_one, Bool.and_comm]
  exact hscan

private theorem rankSeek_of_stencil (s : Shape) (b : Int) (p : Point) (rs : Rows)
    (lo hi : Int) (k : Nat) (P : Point → Prop)
    (hst : ∀ v, P v → bounded b v →
      stencilWeight s b p v = if s.qualifies p v then (1 : Int) else 0)
    (hP : ∀ r ∈ rs, P r.1)
    (hb : ∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi)
    (hk : k < ((rs.filter (fun r => s.qualifies p r.1)).map Prod.snd).mergeSort.length) :
    ((rs.filter (fun r => s.qualifies p r.1)).map Prod.snd).mergeSort[k]? = some
      (seek (beforeScore s b p rs) k ((hi - lo).toNat.log2 + 2) lo hi) := by
  let matched := rs.filter (fun r => s.qualifies p r.1)
  let scores := matched.map Prod.snd
  let sorted := scores.mergeSort
  have hsorted : sorted.Pairwise (· ≤ ·) := by
    have h := List.pairwise_mergeSort (le := fun a b : Int => decide (a ≤ b))
      (fun a b c hab hbc => decide_eq_true (Int.le_trans (of_decide_eq_true hab) (of_decide_eq_true hbc)))
      (fun a b => by simp [Bool.or_eq_true, decide_eq_true_eq]; omega) scores
    simpa [sorted] using h
  have hk' : k < sorted.length := by simpa [sorted, scores, matched] using hk
  have hmem : ∀ z ∈ sorted, lo ≤ z ∧ z ≤ hi := by
    intro z hz
    rw [List.mem_mergeSort] at hz
    rcases List.mem_map.mp hz with ⟨r, hr, rfl⟩
    exact ⟨(hb r (List.mem_filter.mp hr).1).2.1,
      (hb r (List.mem_filter.mp hr).1).2.2⟩
  have htarget := hmem sorted[k] (List.getElem_mem hk')
  have hthresh : ∀ x, beforeScore s b p rs x ≤ (k : Int) ↔ x < sorted[k] := by
    intro x
    rw [beforeScore_eq_count s b p rs x P hst hP (fun r hr => (hb r hr).1)]
    have hc : sorted.countP (fun z => z ≤ x) =
        matched.countP (fun r => r.2 ≤ x) := by
      simpa [sorted, scores, List.countP_map, Function.comp_def] using
        (List.mergeSort_perm scores (fun a b => decide (a ≤ b))).countP_eq
          (fun z => decide (z ≤ x))
    rw [← List.countP_eq_length_filter, ← hc]
    exact_mod_cast count_le_index_iff sorted hsorted k hk' x
  have hfuel : (hi - lo).toNat < 2 ^ ((hi - lo).toNat.log2 + 2) := by
    have hbase := lt_two_pow_log2_succ (hi - lo).toNat
    rw [show (hi - lo).toNat.log2 + 2 = ((hi - lo).toNat.log2 + 1) + 1 by omega,
      Nat.pow_succ]
    omega
  rw [List.getElem?_eq_getElem hk']
  apply congrArg some
  exact (seek_eq_target (beforeScore s b p rs) k sorted[k] hthresh
    ((hi - lo).toNat.log2 + 2) lo hi htarget.1 htarget.2 hfuel).symm


namespace AdjacentTiles

open TinylambOpt.ResearchGoals.Atlas.RangePlans.AdjacentTiles

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, boxIn, stencilWeight, bounded] at hv ⊢
  omega

theorem sound_RankSeek : RankSeek.Goal := by
  intro b p rs lo hi k hb
  dsimp only
  intro hk
  exact rankSeek_of_stencil shape b p rs lo hi k (P := fun _ => True)
    (fun v _ hv => stencil_id b p v hv) (fun _ _ => True.intro) hb hk

end AdjacentTiles

namespace AlignedWeek

open TinylambOpt.ResearchGoals.Atlas.RangePlans.AlignedWeek

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, boxIn, stencilWeight, bounded] at hv ⊢
  omega

theorem sound_RankSeek : RankSeek.Goal := by
  intro b p rs lo hi k hb
  dsimp only
  intro hk
  exact rankSeek_of_stencil shape b p rs lo hi k (P := fun _ => True)
    (fun v _ hv => stencil_id b p v hv) (fun _ _ => True.intro) hb hk

end AlignedWeek

namespace CrossChannel

open TinylambOpt.ResearchGoals.Atlas.RangePlans.CrossChannel

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, boxIn, stencilWeight, bounded] at hv ⊢
  omega

theorem sound_RankSeek : RankSeek.Goal := by
  intro b p rs lo hi k hb
  dsimp only
  intro hk
  exact rankSeek_of_stencil shape b p rs lo hi k (P := fun _ => True)
    (fun v _ hv => stencil_id b p v hv) (fun _ _ => True.intro) hb hk

end CrossChannel

namespace EitherEndpointInside

open TinylambOpt.ResearchGoals.Atlas.RangePlans.EitherEndpointInside

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, boxIn, stencilWeight, bounded] at hv ⊢
  omega

theorem sound_RankSeek : RankSeek.Goal := by
  intro b p rs lo hi k hb
  dsimp only
  intro hk
  exact rankSeek_of_stencil shape b p rs lo hi k (P := fun _ => True)
    (fun v _ hv => stencil_id b p v hv) (fun _ _ => True.intro) hb hk

end EitherEndpointInside

namespace ExactlyOneKey

open TinylambOpt.ResearchGoals.Atlas.RangePlans.ExactlyOneKey

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, stencilWeight, boxIn, bounded] at hv ⊢
  by_cases hx : v.x = p.x <;> by_cases hy : v.y = p.y
  all_goals
    have hxb : (v.x == p.x) = decide (v.x = p.x) := by rfl
    have hyb : (v.y == p.y) = decide (v.y = p.y) := by rfl
    simp only [hxb, hyb, hx, hy, if_false, if_true]
    repeat' split <;> simp_all <;> omega

theorem sound_RankSeek : RankSeek.Goal := by
  intro b p rs lo hi k hb
  dsimp only
  intro hk
  exact rankSeek_of_stencil shape b p rs lo hi k (P := fun _ => True)
    (fun v _ hv => stencil_id b p v hv) (fun _ _ => True.intro) hb hk

end ExactlyOneKey

namespace GapSession

open TinylambOpt.ResearchGoals.Atlas.RangePlans.GapSession

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, boxIn, stencilWeight, bounded] at hv ⊢
  omega

theorem sound_RankSeek : RankSeek.Goal := by
  intro b p rs lo hi k hb
  dsimp only
  intro hk
  exact rankSeek_of_stencil shape b p rs lo hi k (P := fun _ => True)
    (fun v _ hv => stencil_id b p v hv) (fun _ _ => True.intro) hb hk

end GapSession

namespace LexSuccessor

open TinylambOpt.ResearchGoals.Atlas.RangePlans.LexSuccessor

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, boxIn, stencilWeight, bounded] at hv ⊢
  omega

theorem sound_RankSeek : RankSeek.Goal := by
  intro b p rs lo hi k hb
  dsimp only
  intro hk
  exact rankSeek_of_stencil shape b p rs lo hi k (P := fun _ => True)
    (fun v _ hv => stencil_id b p v hv) (fun _ _ => True.intro) hb hk

end LexSuccessor

namespace OutsideBand

open TinylambOpt.ResearchGoals.Atlas.RangePlans.OutsideBand

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, boxIn, stencilWeight, bounded] at hv ⊢
  omega

theorem sound_RankSeek : RankSeek.Goal := by
  intro b p rs lo hi k hb
  dsimp only
  intro hk
  exact rankSeek_of_stencil shape b p rs lo hi k (P := fun _ => True)
    (fun v _ hv => stencil_id b p v hv) (fun _ _ => True.intro) hb hk

end OutsideBand

namespace OverdueOrLost

open TinylambOpt.ResearchGoals.Atlas.RangePlans.OverdueOrLost

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, boxIn, stencilWeight, bounded] at hv ⊢
  omega

theorem sound_RankSeek : RankSeek.Goal := by
  intro b p rs lo hi k hb
  dsimp only
  intro hk
  exact rankSeek_of_stencil shape b p rs lo hi k (P := fun _ => True)
    (fun v _ hv => stencil_id b p v hv) (fun _ _ => True.intro) hb hk

end OverdueOrLost

namespace ParetoFront

open TinylambOpt.ResearchGoals.Atlas.RangePlans.ParetoFront

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, boxIn, stencilWeight, bounded] at hv ⊢
  omega

theorem sound_RankSeek : RankSeek.Goal := by
  intro b p rs lo hi k hb
  dsimp only
  intro hk
  exact rankSeek_of_stencil shape b p rs lo hi k (P := fun _ => True)
    (fun v _ hv => stencil_id b p v hv) (fun _ _ => True.intro) hb hk

end ParetoFront

namespace PriceCorridor

open TinylambOpt.ResearchGoals.Atlas.RangePlans.PriceCorridor

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, boxIn, stencilWeight, bounded] at hv ⊢
  omega

theorem sound_RankSeek : RankSeek.Goal := by
  intro b p rs lo hi k hb
  dsimp only
  intro hk
  exact rankSeek_of_stencil shape b p rs lo hi k (P := fun _ => True)
    (fun v _ hv => stencil_id b p v hv) (fun _ _ => True.intro) hb hk

end PriceCorridor

namespace PriorTradingDay

open TinylambOpt.ResearchGoals.Atlas.RangePlans.PriorTradingDay

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, boxIn, stencilWeight, bounded] at hv ⊢
  omega

theorem sound_RankSeek : RankSeek.Goal := by
  intro b p rs lo hi k hb
  dsimp only
  intro hk
  exact rankSeek_of_stencil shape b p rs lo hi k (P := fun _ => True)
    (fun v _ hv => stencil_id b p v hv) (fun _ _ => True.intro) hb hk

end PriorTradingDay

namespace ReversedRank

open TinylambOpt.ResearchGoals.Atlas.RangePlans.ReversedRank

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, boxIn, stencilWeight, bounded] at hv ⊢
  omega

theorem sound_RankSeek : RankSeek.Goal := by
  intro b p rs lo hi k hb
  dsimp only
  intro hk
  exact rankSeek_of_stencil shape b p rs lo hi k (P := fun _ => True)
    (fun v _ hv => stencil_id b p v hv) (fun _ _ => True.intro) hb hk

end ReversedRank

namespace RingNeighborhood

open TinylambOpt.ResearchGoals.Atlas.RangePlans.RingNeighborhood

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, boxIn, stencilWeight, bounded] at hv ⊢
  omega

theorem sound_RankSeek : RankSeek.Goal := by
  intro b p rs lo hi k hb
  dsimp only
  intro hk
  exact rankSeek_of_stencil shape b p rs lo hi k (P := fun _ => True)
    (fun v _ hv => stencil_id b p v hv) (fun _ _ => True.intro) hb hk

end RingNeighborhood

namespace VarianceBudget

open TinylambOpt.ResearchGoals.Atlas.RangePlans.VarianceBudget

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, boxIn, stencilWeight, bounded] at hv ⊢
  omega

theorem sound_RankSeek : RankSeek.Goal := by
  intro b p rs lo hi k hb
  dsimp only
  intro hk
  exact rankSeek_of_stencil shape b p rs lo hi k (P := fun _ => True)
    (fun v _ hv => stencil_id b p v hv) (fun _ _ => True.intro) hb hk

end VarianceBudget

end TinylambOpt.ResearchProofs.Claude1125.RangeRankSeek
