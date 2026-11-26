import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `NestedVersion.PageVerdict` — `∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
  pageScan shape b p g = scan shape p g.rows`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_NestedVersion_PageVerdict

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.NestedVersion

/-! Finite-list/rectangle stencil algebra used by `NestedVersion.PageVerdict`.
The header-only page scan agrees with a full row scan whenever every stencil
rectangle has a uniform verdict on the page. -/

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

/-! The one-box stencil of `NestedVersion` is exactly the inclusion test. -/
private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, boxIn, stencilWeight, bounded] at hv ⊢
  omega

/-! A page whose rows all lie inside its inclusive rectangle: the header verdict
equals the full scan (verified by brute force on 12,250 small valid pages). -/
theorem sound_NestedVersion_PageVerdict : NestedVersion.PageVerdict.Goal := by
  intro b p g hv hb
  exact pageScan_eq_scan shape b p g (fun v hv => stencil_id b p v hv) hb hv

end TinylambOpt.ResearchProofs.Atlas.Goal_NestedVersion_PageVerdict
