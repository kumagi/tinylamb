import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.RP_NestedVersion_StaggeredHandoff

open TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
# Proofs for `NestedVersion` and `StaggeredHandoff` goals of `Atlas.RangePlans`

Shared infrastructure: prefix-mass / corners / indexed lemmas (the
`prefixIndex_of_stencil` master lemma), scan/filter helpers, the
`events`/`atPoint`/`compile` transposition lemmas, `uniform`/`pageScan`
case analysis, and a generic binary-search (`seek`) correctness lemma.
-/

/-! ## Box-membership predicate -/

private def memBox (q : Box) (v : Point) : Prop :=
  q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1

private instance (q : Box) (v : Point) : Decidable (memBox q v) := by
  unfold memBox
  infer_instance

/-! ## `prefixMass` lemmas -/

private theorem prefixMass_singleton (r : Point × Int) (x y : Int) :
    prefixMass [r] x y = if r.1.x < x && r.1.y < y then r.2 else 0 := by
  by_cases h : r.1.x < x && r.1.y < y <;> simp [prefixMass, h]

private theorem prefixMass_cons (r : Point × Int) (rs : Rows) (x y : Int) :
    prefixMass (r :: rs) x y =
      prefixMass rs x y + (if r.1.x < x && r.1.y < y then r.2 else 0) := by
  by_cases h : r.1.x < x && r.1.y < y <;> simp [prefixMass, h] <;> omega

private theorem prefixMass_append (rs ts : Rows) (x y : Int) :
    prefixMass (rs ++ ts) x y = prefixMass rs x y + prefixMass ts x y := by
  induction rs with
  | nil => simp [prefixMass]
  | cons r rs ih =>
      simp [prefixMass_cons, ih]
      omega

/-! ## `corners` / `indexed` lemmas -/

private theorem corners_add (q : Box) (f g : Int → Int → Int) :
    corners q (fun x y => f x y + g x y) = corners q f + corners q g := by
  unfold corners
  by_cases hx : q.x0 < q.x1
  · by_cases hy : q.y0 < q.y1
    · simp [hx, hy, Int.mul_add, Int.mul_sub] <;> omega
    · simp [hx, hy]
  · simp [hx]

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
      have hnot : ¬ memBox q v := by
        unfold memBox
        omega
      simp [hnot]
  · simp [corners, hx1]
    have hnot : ¬ memBox q v := by
      unfold memBox
      omega
    simp [hnot]

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

private theorem indexed_add (s : Shape) (b : Int) (p : Point) (f g : Int → Int → Int) :
    indexed s b p (fun x y => f x y + g x y) = indexed s b p f + indexed s b p g := by
  unfold indexed
  rw [show (s.stencil b p).map (fun q => corners q (fun x y => f x y + g x y)) =
        (s.stencil b p).map (fun q => corners q f + corners q g) by
        apply List.map_congr_left
        intro q hq
        exact corners_add q f g]
  exact sum_map_add (s.stencil b p) (fun q => corners q f) (fun q => corners q g)

private theorem indexed_prefixMass_eq (s : Shape) (b : Int) (p : Point) (rs : Rows) :
    indexed s b p (prefixMass rs) =
      (rs.map (fun r => indexed s b p (prefixMass [r]))).sum := by
  induction rs with
  | nil => simp [prefixMass, indexed, corners, sum_map_zero]
  | cons r rs ih =>
      have hfun : prefixMass (r :: rs) =
          (fun x y => prefixMass [r] x y + prefixMass rs x y) := by
        funext x y
        rw [prefixMass_cons, prefixMass_singleton]
        by_cases h : r.1.x < x && r.1.y < y <;> simp [h] <;> omega
      rw [hfun, indexed_add, ih]
      simp [List.map_cons, List.sum_cons]

private theorem stencil_weight (s : Shape) (b : Int) (p : Point) (v : Point) (w : Int) :
    ((s.stencil b p).map (fun q => q.coeff * (if memBox q v then w else 0))).sum =
      w * ((s.stencil b p).map (fun q => q.coeff * (if memBox q v then 1 else 0))).sum := by
  induction s.stencil b p with
  | nil => simp
  | cons q qs ih =>
      by_cases h : memBox q v
      · simp [h, Int.mul_add, ih]
        rw [Int.mul_comm q.coeff w]
      · simp [h, ih]

private theorem indexed_singleton (s : Shape) (b : Int) (p : Point) (v : Point) (w : Int) :
    indexed s b p (prefixMass [(v, w)]) =
      w * ((s.stencil b p).map (fun q => q.coeff * (if memBox q v then 1 else 0))).sum := by
  unfold indexed
  rw [show (s.stencil b p).map (fun q => corners q (prefixMass [(v, w)])) =
        (s.stencil b p).map (fun q => q.coeff * (if memBox q v then w else 0)) by
        apply List.map_congr_left
        intro q hq
        exact corners_singleton q v w]
  exact stencil_weight s b p v w

/-- Master lemma: if the signed stencil computes the 0/1 indicator of
`qualifies` on every bounded point, the 2-D prefix index equals the scan. -/
private theorem prefixIndex_of_stencil (s : Shape) (b : Int) (p : Point)
    (hst : ∀ v : Point, bounded b v →
      ((s.stencil b p).map (fun q => q.coeff * (if memBox q v then 1 else 0))).sum =
        if s.qualifies p v then 1 else 0) :
    ∀ rs : Rows, (∀ r ∈ rs, bounded b r.1) →
      indexed s b p (prefixMass rs) = scan s p rs := by
  intro rs hb
  rw [indexed_prefixMass_eq]
  induction rs with
  | nil => simp [scan]
  | cons r rs ih =>
      have hrb : bounded b r.1 := hb r (by simp)
      have hrs : ∀ r' ∈ rs, bounded b r'.1 := by
        intro r' hm
        exact hb r' (by simp [hm])
      have ih' := ih hrs
      simp [scan] at ih' ⊢
      by_cases hq : s.qualifies p r.1
      · rw [indexed_singleton, hst r.1 hrb]
        simp [hq, ih']
      · rw [indexed_singleton, hst r.1 hrb]
        simp [hq, ih']

/-! ## `scan` / filter helpers -/

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool) (g : Point × Int → Int) :
    ((rs.filter f).map g).sum = (rs.map (fun r => if f r then g r else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih]

private theorem scan_eq_sum_if (s : Shape) (p : Point) (rs : Rows) :
    scan s p rs = (rs.map (fun r => if s.qualifies p r.1 then r.2 else 0)).sum := by
  unfold scan
  exact sum_filter_eq_sum_if rs (fun r => s.qualifies p r.1) Prod.snd

private theorem scan_eq_zero_of_forall_not (s : Shape) (p : Point) (rs : Rows)
    (h : ∀ r ∈ rs, s.qualifies p r.1 = false) :
    scan s p rs = 0 := by
  rw [scan_eq_sum_if]
  induction rs with
  | nil => simp
  | cons r rs ih =>
      have hr : s.qualifies p r.1 = false := h r (by simp)
      have hrs : ∀ r' ∈ rs, s.qualifies p r'.1 = false :=
        fun r' hm => h r' (by simp [hm])
      simp [hr, ih hrs]

private theorem scan_eq_sum_of_forall (s : Shape) (p : Point) (rs : Rows)
    (h : ∀ r ∈ rs, s.qualifies p r.1 = true) :
    scan s p rs = (rs.map Prod.snd).sum := by
  rw [scan_eq_sum_if]
  induction rs with
  | nil => simp
  | cons r rs ih =>
      have hr : s.qualifies p r.1 = true := h r (by simp)
      have hrs : ∀ r' ∈ rs, s.qualifies p r'.1 = true :=
        fun r' hm => h r' (by simp [hm])
      simp [hr, ih hrs]

private theorem sum_map_const_one (rs : Rows) :
    (rs.map (fun _ => (1 : Int))).sum = rs.length := by
  induction rs with
  | nil => simp
  | cons r rs ih => simp [ih] <;> omega

private theorem sum_le_sum' {α : Type} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x ≤ g x) :
    (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx := h x (by simp)
      have hxs := ih (fun y hy => h y (by simp [hy]))
      omega

private theorem filter_comm {α : Type} (l : List α) (p q : α → Bool) :
    (l.filter p).filter q = (l.filter q).filter p := by
  rw [List.filter_filter, List.filter_filter]
  apply List.filter_congr
  intro x hx
  simp [Bool.and_comm]

/-! ## `events` / `atPoint` / `compile` lemmas -/

private def inBox (q : Box) (r : Point) : Prop :=
  q.x0 ≤ r.x ∧ r.x < q.x1 ∧ q.y0 ≤ r.y ∧ r.y < q.y1

private instance (q : Box) (r : Point) : Decidable (inBox q r) := by
  unfold inBox
  infer_instance

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

private theorem atPoint_flatMap {α : Type} (f : α → Rows) (l : List α) (r : Point) :
    atPoint (l.flatMap f) r = (l.map (fun a => atPoint (f a) r)).sum := by
  induction l with
  | nil => simp [atPoint, prefixMass]
  | cons a as ih =>
      simp only [List.flatMap_cons]
      rw [atPoint_append, ih]
      simp

/-! ## `uniform` / `pageScan` lemmas -/

/-- `pageScan` on a singleton stencil unfolds to a match on `uniform`. -/
private theorem pageScan_singleton (s : Shape) (b : Int) (p : Point) (g : Page) (q : Box)
    (hst : s.stencil b p = [q]) :
    pageScan s b p g =
      match uniform g q with
      | some w => w * (g.rows.map Prod.snd).sum
      | none => scan s p g.rows := by
  have hm : List.mapM (uniform g) [q] = (uniform g q).map (fun w => [w]) := by
    rw [List.mapM_cons, List.mapM_nil]
    cases uniform g q <;> rfl
  unfold pageScan
  rw [hst, hm]
  cases uniform g q <;> simp

/-! ## `seek` correctness -/

private theorem seek_zero (rank : Int → Int) (k : Nat) (lo hi : Int) :
    seek rank k 0 lo hi = lo := rfl

private theorem seek_succ (rank : Int → Int) (k : Nat) (fuel : Nat) (lo hi : Int) :
    seek rank k (fuel + 1) lo hi =
      if hi ≤ lo then lo else
        let mid := (lo + hi) / 2
        if (k : Int) < rank mid then seek rank k fuel lo mid
        else seek rank k fuel (mid + 1) hi := rfl

private theorem seek_succ' (rank : Int → Int) (k : Nat) (fuel : Nat) (lo hi : Int) :
    seek rank k (fuel + 1) lo hi =
      if hi ≤ lo then lo else
        if (k : Int) < rank ((lo + hi) / 2) then seek rank k fuel lo ((lo + hi) / 2)
        else seek rank k fuel ((lo + hi) / 2 + 1) hi := rfl

/-- Generic binary-search correctness: if `k < rank v ↔ s ≤ v` for all `v`,
and `s ∈ [lo, hi]`, and the fuel suffices, `seek` returns `s`. -/
private theorem seek_eq_of_char {rank : Int → Int} {k : Nat} {s : Int} :
    ∀ (fuel : Nat) (lo hi : Int),
    (∀ v : Int, (k : Int) < rank v ↔ s ≤ v) →
    lo ≤ s → s ≤ hi → (hi - lo).toNat < 2 ^ fuel →
    seek rank k fuel lo hi = s := by
  intro fuel
  induction fuel with
  | zero =>
      intro lo hi hchar hlo hhi hfuel
      have hle : hi ≤ lo := by
        have h1 : (hi - lo).toNat = 0 := by omega
        rw [Int.toNat_eq_zero] at h1
        omega
      have : s = lo := by omega
      rw [this, seek_zero]
  | succ fuel ih =>
      intro lo hi hchar hlo hhi hfuel
      rw [seek_succ']
      by_cases hle : hi ≤ lo
      · rw [if_pos hle]
        omega
      · rw [if_neg hle]
        have hpow : (2 : Nat) ^ (fuel + 1) = 2 * 2 ^ fuel := by
          show (2 : Nat) ^ fuel.succ = 2 * 2 ^ fuel
          rw [Nat.pow_succ]
          omega
        by_cases hk : (k : Int) < rank ((lo + hi) / 2)
        · have hsm : s ≤ (lo + hi) / 2 := (hchar _).mp hk
          have hfuel' : (((lo + hi) / 2) - lo).toNat < 2 ^ fuel := by omega
          rw [if_pos hk]
          exact ih lo _ hchar hlo hsm hfuel'
        · have hns : ¬ s ≤ (lo + hi) / 2 := fun h => hk ((hchar _).mpr h)
          have hsm : (lo + hi) / 2 + 1 ≤ s := by omega
          have hfuel' : (hi - ((lo + hi) / 2 + 1)).toNat < 2 ^ fuel := by omega
          rw [if_neg hk]
          exact ih (_ + 1) hi hchar hsm hhi hfuel'

/-! ## Goal proofs -/

/-- If `q`-qualification implies `p`-qualification pointwise and all weights
are nonnegative, the `q`-scan is bounded by the `p`-scan. -/
private theorem scan_le_scan_of_forall {s : Shape} {p q : Point} {rs : Rows}
    (hmono : ∀ r ∈ rs, s.qualifies q r.1 = true → s.qualifies p r.1 = true)
    (hnn : ∀ r ∈ rs, 0 ≤ r.2) :
    scan s q rs ≤ scan s p rs := by
  rw [scan_eq_sum_if, scan_eq_sum_if]
  induction rs with
  | nil => simp
  | cons r rs ih =>
      have hnn' : ∀ r' ∈ rs, 0 ≤ r'.2 := fun r' hm => hnn r' (by simp [hm])
      have hmono' : ∀ r' ∈ rs, s.qualifies q r'.1 = true → s.qualifies p r'.1 = true :=
        fun r' hm => hmono r' (by simp [hm])
      have ih' := ih hmono' hnn'
      have hr : 0 ≤ r.2 := hnn r (by simp)
      simp only [List.map_cons, List.sum_cons]
      split <;> omega

/-- `NestedVersion.PrefixIndex`: the single stencil box
`⟨p.x, b+1, -b, p.y+1⟩` covers exactly `p.x ≤ v.x ∧ v.y ≤ p.y` on bounded
points. -/
theorem NestedVersion_PrefixIndex_sound : NestedVersion.PrefixIndex.Goal := by
  intro b p rs hb
  refine prefixIndex_of_stencil NestedVersion.shape b p ?_ rs hb
  intro v hbnd
  have hbnd' : -b ≤ v.x ∧ v.x ≤ b ∧ -b ≤ v.y ∧ v.y ≤ b := hbnd
  obtain ⟨hx0, hx1, hy0, hy1⟩ := hbnd'
  have hstencil : NestedVersion.shape.stencil b p =
      [box p.x (b + 1) (-b) (p.y + 1)] := rfl
  rw [hstencil]
  simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, add_zero]
  have hcoeff : (box p.x (b + 1) (-b) (p.y + 1)).coeff = 1 := rfl
  rw [hcoeff, one_mul]
  have hmb : memBox (box p.x (b + 1) (-b) (p.y + 1)) v ↔
      (p.x ≤ v.x ∧ v.x < b + 1 ∧ -b ≤ v.y ∧ v.y < p.y + 1) := Iff.rfl
  have hqiff : (NestedVersion.shape.qualifies p v = true) ↔
      (p.x ≤ v.x ∧ v.y ≤ p.y) := by
    show ((p.x ≤ v.x && v.y ≤ p.y) = true) ↔ _
    simp only [Bool.and_eq_true, decide_eq_true_eq]
  by_cases h1 : p.x ≤ v.x ∧ v.x < b + 1 ∧ -b ≤ v.y ∧ v.y < p.y + 1
  · have hmb' : memBox (box p.x (b + 1) (-b) (p.y + 1)) v := hmb.mpr h1
    have hq : NestedVersion.shape.qualifies p v = true := by
      apply hqiff.mpr
      obtain ⟨a1, -, -, a4⟩ := h1
      exact ⟨a1, by omega⟩
    rw [if_pos hmb', if_pos hq]
  · have hmb' : ¬ memBox (box p.x (b + 1) (-b) (p.y + 1)) v := fun h => h1 (hmb.mp h)
    have hq : NestedVersion.shape.qualifies p v ≠ true := by
      intro hqt
      apply h1
      apply hmb.mpr
      have hqq := hqiff.mp hqt
      obtain ⟨a1, a2⟩ := hqq
      exact ⟨a1, by omega, hy0, by omega⟩
    rw [if_neg hmb']
    by_cases hqt : NestedVersion.shape.qualifies p v = true
    · exact absurd hqt hq
    · rw [if_neg hqt]

/-- `StaggeredHandoff.PrefixIndex`: the single stencil box
`⟨p.x+1, p.y, p.y+1, b+1⟩` covers exactly
`p.x < v.x ∧ v.x < p.y ∧ p.y < v.y` on bounded points. -/
theorem StaggeredHandoff_PrefixIndex_sound : StaggeredHandoff.PrefixIndex.Goal := by
  intro b p rs hb
  refine prefixIndex_of_stencil StaggeredHandoff.shape b p ?_ rs hb
  intro v hbnd
  have hbnd' : -b ≤ v.x ∧ v.x ≤ b ∧ -b ≤ v.y ∧ v.y ≤ b := hbnd
  obtain ⟨hx0, hx1, hy0, hy1⟩ := hbnd'
  have hstencil : StaggeredHandoff.shape.stencil b p =
      [box (p.x + 1) p.y (p.y + 1) (b + 1)] := rfl
  rw [hstencil]
  simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, add_zero]
  have hcoeff : (box (p.x + 1) p.y (p.y + 1) (b + 1)).coeff = 1 := rfl
  rw [hcoeff, one_mul]
  have hmb : memBox (box (p.x + 1) p.y (p.y + 1) (b + 1)) v ↔
      (p.x + 1 ≤ v.x ∧ v.x < p.y ∧ p.y + 1 ≤ v.y ∧ v.y < b + 1) := Iff.rfl
  have hqiff : (StaggeredHandoff.shape.qualifies p v = true) ↔
      (p.x < v.x ∧ v.x < p.y ∧ p.y < v.y) := by
    show ((p.x < v.x && v.x < p.y && p.y < v.y) = true) ↔ _
    simp only [Bool.and_eq_true, decide_eq_true_eq, and_assoc]
  by_cases h1 : p.x + 1 ≤ v.x ∧ v.x < p.y ∧ p.y + 1 ≤ v.y ∧ v.y < b + 1
  · have hmb' : memBox (box (p.x + 1) p.y (p.y + 1) (b + 1)) v := hmb.mpr h1
    have hq : StaggeredHandoff.shape.qualifies p v = true := by
      apply hqiff.mpr
      obtain ⟨a1, a2, a3, -⟩ := h1
      exact ⟨by omega, a2, by omega⟩
    rw [if_pos hmb', if_pos hq]
  · have hmb' : ¬ memBox (box (p.x + 1) p.y (p.y + 1) (b + 1)) v := fun h => h1 (hmb.mp h)
    have hq : StaggeredHandoff.shape.qualifies p v ≠ true := by
      intro hqt
      apply h1
      apply hmb.mpr
      have hqq := hqiff.mp hqt
      obtain ⟨a1, a2, a3⟩ := hqq
      exact ⟨by omega, a2, by omega, by omega⟩
    rw [if_neg hmb']
    by_cases hqt : StaggeredHandoff.shape.qualifies p v = true
    · exact absurd hqt hq
    · rw [if_neg hqt]

/-- `NestedVersion.OuterCertificate`: when `p`'s region contains `q`'s region
and all weights are nonnegative, the `q`-scan is bounded by the `p`-scan. -/
theorem NestedVersion_OuterCertificate_sound : NestedVersion.OuterCertificate.Goal := by
  intro p q rs hpq hqy hnn
  refine scan_le_scan_of_forall (s := NestedVersion.shape) (p := p) (q := q) (rs := rs) ?_ hnn
  intro r hr hq
  have hqq : ((q.x ≤ r.1.x && r.1.y ≤ q.y) = true) := hq
  simp only [Bool.and_eq_true, decide_eq_true_eq] at hqq
  show ((p.x ≤ r.1.x && r.1.y ≤ p.y) = true)
  simp only [Bool.and_eq_true, decide_eq_true_eq]
  obtain ⟨h1, h2⟩ := hqq
  exact ⟨by omega, by omega⟩

end TinylambOpt.ResearchProofs.DevinBatch.RP_NestedVersion_StaggeredHandoff
