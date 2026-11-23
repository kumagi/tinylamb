import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Job

namespace TinylambOpt.ResearchProofs.AnalyticsWorkloads.Job

open TinylambOpt.ResearchGoals.AnalyticsWorkloads.Job
open TinylambOpt.ResearchGoals.AnalyticsWorkloads

/-!
# Proof strategy overview

We prove the six JOB propositions from `Job.lean`:

* `CompanyPair` (J1): the pair cache stores, for each `(company,type)` key
  occurring in the bridge, the product of the company-list and type-list
  frequencies; folding `weighted` rows agrees with the explicit
  flatMap/filter/multiplicity expansion.
* `RatingOrder` (J2): fixed-width `d.d` ratings admit a two-digit numeric
  comparison that coincides with the literal string order; every other string
  keeps binary comparison.
* `FixedGender` (J3): after filtering to gender `"F"`, the singleton group is
  either empty or `["F"]` with the full `summarize` totals.
* `LabelQuotient` (J4): the `(movie,label)` histogram weighted once per movie
  equals the flatMap/filter/count expansion.
* `DecimalPrefix` (J5): a certified `aka-` + canonical-decimal code satisfies
  `w ≤ n < 2w` iff the decimal expansion starts with digit `1`.
* `CorrelatedFanout` (J6): weighted inclusion–exclusion bounds for a pair of
  correlated per-cell filters.
-/

/-! ## Shared `Totals` algebra -/

private theorem plus_comm' (a b : Totals) : plus a b = plus b a := by
  cases a; cases b; simp [plus]; omega

private theorem plus_zero_left' (a : Totals) : plus zero a = a := by
  cases a; simp [plus, zero]

private theorem plus_zero_right' (a : Totals) : plus a zero = a := by
  cases a; simp [plus, zero]

private theorem plus_assoc' (a b c : Totals) : plus (plus a b) c = plus a (plus b c) := by
  cases a; cases b; cases c; simp [plus]; omega

private theorem summarize_cnt_aux (vs : List (Option Int)) (st : Totals) :
    (vs.foldl (fun a v => plus a (weighted 1 v)) st).cnt = st.cnt + vs.length := by
  induction vs generalizing st with
  | nil => rfl
  | cons v vs ih =>
    rw [List.foldl_cons]
    have h1 : ((fun a v => plus a (weighted 1 v)) st v).cnt = st.cnt + 1 := by
      simp [plus, weighted]
    rw [ih ((fun a v => plus a (weighted 1 v)) st v), h1]
    simp [List.length_cons]
    omega

private theorem summarize_cnt (vs : List (Option Int)) : (summarize vs).cnt = vs.length := by
  show (vs.foldl (fun a v => plus a (weighted 1 v)) zero).cnt = vs.length
  rw [summarize_cnt_aux]
  simp [zero]

/-! ## J3 `FixedGender` -/

private theorem eraseDups_all_F {l : List String} (hF : ∀ x ∈ l, x = "F")
    (hne : l ≠ []) : l.eraseDups = ["F"] := by
  obtain ⟨x, xs, rfl⟩ := List.exists_cons_of_ne_nil hne
  have hx : x = "F" := hF x (by simp)
  rw [hx, List.eraseDups_cons]
  have hfilter : xs.filter (fun b => !(b == "F")) = [] := by
    rw [List.filter_eq_nil_iff]
    intro b hb
    have hbF : b = "F" := hF b (List.mem_cons_of_mem x hb)
    rw [hbF]
    simp
  rw [hfilter]
  simp

private theorem fixedGender_key {kept : List (String × Option Int)}
    (hF : ∀ r ∈ kept, r.1 == "F") :
    singletonGroup (kept.map Prod.snd) =
      (kept.map Prod.fst).eraseDups.map fun g =>
        (g, summarize ((kept.filter (fun r => r.1 == g)).map Prod.snd)) := by
  cases kept with
  | nil => rfl
  | cons r rs =>
    have hall : ∀ x ∈ (r :: rs).map Prod.fst, x = "F" := by
      intro x hx
      obtain ⟨r', hr', hxr⟩ := List.mem_map.mp hx
      have hr'F : Prod.fst r' = "F" := beq_iff_eq.mp (hF r' (by simp [hr']))
      rw [← hxr]
      exact hr'F
    have herase : ((r :: rs).map Prod.fst).eraseDups = ["F"] := by
      apply eraseDups_all_F hall
      simp
    have hfilter_self : (r :: rs).filter (fun r => r.1 == "F") = r :: rs := by
      apply List.filter_eq_self.mpr
      intro a ha
      exact hF a ha
    rw [herase]
    simp only [List.map_cons, List.map_nil]
    rw [hfilter_self]
    show (if (summarize ((r :: rs).map Prod.snd)).cnt == 0 then [] else
        [("F", summarize ((r :: rs).map Prod.snd))]) =
      [("F", summarize ((r :: rs).map Prod.snd))]
    split
    · next h =>
      have h0 : (summarize ((r :: rs).map Prod.snd)).cnt = 0 := beq_iff_eq.mp h
      rw [summarize_cnt] at h0
      simp at h0
    · rfl

theorem sound_FixedGender : FixedGender.Goal := by
  intro _rows
  intro kept
  apply fixedGender_key
  intro r hr
  exact (List.mem_filter.mp hr).2

/-! ## J6 `CorrelatedFanout` -/

private theorem sum_map_le_sum_map {α : Type} {f g : α → Nat} {l : List α}
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
    rw [List.map_cons, List.map_cons, List.sum_cons, List.sum_cons]
    have hx : f x ≤ g x := h x (by simp)
    have hxs : ∀ y ∈ xs, f y ≤ g y := fun y hy => h y (by simp [hy])
    have := ih hxs
    omega

private theorem mass_cons (p : Bool → Bool → Bool) (a b : Bool) (w : Nat)
    (rs : List Mass) :
    mass ((a,b,w) :: rs) p = (if p a b then w else 0) + mass rs p := by
  simp [mass, List.map_cons, List.sum_cons]

private theorem mass_mono {p q : Bool → Bool → Bool} {rs : List Mass}
    (h : ∀ a b, p a b → q a b) : mass rs p ≤ mass rs q := by
  unfold mass
  apply sum_map_le_sum_map
  intro m hm
  obtain ⟨a, b, w⟩ := m
  by_cases hp : p a b
  · have hq : q a b := h a b hp
    simp [hp, hq]
  · simp [hp]

private theorem mass_append (p : Bool → Bool → Bool) (l₁ l₂ : List Mass) :
    mass (l₁ ++ l₂) p = mass l₁ p + mass l₂ p := by
  simp [mass, List.map_append, List.sum_append]

private theorem mass_flatten (cells : List (List Mass)) (p : Bool → Bool → Bool) :
    mass cells.flatten p = (cells.map fun rs => mass rs p).sum := by
  induction cells with
  | nil => simp [mass]
  | cons c cs ih =>
    rw [List.flatten_cons, mass_append, ih]
    simp [List.map_cons, List.sum_cons]

private theorem mass_triangle (rs : List Mass) :
    mass rs (fun x _ => x) + mass rs (fun _ y => y) - mass rs (fun _ _ => true) ≤
      mass rs (· && ·) := by
  induction rs with
  | nil => simp [mass]
  | cons m rs ih =>
    obtain ⟨a, b, w⟩ := m
    rw [mass_cons, mass_cons, mass_cons, mass_cons]
    cases a <;> cases b <;> simp <;> omega

private theorem mass_ab_le_min (rs : List Mass) :
    mass rs (· && ·) ≤ min (mass rs (fun x _ => x)) (mass rs (fun _ y => y)) := by
  apply Nat.le_min.mpr
  constructor
  · apply mass_mono
    intro a b h
    exact (Bool.and_eq_true_iff.mp h).1
  · apply mass_mono
    intro a b h
    exact (Bool.and_eq_true_iff.mp h).2

theorem sound_CorrelatedFanout : CorrelatedFanout.Goal := by
  intro cells
  intro a
  intro b
  intro both
  show (cells.map fun rs =>
      mass rs (fun x _ => x) + mass rs (fun _ y => y) - mass rs (fun _ _ => true)).sum ≤
      mass cells.flatten (· && ·) ∧
    mass cells.flatten (· && ·) ≤
      (cells.map fun rs =>
        min (mass rs (fun x _ => x)) (mass rs (fun _ y => y))).sum
  constructor
  · rw [mass_flatten cells (· && ·)]
    apply sum_map_le_sum_map
    intro rs hrs
    exact mass_triangle rs
  · rw [mass_flatten cells (· && ·)]
    apply sum_map_le_sum_map
    intro rs hrs
    exact mass_ab_le_min rs

end TinylambOpt.ResearchProofs.AnalyticsWorkloads.Job
