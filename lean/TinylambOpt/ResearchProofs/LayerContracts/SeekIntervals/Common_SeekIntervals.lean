import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals

/-!
An index range plan must cover every qualifying key and preserve any gaps excluded by
the SQL predicate.

Membership in a clamped span is the raw span membership intersected with `[0,cap)`.

`any` over the (clamped, filtered) ranges equals `any` over the raw plan spans.

Named results include `inside_clamped_eq`, `any_ranges_eq`, `separated_clamped`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals

/-- Membership in a clamped span is the raw span membership intersected with `[0,cap)`. -/
theorem inside_clamped_eq (l h cap x : Nat) :
    inside (min l cap, min h cap) x = (l ≤ x && x < h && x < cap) := by
  apply Bool.eq_iff_iff.mpr
  simp [inside]
  omega

/-- `any` over the (clamped, filtered) ranges equals `any` over the raw plan spans. -/
theorem any_ranges_eq (ps : List Span) (cap x : Nat) :
    ((ps.map (fun (l,h) => (min l cap, min h cap))).filter (fun (l,h) => l < h)).any (inside · x) =
      ps.any (fun (l,h) => l ≤ x && x < h && x < cap) := by
  induction ps with
  | nil => simp
  | cons s ps ih =>
      rcases s with ⟨l,h⟩
      rw [List.map_cons, List.filter_cons]
      by_cases hm : min l cap < min h cap
      · simp [hm, ih, inside_clamped_eq]
      · simp [hm, ih]
        omega

/-- Clamping and filtering preserve separation of spans. -/
theorem separated_clamped (ps : List Span) (cap : Nat)
    (hsep : ps.Pairwise (fun s t : Span => s.2 ≤ t.1)) :
    ((ps.map (fun (l,h) => (min l cap, min h cap))).filter (fun (l,h) => l < h)).Pairwise
      (fun s t : Span => s.2 ≤ t.1) := by
  induction ps with
  | nil => simp
  | cons s ps ih =>
      rcases s with ⟨l,h⟩
      rw [List.pairwise_cons] at hsep
      rw [List.map_cons, List.filter_cons]
      by_cases hm : min l cap < min h cap
      · simp [hm]
        constructor
        · intro a b x x1 hmem ha hb hab
          have hx : h ≤ x := hsep.1 (x, x1) (by simpa using hmem)
          subst a
          omega
        · apply ih hsep.2
      · simp [hm, ih hsep.2]

/-- If two predicates are disjoint on a list, filtering by each separately is a
    permutation of filtering by their disjunction. -/
theorem filter_disjoint_append_perm (ks : List Nat) (P Q : Nat → Bool)
    (hd : ∀ x, P x = true → Q x = false) :
    List.Perm (ks.filter P ++ ks.filter Q) (ks.filter (fun x => P x || Q x)) := by
  induction ks with
  | nil => simp
  | cons x xs ih =>
      by_cases hP : P x = true
      · have hQ : Q x = false := hd x hP
        simp [hP, hQ, ih]
      · by_cases hQ : Q x = true
        · simp [hP, hQ] at ih ⊢
          exact List.Perm.trans (by simp : List.Perm (xs.filter P ++ x :: xs.filter Q) (x :: (xs.filter P ++ xs.filter Q)))
            (List.Perm.cons x ih)
        · simp [hP, hQ, ih]

/-- For separated spans, the flat-mapped scan is a permutation of the filter by
    membership in any span. No ordering of `ks` is required. -/
theorem scan_perm_filter_any (rs : List Span) (ks : List Nat)
    (hsep : rs.Pairwise (fun s t : Span => s.2 ≤ t.1)) :
    List.Perm (rs.flatMap (fun s => ks.filter (inside s))) (ks.filter (fun x => rs.any (inside · x))) := by
  induction rs with
  | nil => simp
  | cons s rs ih =>
      rw [List.pairwise_cons] at hsep
      rw [List.flatMap_cons]
      have hseprs : rs.Pairwise (fun s t : Span => s.2 ≤ t.1) := hsep.2
      have hhead : ∀ t ∈ rs, s.2 ≤ t.1 := hsep.1
      have hd : ∀ x, inside s x = true → rs.any (inside · x) = false := by
        intro x hx
        apply Bool.eq_false_iff.mpr
        intro hany
        rw [List.any_eq_true] at hany
        rcases hany with ⟨t, ht, hti⟩
        have hst := hhead t ht
        simp [inside] at hx hti
        omega
      have perm1 := filter_disjoint_append_perm ks (inside s) (fun x => rs.any (inside · x)) hd
      have perm2 := ih hseprs
      exact List.Perm.trans (List.Perm.append_left _ perm2) perm1

/-- The scan list is sorted when the input keys are sorted and spans are separated. -/
theorem scan_pairwise (rs : List Span) (ks : List Nat)
    (hks : ks.Pairwise (· ≤ ·)) (hsep : rs.Pairwise (fun s t : Span => s.2 ≤ t.1)) :
    (rs.flatMap (fun s => ks.filter (inside s))).Pairwise (· ≤ ·) := by
  induction rs with
  | nil => simp
  | cons s rs ih =>
      rw [List.flatMap_cons]
      rw [List.pairwise_cons] at hsep
      rw [List.pairwise_append]
      constructor
      · exact hks.filter _
      constructor
      · exact ih hsep.2
      · intro x hx y hy
        rw [List.mem_flatMap] at hy
        rcases hy with ⟨t, ht, hyt⟩
        rw [List.mem_filter] at hx hyt
        have hst := hsep.1 t ht
        simp [inside] at hx hyt
        omega

theorem scan_eq_filter_any (rs : List Span) (ks : List Nat)
    (hks : ks.Pairwise (· ≤ ·)) (hsep : rs.Pairwise (fun s t : Span => s.2 ≤ t.1)) :
    rs.flatMap (fun s => ks.filter (inside s)) = ks.filter (fun x => rs.any (inside · x)) := by
  apply List.Perm.eq_of_pairwise (le := fun a b : Nat => a ≤ b)
  · intro a b ha hb hab hba
    omega
  · exact scan_pairwise rs ks hks hsep
  · exact hks.filter _
  · exact scan_perm_filter_any rs ks hsep

/-- The clamped ranges of a separated plan are separated. -/
theorem ranges_sep (m : Model) (hsep : ∀ p, (m.plan p).Pairwise (fun s t : Span => s.2 ≤ t.1)) (p : Args) :
    (ranges m p).Pairwise (fun s t : Span => s.2 ≤ t.1) := by
  unfold ranges
  apply separated_clamped
  exact hsep p

/-- Pointwise equality of predicates gives equality of the corresponding filters. -/
theorem filter_congr_eq (ks : List Nat) (P Q : Nat → Bool) (h : ∀ x, P x = Q x) :
    ks.filter P = ks.filter Q := by
  apply List.filter_congr
  intro x hx
  exact h x

theorem multiplicity_of_exact (m : Model)
    (hex : exactSeek m)
    (hsep : ∀ p, (m.plan p).Pairwise (fun s t : Span => s.2 ≤ t.1)) : multiplicity m := by
  intro p ks x
  have hperm := scan_perm_filter_any (ranges m p) ks (ranges_sep m hsep p)
  have hf : ks.filter (fun y => (ranges m p).any (inside · y)) = refScan m p ks := by
    unfold refScan
    apply filter_congr_eq
    intro y
    exact hex p y
  rw [← hf]
  exact hperm.count x

theorem orderedLimit_of_exact (m : Model)
    (hex : exactSeek m)
    (hsep : ∀ p, (m.plan p).Pairwise (fun s t : Span => s.2 ≤ t.1)) : orderedLimit m := by
  intro p ks n hks
  unfold scan
  have hs := scan_eq_filter_any (ranges m p) ks hks (ranges_sep m hsep p)
  have hf : ks.filter (fun y => (ranges m p).any (inside · y)) = refScan m p ks := by
    unfold refScan
    apply filter_congr_eq
    intro y
    exact hex p y
  rw [hs, hf]

theorem reverseWalk_of_exact (m : Model)
    (hex : exactSeek m)
    (hsep : ∀ p, (m.plan p).Pairwise (fun s t : Span => s.2 ≤ t.1)) : reverseWalk m := by
  intro p ks hks
  have hs := scan_eq_filter_any (ranges m p) ks hks (ranges_sep m hsep p)
  calc
    (ranges m p).reverse.flatMap (fun s => ks.reverse.filter (inside s))
        = (ranges m p).reverse.flatMap (fun s => (ks.filter (inside s)).reverse) := by
            congr 1
            funext s
            simp
    _ = ((ranges m p).flatMap (fun s => ks.filter (inside s))).reverse := by
            rw [List.reverse_flatMap]
            rfl
    _ = (refScan m p ks).reverse := by
            rw [hs]
            congr 1
            unfold refScan
            apply filter_congr_eq
            intro y
            exact hex p y

/-- For a span whose upper bound is at most `cap`, its interval predicate holds
    for exactly `h - l` values of `List.range cap`. -/
theorem countP_span_range_le (l h cap : Nat) (hle : h ≤ cap) :
    (List.range cap).countP (inside (l,h)) = h - l := by
  induction cap generalizing l h with
  | zero => simp; omega
  | succ cap ih =>
      rw [List.range_succ, List.countP_append]
      by_cases hh : h ≤ cap
      · rw [ih l h hh]
        have hsing : inside (l,h) cap = false := by simp [inside, Nat.not_lt_of_ge hh]
        simp [hsing]
      · have heq : h = cap+1 := by omega
        subst h
        have hcongr : (List.range cap).countP (inside (l,cap+1)) = (List.range cap).countP (inside (l,cap)) := by
          apply List.countP_congr
          intro x hx
          simp [inside]
          have hx' : x < cap := by simpa using (List.mem_range.mp hx)
          omega
        rw [hcongr, ih l cap (Nat.le_refl cap)]
        have hsing : inside (l,cap+1) cap = true ↔ l ≤ cap := by simp [inside]
        by_cases hl : l ≤ cap
        · simp [hsing.mpr hl, hl]
          omega
        · have hf : inside (l,cap+1) cap = false := by simp [inside]; omega
          simp [hf, hl]
          omega

/-- Disjoint predicates add their counts. -/
theorem countP_or_of_disjoint (l : List Nat) (P Q : Nat → Bool)
    (hd : ∀ x, P x = true → Q x = false) :
    l.countP (fun x => P x || Q x) = l.countP P + l.countP Q := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hP : P x = true
      · have hQ : Q x = false := hd x hP
        simp [hP, hQ, ih]
        omega
      · by_cases hQ : Q x = true
        · simp [hP, hQ, ih]
          omega
        · simp [hP, hQ, ih]

theorem coverage_of_sep (rs : List Span) (cap : Nat)
    (hsep : rs.Pairwise (fun s t : Span => s.2 ≤ t.1))
    (hle : ∀ s ∈ rs, s.2 ≤ cap) :
    (List.range cap).countP (fun x => rs.any (inside · x)) =
      (rs.map (fun s => s.2 - s.1)).sum := by
  induction rs with
  | nil => simp
  | cons s rs ih =>
      rw [List.pairwise_cons] at hsep
      have hd : ∀ x, inside s x = true → rs.any (inside · x) = false := by
        intro x hx
        apply Bool.eq_false_iff.mpr
        intro hany
        rw [List.any_eq_true] at hany
        rcases hany with ⟨t, ht, hti⟩
        have hst := hsep.1 t ht
        simp [inside] at hx hti
        omega
      simp only [List.any_cons, List.map_cons, List.sum_cons]
      rw [countP_or_of_disjoint]
      · rw [ih hsep.2]
        · rw [countP_span_range_le]
          exact hle s (by simp)
        · intro t ht
          exact hle t (by simp [ht])
      · exact hd

theorem coverage_of_exact (m : Model)
    (hex : exactSeek m)
    (hsep : ∀ p, (m.plan p).Pairwise (fun s t : Span => s.2 ≤ t.1)) : exactCoverage m := by
  intro p
  have hs := ranges_sep m hsep p
  have hle : ∀ s ∈ ranges m p, s.2 ≤ p.cap := by
    intro s hs'
    unfold ranges at hs'
    have hs'mem : s ∈ (m.plan p).map (fun (l,h) => (min l p.cap, min h p.cap)) := (List.mem_filter.mp hs').1
    rw [List.mem_map] at hs'mem
    rcases hs'mem with ⟨⟨l, h⟩, hm, rfl⟩
    exact Nat.min_le_right _ _
  have hcov := coverage_of_sep (ranges m p) p.cap hs hle
  rw [← hcov]
  apply List.countP_congr
  intro x hx
  have hxcap : x < p.cap := by simpa using (List.mem_range.mp hx)
  have hb := Bool.eq_iff_iff.mp (hex p x)
  simpa [hxcap] using hb

/-- If a model's ranges are a single span (or empty), the hull is exact. -/
theorem useHull_of_exact_single (m : Model) (hex : exactSeek m)
    (hsingle : ∀ p, (ranges m p).length ≤ 1) : useHull m := by
  intro p x
  cases hrs : ranges m p with
  | nil =>
      have h := hex p x
      rw [hrs] at h
      simp [hull, inside] at h ⊢
      exact h
  | cons s rs =>
      have hnil : rs = [] := by
        have hh := hsingle p
        simp [hrs] at hh
        cases rs with
        | nil => rfl
        | cons t ts => simp at hh
      subst rs
      have h := hex p x
      rw [hrs] at h
      simp [hull, inside] at h ⊢
      exact h

/-- A plan whose raw membership is cap-stable (for keys below `n`) has a
    shrink-domain contract. -/
theorem shrinkDomain_of_raw (m : Model)
  (hraw : ∀ p n x, x < n → n ≤ p.cap →
    (m.plan {p with cap := n}).any (fun s : Span => s.1 ≤ x && x < s.2) =
    (m.plan p).any (fun s : Span => s.1 ≤ x && x < s.2)) : shrinkDomain m := by
  intro p n x hx hn
  unfold ranges
  rw [any_ranges_eq (m.plan {p with cap := n}) n x]
  rw [any_ranges_eq (m.plan p) p.cap x]
  have hcap : x < p.cap := Nat.lt_of_lt_of_le hx hn
  simp [hx, hcap]
  exact hraw p n x hx hn

def runsFrom (prev : Bool) (bs : List Bool) : Nat := ((prev :: bs).zip bs).countP (fun p => !p.1 && p.2)

theorem runsFrom_cons (prev : Bool) (b : Bool) (bs : List Bool) :
    runsFrom prev (b :: bs) = (if !prev && b then 1 else 0) + runsFrom b bs := by
  unfold runsFrom
  by_cases hpb : (!prev && b) = true <;> simp [hpb] <;> omega

theorem getLastD_cons (x : Bool) (xs : List Bool) (prev : Bool) :
    (x :: xs).getLast?.getD prev = xs.getLast?.getD x := by
  cases xs with
  | nil => simp
  | cons y ys => simp [List.getLast?]

theorem runsFrom_append_singleton (prev : Bool) (bs : List Bool) (b : Bool) :
    runsFrom prev (bs ++ [b]) = runsFrom prev bs + (if b && !(bs.getLast?.getD prev) then 1 else 0) := by
  induction bs generalizing prev with
  | nil =>
      unfold runsFrom
      by_cases hpb : (!prev && b) = true <;> simp [hpb, Bool.and_comm]
  | cons x xs ih =>
      rw [List.cons_append, runsFrom_cons, ih]
      rw [runsFrom_cons]
      rw [getLastD_cons]
      omega

theorem runs_eq_runsFrom (bs : List Bool) : runs bs = runsFrom false bs := rfl

theorem runs_eq_countP_rising (P : Nat → Bool) (cap : Nat) :
    runs ((List.range cap).map P) =
      (List.range cap).countP (fun x => P x && (x = 0 || !P (x-1))) := by
  induction cap with
  | zero => simp [runs]
  | succ cap ih =>
      rw [List.range_succ, List.map_append]
      simp only [List.map_cons, List.map_nil]
      rw [runs_eq_runsFrom]
      rw [runsFrom_append_singleton]
      rw [← runs_eq_runsFrom]
      rw [ih]
      rw [List.countP_append]
      simp only [List.countP_singleton]
      have hlast : ((List.range cap).map P).getLast?.getD false = (if cap = 0 then false else P (cap-1)) := by
        cases cap with
        | zero => simp
        | succ cap => simp [List.getLast?_range]
      rw [hlast]
      by_cases hcap : cap = 0
      · subst cap
        simp
      · have hlast' : ((List.range cap).map P).getLast?.getD false = P (cap-1) := by
          simpa [hcap] using hlast
        simp [hcap, hlast']

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common
