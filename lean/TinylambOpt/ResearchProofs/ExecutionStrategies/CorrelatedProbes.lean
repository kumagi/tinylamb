import TinylambOpt.ResearchGoals.ExecutionStrategies.CorrelatedProbes

namespace TinylambOpt.ResearchProofs.ExecutionStrategies.CorrelatedProbes

open TinylambOpt.ResearchGoals.CorrelatedProbes

/-!
# Proofs for the correlated-subquery probes

Goal file: `TinylambOpt.ResearchGoals.ExecutionStrategies.CorrelatedProbes`.

* `OptionalSiblings` — `countP` over the padded Cartesian product factorizes
  as `countP p (pad a) * countP q (pad b)` (`countP_prod`, by induction on the
  left list using `List.countP_append`/`countP_map`/`countP_congr`).
* `OrExists` — the two filtered `zipIdx` branches are disjoint and their
  union predicate is the `||` predicate, so their concatenation is a
  permutation of the combined filter (`filter_or_perm`); both sides are
  sorted by the `zipIdx` occurrence index with injective indices, hence equal
  by `List.Perm.eq_of_pairwise` (`merge_filter_eq`).
-/

/-! ### `OptionalSiblings`: `countP` factorizes over the product -/

private theorem countP_and_const {β : Type} (c : Bool) (q : β → Bool) (bs : List β) :
    bs.countP (fun y => c && q y) = if c then bs.countP q else 0 := by
  cases c with
  | true =>
      simp only [eq_self_iff_true, ite_true]
      apply List.countP_congr
      intro y _
      simp
  | false =>
      rw [show (if (false = true) then bs.countP q else 0) = 0 from by simp]
      rw [List.countP_eq_zero]
      intro a _
      show ¬ ((fun y => false && q y) a) = true
      show ¬ (false = true)
      exact Bool.false_ne_true

private theorem countP_prod {α β : Type} (p : α → Bool) (q : β → Bool) :
    ∀ (as : List α) (bs : List β),
      List.countP (fun (x,y) => p x && q y)
        (as.flatMap fun x => bs.map fun y => (x, y))
        = as.countP p * bs.countP q := by
  intro as
  induction as with
  | nil =>
      intro bs
      simp
  | cons a as ih =>
      intro bs
      rw [List.flatMap_cons, List.countP_append, List.countP_map]
      rw [show ((fun (x,y) => p x && q y) ∘ (fun y => (a,y))) = (fun y => p a && q y) from rfl]
      rw [countP_and_const, ih, List.countP_cons]
      cases h : p a <;> simp [h]
      · rw [Nat.add_mul, Nat.one_mul]
        exact Nat.add_comm _ _

theorem sound_OptionalSiblings : OptionalSiblings.Goal := by
  intro a b p q
  exact countP_prod p q (pad a) (pad b)

/-! ### `OrExists`: merge of two disjoint index-ordered filters -/

/-- Concatenating two pointwise-disjoint filters is a permutation of their
union filter. -/
private theorem filter_or_perm {α : Type} {l : List α} {p q : α → Bool}
    (h : ∀ x ∈ l, ¬ (p x = true ∧ q x = true)) :
    (l.filter p ++ l.filter q).Perm (l.filter fun x => p x || q x) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hxs : ∀ y ∈ xs, ¬ (p y = true ∧ q y = true) :=
        fun y hy => h y (List.mem_cons_of_mem x hy)
      rw [List.filter_cons, List.filter_cons, List.filter_cons]
      by_cases hpx : p x = true
      · by_cases hqx : q x = true
        · exact absurd ⟨hpx, hqx⟩ (h x List.mem_cons_self)
        · have hpq : (p x || q x) = true := by simp [hpx]
          simp only [hpx, hqx, hpq, ↓reduceIte]
          exact (ih hxs).cons x
      · by_cases hqx : q x = true
        · have hpq : (p x || q x) = true := by simp [hqx]
          simp only [hpx, hqx, hpq, ↓reduceIte]
          exact ((List.perm_middle ..).trans ((ih hxs).cons x))
        · have hpq : (p x || q x) = false := by
            have hpx' : p x = false := by cases h' : p x <;> simp_all
            have hqx' : q x = false := by cases h' : q x <;> simp_all
            simp [hpx', hqx']
          simp only [hpx, hqx, hpq, ↓reduceIte]
          exact ih hxs

/-- `zipIdx` occurrences carry strictly increasing indices. -/
private theorem zipIdx_pairwise {α : Type} : ∀ (xs : List α) (i : Nat),
    (xs.zipIdx i).Pairwise (fun a b => a.2 < b.2) := by
  intro xs
  induction xs with
  | nil => intro i; simp
  | cons x xs ih =>
      intro i
      rw [List.zipIdx_cons]
      apply List.Pairwise.cons
      · intro b hb
        obtain ⟨h1, -, -⟩ := List.mem_zipIdx hb
        exact Nat.lt_of_lt_of_le (Nat.lt_succ_self i) h1
      · exact ih (i+1)

/-- Elements of `zipIdx` are determined by their index. -/
private theorem zipIdx_inj {α : Type} {xs : List α} {a b : α × Nat}
    (ha : a ∈ xs.zipIdx) (hb : b ∈ xs.zipIdx) (h : a.2 = b.2) : a = b := by
  obtain ⟨x, i⟩ := a
  obtain ⟨y, j⟩ := b
  obtain ⟨hx, hi, hxeq⟩ := List.mem_zipIdx ha
  obtain ⟨hy, hj, hyeq⟩ := List.mem_zipIdx hb
  have hij : i = j := h
  subst hij
  have hxy : x = y := by rw [hxeq, hyeq]
  cases hxy
  rfl

/-- Generic merge-of-two-filters lemma: if `zs` is index-sorted with injective
indices, `Q1`,`Q2` are pointwise-disjoint and their union predicate is `P`,
then `filter P zs` equals the index-merge of the two filters. -/
private theorem merge_filter_eq {α : Type} {zs : List (α × Nat)}
    {Q1 Q2 P : (α × Nat) → Bool}
    (hzip : zs.Pairwise (fun u v => u.2 ≤ v.2))
    (hinj : ∀ u ∈ zs, ∀ v ∈ zs, u.2 = v.2 → u = v)
    (hdisj : ∀ u ∈ zs, ¬ (Q1 u = true ∧ Q2 u = true))
    (hPQ : ∀ u ∈ zs, (Q1 u || Q2 u) = P u) :
    zs.filter P = List.merge (zs.filter Q1) (zs.filter Q2) (fun u v => u.2 ≤ v.2) := by
  have hperm12 : (zs.filter Q1 ++ zs.filter Q2).Perm (zs.filter fun u => Q1 u || Q2 u) :=
    filter_or_perm hdisj
  have hperm2 : (zs.filter fun u => Q1 u || Q2 u) = (zs.filter P) := by
    apply List.filter_congr
    intro u hu
    exact hPQ u hu
  have hmerge_append_perm : (List.merge (zs.filter Q1) (zs.filter Q2) (fun u v => u.2 ≤ v.2)).Perm
      (zs.filter Q1 ++ zs.filter Q2) :=
    List.merge_perm_append (fun u v => u.2 ≤ v.2) (xs := zs.filter Q1) (ys := zs.filter Q2)
  have hmerge_perm : (List.merge (zs.filter Q1) (zs.filter Q2) (fun u v => u.2 ≤ v.2)).Perm (zs.filter P) := by
    exact hmerge_append_perm.trans (hperm12.trans (List.Perm.of_eq hperm2))
  have hLHS_pw : (zs.filter P).Pairwise (fun u v => u.2 ≤ v.2) :=
    List.Pairwise.filter P hzip
  have hL1_pw : (zs.filter Q1).Pairwise (fun u v => u.2 ≤ v.2) :=
    List.Pairwise.filter Q1 hzip
  have hL2_pw : (zs.filter Q2).Pairwise (fun u v => u.2 ≤ v.2) :=
    List.Pairwise.filter Q2 hzip
  have htrans : ∀ u v w : α × Nat,
      decide (u.2 ≤ v.2) = true → decide (v.2 ≤ w.2) = true → decide (u.2 ≤ w.2) = true := by
    intro u v w h1 h2
    exact decide_eq_true (Nat.le_trans (of_decide_eq_true h1) (of_decide_eq_true h2))
  have htotal : ∀ u v : α × Nat,
      (decide (u.2 ≤ v.2) || decide (v.2 ≤ u.2)) = true := by
    intro u v
    rcases Nat.le_total u.2 v.2 with h | h <;> simp [decide_eq_true h]
  have hL1_pw' : (zs.filter Q1).Pairwise (fun u v => decide (u.2 ≤ v.2) = true) := by
    apply List.Pairwise.imp _ hL1_pw
    intro u v h
    exact decide_eq_true h
  have hL2_pw' : (zs.filter Q2).Pairwise (fun u v => decide (u.2 ≤ v.2) = true) := by
    apply List.Pairwise.imp _ hL2_pw
    intro u v h
    exact decide_eq_true h
  have hmerge_pw_bool : (List.merge (zs.filter Q1) (zs.filter Q2) (fun u v => u.2 ≤ v.2)).Pairwise
      (fun u v => decide (u.2 ≤ v.2) = true) := by
    exact List.pairwise_merge htrans htotal (zs.filter Q1) (zs.filter Q2) hL1_pw' hL2_pw'
  have hmerge_pw : (List.merge (zs.filter Q1) (zs.filter Q2) (fun u v => u.2 ≤ v.2)).Pairwise
      (fun u v => u.2 ≤ v.2) := by
    apply List.Pairwise.imp _ hmerge_pw_bool
    intro u v h
    exact of_decide_eq_true h
  have hanti' : ∀ u v : α × Nat,
      u ∈ zs.filter P → v ∈ List.merge (zs.filter Q1) (zs.filter Q2) (fun u v => u.2 ≤ v.2) →
      u.2 ≤ v.2 → v.2 ≤ u.2 → u = v := by
    intro u v hu hv h1 h2
    have hu_zip : u ∈ zs := (List.mem_filter.mp hu).1
    have hv' : v ∈ zs.filter Q1 ++ zs.filter Q2 := hmerge_append_perm.subset hv
    have hv_zip : v ∈ zs := by
      rcases List.mem_append.mp hv' with h | h
      · exact (List.mem_filter.mp h).1
      · exact (List.mem_filter.mp h).1
    have h2eq : u.2 = v.2 := Nat.le_antisymm h1 h2
    exact hinj u hu_zip v hv_zip h2eq
  exact List.Perm.eq_of_pairwise hanti' hLHS_pw hmerge_pw hmerge_perm.symm

theorem sound_OrExists : OrExists.Goal := by
  intro xs a b
  have hzip : (xs.zipIdx).Pairwise (fun u v => u.2 ≤ v.2) := by
    have h := zipIdx_pairwise xs 0
    exact List.Pairwise.imp (fun hh => Nat.le_of_lt hh) h
  have hinj : ∀ u ∈ xs.zipIdx, ∀ v ∈ xs.zipIdx, u.2 = v.2 → u = v := by
    intro u hu v hv h
    exact zipIdx_inj hu hv h
  have hdisj : ∀ u ∈ xs.zipIdx,
      ¬ ((fun (r,_) => a.contains r.1) u = true ∧
         (fun (r,_) => !a.contains r.1 && b.contains r.2) u = true) := by
    intro u hu
    obtain ⟨r, i⟩ := u
    rintro ⟨h1, h2⟩
    dsimp only at h1 h2
    have h1' : a.contains r.1 = true := h1
    obtain ⟨h2a, -⟩ := Bool.and_eq_true_iff.mp h2
    have h2b : (!a.contains r.1) = true := h2a
    have h2c : a.contains r.1 = false := by
      cases h' : a.contains r.1 <;> simp_all
    rw [h1'] at h2c
    exact Bool.noConfusion h2c
  have hPQ : ∀ u ∈ xs.zipIdx,
      ((fun (r,_) => a.contains r.1) u || (fun (r,_) => !a.contains r.1 && b.contains r.2) u) =
        (fun (r,_) => a.contains r.1 || b.contains r.2) u := by
    intro u hu
    obtain ⟨r, i⟩ := u
    show (a.contains r.1 || (!a.contains r.1 && b.contains r.2)) =
      (a.contains r.1 || b.contains r.2)
    cases h : a.contains r.1 <;> simp
  exact merge_filter_eq hzip hinj hdisj hPQ

/-! ### `PrefixAntichain`: the minimal antichain preserves prefix-existence -/

/-- Every nonempty list has an element minimizing `f`. -/
private theorem exists_min_image {α : Type} (f : α → Nat) :
    ∀ (l : List α), l ≠ [] → ∃ x ∈ l, ∀ y ∈ l, f x ≤ f y := by
  intro l
  induction l with
  | nil => intro h; exact absurd rfl h
  | cons x xs ih =>
      intro h
      cases xs with
      | nil =>
          refine ⟨x, List.mem_cons_self, fun y hy => ?_⟩
          have hyx : y = x := List.mem_singleton.mp hy
          rw [hyx]
          exact Nat.le_refl _
      | cons y ys =>
          obtain ⟨m, hm, hmin⟩ := ih (by simp)
          by_cases hle : f x ≤ f m
          · refine ⟨x, List.mem_cons_self, fun z hz => ?_⟩
            rcases List.mem_cons.mp hz with hzx | hz'
            · rw [hzx]
              exact Nat.le_refl _
            · have h2 := hmin z hz'
              omega
          · refine ⟨m, List.mem_cons_of_mem x hm, fun z hz => ?_⟩
            rcases List.mem_cons.mp hz with hzx | hz'
            · rw [hzx]
              have : f m ≤ f x := by omega
              exact this
            · exact hmin z hz'

/-- `ps.any (·.isPrefixOf s)` agrees with the same test over `minimal ps`:
if a pattern prefixes `s`, the *shortest* such pattern in `ps` survives
`minimal` (no strictly shorter pattern of `ps` prefixes it), and it still
prefixes `s`; conversely every minimal pattern is already in `ps`. -/
private theorem minimal_any_prefix (ps : List (List UInt8)) (s : List UInt8) :
    ps.any (fun p => p.isPrefixOf s) = (minimal ps).any (fun p => p.isPrefixOf s) := by
  by_cases h : ps.any (fun p => p.isPrefixOf s) = true
  · obtain ⟨p, hp_ps, hpP⟩ := List.any_eq_true.mp h
    have hT : (ps.filter fun p => p.isPrefixOf s) ≠ [] := by
      intro hTnil
      have hmem : p ∈ ps.filter (fun p => p.isPrefixOf s) := List.mem_filter.mpr ⟨hp_ps, hpP⟩
      rw [hTnil] at hmem
      simp at hmem
    obtain ⟨m, hmT, hmin⟩ := exists_min_image (fun p => p.length) _ hT
    obtain ⟨hm_ps, hmP⟩ := List.mem_filter.mp hmT
    have hm_min : m ∈ minimal ps := by
      unfold minimal
      rw [List.mem_filter]
      refine ⟨?_, ?_⟩
      · rw [List.mem_eraseDups]
        exact hm_ps
      · have hno : ps.any (fun q => q.length < m.length && q.isPrefixOf m) = false := by
          by_cases hb : ps.any (fun q => q.length < m.length && q.isPrefixOf m) = true
          · obtain ⟨q, hq_ps, hqP⟩ := List.any_eq_true.mp hb
            obtain ⟨hq_len, hq_pre⟩ := Bool.and_eq_true_iff.mp hqP
            have hq_len' : q.length < m.length := of_decide_eq_true hq_len
            have hq_m : q <+: m := List.isPrefixOf_iff_prefix.mp hq_pre
            have hm_s : m <+: s := List.isPrefixOf_iff_prefix.mp hmP
            have hq_s : q <+: s := List.IsPrefix.trans hq_m hm_s
            have hqP' : q.isPrefixOf s = true := List.isPrefixOf_iff_prefix.mpr hq_s
            have hqT : q ∈ ps.filter (fun p => p.isPrefixOf s) :=
              List.mem_filter.mpr ⟨hq_ps, hqP'⟩
            have hle := hmin q hqT
            exact absurd hle (by omega)
          · exact Bool.eq_false_of_ne_true hb
        show (!(ps.any fun q => q.length < m.length && q.isPrefixOf m)) = true
        rw [hno]
        rfl
    have hmin_any : (minimal ps).any (fun p => p.isPrefixOf s) = true :=
      List.any_eq_true.mpr ⟨m, hm_min, hmP⟩
    rw [h, hmin_any]
  · have hfalse : ps.any (fun p => p.isPrefixOf s) = false :=
      Bool.eq_false_of_ne_true h
    have hmin_false : (minimal ps).any (fun p => p.isPrefixOf s) = false := by
      by_cases hb : (minimal ps).any (fun p => p.isPrefixOf s) = true
      · obtain ⟨q, hq_min, hqP⟩ := List.any_eq_true.mp hb
        have hq_ps : q ∈ ps := by
          unfold minimal at hq_min
          exact List.mem_eraseDups.mp (List.mem_filter.mp hq_min).1
        have hps_true : ps.any (fun p => p.isPrefixOf s) = true :=
          List.any_eq_true.mpr ⟨q, hq_ps, hqP⟩
        rw [hfalse] at hps_true
        exact Bool.noConfusion hps_true
      · exact Bool.eq_false_of_ne_true hb
    rw [hfalse, hmin_false]

theorem sound_PrefixAntichain : PrefixAntichain.Goal := by
  intro ps xs
  apply List.filter_congr
  intro x hx
  cases x with
  | none => simp [Option.any_none]
  | some s =>
      rw [Option.any_some, Option.any_some]
      exact minimal_any_prefix ps s

end TinylambOpt.ResearchProofs.ExecutionStrategies.CorrelatedProbes
