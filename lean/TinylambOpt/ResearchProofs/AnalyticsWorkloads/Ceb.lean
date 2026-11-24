import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Ceb

namespace TinylambOpt.ResearchProofs.AnalyticsWorkloads.Ceb

open TinylambOpt.ResearchGoals.AnalyticsWorkloads.Ceb
open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.SemijoinGuardedCheckedEagerSum (i64Min i64Max inI64)

/-!
# Proof strategy overview

We prove the six STATS-CEB propositions from `Ceb.lean`:

* `RepeatedScore`: `repeatChecked` computes, via integer division, the number of
  times `v` can be added to `acc` while staying inside the `Int64` range, and
  agrees with the naive checked loop `repeatedReference`.
* `RoleRouting`: under unique user/post ids, the fused owner-map + eligible-set
  routing produces exactly the flatMap expansion of the reference query.
* `EndpointProfiles`: `linkState` aggregates each source post once and multiplies
  by the typed-destination frequency, matching `summarize` of the flatMap.
* `SparseOuterCorrection`: the sparse correction table assigns each hit a weight
  equal to `max 1` of its non-NULL tag multiplicity.
* `DemandBadges`: the badge-demand histogram lookup equals the product of the
  eligible-user multiplicity and the badge multiplicity at the demanded key.
* `YearOnlyOrder`: bucketing rows by year and sorting bucket keys reproduces the
  stable `mergeSort` by year.
-/

variable {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulBEq α] [LawfulHashable α]

private theorem foldl_hist_getD (xs : List α) (H : Std.HashMap α Nat) (a : α) :
    (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) H).getD a 0 =
      H.getD a 0 + xs.countP (· == a) := by
  induction xs generalizing H with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih, Std.HashMap.getD_insert, List.countP_cons]
      by_cases hxa : x = a
      · obtain rfl : x = a := hxa
        simp
        omega
      · have hx : (x == a) = false := beq_eq_false_iff_ne.mpr hxa
        simp [hx]

private theorem histogram_getD (xs : List α) (a : α) :
    (histogram xs).getD a 0 = xs.countP (· == a) := by
  unfold histogram
  rw [foldl_hist_getD]
  simp

private theorem foldl_hist_contains (xs : List α) (H : Std.HashMap α Nat) (a : α) :
    (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) H).contains a =
      (H.contains a || xs.contains a) := by
  induction xs generalizing H with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih, Std.HashMap.contains_insert, List.contains_cons]
      have hcomm : (x == a) = (a == x) := by
        by_cases h : x = a
        · obtain rfl : x = a := h
          rfl
        · rw [beq_eq_false_iff_ne.mpr h, beq_eq_false_iff_ne.mpr (fun e => h e.symm)]
      rw [hcomm]
      generalize hb : H.contains a = B
      generalize hc : xs.contains a = C
      generalize hp : (a == x) = P
      cases B <;> cases C <;> cases P <;> rfl

private theorem histogram_contains (xs : List α) (a : α) :
    (histogram xs).contains a = xs.contains a := by
  unfold histogram
  rw [foldl_hist_contains]
  simp

/-- Filtering by a `==`-invariant predicate keeps `contains k` iff `p k && l.contains k`. -/
private theorem filter_contains_beq {p : α → Bool} {l : List α} {k : α}
    (hp : ∀ e ∈ l, k == e → p e = p k) :
    (l.filter p).contains k = (p k && l.contains k) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      rw [List.filter_cons]
      have hih := ih (fun e he hke => hp e (List.mem_cons_of_mem x he) hke)
      by_cases hpx : p x = true
      · rw [if_pos hpx, List.contains_cons, hih]
        by_cases hkx : (k == x) = true
        · have hpk : p k = true := by
            have h2 := hp x (List.mem_cons_self) hkx
            rw [hpx] at h2
            exact h2.symm
          simp [hkx, hpk]
        · have hkx' : (k == x) = false := Bool.eq_false_iff.mpr hkx
          simp [hkx']
      · have hpx' : p x = false := Bool.eq_false_iff.mpr hpx
        rw [if_neg hpx, List.contains_cons, hih]
        by_cases hkx : (k == x) = true
        · have hpk : p k = false := by
            have h2 := hp x (List.mem_cons_self) hkx
            rw [hpx'] at h2
            exact h2.symm
          simp [hpk]
        · have hkx' : (k == x) = false := Bool.eq_false_iff.mpr hkx
          simp [hkx']

/-- Counting `k`-elements after filtering by a `==`-invariant predicate is
unaffected when `p k` holds. -/
private theorem filter_countP_beq {p : α → Bool} {l : List α} {k : α}
    (hpk : p k = true) (hp : ∀ e ∈ l, k == e → p e = p k) :
    (l.filter p).countP (· == k) = l.countP (· == k) := by
  rw [List.countP_filter, List.countP_eq_length_filter,
      List.countP_eq_length_filter]
  congr 1
  apply List.filter_congr
  intro a ha
  by_cases hak : (a == k) = true
  · have hae : a = k := beq_iff_eq.mp hak
    have hpa : p a = true := by
      have h2 := hp a ha (beq_iff_eq.mpr hae.symm)
      exact h2.trans hpk
    simp [hak, hpa]
  · have hak' : (a == k) = false := Bool.eq_false_iff.mpr hak
    simp [hak']

/-- `m.keys.contains k` agrees with `m.contains k`. -/
private theorem hashmap_keys_contains {m : Std.HashMap α Nat} (k : α) :
    m.keys.contains k = m.contains k := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have hkk : k ∈ m.keys := by
      have hce := List.contains_eq_mem k m.keys
      rw [hce] at h
      exact decide_eq_true_iff.mp h
    exact Std.HashMap.mem_iff_contains.mp (Std.HashMap.mem_keys.mp hkk)
  · intro h
    have hkm : k ∈ m := Std.HashMap.mem_iff_contains.mp h
    have hkk : k ∈ m.keys := Std.HashMap.mem_keys.mpr hkm
    have hce := List.contains_eq_mem k m.keys
    rw [hce]
    exact decide_eq_true hkk

/-- The badge map preserves first components. -/
private theorem badgeMap_fst {U : Std.HashMap Nat Nat} (l : List (Nat × Nat)) :
    (l.map fun x => (x.fst, x.snd * U.getD x.fst 0)).map Prod.fst =
      l.map Prod.fst := by
  induction l with
  | nil => rfl
  | cons p tl ih =>
      obtain ⟨a, b⟩ := p
      change a :: (tl.map fun x => (x.fst, x.snd * U.getD x.fst 0)).map Prod.fst =
        a :: tl.map Prod.fst
      exact congrArg (List.cons a) ih

/-- `l.contains k = false` implies the `== k` count is zero. -/
private theorem countP_beq_eq_zero_of_contains_false {l : List Nat} {k : Nat}
    (h : l.contains k = false) : l.countP (· == k) = 0 := by
  rw [List.countP_eq_zero]
  intro a ha hak
  have hae : a = k := beq_iff_eq.mp hak
  have hca : l.contains a = true := by
    rw [List.contains_eq_mem]
    exact decide_eq_true ha
  have hce : l.contains a = l.contains k := congrArg (fun x => l.contains x) hae
  have hck : l.contains k = true := hce.symm.trans hca
  rw [h] at hck
  simp at hck

/-! ### DemandBadges -/

theorem sound_DemandBadges : DemandBadges.Goal := by
  intro ps us bs k hk
  unfold badgeDemand
  simp only []
  generalize hD : Std.HashSet.ofList ps = Dd
  generalize hU : histogram (us.filter Dd.contains) = Uu
  generalize hM : histogram (bs.filter Uu.contains) = Mm
  have hDinv : ∀ e ∈ us, k == e → Dd.contains e = Dd.contains k :=
    fun e _ hke => (Std.HashSet.contains_congr hke).symm
  have hUinv : ∀ e ∈ bs, k == e → Uu.contains e = Uu.contains k :=
    fun e _ hke => (Std.HashMap.contains_congr hke).symm
  have hDk : Dd.contains k = true := by
    rw [← hD, Std.HashSet.contains_ofList, List.contains_eq_mem]
    exact decide_eq_true hk
  have hUk : Uu.contains k = us.contains k := by
    have h1 : Uu.contains k = (us.filter Dd.contains).contains k := by
      have hc := histogram_contains (us.filter Dd.contains) k
      rw [hU] at hc
      exact hc
    rw [h1]
    have h2 := filter_contains_beq (p := Dd.contains) (l := us) (k := k) hDinv
    rw [h2, hDk]
    simp
  have hUgetD : Uu.getD k 0 = (us.filter (· == k)).length := by
    rw [← hU, histogram_getD,
      filter_countP_beq (p := Dd.contains) hDk hDinv, List.countP_eq_length_filter]
  have hMcontains : Mm.contains k = (bs.filter Uu.contains).contains k := by
    have hc := histogram_contains (bs.filter Uu.contains) k
    rw [hM] at hc
    exact hc
  have hMk : Mm.contains k = (Uu.contains k && bs.contains k) := by
    rw [hMcontains]
    exact filter_contains_beq (p := Uu.contains) (l := bs) (k := k) hUinv
  have hLfst : ((Mm.toList.map fun x => (x.fst, x.snd * Uu.getD x.fst 0)).map
      Prod.fst) = Mm.keys := by
    rw [badgeMap_fst (U := Uu) (l := Mm.toList), Std.HashMap.map_fst_toList_eq_keys]
  have hgetD0_of : Mm.contains k = false →
      (Std.HashMap.ofList
        (Mm.toList.map fun x => (x.fst, x.snd * Uu.getD x.fst 0))).getD k 0 = 0 := by
    intro hMf
    apply Std.HashMap.getD_ofList_of_contains_eq_false
    rw [hLfst, hashmap_keys_contains]
    exact hMf
  by_cases hUc : Uu.contains k = true
  · by_cases hBc : bs.contains k = true
    · have hMk_true : Mm.contains k = true := by
        rw [hMk, hUc, hBc]
        rfl
      have hMk_mem : k ∈ Mm := Std.HashMap.mem_iff_contains.mpr hMk_true
      have hpair : (k, Mm.getD k 0) ∈ Mm.toList := by
        rw [Std.HashMap.mem_toList_iff_getElem?_eq_some]
        exact Std.HashMap.getElem?_eq_some_getD hMk_mem
      have hdistL : (Mm.toList.map fun x => (x.fst, x.snd * Uu.getD x.fst 0)).Pairwise
          (fun a b => (a.1 == b.1) = false) := by
        have hd := Std.HashMap.distinct_keys_toList (m := Mm)
        exact hd.map (fun (x : Nat × Nat) => (x.fst, x.snd * Uu.getD x.fst 0))
          (fun a b h => h)
      have hmemL : (k, Mm.getD k 0 * Uu.getD k 0) ∈
          Mm.toList.map (fun x => (x.fst, x.snd * Uu.getD x.fst 0)) := by
        rw [List.mem_map]
        exact ⟨(k, Mm.getD k 0), hpair, rfl⟩
      have hgetD : (Std.HashMap.ofList
          (Mm.toList.map fun x => (x.fst, x.snd * Uu.getD x.fst 0))).getD k 0
          = Mm.getD k 0 * Uu.getD k 0 := by
        exact Std.HashMap.getD_ofList_of_mem (beq_iff_eq.mpr rfl) hdistL hmemL
      have hMgetD : Mm.getD k 0 = (bs.filter (· == k)).length := by
        rw [← hM, histogram_getD,
          filter_countP_beq (p := Uu.contains) hUc hUinv, List.countP_eq_length_filter]
      rw [hgetD, hMgetD, hUgetD]
      exact Nat.mul_comm _ _
    · have hBc' : bs.contains k = false := Bool.eq_false_iff.mpr hBc
      have hMk_false : Mm.contains k = false := by
        rw [hMk, hUc, hBc']
        rfl
      have hgetD0 := hgetD0_of hMk_false
      have hbs0 : (bs.filter (· == k)).length = 0 := by
        rw [← List.countP_eq_length_filter]
        exact countP_beq_eq_zero_of_contains_false hBc'
      rw [hgetD0, hbs0]
      simp
  · have hUc' : Uu.contains k = false := Bool.eq_false_iff.mpr hUc
    have hMk_false : Mm.contains k = false := by
      rw [hMk, hUc']
      rfl
    have hgetD0 := hgetD0_of hMk_false
    have h1 : (us.filter Dd.contains).contains k = false := by
      have hc := histogram_contains (us.filter Dd.contains) k
      rw [hU] at hc
      rw [hUc'] at hc
      exact hc.symm
    have hus0 : (us.filter (· == k)).length = 0 := by
      rw [← List.countP_eq_length_filter,
        ← filter_countP_beq (p := Dd.contains) hDk hDinv]
      exact countP_beq_eq_zero_of_contains_false h1
    rw [hgetD0, hus0]
    simp

/-! ### SparseOuterCorrection -/

private theorem countP_pos_mem {l : List α} {s : α} (h : 0 < l.countP (· == s)) : s ∈ l := by
  by_cases hsn : s ∈ l
  · exact hsn
  · exfalso
    have h0 : l.countP (· == s) = 0 := by
      rw [List.countP_eq_zero]
      intro a ha
      have hne : a ≠ s := fun e => hsn (e ▸ ha)
      have hbeq : (a == s) = false := beq_eq_false_iff_ne.mpr hne
      simp [hbeq]
    omega

private theorem hist_mem_of_getD_pos (tags : List (Option String)) (s : String)
    (h : 0 < (histogram (tags.filterMap id)).getD s 0) :
    s ∈ tags.filterMap id := by
  rw [histogram_getD] at h
  exact countP_pos_mem h

private theorem hist_mem_of_getD_pos' (tags : List (Option String)) (s : String)
    (h : 0 < (histogram (tags.filterMap id)).getD s 0) :
    s ∈ histogram (tags.filterMap id) := by
  rw [Std.HashMap.mem_iff_contains, histogram_contains, List.contains_eq_mem]
  exact decide_eq_true (hist_mem_of_getD_pos tags s h)

-- Totals algebra
private theorem Totals.ext {a b : Totals} (h1 : a.cnt = b.cnt) (h2 : a.nn = b.nn)
    (h3 : a.sum = b.sum) : a = b := by
  obtain ⟨c1, n1, s1⟩ := a
  obtain ⟨c2, n2, s2⟩ := b
  simp_all

private theorem plus_zero_left (a : Totals) : plus zero a = a := by
  apply Totals.ext <;> simp [plus, zero]

private theorem plus_zero_right (a : Totals) : plus a zero = a := by
  apply Totals.ext <;> simp [plus, zero]

private theorem plus_comm (a b : Totals) : plus a b = plus b a := by
  apply Totals.ext <;> simp [plus] <;> omega

private theorem plus_assoc (a b c : Totals) : plus (plus a b) c = plus a (plus b c) := by
  apply Totals.ext <;> simp [plus] <;> omega

private theorem weighted_zero (v : Option Int) : weighted 0 v = zero := by
  apply Totals.ext <;> simp [weighted, zero]

private theorem plus_weighted (k : Nat) (v : Option Int) :
    plus (weighted k v) (weighted 1 v) = weighted (k+1) v := by
  apply Totals.ext
  · simp [plus, weighted]
  · simp only [plus, weighted]
    by_cases hv : v.isSome = true <;> simp [hv]
  · simp [plus, weighted, Int.add_mul, Int.one_mul]

private theorem foldl_plus_acc {β : Type} (g : β → Totals) (xs : List β) (st : Totals) :
    xs.foldl (fun a v => plus a (g v)) st =
      plus st (xs.foldl (fun a v => plus a (g v)) zero) := by
  induction xs generalizing st with
  | nil => exact (plus_zero_right st).symm
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [plus_zero_left]
      rw [ih (plus st (g x)), ih (g x)]
      exact plus_assoc _ _ _

private theorem replicate_weighted (n : Nat) (v : Option Int) (st : Totals) :
    (List.replicate n v).foldl (fun a x => plus a (weighted 1 x)) st =
      plus st (weighted n v) := by
  induction n generalizing st with
  | zero => simp [weighted_zero, plus_zero_right]
  | succ n ih =>
      rw [List.replicate_succ, List.foldl_cons, ih]
      rw [plus_assoc]
      have h : plus (weighted 1 v) (weighted n v) = weighted (n+1) v := by
        rw [plus_comm (weighted 1 v) (weighted n v), plus_weighted]
      rw [h]

private theorem summarize_replicate (n : Nat) (v : Option Int) :
    summarize (List.replicate n v) = weighted n v := by
  unfold summarize
  rw [replicate_weighted]
  exact plus_zero_left _

private theorem summarize_cons (x : Option Int) (xs : List (Option Int)) :
    summarize (x :: xs) = plus (weighted 1 x) (summarize xs) := by
  unfold summarize
  rw [List.foldl_cons]
  rw [plus_zero_left]
  exact foldl_plus_acc (fun v => weighted 1 v) xs (weighted 1 x)

private theorem foldl_congr {β γ : Type} (f g : γ → β → γ) (l : List β) (b : γ)
    (h : ∀ acc x, f acc x = g acc x) : l.foldl f b = l.foldl g b := by
  induction l generalizing b with
  | nil => rfl
  | cons x xs ih =>
      rw [List.foldl_cons, List.foldl_cons, h b x]
      exact ih _

-- countP bridges
private theorem countP_none_tag (tags : List (Option String)) :
    tags.countP (fun s => s.isSome && s == none) = 0 := by
  rw [List.countP_eq_zero]
  intro a ha
  cases a with
  | none => simp
  | some x => simp

private theorem countP_some_tag (tags : List (Option String)) (name : String) :
    tags.countP (fun s => s.isSome && s == some name) =
      (histogram (tags.filterMap id)).getD name 0 := by
  rw [histogram_getD, List.countP_filterMap]
  have h : ∀ a, (a.isSome && a == some name) =
      (Option.map (· == name) (id a)).getD false := by
    intro a
    cases a with
    | none => rfl
    | some x => simp
  simp only [h]

private theorem countP_tag (tags : List (Option String)) (t : Option String) :
    tags.countP (fun s => s.isSome && s == t) =
      t.elim 0 ((histogram (tags.filterMap id)).getD · 0) := by
  cases t with
  | none =>
      rw [countP_none_tag]
      rfl
  | some name => exact countP_some_tag tags name

-- extras membership characterization
private theorem extras_mem_iff (tags : List (Option String)) (s : String) (c : Nat) :
    (s, c) ∈ extras tags ↔
      (histogram (tags.filterMap id)).getD s 0 = c + 1 ∧ 1 < c + 1 := by
  unfold extras
  rw [List.mem_filterMap]
  constructor
  · rintro ⟨⟨s', n'⟩, hp, hg⟩
    rw [Std.HashMap.mem_toList_iff_getElem?_eq_some] at hp
    -- hp : hist[s']? = some n'
    obtain ⟨hmem, -⟩ := Std.HashMap.getElem?_eq_some_iff.mp hp
    have hgetD : (histogram (tags.filterMap id)).getD s' 0 = n' := by
      have h2 := Std.HashMap.getElem?_eq_some_getD (fallback := (0:Nat)) hmem
      rw [hp] at h2
      exact (Option.some.inj h2).symm
    simp only [] at hg
    -- hg : (if n' > 1 then some (s', n'-1) else none) = some (s,c)
    by_cases hn : n' > 1
    · simp only [hn] at hg
      obtain ⟨hs', hc'⟩ := Prod.mk.injEq .. |>.mp (Option.some.inj hg)
      -- hs' : s' = s, hc' : n' - 1 = c
      subst hs'
      constructor
      · rw [hgetD]
        omega
      · omega
    · simp only [hn] at hg
      simp at hg
  · rintro ⟨hget, hgt⟩
    -- getD s = c+1 > 1 > 0 → s ∈ hist
    have hpos : 0 < (histogram (tags.filterMap id)).getD s 0 := by omega
    have hmem : s ∈ histogram (tags.filterMap id) := hist_mem_of_getD_pos' tags s hpos
    have hgetElem : (histogram (tags.filterMap id))[s]? = some (c+1) := by
      rw [Std.HashMap.getElem?_eq_some_getD hmem, hget]
    have hp : (s, c+1) ∈ (histogram (tags.filterMap id)).toList := by
      rw [Std.HashMap.mem_toList_iff_getElem?_eq_some]
      exact hgetElem
    refine ⟨(s, c+1), hp, ?_⟩
    show (if c+1 > 1 then some (s, c+1-1) else none) = some (s,c)
    rw [if_pos hgt]
    have : c + 1 - 1 = c := by omega
    rw [this]



private theorem extras_map_fst {s : String} {n : Nat} {b : String × Nat}
    (h : (if n > 1 then some (s, n - 1) else none) = some b) :
    b.1 = s := by
  by_cases hn : n > 1
  · rw [if_pos hn] at h
    obtain ⟨t, m⟩ := b
    obtain ⟨h1, -⟩ := Prod.mk.inj (Option.some.inj h)
    exact h1.symm
  · rw [if_neg hn] at h
    simp at h

private theorem extras_pairwise (tags : List (Option String)) :
    List.Pairwise (fun a b => (a.fst == b.fst) = false) (extras tags) := by
  unfold extras
  refine List.Pairwise.filterMap _ ?_ Std.HashMap.distinct_keys_toList
  intro a a' ha b₁ hg1 b₂ hg2
  obtain ⟨s, n⟩ := a
  obtain ⟨s', n'⟩ := a'
  simp only [] at hg1 hg2 ha ⊢
  have hb1 := extras_map_fst hg1
  have hb2 := extras_map_fst hg2
  rw [hb1, hb2]
  exact ha

private theorem ofList_extras_getD (tags : List (Option String)) (s : String) :
    (Std.HashMap.ofList (extras tags)).getD s 0 =
      if 1 < (histogram (tags.filterMap id)).getD s 0
        then (histogram (tags.filterMap id)).getD s 0 - 1 else 0 := by
  by_cases hgt : 1 < (histogram (tags.filterMap id)).getD s 0
  · rw [if_pos hgt]
    have hmem : (s, (histogram (tags.filterMap id)).getD s 0 - 1) ∈ extras tags := by
      rw [extras_mem_iff]
      constructor <;> omega
    exact Std.HashMap.getD_ofList_of_mem (beq_iff_eq.mpr rfl) (extras_pairwise tags) hmem
  · rw [if_neg hgt]
    apply Std.HashMap.getD_ofList_of_contains_eq_false
    rw [List.contains_eq_mem, decide_eq_false_iff_not]
    intro hmem
    rw [List.mem_map] at hmem
    obtain ⟨⟨s', c⟩, hp, hseq⟩ := hmem
    rw [extras_mem_iff] at hp
    obtain ⟨hget, hgt2⟩ := hp
    have hs' : s' = s := hseq
    rw [hs'] at hget
    have : 1 < (histogram (tags.filterMap id)).getD s 0 := by omega
    exact hgt this

private theorem summarize_flatMap {α : Type} (l : List α) (f : α → List (Option Int)) :
    summarize (l.flatMap f) =
      l.foldl (fun st x => plus st (summarize (f x))) zero := by
  unfold summarize
  rw [List.foldl_flatMap]
  apply foldl_congr
  intro st x
  rw [foldl_plus_acc]

private theorem corrected_eq (hits : List Hit) (tags : List (Option String)) :
    corrected hits tags =
      hits.foldl (fun st (t, v) => plus st (weighted
        ((match extras tags with
          | [] => fun _ => 1
          | [(name, n)] => fun k => if k == some name then n + 1 else 1
          | more =>
            let extra := Std.HashMap.ofList more
            fun k => 1 + k.elim 0 (extra.getD · 0)) t) v)) zero := rfl

private theorem corrected_weight (tags : List (Option String)) (t : Option String) :
    (match extras tags with
      | [] => fun _ => 1
      | [(name, n)] => fun k => if k == some name then n + 1 else 1
      | more =>
        let extra := Std.HashMap.ofList more
        fun k => 1 + k.elim 0 (extra.getD · 0)) t
      = max 1 (tags.countP fun s => s.isSome && s == t) := by
  cases h : extras tags with
  | nil =>
      simp only []
      rw [countP_tag]
      cases t with
      | none =>
          show (1 : Nat) = max 1 0
          exact (Nat.max_eq_left (by omega)).symm
      | some s =>
          show (1 : Nat) = max 1 ((histogram (tags.filterMap id)).getD s 0)
          have hle : (histogram (tags.filterMap id)).getD s 0 ≤ 1 := by
            by_cases hgt : 1 < (histogram (tags.filterMap id)).getD s 0
            · have hmem :
                (s, (histogram (tags.filterMap id)).getD s 0 - 1) ∈ extras tags := by
                rw [extras_mem_iff]
                constructor <;> omega
              rw [h] at hmem
              cases hmem
            · omega
          exact (Nat.max_eq_left hle).symm
  | cons p ps =>
      cases ps with
      | nil =>
          obtain ⟨name, n⟩ := p
          simp only []
          rw [countP_tag]
          cases t with
          | none =>
              show (1 : Nat) = max 1 0
              exact (Nat.max_eq_left (by omega)).symm
          | some s =>
              show (if s == name then n + 1 else 1) =
                max 1 ((histogram (tags.filterMap id)).getD s 0)
              by_cases hsn : s = name
              · have hpos : (s == name) = true := beq_iff_eq.mpr hsn
                rw [if_pos hpos]
                have hget : (histogram (tags.filterMap id)).getD s 0 = n + 1 := by
                  have hm : (s, n) ∈ extras tags := by
                    rw [h]
                    exact List.mem_singleton.mpr (Prod.ext_iff.mpr ⟨hsn, rfl⟩)
                  rw [extras_mem_iff] at hm
                  obtain ⟨h1, -⟩ := hm
                  exact h1
                rw [hget]
                exact (Nat.max_eq_right (by omega)).symm
              · have hb : (s == name) = false := beq_eq_false_iff_ne.mpr hsn
                have hneg : ¬((s == name) = true) := by simp [hb]
                rw [if_neg hneg]
                have hle : (histogram (tags.filterMap id)).getD s 0 ≤ 1 := by
                  by_cases hgt : 1 < (histogram (tags.filterMap id)).getD s 0
                  · have hmem :
                      (s, (histogram (tags.filterMap id)).getD s 0 - 1) ∈ extras tags := by
                      rw [extras_mem_iff]
                      constructor <;> omega
                    rw [h] at hmem
                    rw [List.mem_singleton] at hmem
                    obtain ⟨h1, -⟩ := Prod.mk.inj hmem
                    exact absurd h1 hsn
                  · omega
                exact (Nat.max_eq_left hle).symm
      | cons q qs =>
          obtain ⟨name, n⟩ := p
          obtain ⟨name2, n2⟩ := q
          simp only []
          rw [countP_tag, ← h]
          cases t with
          | none =>
              show (1 : Nat) = max 1 0
              exact (Nat.max_eq_left (by omega)).symm
          | some s =>
              show 1 + (Std.HashMap.ofList (extras tags)).getD s 0 =
                max 1 ((histogram (tags.filterMap id)).getD s 0)
              rw [ofList_extras_getD]
              by_cases hgt : 1 < (histogram (tags.filterMap id)).getD s 0
              · rw [if_pos hgt]
                have h1 : 1 + ((histogram (tags.filterMap id)).getD s 0 - 1) =
                    (histogram (tags.filterMap id)).getD s 0 := by omega
                rw [h1]
                exact (Nat.max_eq_right (by omega)).symm
              · rw [if_neg hgt]
                show (1 : Nat) = max 1 ((histogram (tags.filterMap id)).getD s 0)
                exact (Nat.max_eq_left (by omega)).symm

theorem sound_SparseOuterCorrection : SparseOuterCorrection.Goal := by
  intro hits tags
  rw [corrected_eq]
  have hsf := summarize_flatMap hits
    (fun (t, v) => List.replicate
      (max 1 (tags.countP fun s => s.isSome && s == t)) v)
  rw [hsf]
  apply foldl_congr
  intro st ht
  obtain ⟨t, v⟩ := ht
  simp only []
  rw [corrected_weight, summarize_replicate]


/-! ### EndpointProfiles -/

section EndpointProfiles

/-- The source-aggregate fold: `getD u` equals the `plus`-fold of `weighted 1`
over posts whose id `== u`, seeded with the initial map's value. -/
private theorem src_fold_getD (ps : List PostValue) (H : Std.HashMap Nat Totals)
    (u : Nat) :
    (ps.foldl (fun (h : Std.HashMap Nat Totals) (k, _, v) =>
      h.insert k (plus (h.getD k zero) (weighted 1 v))) H).getD u zero =
      (ps.filter (fun p => p.1 == u)).foldl
        (fun acc p => plus acc (weighted 1 p.2.2)) (H.getD u zero) := by
  induction ps generalizing H with
  | nil => rfl
  | cons p ps ih =>
      obtain ⟨pid, typed, score⟩ := p
      simp only [List.foldl_cons, List.filter_cons]
      rw [ih]
      by_cases hpu : pid == u
      · rw [if_pos hpu]
        have hpid : pid = u := beq_iff_eq.mp hpu
        have hget : (H.insert pid
            (plus (H.getD pid zero) (weighted 1 score))).getD u zero =
            plus (H.getD u zero) (weighted 1 score) := by
          rw [Std.HashMap.getD_insert]
          rw [if_pos hpu]
          have : H.getD pid zero = H.getD u zero := by rw [hpid]
          rw [this]
        rw [hget]
        rfl
      · rw [if_neg hpu]
        have hget : (H.insert pid
            (plus (H.getD pid zero) (weighted 1 score))).getD u zero =
            H.getD u zero := by
          rw [Std.HashMap.getD_insert, if_neg hpu]
        rw [hget]


/-- The destination frequency at `v` equals the number of typed posts with id `v`. -/
private theorem dst_getD (ps : List PostValue) (v : Nat) :
    (histogram ((ps.filter (fun p => p.2.1)).map Prod.fst)).getD v 0 =
      (ps.filter (fun p => p.1 == v && p.2.1)).length := by
  rw [histogram_getD, List.countP_map, List.countP_eq_length_filter,
      List.filter_filter]
  rfl


/-- Componentwise natural-number scaling of a `Totals`. -/
private def scaleTotals (n : Nat) (t : Totals) : Totals :=
  ⟨n * t.cnt, n * t.nn, (n : Int) * t.sum⟩

/-- Scaling commutes with adding a `weighted 1` term. -/
private theorem plus_weighted_scale (n : Nat) (v : Option Int) (t : Totals) :
    plus (weighted n v) (scaleTotals n t) =
      scaleTotals n (plus (weighted 1 v) t) := by
  apply Totals.ext
  · simp only [scaleTotals, plus, weighted]
    rw [Nat.mul_add, Nat.mul_one]
  · simp only [scaleTotals, plus, weighted]
    by_cases hv : v.isSome = true
    · rw [if_pos hv, if_pos hv]
      rw [Nat.mul_add, Nat.mul_one]
    · rw [if_neg hv, if_neg hv]
      simp [Nat.zero_add]
  · simp only [scaleTotals, plus, weighted]
    rw [Int.mul_add]
    simp

/-- `weighted n` distributes over a `plus`-fold: folding `weighted n` gives `n`
times the fold of `weighted 1`, componentwise. -/
private theorem weighted_fold_scale {β : Type} (g : β → Option Int) (n : Nat)
    (xs : List β) :
    xs.foldl (fun acc x => plus acc (weighted n (g x))) zero =
      scaleTotals n (xs.foldl (fun acc x => plus acc (weighted 1 (g x))) zero) := by
  induction xs with
  | nil =>
      apply Totals.ext <;> simp [scaleTotals, zero]
  | cons x xs ih =>
      simp only [List.foldl_cons, plus_zero_left]
      rw [foldl_plus_acc (fun v => weighted n (g v)) xs (weighted n (g x)),
          foldl_plus_acc (fun v => weighted 1 (g v)) xs (weighted 1 (g x)),
          ih]
      exact plus_weighted_scale _ _ _


/-- `summarize` of the empty list is `zero`. -/
private theorem summarize_nil : summarize ([] : List (Option Int)) = zero := by
  rfl

/-- Source aggregate at post id `u`. -/
private def srcAgg (ps : List PostValue) (u : Nat) : Totals :=
  (ps.filter (fun p => p.1 == u)).foldl
    (fun acc p => plus acc (weighted 1 p.2.2)) zero

/-- Target frequency at `v`. -/
private def dstFreq (ps : List PostValue) (v : Nat) : Nat :=
  (ps.filter (fun p => p.1 == v && p.2.1)).length

private theorem scaleTotals_comm (n : Nat) (t : Totals) :
    scaleTotals n t = ⟨t.cnt * n, t.nn * n, t.sum * (n : Int)⟩ := by
  apply Totals.ext
  · exact Nat.mul_comm _ _
  · exact Nat.mul_comm _ _
  · exact Int.mul_comm _ _

private theorem endpoint_contrib (ps : List PostValue) (u v : Nat) :
    summarize ((ps.filter (fun p => p.1 == u)).flatMap fun a =>
        (ps.filter (fun p => p.1 == v && p.2.1)).map fun _ => a.2.2) =
      scaleTotals (dstFreq ps v) (srcAgg ps u) := by
  rw [summarize_flatMap]
  simp only [dstFreq, srcAgg, List.map_const', summarize_replicate]
  exact weighted_fold_scale _ _ _


/-- The fused `linkState` equals a clean per-edge fold. -/
private theorem linkState_fold (ps : List PostValue) (es : List Link) (k : Nat) :
    linkState ps es k =
      es.foldl (fun st (u, v, t) =>
        if t != k then st
        else plus st ⟨(srcAgg ps u).cnt * dstFreq ps v,
                          (srcAgg ps u).nn * dstFreq ps v,
                          (srcAgg ps u).sum * (dstFreq ps v : Int)⟩) zero := by
  unfold linkState
  dsimp only
  apply foldl_congr
  intro st x
  obtain ⟨u, v, t⟩ := x
  dsimp only
  by_cases htk : t != k
  · simp only [ite_eq_left htk]
  · simp only [ite_eq_right htk]
    rw [src_fold_getD, dst_getD]
    have hge : ({} : Std.HashMap Nat Totals).getD u zero = zero := by simp
    rw [hge]
    rfl

/-- C3: the fused endpoint-profile state equals the reference `summarize`. -/
theorem sound_EndpointProfiles : EndpointProfiles.Goal := by
  intro ps es k
  rw [linkState_fold]
  rw [summarize_flatMap]
  apply foldl_congr
  intro st x
  obtain ⟨u, v, t⟩ := x
  dsimp only
  by_cases htk : t != k
  · simp only [ite_eq_left htk]
    have hs : summarize ([] : List (Option Int)) = zero := rfl
    rw [hs]
    exact (plus_zero_right st).symm
  · simp only [ite_eq_right htk]
    have hcontrib := endpoint_contrib ps u v
    rw [scaleTotals_comm] at hcontrib
    rw [hcontrib]

end EndpointProfiles


section RepeatedScore

abbrev P := Option (Except Nat Int) × Int

private def rrPost (st : P) : Except Nat Int :=
  match st.1 with
  | some e => e
  | none => .ok st.2

private def rrPostId (st : P) : Id (Except Nat Int) :=
  Break.runK st.1 (fun _ => pure (.ok st.2)) (fun r => pure r)

private def rrStep (v : Int) (i : Nat) (st : P) : Id (ForInStep P) :=
  let s := st.2 + v
  if (!inI64 s) = true then
    ForInStep.done (some (Except.error (i + 1)), s)
  else
    ForInStep.yield (none, s)

private def refScan (s : Nat) (acc v : Int) : Nat → Except Nat Int
  | 0 => .ok acc
  | n + 1 =>
    let t := acc + v
    if inI64 t then refScan (s + 1) t v n else .error (s + 1)

private theorem rr_bridge (s : Nat) (acc v : Int) (n : Nat) :
    rrPost ((forIn (List.range' s n) (none, acc) (rrStep v)).run) =
      refScan s acc v n := by
  induction n generalizing s acc with
  | zero => rfl
  | succ n ih =>
    rw [List.range'_succ, List.forIn_cons]
    simp only [rrStep, rrPost, refScan]
    dsimp
    cases h : inI64 (acc + v) <;> simp
    · show (Except.error (s + 1) : Except Nat Int) = Except.error (s + 1)
      rfl
    · show rrPost ((forIn (List.range' (s + 1) n) (none, acc + v) (rrStep v)).run) =
        refScan (s + 1) (acc + v) v n
      exact ih (s + 1) (acc + v)

private theorem rr_connect (acc v : Int) (n : Nat) :
    repeatedReference acc v n =
      Id.run (rrPostId ((forIn (List.range' 0 n) (none, acc) (rrStep v)).run)) := by
  unfold repeatedReference rrPostId rrStep
  rw [List.range_eq_range']
  simp only [Id.run_bind]
  rfl

private theorem postId_eq (st : P) :
    Id.run (rrPostId st) = rrPost st := by
  unfold rrPostId rrPost
  cases st with
  | mk o i =>
    cases o <;> rfl

private theorem repeatedReference_eq_refScan (acc v : Int) (n : Nat) :
    repeatedReference acc v n = refScan 0 acc v n := by
  rw [rr_connect]
  have h := postId_eq ((forIn (List.range' 0 n) (none, acc) (rrStep v)).run)
  rw [h]
  exact rr_bridge 0 acc v n

-- inI64 helpers
private theorem inI64_iff {z : Int} : inI64 z = true ↔ i64Min ≤ z ∧ z ≤ i64Max := by
  simp [inI64]
private theorem inI64_bounds {z : Int} (h : inI64 z = true) : i64Min ≤ z ∧ z ≤ i64Max :=
  inI64_iff.mp h
private theorem inI64_mk {z : Int} (h1 : i64Min ≤ z) (h2 : z ≤ i64Max) : inI64 z = true :=
  inI64_iff.mpr ⟨h1, h2⟩
private theorem inI64_of_nonneg_le {z : Int} (h0 : 0 ≤ z) (h1 : z ≤ i64Max) : inI64 z = true := by
  apply inI64_mk
  · unfold i64Min; omega
  · exact h1
private theorem inI64_of_nonpos_ge {z : Int} (h0 : z ≤ 0) (h1 : i64Min ≤ z) : inI64 z = true := by
  apply inI64_mk
  · exact h1
  · unfold i64Max; omega
private theorem inI64_false {z : Int} (h : inI64 z = false) :
    ¬ (i64Min ≤ z ∧ z ≤ i64Max) := by
  have hne : inI64 z ≠ true := by rw [h]; simp
  intro hp
  exact hne (inI64_iff.mpr hp)
private theorem inI64_eq_false_iff {z : Int} :
    inI64 z = false ↔ ¬ (i64Min ≤ z ∧ z ≤ i64Max) := by
  constructor
  · intro h hp
    have ht : inI64 z = true := inI64_iff.mpr hp
    rw [h] at ht
    cases ht
  · intro h
    by_cases ht : inI64 z = true
    · exact absurd (inI64_iff.mp ht) h
    · cases hbb : inI64 z <;> simp_all

-- safeAdds
private def safeAdds (acc v : Int) : Nat :=
  if 0 < v then ((i64Max - acc) / v).toNat
  else if v < 0 then ((acc - i64Min) / (-v)).toNat
  else 0

private theorem safeAdds_pos (acc v : Int) (hv : 0 < v) :
    safeAdds acc v = ((i64Max - acc) / v).toNat := by
  unfold safeAdds; rw [ite_eq_left hv]

private theorem safeAdds_neg (acc v : Int) (hv : v < 0) :
    safeAdds acc v = ((acc - i64Min) / (-v)).toNat := by
  unfold safeAdds
  rw [ite_eq_right (show ¬ (0:Int) < v by omega), ite_eq_left hv]

private theorem safeAdds_eq_zero_iff (acc v : Int) (hacc : inI64 acc = true)
    (hv : v ≠ 0) :
    safeAdds acc v = 0 ↔ inI64 (acc + v) = false := by
  obtain ⟨hlo, hhi⟩ := inI64_bounds hacc
  by_cases hvn : v < 0
  · have hd : 0 < -v := by omega
    have hne : -v ≠ 0 := by omega
    rw [safeAdds_neg _ _ hvn]
    have hnum : 0 ≤ acc - i64Min := by unfold i64Min at hlo ⊢; omega
    have htoNat : ((acc - i64Min) / (-v)).toNat = 0 ↔ (acc - i64Min) / (-v) = 0 := by
      rw [Int.toNat_eq_zero]
      have hq : 0 ≤ (acc - i64Min) / (-v) := Int.ediv_nonneg hnum (Int.le_of_lt hd)
      omega
    have hq0 : (acc - i64Min) / (-v) = 0 ↔ acc - i64Min < -v := by
      constructor
      · intro h
        by_cases hge : -v ≤ acc - i64Min
        · have h1 : 1 ≤ (acc - i64Min) / (-v) := by
            rw [Int.le_ediv_iff_mul_le hd, Int.one_mul]
            exact hge
          omega
        · exact Int.not_le.mp hge
      · intro hlt
        exact Int.ediv_eq_zero_of_lt hnum hlt
    rw [htoNat, hq0]
    have hlin : acc - i64Min < -v ↔ acc + v < i64Min := by unfold i64Min; omega
    rw [hlin, inI64_eq_false_iff]
    have hup : acc + v ≤ i64Max := by unfold i64Max at hhi ⊢; omega
    constructor
    · intro hlt hp
      have : i64Min ≤ acc + v := hp.1
      unfold i64Min at hlt this; omega
    · intro h
      by_cases hlo2 : i64Min ≤ acc + v
      · exact absurd ⟨hlo2, hup⟩ h
      · exact Int.not_le.mp hlo2
  · by_cases hvp : 0 < v
    · have hne : v ≠ 0 := hv
      rw [safeAdds_pos _ _ hvp]
      have hnum : 0 ≤ i64Max - acc := by unfold i64Max at hhi ⊢; omega
      have htoNat : ((i64Max - acc) / v).toNat = 0 ↔ (i64Max - acc) / v = 0 := by
        rw [Int.toNat_eq_zero]
        have hq : 0 ≤ (i64Max - acc) / v := Int.ediv_nonneg hnum (Int.le_of_lt hvp)
        omega
      have hq0 : (i64Max - acc) / v = 0 ↔ i64Max - acc < v := by
        constructor
        · intro h
          by_cases hge : v ≤ i64Max - acc
          · have h1 : 1 ≤ (i64Max - acc) / v := by
              rw [Int.le_ediv_iff_mul_le hvp, Int.one_mul]
              exact hge
            omega
          · exact Int.not_le.mp hge
        · intro hlt
          exact Int.ediv_eq_zero_of_lt hnum hlt
      rw [htoNat, hq0]
      have hlin : i64Max - acc < v ↔ i64Max < acc + v := by unfold i64Max; omega
      rw [hlin, inI64_eq_false_iff]
      have hlo2 : i64Min ≤ acc + v := by unfold i64Min at hlo ⊢; omega
      constructor
      · intro hlt hp
        have : acc + v ≤ i64Max := hp.2
        unfold i64Max at hlt this; omega
      · intro h
        by_cases hup2 : acc + v ≤ i64Max
        · exact absurd ⟨hlo2, hup2⟩ h
        · exact Int.not_le.mp hup2
    · have : v = 0 := by omega
      exact absurd this hv

private theorem safeAdds_succ (acc v : Int) (hacc : inI64 acc = true)
    (hv : v ≠ 0) (hnext : inI64 (acc + v) = true) :
    safeAdds (acc + v) v = safeAdds acc v - 1 := by
  obtain ⟨hlo, hhi⟩ := inI64_bounds hacc
  obtain ⟨hnlo, hnhi⟩ := inI64_bounds hnext
  by_cases hvn : v < 0
  · have hd : 0 < -v := by omega
    have hne : -v ≠ 0 := by omega
    rw [safeAdds_neg _ _ hvn, safeAdds_neg _ _ hvn]
    have hsub : acc + v - i64Min = (acc - i64Min) - (-v) := by omega
    have hdiv : ((acc - i64Min) - (-v)) / (-v) = (acc - i64Min) / (-v) - 1 := by
      have h := Int.sub_mul_ediv_left (acc - i64Min) 1 hne
      rwa [Int.mul_one] at h
    have hq : 1 ≤ (acc - i64Min) / (-v) := by
      rw [Int.le_ediv_iff_mul_le hd]
      omega
    rw [hsub, hdiv]
    omega
  · by_cases hvp : 0 < v
    · rw [safeAdds_pos _ _ hvp, safeAdds_pos _ _ hvp]
      have hsub : i64Max - (acc + v) = (i64Max - acc) - v := by omega
      have hne : v ≠ 0 := hv
      have hdiv : ((i64Max - acc) - v) / v = (i64Max - acc) / v - 1 := by
        have h := Int.sub_mul_ediv_left (i64Max - acc) 1 hne
        rwa [Int.mul_one] at h
      have hq : 1 ≤ (i64Max - acc) / v := by
        rw [Int.le_ediv_iff_mul_le hvp]
        omega
      rw [hsub, hdiv]
      omega
    · have : v = 0 := by omega
      exact absurd this hv

private def checkedSafe (acc v : Int) (n : Nat) : Nat :=
  if 0 < v then ((i64Max - acc) / v).toNat
  else if v < 0 then ((acc - i64Min) / (-v)).toNat else n

private theorem repeatChecked_eq (acc v : Int) (n : Nat) :
    repeatChecked acc v n =
      if checkedSafe acc v n < n then .error (checkedSafe acc v n + 1)
      else .ok (acc + v * n) := by
  unfold repeatChecked checkedSafe
  rfl

private theorem checkedSafe_eq_safeAdds (acc v : Int) (hv : v ≠ 0) (n : Nat) :
    checkedSafe acc v n = safeAdds acc v := by
  unfold checkedSafe safeAdds
  by_cases hvp : 0 < v
  · rw [ite_eq_left hvp, ite_eq_left hvp]
  · by_cases hvn : v < 0
    · rw [ite_eq_right (show ¬ (0:Int) < v by omega), ite_eq_left hvn,
          ite_eq_right (show ¬ (0:Int) < v by omega), ite_eq_left hvn]
    · have : v = 0 := by omega
      exact absurd this hv

-- refScan characterization for v ≠ 0
private theorem refScan_spec (v : Int) (hv : v ≠ 0) :
    ∀ acc s n, inI64 acc = true →
      refScan s acc v n =
        if n ≤ safeAdds acc v then .ok (acc + v * n)
        else .error (s + safeAdds acc v + 1) := by
  intro acc s n hacc
  induction n generalizing acc s with
  | zero =>
    show (Except.ok acc : Except Nat Int) = _
    rw [ite_eq_left (Nat.zero_le _)]
    congr 1
    simp
  | succ n ih =>
    show (let t := acc + v; if inI64 t then refScan (s + 1) t v n
          else .error (s + 1)) = _
    dsimp only
    by_cases hnext : inI64 (acc + v) = true
    · rw [ite_eq_left hnext]
      -- LHS = refScan (s+1) (acc+v) v n ; apply IH
      rw [ih (acc + v) (s + 1) hnext]
      -- safeAdds_succ
      have hsucc := safeAdds_succ acc v hacc hv hnext
      by_cases hle : n + 1 ≤ safeAdds acc v
      · -- all safe: n ≤ safe-1 = safeAdds(acc+v)
        have hsafe1 : 1 ≤ safeAdds acc v := by omega
        have hnle : n ≤ safeAdds (acc + v) v := by omega
        rw [ite_eq_left hnle, ite_eq_left hle]
        congr 1
        -- (acc+v) + v*n = acc + v*(n+1)
        rw [Int.natCast_add_one, Int.mul_add, Int.mul_one]
        omega
      · -- overflow at safe+1
        have hsafe1 : 1 ≤ safeAdds acc v := by
          have hne : safeAdds acc v ≠ 0 := by
            intro hz
            have h2 := (safeAdds_eq_zero_iff acc v hacc hv).mp hz
            rw [h2] at hnext
            cases hnext
          omega
        have hnle : ¬ n ≤ safeAdds (acc + v) v := by omega
        rw [ite_eq_right hnle, ite_eq_right hle]
        congr 1
        omega
    · -- inI64(acc+v) ≠ true → = false → safeAdds = 0
      have hfalse : inI64 (acc + v) = false := by
        cases hb : inI64 (acc + v) <;> simp_all
      have hs0 : safeAdds acc v = 0 := (safeAdds_eq_zero_iff acc v hacc hv).mpr hfalse
      rw [ite_eq_right hnext]
      rw [ite_eq_right (by omega : ¬ n + 1 ≤ safeAdds acc v)]
      congr 1
      omega

-- v = 0: every step keeps acc
private theorem refScan_zero (acc : Int) (hacc : inI64 acc = true) :
    ∀ s n, refScan s acc 0 n = .ok acc := by
  intro s n
  induction n generalizing s with
  | zero => rfl
  | succ n ih =>
    show (let t := acc + 0; if inI64 t then refScan (s + 1) t 0 n
          else .error (s + 1)) = _
    have h0 : acc + (0 : Int) = acc := by omega
    rw [h0]
    dsimp only
    rw [ite_eq_left hacc]
    exact ih (s + 1)

theorem sound_RepeatedScore : RepeatedScore.Goal := by
  intro acc v n hacc hvi
  by_cases hvz : v = 0
  · subst hvz
    rw [repeatedReference_eq_refScan, refScan_zero acc hacc 0 n]
    rw [repeatChecked_eq]
    have hcs : checkedSafe acc 0 n = n := by
      unfold checkedSafe
      rw [ite_eq_right (by decide : ¬ ((0:Int) < 0)),
          ite_eq_right (by decide : ¬ ((0:Int) < 0))]
    rw [hcs, ite_eq_right (by omega : ¬ n < n)]
    congr 1
    simp
  · rw [repeatChecked_eq, checkedSafe_eq_safeAdds acc v hvz n]
    rw [repeatedReference_eq_refScan,
        refScan_spec v hvz acc 0 n hacc]
    by_cases h : safeAdds acc v < n
    · rw [ite_eq_left h]
      have hn : ¬ n ≤ safeAdds acc v := by omega
      rw [ite_eq_right hn]
      congr 1
      omega
    · have hn : n ≤ safeAdds acc v := by omega
      rw [ite_eq_right h, ite_eq_left hn]

end RepeatedScore


section YearOnlyOrder

private def yearStep (H : Std.HashMap Int (List Group)) (r : Group) : Std.HashMap Int (List Group) :=
  H.insert r.1 (r :: H.getD r.1 [])

private theorem yearLe_trans : ∀ a b c : Group,
    decide (a.1 ≤ b.1) = true → decide (b.1 ≤ c.1) = true →
    decide (a.1 ≤ c.1) = true := by
  intro a b c h1 h2
  simp only [decide_eq_true_eq] at h1 h2 ⊢
  omega

private theorem yearLe_total : ∀ a b : Group,
    (decide (a.1 ≤ b.1) || decide (b.1 ≤ a.1)) = true := by
  intro a b
  by_cases h : a.1 ≤ b.1
  · rw [decide_eq_true h]; simp
  · have hb : b.1 ≤ a.1 := by omega
    rw [decide_eq_true hb]; simp

private theorem intLe_trans : ∀ a b c : Int,
    decide (a ≤ b) = true → decide (b ≤ c) = true → decide (a ≤ c) = true := by
  intro a b c h1 h2
  simp only [decide_eq_true_eq] at h1 h2 ⊢
  omega

private theorem intLe_total : ∀ a b : Int,
    (decide (a ≤ b) || decide (b ≤ a)) = true := by
  intro a b
  by_cases h : a ≤ b
  · rw [decide_eq_true h]; simp
  · have hb : b ≤ a := by omega
    rw [decide_eq_true hb]; simp

private theorem mergeSort_filter_year (rows : List Group) (y : Int) :
    ((rows.mergeSort (fun a b => a.1 ≤ b.1)).filter (fun r => r.1 == y)) =
      rows.filter (fun r => r.1 == y) := by
  induction rows with
  | nil => simp
  | cons a l ih =>
    obtain ⟨l₁, l₂, hms, hmsl, hbefore⟩ := List.mergeSort_cons
      (le := fun a b : Group => a.1 ≤ b.1) yearLe_trans yearLe_total a l
    rw [hms, List.filter_append]
    simp only [List.filter_cons]
    by_cases hay : (a.1 == y) = true
    · have hay' : a.1 = y := beq_iff_eq.mp hay
      have hl1 : l₁.filter (fun r => r.1 == y) = [] := by
        rw [List.filter_eq_nil_iff]
        intro b hb hby
        have hbye : b.1 = y := by simpa using hby
        have hb2 := hbefore b hb
        simp at hb2
        omega
      have h2 := ih
      rw [hmsl, List.filter_append, hl1, List.nil_append] at h2
      rw [ite_eq_left hay, ite_eq_left hay]
      rw [hl1, List.nil_append]
      exact congrArg (List.cons a) h2
    · rw [ite_eq_right hay, ite_eq_right hay]
      have h2 := ih
      rw [hmsl, List.filter_append] at h2
      exact h2

private theorem filter_split (L : List Group) (y : Int)
    (hL : L.Pairwise (fun a b => a.1 ≤ b.1)) (hy : ∀ x ∈ L, y ≤ x.1) :
    L.filter (fun x => x.1 == y) ++ L.filter (fun x => !(x.1 == y)) = L := by
  induction L with
  | nil => rfl
  | cons a l ih =>
    obtain ⟨hhead, hl⟩ := List.pairwise_cons.mp hL
    have hyl : ∀ x ∈ l, y ≤ x.1 := fun x hx => hy x (List.mem_cons_of_mem a hx)
    have hymin : y ≤ a.1 := hy a (List.mem_cons_self)
    by_cases hay : (a.1 == y) = true
    · rw [List.filter_cons, List.filter_cons]
      have hnot : (!(a.1 == y)) = false := by simp [hay]
      have hnc : ¬ ((!(a.1 == y)) = true) := by simp [hnot]
      rw [ite_eq_left hay, ite_eq_right hnc]
      rw [List.cons_append]
      congr 1
      exact ih hl hyl
    · have hayf : (a.1 == y) = false := by cases h2 : (a.1 == y) <;> simp_all
      have hgt : y < a.1 := by
        have hne : y ≠ a.1 := by
          intro he
          have hyeq : a.1 = y := he.symm
          rw [hyeq] at hayf
          simp at hayf
        omega
      have hlf : l.filter (fun x => x.1 == y) = [] := by
        rw [List.filter_eq_nil_iff]
        intro b hb hby
        have hbge : a.1 ≤ b.1 := hhead b hb
        have hbye : b.1 = y := by simpa using hby
        have hbne : b.1 ≠ y := by omega
        exact hbne hbye
      rw [List.filter_cons, List.filter_cons]
      have hnc1 : ¬ ((a.1 == y) = true) := by simp [hayf]
      have hnot2 : ((!(a.1 == y)) = true) := by simp [hayf]
      rw [ite_eq_right hnc1, ite_eq_left hnot2]
      rw [hlf, List.nil_append]
      have hlf2 : l.filter (fun x => !(x.1 == y)) = l := by
        have h2 := ih hl hyl
        rw [hlf, List.nil_append] at h2
        exact h2
      rw [hlf2]

private theorem flatMap_congr_mem {S : List Int} {f g : Int → List Group}
    (h : ∀ y ∈ S, f y = g y) : S.flatMap f = S.flatMap g := by
  induction S with
  | nil => rfl
  | cons y S ih =>
    rw [List.flatMap_cons, List.flatMap_cons]
    rw [h y (List.mem_cons_self)]
    congr 1
    exact ih (fun z hz => h z (List.mem_cons_of_mem y hz))

private theorem groupBy_reconstruct {S : List Int} {L : List Group}
    (hSsort : S.Pairwise (· ≤ ·)) (hSnodup : S.Nodup)
    (hmem : ∀ y, y ∈ S ↔ ∃ x ∈ L, x.1 = y)
    (hLsort : L.Pairwise (fun a b => a.1 ≤ b.1)) :
    L = S.flatMap (fun y => L.filter (fun x => x.1 == y)) := by
  induction S generalizing L with
  | nil =>
    have hL : L = [] := by
      cases L with
      | nil => rfl
      | cons x xs =>
        exfalso
        have hx : x.1 ∈ ([] : List Int) := (hmem x.1).mpr ⟨x, List.mem_cons_self, rfl⟩
        simp at hx
    rw [hL]
    rfl
  | cons y S ih =>
    obtain ⟨hytail, hSsort⟩ := List.pairwise_cons.mp hSsort
    obtain ⟨hyny, hSnodup⟩ := List.nodup_cons.mp hSnodup
    have hymin : ∀ x ∈ L, y ≤ x.1 := by
      intro x hx
      have hxmem : x.1 ∈ y :: S := (hmem x.1).mpr ⟨x, hx, rfl⟩
      rcases List.mem_cons.mp hxmem with hxy | hxS
      · omega
      · exact hytail x.1 hxS
    have hsplit := filter_split L y hLsort hymin
    have hLssort : (L.filter (fun x => !(x.1 == y))).Pairwise (fun a b => a.1 ≤ b.1) :=
      List.Pairwise.filter _ hLsort
    have hmemS : ∀ z, z ∈ S ↔ ∃ x ∈ L.filter (fun x => !(x.1 == y)), x.1 = z := by
      intro z
      constructor
      · intro hzS
        have hzmem : z ∈ y :: S := List.mem_cons_of_mem y hzS
        obtain ⟨x, hxL, hxyz⟩ := (hmem z).mp hzmem
        have hzny : z ≠ y := by
          intro hzy
          rw [hzy] at hzS
          exact hyny hzS
        refine ⟨x, ?_, hxyz⟩
        rw [List.mem_filter]
        refine ⟨hxL, ?_⟩
        rw [hxyz]
        have hzeq : (z == y) = false := beq_eq_false_iff_ne.mpr hzny
        rw [hzeq]
        rfl
      · intro ⟨x, hxmem, hxyz⟩
        rw [List.mem_filter] at hxmem
        obtain ⟨hxL, hxne⟩ := hxmem
        have hxmem' : x.1 ∈ y :: S := (hmem x.1).mpr ⟨x, hxL, rfl⟩
        have hzmem : z ∈ y :: S := by rw [← hxyz]; exact hxmem'
        rcases List.mem_cons.mp hzmem with hzy | hzS
        · exfalso
          have hxy : x.1 = y := hxyz.trans hzy
          rw [hxy] at hxne
          simp at hxne
        · exact hzS
    have hrec : L.filter (fun x => !(x.1 == y)) =
        S.flatMap (fun z => (L.filter (fun x => !(x.1 == y))).filter (fun x => x.1 == z)) :=
      ih hSsort hSnodup hmemS hLssort
    have hzne : ∀ z ∈ S, z ≠ y := by
      intro z hzS hzy
      rw [hzy] at hzS
      exact hyny hzS
    have hcongr : ∀ z ∈ S, L.filter (fun x => x.1 == z) =
        (L.filter (fun x => !(x.1 == y))).filter (fun x => x.1 == z) := by
      intro z hzS
      rw [List.filter_filter]
      apply List.filter_congr
      intro x hxL
      have hzny := hzne z hzS
      by_cases hxz : (x.1 == z) = true
      · have hxz' : x.1 = z := beq_iff_eq.mp hxz
        have hxne : x.1 ≠ y := by
          intro h
          apply hzny
          rw [← hxz']
          exact h
        have hxy : (x.1 == y) = false := beq_eq_false_iff_ne.mpr hxne
        rw [hxz, hxy]
        decide
      · simp [hxz]
    have hfm : S.flatMap (fun z => L.filter (fun x => x.1 == z)) =
        S.flatMap (fun z => (L.filter (fun x => !(x.1 == y))).filter (fun x => x.1 == z)) :=
      flatMap_congr_mem hcongr
    have hfinal : L = (y :: S).flatMap (fun y => L.filter (fun x => x.1 == y)) := by
      rw [List.flatMap_cons, hfm, ← hrec]
      exact hsplit.symm
    exact hfinal

private theorem foldl_year_getD (H : Std.HashMap Int (List Group)) (xs : List Group) (a : Int) :
    (xs.foldl yearStep H).getD a [] = (xs.filter (fun r => r.1 == a)).reverse ++ H.getD a [] := by
  induction xs generalizing H with
  | nil => simp [List.foldl, List.filter, List.nil_append]
  | cons r xs ih =>
    rw [List.foldl_cons]
    show (List.foldl yearStep (H.insert r.1 (r :: H.getD r.1 [])) xs).getD a [] = _
    rw [ih, Std.HashMap.getD_insert]
    by_cases hra : (r.1 == a) = true
    · have hra' : r.1 = a := beq_iff_eq.mp hra
      rw [ite_eq_left hra, hra']
      rw [List.filter_cons, ite_eq_left hra]
      rw [List.reverse_cons]
      simp only [List.cons_append, List.nil_append, List.append_assoc]
    · rw [List.filter_cons]
      rw [ite_eq_right hra, ite_eq_right hra]

private theorem foldl_year_mem (H : Std.HashMap Int (List Group)) (xs : List Group) (y : Int) :
    y ∈ (xs.foldl yearStep H) ↔ (∃ r ∈ xs, r.1 = y) ∨ y ∈ H := by
  induction xs generalizing H with
  | nil => simp [List.foldl]
  | cons r xs ih =>
    rw [List.foldl_cons]
    show y ∈ (List.foldl yearStep (H.insert r.1 (r :: H.getD r.1 [])) xs) ↔ _
    rw [ih, Std.HashMap.mem_insert]
    constructor
    · intro h
      rcases h with ⟨r', hr', hr'y⟩ | h | h
      · exact Or.inl ⟨r', List.mem_cons.mpr (Or.inr hr'), hr'y⟩
      · exact Or.inl ⟨r, List.mem_cons_self, beq_iff_eq.mp h⟩
      · exact Or.inr h
    · intro h
      rcases h with ⟨r', hr', hr'y⟩ | h
      · rcases List.mem_cons.mp hr' with heq | hxs
        · subst heq
          exact Or.inr (Or.inl (beq_iff_eq.mpr hr'y))
        · exact Or.inl ⟨r', hxs, hr'y⟩
      · exact Or.inr (Or.inr h)

private theorem yearKeys_mem (rows : List Group) (y : Int) :
    y ∈ (rows.foldl yearStep {}).keys ↔ ∃ r ∈ rows, r.1 = y := by
  rw [Std.HashMap.mem_keys]
  rw [foldl_year_mem]
  simp [Std.HashMap.not_mem_empty, or_false]

private theorem yearBuckets_eq (rows : List Group) :
    yearBuckets rows =
      (rows.foldl yearStep {}).keys.mergeSort.flatMap (fun y => rows.filter (fun x => x.1 == y)) := by
  have hbuck : ∀ y : Int,
      (((rows.foldl yearStep {} : Std.HashMap Int (List Group)).getD y []).reverse) =
        rows.filter (fun x => x.1 == y) := by
    intro y
    rw [foldl_year_getD, Std.HashMap.getD_empty, List.append_nil, List.reverse_reverse]
  unfold yearBuckets
  show ((rows.foldl yearStep {} : Std.HashMap Int (List Group)).keys.mergeSort).flatMap
      (fun y => ((rows.foldl yearStep {} : Std.HashMap Int (List Group)).getD y []).reverse) = _
  rw [flatMap_congr_mem (fun y _ => hbuck y)]

theorem sound_YearOnlyOrder : YearOnlyOrder.Goal := by
  intro rows
  have hSsort : ((rows.foldl yearStep {}).keys.mergeSort).Pairwise (· ≤ ·) := by
    have hp := List.pairwise_mergeSort (le := fun a b : Int => a ≤ b) intLe_trans intLe_total (rows.foldl yearStep {}).keys
    exact List.Pairwise.imp (fun {a b} h => of_decide_eq_true h) hp
  have hSnodup : ((rows.foldl yearStep {}).keys.mergeSort).Nodup := by
    have hperm := List.mergeSort_perm (rows.foldl yearStep {}).keys (fun a b : Int => a ≤ b)
    have hnodup : ((rows.foldl yearStep {}).keys.mergeSort (fun a b : Int => a ≤ b)).Nodup ↔ (rows.foldl yearStep {}).keys.Nodup := hperm.nodup_iff
    rw [hnodup]
    exact Std.HashMap.nodup_keys
  have hmem : ∀ y, y ∈ (rows.foldl yearStep {}).keys.mergeSort ↔ ∃ x ∈ rows.mergeSort (fun a b => a.1 ≤ b.1), x.1 = y := by
    intro y
    have h1 : y ∈ (rows.foldl yearStep {}).keys.mergeSort ↔ y ∈ (rows.foldl yearStep {}).keys := by
      have hperm := List.mergeSort_perm (rows.foldl yearStep {}).keys (fun a b : Int => a ≤ b)
      exact List.Perm.mem_iff hperm
    have h2 : y ∈ (rows.foldl yearStep {}).keys ↔ ∃ r ∈ rows, r.1 = y := yearKeys_mem rows y
    have h3 : (∃ r ∈ rows, r.1 = y) ↔ ∃ x ∈ rows.mergeSort (fun a b => a.1 ≤ b.1), x.1 = y := by
      have hperm := List.mergeSort_perm rows (fun a b : Group => a.1 ≤ b.1)
      constructor
      · intro ⟨r, hr, hry⟩
        have hrm : r ∈ rows.mergeSort (fun a b => a.1 ≤ b.1) := (List.Perm.mem_iff hperm).mpr hr
        exact ⟨r, hrm, hry⟩
      · intro ⟨x, hxm, hxy⟩
        have hxr : x ∈ rows := (List.Perm.mem_iff hperm).mp hxm
        exact ⟨x, hxr, hxy⟩
    exact h1.trans (h2.trans h3)
  have hLsort : (rows.mergeSort (fun a b => a.1 ≤ b.1)).Pairwise (fun a b => a.1 ≤ b.1) := by
    have hp := List.pairwise_mergeSort (le := fun a b : Group => a.1 ≤ b.1) yearLe_trans yearLe_total rows
    exact List.Pairwise.imp (fun {a b} h => of_decide_eq_true h) hp
  have hrec := groupBy_reconstruct hSsort hSnodup hmem hLsort
  have hyb := yearBuckets_eq rows
  have hfm : (rows.foldl yearStep {}).keys.mergeSort.flatMap (fun y => rows.filter (fun x => x.1 == y)) =
      (rows.foldl yearStep {}).keys.mergeSort.flatMap (fun y => (rows.mergeSort (fun a b => a.1 ≤ b.1)).filter (fun x => x.1 == y)) := by
    apply flatMap_congr_mem
    intro y _
    exact (mergeSort_filter_year rows y).symm
  rw [hyb, hfm]
  exact hrec.symm

end YearOnlyOrder

end TinylambOpt.ResearchProofs.AnalyticsWorkloads.Ceb
