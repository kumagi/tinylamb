import TinylambOpt.ResearchGoals.PackedGidDistinctSortDedup

/-!
Proved

- `Goal` — Zero-merge `COUNT(DISTINCT)` strategies (TPC-H Q16, ClickBench Q09/Q12/Q14):
  (1) sorting packed `(gid, val)` pairs and adjacent-deduping matches exact
  `COUNT(DISTINCT val)`, and (2) partitioning non-null values by `h v % W` makes
  worker local distinct counts disjoint so summing equals the global distinct count.

The reference model is defined in
`TinylambOpt.ResearchGoals.PackedGidDistinctSortDedup`.
-/

namespace TinylambOpt.ResearchProofs.PackedGidDistinctSortDedup

open TinylambOpt.ResearchGoals.PackedGidDistinctSortDedup

-- ---------------------------------------------------------------------------
-- Lexicographic order on packed pairs
-- ---------------------------------------------------------------------------

private theorem lePair_refl (a : Nat × Int) : lePair a a = true := by
  simp [lePair]

private theorem lePair_total (a b : Nat × Int) : lePair a b = true ∨ lePair b a = true := by
  by_cases hg : a.1 = b.1
  · by_cases h : a.2 ≤ b.2
    · exact Or.inl (by simpa [lePair, hg] using (show decide (a.2 ≤ b.2) = true from by simp [h]))
    · have h' : b.2 ≤ a.2 := by omega
      exact Or.inr (by simpa [lePair, hg] using (show decide (b.2 ≤ a.2) = true from by simp [h']))
  · by_cases h : a.1 ≤ b.1
    · exact Or.inl (by simpa [lePair, hg] using (show decide (a.1 ≤ b.1) = true from by simp [h]))
    · have h' : b.1 ≤ a.1 := by omega
      have hbg : b.1 ≠ a.1 := by intro hba; exact hg hba.symm
      exact Or.inr (by simpa [lePair, hbg] using (show decide (b.1 ≤ a.1) = true from by simp [h']))

private theorem lePair_trans {a b c : Nat × Int}
    (hab : lePair a b = true) (hbc : lePair b c = true) : lePair a c = true := by
  by_cases habg : a.1 = b.1
  · by_cases hbcg : b.1 = c.1
    · have h1 : a.2 ≤ b.2 := by simpa [lePair, habg] using hab
      have h2 : b.2 ≤ c.2 := by simpa [lePair, hbcg] using hbc
      have h3 : a.2 ≤ c.2 := by omega
      have hacg : a.1 = c.1 := habg.trans hbcg
      simpa [lePair, hacg] using h3
    · have h2 : b.1 ≤ c.1 := by simpa [lePair, hbcg] using hbc
      have h3 : a.1 ≤ c.1 := by omega
      have hacg : a.1 ≠ c.1 := by intro hac; exact hbcg (habg.symm.trans hac)
      simpa [lePair, hacg] using h3
  · have h1 : a.1 ≤ b.1 := by simpa [lePair, habg] using hab
    by_cases hbcg : b.1 = c.1
    · have h3 : a.1 ≤ c.1 := by omega
      have hacg : a.1 ≠ c.1 := by intro hac; exact habg (hac.trans hbcg.symm)
      simpa [lePair, hacg] using h3
    · have h2 : b.1 ≤ c.1 := by simpa [lePair, hbcg] using hbc
      have h3 : a.1 ≤ c.1 := by omega
      by_cases hacg : a.1 = c.1
      · exfalso
        exact habg (by omega)
      · simpa [lePair, hacg] using h3

private theorem lePair_antisym {a b : Nat × Int}
    (hab : lePair a b = true) (hba : lePair b a = true) : a = b := by
  by_cases habg : a.1 = b.1
  · have h1 : a.2 ≤ b.2 := by simpa [lePair, habg] using hab
    have h2 : b.2 ≤ a.2 := by simpa [lePair, habg] using hba
    exact Prod.ext habg (by omega)
  · have h1 : a.1 ≤ b.1 := by simpa [lePair, habg] using hab
    have hbg : b.1 ≠ a.1 := by intro hba'; exact habg hba'.symm
    have h2 : b.1 ≤ a.1 := by simpa [lePair, hbg] using hba
    exact False.elim (habg (by omega))

-- ---------------------------------------------------------------------------
-- A generic "first occurrence wins" duplicate elimination list
-- ---------------------------------------------------------------------------

private def uniqF {α : Type} [BEq α] (acc : List α) (v : α) : List α :=
  if acc.contains v then acc else acc ++ [v]

private def uniqList {α : Type} [BEq α] (l : List α) : List α :=
  l.foldl uniqF []

private theorem uniqList_go {α : Type} [BEq α] [LawfulBEq α] :
    ∀ (acc : List α) (l : List α), acc.Nodup →
      (l.foldl uniqF acc).Nodup ∧
      ∀ x, x ∈ (l.foldl uniqF acc) ↔ x ∈ acc ∨ x ∈ l := by
  intro acc l
  induction l generalizing acc with
  | nil =>
      intro hacc
      constructor
      · exact hacc
      · intro x
        simp
  | cons a l ih =>
      intro hacc
      change (l.foldl uniqF (uniqF acc a)).Nodup ∧
        (∀ x, x ∈ l.foldl uniqF (uniqF acc a) ↔ x ∈ acc ∨ x ∈ a :: l)
      by_cases hc : acc.contains a = true
      · have ha : a ∈ acc := List.contains_iff_mem.mp hc
        have hstep : uniqF acc a = acc := by
          rw [uniqF, hc]
          simp
        rw [hstep]
        have hr := ih acc hacc
        constructor
        · exact hr.1
        · intro x
          rw [hr.2]
          constructor
          · intro hx
            rcases hx with hx | hx
            · exact Or.inl hx
            · exact Or.inr (by simp [hx])
          · intro hx
            rcases hx with hx | hx
            · exact Or.inl hx
            · rcases List.mem_cons.mp hx with hxa | hxl
              · subst x
                exact Or.inl ha
              · exact Or.inr hxl
      · have ha : a ∉ acc := by
          intro ha
          exact hc (List.contains_iff_mem.mpr ha)
        have hcf : acc.contains a = false := by
          cases hca : acc.contains a <;> simp_all
        have hstep : uniqF acc a = acc ++ [a] := by
          rw [uniqF, hcf]
          simp
        rw [hstep]
        have hacc' : (acc ++ [a]).Nodup := by
          rw [List.nodup_append]
          refine ⟨hacc, by simp, ?_⟩
          intro x hx y hy
          simp only [List.mem_singleton] at hy
          subst y
          intro hxy
          exact ha (hxy ▸ hx)
        have hr := ih (acc ++ [a]) hacc'
        constructor
        · exact hr.1
        · intro x
          rw [hr.2]
          constructor
          · intro hx
            rcases hx with hx | hx
            · rcases List.mem_append.mp hx with hxacc | hxa
              · exact Or.inl hxacc
              · have hxa' : x = a := by simpa using hxa
                subst x
                exact Or.inr (by simp)
            · exact Or.inr (by simp [hx])
          · intro hx
            rcases hx with hx | hx
            · exact Or.inl (by simp [hx])
            · rcases List.mem_cons.mp hx with hxa | hxl
              · subst x
                exact Or.inl (by simp)
              · exact Or.inr hxl

private theorem uniqList_nodup {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    (uniqList l).Nodup := by
  exact (uniqList_go [] l (by simp)).1

private theorem mem_uniqList {α : Type} [BEq α] [LawfulBEq α] (l : List α) (x : α) :
    x ∈ uniqList l ↔ x ∈ l := by
  have h := (uniqList_go ([] : List α) l (by simp)).2 x
  simpa [uniqList] using h

private theorem uniqList_eq_foldl {α : Type} [BEq α] (l : List α) :
    uniqList l = l.foldl (fun acc v => if acc.contains v then acc else acc ++ [v]) [] := by
  rfl

-- ---------------------------------------------------------------------------
-- Equality of length from nodup + identical membership
-- ---------------------------------------------------------------------------

private theorem perm_cons_of_mem {α : Type} [DecidableEq α] {a : α} {l : List α}
    (h : a ∈ l) : ∃ t, l.Perm (a :: t) := by
  induction l with
  | nil => simp at h
  | cons b bs ih =>
      simp only [List.mem_cons] at h
      rcases h with h | h
      · subst h
        exact ⟨bs, List.Perm.refl _⟩
      · rcases ih h with ⟨t, ht⟩
        refine ⟨b :: t, ?_⟩
        exact (List.Perm.cons b ht).trans (List.Perm.swap a b t)

private theorem perm_of_nodup_mem_equiv {α : Type} [DecidableEq α] {l₁ l₂ : List α}
    (h₁ : l₁.Nodup) (h₂ : l₂.Nodup) (hmem : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁.Perm l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      have hnil : l₂ = [] := by
        rw [List.eq_nil_iff_forall_not_mem]
        intro x hx
        have : x ∈ ([] : List α) := (hmem x).mpr hx
        simpa using this
      simpa [hnil]
  | cons a t ih =>
      have ha_t : a ∉ t := (List.nodup_cons.mp h₁).1
      have ht : t.Nodup := (List.nodup_cons.mp h₁).2
      have ha₂ : a ∈ l₂ := (hmem a).mp (by simp)
      rcases perm_cons_of_mem ha₂ with ⟨u, hperm₂⟩
      have hau_nodup : (a :: u).Nodup := hperm₂.nodup h₂
      have ha_u : a ∉ u := (List.nodup_cons.mp hau_nodup).1
      have hu : u.Nodup := (List.nodup_cons.mp hau_nodup).2
      have htperm : t.Perm u := ih ht hu (by
        intro x
        constructor
        · intro hx
          have hx₁ : x ∈ a :: t := by simp [hx]
          have hx₂ : x ∈ l₂ := (hmem x).mp hx₁
          have hx₃ : x ∈ a :: u := (hperm₂.mem_iff).mp hx₂
          have hxa : x ≠ a := by
            intro h
            subst x
            exact ha_t hx
          simpa [hxa] using hx₃
        · intro hx
          have hx₂' : x ∈ a :: u := by simp [hx]
          have hx₁' : x ∈ l₂ := (hperm₂.mem_iff).mpr hx₂'
          have hx₁ : x ∈ a :: t := (hmem x).mpr hx₁'
          have hxa : x ≠ a := by
            intro h
            subst x
            exact ha_u hx
          simpa [hxa] using hx₁)
      exact (List.Perm.cons a htperm).trans hperm₂.symm

private theorem length_eq_of_nodup_mem_equiv {α : Type} [DecidableEq α] {l₁ l₂ : List α}
    (h₁ : l₁.Nodup) (h₂ : l₂.Nodup) (hmem : ∀ x, x ∈ l₁ ↔ x ∈ l₂) :
    l₁.length = l₂.length :=
  (perm_of_nodup_mem_equiv h₁ h₂ hmem).length_eq

-- ---------------------------------------------------------------------------
-- Insertion sort and adjacent dedup
-- ---------------------------------------------------------------------------

private def SortedPair (l : List (Nat × Int)) : Prop :=
  List.Pairwise (fun a b => lePair a b = true) l

private theorem mem_insertSortedPair {x z : Nat × Int} {l : List (Nat × Int)} :
    z ∈ insertSortedPair x l ↔ z = x ∨ z ∈ l := by
  induction l with
  | nil => simp [insertSortedPair]
  | cons y ys ih =>
      change z ∈ (if lePair x y then x :: y :: ys else y :: insertSortedPair x ys) ↔
        z = x ∨ z ∈ y :: ys
      by_cases h : lePair x y = true
      · rw [ite_eq_left h]
        simp only [List.mem_cons]
      · rw [ite_eq_right h]
        simp only [List.mem_cons]
        rw [ih]
        constructor
        · intro hz
          rcases hz with rfl | hz
          · exact Or.inr (Or.inl rfl)
          · rcases hz with rfl | hz
            · exact Or.inl rfl
            · exact Or.inr (Or.inr hz)
        · intro hz
          rcases hz with rfl | hz
          · exact Or.inr (Or.inl rfl)
          · rcases hz with rfl | hz
            · exact Or.inl rfl
            · exact Or.inr (Or.inr hz)

private theorem insertSortedPair_perm {x : Nat × Int} {l : List (Nat × Int)} :
    (insertSortedPair x l).Perm (x :: l) := by
  induction l with
  | nil => simp [insertSortedPair]
  | cons y ys ih =>
      change (if lePair x y then x :: y :: ys else y :: insertSortedPair x ys).Perm (x :: y :: ys)
      by_cases h : lePair x y = true
      · rw [ite_eq_left h]
      · rw [ite_eq_right h]
        exact (List.Perm.cons y ih).trans (List.Perm.swap x y ys)

private theorem sortPairs_perm (l : List (Nat × Int)) : (sortPairs l).Perm l := by
  induction l with
  | nil => simp [sortPairs]
  | cons x xs ih =>
      exact (insertSortedPair_perm (x := x) (l := sortPairs xs)).trans (List.Perm.cons x ih)

private theorem mem_sortPairs {x : Nat × Int} {l : List (Nat × Int)} :
    x ∈ sortPairs l ↔ x ∈ l :=
  (sortPairs_perm l).mem_iff

private theorem insertSortedPair_sorted {x : Nat × Int} {l : List (Nat × Int)}
    (h : SortedPair l) : SortedPair (insertSortedPair x l) := by
  induction l with
  | nil => simp [insertSortedPair, SortedPair]
  | cons y ys ih =>
      have hy_all : ∀ z ∈ ys, lePair y z = true := (List.pairwise_cons.mp h).1
      have hys : SortedPair ys := (List.pairwise_cons.mp h).2
      change SortedPair (if lePair x y then x :: y :: ys else y :: insertSortedPair x ys)
      by_cases hxy : lePair x y = true
      · rw [ite_eq_left hxy]
        change List.Pairwise (fun a b : Nat × Int => lePair a b = true) (x :: y :: ys)
        rw [List.pairwise_cons]
        constructor
        · intro z hz
          rcases List.mem_cons.mp hz with rfl | hz
          · exact hxy
          · exact lePair_trans hxy (hy_all z hz)
        · exact h
      · rw [ite_eq_right hxy]
        change List.Pairwise (fun a b : Nat × Int => lePair a b = true) (y :: insertSortedPair x ys)
        have hyx : lePair y x = true := by
          rcases lePair_total x y with h1 | h1
          · exact False.elim (hxy h1)
          · exact h1
        have hres : SortedPair (insertSortedPair x ys) := ih hys
        rw [List.pairwise_cons]
        constructor
        · intro z hz
          rcases (mem_insertSortedPair.mp hz) with rfl | hz
          · exact hyx
          · exact hy_all z hz
        · exact hres

private theorem sortPairs_sorted (l : List (Nat × Int)) : SortedPair (sortPairs l) := by
  induction l with
  | nil => simp [sortPairs, SortedPair]
  | cons x xs ih => exact insertSortedPair_sorted ih

private theorem mem_dedupAdjacent {x : Nat × Int} {l : List (Nat × Int)} :
    x ∈ dedupAdjacent l ↔ x ∈ l := by
  induction l with
  | nil => simp [dedupAdjacent]
  | cons a t ih =>
      cases t with
      | nil => simp [dedupAdjacent]
      | cons b bs =>
          change x ∈ (if a == b then dedupAdjacent (b :: bs) else a :: dedupAdjacent (b :: bs)) ↔
            x ∈ a :: b :: bs
          by_cases h : (a == b) = true
          · rw [ite_eq_left h]
            have hab : a = b := beq_iff_eq.mp h
            subst b
            simpa [dedupAdjacent] using ih
          · rw [ite_eq_right h]
            simp only [List.mem_cons]
            rw [ih]
            simp only [List.mem_cons]

private theorem nodup_dedupAdjacent {l : List (Nat × Int)} (h : SortedPair l) :
    (dedupAdjacent l).Nodup := by
  induction l with
  | nil => simp [dedupAdjacent]
  | cons a t ih =>
      cases t with
      | nil => simp [dedupAdjacent]
      | cons b bs =>
          have hab_le : lePair a b = true := (List.pairwise_cons.mp h).1 b (by simp)
          have ht_sorted : SortedPair (b :: bs) := (List.pairwise_cons.mp h).2
          change (if a == b then dedupAdjacent (b :: bs) else a :: dedupAdjacent (b :: bs)).Nodup
          by_cases heq : (a == b) = true
          · rw [ite_eq_left heq]
            have hab : a = b := beq_iff_eq.mp heq
            subst b
            simpa [dedupAdjacent] using ih ht_sorted
          · rw [ite_eq_right heq]
            have hnot : a ∉ dedupAdjacent (b :: bs) := by
              intro ha
              have ha_in : a ∈ b :: bs := (mem_dedupAdjacent.mp ha)
              have hba : lePair b a = true := by
                rcases List.mem_cons.mp ha_in with hab' | hb
                · rw [hab']
                  exact lePair_refl b
                · exact (List.pairwise_cons.mp ht_sorted).1 a hb
              have heq' : a = b := lePair_antisym hab_le hba
              exact heq (by simp [heq'])
            exact List.nodup_cons.mpr ⟨hnot, ih ht_sorted⟩

-- ---------------------------------------------------------------------------
-- Per-gid distinct count via sorted packed pairs
-- ---------------------------------------------------------------------------

private def valsFor (l : List (Nat × Int)) (gid : Nat) : List Int :=
  l.filterMap (fun q => if q.1 == gid then some q.2 else none)

private theorem nodup_filter {α : Type} (p : α → Bool) {l : List α} (h : l.Nodup) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons a t ih =>
      have ha : a ∉ t := (List.nodup_cons.mp h).1
      have ht : t.Nodup := (List.nodup_cons.mp h).2
      by_cases hp : p a = true
      · rw [List.filter_cons_of_pos hp]
        exact List.nodup_cons.mpr ⟨by
          intro hat
          exact ha ((List.mem_filter.mp hat).1), ih ht⟩
      · rw [List.filter_cons_of_neg hp]
        exact ih ht

private theorem nodup_map_of_inj {α β : Type} [DecidableEq β] (f : α → β) {l : List α}
    (h : l.Nodup) (hinj : ∀ x ∈ l, ∀ y ∈ l, f x = f y → x = y) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a t ih =>
      have ha : a ∉ t := (List.nodup_cons.mp h).1
      have ht : t.Nodup := (List.nodup_cons.mp h).2
      have hinj_t : ∀ x ∈ t, ∀ y ∈ t, f x = f y → x = y := by
        intro x hx y hy hxy
        exact hinj x (by simp [hx]) y (by simp [hy]) hxy
      have hnot : f a ∉ t.map f := by
        intro hf
        rcases List.mem_map.mp hf with ⟨y, hy, hfy⟩
        have hay : a = y := hinj a (by simp) y (by simp [hy]) hfy.symm
        exact ha (hay ▸ hy)
      simpa [List.nodup_cons] using And.intro hnot (ih ht hinj_t)

private theorem nonNullPairs_of_pairs (sh : List (Nat × Int)) :
    nonNullPairs (sh.map (fun q => (q.1, some q.2))) = sh := by
  simp [nonNullPairs, List.filterMap_map, Function.comp_def]

private theorem filterMap_filter_gid (pairs : List (Nat × Int)) (gid : Nat) (pred : Int → Bool) :
    (pairs.filter (fun q => pred q.2)).filterMap (fun q => if q.1 == gid then some q.2 else none) =
      (pairs.filterMap (fun q => if q.1 == gid then some q.2 else none)).filter pred := by
  induction pairs with
  | nil => simp
  | cons q qs ih =>
      by_cases hp : pred q.2 = true
      · by_cases hg : (q.1 == gid) = true
        · have hgq : (if q.1 == gid then some q.2 else none) = some q.2 := by rw [ite_eq_left hg]
          simp only [List.filterMap_cons, List.filter_cons, hp, hgq, ih, ite_true, ite_false,
            Bool.false_eq_true]
        · have hgq : (if q.1 == gid then some q.2 else none) = none := by rw [ite_eq_right hg]
          simp only [List.filterMap_cons, List.filter_cons, hp, hgq, ih, ite_true, ite_false,
            Bool.false_eq_true]
      · by_cases hg : (q.1 == gid) = true
        · have hgq : (if q.1 == gid then some q.2 else none) = some q.2 := by rw [ite_eq_left hg]
          simp only [List.filterMap_cons, List.filter_cons, hp, hgq, ih, ite_true, ite_false,
            Bool.false_eq_true]
        · have hgq : (if q.1 == gid then some q.2 else none) = none := by rw [ite_eq_right hg]
          simp only [List.filterMap_cons, List.filter_cons, hp, hgq, ih, ite_true, ite_false,
            Bool.false_eq_true]

private theorem sortedDedup_gid_count (l : List (Nat × Int)) (gid : Nat) :
    ((dedupAdjacent (sortPairs l)).filter (fun p => p.1 == gid)).length =
      (uniqList (valsFor l gid)).length := by
  let u := dedupAdjacent (sortPairs l)
  let f := u.filter (fun p : Nat × Int => p.1 == gid)
  let m := f.map Prod.snd
  have hu_nodup : u.Nodup := nodup_dedupAdjacent (sortPairs_sorted l)
  have hu_mem : ∀ p : Nat × Int, p ∈ u ↔ p ∈ l := by
    intro p
    exact (mem_dedupAdjacent (x := p) (l := sortPairs l)).trans mem_sortPairs
  have hf_nodup : f.Nodup := nodup_filter (fun p : Nat × Int => p.1 == gid) hu_nodup
  have hinj : ∀ x ∈ f, ∀ y ∈ f, Prod.snd x = Prod.snd y → x = y := by
    intro x hx y hy hsnd
    have hxg : (x.1 == gid) = true := (List.mem_filter.mp hx).2
    have hyg : (y.1 == gid) = true := (List.mem_filter.mp hy).2
    have hx1 : x.1 = gid := beq_iff_eq.mp hxg
    have hy1 : y.1 = gid := beq_iff_eq.mp hyg
    cases x with
    | mk xg xv =>
      cases y with
      | mk yg yv =>
        simp only [Prod.mk.injEq] at hx1 hy1 hsnd ⊢
        exact ⟨hx1.trans hy1.symm, hsnd⟩
  have hm_nodup : m.Nodup := nodup_map_of_inj Prod.snd hf_nodup hinj
  have hmem : ∀ v : Int, v ∈ m ↔ v ∈ uniqList (valsFor l gid) := by
    intro v
    rw [mem_uniqList]
    constructor
    · intro hv
      rcases List.mem_map.mp hv with ⟨p, hp_f, hp_snd⟩
      have hp_u : p ∈ u := (List.mem_filter.mp hp_f).1
      have hp_gid : (p.1 == gid) = true := (List.mem_filter.mp hp_f).2
      have hp_l : p ∈ l := (hu_mem p).mp hp_u
      rw [valsFor, List.mem_filterMap]
      refine ⟨p, hp_l, ?_⟩
      cases p with
      | mk g w =>
        simp [valsFor, hp_gid] at hp_snd ⊢
        subst w
        rfl
    · intro hv
      rw [valsFor, List.mem_filterMap] at hv
      rcases hv with ⟨a, ha_l, ha_f⟩
      cases a with
      | mk g w =>
        by_cases hgg : (g == gid) = true
        · simp [valsFor, hgg] at ha_f
          subst w
          apply List.mem_map.mpr
          refine ⟨(gid, v), ?_, rfl⟩
          apply List.mem_filter.mpr
          refine ⟨(hu_mem (gid, v)).mpr ?_, by simp⟩
          have hg_eq : g = gid := beq_iff_eq.mp hgg
          simpa [hg_eq] using ha_l
        · simp [valsFor, hgg] at ha_f
  have hlen := length_eq_of_nodup_mem_equiv hm_nodup (uniqList_nodup (valsFor l gid)) hmem
  simpa [m, f, u] using hlen

-- ---------------------------------------------------------------------------
-- Hash-partition sum
-- ---------------------------------------------------------------------------

private theorem nodup_flatten_indexed {α : Type} [DecidableEq α] (idx : List Nat) (F : Nat → List α)
    (hidx : idx.Nodup)
    (hnd : ∀ s ∈ idx, (F s).Nodup)
    (hdisj : ∀ s ∈ idx, ∀ t ∈ idx, s ≠ t → ∀ x ∈ F s, ∀ y ∈ F t, x ≠ y) :
    (idx.map F).flatten.Nodup := by
  induction idx with
  | nil => simp
  | cons s ss ih =>
      have hs_not : s ∉ ss := (List.nodup_cons.mp hidx).1
      have hss : ss.Nodup := (List.nodup_cons.mp hidx).2
      rw [List.map_cons, List.flatten_cons, List.nodup_append]
      refine ⟨hnd s (by simp), ?_, ?_⟩
      · exact ih hss
          (by intro t ht; exact hnd t (by simp [ht]))
          (by intro t ht u hu htu; exact hdisj t (by simp [ht]) u (by simp [hu]) htu)
      · intro x hx y hy
        rcases List.mem_flatten.mp hy with ⟨l, hl, hyl⟩
        rcases List.mem_map.mp hl with ⟨t, ht, hlt⟩
        subst l
        have hst : s ≠ t := by
          intro hst
          subst t
          exact hs_not ht
        exact hdisj s (by simp) t (by simp [ht]) hst x hx y hyl

private theorem nodup_flatten_shards (w : Nat) (h : Int → Nat) (V : List Int) :
    (((List.range w).map fun s => uniqList (V.filter (fun v => h v % w == s))).flatten).Nodup := by
  let F := fun s => uniqList (V.filter (fun v => h v % w == s))
  apply nodup_flatten_indexed (List.range w) F List.nodup_range
  · intro s hs
    exact uniqList_nodup _
  · intro s hs t ht hst x hx y hy
    have hxs : x ∈ V.filter (fun v => h v % w == s) := (mem_uniqList _ x).mp hx
    have hys : y ∈ V.filter (fun v => h v % w == t) := (mem_uniqList _ y).mp hy
    have hxmod : (h x % w == s) = true := (List.mem_filter.mp hxs).2
    have hymod : (h y % w == t) = true := (List.mem_filter.mp hys).2
    intro hxy
    subst y
    have hst' : s = t := by
      have hs_eq : h x % w = s := beq_iff_eq.mp hxmod
      have ht_eq : h x % w = t := beq_iff_eq.mp hymod
      exact hs_eq.symm.trans ht_eq
    exact hst hst'

private theorem sum_shard_lengths (w : Nat) (hw : 0 < w) (h : Int → Nat) (V : List Int) :
    ((List.range w).map fun s => (uniqList (V.filter (fun v => h v % w == s))).length).sum =
      (uniqList V).length := by
  let F := (List.range w).map fun s => uniqList (V.filter (fun v => h v % w == s))
  have hF_nodup : F.flatten.Nodup := by
    simpa [F] using nodup_flatten_shards w h V
  have hF_mem : ∀ v : Int, v ∈ F.flatten ↔ v ∈ uniqList V := by
    intro v
    rw [mem_uniqList]
    constructor
    · intro hv
      rcases List.mem_flatten.mp hv with ⟨l, hlF, hvl⟩
      rcases List.mem_map.mp hlF with ⟨s, hsRange, hsl⟩
      subst l
      have hvshard : v ∈ V.filter (fun y => h y % w == s) := (mem_uniqList _ v).mp hvl
      exact (List.mem_filter.mp hvshard).1
    · intro hv
      have hmodlt : h v % w < w := Nat.mod_lt (h v) hw
      apply List.mem_flatten.mpr
      refine ⟨uniqList (V.filter (fun y => h y % w == h v % w)), ?_, (mem_uniqList _ v).mpr ?_⟩
      · apply List.mem_map.mpr
        exact ⟨h v % w, List.mem_range.mpr hmodlt, rfl⟩
      · exact List.mem_filter.mpr ⟨hv, by simp⟩
  have hp := perm_of_nodup_mem_equiv hF_nodup (uniqList_nodup V) hF_mem
  have hlen := hp.length_eq
  simpa [F, List.length_flatten, Function.comp_def] using hlen

-- ---------------------------------------------------------------------------
-- Soundness
-- ---------------------------------------------------------------------------

private theorem sortDedup_sound (rows : List Row) (gid : Nat) :
    sortDedupCount rows gid = refDistinctCount rows gid := by
  have h := sortedDedup_gid_count (nonNullPairs rows) gid
  rw [uniqList_eq_foldl] at h
  simpa [sortDedupCount, refDistinctCount, valsFor] using h

private theorem hashPartition_sound (w : Nat) (h : Int → Nat) (rows : List Row) (gid : Nat)
    (hw : 0 < w) : hashPartitionCount w h rows gid = refDistinctCount rows gid := by
  let pairs := nonNullPairs rows
  let V := valsFor pairs gid
  have hshard : ∀ s ∈ List.range w,
      refDistinctCount ((pairs.filter (fun q => h q.2 % w == s)).map (fun q => (q.1, some q.2))) gid =
        (uniqList (V.filter (fun v => h v % w == s))).length := by
    intro s hs
    unfold refDistinctCount
    rw [nonNullPairs_of_pairs]
    have hfilt :
        (pairs.filter (fun q : Nat × Int => h q.2 % w == s)).filterMap
            (fun q => if q.1 == gid then some q.2 else none) =
          V.filter (fun v => h v % w == s) := by
      exact filterMap_filter_gid pairs gid (fun v => h v % w == s)
    rw [hfilt]
    rfl
  have hmap :
      (List.range w).map (fun s =>
        refDistinctCount ((pairs.filter (fun q => h q.2 % w == s)).map (fun q => (q.1, some q.2))) gid) =
      (List.range w).map (fun s => (uniqList (V.filter (fun v => h v % w == s))).length) := by
    apply List.map_congr_left
    intro s hs
    exact hshard s hs
  have hsum := sum_shard_lengths w hw h V
  have hpc : hashPartitionCount w h rows gid =
      ((List.range w).map (fun s =>
        refDistinctCount ((pairs.filter (fun q => h q.2 % w == s)).map (fun q => (q.1, some q.2))) gid)).sum := by
    simp [hashPartitionCount, pairs, Function.comp_def]
  have href : refDistinctCount rows gid = (uniqList V).length := by
    rw [uniqList_eq_foldl]
    simp [refDistinctCount, V, valsFor, pairs]
  rw [hpc, hmap, href]
  exact hsum

theorem sound : TinylambOpt.ResearchGoals.PackedGidDistinctSortDedup.Goal := by
  intro w h rows gid hw
  exact ⟨sortDedup_sound rows gid, hashPartition_sound w h rows gid hw⟩

end TinylambOpt.ResearchProofs.PackedGidDistinctSortDedup
