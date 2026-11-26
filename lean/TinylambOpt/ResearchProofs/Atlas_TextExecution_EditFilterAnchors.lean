import TinylambOpt.ResearchGoals.Atlas.TextExecution

/-!
SQL string predicates can use encoded or summarized text when the modeled match,
boundaries, and collation are preserved.

Proved

- `EditFilter.PigeonholeAnchor` — `∀ (ps : List Word) t k, k < ps.length → (∀ p ∈ ps, p
  ≠ []) → Edits k ps.flatten t → ∃ p ∈ ps, occurs t p = true`
- `EditFilter.PositionalAnchor` — `∀ (ps : List Word) t k, k < ps.length → (∀ p ∈ ps, p
  ≠ []) → Edits k ps.flatten t → ∃ i < ps.length, ∃ j ∈ positions t (ps[i]?.getD []),
  ((j:Int) - ((ps.take i).map List.length).sum).natAbs ≤ k`
-/

namespace TinylambOpt.ResearchProofs.Atlas.TextExecution.EditFilterAnchors

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.EditFilter

set_option maxHeartbeats 0
set_option maxRecDepth 10000

private def startOf (ps : List Word) (i : Nat) : Nat :=
  ((ps.take i).map List.length).sum

private def partAt (ps : List Word) (i : Nat) : Word :=
  ps[i]?.getD []

private def EntryOK (ps : List Word) (K : Nat) (s : Word) (e : Nat × Nat) : Prop :=
  e.1 < ps.length ∧ partAt ps e.1 ≠ [] ∧
    (partAt ps e.1).isPrefixOf (s.drop e.2) = true ∧
    (((e.2 : Int) - (startOf ps e.1 : Int)).natAbs ≤ K)

private def ChainOK (ps : List Word) (K : Nat) (s : Word) (es : List (Nat × Nat)) : Prop :=
  es.Pairwise (fun e₁ e₂ => e₁.1 < e₂.1 ∧ e₁.2 + (partAt ps e₁.1).length ≤ e₂.2) ∧
    ∀ e ∈ es, EntryOK ps K s e

private theorem pairwise_map_of_mem {α β : Type} {R : α → α → Prop} {S : β → β → Prop}
    (f : α → β) (l : List α)
    (H : ∀ a ∈ l, ∀ b ∈ l, R a b → S (f a) (f b))
    (hp : l.Pairwise R) : (l.map f).Pairwise S := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons]
      rw [List.pairwise_cons] at hp ⊢
      constructor
      · intro b' hb'
        rcases List.mem_map.mp hb' with ⟨b, hb, rfl⟩
        exact H a (by simp) b (by simp [hb]) (hp.1 b hb)
      · exact ih (fun b hb c hc hbc => H b (by simp [hb]) c (by simp [hc]) hbc) hp.2

private theorem countP_compl {α : Type} (p : α → Bool) (l : List α) :
    l.countP p + l.countP (fun x => !(p x)) = l.length := by
  induction l with
  | nil => simp
  | cons a l ih => by_cases h : p a = true <;> simp [h] <;> omega

private theorem filter_length_ge_sub_one {α : Type} (p : α → Bool) (l : List α)
    (h : l.countP (fun x => !(p x)) ≤ 1) : l.length - 1 ≤ (l.filter p).length := by
  have hc := countP_compl p l
  rw [List.countP_eq_length_filter] at h
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter] at hc
  omega

private theorem occurs_of_prefix {s p : Word} (hp : p ≠ []) {j : Nat}
    (h : p.isPrefixOf (s.drop j) = true) : occurs s p = true := by
  unfold occurs positions
  have hpref : ∃ u, p ++ u = s.drop j := (List.isPrefixOf_iff_prefix).mp h
  have hjle : j ≤ s.length := by
    rcases hpref with ⟨u, hu⟩
    have hlen := congrArg List.length hu
    rw [List.length_drop, List.length_append] at hlen
    have hppos : 0 < p.length := List.length_pos_iff.mpr hp
    omega
  have hmem : j ∈ (List.range (s.length + 1)).filter (fun i => p.isPrefixOf (s.drop i)) := by
    rw [List.mem_filter]
    exact ⟨by simp [List.mem_range]; omega, h⟩
  have hne : (List.range (s.length + 1)).filter (fun i => p.isPrefixOf (s.drop i)) ≠ [] := by
    intro he
    rw [he] at hmem
    cases hmem
  simp [hne]

private theorem isPrefix_append_of_length_le {p u A B : Word}
    (h : p ++ u = A ++ B) (hlen : p.length ≤ A.length) : p.isPrefixOf A = true := by
  rw [List.isPrefixOf_iff_prefix]
  have ht := congrArg (List.take p.length) h
  rw [List.take_left, List.take_append_of_le_length hlen] at ht
  refine ⟨A.drop p.length, ?_⟩
  calc
    p ++ A.drop p.length = (A.take p.length) ++ A.drop p.length :=
      congrArg (fun x => x ++ A.drop p.length) ht
    _ = A := List.take_append_drop p.length A

private theorem drop_drop_cons_eq (l r : Word) (x : Nat) {j : Nat} (h : l.length < j) :
    (l ++ x :: r).drop j = (l ++ r).drop (j - 1) := by
  rw [List.drop_append, List.drop_append]
  have hjl : l.drop j = [] := by apply List.drop_eq_nil_of_le; omega
  have hj1l : l.drop (j - 1) = [] := by apply List.drop_eq_nil_of_le; omega
  rw [hjl, hj1l]
  simp only [List.nil_append]
  have hjpos : 0 < j - l.length := by omega
  rw [List.drop_cons hjpos]
  have hj1 : (j - 1) - l.length = j - l.length - 1 := by omega
  rw [hj1]

private theorem drop_drop_add_eq (l r : Word) (x : Nat) {j : Nat} (h : l.length ≤ j) :
    (l ++ r).drop j = (l ++ x :: r).drop (j + 1) := by
  rw [List.drop_append, List.drop_append]
  have hjl : l.drop j = [] := by apply List.drop_eq_nil_of_le; omega
  have hj1l : l.drop (j + 1) = [] := by apply List.drop_eq_nil_of_le; omega
  rw [hjl, hj1l]
  simp only [List.nil_append]
  have hjpos : 0 < j + 1 - l.length := by omega
  rw [List.drop_cons hjpos]
  have hj1 : j + 1 - l.length - 1 = j - l.length := by omega
  rw [hj1]

private theorem drop_drop_subst_eq (l r : Word) (x y : Nat) {j : Nat} (h : l.length < j) :
    (l ++ x :: r).drop j = (l ++ y :: r).drop j := by
  rw [List.drop_append, List.drop_append]
  have hjl : l.drop j = [] := by apply List.drop_eq_nil_of_le; omega
  rw [hjl]
  simp only [List.nil_append]
  have hjpos : 0 < j - l.length := by omega
  rw [List.drop_cons hjpos, List.drop_cons hjpos]

private theorem startOf_succ (ps : List Word) (i : Nat) (h : i < ps.length) :
    startOf ps (i + 1) = startOf ps i + (partAt ps i).length := by
  induction ps generalizing i with
  | nil => simp at h
  | cons p ps ih =>
      cases i with
      | zero => simp [startOf, partAt]
      | succ i =>
          have hi : i < ps.length := by simpa using h
          have ih' := ih i hi
          simp [startOf, partAt] at ih' ⊢
          omega

private theorem startOf_succ_le (ps : List Word) (j : Nat) : startOf ps j ≤ startOf ps (j + 1) := by
  by_cases hj : j < ps.length
  · rw [startOf_succ ps j hj]
    omega
  · unfold startOf
    rw [List.take_of_length_le (by omega : ps.length ≤ j)]
    rw [List.take_of_length_le (by omega : ps.length ≤ j + 1)]
    exact Nat.le_refl _

private theorem startOf_mono (ps : List Word) {i j : Nat} (h : i ≤ j) :
    startOf ps i ≤ startOf ps j := by
  induction j generalizing i with
  | zero =>
      have : i = 0 := by omega
      subst i
      exact Nat.le_refl _
  | succ j ih =>
      by_cases hij : i ≤ j
      · exact Nat.le_trans (ih hij) (startOf_succ_le ps j)
      · have : i = j + 1 := by omega
        subst i
        exact Nat.le_refl _

private theorem flatten_drop_start : ∀ (ps : List Word) (i : Nat), i < ps.length →
    (ps.flatten).drop (startOf ps i) = partAt ps i ++ (ps.drop (i + 1)).flatten
  | [], i, h => by simp at h
  | p :: ps, 0, h => by simp [startOf, partAt]
  | p :: ps, i + 1, h => by
      have hi : i < ps.length := by simpa using h
      have hs : startOf (p :: ps) (i + 1) = p.length + startOf ps i := by
        unfold startOf
        rw [List.take_succ_cons]
        simp
      rw [hs]
      rw [show (p :: ps).flatten = p ++ ps.flatten by rfl]
      rw [List.drop_append]
      have hp_le : p.length ≤ p.length + startOf ps i := by omega
      rw [List.drop_eq_nil_of_le hp_le]
      simp only [List.nil_append]
      have hsub : p.length + startOf ps i - p.length = startOf ps i := by omega
      rw [hsub]
      rw [flatten_drop_start ps i hi]
      simp [partAt]

private def initEs (ps : List Word) : List (Nat × Nat) :=
  (List.range ps.length).map (fun i => (i, startOf ps i))

private theorem init_chain (ps : List Word) (hne : ∀ p ∈ ps, p ≠ []) :
    ChainOK ps 0 ps.flatten (initEs ps) := by
  constructor
  · unfold initEs
    apply pairwise_map_of_mem
    · intro i hi i' hi' hii'
      have hi_lt : i < ps.length := by simpa [List.mem_range] using hi
      have hs := startOf_succ ps i hi_lt
      have hle : startOf ps (i + 1) ≤ startOf ps i' := startOf_mono ps hii'
      have hsle : startOf ps i + (partAt ps i).length ≤ startOf ps i' := by omega
      exact ⟨hii', by simpa using hsle⟩
    · exact List.pairwise_lt_range
  · intro e he
    unfold initEs at he
    rw [List.mem_map] at he
    rcases he with ⟨i, hi, rfl⟩
    have hi_lt : i < ps.length := by simpa [List.mem_range] using hi
    have hpart : partAt ps i = ps[i] := by simp [partAt, List.getElem?_eq_getElem hi_lt]
    refine ⟨hi_lt, ?_, ?_, ?_⟩
    · rw [hpart]
      exact hne (ps[i]) (List.getElem_mem hi_lt)
    · rw [flatten_drop_start ps i hi_lt]
      apply (List.isPrefixOf_iff_prefix).mpr
      exact ⟨(ps.drop (i + 1)).flatten, by rw [hpart]⟩
    · simp

private theorem destroyed_drop_le_one (ps : List Word) (q : Nat) (es : List (Nat × Nat))
    (hp : es.Pairwise (fun e₁ e₂ => e₁.1 < e₂.1 ∧ e₁.2 + (partAt ps e₁.1).length ≤ e₂.2))
    (hn : ∀ e ∈ es, partAt ps e.1 ≠ []) :
    es.countP (fun e => decide (e.2 ≤ q ∧ q < e.2 + (partAt ps e.1).length)) ≤ 1 := by
  induction es with
  | nil => simp
  | cons e es ih =>
      rw [List.pairwise_cons] at hp
      simp only [List.countP_cons]
      by_cases hdead : e.2 ≤ q ∧ q < e.2 + (partAt ps e.1).length
      · have hnone : es.countP (fun x => decide (x.2 ≤ q ∧ q < x.2 + (partAt ps x.1).length)) = 0 := by
          apply List.countP_eq_zero.mpr
          intro x hx
          have hle := hp.1 x hx
          have hne := hn e (by simp)
          have hpos : 0 < (partAt ps e.1).length := List.length_pos_iff.mpr hne
          have hxnot : ¬ (x.2 ≤ q ∧ q < x.2 + (partAt ps x.1).length) := by omega
          intro ht
          exact hxnot (of_decide_eq_true ht)
        have hdt : decide (e.2 ≤ q ∧ q < e.2 + (partAt ps e.1).length) = true := by simp [hdead]
        rw [hdt, hnone]
        simp
      · have hdf : decide (e.2 ≤ q ∧ q < e.2 + (partAt ps e.1).length) = false := by simp [hdead]
        simpa [hdf] using ih hp.2 (fun x hx => hn x (by simp [hx]))

private theorem destroyed_subst_le_one (ps : List Word) (q : Nat) (es : List (Nat × Nat))
    (hp : es.Pairwise (fun e₁ e₂ => e₁.1 < e₂.1 ∧ e₁.2 + (partAt ps e₁.1).length ≤ e₂.2))
    (hn : ∀ e ∈ es, partAt ps e.1 ≠ []) :
    es.countP (fun e => decide (e.2 ≤ q ∧ q < e.2 + (partAt ps e.1).length)) ≤ 1 :=
  destroyed_drop_le_one ps q es hp hn

private theorem destroyed_add_le_one (ps : List Word) (q : Nat) (es : List (Nat × Nat))
    (hp : es.Pairwise (fun e₁ e₂ => e₁.1 < e₂.1 ∧ e₁.2 + (partAt ps e₁.1).length ≤ e₂.2))
    (hn : ∀ e ∈ es, partAt ps e.1 ≠ []) :
    es.countP (fun e => decide (e.2 < q ∧ q < e.2 + (partAt ps e.1).length)) ≤ 1 := by
  induction es with
  | nil => simp
  | cons e es ih =>
      rw [List.pairwise_cons] at hp
      simp only [List.countP_cons]
      by_cases hdead : e.2 < q ∧ q < e.2 + (partAt ps e.1).length
      · have hnone : es.countP (fun x => decide (x.2 < q ∧ q < x.2 + (partAt ps x.1).length)) = 0 := by
          apply List.countP_eq_zero.mpr
          intro x hx
          have hle := hp.1 x hx
          have hne := hn e (by simp)
          have hpos : 0 < (partAt ps e.1).length := List.length_pos_iff.mpr hne
          have hxnot : ¬ (x.2 < q ∧ q < x.2 + (partAt ps x.1).length) := by omega
          intro ht
          exact hxnot (of_decide_eq_true ht)
        have hdt : decide (e.2 < q ∧ q < e.2 + (partAt ps e.1).length) = true := by simp [hdead]
        rw [hdt, hnone]
        simp
      · have hdf : decide (e.2 < q ∧ q < e.2 + (partAt ps e.1).length) = false := by simp [hdead]
        simpa [hdf] using ih hp.2 (fun x hx => hn x (by simp [hx]))

private theorem chain_drop_step (ps : List Word) (l r : Word) (x : Nat) {k : Nat}
    {es : List (Nat × Nat)} (hes : ChainOK ps k (l ++ x :: r) es) :
    ∃ es', ChainOK ps (k + 1) (l ++ r) es' ∧ es.length - 1 ≤ es'.length := by
  let q := l.length
  let pkeep := fun e : Nat × Nat => decide (¬ (e.2 ≤ q ∧ q < e.2 + (partAt ps e.1).length))
  let f := fun e : Nat × Nat => (e.1, if q < e.2 then e.2 - 1 else e.2)
  let es' := (es.filter pkeep).map f
  have hpair := hes.1
  have hmem := hes.2
  have hpair_kept : (es.filter pkeep).Pairwise
      (fun e₁ e₂ => e₁.1 < e₂.1 ∧ e₁.2 + (partAt ps e₁.1).length ≤ e₂.2) :=
    hpair.filter pkeep
  have hH : ∀ a ∈ es.filter pkeep, ∀ b ∈ es.filter pkeep,
      (fun e₁ e₂ : Nat × Nat => e₁.1 < e₂.1 ∧ e₁.2 + (partAt ps e₁.1).length ≤ e₂.2) a b →
      (fun e₁ e₂ : Nat × Nat => e₁.1 < e₂.1 ∧ e₁.2 + (partAt ps e₁.1).length ≤ e₂.2) (f a) (f b) := by
    intro a ha b hb hab
    have hka : pkeep a = true := (List.mem_filter.mp ha).2
    have hkb : pkeep b = true := (List.mem_filter.mp hb).2
    have hna : ¬ (a.2 ≤ q ∧ q < a.2 + (partAt ps a.1).length) := of_decide_eq_true hka
    have hnb : ¬ (b.2 ≤ q ∧ q < b.2 + (partAt ps b.1).length) := of_decide_eq_true hkb
    have hne_a := (hmem a (List.mem_filter.mp ha).1).2.1
    have hpos_a : 0 < (partAt ps a.1).length := List.length_pos_iff.mpr hne_a
    refine ⟨hab.1, ?_⟩
    by_cases hqa : q < a.2
    · have hqb : q < b.2 := by omega
      simp [f, hqa, hqb]
      omega
    · have hale : a.2 + (partAt ps a.1).length ≤ q := by omega
      by_cases hqb : q < b.2
      · simp [f, hqa, hqb]
        omega
      · simp [f, hqa, hqb]
        exact hab.2
  have hpair' : es'.Pairwise
      (fun e₁ e₂ => e₁.1 < e₂.1 ∧ e₁.2 + (partAt ps e₁.1).length ≤ e₂.2) :=
    pairwise_map_of_mem f (es.filter pkeep) hH hpair_kept
  have hmem' : ∀ e ∈ es', EntryOK ps (k + 1) (l ++ r) e := by
    intro e he
    rw [List.mem_map] at he
    rcases he with ⟨a, ha, rfl⟩
    have hka : pkeep a = true := (List.mem_filter.mp ha).2
    have haes : a ∈ es := (List.mem_filter.mp ha).1
    have hna : ¬ (a.2 ≤ q ∧ q < a.2 + (partAt ps a.1).length) := of_decide_eq_true hka
    rcases hmem a haes with ⟨hai, hane, haprefix, habound⟩
    have hpos_a : 0 < (partAt ps a.1).length := List.length_pos_iff.mpr hane
    refine ⟨hai, hane, ?_, ?_⟩
    · by_cases hqa : q < a.2
      · have hdrop := drop_drop_cons_eq l r x (j := a.2) hqa
        change (partAt ps a.1).isPrefixOf ((l ++ r).drop (if q < a.2 then a.2 - 1 else a.2)) = true
        simp only [hqa, ↓reduceIte]
        rw [hdrop.symm]
        exact haprefix
      · have hale : a.2 + (partAt ps a.1).length ≤ q := by omega
        simp only [f, hqa, ↓reduceIte]
        have ha2le : a.2 ≤ l.length := by omega
        have hdropx : (l ++ x :: r).drop a.2 = l.drop a.2 ++ x :: r :=
          List.drop_append_of_le_length ha2le
        rw [hdropx] at haprefix
        have hple : (partAt ps a.1).length ≤ (l.drop a.2).length := by
          rw [List.length_drop]
          omega
        rcases (List.isPrefixOf_iff_prefix).mp haprefix with ⟨u, hu⟩
        have hp_l : (partAt ps a.1).isPrefixOf (l.drop a.2) = true :=
          isPrefix_append_of_length_le hu hple
        change (partAt ps a.1).isPrefixOf ((l ++ r).drop a.2) = true
        rw [List.drop_append_of_le_length ha2le]
        apply (List.isPrefixOf_iff_prefix).mpr
        rcases (List.isPrefixOf_iff_prefix).mp hp_l with ⟨u, hu⟩
        refine ⟨u ++ r, ?_⟩
        rw [← List.append_assoc, hu]
    · change (((if q < a.2 then a.2 - 1 else a.2 : Nat) : Int) - (startOf ps a.1 : Int)).natAbs ≤ k + 1
      by_cases hqa : q < a.2
      · simp [hqa]
        omega
      · simp [hqa]
        omega
  refine ⟨es', ⟨hpair', hmem'⟩, ?_⟩
  have hcount := destroyed_drop_le_one ps q es hpair (fun e he => (hmem e he).2.1)
  have hcomp : es.countP (fun x => !(pkeep x)) =
      es.countP (fun e => decide (e.2 ≤ q ∧ q < e.2 + (partAt ps e.1).length)) := by
    apply List.countP_congr
    intro x hx
    simp [pkeep]
  have hkcount := filter_length_ge_sub_one pkeep es (by rw [hcomp]; exact hcount)
  change es.length - 1 ≤ ((es.filter pkeep).map f).length
  simpa using hkcount

private theorem chain_add_step (ps : List Word) (l r : Word) (x : Nat) {k : Nat}
    {es : List (Nat × Nat)} (hes : ChainOK ps k (l ++ r) es) :
    ∃ es', ChainOK ps (k + 1) (l ++ x :: r) es' ∧ es.length - 1 ≤ es'.length := by
  let q := l.length
  let pkeep := fun e : Nat × Nat => decide (¬ (e.2 < q ∧ q < e.2 + (partAt ps e.1).length))
  let f := fun e : Nat × Nat => (e.1, if q ≤ e.2 then e.2 + 1 else e.2)
  let es' := (es.filter pkeep).map f
  have hpair := hes.1
  have hmem := hes.2
  have hpair_kept : (es.filter pkeep).Pairwise
      (fun e₁ e₂ => e₁.1 < e₂.1 ∧ e₁.2 + (partAt ps e₁.1).length ≤ e₂.2) :=
    hpair.filter pkeep
  have hH : ∀ a ∈ es.filter pkeep, ∀ b ∈ es.filter pkeep,
      (fun e₁ e₂ : Nat × Nat => e₁.1 < e₂.1 ∧ e₁.2 + (partAt ps e₁.1).length ≤ e₂.2) a b →
      (fun e₁ e₂ : Nat × Nat => e₁.1 < e₂.1 ∧ e₁.2 + (partAt ps e₁.1).length ≤ e₂.2) (f a) (f b) := by
    intro a ha b hb hab
    have hka : pkeep a = true := (List.mem_filter.mp ha).2
    have hkb : pkeep b = true := (List.mem_filter.mp hb).2
    have hna : ¬ (a.2 < q ∧ q < a.2 + (partAt ps a.1).length) := of_decide_eq_true hka
    have hnb : ¬ (b.2 < q ∧ q < b.2 + (partAt ps b.1).length) := of_decide_eq_true hkb
    have hne_a := (hmem a (List.mem_filter.mp ha).1).2.1
    have hpos_a : 0 < (partAt ps a.1).length := List.length_pos_iff.mpr hne_a
    refine ⟨hab.1, ?_⟩
    by_cases hqa : q ≤ a.2
    · have hqb : q ≤ b.2 := by omega
      simp [f, hqa, hqb]
      omega
    · have hale : a.2 + (partAt ps a.1).length ≤ q := by omega
      by_cases hqb : q ≤ b.2
      · simp [f, hqa, hqb]
        omega
      · simp [f, hqa, hqb]
        exact hab.2
  have hpair' : es'.Pairwise
      (fun e₁ e₂ => e₁.1 < e₂.1 ∧ e₁.2 + (partAt ps e₁.1).length ≤ e₂.2) :=
    pairwise_map_of_mem f (es.filter pkeep) hH hpair_kept
  have hmem' : ∀ e ∈ es', EntryOK ps (k + 1) (l ++ x :: r) e := by
    intro e he
    rw [List.mem_map] at he
    rcases he with ⟨a, ha, rfl⟩
    have hka : pkeep a = true := (List.mem_filter.mp ha).2
    have haes : a ∈ es := (List.mem_filter.mp ha).1
    have hna : ¬ (a.2 < q ∧ q < a.2 + (partAt ps a.1).length) := of_decide_eq_true hka
    rcases hmem a haes with ⟨hai, hane, haprefix, habound⟩
    have hpos_a : 0 < (partAt ps a.1).length := List.length_pos_iff.mpr hane
    refine ⟨hai, hane, ?_, ?_⟩
    · by_cases hqa : q ≤ a.2
      · have hdrop := drop_drop_add_eq l r x (j := a.2) hqa
        change (partAt ps a.1).isPrefixOf ((l ++ x :: r).drop (if q ≤ a.2 then a.2 + 1 else a.2)) = true
        simp only [hqa, ↓reduceIte]
        rw [hdrop.symm]
        exact haprefix
      · have hale : a.2 + (partAt ps a.1).length ≤ q := by omega
        simp only [f, hqa, ↓reduceIte]
        have ha2le : a.2 ≤ l.length := by omega
        have hdropx : (l ++ r).drop a.2 = l.drop a.2 ++ r :=
          List.drop_append_of_le_length ha2le
        rw [hdropx] at haprefix
        have hple : (partAt ps a.1).length ≤ (l.drop a.2).length := by
          rw [List.length_drop]
          omega
        rcases (List.isPrefixOf_iff_prefix).mp haprefix with ⟨u, hu⟩
        have hp_l : (partAt ps a.1).isPrefixOf (l.drop a.2) = true :=
          isPrefix_append_of_length_le hu hple
        change (partAt ps a.1).isPrefixOf ((l ++ x :: r).drop a.2) = true
        rw [List.drop_append_of_le_length ha2le]
        apply (List.isPrefixOf_iff_prefix).mpr
        rcases (List.isPrefixOf_iff_prefix).mp hp_l with ⟨v, hv⟩
        refine ⟨v ++ x :: r, ?_⟩
        rw [← List.append_assoc, hv]
    · change (((if q ≤ a.2 then a.2 + 1 else a.2 : Nat) : Int) - (startOf ps a.1 : Int)).natAbs ≤ k + 1
      by_cases hqa : q ≤ a.2
      · simp [hqa]
        omega
      · simp [hqa]
        omega
  refine ⟨es', ⟨hpair', hmem'⟩, ?_⟩
  have hcount := destroyed_add_le_one ps q es hpair (fun e he => (hmem e he).2.1)
  have hcomp : es.countP (fun x => !(pkeep x)) =
      es.countP (fun e => decide (e.2 < q ∧ q < e.2 + (partAt ps e.1).length)) := by
    apply List.countP_congr
    intro x hx
    simp [pkeep]
  have hkcount := filter_length_ge_sub_one pkeep es (by rw [hcomp]; exact hcount)
  change es.length - 1 ≤ ((es.filter pkeep).map f).length
  simpa using hkcount

private theorem chain_subst_step (ps : List Word) (l r : Word) (x y : Nat) {k : Nat}
    {es : List (Nat × Nat)} (hes : ChainOK ps k (l ++ x :: r) es) :
    ∃ es', ChainOK ps (k + 1) (l ++ y :: r) es' ∧ es.length - 1 ≤ es'.length := by
  let q := l.length
  let pkeep := fun e : Nat × Nat => decide (¬ (e.2 ≤ q ∧ q < e.2 + (partAt ps e.1).length))
  let es' := es.filter pkeep
  have hpair := hes.1
  have hmem := hes.2
  have hpair' : es'.Pairwise
      (fun e₁ e₂ => e₁.1 < e₂.1 ∧ e₁.2 + (partAt ps e₁.1).length ≤ e₂.2) :=
    hpair.filter pkeep
  have hmem' : ∀ e ∈ es', EntryOK ps (k + 1) (l ++ y :: r) e := by
    intro e he
    have hka : pkeep e = true := (List.mem_filter.mp he).2
    have hees : e ∈ es := (List.mem_filter.mp he).1
    have hne : ¬ (e.2 ≤ q ∧ q < e.2 + (partAt ps e.1).length) := of_decide_eq_true hka
    rcases hmem e hees with ⟨hei, hpartne, haprefix, habound⟩
    have hpos : 0 < (partAt ps e.1).length := List.length_pos_iff.mpr hpartne
    refine ⟨hei, hpartne, ?_, ?_⟩
    · by_cases hqe : q < e.2
      · have hdrop := drop_drop_subst_eq l r x y (j := e.2) hqe
        rw [← hdrop]
        exact haprefix
      · have hale : e.2 + (partAt ps e.1).length ≤ q := by omega
        have he2le : e.2 ≤ l.length := by omega
        have hdropx : (l ++ x :: r).drop e.2 = l.drop e.2 ++ x :: r :=
          List.drop_append_of_le_length he2le
        rw [hdropx] at haprefix
        have hple : (partAt ps e.1).length ≤ (l.drop e.2).length := by
          rw [List.length_drop]
          omega
        rcases (List.isPrefixOf_iff_prefix).mp haprefix with ⟨u, hu⟩
        have hp_l : (partAt ps e.1).isPrefixOf (l.drop e.2) = true :=
          isPrefix_append_of_length_le hu hple
        change (partAt ps e.1).isPrefixOf ((l ++ y :: r).drop e.2) = true
        rw [List.drop_append_of_le_length he2le]
        apply (List.isPrefixOf_iff_prefix).mpr
        rcases (List.isPrefixOf_iff_prefix).mp hp_l with ⟨v, hv⟩
        refine ⟨v ++ y :: r, ?_⟩
        rw [← List.append_assoc, hv]
    · omega
  refine ⟨es', ⟨hpair', hmem'⟩, ?_⟩
  have hcount := destroyed_subst_le_one ps q es hpair (fun e he => (hmem e he).2.1)
  have hcomp : es.countP (fun x => !(pkeep x)) =
      es.countP (fun e => decide (e.2 ≤ q ∧ q < e.2 + (partAt ps e.1).length)) := by
    apply List.countP_congr
    intro x hx
    simp [pkeep]
  have hkcount := filter_length_ge_sub_one pkeep es (by rw [hcomp]; exact hcount)
  change es.length - 1 ≤ (es.filter pkeep).length
  exact hkcount

private theorem anchors_survive (ps : List Word) {K : Nat} {a t : Word}
    (h : Edits K a t) (es : List (Nat × Nat)) (hes : ChainOK ps 0 a es) :
    ∃ es', ChainOK ps K t es' ∧ es.length - K ≤ es'.length := by
  induction h generalizing es with
  | refl a =>
      exact ⟨es, hes, by simp⟩
  | drop l r x h ih =>
      obtain ⟨es_mid, hmid, hlen_mid⟩ := ih es hes
      obtain ⟨es', hes', hlen'⟩ := chain_drop_step ps l r x hmid
      refine ⟨es', hes', ?_⟩
      omega
  | add l r x h ih =>
      obtain ⟨es_mid, hmid, hlen_mid⟩ := ih es hes
      obtain ⟨es', hes', hlen'⟩ := chain_add_step ps l r x hmid
      refine ⟨es', hes', ?_⟩
      omega
  | subst l r x y h ih =>
      obtain ⟨es_mid, hmid, hlen_mid⟩ := ih es hes
      obtain ⟨es', hes', hlen'⟩ := chain_subst_step ps l r x y hmid
      refine ⟨es', hes', ?_⟩
      omega

private theorem anchors_nonempty (ps : List Word) {k : Nat} {t : Word}
    (hk : k < ps.length) (hne : ∀ p ∈ ps, p ≠ []) (h : Edits k ps.flatten t) :
    ∃ es, ChainOK ps k t es ∧ 0 < es.length := by
  have hinit := init_chain ps hne
  obtain ⟨es, hes, hlen⟩ := anchors_survive ps h (initEs ps) hinit
  have hinitlen : (initEs ps).length = ps.length := by simp [initEs]
  rw [hinitlen] at hlen
  exact ⟨es, hes, by omega⟩

theorem sound_PigeonholeAnchor :
    TinylambOpt.ResearchGoals.Atlas.TextExecution.EditFilter.PigeonholeAnchor.Goal := by
  intro ps t k hk hne hedit
  obtain ⟨es, hes, hpos⟩ := anchors_nonempty ps hk hne hedit
  cases es with
  | nil => simp at hpos
  | cons e es_tail =>
      have heok := hes.2 e (by simp)
      rcases heok with ⟨hei, hpartne, haprefix, habound⟩
      have hmemps : partAt ps e.1 ∈ ps := by
        have hsome : ps[e.1]? = some ps[e.1] := List.getElem?_eq_getElem hei
        unfold partAt
        rw [hsome]
        simp only [Option.getD_some]
        exact List.getElem_mem hei
      exact ⟨partAt ps e.1, hmemps, occurs_of_prefix hpartne haprefix⟩

theorem sound_PositionalAnchor :
    TinylambOpt.ResearchGoals.Atlas.TextExecution.EditFilter.PositionalAnchor.Goal := by
  intro ps t k hk hne hedit
  obtain ⟨es, hes, hpos⟩ := anchors_nonempty ps hk hne hedit
  cases es with
  | nil => simp at hpos
  | cons e es_tail =>
      have heok := hes.2 e (by simp)
      rcases heok with ⟨hei, hpartne, haprefix, habound⟩
      refine ⟨e.1, hei, e.2, ?_, ?_⟩
      · unfold positions
        rw [List.mem_filter]
        have hjle : e.2 ≤ t.length := by
          have hdropne : t.drop e.2 ≠ [] := by
            intro hnil
            rcases (List.isPrefixOf_iff_prefix).mp haprefix with ⟨u, hu⟩
            rw [hnil] at hu
            have hlen0 := congrArg List.length hu
            simp at hlen0
            exact hpartne hlen0.1
          have hjlt : e.2 < t.length := by
            by_cases hle : t.length ≤ e.2
            · exact False.elim (hdropne (List.drop_eq_nil_of_le hle))
            · omega
          exact Nat.le_of_lt hjlt
        refine ⟨?_, ?_⟩
        · simpa [List.mem_range] using Nat.lt_succ_of_le hjle
        · simpa [partAt] using haprefix
      · simpa [startOf] using habound

end TinylambOpt.ResearchProofs.Atlas.TextExecution.EditFilterAnchors
