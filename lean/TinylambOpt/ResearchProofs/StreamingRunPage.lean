import TinylambOpt.ResearchGoals.StreamingRunPage

namespace TinylambOpt.ResearchProofs.StreamingRunPage

open TinylambOpt.ResearchGoals.StreamingRunPage
open TinylambOpt.Research.IncrementalSortPage

/-! Key order on rows (the SQL ORDER BY key: pkey, then skey). -/

private def keyLt (a b : Row) : Prop :=
  a.pkey < b.pkey ∨ (a.pkey = b.pkey ∧ a.skey < b.skey)

private def keyEq (a b : Row) : Prop :=
  a.pkey = b.pkey ∧ a.skey = b.skey

/-- The key order refined by appearance id: a strict total order on any
    input whose ids strictly increase. -/
private def Ties (a b : Row) : Prop :=
  keyLt a b ∨ (keyEq a b ∧ a.id < b.id)

/-- The comparator used by `pageSQL`'s merge sort. -/
private def cmpR (a b : Row) : Ordering :=
  if a.pkey = b.pkey then compare a.skey b.skey else compare a.pkey b.pkey

private def kLeB (a b : Row) : Bool := cmpR a b != Ordering.gt

private def kLtB (a b : Row) : Bool := cmpR a b == Ordering.lt

private theorem keyLt_irrefl (a : Row) : ¬ keyLt a a := by
  unfold keyLt
  omega

private theorem keyLt_trans {a b c : Row} (hab : keyLt a b) (hbc : keyLt b c) :
    keyLt a c := by
  unfold keyLt at *
  omega

private theorem keyLt_asym {a b : Row} (h : keyLt a b) : ¬ keyLt b a :=
  fun hba => keyLt_irrefl a (keyLt_trans h hba)

private theorem keyLt_trichotomy (a b : Row) :
    keyLt a b ∨ keyEq a b ∨ keyLt b a := by
  unfold keyLt keyEq
  by_cases hp : a.pkey = b.pkey
  · by_cases hs : a.skey = b.skey
    · exact Or.inr (Or.inl ⟨hp, hs⟩)
    · have h : a.skey < b.skey ∨ b.skey < a.skey := by omega
      rcases h with h | h
      · exact Or.inl (Or.inr ⟨hp, h⟩)
      · exact Or.inr (Or.inr (Or.inr ⟨hp.symm, h⟩))
  · have h : a.pkey < b.pkey ∨ b.pkey < a.pkey := by omega
    rcases h with h | h
    · exact Or.inl (Or.inl h)
    · exact Or.inr (Or.inr (Or.inl h))

private theorem Ties_of_keyLt {a b : Row} (h : keyLt a b) : Ties a b := Or.inl h

private theorem Ties_trans {a b c : Row} (hab : Ties a b) (hbc : Ties b c) : Ties a c := by
  simp only [Ties, keyLt, keyEq] at *
  rcases hab with h1 | ⟨h2, hid⟩
  · rcases hbc with h3 | ⟨h4, _⟩
    · omega
    · omega
  · rcases h2 with ⟨p1, s1⟩
    rcases hbc with h3 | ⟨h4, _⟩
    · rcases h3 with h3 | ⟨p3, s3⟩
      · omega
      · omega
    · rcases h4 with ⟨p4, s4⟩
      omega

private theorem keyLt_keyEq_false {a b : Row} (hab : keyLt a b) (hba : keyEq b a) : False := by
  unfold keyLt keyEq at *
  rcases hab with h | ⟨hp, hs⟩
  · rcases hba with ⟨q1, q2⟩
    omega
  · rcases hba with ⟨q1, q2⟩
    omega

private theorem Ties_asym {a b : Row} (hab : Ties a b) : ¬ Ties b a := by
  intro hba
  rcases hab with h1 | ⟨h2, hid⟩
  · rcases hba with h3 | ⟨h4, _⟩
    · exact keyLt_asym h1 h3
    · exact keyLt_keyEq_false h1 h4
  · rcases hba with h3 | ⟨h4, hid'⟩
    · exact keyLt_keyEq_false (a := b) (b := a) h3 h2
    · omega

/-! Relating the spec comparators to `keyLt` / `keyEq`. -/

private theorem cmpInt_lt (a b : Int) : compare a b = Ordering.lt ↔ a < b := by
  show compareOfLessAndEq a b = Ordering.lt ↔ a < b
  rw [compareOfLessAndEq_eq_lt]

private theorem cmpInt_gt (a b : Int) : compare a b = Ordering.gt ↔ b < a := by
  show compareOfLessAndEq a b = Ordering.gt ↔ b < a
  rw [compareOfLessAndEq_eq_gt (α := Int)
    (by intro x y hx hy; omega) (by intro x y; omega) (by intro x y; omega)]

private theorem cmpR_lt (a b : Row) : cmpR a b = Ordering.lt ↔ keyLt a b := by
  unfold cmpR keyLt
  by_cases hp : a.pkey = b.pkey
  · rw [if_pos hp]
    constructor
    · intro h
      rw [cmpInt_lt] at h
      exact Or.inr ⟨hp, h⟩
    · intro h
      rcases h with h | ⟨heq, h⟩
      · omega
      · rw [cmpInt_lt]
        exact h
  · rw [if_neg hp]
    constructor
    · intro h
      rw [cmpInt_lt] at h
      exact Or.inl h
    · intro h
      rcases h with h | ⟨heq, _⟩
      · rw [cmpInt_lt]
        exact h
      · exfalso
        exact hp heq

private theorem cmpR_gt (a b : Row) : cmpR a b = Ordering.gt ↔ keyLt b a := by
  unfold cmpR keyLt
  by_cases hp : a.pkey = b.pkey
  · rw [if_pos hp]
    constructor
    · intro h
      rw [cmpInt_gt] at h
      exact Or.inr ⟨hp.symm, h⟩
    · intro h
      rcases h with h | ⟨heq, h⟩
      · omega
      · rw [cmpInt_gt]
        exact h
  · rw [if_neg hp]
    constructor
    · intro h
      rw [cmpInt_gt] at h
      exact Or.inl h
    · intro h
      rcases h with h | ⟨heq, _⟩
      · rw [cmpInt_gt]
        exact h
      · exfalso
        exact hp heq.symm

private theorem bneq_gt_true {x : Ordering} : (x != Ordering.gt) = true ↔ ¬ (x = Ordering.gt) := by
  cases x <;> decide

private theorem bneq_gt_false {x : Ordering} : (x != Ordering.gt) = false ↔ x = Ordering.gt := by
  cases x <;> decide

private theorem beq_lt_true {x : Ordering} : (x == Ordering.lt) = true ↔ x = Ordering.lt := by
  cases x <;> decide

private theorem kLeB_iff (a b : Row) : kLeB a b = true ↔ keyLt a b ∨ keyEq a b := by
  unfold kLeB
  rw [bneq_gt_true]
  constructor
  · intro h
    rcases keyLt_trichotomy a b with hab | heq | hba
    · exact Or.inl hab
    · exact Or.inr heq
    · exact absurd (cmpR_gt (a := a) (b := b) |>.mpr hba) h
  · rintro (hab | heq) hba
    · exact absurd (cmpR_gt (a := a) (b := b) |>.mp hba) (keyLt_asym hab)
    · exact keyLt_keyEq_false (a := b) (b := a) (cmpR_gt (a := a) (b := b) |>.mp hba) heq

private theorem kLtB_iff (a b : Row) : kLtB a b = true ↔ keyLt a b := by
  unfold kLtB
  rw [beq_lt_true, cmpR_lt]

private theorem not_kLeB_of_keyLt {a b : Row} (h : keyLt b a) : kLeB a b = false := by
  unfold kLeB
  rw [bneq_gt_false, cmpR_gt]
  exact h


/-! ---- Part 2: stable merge sort matches the id-refined key order ---- -/

private theorem keyLt_of_keyLt_keyEq {a b c : Row} (hab : keyLt a b) (hbc : keyEq b c) :
    keyLt a c := by
  unfold keyLt keyEq at *
  omega

private theorem keyLt_of_keyEq_keyLt {a b c : Row} (hab : keyEq a b) (hbc : keyLt b c) :
    keyLt a c := by
  unfold keyLt keyEq at *
  omega

private theorem keyEq_trans {a b c : Row} (hab : keyEq a b) (hbc : keyEq b c) :
    keyEq a c := by
  unfold keyEq at *
  exact ⟨hab.1.trans hbc.1, hab.2.trans hbc.2⟩

private theorem kLeB_trans_bool :
    ∀ a b c : Row, kLeB a b = true → kLeB b c = true → kLeB a c = true := by
  intro a b c h1 h2
  rcases kLeB_iff a b |>.mp h1 with hab | hab
  · rcases kLeB_iff b c |>.mp h2 with hbc | hbc
    · exact kLeB_iff a c |>.mpr (Or.inl (keyLt_trans hab hbc))
    · exact kLeB_iff a c |>.mpr (Or.inl (keyLt_of_keyLt_keyEq hab hbc))
  · rcases kLeB_iff b c |>.mp h2 with hbc | hbc
    · exact kLeB_iff a c |>.mpr (Or.inl (keyLt_of_keyEq_keyLt hab hbc))
    · exact kLeB_iff a c |>.mpr (Or.inr (keyEq_trans hab hbc))

private theorem kLeB_total_bool (a b : Row) : kLeB a b || kLeB b a = true := by
  rcases keyLt_trichotomy a b with hab | heq | hba
  · simp [kLeB_iff a b |>.mpr (Or.inl hab)]
  · simp [kLeB_iff a b |>.mpr (Or.inr heq)]
  · simp [kLeB_iff b a |>.mpr (Or.inl hba)]

private theorem kLeB_trans : ∀ a b c : Row, kLeB a b → kLeB b c → kLeB a c :=
  fun a b c h1 h2 => kLeB_trans_bool a b c h1 h2

private theorem kLeB_total : ∀ a b : Row, kLeB a b || kLeB b a :=
  fun a b => kLeB_total_bool a b

private theorem mem_zipIdxN {l : List α} {n : Nat} {p : α × Nat}
    (hp : p ∈ l.zipIdx n) :
    ∃ (i : Nat) (hi : i < l.length), p = (l[i]'hi, i + n) := by
  match l, n, p, hp with
  | [], _, _, h => cases h
  | x :: xs, n, (a, m), hp =>
      simp only [List.zipIdx_cons, List.mem_cons] at hp
      rcases hp with h | h
      · rcases Prod.mk.inj h with ⟨rfl, rfl⟩
        exact ⟨0, by simpa, by simp⟩
      · obtain ⟨i, hi, e⟩ := mem_zipIdxN (l := xs) (n := n + 1) (p := (a, m)) h
        refine ⟨i + 1, by simpa using hi, ?_⟩
        rw [e, List.getElem_cons_succ x xs i (by simpa using hi)]
        rw [show i + 1 + n = i + (n + 1) by omega]

private theorem nodup_zipIdx (l : List α) (n : Nat) : (l.zipIdx n).Nodup := by
  match l, n with
  | [], _ => simp
  | x :: xs, n =>
      rw [List.zipIdx_cons, List.nodup_cons]
      refine ⟨?_, nodup_zipIdx xs (n + 1)⟩
      intro hx
      obtain ⟨i, hi, e⟩ := mem_zipIdxN hx
      rcases Prod.mk.inj e with ⟨_, r2⟩
      omega

private theorem ids_of_index {l : List Row} (h : l.Pairwise (fun a b => a.id < b.id))
    {i j : Nat} (hi : i < l.length) (hj : j < l.length) (hij : i < j) :
    l[i].id < l[j].id :=
  List.Pairwise.rel_getElem_of_lt hi hj h hij

private theorem ids_ne_of_mem {l : List Row} (hind : l.Pairwise (fun a b => a.id < b.id))
    {a b : Row} (ha : a ∈ l) (hb : b ∈ l) (hab : a.id = b.id) : a = b := by
  obtain ⟨i, hi, eia⟩ := List.mem_iff_getElem.mp ha
  obtain ⟨j, hj, eib⟩ := List.mem_iff_getElem.mp hb
  rcases Nat.lt_trichotomy i j with h | h | h
  · rw [← eia, ← eib] at hab ⊢
    have h1 := ids_of_index hind hi hj h
    omega
  · subst h
    exact eia.symm.trans eib
  · rw [← eia, ← eib] at hab ⊢
    have h1 := ids_of_index hind hj hi h
    omega

private theorem nodup_of_ids {l : List Row} (hind : l.Pairwise (fun a b => a.id < b.id)) :
    l.Nodup := by
  refine List.nodup_iff_pairwise_ne.mpr ?_
  induction l with
  | nil => exact List.Pairwise.nil
  | cons x xs ih =>
      obtain ⟨h1, h2⟩ := List.pairwise_cons.mp hind
      refine List.pairwise_cons.mpr ⟨?_, ih h2⟩
      intro y hy heq
      have hlt := h1 y hy
      rw [← heq] at hlt
      omega

private theorem getElem_congr {l : List α} {i j : Nat} (hij : i = j)
    (hi : i < l.length) (hj : j < l.length) : l[i]'hi = l[j]'hj := by
  cases hij
  rfl

private theorem ties_zip_step
    {l : List Row} (hind : l.Pairwise (fun a b => a.id < b.id))
    {w : Row × Nat} {ws : List (Row × Nat)}
    (hmem : ∀ p ∈ w :: ws, ∃ (hi : p.2 < l.length), p = (l[p.2]'hi, p.2))
    (hznd : (w :: ws).Pairwise (· ≠ ·))
    (hz : (w :: ws).Pairwise (fun p q => List.zipIdxLE kLeB p q))
    {x : Row × Nat} (hx : x ∈ ws) :
    Ties w.1 x.1 := by
  obtain ⟨iw, ew⟩ := hmem w List.mem_cons_self
  obtain ⟨ix, ex⟩ := hmem x (List.mem_cons_of_mem w hx)
  have h1w : w.1 = l[w.2]'iw := (Prod.mk.inj ew).1
  have h1x : x.1 = l[x.2]'ix := (Prod.mk.inj ex).1
  have hwx := (List.pairwise_cons.mp hz).1 x hx
  simp only [List.zipIdxLE] at hwx
  by_cases hA : kLeB w.1 x.1 = true
  · rw [if_pos hA] at hwx
    by_cases hB : kLeB x.1 w.1 = true
    · rw [if_pos hB, decide_eq_true_eq] at hwx
      rcases keyLt_trichotomy w.1 x.1 with hlt | heq | hgt
      · exact Or.inl hlt
      · have hij : w.2 < x.2 := by
          rcases Nat.eq_or_lt_of_le hwx with hxy | hxy
          · exfalso
            exact (List.pairwise_cons.mp hznd).1 x hx
              (Prod.ext (h1w.trans (getElem_congr hxy iw ix) |>.trans h1x.symm) hxy)
          · exact hxy
        have hids := ids_of_index hind iw ix hij
        rw [← h1w, ← h1x] at hids
        exact Or.inr ⟨heq, hids⟩
      · exfalso
        have hnc := not_kLeB_of_keyLt hgt
        rw [hnc] at hA
        exact absurd hA (by simp)
    · rw [if_neg hB] at hwx
      rcases keyLt_trichotomy w.1 x.1 with hlt | heq | hgt
      · exact Or.inl hlt
      · exfalso
        exact absurd ((kLeB_iff x.1 w.1).mpr (Or.inr ⟨heq.1.symm, heq.2.symm⟩)) hB
      · exfalso
        exact absurd ((kLeB_iff x.1 w.1).mpr (Or.inl hgt)) hB
  · rw [if_neg hA] at hwx
    exact absurd hwx (by simp)

private theorem ties_zip_aux
    {l : List Row} (hind : l.Pairwise (fun a b => a.id < b.id))
    {z : List (Row × Nat)}
    (hmem : ∀ p ∈ z, ∃ (hi : p.2 < l.length), p = (l[p.2]'hi, p.2))
    (hznd : z.Pairwise (· ≠ ·))
    (hz : z.Pairwise (fun p q => List.zipIdxLE kLeB p q)) :
    (z.map (·.1)).Pairwise Ties := by
  induction z with
  | nil => exact List.Pairwise.nil
  | cons w ws ih =>
      obtain ⟨h1, h2⟩ := List.pairwise_cons.mp hz
      refine List.pairwise_cons.mpr ⟨?_, ih ?_ ?_ h2⟩
      · intro y hy
        obtain ⟨p, hp, rfl⟩ := List.mem_map.mp hy
        exact ties_zip_step hind hmem hznd hz hp
      · intro p hp
        exact hmem p (List.mem_cons_of_mem w hp)
      · exact (List.pairwise_cons.mp hznd).2


private theorem mergeSort_Ties (l : List Row)
    (hind : l.Pairwise (fun a b => a.id < b.id)) :
    (l.mergeSort kLeB).Pairwise Ties := by
  have hperm : ((l.zipIdx).mergeSort (List.zipIdxLE kLeB)).Perm l.zipIdx :=
    List.mergeSort_perm _ _
  have hz : ((l.zipIdx).mergeSort (List.zipIdxLE kLeB)).Pairwise
      (fun p q => List.zipIdxLE kLeB p q) :=
    List.pairwise_mergeSort (le := List.zipIdxLE kLeB)
      (List.zipIdxLE_trans kLeB_trans) (List.zipIdxLE_total kLeB_total) _
  have hnd : ((l.zipIdx).mergeSort (List.zipIdxLE kLeB)).Pairwise (· ≠ ·) :=
    List.nodup_iff_pairwise_ne.mp (hperm.nodup_iff.mpr (nodup_zipIdx l 0))
  have hmem : ∀ p ∈ (l.zipIdx).mergeSort (List.zipIdxLE kLeB),
      ∃ (hi : p.2 < l.length), p = (l[p.2]'hi, p.2) := by
    intro p hp
    obtain ⟨i, hi, e⟩ := mem_zipIdxN (hperm.mem_iff.mp hp)
    rcases Prod.mk.inj e with ⟨e1, e2⟩
    rw [Nat.add_zero] at e2
    subst e2
    exact ⟨by simpa using hi, by simp [← e1]⟩
  rw [← List.mergeSort_zipIdx (le := kLeB)]
  exact ties_zip_aux hind hmem hnd hz

private theorem Ties_rowLt {a b : Row} (h : Ties a b) : rowLt a b := by
  unfold Ties keyLt keyEq rowLt at *
  omega

private theorem pairwise_Ties_rowLt {l : List Row} (h : l.Pairwise Ties) : SortedRows l := by
  induction l with
  | nil => exact List.Pairwise.nil
  | cons x xs ih =>
      obtain ⟨h1, h2⟩ := List.pairwise_cons.mp h
      exact List.pairwise_cons.mpr ⟨fun b hb => Ties_rowLt (h1 b hb), ih h2⟩

private theorem isort_eq_mergeSort (l : List Row)
    (hind : l.Pairwise (fun a b => a.id < b.id)) :
    isort l = l.mergeSort kLeB :=
  sorted_perm_eq
    (isort_sorted l (nodup_of_ids hind))
    (pairwise_Ties_rowLt (mergeSort_Ties l hind))
    ((isort_perm l).trans (List.mergeSort_perm l kLeB).symm)

private theorem runSort_eq_mergeSort (files : List (List Row))
    (hps : PrefixSorted files.flatten)
    (hind : files.flatten.Pairwise (fun a b => a.id < b.id)) :
    runSort files = files.flatten.mergeSort kLeB :=
  (runSort_eq_isort files hps (nodup_of_ids hind)).trans (isort_eq_mergeSort files.flatten hind)

open TinylambOpt.ResearchGoals in
private theorem pageSQL_eq (off k : Nat) (l : List Row) :
    SqlFetchWithTies.pageSQL off k l =
      SqlFetchWithTies.fetch kLtB off k (l.mergeSort kLeB) := rfl

/-! ## Part 3: the streaming cursor emits exactly the SQL page. -/

private abbrev KeyLeP (a b : KeyPair) : Prop :=
  a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 ≤ b.2)

private def pLt (a b : KeyPair) : Prop :=
  a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 < b.2)

private theorem KeyLeP_refl (a : KeyPair) : KeyLeP a a := Or.inr ⟨rfl, Int.le_refl _⟩

private theorem keyLeP_of_rowLt {x y : Row} (h : rowLt x y) : KeyLeP (rowKey x) (rowKey y) := by
  unfold rowKey KeyLeP
  rcases h with hlt | ⟨heq, hs⟩
  · exact Or.inl hlt
  · rcases hs with hs1 | ⟨hs2, _⟩
    · exact Or.inr ⟨heq, by omega⟩
    · exact Or.inr ⟨heq, by omega⟩

private theorem not_KeyLeP_of_pLt {a b : KeyPair} (h : pLt a b) : ¬ KeyLeP b a := by
  unfold pLt KeyLeP at *
  intro hh
  rcases hh with h2 | ⟨he, hle⟩ <;> rcases h with h3 | ⟨he3, hlt⟩ <;> omega

private theorem pLt_of_pLt_KeyLeP {a b c : KeyPair} (h1 : pLt a b) (h2 : KeyLeP b c) : pLt a c := by
  unfold pLt KeyLeP at *
  rcases h1 with h | ⟨e, h⟩ <;> rcases h2 with h' | ⟨e', h'⟩ <;>
    (first | exact Or.inl (by omega) | exact Or.inr ⟨by omega, by omega⟩)

private theorem KeyLeP_of_not_pLt {a b : KeyPair} (h : ¬ pLt b a) : KeyLeP a b := by
  by_cases hab : a.1 < b.1
  · exact Or.inl hab
  · by_cases h2 : b.1 < a.1
    · exact absurd (Or.inl h2) h
    · right
      have h1 : a.1 = b.1 := by omega
      have hlt : ¬ b.2 < a.2 := fun hh => h (Or.inr ⟨h1.symm, hh⟩)
      exact ⟨h1, by omega⟩

private theorem keyLe_iff_KeyLeP (r : Row) (b : KeyPair) : keyLe r b ↔ KeyLeP (rowKey r) b := by
  unfold keyLe rowKey KeyLeP
  omega

private theorem pLt_keyLtB (x y : Row) : kLtB x y = true ↔ pLt (rowKey x) (rowKey y) := by
  rw [kLtB_iff]
  unfold keyLt rowKey pLt
  omega

/-- In a sorted list every element of the first part precedes every element of
    the second part in key order. -/
private theorem keyOrd_sorted {l : List Row} (h : SortedRows l) :
    ∀ (u v : List Row), l = u ++ v → ∀ y ∈ u, ∀ z ∈ v, KeyLeP (rowKey y) (rowKey z) := by
  induction l with
  | nil =>
      intro u v hl y hy
      cases u with
      | nil => cases hy
      | cons a as => rw [List.cons_append] at hl; exact absurd hl (by simp)
  | cons x xs ih =>
      intro u v hl y hy z hz
      cases u with
      | nil => simp_all
      | cons u' u'' =>
          rw [List.cons_append] at hl
          obtain ⟨rfl, hxs⟩ := List.cons.inj hl
          subst hxs
          by_cases hyx : y = x
          · subst hyx
            exact keyLeP_of_rowLt ((List.pairwise_cons.mp h).1 z
              (List.mem_append.mpr (Or.inr hz)))
          · have hy' : y ∈ u'' := (List.mem_cons.mp hy).resolve_left hyx
            exact ih ((List.pairwise_cons.mp h).2) u'' v rfl y hy' z hz

private theorem emit_skip {r : Row} {s : Cursor} (hstop : ¬ s.stopped) (hsk : s.skip > 0) :
    emit s r = { s with skip := s.skip - 1 } := by
  show (if s.stopped then _ else if s.skip > 0 then _ else _) = _
  rw [if_neg hstop, if_pos hsk]

private theorem emit_need_pos {r : Row} {s : Cursor} (hstop : ¬ s.stopped) (hsk : ¬ s.skip > 0)
    (hneed : s.need > 0) :
    emit s r = { s with need := s.need - 1, output := s.output ++ [r], boundary := some (rowKey r) } := by
  show (if s.stopped then _ else if s.skip > 0 then _ else if s.need > 0 then _ else _) = _
  rw [if_neg hstop, if_neg hsk, if_pos hneed]

private theorem emit_need_zero_tie {r : Row} {s : Cursor} (hstop : ¬ s.stopped) (hsk : ¬ s.skip > 0)
    (hneed : s.need = 0) (hb : (s.boundary == some (rowKey r)) = true) :
    emit s r = { s with output := s.output ++ [r] } := by
  show (if s.stopped then _ else if s.skip > 0 then _ else if s.need > 0 then _
    else if s.boundary == some (rowKey r) then _ else _) = _
  rw [if_neg hstop, if_neg hsk, if_neg (by simp [hneed] : ¬ s.need > 0), if_pos hb]

private theorem emit_need_zero_stop {r : Row} {s : Cursor} (hstop : ¬ s.stopped) (hsk : ¬ s.skip > 0)
    (hneed : s.need = 0) (hb : (s.boundary == some (rowKey r)) = false) :
    emit s r = { s with stopped := true } := by
  show (if s.stopped then _ else if s.skip > 0 then _ else if s.need > 0 then _
    else if s.boundary == some (rowKey r) then _ else _) = _
  rw [if_neg hstop, if_neg hsk, if_neg (by simp [hneed] : ¬ s.need > 0), if_neg (by simpa using hb)]

/-- Reachable-cursor invariants: a positive skip only occurs while demand
    remains, and a zero demand only occurs after the skip phase. -/
private def Inv (s : Cursor) : Prop :=
  (¬ s.stopped → s.skip > 0 → s.need > 0) ∧ (¬ s.stopped → s.need = 0 → s.skip = 0)

/-- Observable state of a cursor: everything except `pending` and `peak`. -/
private abbrev Obs (s : Cursor) := (s.output, s.skip, s.need, s.boundary, s.stopped)

private theorem emit_inv {s : Cursor} (h : Inv s) (r : Row) : Inv (emit s r) := by
  obtain ⟨h1, h2⟩ := h
  by_cases hst : s.stopped
  · have hstep : emit s r = s := if_pos hst
    rw [hstep]
    exact ⟨h1, h2⟩
  · by_cases hsk : s.skip > 0
    · rw [emit_skip hst hsk]
      refine ⟨?_, ?_⟩
      · exact fun hn hk => h1 (by simpa using hn) (by omega)
      · intro hn hz
        have hs0 : s.skip = 0 := by simpa using h2 (by simpa using hn) (by simpa using hz)
        rw [hs0]
    · by_cases hneed : s.need > 0
      · rw [emit_need_pos hst hsk hneed]
        refine ⟨?_, ?_⟩
        · exact fun hn hk => absurd (by simpa using hk) hsk
        · intro _ _
          simpa using Nat.eq_zero_of_le_zero (Nat.le_of_not_lt hsk)
      · have hneed0 : s.need = 0 := Nat.eq_zero_of_le_zero (Nat.le_of_not_lt hneed)
        cases hb : (s.boundary == some (rowKey r)) with
        | true =>
            rw [emit_need_zero_tie hst hsk hneed0 hb]
            refine ⟨?_, ?_⟩
            · exact fun hn hk => h1 (by simpa using hn) (by simpa using hk)
            · exact fun hn hz => h2 (by simpa using hn) (by simpa using hz)
        | false =>
            rw [emit_need_zero_stop hst hsk hneed0 hb]
            refine ⟨?_, ?_⟩
            · intro hn
              exact absurd hn (by simp)
            · intro hn
              exact absurd hn (by simp)

private theorem fold_emit_inv {l : List Row} {s : Cursor} (h : Inv s) : Inv (l.foldl emit s) := by
  induction l generalizing s with
  | nil => exact h
  | cons x xs ih => exact ih (emit_inv h x)

private theorem emit_pending {r : Row} {s : Cursor} : (emit s r).pending = s.pending := by
  by_cases h1 : s.stopped <;> by_cases h2 : s.skip > 0 <;> by_cases h3 : s.need > 0 <;>
    by_cases h4 : (s.boundary == some (rowKey r)) <;> simp [emit, h1, h2, h3, h4]

private theorem fold_emit_pending {l : List Row} {s : Cursor} :
    (l.foldl emit s).pending = s.pending := by
  induction l generalizing s with
  | nil => rfl
  | cons x xs ih => rw [List.foldl_cons, ih, emit_pending]

private theorem emit_peak {r : Row} {s : Cursor} : (emit s r).peak = s.peak := by
  by_cases h1 : s.stopped <;> by_cases h2 : s.skip > 0 <;> by_cases h3 : s.need > 0 <;>
    by_cases h4 : (s.boundary == some (rowKey r)) <;> simp [emit, h1, h2, h3, h4]

private theorem fold_emit_peak {l : List Row} {s : Cursor} : (l.foldl emit s).peak = s.peak := by
  induction l generalizing s with
  | nil => rfl
  | cons x xs ih => rw [List.foldl_cons, ih, emit_peak]

private theorem flush_pending (s : Cursor) : (flush s).pending = [] := by
  show ((isort s.pending).foldl emit {s with pending := []}).pending = []
  rw [fold_emit_pending]

private theorem flush_peak (s : Cursor) : (flush s).peak = s.peak := by
  show ((isort s.pending).foldl emit {s with pending := []}).peak = s.peak
  rw [fold_emit_peak]

private theorem fold_emit_stopped {l : List Row} {s : Cursor} (h : s.stopped) :
    l.foldl emit s = s := by
  induction l generalizing s with
  | nil => rfl
  | cons x xs ih =>
      rw [List.foldl_cons]
      have hx : emit s x = s := if_pos h
      rw [hx, ih h]

private theorem foldl_feed_stopped {l : List (List Row)} {s : Cursor} (h : s.stopped) :
    l.foldl (fun acc f => f.foldl feed acc) s = s := by
  induction l generalizing s with
  | nil => rfl
  | cons f fs ih =>
      rw [List.foldl_cons]
      have hf : f.foldl feed s = s := by
        induction f generalizing s with
        | nil => rfl
        | cons r rs ih' =>
            rw [List.foldl_cons]
            have hr : feed s r = s := if_pos h
            rw [hr, ih' h]
      rw [hf, ih h]

private theorem fold_emit_obs_congr {l : List Row} {s t : Cursor} (h : Obs s = Obs t) :
    Obs (l.foldl emit s) = Obs (l.foldl emit t) := by
  induction l generalizing s t with
  | nil => exact h
  | cons x xs ih =>
      have h' : Obs (emit s x) = Obs (emit t x) := by
        cases s with
        | mk so sp sk sn sb ss spk =>
            cases t with
            | mk to tp tk tn tb ts tpk =>
                simp [Obs, Prod.mk.injEq] at h
                obtain ⟨e1, e2, e3, e4, e5⟩ := h
                subst e1; subst e2; subst e3; subst e4; subst e5
                by_cases hb : ss <;> by_cases hc : 0 < sk <;> by_cases hd : 0 < sn <;>
                  by_cases he : (sb == some (rowKey x)) <;> simp [Obs, emit, hb, hc, hd, he]
      rw [List.foldl_cons, List.foldl_cons]
      exact ih h'

/-- The page a `FETCH FIRST k ROWS WITH TIES` produces from a sorted list. -/
private def pageSel (after : List Row) (k : Nat) : List Row :=
  let b := (after.take k).getLast?.map rowKey
  after.take k ++ (after.drop k).takeWhile (fun row => b == some (rowKey row))

private theorem List.take_of_len_le {l : List α} (h : l.length ≤ n) : l.take n = l := by
  cases l with
  | nil => simp
  | cons a as =>
      cases n with
      | zero => simp at h
      | succ n =>
          rw [List.take_succ_cons]
          congr 1
          exact List.take_of_len_le (by simpa using h)

private theorem List.drop_of_len_le {l : List α} (h : l.length ≤ n) : l.drop n = [] := by
  cases l with
  | nil => simp
  | cons a as =>
      cases n with
      | zero => simp at h
      | succ n =>
          rw [List.drop_succ_cons]
          exact List.drop_of_len_le (by simpa using h)

private theorem List.exists_append_last {l : List α} (h : l ≠ []) : ∃ w c, l = w ++ [c] := by
  cases l with
  | nil => exact absurd rfl h
  | cons a as =>
      cases as with
      | nil => exact ⟨[], a, by simp⟩
      | cons b bs =>
          obtain ⟨w, c, hw⟩ := exists_append_last (by simp : (b :: bs) ≠ [])
          exact ⟨a :: w, c, by rw [List.cons_append, hw]⟩

private theorem getLast?_snoc {l : List α} (a : α) : (l ++ [a]).getLast? = some a := by
  rw [List.getLast?_append]
  simp

private theorem filter_all {l : List Row} (p : Row → Bool) (h : ∀ x ∈ l, p x = true) :
    l.filter p = l := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      rw [List.filter_cons_of_pos (h x ((by simp))),
        ih (fun y hy => h y ((by simp [hy])))]

private theorem beq_key_iff (u v : KeyPair) : (u == v) = true ↔ u = v := by
  cases u with
  | mk a b => cases v with
    | mk c d =>
        simp [beq_iff_eq]

private theorem keyEq_of_KeyLeP_both {a b : KeyPair} (h1 : KeyLeP a b) (h2 : KeyLeP b a) : a = b := by
  unfold KeyLeP at h1 h2
  refine Prod.ext ?_ ?_
  · rcases h1 with h | ⟨e, _⟩ <;> rcases h2 with h' | ⟨e', _⟩ <;> omega
  · rcases h1 with _ | ⟨_, h⟩ <;> rcases h2 with _ | ⟨_, h'⟩ <;> omega

private theorem takeWhile_all {xs ys : List Row} (p : Row → Bool)
    (h : ∀ x ∈ xs, p x = true) : (xs ++ ys).takeWhile p = xs ++ ys.takeWhile p := by
  induction xs generalizing ys with
  | nil => simp
  | cons x xs ih =>
      rw [List.cons_append, List.takeWhile_cons, if_pos (h x ((by simp)))]
      congr 1
      exact ih (fun y hy => h y ((by simp [hy])))

private theorem takeWhile_congr_on {l : List Row} (p q : Row → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.takeWhile p = l.takeWhile q := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      rw [List.takeWhile_cons, List.takeWhile_cons, h x ((by simp))]
      by_cases hq : (q x = true)
      · rw [if_pos hq, if_pos hq]
        congr 1
        exact ih (fun y hy => h y ((by simp [hy])))
      · rw [if_neg hq, if_neg hq]

private theorem filter_congr_on {l : List Row} (p q : Row → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.filter p = l.filter q := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have hx : p x = q x := h x ((by simp))
      cases hp : p x with
      | true =>
          rw [List.filter_cons_of_pos hp,
            List.filter_cons_of_pos (by rw [← hx]; exact hp)]
          congr 1
          exact ih (fun y hy => h y ((by simp [hy])))
      | false =>
          have hn : ¬ q x = true := by
            intro hh
            rw [← hx] at hh
            exact absurd hh (by simp [hp])
          rw [List.filter_cons_of_neg hn, List.filter_cons_of_neg (by simp [hp])]
          exact ih (fun y hy => h y ((by simp [hy])))

/-! ## Part 4: closed forms for `List.foldl emit` (skip / take / tie phases). -/

private def e₀ (off k : Nat) : Cursor :=
  { skip := off, need := k, stopped := k == 0 }

private theorem inv_e₀ (off k : Nat) : Inv (e₀ off k) := by
  constructor
  · intro hns hsk
    cases k with
    | zero => exact absurd (by simp [e₀]) hns
    | succ k' => simp [e₀]
  · intro hns hneed
    cases k with
    | zero => exact absurd (by simp [e₀]) hns
    | succ k' => exact absurd hneed (by simp [e₀])

private theorem fold_emit_skipphase {s : Cursor} (q : List Row)
    (hlen : q.length ≤ s.skip) (hns : ¬ s.stopped) :
    q.foldl emit s = { s with skip := s.skip - q.length } := by
  induction q generalizing s with
  | nil => simp
  | cons r t ih =>
      have hsk1 : s.skip > 0 := by
        have : t.length + 1 ≤ s.skip := hlen
        omega
      rw [List.foldl_cons, emit_skip hns hsk1,
        ih (s := ({ s with skip := s.skip - 1 } : Cursor))
          (show t.length ≤ s.skip - 1 from by
          have := hlen
          simp only [List.length_cons] at this
          omega) hns]
      simp only [List.length_cons]
      congr 1 <;> first | rfl | omega | simp

private theorem fold_emit_needtake {s : Cursor} (q : List Row)
    (hq : q ≠ []) (hlen : q.length ≤ s.need) (hsk : ¬ s.skip > 0) (hns : ¬ s.stopped) :
    q.foldl emit s =
      { s with
        need := s.need - q.length, output := s.output ++ q,
        boundary := q.getLast?.map rowKey } := by
  cases q with
  | nil => exact absurd rfl hq
  | cons r t =>
      have hneed : s.need > 0 := by
        have : t.length + 1 ≤ s.need := hlen
        omega
      rw [List.foldl_cons, emit_need_pos hns hsk hneed]
      cases t with
      | nil =>
          congr 1 <;> first | rfl | omega | simp | (simp; omega)
      | cons r' t' =>
          have h1 : (r' :: t').length + 1 ≤ s.need := hlen
          have hgh : ((r :: r' :: t').getLast? : Option Row) = (r' :: t').getLast? := rfl
          rw [hgh, fold_emit_needtake (s := ({ s with need := s.need - 1, output := s.output ++ [r], boundary := some (rowKey r) } : Cursor)) (r' :: t') (by simp) (show (r' :: t').length ≤ s.need - 1 from by omega) hsk hns]
          simp only [List.length_cons]
          congr 1 <;> first | rfl | omega | simp

private def tieP (b : Option (Int × Int)) (r : Row) : Bool :=
  b == some (rowKey r)

private theorem fold_emit_needzero {s : Cursor} (q : List Row)
    (hsk : ¬ s.skip > 0) (hneed : s.need = 0) (hns : ¬ s.stopped) :
    q.foldl emit s =
      { s with
        output := s.output ++ q.takeWhile (tieP s.boundary),
        stopped := decide (q.takeWhile (tieP s.boundary) ≠ q) } := by
  cases q with
  | nil =>
      have hs : s.stopped = false := by simpa using hns
      rw [List.foldl_nil, List.takeWhile_nil, List.append_nil]
      have hd : decide ([] ≠ ([] : List Row)) = false := by simp
      rw [hd, ← hs]
  | cons r t =>
      by_cases hr : tieP s.boundary r
      · -- the row ties with the boundary: it is appended, demand stays 0
        rw [List.foldl_cons, emit_need_zero_tie hns hsk hneed hr,
          fold_emit_needzero (s := ({ s with output := s.output ++ [r] } : Cursor))
            t hsk hneed hns]
        have htw : (r :: t).takeWhile (tieP s.boundary) =
            r :: t.takeWhile (tieP s.boundary) := by
          rw [List.takeWhile_cons, if_pos hr]
        rw [htw]
        congr 1 <;> first | rfl | omega | simp | (simp; omega)
      · -- the row passes the boundary: the cursor stops here and now
        have htw : (r :: t).takeWhile (tieP s.boundary) = [] := by
          rw [List.takeWhile_cons, if_neg hr]
        rw [List.foldl_cons,
          emit_need_zero_stop hns hsk hneed (by simpa [tieP] using hr)]
        rw [fold_emit_stopped (by simp : ({ s with stopped := true } : Cursor).stopped)]
        rw [htw, List.append_nil]
        have hd : decide ([] ≠ (r :: t : List Row)) = true := by simp
        rw [hd]



private theorem takeWhile_false {l : List Row} (p : Row → Bool)
    (h : ∀ x ∈ l, p x = false) : l.takeWhile p = [] := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      rw [List.takeWhile_cons, if_neg (by simpa using h x (by simp))]

private theorem pageSel_nil (l : List Row) : pageSel l 0 = [] := by
  unfold pageSel
  show ([] : List Row) ++
      l.takeWhile (fun row => (none : Option KeyPair) == some (rowKey row)) = _
  rw [List.nil_append]
  exact takeWhile_false _ (fun x _ => rfl)

private theorem pageSel_decomp (z : List Row) (k : Nat) :
    pageSel z k = z.take k ++ (z.drop k).takeWhile
      (tieP ((z.take k).getLast?.map rowKey)) := by
  unfold pageSel
  rfl

private theorem pageSel_nil' (n : Nat) : pageSel ([] : List Row) n = [] := by
  unfold pageSel
  cases n with
  | zero => rfl
  | succ m => rfl

private theorem fold_emit_page_output (after : List Row) (off k : Nat) :
    (after.foldl emit (e₀ off k)).output = pageSel (after.drop off) k := by
  cases k with
  | zero =>
      rw [fold_emit_stopped (show (e₀ off 0).stopped from rfl)]
      show ([] : List Row) = pageSel (after.drop off) 0
      rw [pageSel_nil]
  | succ kk =>
      have hns : ¬ (e₀ off (kk + 1)).stopped := by
        intro h
        simpa [e₀] using h
      let z : List Row := after.drop off
      let x : List Row := after.take off
      have hsplit : after = x ++ z := (List.take_append_drop off after).symm
      have hdrop : (x ++ z).drop off = z := by
        show (after.take off ++ after.drop off).drop off = after.drop off
        rw [List.take_append_drop]
      have hxl : x.length ≤ off := by
        show (after.take off).length ≤ off
        exact List.length_take_le off after
      have hx1 : x.foldl emit (e₀ off (kk + 1)) =
          { e₀ off (kk + 1) with skip := off - x.length } :=
        fold_emit_skipphase (s := e₀ off (kk + 1)) x (by simpa [e₀] using hxl) hns
      rcases Nat.eq_or_lt_of_le hxl with hxe | hxl'
      · -- the skip phase consumed exactly the offset
        rw [hsplit, List.foldl_append, hx1, hdrop, hxe, Nat.sub_self]
        let s1 : Cursor := { e₀ off (kk + 1) with skip := 0 }
        have hsk1 : ¬ s1.skip > 0 := by
          show ¬ (0 : Nat) > 0
          omega
        have hns1 : ¬ s1.stopped := hns
        have hneed1 : s1.need = kk + 1 := rfl
        have hout1 : s1.output = [] := rfl
        have hcase : z.length ≤ kk + 1 ∨ kk + 1 < z.length := by omega
        rcases hcase with hzn | hzn
        · -- all remaining rows are consumed by the demand phase
          rcases Nat.eq_zero_or_pos (z.length) with hz0 | hzp
          · have hze : z = [] := List.length_eq_zero_iff.mp hz0
            rw [hze, List.foldl_nil, hout1, pageSel_nil']
          · have hne : z ≠ [] := by
              intro h
              rw [h, List.length_nil] at hzp
              omega
            rw [fold_emit_needtake (s := s1) z hne
              (by rw [hneed1]; exact hzn) hsk1 hns1, hout1, List.nil_append]
            unfold pageSel
            show (z : List Row) =
              z.take (kk + 1) ++
                (z.drop (kk + 1)).takeWhile
                  (fun row => (z.take (kk + 1)).getLast?.map rowKey == some (rowKey row))
            rw [List.take_of_length_le hzn, List.drop_eq_nil_of_le hzn]
            simp
        · -- the demand phase ends inside `z`; the rest is the tie phase
          have hmne : (z.take (kk + 1) : List Row) ≠ [] := by
            intro h
            have := congrArg List.length h
            rw [List.length_take, List.length_nil] at this
            omega
          have hmL : (z.take (kk + 1) : List Row).length = kk + 1 := by
            rw [List.length_take]
            omega
          rw [show (z : List Row) = z.take (kk + 1) ++ z.drop (kk + 1) from
            (List.take_append_drop (kk + 1) z).symm, List.foldl_append]
          rw [fold_emit_needtake (s := s1) (z.take (kk + 1)) hmne
            (by rw [hneed1, hmL]; omega) hsk1 hns1, hout1, List.nil_append]
          let s2 : Cursor :=
            { s1 with
              need := (kk + 1) - (z.take (kk + 1)).length,
              output := z.take (kk + 1),
              boundary := (z.take (kk + 1)).getLast?.map rowKey }
          have hnd2 : s2.need = 0 := by
            show (kk + 1) - (z.take (kk + 1)).length = 0
            rw [hmL]
            omega
          rw [fold_emit_needzero
            (s := { s1 with
              need := s1.need - (z.take (kk + 1)).length,
              output := z.take (kk + 1),
              boundary := (z.take (kk + 1)).getLast?.map rowKey })
            (z.drop (kk + 1)) hsk1 hnd2 hns1]
          have h1 : (z.take (kk + 1) ++ z.drop (kk + 1)).take (kk + 1)
              = z.take (kk + 1) := by
            have h1' : (z.take (kk + 1) ++ z.drop (kk + 1)).take
                (z.take (kk + 1)).length = z.take (kk + 1) :=
              List.take_append_length
            rw [hmL] at h1'
            exact h1'
          have h2 : (z.take (kk + 1) ++ z.drop (kk + 1)).drop (kk + 1)
              = z.drop (kk + 1) := by
            have h2' : (z.take (kk + 1) ++ z.drop (kk + 1)).drop
                (z.take (kk + 1)).length = z.drop (kk + 1) :=
              List.drop_append_length
            rw [hmL] at h2'
            exact h2'
          rw [pageSel_decomp, h1, h2]
      · -- the input ended before the offset: nothing is skipped away
        have h2 : (List.take off after).length < off := hxl'
        have hall : after.length ≤ off := by
          rcases Nat.lt_or_ge (after.length) off with h | h
          · exact Nat.le_of_lt h
          · exfalso
            rw [List.length_take, Nat.min_eq_left h] at h2
            exact Nat.lt_irrefl off h2
        have hzl : z = [] := by
          show after.drop off = []
          exact List.drop_eq_nil_of_le hall
        rw [fold_emit_skipphase (s := e₀ off (kk + 1)) after
          (by simpa [e₀] using hall) hns]
        show ([] : List Row) = pageSel z (kk + 1)
        rw [hzl, pageSel_nil']


-- ─────────────────── the SQL `fetch` page equals `pageSel` ─────────────────

private theorem KeyLeP_total (a b : KeyPair) : KeyLeP a b ∨ KeyLeP b a := by
  unfold KeyLeP
  omega

private theorem KeyLeP_trans {a b c : KeyPair} (h1 : KeyLeP a b) (h2 : KeyLeP b c) :
    KeyLeP a c := by
  unfold KeyLeP at *
  rcases h1 with h | ⟨e, h⟩ <;> rcases h2 with h' | ⟨e', h'⟩ <;>
    (first | exact Or.inl (by omega) | exact Or.inr ⟨by omega, by omega⟩)

private theorem pLt_of_KeyLeP_pLt {a b c : KeyPair} (h1 : KeyLeP a b) (h2 : pLt b c) :
    pLt a c := by
  unfold KeyLeP pLt at *
  rcases h1 with h | ⟨e, h⟩ <;> rcases h2 with h' | ⟨e', h'⟩ <;>
    (first | exact Or.inl (by omega) | exact Or.inr ⟨by omega, by omega⟩)

private theorem pLt_of_KeyLeP_not {a b : KeyPair} (h1 : KeyLeP a b) (h2 : ¬ KeyLeP b a) :
    pLt a b := by
  unfold KeyLeP pLt at *
  rcases h1 with h | ⟨e, h⟩
  · exact Or.inl h
  · refine Or.inr ⟨e, ?_⟩
    omega

private theorem sortedRows_take {l : List Row} (h : SortedRows l) :
    ∀ n, SortedRows (l.take n) := by
  induction l with
  | nil => intro n; cases n <;> exact List.Pairwise.nil
  | cons x xs ih =>
      intro n
      cases n with
      | zero => exact List.Pairwise.nil
      | succ n =>
          rw [List.take_succ_cons]
          refine List.Pairwise.cons ?_ (ih (List.pairwise_cons.mp h).2 n)
          intro y hy
          exact (List.pairwise_cons.mp h).1 y (List.mem_of_mem_take hy)

private theorem nodup_take {l : List Row} (h : l.Nodup) : ∀ n, (l.take n).Nodup := by
  induction l with
  | nil => intro n; cases n <;> simp [List.take]
  | cons x xs ih =>
      intro n
      cases n with
      | zero => simp [List.take]
      | succ n =>
          rw [List.take_succ_cons]
          obtain ⟨h1, h2⟩ := List.nodup_cons.mp h
          exact List.nodup_cons.mpr
            ⟨by intro hy; exact h1 (List.mem_of_mem_take hy), ih h2 n⟩

private theorem nodup_filter {l : List Row} (h : l.Nodup) (p : Row → Bool) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      obtain ⟨h1, h2⟩ := List.nodup_cons.mp h
      by_cases hx : p x = true
      · rw [List.filter_cons_of_pos hx]
        exact List.nodup_cons.mpr
          ⟨by intro hy; exact h1 ((List.mem_filter.mp hy).1), ih h2⟩
      · rw [List.filter_cons_of_neg (by simp [hx])]
        exact ih h2

private theorem mem_erase_of_mem_ne {l : List Row} {a x : Row} (hx : x ∈ l)
    (hne : x ≠ a) : x ∈ l.erase a := by
  cases l with
  | nil => exact absurd hx (by simp)
  | cons y t =>
      rcases List.mem_cons.mp hx with hy | ht
      · subst hy
        rw [List.erase_cons, if_neg (by simpa [beq_iff_eq] using hne)]
        exact List.mem_cons_self
      · rw [List.erase_cons]
        by_cases hya : (y == a) = true
        · rw [if_pos hya]
          exact ht
        · rw [if_neg hya]
          exact List.mem_cons_of_mem y (mem_erase_of_mem_ne ht hne)

private theorem length_le_of_mem {l : List Row} (hn : l.Nodup) :
    ∀ (l' : List Row), (∀ x ∈ l, x ∈ l') → l.length ≤ l'.length := by
  induction l with
  | nil => intro l' _; simp
  | cons a t ih =>
      intro l' h
      obtain ⟨h1, h2⟩ := List.nodup_cons.mp hn
      have ha : a ∈ l' := h a List.mem_cons_self
      have h2' : ∀ x ∈ t, x ∈ l'.erase a := by
        intro x hx
        exact mem_erase_of_mem_ne (h x (List.mem_cons_of_mem a hx))
          (by intro he; exact h1 (he ▸ hx))
      have hle := ih h2 (l'.erase a) h2'
      have hER : (l'.erase a).length + 1 ≤ l'.length := by
        have hlen : (l'.erase a).length = l'.length - 1 := by
          rw [List.length_erase, if_pos ha]
        rw [hlen]
        cases l' with
        | nil => exact absurd ha (by simp)
        | cons y t => rw [List.length_cons]; omega
      show t.length + 1 ≤ l'.length
      exact Nat.le_trans (Nat.succ_le_succ hle) hER

/-- In a sorted list, every element is key-below the last one. -/
private theorem keyLeP_last_of_mem {l : List Row} (h : SortedRows l) {c : Row}
    (hl : ∃ w, l = w ++ [c]) {x : Row} (hx : x ∈ l) : KeyLeP (rowKey x) (rowKey c) := by
  obtain ⟨w, rfl⟩ := hl
  rcases List.mem_append.mp hx with hxw | hxc
  · exact keyOrd_sorted h w [c] rfl x hxw c (List.mem_singleton.mpr rfl)
  · rw [List.mem_singleton] at hxc
    subst hxc
    exact KeyLeP_refl _

/-- The `FETCH FIRST k ROWS WITH TIES` rank predicate agrees, row by row,
    with the key comparison against the last taken row. -/
private theorem fetch_rank_iff {z : List Row} (hz : SortedRows z) (hn : z.Nodup)
    {k : Nat} (hk : 0 < k) {b : KeyPair} (hb : (z.take k).getLast?.map rowKey = some b)
    {row : Row} (hrow : row ∈ z) :
    decide ((z.filter (fun other => kLtB other row)).length < k) =
      decide (KeyLeP (rowKey row) b) := by
  have hzk : z.take k ≠ [] := by
    intro h
    rw [h] at hb
    exact absurd hb (by simp)
  obtain ⟨wc, c, hwc⟩ : ∃ w cc, z.take k = w ++ [cc] := List.exists_append_last hzk
  rw [hwc] at hb
  have hb2 : ((wc ++ [c]).getLast?).map rowKey = some (rowKey c) := by
    rw [getLast?_snoc]
    rfl
  have : rowKey c = b := Option.some.inj (hb2.symm.trans hb)
  subst this
  have hz' : SortedRows (wc ++ [c]) := by
    rw [← hwc]
    exact sortedRows_take hz k
  have hc : c ∈ z.take k := by
    rw [hwc]
    exact (by simp)
  have hmub : ∀ x ∈ z.take k, KeyLeP (rowKey x) (rowKey c) := by
    intro x hx
    have hx' : x ∈ wc ++ [c] := by rw [← hwc]; exact hx
    exact keyLeP_last_of_mem hz' ⟨wc, rfl⟩ hx'
  have hsplitord : ∀ y ∈ z.take k, ∀ z2 ∈ z.drop k,
      KeyLeP (rowKey y) (rowKey z2) :=
    keyOrd_sorted hz (z.take k) (z.drop k) (List.take_append_drop k z).symm
  refine decide_eq_decide.mpr ⟨?_, ?_⟩
  · -- rank below `k`: the row cannot key-exceed the last taken row
    intro hlt
    by_cases hge : KeyLeP (rowKey row) (rowKey c)
    · exact hge
    · have hcr : KeyLeP (rowKey c) (rowKey row) :=
        (KeyLeP_total (rowKey row) (rowKey c)).elim (fun h => absurd h hge) id
      have pcr : pLt (rowKey c) (rowKey row) := pLt_of_KeyLeP_not hcr hge
      have hsub : ∀ x ∈ z.take k,
          x ∈ z.filter (fun other => kLtB other row) := by
        intro x hx
        rw [List.mem_filter]
        exact ⟨List.mem_of_mem_take hx,
          (pLt_keyLtB x row).mpr (pLt_of_KeyLeP_pLt (hmub x hx) pcr)⟩
      rcases Nat.lt_or_ge k z.length with hkl | hkl
      · have hlen :=
          length_le_of_mem (nodup_take hn k)
            (z.filter (fun other => kLtB other row)) hsub
        rw [List.length_take, Nat.min_eq_left (Nat.le_of_lt hkl)] at hlen
        omega
      · have hrowt : row ∈ z.take k := by
          rw [List.take_of_length_le hkl]
          exact hrow
        exact absurd (hmub row hrowt) hge
  · -- the row is key-below the boundary: all counted rows live before it
    intro hle
    have hsub : ∀ x ∈ z.filter (fun other => kLtB other row),
        x ∈ (z.take k).erase c := by
      intro x hx
      rw [List.mem_filter] at hx
      obtain ⟨hzx, hkx⟩ := hx
      have pxc : pLt (rowKey x) (rowKey c) :=
        pLt_of_pLt_KeyLeP ((pLt_keyLtB x row).mp hkx) hle
      have hnx : ¬ KeyLeP (rowKey c) (rowKey x) := not_KeyLeP_of_pLt pxc
      have htk : x ∈ z.take k := by
        rcases List.mem_append.mp ((List.take_append_drop k z).symm ▸ hzx) with h1 | h2
        · exact h1
        · exact absurd (hsplitord c hc x h2) hnx
      exact mem_erase_of_mem_ne htk (by
        intro he; subst he
        exact hnx (KeyLeP_refl _))
    have hlen :=
      length_le_of_mem (nodup_filter hn _) ((z.take k).erase c) hsub
    have hE : ((z.take k).erase c).length = (z.take k).length - 1 := by
      rw [List.length_erase, if_pos hc]
    have hmin : (z.take k).length ≤ k := by
      rw [List.length_take]
      exact Nat.min_le_left _ _
    rw [hE] at hlen
    omega

/-- `takeWhile` over an append where every element of the prefix passes. -/
private theorem takeWhile_append_all {p : Row → Bool} {u : List Row}
    (hu : ∀ x ∈ u, p x = true) (v : List Row) :
    (u ++ v).takeWhile p = u ++ v.takeWhile p := by
  induction u with
  | nil => rfl
  | cons a t ih =>
      rw [List.cons_append, List.takeWhile_cons,
        if_pos (hu a List.mem_cons_self)]
      rw [ih (fun x hx => hu x (List.mem_cons_of_mem a hx))]
      rfl

/-- The page selector and the SQL rank-based fetch agree on sorted input. -/
private theorem pageSel_eq_fetch {z : List Row} (hz : SortedRows z) (hn : z.Nodup)
    (k : Nat) :
    pageSel z k
      = z.filter fun row => decide ((z.filter (fun other => kLtB other row)).length < k) := by
  cases k with
  | zero =>
      rw [pageSel_nil]
      symm
      rw [List.filter_eq_nil_iff]
      intro r _ hh
      exact Nat.not_lt_zero _ (of_decide_eq_true hh)
  | succ kk =>
      rcases Nat.eq_zero_or_pos z.length with hz0 | hzp
      · have hze : z = [] := List.length_eq_zero_iff.mp hz0
        rw [hze, pageSel_nil', List.filter_nil]
      · obtain ⟨wc, c, hwc⟩ : ∃ w cc, z.take (kk + 1) = w ++ [cc] := by
          apply List.exists_append_last
          intro h
          have h2 := congrArg List.length h
          rw [List.length_take, List.length_nil] at h2
          omega
        have hb : ((z.take (kk + 1)).getLast?).map rowKey = some (rowKey c) := by
          rw [hwc, getLast?_snoc]
          simp
        have hz' : SortedRows (wc ++ [c]) := by
          rw [← hwc]
          exact sortedRows_take hz (kk + 1)
        have hc : c ∈ z.take (kk + 1) := by
          rw [hwc]
          exact (by simp)
        have hmub : ∀ x ∈ z.take (kk + 1), KeyLeP (rowKey x) (rowKey c) := by
          intro x hx
          have hx' : x ∈ wc ++ [c] := by rw [← hwc]; exact hx
          exact keyLeP_last_of_mem hz' ⟨wc, rfl⟩ hx'
        have hsplitord : ∀ y ∈ z.take (kk + 1), ∀ z2 ∈ z.drop (kk + 1),
            KeyLeP (rowKey y) (rowKey z2) :=
          keyOrd_sorted hz (z.take (kk + 1)) (z.drop (kk + 1))
            (List.take_append_drop (kk + 1) z).symm
        -- rank predicate → comparison against the boundary key
        have h1 : z.filter (fun row =>
            decide ((z.filter (fun other => kLtB other row)).length < kk + 1))
            = z.filter (fun row => decide (KeyLeP (rowKey row) (rowKey c))) :=
          filter_congr_on
            (p := fun row => decide ((z.filter (fun other => kLtB other row)).length < kk + 1))
            (q := fun row => decide (KeyLeP (rowKey row) (rowKey c)))
            (fun row hrow => fetch_rank_iff hz hn (by omega) hb hrow)
        rw [h1,
          filter_congr_on (p := fun row => decide (KeyLeP (rowKey row) (rowKey c)))
            (q := fun row => decide (keyLe row (rowKey c)))
            (fun row _ => decide_eq_decide.mpr (keyLe_iff_KeyLeP row (rowKey c)).symm),
          filter_eq_takeWhile_keyLe (rowKey c) hz]
        have hum : ∀ x ∈ wc ++ [c], decide (keyLe x (rowKey c)) = true := by
          intro x hx
          refine decide_eq_true
            ((keyLe_iff_KeyLeP x (rowKey c)).mpr ?_)
          have : x ∈ z.take (kk + 1) := by rw [hwc]; exact hx
          exact hmub x this
        have hzz : z = (wc ++ [c]) ++ z.drop (kk + 1) := by
          rw [← hwc, List.take_append_drop]
        have htw : (z.drop (kk + 1)).takeWhile (tieP (some (rowKey c)))
            = (z.drop (kk + 1)).takeWhile (fun row => decide (keyLe row (rowKey c))) := by
          refine takeWhile_congr_on (p := tieP (some (rowKey c)))
            (q := fun row => decide (keyLe row (rowKey c))) (fun x hx => ?_)
          have hcr : KeyLeP (rowKey c) (rowKey x) := hsplitord c hc x hx
          show ((some (rowKey c) : Option KeyPair) == some (rowKey x))
            = decide (keyLe x (rowKey c))
          by_cases h : keyLe x (rowKey c)
          · rw [decide_eq_true h, beq_iff_eq]
            exact congrArg some
              (keyEq_of_KeyLeP_both ((keyLe_iff_KeyLeP x (rowKey c)).mp h) hcr).symm
          · rw [decide_eq_false h, beq_eq_false_iff_ne]
            intro heq
            have ecx : rowKey c = rowKey x := Option.some.inj heq
            exact h ((keyLe_iff_KeyLeP x (rowKey c)).mpr
              (by rw [ecx]; exact KeyLeP_refl _))
        have htw2 : z.takeWhile (fun row => decide (keyLe row (rowKey c)))
            = wc ++ [c] ++ (z.drop (kk + 1)).takeWhile
              (fun row => decide (keyLe row (rowKey c))) := by
          rw [← List.take_append_drop (kk + 1) z, hwc, takeWhile_append_all hum,
            ← hwc, List.take_append_drop]
        rw [pageSel_decomp z (kk + 1), hb, hwc, htw, htw2]

/-! ## Part 3: the block-level machine simulation -/

private def ValidBlocks (bs : List (List Row)) : Prop :=
  (∀ b ∈ bs, ∀ x ∈ b, ∀ y ∈ b, x.pkey = y.pkey) ∧
    List.Pairwise (fun A B : List Row => ∀ a ∈ A, ∀ b ∈ B, a.pkey < b.pkey) bs

private theorem beq_false_of_not (b : Bool) (h : ¬ b = true) : b = false := by
  cases b with
  | true => exact absurd rfl h
  | false => rfl

private theorem any_some {α : Type} (f : α → Bool) (x : α) : (some x).any f = f x := rfl

private theorem upd_stopped_self (s : Cursor) (h : s.stopped) : {s with stopped := true} = s := by
  cases s with
  | mk pp oo sk nd bd st pk =>
      exact (congrArg (fun x => Cursor.mk pp oo sk nd bd x pk) h).symm

private theorem upd_pending_self (s : Cursor) (h : s.pending = []) : {s with pending := []} = s := by
  cases s with
  | mk pp oo sk nd bd st pk =>
      exact (congrArg (fun a => Cursor.mk a oo sk nd bd st pk) h).symm

private theorem sublist_nil {l : List Row} : List.Sublist ([] : List Row) l := by
  induction l with
  | nil => exact List.Sublist.slnil
  | cons a t ih => exact List.Sublist.cons a ih

private theorem sublist_prefix (B l : List Row) : List.Sublist l (B ++ l) := by
  induction B with
  | nil => rw [List.nil_append]; exact List.Sublist.refl l
  | cons a t ih => rw [List.cons_append]; exact List.Sublist.cons a ih

private theorem max_le_max' (a b c d : Nat) (h1 : a ≤ c) (h2 : b ≤ d) : max a b ≤ max c d := by
  omega

private theorem feed_buffer {s : Cursor} {r : Row} (hst : ¬ s.stopped)
    (hfl : (s.pending.head?.any (fun h => h.pkey != r.pkey)) = false)
    (hc : (s.need == 0 && s.boundary.any (fun key => key.1 != r.pkey)) = false) :
    feed s r = {s with pending := r :: s.pending,
                                        peak := max s.peak (r :: s.pending).length} := by
  unfold feed
  simp [hst, hfl, hc]

private theorem feed_stop {s : Cursor} {r : Row} (hst : ¬ s.stopped)
    (hfl : (s.pending.head?.any (fun h => h.pkey != r.pkey)) = false)
    (hc : (s.need == 0 && s.boundary.any (fun key => key.1 != r.pkey)) = true) :
    feed s r = {s with stopped := true} := by
  unfold feed
  simp [hst, hfl, hc]

private theorem feed_eq_feed_flush {s : Cursor} {r : Row} (hst : ¬ s.stopped)
    (hfl : (s.pending.head?.any (fun h => h.pkey != r.pkey)) = true) :
    feed s r = feed (flush s) r := by
  have hfp : (flush s).pending = [] := flush_pending s
  have hfl' : ((flush s).pending.head?.any (fun h => h.pkey != r.pkey)) = false := by
    rw [hfp]
    rfl
  by_cases hfst : (flush s).stopped
  · have h1 : feed s r = flush s := by
      unfold feed
      simp [hst, hfl, hfst]
      rw [upd_stopped_self (flush s) hfst]
    have h2 : feed (flush s) r = flush s := if_pos hfst
    rw [h1, h2]
  · have hfn : ¬ (flush s).stopped := hfst
    by_cases hcb : ((flush s).need == 0 &&
        (flush s).boundary.any (fun key => key.1 != r.pkey))
    · have h1 : feed s r = {flush s with stopped := true} := by
        unfold feed
        simp [hst, hfl, hfst, hcb]
      have h2 : feed (flush s) r = {flush s with stopped := true} :=
        feed_stop hfn hfl' hcb
      rw [h1, h2]
    · have h1 : feed s r =
          { flush s with
            pending := r :: (flush s).pending,
            peak := max (flush s).peak (r :: (flush s).pending).length } := by
        unfold feed
        simp [hst, hfl, hfst, hcb]
      have h2 : feed (flush s) r =
          { flush s with
            pending := r :: (flush s).pending,
            peak := max (flush s).peak (r :: (flush s).pending).length } :=
        feed_buffer hfn hfl' (by simpa using hcb)
      rw [h1, h2]

private theorem flush_nil (s : Cursor) (h : s.pending = []) : flush s = s := by
  show (isort s.pending).foldl emit {s with pending := []} = s
  rw [h, upd_pending_self s h]
  rfl

private theorem flush_flush (s : Cursor) : flush (flush s) = flush s :=
  flush_nil (flush s) (flush_pending s)

private theorem fold_feed_stopped (B : List Row) {s : Cursor} (h : s.stopped) :
    B.foldl feed s = s := by
  induction B generalizing s with
  | nil => rfl
  | cons r t ih =>
      rw [List.foldl_cons]
      have hx : feed s r = s := if_pos h
      rw [hx, ih h]

private theorem feed_inv {s : Cursor} (hin : Inv s) (r : Row) : Inv (feed s r) := by
  have hfs : Inv (flush s) := by
    show Inv ((isort s.pending).foldl emit {s with pending := []})
    exact fold_emit_inv hin
  obtain ⟨h1, h2⟩ := hin
  constructor
  · intro hns hsk
    by_cases hst : s.stopped
    · rw [show feed s r = s from if_pos hst] at hns
      exact absurd hst hns
    · unfold feed at hns hsk ⊢
      rw [if_neg hst] at hns hsk ⊢
      by_cases hfl : (s.pending.head?.any (fun h => h.pkey != r.pkey))
      · rw [if_pos hfl] at hns hsk ⊢
        by_cases hn : ((flush s).stopped ||
            ((flush s).need == 0 && (flush s).boundary.any (fun key => key.1 != r.pkey)))
        · rw [if_pos hn] at hns hsk ⊢
          simp at hns
        · rw [if_neg hn] at hns hsk ⊢
          exact hfs.1 hns hsk
      · rw [if_neg hfl] at hns hsk ⊢
        by_cases hn : (s.stopped ||
            (s.need == 0 && s.boundary.any (fun key => key.1 != r.pkey)))
        · rw [if_pos hn] at hns hsk ⊢
          simp at hns
        · rw [if_neg hn] at hns hsk ⊢
          exact h1 hst hsk
  · intro hns hneed
    by_cases hst : s.stopped
    · rw [show feed s r = s from if_pos hst] at hns
      exact absurd hst hns
    · unfold feed at hns hneed ⊢
      rw [if_neg hst] at hns hneed ⊢
      by_cases hfl : (s.pending.head?.any (fun h => h.pkey != r.pkey))
      · rw [if_pos hfl] at hns hneed ⊢
        by_cases hn : ((flush s).stopped ||
            ((flush s).need == 0 && (flush s).boundary.any (fun key => key.1 != r.pkey)))
        · rw [if_pos hn] at hns hneed ⊢
          simp at hns
        · rw [if_neg hn] at hns hneed ⊢
          exact hfs.2 hns hneed
      · rw [if_neg hfl] at hns hneed ⊢
        by_cases hn : (s.stopped ||
            (s.need == 0 && s.boundary.any (fun key => key.1 != r.pkey)))
        · rw [if_pos hn] at hns hneed ⊢
          simp at hns
        · rw [if_neg hn] at hns hneed ⊢
          exact h2 hst hneed

private theorem fold_feed_inv {B : List Row} {s : Cursor} (hin : Inv s) :
    Inv (B.foldl feed s) := by
  induction B generalizing s with
  | nil => exact hin
  | cons r t ih => rw [List.foldl_cons]; exact ih (feed_inv hin r)

private theorem fold_feed_buffer {s : Cursor} (B : List Row) (p : Int)
    (hst : ¬ s.stopped) (hp : ∀ x ∈ s.pending, x.pkey = p) (hB : ∀ r ∈ B, r.pkey = p)
    (hc : (s.need == 0 && s.boundary.any (fun key => key.1 != p)) = false)
    (hpk : s.pending.length ≤ s.peak) :
    B.foldl feed s = {s with pending := B.reverse ++ s.pending,
                                        peak := max s.peak (B.length + s.pending.length)} := by
  induction B generalizing s with
  | nil =>
      rw [List.reverse_nil, List.nil_append, List.length_nil, Nat.zero_add,
        Nat.max_eq_left hpk]
      cases s with
      | mk pp oo sk nd bd st pk =>
          rfl
  | cons r t ih =>
      have hr : r.pkey = p := hB r List.mem_cons_self
      have hfl : (s.pending.head?.any (fun h => h.pkey != r.pkey)) = false := by
        rcases h : s.pending with _ | ⟨x, xs⟩
        · rfl
        · rw [List.head?_cons, any_some]
          have hpeq : x.pkey = p := hp x (by rw [h]; exact List.mem_cons_self)
          simp [hpeq, hr]
      have hcr : (s.need == 0 && s.boundary.any (fun key => key.1 != r.pkey)) = false := by
        have hfun : (fun (key : Int × Int) => key.1 != r.pkey) =
            (fun key => key.1 != p) := by
          funext (key : Int × Int)
          simp [hr]
        rw [hfun, ← hc]
      have hstep : feed s r =
          { s with
            pending := r :: s.pending,
            peak := max s.peak (r :: s.pending).length } := feed_buffer hst hfl hcr
      rw [List.foldl_cons]
      have htp : ∀ x ∈ t, x.pkey = p := by
        intro x hx
        exact hB x (List.mem_cons_of_mem r hx)
      have hns : ¬ (feed s r).stopped := by
        rw [hstep]
        simpa using hst
      have hp' : ∀ x ∈ (feed s r).pending, x.pkey = p := by
        intro x hx
        rw [hstep] at hx
        rcases List.mem_cons.mp hx with rfl | hx2
        · exact hr
        · exact hp x hx2
      have hc' : ((feed s r).need == 0 &&
          (feed s r).boundary.any (fun key => key.1 != p)) = false := by
        rw [hstep]
        simpa using hc
      have hpk' : (feed s r).pending.length ≤ (feed s r).peak := by
        rw [hstep]
        rw [List.length_cons]
        exact Nat.le_max_right _ _
      rw [ih (s := feed s r) hns hp' htp hc' hpk', hstep]
      show Cursor.mk (t.reverse ++ (r :: s.pending)) s.output s.skip s.need s.boundary s.stopped
          (max (max s.peak (r :: s.pending).length) (t.length + (r :: s.pending).length)) =
        Cursor.mk ((r :: t).reverse ++ s.pending) s.output s.skip s.need s.boundary s.stopped
          (max s.peak ((r :: t).length + s.pending.length))
      rw [Cursor.mk.injEq]
      refine ⟨?_, rfl, rfl, rfl, rfl, rfl, ?_⟩
      · rw [List.reverse_cons, List.append_assoc, List.cons_append, List.nil_append]
      · rw [List.length_cons, List.length_cons]
        omega

private theorem fold_feed_peak {B : List Row} {s : Cursor} :
    (B.foldl feed s).peak ≤ max s.peak (B.length + s.pending.length) := by
  have hstep1 : ∀ (s' : Cursor) (r : Row) (hst : ¬ s'.stopped)
      (hfl : (s'.pending.head?.any (fun h => h.pkey != r.pkey)) = false),
      (feed s' r).peak ≤ max s'.peak (s'.pending.length + 1) := by
    intro s' r hst hfl
    by_cases hcond : (s'.need == 0 && s'.boundary.any (fun key => key.1 != r.pkey))
    · rw [feed_stop hst hfl hcond]
      show s'.peak ≤ max s'.peak (s'.pending.length + 1)
      exact Nat.le_max_left _ _
    · rw [feed_buffer hst hfl (by simpa using hcond)]
      show max s'.peak (r :: s'.pending).length ≤ max s'.peak (s'.pending.length + 1)
      rw [List.length_cons]
      exact Nat.le_refl _
  have hstep : ∀ (s' : Cursor) (r : Row),
      (feed s' r).peak ≤ max s'.peak (s'.pending.length + 1) := by
    intro s' r
    by_cases hst : s'.stopped
    · rw [show feed s' r = s' from if_pos hst]
      exact Nat.le_max_left _ _
    · by_cases hfl : (s'.pending.head?.any (fun h => h.pkey != r.pkey))
      · by_cases hfst : (flush s').stopped
        · rw [feed_eq_feed_flush hst hfl,
              (show feed (flush s') r = flush s' from if_pos hfst), flush_peak]
          exact Nat.le_max_left _ _
        · have h := hstep1 (flush s') r hfst (by rw [flush_pending]; rfl)
          rw [← feed_eq_feed_flush hst hfl] at h
          refine Nat.le_trans h (max_le_max' _ _ _ _ ?_ ?_)
          · exact Nat.le_of_eq (flush_peak s')
          · rw [flush_pending, List.length_nil, Nat.zero_add]
            exact Nat.le_add_left _ _
      · exact hstep1 s' r hst (by simpa using hfl)
  have hpend1 : ∀ (s' : Cursor) (r : Row) (hst : ¬ s'.stopped)
      (hfl : (s'.pending.head?.any (fun h => h.pkey != r.pkey)) = false),
      (feed s' r).pending.length ≤ s'.pending.length + 1 := by
    intro s' r hst hfl
    by_cases hcond : (s'.need == 0 && s'.boundary.any (fun key => key.1 != r.pkey))
    · rw [feed_stop hst hfl hcond]
      show s'.pending.length ≤ s'.pending.length + 1
      omega
    · rw [feed_buffer hst hfl (by simpa using hcond)]
      show (r :: s'.pending).length ≤ s'.pending.length + 1
      rw [List.length_cons]
      exact Nat.le_refl _
  have hpend : ∀ (s' : Cursor) (r : Row),
      (feed s' r).pending.length ≤ s'.pending.length + 1 := by
    intro s' r
    by_cases hst : s'.stopped
    · rw [show feed s' r = s' from if_pos hst]
      omega
    · by_cases hfl : (s'.pending.head?.any (fun h => h.pkey != r.pkey))
      · by_cases hfst : (flush s').stopped
        · rw [feed_eq_feed_flush hst hfl,
              (show feed (flush s') r = flush s' from if_pos hfst),
              flush_pending, List.length_nil]
          omega
        · have h := hpend1 (flush s') r hfst (by rw [flush_pending]; rfl)
          rw [← feed_eq_feed_flush hst hfl] at h
          refine Nat.le_trans h ?_
          rw [flush_pending, List.length_nil]
          omega
      · exact hpend1 s' r hst (by simpa using hfl)
  induction B generalizing s with
  | nil =>
      rw [List.foldl_nil, List.length_nil, Nat.zero_add]
      exact Nat.le_max_left _ _
  | cons r t ih =>
      rw [List.foldl_cons]
      have h1 := ih (s := feed s r)
      have h2 := hstep s r
      have h3 := hpend s r
      rw [List.length_cons]
      refine Nat.le_trans h1 ?_
      omega

private theorem isort_reverse (B : List Row) (hnd : B.Nodup) : isort (B.reverse) = isort B :=
  sorted_perm_eq (isort_sorted _ (List.Perm.nodup ((List.reverse_perm B).symm) hnd))
    (isort_sorted _ hnd)
    (((isort_perm _).trans (List.reverse_perm B)).trans (isort_perm _).symm)

private theorem obs_flush_feed_block {s : Cursor} (B : List Row) (p : Int)
    (hin : Inv s) (hnd : B.Nodup) (hp : s.pending = []) (hB : ∀ r ∈ B, r.pkey = p) :
    Obs (flush (B.foldl feed s)) = Obs ((isort B).foldl emit s) := by
  obtain ⟨_, h2⟩ := hin
  by_cases hst : s.stopped
  · rw [fold_feed_stopped B hst, flush_nil s hp, fold_emit_stopped hst]
  · rcases B with _ | ⟨b, t⟩
    · rw [List.foldl_nil, flush_nil s hp]
      rfl
    · have hbp : b.pkey = p := hB b List.mem_cons_self
      have hfl : (s.pending.head?.any (fun h => h.pkey != b.pkey)) = false := by
        rw [hp]
        rfl
      by_cases hc : (s.need == 0 && s.boundary.any (fun key => key.1 != p))
      · -- the stop fires on the first row of the block, on both sides
        have hbuf : (b :: t).foldl feed s = {s with stopped := true} := by
          have hcr : (s.need == 0 && s.boundary.any (fun key => key.1 != b.pkey)) = true := by
            rw [hbp]
            exact hc
          have hf : feed s b = {s with stopped := true} := feed_stop hst hfl hcr
          have ht : t.foldl feed {s with stopped := true} = {s with stopped := true} :=
            fold_feed_stopped t (by simp)
          rw [List.foldl_cons, hf, ht]
        have hs1 : (isort (b :: t)).foldl emit s = {s with stopped := true} := by
          have hneed : s.need = 0 := by
            cases h0 : s.need == 0 with
            | false =>
                rw [h0] at hc
                simp at hc
            | true =>
                rw [beq_iff_eq] at h0
                exact h0
          have hskip : s.skip = 0 := h2 hst hneed
          have hlen : isort (b :: t) ≠ [] := by
            intro hnil
            have hl : (isort (b :: t)).length = (b :: t).length := (isort_perm (b :: t)).length_eq
            rw [hnil, List.length_nil] at hl
            simp at hl
          obtain ⟨c, cs, hcse⟩ := List.exists_cons_of_ne_nil hlen
          have hcmm : c ∈ isort (b :: t) := by
            rw [hcse]
            exact List.mem_cons_self
          have hcp : c.pkey = p := hB _ ((isort_perm (b :: t)).mem_iff.mp hcmm)
          have hbnc : (s.boundary == some (rowKey c)) = false := by
            by_cases hb2 : s.boundary == some (rowKey c)
            · have hbad : False := by
                rw [beq_iff_eq.mp hb2] at hc
                simp [rowKey, hcp] at hc
              exact False.elim hbad
            · exact beq_false_of_not _ hb2
          have hfirst : emit s c = {s with stopped := true} :=
            emit_need_zero_stop hst (by simp [hskip]) hneed hbnc
          rw [hcse, List.foldl_cons, hfirst, fold_emit_stopped (by simp)]
        calc Obs (flush ((b :: t).foldl feed s))
            = Obs (flush {s with stopped := true}) := congrArg (Obs ∘ flush) hbuf
          _ = Obs {s with stopped := true} :=
              congrArg Obs (flush_nil _ (by simpa using hp))
          _ = Obs ((isort (b :: t)).foldl emit s) := by rw [hs1]
      · -- the whole block is buffered, then flushed
        have hbuf : (b :: t).foldl feed s =
            { s with
              pending := (b :: t).reverse,
              peak := max s.peak (b :: t).length } := by
          have := fold_feed_buffer (b :: t) p hst (fun x hx => absurd hx (by simp [hp])) hB
            (beq_false_of_not _ hc) (by simp [hp])
          rwa [hp, List.append_nil, List.length_nil, Nat.add_zero] at this
        have hh : flush { s with
              pending := (b :: t).reverse,
              peak := max s.peak (b :: t).length }
            = (isort (b :: t).reverse).foldl emit { s with
                pending := [],
                peak := max s.peak (b :: t).length } := rfl
        calc Obs (flush ((b :: t).foldl feed s))
            = Obs ((isort (b :: t).reverse).foldl emit { s with
                pending := [],
                peak := max s.peak (b :: t).length }) := by rw [hbuf, hh]
          _ = Obs ((isort (b :: t)).foldl emit { s with
                pending := [],
                peak := max s.peak (b :: t).length }) := by rw [isort_reverse _ hnd]
          _ = Obs ((isort (b :: t)).foldl emit s) :=
              fold_emit_obs_congr (by cases s with
                | mk pp oo sk nd bd st pk => rfl)

private theorem foldl_feed_pending_sub (B : List Row) {s : Cursor} (x : Row)
    (hx : x ∈ (B.foldl feed s).pending) : x ∈ B ∨ x ∈ s.pending := by
  have hstep : ∀ (s' : Cursor) (r : Row), x ∈ (feed s' r).pending → x = r ∨ x ∈ s'.pending := by
    intro s' r hmem
    by_cases hst : s'.stopped
    · rw [show feed s' r = s' from if_pos hst] at hmem
      exact Or.inr hmem
    · by_cases hfl : (s'.pending.head?.any (fun h => h.pkey != r.pkey))
      · rw [feed_eq_feed_flush hst hfl] at hmem
        by_cases hfst : (flush s').stopped
        · rw [(show feed (flush s') r = flush s' from if_pos hfst), flush_pending] at hmem
          exact absurd hmem (by simp)
        · have hfl' : (flush s').pending.head?.any (fun h => h.pkey != r.pkey) = false := by
              rw [flush_pending]
              rfl
          by_cases hcb : (flush s').need == 0 &&
              (flush s').boundary.any (fun key => key.1 != r.pkey)
          · rw [feed_stop hfst hfl' hcb, flush_pending] at hmem
            exact absurd hmem (by simp)
          · rw [feed_buffer hfst hfl' (by simpa using hcb), flush_pending] at hmem
            rcases List.mem_cons.mp hmem with heq | hnil
            · exact Or.inl heq
            · exact absurd hnil (by simp)
      · unfold feed at hmem
        rw [if_neg hst, if_neg hfl] at hmem
        by_cases hcb : s'.stopped || (s'.need == 0 &&
            s'.boundary.any (fun key => key.1 != r.pkey))
        · rw [if_pos hcb] at hmem
          exact Or.inr hmem
        · rw [if_neg hcb] at hmem
          rcases List.mem_cons.mp hmem with heq | hmem2
          · exact Or.inl heq
          · exact Or.inr hmem2
  induction B generalizing s with
  | nil => exact Or.inr hx
  | cons r t ih =>
      rw [List.foldl_cons] at hx
      rcases ih (s := feed s r) hx with h | h
      · exact Or.inl (List.mem_cons_of_mem r h)
      · rcases hstep s r h with heq | hmem
        · exact Or.inl (by rw [heq]; exact List.mem_cons_self)
        · exact Or.inr hmem

private theorem foldl_feed_flush_final (l : List Row) (s : Cursor)
    (hhp : ∀ x ∈ s.pending, ∀ y ∈ l, x.pkey < y.pkey) :
    flush (l.foldl feed s) = flush (l.foldl feed (flush s)) := by
  by_cases hst : s.stopped
  · have hf : flush s = {s with pending := []} := by
      unfold flush
      rw [fold_emit_stopped (by simpa using hst)]
    have hfs : (flush s).stopped := by
      rw [hf]
      simpa using hst
    rw [fold_feed_stopped l hst, fold_feed_stopped l hfs]
    exact (flush_flush s).symm
  · rcases l with _ | ⟨y, ys⟩
    · rw [List.foldl_nil, List.foldl_nil]
      exact (flush_flush s).symm
    · have hstep : feed s y = feed (flush s) y := by
        by_cases hpnd : s.pending = []
        · rw [flush_nil s hpnd]
        · cases h : s.pending with
          | nil => exact (hpnd h).elim
          | cons x xs =>
              have hgt : x.pkey < y.pkey :=
                hhp x (by rw [h]; exact List.mem_cons_self) y List.mem_cons_self
              have hfl : (s.pending.head?.any (fun h => h.pkey != y.pkey)) = true := by
                rw [h, List.head?_cons, Option.any_some]
                exact (by simpa using (by omega : x.pkey ≠ y.pkey))
              exact feed_eq_feed_flush hst hfl
      rw [List.foldl_cons, List.foldl_cons, hstep]

private theorem foldl_feed_files (files : List (List Row)) (s : Cursor) :
    files.foldl (fun acc f => f.foldl feed acc) s = files.flatten.foldl feed s := by
  induction files generalizing s with
  | nil => rfl
  | cons f rest ih =>
      rw [List.foldl_cons, List.flatten_cons, List.foldl_append, ih]

private theorem nodup_of_sublist {l₁ l₂ : List Row} (h : List.Sublist l₁ l₂) (hn : l₂.Nodup) :
    l₁.Nodup := h.nodup hn

private theorem spanRun_fst_sublist (p : Int) (rs : List Row) :
    List.Sublist (spanRun p rs).1 rs := by
  induction rs with
  | nil => exact List.Sublist.slnil
  | cons x xs ih =>
      rw [spanRun.eq_2]
      split
      · rcases hspan : spanRun p xs with ⟨run, rest⟩
        rw [hspan] at ih
        exact List.Sublist.cons_cons x ih
      · exact List.Sublist.cons x (sublist_nil)

private theorem spanRun_snd_sublist (p : Int) (rs : List Row) :
    List.Sublist (spanRun p rs).2 rs := by
  induction rs with
  | nil => exact List.Sublist.slnil
  | cons x xs ih =>
      rw [spanRun.eq_2]
      split
      · rcases hspan : spanRun p xs with ⟨run, rest⟩
        rw [hspan] at ih
        exact ih.trans (List.Sublist.cons x (List.Sublist.refl xs))
      · exact List.Sublist.refl _

private theorem splitRuns_blocks_subset (l : List Row) :
    ∀ b ∈ splitRuns l, List.Sublist b l := by
  induction l using splitRuns.induct with
  | case1 =>
      intro b hb
      rw [splitRuns.eq_1] at hb
      exact absurd hb (by simp)
  | case2 r rest ih =>
      intro b hb
      rw [splitRuns.eq_2] at hb
      rcases List.mem_cons.mp hb with h | hb2
      · rw [h]
        exact List.Sublist.cons_cons r (spanRun_fst_sublist r.pkey rest)
      · exact (ih b hb2).trans
          ((spanRun_snd_sublist r.pkey rest).trans
            (List.Sublist.cons r (List.Sublist.refl rest)))

private theorem splitRuns_blocks_homo (l : List Row) :
    ∀ b ∈ splitRuns l, ∀ x ∈ b, ∀ y ∈ b, x.pkey = y.pkey := by
  induction l using splitRuns.induct with
  | case1 =>
      intro b hb
      rw [splitRuns.eq_1] at hb
      exact absurd hb (by simp)
  | case2 r rest ih =>
      intro b hb
      rw [splitRuns.eq_2] at hb
      rcases List.mem_cons.mp hb with h | hb2
      · subst h
        intro x hx y hy
        rcases List.mem_cons.mp hx with heqx | hx2
        · rcases List.mem_cons.mp hy with heqy | hy2
          · rw [heqx, heqy]
          · rw [heqx]
            exact (spanRun_prefix_all r.pkey rest y hy2).symm
        · rcases List.mem_cons.mp hy with heqy | hy2
          · rw [heqy]
            exact spanRun_prefix_all r.pkey rest x hx2
          · rw [spanRun_prefix_all r.pkey rest x hx2,
              spanRun_prefix_all r.pkey rest y hy2]
      · exact ih b hb2

private theorem flatten_mem_blocks (fs : List (List Row)) :
    ∀ y ∈ fs.flatten, ∃ b ∈ fs, y ∈ b := by
  induction fs with
  | nil => intro y hy; exact absurd hy (by simp)
  | cons f rest ih =>
      intro y hy
      rw [List.flatten_cons] at hy
      rcases List.mem_append.mp hy with hy | hy
      · exact ⟨f, List.mem_cons_self, hy⟩
      · obtain ⟨b, hb, hyb⟩ := ih y hy
        exact ⟨b, List.mem_cons_of_mem f hb, hyb⟩

private theorem validBlocks_splitRuns (l : List Row) :
    PrefixSorted l → ValidBlocks (splitRuns l) := by
  induction l using splitRuns.induct with
  | case1 =>
      intro _
      rw [splitRuns.eq_1]
      refine ⟨?_, List.Pairwise.nil⟩
      intro b hb
      exact absurd hb (by simp)
  | case2 r rest ih =>
      intro hps
      have hps' : PrefixSorted rest := (List.pairwise_cons.mp hps).2
      have hvps : PrefixSorted (spanRun r.pkey rest).2 :=
        pairwise_of_sublist
          ((spanRun_snd_sublist r.pkey rest).trans
            (List.Sublist.cons r (List.Sublist.refl rest)))
          hps
      have hih := ih hvps
      refine ⟨?_, ?_⟩
      · intro b hb
        rw [splitRuns.eq_2] at hb
        rcases List.mem_cons.mp hb with h | hb2
        · subst h
          exact splitRuns_blocks_homo (r :: rest) _
            (by rw [splitRuns.eq_2]; exact List.mem_cons_self)
        · exact hih.1 b hb2
      · rw [splitRuns.eq_2]
        refine List.Pairwise.cons ?_ hih.2
        intro b' hb' a ha y hy
        have hyv : y ∈ (spanRun r.pkey rest).2 :=
          (splitRuns_blocks_subset (spanRun r.pkey rest).2 b' hb').subset hy
        have hgt : r.pkey < y.pkey := spanRun_rest_gt r rest hps y hyv
        have har : a.pkey = r.pkey := by
          rcases List.mem_cons.mp ha with heq | ha2
          · subst heq
            rfl
          · exact spanRun_prefix_all r.pkey rest a ha2
        omega

private theorem fold_feed_blocks {bs : List (List Row)} (hvalid : ValidBlocks bs)
    (hnd : bs.flatten.Nodup) {s : Cursor} (hin : Inv s) (hp0 : s.pending = []) :
    Obs (flush (bs.foldl (fun acc f => f.foldl feed acc) s))
      = Obs ((bs.map isort).flatten.foldl emit s) ∧
    (flush (bs.foldl (fun acc f => f.foldl feed acc) s)).peak
      ≤ max s.peak (maxRunLen bs) := by
  induction bs generalizing s with
  | nil =>
      constructor
      · rw [List.foldl_nil, flush_nil s hp0]
        rfl
      · rw [List.foldl_nil, flush_peak]
        rw [show maxRunLen ([] : List (List Row)) = 0 from rfl]
        exact Nat.le_max_left _ _
  | cons B rest ih =>
      obtain ⟨hall, hpair⟩ := hvalid
      obtain ⟨hcross, hpair'⟩ := List.pairwise_cons.mp hpair
      have hB : ∀ x ∈ B, ∀ y ∈ B, x.pkey = y.pkey := hall B List.mem_cons_self
      have hrestv : ValidBlocks rest :=
        ⟨fun b hb => hall b (List.mem_cons_of_mem B hb), hpair'⟩
      have hflatt : (B :: rest).flatten = B ++ rest.flatten := rfl
      have hnd' : rest.flatten.Nodup :=
        nodup_of_sublist (sublist_prefix B rest.flatten) hnd
      rcases B with _ | ⟨b, t⟩
      · have hnoop : ∀ z : Cursor,
            (([] : List Row) :: rest).foldl (fun acc f => f.foldl feed acc) z
              = rest.foldl (fun acc f => f.foldl feed acc) z := by
          intro z
          rw [List.foldl_cons, List.foldl_nil]
        obtain ⟨ih₁, ih₂⟩ := ih hrestv hnd' hin hp0
        constructor
        · rw [hnoop s]
          rw [show (([] : List Row) :: rest).map isort = ([] : List Row) :: rest.map isort from
              rfl, List.flatten_cons, List.nil_append]
          exact ih₁
        · rw [hnoop s,
            show maxRunLen (([] : List Row) :: rest) = max ([] : List Row).length
              (maxRunLen rest) from rfl, List.length_nil]
          refine Nat.le_trans ih₂ (max_le_max' _ _ _ _ (Nat.le_refl _) (Nat.le_max_right _ _))
      · have hB0 : ∀ x ∈ (b :: t), x.pkey = b.pkey := by
          intro x hx
          exact hB x hx b List.mem_cons_self
        have hBnd : (b :: t).Nodup :=
          (sublist_append_left (b :: t) rest.flatten).nodup hnd
        have hobsB : Obs (flush ((b :: t).foldl feed s)) =
            Obs ((isort (b :: t)).foldl emit s) :=
          obs_flush_feed_block (b :: t) b.pkey hin hBnd hp0 hB0
        have hpeakB : (flush ((b :: t).foldl feed s)).peak ≤ max s.peak (b :: t).length := by
          rw [flush_peak]
          have h := fold_feed_peak (B := b :: t) (s := s)
          rwa [hp0, List.length_nil, Nat.add_zero] at h
        let s₁ := flush ((b :: t).foldl feed s)
        have hhp : ∀ x ∈ ((b :: t).foldl feed s).pending, ∀ y ∈ rest.flatten,
            x.pkey < y.pkey := by
          intro x hx y hxy
          rcases foldl_feed_pending_sub (b :: t) x hx with hm | hm
          · have hx' : x.pkey = b.pkey := hB0 x hm
            obtain ⟨b', hb', hy'⟩ := flatten_mem_blocks rest y hxy
            have := (hcross b' hb') x hm y hy'
            omega
          · exact absurd hm (by rw [hp0]; simp)
        have hs₁inv : Inv s₁ := fold_emit_inv (fold_feed_inv hin)
        obtain ⟨ih₁, ih₂⟩ := ih hrestv hnd' hs₁inv (flush_pending _)
        have hreal : ∀ z : Cursor,
            ((b :: t) :: rest).foldl (fun acc f => f.foldl feed acc) z
              = rest.flatten.foldl feed ((b :: t).foldl feed z) := by
          intro z
          rw [List.foldl_cons, foldl_feed_files rest]
        have hflush_rest : flush (rest.flatten.foldl feed ((b :: t).foldl feed s))
            = flush (rest.flatten.foldl feed s₁) :=
          foldl_feed_flush_final rest.flatten ((b :: t).foldl feed s) hhp
        constructor
        · calc Obs (flush (((b :: t) :: rest).foldl (fun acc f => f.foldl feed acc) s))
              = Obs (flush (rest.flatten.foldl feed ((b :: t).foldl feed s))) :=
                  congrArg (Obs ∘ flush) (hreal s)
            _ = Obs (flush (rest.flatten.foldl feed s₁)) := congrArg Obs hflush_rest
            _ = Obs (flush (rest.foldl (fun acc f => f.foldl feed acc) s₁)) :=
                  congrArg (Obs ∘ flush) (foldl_feed_files rest s₁).symm
            _ = Obs ((rest.map isort).flatten.foldl emit s₁) := ih₁
            _ = Obs ((rest.map isort).flatten.foldl emit ((isort (b :: t)).foldl emit s)) :=
                  fold_emit_obs_congr hobsB
            _ = Obs (((b :: t) :: rest).map isort |>.flatten.foldl emit s) := by
                  rw [List.map_cons, List.flatten_cons, List.foldl_append]
        · rw [hreal, hflush_rest, ← foldl_feed_files rest s₁,
              show maxRunLen ((b :: t) :: rest) = max (b :: t).length (maxRunLen rest) from rfl]
          refine Nat.le_trans ih₂
            (Nat.le_trans (max_le_max' _ _ _ _ hpeakB (Nat.le_refl _)) ?_)
          rw [Nat.max_assoc]
          omega


/-! ## Part 5: the main theorem -/


private theorem sortedRows_drop : ∀ {l : List Row}, SortedRows l → ∀ n, SortedRows (l.drop n) := by
  intro l h
  induction l with
  | nil =>
      intro n
      cases n with
      | zero => exact List.Pairwise.nil
      | succ m => exact List.Pairwise.nil
  | cons x xs ih =>
      intro n
      cases n with
      | zero => exact h
      | succ m => exact ih (List.pairwise_cons.mp h).2 m

private theorem nodup_drop : ∀ {l : List Row}, l.Nodup → ∀ n, (l.drop n).Nodup := by
  intro l h
  induction l with
  | nil =>
      intro n
      cases n with
      | zero => exact List.nodup_nil
      | succ m => exact List.nodup_nil
  | cons x xs ih =>
      intro n
      cases n with
      | zero => exact h
      | succ m => exact ih (List.nodup_cons.mp h).2 m

private theorem fold_blocks_stopped (files : List (List Row)) {s : Cursor} (h : s.stopped) :
    List.foldl (fun acc f => f.foldl feed acc) s files = s := by
  induction files with
  | nil => rfl
  | cons f rest ih =>
      have h1 : List.foldl (fun acc f => f.foldl feed acc) s (f :: rest)
          = List.foldl (fun acc f => f.foldl feed acc) (List.foldl feed s f) rest := rfl
      rw [h1, fold_feed_stopped f h]
      exact ih

theorem sound : Goal := by
  intro off k files hps hind
  show (run off k files).output = TinylambOpt.ResearchGoals.SqlFetchWithTies.pageSQL off k files.flatten ∧
    (run off k files).peak ≤ maxRunLen (splitRuns files.flatten) ∧
    (k = 0 → (run off k files).peak = 0)
  let s₀ := e₀ off k
  have hrun : run off k files = flush (List.foldl (fun acc f => f.foldl feed acc) s₀ files) :=
    rfl
  have hnd₀ : files.flatten.Nodup := nodup_of_ids hind
  have hv : ValidBlocks (splitRuns files.flatten) :=
    validBlocks_splitRuns files.flatten hps
  have hnd' : (splitRuns files.flatten).flatten.Nodup := by
    rw [splitRuns_concat]
    exact hnd₀
  obtain ⟨hsim, hpeak⟩ :=
    fold_feed_blocks hv hnd' (inv_e₀ off k) (show s₀.pending = [] from rfl)
  have hb1 : List.foldl (fun acc f => f.foldl feed acc) s₀ files
      = List.foldl (fun acc f => f.foldl feed acc) s₀ (splitRuns files.flatten) := by
    rw [foldl_feed_files, foldl_feed_files, splitRuns_concat]
  let S : List Row := ((splitRuns files.flatten).map isort).flatten
  have hS : runSort files = S := rfl
  have hsort : SortedRows S := by
    rw [← hS]
    exact runSort_sorted files hps hnd₀
  have hndS : S.Nodup := by
    rw [← hS]
    exact List.Perm.nodup (runSort_perm files).symm hnd₀
  have hz : s₀.peak = 0 := rfl
  constructor
  · rw [hrun]
    calc
      (flush (List.foldl (fun acc f => f.foldl feed acc) s₀ files)).output =
          (flush (List.foldl (fun acc f => f.foldl feed acc) s₀
            (splitRuns files.flatten))).output :=
        congrArg (fun x => (flush x).output) hb1
      _ = (S.foldl emit s₀).output := congrArg (fun o => o.1) hsim
      _ = pageSel (S.drop off) k := fold_emit_page_output S off k
      _ = (S.drop off).filter fun row =>
            decide (((S.drop off).filter fun other => kLtB other row).length < k) :=
        pageSel_eq_fetch (sortedRows_drop hsort off) (nodup_drop hndS off) k
      _ = TinylambOpt.ResearchGoals.SqlFetchWithTies.fetch kLtB off k S := rfl
      _ = TinylambOpt.ResearchGoals.SqlFetchWithTies.fetch kLtB off k
          (files.flatten.mergeSort kLeB) := by
        rw [← hS, runSort_eq_mergeSort files hps hind]
      _ = TinylambOpt.ResearchGoals.SqlFetchWithTies.pageSQL off k files.flatten :=
        (pageSQL_eq off k files.flatten).symm
  · constructor
    · rw [hrun, hb1]
      refine Nat.le_trans hpeak ?_
      rw [hz]
      show max 0 (maxRunLen (splitRuns files.flatten)) ≤ _
      omega
    · intro hk0
      have hst₀ : s₀.stopped = true := by
        subst hk0
        rfl
      rw [hrun, fold_blocks_stopped files hst₀,
        flush_nil s₀ (show s₀.pending = [] from rfl)]
      exact hz

end TinylambOpt.ResearchProofs.StreamingRunPage
