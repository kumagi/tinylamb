import TinylambOpt.ResearchGoals.Atlas.TextExecution

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_EditFilter_PigeonholeAnchor

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.EditFilter

abbrev Tagged := List (Nat × Nat)

/-! Each source symbol is tagged with the index of the piece it belongs to.
Inserted symbols are tagged with `n = ps.length`, which is a fresh tag. -/

def tagPiecesFrom (offset : Nat) : List Word → Tagged
  | [] => []
  | p :: ps => (p.map (fun c => (offset, c))) ++ tagPiecesFrom (offset + 1) ps

def tagPieces (ps : List Word) : Tagged := tagPiecesFrom 0 ps

theorem tagPiecesFrom_snd (offset : Nat) (ps : List Word) :
    (tagPiecesFrom offset ps).map Prod.snd = ps.flatten := by
  induction ps generalizing offset with
  | nil => simp [tagPiecesFrom]
  | cons p ps ih => simp [tagPiecesFrom, ih, Function.comp_def]

theorem tagPieces_snd (ps : List Word) : (tagPieces ps).map Prod.snd = ps.flatten :=
  tagPiecesFrom_snd 0 ps

/-! `Alive i p v` : the block of piece `i` is still present in the tagged word `v`
at some index `j`.  Since every element of the block carries tag `i`, the untagged
word then contains `p` as a contiguous subword. -/

def Alive (i : Nat) (p : Word) (v : Tagged) : Prop :=
  ∃ j, j + p.length ≤ v.length ∧
    ∀ m, m < p.length → v[j + m]? = (p[m]?).map (fun c => (i, c))

theorem alive_tagPiecesFrom (i offset : Nat) (ps : List Word) :
    Alive (offset + i) (ps[i]?.getD []) (tagPiecesFrom offset ps) := by
  induction ps generalizing i offset with
  | nil =>
      simp [Alive, tagPiecesFrom]
  | cons p ps ih =>
      cases i with
      | zero =>
          simp only [List.getElem?_cons_zero, Option.getD_some, Nat.add_zero]
          refine ⟨0, ?_, ?_⟩
          · simp [tagPiecesFrom]
          · intro m hm
            rw [Nat.zero_add]
            change (tagPiecesFrom offset (p :: ps))[m]? = (p[m]?).map (fun c => (offset, c))
            rw [show tagPiecesFrom offset (p :: ps) = p.map (fun c => (offset, c)) ++
                tagPiecesFrom (offset + 1) ps by rfl]
            rw [List.getElem?_append_left]
            · simp [List.getElem?_map]
            · simpa using hm
      | succ i =>
          simp only [List.getElem?_cons_succ]
          have hrec := ih i (offset + 1)
          rcases hrec with ⟨j, hjlen, hj⟩
          refine ⟨p.length + j, ?_, ?_⟩
          · simp [tagPiecesFrom]
            omega
          · intro m hm
            have hm' : m < (ps[i]?.getD []).length := by simpa using hm
            have h := hj m hm'
            change (tagPiecesFrom offset (p :: ps))[p.length + j + m]? =
              Option.map (fun c => (offset + (i + 1), c)) ((ps[i]?.getD [])[m]?)
            rw [show tagPiecesFrom offset (p :: ps) = p.map (fun c => (offset, c)) ++
                tagPiecesFrom (offset + 1) ps by rfl]
            rw [List.getElem?_append_right (by simp; omega)]
            rw [List.length_map]
            rw [show p.length + j + m - p.length = j + m by omega]
            simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using h

theorem alive_tagPieces (ps : List Word) (i : Nat) (hi : i < ps.length) :
    Alive i (ps[i]?.getD []) (tagPieces ps) := by
  simpa [tagPieces, Nat.zero_add] using alive_tagPiecesFrom i 0 ps

theorem mem_getD_of_lt (ps : List Word) (i : Nat) (hi : i < ps.length) :
    ps[i]?.getD [] ∈ ps := by
  have heq : ps[i]? = some ps[i] := List.getElem?_eq_getElem hi
  have hval : ps[i]?.getD [] = ps[i] := by simp [heq]
  rw [hval]
  exact List.getElem_mem hi

/-! A damage tag is recorded for every edit.  A drop or substitution damages the
piece the old symbol belonged to, and an insertion damages the piece whose symbol
immediately precedes the insertion point.  Any other piece keeps its block. -/

def gapTag (n : Nat) (l : Tagged) : Nat := (l.getLast?.map Prod.fst).getD n

inductive TEdits (n : Nat) (init : Tagged) : List Nat → Tagged → Prop where
  | refl : TEdits n init [] init
  | drop (l r : Tagged) (x : Nat × Nat) {D : List Nat}
      (h : TEdits n init D (l ++ x :: r)) : TEdits n init (x.1 :: D) (l ++ r)
  | add (l r : Tagged) (x : Nat × Nat) {D : List Nat}
      (h : TEdits n init D (l ++ r)) : TEdits n init (gapTag n l :: D) (l ++ x :: r)
  | subst (l r : Tagged) (x y : Nat × Nat) {D : List Nat}
      (h : TEdits n init D (l ++ x :: r)) : TEdits n init (x.1 :: D) (l ++ y :: r)

/-! Splitting a tagged word according to a split of its projection. -/

theorem split_tagged_cons {l r : Word} {x : Nat} {v : Tagged}
    (h : v.map Prod.snd = l ++ x :: r) :
    ∃ (lv : Tagged) (tag : Nat) (rv : Tagged),
      v = lv ++ (tag, x) :: rv ∧ lv.map Prod.snd = l ∧ rv.map Prod.snd = r := by
  induction l generalizing v with
  | nil =>
      cases v with
      | nil => simp at h
      | cons y v' =>
          rcases y with ⟨tag, c⟩
          simp only [List.map_cons, List.nil_append, List.cons.injEq] at h
          have hc : c = x := h.1
          subst x
          exact ⟨[], tag, v', rfl, rfl, h.2⟩
  | cons a l' ih =>
      cases v with
      | nil => simp at h
      | cons y v' =>
          rcases y with ⟨tag, c⟩
          simp only [List.map_cons, List.cons_append, List.cons.injEq] at h
          have hc : c = a := h.1
          subst a
          obtain ⟨lv, t, rv, hv, hl, hr⟩ := ih (v := v') h.2
          exact ⟨(tag, c) :: lv, t, rv, by simp [hv], by simp [hl], hr⟩

theorem split_tagged {l r : Word} {v : Tagged} (h : v.map Prod.snd = l ++ r) :
    ∃ (lv rv : Tagged), v = lv ++ rv ∧ lv.map Prod.snd = l ∧ rv.map Prod.snd = r := by
  induction l generalizing v with
  | nil =>
      exact ⟨[], v, by simp, by simp, by simpa using h⟩
  | cons a l' ih =>
      cases v with
      | nil => simp at h
      | cons y v' =>
          rcases y with ⟨tag, c⟩
          simp only [List.map_cons, List.cons_append, List.cons.injEq] at h
          have hc : c = a := h.1
          subst a
          obtain ⟨lv, rv, hv, hl, hr⟩ := ih (v := v') h.2
          exact ⟨(tag, c) :: lv, rv, by simp [hv], by simp [hl], hr⟩

theorem lift_edits {k : Nat} {a t : Word} (h : Edits k a t) (ps : List Word)
    (ha : a = ps.flatten) :
    ∃ (D : List Nat) (v : Tagged),
      D.length = k ∧ v.map Prod.snd = t ∧ TEdits ps.length (tagPieces ps) D v := by
  induction h generalizing ps with
  | refl s =>
      refine ⟨[], tagPieces ps, rfl, ?_, TEdits.refl⟩
      simpa [ha] using tagPieces_snd ps
  | drop l r x h ih =>
      rename_i k a
      obtain ⟨D, v, hD, hvmap, hv⟩ := ih ps ha
      obtain ⟨lv, tag, rv, hv', hl, hr⟩ := split_tagged_cons (l := l) (r := r) (x := x) hvmap
      subst v
      refine ⟨tag :: D, lv ++ rv, by simp [hD], ?_,
        TEdits.drop lv rv (tag, x) hv⟩
      · simp [hl, hr]
  | add l r x h ih =>
      rename_i k a
      obtain ⟨D, v, hD, hvmap, hv⟩ := ih ps ha
      obtain ⟨lv, rv, hv', hl, hr⟩ := split_tagged (l := l) (r := r) hvmap
      subst v
      refine ⟨gapTag ps.length lv :: D, lv ++ (ps.length, x) :: rv, by simp [hD], ?_,
        TEdits.add lv rv (ps.length, x) hv⟩
      simp [hl, hr]
  | subst l r x y h ih =>
      rename_i k a
      obtain ⟨D, v, hD, hvmap, hv⟩ := ih ps ha
      obtain ⟨lv, tag, rv, hv', hl, hr⟩ := split_tagged_cons (l := l) (r := r) (x := x) hvmap
      subst v
      refine ⟨tag :: D, lv ++ (tag, y) :: rv, by simp [hD], ?_,
        TEdits.subst lv rv (tag, x) (tag, y) hv⟩
      simp [hl, hr]

/-! Element access facts used by the preservation lemmas. -/

theorem getElem_self_split (l r : Tagged) (x : Nat × Nat) :
    (l ++ x :: r)[l.length]? = some x := by
  rw [List.getElem?_append_right (Nat.le_refl l.length)]
  simp

theorem getElem_erase_split (l r : Tagged) (x : Nat × Nat) (j : Nat) :
    (l ++ r)[j]? = if j < l.length then (l ++ x :: r)[j]?
      else (l ++ x :: r)[j + 1]? := by
  by_cases hj : j < l.length
  · rw [if_pos hj]
    rw [List.getElem?_append_left hj, List.getElem?_append_left hj]
  · rw [if_neg hj]
    have hle : l.length ≤ j := Nat.le_of_not_gt hj
    rw [List.getElem?_append_right hle]
    rw [List.getElem?_append_right (show l.length ≤ j + 1 by omega)]
    rw [show j + 1 - l.length = (j - l.length) + 1 by omega]
    rw [List.getElem?_cons_succ]

theorem getElem_subst_split (l r : Tagged) (x y : Nat × Nat) (j : Nat) (hj : j ≠ l.length) :
    (l ++ y :: r)[j]? = (l ++ x :: r)[j]? := by
  by_cases hlt : j < l.length
  · rw [List.getElem?_append_left hlt, List.getElem?_append_left hlt]
  · have hge : l.length < j := by omega
    rw [List.getElem?_append_right (Nat.le_of_lt hge),
      List.getElem?_append_right (Nat.le_of_lt hge)]
    rw [show j - l.length = (j - l.length - 1) + 1 by omega]
    rw [List.getElem?_cons_succ, List.getElem?_cons_succ]

theorem getElem_insert_split (l r : Tagged) (x : Nat × Nat) (j : Nat) (hj : l.length < j) :
    (l ++ x :: r)[j]? = r[j - l.length - 1]? := by
  rw [List.getElem?_append_right (Nat.le_of_lt hj)]
  rw [show j - l.length = (j - l.length - 1) + 1 by omega]
  rw [List.getElem?_cons_succ]
  congr 1

theorem getElem_append_split (l r : Tagged) (j : Nat) (hj : l.length ≤ j) :
    (l ++ r)[j]? = r[j - l.length]? := by
  rw [List.getElem?_append_right hj]

theorem getLast_of_getElem_last (l : Tagged) (z : Nat × Nat)
    (h : l[l.length - 1]? = some z) (hpos : 0 < l.length) : l.getLast? = some z := by
  rw [List.getLast?_eq_getElem?]
  exact h

/-! One edit destroys the block of at most its damage tag. -/

theorem alive_drop {i : Nat} {p : Word} (hp : p ≠ []) {l r : Tagged} {x : Nat × Nat}
    (hix : i ≠ x.1) (hA : Alive i p (l ++ x :: r)) : Alive i p (l ++ r) := by
  rcases hA with ⟨j, hjlen, hj⟩
  by_cases hleft : l.length < j
  · refine ⟨j - 1, ?_, ?_⟩
    · simp only [List.length_append, List.length_cons, List.length_nil, Nat.add_zero] at hjlen ⊢
      omega
    · intro m hm
      rw [getElem_erase_split]
      rw [if_neg]
      · have hidx : j - 1 + m + 1 = j + m := by omega
        rw [hidx]
        exact hj m hm
      · omega
  · by_cases hright : j + p.length ≤ l.length
    · refine ⟨j, ?_, ?_⟩
      · simp only [List.length_append, List.length_nil, Nat.add_zero]
        omega
      · intro m hm
        rw [getElem_erase_split]
        rw [if_pos]
        · exact hj m hm
        · omega
    · have hjle : j ≤ l.length := by omega
      have hPj : l.length - j < p.length := by omega
      have hmemlen : (p[l.length - j]?).isSome := by
        rw [List.getElem?_eq_getElem (by omega)]
        simp
      have hval := hj (l.length - j) hPj
      have hidx : j + (l.length - j) = l.length := by omega
      rw [hidx, getElem_self_split] at hval
      obtain ⟨c, hc⟩ := Option.isSome_iff_exists.mp hmemlen
      rw [hc] at hval
      simp at hval
      exact False.elim (hix ((congrArg Prod.fst hval).symm))

theorem alive_subst {i : Nat} {p : Word} (hp : p ≠ []) {l r : Tagged} {x y : Nat × Nat}
    (hix : i ≠ x.1) (hA : Alive i p (l ++ x :: r)) : Alive i p (l ++ y :: r) := by
  rcases hA with ⟨j, hjlen, hj⟩
  refine ⟨j, ?_, ?_⟩
  · simp only [List.length_append, List.length_cons, List.length_nil, Nat.add_zero] at hjlen ⊢
    omega
  · intro m hm
    rw [getElem_subst_split l r x y (j + m)]
    · exact hj m hm
    · intro heq
      have hjl : j ≤ l.length := by
        have : j ≤ j + m := Nat.le_add_right _ _
        omega
      have hjge : l.length ≤ j + p.length := by
        have hlen : (l ++ x :: r).length = l.length + 1 + r.length := by
          simp [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
        omega
      -- j <= l.length <= j+m < j+p.length, so the position is inside the block
      have hmpos : l.length - j < p.length := by omega
      have hmemlen : (p[l.length - j]?).isSome := by
        rw [List.getElem?_eq_getElem (by omega)]
        simp
      have hval := hj (l.length - j) hmpos
      have hidx : j + (l.length - j) = l.length := by omega
      rw [hidx, getElem_self_split] at hval
      obtain ⟨c, hc⟩ := Option.isSome_iff_exists.mp hmemlen
      rw [hc] at hval
      simp at hval
      exact hix ((congrArg Prod.fst hval).symm)

theorem alive_add {n i : Nat} {p : Word} (hi : i < n) {l r : Tagged} {x : Nat × Nat}
    (higap : i ≠ gapTag n l) (hA : Alive i p (l ++ r)) : Alive i p (l ++ x :: r) := by
  rcases hA with ⟨j, hjlen, hj⟩
  by_cases hleft : l.length ≤ j
  · refine ⟨j + 1, ?_, ?_⟩
    · simp only [List.length_append, List.length_cons, List.length_nil, Nat.add_zero] at hjlen ⊢
      omega
    · intro m hm
      have h := hj m hm
      rw [getElem_append_split l r (j + m) (by omega)] at h
      rw [getElem_insert_split]
      · have hsub : j + 1 + m - l.length - 1 = j + m - l.length := by omega
        rw [hsub]
        exact h
      · omega
  · by_cases hright : j + p.length ≤ l.length
    · refine ⟨j, ?_, ?_⟩
      · simp only [List.length_append, List.length_cons, List.length_nil, Nat.add_zero] at hjlen ⊢
        omega
      · intro m hm
        have h := hj m hm
        rw [List.getElem?_append_left (by omega)] at h
        rw [List.getElem?_append_left (by omega)]
        exact h
    · have hjlt : j < l.length := by omega
      have hPj : l.length - 1 - j < p.length := by omega
      have hmemlen1 : (p[l.length - 1 - j]?).isSome := by
        rw [List.getElem?_eq_getElem (by omega)]
        simp
      have hval1 := hj (l.length - 1 - j) hPj
      have hidx1 : j + (l.length - 1 - j) = l.length - 1 := by omega
      rw [hidx1, List.getElem?_append_left (by omega)] at hval1
      obtain ⟨c1, hc1⟩ := Option.isSome_iff_exists.mp hmemlen1
      rw [hc1] at hval1
      simp at hval1
      have hlast : l.getLast? = some (i, c1) := by
        apply getLast_of_getElem_last l (i, c1)
        · simpa using hval1
        · omega
      have hgt : gapTag n l = i := by
        simp [gapTag, hlast]
      exact False.elim (higap hgt.symm)

theorem alive_of_noDamage (ps : List Word) (hne : ∀ p ∈ ps, p ≠ [])
    (Dall : List Nat) (v : Tagged) (hv : TEdits ps.length (tagPieces ps) Dall v) :
    ∀ i, i < ps.length → i ∉ Dall → Alive i (ps[i]?.getD []) v := by
  induction hv with
  | refl =>
      intro i hi _
      exact alive_tagPieces ps i hi
  | drop l r x h ih =>
      rename_i D0
      intro i hi hiD
      have hiD0 : i ∉ D0 := by intro hmem; exact hiD (by simp [hmem])
      have hix : i ≠ x.1 := by intro he; subst he; exact hiD (by simp)
      have hp : (ps[i]?.getD []) ≠ [] := hne _ (mem_getD_of_lt ps i hi)
      exact alive_drop hp hix (ih i hi hiD0)
  | add l r x h ih =>
      rename_i D0
      intro i hi hiD
      have hiD0 : i ∉ D0 := by intro hmem; exact hiD (by simp [hmem])
      have higap : i ≠ gapTag ps.length l := by
        intro he; exact hiD (by simp [he])
      have hp : (ps[i]?.getD []) ≠ [] := hne _ (mem_getD_of_lt ps i hi)
      exact alive_add hi higap (ih i hi hiD0)
  | subst l r x y h ih =>
      rename_i D0
      intro i hi hiD
      have hiD0 : i ∉ D0 := by intro hmem; exact hiD (by simp [hmem])
      have hix : i ≠ x.1 := by intro he; subst he; exact hiD (by simp)
      have hp : (ps[i]?.getD []) ≠ [] := hne _ (mem_getD_of_lt ps i hi)
      exact alive_subst hp hix (ih i hi hiD0)

/-! A list of distinct naturals contained in `D` has length at most the length of `D`. -/

theorem nodup_length_le {l D : List Nat} (hnd : l.Nodup) (hmem : ∀ x ∈ l, x ∈ D) :
    l.length ≤ D.length := by
  induction l generalizing D with
  | nil => simp
  | cons x xs ih =>
      have hnd' := (List.nodup_cons.mp hnd).2
      have hxnot : x ∉ xs := (List.nodup_cons.mp hnd).1
      have hxD : x ∈ D := hmem x (by simp)
      have hmem' : ∀ y ∈ xs, y ∈ D.erase x := by
        intro y hy
        have hyD : y ∈ D := hmem y (by simp [hy])
        have hyne : y ≠ x := by intro he; subst y; exact hxnot hy
        exact (List.mem_erase_of_ne hyne).2 hyD
      have hrec := ih (D := D.erase x) hnd' hmem'
      have hlen : (D.erase x).length = D.length - 1 := List.length_erase_of_mem hxD
      have hDpos : 0 < D.length := by
        cases D with
        | nil => simp at hxD
        | cons y ys => simp
      have hsub : D.length - 1 + 1 = D.length :=
        Nat.sub_add_cancel (Nat.succ_le_of_lt hDpos)
      calc
        (x :: xs).length = xs.length + 1 := by simp
        _ ≤ (D.erase x).length + 1 := Nat.succ_le_succ hrec
        _ = (D.length - 1) + 1 := by rw [hlen]
        _ = D.length := hsub

theorem exists_index_not_mem {D : List Nat} {n : Nat} (h : D.length < n) :
    ∃ i, i < n ∧ i ∉ D := by
  by_cases hex : ∃ i, i < n ∧ i ∉ D
  · exact hex
  · have hall : ∀ i ∈ List.range n, i ∈ D := by
      intro i hi
      by_cases hmem : i ∈ D
      · exact hmem
      · exact False.elim (hex ⟨i, List.mem_range.mp hi, hmem⟩)
    have hle := nodup_length_le List.nodup_range hall
    have hle' : n ≤ D.length := by simpa using hle
    exfalso
    omega

/-! Extracting an occurrence from a surviving block. -/

theorem isPrefixOf_of_take {p a : Word} (h : p = a.take p.length) : p.isPrefixOf a = true := by
  have ht : p ++ a.drop p.length = a := by
    calc
      p ++ a.drop p.length = a.take p.length ++ a.drop p.length :=
        congrArg (fun q => q ++ a.drop p.length) h
      _ = a := List.take_append_drop p.length a
  have hp : List.IsPrefix p a := ⟨a.drop p.length, ht⟩
  simpa using hp

theorem occurs_of_alive {i : Nat} {p : Word} (hp : p ≠ []) {v : Tagged} (htv : v.map Prod.snd = t)
    (hA : Alive i p v) : occurs t p = true := by
  rcases hA with ⟨j, hjlen, hj⟩
  have htake : (t.drop j).take p.length = p := by
    apply List.ext_getElem?
    intro m
    rw [List.getElem?_take]
    by_cases hm : m < p.length
    · rw [if_pos hm]
      rw [List.getElem?_drop]
      have htvm : t[j + m]? = (v[j + m]?).map Prod.snd := by
        rw [← htv, List.getElem?_map]
      rw [hj m hm] at htvm
      simpa [Function.comp_def] using htvm
    · rw [if_neg hm]
      symm
      apply List.getElem?_eq_none
      omega
  have hjle : j ≤ t.length := by
    rw [← htv, List.length_map]
    omega
  have hmem : j ∈ (List.range (t.length + 1)).filter
      (fun k => p.isPrefixOf (t.drop k)) := by
    rw [List.mem_filter]
    constructor
    · exact List.mem_range.mpr (by omega)
    · exact isPrefixOf_of_take htake.symm
  unfold occurs positions
  have hne : (List.range (t.length + 1)).filter
      (fun k => p.isPrefixOf (t.drop k)) ≠ [] := List.ne_nil_of_mem hmem
  simp [hne]

theorem sound_EditFilter_PigeonholeAnchor : EditFilter.PigeonholeAnchor.Goal := by
  intro ps t k hk hne hed
  obtain ⟨D, v, hDlen, hvt, hv⟩ := lift_edits hed ps rfl
  obtain ⟨i, hi, hiD⟩ := exists_index_not_mem (D := D) (n := ps.length) (by omega)
  have hA := alive_of_noDamage ps hne D v hv i hi hiD
  refine ⟨ps[i]?.getD [], mem_getD_of_lt ps i hi, ?_⟩
  have hp : (ps[i]?.getD []) ≠ [] := hne _ (mem_getD_of_lt ps i hi)
  exact occurs_of_alive hp hvt hA

end TinylambOpt.ResearchProofs.Atlas.Goal_EditFilter_PigeonholeAnchor
