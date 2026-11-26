import TinylambOpt.ResearchGoals.Atlas.TextExecution

/-!
SQL string predicates can use encoded or summarized text when the modeled match,
boundaries, and collation are preserved.

Proved

- `EditFilter.PositionalAnchor` — `∀ (ps : List Word) t k, k < ps.length → (∀ p ∈ ps, p
  ≠ []) → Edits k ps.flatten t → ∃ i < ps.length, ∃ j ∈ positions t (ps[i]?.getD []),
  ((j:Int) - ((ps.take i).map List.length).sum).natAbs ≤ k`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_EditFilter_PositionalAnchor

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.EditFilter

private def start (ps : List Word) (i : Nat) : Nat := ((ps.take i).map List.length).sum

private def Occ (p u : Word) (j : Nat) : Prop := p <+: u.drop j

private theorem occ_mem_positions {p u : Word} {j : Nat} (hp : p ≠ []) (hj : Occ p u j) :
    j ∈ positions u p := by
  simp only [positions, List.mem_filter]
  constructor
  · simp only [List.mem_range]
    have hp_pos : 0 < p.length := by
      cases p with
      | nil => contradiction
      | cons a as => simp
    have hlen : p.length ≤ (u.drop j).length := hj.length_le
    have hlen' : p.length ≤ u.length - j := by simpa [List.length_drop] using hlen
    omega
  · simpa using (List.isPrefixOf_iff_prefix.mpr hj : p.isPrefixOf (u.drop j))

private theorem prefix_of_prefix_append {p a r : Word} (h : p <+: a ++ r)
    (hlen : p.length ≤ a.length) : p <+: a := by
  rcases h with ⟨suf, hsuf⟩
  rcases List.append_eq_append_iff.mp hsuf with hcase | hcase
  · rcases hcase with ⟨as, ha, _⟩
    exact ⟨as, ha.symm⟩
  · rcases hcase with ⟨bs, hb, _⟩
    have hbs : bs = [] := by
      apply List.eq_nil_of_length_eq_zero
      have hlenp : p.length = a.length + bs.length := by rw [hb, List.length_append]
      omega
    subst bs
    simp at hb
    subst p
    exact ⟨[], by simp⟩

private theorem occ_append (p rest : Word) : Occ p (p ++ rest) 0 := by
  simp [Occ]

private theorem occ_insert_before {p l r : Word} {x : Nat} {j : Nat}
    (h : Occ p (l ++ r) j) (hj : j + p.length ≤ l.length) : Occ p (l ++ x :: r) j := by
  have hjle : j ≤ l.length := by omega
  unfold Occ at h ⊢
  rw [List.drop_append_of_le_length hjle] at h ⊢
  have hlen : p.length ≤ (l.drop j).length := by simp [List.length_drop]; omega
  exact List.prefix_append_of_prefix (l₃ := x :: r) (prefix_of_prefix_append (a := l.drop j) (r := r) h hlen)

private theorem occ_delete_before {p l r : Word} {x : Nat} {j : Nat}
    (h : Occ p (l ++ x :: r) j) (hj : j + p.length ≤ l.length) : Occ p (l ++ r) j := by
  have hjle : j ≤ l.length := by omega
  unfold Occ at h ⊢
  rw [List.drop_append_of_le_length hjle] at h ⊢
  have hlen : p.length ≤ (l.drop j).length := by simp [List.length_drop]; omega
  exact List.prefix_append_of_prefix (l₃ := r) (prefix_of_prefix_append (a := l.drop j) (r := x :: r) h hlen)

private theorem occ_insert_after {p l r : Word} {x : Nat} {j : Nat}
    (h : Occ p (l ++ r) j) (hj : l.length ≤ j) : Occ p (l ++ x :: r) (j + 1) := by
  have hd1 : (l ++ x :: r).drop (j + 1) = r.drop (j - l.length) := by
    rw [List.drop_append, List.drop_eq_nil_of_le (by omega : l.length ≤ j + 1)]
    have hsub : j + 1 - l.length = (j - l.length) + 1 := by omega
    rw [hsub]
    cases r with
    | nil => simp
    | cons a as => simp
  have hd0 : (l ++ r).drop j = r.drop (j - l.length) := by
    rw [List.drop_append, List.drop_eq_nil_of_le (by omega : l.length ≤ j)]
    simp
  rw [Occ, hd0] at h
  rw [Occ, hd1]
  exact h

private theorem occ_delete_after {p l r : Word} {x : Nat} {j : Nat}
    (h : Occ p (l ++ x :: r) j) (hj : l.length < j) : Occ p (l ++ r) (j - 1) := by
  have hd1 : (l ++ r).drop (j - 1) = r.drop (j - l.length - 1) := by
    rw [List.drop_append, List.drop_eq_nil_of_le (by omega : l.length ≤ j - 1)]
    have hsub : j - 1 - l.length = j - l.length - 1 := by omega
    rw [hsub]
    simp
  have hd0 : (l ++ x :: r).drop j = r.drop (j - l.length - 1) := by
    rw [List.drop_append, List.drop_eq_nil_of_le (by omega : l.length ≤ j)]
    have hsub : j - l.length = (j - l.length - 1) + 1 := by omega
    rw [hsub]
    cases r with
    | nil => simp
    | cons a as => simp
  rw [Occ, hd0] at h
  rw [Occ, hd1]
  exact h

private theorem start_zero (ps : List Word) : start ps 0 = 0 := by simp [start]

private theorem start_cons_succ (p : Word) (ps : List Word) (i : Nat) :
    start (p :: ps) (i + 1) = p.length + start ps i := by
  simp [start]

private def initMarks : List Word → List (Nat × Nat)
  | [] => []
  | p :: ps => (0, 0) :: (initMarks ps).map (fun q => (q.1 + 1, q.2 + p.length))

private def lenAt (ps : List Word) (q : Nat × Nat) : Nat := (ps[q.1]?.getD []).length

private def disjoint (ps : List Word) (q1 q2 : Nat × Nat) : Prop :=
  q1.2 + lenAt ps q1 ≤ q2.2 ∨ q2.2 + lenAt ps q2 ≤ q1.2

private theorem disjoint_symm (ps : List Word) :
    ∀ a b, disjoint ps a b → disjoint ps b a := by
  intro a b h
  rcases h with h | h
  · exact Or.inr h
  · exact Or.inl h

private theorem pairwise_rel_of_mem {α : Type} {R : α → α → Prop}
    (hsym : ∀ a b, R a b → R b a) {l : List α} (hp : l.Pairwise R)
    {a b : α} (ha : a ∈ l) (hb : b ∈ l) (hab : a ≠ b) : R a b := by
  induction l with
  | nil => cases ha
  | cons c cs ih =>
      cases hp with
      | cons hfirst hrest =>
          rcases List.mem_cons.mp ha with rfl | ha'
          · rcases List.mem_cons.mp hb with rfl | hb'
            · exact False.elim (hab rfl)
            · exact hfirst b hb'
          · rcases List.mem_cons.mp hb with rfl | hb'
            · exact hsym _ a (hfirst a ha')
            · exact ih hrest ha' hb'

private theorem pairwise_map_succ (ps : List Word) (p : Word) (l : List (Nat × Nat))
    (hall : ∀ q ∈ l, q.1 < ps.length) (hdisj : l.Pairwise (disjoint ps)) :
    (l.map (fun q => (q.1 + 1, q.2 + p.length))).Pairwise (disjoint (p :: ps)) := by
  induction l with
  | nil => simp
  | cons a as ih =>
      cases hdisj with
      | cons hfirst hrest =>
          simp only [List.map_cons]
          refine List.Pairwise.cons ?_ ?_
          · intro q' hq'
            rcases List.mem_map.mp hq' with ⟨b, hb, rfl⟩
            have ha_idx : a.1 < ps.length := hall a (by simp)
            have hb_idx : b.1 < ps.length := hall b (by simp [hb])
            have hd : disjoint ps a b := hfirst b hb
            have heqa : lenAt (p :: ps) (a.1 + 1, a.2 + p.length) = lenAt ps a := by
              simp [lenAt, ha_idx]
            have heqb : lenAt (p :: ps) (b.1 + 1, b.2 + p.length) = lenAt ps b := by
              simp [lenAt, hb_idx]
            rcases hd with hd | hd
            · left
              simp [heqa]
              omega
            · right
              simp [heqb]
              omega
          · apply ih
            · intro q hq
              exact hall q (by simp [hq])
            · exact hrest

private theorem initMarks_props (ps : List Word)
    (hnemp : ∀ p ∈ ps, p ≠ []) :
    (∀ q ∈ initMarks ps,
      q.1 < ps.length ∧
      Occ (ps[q.1]?.getD []) ps.flatten q.2 ∧
      q.2 = start ps q.1 ∧
      0 < lenAt ps q ∧
      (ps[q.1]?.getD []) ≠ []) ∧
    (initMarks ps).Pairwise (disjoint ps) ∧
    (initMarks ps).length = ps.length := by
  induction ps with
  | nil => simp [initMarks, start]
  | cons p ps ih =>
      have ih' := ih (by intro x hx; exact hnemp x (by simp [hx]))
      rcases ih' with ⟨hval, hdisj, hlen⟩
      constructor
      · intro q hq
        simp only [initMarks, List.mem_cons] at hq
        rcases hq with hq | hq
        · subst q
          have hp_len : 0 < p.length := by
            cases p with
            | nil => exact False.elim ((hnemp [] (by simp)) rfl)
            | cons a as => simp
          refine ⟨by simp, ?_, ?_, ?_, ?_⟩
          · exact occ_append p ps.flatten
          · simp [start]
          · simpa [lenAt] using hp_len
          · simpa using hnemp p (by simp)
        · rcases List.mem_map.mp hq with ⟨r, hr, rfl⟩
          have hrval := hval r hr
          rcases hrval with ⟨hri, hrocc, hrstart, hrnemp, hrne⟩
          have heq : (p :: ps)[r.1 + 1]?.getD [] = ps[r.1]?.getD [] := by
            simp [hri]
          rw [heq]
          refine ⟨by simp; omega, ?_, ?_, ?_, hrne⟩
          · unfold Occ at hrocc ⊢
            change (ps[r.1]?.getD []) <+: List.drop (r.2 + p.length) (p ++ ps.flatten)
            rw [List.drop_append, List.drop_eq_nil_of_le (by omega : p.length ≤ r.2 + p.length)]
            have hsub : r.2 + p.length - p.length = r.2 := by omega
            rw [hsub]
            simpa using hrocc
          · rw [start_cons_succ, ← hrstart]
            omega
          · have hleq : lenAt (p :: ps) (r.1 + 1, r.2 + p.length) = lenAt ps r := by
              simp [lenAt, hri]
            rw [hleq]
            exact hrnemp
      · constructor
        · simp only [initMarks]
          refine List.Pairwise.cons ?_ ?_
          · intro q' hq'
            rcases List.mem_map.mp hq' with ⟨r, hr, rfl⟩
            have hrstart := (hval r hr).2.2.1
            have hri := (hval r hr).1
            have hlenmap : lenAt (p :: ps) (r.1 + 1, r.2 + p.length) = lenAt ps r := by
              simp [lenAt, hri]
            simp [disjoint, lenAt, hrstart]
          · apply pairwise_map_succ
            · intro q hq
              exact (hval q hq).1
            · exact hdisj
        · simp [initMarks, hlen]

private theorem initInv (ps : List Word) (hnemp : ∀ p ∈ ps, p ≠ []) :
    ∃ ms : List (Nat × Nat),
      (∀ q ∈ ms, q.1 < ps.length ∧ Occ (ps[q.1]?.getD []) ps.flatten q.2 ∧
        (((q.2 : Int) - (start ps q.1 : Int)).natAbs ≤ 0) ∧
        0 < lenAt ps q ∧ (ps[q.1]?.getD []) ≠ []) ∧
      ms.Pairwise (disjoint ps) ∧
      ps.length ≤ ms.length + 0 := by
  rcases initMarks_props ps hnemp with ⟨hval, hdisj, hlen⟩
  refine ⟨initMarks ps, ?_, hdisj, ?_⟩
  · intro q hq
    rcases hval q hq with ⟨hi, hocc, hstart, hn, hne⟩
    refine ⟨hi, hocc, ?_, hn, hne⟩
    rw [hstart]
    simp
  · omega

private def Inv (ps : List Word) (u : Word) (e : Nat) : Prop :=
  ∃ ms : List (Nat × Nat),
    (∀ q ∈ ms, q.1 < ps.length ∧ Occ (ps[q.1]?.getD []) u q.2 ∧
      (((q.2 : Int) - (start ps q.1 : Int)).natAbs ≤ e) ∧
      0 < lenAt ps q ∧ (ps[q.1]?.getD []) ≠ []) ∧
    ms.Pairwise (disjoint ps) ∧
    ps.length ≤ ms.length + e

private def killedAdd (ps : List Word) (m : Nat) (q : Nat × Nat) : Prop :=
  q.2 < m ∧ m < q.2 + lenAt ps q

private def killedDel (ps : List Word) (m : Nat) (q : Nat × Nat) : Prop :=
  q.2 ≤ m ∧ m < q.2 + lenAt ps q

private def addShift (ps : List Word) (m : Nat) (q : Nat × Nat) : Option (Nat × Nat) :=
  if q.2 + lenAt ps q ≤ m then some q
  else if m ≤ q.2 then some (q.1, q.2 + 1)
  else none

private def delShift (ps : List Word) (m : Nat) (q : Nat × Nat) : Option (Nat × Nat) :=
  if q.2 + lenAt ps q ≤ m then some q
  else if m < q.2 then some (q.1, q.2 - 1)
  else none

private def subShift (ps : List Word) (m : Nat) (q : Nat × Nat) : Option (Nat × Nat) :=
  if q.2 ≤ m && m < q.2 + lenAt ps q then none else some q

private theorem addShift_none_iff (ps : List Word) (m : Nat) (q : Nat × Nat) :
    addShift ps m q = none ↔ killedAdd ps m q := by
  unfold addShift killedAdd
  by_cases h : q.2 + lenAt ps q ≤ m <;> by_cases h' : m ≤ q.2 <;> simp [h, h'] <;> omega

private theorem delShift_none_iff (ps : List Word) (m : Nat) (q : Nat × Nat) :
    delShift ps m q = none ↔ killedDel ps m q := by
  unfold delShift killedDel
  by_cases h : q.2 + lenAt ps q ≤ m <;> by_cases h' : m < q.2 <;> simp [h, h'] <;> omega

private theorem subShift_none_iff (ps : List Word) (m : Nat) (q : Nat × Nat) :
    subShift ps m q = none ↔ killedDel ps m q := by
  unfold subShift killedDel
  by_cases h : q.2 ≤ m <;> by_cases h' : m < q.2 + lenAt ps q <;> simp [h, h'] <;> omega

private theorem addShift_out (ps : List Word) (m : Nat) {q q' : Nat × Nat}
    (h : addShift ps m q = some q') :
    (q' = q ∧ q.2 + lenAt ps q ≤ m) ∨
    (q' = (q.1, q.2 + 1) ∧ m ≤ q.2) := by
  unfold addShift at h
  by_cases hle : q.2 + lenAt ps q ≤ m
  · simp [hle] at h
    exact Or.inl ⟨h.symm, hle⟩
  · by_cases hge : m ≤ q.2
    · simp [hle, hge] at h
      exact Or.inr ⟨h.symm, hge⟩
    · simp [hle, hge] at h

private theorem delShift_out (ps : List Word) (m : Nat) {q q' : Nat × Nat}
    (h : delShift ps m q = some q') :
    (q' = q ∧ q.2 + lenAt ps q ≤ m) ∨
    (q' = (q.1, q.2 - 1) ∧ m < q.2) := by
  unfold delShift at h
  by_cases hle : q.2 + lenAt ps q ≤ m
  · simp [hle] at h
    exact Or.inl ⟨h.symm, hle⟩
  · by_cases hlt : m < q.2
    · simp [hle, hlt] at h
      exact Or.inr ⟨h.symm, hlt⟩
    · simp [hle, hlt] at h

private theorem killedAdd_contra (ps : List Word) (m : Nat) {q1 q2 : Nat × Nat}
    (hk1 : killedAdd ps m q1) (hk2 : killedAdd ps m q2) (hd : disjoint ps q1 q2) : False := by
  rcases hk1 with ⟨h1a, h1b⟩
  rcases hk2 with ⟨h2a, h2b⟩
  rcases hd with hd | hd <;> omega

private theorem killedDel_contra (ps : List Word) (m : Nat) {q1 q2 : Nat × Nat}
    (hk1 : killedDel ps m q1) (hk2 : killedDel ps m q2) (hd : disjoint ps q1 q2) : False := by
  rcases hk1 with ⟨h1a, h1b⟩
  rcases hk2 with ⟨h2a, h2b⟩
  rcases hd with hd | hd <;> omega

private theorem addShift_disjoint (ps : List Word) (m : Nat) {q1 q2 q1' q2' : Nat × Nat}
    (hd : disjoint ps q1 q2)
    (h1 : addShift ps m q1 = some q1') (h2 : addShift ps m q2 = some q2') :
    disjoint ps q1' q2' := by
  rcases addShift_out ps m h1 with ⟨rfl, hle1⟩ | ⟨rfl, hge1⟩ <;>
  rcases addShift_out ps m h2 with ⟨rfl, hle2⟩ | ⟨rfl, hge2⟩ <;>
  simp [disjoint, lenAt] at * <;> omega

private theorem delShift_disjoint (ps : List Word) (m : Nat) {q1 q2 q1' q2' : Nat × Nat}
    (hd : disjoint ps q1 q2) (hn1 : 0 < lenAt ps q1) (hn2 : 0 < lenAt ps q2)
    (h1 : delShift ps m q1 = some q1') (h2 : delShift ps m q2 = some q2') :
    disjoint ps q1' q2' := by
  rcases delShift_out ps m h1 with ⟨rfl, hle1⟩ | ⟨rfl, hlt1⟩ <;>
  rcases delShift_out ps m h2 with ⟨rfl, hle2⟩ | ⟨rfl, hlt2⟩ <;>
  simp [disjoint, lenAt] at * <;> omega

private theorem length_filterMap_eq_of_forall_some {α β : Type} (f : α → Option β)
    (l : List α) (h : ∀ a ∈ l, ∃ b, f a = some b) : (l.filterMap f).length = l.length := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rcases h a (by simp) with ⟨b, hb⟩
      simp [hb, ih (by intro x hx; exact h x (by simp [hx]))]

private theorem filterMap_count_le_succ {α : Type} (f : α → Option α) (kill : α → Prop)
    (R : α → α → Prop)
    (hkill : ∀ a, f a = none ↔ kill a)
    (hcontra : ∀ a b, kill a → kill b → R a b → False)
    (l : List α) (hpw : l.Pairwise R) :
    l.length ≤ (l.filterMap f).length + 1 := by
  induction l with
  | nil => simp
  | cons a as ih =>
      cases hpw with
      | cons hfirst hrest =>
          by_cases hka : kill a
          · have hfa : f a = none := (hkill a).mpr hka
            have hnone : ∀ b ∈ as, ¬ kill b := by
              intro b hb hkb
              exact hcontra a b hka hkb (hfirst b hb)
            have hsome : ∀ b ∈ as, ∃ c, f b = some c := by
              intro b hb
              have hnb : f b ≠ none := by
                intro hn
                exact hnone b hb ((hkill b).mp hn)
              cases hfb : f b with
              | none => exact False.elim (hnb hfb)
              | some c => exact ⟨c, by simp⟩
            have hlen_as := length_filterMap_eq_of_forall_some f as hsome
            simp [hfa, hlen_as]
          · have hn : f a ≠ none := by
              intro hf
              exact hka ((hkill a).mp hf)
            cases hfa : f a with
            | none => exact False.elim (hn hfa)
            | some c =>
                have ih' := ih hrest
                simp [hfa]
                omega

private theorem bound_succ {ps : List Word} {q : Nat × Nat} {e : Nat}
    (h : (((q.2 : Int) - (start ps q.1 : Int)).natAbs ≤ e)) :
    ((((q.2 + 1 : Nat) : Int) - (start ps q.1 : Int)).natAbs ≤ e + 1) := by
  have hdecomp : (((q.2 + 1 : Nat) : Int) - (start ps q.1 : Int)) =
      (((q.2 : Int) - (start ps q.1 : Int)) + 1) := by omega
  rw [hdecomp]
  exact Nat.le_trans (by simpa using (Int.natAbs_add_le (((q.2 : Int) - (start ps q.1 : Int))) (1 : Int)))
    (Nat.add_le_add_right h 1)

private theorem bound_pred {ps : List Word} {q : Nat × Nat} {e : Nat}
    (hpos : 0 < q.2) (h : (((q.2 : Int) - (start ps q.1 : Int)).natAbs ≤ e)) :
    ((((q.2 - 1 : Nat) : Int) - (start ps q.1 : Int)).natAbs ≤ e + 1) := by
  have hdecomp : (((q.2 - 1 : Nat) : Int) - (start ps q.1 : Int)) =
      (((q.2 : Int) - (start ps q.1 : Int)) - 1) := by omega
  rw [hdecomp]
  have hsubeq : (((q.2 : Int) - (start ps q.1 : Int)) - 1) =
      (((q.2 : Int) - (start ps q.1 : Int)) + (-1 : Int)) := by omega
  rw [hsubeq]
  have hs := Int.natAbs_add_le (((q.2 : Int) - (start ps q.1 : Int))) (-1 : Int)
  simpa using (Nat.le_trans (by simpa using hs) (Nat.add_le_add_right h 1))

private theorem occ_subst_before {p l r : Word} {x y : Nat} {j : Nat}
    (h : Occ p (l ++ x :: r) j) (hj : j + p.length ≤ l.length) : Occ p (l ++ y :: r) j := by
  have hjle : j ≤ l.length := by omega
  unfold Occ at h ⊢
  rw [List.drop_append_of_le_length hjle] at h ⊢
  have hlen : p.length ≤ (l.drop j).length := by simp [List.length_drop]; omega
  exact List.prefix_append_of_prefix (l₃ := y :: r) (prefix_of_prefix_append (a := l.drop j) (r := x :: r) h hlen)

private theorem occ_subst_after {p l r : Word} {x y : Nat} {j : Nat}
    (h : Occ p (l ++ x :: r) j) (hj : l.length < j) : Occ p (l ++ y :: r) j := by
  have hd1 : (l ++ y :: r).drop j = r.drop (j - l.length - 1) := by
    rw [List.drop_append, List.drop_eq_nil_of_le (by omega : l.length ≤ j)]
    have hsub : j - l.length = (j - l.length - 1) + 1 := by omega
    rw [hsub]
    cases r with
    | nil => simp
    | cons a as => simp
  have hd0 : (l ++ x :: r).drop j = r.drop (j - l.length - 1) := by
    rw [List.drop_append, List.drop_eq_nil_of_le (by omega : l.length ≤ j)]
    have hsub : j - l.length = (j - l.length - 1) + 1 := by omega
    rw [hsub]
    cases r with
    | nil => simp
    | cons a as => simp
  rw [Occ, hd0] at h
  rw [Occ, hd1]
  exact h

private theorem subShift_eq (ps : List Word) (m : Nat) {q q' : Nat × Nat}
    (h : subShift ps m q = some q') : q' = q := by
  unfold subShift at h
  by_cases hk : q.2 ≤ m && m < q.2 + lenAt ps q
  · simp [hk] at h
  · have hs : some q = some q' := by simpa [hk] using h
    exact (Option.some.inj hs).symm

private theorem filterMap_pairwise {α β : Type} (f : α → Option β)
    (R : β → β → Prop) (S : α → α → Prop)
    (l : List α)
    (hpres : ∀ a ∈ l, ∀ b ∈ l, S a b → ∀ a', f a = some a' → ∀ b', f b = some b' → R a' b')
    (hpw : l.Pairwise S) : (l.filterMap f).Pairwise R := by
  induction l with
  | nil => simp
  | cons a as ih =>
      cases hpw with
      | cons hfirst hrest =>
          have ihpres : ∀ a' ∈ as, ∀ b' ∈ as, S a' b' →
              ∀ a'', f a' = some a'' → ∀ b'', f b' = some b'' → R a'' b'' := by
            intro x hx y hy hxy a'' ha'' b'' hb''
            exact hpres x (by simp [hx]) y (by simp [hy]) hxy a'' ha'' b'' hb''
          cases hfa : f a with
          | none => simpa [hfa] using ih ihpres hrest
          | some a' =>
              simp only [List.filterMap_cons, hfa]
              refine List.Pairwise.cons ?_ ?_
              · intro b' hb'
                rcases List.mem_filterMap.mp hb' with ⟨b, hbmem, hfb⟩
                exact hpres a (by simp) b (by simp [hbmem]) (hfirst b hbmem) a' hfa b' hfb
              · exact ih ihpres hrest

private theorem inv_add (ps : List Word) {e : Nat} {l r : Word} {x : Nat}
    (hinv : Inv ps (l ++ r) e) : Inv ps (l ++ x :: r) (e + 1) := by
  rcases hinv with ⟨ms, hval, hdisj, hcount⟩
  let m := l.length
  refine ⟨ms.filterMap (addShift ps m), ?_, ?_, ?_⟩
  · intro q hq
    rcases List.mem_filterMap.mp hq with ⟨q0, hq0mem, hshift⟩
    rcases hval q0 hq0mem with ⟨hidx, hocc, hbnd, hn, hne⟩
    rcases addShift_out ps m hshift with ⟨hq, hle⟩ | ⟨hq, hge⟩
    · subst q
      refine ⟨hidx, ?_, ?_, hn, hne⟩
      · exact occ_insert_before (x := x) hocc (by simpa [m, lenAt] using hle)
      · exact Nat.le_trans hbnd (Nat.le_succ e)
    · subst q
      refine ⟨hidx, ?_, ?_, ?_, ?_⟩
      · exact occ_insert_after (x := x) hocc (by simpa [m, lenAt] using hge)
      · exact bound_succ hbnd
      · simpa [lenAt] using hn
      · simpa [lenAt] using hne
  · apply filterMap_pairwise (f := addShift ps m) (R := disjoint ps) (S := disjoint ps)
    · intro a ha b hb hab a' ha' b' hb'
      exact addShift_disjoint ps m hab ha' hb'
    · exact hdisj
  · have hc := filterMap_count_le_succ (addShift ps m) (killedAdd ps m) (disjoint ps)
        (addShift_none_iff ps m) (fun a b hka hkb hd => killedAdd_contra ps m hka hkb hd) ms hdisj
    omega

private theorem inv_del (ps : List Word) {e : Nat} {l r : Word} {x : Nat}
    (hinv : Inv ps (l ++ x :: r) e) : Inv ps (l ++ r) (e + 1) := by
  rcases hinv with ⟨ms, hval, hdisj, hcount⟩
  let m := l.length
  refine ⟨ms.filterMap (delShift ps m), ?_, ?_, ?_⟩
  · intro q hq
    rcases List.mem_filterMap.mp hq with ⟨q0, hq0mem, hshift⟩
    rcases hval q0 hq0mem with ⟨hidx, hocc, hbnd, hn, hne⟩
    rcases delShift_out ps m hshift with ⟨hq, hle⟩ | ⟨hq, hlt⟩
    · subst q
      refine ⟨hidx, ?_, ?_, hn, hne⟩
      · exact occ_delete_before (x := x) hocc (by simpa [m, lenAt] using hle)
      · exact Nat.le_trans hbnd (Nat.le_succ e)
    · subst q
      refine ⟨hidx, ?_, ?_, ?_, ?_⟩
      · exact occ_delete_after (x := x) hocc (by simpa [m, lenAt] using hlt)
      · exact bound_pred (by omega) hbnd
      · simpa [lenAt] using hn
      · simpa [lenAt] using hne
  · apply filterMap_pairwise (f := delShift ps m) (R := disjoint ps) (S := disjoint ps)
    · intro a ha b hb hab a' ha' b' hb'
      have hna : 0 < lenAt ps a := (hval a ha).2.2.2.1
      have hnb : 0 < lenAt ps b := (hval b hb).2.2.2.1
      exact delShift_disjoint ps m hab hna hnb ha' hb'
    · exact hdisj
  · have hc := filterMap_count_le_succ (delShift ps m) (killedDel ps m) (disjoint ps)
        (delShift_none_iff ps m) (fun a b hka hkb hd => killedDel_contra ps m hka hkb hd) ms hdisj
    omega

private theorem inv_sub (ps : List Word) {e : Nat} {l r : Word} {x y : Nat}
    (hinv : Inv ps (l ++ x :: r) e) : Inv ps (l ++ y :: r) (e + 1) := by
  rcases hinv with ⟨ms, hval, hdisj, hcount⟩
  let m := l.length
  refine ⟨ms.filterMap (subShift ps m), ?_, ?_, ?_⟩
  · intro q hq
    rcases List.mem_filterMap.mp hq with ⟨q0, hq0mem, hshift⟩
    have hq : q = q0 := subShift_eq ps m hshift
    subst q
    rcases hval q0 hq0mem with ⟨hidx, hocc, hbnd, hn, hne⟩
    refine ⟨hidx, ?_, Nat.le_trans hbnd (Nat.le_succ e), hn, hne⟩
    unfold subShift at hshift
    by_cases hk : q0.2 ≤ m && m < q0.2 + lenAt ps q0
    · simp [hk] at hshift
    · have hle_or : q0.2 + lenAt ps q0 ≤ m ∨ m < q0.2 := by
        simp only [Bool.and_eq_true, decide_eq_true_eq] at hk
        omega
      rcases hle_or with hle | hlt
      · exact occ_subst_before (x := x) (y := y) hocc (by simpa [m, lenAt] using hle)
      · exact occ_subst_after (x := x) (y := y) hocc (by simpa [m, lenAt] using hlt)
  · apply filterMap_pairwise (f := subShift ps m) (R := disjoint ps) (S := disjoint ps)
    · intro a ha b hb hab a' ha' b' hb'
      rw [subShift_eq ps m ha', subShift_eq ps m hb']
      exact hab
    · exact hdisj
  · have hc := filterMap_count_le_succ (subShift ps m) (killedDel ps m) (disjoint ps)
        (subShift_none_iff ps m) (fun a b hka hkb hd => killedDel_contra ps m hka hkb hd) ms hdisj
    omega

private theorem inv_of_edits (ps : List Word) {k : Nat} {s t : Word}
    (hed : Edits k s t) : ∀ e, Inv ps s e → Inv ps t (e + k) := by
  induction hed with
  | refl s =>
      intro e h
      simpa using h
  | drop l r x h ih =>
      intro e h0
      have h1 := ih e h0
      have h2 := inv_del ps (x := x) h1
      simpa [Nat.add_assoc] using h2
  | add l r x h ih =>
      intro e h0
      have h1 := ih e h0
      have h2 := inv_add ps (x := x) h1
      simpa [Nat.add_assoc] using h2
  | subst l r x y h ih =>
      intro e h0
      have h1 := ih e h0
      have h2 := inv_sub ps (x := x) (y := y) h1
      simpa [Nat.add_assoc] using h2

theorem sound_EditFilter_PositionalAnchor : PositionalAnchor.Goal := by
  intro ps t k hk hnemp hed
  have h0 : Inv ps ps.flatten 0 := initInv ps hnemp
  have hf : Inv ps t (0 + k) := inv_of_edits ps hed 0 h0
  simp only [Nat.zero_add] at hf
  rcases hf with ⟨ms, hval, hdisj, hcount⟩
  have hms : 0 < ms.length := by omega
  cases ms with
  | nil => simp at hms
  | cons q qs =>
      rcases hval q (by simp) with ⟨hidx, hocc, hbnd, hn, hne⟩
      refine ⟨q.1, hidx, q.2, occ_mem_positions hne hocc, ?_⟩
      simpa [start] using hbnd

end TinylambOpt.ResearchProofs.Atlas.Goal_EditFilter_PositionalAnchor
