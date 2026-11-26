import TinylambOpt.ResearchGoals.Atlas.TextExecution

/-!
SQL string predicates can use encoded or summarized text when the modeled match,
boundaries, and collation are preserved.

Proved

- `EditFilter.HistogramFence` — `∀ k s t, Edits k s t → ((s++t).eraseDups.map (fun a =>
  ((s.count a:Int)-t.count a).natAbs)).sum ≤ 2*k`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_EditFilter_HistogramFence

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.EditFilter

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  induction h : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
      cases l with
      | nil => simp
      | cons a l =>
          rw [List.eraseDups_cons]
          refine List.nodup_cons.mpr ⟨?_, ?_⟩
          · simp
          · apply ih ((l.filter (fun b => !b == a)).length)
            · have hle := List.length_filter_le (l := l) (p := fun b => !b == a)
              have hn : n = l.length + 1 := by simpa using h.symm
              omega
            · rfl

private theorem nodup_decomp {l : List Nat} (hnd : l.Nodup) {a : Nat} (ha : a ∈ l) :
    ∃ l', List.Perm l (a :: l') ∧ a ∉ l' ∧ l'.Nodup := by
  induction l with
  | nil => cases ha
  | cons b bs ih =>
      by_cases hba : b = a
      · subst b
        exact ⟨bs, List.Perm.refl _, (List.nodup_cons.mp hnd).1, (List.nodup_cons.mp hnd).2⟩
      · have habs : a ∈ bs := by
          rcases List.mem_cons.mp ha with h | h
          · exact False.elim (hba h.symm)
          · exact h
        rcases ih (List.nodup_cons.mp hnd).2 habs with ⟨l', hp, hnot, hnd'⟩
        refine ⟨b :: l', ?_, ?_, ?_⟩
        · exact (List.Perm.cons b hp).trans (List.Perm.swap a b l')
        · have hba' : a ≠ b := by intro h; exact hba h.symm
          simp [hba', hnot]
        · refine List.nodup_cons.mpr ⟨?_, hnd'⟩
          intro hbin
          exact (List.nodup_cons.mp hnd).1 ((hp.mem_iff).mpr (by simp [hbin]))

private theorem sum_map_filter_eq {α : Type} (l : List α) (f : α → Nat) (p : α → Bool)
    (hz : ∀ x ∈ l, p x = false → f x = 0) :
    (l.map f).sum = ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.filter_cons]
      by_cases h : p x = true
      · simp [h]
        rw [ih (by intro y hy hpy; exact hz y (by simp [hy]) hpy)]
      · have hf : p x = false := Bool.eq_false_of_ne_true h
        have hx := hz x (by simp) hf
        simp [h, hx]
        exact ih (by intro y hy hpy; exact hz y (by simp [hy]) hpy)

private theorem sum_eraseDups_eq_of_mem (f : Nat → Nat) (l₁ l₂ : List Nat)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup)
    (hmem : ∀ d, d ∈ l₁ ↔ d ∈ l₂) : (l₁.map f).sum = (l₂.map f).sum := by
  induction l₁ generalizing l₂ with
  | nil =>
      have hnil : l₂ = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro d hd
        cases ((hmem d).mpr (by simpa using hd))
      subst l₂
      simp
  | cons a l₁ ih =>
      have ha2 : a ∈ l₂ := (hmem a).mp (by simp)
      rcases nodup_decomp hnd₂ ha2 with ⟨l₂', hp, hnot, hnd₂'⟩
      have hmem' : ∀ d, d ∈ l₁ ↔ d ∈ l₂' := by
        intro d
        constructor
        · intro hd
          have hd2 : d ∈ l₂ := (hmem d).mp (by simp [hd])
          have hda : d ∈ a :: l₂' := hp.mem_iff.mp hd2
          rcases List.mem_cons.mp hda with h | h
          · subst h
            exact False.elim ((List.nodup_cons.mp hnd₁).1 hd)
          · exact h
        · intro hd
          have hd2 : d ∈ a :: l₂' := by simp [hd]
          have hd1 : d ∈ a :: l₁ := (hmem d).mpr (hp.mem_iff.mpr hd2)
          rcases List.mem_cons.mp hd1 with h | h
          · subst h
            exact False.elim (hnot hd)
          · exact h
      have ihres := ih l₂' (List.nodup_cons.mp hnd₁).2 hnd₂' hmem'
      have hperm2 : (l₂.map f).sum = ((a :: l₂').map f).sum :=
        List.Perm.sum_nat (hp.map f)
      simp only [List.map_cons, List.sum_cons]
      rw [hperm2]
      simp only [List.map_cons, List.sum_cons]
      rw [ihres]

private theorem sum_eraseDups_eq_of_mem_zero (f : Nat → Nat) (l₁ l₂ : List Nat)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup)
    (hsub : ∀ d ∈ l₁, d ∈ l₂) (hzero : ∀ d ∈ l₂, d ∉ l₁ → f d = 0) :
    (l₁.map f).sum = (l₂.map f).sum := by
  let l₂' := l₂.filter (fun d => d ∈ l₁)
  have hnd' : l₂'.Nodup := by
    dsimp [l₂']
    exact hnd₂.filter _
  have hmem : ∀ d, d ∈ l₁ ↔ d ∈ l₂' := by
    intro d
    constructor
    · intro hd
      exact List.mem_filter.mpr ⟨hsub d hd, by simp [hd]⟩
    · intro hd
      exact of_decide_eq_true (List.mem_filter.mp hd).2
  have heq1 : (l₁.map f).sum = (l₂'.map f).sum := sum_eraseDups_eq_of_mem f l₁ l₂' hnd₁ hnd' hmem
  have heq2 : (l₂.map f).sum = (l₂'.map f).sum := by
    dsimp [l₂']
    apply sum_map_filter_eq
    intro d hd hpd
    exact hzero d hd (of_decide_eq_false hpd)
  omega

private def L1 (keys s t : Word) : Nat :=
  (keys.map (fun a => ((s.count a : Int) - t.count a).natAbs)).sum

private def fence (s t : Word) : Nat := L1 (s ++ t).eraseDups s t

private theorem L1_eq_of_superset (keys s t : Word) (hsub : ∀ a ∈ s ++ t, a ∈ keys) :
    fence s t = L1 keys.eraseDups s t := by
  apply sum_eraseDups_eq_of_mem_zero (fun a => ((s.count a : Int) - t.count a).natAbs)
    (s ++ t).eraseDups keys.eraseDups (nodup_eraseDups _) (nodup_eraseDups _)
  · intro d hd
    exact List.mem_eraseDups.mpr (hsub d (List.mem_eraseDups.mp hd))
  · intro d hd hnot
    have hdnot : d ∉ s ++ t := by
      intro hmem
      exact hnot (List.mem_eraseDups.mpr hmem)
    have hcs : s.count d = 0 := List.count_eq_zero_of_not_mem (by
      intro hmem; exact hdnot (List.mem_append_left _ hmem))
    have hct : t.count d = 0 := List.count_eq_zero_of_not_mem (by
      intro hmem; exact hdnot (List.mem_append_right _ hmem))
    simp [hcs, hct]

private theorem L1_insert_diff (keys s l r : Word) (x : Nat) :
    keys.Nodup →
    L1 keys s (l ++ x :: r) ≤ L1 keys s (l ++ r) + 1 ∧
    L1 keys s (l ++ r) ≤ L1 keys s (l ++ x :: r) + 1 := by
  induction keys with
  | nil => intro hnd; simp [L1]
  | cons a keys ih =>
      intro hnd
      unfold L1 at *
      simp only [List.map_cons, List.sum_cons] at *
      have hnd_tail : keys.Nodup := (List.nodup_cons.mp hnd).2
      have hc_one : (l ++ x :: r).count x = (l ++ r).count x + 1 := by
        simp only [List.count_append, List.count_cons, beq_self_eq_true, ↓reduceIte]
        omega
      have htail_eq (hnx : x ∉ keys) :
          (keys.map (fun d => ((s.count d : Int) - ((l ++ x :: r).count d : Nat)).natAbs)).sum =
          (keys.map (fun d => ((s.count d : Int) - ((l ++ r).count d : Nat)).natAbs)).sum := by
        have hmap : keys.map (fun d => ((s.count d : Int) - ((l ++ x :: r).count d : Nat)).natAbs) =
            keys.map (fun d => ((s.count d : Int) - ((l ++ r).count d : Nat)).natAbs) := by
          apply List.map_congr_left
          intro d hd
          have hxd : x ≠ d := by
            intro h
            exact hnx (by simpa [h] using hd)
          have hc : (l ++ x :: r).count d = (l ++ r).count d := by
            simp [hxd, List.count_append] <;> omega
          rw [hc]
        exact congrArg List.sum hmap
      constructor
      · by_cases hax : a = x
        · subst a
          rw [hc_one, htail_eq (List.nodup_cons.mp hnd).1]
          have hdecomp : ((s.count x : Int) - ((l ++ r).count x + 1 : Nat)) =
              (((s.count x : Int) - (l ++ r).count x : Int)) + (-1 : Int) := by
            omega
          have hstep : (((s.count x : Int) - ((l ++ r).count x + 1 : Nat)).natAbs) ≤
              (((s.count x : Int) - (l ++ r).count x : Int).natAbs) + 1 := by
            rw [hdecomp]
            simpa using Int.natAbs_add_le (((s.count x : Int) - (l ++ r).count x : Int)) (-1 : Int)
          omega
        · have hxa : x ≠ a := by intro h; exact hax h.symm
          have hc : (l ++ x :: r).count a = (l ++ r).count a := by
            simp [hxa, List.count_append] <;> omega
          rw [hc]
          simpa [Nat.add_assoc] using
            (Nat.add_le_add_left (ih hnd_tail).1
              (((s.count a : Int) - ((l ++ r).count a : Nat)).natAbs))
      · by_cases hax : a = x
        · subst a
          rw [hc_one, htail_eq (List.nodup_cons.mp hnd).1]
          have hdecomp : ((s.count x : Int) - (l ++ r).count x : Int) =
              (((s.count x : Int) - ((l ++ r).count x + 1 : Nat))) + (1 : Int) := by
            omega
          have hstep : (((s.count x : Int) - (l ++ r).count x : Int).natAbs) ≤
              (((s.count x : Int) - ((l ++ r).count x + 1 : Nat)).natAbs) + 1 := by
            rw [hdecomp]
            simpa using Int.natAbs_add_le (((s.count x : Int) - ((l ++ r).count x + 1 : Nat))) (1 : Int)
          omega
        · have hxa : x ≠ a := by intro h; exact hax h.symm
          have hc : (l ++ x :: r).count a = (l ++ r).count a := by
            simp [hxa, List.count_append] <;> omega
          rw [hc]
          simpa [Nat.add_assoc] using
            (Nat.add_le_add_left (ih hnd_tail).2
              (((s.count a : Int) - ((l ++ r).count a : Nat)).natAbs))

private theorem L1_insert_le_base (keys s l r : Word) (x : Nat) (hnd : keys.Nodup) :
    L1 keys s (l ++ x :: r) ≤ L1 keys s (l ++ r) + 1 :=
  (L1_insert_diff keys s l r x hnd).1

private theorem L1_base_le_insert (keys s l r : Word) (x : Nat) (hnd : keys.Nodup) :
    L1 keys s (l ++ r) ≤ L1 keys s (l ++ x :: r) + 1 :=
  (L1_insert_diff keys s l r x hnd).2

private theorem L1_subst_le (keys s l r : Word) (x y : Nat) (hnd : keys.Nodup) :
    L1 keys s (l ++ x :: r) ≤ L1 keys s (l ++ y :: r) + 2 := by
  calc
    L1 keys s (l ++ x :: r) ≤ L1 keys s (l ++ r) + 1 := L1_insert_le_base keys s l r x hnd
    _ ≤ (L1 keys s (l ++ y :: r) + 1) + 1 :=
      Nat.add_le_add_right (L1_base_le_insert keys s l r y hnd) 1
    _ = L1 keys s (l ++ y :: r) + 2 := by omega

theorem sound_EditFilter_HistogramFence : HistogramFence.Goal := by
  intro k s t hed
  change fence s t ≤ 2 * k
  induction hed with
  | refl s =>
      have hz : ((s ++ s).eraseDups.map (fun _ : Nat => 0)).sum = 0 := by
        induction (s ++ s).eraseDups with
        | nil => simp
        | cons a l ih => simp [ih]
      simpa [fence, L1] using hz
  | drop l r x h ih =>
      rename_i k a
      let K : Word := (a ++ (l ++ x :: r)) ++ (l ++ r)
      have hold : fence a (l ++ x :: r) = L1 K.eraseDups a (l ++ x :: r) := by
        apply L1_eq_of_superset
        intro d hd
        simp only [K, List.mem_append] at hd ⊢
        exact Or.inl hd
      have hnew : fence a (l ++ r) = L1 K.eraseDups a (l ++ r) := by
        apply L1_eq_of_superset
        intro d hd
        simp only [K, List.mem_append] at hd ⊢
        rcases hd with hda | hdr
        · exact Or.inl (Or.inl hda)
        · exact Or.inr hdr
      calc
        fence a (l ++ r) = L1 K.eraseDups a (l ++ r) := hnew
        _ ≤ L1 K.eraseDups a (l ++ x :: r) + 1 := L1_base_le_insert K.eraseDups a l r x (nodup_eraseDups K)
        _ ≤ fence a (l ++ x :: r) + 1 := by simp [← hold]
        _ ≤ 2 * k + 1 := Nat.add_le_add_right ih 1
        _ ≤ 2 * (k + 1) := by omega
  | add l r x h ih =>
      rename_i k a
      let K : Word := (a ++ (l ++ r)) ++ (l ++ x :: r)
      have hold : fence a (l ++ r) = L1 K.eraseDups a (l ++ r) := by
        apply L1_eq_of_superset
        intro d hd
        simp only [K, List.mem_append] at hd ⊢
        exact Or.inl hd
      have hnew : fence a (l ++ x :: r) = L1 K.eraseDups a (l ++ x :: r) := by
        apply L1_eq_of_superset
        intro d hd
        simp only [K, List.mem_append] at hd ⊢
        rcases hd with hda | hdr
        · exact Or.inl (Or.inl hda)
        · exact Or.inr hdr
      calc
        fence a (l ++ x :: r) = L1 K.eraseDups a (l ++ x :: r) := hnew
        _ ≤ L1 K.eraseDups a (l ++ r) + 1 := L1_insert_le_base K.eraseDups a l r x (nodup_eraseDups K)
        _ ≤ fence a (l ++ r) + 1 := by simp [← hold]
        _ ≤ 2 * k + 1 := Nat.add_le_add_right ih 1
        _ ≤ 2 * (k + 1) := by omega
  | subst l r x y h ih =>
      rename_i k a
      let K : Word := (a ++ (l ++ x :: r)) ++ (l ++ y :: r)
      have hold : fence a (l ++ x :: r) = L1 K.eraseDups a (l ++ x :: r) := by
        apply L1_eq_of_superset
        intro d hd
        simp only [K, List.mem_append] at hd ⊢
        exact Or.inl hd
      have hnew : fence a (l ++ y :: r) = L1 K.eraseDups a (l ++ y :: r) := by
        apply L1_eq_of_superset
        intro d hd
        simp only [K, List.mem_append] at hd ⊢
        rcases hd with hda | hdr
        · exact Or.inl (Or.inl hda)
        · exact Or.inr hdr
      calc
        fence a (l ++ y :: r) = L1 K.eraseDups a (l ++ y :: r) := hnew
        _ ≤ L1 K.eraseDups a (l ++ x :: r) + 2 := L1_subst_le K.eraseDups a l r y x (nodup_eraseDups K)
        _ ≤ fence a (l ++ x :: r) + 2 := by simp [← hold]
        _ ≤ 2 * k + 2 := Nat.add_le_add_right ih 2
        _ ≤ 2 * (k + 1) := by omega

end TinylambOpt.ResearchProofs.Atlas.Goal_EditFilter_HistogramFence
