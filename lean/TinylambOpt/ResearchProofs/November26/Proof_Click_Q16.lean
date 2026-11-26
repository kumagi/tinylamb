import TinylambOpt.ResearchGoals.November26.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.November26.Click.Goal_Q16
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Click

theorem perm_cons_append {α : Type} (a : α) : ∀ l₁ l₂ : List α,
    (a :: (l₁ ++ l₂)).Perm (l₁ ++ a :: l₂)
  | [], l₂ => List.Perm.refl _
  | b :: l₁, l₂ => by
      have ih := perm_cons_append a l₁ l₂
      exact (List.Perm.swap a b (l₁ ++ l₂)).symm.trans (ih.cons b)

theorem partition_perm {α : Type} (p : α → Bool) : ∀ l : List α,
    l.Perm (l.filter p ++ l.filter (fun x => !(p x) = true))
  | [] => List.Perm.refl []
  | a :: l => by
      by_cases h : p a = true
      · have ih := partition_perm p l
        calc
          a :: l |>.Perm (a :: (l.filter p ++ l.filter (fun x => !(p x) = true))) := ih.cons a
          _ = (a :: l).filter p ++ (a :: l).filter (fun x => !(p x) = true) := by simp [h]
      · have hf : p a = false := by simpa using h
        have ih := partition_perm p l
        calc
          a :: l |>.Perm (a :: (l.filter p ++ l.filter (fun x => !(p x) = true))) := ih.cons a
          _ |>.Perm (l.filter p ++ a :: l.filter (fun x => !(p x) = true)) :=
            perm_cons_append a (l.filter p) (l.filter (fun x => !(p x) = true))
          _ = (a :: l).filter p ++ (a :: l).filter (fun x => !(p x) = true) := by simp [hf]

theorem mergeSort_of_perm {α : Type} (le : α → α → Bool)
    (trans : ∀ a b c, le a b = true → le b c = true → le a c = true)
    (total : ∀ a b, (le a b || le b a) = true)
    (antisym : ∀ a b, le a b = true → le b a = true → a = b) :
    ∀ {l₁ l₂ : List α}, l₁.Perm l₂ → l₁.mergeSort le = l₂.mergeSort le := by
  intro l₁ l₂ h
  apply List.Perm.eq_of_pairwise (fun a b _ _ h₁ h₂ => antisym a b h₁ h₂)
  · exact List.pairwise_mergeSort trans total l₁
  · exact List.pairwise_mergeSort trans total l₂
  · exact (List.mergeSort_perm l₁ le).trans (h.trans (List.mergeSort_perm l₂ le).symm)

theorem mergeSort_append_of_dominates {α : Type} (le : α → α → Bool)
    (trans : ∀ a b c, le a b = true → le b c = true → le a c = true)
    (total : ∀ a b, (le a b || le b a) = true)
    (antisym : ∀ a b, le a b = true → le b a = true → a = b)
    (A B : List α) (hdom : ∀ a ∈ A, ∀ b ∈ B, le a b = true) :
    (A ++ B).mergeSort le = A.mergeSort le ++ B.mergeSort le := by
  apply List.Perm.eq_of_pairwise (fun a b _ _ h₁ h₂ => antisym a b h₁ h₂)
  · exact List.pairwise_mergeSort trans total (A ++ B)
  · rw [List.pairwise_append]
    refine ⟨List.pairwise_mergeSort trans total A, List.pairwise_mergeSort trans total B, ?_⟩
    intro a ha b hb
    exact hdom a ((List.mergeSort_perm A le).mem_iff.mp ha)
      b ((List.mergeSort_perm B le).mem_iff.mp hb)
  · exact (List.mergeSort_perm (A ++ B) le).trans
      (((List.mergeSort_perm A le).append (List.mergeSort_perm B le)).symm)

theorem take_topn_of_dominates {α : Type} (le : α → α → Bool)
    (trans : ∀ a b c, le a b = true → le b c = true → le a c = true)
    (total : ∀ a b, (le a b || le b a) = true)
    (antisym : ∀ a b, le a b = true → le b a = true → a = b)
    (A B : List α) (hdom : ∀ a ∈ A, ∀ b ∈ B, le a b = true) n (hn : n ≤ A.length) :
    ((A ++ B).mergeSort le).take n = (A.mergeSort le).take n := by
  rw [mergeSort_append_of_dominates le trans total antisym A B hdom]
  exact List.take_append_of_le_length (by simpa using hn)

theorem sound : TinylambOpt.ResearchGoals.November26.Click.Q16.Goal := by
  intro users candidates threshold keys kept hten hlow rank
  let cnt : Nat → Nat := fun k => users.count k
  let le : Nat → Nat → Bool := fun a b => decide (cnt a > cnt b ∨ (cnt a = cnt b ∧ a ≤ b))
  have htrans : ∀ a b c, le a b = true → le b c = true → le a c = true := by
    intro a b c hab hbc
    simp [le, cnt] at hab hbc ⊢
    rcases hab with hab | ⟨hab, hab'⟩
    · rcases hbc with hbc | ⟨hbc, hbc'⟩
      · omega
      · left; omega
    · rcases hbc with hbc | ⟨hbc, hbc'⟩
      · left; omega
      · right; constructor <;> omega
  have htotal : ∀ a b, (le a b || le b a) = true := by
    intro a b
    by_cases h : le a b = true
    · simp [h]
    · have h' : le b a = true := by
        simp [le] at h ⊢
        by_cases hgt : cnt b > cnt a
        · exact Or.inl hgt
        · right
          constructor
          · omega
          · have heqcnt : cnt a = cnt b := by omega
            have hlt : b < a := h.2 heqcnt
            omega
      have hf : le a b = false := by simpa using h
      simp [hf, h']
  have hantisym : ∀ a b, le a b = true → le b a = true → a = b := by
    intro a b hab hba
    simp [le, cnt] at hab hba
    rcases hab with hab | ⟨habc, hleab⟩
    · rcases hba with hba | ⟨hbabc, hleba⟩
      · omega
      · omega
    · rcases hba with hba | ⟨hbabc, hleba⟩
      · omega
      · omega
  let p : Nat → Bool := fun k => decide (candidates.contains k = true ∧ threshold ≤ cnt k)
  let A := keys.filter p
  let O := keys.filter (fun k => !(p k) = true)
  have hkeys_perm : keys.Perm (A ++ O) := by
    simpa [A, O] using partition_perm p keys
  have hkeys_sort : keys.mergeSort le = (A ++ O).mergeSort le :=
    mergeSort_of_perm le htrans htotal hantisym hkeys_perm
  have hAeq : A = kept.filter (fun k => decide (threshold ≤ cnt k)) := by
    unfold A p kept keys
    rw [List.filter_filter]
    apply List.filter_congr
    intro k hk
    simp [Bool.and_comm]
  have hA_len : 10 ≤ A.length := by
    rw [hAeq]
    simpa [countWhere, cnt] using hten
  have hdom : ∀ a ∈ A, ∀ b ∈ O, le a b = true := by
    intro a ha b hb
    have ha' := List.mem_filter.mp ha
    have hb' := List.mem_filter.mp hb
    have hap : p a = true := ha'.2
    have hbp : !(p b) = true := hb'.2
    simp [p] at hap hbp
    have hacnt : threshold ≤ cnt a := hap.2
    have hblt : cnt b < threshold := by
      rcases hbp with hcand | hlt
      · have hcand' : ¬ b ∈ candidates := by simpa using hcand
        exact hlow b hb'.1 hcand'
      · exact hlt
    simp [le]
    left
    omega
  have htop_keys_A : (keys.mergeSort le).take 10 = (A.mergeSort le).take 10 := by
    rw [hkeys_sort]
    exact take_topn_of_dominates le htrans htotal hantisym A O hdom 10 hA_len
  let C := kept.filter (fun k => !(decide (threshold ≤ cnt k)) = true)
  have hkept_perm : kept.Perm (A ++ C) := by
    rw [hAeq]
    unfold C
    simpa using partition_perm (fun k => decide (threshold ≤ cnt k)) kept
  have hkept_sort : kept.mergeSort le = (A ++ C).mergeSort le :=
    mergeSort_of_perm le htrans htotal hantisym hkept_perm
  have hdom2 : ∀ a ∈ A, ∀ b ∈ C, le a b = true := by
    intro a ha b hb
    have ha' := List.mem_filter.mp ha
    have hbp : !(decide (threshold ≤ cnt b)) = true := (List.mem_filter.mp hb).2
    have hap : p a = true := ha'.2
    simp [p] at hap
    simp at hbp
    have hacnt : threshold ≤ cnt a := hap.2
    have hblt : cnt b < threshold := by omega
    simp [le]
    left
    omega
  have htop_kept_A : (kept.mergeSort le).take 10 = (A.mergeSort le).take 10 := by
    rw [hkept_sort]
    exact take_topn_of_dominates le htrans htotal hantisym A C hdom2 10 hA_len
  change (kept.mergeSort le).take 10 = (keys.mergeSort le).take 10
  rw [htop_kept_A, htop_keys_A]
end TinylambOpt.ResearchProofs.November26.Click.Goal_Q16
