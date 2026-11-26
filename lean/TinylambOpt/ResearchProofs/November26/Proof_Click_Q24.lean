import TinylambOpt.ResearchGoals.November26.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.November26.Click.Goal_Q24
open TinylambOpt.ResearchGoals.BenchmarkRematch
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

theorem le_trans_bool : ∀ a b c : Nat, (decide (a ≤ b) : Bool) = true →
    (decide (b ≤ c) : Bool) = true → (decide (a ≤ c) : Bool) = true := by
  intro a b c h₁ h₂
  simp at h₁ h₂ ⊢
  omega

theorem le_total_bool : ∀ a b : Nat, ((decide (a ≤ b) : Bool) || decide (b ≤ a)) = true := by
  intro a b
  simp
  omega

theorem le_antisym_bool : ∀ a b : Nat, (decide (a ≤ b) : Bool) = true →
    (decide (b ≤ a) : Bool) = true → a = b := by
  intro a b h₁ h₂
  simp at h₁ h₂
  omega

theorem mergeSort_of_perm {l₁ l₂ : List Nat} (h : l₁.Perm l₂) : l₁.mergeSort = l₂.mergeSort := by
  apply List.Perm.eq_of_pairwise (fun a b _ _ h₁ h₂ => le_antisym_bool a b h₁ h₂)
  · exact List.pairwise_mergeSort le_trans_bool le_total_bool l₁
  · exact List.pairwise_mergeSort le_trans_bool le_total_bool l₂
  · exact (List.mergeSort_perm l₁ _).trans (h.trans (List.mergeSort_perm l₂ _).symm)

theorem mergeSort_append_of_dominates (A B : List Nat)
    (hdom : ∀ a ∈ A, ∀ b ∈ B, a ≤ b) :
    (A ++ B).mergeSort = A.mergeSort ++ B.mergeSort := by
  apply List.Perm.eq_of_pairwise (fun a b _ _ h₁ h₂ => le_antisym_bool a b h₁ h₂)
  · exact List.pairwise_mergeSort le_trans_bool le_total_bool (A ++ B)
  · rw [List.pairwise_append]
    refine ⟨List.pairwise_mergeSort le_trans_bool le_total_bool A,
      List.pairwise_mergeSort le_trans_bool le_total_bool B, ?_⟩
    intro a ha b hb
    simp
    exact hdom a ((List.mergeSort_perm A _).mem_iff.mp ha)
      b ((List.mergeSort_perm B _).mem_iff.mp hb)
  · exact (List.mergeSort_perm (A ++ B) _).trans
      (((List.mergeSort_perm A _).append (List.mergeSort_perm B _)).symm)

theorem take_topn_of_dominates (A B : List Nat) (hdom : ∀ a ∈ A, ∀ b ∈ B, a ≤ b)
    n (hn : n ≤ A.length) : ((A ++ B).mergeSort).take n = (A.mergeSort).take n := by
  rw [mergeSort_append_of_dominates A B hdom]
  exact List.take_append_of_le_length (by simpa using hn)

theorem sound : TinylambOpt.ResearchGoals.November26.Click.Q24.Goal := by
  intro previous page passes lower cutoff hten hcut hpage
  let L := previous.filter (fun x => decide (x ≤ cutoff))
  let H := previous.filter (fun x => !(decide (x ≤ cutoff)) = true)
  let B := page.filter passes
  have hprev_perm : previous.Perm (L ++ H) := by
    simpa [L, H] using partition_perm (fun x : Nat => decide (x ≤ cutoff)) previous
  have hprevB_perm : (previous ++ B).Perm (L ++ (H ++ B)) := by
    have h1 := hprev_perm.append (List.Perm.refl B)
    simpa [List.append_assoc] using h1
  have hLlen : 10 ≤ L.length := by
    simpa [L, countWhere] using hten
  have hdomH : ∀ a ∈ L, ∀ b ∈ H, a ≤ b := by
    intro a ha b hb
    have ha' := List.mem_filter.mp ha
    have hb' := List.mem_filter.mp hb
    simp at ha' hb'
    omega
  have hdomB : ∀ a ∈ L, ∀ b ∈ B, a ≤ b := by
    intro a ha b hb
    have ha' := List.mem_filter.mp ha
    have hb' := List.mem_filter.mp hb
    simp at ha'
    have hbpage : b ∈ page := hb'.1
    have hlower := hpage b hbpage
    omega
  have htop_prev : best 10 previous = (L.mergeSort).take 10 := by
    change (previous.mergeSort).take 10 = (L.mergeSort).take 10
    rw [mergeSort_of_perm hprev_perm]
    have hm := take_topn_of_dominates L H hdomH 10 hLlen
    simpa using hm
  have htop_prevB : best 10 (previous ++ B) = (L.mergeSort).take 10 := by
    change ((previous ++ B).mergeSort).take 10 = (L.mergeSort).take 10
    rw [mergeSort_of_perm hprevB_perm]
    have hdom : ∀ a ∈ L, ∀ b ∈ H ++ B, a ≤ b := by
      intro a ha b hb
      rcases List.mem_append.mp hb with hb | hb
      · exact hdomH a ha b hb
      · exact hdomB a ha b hb
    have hm := take_topn_of_dominates L (H ++ B) hdom 10 hLlen
    simpa using hm
  change best 10 (previous ++ page.filter passes) = best 10 previous
  rw [htop_prevB, htop_prev]
end TinylambOpt.ResearchProofs.November26.Click.Goal_Q24
