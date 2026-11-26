import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q25_LateSearchPhraseDecodeAfterTimeTopKGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

private theorem perm_cons_append {α : Type} (a : α) : ∀ l₁ l₂ : List α,
    (a :: (l₁ ++ l₂)).Perm (l₁ ++ a :: l₂)
  | [], l₂ => List.Perm.refl _
  | b :: l₁, l₂ => by
      have ih := perm_cons_append a l₁ l₂
      exact (List.Perm.swap a b (l₁ ++ l₂)).symm.trans (ih.cons b)

private theorem partition_perm {α : Type} (p : α → Bool) : ∀ l : List α,
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

private theorem le_trans_bool : ∀ a b c : Nat, (decide (a ≤ b) : Bool) = true →
    (decide (b ≤ c) : Bool) = true → (decide (a ≤ c) : Bool) = true := by
  intro a b c h₁ h₂
  simp at h₁ h₂ ⊢
  omega

private theorem le_total_bool : ∀ a b : Nat, ((decide (a ≤ b) : Bool) || decide (b ≤ a)) = true := by
  intro a b
  simp
  omega

private theorem le_antisym_bool : ∀ a b : Nat, (decide (a ≤ b) : Bool) = true →
    (decide (b ≤ a) : Bool) = true → a = b := by
  intro a b h₁ h₂
  simp at h₁ h₂
  omega

private theorem mergeSort_of_perm {l₁ l₂ : List Nat} (h : l₁.Perm l₂) :
    l₁.mergeSort = l₂.mergeSort := by
  apply List.Perm.eq_of_pairwise (fun a b _ _ h₁ h₂ => le_antisym_bool a b h₁ h₂)
  · exact List.pairwise_mergeSort le_trans_bool le_total_bool l₁
  · exact List.pairwise_mergeSort le_trans_bool le_total_bool l₂
  · exact (List.mergeSort_perm l₁ _).trans (h.trans (List.mergeSort_perm l₂ _).symm)

private theorem mergeSort_append_of_dominates (A B : List Nat)
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

private theorem take_topn_of_dominates (A B : List Nat)
    (hdom : ∀ a ∈ A, ∀ b ∈ B, a ≤ b)
    n (hn : n ≤ A.length) : ((A ++ B).mergeSort).take n = (A.mergeSort).take n := by
  rw [mergeSort_append_of_dominates A B hdom]
  exact List.take_append_of_le_length (by simpa using hn)

theorem sound : Q25_LateSearchPhraseDecodeAfterTimeTopKGoal := by
  intro rows k tau
  dsimp only
  intro hk
  let matchingTimes := (rows.filter Prod.snd).map Prod.fst
  let low := matchingTimes.filter (fun t => decide (t ≤ tau))
  let high := matchingTimes.filter (fun t => !(decide (t ≤ tau)) = true)
  have hperm : matchingTimes.Perm (low ++ high) := by
    simpa [low, high] using partition_perm (fun t => decide (t ≤ tau)) matchingTimes
  have hlenNat : k ≤ low.length := by simpa [low, countWhere] using hk
  have hdom : ∀ a ∈ low, ∀ b ∈ high, a ≤ b := by
    intro a ha b hb
    have ha' := List.mem_filter.mp ha
    have hb' := List.mem_filter.mp hb
    have hbnot : ¬ decide (b ≤ tau) = true := by simpa using hb'.2
    have haLe : a ≤ tau := by simpa using ha'.2
    have hbGt : tau < b := by
      apply Nat.lt_of_not_ge
      intro hbLe
      exact hbnot (by simp [hbLe])
    omega
  have hsort := mergeSort_of_perm hperm
  change (low.mergeSort).take k = (matchingTimes.mergeSort).take k
  rw [hsort]
  exact (take_topn_of_dominates low high hdom k hlenNat).symm

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q25_LateSearchPhraseDecodeAfterTimeTopKGoal
