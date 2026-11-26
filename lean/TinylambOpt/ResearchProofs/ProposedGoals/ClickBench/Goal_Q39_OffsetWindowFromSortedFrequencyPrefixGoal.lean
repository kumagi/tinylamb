import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q39_OffsetWindowFromSortedFrequencyPrefixGoal

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

theorem sound : Q39_OffsetWindowFromSortedFrequencyPrefixGoal := by
  intro counts cut off len
  dsimp only
  intro hskipped
  let low := counts.filter (fun c => decide (c < cut))
  let high := counts.filter (fun c => !(decide (c < cut)) = true)
  have hlowLen : low.length = countWhere counts (fun c => decide (c < cut)) := rfl
  have hskip : low.length ≤ off := by simpa [hlowLen] using hskipped
  have hpartition : counts.Perm (low ++ high) := by
    exact partition_perm (fun c => decide (c < cut)) counts
  have hhigh : high = counts.filter (fun c => decide (cut ≤ c)) := by
    apply List.filter_congr
    intro c hc
    by_cases hlt : c < cut
    · have hnot : ¬ cut ≤ c := by omega
      simp [hlt, hnot]
    · have hge : cut ≤ c := by omega
      simp [hlt, hge]
  have hsort : counts.mergeSort = (low ++ high).mergeSort :=
    mergeSort_of_perm hpartition
  have hdom : ∀ a ∈ low, ∀ b ∈ high, a ≤ b := by
    intro a ha b hb
    have ha' := List.mem_filter.mp ha
    have hb' := List.mem_filter.mp hb
    have haLt : a < cut := by simpa using ha'.2
    have hbnot : (!decide (b < cut)) = true := hb'.2
    have hbNotLt : ¬ b < cut := by
      intro hbLt
      have hdec : decide (b < cut) = true := by simp [hbLt]
      simp [hdec] at hbnot
    omega
  have hlowSort : (low ++ high).mergeSort = low.mergeSort ++ high.mergeSort :=
    mergeSort_append_of_dominates low high hdom
  have hdropHigh : high.mergeSort.drop (off - low.length) =
      ((low.mergeSort ++ high.mergeSort).drop off) := by
    have hdropLow : (low.mergeSort ++ high.mergeSort).drop low.length = high.mergeSort := by simp
    calc
      high.mergeSort.drop (off - low.length) =
          ((low.mergeSort ++ high.mergeSort).drop low.length).drop (off - low.length) := by
            rw [hdropLow]
      _ = (low.mergeSort ++ high.mergeSort).drop (low.length + (off - low.length)) := List.drop_drop
      _ = (low.mergeSort ++ high.mergeSort).drop off := by
            rw [Nat.add_sub_of_le hskip]
  change ((counts.filter (fun c => decide (cut ≤ c))).mergeSort.drop
      (off - countWhere counts (fun c => decide (c < cut)))).take len =
    (counts.mergeSort.drop off).take len
  rw [← hhigh, ← hlowLen]
  rw [hsort, hlowSort, hdropHigh]

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q39_OffsetWindowFromSortedFrequencyPrefixGoal
