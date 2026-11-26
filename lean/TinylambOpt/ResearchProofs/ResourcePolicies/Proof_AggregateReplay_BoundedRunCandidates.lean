import TinylambOpt.ResearchGoals.QueryRewrites.Common
import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

namespace TinylambOpt.ResearchProofs.ResourcePolicies.AggregateReplay.Goal_BoundedRunCandidates

open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay


/-! Scratch proof: truncating each sorted run to its top k candidates before
    the final ordered merge loses no globally top-k rows, so bounded replay of
    local runs equals replay of the unsplit stream. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

/-- Sorted lists with the same elements are equal. -/
theorem sorted_perm_eq {l l' : List Nat}
    (hl : l.Pairwise (fun a b : Nat => a ≤ b))
    (hl' : l'.Pairwise (fun a b : Nat => a ≤ b)) (hp : List.Perm l l') : l = l' := by
  induction l generalizing l' with
  | nil =>
      cases l' with
      | nil => rfl
      | cons b bs =>
          have hb : b ∈ ([] : List Nat) := hp.symm.mem_iff.mp (by simp)
          simp at hb
  | cons a as ih =>
      cases l' with
      | nil =>
          have ha : a ∈ ([] : List Nat) := hp.mem_iff.mp (by simp)
          simp at ha
      | cons b bs =>
          have ha_mem : a ∈ b :: bs := hp.mem_iff.mp (by simp)
          have hb_mem : b ∈ a :: as := hp.symm.mem_iff.mp (by simp)
          have hba : b ≤ a := by
            cases List.mem_cons.mp ha_mem with
            | inl hEq => omega
            | inr hmem => exact List.rel_of_pairwise_cons hl' hmem
          have hab : a ≤ b := by
            cases List.mem_cons.mp hb_mem with
            | inl hEq => omega
            | inr hmem => exact List.rel_of_pairwise_cons hl hmem
          have heq : a = b := Nat.le_antisymm hab hba
          subst b
          rw [ih hl.of_cons hl'.of_cons hp.cons_inv]

/-- `mergeSort` is sorted, in both Boolean and proposition form. -/
theorem mergeSort_pairwiseBool (xs : List Nat) :
    (xs.mergeSort).Pairwise (fun a b : Nat => decide (a ≤ b) = true) := by
  exact List.pairwise_mergeSort (le := fun a b : Nat => decide (a ≤ b))
    (by
      intro a b c hab hbc
      have hab' : a ≤ b := by simpa using hab
      have hbc' : b ≤ c := by simpa using hbc
      simpa using Nat.le_trans hab' hbc')
    (by
      intro a b
      by_cases h : a ≤ b
      · simp [h]
      · have hba : b ≤ a := by omega
        simp [h, hba]) xs

theorem mergeSort_pairwise (xs : List Nat) :
    (xs.mergeSort).Pairwise (fun a b : Nat => a ≤ b) := by
  simpa using mergeSort_pairwiseBool xs

/-- Sorting a concatenation equals merging the two sorted halves. -/
theorem mergeSort_append_eq (xs ys : List Nat) :
    (xs ++ ys).mergeSort = xs.mergeSort.merge ys.mergeSort := by
  apply sorted_perm_eq
  · exact mergeSort_pairwise _
  · have h1 := mergeSort_pairwiseBool xs
    have h2 := mergeSort_pairwiseBool ys
    simpa using List.pairwise_merge (le := fun a b : Nat => decide (a ≤ b))
      (by
        intro a b c hab hbc
        have hab' : a ≤ b := by simpa using hab
        have hbc' : b ≤ c := by simpa using hbc
        simpa using Nat.le_trans hab' hbc')
      (by
        intro a b
        by_cases h : a ≤ b
        · simp [h]
        · have hba : b ≤ a := by omega
          simp [h, hba])
      xs.mergeSort ys.mergeSort h1 h2
  · have hsort : List.Perm (xs ++ ys).mergeSort (xs ++ ys) := List.mergeSort_perm _ _
    have happend : List.Perm (xs.mergeSort ++ ys.mergeSort) (xs ++ ys) :=
      (List.mergeSort_perm xs _).append (List.mergeSort_perm ys _)
    have hmerge : List.Perm (xs.mergeSort.merge ys.mergeSort) (xs.mergeSort ++ ys.mergeSort) :=
      List.merge_perm_append _
    exact hsort.trans (happend.symm.trans hmerge.symm)

/-- Prepending a head and truncating commutes under an outer truncation. -/
theorem take_cons_take (b : Nat) (bs : List Nat) (k : Nat) :
    (b :: bs.take k).take k = (b :: bs).take k := by
  cases k with
  | zero => rfl
  | succ k => simp [List.take_take]

/-- Taking the first k outputs of a merge only inspects the first k inputs of
    each side. -/
theorem merge_take (s t : List Nat) (k : Nat) :
    (s.merge t).take k = ((s.take k).merge (t.take k)).take k := by
  induction k generalizing s t with
  | zero => simp
  | succ k ih =>
      cases s with
      | nil =>
          cases t with
          | nil => simp
          | cons b bs => simp [List.take_take]
      | cons a as =>
          cases t with
          | nil => simp [List.take_take]
          | cons b bs =>
              by_cases hab : a ≤ b
              · have h1 := ih as (b :: bs)
                have h3 := ih (as.take k) (b :: bs.take k)
                simp [hab, h1, h3, List.take_take, take_cons_take]
              · have h1 := ih (a :: as) bs
                have h3 := ih (a :: as.take k) (bs.take k)
                simp [hab, h1, h3, List.take_take, take_cons_take]

/-- Top-k of a concatenation depends only on the top-k of each part. -/
theorem best_append (xs ys : List Nat) (k : Nat) :
    best k (xs ++ ys) = best k (best k xs ++ best k ys) := by
  have hleft : (xs ++ ys).mergeSort.take k = (xs.mergeSort.merge ys.mergeSort).take k := by
    rw [mergeSort_append_eq]
  have hright : (best k xs ++ best k ys).mergeSort =
      (xs.mergeSort.take k).merge (ys.mergeSort.take k) := by
    unfold best
    rw [mergeSort_append_eq]
    rw [List.mergeSort_of_pairwise (mergeSort_pairwiseBool xs).take]
    rw [List.mergeSort_of_pairwise (mergeSort_pairwiseBool ys).take]
  change (xs ++ ys).mergeSort.take k = (best k xs ++ best k ys).mergeSort.take k
  rw [hleft, hright]
  exact merge_take xs.mergeSort ys.mergeSort k

/-- Truncating an already truncated sorted list changes nothing. -/
theorem best_idem (xs : List Nat) (k : Nat) : best k (best k xs) = best k xs := by
  unfold best
  rw [List.mergeSort_of_pairwise (mergeSort_pairwiseBool xs).take]
  simp [List.take_take]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay.BoundedRunCandidates.Goal := by
  intro k runs
  induction runs with
  | nil => simp [best]
  | cons run runs ih =>
      calc
        best k ((run :: runs).flatMap (best k)) =
            best k (best k (best k run) ++ best k (runs.flatMap (best k))) := by
              simp only [List.flatMap_cons]
              exact best_append (best k run) (runs.flatMap (best k)) k
        _ = best k (best k run ++ best k (runs.flatMap (best k))) := by
              rw [best_idem]
        _ = best k (best k run ++ best k runs.flatten) := by
              rw [ih]
        _ = best k (run ++ runs.flatten) := by
              exact (best_append run runs.flatten k).symm
        _ = best k (run :: runs).flatten := rfl
end TinylambOpt.ResearchProofs.ResourcePolicies.AggregateReplay.Goal_BoundedRunCandidates
