import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RepeatedSlices_DistinctOutput

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RepeatedSlices

private theorem count_eq_one_of_mem_nodup {α : Type} [BEq α] [LawfulBEq α]
    {l : List α} {a : α} (hn : l.Nodup) (ha : a ∈ l) : l.count a = 1 := by
  induction l with
  | nil => cases ha
  | cons b bs ih =>
      rw [List.nodup_cons] at hn
      simp only [List.mem_cons] at ha
      rw [List.count_cons]
      by_cases hba : b = a
      · subst a
        simp [List.count_eq_zero_of_not_mem hn.1]
      · have ha' : a ∈ bs := by
          cases ha with
          | inl h => exact (hba h.symm).elim
          | inr h => exact h
        have hbeq : (b == a) = false := by simp [hba]
        simp [hbeq, ih hn.2 ha']

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ l : List α, l.length = n → l.eraseDups.Nodup
  have h : motive l.length := Nat.strongRecOn l.length (fun n ih l hl => by
    cases l with
    | nil => simp [List.eraseDups_nil]
    | cons b bs =>
        rw [List.eraseDups_cons, List.nodup_cons]
        constructor
        · intro hb
          rw [List.mem_eraseDups] at hb
          have hb' := (List.mem_filter.mp hb).2
          simp at hb'
        · apply ih (bs.filter (fun x => !x == b)).length
          · have hle := List.length_filter_le (fun x => !x == b) bs
            simp only [List.length_cons] at hl
            omega
          · rfl)
  exact h l rfl

private theorem mem_drop_subset {xs : List Nat} {i k : Nat} (h : k ∈ xs.drop i) : k ∈ xs := by
  induction xs generalizing i with
  | nil => cases i <;> simp at h
  | cons a t ih =>
      cases i with
      | zero => exact h
      | succ i =>
          have ht : k ∈ t := by simpa using ih (i := i) h
          exact List.mem_cons_of_mem a ht

private theorem mem_expand_iff (xs : List Nat) (k : Nat) : k ∈ bag.expand xs ↔ k ∈ xs := by
  constructor
  · intro h
    simp only [bag, List.mem_flatMap] at h
    rcases h with ⟨i, hi, hki⟩
    exact mem_drop_subset hki
  · intro h
    simp only [bag, List.mem_flatMap]
    have hlen : 0 < xs.length := List.length_pos_of_mem h
    refine ⟨0, List.mem_range.mpr hlen, ?_⟩
    simpa using h

private def massFrom (i : Nat) (xs : List Nat) (k : Nat) : Nat :=
  ((xs.zipIdx i).filter (fun e => e.1 == k)).map (fun e => e.2 + 1) |>.sum

private theorem massFrom_cons (i a k : Nat) (t : List Nat) :
    massFrom i (a :: t) k = (if a == k then i + 1 else 0) + massFrom (i + 1) t k := by
  by_cases h : a = k <;> simp [massFrom, List.zipIdx_cons, h]

private theorem massFrom_pos_of_mem (i : Nat) (xs : List Nat) (k : Nat) (h : k ∈ xs) :
    0 < massFrom i xs k := by
  induction xs generalizing i with
  | nil => cases h
  | cons a t ih =>
      simp only [List.mem_cons] at h
      rw [massFrom_cons]
      by_cases hak : a = k
      · simp [hak]
        omega
      · have hkt : k ∈ t := by
          cases h with
          | inl h' => exact (hak h'.symm).elim
          | inr h' => exact h'
        simp [hak]
        exact ih (i + 1) hkt

private theorem mass_pos_of_mem (xs : List Nat) (k : Nat) (h : k ∈ xs) : 0 < bag.mass xs k := by
  simpa [bag, massFrom] using massFrom_pos_of_mem 0 xs k h

private theorem nodup_live (xs : List Nat) : (live bag xs).Nodup := by
  unfold live
  exact List.Pairwise.filter _ (nodup_eraseDups (bag.keys xs))

theorem sound_DistinctOutput : RepeatedSlices.DistinctOutput.Goal := by
  intro xs
  rw [List.perm_iff_count]
  intro k
  by_cases hk : k ∈ xs
  · have hm : 0 < bag.mass xs k := mass_pos_of_mem xs k hk
    have hl : k ∈ live bag xs := by
      unfold live
      simp only [List.mem_filter, List.mem_eraseDups]
      exact ⟨hk, by simpa using hm⟩
    have he : k ∈ (bag.expand xs).eraseDups := by
      rw [List.mem_eraseDups]
      exact (mem_expand_iff xs k).2 hk
    rw [count_eq_one_of_mem_nodup (nodup_live xs) hl,
        count_eq_one_of_mem_nodup (nodup_eraseDups (bag.expand xs)) he]
  · have hl : k ∉ live bag xs := by
      unfold live
      simp only [List.mem_filter, List.mem_eraseDups]
      intro h'
      exact hk h'.1
    have he : k ∉ (bag.expand xs).eraseDups := by
      intro h
      rw [List.mem_eraseDups, mem_expand_iff] at h
      exact hk h
    rw [List.count_eq_zero_of_not_mem hl, List.count_eq_zero_of_not_mem he]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RepeatedSlices_DistinctOutput
