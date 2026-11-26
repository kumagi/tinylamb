import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RepeatedSlices_IntersectAll

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RepeatedSlices

/-! ## Generic list lemmas -/

private theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α] (l : List α) : l.eraseDups.Nodup := by
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
          · have hle := List.length_filter_le (fun x : α => !x == b) bs
            simp only [List.length_cons] at hl
            omega
          · rfl)
  exact h l rfl

private theorem count_flatMap_replicate (xs : List Nat) (f : Nat → Nat) (k : Nat) :
    (xs.flatMap (fun x => List.replicate (f x) x)).count k =
      (xs.map (fun x => if x = k then f x else 0)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [ih, List.count_replicate, beq_iff_eq]

private theorem sum_eq_single_of_nodup (xs : List Nat) (hn : xs.Nodup) (k : Nat) (f : Nat → Nat) :
    (xs.map (fun x => if x = k then f x else 0)).sum = if k ∈ xs then f k else 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rw [List.nodup_cons] at hn
      simp only [List.map_cons, List.sum_cons]
      by_cases hxk : x = k
      · subst x
        have hnot : k ∉ xs := hn.1
        simpa [hnot] using (ih hn.2)
      · have hi := ih hn.2
        have hkx : k ≠ x := fun h => hxk h.symm
        by_cases hk : k ∈ xs
        · simp [hxk, hkx, hk, hi]
        · simp [hxk, hkx, hk, hi]

/-! ## RepeatedSlices bag facts -/

private def massFrom (i : Nat) (xs : List Nat) (k : Nat) : Nat :=
  ((xs.zipIdx i).filter (fun e => e.1 == k)).map (fun e => e.2 + 1) |>.sum

private theorem mass_eq_massFrom (xs : List Nat) (k : Nat) : bag.mass xs k = massFrom 0 xs k := by
  simp [bag, massFrom]

private theorem massFrom_cons (i a k : Nat) (t : List Nat) :
    massFrom i (a :: t) k = (if a == k then i + 1 else 0) + massFrom (i + 1) t k := by
  simp only [massFrom, List.zipIdx_cons]
  by_cases h : a == k <;> simp [h]

private theorem massFrom_add (i : Nat) (xs : List Nat) (k : Nat) :
    massFrom i xs k = massFrom 0 xs k + i * xs.count k := by
  induction xs generalizing i with
  | nil => simp [massFrom]
  | cons a t ih =>
      rw [massFrom_cons, massFrom_cons, ih (i := i + 1), ih (i := 1)]
      by_cases h : a == k
      · simp [h, List.count_cons, Nat.succ_mul, Nat.mul_add, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      · simp [h, List.count_cons, Nat.succ_mul, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem expand_cons (a : Nat) (t : List Nat) :
    bag.expand (a :: t) = (a :: t) ++ bag.expand t := by
  simp [bag, List.range_succ_eq_map]
  rw [List.flatMap_map]
  simp [List.drop_succ_cons]

private theorem count_expand_eq_mass (xs : List Nat) (k : Nat) :
    (bag.expand xs).count k = bag.mass xs k := by
  induction xs with
  | nil => simp [bag]
  | cons a t ih =>
      rw [expand_cons, List.count_append, ih]
      rw [mass_eq_massFrom (a :: t) k]
      rw [massFrom_cons, massFrom_add (i := 1) t k]
      simp [mass_eq_massFrom, List.count_cons, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem mem_drop_subset {xs : List Nat} {i k : Nat} (h : k ∈ xs.drop i) : k ∈ xs := by
  induction xs generalizing i with
  | nil => simp at h
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

private theorem nodup_live (xs : List Nat) : (live bag xs).Nodup := by
  unfold live
  exact List.Pairwise.filter _ (eraseDups_nodup (bag.keys xs))

private theorem mem_live_of_mass_pos (xs : List Nat) (k : Nat) (h : 0 < bag.mass xs k) :
    k ∈ live bag xs := by
  have hc : 0 < (bag.expand xs).count k := by
    rw [count_expand_eq_mass]
    exact h
  have hmem : k ∈ bag.expand xs := List.count_pos_iff.mp hc
  have hk : k ∈ xs := (mem_expand_iff xs k).1 hmem
  unfold live
  rw [List.mem_filter]
  constructor
  · rw [List.mem_eraseDups]
    simpa [bag] using hk
  · simp [h]

/-! ## Goal -/

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a c k
  rw [count_flatMap_replicate]
  rw [sum_eq_single_of_nodup (live bag a) (nodup_live a)]
  rw [count_expand_eq_mass, count_expand_eq_mass]
  by_cases hk : k ∈ live bag a
  · simp [hk]
  · have hm0 : bag.mass a k = 0 := by
      by_cases hz : bag.mass a k = 0
      · exact hz
      · exact False.elim (hk (mem_live_of_mass_pos a k (Nat.pos_of_ne_zero hz)))
    simp [hk, hm0]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RepeatedSlices_IntersectAll
