import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_DistinctOutput

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns

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

private theorem count_flatMap_replicate_eq (rs : List (Nat × Nat)) (k : Nat) :
    (rs.flatMap (fun (x, n) => List.replicate n x)).count k =
      ((rs.filter (fun r => r.1 == k)).map Prod.snd).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      obtain ⟨x, n⟩ := r
      by_cases hx : x == k
      · simp [List.count_replicate, hx, ih]
      · simp [List.count_replicate, hx, ih]

private theorem count_expand_eq_mass (rs : List (Nat × Nat)) (k : Nat) :
    (bag.expand rs).count k = bag.mass rs k := by
  exact count_flatMap_replicate_eq rs k

private theorem mem_keys_of_mass_pos (rs : List (Nat × Nat)) (k : Nat)
    (h : 0 < bag.mass rs k) : k ∈ bag.keys rs := by
  induction rs with
  | nil => simp [bag] at h
  | cons r rs ih =>
      obtain ⟨x, n⟩ := r
      simp only [bag, List.map_cons, List.mem_cons]
      by_cases hx : x == k
      · exact Or.inl (eq_of_beq hx).symm
      · right
        apply ih
        simpa [bag, hx] using h

private theorem mem_live_iff (rs : List (Nat × Nat)) (k : Nat) :
    k ∈ live bag rs ↔ 0 < bag.mass rs k ∧ k ∈ bag.keys rs := by
  unfold live
  simp [List.mem_filter, List.mem_eraseDups, and_comm]

private theorem nodup_live (rs : List (Nat × Nat)) : (live bag rs).Nodup := by
  unfold live
  exact List.Pairwise.filter _ (nodup_eraseDups (bag.keys rs))

theorem sound_DistinctOutput : MultiplicityRuns.DistinctOutput.Goal := by
  intro rs
  rw [List.perm_iff_count]
  intro k
  by_cases h : 0 < bag.mass rs k
  · have hl : k ∈ live bag rs := (mem_live_iff rs k).2 ⟨h, mem_keys_of_mass_pos rs k h⟩
    have he : k ∈ (bag.expand rs).eraseDups := by
      rw [List.mem_eraseDups, ← List.count_pos_iff, count_expand_eq_mass]
      exact h
    rw [count_eq_one_of_mem_nodup (nodup_live rs) hl,
        count_eq_one_of_mem_nodup (nodup_eraseDups (bag.expand rs)) he]
  · have hl : k ∉ live bag rs := by
      intro hk
      exact h ((mem_live_iff rs k).1 hk).1
    have he : k ∉ (bag.expand rs).eraseDups := by
      intro hk
      rw [List.mem_eraseDups] at hk
      exact h (by simpa [count_expand_eq_mass] using (List.count_pos_iff.mpr hk))
    rw [List.count_eq_zero_of_not_mem hl, List.count_eq_zero_of_not_mem he]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_DistinctOutput
