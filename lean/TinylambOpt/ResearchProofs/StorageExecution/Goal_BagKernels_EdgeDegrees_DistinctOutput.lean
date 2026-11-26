import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_EdgeDegrees_DistinctOutput

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.EdgeDegrees

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

private theorem count_flatMap_pairs (es : List (Nat × Nat)) (k : Nat) :
    List.count k (es.flatMap (fun (a, b) => [a, b])) =
      List.count k (es.map Prod.fst) + List.count k (es.map Prod.snd) := by
  induction es with
  | nil => simp
  | cons e es ih =>
      obtain ⟨a, b⟩ := e
      change List.count k ([a, b] ++ es.flatMap (fun (a, b) => [a, b])) =
        List.count k (a :: es.map Prod.fst) + List.count k (b :: es.map Prod.snd)
      rw [List.count_append, List.count_cons, List.count_cons, ih]
      by_cases hak : a == k <;> by_cases hbk : b == k <;>
        simp [hak, hbk, List.count_cons, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] <;> omega

private theorem count_expand_eq_mass (es : List (Nat × Nat)) (k : Nat) :
    (bag.expand es).count k = bag.mass es k := by
  change List.count k (es.flatMap (fun (a, b) => [a, b])) = bag.mass es k
  simpa [bag] using count_flatMap_pairs es k

private theorem mem_keys_iff_mass_pos (es : List (Nat × Nat)) (k : Nat) :
    k ∈ bag.keys es ↔ 0 < bag.mass es k := by
  constructor
  · intro hk
    change k ∈ es.map Prod.fst ++ es.map Prod.snd at hk
    rcases List.mem_append.mp hk with hfst | hsnd
    · have hp : 0 < List.count k (es.map Prod.fst) := List.count_pos_iff.mpr hfst
      simp [bag]
      omega
    · have hp : 0 < List.count k (es.map Prod.snd) := List.count_pos_iff.mpr hsnd
      simp [bag]
      omega
  · intro hm
    change k ∈ es.map Prod.fst ++ es.map Prod.snd
    have hsum : 0 < List.count k (es.map Prod.fst) + List.count k (es.map Prod.snd) := by
      simpa [bag] using hm
    have hpos : 0 < List.count k (es.map Prod.fst) ∨ 0 < List.count k (es.map Prod.snd) := by
      omega
    rcases hpos with hpos | hpos
    · exact List.mem_append.mpr (Or.inl (List.count_pos_iff.mp hpos))
    · exact List.mem_append.mpr (Or.inr (List.count_pos_iff.mp hpos))

private theorem mem_live_iff (es : List (Nat × Nat)) (k : Nat) :
    k ∈ live bag es ↔ 0 < bag.mass es k ∧ k ∈ bag.keys es := by
  unfold live
  simp [List.mem_filter, List.mem_eraseDups, and_comm]

private theorem nodup_live (es : List (Nat × Nat)) : (live bag es).Nodup := by
  unfold live
  exact List.Pairwise.filter _ (nodup_eraseDups (bag.keys es))

theorem sound_DistinctOutput : EdgeDegrees.DistinctOutput.Goal := by
  intro es
  rw [List.perm_iff_count]
  intro k
  by_cases h : 0 < bag.mass es k
  · have hl : k ∈ live bag es := (mem_live_iff es k).2 ⟨h, (mem_keys_iff_mass_pos es k).2 h⟩
    have he : k ∈ (bag.expand es).eraseDups := by
      rw [List.mem_eraseDups, ← List.count_pos_iff, count_expand_eq_mass]
      exact h
    rw [count_eq_one_of_mem_nodup (nodup_live es) hl,
        count_eq_one_of_mem_nodup (nodup_eraseDups (bag.expand es)) he]
  · have hl : k ∉ live bag es := by
      intro hk
      exact h ((mem_live_iff es k).1 hk).1
    have he : k ∉ (bag.expand es).eraseDups := by
      intro hk
      rw [List.mem_eraseDups] at hk
      exact h (by simpa [count_expand_eq_mass] using (List.count_pos_iff.mpr hk))
    rw [List.count_eq_zero_of_not_mem hl, List.count_eq_zero_of_not_mem he]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_EdgeDegrees_DistinctOutput
