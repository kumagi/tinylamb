import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_DistinctOutput

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.CoGroup

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

-- Each retained run of the probe contributes exactly one occurrence of the key.
private theorem count_filter_beq (ys : List Nat) (x k : Nat) :
    (ys.filter (fun y => y == x)).count k = (x == k).toNat * ys.count k := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      by_cases hyx : (y == x) = true
      · have hy_eq_x : y = x := eq_of_beq hyx
        subst y
        rw [List.filter_cons_of_pos (p := fun y => y == x) hyx,
            List.count_cons, ih, List.count_cons]
        cases hxk : (x == k) <;> simp
      · rw [List.filter_cons_of_neg (p := fun y => y == x) hyx,
            List.count_cons, ih]
        cases hxk : (x == k) <;> simp
        have hyk : (y == k) = false := by
          apply beq_eq_false_iff_ne.mpr
          intro hyk_eq
          have hy_eq_x : y = x := hyk_eq.trans (eq_of_beq hxk).symm
          exact hyx (by simp [hy_eq_x])
        have hyne : y ≠ k := beq_eq_false_iff_ne.mp hyk
        simp [hyne]

private theorem count_expand_eq_mass (xs ys : List Nat) (k : Nat) :
    (bag.expand (xs, ys)).count k = bag.mass (xs, ys) k := by
  induction xs with
  | nil => simp [bag]
  | cons x xs ih =>
      dsimp [bag] at ih ⊢
      rw [List.flatMap_cons, List.count_append, ih, List.count_cons]
      rw [count_filter_beq]
      by_cases h : (x == k) = true
      · simp [h, Nat.add_mul]
        ac_rfl
      · simp [h]

private theorem mem_keys_of_mass_pos (xs ys : List Nat) (k : Nat)
    (h : 0 < bag.mass (xs, ys) k) : k ∈ bag.keys (xs, ys) := by
  change k ∈ xs
  dsimp [bag] at h
  have hxpos : 0 < xs.count k := by
    have hxne : xs.count k ≠ 0 := by
      intro hx0
      have hprod0 : xs.count k * ys.count k = 0 := by simp [hx0]
      omega
    exact Nat.pos_iff_ne_zero.mpr hxne
  exact List.count_pos_iff.mp hxpos

private theorem mem_live_iff (p : List Nat × List Nat) (k : Nat) :
    k ∈ live bag p ↔ 0 < bag.mass p k ∧ k ∈ bag.keys p := by
  unfold live
  simp [List.mem_filter, List.mem_eraseDups, and_comm]

private theorem nodup_live (p : List Nat × List Nat) : (live bag p).Nodup := by
  unfold live
  exact List.Pairwise.filter _ (nodup_eraseDups (bag.keys p))

theorem sound_DistinctOutput : CoGroup.DistinctOutput.Goal := by
  intro p
  obtain ⟨xs, ys⟩ := p
  rw [List.perm_iff_count]
  intro k
  by_cases h : 0 < bag.mass (xs, ys) k
  · have hl : k ∈ live bag (xs, ys) :=
      (mem_live_iff (xs, ys) k).2 ⟨h, mem_keys_of_mass_pos xs ys k h⟩
    have he : k ∈ (bag.expand (xs, ys)).eraseDups := by
      rw [List.mem_eraseDups, ← List.count_pos_iff, count_expand_eq_mass]
      exact h
    rw [count_eq_one_of_mem_nodup (nodup_live (xs, ys)) hl,
        count_eq_one_of_mem_nodup (nodup_eraseDups (bag.expand (xs, ys))) he]
  · have hl : k ∉ live bag (xs, ys) := by
      intro hk
      exact h ((mem_live_iff (xs, ys) k).1 hk).1
    have he : k ∉ (bag.expand (xs, ys)).eraseDups := by
      intro hk
      rw [List.mem_eraseDups] at hk
      exact h (by simpa [count_expand_eq_mass] using (List.count_pos_iff.mpr hk))
    rw [List.count_eq_zero_of_not_mem hl, List.count_eq_zero_of_not_mem he]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_DistinctOutput
