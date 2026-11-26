import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `RotationCycle.DistinctOutput` — Output live grouping keys once even if physical
  codes alias.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_DistinctOutput

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RotationCycle

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

private theorem mem_live_iff {α : Type} (b : Bag α) (p : α) (k : Nat) :
    k ∈ live b p ↔ 0 < b.mass p k ∧ k ∈ b.keys p := by
  unfold live
  simp [List.mem_filter, List.mem_eraseDups, and_comm]

private theorem nodup_live (n m s : Nat) : (live bag (n, m, s)).Nodup := by
  unfold live
  exact List.Pairwise.filter _ (nodup_eraseDups (bag.keys (n, m, s)))

/-! Rotation identity: for `i < m+1`, `(s+i) % (m+1) = k` exactly when the
   "distance" `(k + (m+1) - s % (m+1)) % (m+1)` equals `i`. -/
private theorem rotation_mod_iff (m s k i : Nat) (hk : k ≤ m) (hi : i < m + 1) :
    (s + i) % (m + 1) = k ↔
      (k + (m + 1) - s % (m + 1)) % (m + 1) = i := by
  have hm : 0 < m + 1 := by omega
  have hklt : k < m + 1 := by omega
  have hs : s % (m + 1) < m + 1 := Nat.mod_lt _ hm
  have hsi : (s + i) % (m + 1) = (s % (m + 1) + i) % (m + 1) := by
    calc
      (s + i) % (m + 1) = (s % (m + 1) + i % (m + 1)) % (m + 1) :=
        Nat.add_mod s i (m + 1)
      _ = (s % (m + 1) + i) % (m + 1) := by rw [Nat.mod_eq_of_lt hi]
  constructor
  · intro h
    have hEq : (s % (m + 1) + i) % (m + 1) = k := by
      rw [hsi] at h
      exact h
    by_cases hlt : s % (m + 1) + i < m + 1
    · have hk_eq : s % (m + 1) + i = k := by
        simpa [Nat.mod_eq_of_lt hlt] using hEq
      have hsub : k + (m + 1) - s % (m + 1) = i + (m + 1) := by omega
      rw [hsub]
      calc
        (i + (m + 1)) % (m + 1) =
            (i % (m + 1) + (m + 1) % (m + 1)) % (m + 1) :=
          Nat.add_mod i (m + 1) (m + 1)
        _ = i := by simp [Nat.mod_eq_of_lt hi]
    · have hge : m + 1 ≤ s % (m + 1) + i := by omega
      have hmod_eq : (s % (m + 1) + i) % (m + 1) =
          s % (m + 1) + i - (m + 1) := by
        rw [Nat.mod_eq_sub_mod hge]
        exact Nat.mod_eq_of_lt (by omega)
      have hk_eq : s % (m + 1) + i - (m + 1) = k := by
        simpa [hmod_eq] using hEq
      have hsub : k + (m + 1) - s % (m + 1) = i := by omega
      rw [hsub]
      exact Nat.mod_eq_of_lt hi
  · intro hd
    by_cases hle : s % (m + 1) ≤ k
    · have hge : m + 1 ≤ k + (m + 1) - s % (m + 1) := by omega
      have hmod_eq : (k + (m + 1) - s % (m + 1)) % (m + 1) =
          k - s % (m + 1) := by
        rw [Nat.mod_eq_sub_mod hge]
        have hsub : k + (m + 1) - s % (m + 1) - (m + 1) = k - s % (m + 1) := by omega
        rw [hsub]
        exact Nat.mod_eq_of_lt (by omega)
      have hki : k - s % (m + 1) = i := by simpa [hmod_eq] using hd
      have hk_eq : k = s % (m + 1) + i := by omega
      have hsumlt : s % (m + 1) + i < m + 1 := by omega
      rw [hsi, hk_eq, Nat.mod_eq_of_lt hsumlt]
    · have hlt : k + (m + 1) - s % (m + 1) < m + 1 := by omega
      have hsum_eq : s % (m + 1) + i = k + (m + 1) := by
        have : k + (m + 1) - s % (m + 1) = i := by
          simpa [Nat.mod_eq_of_lt hlt] using hd
        omega
      have hge : m + 1 ≤ s % (m + 1) + i := by omega
      have hmod_eq : (s % (m + 1) + i) % (m + 1) =
          s % (m + 1) + i - (m + 1) := by
        rw [Nat.mod_eq_sub_mod hge]
        exact Nat.mod_eq_of_lt (by omega)
      rw [hsi, hmod_eq]
      omega

private theorem mem_expand_iff_mass_pos (n m s k : Nat) :
    k ∈ bag.expand (n, m, s) ↔ 0 < bag.mass (n, m, s) k := by
  simp only [bag, List.mem_map, List.mem_range]
  constructor
  · rintro ⟨i, hi, rfl⟩
    have hle : (s + i) % (m + 1) ≤ m := by
      have := Nat.mod_lt (s + i) (by omega : 0 < m + 1)
      omega
    simp only [hle, ↓reduceIte]
    by_cases hq : 1 ≤ n / (m + 1)
    · have hqpos : 0 < n / (m + 1) := by omega
      omega
    · have hq0 : n / (m + 1) = 0 := by
        have hnonneg : 0 ≤ n / (m + 1) := Nat.zero_le _
        omega
      have hnlt : n < m + 1 := by
        have hmodlt := Nat.mod_lt n (by omega : 0 < m + 1)
        have hmad := Nat.mod_add_div n (m + 1)
        have heq0 : (m + 1) * (n / (m + 1)) = 0 := by simp [hq0]
        omega
      have hi_m1 : i < m + 1 := by omega
      have hnmod : n % (m + 1) = n := Nat.mod_eq_of_lt hnlt
      have hdist : ((s + i) % (m + 1) + (m + 1) - s % (m + 1)) % (m + 1) = i := by
        exact (rotation_mod_iff m s ((s + i) % (m + 1)) i hle hi_m1).1 rfl
      rw [hq0, hnmod, hdist]
      simp [hi]
  · intro hpos
    by_cases hle : k ≤ m
    · let i := (k + (m + 1) - s % (m + 1)) % (m + 1)
      have hi_m1 : i < m + 1 := Nat.mod_lt _ (by omega)
      have hf : (s + i) % (m + 1) = k :=
        (rotation_mod_iff m s k i hle hi_m1).2 rfl
      refine ⟨i, ?_, hf⟩
      by_cases hq : 1 ≤ n / (m + 1)
      · have hb : m + 1 ≤ n / (m + 1) * (m + 1) := by
          have hmul := Nat.mul_le_mul_right (m + 1) hq
          simpa using hmul
        have hn : n / (m + 1) * (m + 1) ≤ n := by
          rw [Nat.mul_comm]
          exact Nat.mul_div_le n (m + 1)
        omega
      · have hq0 : n / (m + 1) = 0 := by
          have hnonneg : 0 ≤ n / (m + 1) := Nat.zero_le _
          omega
        have hnlt : n < m + 1 := by
          have hmodlt := Nat.mod_lt n (by omega : 0 < m + 1)
          have hmad := Nat.mod_add_div n (m + 1)
          have heq0 : (m + 1) * (n / (m + 1)) = 0 := by simp [hq0]
          omega
        have hnmod : n % (m + 1) = n := Nat.mod_eq_of_lt hnlt
        by_cases hnot : i < n
        · exact hnot
        · simp [i, hle, hq0, hnmod, hnot] at hpos
    · simp [hle] at hpos

theorem sound_DistinctOutput : RotationCycle.DistinctOutput.Goal := by
  intro p
  obtain ⟨n, m, s⟩ := p
  rw [List.perm_iff_count]
  intro k
  by_cases h : 0 < bag.mass (n, m, s) k
  · have hk_le : k ≤ m := by
      by_cases hk : k ≤ m
      · exact hk
      · simp [bag, hk] at h
    have hklt : k < m + 1 := by omega
    have hl : k ∈ live bag (n, m, s) :=
      (mem_live_iff bag (n, m, s) k).2 ⟨h, by simpa [bag] using hklt⟩
    have he : k ∈ (bag.expand (n, m, s)).eraseDups := by
      rw [List.mem_eraseDups]
      exact (mem_expand_iff_mass_pos n m s k).2 h
    rw [count_eq_one_of_mem_nodup (nodup_live n m s) hl,
        count_eq_one_of_mem_nodup (nodup_eraseDups (bag.expand (n, m, s))) he]
  · have hl : k ∉ live bag (n, m, s) := by
      intro hk
      exact h ((mem_live_iff bag (n, m, s) k).1 hk).1
    have he : k ∉ (bag.expand (n, m, s)).eraseDups := by
      intro hk
      rw [List.mem_eraseDups] at hk
      exact h ((mem_expand_iff_mass_pos n m s k).1 hk)
    rw [List.count_eq_zero_of_not_mem hl, List.count_eq_zero_of_not_mem he]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_DistinctOutput
