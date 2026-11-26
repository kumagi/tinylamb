import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `IntervalCover.DistinctOutput` — Output live grouping keys once even if physical
  codes alias.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_DistinctOutput

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem max?_some_of_ne_nil : ∀ {xs : List Nat}, xs ≠ [] → ∃ m, xs.max? = some m := by
  intro xs
  cases xs with
  | nil => simp
  | cons a as =>
      intro _
      rw [List.max?_cons]
      exact ⟨as.max?.elim a (max a), rfl⟩

private theorem le_peak {xs : List Nat} {x : Nat} (hx : x ∈ xs) : x ≤ peak xs := by
  have hxsne : xs ≠ [] := by
    intro hnil
    subst xs
    simp at hx
  obtain ⟨m, hm⟩ := max?_some_of_ne_nil (xs := xs) hxsne
  have hmprops := List.max?_eq_some_iff.mp hm
  have hxm : x ≤ m := hmprops.2 x hx
  have hp : peak xs = m := by simp [peak, hm]
  rw [hp]
  exact hxm

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ l : List α, l.length = n → l.eraseDups.Nodup
  have H : motive l.length := by
    apply Nat.strongRecOn
    intro n ih l hlen
    cases l with
    | nil => simp
    | cons a as =>
        rw [List.eraseDups_cons]
        apply List.nodup_cons.mpr
        constructor
        · intro ha
          have hfa : a ∈ as.filter (fun b => !b == a) := List.mem_eraseDups.mp ha
          simp at hfa
        · have hle : (as.filter (fun b => !b == a)).length ≤ as.length :=
            List.length_filter_le (fun b => !b == a) as
          have hlt : (as.filter (fun b => !b == a)).length < n := by
            simp at hlen
            omega
          exact ih _ hlt _ rfl
  exact H l rfl

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

private theorem exists_cover_of_mass_pos (rs : List (Nat × Nat)) (k : Nat) :
    0 < bag.mass rs k ↔ ∃ r ∈ rs, r.1 ≤ k ∧ k < r.1 + r.2 := by
  induction rs with
  | nil => simp [bag]
  | cons r rs ih =>
      obtain ⟨s, n⟩ := r
      have ih' : 0 < List.countP (fun x : Nat × Nat => x.1 ≤ k && k < x.1 + x.2) rs ↔
          ∃ r ∈ rs, r.1 ≤ k ∧ k < r.1 + r.2 := by
        simpa only [bag] using ih
      simp only [bag, List.countP_cons, List.mem_cons]
      by_cases hcov : s ≤ k ∧ k < s + n
      · simp [hcov]
      · simp [hcov, ih']

private theorem mem_expand_iff_cover (rs : List (Nat × Nat)) (k : Nat) :
    k ∈ bag.expand rs ↔ ∃ r ∈ rs, r.1 ≤ k ∧ k < r.1 + r.2 := by
  change k ∈ rs.flatMap (fun (r : Nat × Nat) => (List.range r.2).map (r.1 + ·)) ↔
    ∃ r ∈ rs, r.1 ≤ k ∧ k < r.1 + r.2
  rw [List.mem_flatMap]
  constructor
  · rintro ⟨⟨s, n⟩, hrs, hmem⟩
    rw [List.mem_map] at hmem
    obtain ⟨i, hi, hi_eq⟩ := hmem
    rw [List.mem_range] at hi
    refine ⟨(s, n), hrs, ?_, ?_⟩ <;> omega
  · rintro ⟨⟨s, n⟩, hrs, hsk, hksn⟩
    refine ⟨(s, n), hrs, ?_⟩
    rw [List.mem_map]
    refine ⟨k - s, ?_, ?_⟩
    · rw [List.mem_range]
      omega
    · omega

private theorem mem_expand_iff_mass_pos (rs : List (Nat × Nat)) (k : Nat) :
    k ∈ bag.expand rs ↔ 0 < bag.mass rs k := by
  rw [mem_expand_iff_cover, exists_cover_of_mass_pos]

private theorem mem_keys_of_mass_pos (rs : List (Nat × Nat)) (k : Nat)
    (h : 0 < bag.mass rs k) : k ∈ bag.keys rs := by
  obtain ⟨⟨s, n⟩, hrs, hsk, hksn⟩ := (exists_cover_of_mass_pos rs k).1 h
  simp only [bag, List.mem_range]
  have hmem : s + n ∈ rs.map (fun (r : Nat × Nat) => r.1 + r.2) :=
    List.mem_map.mpr ⟨(s, n), hrs, rfl⟩
  have hle : s + n ≤ peak (rs.map (fun (r : Nat × Nat) => r.1 + r.2)) := le_peak hmem
  omega

private theorem mem_live_iff (rs : List (Nat × Nat)) (k : Nat) :
    k ∈ live bag rs ↔ 0 < bag.mass rs k ∧ k ∈ bag.keys rs := by
  unfold live
  simp [List.mem_filter, List.mem_eraseDups, and_comm]

private theorem nodup_live (rs : List (Nat × Nat)) : (live bag rs).Nodup := by
  unfold live
  exact List.Pairwise.filter _ (nodup_eraseDups (bag.keys rs))

theorem sound_DistinctOutput : IntervalCover.DistinctOutput.Goal := by
  intro rs
  rw [List.perm_iff_count]
  intro k
  by_cases h : 0 < bag.mass rs k
  · have hl : k ∈ live bag rs := (mem_live_iff rs k).2 ⟨h, mem_keys_of_mass_pos rs k h⟩
    have he : k ∈ (bag.expand rs).eraseDups := by
      rw [List.mem_eraseDups]
      exact (mem_expand_iff_mass_pos rs k).2 h
    rw [count_eq_one_of_mem_nodup (nodup_live rs) hl,
        count_eq_one_of_mem_nodup (nodup_eraseDups (bag.expand rs)) he]
  · have hl : k ∉ live bag rs := by
      intro hk
      exact h ((mem_live_iff rs k).1 hk).1
    have he : k ∉ (bag.expand rs).eraseDups := by
      intro hk
      rw [List.mem_eraseDups] at hk
      exact h ((mem_expand_iff_mass_pos rs k).1 hk)
    rw [List.count_eq_zero_of_not_mem hl, List.count_eq_zero_of_not_mem he]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_DistinctOutput
