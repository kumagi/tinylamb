import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_AdditiveProduct_DistinctOutput

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct

#eval decide ((live bag ([1, 2], [3, 4])).Perm ((bag.expand ([1, 2], [3, 4])).eraseDups))

private theorem sum_map_eq_zero_iff {α : Type} (l : List α) (f : α → Nat) :
    (l.map f).sum = 0 ↔ ∀ x ∈ l, f x = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      constructor
      · intro h
        have hx : f x = 0 := by omega
        have hxs : (xs.map f).sum = 0 := by omega
        intro y hy
        rw [List.mem_cons] at hy
        rcases hy with rfl | hy
        · exact hx
        · exact (ih.mp hxs) y hy
      · intro h
        have hx : f x = 0 := h x (by simp)
        have hxs : (xs.map f).sum = 0 := by
          apply ih.mpr
          intro y hy
          exact h y (by simp [hy])
        omega

private theorem mass_eq_zero_iff (a b : List Nat) (k : Nat) :
    bag.mass (a, b) k = 0 ↔
      ∀ x, x ∈ a.eraseDups → (if x ≤ k then a.count x * b.count (k - x) else 0) = 0 := by
  unfold bag
  exact sum_map_eq_zero_iff _ _

private theorem mem_expand_iff_mass_pos (a b : List Nat) (k : Nat) :
    k ∈ bag.expand (a, b) ↔ 0 < bag.mass (a, b) k := by
  constructor
  · intro hk
    change k ∈ a.flatMap (fun x => b.map (fun y => x + y)) at hk
    rw [List.mem_flatMap] at hk
    obtain ⟨x, hxa, hkm⟩ := hk
    rw [List.mem_map] at hkm
    obtain ⟨y, hyb, hxy⟩ := hkm
    have hxle : x ≤ k := by omega
    have hkx : k - x = y := by omega
    have hyb' : k - x ∈ b := by simpa [← hkx] using hyb
    apply Nat.pos_of_ne_zero
    intro hzero
    have hterm := (mass_eq_zero_iff a b k).mp hzero x (by rw [List.mem_eraseDups]; exact hxa)
    simp [hxle] at hterm
    have hmul : 0 < a.count x * b.count (k - x) :=
      Nat.mul_pos (List.count_pos_iff.mpr hxa) (List.count_pos_iff.mpr hyb')
    omega
  · intro hpos
    by_cases hk : k ∈ bag.expand (a, b)
    · exact hk
    · have hzero : bag.mass (a, b) k = 0 := by
        apply (mass_eq_zero_iff a b k).mpr
        intro x hx
        by_cases hxle : x ≤ k
        · simp [hxle]
          have hbx : b.count (k - x) = 0 := by
            by_cases hb0 : b.count (k - x) = 0
            · exact hb0
            · have hbmem : k - x ∈ b := List.count_pos_iff.mp (Nat.pos_of_ne_zero hb0)
              have hxmem : x ∈ a := List.mem_eraseDups.mp hx
              exfalso
              apply hk
              change k ∈ a.flatMap (fun x => b.map (fun y => x + y))
              rw [List.mem_flatMap]
              refine ⟨x, hxmem, ?_⟩
              rw [List.mem_map]
              refine ⟨k - x, hbmem, ?_⟩
              omega
          simp [hbx]
        · simp [hxle]
      omega

private theorem mem_keys_of_mass_pos (a b : List Nat) (k : Nat)
    (h : 0 < bag.mass (a, b) k) : k ∈ bag.keys (a, b) := by
  have hk : k ∈ bag.expand (a, b) := (mem_expand_iff_mass_pos a b k).2 h
  change k ∈ a.flatMap (fun x => b.map (fun y => x + y)) at hk
  rw [List.mem_flatMap] at hk
  obtain ⟨x, hxa, hkm⟩ := hk
  rw [List.mem_map] at hkm
  obtain ⟨y, hyb, hxy⟩ := hkm
  change k ∈ a.eraseDups.flatMap (fun x => b.eraseDups.map (fun y => x + y))
  rw [List.mem_flatMap]
  refine ⟨x, ?_, ?_⟩
  · rw [List.mem_eraseDups]
    exact hxa
  · rw [List.mem_map]
    refine ⟨y, ?_, ?_⟩
    · rw [List.mem_eraseDups]
      exact hyb
    · omega

private theorem mem_live_iff (a b : List Nat) (k : Nat) :
    k ∈ live bag (a, b) ↔ 0 < bag.mass (a, b) k ∧ k ∈ bag.keys (a, b) := by
  unfold live
  simp [List.mem_filter, List.mem_eraseDups, and_comm]

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

private theorem nodup_live (a b : List Nat) : (live bag (a, b)).Nodup := by
  unfold live
  exact List.Pairwise.filter _ (nodup_eraseDups (bag.keys (a, b)))

theorem sound_DistinctOutput : AdditiveProduct.DistinctOutput.Goal := by
  intro p
  obtain ⟨a, b⟩ := p
  rw [List.perm_iff_count]
  intro k
  by_cases h : 0 < bag.mass (a, b) k
  · have hl : k ∈ live bag (a, b) := (mem_live_iff a b k).2 ⟨h, mem_keys_of_mass_pos a b k h⟩
    have he : k ∈ (bag.expand (a, b)).eraseDups := by
      rw [List.mem_eraseDups]
      exact (mem_expand_iff_mass_pos a b k).2 h
    rw [count_eq_one_of_mem_nodup (nodup_live a b) hl,
        count_eq_one_of_mem_nodup (nodup_eraseDups (bag.expand (a, b))) he]
  · have hl : k ∉ live bag (a, b) := by
      intro hk
      exact h ((mem_live_iff a b k).1 hk).1
    have he : k ∉ (bag.expand (a, b)).eraseDups := by
      intro hk
      rw [List.mem_eraseDups] at hk
      exact h ((mem_expand_iff_mass_pos a b k).1 hk)
    rw [List.count_eq_zero_of_not_mem hl, List.count_eq_zero_of_not_mem he]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_AdditiveProduct_DistinctOutput
