import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RotatedPlane_RotatedLocations

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane

/-- A rotation of `List.range n` is injective on that range when `0 < n`. -/
private theorem rot_mod_injective (n i j a : Nat) (hi : i < n) (hj : j < n)
    (h : (i + a) % n = (j + a) % n) : i = j := by
  have hn : 0 < n := by omega
  have hr : a % n < n := Nat.mod_lt a hn
  have hi' : (i + a) % n = (i + a % n) % n := by
    rw [Nat.add_mod, Nat.mod_eq_of_lt hi]
  have hj' : (j + a) % n = (j + a % n) % n := by
    rw [Nat.add_mod, Nat.mod_eq_of_lt hj]
  have h2 : (i + a % n) % n = (j + a % n) % n := by omega
  by_cases hi_small : i + a % n < n
  · by_cases hj_small : j + a % n < n
    · rw [Nat.mod_eq_of_lt hi_small, Nat.mod_eq_of_lt hj_small] at h2
      omega
    · have hj_sub_lt : j + a % n - n < n := by omega
      rw [Nat.mod_eq_of_lt hi_small, Nat.mod_eq_sub_mod (Nat.le_of_not_gt hj_small),
          Nat.mod_eq_of_lt hj_sub_lt] at h2
      omega
  · by_cases hj_small : j + a % n < n
    · have hi_sub_lt : i + a % n - n < n := by omega
      rw [Nat.mod_eq_sub_mod (Nat.le_of_not_gt hi_small), Nat.mod_eq_of_lt hi_sub_lt,
          Nat.mod_eq_of_lt hj_small] at h2
      omega
    · have hi_sub_lt : i + a % n - n < n := by omega
      have hj_sub_lt : j + a % n - n < n := by omega
      rw [Nat.mod_eq_sub_mod (Nat.le_of_not_gt hi_small), Nat.mod_eq_of_lt hi_sub_lt,
          Nat.mod_eq_sub_mod (Nat.le_of_not_gt hj_small), Nat.mod_eq_of_lt hj_sub_lt] at h2
      omega

/-- Mapping an injective function over a duplicate-free list keeps it duplicate-free. -/
private theorem nodup_map_of_injOn {α β : Type} (f : α → β) (l : List α)
    (hinj : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) (hl : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.nodup_cons] at hl ⊢
      rcases hl with ⟨hxnot, hxs⟩
      constructor
      · intro hmem
        rcases List.mem_map.mp hmem with ⟨y, hy, hfy⟩
        have hyx : y = x := hinj y (by simp [hy]) x (by simp) hfy
        subst y
        exact hxnot hy
      · exact ih (fun a ha b hb hab => hinj a (by simp [ha]) b (by simp [hb]) hab) hxs

/-- The rotated offsets `(i + b) % n` visit every value below `n` exactly once. -/
private theorem rotated_range_perm (n b : Nat) (hn : 0 < n) :
    ((List.range n).map (fun i => (i + b) % n)).Perm (List.range n) := by
  apply List.perm_iff_count.mpr
  intro k
  have hnodup : ((List.range n).map (fun i => (i + b) % n)).Nodup :=
    nodup_map_of_injOn (fun i => (i + b) % n) (List.range n)
      (by
        intro a ha c hc heq
        exact rot_mod_injective n a c b (List.mem_range.mp ha) (List.mem_range.mp hc) heq)
      List.nodup_range
  rw [hnodup.count, List.nodup_range.count]
  have hmem : ∀ k, k ∈ (List.range n).map (fun i => (i + b) % n) ↔ k < n := by
    intro k
    constructor
    · intro hk
      rcases List.mem_map.mp hk with ⟨i, hi, hik⟩
      have hi_lt : i < n := List.mem_range.mp hi
      have hik_lt : (i + b) % n < n := Nat.mod_lt (i + b) hn
      omega
    · intro hk
      let r := b % n
      let s := k + (n - r)
      let i := s % n
      have hr : r < n := by
        dsimp [r]
        exact Nat.mod_lt b hn
      have hi_lt : i < n := by
        dsimp [i]
        exact Nat.mod_lt s hn
      have hs_decomp : i + n * (s / n) = s := by
        dsimp [i]
        exact Nat.mod_add_div s n
      have hs_lt : s < 2 * n := by
        dsimp [s]
        omega
      have htlt : s / n < 2 := (Nat.div_lt_iff_lt_mul hn).mpr (by simpa [Nat.mul_comm] using hs_lt)
      have ht : s / n ≤ 1 := by omega
      have hmod_b : (i + b) % n = (i + r) % n := by
        have hbdecomp : b = n * (b / n) + r := by
          dsimp [r]
          exact (Nat.div_add_mod b n).symm
        have hsum : i + b = (i + r) + n * (b / n) := by omega
        rw [hsum, Nat.add_mul_mod_self_left]
      have hir_mod : (i + r) % n = k := by
        by_cases ht0 : s / n = 0
        · have hir : i + r = k + n := by
            rw [ht0, Nat.mul_zero, Nat.add_zero] at hs_decomp
            dsimp [s] at hs_decomp
            omega
          rw [hir]
          rw [show k + n = k + n * 1 by omega, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hk]
        · have ht1 : s / n = 1 := Nat.eq_of_lt_succ_of_not_lt htlt (by simpa using ht0)
          have hir : i + r = k := by
            rw [ht1, Nat.mul_one] at hs_decomp
            dsimp [s] at hs_decomp
            omega
          rw [hir, Nat.mod_eq_of_lt hk]
      exact List.mem_map.mpr ⟨i, List.mem_range.mpr hi_lt, by rw [hmod_b, hir_mod]⟩
  by_cases hk : k < n
  · have hkin : k ∈ (List.range n).map (fun i => (i + b) % n) := (hmem k).mpr hk
    simp [hk, hkin]
  · have hkin : k ∉ (List.range n).map (fun i => (i + b) % n) := by
      intro hk'
      exact hk ((hmem k).mp hk')
    simp [hk, hkin]

theorem sound_RotatedLocations : RotatedPlane.RotatedLocations.Goal := by
  intro n w b hn
  have hperm := rotated_range_perm n b hn
  have hmapped := hperm.map (fun k => b * n + k)
  simpa [layout, List.map_map, Function.comp_def, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc] using hmapped

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RotatedPlane_RotatedLocations
