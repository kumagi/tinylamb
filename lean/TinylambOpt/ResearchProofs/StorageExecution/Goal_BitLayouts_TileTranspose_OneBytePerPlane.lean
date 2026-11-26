import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `TileTranspose.OneBytePerPlane` — `∀ n w tile, (addressSet layout n w ((List.range
  8).map (tile*8+·))).length=w`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_OneBytePerPlane

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose

private theorem eraseDups_eq_self_of_nodup {α : Type} [BEq α] [LawfulBEq α]
    {l : List α} (h : l.Nodup) : l.eraseDups = l := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.eraseDups_cons]
      have hnot : a ∉ as := (List.nodup_cons.mp h).1
      have hnd : as.Nodup := (List.nodup_cons.mp h).2
      have hf : (as.filter fun b => !b == a) = as := by
        exact List.filter_eq_self.mpr (by
          intro b hb
          have hne : b ≠ a := by intro he; exact hnot (he ▸ hb)
          simp [hne])
      rw [hf, ih hnd]

private theorem removeAll_eq_nil_of_subset {α : Type} [BEq α] [LawfulBEq α]
    {xs ys : List α} (h : ∀ a ∈ xs, a ∈ ys) : xs.removeAll ys = [] := by
  induction xs with
  | nil => simp
  | cons a as ih =>
      rw [List.cons_removeAll]
      have hay : a ∈ ys := h a (by simp)
      simp [hay, ih (by intro b hb; exact h b (by simp [hb]))]

private theorem eraseDups_replicate (l : List Nat) (hl : l.Nodup) : ∀ k,
    ((List.replicate (k + 1) l).flatMap id).eraseDups = l :=
  fun k => match k with
  | 0 => by simp [eraseDups_eq_self_of_nodup hl]
  | k + 1 => by
      rw [List.replicate_succ, List.flatMap_cons]
      rw [List.eraseDups_append]
      change l.eraseDups ++ (((List.replicate (k + 1) l).flatMap id).removeAll l).eraseDups = l
      rw [eraseDups_eq_self_of_nodup hl]
      have hsub : ∀ x ∈ ((List.replicate (k + 1) l).flatMap id), x ∈ l := by
        intro x hx
        rw [List.mem_flatMap] at hx
        rcases hx with ⟨t, ht, hxt⟩
        exact (List.mem_replicate.mp ht).2 ▸ hxt
      rw [removeAll_eq_nil_of_subset hsub]
      simp

theorem sound_OneBytePerPlane : TileTranspose.OneBytePerPlane.Goal := by
  intro n w tile
  let L := (List.range w).map (fun b => tile * w + b)
  have hl : L.Nodup := by
    dsimp [L]
    rw [List.nodup_iff_pairwise_ne]
    apply List.Pairwise.map (R := (· ≠ ·)) (S := (· ≠ ·)) (f := fun b => tile * w + b)
    · intro a b h
      omega
    · exact List.nodup_iff_pairwise_ne.mp (List.nodup_range : (List.range w).Nodup)
  have hfp : ∀ r, r < 8 →
      ((List.range w).map (fun b => (((tile * 8 + r) / 8) * 8 * w + b * 8 + (tile * 8 + r) % 8) / 8)).eraseDups
        = L := by
    intro r hr
    have hr8 : (tile * 8 + r) / 8 = tile := by omega
    have hrm : (tile * 8 + r) % 8 = r := by omega
    simp only [hr8, hrm]
    have hmap : (List.range w).map (fun b => (tile * 8 * w + b * 8 + r) / 8) = L := by
      dsimp [L]
      apply List.map_congr_left
      intro b hb
      have hdecomp : tile * 8 * w + b * 8 + r = 8 * (tile * w + b) + r := by
        simp [Nat.add_mul, Nat.mul_assoc, Nat.mul_comm]
      rw [hdecomp]
      omega
    rw [hmap]
    exact eraseDups_eq_self_of_nodup hl
  change ((((List.range 8).map (fun r =>
      ((List.range w).map (fun b => (((tile * 8 + r) / 8) * 8 * w + b * 8 + (tile * 8 + r) % 8) / 8)).eraseDups)).flatMap id).eraseDups).length = w
  have hinner : (List.range 8).map (fun r =>
      ((List.range w).map (fun b => (((tile * 8 + r) / 8) * 8 * w + b * 8 + (tile * 8 + r) % 8) / 8)).eraseDups)
      = List.replicate 8 L := by
    apply List.ext_getElem
    · simp
    · intro i hi _
      rw [List.getElem_map]
      rw [List.getElem_replicate]
      rw [List.getElem_range]
      exact hfp i hi
  rw [hinner]
  rw [show 8 = 7 + 1 by omega]
  rw [eraseDups_replicate L hl]
  simp [L]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_OneBytePerPlane
