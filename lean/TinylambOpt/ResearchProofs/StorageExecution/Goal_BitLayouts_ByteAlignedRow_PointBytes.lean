import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_PointBytes

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) : xs.eraseDups.Nodup := by
  match xs with
  | [] => simp
  | x :: xs =>
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro hx
        rw [List.mem_eraseDups, List.mem_filter] at hx
        simp at hx
      · exact nodup_eraseDups (xs.filter (fun y => !y == x))
  termination_by xs.length
  decreasing_by
    simp_wf
    exact Nat.lt_succ_of_le (List.length_filter_le _ _)

/-- A byte-aligned row stores bit `b` of value `i` at physical bit
`i * align 8 w + b`.  Since `align 8 w` is a multiple of 8, dividing by 8
yields the byte address `i * (align 8 w / 8) + b / 8`.  As `b` ranges over
`0 .. w-1`, the quotient `b / 8` attains exactly the values `0 .. byteCount w - 1`,
so the footprint has exactly `byteCount w` distinct bytes. -/
theorem sound_PointBytes : ByteAlignedRow.PointBytes.Goal := by
  intro n w i
  simp only [footprint, ByteAlignedRow.layout, lanes]
  have haw8 : align 8 w / 8 * 8 = align 8 w := by
    unfold align
    omega
  have hmap : (List.range w).map (fun b => (i * align 8 w + b) / 8) =
      (List.range w).map (fun b => i * (align 8 w / 8) + b / 8) := by
    apply List.map_congr_left
    intro b hb
    have : i * align 8 w + b = (i * (align 8 w / 8)) * 8 + b := by
      calc
        i * align 8 w + b = i * (align 8 w / 8 * 8) + b := by rw [haw8]
        _ = (i * (align 8 w / 8)) * 8 + b := by
          simp [Nat.mul_comm, Nat.mul_left_comm]
    rw [this]
    omega
  rw [hmap]
  have hnodup_shift : ∀ (c m : Nat), ((List.range m).map (fun k => c + k)).Nodup :=
    fun c m => List.Pairwise.map (fun k => c + k) (fun a b h => by omega) (List.nodup_range)
  apply Nat.le_antisymm
  · have hsub : ((List.range w).map (fun b => i * (align 8 w / 8) + b / 8)).eraseDups ⊆
        (List.range (byteCount w)).map (fun k => i * (align 8 w / 8) + k) := by
      intro x hx
      rw [List.mem_eraseDups, List.mem_map] at hx
      rcases hx with ⟨b, hb, rfl⟩
      rw [List.mem_range] at hb
      rw [List.mem_map]
      exact ⟨b / 8, List.mem_range.mpr (by simp only [byteCount]; omega), rfl⟩
    calc ((List.range w).map (fun b => i * (align 8 w / 8) + b / 8)).eraseDups.length
        ≤ ((List.range (byteCount w)).map (fun k => i * (align 8 w / 8) + k)).length :=
          (nodup_eraseDups _).length_le_of_subset hsub
      _ = byteCount w := by simp
  · have hsub : (List.range (byteCount w)).map (fun k => i * (align 8 w / 8) + k) ⊆
        ((List.range w).map (fun b => i * (align 8 w / 8) + b / 8)).eraseDups := by
      intro x hx
      rw [List.mem_map] at hx
      rcases hx with ⟨k, hk, rfl⟩
      rw [List.mem_range] at hk
      rw [List.mem_eraseDups, List.mem_map]
      refine ⟨k * 8, List.mem_range.mpr (by simp only [byteCount] at hk; omega), ?_⟩
      have : k * 8 / 8 = k := by omega
      simp [this]
    calc byteCount w
        = ((List.range (byteCount w)).map (fun k => i * (align 8 w / 8) + k)).length := by simp
      _ ≤ ((List.range w).map (fun b => i * (align 8 w / 8) + b / 8)).eraseDups.length :=
          (hnodup_shift _ _).length_le_of_subset hsub

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_PointBytes
