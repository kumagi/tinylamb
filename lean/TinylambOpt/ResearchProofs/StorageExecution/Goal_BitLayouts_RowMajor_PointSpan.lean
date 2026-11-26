import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `RowMajor.PointSpan` — `∀ n w i, (footprint layout n w i).length ≤ byteCount (w+7)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_PointSpan

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor

/-- Removing duplicates cannot increase a list's length. -/
private theorem length_eraseDups_le {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) : xs.eraseDups.length ≤ xs.length := by
  match xs with
  | [] => simp
  | x :: xs =>
      rw [List.eraseDups_cons]
      simp only [List.length_cons]
      have h1 := length_eraseDups_le (xs.filter (fun y => !y == x))
      have h2 := List.length_filter_le (fun y => !y == x) xs
      omega
  termination_by xs.length
  decreasing_by
    simp_wf
    exact Nat.lt_succ_of_le (List.length_filter_le _ _)

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

/-- The bytes touched by a contiguous span of `w` bits are contained in at
most `byteCount (w + 7)` consecutive addresses starting at its first byte. -/
theorem sound_PointSpan : RowMajor.PointSpan.Goal := by
  intro n w i
  unfold footprint layout lanes byteCount
  let k := (w + 14) / 8
  have hsub : ((List.range w).map (fun b => (i * w + b) / 8)).eraseDups ⊆
      (List.range k).map (fun j => i * w / 8 + j) := by
    intro x hx
    rw [List.mem_eraseDups, List.mem_map] at hx
    obtain ⟨b, hb, rfl⟩ := hx
    have hb' : b < w := List.mem_range.mp hb
    have hbase := Nat.mod_add_div (i * w) 8
    have hs : i * w % 8 < 8 := Nat.mod_lt _ (by decide)
    have hdiff : (i * w + b) / 8 - i * w / 8 = (i * w % 8 + b) / 8 := by omega
    rw [List.mem_map]
    refine ⟨(i * w + b) / 8 - i * w / 8, ?_, ?_⟩
    · rw [List.mem_range, hdiff]
      dsimp [k]
      omega
    · omega
  have hnodup : ((List.range k).map (fun j => i * w / 8 + j)).Nodup := by
    apply List.Pairwise.map (R := (· ≠ ·)) (S := (· ≠ ·))
      (f := fun j => i * w / 8 + j)
    · intro a b h
      omega
    · exact List.nodup_iff_pairwise_ne.mp List.nodup_range
  calc ((List.range w).map (fun b => (i * w + b) / 8)).eraseDups.length
      ≤ ((List.range k).map (fun j => i * w / 8 + j)).length :=
        (nodup_eraseDups _).length_le_of_subset hsub
    _ = k := by simp
    _ = (w + 7 + 7) / 8 := rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_PointSpan
