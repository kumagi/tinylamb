import TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_Lossless

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `TileTranspose.Transcode` — Convert from this physical order to tight row-major bytes
  using lane reads.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_Transcode

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose
open TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_Lossless

private theorem word_map_mod (w : Nat) (xs : List Nat) :
    word w (xs.map (fun x => x % 2 ^ w)) = word w xs := by
  induction xs with
  | nil => simp [word]
  | cons x xs ih => simp [word, ih, Nat.mod_mod]

private theorem getD_eq_map (xs : List Nat) (f : Nat → Nat) :
    (List.range xs.length).map (fun i => f (xs[i]?.getD 0)) = xs.map f := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have h : (List.range (x :: xs).length).map
          (fun i => f ((x :: xs)[i]?.getD 0)) =
          f x :: (List.range xs.length).map (fun i => f (xs[i]?.getD 0)) := by
        rw [show (x :: xs).length = xs.length + 1 from rfl, List.range_succ_eq_map]
        simp [List.getElem?_cons_succ]
      rw [h, ih]
      rfl

/-- Lossless lane reads convert the tile-transposed frame to tight row-major
words; reduction modulo the lane width leaves packed words unchanged. -/
theorem sound_Transcode : TileTranspose.Transcode.Goal := by
  intro w xs
  have hfetch : (List.range xs.length).map
      (fun i => fetch layout xs.length w i (atByte (file layout w xs))) =
      xs.map (· % 2 ^ w) := by
    rw [← getD_eq_map xs (· % 2 ^ w)]
    apply List.map_congr_left
    intro i hi
    have hi' : i < xs.length := by simpa using hi
    exact sound_Lossless w xs i hi'
  rw [hfetch]
  unfold pack
  simp only [List.length_map, List.length_range]
  rw [← word_map_mod w xs]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_Transcode
