import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
import TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_Lossless

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `SplitNibble.Transcode` — Convert from this physical order to tight row-major bytes
  using lane reads.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_Transcode

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble

-- Small-input truth checks, covering both regimes w ≤ 4 and w > 4, plus w = 0.
#eval pack 4 ((List.range 3).map (fun i => fetch layout 3 4 i (atByte (file layout 4 [1, 2, 3])))) == pack 4 [1, 2, 3]
#eval pack 0 ((List.range 3).map (fun i => fetch layout 0 3 i (atByte (file layout 0 [1, 2, 3])))) == pack 0 [1, 2, 3]
#eval pack 5 ((List.range 9).map (fun i => fetch layout 9 5 i (atByte (file layout 5 [31, 0, 7, 255, 16, 3, 9, 2, 77])))) == pack 5 [31, 0, 7, 255, 16, 3, 9, 2, 77]
#eval pack 9 ((List.range 6).map (fun i => fetch layout 6 9 i (atByte (file layout 9 [511, 0, 333, 255, 16, 77])))) == pack 9 [511, 0, 333, 255, 16, 77]

private theorem word_congr_mod (w : Nat) (xs ys : List Nat)
    (h : List.map (fun x => x % 2 ^ w) xs = List.map (fun x => x % 2 ^ w) ys) :
    word w xs = word w ys := by
  induction xs generalizing ys with
  | nil =>
      cases ys with
      | nil => rfl
      | cons y ys => simp at h
  | cons x xs ih =>
      cases ys with
      | nil => simp at h
      | cons y ys =>
          simp only [List.map_cons, List.cons.injEq] at h
          simp [word, h.1, ih ys h.2]

theorem sound_Transcode : SplitNibble.Transcode.Goal := by
  intro w xs
  have hfetch : (List.range xs.length).map
      (fun i => fetch layout xs.length w i (atByte (file layout w xs))) =
      xs.map (fun x => x % 2 ^ w) := by
    apply List.ext_getElem?
    intro k
    by_cases hk : k < xs.length
    · simp [hk, TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_Lossless.sound_Lossless w xs k hk]
    · simp [hk]
  rw [hfetch]
  unfold pack
  rw [word_congr_mod w (xs.map (fun x => x % 2 ^ w)) xs]
  · simp
  · simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_Transcode
