import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
import TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_Lossless

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_DictionaryProbe

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble

/-- Small-input truth checks: both width regimes, short dictionaries (some
    lookups return `none`), and repeated / out-of-order keys. -/
private def probeEq (w : Nat) (xs dict keys : List Nat) : Bool :=
  (keys.map (fun i => dict[fetch layout xs.length w i (atByte (file layout w xs))]?)) ==
  (keys.map (fun i => dict[xs[i]?.getD 0 % 2^w]?))

#eval probeEq 4 [1, 2, 3] [0, 10, 20, 30] [0, 1, 2]
#eval probeEq 4 [1, 2, 3] [0, 10] [2, 0, 1, 1]
#eval probeEq 6 [1, 17, 48, 63, 7] [0, 1, 2, 3, 4, 5, 6, 7] [3, 0, 4, 4, 1]
#eval probeEq 0 [9, 9] [5] [0, 1]

/-- A dictionary lookup only depends on the decoded value, so losslessness of
    the layout suffices: replace the fetched value by the original lane value. -/
private theorem dictionaryProbe_of_lossless (l : Layout) (hloss : lossless l) : dictionaryProbe l := by
  intro w xs dict keys hkeys
  apply List.map_congr_left
  intro i hi
  rw [hloss w xs i (hkeys i hi)]

theorem sound_DictionaryProbe : SplitNibble.DictionaryProbe.Goal := by
  exact dictionaryProbe_of_lossless layout
    (TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_Lossless.sound_Lossless)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_DictionaryProbe
