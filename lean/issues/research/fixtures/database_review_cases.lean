import TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex
import TinylambOpt.ResearchGoals.DatabaseCorrectness.WinnerRecovery

/-! Witnesses for implementation assumptions identified in the correctness review.
Each example compares observable outputs. The sorter example tracks occupied
positions, without assuming any particular value for a moved-from C++ row. -/

namespace DatabaseReviewCases

section
open TinylambOpt.ResearchGoals.DatabaseCorrectness.WinnerRecovery

-- A transaction writes before the cut and commits after it. Collecting winners
-- independently on each side loses its update; collecting them on the full log retains it.
#guard
  let pre : List Entry := [⟨5, .put 3 (some 7)⟩]
  let post : List Entry := [⟨5, .commit⟩]
  replay post 3 (replay pre 3 none) == none && replay (pre ++ post) 3 none == some 7
end

section
open TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex

-- Physical candidates come from the current index. An update moved position 4
-- out of range; snapshot rechecking alone cannot find an absent candidate.
#guard
  let oldKey : Key := [[97]]
  let newKey : Key := [[98]]
  let old : Heap := fun i => if i == 4 then some oldKey else none
  let now : Heap := fun i => if i == 4 then some newKey else none
  let inRange := (· == oldKey)
  let physical := scan [4] now inRange
  scan [4] old inRange == [4] && checkedIndex physical [] old inRange == [] &&
    checkedIndex physical [4] old inRange == [4]
end

-- RefreshStreamKey can fail after moving a winner while another row remains
-- in the same block. AdvanceMergeStream then did not discard the old block.
-- Prepending to the block and resetting the index revisits a moved-from slot.
#guard
  let block : List (Option Nat) := [some 5, some 6]
  let afterMove := block.set 0 none
  let afterPrepend := some 5 :: afterMove
  let afterRetry := afterPrepend.drop 1
  afterRetry == [none, some 6] && afterRetry.length != (block.drop 1).length

end DatabaseReviewCases
