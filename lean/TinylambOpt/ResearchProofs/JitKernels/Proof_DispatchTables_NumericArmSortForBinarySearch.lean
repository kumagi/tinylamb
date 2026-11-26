import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-! FALSE: Sorting CASE arms by lower bound destroys first-match priority among overlapping
arms. Counterexample: arms `[5,6) ↦ 2` then `[0,10) ↦ 1`; key 5 matches the narrower first
arm in the original order, but after sorting by `lo` the wide arm `[0,10)` precedes it and
returns 1 instead. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DispatchTables_NumericArmSortForBinarySearch
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-- Refutation of result preservation under lower-bound sorting. -/
theorem refute : ¬ NumericArmSortForBinarySearch.Goal := by
  intro h
  have key := h [⟨5, 6, some 2⟩, ⟨0, 10, some 1⟩] 5
  have hs : ([⟨5, 6, some 2⟩, ⟨0, 10, some 1⟩] : List Arm).mergeSort
      (fun a b => decide (a.lo ≤ b.lo)) = [⟨0, 10, some 1⟩, ⟨5, 6, some 2⟩] := by
    simp [List.mergeSort, List.merge]
  rw [hs] at key
  exact absurd key (by decide)

end TinylambOpt.ResearchProofs.JitKernels.DispatchTables_NumericArmSortForBinarySearch
