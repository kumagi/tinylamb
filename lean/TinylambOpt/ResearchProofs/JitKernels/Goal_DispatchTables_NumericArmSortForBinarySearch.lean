import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Refutation: sorting CASE arms by lower bound destroys first-match priority
when ranges overlap. A narrow arm listed first wins at its key; after sorting, a
wider arm with a smaller lower bound wins instead, changing the SQL result. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_NumericArmSortForBinarySearch

open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

theorem refute : ¬ NumericArmSortForBinarySearch.Goal := by
  intro h
  let arms : List Arm := [{ lo := 2, hi := 3, result := some 2 }, { lo := 0, hi := 10, result := some 1 }]
  have hbad := h arms 2
  have hc : pick (arms.mergeSort (fun a b => decide (a.lo ≤ b.lo))) 2 ≠ pick arms 2 := by
    native_decide
  exact hc hbad

end TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_NumericArmSortForBinarySearch
