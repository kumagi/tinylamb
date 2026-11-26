import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-! Sorting CASE arms by lower bound is refuted: an overlapping arm with a
    larger lower bound can lose first-match priority at an interior key. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_NumericArmSortForBinarySearch

open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.DispatchTables.NumericArmSortForBinarySearch.Goal := by
  intro h
  let a : Arm := { lo := 1, hi := 10, result := some 1 }
  let b : Arm := { lo := 0, hi := 5, result := some 2 }
  have hh := h [a,b] 1
  have hne : (pick ([a,b].mergeSort (fun x y => decide (x.lo ≤ y.lo))) 1 ≠ pick [a,b] 1) := by
    native_decide
  exact hne hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_NumericArmSortForBinarySearch
