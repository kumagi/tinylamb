import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

/-! A membership bitmap loses multiplicity, so a selection vector with a
    repeated ordinal cannot be replaced by a compress for SUM. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SelectionNetworks_BitmapSelectionMultiplicity

open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.BitmapSelectionMultiplicity.Goal := by
  intro h
  have hh := h [1] [0,0]
  have hne : ((gather [1] [0,0]).sum ≠
      (compress [1] ((List.range [1].length).map [0,0].contains)).sum) := by native_decide
  exact hne hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_SelectionNetworks_BitmapSelectionMultiplicity
