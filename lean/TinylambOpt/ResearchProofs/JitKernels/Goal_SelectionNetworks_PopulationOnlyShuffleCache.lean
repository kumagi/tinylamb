import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

-- Refutation: equal mask populations at different positions select different rows.

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SelectionNetworks_PopulationOnlyShuffleCache

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.PopulationOnlyShuffleCache.Goal := by
  intro h
  have c := h ([1, 2] : List Nat) [true, false] [false, true]
    (by native_decide) (by native_decide) (by native_decide)
  exact (by native_decide : ¬ (
    TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.compress ([1, 2] : List Nat) [true, false] =
    TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.compress ([1, 2] : List Nat) [false, true])) c

end TinylambOpt.ResearchProofs.JitKernels.Goal_SelectionNetworks_PopulationOnlyShuffleCache
