import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

-- Refutation: a previously selected entry may be shadowed by a higher-priority guard on a new row.

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_ReuseLastPassingEntry

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.ReuseLastPassingEntry.Goal := by
  intro h
  have c := h
    ([⟨1, 1, 1⟩, ⟨2, 2, 2⟩] : List TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.Entry)
    (2 : BitVec 64) (3 : BitVec 64) ⟨2, 2, 2⟩ (by native_decide) (by native_decide)
  exact (by native_decide : ¬ (
    TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.dispatch
      ([⟨1, 1, 1⟩, ⟨2, 2, 2⟩] : List TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.Entry)
      (3 : BitVec 64) = some (2 : Nat))) c

end TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_ReuseLastPassingEntry
