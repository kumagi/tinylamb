import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
set_option linter.unusedSimpArgs false

/-! A strided lane scan only adds deltas from the same residue class, while a
logical row uses the whole prefix. With nonnegative deltas the two disagree at
row one of `[1,2,3]` with stride two. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_CrossLanePrefixIsolation

open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

theorem refute : ¬ CrossLanePrefixIsolation.Goal := by
  intro h
  have hw := h 0 [1, 2, 3] 1 2 (by decide) (by decide) (by decide)
  have hs : strideScan 2 0 [1, 2, 3] 1 = 2 := by native_decide
  have hd : (2 : Int) ≠ TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.get 0 [1, 2, 3] 1 := by
    native_decide
  exact hd (hs.symm.trans hw)

end TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_CrossLanePrefixIsolation
