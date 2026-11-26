import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
Length, extrema and NULL presence do not determine membership of a build list.
Two lists of length three with the same minimum, maximum and no NULL can differ
in their interior value, so sharing one cached bitset changes the IN result.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.InListLowering.Goal_SameCardinalityConstantSetCache

open TinylambOpt.ResearchGoals.JitKernels.InListLowering

theorem refute : ¬ SameCardinalityConstantSetCache.Goal := by
  intro h
  have hmn : ([some (0 : Int), some 1, some 2].filterMap (fun z => z)).min? =
      ([some (0 : Int), some 0, some 2].filterMap (fun z => z)).min? := by decide
  have hmx : ([some (0 : Int), some 1, some 2].filterMap (fun z => z)).max? =
      ([some (0 : Int), some 0, some 2].filterMap (fun z => z)).max? := by decide
  have hc : ([some (0 : Int), some 1, some 2] : Keys).contains none =
      ([some (0 : Int), some 0, some 2] : Keys).contains none := by decide
  have t := h [some (0 : Int), some 1, some 2] [some (0 : Int), some 0, some 2] (some 1)
    (by decide) hmn hmx hc
  simp [test] at t

end TinylambOpt.ResearchProofs.JitKernels.InListLowering.Goal_SameCardinalityConstantSetCache
