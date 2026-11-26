import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `BoundComparison.ThresholdEdge` — `∀ c, c.slot=0 → shape.code c [some c.literal]=.val
  0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_BoundComparison_ThresholdEdge

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.BoundComparison
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

private theorem get0 (v : Int) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get [some v] 0 = Out.val v := by rfl

private theorem get0none :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get [none] 0 = Out.null := by rfl

theorem sound : BoundComparison.ThresholdEdge.Goal := by
  intro c h
  simp [BoundComparison.shape, h, get0, mapV]

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_BoundComparison_ThresholdEdge
