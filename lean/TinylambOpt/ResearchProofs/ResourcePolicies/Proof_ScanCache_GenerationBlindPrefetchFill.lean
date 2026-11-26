import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache

namespace TinylambOpt.ResearchProofs.ResourcePolicies.ScanCache.Goal_GenerationBlindPrefetchFill

open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache


/-!
A speculative response keyed only by file and block is not interchangeable
across generations: the reader returns the generation itself in the
counterexample, so an older response would install stale bytes.  This models
why prefetch admission must carry the immutable file generation.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false



open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache.GenerationBlindPrefetchFill.Goal := by
  intro h
  let readBlock : Request → List Nat := fun r => [r.generation]
  have hbad := h readBlock 0 0 0 1 (by decide)
  simp [readBlock] at hbad
end TinylambOpt.ResearchProofs.ResourcePolicies.ScanCache.Goal_GenerationBlindPrefetchFill
