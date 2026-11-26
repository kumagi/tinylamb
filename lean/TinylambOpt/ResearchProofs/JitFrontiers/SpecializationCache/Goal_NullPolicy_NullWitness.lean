import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `NullPolicy.NullWitness` — `∀ c, c.slot=0 → c.mode=true → shape.code c [none]=.val
  c.literal`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_NullPolicy_NullWitness

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.NullPolicy
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

private theorem get0 (v : Int) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get [some v] 0 = Out.val v := by rfl

private theorem get0none :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get [none] 0 = Out.null := by rfl

theorem sound : NullPolicy.NullWitness.Goal := by
  intro c hs hm
  simp [NullPolicy.shape, hs, hm, get0none]

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_NullPolicy_NullWitness
