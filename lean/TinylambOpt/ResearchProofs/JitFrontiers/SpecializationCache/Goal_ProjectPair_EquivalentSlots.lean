import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `ProjectPair.EquivalentSlots` — `∀ (c : Cfg) r, shape.code {c with mode:=true}
  r=shape.code {c with mode:=false,slot:=c.slot+1} r`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ProjectPair_EquivalentSlots

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.ProjectPair
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : ProjectPair.EquivalentSlots.Goal := by
  intro c r
  rfl

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ProjectPair_EquivalentSlots
