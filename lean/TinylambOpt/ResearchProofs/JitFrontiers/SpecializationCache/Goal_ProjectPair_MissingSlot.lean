import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `ProjectPair.MissingSlot` — `∀ c r, r.length≤c.slot → shape.code c r=.fault`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ProjectPair_MissingSlot

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.ProjectPair
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : ProjectPair.MissingSlot.Goal := by
  intro c r h
  unfold ProjectPair.shape
  dsimp
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  rw [List.getElem?_eq_none (by omega : r.length ≤ c.slot + c.mode.toNat)]

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ProjectPair_MissingSlot
