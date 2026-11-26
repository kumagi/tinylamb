import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common_PredicateBallots
import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes_PredicateBallots

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ExclusiveOr_MinimalDemand

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes

theorem refute : ¬ ExclusiveOr.MinimalDemand.Goal := by
  intro h
  have hh := h (⟨1,0,1⟩ : Bits) 0
  have hleft : (well (⟨1,0,1⟩ : Bits) → 0 < 64 → (ExclusiveOr.test.demand (⟨1,0,1⟩ : Bits)).getLsbD 0 = true) := by
    intro hw
    simp [well] at hw
  have hright : ¬ ∃ u v : T, ExclusiveOr.test.scalar (lane (⟨1,0,1⟩ : Bits) 0) u ≠ ExclusiveOr.test.scalar (lane (⟨1,0,1⟩ : Bits) 0) v := by
    rintro ⟨u,v,hne⟩
    apply hne
    cases u <;> cases v <;> native_decide
  exact hright (hh.mp hleft)

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ExclusiveOr_MinimalDemand
