import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_AlignedMarginals

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Couplings

/-- Counterexample: the rows [(0,1),(1,0)] have identical column marginals,
    yet their per-row absolute differences sum to 2, not 0. -/
theorem disproof_Couplings_AlignedMarginals :
    ¬ Couplings.AlignedMarginals.Goal := by
  intro h
  let rs : Couplings.Rows := [(0, 1), (1, 0)]
  have hperm : (rs.map Prod.fst).Perm (rs.map Prod.snd) := by native_decide
  have hcost : Couplings.cost rs = 2 := by native_decide
  have hz := h rs hperm
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_AlignedMarginals
