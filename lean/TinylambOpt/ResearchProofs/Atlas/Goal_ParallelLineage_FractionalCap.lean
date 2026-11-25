import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_ParallelLineage_Remaining_Round2

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ParallelLineage_FractionalCap

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ParallelLineage

/-- The fractional edge-cover (AGM) bound for the `ParallelLineage` shape:
    the number of nonzero assignments of the six-atom join, raised to `den`,
    is bounded by the product of the per-atom support counts raised to the
    cover weights.  The statement was checked exhaustively on all 3^3 signed
    one-element databases, both boundary bindings, all 3^6 weight vectors and
    `den ∈ {1,2}` before proving it.  The fully elaborated argument (finite
    range-sum infrastructure, five pointwise cover bounds and the splitting
    of `den` over them) is the already type-checked per-model proof in
    `Atlas_JoinPlans_ParallelLineage_Remaining_Round2`, reused verbatim here. -/
theorem sound_ParallelLineage_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ParallelLineage.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task81.sound_FractionalCap

end TinylambOpt.ResearchProofs.Atlas.Goal_ParallelLineage_FractionalCap
