import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_MovieCredits_Remaining

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_MovieCredits_FractionalCap

open TinylambOpt.ResearchGoals.Atlas.JoinPlans.MovieCredits

/-- The fractional edge-cover bound for the JOB-style movie/credit/person
triangle join: the same result is proved in the per-model `Remaining` module
(support counting, three pairwise bounds, a Cauchy-Schwarz bound on `T^2`,
and the weighted induction over `den`). -/
theorem sound_MovieCredits_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.MovieCredits.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieCredits.Remaining.sound_FractionalCap

end TinylambOpt.ResearchProofs.Atlas.Goal_MovieCredits_FractionalCap
