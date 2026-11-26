import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedSub_Unguarded

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises
open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises.UnsignedSub


theorem refute : ¬ UnsignedSub.Unguarded.Goal := by
  intro h
  have hbad : ir UnsignedSub.op (0, 1) ≠ some ((0 : W) - 1) := by native_decide
  exact hbad (h _)

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedSub_Unguarded
