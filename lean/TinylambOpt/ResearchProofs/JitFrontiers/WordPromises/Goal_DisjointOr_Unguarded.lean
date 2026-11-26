import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_DisjointOr_Unguarded

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises
open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises.DisjointOr


theorem refute : ¬ DisjointOr.Unguarded.Goal := by
  intro h
  have hbad : ir DisjointOr.op (1, 1) ≠ some ((1 : W) ||| 1) := by native_decide
  exact hbad (h _)

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_DisjointOr_Unguarded
