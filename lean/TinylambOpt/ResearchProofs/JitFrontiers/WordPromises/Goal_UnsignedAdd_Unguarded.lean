import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedAdd_Unguarded

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises
open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises.UnsignedAdd


theorem refute : ¬ UnsignedAdd.Unguarded.Goal := by
  intro h
  have hbad : ir UnsignedAdd.op (0xffffffffffffffff, 1) ≠ some ((0xffffffffffffffff : W) + 1) := by native_decide
  exact hbad (h _)

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedAdd_Unguarded
