import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises
import TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.BV64

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_DisjointOr_SelfMask

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : DisjointOr.SelfMask.Goal := by
  intro x
  dsimp [DisjointOr.op]
  rw [BV64.and_eq_zero_iff_beq]
  rw [Bool.beq_eq_decide_eq, decide_eq_true_eq, BitVec.and_self]
  simp

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_DisjointOr_SelfMask
