import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedSub_SameSign

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : SignedSub.SameSign.Goal := by
  intro x y h
  unfold negative at h
  dsimp [SignedSub.op]
  unfold negative
  rw [BitVec.getLsbD_and, BitVec.getLsbD_xor]
  rw [show (x.getLsbD 63) = y.getLsbD 63 from h]
  cases y.getLsbD 63 <;> simp

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedSub_SameSign
