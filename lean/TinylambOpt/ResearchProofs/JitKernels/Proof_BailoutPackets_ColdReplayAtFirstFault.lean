import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets
import TinylambOpt.ResearchProofs.JitKernels.BailoutPacketsLemmas

/-! Replaying from the reported physical fault row observes that fault in lane
zero. Execution may drop the already committed prefix and start scalar replay at
the reported row. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_BailoutPackets_ColdReplayAtFirstFault

open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets
open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.ColdReplayAtFirstFault
open Nov26.BailoutPacketsLemmas

theorem sound : Goal := by
  intro xs i e h
  have h0 : firstAt xs 0 = some (0 + i, e) := by
    have heq : first xs = firstAt xs 0 := by rfl
    rw [heq] at h
    simpa using h
  have hd := firstAt_drop xs 0 i e h0
  change firstAt (xs.drop i) 0 = some (0,e)
  exact hd

end Nov26.Proof_BailoutPackets_ColdReplayAtFirstFault
