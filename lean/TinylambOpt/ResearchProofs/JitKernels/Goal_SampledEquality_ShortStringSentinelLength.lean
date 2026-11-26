import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

/-!
For strings of at most four bytes, zero padding after the last nonzero byte
encodes the length as well as the content. A padded word comparison is
therefore an exact equality test when both last bytes are nonzero.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_ShortStringSentinelLength

open TinylambOpt.ResearchGoals.JitKernels.SampledEquality

theorem sound : TinylambOpt.ResearchGoals.JitKernels.SampledEquality.ShortStringSentinelLength.Goal := by
  intro a b halen hblen hlasta hlastb hpad
  rcases a with _ | ⟨a0, _ | ⟨a1, _ | ⟨a2, _ | ⟨a3, a4⟩⟩⟩⟩ <;>
    rcases b with _ | ⟨b0, _ | ⟨b1, _ | ⟨b2, _ | ⟨b3, b4⟩⟩⟩⟩ <;>
    simp_all <;> grind

end TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_ShortStringSentinelLength