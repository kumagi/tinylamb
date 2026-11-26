import TinylambOpt.ResearchProofs.JitKernels.Proof_FloatingReduction_ExactBinaryPowerScale

/-! Aggregation alias for the exact binary-power-scale certificate. The proof
shows that a JIT kernel may distribute a factor of two into both summands when
the operands and their doublings stay finite; this alias registers that
soundness result with the `JitKernels.FloatingReduction` proof aggregates.
-/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_ExactBinaryPowerScale

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

theorem sound : ExactBinaryPowerScale.Goal :=
  Nov26.Proof_FloatingReduction_ExactBinaryPowerScale.sound

end TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_ExactBinaryPowerScale
