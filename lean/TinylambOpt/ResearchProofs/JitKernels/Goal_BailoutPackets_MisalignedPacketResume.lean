import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

-- Refutation: a vector-aligned resume cursor can skip committed output after a fault.

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BailoutPackets_MisalignedPacketResume

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.MisalignedPacketResume.Goal := by
  intro h
  have c := h ([.ok (some 1), .error 0, .ok (some 2)] :
    List TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.Cell) 1 0 4
      (by native_decide) (by native_decide)
  exact (by native_decide : ¬ (
    TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.prefixOut
      (([.ok (some 1), .error 0, .ok (some 2)] :
        List TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.Cell).drop (1 + 1)) =
    TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.prefixOut
      (([.ok (some 1), .error 0, .ok (some 2)] :
        List TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.Cell).drop ((1 / 4 + 1) * 4)))) c

end TinylambOpt.ResearchProofs.JitKernels.Goal_BailoutPackets_MisalignedPacketResume
