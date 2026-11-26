import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

/-! The lane-major fallback is refuted: an error in an earlier lane of the
    primary packet can be hidden by a fault selected from the secondary packet. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BailoutPackets_LaneMajorSitePriority

open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.LaneMajorSitePriority.Goal := by
  intro h
  let a : List Cell := [.ok (some 0), .error 5]
  let b : List Cell := [.error 7, .ok (some 1)]
  have hh := h a b (by native_decide)
  have hne : (((first a).orElse (fun _ => first b)) ≠
      (first (a.zipWith (fun x y => if bad x then x else y) b))) := by native_decide
  exact hne hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_BailoutPackets_LaneMajorSitePriority
