import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/-! In a canonical nineteen-byte row both two-byte components admit a four-byte
load: the largest starting offset leaves two bytes of slack. The proof preserves
the modeled gather result at the final row as well. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_CanonicalHourMinuteWideGather

open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

theorem sound : CanonicalHourMinuteWideGather.Goal := by
  intro start limit off hstart hoff
  simp only [List.mem_cons, List.mem_nil_iff, or_false] at hoff
  rcases hoff with rfl | rfl <;> omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_CanonicalHourMinuteWideGather
