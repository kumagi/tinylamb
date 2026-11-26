import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/-! A canonical nineteen-byte row has room for the four-byte gathers at both hour and minute offsets. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_CivilTextGathers_CanonicalHourMinuteWideGather

open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers
open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers.CanonicalHourMinuteWideGather

theorem sound : Goal := by
  intro start limit off hstart hoff
  simp only [List.mem_cons, List.mem_nil_iff, or_false] at hoff
  rcases hoff with rfl | rfl <;> omega

end Nov26.Proof_CivilTextGathers_CanonicalHourMinuteWideGather
