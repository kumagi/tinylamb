import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/-! The bounded row slice and the absolute gather read the same two digits, so execution may use the shared buffer directly. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_CivilTextGathers_FixedOffsetRowSlice

open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers
open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers.FixedOffsetRowSlice

theorem sound : Goal := by
  intro s start finish off hstart hfinish
  unfold part pair
  by_cases hguard : start + off + 2 ≤ finish
  · simp [hguard]
    have hlen : off + 2 ≤ finish - start := by omega
    have hoff : off < finish - start := by omega
    have hoff1 : off + 1 < finish - start := by omega
    simp [List.getElem?_drop, hoff, hoff1]
    congr 2 <;> omega
  · simp [hguard]
    have hlen : finish - start < off + 2 := by omega
    by_cases hoff : off < finish - start
    · have hoff1 : ¬ off + 1 < finish - start := by omega
      simp [List.getElem?_take, hoff1]
    · simp [List.getElem?_take, hoff]

end Nov26.Proof_CivilTextGathers_FixedOffsetRowSlice
