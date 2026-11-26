import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/-! Reading a two-byte field through the bounded row slice is equivalent to
reading it at the absolute offset: the slice has exactly the rows in
`[start, finish)`, and the guard ensures both bytes are inside it. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_FixedOffsetRowSlice

open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

theorem sound : FixedOffsetRowSlice.Goal := by
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

end TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_FixedOffsetRowSlice
