import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_FixedOffsetRowSlice

open TinylambOpt.ResearchGoals.JitKernels

theorem sound : TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers.FixedOffsetRowSlice.Goal := by
  intro s start finish off hstart hfinish
  simp only [CivilTextGathers.part, CivilTextGathers.pair]
  by_cases h : start + off + 2 ≤ finish
  · have hoff1 : off < finish - start := by omega
    have hoff2 : off + 1 < finish - start := by omega
    have h1 : start + off < s.length := by omega
    have h2 : start + off + 1 < s.length := by omega
    have hcomm : ∀ x y : Nat, y - 48 + 10 * (x - 48) = 10 * (x - 48) + (y - 48) := by
      intro x y
      omega
    have hsidx : s[start + (off + 1)]? = some s[start + off + 1] := by
      rw [show start + (off + 1) = start + off + 1 by omega]
      simp [h2]
    simp [h, hoff1, hoff2, h1, h2, hcomm, hsidx, List.getElem?_drop]
  · have hnot : ¬ off + 1 < finish - start := by omega
    have hidx : (List.take (finish - start) (List.drop start s))[off + 1]? = none := by
      rw [List.getElem?_take]
      simp [hnot]
    simp [h, hidx]

end TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_FixedOffsetRowSlice
