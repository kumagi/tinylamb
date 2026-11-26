import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_LowerMedian

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `LowerMedian.CombineFrames` — An admitted merge reconstructs the final scalar without
  retaining input rows.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_CombineFrames

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.CommonLowerMedian
open List

theorem sound : LowerMedian.CombineFrames.Goal := by
  intro a b out hm
  by_cases ha : a = []
  · subst a
    simp [merged] at hm
    simpa using hm
  by_cases hb : b = []
  · subst b
    simp [merged, ha] at hm
    simpa using hm
  · have hais : a.isEmpty = false := List.isEmpty_eq_false_iff.mpr ha
    have hbis : b.isEmpty = false := List.isEmpty_eq_false_iff.mpr hb
    simp [merged, hais, hbis, LowerMedian.model] at hm
    rcases hm with ⟨hvw, hzout⟩
    have happ : lowerMedian (a ++ b) = lowerMedian a :=
      lowerMedian_append_of_eq ha hb (rfl : lowerMedian a = lowerMedian a) hvw.symm
    have hnonempty : a ++ b ≠ [] := by simp [ha]
    simpa [value, LowerMedian.model, List.isEmpty_eq_false_iff.mpr hnonempty, happ] using hzout

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_CombineFrames
