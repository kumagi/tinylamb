import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MorselRetirement.Goal_EmptyMorselRetirement

open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement


/-!
A completed empty filter result still advances the contiguous retirement frontier: the empty row list is emitted as no rows and the next morsel becomes the new head.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

private theorem readyIds_cons (rs : List Report) (start count : Nat) (h : (lookup rs start).isSome = true) :
    readyIds rs start (count + 1) = start :: readyIds rs (start + 1) count := by
  unfold readyIds
  rw [← List.range'_eq_map_range (s := start) (n := count + 1)]
  rw [← List.range'_eq_map_range (s := start + 1) (n := count)]
  rw [List.range'_succ]
  simp [h]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.EmptyMorselRetirement.Goal := by
  intro rs start n hsome
  have hs : (lookup rs start).isSome = true := by simp [hsome]
  unfold emit
  rw [readyIds_cons rs start n hs]
  simp [emit, hsome]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MorselRetirement.Goal_EmptyMorselRetirement
