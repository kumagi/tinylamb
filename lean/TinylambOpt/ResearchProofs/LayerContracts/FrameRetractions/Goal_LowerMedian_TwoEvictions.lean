import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_LowerMedian

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `LowerMedian.TwoEvictions` — Repaired deletions in either order agree, including
  repeated outgoing values.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_TwoEvictions

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.CommonLowerMedian
open List

theorem sound : LowerMedian.TwoEvictions.Goal := by
  intro xs x y hx hy
  have hrep : ∀ (ys : List Nat) (z : Nat), z ∈ ys →
      repaired LowerMedian.model ys z = value LowerMedian.model (ys.erase z) := by
    intro ys z hz
    unfold repaired
    cases hd : dropOne LowerMedian.model ys z with
    | none => simp [hd]
    | some out =>
        have hv := fastRetraction_sound ys z out hz hd
        simp [hd, hv]
  by_cases hxy : x = y
  · subst y
    rfl
  have hxiny : x ∈ xs.erase y := (List.mem_erase_of_ne hxy).2 hx
  rw [hrep (xs.erase x) y hy, hrep (xs.erase y) x hxiny]
  have hperm : ((xs.erase x).erase y).Perm ((xs.erase y).erase x) :=
    erase_erase_perm xs x y hx hy
  have hlen := hperm.length_eq
  have hmed := lowerMedian_of_perm hperm
  by_cases hn1 : (xs.erase x).erase y = []
  · have hn2 : (xs.erase y).erase x = [] := by
      apply List.eq_nil_of_length_eq_zero
      rw [← hlen]
      simp [hn1]
    simp [value, LowerMedian.model, hn1, hn2]
  · have hn2 : (xs.erase y).erase x ≠ [] := by
      intro h
      apply hn1
      apply List.eq_nil_of_length_eq_zero
      rw [hlen]
      simp [h]
    simp [value, LowerMedian.model, List.isEmpty_eq_false_iff.mpr hn1,
      List.isEmpty_eq_false_iff.mpr hn2, hmed]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_TwoEvictions
