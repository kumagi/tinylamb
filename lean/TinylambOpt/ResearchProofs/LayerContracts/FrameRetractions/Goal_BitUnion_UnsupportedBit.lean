import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_UnsupportedBit

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : BitUnion.UnsupportedBit.Goal := by
  intro xs x i hx hall
  by_cases hlen : xs.length = 1
  · have hxs : xs = [x] := by
      cases xs with
      | nil => simp at hlen
      | cons y ys =>
          simp at hlen
          subst ys
          have hxy : x = y := by simpa using hx
          subst y
          rfl
    subst xs
    simp [repaired, dropOne, BitUnion.model]
  have hlenb : (xs.length == 1) = false := by
    cases hb : xs.length == 1
    · rfl
    · have hn : xs.length = 1 := by simpa using hb
      exact False.elim (hlen hn)
  let v := xs.foldl (fun a b => a ||| b) 0
  let w := (xs.erase x).foldl (fun a b => a ||| b) 0
  have hvw : v = x ||| w := by
    simpa [v, w] using (or_erase_of_mem hx)
  have hall' : ∀ z, z ∈ xs.erase x → z.testBit i = false := by
    intro z hz
    have hz' := (List.all_eq_true.mp hall) z hz
    cases hzbit : z.testBit i <;> simp_all
  have hbitw : w.testBit i = false := by
    simpa [w] using (testBit_or_fold_false (xs.erase x) i hall')
  have hvalget : (value BitUnion.model (xs.erase x)).getD 0 = w := by
    by_cases hne : xs.erase x = []
    · simp [value, BitUnion.model, hne, w]
    · simp [value, BitUnion.model, isEmpty_false_of_ne_nil hne, w]
  by_cases hcase : x == 0 || v == 0
  · have hxv : x = 0 ∨ v = 0 := by simpa [v] using hcase
    have hrep : repaired BitUnion.model xs x = some v := by
      simp [repaired, dropOne, BitUnion.model, hlenb, hxv, v]
    rw [hrep]
    simp only [Option.getD_some]
    rcases hxv with hx0 | hv0
    · subst x
      simpa [hvw] using hbitw
    · simp [hv0]
  · have hnxv : ¬ (x = 0 ∨ v = 0) := by simpa [v] using hcase
    have hrep : repaired BitUnion.model xs x = value BitUnion.model (xs.erase x) := by
      simp [repaired, dropOne, BitUnion.model, hlenb, hnxv, v]
    rw [hrep, hvalget]
    exact hbitw

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_UnsupportedBit
