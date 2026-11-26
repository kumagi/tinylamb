import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `BitUnion.FallbackNecessary` — A rejected deletion has indistinguishable old
  summaries requiring different results.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_FallbackNecessary

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : BitUnion.FallbackNecessary.Goal := by
  intro xs x hx hd
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
    simp [dropOne] at hd
  have hlenb : (xs.length == 1) = false := by
    cases hb : xs.length == 1
    · rfl
    · have hn : xs.length = 1 := by simpa using hb
      exact False.elim (hlen hn)
  let v := xs.foldl (fun a b => a ||| b) 0
  have hd' := hd
  simp [dropOne, BitUnion.model, hlenb, v] at hd'
  have hxne0 : x ≠ 0 := hd'.1
  have hgt : 1 < xs.length := by
    have hp : 0 < xs.length := List.length_pos_of_mem hx
    omega
  have hne : xs.erase x ≠ [] := by
    intro h
    have hl := congrArg List.length h
    rw [List.length_erase_of_mem hx] at hl
    simp at hl
    omega
  have hllen : (xs.erase x).length = xs.length - 1 := List.length_erase_of_mem hx
  have hne_xs : xs ≠ [] := by
    intro h
    subst xs
    simp at hx
  let w := (xs.erase x).foldl (fun a b => a ||| b) 0
  have hvw : v = x ||| w := by
    simpa [v, w] using (or_erase_of_mem hx)
  have hsub : x ||| v = v := by
    rw [hvw]
    rw [← Nat.or_assoc, Nat.or_self]
  have build : ∀ c, c ||| x = v → c ≠ w →
      ∃ ys, x ∈ ys ∧ ys.length = xs.length ∧
        value BitUnion.model ys = value BitUnion.model xs ∧
        value BitUnion.model (ys.erase x) ≠ value BitUnion.model (xs.erase x) := by
    intro c hc_or hcne
    let ys := x :: (xs.erase x).map (fun _ => c)
    refine ⟨ys, ?_, ?_, ?_, ?_⟩
    · simp [ys]
    · simp [ys, hllen]
      omega
    · have hysne : ys ≠ [] := by simp [ys]
      have hmapfold : ((xs.erase x).map (fun _ => c)).foldl (fun a b => a ||| b) 0 = c := by
        rw [or_fold_map_const]
        simp [hne]
      have hysfold : ys.foldl (fun a b => a ||| b) 0 = v := by
        simp [ys]
        rw [foldl_or_acc, hmapfold]
        simpa [Nat.or_comm] using hc_or
      simp [value, BitUnion.model, isEmpty_false_of_ne_nil hysne,
        isEmpty_false_of_ne_nil hne_xs, hysfold, v]
    · have herase : ys.erase x = (xs.erase x).map (fun _ => c) := by
        simp [ys]
      have hmapne : (xs.erase x).map (fun _ => c) ≠ [] := by simp [hne]
      have hmapfold : ((xs.erase x).map (fun _ => c)).foldl (fun a b => a ||| b) 0 = c := by
        rw [or_fold_map_const]
        simp [hne]
      have hval_ys : value BitUnion.model (ys.erase x) = some c := by
        rw [herase]
        simp [value, BitUnion.model, isEmpty_false_of_ne_nil hmapne, hmapfold]
      have hval_xs : value BitUnion.model (xs.erase x) = some w := by
        simp [value, BitUnion.model, isEmpty_false_of_ne_nil hne, w]
      rw [hval_ys, hval_xs]
      simp [hcne]
  by_cases hwv : w = v
  · refine build (v ^^^ x) ?_ ?_
    · simpa [Nat.or_comm] using (xor_or_of_subset hsub)
    · rw [hwv]
      exact xor_ne_self_of_subset_ne_zero hxne0 hsub
  · refine build v ?_ ?_
    · simpa [Nat.or_comm] using hsub
    · exact Ne.symm hwv

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_FallbackNecessary
