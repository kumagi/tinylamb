import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MorselRetirement.Goal_CompletePrefixSplit

open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement


/-!
Once the first window has a complete contiguous prefix of reports, its output can be published and the next window resumed at the retirement frontier without changing the ordered result.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

private theorem readyIds_eq_takeWhile_range' (rs : List Report) (start count : Nat) :
    readyIds rs start count = (List.range' start count).takeWhile (fun i => (lookup rs i).isSome) := by
  unfold readyIds
  rw [List.range'_eq_map_range]

private theorem readyIds_append_of_complete (rs : List Report) (start a b : Nat)
    (h : (readyIds rs start a).length = a) :
    readyIds rs start (a + b) = readyIds rs start a ++ readyIds rs (start + a) b := by
  rw [readyIds_eq_takeWhile_range' rs start (a + b), readyIds_eq_takeWhile_range' rs start a,
      readyIds_eq_takeWhile_range' rs (start + a) b]
  rw [readyIds_eq_takeWhile_range' rs start a] at h
  have heq : (List.range' start a).takeWhile (fun i => (lookup rs i).isSome) = List.range' start a :=
    List.IsPrefix.eq_of_length (List.takeWhile_prefix (p := fun i => (lookup rs i).isSome)
      (l := List.range' start a)) (by simpa using h)
  rw [← List.range'_append_1, List.takeWhile_append]
  simp [heq]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.CompletePrefixSplit.Goal := by
  intro rs start a b h
  unfold emit
  rw [readyIds_append_of_complete rs start a b h, List.flatMap_append]
end TinylambOpt.ResearchProofs.ResourcePolicies.MorselRetirement.Goal_CompletePrefixSplit
