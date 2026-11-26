import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

/-!
SQL operators may batch or defer work under the modeled memory and resource ownership
conditions.

Proved

- `Gather.ScatterBack` — `∀ (payload : List Int) (ids : List Nat), let addresses :=
  ids.mergeSort.eraseDups let cache := addresses.map (fun i => payload[i]?) ids.map
  (fun i => (cache[addresses.idxOf i]?).join) = ids.map (fun i => payload[i]?)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Gather_ScatterBack

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Gather

-- The cache is indexed by the first occurrence of the ordinal, so looking it
-- up with `idxOf` returns the original payload slot of that ordinal.
private theorem cache_lookup (payload : List Int) (addresses : List Nat) (i : Nat)
    (hi : i ∈ addresses) :
    ((addresses.map (fun j => payload[j]?))[addresses.idxOf i]?).join = payload[i]? := by
  have hlt : addresses.idxOf i < addresses.length :=
    List.idxOf_lt_length_of_mem hi
  have hget : addresses[addresses.idxOf i] = i := List.getElem_idxOf hlt
  have hsome : addresses[addresses.idxOf i]? = some i :=
    List.getElem?_eq_some_iff.mpr ⟨hlt, hget⟩
  rw [List.getElem?_map, hsome]
  rfl

theorem sound_Gather_ScatterBack : ScatterBack.Goal := by
  intro payload ids
  dsimp
  apply List.map_congr_left
  intro i hi
  apply cache_lookup
  rw [List.mem_eraseDups]
  exact (List.mergeSort_perm ids (fun a b : Nat => decide (a ≤ b))).mem_iff.mpr hi

end TinylambOpt.ResearchProofs.Atlas.Goal_Gather_ScatterBack
