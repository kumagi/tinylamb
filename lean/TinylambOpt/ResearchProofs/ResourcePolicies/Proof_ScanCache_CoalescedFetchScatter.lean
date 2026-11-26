import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache

namespace TinylambOpt.ResearchProofs.ResourcePolicies.ScanCache.Goal_CoalescedFetchScatter

open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache


/-!
Coalescing duplicate asynchronous block fetches by the full immutable request
identity is exact: every original request appears in the deduplicated batch, and
its index there selects precisely that request.  Thus all duplicated fetches
share one response, while differing file, generation, or block never collide.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false



open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache.CoalescedFetchScatter.Goal := by
  intro rs readBlock
  dsimp
  apply List.ext_getElem
  · simp
  · intro i hiL hiR
    have hi : i < rs.length := by simpa using hiL
    have hmem : rs[i] ∈ rs := List.getElem_mem hi
    have hmemBatch : rs[i] ∈ rs.eraseDups := (List.mem_eraseDups).2 hmem
    have hlt : List.idxOf (rs[i]) (rs.eraseDups) < (rs.eraseDups).length :=
      (List.idxOf_lt_length_iff).2 hmemBatch
    have hget : (rs.eraseDups)[List.idxOf (rs[i]) (rs.eraseDups)] = rs[i] :=
      List.getElem_idxOf hlt
    simp only [List.getElem_map]
    rw [List.getElem?_map, List.getElem?_eq_getElem hlt, hget]
    rfl
end TinylambOpt.ResearchProofs.ResourcePolicies.ScanCache.Goal_CoalescedFetchScatter
