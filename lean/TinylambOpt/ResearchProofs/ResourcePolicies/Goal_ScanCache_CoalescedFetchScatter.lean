import TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache
/-!
Registers the checked result for `ScanCache.CoalescedFetchScatter.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ScanCache_CoalescedFetchScatter
open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache CoalescedFetchScatter

private theorem getElem?_map_idxOf {α β : Type} [DecidableEq α] (f : α → β) (batch : List α) (r : α)
    (hr : r ∈ batch) : (batch.map f)[batch.idxOf r]? = some (f r) := by
  have hidx : batch.idxOf r < batch.length := List.idxOf_lt_length_of_mem hr
  rw [List.getElem?_eq_getElem (by simp; exact hidx)]
  simp [List.getElem_map, List.getElem_idxOf hidx]

theorem sound : Goal := by
  intro rs readBlock
  have hgen : ∀ (l batch : List Request), (∀ r ∈ l, r ∈ batch) →
      l.map (fun r => (batch.map readBlock)[batch.idxOf r]?) = (l.map readBlock).map some := by
    intro l batch hsub
    induction l with
    | nil => simp
    | cons r tl ih =>
      simp only [List.map_cons, List.cons.injEq]
      refine ⟨getElem?_map_idxOf readBlock batch r (hsub r (by simp)), ?_⟩
      exact ih (fun x hx => hsub x (by simp [hx]))
  exact hgen rs rs.eraseDups (fun r hr => List.mem_eraseDups.mpr hr)
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ScanCache_CoalescedFetchScatter
