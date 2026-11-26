import TinylambOpt.ResearchGoals.QueryRewrites.Common
import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

namespace TinylambOpt.ResearchProofs.ResourcePolicies.AggregateReplay.Goal_DisjointDistinctSpillDomains

open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay


/-! Scratch proof: when two spilled value domains are disjoint, deduplicating
    the concatenation counts each non-NULL value exactly once, so local
    COUNT DISTINCT results add up. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

/-- A value in `filterMap id vs` corresponds to `some x ∈ vs`. -/
theorem some_mem_of_mem_filterMap {vs : List (Option Int)} {x : Int}
    (hx : x ∈ vs.filterMap id) : some x ∈ vs := by
  rcases List.mem_filterMap.mp hx with ⟨v, hv, hvx⟩
  cases v with
  | none => simp at hvx
  | some y =>
      have hyx : y = x := by simpa using hvx
      subst x
      exact hv

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay.DisjointDistinctSpillDomains.Goal := by
  intro a b hdisj
  unfold distinctCount
  rw [List.filterMap_append, List.eraseDups_append]
  have hrm : (b.filterMap id).removeAll (a.filterMap id) = b.filterMap id := by
    unfold List.removeAll
    apply List.filter_eq_self.mpr
    intro x hx
    have hxb : some x ∈ b := some_mem_of_mem_filterMap hx
    have hnot : x ∉ a.filterMap id := by
      intro hxa
      exact hdisj x (some_mem_of_mem_filterMap hxa) hxb
    simp [hnot]
  rw [hrm]
  simp [List.length_append]
end TinylambOpt.ResearchProofs.ResourcePolicies.AggregateReplay.Goal_DisjointDistinctSpillDomains
