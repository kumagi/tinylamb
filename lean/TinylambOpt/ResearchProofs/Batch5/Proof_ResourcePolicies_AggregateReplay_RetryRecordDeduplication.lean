import TinylambOpt.ResearchGoals.QueryRewrites.Common
import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.AggregateReplay.Goal_RetryRecordDeduplication

open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay


/-! Scratch proof: immutable retry records that already occur in the primary
    batch are removed by identity-based deduplication, so replaying the merged
    batch is identical to replaying the primary batch. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

/-- Identity deduplication drops exactly the already-seen records, so appending
    a batch drawn from a nodup primary list leaves the primary list unchanged. -/
theorem uniqueRecords_append_of_mem {rs retry : List Record}
    (hmem : ∀ x ∈ retry, x ∈ rs) (hnodup : (rs.map Record.id).Nodup) :
    uniqueRecords (rs ++ retry) = rs := by
  induction rs generalizing retry with
  | nil =>
      have hretry : retry = [] := by
        cases retry with
        | nil => rfl
        | cons x xs =>
            exact False.elim (by simpa using hmem x (by simp))
      simp [hretry, uniqueRecords]
  | cons r rs ih =>
      simp only [List.map_cons] at hnodup
      have htailnodup : (rs.map Record.id).Nodup := (List.nodup_cons.mp hnodup).2
      have hheadnot : r.id ∉ rs.map Record.id := (List.nodup_cons.mp hnodup).1
      rw [List.cons_append]
      unfold uniqueRecords
      rw [List.eraseDupsBy_cons]
      simp only [List.filter_append]
      have hfilrs : rs.filter (fun x : Record => (x.id == r.id) = false) = rs := by
        apply List.filter_eq_self.mpr
        intro x hx
        have hxmem : x.id ∈ rs.map Record.id := List.mem_map.mpr ⟨x, hx, rfl⟩
        have hne : x.id ≠ r.id := by
          intro heq
          exact hheadnot (by simpa [heq] using hxmem)
        simp [hne]
      rw [hfilrs]
      have hfilmem : ∀ x ∈ retry.filter (fun y : Record => (y.id == r.id) = false), x ∈ rs := by
        intro x hx
        have hxretry : x ∈ retry := (List.mem_filter.mp hx).1
        have hxnot : (x.id == r.id) = false := by simpa using (List.mem_filter.mp hx).2
        have hm : x = r ∨ x ∈ rs := by simpa using hmem x hxretry
        rcases hm with heq | htail
        · subst x
          simp at hxnot
        · exact htail
      have ih' := ih hfilmem htailnodup
      unfold uniqueRecords at ih'
      rw [ih']

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay.RetryRecordDeduplication.Goal := by
  intro rs retry k hnodup hmem
  rw [uniqueRecords_append_of_mem hmem hnodup]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.AggregateReplay.Goal_RetryRecordDeduplication
