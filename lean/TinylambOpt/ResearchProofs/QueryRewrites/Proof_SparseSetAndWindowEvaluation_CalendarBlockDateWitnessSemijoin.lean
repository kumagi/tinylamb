import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_CalendarBlockDateWitnessSemijoin

open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation


/-!
# CalendarBlockDateWitnessSemijoin

A page contributes selected rows iff its day dictionary has a selected witness, so the
page-level semijoin filter can be applied before flattening without changing the bag of
selected dates. This lets a scan skip pages whose dictionary misses every requested day.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.CalendarBlockDateWitnessSemijoin.Goal := by
  intro pages days
  induction pages with
  | nil => simp
  | cons p pages ih =>
      simp only [List.filter_cons, List.flatMap_cons, List.flatten_cons, List.filter_append]
      by_cases h : (TinylambOpt.ResearchGoals.QueryRewrites.members p days).isEmpty
      · have hmem : TinylambOpt.ResearchGoals.QueryRewrites.members p days = [] :=
          List.isEmpty_iff.mp h
        have hp : p.filter days.contains = [] := by
          apply List.eq_nil_iff_forall_not_mem.mpr
          intro x hx
          have hx' := List.mem_filter.mp hx
          have hxmem : x ∈ TinylambOpt.ResearchGoals.QueryRewrites.members p days := by
            dsimp [TinylambOpt.ResearchGoals.QueryRewrites.members]
            exact List.mem_filter.mpr ⟨List.mem_eraseDups.mpr hx'.1, hx'.2⟩
          have : x ∈ ([] : List Nat) := by
            rw [← hmem]
            exact hxmem
          simp at this
        simp [h, hp, ih]
      · have h' : (TinylambOpt.ResearchGoals.QueryRewrites.members p days).isEmpty = false := by
          simpa using h
        simp [h', ih]
end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_CalendarBlockDateWitnessSemijoin
