import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.SparseSetAndWindowEvaluation.Goal_DemandedManufacturerWitnessCache

open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation


/-!
# DemandedManufacturerWitnessCache

Restricting item classification to keys requested by a non-null outer name preserves
witnesses: an item that matches an outer occurrence is itself demanded by that occurrence.
This allows expensive per-item work to be cached only for requested manufacturers while
keeping the same existential semijoin result for every outer row.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace DemandedWitnessHelpers

open TinylambOpt.ResearchGoals.QueryRewrites (hit)

theorem hit_symm {o r : Option String} (h : hit o r = true) : hit r o = true := by
  cases o with
  | none => simp [hit] at h
  | some n =>
      cases r with
      | none => simp [hit] at h
      | some m =>
          simp [hit] at h ⊢
          exact h.symm

theorem any_filter_hit (wanted : List (Option String))
    (items : List (Option String × Nat)) (admit : Nat → Bool)
    (o : Option String) (ho : o ∈ wanted) :
    (items.filter (fun r => wanted.any (hit r.1))).any
        (fun r => hit o r.1 && admit r.2) =
      items.any (fun r => hit o r.1 && admit r.2) := by
  induction items with
  | nil => rfl
  | cons r items ih =>
      by_cases hP : wanted.any (hit r.1) = true
      · have hfilter : (List.filter (fun x => wanted.any (hit x.1)) (r :: items)) =
            r :: (List.filter (fun x => wanted.any (hit x.1)) items) := by
          simp [hP]
        rw [hfilter]
        simp [ih]
      · have hP' : wanted.any (hit r.1) = false := by simpa using hP
        have hfilter : (List.filter (fun x => wanted.any (hit x.1)) (r :: items)) =
            List.filter (fun x => wanted.any (hit x.1)) items := by
          simp [hP']
        rw [hfilter]
        by_cases hr : hit o r.1 = true
        · have hrsym : hit r.1 o = true := hit_symm hr
          have hPtrue : wanted.any (hit r.1) = true :=
            List.any_eq_true.mpr ⟨o, ho, hrsym⟩
          rw [hP'] at hPtrue
          contradiction
        · have hr' : hit o r.1 = false := by simpa using hr
          simp [hr', ih]

end DemandedWitnessHelpers

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.DemandedManufacturerWitnessCache.Goal := by
  intro wanted items admit
  dsimp only
  apply List.map_congr_left
  intro o ho
  exact DemandedWitnessHelpers.any_filter_hit wanted items admit o ho
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.SparseSetAndWindowEvaluation.Goal_DemandedManufacturerWitnessCache
