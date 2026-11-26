import TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_MergeIntegrity_NullMergeAntiRowsRetainMultiplicity

open TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity


theorem map_fst_zipIdx_filter (src : List Key) (p : Key → Bool) (n : Nat) :
    ((src.zipIdx n).filter (fun s => p s.1)).map Prod.fst = src.filter p := by
  induction src generalizing n with
  | nil => simp
  | cons a src ih =>
      by_cases h : p a = true
      · simp [List.zipIdx, h, ih (n+1)]
      · simp [List.zipIdx, h, ih (n+1)]

theorem count_none_filter_eqOn (target : List Key) (src : List Key) :
    (src.filter (fun a => !target.any (eqOn a))).count none = src.count none := by
  induction src with
  | nil => simp
  | cons a src ih =>
      by_cases ha : a = none
      · subst a
        have hp : (fun x => !target.any (eqOn x)) none = true := by simp [eqOn]
        rw [List.filter_cons_of_pos (p := fun x => !target.any (eqOn x)) hp]
        simpa [List.count_cons] using ih
      · by_cases hp : (fun x => !target.any (eqOn x)) a = true
        · rw [List.filter_cons_of_pos (p := fun x => !target.any (eqOn x)) hp]
          simpa [List.count_cons, ha] using ih
        · rw [List.filter_cons_of_neg (p := fun x => !target.any (eqOn x)) hp]
          simpa [List.count_cons, ha] using ih

theorem sound : NullMergeAntiRowsRetainMultiplicity.Goal := by
  intro target src
  rw [unmatched]
  rw [map_fst_zipIdx_filter src (fun a => !target.any (eqOn a)) 0]
  exact count_none_filter_eqOn target src

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_MergeIntegrity_NullMergeAntiRowsRetainMultiplicity
