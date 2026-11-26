import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.SparseSetAndWindowEvaluation.Goal_PeerRankBoundsFromPartitionExtrema

open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation


/-!
# PeerRankBoundsFromPartitionExtrema

A page partition that is ordered by peer keys can be ranked from zone extrema alone.
If the maximum is below the probe, every row is strictly below it; if the minimum is at
or above the probe, no row is below it. Otherwise the page is partially relevant and the
exact count is used. This lets the executor skip full-page work and still compute SQL
RANK's strict-below count.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites

namespace PeerRankHelpers

theorem smaller_eq_count (p : List Nat) (probe : Nat) :
    (if p.max?.any (fun x => decide (x < probe)) then p.length
      else if p.min?.any (fun x => decide (probe ≤ x)) then 0
      else countWhere p (fun x => decide (x < probe))) =
      countWhere p (fun x => decide (x < probe)) := by
  by_cases hmax : p.max?.any (fun x => decide (x < probe)) = true
  · rw [if_pos hmax]
    obtain ⟨m, hm, hmlt⟩ := (Option.any_eq_true (fun x => decide (x < probe)) p.max?).mp hmax
    have hm_lt : m < probe := by simpa using hmlt
    have hmprops := (List.max?_eq_some_iff.mp hm).2
    have hall : ∀ a ∈ p, decide (a < probe) = true := by
      intro a ha
      have ham : a ≤ m := hmprops a ha
      have halt : a < probe := by omega
      simp [halt]
    have hfilter : p.filter (fun x => decide (x < probe)) = p :=
      List.filter_eq_self.mpr hall
    dsimp [countWhere]
    rw [hfilter]
  · rw [if_neg hmax]
    by_cases hmin : p.min?.any (fun x => decide (probe ≤ x)) = true
    · rw [if_pos hmin]
      obtain ⟨m, hm, hmge⟩ := (Option.any_eq_true (fun x => decide (probe ≤ x)) p.min?).mp hmin
      have hprobe_m : probe ≤ m := by simpa using hmge
      have hmprops := (List.min?_eq_some_iff.mp hm).2
      have hnone : ∀ a ∈ p, ¬ decide (a < probe) = true := by
        intro a ha hlt
        have hma : m ≤ a := hmprops a ha
        have halt : a < probe := by simpa using hlt
        omega
      have hfilter : p.filter (fun x => decide (x < probe)) = [] :=
        List.filter_eq_nil_iff.mpr hnone
      dsimp [countWhere]
      rw [hfilter]
      simp
    · rw [if_neg hmin]

theorem flatten_count (pages : List (List Nat)) (probe : Nat) :
    countWhere pages.flatten (fun x => decide (x < probe)) =
      (pages.map (fun p => countWhere p (fun x => decide (x < probe)))).sum := by
  induction pages with
  | nil => simp [countWhere]
  | cons p pages ih =>
      simp [countWhere, List.filter_append, Function.comp_def, ih, Nat.add_assoc]

end PeerRankHelpers

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.PeerRankBoundsFromPartitionExtrema.Goal := by
  intro pages probe
  let smaller := fun (p : List Nat) =>
    if p.max?.any (fun x => decide (x < probe)) then p.length
    else if p.min?.any (fun x => decide (probe ≤ x)) then 0
    else countWhere p (fun x => decide (x < probe))
  have hsum : (pages.map smaller).sum =
      (pages.map (fun p => countWhere p (fun x => decide (x < probe)))).sum := by
    induction pages with
    | nil => simp
    | cons p pages ih =>
        simp [smaller, PeerRankHelpers.smaller_eq_count, Function.comp_def, ih]
  have hflat := PeerRankHelpers.flatten_count pages probe
  change 1 + (pages.map smaller).sum = 1 + countWhere pages.flatten (fun x => decide (x < probe))
  rw [hsum, hflat]
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.SparseSetAndWindowEvaluation.Goal_PeerRankBoundsFromPartitionExtrema
