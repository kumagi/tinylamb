import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# MultiplicityWeightedSecondMoments

Weighting quantity moments by join multiplicity preserves the modeled moments of the
expanded data. The executor can update a STDDEV state without repeating contributions.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.MultiplicityWeightedSecondMoments
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.MultiplicityWeightedSecondMoments.Goal := by
  intro xs n
  induction xs with
  | nil => simp [total]
  | cons x xs ih =>
      simp [total] at ih ⊢
      rw [ih]
      rw [Int.mul_add]
