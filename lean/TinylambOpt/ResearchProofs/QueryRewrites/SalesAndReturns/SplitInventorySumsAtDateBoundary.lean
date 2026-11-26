import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# SplitInventorySumsAtDateBoundary

Separate streams on either side of the inventory date boundary preserve the modeled
before and after sums. The executor can avoid testing that boundary during each
aggregate update.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.SplitInventorySumsAtDateBoundary
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.SplitInventorySumsAtDateBoundary.Goal := by
  intro rows cut
  induction rows with
  | nil => simp [total]
  | cons r rows ih =>
      rcases r with ⟨d, q⟩
      simp [total] at ih ⊢
      constructor
      · by_cases h : d < cut
        · simp [h, ih.1]
        · simp [h, ih.1]
      · by_cases h : d < cut
        · simp [h, ih.2]
        · have hge : cut ≤ d := by omega
          simp [h, hge, ih.2]
