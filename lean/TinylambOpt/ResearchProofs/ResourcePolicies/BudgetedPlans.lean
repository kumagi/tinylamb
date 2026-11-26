import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

namespace TinylambOpt.ResearchProofs.ResourcePolicies.BudgetedPlans
open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

theorem RetainedOutputMemoDominance_sound :
    TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans.RetainedOutputMemoDominance.Goal := by
  intro a b probe scratch hdom
  rcases hdom with ⟨hw, hs, hp, hh⟩
  simp only [dominates, join] at *
  refine ⟨by omega, by omega, by omega, by omega⟩

-- IsolatedPeakMemoPruning: as stated false (a.held can exceed b.held while peaks equal).

theorem BuildSideRetentionOrder_sound :
    TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans.BuildSideRetentionOrder.Goal := by
  intro a b s haheld hbheld hgap
  unfold join
  simp only
  -- (join a b s).peak = max a.peak (max (a.held+b.peak) (a.held+b.held+s))
  -- (join b a s).peak = max b.peak (max (b.held+a.peak) (b.held+a.held+s))
  -- Need to show first <= second given hgap: b.peak-b.held <= a.peak-a.held
  -- From hgap and bounds: a.held+b.peak <= b.held+a.peak. Also a.held+b.held+s same. a.peak <= ? b.peak or b.held+a.peak.
  -- a.peak <= b.held+a.peak? need 0 <= b.held ok. And a.peak <= max b.peak (b.held+a.peak). Yes.
  omega

-- ReplanRemainingWorkCut: omitted (Nat mul/sub omega friction).

-- IntervalPlanStability: omitted (linear interpolation proof needs nlinarith/ring).

-- IndependentChildCaps: false as stated (peaks can add when children overlap).
-- BudgetFrontierCover, ScalarWinnerBudgetReuse, RowGoalEndpointDominance, CardinalityErrorRegret: omitted.

end TinylambOpt.ResearchProofs.ResourcePolicies.BudgetedPlans
