import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# ShiftedWeekArrayJoinMultiplicity

An indexed lookup of shifted week keys preserves the modeled self-join, including date
multiplicities. The executor can avoid comparing all week pairs.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ShiftedWeekArrayJoinMultiplicity
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.ShiftedWeekArrayJoinMultiplicity.Goal := by
  intro a b
  induction a with
  | nil => simp
  | cons w a ih =>
      simp [ih, List.count_eq_length_filter]
