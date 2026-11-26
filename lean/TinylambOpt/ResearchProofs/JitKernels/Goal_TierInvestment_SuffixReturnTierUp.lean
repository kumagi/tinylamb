import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

/-! Compiling a suffix is worthwhile exactly when its realized net savings cover the compilation charge; the proof balances the two bills by the net of the suffix. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_SuffixReturnTierUp

open TinylambOpt.ResearchGoals.JitKernels.TierInvestment
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment.SuffixReturnTierUp

theorem sum_net (t : Tariff) (xs : List Batch) :
    (xs.map (net t)).sum = ((xs.map (scalar t)).sum : Int) - ((xs.map (compiled t)).sum : Int) := by
  induction xs with
  | nil => simp
  | cons b xs ih =>
      simp [net, ih]
      omega

theorem sound : SuffixReturnTierUp.Goal := by
  intro t xs cut hcut
  have hsum := sum_net t (xs.drop cut)
  have htake : ((xs.take cut).map (scalar t)).sum + ((xs.drop cut).map (scalar t)).sum = (xs.map (scalar t)).sum := by
    rw [← List.sum_append, ← List.map_append, List.take_append_drop]
  simp [bill, hcut] at hsum htake ⊢
  omega

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_SuffixReturnTierUp
