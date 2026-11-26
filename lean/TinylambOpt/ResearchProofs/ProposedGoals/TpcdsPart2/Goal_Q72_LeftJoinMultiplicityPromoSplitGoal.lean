import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Left-join multiplicity split between promo counters (TPC-DS Q72)

Each sale row contributes the same return multiplicity whether it is counted in the promo
or no-promo partition, so the two weighted sums add to the unpartitioned weighted sum.
The executor may account for left-joined returns by a per-key multiplier rather than
materializing duplicate joined tuples.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q72_LeftJoinMultiplicityPromoSplitGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : Q72_LeftJoinMultiplicityPromoSplitGoal := by
  intro sales retCount
  dsimp only
  let weight : Nat → Nat := fun k => if retCount k == 0 then 1 else retCount k
  change (sales.filter Prod.snd |>.map (fun r => weight r.1)).sum +
      (sales.filter (fun r => !r.2) |>.map (fun r => weight r.1)).sum =
      (sales.map (fun r => weight r.1)).sum
  induction sales with
  | nil => simp
  | cons r sales ih =>
    cases hb : r.2 <;> simp [hb, ih] <;> omega

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q72_LeftJoinMultiplicityPromoSplitGoal
