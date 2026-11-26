import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_BalancedNesting_OddLengthEmpty

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.BalancedNesting

/-- A path of length `l` starting at depth `q` can finish at depth 0 only when
`l` and `q` have the same parity: every open adds one to the depth and every
close subtracts one, so the final depth is `q + (#opens - #closes)`. -/
private theorem parity_zero : ∀ l q, (l + q) % 2 = 1 → count model l q = 0 := by
  intro l
  induction l with
  | zero =>
      intro q h
      simp [count, model]
      omega
  | succ l ih =>
      intro q h
      by_cases hq : q = 0
      · subst q
        have htrue : count model l 1 = 0 := ih 1 (by omega)
        simp [model] at htrue
        simp [count, model, htrue]
      · have hfalse : count model l (q - 1) = 0 := ih (q - 1) (by omega)
        have htrue : count model l (q + 1) = 0 := ih (q + 1) (by omega)
        simp [model] at hfalse htrue
        simp [count, model, hq, hfalse, htrue]

theorem sound_OddLengthEmpty : OddLengthEmpty.Goal := by
  intro n
  apply parity_zero
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_BalancedNesting_OddLengthEmpty
