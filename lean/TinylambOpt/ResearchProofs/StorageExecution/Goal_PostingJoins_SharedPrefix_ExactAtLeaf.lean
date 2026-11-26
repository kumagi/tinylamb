import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SharedPrefix_ExactAtLeaf

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix

/-! At t = 0 the shared-prefix bucket width is `2^0 = 1`, so the only
bucket intersected with `[0,u)` is the singleton `[x, x+1)` whenever
`x < u`. -/

-- sanity checks on small inputs (hypothesis x < u is satisfied)
#eval (search.spans 10 0 0)
#eval (search.spans 10 0 3)
#eval (search.spans 10 0 9)
#eval (search.spans 1 0 0)

private theorem optional_eq (a b : Nat) : optional a b = if a < b then [(a, b)] else [] := rfl

example : ∀ u x, x < u → search.spans u 0 x = [(x, x + 1)] := by
  intro u x hux
  have hs : x + 1 ≤ u := by omega
  simp [search, optional_eq, Nat.min_eq_right hs]

theorem sound_ExactAtLeaf : SharedPrefix.ExactAtLeaf.Goal := by
  intro u x hux
  have hs : x + 1 ≤ u := by omega
  simp [search, optional_eq, Nat.min_eq_right hs]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SharedPrefix_ExactAtLeaf
