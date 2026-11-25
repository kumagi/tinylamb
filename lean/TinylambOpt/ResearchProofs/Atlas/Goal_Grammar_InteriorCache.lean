import TinylambOpt.ResearchGoals.Atlas.TextExecution

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Grammar_InteriorCache

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Grammar

/-! Brute-force check of the smallest counterexample: `a = [1]`, `b = [0]`,
`p = [1,2]`, `tail = [2]`.  The two premises hold (both words have length 1 and
neither contains `p`), but appending `[2]` gives one occurrence vs. zero. -/
#eval ((positions ([1] : Word) [1,2]).length, (positions ([0] : Word) [1,2]).length,
       (positions ([1,2] : Word) [1,2]).length, (positions ([0,2] : Word) [1,2]).length)

theorem disproof_Grammar_InteriorCache : ¬ Grammar.InteriorCache.Goal := by
  intro h
  have hw := h ([1] : Word) ([0] : Word) [1,2]
    (by decide) (by native_decide) ([2] : Word)
  have hne : ¬ (positions ([1,2] : Word) [1,2]).length =
      (positions ([0,2] : Word) [1,2]).length := by
    native_decide
  exact hne (by simpa using hw)

end TinylambOpt.ResearchProofs.Atlas.Goal_Grammar_InteriorCache
