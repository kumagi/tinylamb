import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_DecimalMod_SearchOnce

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-! The assigned name `DecimalMod.SearchOnce.Goal` is not declared in the goal
module: `SearchOnce.Goal` exists only in the `ContainsSlash` namespace.  We
therefore study the identical proposition with the `DecimalMod.reader`
substituted: scanning one phrase accepts iff scanning any positive number of
repeated copies of that phrase accepts. -/

private abbrev SearchOnce (r : Reader) : Prop :=
  ∀ a k, 0 < k → matched r (.rep k a) 0 = matched r a 0

/-! Truth check on small inputs (the hypothesis `0 < k` is respected).
`a = "1"` is a counterexample for `k = 6`: "111111" is divisible by 7,
while "1" is not. -/

#eval matched DecimalMod.reader (.lit [49]) 0
#eval matched DecimalMod.reader (.rep 6 (.lit [49])) 0

example : matched DecimalMod.reader (.lit [49]) 0 = false := by native_decide
example : matched DecimalMod.reader (.rep 6 (.lit [49])) 0 = true := by native_decide

theorem disproof_SearchOnce : ¬ SearchOnce DecimalMod.reader := by
  intro h
  have hh := h (.lit [49]) 6 (by decide)
  have hsingle : matched DecimalMod.reader (.lit [49]) 0 = false := by native_decide
  have hrepeat : matched DecimalMod.reader (.rep 6 (.lit [49])) 0 = true := by native_decide
  rw [hrepeat, hsingle] at hh
  contradiction

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_DecimalMod_SearchOnce
