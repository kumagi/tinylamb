import TinylambOpt.ResearchProofs.StorageExecution.SE_GrammarScans_Common
import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `EscapedTail.ResetAtDelimiter` — `∀ a, matched reader (.cat a (.lit [47,92])) 0=true`
- `EscapedTail.TrailingRun` — `∀ x, matched reader x 0 = decide (((expand
  x).reverse.takeWhile (· == 92)).length%2=1)`
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.GrammarScans

namespace EscapedTail
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EscapedTail
theorem sound_ResetAtDelimiter : ResetAtDelimiter.Goal := Common.sound_EscapedTail_ResetAtDelimiter
theorem sound_TrailingRun : TrailingRun.Goal := Common.sound_EscapedTail_TrailingRun
end EscapedTail

end TinylambOpt.ResearchProofs.StorageExecution.GrammarScans
