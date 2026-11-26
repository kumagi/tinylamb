import TinylambOpt.ResearchProofs.StorageExecution.SE_GrammarScans_Common
import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `SlashDepth.RepeatedDelimiter` — `∀ x n, 2≤n → matched reader (.rep n x) 0 = (expand
  x).contains 47`
- `SlashDepth.TwoDelimiters` — `∀ x, matched reader x 0 = decide (2 ≤ (expand x).count
  47)`
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.GrammarScans

namespace SlashDepth
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.SlashDepth
theorem sound_RepeatedDelimiter : RepeatedDelimiter.Goal := Common.sound_SlashDepth_RepeatedDelimiter
theorem sound_TwoDelimiters : TwoDelimiters.Goal := Common.sound_SlashDepth_TwoDelimiters
end SlashDepth

end TinylambOpt.ResearchProofs.StorageExecution.GrammarScans
