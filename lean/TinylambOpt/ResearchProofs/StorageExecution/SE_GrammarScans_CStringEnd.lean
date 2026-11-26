import TinylambOpt.ResearchProofs.StorageExecution.SE_GrammarScans_Common
import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `CStringEnd.OneTerminator` — `∀ x, matched reader (.cat x (.lit [0])) 0 = !(expand
  x).contains 0`
- `CStringEnd.TruncateAtTerminator` — `∀ pre tail, !(expand pre).contains 0 → matched
  reader (.cat (.cat pre (.lit [0])) tail) 0 = decide (extent tail=0)`
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.GrammarScans

namespace CStringEnd
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.CStringEnd
theorem sound_TruncateAtTerminator : TruncateAtTerminator.Goal := Common.sound_CStringEnd_TruncateAtTerminator
theorem sound_OneTerminator : OneTerminator.Goal := Common.sound_CStringEnd_OneTerminator
end CStringEnd

end TinylambOpt.ResearchProofs.StorageExecution.GrammarScans
