import TinylambOpt.ResearchProofs.StorageExecution.SE_GrammarScans_Common
import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `EndsInDigit.LastByte` — `∀ x, matched reader x 0 = (expand x).getLast?.any (fun b =>
  48≤b && b≤57)`
- `EndsInDigit.NonemptyTail` — `∀ a b, 0<extent b → matched reader (.cat a b) 0 =
  matched reader b 0`
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.GrammarScans

namespace EndsInDigit
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit
theorem sound_NonemptyTail : NonemptyTail.Goal := Common.sound_EndsInDigit_NonemptyTail
theorem sound_LastByte : LastByte.Goal := Common.sound_EndsInDigit_LastByte
end EndsInDigit

end TinylambOpt.ResearchProofs.StorageExecution.GrammarScans
