import TinylambOpt.ResearchProofs.StorageExecution.SE_GrammarScans_Common
import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `QuotedField.DoublePhrase` — `∀ x, compile reader (.rep 2 x) = List.range
  reader.states`
- `QuotedField.QuoteParity` — `∀ x, matched reader x 0 = decide ((expand x).count 34 %
  2 = 1)`
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.GrammarScans

namespace QuotedField
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.QuotedField
theorem sound_DoublePhrase : DoublePhrase.Goal := Common.sound_QuotedField_DoublePhrase
theorem sound_QuoteParity : QuoteParity.Goal := Common.sound_QuotedField_QuoteParity
end QuotedField

end TinylambOpt.ResearchProofs.StorageExecution.GrammarScans
