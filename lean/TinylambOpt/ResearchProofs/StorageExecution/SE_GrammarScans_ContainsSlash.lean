import TinylambOpt.ResearchProofs.StorageExecution.SE_GrammarScans_Common
import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `ContainsSlash.SearchOnce` — `∀ a k, 0<k → matched reader (.rep k a) 0 = matched
  reader a 0`
- `ContainsSlash.SlashWitness` — `∀ x, matched reader x 0 = (expand x).contains 47`
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.GrammarScans

namespace ContainsSlash
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.ContainsSlash
theorem sound_SearchOnce : SearchOnce.Goal := Common.sound_ContainsSlash_SearchOnce
theorem sound_SlashWitness : SlashWitness.Goal := Common.sound_ContainsSlash_SlashWitness
end ContainsSlash

end TinylambOpt.ResearchProofs.StorageExecution.GrammarScans
