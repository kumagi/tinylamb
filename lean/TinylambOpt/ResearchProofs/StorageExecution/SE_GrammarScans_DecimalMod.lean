import TinylambOpt.ResearchProofs.StorageExecution.SE_GrammarScans_Common
import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `DecimalMod.DecimalSuffix` — `∀ a x y, x<10 → y<10 → let q:=applyT (compile reader a)
  0; q<7 → applyT (compile reader (.cat a (.lit [48+x,48+y]))) 0 = (100*q+10*x+y)%7`
- `DecimalMod.LeadingZero` — `∀ x, matched reader (.cat (.lit [48]) x) 0 = matched
  reader x 0`
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.GrammarScans

namespace DecimalMod
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.DecimalMod
theorem sound_DecimalSuffix : DecimalSuffix.Goal := Common.sound_DecimalMod_DecimalSuffix
theorem sound_LeadingZero : LeadingZero.Goal := Common.sound_DecimalMod_LeadingZero
end DecimalMod

end TinylambOpt.ResearchProofs.StorageExecution.GrammarScans
