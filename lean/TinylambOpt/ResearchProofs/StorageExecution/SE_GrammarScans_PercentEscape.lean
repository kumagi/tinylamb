import TinylambOpt.ResearchProofs.StorageExecution.SE_GrammarScans_Common
import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `PercentEscape.EscapeAcrossBoundary` — `∀ x, matched reader (.cat x (.lit
  [37,52,70])) 0 = true`
- `PercentEscape.LoneIntroducer` — `∀ x k, partialScan reader (.cat x (.lit [37,37]))
  (extent x+2) (k%4)=1`
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.GrammarScans

namespace PercentEscape
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.PercentEscape
theorem sound_LoneIntroducer : LoneIntroducer.Goal := Common.sound_PercentEscape_LoneIntroducer
theorem sound_EscapeAcrossBoundary : EscapeAcrossBoundary.Goal := Common.sound_PercentEscape_EscapeAcrossBoundary
end PercentEscape

end TinylambOpt.ResearchProofs.StorageExecution.GrammarScans
