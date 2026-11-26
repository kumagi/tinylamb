import TinylambOpt.ResearchProofs.StorageExecution.SE_GrammarScans_Common
import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `LineBoundary.FinalPair` — `∀ x, matched reader x 0 = [13,10].isSuffixOf (expand x)`
- `LineBoundary.RunBoundaryCRLF` — `∀ x k, 1<k → extent x=1 → matched reader (.rep k x)
  0=false`
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.GrammarScans

namespace LineBoundary
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.LineBoundary
theorem sound_RunBoundaryCRLF : RunBoundaryCRLF.Goal := Common.sound_LineBoundary_RunBoundaryCRLF
theorem sound_FinalPair : FinalPair.Goal := Common.sound_LineBoundary_FinalPair
end LineBoundary

end TinylambOpt.ResearchProofs.StorageExecution.GrammarScans
