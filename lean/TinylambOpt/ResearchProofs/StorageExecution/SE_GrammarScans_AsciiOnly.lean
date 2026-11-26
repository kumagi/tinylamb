import TinylambOpt.ResearchProofs.StorageExecution.SE_GrammarScans_Common
import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `AsciiOnly.RejectEarly` — `∀ a b, matched reader a 0=false → matched reader (.cat a
  b) 0=false`
- `AsciiOnly.SevenBitFastPath` — `∀ x, matched reader x 0 = (expand x).all (· < 128)`
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.GrammarScans

namespace AsciiOnly
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.AsciiOnly
theorem sound_RejectEarly : RejectEarly.Goal := Common.sound_AsciiOnly_RejectEarly
theorem sound_SevenBitFastPath : SevenBitFastPath.Goal := Common.sound_AsciiOnly_SevenBitFastPath
end AsciiOnly

end TinylambOpt.ResearchProofs.StorageExecution.GrammarScans
