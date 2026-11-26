import TinylambOpt.ResearchProofs.StorageExecution.SE_GrammarScans_Common
import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

namespace TinylambOpt.ResearchProofs.StorageExecution.GrammarScans

namespace EndsInDigit
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit
theorem sound_NonemptyTail : NonemptyTail.Goal := Common.sound_EndsInDigit_NonemptyTail
theorem sound_LastByte : LastByte.Goal := Common.sound_EndsInDigit_LastByte
end EndsInDigit

end TinylambOpt.ResearchProofs.StorageExecution.GrammarScans
