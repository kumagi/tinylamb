import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CsvRecord_JoinedLines

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : CsvRecord.JoinedLines.Goal := by
  have hs : CsvRecord.model.step 0 Sym.lf = (0,true) := by native_decide
  intro n
  induction n with
  | zero => simp [scan]
  | succ n ih =>
      rw [List.replicate_succ, Common.scan_cons, hs]
      simp [ih]

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CsvRecord_JoinedLines
