import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_NestedComment_Depth

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : NestedComment.Depth.Goal := by
  have hs : ∀ q, NestedComment.model.step q Sym.closeComment = (q-1,false) := by
    intro q
    simp [NestedComment.model, sym_beq_true]
  intro q n
  induction n generalizing q with
  | zero => simp [scan]
  | succ n ih =>
      rw [List.replicate_succ, Common.scan_cons]
      simp only [hs]
      rw [ih]
      omega

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_NestedComment_Depth
