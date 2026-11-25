import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_RaggedStripe_Remaining_Round2

/-! Batch P5: remaining `RaggedStripe` Atlas EncodedPages goals.

The CRT core and the generic row/lane permutation lemmas for these goals live
in `TinylambOpt.ResearchProofs.Atlas_EncodedPages_RaggedStripe_Remaining_Round2`
(a self-contained module importing only the goal file).  That module proves
all three `RaggedStripe` goals; here we expose them under fully qualified
statements so they are picked up by proof checks.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P5

theorem RaggedStripe_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.RaggedStripe.ResidueJoin.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task44.sound_ResidueJoin

theorem RaggedStripe_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.RaggedStripe.DeleteProgression.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task44.sound_DeleteProgression

theorem RaggedStripe_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.RaggedStripe.AlignedProduct.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task44.sound_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P5
