import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_SparseSample_Remaining

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Batch P4: remaining `SparseSample` Atlas EncodedPages goals.

The CRT core and the generic row/lane permutation lemmas for these two goals
live in `TinylambOpt.ResearchProofs.Atlas_EncodedPages_SparseSample_Remaining`
(a self-contained module importing only the goal file).  That module proves
all three `SparseSample` goals; here we expose the two goals assigned to this
batch under fully qualified statements so they are picked up by proof checks.
-/
namespace TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P4

theorem SparseSample_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.DeleteProgression.Goal :=
  TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSample.Remaining.sound_DeleteProgression

theorem SparseSample_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.AlignedProduct.Goal :=
  TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSample.Remaining.sound_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P4
