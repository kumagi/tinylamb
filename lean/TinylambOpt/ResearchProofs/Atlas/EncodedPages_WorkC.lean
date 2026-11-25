import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E4
import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E5
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_PeriodicPatch_Remaining_Round2
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_RaggedStripe_Remaining_Round2
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_SparseSample_Remaining

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages_WorkC

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

namespace LaneMajor
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LaneMajor
theorem sound_LaneMajor_ResidueJoin : ResidueJoin.Goal :=
  TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E4.LaneMajor.LaneMajor_ResidueJoin
theorem sound_LaneMajor_DeleteProgression : DeleteProgression.Goal :=
  TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E4.LaneMajor.LaneMajor_DeleteProgression
theorem sound_LaneMajor_AlignedProduct : AlignedProduct.Goal :=
  TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E4.LaneMajor.LaneMajor_AlignedProduct
end LaneMajor

namespace Boustrophedon
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.Boustrophedon
theorem sound_Boustrophedon_ResidueJoin : ResidueJoin.Goal :=
  TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E4.Boustrophedon.Boustrophedon_ResidueJoin
theorem sound_Boustrophedon_DeleteProgression : DeleteProgression.Goal :=
  TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E4.Boustrophedon.Boustrophedon_DeleteProgression
theorem sound_Boustrophedon_AlignedProduct : AlignedProduct.Goal :=
  TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E4.Boustrophedon.Boustrophedon_AlignedProduct
end Boustrophedon

namespace RaggedStripe
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RaggedStripe
theorem sound_RaggedStripe_ResidueJoin : ResidueJoin.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task44.sound_ResidueJoin
theorem sound_RaggedStripe_DeleteProgression : DeleteProgression.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task44.sound_DeleteProgression
theorem sound_RaggedStripe_AlignedProduct : AlignedProduct.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task44.sound_AlignedProduct
end RaggedStripe

namespace PeriodicPatch
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.PeriodicPatch
theorem sound_PeriodicPatch_ResidueJoin : ResidueJoin.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task45.sound_ResidueJoin
theorem sound_PeriodicPatch_DeleteProgression : DeleteProgression.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task45.sound_DeleteProgression
theorem sound_PeriodicPatch_AlignedProduct : AlignedProduct.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task45.sound_AlignedProduct
end PeriodicPatch

namespace SparseSample
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample
theorem sound_SparseSample_ResidueJoin : ResidueJoin.Goal :=
  TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E5.SparseSample.sound_SparseSample_ResidueJoin
theorem sound_SparseSample_DeleteProgression : DeleteProgression.Goal :=
  TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSample.Remaining.sound_DeleteProgression
theorem sound_SparseSample_AlignedProduct : AlignedProduct.Goal :=
  TinylambOpt.ResearchProofs.Atlas.EncodedPages.SparseSample.Remaining.sound_AlignedProduct
end SparseSample

end TinylambOpt.ResearchProofs.Atlas.EncodedPages_WorkC
