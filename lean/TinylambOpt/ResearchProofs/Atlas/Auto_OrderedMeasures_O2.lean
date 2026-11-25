import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
import TinylambOpt.ResearchProofs.Atlas_OrderedMeasures_BestFrame_Remaining
import TinylambOpt.ResearchProofs.Atlas_OrderedMeasures_PlateauFrames_Remaining

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Auto_OrderedMeasures_O2

/-! Proofs for the ten assigned Atlas/OrderedMeasures goals.  The two imported
    `*_Remaining` modules contain the fully checked arguments; these declarations
    give each of them a stable name in this batch's namespace. -/

theorem proof_BestFrame_CrossFrame :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.BestFrame.CrossFrame.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.Remaining.sound_CrossFrame

theorem proof_BestFrame_RepeatedGain :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.BestFrame.RepeatedGain.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.Remaining.sound_RepeatedGain

theorem proof_BestFrame_NegativePage :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.BestFrame.NegativePage.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.Remaining.sound_NegativePage

theorem proof_BestFrame_PrefixDrawdown :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.BestFrame.PrefixDrawdown.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.Remaining.sound_PrefixDrawdown

theorem proof_BestFrame_DeletionFence :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.BestFrame.DeletionFence.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.Remaining.sound_DeletionFence

theorem proof_PlateauFrames_BoundaryRuns :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.BoundaryRuns.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PlateauFrames.Remaining.sound_BoundaryRuns

theorem proof_PlateauFrames_Uniform :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.Uniform.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PlateauFrames.Remaining.sound_Uniform

theorem proof_PlateauFrames_InjectiveCodes :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.InjectiveCodes.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PlateauFrames.Remaining.sound_InjectiveCodes

theorem proof_PlateauFrames_ExpandRuns :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.ExpandRuns.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PlateauFrames.Remaining.sound_ExpandRuns

theorem proof_PlateauFrames_TieFence :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.TieFence.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PlateauFrames.Remaining.sound_TieFence

end TinylambOpt.ResearchProofs.Atlas.Auto_OrderedMeasures_O2
