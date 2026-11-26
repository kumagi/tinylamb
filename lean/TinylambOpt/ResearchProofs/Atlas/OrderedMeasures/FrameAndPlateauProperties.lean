import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
import TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame
import TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PlateauFrames

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved (10 claims, including the following)

- `BestFrame.CrossFrame` — `∀ x y, measure (x++y) = max (max (measure x) (measure y))
  (max0 (suffs x)+max0 (prefs y))`
- `BestFrame.DeletionFence` — `∀ x (keep : Int → Bool), measure (x.filter keep) ≤
  measure x + ((x.filter (fun v => !keep v)).map (fun v => max 0 (-v))).sum`
- `BestFrame.NegativePage` — `∀ x, (∀ v ∈ x, v ≤ 0) → measure x = 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.FrameAndPlateauProperties

/-! Proofs for the ten assigned Atlas/OrderedMeasures goals.  The two imported
    `*_Remaining` modules contain the fully checked arguments; these declarations
    give each of them a stable name in this batch's namespace. -/

theorem sound_BestFrame_CrossFrame :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.BestFrame.CrossFrame.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.sound_CrossFrame

theorem sound_BestFrame_RepeatedGain :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.BestFrame.RepeatedGain.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.sound_RepeatedGain

theorem sound_BestFrame_NegativePage :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.BestFrame.NegativePage.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.sound_NegativePage

theorem sound_BestFrame_PrefixDrawdown :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.BestFrame.PrefixDrawdown.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.sound_PrefixDrawdown

theorem sound_BestFrame_DeletionFence :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.BestFrame.DeletionFence.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.sound_DeletionFence

theorem sound_PlateauFrames_BoundaryRuns :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.BoundaryRuns.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PlateauFrames.sound_BoundaryRuns

theorem sound_PlateauFrames_Uniform :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.Uniform.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PlateauFrames.sound_Uniform

theorem sound_PlateauFrames_InjectiveCodes :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.InjectiveCodes.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PlateauFrames.sound_InjectiveCodes

theorem sound_PlateauFrames_ExpandRuns :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.ExpandRuns.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PlateauFrames.sound_ExpandRuns

theorem sound_PlateauFrames_TieFence :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.TieFence.Goal :=
  by exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PlateauFrames.sound_TieFence

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.FrameAndPlateauProperties
