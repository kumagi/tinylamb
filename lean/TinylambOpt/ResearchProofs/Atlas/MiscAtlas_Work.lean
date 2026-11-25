import TinylambOpt.ResearchGoals.Atlas.ExecutorResources
import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance
import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
import TinylambOpt.ResearchGoals.Atlas.TextExecution
import TinylambOpt.ResearchProofs.Atlas.Goal_Exchanges_GlobalPacking
import TinylambOpt.ResearchProofs.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_MeanReserve
import TinylambOpt.ResearchProofs.Atlas.Goal_Versions_DropTombstones
import TinylambOpt.ResearchProofs.Atlas.Goal_BatchWatermarks_FinalSummary
import TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_PrefixTransport
import TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_AlignedMarginals
import TinylambOpt.ResearchProofs.Atlas.Goal_PageMoments_MomentHistogram
import TinylambOpt.ResearchProofs.Atlas.Goal_Grammar_InteriorCache
import TinylambOpt.ResearchProofs.Atlas_TextExecution_EditFilterAnchors

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.MiscAtlas_Work

open TinylambOpt.ResearchGoals.Atlas

-- 15. Exchanges.GlobalPacking.Goal (disproved)
theorem disproof_Exchanges_GlobalPacking :
    ExecutorResources.Exchanges.GlobalPacking.Goal |> Not :=
  TinylambOpt.ResearchProofs.Atlas.Goal_Exchanges_GlobalPacking.disproof_Exchanges_GlobalPacking

-- 16. PredicateOrder.MarginalTie.Goal (disproved)
theorem disproof_PredicateOrder_MarginalTie :
    PlanSearch.PredicateOrder.MarginalTie.Goal |> Not :=
  TinylambOpt.ResearchProofs.Atlas.PlanSearch.disproof_MarginalTie

-- 17. RiskEnvelope.MeanReserve.Goal (disproved)
theorem disproof_RiskEnvelope_MeanReserve :
    PlanSearch.RiskEnvelope.MeanReserve.Goal |> Not :=
  TinylambOpt.ResearchProofs.Atlas.PlanSearch_MeanReserve.disproof_MeanReserve

-- 18. Versions.DropTombstones.Goal (disproved)
theorem disproof_Versions_DropTombstones :
    SnapshotMaintenance.Versions.DropTombstones.Goal |> Not :=
  TinylambOpt.ResearchProofs.Atlas.Goal_Versions_DropTombstones.disproof_Versions_DropTombstones

-- 19. BatchWatermarks.FinalSummary.Goal (disproved)
theorem disproof_BatchWatermarks_FinalSummary :
    SnapshotMaintenance.BatchWatermarks.FinalSummary.Goal |> Not :=
  TinylambOpt.ResearchProofs.Atlas.Goal_BatchWatermarks_FinalSummary.disproof_BatchWatermarks_FinalSummary

-- 20. Couplings.PrefixTransport.Goal (proved)
theorem sound_Couplings_PrefixTransport :
    SynopsisBounds.Couplings.PrefixTransport.Goal :=
  TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_PrefixTransport.sound_Couplings_PrefixTransport

-- 21. Couplings.AlignedMarginals.Goal (disproved)
theorem disproof_Couplings_AlignedMarginals :
    SynopsisBounds.Couplings.AlignedMarginals.Goal |> Not :=
  TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_AlignedMarginals.disproof_Couplings_AlignedMarginals

-- 22. PageMoments.MomentHistogram.Goal (disproved)
theorem disproof_PageMoments_MomentHistogram :
    SynopsisBounds.PageMoments.MomentHistogram.Goal |> Not :=
  TinylambOpt.ResearchProofs.Atlas.Goal_PageMoments_MomentHistogram.disproof_PageMoments_MomentHistogram

-- 23. Grammar.InteriorCache.Goal (disproved)
theorem disproof_Grammar_InteriorCache :
    TextExecution.Grammar.InteriorCache.Goal |> Not :=
  TinylambOpt.ResearchProofs.Atlas.Goal_Grammar_InteriorCache.disproof_Grammar_InteriorCache

-- 24. EditFilter.PositionalAnchor.Goal (proved)
theorem sound_EditFilter_PositionalAnchor :
    TextExecution.EditFilter.PositionalAnchor.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.EditFilterAnchors.sound_PositionalAnchor

end TinylambOpt.ResearchProofs.Atlas.MiscAtlas_Work
