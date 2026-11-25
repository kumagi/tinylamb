import TinylambOpt.ResearchGoals.Atlas.TextExecution
import TinylambOpt.ResearchProofs.Atlas_TextExecution_Wavelet
import TinylambOpt.ResearchProofs.Atlas_TextExecution_Lcp

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.TextExecution

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Grammar
open TinylambOpt.ResearchGoals.Atlas.TextExecution.EditFilter
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Wavelet
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Lcp

theorem sound_Wavelet_RangeMap : Wavelet.RangeMap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.Wavelet.sound_RangeMap

theorem sound_Wavelet_Quantile : Wavelet.Quantile.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.Wavelet.sound_Quantile

theorem sound_Wavelet_SameBranch : Wavelet.SameBranch.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.Wavelet.sound_SameBranch

theorem sound_Wavelet_BetweenCodes : Wavelet.BetweenCodes.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.Wavelet.sound_BetweenCodes

theorem sound_Wavelet_DictionaryGrowth : Wavelet.DictionaryGrowth.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.Wavelet.sound_DictionaryGrowth

theorem sound_Lcp_RangeMinimum : Lcp.RangeMinimum.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.Lcp.sound_RangeMinimum

theorem sound_Lcp_DictionarySize : Lcp.DictionarySize.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.Lcp.sound_DictionarySize

theorem sound_Lcp_PagePrefix : Lcp.PagePrefix.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.Lcp.sound_PagePrefix

theorem sound_Lcp_CompareSkip : Lcp.CompareSkip.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.Lcp.sound_CompareSkip

theorem sound_Lcp_ShardBridge : Lcp.ShardBridge.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.Lcp.sound_ShardBridge

end TinylambOpt.ResearchProofs.Atlas.TextExecution
