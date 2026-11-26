import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchGoals.Atlas.RangePlans
import TinylambOpt.ResearchProofs.Atlas.EncodedPages3
import TinylambOpt.ResearchProofs.Atlas.JoinPlans1
import TinylambOpt.ResearchProofs.Atlas.RangePlans3
import TinylambOpt.ResearchProofs.Claude1125.RangeRankSeek

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.Final5

/-!
# Final5 — last five Atlas goals

All five assigned goals were found to be **already proved** by earlier
proof files in `TinylambOpt/ResearchProofs/`.  This file registers each
goal under the canonical `<Leaf>_sound` name required by the DevinBatch
briefing; every theorem below is a direct alias of the covering proof
(verified by `#check`/`example` against the exact goal types, and the
covering files compile cleanly with no `sorry`/`admit`/`axiom`).

| Goal | Covering theorem | Location |
|---|---|---|
| `EncodedPages.SparseSample.SliceState.Goal` | `sound_SparseSample_SliceState` | `ResearchProofs/Atlas/EncodedPages3.lean:1392` |
| `JoinPlans.DoubleBilling.AnchoredRepair.Goal` | `DoubleBilling.sound_AnchoredRepair` | `ResearchProofs/Atlas/JoinPlans1.lean:1029` |
| `RangePlans.CrossChannel.PrefixIndex.Goal` | `sound_CrossChannel_PrefixIndex` | `ResearchProofs/Atlas/RangePlans3.lean` (`CrossChannelProofs`) |
| `RangePlans.EitherEndpointInside.PrefixIndex.Goal` | `sound_EitherEndpointInside_PrefixIndex` | `ResearchProofs/Atlas/RangePlans3.lean` (`EitherEndpointInsideProofs`) |
| `RangePlans.VarianceBudget.RankSeek.Goal` | `VarianceBudget.sound_RankSeek` | `ResearchProofs/Claude1125/RangeRankSeek.lean:965` |

The underlying proofs rely on `private` infrastructure inside those files
(`slice_from_rows_eq`, `anchored_repair_generic`, `prefixIndex_of_weight`,
`rankSeek_of_stencil`, …), so self-contained re-proofs are not practical;
the aliases below are the honest coverage record.
-/

/-- `SparseSample.SliceState.Goal` — proved by
`TinylambOpt.ResearchProofs.Atlas.EncodedPages3.SparseSample.sound_SparseSample_SliceState`. -/
theorem SparseSample_SliceState_sound :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample.SliceState.Goal :=
  TinylambOpt.ResearchProofs.Atlas.EncodedPages3.SparseSample.sound_SparseSample_SliceState

/-- `DoubleBilling.AnchoredRepair.Goal` — proved by
`TinylambOpt.ResearchProofs.Atlas.JoinPlans1.DoubleBilling.sound_AnchoredRepair`
(via the public generic lemma `anchored_repair_generic`). -/
theorem DoubleBilling_AnchoredRepair_sound :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.DoubleBilling.AnchoredRepair.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans1.DoubleBilling.sound_AnchoredRepair

/-- `CrossChannel.PrefixIndex.Goal` — proved by
`TinylambOpt.ResearchProofs.Atlas.RangePlans3.CrossChannelProofs.sound_CrossChannel_PrefixIndex`. -/
theorem CrossChannel_PrefixIndex_sound :
    TinylambOpt.ResearchGoals.Atlas.RangePlans.CrossChannel.PrefixIndex.Goal :=
  TinylambOpt.ResearchProofs.Atlas.RangePlans3.CrossChannelProofs.sound_CrossChannel_PrefixIndex

/-- `EitherEndpointInside.PrefixIndex.Goal` — proved by
`TinylambOpt.ResearchProofs.Atlas.RangePlans3.EitherEndpointInsideProofs.sound_EitherEndpointInside_PrefixIndex`. -/
theorem EitherEndpointInside_PrefixIndex_sound :
    TinylambOpt.ResearchGoals.Atlas.RangePlans.EitherEndpointInside.PrefixIndex.Goal :=
  TinylambOpt.ResearchProofs.Atlas.RangePlans3.EitherEndpointInsideProofs.sound_EitherEndpointInside_PrefixIndex

/-- `VarianceBudget.RankSeek.Goal` — proved by
`TinylambOpt.ResearchProofs.Claude1125.RangeRankSeek.VarianceBudget.sound_RankSeek`. -/
theorem VarianceBudget_RankSeek_sound :
    TinylambOpt.ResearchGoals.Atlas.RangePlans.VarianceBudget.RankSeek.Goal :=
  TinylambOpt.ResearchProofs.Claude1125.RangeRankSeek.VarianceBudget.sound_RankSeek

end TinylambOpt.ResearchProofs.DevinBatch.Final5
