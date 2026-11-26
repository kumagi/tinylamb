import TinylambOpt.ResearchProofs.Atlas.RangePlans.OrderedPredecessorAndCrossChannelIndexes

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `EitherEndpointInside.PrefixIndex` — `∀ b p rs, (∀ r ∈ rs, bounded b r.1) → indexed
  shape b p (prefixMass rs) = scan shape p rs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RangePlans_EitherEndpointInside_PrefixIndex

theorem sound : TinylambOpt.ResearchGoals.Atlas.RangePlans.EitherEndpointInside.PrefixIndex.Goal :=
  TinylambOpt.ResearchProofs.Atlas.RangePlans.OrderedPredecessorAndCrossChannelIndexes.EitherEndpointInsideProofs.sound_EitherEndpointInside_PrefixIndex

end TinylambOpt.ResearchProofs.Atlas.Goal_RangePlans_EitherEndpointInside_PrefixIndex
