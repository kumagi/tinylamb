import TinylambOpt.ResearchProofs.Atlas.RangePlans.OrderedPredecessorAndCrossChannelIndexes

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `CrossChannel.PrefixIndex` — `∀ b p rs, (∀ r ∈ rs, bounded b r.1) → indexed shape b p
  (prefixMass rs) = scan shape p rs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RangePlans_CrossChannel_PrefixIndex

theorem sound : TinylambOpt.ResearchGoals.Atlas.RangePlans.CrossChannel.PrefixIndex.Goal :=
  TinylambOpt.ResearchProofs.Atlas.RangePlans.OrderedPredecessorAndCrossChannelIndexes.CrossChannelProofs.sound_CrossChannel_PrefixIndex

end TinylambOpt.ResearchProofs.Atlas.Goal_RangePlans_CrossChannel_PrefixIndex
