import TinylambOpt.ResearchProofs.Atlas.RangePlans.OrderedPredecessorAndCrossChannelIndexes

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `CrossChannel.PageVerdict` — `∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
  pageScan shape b p g = scan shape p g.rows`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RangePlans_CrossChannel_PageVerdict

theorem sound : TinylambOpt.ResearchGoals.Atlas.RangePlans.CrossChannel.PageVerdict.Goal :=
  TinylambOpt.ResearchProofs.Atlas.RangePlans.OrderedPredecessorAndCrossChannelIndexes.sound_CrossChannel_PageVerdict

end TinylambOpt.ResearchProofs.Atlas.Goal_RangePlans_CrossChannel_PageVerdict
