import TinylambOpt.ResearchProofs.Atlas.RangePlans3

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RangePlans_CrossChannel_PrefixIndex

theorem sound : TinylambOpt.ResearchGoals.Atlas.RangePlans.CrossChannel.PrefixIndex.Goal :=
  TinylambOpt.ResearchProofs.Atlas.RangePlans3.CrossChannelProofs.sound_CrossChannel_PrefixIndex

end TinylambOpt.ResearchProofs.Atlas.Goal_RangePlans_CrossChannel_PrefixIndex
