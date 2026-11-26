import TinylambOpt.ResearchProofs.Atlas.RangePlans3

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RangePlans_CrossChannel_UnionAnti

theorem sound : TinylambOpt.ResearchGoals.Atlas.RangePlans.CrossChannel.UnionAnti.Goal :=
  TinylambOpt.ResearchProofs.Atlas.RangePlans3.sound_CrossChannel_UnionAnti

end TinylambOpt.ResearchProofs.Atlas.Goal_RangePlans_CrossChannel_UnionAnti
