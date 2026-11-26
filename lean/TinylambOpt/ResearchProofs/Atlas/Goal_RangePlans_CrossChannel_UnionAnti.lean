import TinylambOpt.ResearchProofs.Atlas.RangePlans.OrderedPredecessorAndCrossChannelIndexes

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `CrossChannel.UnionAnti` — `∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b
  r.1) → let es := compile shape b (ps.map (fun p => (p,1))) rs.filter (fun r =>
  !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RangePlans_CrossChannel_UnionAnti

theorem sound : TinylambOpt.ResearchGoals.Atlas.RangePlans.CrossChannel.UnionAnti.Goal :=
  TinylambOpt.ResearchProofs.Atlas.RangePlans.OrderedPredecessorAndCrossChannelIndexes.sound_CrossChannel_UnionAnti

end TinylambOpt.ResearchProofs.Atlas.Goal_RangePlans_CrossChannel_UnionAnti
