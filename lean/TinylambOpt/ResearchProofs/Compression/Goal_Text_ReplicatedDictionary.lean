import TinylambOpt.ResearchProofs.Compression.Text

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_ReplicatedDictionary

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.ReplicatedDictionary.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.ReplicatedDictionary_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_ReplicatedDictionary
