import TinylambOpt.ResearchProofs.Compression.Text

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_FrontInverse

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.FrontInverse.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.FrontInverse_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_FrontInverse
