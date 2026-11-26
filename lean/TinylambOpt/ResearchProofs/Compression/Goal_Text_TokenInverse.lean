import TinylambOpt.ResearchProofs.Compression.Text

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_TokenInverse

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.TokenInverse.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.TokenInverse_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_TokenInverse
