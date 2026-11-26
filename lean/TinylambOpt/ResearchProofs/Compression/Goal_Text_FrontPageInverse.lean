import TinylambOpt.ResearchProofs.Compression.Text

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_FrontPageInverse

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.FrontPageInverse.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.FrontPageInverse_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_FrontPageInverse
