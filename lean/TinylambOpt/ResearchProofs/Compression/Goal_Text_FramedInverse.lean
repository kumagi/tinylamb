import TinylambOpt.ResearchProofs.Compression.Text

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_FramedInverse

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.FramedInverse.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.FramedInverse_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_FramedInverse
