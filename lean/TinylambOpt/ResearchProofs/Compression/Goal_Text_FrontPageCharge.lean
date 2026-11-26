import TinylambOpt.ResearchProofs.Compression.Text

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_FrontPageCharge

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.FrontPageCharge.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.FrontPageCharge_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_FrontPageCharge
