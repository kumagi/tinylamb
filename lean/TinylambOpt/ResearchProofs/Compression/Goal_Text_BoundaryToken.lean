import TinylambOpt.ResearchProofs.Compression.Text

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_BoundaryToken

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.BoundaryToken.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.BoundaryToken_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_BoundaryToken
