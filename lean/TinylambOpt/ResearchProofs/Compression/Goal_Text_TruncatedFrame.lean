import TinylambOpt.ResearchProofs.Compression.Text

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_TruncatedFrame

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.TruncatedFrame.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.TruncatedFrame_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_TruncatedFrame
