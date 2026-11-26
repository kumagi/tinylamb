import TinylambOpt.ResearchProofs.Compression.Text

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_EndOffsetWidth

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.EndOffsetWidth.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.EndOffsetWidth_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_EndOffsetWidth
