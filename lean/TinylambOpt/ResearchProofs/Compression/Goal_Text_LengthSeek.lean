import TinylambOpt.ResearchProofs.Compression.Text

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_LengthSeek

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.LengthSeek.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.LengthSeek_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_LengthSeek
