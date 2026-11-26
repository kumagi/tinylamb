import TinylambOpt.ResearchProofs.Compression.Text

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_OffsetSaving

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.OffsetSaving.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.OffsetSaving_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_OffsetSaving
