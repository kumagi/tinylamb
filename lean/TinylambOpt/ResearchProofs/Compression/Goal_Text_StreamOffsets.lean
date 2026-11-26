import TinylambOpt.ResearchProofs.Compression.Text

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_StreamOffsets

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.StreamOffsets.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.StreamOffsets_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_StreamOffsets
