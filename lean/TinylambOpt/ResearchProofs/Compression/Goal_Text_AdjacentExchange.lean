import TinylambOpt.ResearchProofs.Compression.Text

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_AdjacentExchange

/-- Adjacent-exchange charge: already proved in `Compression/Text.lean`. -/
theorem sound : TinylambOpt.ResearchGoals.Compression.Text.AdjacentExchange.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.AdjacentExchange_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_AdjacentExchange
