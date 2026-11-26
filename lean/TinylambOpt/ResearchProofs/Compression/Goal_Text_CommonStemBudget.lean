import TinylambOpt.ResearchProofs.Compression.Text

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_CommonStemBudget

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.CommonStemBudget.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.CommonStemBudget_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_CommonStemBudget
