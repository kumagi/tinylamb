import TinylambOpt.ResearchProofs.Compression.Text

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_SuffixDictionary

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.SuffixDictionary.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.SuffixDictionary_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_SuffixDictionary
