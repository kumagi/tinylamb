import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `SuffixDictionary` — `∀ ws, (readBody ws.length (frontBody (ws.map
  List.reverse))).map (List.map List.reverse) = some ws`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_SuffixDictionary

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.SuffixDictionary.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.SuffixDictionary_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_SuffixDictionary
