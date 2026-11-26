import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `FrontInverse` — `∀ prev ws tail, readEntries ws.length prev (entries prev ws ++
  tail) = some (ws,tail)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_FrontInverse

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.FrontInverse.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.FrontInverse_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_FrontInverse
