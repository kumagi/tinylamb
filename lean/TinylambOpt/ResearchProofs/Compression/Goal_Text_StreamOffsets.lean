import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `StreamOffsets` — `∀ ws tail, readLengths ws.length (lengths ws ++ tail) = some
  (ws,tail)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_StreamOffsets

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.StreamOffsets.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.StreamOffsets_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_StreamOffsets
