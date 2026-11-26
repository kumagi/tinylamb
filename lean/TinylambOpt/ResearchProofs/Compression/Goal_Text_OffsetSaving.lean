import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `OffsetSaving` — `∀ ws overhead, (overhead + (lengths ws).length < raw ws ↔ overhead
  + streamBytes (ws.map List.length) < 4*ws.length)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_OffsetSaving

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.OffsetSaving.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.OffsetSaving_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_OffsetSaving
