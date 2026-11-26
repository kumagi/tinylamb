import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `FramedInverse` — `∀ ws, ws.length < 2^32 → (frontBody ws).length < 2^32 → readPacket
  (packet 1 ws.length (frontBody ws)) = some ws`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_FramedInverse

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.FramedInverse.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.FramedInverse_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_FramedInverse
