import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `TruncatedFrame` — `∀ ws n, (frontBody ws).length < 2^32 → let frame := packet 1
  ws.length (frontBody ws) n < frame.length → readPacket (frame.take n) = none`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_TruncatedFrame

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.TruncatedFrame.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.TruncatedFrame_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_TruncatedFrame
