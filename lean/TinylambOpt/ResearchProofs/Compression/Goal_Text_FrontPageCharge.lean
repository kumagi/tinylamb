import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `FrontPageCharge` — `∀ ws, let d := dictionary ws (packet 1 ws.length (frontBody
  ws)).length = 9 + ubytes d.length + (entries [] d).length + byteCount
  (ws.length*width (d.length-1))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_FrontPageCharge

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.FrontPageCharge.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.FrontPageCharge_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_FrontPageCharge
