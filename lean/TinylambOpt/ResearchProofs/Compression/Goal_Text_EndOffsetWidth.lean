import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `EndOffsetWidth` — `∀ (ws : Words) w, (∀ i ≤ ws.length, ((ws.take i).map
  List.length).sum < 2^w) ↔ ws.flatten.length < 2^w`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_EndOffsetWidth

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.EndOffsetWidth.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.EndOffsetWidth_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_EndOffsetWidth
