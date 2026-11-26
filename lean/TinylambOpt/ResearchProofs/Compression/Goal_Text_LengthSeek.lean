import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `LengthSeek` — `∀ (ws : Words) (i : Nat), i < ws.length → (((ws.flatten.drop
  ((ws.take i).map List.length).sum).take (ws[i]?.getD []).length)) = ws[i]?.getD []`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_LengthSeek

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.LengthSeek.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.LengthSeek_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_LengthSeek
