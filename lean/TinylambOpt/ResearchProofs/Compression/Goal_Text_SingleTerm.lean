import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `SingleTerm` — `∀ w n, 0 < n → frontBody (List.replicate n w) = frontBody [w]`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_SingleTerm

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.SingleTerm.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.SingleTerm_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_SingleTerm
