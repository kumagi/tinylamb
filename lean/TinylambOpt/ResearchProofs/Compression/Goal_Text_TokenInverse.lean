import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `TokenInverse` — `∀ pat s fuel, (∀ x ∈ s, x < 256) → expand pat (symbols pat fuel s)
  = s`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_TokenInverse

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.TokenInverse.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.TokenInverse_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_TokenInverse
