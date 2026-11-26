import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `TokenBreakEven` — `∀ pat s fee, pat ≠ [] → (∀ x ∈ s, x < 256) → let ts := symbols
  pat s.length s (fee + ts.length < s.length ↔ fee < ts.count 256 * (pat.length-1))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_TokenBreakEven

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.TokenBreakEven.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.TokenBreakEven_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_TokenBreakEven
