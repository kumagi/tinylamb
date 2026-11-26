import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `CommonStemBudget` — `∀ stem tails, let ws := tails.map (stem ++ ·)
  tails.length*(stem.length) + (lengths tails).length ≤ (lengths ws).length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_CommonStemBudget

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.CommonStemBudget.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.CommonStemBudget_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_CommonStemBudget
