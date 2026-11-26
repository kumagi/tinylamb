import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `EmptyTerm` — `∀ n tail, readEntries n [] (entries [] (List.replicate n []) ++ tail)
  = some (List.replicate n [],tail)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_EmptyTerm

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.EmptyTerm.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.EmptyTerm_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_EmptyTerm
