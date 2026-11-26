import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `ReplicatedDictionary` — `∀ ws n, 0 < n → let repeated := (List.replicate n
  ws).flatten let d := dictionary ws (frontBody repeated).length = ubytes d.length +
  (entries [] d).length + byteCount (n*ws.length*width (d.length-1))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_ReplicatedDictionary

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.ReplicatedDictionary.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.ReplicatedDictionary_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_ReplicatedDictionary
