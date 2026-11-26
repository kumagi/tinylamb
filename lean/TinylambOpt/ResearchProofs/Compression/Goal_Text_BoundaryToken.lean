import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `BoundaryToken` — `∀ (a b : Bytes) (x y : Nat), x < 256 → y < 256 → x ≠ y → (symbols
  [x,y] (a++b).length (a++b)).count 256 = (symbols [x,y] a.length a).count 256 +
  (symbols [x,y] b.length b).count 256 + (if a.getLast? == some x && b.head? == some
  y then 1 else 0)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_BoundaryToken

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.BoundaryToken.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.BoundaryToken_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_BoundaryToken
