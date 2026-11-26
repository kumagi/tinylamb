import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `AdjacentExchange` — `∀ prev a b tail, ((entries prev (a::b::tail)).length:Int) -
  (entries prev (b::a::tail)).length = ((entry prev a).length:Int)+(entry a
  b).length-(entry prev b).length-(entry b a).length + (tail.head?.map (fun w =>
  ((entry b w).length:Int)-(entry a w).length)).getD 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_AdjacentExchange

/-- Adjacent-exchange charge: already proved in `Compression/Text.lean`. -/
theorem sound : TinylambOpt.ResearchGoals.Compression.Text.AdjacentExchange.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.AdjacentExchange_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_AdjacentExchange
