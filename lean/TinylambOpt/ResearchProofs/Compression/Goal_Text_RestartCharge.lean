import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `RestartCharge` — `∀ prev pre post, let last := pre.getLast?.getD prev ((entries prev
  pre).length:Int)+(entries [] post).length-(entries prev (pre++post)).length =
  (post.head?.map (fun w => ((entry [] w).length:Int)-(entry last w).length)).getD 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_RestartCharge

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.RestartCharge.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.RestartCharge_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_RestartCharge
