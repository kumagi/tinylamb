import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `ShardDictionaryBudget` — `∀ a b, let charge := fun (ws : Words) => let d :=
  dictionary ws ubytes d.length + (entries [] d).length let ids := fun (ws : Words)
  => byteCount (ws.length*width ((dictionary ws).length-1)) ((frontBody
  (a++b)).length:Int) - (frontBody a).length - (frontBody b).length = (charge
  (a++b):Int)-charge a-charge b + ids (a++b)-ids a-ids b`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_ShardDictionaryBudget

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.ShardDictionaryBudget.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.ShardDictionaryBudget_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_ShardDictionaryBudget
