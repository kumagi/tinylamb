import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `FrontEntryCharge` — `∀ a b, let k := common a b (entry a b).length = ubytes k +
  ubytes (b.length-k) + b.length-k`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_FrontEntryCharge

theorem sound : TinylambOpt.ResearchGoals.Compression.Text.FrontEntryCharge.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.FrontEntryCharge_sound

end TinylambOpt.ResearchProofs.Compression.Goal_Text_FrontEntryCharge
