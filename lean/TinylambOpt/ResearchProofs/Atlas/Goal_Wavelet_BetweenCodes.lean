import TinylambOpt.ResearchGoals.Atlas.TextExecution

/-!
SQL string predicates can use encoded or summarized text when the modeled match,
boundaries, and collation are preserved.

Proved

- `Wavelet.BetweenCodes` — `∀ (xs : Word) level lo hi, lo ≤ hi → xs.countP (fun x =>
  lo*2^level ≤ x && x < hi*2^level) = xs.countP (fun x => lo ≤ x/2^level && x/2^level
  < hi)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Wavelet_BetweenCodes

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Wavelet

theorem sound_Wavelet_BetweenCodes : Wavelet.BetweenCodes.Goal := by
  intro xs level lo hi hlo
  apply List.countP_congr
  intro x hx
  have hp : 0 < 2 ^ level := Nat.two_pow_pos level
  simp [Nat.le_div_iff_mul_le hp, Nat.div_lt_iff_lt_mul hp]

end TinylambOpt.ResearchProofs.Atlas.Goal_Wavelet_BetweenCodes
