import TinylambOpt.ResearchGoals.Atlas.TextExecution
import TinylambOpt.ResearchProofs.Atlas_TextExecution_Grammar

/-!
SQL string predicates can use encoded or summarized text when the modeled match,
boundaries, and collation are preserved.

Proved

- `Grammar.BoundaryHits` — `∀ a b p, p ≠ [] → (positions (a++b) p).length = (positions
  a p).length + (positions b p).length + crossing a b p`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Grammar_BoundaryHits

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Grammar

/-! Exhaustive check over all binary words of length <= 3 for `a`, `b` and
    non-empty `p` (`false` means no counterexample). The general counting
    argument is established as `sound_BoundaryHits` in the grammar proof module. -/
#eval
  let words : List Word :=
    (List.range 4).flatMap (fun n =>
      (List.range (2 ^ n)).map (fun k =>
        (List.range n).map (fun j => (k / 2 ^ (n - 1 - j)) % 2)))
  let check (a b p : Word) : Bool :=
    decide ((positions (a ++ b) p).length =
      (positions a p).length + (positions b p).length + crossing a b p)
  (words.flatMap (fun a =>
    words.flatMap (fun b =>
      (words.filter (fun p => p ≠ [])).map (fun p => check a b p)))).contains false

theorem sound_Grammar_BoundaryHits : Grammar.BoundaryHits.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.TextExecution.Grammar.sound_BoundaryHits

end TinylambOpt.ResearchProofs.Atlas.Goal_Grammar_BoundaryHits
