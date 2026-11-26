import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
/-!
Registers the checked result for `SharedVocabulary.OverlapPaysWiderIds.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/

/-!
`OverlapPaysWiderIds`: refuted.  Any overlap at all makes the greedy "share the
dictionary" heuristic pass its test, but the merged id stream can jump to a wider
varint class.  Counterexample: two dictionaries of 65 symbols each — the empty
symbol plus 64 one-byte payloads — overlapping only in the empty symbol.  Each
group packs its ids in 7 bits; the merge dedups to 129 keys, forcing 8-bit ids
across the whole 130-row stream, and the one saved length prefix does not pay for
the extra id byte, so `charge (a ++ b) > charge a + charge b` despite the overlap.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SharedVocabulary_OverlapPaysWiderIds
open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
open TinylambOpt.ResearchGoals.Compression.Wire

private def AA : List Symbol := [] :: (List.range 64).map (fun i => [i])
private def BB : List Symbol := [] :: (List.range 64).map (fun i => [i + 64])

theorem refute : ¬ OverlapPaysWiderIds.Goal := by
  intro h
  have hov : (vocab AA).any (fun s => (vocab BB).contains s) = true := by
    native_decide
  have hle := h AA BB hov
  have hnot : ¬ charge (AA ++ BB) ≤ charge AA + charge BB := by
    native_decide
  exact hnot hle

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SharedVocabulary_OverlapPaysWiderIds
