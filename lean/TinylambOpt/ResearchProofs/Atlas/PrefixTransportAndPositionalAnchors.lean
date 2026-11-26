import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
import TinylambOpt.ResearchGoals.Atlas.TextExecution

-- The two assigned goals already have fully checked proofs in the
-- workspace (both modules are imported by the root `TinylambOpt.lean`).
-- Reuse those proofs here so that this batch file exposes the goals under the
-- mandated `Auto_X1` namespace without duplicating hundreds of lines.
import TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_PrefixTransport
import TinylambOpt.ResearchProofs.Atlas.Goal_EditFilter_PositionalAnchor

/-!
SQL string predicates can use encoded or summarized text when the modeled match,
boundaries, and collation are preserved.

Proved

- `Couplings.PrefixTransport` — `∀ rs, let bins := ((rs.map Prod.fst)++(rs.map
  Prod.snd)).mergeSort.eraseDups cost (monotone rs) = ((bins.zip (bins.drop 1)).map
  (fun (a,b) => (b-a).toNat * ((rs.countP (·.1 ≤ a):Int)-rs.countP (·.2 ≤
  a)).natAbs)).sum`
- `EditFilter.PositionalAnchor` — `∀ (ps : List Word) t k, k < ps.length → (∀ p ∈ ps, p
  ≠ []) → Edits k ps.flatten t → ∃ i < ps.length, ∃ j ∈ positions t (ps[i]?.getD []),
  ((j:Int) - ((ps.take i).map List.length).sum).natAbs ≤ k`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.PrefixTransportAndPositionalAnchors

/-- The prefix-CDF transport formula is true: `Couplings.PrefixTransport.Goal`.
    Proof carried over from
    `Goal_Couplings_PrefixTransport.sound_Couplings_PrefixTransport`. -/
theorem sound_Couplings_PrefixTransport :
    TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Couplings.PrefixTransport.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_PrefixTransport.sound_Couplings_PrefixTransport

/-- The positional-anchor bound is true: `EditFilter.PositionalAnchor.Goal`.
    Proof carried over from
    `Goal_EditFilter_PositionalAnchor.sound_EditFilter_PositionalAnchor`. -/
theorem sound_EditFilter_PositionalAnchor :
    TinylambOpt.ResearchGoals.Atlas.TextExecution.EditFilter.PositionalAnchor.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Goal_EditFilter_PositionalAnchor.sound_EditFilter_PositionalAnchor

end TinylambOpt.ResearchProofs.Atlas.PrefixTransportAndPositionalAnchors
