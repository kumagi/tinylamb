import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
import TinylambOpt.ResearchGoals.Atlas.TextExecution

-- The two assigned goals already have fully checked proofs in the
-- workspace (both modules are imported by the root `TinylambOpt.lean`).
-- Reuse those proofs here so that this batch file exposes the goals under the
-- mandated `Auto_X1` namespace without duplicating hundreds of lines.
import TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_PrefixTransport
import TinylambOpt.ResearchProofs.Atlas.Goal_EditFilter_PositionalAnchor

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Auto_X1

/-- The prefix-CDF transport formula is true: `Couplings.PrefixTransport.Goal`.
    Proof carried over from
    `Goal_Couplings_PrefixTransport.sound_Couplings_PrefixTransport`. -/
theorem sound_Couplings_PrefixTransport_X1 :
    TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Couplings.PrefixTransport.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_PrefixTransport.sound_Couplings_PrefixTransport

/-- The positional-anchor bound is true: `EditFilter.PositionalAnchor.Goal`.
    Proof carried over from
    `Goal_EditFilter_PositionalAnchor.sound_EditFilter_PositionalAnchor`. -/
theorem sound_EditFilter_PositionalAnchor_X1 :
    TinylambOpt.ResearchGoals.Atlas.TextExecution.EditFilter.PositionalAnchor.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Goal_EditFilter_PositionalAnchor.sound_EditFilter_PositionalAnchor

end TinylambOpt.ResearchProofs.Atlas.Auto_X1
