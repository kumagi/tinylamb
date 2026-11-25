import TinylambOpt.ResearchGoals.Atlas.TextExecution
import TinylambOpt.ResearchProofs.Atlas_TextExecution_Grammar
import TinylambOpt.ResearchProofs.Atlas_TextExecution_EditFilter

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.Atlas_TextExecution_A

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Grammar
open TinylambOpt.ResearchGoals.Atlas.TextExecution.EditFilter

/-!
### Coverage notes

The following goals already have fully checked proofs in earlier proof files
(`TinylambOpt/ResearchProofs/Atlas_TextExecution_Grammar.lean` and
`TinylambOpt/ResearchProofs/Atlas_TextExecution_EditFilter.lean`, both verified
with `lake env lean`). The theorems below restate them under the required
`<Leaf>_sound` names so this file is a complete record for the batch.

* `Grammar.InteriorCache` is the documented counterexample (issues/research/atlas.md):
  `[1]` and `[0]` both have 0 occurrences of `[1,2]` internally, but after
  appending `[2]` the counts are 1 and 0 — refuted below.
* `EditFilter.PigeonholeAnchor` and `EditFilter.PositionalAnchor` are
  UNRESOLVED in this file: the only prior attempt at `PigeonholeAnchor`
  (`TinylambOpt/ResearchProofs/Atlas/Goal_EditFilter_PigeonholeAnchor.lean`)
  does not compile in this toolchain (`List.getElem?_mem` unknown), and no
  covering proof of `PositionalAnchor` exists.
-/

/-- Already proved in `TinylambOpt.ResearchProofs.Atlas.TextExecution.Grammar`. -/
theorem BoundaryHits_sound :
    TinylambOpt.ResearchGoals.Atlas.TextExecution.Grammar.BoundaryHits.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.Grammar.sound_BoundaryHits

/-- Already proved in `TinylambOpt.ResearchProofs.Atlas.TextExecution.Grammar`. -/
theorem LongPower_sound :
    TinylambOpt.ResearchGoals.Atlas.TextExecution.Grammar.LongPower.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.Grammar.sound_LongPower

/-- Already proved in `TinylambOpt.ResearchProofs.Atlas.TextExecution.Grammar`. -/
theorem ShortPower_sound :
    TinylambOpt.ResearchGoals.Atlas.TextExecution.Grammar.ShortPower.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.Grammar.sound_ShortPower

/-- Already proved in `TinylambOpt.ResearchProofs.Atlas.TextExecution.Grammar`. -/
theorem SliceDecode_sound :
    TinylambOpt.ResearchGoals.Atlas.TextExecution.Grammar.SliceDecode.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.Grammar.sound_SliceDecode

/-- `Grammar.InteriorCache` is false: `[1]` and `[0]` have equal length and equal
    internal `[1,2]`-occurrence counts (0), but appending `[2]` yields 1 vs 0. -/
theorem InteriorCache_disproof :
    ¬ TinylambOpt.ResearchGoals.Atlas.TextExecution.Grammar.InteriorCache.Goal := by
  intro h
  have hh := h [1] [0] [1, 2] (by decide) (by decide) [2]
  have hneq : ¬ ((positions ([1] ++ [2] : Word) [1, 2]).length =
      (positions ([0] ++ [2] : Word) [1, 2]).length) := by
    native_decide
  exact hneq hh

/-- Already proved in `TinylambOpt.ResearchProofs.Atlas.TextExecution.EditFilter`. -/
theorem GramLowerBound_sound :
    TinylambOpt.ResearchGoals.Atlas.TextExecution.EditFilter.GramLowerBound.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.EditFilter.sound_GramLowerBound

/-- Already proved in `TinylambOpt.ResearchProofs.Atlas.TextExecution.EditFilter`. -/
theorem ClassLength_sound :
    TinylambOpt.ResearchGoals.Atlas.TextExecution.EditFilter.ClassLength.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.EditFilter.sound_ClassLength

/-- Already proved in `TinylambOpt.ResearchProofs.Atlas.TextExecution.EditFilter`. -/
theorem HistogramFence_sound :
    TinylambOpt.ResearchGoals.Atlas.TextExecution.EditFilter.HistogramFence.Goal :=
  TinylambOpt.ResearchProofs.Atlas.TextExecution.EditFilter.sound_HistogramFence

end TinylambOpt.ResearchProofs.DevinBatch.Atlas_TextExecution_A
