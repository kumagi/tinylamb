import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.SeekMinimumCommon

/-!
An index range plan must cover every qualifying key and preserve any gaps excluded by
the SQL predicate.

Proved (8 claims, including the following)

- `DistanceBand.SeekMinimum` — The emitted seek count equals the number of maximal
  contiguous true regions.
- `MinClamp.SeekMinimum` — The emitted seek count equals the number of maximal
  contiguous true regions.
- `NotEqual.SeekMinimum` — The emitted seek count equals the number of maximal
  contiguous true regions.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.SeekMinimumModels

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.SeekMinimumCommon

theorem notEqual_sep_strict : ∀ p, (NotEqual.model.plan p).Pairwise (fun s t : Span => s.2 < t.1) := by
  intro p
  simp [NotEqual.model]

theorem outsideClosed_sep_strict : ∀ p, (OutsideClosed.model.plan p).Pairwise (fun s t : Span => s.2 < t.1) := by
  intro p
  by_cases h : p.a ≤ p.b <;> simp [OutsideClosed.model, h] <;> omega

theorem ringSlice_sep_strict : ∀ p, (RingSlice.model.plan p).Pairwise (fun s t : Span => s.2 < t.1) := by
  intro p
  by_cases h : p.a ≤ p.b <;> simp [RingSlice.model, h] <;> omega

theorem distanceBand_sep_strict : ∀ p, (DistanceBand.model.plan p).Pairwise (fun s t : Span => s.2 < t.1) := by
  intro p
  simp [DistanceBand.model]

theorem quotientBand_sep_strict : ∀ p, (QuotientBand.model.plan p).Pairwise (fun s t : Span => s.2 < t.1) := by
  intro p
  simp [QuotientBand.model]

theorem saturatingSubtract_sep_strict : ∀ p, (SaturatingSubtract.model.plan p).Pairwise (fun s t : Span => s.2 < t.1) := by
  intro p
  by_cases h : p.b = 0 <;> simp [SaturatingSubtract.model, h]

theorem minClamp_sep_strict : ∀ p, (MinClamp.model.plan p).Pairwise (fun s t : Span => s.2 < t.1) := by
  intro p
  by_cases h1 : p.b < p.a <;> by_cases h2 : p.a = p.b <;> simp [MinClamp.model, h1, h2]

theorem xorLowBit_sep_strict : ∀ p, (XorLowBit.model.plan p).Pairwise (fun s t : Span => s.2 < t.1) := by
  intro p
  by_cases hba : p.b < p.a
  · simp [XorLowBit.model, hba]
  · simp [XorLowBit.model, hba]
    by_cases ha : p.a % 2 = 1 <;> by_cases hb : p.b % 2 = 0 <;> simp [ha, hb] <;> omega

theorem notEqual_seekMin : NotEqual.SeekMinimum.Goal :=
  seekMinimum_of_exact_strict NotEqual.model notEqual_exact notEqual_sep_strict

theorem outsideClosed_seekMin : OutsideClosed.SeekMinimum.Goal :=
  seekMinimum_of_exact_strict OutsideClosed.model outsideClosed_exact outsideClosed_sep_strict

theorem ringSlice_seekMin : RingSlice.SeekMinimum.Goal :=
  seekMinimum_of_exact_strict RingSlice.model ringSlice_exact ringSlice_sep_strict

theorem distanceBand_seekMin : DistanceBand.SeekMinimum.Goal :=
  seekMinimum_of_exact_strict DistanceBand.model distanceBand_exact distanceBand_sep_strict

theorem quotientBand_seekMin : QuotientBand.SeekMinimum.Goal :=
  seekMinimum_of_exact_strict QuotientBand.model quotientBand_exact quotientBand_sep_strict

theorem saturatingSubtract_seekMin : SaturatingSubtract.SeekMinimum.Goal :=
  seekMinimum_of_exact_strict SaturatingSubtract.model saturatingSubtract_exact saturatingSubtract_sep_strict

theorem minClamp_seekMin : MinClamp.SeekMinimum.Goal :=
  seekMinimum_of_exact_strict MinClamp.model minClamp_exact minClamp_sep_strict

theorem xorLowBit_seekMin : XorLowBit.SeekMinimum.Goal :=
  seekMinimum_of_exact_strict XorLowBit.model xorLowBit_exact xorLowBit_sep_strict

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.SeekMinimumModels
