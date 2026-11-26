import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets
import TinylambOpt.ResearchProofs.JitKernels.BailoutPacketsLemmas

/-! A fault reported by a compacted packet comes from the selected row at the
reported compacted lane. With in-bounds selection ordinals, the gather preserves
selection order, so execution can map the reported lane back to the physical row
and its error code. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_BailoutPackets_CompactedBailoutOrdinal

open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets
open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.CompactedBailoutOrdinal
open Nov26.BailoutPacketsLemmas

theorem sound : Goal := by
  intro xs sel j e hb hfirst
  have hg : (gather xs sel)[j]? = some (.error e) :=
    first_error_at (gather xs sel) j e hfirst
  rw [gather_getElem? xs sel hb j] at hg
  cases hselj : sel[j]? with
  | none => simp [hselj] at hg
  | some i =>
      refine ⟨i, rfl, ?_⟩
      simpa [hselj] using hg

end Nov26.Proof_BailoutPackets_CompactedBailoutOrdinal
