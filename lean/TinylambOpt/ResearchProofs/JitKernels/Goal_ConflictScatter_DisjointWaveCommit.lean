import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
import TinylambOpt.ResearchProofs.JitKernels.Common_ConflictScatter

/-! Two waves with disjoint leader sets can publish independently: for any slot
only one wave contributes, and the concatenated wave has the same delta. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_DisjointWaveCommit

open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
open TinylambOpt.ResearchProofs.JitKernels.Common_ConflictScatter

theorem sound : DisjointWaveCommit.Goal := by
  intro a b k hdisj
  rw [delta_append]
  by_cases hka : k ∈ leaders a
  · have hkb : k ∉ leaders b := fun hb => hdisj k hka hb
    have hdb : delta b k = 0 := delta_zero_of_not_mem b k (by simpa [leaders] using hkb)
    simp [hka, hdb]
  · have hda : delta a k = 0 := delta_zero_of_not_mem a k (by simpa [leaders] using hka)
    by_cases hkb : k ∈ leaders b
    · simp [hka, hda]
    · have hdb : delta b k = 0 := delta_zero_of_not_mem b k (by simpa [leaders] using hkb)
      simp [hka, hda, hdb]

end TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_DisjointWaveCommit
