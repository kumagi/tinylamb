import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
import TinylambOpt.ResearchProofs.JitKernels.Common_ConflictScatter

/-! When every matching delta is nonnegative, the partial sums of that slot's
updates lie between the initial counter and the final counter. A two-endpoint
range certificate is then sufficient to check all intermediate values. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_MonotoneScatterRangeCertificate

open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
open TinylambOpt.ResearchProofs.JitKernels.Common_ConflictScatter

theorem sound : MonotoneScatterRangeCertificate.Goal := by
  intro us k lo hi s hnonneg hlo hhi
  unfold check
  apply scanl_bounds
  · intro x hx
    rcases List.mem_map.mp hx with ⟨u, hu, rfl⟩
    exact hnonneg u (List.mem_filter.mp hu).1 (by
      have := (List.mem_filter.mp hu).2
      simpa using this)
  · exact hlo
  · simpa [delta] using hhi

end TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_MonotoneScatterRangeCertificate
