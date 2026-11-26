import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
Truncating a negative biased bitset index to zero does NOT implement the
range check: the claimed identity instead wraps the biased index modulo the
bitset size, so an out-of-domain key below `lo` aliases onto an in-domain
slot. With keys `[[0]]`, `lo = 0`, `n = 1`, probing `-1` misses while the
wrapped probe `0 + ((-1 - 0).toNat % 1) = 0` hits, so `keep` differs. The
resulting equation is a closed ground statement, refuted by `decide`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.InListLowering_BiasedBitsetOutlier
open TinylambOpt.ResearchGoals.JitKernels.InListLowering

theorem refute : ¬ BiasedBitsetOutlier.Goal := by
  intro h
  have hbounds : ∀ v : Int, some v ∈ ([some 0] : Keys) → 0 ≤ v ∧ v < 0 + (1 : Nat) := by
    intro v hv
    rw [List.mem_singleton] at hv
    have h2 := Option.some.inj hv
    subst h2
    exact ⟨by omega, by omega⟩
  exact absurd (h [some 0] 0 1 (-1) (by decide) hbounds) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.InListLowering_BiasedBitsetOutlier
