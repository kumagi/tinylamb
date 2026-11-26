import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Soundness of "late gather inside the matching-signature bucket": pre-filtering the build
pool by the raw signature test `sig s == sig probe` before the exact-byte test
`s == probe` removes no rows, because equal strings always have equal signatures — the
signature test is redundant for strings that already pass the byte test, and duplicate
build rows are preserved (both filters are order-preserving and keep every byte-match).
Claim: `(pool.filter (sig · == sig probe)).filter (· == probe) = pool.filter (· == probe)`.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.SampledEquality_VerifiedBucketLateGather
open TinylambOpt.ResearchGoals.JitKernels.SampledEquality

theorem sound : VerifiedBucketLateGather.Goal := by
  intro pool probe
  rw [List.filter_filter]
  refine List.filter_congr (fun x _ => ?_)
  by_cases hbe : (x == probe) = true
  · obtain rfl : x = probe := (beq_iff_eq (α := Bytes)).mp hbe
    simp
  · rw [show (x == probe) = false from by rw [← Bool.not_eq_true]; exact hbe]
    simp

end TinylambOpt.ResearchProofs.JitKernels.SampledEquality_VerifiedBucketLateGather
