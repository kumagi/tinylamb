import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

/-!
`SegmentTailSharingBound`: for a positive unit size, the block count of a fused
segment is within one block of the sum of the separate counts. This supports SQL
spill replacement where two adjacent runs may share one partially filled tail
block but never two.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_SegmentTailSharingBound

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

private theorem blocks_mul_le (hu : 0 < u) (n : Nat) : n ≤ blocks u n * u := by
  unfold blocks
  have hmod := Nat.mod_lt (n + u - 1) hu
  have hdecomp := Nat.div_add_mod (n + u - 1) u
  rw [Nat.mul_comm u ((n + u - 1) / u)] at hdecomp
  omega

private theorem blocks_mul_lt (hu : 0 < u) (n : Nat) : blocks u n * u < n + u := by
  have h := blocks_mul_le hu n
  unfold blocks at h ⊢
  have hmod := Nat.mod_lt (n + u - 1) hu
  have hdecomp := Nat.div_add_mod (n + u - 1) u
  rw [Nat.mul_comm u ((n + u - 1) / u)] at hdecomp
  omega

theorem sound : SegmentTailSharingBound.Goal := by
  intro u a b hu
  constructor
  · unfold blocks
    rw [Nat.div_le_iff_le_mul hu]
    rw [Nat.add_mul]
    have ha := blocks_mul_le hu a
    have hb := blocks_mul_le hu b
    unfold blocks at ha hb
    omega
  · let q := blocks u (a + b)
    let qa := blocks u a
    let qb := blocks u b
    have hab : a + b ≤ q * u := blocks_mul_le hu (a + b)
    have hqa : qa * u < a + u := blocks_mul_lt hu a
    have hqb : qb * u < b + u := blocks_mul_lt hu b
    change qa + qb ≤ q + 1
    by_cases hle : qa + qb ≤ q + 1
    · exact hle
    · have hc : q + 2 ≤ qa + qb := by omega
      have hmul : (q + 2) * u ≤ (qa + qb) * u := Nat.mul_le_mul_right u hc
      rw [Nat.add_mul] at hmul
      have hsum : (qa + qb) * u < a + b + 2 * u := by
        rw [Nat.add_mul]
        omega
      have hle2 : a + b + 2 * u ≤ q * u + 2 * u := Nat.add_le_add_right hab (2 * u)
      omega

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_SegmentTailSharingBound
