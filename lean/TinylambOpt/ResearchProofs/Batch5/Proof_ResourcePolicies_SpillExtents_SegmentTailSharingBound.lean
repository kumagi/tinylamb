import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SpillExtents.Goal_SegmentTailSharingBound

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents


/-! Scratch proof: two consecutive segments can share at most the unused part
    of one alignment block. Combining their byte lengths therefore changes the
    block charge by at most one, bounding the benefit of tail sharing. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

private theorem le_unit_mul_blocks (u n : Nat) (hu : 0 < u) : n ≤ u * blocks u n := by
  unfold blocks
  have h := Nat.lt_div_mul_add (a := n + u - 1) hu
  have h' : n + u - 1 < u * ((n + u - 1) / u) + u := by
    simpa [Nat.mul_comm] using h
  omega

private theorem pred_mul_blocks_lt (u n : Nat) (hu : 0 < u) (hn : 0 < n) :
    u * (blocks u n - 1) < n := by
  unfold blocks
  have hpos : 0 < (n + u - 1) / u := Nat.div_pos (by omega) hu
  have hlt : (n + u - 1) / u - 1 < (n + u - 1) / u := by omega
  have h := (Nat.lt_div_iff_mul_lt hu).mp hlt
  have hsub : n + u - 1 - (u - 1) = n := by omega
  rw [hsub] at h
  simpa [Nat.mul_comm] using h

private theorem blocks_zero_iff (u n : Nat) (hu : 0 < u) : blocks u n = 0 ↔ n = 0 := by
  unfold blocks
  constructor
  · intro h
    have hlt : n + u - 1 < u := by
      have hq : (n + u - 1) / u < 1 := by omega
      simpa using (Nat.div_lt_iff_lt_mul hu).mp hq
    omega
  · intro h
    subst n
    exact Nat.div_eq_of_lt (by omega : 0 + u - 1 < u)

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents.SegmentTailSharingBound.Goal := by
  intro u a b hu
  have ha_le : a ≤ u * blocks u a := le_unit_mul_blocks u a hu
  have hb_le : b ≤ u * blocks u b := le_unit_mul_blocks u b hu
  simp only [blocks] at ha_le hb_le
  constructor
  · unfold blocks
    rw [Nat.div_le_iff_le_mul_add_pred hu]
    have : a + b + u - 1 ≤ u * ((a + u - 1) / u) + u * ((b + u - 1) / u) + (u - 1) := by
      omega
    simpa [Nat.mul_add, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using this
  · by_cases ha : a = 0
    · subst a
      have hz : blocks u 0 = 0 := (blocks_zero_iff u 0 hu).2 rfl
      simp [hz]
    · by_cases hb : b = 0
      · subst b
        have hz : blocks u 0 = 0 := (blocks_zero_iff u 0 hu).2 rfl
        simp [hz]
      · have ha_pos : 0 < a := Nat.pos_of_ne_zero ha
        have hb_pos : 0 < b := Nat.pos_of_ne_zero hb
        have hca_pos : 0 < blocks u a := by
          exact Nat.pos_of_ne_zero (by intro h; exact ha ((blocks_zero_iff u a hu).1 h))
        have hcb_pos : 0 < blocks u b := by
          exact Nat.pos_of_ne_zero (by intro h; exact hb ((blocks_zero_iff u b hu).1 h))
        let ca := blocks u a
        let cb := blocks u b
        have hca : 1 ≤ ca := by dsimp [ca]; omega
        have hcb : 1 ≤ cb := by dsimp [cb]; omega
        have hsum : u * (ca - 1) + u * (cb - 1) + 2 ≤ a + b := by
          dsimp [ca, cb]
          have hla := pred_mul_blocks_lt u a hu ha_pos
          have hlb := pred_mul_blocks_lt u b hu hb_pos
          omega
        have hdecomp : u * (ca + cb - 1) = u * (ca - 1) + u * (cb - 1) + u := by
          have hidx : ca + cb - 1 = (ca - 1) + (cb - 1) + 1 := by omega
          rw [hidx, Nat.mul_add, Nat.mul_add, Nat.mul_one]
        have htarget : (ca + cb - 1) * u ≤ a + b + u - 1 := by
          rw [Nat.mul_comm, hdecomp]
          omega
        have hsub : ca + cb - 1 ≤ blocks u (a + b) := by
          unfold blocks
          rw [Nat.le_div_iff_mul_le hu]
          simpa [ca, cb] using htarget
        dsimp [ca, cb] at hsub
        omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SpillExtents.Goal_SegmentTailSharingBound
