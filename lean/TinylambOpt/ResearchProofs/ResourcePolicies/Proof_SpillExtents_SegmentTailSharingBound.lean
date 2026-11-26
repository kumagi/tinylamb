import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

/-!
Segment tail sharing saves at most one block: rounding two byte counts up to
whole blocks separately differs from rounding their sum by at most one block.
Both directions of the claimed bound hold, so a spill quota planner may budget
either scheme with the other as a one-block envelope.  The proof decomposes the
byte counts into exact block multiples plus remainders via `Nat.div_add_mod`
and reasons by the carry of the two remainders; no arithmetic beyond `Nat`
division bounds and `omega` is required.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents_SegmentTailSharingBound
open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

/-- Quotient-times-divisor form of the division identity, aligned with the
`q * u` product shape used throughout the proof. -/
private theorem divmod (u a : Nat) (hu : 0 < u) : a / u * u + a % u = a := by
  rw [Nat.mul_comm (a / u) u]
  exact Nat.div_add_mod a u

/-- Ceiling division as quotient plus a rounding bit. -/
private theorem blocks_div (u n : Nat) (hu : 0 < u) :
    blocks u n = n / u + if n % u = 0 then 0 else 1 := by
  unfold blocks
  have hmod := Nat.mod_lt n hu
  have hq := divmod u n hu
  rcases Nat.eq_zero_or_pos (n % u) with h | h
  · rw [h] at hq ⊢
    rw [if_pos rfl, Nat.add_zero]
    refine Nat.div_eq_of_lt_le ?_ ?_
    · omega
    · rw [show (n / u + 1) * u = n / u * u + u by rw [Nat.add_mul, Nat.one_mul]]
      omega
  · rw [if_neg (by omega)]
    refine Nat.div_eq_of_lt_le ?_ ?_
    · rw [show (n / u + 1) * u = n / u * u + u by rw [Nat.add_mul, Nat.one_mul]]
      omega
    · rw [Nat.add_mul (n / u) 2 u]
      omega

theorem sound : SegmentTailSharingBound.Goal := by
  intro u a b hu
  rw [blocks_div u a hu, blocks_div u b hu, blocks_div u (a + b) hu]
  have e1 := divmod u a hu
  have e2 := divmod u b hu
  have hsum : a + b = (a / u + b / u) * u + (a % u + b % u) := by
    rw [Nat.add_mul]
    omega
  have h2u : a % u + b % u < 2 * u := by
    have m1 := Nat.mod_lt a hu
    have m2 := Nat.mod_lt b hu
    omega
  rcases Nat.lt_or_ge (a % u + b % u) u with hlt | hge
  · -- no carry: the pooled rounding inherits exactly the two rounding bits
    have hd : (a + b) / u = a / u + b / u := by
      rw [hsum]
      refine Nat.div_eq_of_lt_le ?_ ?_
      · omega
      · rw [show (a / u + b / u + 1) * u = (a / u + b / u) * u + u by
          rw [Nat.add_mul, Nat.one_mul]]
        omega
    have hm : (a + b) % u = a % u + b % u := by
      rw [hsum, Nat.add_comm ((a / u + b / u) * u) (a % u + b % u),
        Nat.mul_comm (a / u + b / u) u, Nat.add_mul_mod_self_left]
      exact Nat.mod_eq_of_lt hlt
    rw [hd, hm]
    rcases Nat.eq_zero_or_pos (a % u) with h1 | h1
    · rw [if_pos h1]
      rcases Nat.eq_zero_or_pos (b % u) with h2 | h2
      · rw [if_pos h2, if_pos (by omega)]
        omega
      · rw [if_neg (by omega), if_neg (by omega)]
        omega
    · rw [if_neg (by omega)]
      rcases Nat.eq_zero_or_pos (b % u) with h2 | h2
      · rw [if_pos h2, if_neg (by omega)]
        omega
      · rw [if_neg (by omega), if_neg (by omega)]
        omega
  · -- carry: one whole block is folded into the quotient, and both segments
    -- contributed a rounding bit, leaving at most one block of slack
    have hrep : a + b = (a / u + b / u + 1) * u + (a % u + b % u - u) := by
      rw [show (a / u + b / u + 1) * u = (a / u + b / u) * u + u by
        rw [Nat.add_mul, Nat.one_mul]]
      have := hsum
      omega
    have hd : (a + b) / u = a / u + b / u + 1 := by
      rw [hrep]
      refine Nat.div_eq_of_lt_le ?_ ?_
      · omega
      · have e1 : (a / u + b / u + 1) * u = (a / u + b / u) * u + u := by
          rw [Nat.add_mul, Nat.one_mul]
        have e2 : (a / u + b / u + 1 + 1) * u = (a / u + b / u) * u + (u + u) := by
          rw [Nat.add_mul, Nat.add_mul, Nat.one_mul, Nat.add_assoc]
        rw [e1, e2]
        omega
    have hm : (a + b) % u = a % u + b % u - u := by
      rw [hrep, Nat.add_comm ((a / u + b / u + 1) * u) (a % u + b % u - u),
        Nat.mul_comm (a / u + b / u + 1) u, Nat.add_mul_mod_self_left]
      exact Nat.mod_eq_of_lt (by omega)
    rw [hd, hm]
    have ha0 : ¬ (a % u = 0) := by
      have m2 := Nat.mod_lt b hu
      omega
    have hb0 : ¬ (b % u = 0) := by
      have m1 := Nat.mod_lt a hu
      omega
    rw [if_neg ha0, if_neg hb0]
    rcases Nat.eq_zero_or_pos (a % u + b % u - u) with h3 | h3
    · rw [if_pos h3]
      omega
    · rw [if_neg (by omega)]
      omega

end TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents_SegmentTailSharingBound
