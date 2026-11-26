import TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

/-!
Window execution must preserve peer membership and frame results at ordering boundaries.

Proved

- `TilePrefix` — A cutoff on NTILE can become a bounded prefix read with no tile
  computations per row.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_TilePrefix

open TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

/-- `1 + x ≤ k` is the same as `x < k` on naturals. -/
theorem one_add_le_iff (x k : Nat) : 1 + x ≤ k ↔ x < k := by
  constructor <;> intro h <;> omega

/-- The frontier plus the whole-bucket stretch re-aligns with `k * small + extra`. -/
theorem front_region_id (k small extra : Nat) (h : extra ≤ k) :
    extra * (small + 1) + (k - extra) * small = k * small + extra := by
  have h1 : extra * (small + 1) = extra * small + extra := by
    rw [Nat.mul_add, Nat.mul_one]
  have h2 : extra * small + (k - extra) * small = k * small := by
    rw [← Nat.add_mul, Nat.add_sub_of_le h]
  omega

/-- A tail-region ordinal never lands in a bucket at or below the big-bucket count. -/
theorem tail_gt (k small extra i : Nat) (h : k ≤ extra) :
    ¬ (1 + extra + (i - extra * (small + 1)) / small ≤ k) := by
  have hz : 0 ≤ (i - extra * (small + 1)) / small := Nat.zero_le _
  omega

/-- Front-region buckets: `1 + i / (small+1) ≤ k` exactly while `i < k * (small+1)`. -/
theorem front_le (i k small : Nat) :
    1 + i / (small + 1) ≤ k ↔ i < k * (small + 1) := by
  rw [one_add_le_iff, Nat.div_lt_iff_lt_mul (k := small + 1) (Nat.succ_pos small)]

/-- Tail-region buckets: with `extra ≤ k` and `0 < small`, a tail ordinal sits in a bucket
`≤ k` exactly while it is below `frontier + (k - extra) * small`. -/
theorem tail_le (i k small extra : Nat) (hfront : extra * (small + 1) ≤ i)
    (hs : 0 < small) (hek : extra ≤ k) :
    1 + extra + (i - extra * (small + 1)) / small ≤ k
      ↔ i < extra * (small + 1) + (k - extra) * small := by
  have hstep : 1 + extra + (i - extra * (small + 1)) / small ≤ k
      ↔ (i - extra * (small + 1)) / small < k - extra := by
    constructor <;> intro h <;> omega
  have hkey : (i - extra * (small + 1)) / small < k - extra
      ↔ i - extra * (small + 1) < (k - extra) * small :=
    Nat.div_lt_iff_lt_mul (k := small) (x := i - extra * (small + 1)) (y := k - extra) hs
  have hfinal : i < extra * (small + 1) + (k - extra) * small
      ↔ i - extra * (small + 1) < (k - extra) * small :=
    (Nat.sub_lt_iff_lt_add' hfront).symm
  rw [hstep, hkey, hfinal]

theorem sound : TilePrefix.Goal := by
  intro n b i k hi hb hk
  let small := n / b
  let extra := n % b
  show (tile n b i ≤ k ↔ i < k * small + min k extra)
  have hdec : b * small + extra = n := Nat.div_add_mod n b
  have ht : tile n b i = if i < extra * (small + 1) then 1 + i / (small + 1)
      else 1 + extra + (i - extra * (small + 1)) / small := rfl
  rcases Nat.eq_zero_or_pos small with hsmall | hs
  · -- `small = 0`: then `extra = n`, the frontier is `n`, and every bucket has size one
    have h2 := hdec
    rw [hsmall] at h2
    have hex : extra = n := by omega
    rw [ht, hsmall]
    simp only [Nat.zero_add, Nat.mul_one, Nat.div_one, Nat.mul_zero]
    rw [ite_eq_left (by omega : i < extra)]
    constructor <;> intro h <;> omega
  · rcases Nat.lt_or_ge k (extra + 1) with hkle | hkg
    · -- `k ≤ extra`: the cutoff lands inside the big-bucket front region
      have hkex : k ≤ extra := by omega
      have hle : k * small ≤ extra * small := Nat.mul_le_mul_right small hkex
      rw [ht]
      rw [Nat.min_eq_left hkex]
      by_cases hlt : i < extra * (small + 1)
      · rw [ite_eq_left hlt, front_le, Nat.mul_add, Nat.mul_one]
      · rw [ite_eq_right hlt]
        have hge : extra * small + extra = extra * (small + 1) := by
          rw [Nat.mul_add, Nat.mul_one]
        have hr : ¬ (i < k * small + k) := by
          have h1 : extra * (small + 1) ≤ i := by omega
          omega
        exact ⟨fun h2 => (tail_gt k small extra i hkex h2).elim, fun h2 => absurd h2 hr⟩
    · -- `extra < k`: every big bucket plus `k - extra` small ones lie below the cutoff
      have hexk : extra ≤ k := Nat.le_of_succ_le hkg
      have hlt : extra * small < k * small :=
        Nat.mul_lt_mul_of_pos_right (Nat.lt_of_succ_le hkg) hs
      have hge : extra * small + extra = extra * (small + 1) := by
        rw [Nat.mul_add, Nat.mul_one]
      have hfid : extra * (small + 1) + (k - extra) * small = k * small + extra :=
        front_region_id k small extra hexk
      rw [ht]
      rw [Nat.min_eq_right hexk]
      by_cases hfront : i < extra * (small + 1)
      · -- front region: every ordinal here is in a bucket `≤ k` and below the bound
        have hp : i / (small + 1) < extra + 1 := by
          refine Nat.div_lt_of_lt_mul ?_
          have h1 : i < (small + 1) * extra := by rw [Nat.mul_comm]; exact hfront
          have h2 : (small + 1) * extra ≤ (small + 1) * (extra + 1) :=
            Nat.mul_le_mul_left _ (Nat.le_succ extra)
          omega
        have hl : 1 + i / (small + 1) ≤ k := by omega
        have hr : i < k * small + extra := by omega
        rw [ite_eq_left hfront]
        exact ⟨fun _ => hr, fun _ => hl⟩
      · -- tail region
        rw [ite_eq_right hfront,
          tail_le i k small extra (by omega) hs hexk, hfid]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_TilePrefix
