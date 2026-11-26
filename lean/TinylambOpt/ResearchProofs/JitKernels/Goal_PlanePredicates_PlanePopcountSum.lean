import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-!
The sum over all lanes can be computed from bitplane populations alone. The
proof shows each lane's contribution is the sum of its set bits times their
place values (binary expansion), then distributes the plane-wise sums over the
lane list.

Proved

- `PlanePopcountSum` — SUM can be accumulated from per-plane ballot counts,
  so no individual residual needs to be reconstructed in the JIT kernel.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_PlanePopcountSum

open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-- Sum of place values for the set planes of one residual. -/
def bitSum (x w : Nat) : Nat := ((List.range w).map (fun j => if bit x j then 2^j else 0)).sum

theorem sum_const_zero (l : List Nat) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

/-- For an in-frame value, plane `w` is set exactly when the value is at
least `2^w`. -/
theorem bit_top_iff (x w : Nat) (hx : x < 2^(w + 1)) : bit x w = true ↔ 2^w ≤ x := by
  constructor
  · intro hb
    by_cases hlt : x < 2^w
    · have hfalse : bit x w = false := Nat.testBit_lt_two_pow hlt
      simp [hb] at hfalse
    · omega
  · intro hle
    exact Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt hle (by omega)

/-- Binary expansion of a residual in the first `w` planes. -/
theorem bitSum_eq (x w : Nat) (hx : x < 2^w) : bitSum x w = x := by
  induction w generalizing x with
  | zero =>
      simp [bitSum] at hx ⊢
      omega
  | succ w ih =>
      have hxw : x < 2^(w + 1) := hx
      unfold bitSum
      rw [List.range_succ, List.map_append, List.sum_append]
      have hlow : ((List.range w).map (fun j => if bit x j then 2^j else 0)).sum = x % 2^w := by
        have hcong : ((List.range w).map (fun j => if bit x j then 2^j else 0)).sum =
            ((List.range w).map (fun j => if bit (x % 2^w) j then 2^j else 0)).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro j hjmem
          have hj : j < w := by simpa using hjmem
          have hb : bit x j = bit (x % 2^w) j := by
            simp [bit, Nat.testBit_mod_two_pow, hj]
          simp [hb]
        rw [hcong]
        exact ih (x % 2^w) (Nat.mod_lt _ (by omega : 0 < 2^w))
      rw [hlow]
      have hdecomp : 2^w * (x / 2^w) + x % 2^w = x := Nat.div_add_mod x (2^w)
      by_cases hb : bit x w
      · have hge : 2^w ≤ x := (bit_top_iff x w hxw).1 hb
        have hq : x / 2^w = 1 := by
          apply Nat.div_eq_of_lt_le
          · simpa using hge
          · simpa [Nat.pow_succ'] using hxw
        simp [hb]
        rw [← hdecomp, hq]
        simp [Nat.add_comm, Nat.mul_comm]
      · have hlt : x < 2^w := by
          by_cases hge : 2^w ≤ x
          · have ht := (bit_top_iff x w hxw).2 hge
            simp [hb] at ht
          · omega
        have hq : x / 2^w = 0 := Nat.div_eq_of_lt hlt
        simp [hb]
        rw [← hdecomp, hq]
        simp

/-- Adding one lane to a plane ballot adds that lane's place value when the
plane is set. -/
theorem weight_cons (x : Nat) (xs : List Nat) (j : Nat) :
    weight (x :: xs) j = weight xs j + (if bit x j then 2^j else 0) := by
  unfold weight planes
  rw [List.map_cons, List.count_cons]
  by_cases h : bit x j
  · simp [h, Nat.add_mul]
  · simp [h]

theorem sum_map_add (f g : Nat → Nat) (l : List Nat) :
    (l.map (fun i => f i + g i)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

theorem sound : PlanePopcountSum.Goal := by
  intro xs w hx
  induction xs with
  | nil =>
      have hzero : ∀ j, weight ([] : List Nat) j = 0 := by
        intro j
        simp [weight, planes]
      have hmap : List.map (weight ([] : List Nat)) (List.range w) =
          List.map (fun _ => 0) (List.range w) := by
        apply List.map_congr_left
        intro j hj
        exact hzero j
      rw [hmap]
      exact sum_const_zero (List.range w)
  | cons x xs ih =>
      have htail : ∀ y ∈ xs, y < 2^w := by
        intro y hy
        exact hx y (by simp [hy])
      have hih := ih htail
      have hw : ((List.range w).map (weight (x :: xs))).sum =
          ((List.range w).map (weight xs)).sum + bitSum x w := by
        unfold bitSum
        rw [← sum_map_add]
        apply congrArg List.sum
        apply List.map_congr_left
        intro j hj
        exact weight_cons x xs j
      rw [hw, hih, bitSum_eq x w (hx x (by simp))]
      simp [Nat.add_comm]

end TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_PlanePopcountSum
