import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-!
The sum of all lanes can be accumulated from bitplane populations alone: each
set bit contributes its place value once per lane, and summing place values
reconstructs each lane exactly when lanes fit the frame width. This permits a
SUM aggregate without decoding individual residuals.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_PlanePredicates_PlanePopcountSum
open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

private theorem sum_map_add (l : List Nat) (f g : Nat → Nat) :
    (l.map (fun i => f i + g i)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_zero_map (w : Nat) : ((List.range w).map (fun _ : Nat => (0 : Nat))).sum = 0 := by
  induction w with
  | zero => simp
  | succ w ih => rw [List.range_succ, List.map_append, List.sum_append, ih]; simp

private theorem sum_bitValues (x w : Nat) (hx : x < 2^w) :
    ((List.range w).map (fun j => if bit x j then 2^j else 0)).sum = x := by
  induction w generalizing x with
  | zero =>
      have hx0 : x = 0 := by omega
      subst x
      simp
  | succ w ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      have hmodx : x % 2^w < 2^w := Nat.mod_lt _ (Nat.two_pow_pos w)
      have ih' := ih (x % 2^w) hmodx
      have hcong :
          ((List.range w).map (fun j => if bit x j then 2^j else 0)).sum =
          ((List.range w).map (fun j => if bit (x % 2^w) j then 2^j else 0)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro j hj
        simp only [List.mem_range] at hj
        unfold bit
        rw [Nat.testBit_mod_two_pow]
        simp [hj]
      rw [hcong, ih']
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
      have hqxlt : x / 2^w < 2 := by
        rw [Nat.div_lt_iff_lt_mul (Nat.two_pow_pos w)]
        simpa [Nat.pow_succ, Nat.mul_comm] using hx
      have hxqx : x = 2^w * (x / 2^w) + x % 2^w := (Nat.div_add_mod x (2^w)).symm
      have hxbits : bit x w = (x / 2^w).testBit 0 := by
        unfold bit
        simpa using (Nat.testBit_add x 0 w)
      have hqx01 : x / 2^w = 0 ∨ x / 2^w = 1 :=
        Nat.le_one_iff_eq_zero_or_eq_one.mp (by omega)
      rcases hqx01 with hqx | hqx
      · have hxv : x = x % 2^w := by rw [hxqx, hqx]; simp
        simp [hxbits, hqx]
        omega
      · have hxv : x = 2^w + x % 2^w := by rw [hxqx, hqx]; simp [Nat.mul_comm]
        have hb : (if bit x w then 2^w else 0) = 2^w := by simp [hxbits, hqx]
        rw [hb]
        omega

theorem sound : PlanePopcountSum.Goal := by
  intro xs w hxs
  induction xs with
  | nil =>
      change ((List.range w).map (fun j => (planes ([] : List Nat) j).count true * 2^j)).sum = 0
      simpa [planes] using sum_zero_map w
  | cons x xs ih =>
      have hxlt : x < 2^w := hxs x (by simp)
      have hxsin : ∀ y ∈ xs, y < 2^w := by
        intro y hy
        exact hxs y (by simp [hy])
      have ih' := ih hxsin
      have hbit := sum_bitValues x w hxlt
      have hdecomp :
          ((List.range w).map (fun j => ((planes (x :: xs) j).count true) * 2^j)).sum =
          ((List.range w).map (fun j => (if bit x j then 2^j else 0))).sum +
          ((List.range w).map (fun j => (weight xs j))).sum := by
        rw [← sum_map_add]
        apply congrArg List.sum
        apply List.map_congr_left
        intro j hj
        by_cases hb : bit x j <;>
          simp [planes, weight, List.count_cons, hb, Nat.mul_add, Nat.mul_comm, Nat.add_comm, Nat.add_left_comm]
      change ((List.range w).map (fun j => (planes (x :: xs) j).count true * 2^j)).sum = (x :: xs).sum
      rw [hdecomp, hbit, ih']
      simp

end Nov26.Proof_PlanePredicates_PlanePopcountSum
