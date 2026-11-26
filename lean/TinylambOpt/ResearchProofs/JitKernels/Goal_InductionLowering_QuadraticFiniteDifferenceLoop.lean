import TinylambOpt.ResearchGoals.JitKernels.InductionLowering

/-!
The finite-difference recurrence emits exactly the quadratic polynomial values
at `start + i * stride`. The proof tracks the full two-state recurrence through
`scanl`, using that the second difference of a quadratic is the constant
`2 * a * stride^2`. SQL execution may replace two per-row multiplications by
two additions without changing the projected values.
-/

set_option maxHeartbeats 0
set_option linter.unusedSimpArgs false
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.InductionLowering.Goal_QuadraticFiniteDifferenceLoop

open TinylambOpt.ResearchGoals.JitKernels.InductionLowering

theorem poly_step (a b c x d : Int) :
    poly a b c (x + d) = poly a b c x + (2 * a * x * d + a * d * d + b * d) := by
  simp [poly, Int.mul_add, Int.add_mul, Int.mul_assoc, Int.mul_comm, Int.mul_left_comm,
    Int.mul_one, Int.one_mul]
  have h : a * (x * (d * 2)) = 2 * (a * (x * d)) := by ac_rfl
  rw [h]
  omega

theorem step_second (a x d b : Int) :
    2 * a * (x + d) * d + a * d * d + b * d - (2 * a * x * d + a * d * d + b * d) =
      2 * a * d * d := by
  simp [Int.mul_add, Int.add_mul, Int.mul_assoc, Int.mul_comm, Int.mul_left_comm,
    Int.mul_one, Int.one_mul]
  omega

def st (a b c start stride : Int) (i : Nat) : Int × Int :=
  (poly a b c (start + stride * (i : Int)),
   poly a b c (start + stride * ((i : Int) + 1)) - poly a b c (start + stride * (i : Int)))

theorem advance_st (a b c start stride : Int) (i : Nat) :
    advance (2 * a * stride * stride) (st a b c start stride i) =
      st a b c start stride (i + 1) := by
  unfold advance st
  simp only [Int.natCast_add, Int.natCast_one]
  have hx1 : start + stride * ((i : Int) + 1) = (start + stride * (i : Int)) + stride := by
    simp [Int.mul_add, Int.add_assoc]
  have hx2 : start + stride * ((i : Int) + 1 + 1) =
      (start + stride * (i : Int)) + stride + stride := by
    simp [Int.mul_add, Int.add_assoc]
    omega
  rw [hx1, hx2]
  apply Prod.ext
  · omega
  · have h1 := poly_step a b c (start + stride * (i : Int)) stride
    have h2 := poly_step a b c ((start + stride * (i : Int)) + stride) stride
    have h3 := step_second a (start + stride * (i : Int)) stride b
    rw [h1, h2]
    omega

theorem foldl_st (a b c start stride : Int) : ∀ n : Nat,
    (List.range n).foldl (fun s _ => advance (2 * a * stride * stride) s)
        (st a b c start stride 0) = st a b c start stride n := by
  intro n
  induction n with
  | zero => simp [st]
  | succ n ih =>
      rw [List.range_succ, List.foldl_append]
      simp [ih, advance_st]

theorem scanl_st (a b c start stride : Int) : ∀ n : Nat,
    (List.range n).scanl (fun s _ => advance (2 * a * stride * stride) s)
        (st a b c start stride 0) = (List.range (n + 1)).map (st a b c start stride) := by
  intro n
  induction n with
  | zero => simp [st]
  | succ n ih =>
      rw [List.range_succ, List.map_append] at ih
      rw [List.range_succ, List.scanl_append]
      rw [ih]
      rw [foldl_st]
      conv => rhs; rw [show n + 1 + 1 = (n + 1) + 1 by omega]
      conv => rhs; rw [List.range_succ, List.map_append]
      rw [List.range_succ, List.map_append]
      simp [advance_st]

theorem sound : QuadraticFiniteDifferenceLoop.Goal := by
  intro a b c start stride n
  unfold differences
  have hinit : (poly a b c start,
      poly a b c (start + stride) - poly a b c start) = st a b c start stride 0 := by
    simp [st]
  rw [hinit, scanl_st]
  rw [List.range_succ]
  simp [st]

end TinylambOpt.ResearchProofs.JitKernels.InductionLowering.Goal_QuadraticFiniteDifferenceLoop
