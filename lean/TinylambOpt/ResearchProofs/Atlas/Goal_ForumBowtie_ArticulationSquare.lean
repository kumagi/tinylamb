import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `ForumBowtie.ArticulationSquare` — `∀ n db b, reference pattern n db b = total n (fun
  x => (loop3 n db 0 1 2 x)^2)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ForumBowtie_ArticulationSquare

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ForumBowtie
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total dot)

/-! Finite sums over `List.range n` used by `reference` and `total`. -/
private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi)),
        h n (Nat.lt_succ_self n)]

private theorem rsum_zero (n : Nat) : rsum n (fun _ => (0 : Int)) = 0 := by
  induction n with
  | zero => rfl
  | succ n ih => simp [rsum_succ, ih]

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero => simp [rsum]; exact (rsum_zero m).symm
  | succ n ih =>
      rw [rsum_succ, ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr
        intro j hj
        rw [rsum_succ]]
      rw [rsum_add]
  where
    rsum_add (k : Nat) (f g : Nat → Int) :
        rsum k (fun i => f i + g i) = rsum k f + rsum k g := by
      induction k with
      | zero => simp [rsum]
      | succ k ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.mul_add]

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.add_mul]

private theorem rsum_split4 (n : Nat) (f g : Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d =>
      f a b * g c d)))) =
    (rsum n (fun a => rsum n (fun b => f a b))) *
      (rsum n (fun c => rsum n (fun d => g c d))) := by
  calc
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d =>
        f a b * g c d))))
        = rsum n (fun a => rsum n (fun b => rsum n (fun c =>
            f a b * rsum n (fun d => g c d)))) := by
            apply rsum_congr; intro a _
            apply rsum_congr; intro b _
            apply rsum_congr; intro c _
            exact rsum_const_mul n (f a b) (fun d => g c d)
    _ = rsum n (fun a => rsum n (fun b =>
            f a b * rsum n (fun c => rsum n (fun d => g c d)))) := by
            apply rsum_congr; intro a _
            apply rsum_congr; intro b _
            exact rsum_const_mul n (f a b) (fun c => rsum n (fun d => g c d))
    _ = rsum n (fun a =>
            (rsum n (fun b => f a b)) * rsum n (fun c => rsum n (fun d => g c d))) := by
            apply rsum_congr; intro a _
            exact rsum_mul_const n (fun b => f a b) (rsum n (fun c => rsum n (fun d => g c d)))
    _ = (rsum n (fun a => rsum n (fun b => f a b))) *
          (rsum n (fun c => rsum n (fun d => g c d))) :=
            rsum_mul_const n (fun a => rsum n (fun b => f a b))
              (rsum n (fun c => rsum n (fun d => g c d)))

private theorem triangle_eq_loop (n : Nat) (db : DB) (x : Nat) :
    rsum n (fun k => rsum n (fun y => db 0 x k * (db 1 k y * db 2 y x))) =
      rsum n (fun y => (rsum n (fun k => db 0 x k * db 1 k y)) * db 2 y x) := by
  calc
    rsum n (fun k => rsum n (fun y => db 0 x k * (db 1 k y * db 2 y x)))
        = rsum n (fun k => rsum n (fun y => (db 0 x k * db 1 k y) * db 2 y x)) := by
            apply rsum_congr; intro k _
            apply rsum_congr; intro y _
            ac_rfl
    _ = rsum n (fun y => rsum n (fun k => (db 0 x k * db 1 k y) * db 2 y x)) := by
            rw [rsum_comm]
    _ = rsum n (fun y => (rsum n (fun k => db 0 x k * db 1 k y)) * db 2 y x) := by
            apply rsum_congr; intro y _
            exact rsum_mul_const n (fun k => db 0 x k * db 1 k y) (db 2 y x)

theorem sound_ForumBowtie_ArticulationSquare : ForumBowtie.ArticulationSquare.Goal := by
  intro n db b
  simp [reference, integrate, pattern, value, put, bridge, total, loop3]
  change rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
      rsum n (fun x0 =>
        db 0 x0 x1 * (db 1 x1 x2 * (db 2 x2 x0 *
          (db 0 x0 x3 * (db 1 x3 x4 * db 2 x4 x0))))))))) =
    rsum n (fun x0 => (rsum n (fun y =>
      (rsum n (fun k => db 0 x0 k * db 1 k y)) * db 2 y x0)) ^ 2)
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
        rsum n (fun x0 =>
          db 0 x0 x1 * (db 1 x1 x2 * (db 2 x2 x0 *
            (db 0 x0 x3 * (db 1 x3 x4 * db 2 x4 x0)))))))))
        = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x0 =>
            rsum n (fun x3 =>
              db 0 x0 x1 * (db 1 x1 x2 * (db 2 x2 x0 *
                (db 0 x0 x3 * (db 1 x3 x4 * db 2 x4 x0))))))))) := by
            apply rsum_congr; intro x1 _
            apply rsum_congr; intro x2 _
            apply rsum_congr; intro x4 _
            rw [rsum_comm]
    _ = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x0 => rsum n (fun x4 =>
            rsum n (fun x3 =>
              db 0 x0 x1 * (db 1 x1 x2 * (db 2 x2 x0 *
                (db 0 x0 x3 * (db 1 x3 x4 * db 2 x4 x0))))))))) := by
            apply rsum_congr; intro x1 _
            apply rsum_congr; intro x2 _
            rw [rsum_comm]
    _ = rsum n (fun x1 => rsum n (fun x0 => rsum n (fun x2 => rsum n (fun x4 =>
            rsum n (fun x3 =>
              db 0 x0 x1 * (db 1 x1 x2 * (db 2 x2 x0 *
                (db 0 x0 x3 * (db 1 x3 x4 * db 2 x4 x0))))))))) := by
            apply rsum_congr; intro x1 _
            rw [rsum_comm]
    _ = rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 =>
            rsum n (fun x3 =>
              db 0 x0 x1 * (db 1 x1 x2 * (db 2 x2 x0 *
                (db 0 x0 x3 * (db 1 x3 x4 * db 2 x4 x0))))))))) := by
            rw [rsum_comm]
    _ = rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
            rsum n (fun x4 =>
              (db 0 x0 x1 * (db 1 x1 x2 * db 2 x2 x0)) *
              (db 0 x0 x3 * (db 1 x3 x4 * db 2 x4 x0))))))) := by
            apply rsum_congr; intro x0 _
            apply rsum_congr; intro x1 _
            apply rsum_congr; intro x2 _
            rw [rsum_comm]
            apply rsum_congr; intro x3 _
            apply rsum_congr; intro x4 _
            ac_rfl
    _ = rsum n (fun x0 =>
          (rsum n (fun x1 => rsum n (fun x2 => db 0 x0 x1 * (db 1 x1 x2 * db 2 x2 x0))) *
           rsum n (fun x3 => rsum n (fun x4 => db 0 x0 x3 * (db 1 x3 x4 * db 2 x4 x0))))) := by
            apply rsum_congr; intro x0 _
            exact rsum_split4 n
              (fun a b => db 0 x0 a * (db 1 a b * db 2 b x0))
              (fun a b => db 0 x0 a * (db 1 a b * db 2 b x0))
    _ = rsum n (fun x0 =>
          (rsum n (fun y => (rsum n (fun k => db 0 x0 k * db 1 k y)) * db 2 y x0) *
           rsum n (fun y => (rsum n (fun k => db 0 x0 k * db 1 k y)) * db 2 y x0))) := by
            apply rsum_congr; intro x0 _
            rw [triangle_eq_loop n db x0]
    _ = rsum n (fun x0 => (rsum n (fun y =>
          (rsum n (fun k => db 0 x0 k * db 1 k y)) * db 2 y x0)) ^ 2) := by
            apply rsum_congr; intro x0 _
            rw [Int.pow_succ, Int.pow_succ]
            simp

end TinylambOpt.ResearchProofs.Atlas.Goal_ForumBowtie_ArticulationSquare
