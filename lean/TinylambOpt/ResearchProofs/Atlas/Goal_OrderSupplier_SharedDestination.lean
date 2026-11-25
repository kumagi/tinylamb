import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_OrderSupplier_SharedDestination

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total)
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.OrderSupplier

/-- Finite sum over `List.range`, the engine behind `total`/`integrate`. -/
private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem sum_map_congr {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem sum_add {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

private theorem sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem sum_mul_left {α} (l : List α) (c : Int) (f : α → Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.mul_add]

private theorem sum_mul_right {α} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_mul]

private theorem sum_swap {α β} (l : List α) (l' : List β) (F : α → β → Int) :
    (l.map (fun a => (l'.map (F a)).sum)).sum =
      (l'.map (fun b => (l.map (fun a => F a b)).sum)).sum := by
  induction l with
  | nil => simp [sum_zero]
  | cons a l ih => simp [ih, sum_add]

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi)),
        h n (Nat.lt_succ_self n)]

private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := sum_mul_left (List.range n) c f

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := sum_mul_right (List.range n) f c

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero => simp [rsum, sum_zero]
  | succ n ih =>
      rw [rsum_succ, ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr
        intro j hj
        rw [rsum_succ]]
      rw [rsum_add]

private theorem rsum_product (n m : Nat) (f g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr
            intro i hi
            exact rsum_const_mul m (f i) g
    _ = rsum n f * rsum m g := rsum_mul_const n f (rsum m g)

theorem sound_OrderSupplier_SharedDestination : OrderSupplier.SharedDestination.Goal := by
  intro n db b
  simp [reference, integrate, pattern, value, put, bridge, total]
  change rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
      db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 1 (b 0) x4 * db 2 x4 x3))))))) =
    rsum n (fun z => (rsum n (fun y => (rsum n (fun k => db 0 (b 0) k * db 1 k y)) * db 2 y z)) *
                      (rsum n (fun j => db 1 (b 0) j * db 2 j z)))
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
        db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 1 (b 0) x4 * db 2 x4 x3)))))))
        = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
          db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 1 (b 0) x4 * db 2 x4 x3))))))) := by
            apply rsum_congr; intro x1 hx1
            apply rsum_congr; intro x2 hx2
            rw [rsum_comm]
    _ = rsum n (fun x2 => rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x4 =>
          db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 1 (b 0) x4 * db 2 x4 x3))))))) := by
            rw [rsum_comm]
    _ = rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x1 => rsum n (fun x4 =>
          db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 1 (b 0) x4 * db 2 x4 x3))))))) := by
            apply rsum_congr; intro x2 hx2
            rw [rsum_comm]
    _ = rsum n (fun x3 => rsum n (fun x2 => rsum n (fun x1 => rsum n (fun x4 =>
          db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 1 (b 0) x4 * db 2 x4 x3))))))) := by
            rw [rsum_comm]
    _ = rsum n (fun x3 => rsum n (fun x2 => rsum n (fun x1 => rsum n (fun x4 =>
          (db 2 x2 x3 * (db 0 (b 0) x1 * db 1 x1 x2)) * (db 1 (b 0) x4 * db 2 x4 x3))))) := by
            apply rsum_congr; intro x3 hx3
            apply rsum_congr; intro x2 hx2
            apply rsum_congr; intro x1 hx1
            apply rsum_congr; intro x4 hx4
            ac_rfl
    _ = rsum n (fun x3 => rsum n (fun x2 => rsum n (fun x1 =>
          (db 2 x2 x3 * (db 0 (b 0) x1 * db 1 x1 x2))) *
          (rsum n (fun x4 => db 1 (b 0) x4 * db 2 x4 x3)))) := by
            apply rsum_congr; intro x3 hx3
            apply rsum_congr; intro x2 hx2
            rw [rsum_product]
    _ = rsum n (fun x3 => rsum n (fun x2 =>
          (db 2 x2 x3 * rsum n (fun x1 => db 0 (b 0) x1 * db 1 x1 x2)) *
          (rsum n (fun x4 => db 1 (b 0) x4 * db 2 x4 x3)))) := by
            apply rsum_congr; intro x3 hx3
            apply rsum_congr; intro x2 hx2
            rw [rsum_const_mul]
    _ = rsum n (fun x3 => (rsum n (fun x2 =>
          db 2 x2 x3 * rsum n (fun x1 => db 0 (b 0) x1 * db 1 x1 x2))) *
          (rsum n (fun x4 => db 1 (b 0) x4 * db 2 x4 x3))) := by
            apply rsum_congr; intro x3 hx3
            rw [rsum_mul_const]
    _ = rsum n (fun z => (rsum n (fun y => (rsum n (fun k => db 0 (b 0) k * db 1 k y)) * db 2 y z)) *
          (rsum n (fun j => db 1 (b 0) j * db 2 j z))) := by
            apply rsum_congr; intro z hz
            have h : (rsum n (fun x2 => db 2 x2 z * rsum n (fun x1 =>
                db 0 (b 0) x1 * db 1 x1 x2))) =
                (rsum n (fun y => (rsum n (fun k => db 0 (b 0) k * db 1 k y)) * db 2 y z)) := by
              apply rsum_congr; intro y hy
              exact Int.mul_comm _ _
            rw [h]

end TinylambOpt.ResearchProofs.Atlas.Goal_OrderSupplier_SharedDestination
