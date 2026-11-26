import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `IntersectingCycles.TwoSeparators` — `∀ n db b, reference pattern n db b = total n
  (fun y => total n (fun z => db 0 y z * total n (fun u => bridge n db 0 0 z u * db 0
  u y) * total n (fun v => bridge n db 1 2 y v * db 2 v z)))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_IntersectingCycles_TwoSeparators

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total)
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.IntersectingCycles

/-! Finite sums over `List.range`, the engine behind `integrate`/`total`/`bridge`. -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem sum_congr {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem sum_add {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

private theorem sum_swap {α β} (l : List α) (l' : List β) (F : α → β → Int) :
    (l.map (fun a => (l'.map (F a)).sum)).sum =
      (l'.map (fun b => (l.map (fun a => F a b)).sum)).sum := by
  induction l with
  | nil => simp [sum_zero]
  | cons a l ih => simp [ih, sum_add]

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n + 1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi)),
        h n (Nat.lt_succ_self n)]

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

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := sum_mul_left (List.range n) c f

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := sum_mul_right (List.range n) f c

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  change ((List.range n).map (fun i => ((List.range m).map (f i)).sum)).sum =
    ((List.range m).map (fun j => ((List.range n).map (fun i => f i j)).sum)).sum
  exact sum_swap _ _ _

private theorem rsum_const_mul₂ (n : Nat) (c : Int) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum n (fun j => c * f i j)) =
      c * rsum n (fun i => rsum n (fun j => f i j)) := by
  calc
    rsum n (fun i => rsum n (fun j => c * f i j))
        = rsum n (fun i => c * rsum n (fun j => f i j)) := by
            apply rsum_congr
            intro i _
            rw [rsum_const_mul]
    _ = c * rsum n (fun i => rsum n (fun j => f i j)) := rsum_const_mul n c _

private theorem rsum₂_factor (n : Nat) (c B : Int) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum n (fun j => (c * f i j) * B)) =
      (c * rsum n (fun i => rsum n (fun j => f i j))) * B := by
  rw [show rsum n (fun i => rsum n (fun j => (c * f i j) * B)) =
      rsum n (fun i => rsum n (fun j => (c * B) * f i j)) by
        apply rsum_congr; intro i _
        apply rsum_congr; intro j _
        ac_rfl]
  rw [rsum_const_mul₂]
  ac_rfl

theorem sound_IntersectingCycles_TwoSeparators : IntersectingCycles.TwoSeparators.Goal := by
  intro n db b
  simp [reference, integrate, pattern, value, put, bridge, total]
  let P := fun (x4 x5 x1 x2 x3 x0 : Nat) =>
    db 0 x0 x1 * (db 0 x1 x2 * (db 0 x2 x3 * (db 0 x3 x0 *
      (db 1 x2 x4 * (db 2 x4 x5 * db 2 x5 x3)))))
  change
    rsum n (fun x4 => rsum n (fun x5 => rsum n (fun x1 => rsum n (fun x2 =>
      rsum n (fun x3 => rsum n (fun x0 => P x4 x5 x1 x2 x3 x0)))))) =
    rsum n (fun y => rsum n (fun z =>
      (db 0 y z * rsum n (fun u => (rsum n (fun k => db 0 z k * db 0 k u)) * db 0 u y)) *
        rsum n (fun v => (rsum n (fun k => db 1 y k * db 2 k v)) * db 2 v z)))
  calc
    rsum n (fun x4 => rsum n (fun x5 => rsum n (fun x1 => rsum n (fun x2 =>
        rsum n (fun x3 => rsum n (fun x0 => P x4 x5 x1 x2 x3 x0))))))
        = rsum n (fun x4 => rsum n (fun x5 => rsum n (fun x2 => rsum n (fun x1 =>
            rsum n (fun x3 => rsum n (fun x0 => P x4 x5 x1 x2 x3 x0)))))) := by
              apply rsum_congr; intro x4 _
              apply rsum_congr; intro x5 _
              rw [rsum_comm]
    _ = rsum n (fun x4 => rsum n (fun x2 => rsum n (fun x5 => rsum n (fun x1 =>
            rsum n (fun x3 => rsum n (fun x0 => P x4 x5 x1 x2 x3 x0)))))) := by
              apply rsum_congr; intro x4 _
              rw [rsum_comm]
    _ = rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x5 => rsum n (fun x1 =>
            rsum n (fun x3 => rsum n (fun x0 => P x4 x5 x1 x2 x3 x0)))))) := by
              rw [rsum_comm]
    _ = rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x5 => rsum n (fun x3 =>
            rsum n (fun x1 => rsum n (fun x0 => P x4 x5 x1 x2 x3 x0)))))) := by
              apply rsum_congr; intro x2 _
              apply rsum_congr; intro x4 _
              apply rsum_congr; intro x5 _
              rw [rsum_comm]
    _ = rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 => rsum n (fun x5 =>
            rsum n (fun x1 => rsum n (fun x0 => P x4 x5 x1 x2 x3 x0)))))) := by
              apply rsum_congr; intro x2 _
              apply rsum_congr; intro x4 _
              rw [rsum_comm]
    _ = rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 => rsum n (fun x5 =>
            rsum n (fun x1 => rsum n (fun x0 => P x4 x5 x1 x2 x3 x0)))))) := by
              apply rsum_congr; intro x2 _
              rw [rsum_comm]
    _ = rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 => rsum n (fun x5 =>
          rsum n (fun x1 => rsum n (fun x0 =>
            (db 0 x2 x3 * (db 0 x0 x1 * (db 0 x1 x2 * db 0 x3 x0))) *
            (db 1 x2 x4 * (db 2 x4 x5 * db 2 x5 x3)))))))) := by
              apply rsum_congr; intro x2 _
              apply rsum_congr; intro x3 _
              apply rsum_congr; intro x4 _
              apply rsum_congr; intro x5 _
              apply rsum_congr; intro x1 _
              apply rsum_congr; intro x0 _
              dsimp [P]
              ac_rfl
    _ = rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 => rsum n (fun x5 =>
          (db 0 x2 x3 * rsum n (fun x1 => rsum n (fun x0 =>
             db 0 x0 x1 * (db 0 x1 x2 * db 0 x3 x0)))) *
          (db 1 x2 x4 * (db 2 x4 x5 * db 2 x5 x3)))))) := by
              apply rsum_congr; intro x2 _
              apply rsum_congr; intro x3 _
              apply rsum_congr; intro x4 _
              apply rsum_congr; intro x5 _
              rw [rsum₂_factor]
    _ = rsum n (fun x2 => rsum n (fun x3 =>
          (db 0 x2 x3 * rsum n (fun x1 => rsum n (fun x0 =>
             db 0 x0 x1 * (db 0 x1 x2 * db 0 x3 x0)))) *
          rsum n (fun x4 => rsum n (fun x5 =>
             db 1 x2 x4 * (db 2 x4 x5 * db 2 x5 x3))))) := by
              apply rsum_congr; intro x2 _
              apply rsum_congr; intro x3 _
              rw [rsum_const_mul₂]
    _ = rsum n (fun y => rsum n (fun z =>
          (db 0 y z * rsum n (fun u => (rsum n (fun k => db 0 z k * db 0 k u)) * db 0 u y)) *
          rsum n (fun v => (rsum n (fun k => db 1 y k * db 2 k v)) * db 2 v z))) := by
            apply rsum_congr; intro y _
            apply rsum_congr; intro z _
            have h1 : rsum n (fun u => rsum n (fun k =>
                db 0 k u * (db 0 u y * db 0 z k))) =
                rsum n (fun u => (rsum n (fun k => db 0 z k * db 0 k u)) * db 0 u y) := by
              calc
                rsum n (fun u => rsum n (fun k => db 0 k u * (db 0 u y * db 0 z k)))
                    = rsum n (fun u => rsum n (fun k => (db 0 z k * db 0 k u) * db 0 u y)) := by
                        apply rsum_congr; intro u _
                        apply rsum_congr; intro k _
                        ac_rfl
                _ = rsum n (fun u => (rsum n (fun k => db 0 z k * db 0 k u)) * db 0 u y) := by
                        apply rsum_congr; intro u _
                        rw [rsum_mul_const]
            have h2a : rsum n (fun x4 => rsum n (fun x5 =>
                db 1 y x4 * (db 2 x4 x5 * db 2 x5 z))) =
                rsum n (fun v => rsum n (fun k => db 1 y k * (db 2 k v * db 2 v z))) := by
              rw [rsum_comm]
            have h2b : rsum n (fun v => rsum n (fun k =>
                db 1 y k * (db 2 k v * db 2 v z))) =
                rsum n (fun v => (rsum n (fun k => db 1 y k * db 2 k v)) * db 2 v z) := by
              calc
                rsum n (fun v => rsum n (fun k => db 1 y k * (db 2 k v * db 2 v z)))
                    = rsum n (fun v => rsum n (fun k => (db 1 y k * db 2 k v) * db 2 v z)) := by
                        apply rsum_congr; intro v _
                        apply rsum_congr; intro k _
                        ac_rfl
                _ = rsum n (fun v => (rsum n (fun k => db 1 y k * db 2 k v)) * db 2 v z) := by
                        apply rsum_congr; intro v _
                        rw [rsum_mul_const]
            rw [h1, h2a, h2b]

end TinylambOpt.ResearchProofs.Atlas.Goal_IntersectingCycles_TwoSeparators
