import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `CastOverlap.GramSquare` — `∀ n db b, reference pattern n db b = total n (fun x =>
  total n (fun y => (overlap n db 0 x y)^2))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_CastOverlap_GramSquare

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total dot)
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.CastOverlap

/-! Finite sums over `List.range`, the engine behind `integrate`/`total`/`dot`. -/

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

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := sum_mul_left (List.range n) c f

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := sum_mul_right (List.range n) f c

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  change ((List.range n).map (fun i => ((List.range m).map (f i)).sum)).sum =
    ((List.range m).map (fun j => ((List.range n).map (fun i => f i j)).sum)).sum
  exact sum_swap _ _ _

private theorem rsum_product (n m : Nat) (f g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr
            intro i hi
            exact rsum_const_mul m (f i) g
    _ = rsum n f * rsum m g := rsum_mul_const n f (rsum m g)

theorem sound_CastOverlap_GramSquare : CastOverlap.GramSquare.Goal := by
  intro n db b
  unfold reference
  simp [integrate, CastOverlap.pattern, value, put, overlap, dot, total]
  change rsum n (fun v2 => rsum n (fun v3 => rsum n (fun v0 => rsum n (fun v1 =>
      db 0 v0 v2 * (db 0 v1 v2 * (db 0 v1 v3 * db 0 v0 v3)))))) =
    rsum n (fun x => rsum n (fun y => (rsum n (fun k => db 0 x k * db 0 y k))^2))
  have hsq : ∀ X : Int, X ^ 2 = X * X := by intro X; grind
  simp only [hsq]
  have hexpand : ∀ x y : Nat,
      (rsum n (fun k => db 0 x k * db 0 y k)) * (rsum n (fun l => db 0 x l * db 0 y l)) =
        rsum n (fun k => rsum n (fun l =>
          (db 0 x k * db 0 y k) * (db 0 x l * db 0 y l))) := by
    intro x y
    rw [rsum_product]
  simp only [hexpand]
  let F : Nat → Nat → Nat → Nat → Int := fun x y k l =>
    (db 0 x k * db 0 y k) * (db 0 x l * db 0 y l)
  have hnorm : rsum n (fun v2 => rsum n (fun v3 => rsum n (fun v0 => rsum n (fun v1 =>
        db 0 v0 v2 * (db 0 v1 v2 * (db 0 v1 v3 * db 0 v0 v3)))))) =
      rsum n (fun v2 => rsum n (fun v3 => rsum n (fun v0 => rsum n (fun v1 =>
        F v0 v1 v2 v3)))) := by
    apply rsum_congr
    intro v2 hv2
    apply rsum_congr
    intro v3 hv3
    apply rsum_congr
    intro v0 hv0
    apply rsum_congr
    intro v1 hv1
    dsimp [F]
    ac_rfl
  rw [hnorm]
  change rsum n (fun k => rsum n (fun l => rsum n (fun x => rsum n (fun y =>
      F x y k l)))) =
    rsum n (fun x => rsum n (fun y => rsum n (fun k => rsum n (fun l =>
      F x y k l))))
  calc
    rsum n (fun k => rsum n (fun l => rsum n (fun x => rsum n (fun y => F x y k l))))
        = rsum n (fun k => rsum n (fun x => rsum n (fun l => rsum n (fun y => F x y k l)))) := by
            apply rsum_congr
            intro k hk
            exact rsum_comm n n (fun l x => rsum n (fun y => F x y k l))
    _ = rsum n (fun k => rsum n (fun x => rsum n (fun y => rsum n (fun l => F x y k l)))) := by
            apply rsum_congr
            intro k hk
            apply rsum_congr
            intro x hx
            exact rsum_comm n n (fun l y => F x y k l)
    _ = rsum n (fun x => rsum n (fun k => rsum n (fun y => rsum n (fun l => F x y k l)))) := by
            exact rsum_comm n n (fun k x => rsum n (fun y => rsum n (fun l => F x y k l)))
    _ = rsum n (fun x => rsum n (fun y => rsum n (fun k => rsum n (fun l => F x y k l)))) := by
            apply rsum_congr
            intro x hx
            exact rsum_comm n n (fun k y => rsum n (fun l => F x y k l))

end TinylambOpt.ResearchProofs.Atlas.Goal_CastOverlap_GramSquare
