import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ReplyCycle_UndirectedTrace

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReplyCycle
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total)

/-! Finite sums over `List.range`, the engine behind `integrate`/`total`/`bridge`. -/

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

/-! Reorder four summed variables from `(a,b,c,d)` to `(d,b,a,c)`. -/

private theorem rsum_rotate4 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => f a b c d)))) =
      rsum n (fun d => rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c d)))) := by
  calc
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => f a b c d))))
        = rsum n (fun a => rsum n (fun b => rsum n (fun d => rsum n (fun c => f a b c d)))) := by
            apply rsum_congr; intro a ha
            apply rsum_congr; intro b hb
            exact rsum_comm n n (fun c d => f a b c d)
    _ = rsum n (fun a => rsum n (fun d => rsum n (fun b => rsum n (fun c => f a b c d)))) := by
            apply rsum_congr; intro a ha
            exact rsum_comm n n (fun b d => rsum n (fun c => f a b c d))
    _ = rsum n (fun d => rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c d)))) :=
            rsum_comm n n (fun a d => rsum n (fun b => rsum n (fun c => f a b c d)))

private theorem rsum_rotateSwap4 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => f a b c d)))) =
      rsum n (fun d => rsum n (fun b => rsum n (fun a => rsum n (fun c => f a b c d)))) := by
  rw [rsum_rotate4]
  apply rsum_congr
  intro d hd
  exact rsum_comm n n (fun a b => rsum n (fun c => f a b c d))

theorem sound_ReplyCycle_UndirectedTrace : ReplyCycle.UndirectedTrace.Goal := by
  intro n db b hsym
  simp [reference, integrate, ReplyCycle.pattern, value, put, bridge, total]
  change rsum n (fun v1 => rsum n (fun v2 => rsum n (fun v3 => rsum n (fun v0 =>
      db 0 v0 v1 * (db 0 v1 v2 * (db 0 v2 v3 * db 0 v3 v0)))))) =
    rsum n (fun i => rsum n (fun j => (rsum n (fun k => db 0 i k * db 0 k j)) ^ 2))
  have hsq : ∀ X : Int, X ^ 2 = X * X := by intro X; grind
  simp only [hsq]
  have hexpand : ∀ i j : Nat,
      (rsum n (fun k => db 0 i k * db 0 k j)) * (rsum n (fun l => db 0 i l * db 0 l j)) =
        rsum n (fun k => rsum n (fun l =>
          (db 0 i k * db 0 k j) * (db 0 i l * db 0 l j))) := by
    intro i j
    rw [rsum_product]
  simp only [hexpand]
  have hrot := rsum_rotateSwap4 n (fun v1 v2 v3 v0 =>
    db 0 v0 v1 * (db 0 v1 v2 * (db 0 v2 v3 * db 0 v3 v0)))
  rw [hrot]
  apply rsum_congr
  intro i hi
  apply rsum_congr
  intro j hj
  apply rsum_congr
  intro k hk
  apply rsum_congr
  intro l hl
  rw [hsym j hj l hl, hsym l hl i hi]
  ac_rfl

end TinylambOpt.ResearchProofs.Atlas.Goal_ReplyCycle_UndirectedTrace
