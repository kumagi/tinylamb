import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_VoteBadge_IndependentArms

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.VoteBadge
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total)

/-! Minimal finite-sum toolkit over `Int`, specialized to enumeration over
`List.range n` used by `reference` and `total`. -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi)),
        h n (Nat.lt_succ_self n)]

private theorem sum_add {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

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

theorem sound_VoteBadge_IndependentArms : IndependentArms.Goal := by
  intro n db b
  simp [reference, integrate, pattern, value, put, total]
  change rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x5 => rsum n (fun x4 => rsum n (fun x1 =>
      db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x1 x3 * (db 3 (b 0) x4 * db 3 x4 x5)))))))) =
    rsum n (fun u => (db 0 (b 0) u * rsum n (fun k => db 1 u k)) *
      rsum n (fun k => db 2 u k)) *
    rsum n (fun v => db 3 (b 0) v * rsum n (fun w => db 3 v w))
  let armB : Int := rsum n (fun v => db 3 (b 0) v * rsum n (fun w => db 3 v w))
  calc
    rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x5 => rsum n (fun x4 => rsum n (fun x1 =>
        db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x1 x3 * (db 3 (b 0) x4 * db 3 x4 x5)))))))) =
        rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x5 => rsum n (fun x1 => rsum n (fun x4 =>
        db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x1 x3 * (db 3 (b 0) x4 * db 3 x4 x5)))))))) := by
          apply rsum_congr; intro x2 h2; apply rsum_congr; intro x3 h3
          apply rsum_congr; intro x5 h5
          rw [rsum_comm]
    _ = rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x1 => rsum n (fun x5 => rsum n (fun x4 =>
        db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x1 x3 * (db 3 (b 0) x4 * db 3 x4 x5)))))))) := by
          apply rsum_congr; intro x2 h2; apply rsum_congr; intro x3 h3
          rw [rsum_comm]
    _ = rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x1 => rsum n (fun x4 => rsum n (fun x5 =>
        db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x1 x3 * (db 3 (b 0) x4 * db 3 x4 x5)))))))) := by
          apply rsum_congr; intro x2 h2; apply rsum_congr; intro x3 h3
          apply rsum_congr; intro x1 h1
          rw [rsum_comm]
    _ = rsum n (fun x2 => rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x4 => rsum n (fun x5 =>
        db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x1 x3 * (db 3 (b 0) x4 * db 3 x4 x5)))))))) := by
          apply rsum_congr; intro x2 h2
          rw [rsum_comm]
    _ = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 => rsum n (fun x5 =>
        db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x1 x3 * (db 3 (b 0) x4 * db 3 x4 x5)))))))) := by
          rw [rsum_comm]
    _ = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 => rsum n (fun x5 =>
        (db 0 (b 0) x1 * db 1 x1 x2 * db 2 x1 x3) * (db 3 (b 0) x4 * db 3 x4 x5)))))) := by
          apply rsum_congr; intro x1 h1; apply rsum_congr; intro x2 h2
          apply rsum_congr; intro x3 h3; apply rsum_congr; intro x4 h4
          apply rsum_congr; intro x5 h5
          ac_rfl
    _ = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
        (db 0 (b 0) x1 * db 1 x1 x2 * db 2 x1 x3) *
          (db 3 (b 0) x4 * rsum n (fun w => db 3 x4 w)))))) := by
          apply rsum_congr; intro x1 h1; apply rsum_congr; intro x2 h2
          apply rsum_congr; intro x3 h3; apply rsum_congr; intro x4 h4
          rw [rsum_const_mul, rsum_const_mul]
    _ = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
        (db 0 (b 0) x1 * db 1 x1 x2 * db 2 x1 x3) * armB))) := by
          apply rsum_congr; intro x1 h1; apply rsum_congr; intro x2 h2
          apply rsum_congr; intro x3 h3
          rw [rsum_const_mul]
    _ = rsum n (fun x1 => rsum n (fun x2 =>
        ((db 0 (b 0) x1 * db 1 x1 x2) * rsum n (fun k => db 2 x1 k)) * armB)) := by
          apply rsum_congr; intro x1 h1; apply rsum_congr; intro x2 h2
          calc
            rsum n (fun x3 => (db 0 (b 0) x1 * db 1 x1 x2 * db 2 x1 x3) * armB)
                = rsum n (fun x3 => ((db 0 (b 0) x1 * db 1 x1 x2) * armB) * db 2 x1 x3) := by
                    apply rsum_congr; intro x3 h3; ac_rfl
            _ = ((db 0 (b 0) x1 * db 1 x1 x2) * armB) * rsum n (fun k => db 2 x1 k) :=
                    rsum_const_mul n ((db 0 (b 0) x1 * db 1 x1 x2) * armB) (fun k => db 2 x1 k)
            _ = ((db 0 (b 0) x1 * db 1 x1 x2) * rsum n (fun k => db 2 x1 k)) * armB := by ac_rfl
    _ = rsum n (fun x1 =>
        ((db 0 (b 0) x1 * rsum n (fun k => db 1 x1 k)) *
          rsum n (fun k => db 2 x1 k)) * armB) := by
          apply rsum_congr; intro x1 h1
          calc
            rsum n (fun x2 => ((db 0 (b 0) x1 * db 1 x1 x2) * rsum n (fun k => db 2 x1 k)) * armB)
                = rsum n (fun x2 => ((db 0 (b 0) x1 * rsum n (fun k => db 2 x1 k)) * armB) * db 1 x1 x2) := by
                    apply rsum_congr; intro x2 h2; ac_rfl
            _ = ((db 0 (b 0) x1 * rsum n (fun k => db 2 x1 k)) * armB) *
                  rsum n (fun k => db 1 x1 k) :=
                    rsum_const_mul n ((db 0 (b 0) x1 * rsum n (fun k => db 2 x1 k)) * armB) (fun k => db 1 x1 k)
            _ = ((db 0 (b 0) x1 * rsum n (fun k => db 1 x1 k)) *
                  rsum n (fun k => db 2 x1 k)) * armB := by ac_rfl
    _ = rsum n (fun x1 =>
        (db 0 (b 0) x1 * rsum n (fun k => db 1 x1 k)) *
          rsum n (fun k => db 2 x1 k)) * armB := by
          rw [rsum_mul_const]
    _ = rsum n (fun u => (db 0 (b 0) u * rsum n (fun k => db 1 u k)) *
          rsum n (fun k => db 2 u k)) *
        rsum n (fun v => db 3 (b 0) v * rsum n (fun w => db 3 v w)) := by
          simp [armB]

end TinylambOpt.ResearchProofs.Atlas.Goal_VoteBadge_IndependentArms
