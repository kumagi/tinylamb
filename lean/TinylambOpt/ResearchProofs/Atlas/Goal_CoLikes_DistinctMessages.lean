import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_CoLikes_DistinctMessages

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total dot)
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.CoLikes

/-- Finite sums over `List.range`, the engine behind `integrate`, `total` and `dot`. -/
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

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi)),
        h n (Nat.lt_succ_self n)]

private theorem rsum_sub (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i - g i) = rsum n f - rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := sum_mul_left (List.range n) c f

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
      -- distribute the trailing sum over the addition
      change ((List.range m).map (fun j => rsum n (fun i => f i j))).sum +
          ((List.range m).map (fun j => f n j)).sum =
        ((List.range m).map (fun j => rsum n (fun i => f i j) + f n j)).sum
      rw [sum_add]

private theorem rsum_product (n m : Nat) (f g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr
            intro i hi
            exact rsum_const_mul m (f i) g
    -- factor a common right multiplier out of the outer sum
    _ = rsum n f * rsum m g := by
            change ((List.range n).map (fun i => f i * rsum m g)).sum =
              ((List.range n).map f).sum * rsum m g
            induction List.range n with
            | nil => simp
            | cons a l ih => simp [ih, Int.add_mul]

private theorem sq (X : Int) : X ^ 2 = X * X := by grind

/-- The only index `k < n` with `j = k` is `k = j`. -/
private theorem rsum_diag (n j : Nat) (f : Nat → Int) :
    rsum n (fun k => if j = k then f k else 0) = if j < n then f j else 0 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, ih]
      by_cases h : j = n
      · subst j
        simp
      · by_cases hlt : j < n
        · simp [h, hlt, Nat.lt_succ_of_lt hlt]
        · have hn : ¬ j < n + 1 := by omega
          simp [h, hlt, hn]

/-- Off-diagonal double sum: `sum_{j≠k} A j A k = (sum A)^2 - sum A j^2`. -/
private theorem rsum_offdiag (n : Nat) (A : Nat → Int) :
    rsum n (fun j => rsum n (fun k => if j = k then 0 else A j * A k)) =
      (rsum n A) ^ 2 - rsum n (fun j => (A j) ^ 2) := by
  have hpoint : ∀ j k, (if j = k then 0 else A j * A k) =
      A j * A k - (if j = k then A j * A k else 0) := by
    intro j k
    by_cases h : j = k <;> simp [h]
  calc
    rsum n (fun j => rsum n (fun k => if j = k then 0 else A j * A k))
        = rsum n (fun j => rsum n (fun k =>
            A j * A k - (if j = k then A j * A k else 0))) := by
            apply rsum_congr
            intro j hj
            apply rsum_congr
            intro k hk
            exact hpoint j k
    _ = rsum n (fun j => rsum n (fun k => A j * A k)) -
        rsum n (fun j => rsum n (fun k => if j = k then A j * A k else 0)) := by
            calc
              rsum n (fun j => rsum n (fun k =>
                  A j * A k - (if j = k then A j * A k else 0)))
                  = rsum n (fun j => rsum n (fun k => A j * A k) -
                      rsum n (fun k => if j = k then A j * A k else 0)) := by
                      apply rsum_congr
                      intro j hj
                      exact rsum_sub n _ _
              _ = rsum n (fun j => rsum n (fun k => A j * A k)) -
                  rsum n (fun j => rsum n (fun k => if j = k then A j * A k else 0)) :=
                  rsum_sub n _ _
    _ = (rsum n A) ^ 2 - rsum n (fun j => (A j) ^ 2) := by
            have hprod : rsum n (fun j => rsum n (fun k => A j * A k)) = (rsum n A) ^ 2 := by
              rw [rsum_product, sq]
            have hdiag : rsum n (fun j => rsum n (fun k =>
                if j = k then A j * A k else 0)) = rsum n (fun j => (A j) ^ 2) := by
              apply rsum_congr
              intro j hj
              rw [rsum_diag n j (fun k => A j * A k)]
              have hjn : j < n := hj
              simp [hjn, sq]
            rw [hprod, hdiag]

theorem sound_CoLikes_DistinctMessages : DistinctMessages.Goal := by
  intro n db b
  simp [integrate, pattern, factors, value, put, product, overlap, dot, total]
  let A : Nat → Nat → Int := fun u j => db 0 u j * db 0 (b 1) j
  let F : Nat → Nat → Nat → Int := fun u j k =>
    if j = k then 0 else
      db 0 u j * (db 0 (b 1) j * (db 0 (b 1) k * (db 0 u k * db 2 u (b 1))))
  change rsum n (fun j => rsum n (fun k => rsum n (fun u => F u j k))) =
    rsum n (fun u => db 2 u (b 1) * ((rsum n (A u)) ^ 2 - rsum n (fun j => (A u j) ^ 2)))
  have hF : ∀ u j k : Nat, F u j k = db 2 u (b 1) * (if j = k then 0 else A u j * A u k) := by
    intro u j k
    dsimp [F, A]
    by_cases h : j = k <;> simp [h] <;> ac_rfl
  calc
    rsum n (fun j => rsum n (fun k => rsum n (fun u => F u j k)))
        = rsum n (fun u => rsum n (fun j => rsum n (fun k => F u j k))) := by
            calc
              rsum n (fun j => rsum n (fun k => rsum n (fun u => F u j k)))
                  = rsum n (fun j => rsum n (fun u => rsum n (fun k => F u j k))) := by
                      apply rsum_congr
                      intro j hj
                      exact rsum_comm n n (fun k u => F u j k)
              _ = rsum n (fun u => rsum n (fun j => rsum n (fun k => F u j k))) :=
                  rsum_comm n n (fun j u => rsum n (fun k => F u j k))
    _ = rsum n (fun u => rsum n (fun j => rsum n (fun k =>
            db 2 u (b 1) * (if j = k then 0 else A u j * A u k)))) := by
            apply rsum_congr
            intro u hu
            apply rsum_congr
            intro j hj
            apply rsum_congr
            intro k hk
            rw [hF]
    _ = rsum n (fun u => db 2 u (b 1) *
          rsum n (fun j => rsum n (fun k => if j = k then 0 else A u j * A u k))) := by
            apply rsum_congr
            intro u hu
            calc
              rsum n (fun j => rsum n (fun k =>
                  db 2 u (b 1) * (if j = k then 0 else A u j * A u k)))
                  = rsum n (fun j => db 2 u (b 1) *
                      rsum n (fun k => if j = k then 0 else A u j * A u k)) := by
                      apply rsum_congr
                      intro j hj
                      exact rsum_const_mul n (db 2 u (b 1))
                        (fun k => if j = k then 0 else A u j * A u k)
              _ = db 2 u (b 1) * rsum n (fun j =>
                    rsum n (fun k => if j = k then 0 else A u j * A u k)) :=
                  rsum_const_mul n (db 2 u (b 1)) (fun j =>
                    rsum n (fun k => if j = k then 0 else A u j * A u k))
    _ = rsum n (fun u => db 2 u (b 1) * ((rsum n (A u)) ^ 2 - rsum n (fun j => (A u j) ^ 2))) := by
            apply rsum_congr
            intro u hu
            rw [rsum_offdiag n (A u)]

end TinylambOpt.ResearchProofs.Atlas.Goal_CoLikes_DistinctMessages
