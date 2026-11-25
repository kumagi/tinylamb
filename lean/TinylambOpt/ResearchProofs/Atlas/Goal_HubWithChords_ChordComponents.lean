import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_HubWithChords_ChordComponents

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.HubWithChords
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total dot)

/-! Self-contained finite-sum toolkit (integer vectors indexed by `Nat`). -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem sum_congr {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
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
            intro i _
            exact rsum_const_mul m (f i) g
    _ = rsum n f * rsum m g := rsum_mul_const n f (rsum m g)

private theorem put_self (b : Bind) (v x : Nat) : put b v x v = x := by simp [put]

private theorem put_same (b : Bind) (v : Nat) : put b v (b v) = b := by
  funext i
  by_cases h : i == v
  · have hi : i = v := of_decide_eq_true h
    subst i
    simp [put]
  · simp [put, h]

private theorem put_comm_ne (b : Bind) (v w x y : Nat) (h : v ≠ w) :
    put (put b v x) w y = put (put b w y) v x := by
  funext i
  by_cases hv : i = v
  · subst i
    simp [put, h]
  · by_cases hw : i = w
    · subst i
      simp [put, h.symm]
    · simp [put, hv, hw]

private theorem integrate_congr (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ x, f x = g x) : integrate n vs b f = integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum
      exact sum_congr _ _ _ (fun x _ => ih (put b v x))

private theorem integrate_swap (n v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: w :: vs) b f = integrate n (w :: v :: vs) b f := by
  by_cases h : v = w
  · subst w; rfl
  change ((List.range n).map (fun x =>
      ((List.range n).map (fun y => integrate n vs (put (put b v x) w y) f)).sum)).sum =
    ((List.range n).map (fun x =>
      ((List.range n).map (fun y => integrate n vs (put (put b w x) v y) f)).sum)).sum
  rw [show ((List.range n).map (fun x =>
      ((List.range n).map (fun y => integrate n vs (put (put b v x) w y) f)).sum)).sum =
      rsum n (fun x => rsum n (fun y => integrate n vs (put (put b v x) w y) f)) by rfl,
    show ((List.range n).map (fun x =>
      ((List.range n).map (fun y => integrate n vs (put (put b w x) v y) f)).sum)).sum =
      rsum n (fun x => rsum n (fun y => integrate n vs (put (put b w x) v y) f)) by rfl]
  rw [rsum_comm]
  exact sum_congr _ _ _ (fun x _ => sum_congr _ _ _ (fun y _ => by
    rw [put_comm_ne b v w y x h]))

private theorem integrate_perm (n : Nat) {vs vs' : List Nat} (h : vs.Perm vs') :
    ∀ (b : Bind) (f : Bind → Int), integrate n vs b f = integrate n vs' b f := by
  induction h with
  | nil => intro b f; rfl
  | cons x h ih => intro b f; exact integrate_congr n [x] b _ _ (ih · f)
  | swap x y l => intro b f; exact integrate_swap n y x l b f
  | trans h1 h2 ih1 ih2 => intro b f; exact (ih1 b f).trans (ih2 b f)

private theorem rsum_pull_four (n : Nat) (F : Nat → Nat → Nat → Nat → Int) (c : Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun d => rsum n (fun e => F a b d e * c)))) =
      rsum n (fun a => rsum n (fun b => rsum n (fun d => rsum n (fun e => F a b d e)))) * c := by
  rw [show rsum n (fun a => rsum n (fun b => rsum n (fun d => rsum n (fun e => F a b d e * c)))) =
      rsum n (fun a => rsum n (fun b => rsum n (fun d => rsum n (fun e => F a b d e) * c))) by
        apply rsum_congr; intro a _
        apply rsum_congr; intro b _
        apply rsum_congr; intro d _
        exact rsum_mul_const n (fun e => F a b d e) c]
  rw [show rsum n (fun a => rsum n (fun b => rsum n (fun d => rsum n (fun e => F a b d e) * c))) =
      rsum n (fun a => rsum n (fun b => rsum n (fun d => rsum n (fun e => F a b d e)) * c)) by
        apply rsum_congr; intro a _
        apply rsum_congr; intro b _
        exact rsum_mul_const n (fun d => rsum n (fun e => F a b d e)) c]
  rw [show rsum n (fun a => rsum n (fun b => rsum n (fun d => rsum n (fun e => F a b d e)) * c)) =
      rsum n (fun a => rsum n (fun b => rsum n (fun d => rsum n (fun e => F a b d e))) * c) by
        apply rsum_congr; intro a _
        exact rsum_mul_const n (fun b => rsum n (fun d => rsum n (fun e => F a b d e))) c]
  exact rsum_mul_const n (fun a => rsum n (fun b => rsum n (fun d => rsum n (fun e => F a b d e)))) c

/-! Five independent summations grouped as two internal pairs and a singleton. -/
private theorem rsum_prod_blocks (n : Nat) (A B : Nat → Nat → Int) (C : Nat → Int) :
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 => rsum n (fun x5 =>
        A x1 x2 * (B x3 x4 * C x5)))))) =
    (rsum n (fun x1 => rsum n (fun x2 => A x1 x2))) *
      (rsum n (fun x3 => rsum n (fun x4 => B x3 x4))) * rsum n C := by
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 => rsum n (fun x5 =>
        A x1 x2 * (B x3 x4 * C x5))))))
        = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
            (A x1 x2 * B x3 x4) * rsum n C)))) := by
              apply rsum_congr; intro x1 _
              apply rsum_congr; intro x2 _
              apply rsum_congr; intro x3 _
              apply rsum_congr; intro x4 _
              rw [show rsum n (fun x5 => A x1 x2 * (B x3 x4 * C x5)) =
                  rsum n (fun x5 => (A x1 x2 * B x3 x4) * C x5) by
                    apply rsum_congr; intro x5 _; ac_rfl]
              exact rsum_const_mul n (A x1 x2 * B x3 x4) C
    _ = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
            A x1 x2 * B x3 x4)))) * rsum n C := by
          exact rsum_pull_four n (fun x1 x2 x3 x4 => A x1 x2 * B x3 x4) (rsum n C)
    _ = rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x2 => rsum n (fun x4 =>
            A x1 x2 * B x3 x4)))) * rsum n C := by
          congr 1
          apply rsum_congr; intro x1 _
          rw [rsum_comm n n (fun x2 x3 => rsum n (fun x4 => A x1 x2 * B x3 x4))]
    _ = rsum n (fun x1 => rsum n (fun x3 =>
          rsum n (fun x2 => A x1 x2) * rsum n (fun x4 => B x3 x4))) * rsum n C := by
          congr 1
          apply rsum_congr; intro x1 _
          apply rsum_congr; intro x3 _
          rw [rsum_product n n (fun x2 => A x1 x2) (fun x4 => B x3 x4)]
    _ = (rsum n (fun x1 => rsum n (fun x2 => A x1 x2))) *
        (rsum n (fun x3 => rsum n (fun x4 => B x3 x4))) * rsum n C := by
          congr 1
          rw [rsum_product n n (fun x1 => rsum n (fun x2 => A x1 x2))
            (fun x3 => rsum n (fun x4 => B x3 x4))]

theorem sound_HubWithChords_ChordComponents : HubWithChords.ChordComponents.Goal := by
  intro n db b
  unfold reference
  have hperm : HubWithChords.pattern.freeVars.Perm [0, 1, 2, 3, 4, 5] := by native_decide
  rw [integrate_perm n hperm b]
  simp [integrate, HubWithChords.pattern, value, put, dot, total]
  change rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      rsum n (fun x4 => rsum n (fun x5 =>
        db 0 x0 x1 * (db 1 x0 x2 * (db 2 x0 x3 * (db 3 x0 x4 *
          (db 4 x0 x5 * (db 5 x1 x2 * db 6 x3 x4))))))))))) =
    rsum n (fun x0 =>
      (rsum n (fun i => db 0 x0 i * rsum n (fun j => db 5 i j * db 1 x0 j))) *
      (rsum n (fun i => db 2 x0 i * rsum n (fun j => db 6 i j * db 3 x0 j))) *
      rsum n (fun y => db 4 x0 y))
  apply rsum_congr
  intro x0 _
  rw [show rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
        rsum n (fun x5 => db 0 x0 x1 * (db 1 x0 x2 * (db 2 x0 x3 * (db 3 x0 x4 *
          (db 4 x0 x5 * (db 5 x1 x2 * db 6 x3 x4)))))))))) =
      rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
        rsum n (fun x5 =>
          (db 0 x0 x1 * (db 5 x1 x2 * db 1 x0 x2)) *
            ((db 2 x0 x3 * (db 6 x3 x4 * db 3 x0 x4)) * db 4 x0 x5)))))) by
        apply rsum_congr; intro x1 _
        apply rsum_congr; intro x2 _
        apply rsum_congr; intro x3 _
        apply rsum_congr; intro x4 _
        apply rsum_congr; intro x5 _
        ac_rfl]
  rw [rsum_prod_blocks n
    (fun x1 x2 => db 0 x0 x1 * (db 5 x1 x2 * db 1 x0 x2))
    (fun x3 x4 => db 2 x0 x3 * (db 6 x3 x4 * db 3 x0 x4))
    (fun x5 => db 4 x0 x5)]
  have hA : rsum n (fun x1 => rsum n (fun x2 => db 0 x0 x1 * (db 5 x1 x2 * db 1 x0 x2))) =
      rsum n (fun i => db 0 x0 i * rsum n (fun j => db 5 i j * db 1 x0 j)) := by
    apply rsum_congr; intro i _
    exact rsum_const_mul n (db 0 x0 i) (fun j => db 5 i j * db 1 x0 j)
  have hB : rsum n (fun x3 => rsum n (fun x4 => db 2 x0 x3 * (db 6 x3 x4 * db 3 x0 x4))) =
      rsum n (fun i => db 2 x0 i * rsum n (fun j => db 6 i j * db 3 x0 j)) := by
    apply rsum_congr; intro i _
    exact rsum_const_mul n (db 2 x0 i) (fun j => db 6 i j * db 3 x0 j)
  rw [hA, hB]

end TinylambOpt.ResearchProofs.Atlas.Goal_HubWithChords_ChordComponents
