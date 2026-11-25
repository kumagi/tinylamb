import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_HeavyBounds_AliasError

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.HeavyBounds

private theorem sum_map_add_nat {α : Type} (l : List α) (f g : α → Nat) :
    (l.map fun x => f x + g x).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem beq_comm_nat (a b : Nat) : (a == b) = (b == a) := by
  by_cases h : a == b
  · have hab : a = b := of_decide_eq_true h
    subst b
    simp
  · have hne : a ≠ b := by intro heq; exact h (by simp [heq])
    have h' : (b == a) = false := by
      apply Bool.eq_false_of_ne_true
      intro hb
      exact hne (of_decide_eq_true hb).symm
    simp [h, h']

private theorem countP_map_count (b : List Nat) (f : Nat → Nat) (x : Nat) :
    (b.map f).count (f x) = b.countP (fun y => f x == f y) := by
  have h : (b.map f).count (f x) = b.countP (fun y => f y == f x) := by
    induction b with
    | nil => simp
    | cons y ys ih =>
        simp only [List.count_cons, List.map_cons, List.countP_cons]
        by_cases hy : (f y == f x) = true
        · simp [hy, ih]
        · simp [hy, ih]
  rw [h]
  congr 1
  funext y
  rw [beq_comm_nat]

private theorem countP_bucket_split (b : List Nat) (f : Nat → Nat) (x : Nat) :
    b.countP (fun y => f x == f y) =
      b.count x + b.countP (fun y => y != x && f x == f y) := by
  induction b with
  | nil => simp
  | cons y ys ih =>
      simp only [List.countP_cons, List.count_cons]
      by_cases hyb : (y == x) = true
      · have hy : y = x := of_decide_eq_true hyb
        subst y
        simp [ih]
        omega
      · by_cases hfy : (f x == f y) = true
        · simp [hyb, hfy, ih]
          have hyne : ¬ y = x := by intro heq; exact hyb (by simp [heq])
          simp [hyne]
          omega
        · simp [hyb, hfy, ih]

theorem sound_HeavyBounds_AliasError : HeavyBounds.AliasError.Goal := by
  intro a b f
  have hpoint : ∀ x : Nat, (b.map f).count (f x) =
      b.count x + b.countP (fun y => y != x && f x == f y) := by
    intro x
    rw [countP_map_count, countP_bucket_split]
  have hmap : (a.map fun x => (b.map f).count (f x)).sum =
      (a.map fun x => b.count x + b.countP (fun y => y != x && f x == f y)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    exact hpoint x
  have hjoin : joinSize (a.map f) (b.map f) = (a.map fun x => (b.map f).count (f x)).sum := by
    unfold joinSize
    rw [List.map_map]
    rfl
  rw [hjoin]
  change (a.map fun x => (b.map f).count (f x)).sum - (a.map fun x => b.count x).sum =
    (a.map fun x => b.countP (fun y => y != x && f x == f y)).sum
  rw [hmap, sum_map_add_nat]
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_HeavyBounds_AliasError
