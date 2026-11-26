import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `MovieCredits.TriangleNorm` — `∀ n db b, (reference pattern n db b)^2 ≤ normSq n db 0
  * normSq n db 1 * normSq n db 2`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_MovieCredits_TriangleNorm

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.MovieCredits

/-! Finite-range integer sums (Std only), a Cauchy-Schwarz inequality for lists
of integer pairs, and the triangle bound `tr(A B C)^2 ≤ ||A||² ||B||² ||C||²`. -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem list_sum_congr {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp only [List.map_cons, List.sum_cons, h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]
private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) : rsum n f = rsum n g := by
  apply list_sum_congr
  intro i hi
  exact h i (List.mem_range.mp hi)
private theorem rsum_add (n : Nat) (f g : Nat → Int) : rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega
private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) : rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.mul_add]
private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) : rsum n (fun i => f i * c) = rsum n f * c := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.add_mul]
private theorem rsum_const_zero (n : Nat) : rsum n (fun _ => (0 : Int)) = 0 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, ih]; simp
private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [show rsum 0 (fun i => rsum m (fun j => f i j)) = 0 by simp [rsum]]
      rw [show rsum m (fun j => rsum 0 (fun i => f i j)) = rsum m (fun _ => (0 : Int)) by
        apply rsum_congr; intro j hj; simp [rsum]]
      exact (rsum_const_zero m).symm
  | succ n ih =>
      rw [rsum_succ, ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) = rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr; intro j hj; rw [rsum_succ]]
      rw [rsum_add]
private theorem rsum_nonneg (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → 0 ≤ f i) : 0 ≤ rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega
private theorem rsum_le (n : Nat) (f g : Nat → Int) (h : ∀ i, i < n → f i ≤ g i) : rsum n f ≤ rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega
private theorem rsum_product (n m : Nat) (f g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr; intro i hi; rw [rsum_const_mul]
    _ = rsum n f * rsum m g := by rw [rsum_mul_const]
private theorem list_pairs_sum (l : List Nat) (n : Nat) (f : Nat → Nat → Int) :
    (l.map (fun i => rsum n (fun j => f i j))).sum =
      ((l.flatMap (fun i => (List.range n).map (fun j => (i,j)))).map
        (fun p => f p.1 p.2)).sum := by
  induction l with
  | nil => simp
  | cons i l ih =>
      simp only [List.map_cons, List.sum_cons, List.flatMap_cons]
      rw [ih]
      simp [rsum, List.map_append, List.sum_append, Function.comp_def]
private theorem rsum_pairs (n : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum n (fun j => f i j)) =
      (((List.range n).flatMap (fun i => (List.range n).map (fun j => (i,j)))).map
        (fun p => f p.1 p.2)).sum := by
  exact list_pairs_sum (List.range n) n f

private theorem int_sq_eq_mul (x : Int) : x ^ 2 = x * x := by
  rw [Int.pow_succ, Int.pow_succ]
  simp

private theorem int_mul_self_nonneg (z : Int) : 0 ≤ z * z := by
  by_cases hz : 0 ≤ z
  · exact Int.mul_nonneg hz hz
  · have hnz : 0 ≤ -z := by omega
    have h := Int.mul_nonneg hnz hnz
    rw [show z * z = (-z) * (-z) by grind]
    exact h

private theorem cs_aux (l : List (Int × Int)) (a b : Int) :
  (l.map (fun p => p.1*p.1)).sum * (b*b) + (l.map (fun p => p.2*p.2)).sum * (a*a) -
    2*(l.map (fun p => p.1*p.2)).sum*a*b =
  (l.map (fun p => (a*p.2 - p.1*b)^2)).sum := by
  induction l with
  | nil => simp
  | cons p l ih =>
      simp only [List.map_cons, List.sum_cons]
      grind

private theorem cs_sum_sq_nonneg (l : List (Int × Int)) (a b : Int) :
    0 ≤ (l.map (fun p => (a*p.2 - p.1*b)^2)).sum := by
  induction l with
  | nil => simp
  | cons p l ih =>
      simp only [List.map_cons, List.sum_cons]
      have hp : 0 ≤ (a*p.2 - p.1*b)^2 := Int.sq_nonneg _
      omega

private theorem cs_list : ∀ l : List (Int × Int),
    (l.map (fun p => p.1 * p.2)).sum ^ 2 ≤
      (l.map (fun p => p.1 * p.1)).sum * (l.map (fun p => p.2 * p.2)).sum
  | [] => by simp
  | (a,b)::l => by
      simp only [List.map_cons, List.sum_cons]
      have ih := cs_list l
      have hdiff : 0 ≤ (a*a + (l.map (fun p => p.1*p.1)).sum) *
              (b*b + (l.map (fun p => p.2*p.2)).sum) -
            (a*b + (l.map (fun p => p.1*p.2)).sum)^2 := by
          rw [show (a*a + (l.map (fun p => p.1*p.1)).sum) *
                (b*b + (l.map (fun p => p.2*p.2)).sum) -
                (a*b + (l.map (fun p => p.1*p.2)).sum)^2 =
              ((l.map (fun p => p.1*p.1)).sum * (l.map (fun p => p.2*p.2)).sum -
                (l.map (fun p => p.1*p.2)).sum^2) +
              ((l.map (fun p => p.1*p.1)).sum * (b*b) + (l.map (fun p => p.2*p.2)).sum * (a*a) -
                2*(l.map (fun p => p.1*p.2)).sum*a*b) by grind]
          rw [cs_aux]
          have h1 : 0 ≤ (l.map (fun p => p.1*p.1)).sum * (l.map (fun p => p.2*p.2)).sum -
                (l.map (fun p => p.1*p.2)).sum^2 := by omega
          have h2 : 0 ≤ (l.map (fun p => (a*p.2 - p.1*b)^2)).sum := cs_sum_sq_nonneg l a b
          omega
      omega

private theorem cs_rsum (n : Nat) (f g : Nat → Int) :
    (rsum n (fun i => f i * g i)) ^ 2 ≤
      (rsum n (fun i => f i * f i)) * (rsum n (fun i => g i * g i)) := by
  have h := cs_list ((List.range n).map (fun i => (f i, g i)))
  simpa [rsum, List.map_map, Function.comp_def] using h

private theorem cs_rsum2 (n : Nat) (f g : Nat → Nat → Int) :
    (rsum n (fun i => rsum n (fun j => f i j * g i j))) ^ 2 ≤
      (rsum n (fun i => rsum n (fun j => f i j * f i j))) *
      (rsum n (fun i => rsum n (fun j => g i j * g i j))) := by
  rw [rsum_pairs, rsum_pairs, rsum_pairs]
  have h := cs_list (((List.range n).flatMap (fun i => (List.range n).map (fun j => (i,j)))).map
    (fun p => (f p.1 p.2, g p.1 p.2)))
  simpa [List.map_map, Function.comp_def] using h

theorem sound_MovieCredits_TriangleNorm : TriangleNorm.Goal := by
  intro n db b
  let P : Nat → Nat → Int := fun d c => rsum n (fun a => db 0 d a * db 1 a c)
  let Q : Nat → Nat → Int := fun d c => db 2 c d
  let T : Int := rsum n (fun d => rsum n (fun c => P d c * Q d c))
  have href : reference pattern n db b = T := by
    dsimp only [T, P, Q]
    rw [show reference pattern n db b =
        rsum n (fun a => rsum n (fun c => rsum n (fun d =>
          db 0 d a * (db 1 a c * db 2 c d)))) by
          simp [reference, pattern, integrate, value, put, rsum]]
    calc
      rsum n (fun a => rsum n (fun c => rsum n (fun d =>
          db 0 d a * (db 1 a c * db 2 c d))))
          = rsum n (fun c => rsum n (fun a => rsum n (fun d =>
              db 0 d a * (db 1 a c * db 2 c d)))) := by rw [rsum_comm]
      _ = rsum n (fun c => rsum n (fun d => rsum n (fun a =>
              db 0 d a * (db 1 a c * db 2 c d)))) := by
              apply rsum_congr; intro c hc; rw [rsum_comm]
      _ = rsum n (fun d => rsum n (fun c => rsum n (fun a =>
              db 0 d a * (db 1 a c * db 2 c d)))) := by rw [rsum_comm]
      _ = rsum n (fun d => rsum n (fun c => rsum n (fun a =>
              (db 0 d a * db 1 a c) * db 2 c d))) := by
              apply rsum_congr; intro d hd; apply rsum_congr; intro c hc
              apply rsum_congr; intro a ha; ac_rfl
      _ = rsum n (fun d => rsum n (fun c =>
              (rsum n (fun a => db 0 d a * db 1 a c)) * db 2 c d)) := by
              apply rsum_congr; intro d hd; apply rsum_congr; intro c hc
              rw [rsum_mul_const]
  have hn0 : normSq n db 0 = rsum n (fun d => rsum n (fun a => db 0 d a * db 0 d a)) := by
    simp [normSq, TinylambOpt.ResearchGoals.Frontiers.Contractions.dot,
      TinylambOpt.ResearchGoals.Frontiers.Contractions.total, rsum]
  have hn1 : normSq n db 1 = rsum n (fun a => rsum n (fun c => db 1 a c * db 1 a c)) := by
    simp [normSq, TinylambOpt.ResearchGoals.Frontiers.Contractions.dot,
      TinylambOpt.ResearchGoals.Frontiers.Contractions.total, rsum]
  have hn2 : normSq n db 2 = rsum n (fun c => rsum n (fun d => db 2 c d * db 2 c d)) := by
    simp [normSq, TinylambOpt.ResearchGoals.Frontiers.Contractions.dot,
      TinylambOpt.ResearchGoals.Frontiers.Contractions.total, rsum]
  have hQ : rsum n (fun d => rsum n (fun c => Q d c * Q d c)) = normSq n db 2 := by
    dsimp only [Q]
    rw [rsum_comm]
    exact hn2.symm
  have houter : T ^ 2 ≤ (rsum n (fun d => rsum n (fun c => P d c * P d c))) * normSq n db 2 := by
    have h := cs_rsum2 n P Q
    simpa [T, hQ] using h
  have hPpoint : ∀ d c, P d c * P d c ≤
      (rsum n (fun a => db 0 d a * db 0 d a)) * (rsum n (fun a => db 1 a c * db 1 a c)) := by
    intro d c
    dsimp only [P]
    have h := cs_rsum n (fun a => db 0 d a) (fun a => db 1 a c)
    rw [int_sq_eq_mul] at h
    exact h
  have hPsum : rsum n (fun d => rsum n (fun c => P d c * P d c)) ≤
      rsum n (fun d => rsum n (fun c =>
        (rsum n (fun a => db 0 d a * db 0 d a)) * (rsum n (fun a => db 1 a c * db 1 a c)))) := by
    apply rsum_le; intro d hd
    apply rsum_le; intro c hc
    exact hPpoint d c
  have hPsep : rsum n (fun d => rsum n (fun c =>
        (rsum n (fun a => db 0 d a * db 0 d a)) * (rsum n (fun a => db 1 a c * db 1 a c)))) =
      (rsum n (fun d => rsum n (fun a => db 0 d a * db 0 d a))) *
      (rsum n (fun c => rsum n (fun a => db 1 a c * db 1 a c))) := by
    calc
      rsum n (fun d => rsum n (fun c =>
          (rsum n (fun a => db 0 d a * db 0 d a)) * (rsum n (fun a => db 1 a c * db 1 a c))))
          = rsum n (fun d => (rsum n (fun a => db 0 d a * db 0 d a)) *
              (rsum n (fun c => rsum n (fun a => db 1 a c * db 1 a c)))) := by
                apply rsum_congr; intro d hd
                rw [rsum_const_mul]
      _ = (rsum n (fun d => rsum n (fun a => db 0 d a * db 0 d a))) *
              (rsum n (fun c => rsum n (fun a => db 1 a c * db 1 a c))) := by
                rw [rsum_mul_const]
  have hn1' : rsum n (fun c => rsum n (fun a => db 1 a c * db 1 a c)) = normSq n db 1 := by
    rw [rsum_comm]
    exact hn1.symm
  have hPfinal : rsum n (fun d => rsum n (fun c => P d c * P d c)) ≤
      normSq n db 0 * normSq n db 1 := by
    calc
      rsum n (fun d => rsum n (fun c => P d c * P d c))
          ≤ rsum n (fun d => rsum n (fun c =>
              (rsum n (fun a => db 0 d a * db 0 d a)) * (rsum n (fun a => db 1 a c * db 1 a c)))) := hPsum
      _ = (rsum n (fun d => rsum n (fun a => db 0 d a * db 0 d a))) *
          (rsum n (fun c => rsum n (fun a => db 1 a c * db 1 a c))) := hPsep
      _ = normSq n db 0 * normSq n db 1 := by rw [← hn0, hn1']
  have hN0non : 0 ≤ normSq n db 0 := by
    rw [hn0]
    apply rsum_nonneg; intro d hd
    apply rsum_nonneg; intro a ha
    exact int_mul_self_nonneg (db 0 d a)
  have hN1non : 0 ≤ normSq n db 1 := by
    rw [hn1]
    apply rsum_nonneg; intro a ha
    apply rsum_nonneg; intro c hc
    exact int_mul_self_nonneg (db 1 a c)
  have hN2non : 0 ≤ normSq n db 2 := by
    rw [hn2]
    apply rsum_nonneg; intro c hc
    apply rsum_nonneg; intro d hd
    exact int_mul_self_nonneg (db 2 c d)
  have hfinal : T ^ 2 ≤ (normSq n db 0 * normSq n db 1) * normSq n db 2 := by
    have hrefl : normSq n db 2 ≤ normSq n db 2 := by omega
    calc
      T ^ 2 ≤ (rsum n (fun d => rsum n (fun c => P d c * P d c))) * normSq n db 2 := houter
      _ ≤ (normSq n db 0 * normSq n db 1) * normSq n db 2 :=
        Int.mul_le_mul hPfinal hrefl hN2non (Int.mul_nonneg hN0non hN1non)
  simpa [href] using hfinal

end TinylambOpt.ResearchProofs.Atlas.Goal_MovieCredits_TriangleNorm
