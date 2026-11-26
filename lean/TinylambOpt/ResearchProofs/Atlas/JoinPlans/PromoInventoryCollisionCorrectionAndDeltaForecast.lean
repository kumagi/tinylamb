import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `PromoInventory.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
- `PromoInventory.DeltaForecast` — `∀ n db delta b table (t : Nat), (∀ r x y, r ≠ table
  → delta r x y = 0) → let eval := fun u => reference pattern n (slope db delta u) b
  eval t = forecast eval (pattern.atoms.countP (fun a => a.rel == table)) t`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.PromoInventoryCollisionCorrectionAndDeltaForecast

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.PromoInventory
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total dot)
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Samples (choose)

/-! Finite-difference (Newton forward) helpers. -/

private def dd (f : Int → Int) : Int → Int := fun t => f (t + 1) - f t

private theorem diff_dd (d : Nat) (f : Int → Int) (t : Int) :
    diff d (dd f) t = diff (d + 1) f t := by
  induction d generalizing t with
  | zero => rfl
  | succ d ih =>
      simp only [diff, Nat.succ_eq_add_one]
      rw [ih (t + 1), ih t]
      rfl

private theorem diff_add (d : Nat) (f g : Int → Int) (t : Int) :
    diff d (fun u => f u + g u) t = diff d f t + diff d g t := by
  induction d generalizing t with
  | zero => rfl
  | succ d ih =>
      simp only [diff, Nat.succ_eq_add_one]
      rw [ih (t + 1), ih t]
      grind

private theorem diff_const_mul (d : Nat) (c : Int) (f : Int → Int) (t : Int) :
    diff d (fun u => c * f u) t = c * diff d f t := by
  induction d generalizing t with
  | zero => rfl
  | succ d ih =>
      simp only [diff, Nat.succ_eq_add_one]
      rw [ih (t + 1), ih t]
      grind

private theorem diff_shift (d : Nat) (f : Int → Int) (t : Int) :
    diff d (fun u => f (u + 1)) t = diff d f (t + 1) := by
  induction d generalizing t with
  | zero => rfl
  | succ d ih =>
      simp only [diff, Nat.succ_eq_add_one]
      rw [ih (t + 1), ih t]

private theorem choose_zero_right (t : Nat) : choose t 0 = 1 := by
  cases t <;> rfl

private theorem choose_zero_succ (k : Nat) : choose 0 (k + 1) = 0 := by
  cases k <;> rfl

private theorem choose_succ_succ (t k : Nat) :
    choose (t + 1) (k + 1) = choose t k + choose t (k + 1) := by
  rfl

private abbrev deg_le (d : Nat) (f : Int → Int) : Prop := ∀ t : Int, diff (d + 1) f t = 0

private theorem deg_le_add {d : Nat} {f g : Int → Int} (hf : deg_le d f) (hg : deg_le d g) :
    deg_le d (fun u => f u + g u) := by
  intro t
  rw [diff_add, hf t, hg t]
  simp

private theorem deg_le_const_mul {d : Nat} (c : Int) {f : Int → Int} (hf : deg_le d f) :
    deg_le d (fun u => c * f u) := by
  intro t
  rw [diff_const_mul, hf t]
  simp

private theorem deg_le_shift {d : Nat} {f : Int → Int} (hf : deg_le d f) :
    deg_le d (fun u => f (u + 1)) := by
  intro t
  rw [diff_shift, hf]

private theorem deg_le_succ {d : Nat} {f : Int → Int} (hf : deg_le d f) : deg_le (d + 1) f := by
  intro t
  change diff (d + 1) f (t + 1) - diff (d + 1) f t = 0
  rw [hf (t + 1), hf t]
  simp

private theorem deg_le_affine_mul (d : Nat) (A B : Int) (g : Int → Int) (hg : deg_le d g) :
    deg_le (d + 1) (fun u => (A + u * B) * g u) := by
  induction d generalizing g with
  | zero =>
      intro t
      have hzero : dd g = fun _ => (0 : Int) := by
        funext u
        have hu := hg u
        simp only [deg_le, diff, Nat.succ_eq_add_one] at hu
        dsimp [dd]
        grind
      have hrec : dd (fun u => (A + u * B) * g u) = fun u => B * g (u + 1) := by
        funext u
        dsimp [dd]
        have hgu := congrFun hzero u
        dsimp [dd] at hgu
        rw [show (A + (u + 1) * B) * g (u + 1) - (A + u * B) * g u =
            (A + u * B) * (g (u + 1) - g u) + B * g (u + 1) by grind]
        rw [hgu]
        grind
      rw [show diff 2 (fun u => (A + u * B) * g u) t =
          diff 1 (dd (fun u => (A + u * B) * g u)) t by
            rw [diff_dd]]
      rw [hrec]
      rw [diff_const_mul]
      have hsh := deg_le_shift (d := 0) (f := g) hg
      rw [hsh t]
      simp
  | succ d ih =>
      have hdd : deg_le d (dd g) := by
        intro t
        rw [diff_dd]
        exact hg t
      have hmul : deg_le (d + 1) (fun u => (A + u * B) * dd g u) := ih (dd g) hdd
      have hsh : deg_le (d + 1) (fun u => B * g (u + 1)) :=
        deg_le_const_mul B (deg_le_shift (d := d + 1) (f := g) hg)
      have hrec : dd (fun u => (A + u * B) * g u) =
          fun u => (A + u * B) * dd g u + B * g (u + 1) := by
        funext u
        dsimp [dd]
        grind
      intro t
      rw [show diff (Nat.succ d + 2) (fun u => (A + u * B) * g u) t =
          diff (Nat.succ d + 1) (dd (fun u => (A + u * B) * g u)) t by
            rw [diff_dd]]
      rw [hrec]
      exact deg_le_add hmul hsh t

private theorem diff_zero_fun (d : Nat) (t : Int) : diff d (fun _ : Int => (0 : Int)) t = 0 := by
  induction d generalizing t with
  | zero => rfl
  | succ d ih =>
      change diff d (fun _ : Int => (0 : Int)) (t + 1) -
        diff d (fun _ : Int => (0 : Int)) t = 0
      rw [ih (t + 1), ih t]
      omega

private theorem sum_zero_map {α : Type} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simpa using ih

private theorem diff_list_sum (d : Nat) {α : Type} (l : List α) (f : α → Int → Int) (t : Int) :
    diff d (fun u => (l.map (fun x => f x u)).sum) t =
      (l.map (fun x => diff d (f x) t)).sum := by
  induction l with
  | nil => simpa using diff_zero_fun d t
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [diff_add, ih]

private theorem deg_le_integrate (d : Nat) (n : Nat) (vs : List Nat) (b : Bind)
    (f : Bind → Int → Int) (hf : ∀ x, deg_le d (f x)) :
    deg_le d (fun u => integrate n vs b (fun x => f x u)) := by
  induction vs generalizing b with
  | nil => exact hf b
  | cons v vs ih =>
      intro t
      simp only [integrate]
      rw [diff_list_sum]
      have hz : (List.range n).map
          (fun x => diff (d + 1)
            (fun u => integrate n vs (put b v x) (fun y => f y u)) t) =
          (List.range n).map (fun _ => (0 : Int)) := by
        apply List.map_congr_left
        intro x hx
        exact ih (put b v x) t
      rw [hz]
      exact sum_zero_map (List.range n)

private theorem deg_le_prod_atoms (n : Nat) (db delta : DB) (b : Bind) (table : Nat)
    (hdelta : ∀ r x y, r ≠ table → delta r x y = 0) (atoms : List Atom) (x : Bind)
    (d : Nat) (acc : Int → Int) (hacc : deg_le d acc) :
    deg_le (d + atoms.countP (fun a => a.rel == table))
      (fun u => acc u * (atoms.map (fun a => slope db delta u a.rel (x a.src) (x a.dst))).prod) := by
  induction atoms generalizing d acc with
  | nil =>
      simpa using hacc
  | cons a atoms ih =>
      by_cases ha : a.rel == table
      · have hval : (fun u : Int => slope db delta u a.rel (x a.src) (x a.dst)) =
            (fun u => db a.rel (x a.src) (x a.dst) + u * delta a.rel (x a.src) (x a.dst)) := by
          funext u
          rfl
        have haff := deg_le_affine_mul d (db a.rel (x a.src) (x a.dst))
          (delta a.rel (x a.src) (x a.dst)) acc hacc
        have heq : (fun u : Int =>
            (db a.rel (x a.src) (x a.dst) + u * delta a.rel (x a.src) (x a.dst)) * acc u) =
            (fun u => acc u * slope db delta u a.rel (x a.src) (x a.dst)) := by
          funext u
          simp [slope]
          grind
        rw [heq] at haff
        have hi := ih (d + 1) (fun u => acc u * slope db delta u a.rel (x a.src) (x a.dst)) haff
        simpa [List.countP_cons, ha, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm,
          List.prod_cons, Int.mul_assoc] using hi
      · have hrel : a.rel ≠ table := by
          intro h
          apply ha
          simp [h]
        have hzero : delta a.rel (x a.src) (x a.dst) = 0 := hdelta _ _ _ hrel
        have hconst : (fun u : Int => slope db delta u a.rel (x a.src) (x a.dst)) =
            (fun _ => db a.rel (x a.src) (x a.dst)) := by
          funext u
          simp [slope, hzero]
        have hc := deg_le_const_mul (db a.rel (x a.src) (x a.dst)) hacc
        have heq : (fun u : Int => db a.rel (x a.src) (x a.dst) * acc u) =
            (fun u => acc u * slope db delta u a.rel (x a.src) (x a.dst)) := by
          funext u
          simp [slope, hzero]
          grind
        rw [heq] at hc
        have hi := ih d (fun u => acc u * slope db delta u a.rel (x a.src) (x a.dst)) hc
        simpa [List.countP_cons, ha, List.prod_cons, Int.mul_assoc] using hi

private theorem choose_zero_eq (k : Nat) : choose 0 k = if k = 0 then 1 else 0 := by
  cases k with
  | zero => rfl
  | succ k => simp [choose_zero_succ k]

private theorem sum_range_if_zero (d : Nat) (c : Int) :
    ((List.range (d + 1)).map (fun k => if k = 0 then c else 0)).sum = c := by
  induction d with
  | zero => simp
  | succ d ih =>
      rw [show d + 1 + 1 = (d + 1) + 1 by omega, List.range_succ]
      simp only [List.map_append, List.sum_append, List.map_cons, List.map_nil,
        List.sum_cons, List.sum_nil]
      rw [ih]
      simp

private theorem forecast_zero (f : Int → Int) (d : Nat) : forecast f d 0 = f 0 := by
  unfold forecast
  rw [show (List.range (d + 1)).map (fun k => (choose 0 k : Int) * diff k f 0) =
      (List.range (d + 1)).map (fun k => if k = 0 then diff 0 f 0 else 0) by
        apply List.map_congr_left
        intro k hk
        rw [choose_zero_eq]
        by_cases h : k = 0 <;> simp [h]]
  rw [sum_range_if_zero]
  rfl

private theorem forecast_zero_degree (f : Int → Int) (t : Nat) : forecast f 0 t = f 0 := by
  unfold forecast
  simp only [List.range_succ, List.range_zero, List.map_append, List.map_cons, List.map_nil,
    List.sum_append, List.sum_cons, List.sum_nil, Int.zero_add, Int.add_zero]
  rw [choose_zero_right t]
  simp [diff]

private theorem forecast_step (f : Int → Int) (d : Nat) (t : Nat) :
    forecast f (d + 1) (t + 1) = forecast f (d + 1) t + forecast (dd f) d t := by
  induction d generalizing f t with
  | zero =>
      unfold forecast
      simp only [Nat.zero_add, List.range_succ, List.map_append, List.map_cons, List.map_nil,
        List.sum_append, List.sum_cons, List.sum_nil, List.range_zero, Int.zero_add, Int.add_zero]
      change (choose (t + 1) 0 : Int) * diff 0 f 0 +
          (choose (t + 1) 1 : Int) * diff 1 f 0 =
        ((choose t 0 : Int) * diff 0 f 0 + (choose t 1 : Int) * diff 1 f 0) +
          (choose t 0 : Int) * diff 0 (dd f) 0
      have hc0 : (choose t 0 : Int) = 1 := by simp [choose_zero_right]
      have hc0' : (choose (t + 1) 0 : Int) = 1 := by simp [choose_zero_right]
      have hc1 : choose (t + 1) 1 = choose t 0 + choose t 1 := rfl
      have hd : diff 0 (dd f) 0 = diff 1 f 0 := by rfl
      rw [hc0, hc0', hc1, hd]
      simp
      grind
  | succ d ih =>
      unfold forecast
      simp only [List.range_succ, List.map_append, List.map_cons, List.map_nil,
        List.sum_append, List.sum_cons, List.sum_nil]
      have h := ih (f := f) (t := t)
      unfold forecast at h
      simp only [List.range_succ, List.map_append, List.map_cons, List.map_nil,
        List.sum_append, List.sum_cons, List.sum_nil] at h
      rw [h]
      rw [show choose (t + 1) (d + 2) = choose t (d + 1) + choose t (d + 2) by rfl]
      rw [show diff (d + 1) (dd f) 0 = diff (d + 2) f 0 by rw [diff_dd]]
      grind

private theorem forecast_eq_of_deg_le (d : Nat) (f : Int → Int) (hf : deg_le d f) :
    ∀ t : Nat, forecast f d t = f (t : Int) := by
  induction d generalizing f with
  | zero =>
      intro t
      have hconst : f (t : Int) = f 0 := by
        induction t with
        | zero => rfl
        | succ t iht =>
            have hu := hf (t : Int)
            simp only [deg_le, diff, Nat.zero_add] at hu
            rw [show ((t + 1 : Nat) : Int) = (t : Int) + 1 by simp]
            grind
      rw [hconst]
      exact forecast_zero_degree f t
  | succ d ih =>
      intro t
      induction t with
      | zero =>
          simpa using forecast_zero f (d + 1)
      | succ t iht =>
          rw [forecast_step f d t]
          have hdd : deg_le d (dd f) := by
            intro u
            rw [diff_dd]
            exact hf u
          rw [ih (dd f) hdd t]
          rw [iht]
          have hcast : ((t + 1 : Nat) : Int) = (t : Int) + 1 := by simp
          rw [hcast]
          dsimp [dd]
          grind

theorem sound_DeltaForecast : DeltaForecast.Goal := by
  intro n db delta b table t hdelta
  let eval : Int → Int := fun u => reference pattern n (slope db delta u) b
  let k : Nat := pattern.atoms.countP (fun a => a.rel == table)
  have hdeg : deg_le k eval := by
    dsimp [eval, reference]
    apply deg_le_integrate
    intro x
    simpa [k, value] using
      (deg_le_prod_atoms n db delta b table hdelta pattern.atoms x 0
        (fun _ => (1 : Int)) (by intro t; simp [deg_le, diff]))
  exact (forecast_eq_of_deg_le k eval hdeg t).symm

/-! Möbius inversion for the four free variables of the PromoInventory pattern. -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi))]
      rw [h n (Nat.lt_succ_self n)]

private def F (db : DB) (a b c d : Nat) : Int :=
  db 0 c a * (db 0 a b * (db 0 b d * (db 0 d c * (db 2 c b * db 1 a d))))

private theorem hc_test (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b ([[1], [2], [0], [3]] : List (List Nat)) =
      rsum n (fun a => rsum n (fun b' => rsum n (fun c => rsum n (fun d =>
        F db a b' c d)))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F]

private theorem rsum_zero (n : Nat) : rsum n (fun _ => (0 : Int)) = 0 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, ih]; simp

private theorem rsum_ite (n : Nat) (c : Bool) (f g : Nat → Int) :
    rsum n (fun x => if c then f x else g x) = if c then rsum n f else rsum n g := by
  cases c <;> simp [rsum_zero]

private def rsum2 (n : Nat) (f : Nat → Nat → Int) : Int :=
  rsum n (fun a => rsum n (fun b => f a b))

private def rsum3 (n : Nat) (f : Nat → Nat → Nat → Int) : Int :=
  rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c)))

private def rsum4 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) : Int :=
  rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => f a b c d))))

private theorem hinj (n : Nat) (db : DB) (b : Bind) :
    injective pattern n db b =
      rsum4 n (fun a b' c d => if [a, b', c, d].Nodup then F db a b' c d else 0) := by
  simp [injective, pattern, integrate, value, put, rsum, rsum4, F]

private theorem rsum2_ite (n : Nat) (c : Bool) (f g : Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => if c then f a b else g a b)) =
      if c then rsum n (fun a => rsum n (fun b => f a b))
           else rsum n (fun a => rsum n (fun b => g a b)) := by
  rw [show (fun a => rsum n (fun b => if c then f a b else g a b)) =
      (fun a => if c then rsum n (fun b => f a b) else rsum n (fun b => g a b)) by
        funext a
        rw [rsum_ite]]
  cases c <;> simp [rsum_zero]

private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_sub (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i - g i) = rsum n f - rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [show rsum 0 (fun i => rsum m (fun j => f i j)) = (0 : Int) by simp [rsum]]
      rw [show rsum m (fun j => rsum 0 (fun i => f i j)) = rsum m (fun _ => (0 : Int)) by
        apply rsum_congr
        intro j hj
        simp [rsum]]
      exact (rsum_zero m).symm
  | succ n ih =>
      rw [rsum_succ, ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr
        intro j hj
        rw [rsum_succ]]
      rw [rsum_add]

private theorem rsum_const_mul (n : Nat) (k : Int) (f : Nat → Int) :
    rsum n (fun i => k * f i) = k * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.mul_add]

private theorem rsum4_add (n : Nat) (f g : Nat → Nat → Nat → Nat → Int) :
    rsum4 n (fun a b c d => f a b c d + g a b c d) = rsum4 n f + rsum4 n g := by
  unfold rsum4
  rw [show (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => f a b c d + g a b c d)))) =
      (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => f a b c d))) +
                rsum n (fun b => rsum n (fun c => rsum n (fun d => g a b c d))) ) by
        funext a
        rw [show (fun b => rsum n (fun c => rsum n (fun d => f a b c d + g a b c d))) =
            (fun b => rsum n (fun c => rsum n (fun d => f a b c d)) +
                      rsum n (fun c => rsum n (fun d => g a b c d)) ) by
              funext b
              rw [show (fun c => rsum n (fun d => f a b c d + g a b c d)) =
                  (fun c => rsum n (fun d => f a b c d) + rsum n (fun d => g a b c d)) by
                    funext c
                    rw [rsum_add]]
              rw [rsum_add]]
        rw [rsum_add]]
  rw [rsum_add]

private theorem rsum4_sub (n : Nat) (f g : Nat → Nat → Nat → Nat → Int) :
    rsum4 n (fun a b c d => f a b c d - g a b c d) = rsum4 n f - rsum4 n g := by
  unfold rsum4
  rw [show (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => f a b c d - g a b c d)))) =
      (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => f a b c d))) -
                rsum n (fun b => rsum n (fun c => rsum n (fun d => g a b c d))) ) by
        funext a
        rw [show (fun b => rsum n (fun c => rsum n (fun d => f a b c d - g a b c d))) =
            (fun b => rsum n (fun c => rsum n (fun d => f a b c d)) -
                      rsum n (fun c => rsum n (fun d => g a b c d)) ) by
              funext b
              rw [show (fun c => rsum n (fun d => f a b c d - g a b c d)) =
                  (fun c => rsum n (fun d => f a b c d) - rsum n (fun d => g a b c d)) by
                    funext c
                    rw [rsum_sub]]
              rw [rsum_sub]]
        rw [rsum_sub]]
  rw [rsum_sub]

private theorem rsum4_const_mul (n : Nat) (k : Int) (f : Nat → Nat → Nat → Nat → Int) :
    rsum4 n (fun a b c d => k * f a b c d) = k * rsum4 n f := by
  unfold rsum4
  rw [show (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => k * f a b c d)))) =
      (fun a => k * rsum n (fun b => rsum n (fun c => rsum n (fun d => f a b c d))) ) by
        funext a
        rw [show (fun b => rsum n (fun c => rsum n (fun d => k * f a b c d))) =
            (fun b => k * rsum n (fun c => rsum n (fun d => f a b c d)) ) by
              funext b
              rw [show (fun c => rsum n (fun d => k * f a b c d)) =
                  (fun c => k * rsum n (fun d => f a b c d)) by
                    funext c
                    rw [rsum_const_mul]]
              rw [rsum_const_mul]]
        rw [rsum_const_mul]]
  rw [rsum_const_mul]

private theorem rsum_zero_of_false (n a : Nat) (g : Nat → Int)
    (h : ∀ x, x < n → (x == a) = false) :
    rsum n (fun x => if x == a then g x else 0) = 0 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have hn : (n == a) = false := h n (Nat.lt_succ_self n)
      rw [ih (fun x hx => h x (Nat.lt_succ_of_lt hx)), hn]
      simp

private theorem rsum_eq_single (n a : Nat) (ha : a < n) (g : Nat → Int) :
    rsum n (fun x => if x == a then g x else 0) = g a := by
  induction n with
  | zero => simp at ha
  | succ n ih =>
      rw [rsum_succ]
      by_cases h : n == a
      · have hna : n = a := Nat.eq_of_beq_eq_true h
        subst n
        rw [show rsum a (fun x => if x == a then g x else 0) = 0 by
          apply rsum_zero_of_false
          intro x hx
          have hxa : x ≠ a := by omega
          cases hx' : (x == a) <;> simp_all]
        simp
      · have hne : n ≠ a := by intro hn; subst n; simp at h
        have ha' : a < n := by omega
        rw [ih ha']
        simp [h]

private theorem rsum_ite_prop (n : Nat) (p : Prop) [Decidable p] (f g : Nat → Int) :
    rsum n (fun x => if p then f x else g x) = if p then rsum n f else rsum n g := by
  by_cases hp : p <;> simp [hp, rsum_zero]

private theorem rsum2_ite_prop (n : Nat) (p : Prop) [Decidable p] (f g : Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => if p then f a b else g a b)) =
      if p then rsum n (fun a => rsum n (fun b => f a b))
           else rsum n (fun a => rsum n (fun b => g a b)) := by
  rw [show (fun a => rsum n (fun b => if p then f a b else g a b)) =
      (fun a => if p then rsum n (fun b => f a b) else rsum n (fun b => g a b)) by
        funext a
        rw [rsum_ite_prop]]
  by_cases hp : p <;> simp [hp, rsum_zero]

private theorem rsum_eq_single_prop (n a : Nat) (ha : a < n) (g : Nat → Int) :
    rsum n (fun x => if x = a then g x else 0) = g a := by
  rw [show (fun x => if x = a then g x else 0) = (fun x => if x == a then g x else 0) by
    funext x
    by_cases hxa : x = a
    · subst x
      simp
    · have h' : (x == a) = false := by
        cases h'' : (x == a) <;> simp_all [beq_iff_eq]
      simp [h', hxa]]
  exact rsum_eq_single n a ha g

private theorem diag_ab (n : Nat) (G : Nat → Nat → Nat → Nat → Int) :
    rsum4 n (fun a b c d => if b == a then G a b c d else 0) =
      rsum3 n (fun a c d => G a a c d) := by
  unfold rsum4 rsum3
  apply rsum_congr
  intro a ha
  calc
    rsum n (fun b => rsum n (fun c => rsum n (fun d => if b == a then G a b c d else 0)))
        = rsum n (fun b => if b == a then rsum n (fun c => rsum n (fun d => G a b c d)) else 0) := by
            apply rsum_congr
            intro b hb
            rw [rsum2_ite]
            by_cases hba : b == a <;> simp [hba, rsum_zero]
    _ = rsum n (fun c => rsum n (fun d => G a a c d)) := by
            rw [rsum_eq_single n a ha]

private theorem diag_ac (n : Nat) (G : Nat → Nat → Nat → Nat → Int) :
    rsum4 n (fun a b c d => if c == a then G a b c d else 0) =
      rsum3 n (fun a b d => G a b a d) := by
  unfold rsum4 rsum3
  apply rsum_congr
  intro a ha
  apply rsum_congr
  intro b hb
  calc
    rsum n (fun c => rsum n (fun d => if c == a then G a b c d else 0))
        = rsum n (fun c => if c == a then rsum n (fun d => G a b c d) else 0) := by
            apply rsum_congr
            intro c hc
            rw [rsum_ite]
            rw [rsum_zero]
    _ = rsum n (fun d => G a b a d) := by
            rw [rsum_eq_single n a ha]

private theorem diag_ad (n : Nat) (G : Nat → Nat → Nat → Nat → Int) :
    rsum4 n (fun a b c d => if d == a then G a b c d else 0) =
      rsum3 n (fun a b c => G a b c a) := by
  unfold rsum4 rsum3
  apply rsum_congr
  intro a ha
  apply rsum_congr
  intro b hb
  apply rsum_congr
  intro c hc
  rw [rsum_eq_single n a ha]

private theorem diag_bc (n : Nat) (G : Nat → Nat → Nat → Nat → Int) :
    rsum4 n (fun a b c d => if c == b then G a b c d else 0) =
      rsum3 n (fun a b d => G a b b d) := by
  unfold rsum4 rsum3
  apply rsum_congr
  intro a ha
  apply rsum_congr
  intro b hb
  calc
    rsum n (fun c => rsum n (fun d => if c == b then G a b c d else 0))
        = rsum n (fun c => if c == b then rsum n (fun d => G a b c d) else 0) := by
            apply rsum_congr
            intro c hc
            rw [rsum_ite]
            rw [rsum_zero]
    _ = rsum n (fun d => G a b b d) := by
            rw [rsum_eq_single n b hb]

private theorem diag_bd (n : Nat) (G : Nat → Nat → Nat → Nat → Int) :
    rsum4 n (fun a b c d => if d == b then G a b c d else 0) =
      rsum3 n (fun a b c => G a b c b) := by
  unfold rsum4 rsum3
  apply rsum_congr
  intro a ha
  apply rsum_congr
  intro b hb
  apply rsum_congr
  intro c hc
  rw [rsum_eq_single n b hb]

private theorem diag_cd (n : Nat) (G : Nat → Nat → Nat → Nat → Int) :
    rsum4 n (fun a b c d => if d == c then G a b c d else 0) =
      rsum3 n (fun a b c => G a b c c) := by
  unfold rsum4 rsum3
  apply rsum_congr
  intro a ha
  apply rsum_congr
  intro b hb
  apply rsum_congr
  intro c hc
  rw [rsum_eq_single n c hc]

private theorem diag_bc_ad (n : Nat) (G : Nat → Nat → Nat → Nat → Int) :
    rsum4 n (fun a b c d => if c == b && d == a then G a b c d else 0) =
      rsum2 n (fun a b => G a b b a) := by
  unfold rsum4 rsum2
  apply rsum_congr
  intro a ha
  apply rsum_congr
  intro b hb
  calc
    rsum n (fun c => rsum n (fun d => if c == b && d == a then G a b c d else 0))
        = rsum n (fun c => if c == b then rsum n (fun d => if d == a then G a b c d else 0) else 0) := by
            apply rsum_congr
            intro c hc
            by_cases hcb : c == b
            · simp [hcb]
            · have hz : rsum n (fun d => if c == b && d == a then G a b c d else 0) = 0 := by
                rw [show (fun d => if c == b && d == a then G a b c d else 0) = (fun _ => (0 : Int)) by
                  funext d
                  simp [hcb]]
                exact rsum_zero n
              rw [hz]
              simp [hcb]
    _ = rsum n (fun c => if c == b then G a b c a else 0) := by
            apply rsum_congr
            intro c hc
            rw [rsum_eq_single n a ha]
    _ = G a b b a := by
            rw [rsum_eq_single n b hb]

private theorem diag_ac_bd (n : Nat) (G : Nat → Nat → Nat → Nat → Int) :
    rsum4 n (fun a b c d => if c == a && d == b then G a b c d else 0) =
      rsum2 n (fun a b => G a b a b) := by
  unfold rsum4 rsum2
  apply rsum_congr
  intro a ha
  apply rsum_congr
  intro b hb
  calc
    rsum n (fun c => rsum n (fun d => if c == a && d == b then G a b c d else 0))
        = rsum n (fun c => if c == a then rsum n (fun d => if d == b then G a b c d else 0) else 0) := by
            apply rsum_congr
            intro c hc
            by_cases hca : c == a
            · simp [hca]
            · have hz : rsum n (fun d => if c == a && d == b then G a b c d else 0) = 0 := by
                rw [show (fun d => if c == a && d == b then G a b c d else 0) = (fun _ => (0 : Int)) by
                  funext d
                  simp [hca]]
                exact rsum_zero n
              rw [hz]
              simp [hca]
    _ = rsum n (fun c => if c == a then G a b c b else 0) := by
            apply rsum_congr
            intro c hc
            rw [rsum_eq_single n b hb]
    _ = G a b a b := by
            rw [rsum_eq_single n a ha]

private theorem diag_ab_cd (n : Nat) (G : Nat → Nat → Nat → Nat → Int) :
    rsum4 n (fun a b c d => if b == a && d == c then G a b c d else 0) =
      rsum2 n (fun a c => G a a c c) := by
  unfold rsum4 rsum2
  apply rsum_congr
  intro a ha
  calc
    rsum n (fun b => rsum n (fun c => rsum n (fun d => if b == a && d == c then G a b c d else 0)))
        = rsum n (fun b => if b == a then rsum n (fun c => rsum n (fun d => if d == c then G a b c d else 0)) else 0) := by
            apply rsum_congr
            intro b hb
            by_cases hba : b == a
            · simp [hba]
            · have hz : rsum n (fun c => rsum n (fun d => if b == a && d == c then G a b c d else 0)) = 0 := by
                rw [show (fun c => rsum n (fun d => if b == a && d == c then G a b c d else 0)) = (fun _ => (0 : Int)) by
                  funext c
                  rw [show (fun d => if b == a && d == c then G a b c d else 0) = (fun _ => (0 : Int)) by
                    funext d
                    simp [hba]]
                  exact rsum_zero n]
                exact rsum_zero n
              rw [hz]
              simp [hba]
    _ = rsum n (fun b => if b == a then rsum n (fun c => G a b c c) else 0) := by
            apply rsum_congr
            intro b hb
            by_cases hba : b == a
            · simp [hba]
              apply rsum_congr
              intro c hc
              rw [rsum_eq_single_prop n c hc]
            · simp [hba]
    _ = rsum n (fun c => G a a c c) := by
            rw [rsum_eq_single n a ha]

private theorem diag_abc (n : Nat) (G : Nat → Nat → Nat → Nat → Int) :
    rsum4 n (fun a b c d => if b == a && c == a then G a b c d else 0) =
      rsum2 n (fun a d => G a a a d) := by
  unfold rsum4
  apply rsum_congr
  intro a ha
  calc
    rsum n (fun b => rsum n (fun c => rsum n (fun d => if b == a && c == a then G a b c d else 0)))
        = rsum n (fun b => if b == a then rsum n (fun c => rsum n (fun d => if c == a then G a b c d else 0)) else 0) := by
            apply rsum_congr
            intro b hb
            by_cases hba : b == a
            · simp [hba]
            · have hz : rsum n (fun c => rsum n (fun d => if b == a && c == a then G a b c d else 0)) = 0 := by
                rw [show (fun c => rsum n (fun d => if b == a && c == a then G a b c d else 0)) = (fun _ => (0 : Int)) by
                  funext c
                  rw [show (fun d => if b == a && c == a then G a b c d else 0) = (fun _ => (0 : Int)) by
                    funext d
                    simp [hba]]
                  exact rsum_zero n]
                exact rsum_zero n
              rw [hz]
              simp [hba]
    _ = rsum n (fun b => if b == a then rsum n (fun d => G a b a d) else 0) := by
            apply rsum_congr
            intro b hb
            by_cases hba : b == a
            case pos =>
              simp [hba]
              rw [show (fun c => rsum n (fun d => if c = a then G a b c d else 0)) =
                  (fun c => if c = a then rsum n (fun d => G a b c d) else 0) by
                funext c
                rw [rsum_ite_prop, rsum_zero]]
              rw [rsum_eq_single_prop n a ha]
            case neg =>
              simp [hba]
    _ = rsum n (fun d => G a a a d) := by
            rw [rsum_eq_single n a ha]

private theorem diag_bcd (n : Nat) (G : Nat → Nat → Nat → Nat → Int) :
    rsum4 n (fun a b c d => if c == b && d == b then G a b c d else 0) =
      rsum2 n (fun a b => G a b b b) := by
  unfold rsum4
  apply rsum_congr
  intro a ha
  apply rsum_congr
  intro b hb
  calc
    rsum n (fun c => rsum n (fun d => if c == b && d == b then G a b c d else 0))
        = rsum n (fun c => if c == b then rsum n (fun d => if d == b then G a b c d else 0) else 0) := by
            apply rsum_congr
            intro c hc
            by_cases hcb : c == b
            · simp [hcb]
            · have hz : rsum n (fun d => if c == b && d == b then G a b c d else 0) = 0 := by
                rw [show (fun d => if c == b && d == b then G a b c d else 0) = (fun _ => (0 : Int)) by
                  funext d
                  simp [hcb]]
                exact rsum_zero n
              rw [hz]
              simp [hcb]
    _ = rsum n (fun c => if c == b then G a b c b else 0) := by
            apply rsum_congr
            intro c hc
            rw [rsum_eq_single n b hb]
    _ = G a b b b := by
            rw [rsum_eq_single n b hb]

private theorem diag_acd (n : Nat) (G : Nat → Nat → Nat → Nat → Int) :
    rsum4 n (fun a b c d => if c == a && d == a then G a b c d else 0) =
      rsum2 n (fun a b => G a b a a) := by
  unfold rsum4
  apply rsum_congr
  intro a ha
  apply rsum_congr
  intro b hb
  calc
    rsum n (fun c => rsum n (fun d => if c == a && d == a then G a b c d else 0))
        = rsum n (fun c => if c == a then rsum n (fun d => if d == a then G a b c d else 0) else 0) := by
            apply rsum_congr
            intro c hc
            by_cases hca : c == a
            · simp [hca]
            · have hz : rsum n (fun d => if c == a && d == a then G a b c d else 0) = 0 := by
                rw [show (fun d => if c == a && d == a then G a b c d else 0) = (fun _ => (0 : Int)) by
                  funext d
                  simp [hca]]
                exact rsum_zero n
              rw [hz]
              simp [hca]
    _ = rsum n (fun c => if c == a then G a b c a else 0) := by
            apply rsum_congr
            intro c hc
            rw [rsum_eq_single n a ha]
    _ = G a b a a := by
            rw [rsum_eq_single n a ha]

private theorem diag_abd (n : Nat) (G : Nat → Nat → Nat → Nat → Int) :
    rsum4 n (fun a b c d => if b == a && d == a then G a b c d else 0) =
      rsum2 n (fun a c => G a a c a) := by
  unfold rsum4
  apply rsum_congr
  intro a ha
  calc
    rsum n (fun b => rsum n (fun c => rsum n (fun d => if b == a && d == a then G a b c d else 0)))
        = rsum n (fun b => if b == a then rsum n (fun c => rsum n (fun d => if d == a then G a b c d else 0)) else 0) := by
            apply rsum_congr
            intro b hb
            by_cases hba : b == a
            · simp [hba]
            · have hz : rsum n (fun c => rsum n (fun d => if b == a && d == a then G a b c d else 0)) = 0 := by
                rw [show (fun c => rsum n (fun d => if b == a && d == a then G a b c d else 0)) = (fun _ => (0 : Int)) by
                  funext c
                  rw [show (fun d => if b == a && d == a then G a b c d else 0) = (fun _ => (0 : Int)) by
                    funext d
                    simp [hba]]
                  exact rsum_zero n]
                exact rsum_zero n
              rw [hz]
              simp [hba]
    _ = rsum n (fun b => if b == a then rsum n (fun c => G a b c a) else 0) := by
            apply rsum_congr
            intro b hb
            by_cases hba : b == a
            · simp [hba]
              apply rsum_congr
              intro c hc
              rw [rsum_eq_single_prop n a ha]
            · simp [hba]
    _ = rsum n (fun c => G a a c a) := by
            rw [rsum_eq_single n a ha]

private theorem diag_all (n : Nat) (G : Nat → Nat → Nat → Nat → Int) :
    rsum4 n (fun a b c d => if b == a && c == a && d == a then G a b c d else 0) =
      rsum n (fun a => G a a a a) := by
  unfold rsum4
  apply rsum_congr
  intro a ha
  calc
    rsum n (fun b => rsum n (fun c => rsum n (fun d =>
        if b == a && c == a && d == a then G a b c d else 0)))
        = rsum n (fun b => if b == a then rsum n (fun c => rsum n (fun d =>
            if c == a && d == a then G a b c d else 0)) else 0) := by
            apply rsum_congr
            intro b hb
            by_cases hba : b == a
            · simp [hba]
            · have hz : rsum n (fun c => rsum n (fun d =>
                  if b == a && c == a && d == a then G a b c d else 0)) = 0 := by
                rw [show (fun c => rsum n (fun d =>
                      if b == a && c == a && d == a then G a b c d else 0)) = (fun _ => (0 : Int)) by
                  funext c
                  rw [show (fun d => if b == a && c == a && d == a then G a b c d else 0) = (fun _ => (0 : Int)) by
                    funext d
                    simp [hba]]
                  exact rsum_zero n]
                exact rsum_zero n
              rw [hz]
              simp [hba]
    _ = rsum n (fun b => if b == a then G a b a a else 0) := by
            apply rsum_congr
            intro b hb
            by_cases hba : b == a
            · simp [hba]
              rw [show (fun c => rsum n (fun d => if c = a ∧ d = a then G a b c d else 0)) =
                  (fun c => if c = a then rsum n (fun d => if d = a then G a b c d else 0) else 0) by
                funext c
                by_cases hca : c = a <;> simp [hca, rsum_zero]]
              rw [show (fun c => if c = a then rsum n (fun d => if d = a then G a b c d else 0) else 0) =
                  (fun c => if c = a then G a b c a else 0) by
                funext c
                by_cases hca : c = a <;> simp [hca, rsum_zero, rsum_eq_single_prop n a ha]]
              rw [rsum_eq_single_prop n a ha]
            · simp [hba]
    _ = G a a a a := by
            rw [rsum_eq_single n a ha]

private theorem c1 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b ([[1], [2], [0], [3]] : List (List Nat)) = rsum4 n (F db) := by
  simp [collapsed, pattern, integrate, value, put, rsum, rsum4, F]

private theorem c2 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b ([[1, 2], [0], [3]] : List (List Nat)) =
      rsum3 n (fun a c d => F db a a c d) := by
  simp [collapsed, pattern, integrate, value, put, rsum, rsum3, F]

private theorem c3 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b ([[2], [1, 0], [3]] : List (List Nat)) =
      rsum3 n (fun a b' d => F db a b' a d) := by
  simp [collapsed, pattern, integrate, value, put, rsum, rsum3, F]
  change rsum n (fun b' => rsum n (fun a => rsum n (fun d => F db a b' a d))) =
    rsum n (fun a => rsum n (fun b' => rsum n (fun d => F db a b' a d)))
  rw [rsum_comm]

private theorem c4 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b ([[2], [0], [1, 3]] : List (List Nat)) =
      rsum3 n (fun a b' c => F db a b' c a) := by
  simp [collapsed, pattern, integrate, value, put, rsum, rsum3, F]
  change rsum n (fun b' => rsum n (fun c => rsum n (fun a => F db a b' c a))) =
    rsum n (fun a => rsum n (fun b' => rsum n (fun c => F db a b' c a)))
  rw [rsum_comm]
  rw [show rsum n (fun c => rsum n (fun b' => rsum n (fun a => F db a b' c a))) =
      rsum n (fun c => rsum n (fun a => rsum n (fun b' => F db a b' c a))) by
        apply rsum_congr
        intro c hc
        rw [rsum_comm]]
  rw [rsum_comm]
  apply rsum_congr
  intro a ha
  rw [rsum_comm]

private theorem c5 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b ([[1], [2, 0], [3]] : List (List Nat)) =
      rsum3 n (fun a b' d => F db a b' b' d) := by
  simp [collapsed, pattern, integrate, value, put, rsum, rsum3, F]

private theorem c6 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b ([[1, 2, 0], [3]] : List (List Nat)) =
      rsum2 n (fun a d => F db a a a d) := by
  simp [collapsed, pattern, integrate, value, put, rsum, rsum2, F]

private theorem c7 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b ([[2, 0], [1, 3]] : List (List Nat)) =
      rsum2 n (fun a b' => F db a b' b' a) := by
  simp [collapsed, pattern, integrate, value, put, rsum, rsum2, F]
  change rsum n (fun b' => rsum n (fun a => F db a b' b' a)) =
    rsum n (fun a => rsum n (fun b' => F db a b' b' a))
  rw [rsum_comm]

private theorem c8 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b ([[1], [0], [2, 3]] : List (List Nat)) =
      rsum3 n (fun a b' c => F db a b' c b') := by
  simp [collapsed, pattern, integrate, value, put, rsum, rsum3, F]
  change rsum n (fun a => rsum n (fun c => rsum n (fun b' => F db a b' c b'))) =
    rsum n (fun a => rsum n (fun b' => rsum n (fun c => F db a b' c b')))
  apply rsum_congr
  intro a ha
  rw [rsum_comm]

private theorem c9 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b ([[1, 0], [2, 3]] : List (List Nat)) =
      rsum2 n (fun a b' => F db a b' a b') := by
  simp [collapsed, pattern, integrate, value, put, rsum, rsum2, F]

private theorem c10 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b ([[0], [1, 2, 3]] : List (List Nat)) =
      rsum2 n (fun a c => F db a a c a) := by
  simp [collapsed, pattern, integrate, value, put, rsum, rsum2, F]
  change rsum n (fun c => rsum n (fun a => F db a a c a)) =
    rsum n (fun a => rsum n (fun c => F db a a c a))
  rw [rsum_comm]

private theorem c11 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b ([[1], [2], [0, 3]] : List (List Nat)) =
      rsum3 n (fun a b' c => F db a b' c c) := by
  simp [collapsed, pattern, integrate, value, put, rsum, rsum3, F]

private theorem c12 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b ([[1, 2], [0, 3]] : List (List Nat)) =
      rsum2 n (fun a c => F db a a c c) := by
  simp [collapsed, pattern, integrate, value, put, rsum, rsum2, F]

private theorem c13 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b ([[2], [1, 0, 3]] : List (List Nat)) =
      rsum2 n (fun a b' => F db a b' a a) := by
  simp [collapsed, pattern, integrate, value, put, rsum, rsum2, F]
  change rsum n (fun b' => rsum n (fun a => F db a b' a a)) =
    rsum n (fun a => rsum n (fun b' => F db a b' a a))
  rw [rsum_comm]

private theorem c14 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b ([[1], [2, 0, 3]] : List (List Nat)) =
      rsum2 n (fun a b' => F db a b' b' b') := by
  simp [collapsed, pattern, integrate, value, put, rsum, rsum2, F]

private theorem c15 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b ([[1, 2, 0, 3]] : List (List Nat)) =
      rsum n (fun a => F db a a a a) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F]

private theorem parts_eq : partitions pattern.freeVars =
    ([[[1], [2], [0], [3]], [[1, 2], [0], [3]], [[2], [1, 0], [3]],
      [[2], [0], [1, 3]], [[1], [2, 0], [3]], [[1, 2, 0], [3]],
      [[2, 0], [1, 3]], [[1], [0], [2, 3]], [[1, 0], [2, 3]],
      [[0], [1, 2, 3]], [[1], [2], [0, 3]], [[1, 2], [0, 3]],
      [[2], [1, 0, 3]], [[1], [2, 0, 3]], [[1, 2, 0, 3]]] : List (List (List Nat))) := by
  native_decide

private theorem point_ie (G : Nat → Nat → Nat → Nat → Int) (a b c d : Nat) :
    (if [a, b, c, d].Nodup then G a b c d else 0) =
      G a b c d
      - (if b == a then G a b c d else 0)
      - (if c == a then G a b c d else 0)
      - (if d == a then G a b c d else 0)
      - (if c == b then G a b c d else 0)
      - (if d == b then G a b c d else 0)
      - (if d == c then G a b c d else 0)
      + (if c == b && d == a then G a b c d else 0)
      + (if c == a && d == b then G a b c d else 0)
      + (if b == a && d == c then G a b c d else 0)
      + 2 * (if b == a && c == a then G a b c d else 0)
      + 2 * (if c == b && d == b then G a b c d else 0)
      + 2 * (if c == a && d == a then G a b c d else 0)
      + 2 * (if b == a && d == a then G a b c d else 0)
      - 6 * (if b == a && c == a && d == a then G a b c d else 0) := by
  by_cases hab : b = a <;> by_cases hac : c = a <;> by_cases had : d = a <;>
    by_cases hbc : c = b <;> by_cases hbd : d = b <;> by_cases hcd : d = c <;>
      simp [hab, hac, had, hbc, hbd, hcd, beq_iff_eq, List.nodup_cons] <;> omega

private theorem hie_sum (n : Nat) (db : DB) :
    rsum4 n (fun a b c d => if [a, b, c, d].Nodup then F db a b c d else 0) =
      rsum4 n (F db)
      - rsum3 n (fun a c d => F db a a c d)
      - rsum3 n (fun a b d => F db a b a d)
      - rsum3 n (fun a b c => F db a b c a)
      - rsum3 n (fun a b d => F db a b b d)
      - rsum3 n (fun a b c => F db a b c b)
      - rsum3 n (fun a b c => F db a b c c)
      + rsum2 n (fun a b => F db a b b a)
      + rsum2 n (fun a b => F db a b a b)
      + rsum2 n (fun a c => F db a a c c)
      + 2 * rsum2 n (fun a d => F db a a a d)
      + 2 * rsum2 n (fun a b => F db a b b b)
      + 2 * rsum2 n (fun a b => F db a b a a)
      + 2 * rsum2 n (fun a c => F db a a c a)
      - 6 * rsum n (fun a => F db a a a a) := by
  rw [show (fun a b c d => if [a, b, c, d].Nodup then F db a b c d else 0) =
      (fun a b c d =>
        F db a b c d
        - (if b == a then F db a b c d else 0)
        - (if c == a then F db a b c d else 0)
        - (if d == a then F db a b c d else 0)
        - (if c == b then F db a b c d else 0)
        - (if d == b then F db a b c d else 0)
        - (if d == c then F db a b c d else 0)
        + (if c == b && d == a then F db a b c d else 0)
        + (if c == a && d == b then F db a b c d else 0)
        + (if b == a && d == c then F db a b c d else 0)
        + 2 * (if b == a && c == a then F db a b c d else 0)
        + 2 * (if c == b && d == b then F db a b c d else 0)
        + 2 * (if c == a && d == a then F db a b c d else 0)
        + 2 * (if b == a && d == a then F db a b c d else 0)
        - 6 * (if b == a && c == a && d == a then F db a b c d else 0)) by
        funext a b c d
        exact point_ie (F db) a b c d]
  simp only [rsum4_sub, rsum4_add, rsum4_const_mul]
  rw [diag_ab n (F db), diag_ac n (F db), diag_ad n (F db),
      diag_bc n (F db), diag_bd n (F db), diag_cd n (F db),
      diag_bc_ad n (F db), diag_ac_bd n (F db), diag_ab_cd n (F db),
      diag_abc n (F db), diag_bcd n (F db), diag_acd n (F db),
      diag_abd n (F db), diag_all n (F db)]

theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  rw [hinj, parts_eq]
  simp only [List.map_cons, List.sum_cons, List.map_nil, List.sum_nil]
  rw [hie_sum n db]
  rw [c1 n db b, c2 n db b, c3 n db b, c4 n db b, c5 n db b,
      c6 n db b, c7 n db b, c8 n db b, c9 n db b, c10 n db b,
      c11 n db b, c12 n db b, c13 n db b, c14 n db b, c15 n db b]
  have hm1 : mobius ([[1], [2], [0], [3]] : List (List Nat)) = 1 := by native_decide
  have hm2 : mobius ([[1, 2], [0], [3]] : List (List Nat)) = -1 := by native_decide
  have hm3 : mobius ([[2], [1, 0], [3]] : List (List Nat)) = -1 := by native_decide
  have hm4 : mobius ([[2], [0], [1, 3]] : List (List Nat)) = -1 := by native_decide
  have hm5 : mobius ([[1], [2, 0], [3]] : List (List Nat)) = -1 := by native_decide
  have hm6 : mobius ([[1, 2, 0], [3]] : List (List Nat)) = 2 := by native_decide
  have hm7 : mobius ([[2, 0], [1, 3]] : List (List Nat)) = 1 := by native_decide
  have hm8 : mobius ([[1], [0], [2, 3]] : List (List Nat)) = -1 := by native_decide
  have hm9 : mobius ([[1, 0], [2, 3]] : List (List Nat)) = 1 := by native_decide
  have hm10 : mobius ([[0], [1, 2, 3]] : List (List Nat)) = 2 := by native_decide
  have hm11 : mobius ([[1], [2], [0, 3]] : List (List Nat)) = -1 := by native_decide
  have hm12 : mobius ([[1, 2], [0, 3]] : List (List Nat)) = 1 := by native_decide
  have hm13 : mobius ([[2], [1, 0, 3]] : List (List Nat)) = 2 := by native_decide
  have hm14 : mobius ([[1], [2, 0, 3]] : List (List Nat)) = 2 := by native_decide
  have hm15 : mobius ([[1, 2, 0, 3]] : List (List Nat)) = -6 := by native_decide
  rw [hm1, hm2, hm3, hm4, hm5, hm6, hm7, hm8, hm9, hm10, hm11, hm12, hm13, hm14, hm15]
  omega

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.PromoInventoryCollisionCorrectionAndDeltaForecast
