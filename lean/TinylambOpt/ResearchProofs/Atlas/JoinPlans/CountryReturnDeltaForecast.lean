import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `CountryReturn.DeltaForecast` — `∀ n db delta b table (t : Nat), (∀ r x y, r ≠ table
  → delta r x y = 0) → let eval := fun u => reference pattern n (slope db delta u) b
  eval t = forecast eval (pattern.atoms.countP (fun a => a.rel == table)) t`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

universe u

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.CountryReturnDeltaForecast

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.CountryReturn
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

private theorem sum_zero_map {α : Type u} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
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
      rw [choose_zero_right (t + 1), choose_zero_right t]
      rw [show choose (t + 1) 1 = choose t 0 + choose t 1 by rfl]
      rw [choose_zero_right t]
      rw [show diff 0 (dd f) 0 = diff 1 f 0 by rfl]
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
            change f ((t : Int) + 1) - f (t : Int) = 0 at hu
            have hstep : f ((t + 1 : Nat) : Int) = f (t : Int) := by
              have hu' : f ((t : Int) + 1) = f (t : Int) := by grind
              simpa using hu'
            rw [hstep, iht]
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

/-! Generic list / integration helpers (self-contained copies). -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_zero (f : Nat → Int) : rsum 0 f = 0 := by simp [rsum]

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      rw [ih (fun i hi => h i (Nat.lt_succ_of_lt hi))]
      rw [h n (Nat.lt_succ_self n)]

private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; grind

private theorem rsum_const_zero (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
  induction m with
  | zero => simp [rsum]
  | succ m ih => rw [rsum_succ, ih]; simp

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [rsum_zero]
      rw [show rsum m (fun j => rsum 0 (fun i => f i j)) = rsum m (fun _ => (0 : Int)) by
        apply rsum_congr
        intro j hj
        rw [rsum_zero]]
      exact (rsum_const_zero m).symm
  | succ n ih =>
      rw [rsum_succ]
      rw [ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr
        intro j hj
        rw [rsum_succ]]
      rw [rsum_add]

private theorem list_sum_add {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp only [List.map_cons, List.sum_cons]; rw [ih]; grind


private theorem list_sum_congr {α} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem put_self (b : Bind) (v x : Nat) : put b v x v = x := by simp [put]

private theorem put_put_same (b : Bind) (v x y : Nat) : put (put b v x) v y = put b v y := by
  funext i
  by_cases h : i == v <;> simp [put, h]

private theorem put_comm_ne (b : Bind) (v w x y : Nat) (h : (v != w) = true) :
    put (put b v x) w y = put (put b w y) v x := by
  funext i
  by_cases hv : i == v
  · by_cases hw : i == w
    · have hiv : i = v := Nat.eq_of_beq_eq_true hv
      have hiw : i = w := Nat.eq_of_beq_eq_true hw
      have hvw : v = w := by omega
      simp [hvw] at h
    · simp [put, hv, hw]
  · by_cases hw : i == w <;> simp [put, hv, hw]

private theorem put_keep_other (b : Bind) (v w x : Nat) (hvw : v ≠ w) :
    put (put b v x) w (b w) = put b v x := by
  funext i
  by_cases hi : i == v
  · have hiv : i = v := Nat.eq_of_beq_eq_true hi
    subst i
    have hvw' : (v == w) = false := by cases h' : (v == w) <;> simp_all
    simp [put, hvw']
  · by_cases hj : i == w
    · have hiw : i = w := Nat.eq_of_beq_eq_true hj
      subst i
      have hwv' : (w == v) = false := by cases h' : (w == v) <;> simp_all
      simp [put, hwv']
    · simp [put, hi, hj]

private theorem put_put_ne_left (b : Bind) (v w x y : Nat) (hvw : v ≠ w) :
    put (put b v x) w y v = x := by
  have hvw' : (v == w) = false := by cases h' : (v == w) <;> simp_all
  simp [put, hvw']

private theorem put_put_ne_right (b : Bind) (v w x y : Nat) (hvw : v ≠ w) :
    put (put b v x) w y w = y := by
  simp [put]

private theorem erase_eq_filter_ne (vs : List Nat) (hnd : vs.Nodup) (v : Nat) :
    vs.erase v = vs.filter (fun x => x != v) := by
  induction vs with
  | nil => simp
  | cons a as ih =>
      by_cases h : a == v
      · have ha : a = v := Nat.eq_of_beq_eq_true h
        subst a
        rw [List.erase_cons_head]
        have hnot : v ∉ as := (List.nodup_cons.mp hnd).1
        have hf : as.filter (fun x : Nat => x != v) = as := by
          apply List.filter_eq_self.mpr
          intro x hx
          have hxv : x ≠ v := by intro hxv; subst x; exact hnot hx
          cases h' : (x == v) <;> simp_all
        simp [hf]
      · have hndas : as.Nodup := (List.nodup_cons.mp hnd).2
        rw [List.erase_cons_tail h, ih hndas]
        have hane : (a != v) = true := by cases h' : (a == v) <;> simp_all
        rw [List.filter_cons]
        simp [hane]

private theorem integrate_congr (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ x, f x = g x) : integrate n vs b f = integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum
      apply list_sum_congr
      intro x hx
      exact ih (put b v x)

private theorem integrate_zero (n : Nat) (vs : List Nat) (b : Bind) :
    integrate n vs b (fun _ => (0 : Int)) = 0 := by
  induction vs generalizing b with
  | nil => rfl
  | cons v vs ih => simp [integrate, ih, sum_zero_map]

private theorem integrate_swap (n : Nat) (v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: w :: vs) b f = integrate n (w :: v :: vs) b f := by
  by_cases h : v == w
  · have hvw : v = w := Nat.eq_of_beq_eq_true h
    subst w
    rfl
  · have hne : (v != w) = true := by
      cases h' : (v == w) <;> simp_all
    change rsum n (fun x => rsum n (fun y => integrate n vs (put (put b v x) w y) f)) =
      rsum n (fun x => rsum n (fun y => integrate n vs (put (put b w x) v y) f))
    rw [rsum_comm]
    apply list_sum_congr
    intro x hx
    apply list_sum_congr
    intro y hy
    rw [put_comm_ne b v w y x hne]

private theorem integrate_rotate (n : Nat) (v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: vs) b f = integrate n (vs ++ [v]) b f := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      rw [integrate_swap]
      change ((List.range n).map (fun x => integrate n (v :: ws) (put b w x) f)).sum =
        ((List.range n).map (fun x => integrate n (ws ++ [v]) (put b w x) f)).sum
      exact integrate_congr n [w] b _ _ ih

private theorem integrate_snoc (n : Nat) (vs : List Nat) (v : Nat) (b : Bind) (f : Bind → Int) :
    integrate n (vs ++ [v]) b f = integrate n vs b (fun b' => integrate n [v] b' f) := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      change ((List.range n).map (fun x => integrate n (ws ++ [v]) (put b w x) f)).sum =
        ((List.range n).map (fun x => integrate n ws (put b w x) (fun b' => integrate n [v] b' f))).sum
      apply list_sum_congr
      intro x hx
      exact ih (put b w x)

private theorem integrate_cons_singleton (n : Nat) (v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: vs) b f = integrate n vs b (fun b' => integrate n [v] b' f) := by
  rw [integrate_rotate, integrate_snoc]

private theorem integrate_extract (n : Nat) (v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int)
    (hnd : vs.Nodup) (hv : v ∈ vs) :
    integrate n vs b f = integrate n [v] b (fun b' => integrate n (vs.erase v) b' f) := by
  induction vs generalizing b f with
  | nil => simp at hv
  | cons w ws ih =>
      by_cases hwv : w == v
      · have hwv' : w = v := Nat.eq_of_beq_eq_true hwv
        subst w
        rw [List.erase_cons_head]
        rfl
      · have hvw : v ∈ ws := by
          simp only [List.mem_cons] at hv
          rcases hv with rfl | hv
          · simp at hwv
          · exact hv
        have hndws : ws.Nodup := (List.nodup_cons.mp hnd).2
        calc
          integrate n (w :: ws) b f
              = integrate n ws b (fun b' => integrate n [w] b' f) := integrate_cons_singleton n w ws b f
          _ = integrate n [v] b (fun b' => integrate n (ws.erase v) b'
                (fun b'' => integrate n [w] b'' f)) := ih b (fun b' => integrate n [w] b' f) hndws hvw
          _ = integrate n [v] b (fun b' => integrate n (w :: ws.erase v) b' f) := by
                apply integrate_congr
                intro b'
                exact (integrate_cons_singleton n w (ws.erase v) b' f).symm
          _ = integrate n [v] b (fun b' => integrate n ((w :: ws).erase v) b' f) := by
                rw [List.erase_cons_tail hwv]

private theorem integrate_add (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x + g x) = integrate n vs b f + integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, list_sum_add]

private theorem integrate_list_sum {α} (n : Nat) (vs : List Nat) (b : Bind)
    (l : List α) (F : α → Bind → Int) :
    integrate n vs b (fun x => (l.map (fun a => F a x)).sum) =
      (l.map (fun a => integrate n vs b (F a))).sum := by
  induction l with
  | nil => simp [integrate_zero]
  | cons a l ih => simp only [List.map_cons, List.sum_cons]; rw [integrate_add, ih]

private theorem list_sum_filter_eq (l : List Nat) (p : Nat → Bool) (g : Nat → Int)
    (h : ∀ x ∈ l, p x = false → g x = 0) :
    ((l.filter p).map g).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases hp : p a = true
      · simp [hp, ih (fun x hx => h x (by simp [hx]))]
      · have hp' : p a = false := by cases h' : p a <;> simp_all
        simp [hp', h a (by simp) hp', ih (fun x hx => h x (by simp [hx]))]

private theorem filter_singleton_sum (x : Nat) (p : Nat → Bool) (g : Nat → Int)
    (hzero : p x = false → g x = 0) :
    (([x].filter p).map g).sum = g x := by
  rw [List.filter_cons]
  by_cases hp : p x = true
  · simp [hp]
  · have hp' : p x = false := by cases h' : p x <;> simp_all
    simp [hp', hzero hp']

/-! Equality-partition (Möbius) helpers. -/

private def constOn (bs : List (List Nat)) (x : Bind) : Prop :=
  ∀ blk ∈ bs, ∀ v ∈ blk, ∀ w ∈ blk, x v = x w

private def rootOf (bs : List (List Nat)) (v : Nat) : Nat :=
  ((bs.find? (fun blk => blk.contains v)).bind List.head?).getD v

private def repsOf (bs : List (List Nat)) : List Nat := bs.filterMap List.head?

private theorem range_filter_eq_single (n u : Nat) (hu : u < n) (f : Nat → Int) :
    (((List.range n).filter (fun x => x == u)).map f).sum = f u := by
  induction n generalizing u with
  | zero => omega
  | succ n ih =>
      rw [show n+1 = n+1 by rfl, List.range_succ]
      simp only [List.filter_append, List.map_append, List.sum_append]
      by_cases hnu : u == n
      · have hun : u = n := Nat.eq_of_beq_eq_true hnu
        subst u
        have hnil : (List.range n).filter (fun x : Nat => x == n) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro x hx
          have hxlt : x < n := List.mem_range.mp hx
          cases h' : (x == n) <;> simp_all
        simp [hnil]
      · have hne : u ≠ n := by intro he; subst u; simp at hnu
        have hult : u < n := by omega
        rw [ih u hult]
        have hlast : ([n].filter (fun x : Nat => x == u)) = [] := by
          have hnu' : ¬ n = u := fun h => hne h.symm
          simp [hnu']
        simp [hlast]

private theorem map_ite_eq_filter {α} (l : List α) (p : α → Bool) (f : α → Int) :
    (l.map (fun x => if p x then f x else 0)).sum = ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases h : p a = true <;> simp [h, ih]

private theorem sum_range_ite_eq (n u : Nat) (hu : u < n) (f : Nat → Int) :
    ((List.range n).map (fun x => if x == u then f x else 0)).sum = f u := by
  rw [map_ite_eq_filter]
  exact range_filter_eq_single n u hu f

private theorem sum_map_ite_const {α} (l : List α) (c : Bool) (G : α → Int) :
    (l.map (fun x => if c then G x else 0)).sum = if c then (l.map G).sum else 0 := by
  induction l with
  | nil => cases c <;> simp
  | cons a l ih =>
      cases c
      · simpa using sum_zero_map l
      · simp [ih]

private theorem integrate_fixed_w_cond (n : Nat) (vs : List Nat) (b : Bind) (w z u : Nat)
    (F : Bind → Int) (hw : w ∉ vs) :
    integrate n vs (put b w z) (fun y => if y w == u then F y else 0) =
      if z == u then integrate n vs (put b w z) F else 0 := by
  induction vs generalizing b with
  | nil => simp [integrate, put]
  | cons v vs ih =>
      have hvw : v ≠ w := by intro h; subst v; exact hw (by simp)
      have hwvs : w ∉ vs := by intro h; exact hw (by simp [h])
      have hne : (w != v) = true := by cases h' : (w == v) <;> simp_all
      simp only [integrate]
      rw [show (List.range n).map (fun x => integrate n vs (put (put b w z) v x)
            (fun y => if y w == u then F y else 0)) =
          (List.range n).map (fun x => integrate n vs (put (put b v x) w z)
            (fun y => if y w == u then F y else 0)) by
        apply List.map_congr_left
        intro x hx
        rw [put_comm_ne b w v z x hne]]
      rw [show (List.range n).map (fun x => integrate n vs (put (put b v x) w z)
            (fun y => if y w == u then F y else 0)) =
          (List.range n).map (fun x => if z == u then
            integrate n vs (put (put b v x) w z) F else 0) by
        apply List.map_congr_left
        intro x hx
        exact ih (put b v x) hwvs]
      rw [sum_map_ite_const]
      by_cases h : z == u
      · simp [h]
        apply congrArg List.sum
        apply List.map_congr_left
        intro x hx
        rw [put_comm_ne b w v z x hne]
      · simp [h]

private theorem integrate_fix_var (n w u : Nat) (vs : List Nat) (b : Bind) (F : Bind → Int)
    (hu : u < n) (hnd : vs.Nodup) (hw : w ∈ vs) :
    integrate n vs b (fun x => if x w == u then F x else 0) =
      integrate n (vs.erase w) (put b w u) F := by
  rw [integrate_extract n w vs b _ hnd hw]
  rw [show integrate n [w] b (fun b' => integrate n (vs.erase w) b'
        (fun x => if x w == u then F x else 0)) =
      rsum n (fun x => if x == u then integrate n (vs.erase w) (put b w x) F else 0) by
    unfold rsum
    change ((List.range n).map (fun x => integrate n (vs.erase w) (put b w x)
      (fun y => if y w == u then F y else 0))).sum =
      ((List.range n).map (fun x => if x == u then integrate n (vs.erase w) (put b w x) F else 0)).sum
    congr 1
    apply List.map_congr_left
    intro x hx
    simpa [put_put_same] using
      (integrate_fixed_w_cond n (vs.erase w) (put b w x) w x u F
        (by rw [erase_eq_filter_ne vs hnd w]; simp))]
  unfold rsum
  rw [sum_range_ite_eq n u hu]

theorem sound_DeltaForecast :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CountryReturn.DeltaForecast.Goal := by
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

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.CountryReturnDeltaForecast
