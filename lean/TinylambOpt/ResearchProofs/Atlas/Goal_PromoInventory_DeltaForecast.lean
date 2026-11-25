import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PromoInventory_DeltaForecast

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

theorem sound_PromoInventory_DeltaForecast : PromoInventory.DeltaForecast.Goal := by
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

end TinylambOpt.ResearchProofs.Atlas.Goal_PromoInventory_DeltaForecast
