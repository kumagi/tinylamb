import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.ParallelLineage.Remaining

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ParallelLineage
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Samples (choose)
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total dot)

/-! Finite-difference infrastructure used by DeltaForecast. -/

private theorem diff_succ_eq (k : Nat) (f : Int → Int) (t : Int) :
    diff (k+1) f t = diff k f (t+1) - diff k f t := by
  rfl

private theorem diff_k_diff1 (k : Nat) (f : Int → Int) (t : Int) :
    diff k (diff 1 f) t = diff (k+1) f t := by
  induction k generalizing t with
  | zero => rfl
  | succ k ih =>
      rw [diff_succ_eq, ih (t+1), ih t]
      rfl

private theorem diff_add (k : Nat) (f g : Int → Int) (t : Int) :
    diff k (fun s => f s + g s) t = diff k f t + diff k g t := by
  induction k generalizing t with
  | zero => rfl
  | succ k ih =>
      rw [diff_succ_eq, diff_succ_eq, diff_succ_eq]
      rw [ih (t+1), ih t]
      omega

private theorem diff_const_mul (k : Nat) (c : Int) (f : Int → Int) (t : Int) :
    diff k (fun s => c * f s) t = c * diff k f t := by
  induction k generalizing t with
  | zero => rfl
  | succ k ih =>
      rw [diff_succ_eq, diff_succ_eq]
      rw [ih (t+1), ih t]
      grind

private theorem diff_zero_fun (k : Nat) : ∀ t, diff k (fun _ : Int => (0 : Int)) t = 0 := by
  induction k with
  | zero => intro t; rfl
  | succ k ih =>
      intro t
      rw [diff_succ_eq, ih (t+1), ih t]
      omega

private theorem diff_zero_of_zero {k : Nat} {f : Int → Int} (h : ∀ t, diff k f t = 0) :
    ∀ t, diff (k+1) f t = 0 := by
  intro t
  rw [diff_succ_eq, h (t+1), h t]
  omega

private def lin (a d : Int) (t : Int) : Int := a + t * d

private theorem diff1_lin (a d : Int) (t : Int) : diff 1 (lin a d) t = d := by
  unfold lin
  rw [diff_succ_eq]
  simp only [diff]
  grind

private theorem diff1_mul_lin (a d : Int) (f : Int → Int) (t : Int) :
    diff 1 (fun s => lin a d s * f s) t =
      lin (a+d) d t * diff 1 f t + d * f t := by
  unfold lin
  rw [diff_succ_eq]
  simp only [diff]
  grind

private theorem diff_succ_mul_lin (m : Nat) (f : Int → Int)
    (h : ∀ t, diff m f t = 0) (a d : Int) :
    ∀ t, diff (m+1) (fun s => lin a d s * f s) t = 0 := by
  induction m generalizing f a d with
  | zero =>
      intro t
      rw [diff_succ_eq]
      simp only [diff] at h ⊢
      rw [h (t+1), h t]
      simp
  | succ m ih =>
      intro t
      rw [← diff_k_diff1 (m+1)]
      have hf : ∀ t, diff m (diff 1 f) t = 0 := by
        intro t
        rw [diff_k_diff1]
        exact h t
      have hlin : ∀ t, diff (m+1) (fun s => lin (a+d) d s * diff 1 f s) t = 0 :=
        ih (diff 1 f) hf (a+d) d
      have hrest : ∀ t, diff (m+1) (fun s => d * f s) t = 0 := by
        intro t
        rw [diff_const_mul, h t]
        simp
      have hsum : ∀ t, diff (m+1) (fun s => lin (a+d) d s * diff 1 f s + d * f s) t = 0 := by
        intro t
        rw [diff_add, hlin t, hrest t]
        omega
      have heq : (diff 1 (fun u => lin a d u * f u)) =
          (fun s => lin (a+d) d s * diff 1 f s + d * f s) := by
        funext s
        exact diff1_mul_lin a d f s
      rw [heq]
      exact hsum t

private theorem diff_succ_const_mul_lin (m : Nat) (f : Int → Int)
    (h : ∀ t, diff m f t = 0) (a d c : Int) :
    ∀ t, diff (m+1) (fun s => c * (lin a d s * f s)) t = 0 := by
  intro t
  rw [diff_const_mul]
  rw [diff_succ_mul_lin m f h a d t]
  simp

private theorem diff_list_sum (k : Nat) {α : Type} (l : List α) (F : α → Int → Int)
    (h : ∀ a ∈ l, ∀ t, diff k (F a) t = 0) :
    ∀ t, diff k (fun t => (l.map (fun a => F a t)).sum) t = 0 := by
  induction l with
  | nil =>
      intro t
      simpa using diff_zero_fun k t
  | cons a l ih =>
      intro t
      simp only [List.map_cons, List.sum_cons]
      rw [diff_add]
      rw [h a (by simp) t]
      rw [ih (fun b hb => h b (by simp [hb])) t]
      omega

private theorem diff_integrate (k : Nat) (n : Nat) (vs : List Nat) (b : Bind)
    (F : Bind → Int → Int) (h : ∀ x, ∀ t, diff k (F x) t = 0) :
    ∀ t, diff k (fun t => integrate n vs b (fun x => F x t)) t = 0 := by
  induction vs generalizing b with
  | nil =>
      intro t
      simpa [integrate] using h b t
  | cons v vs ih =>
      intro t
      simp only [integrate]
      change diff k (fun t => ((List.range n).map (fun x =>
        integrate n vs (put b v (x : Nat)) (fun y => F y t))).sum) t = 0
      apply diff_list_sum
      intro x hx t'
      exact ih (put b v (x : Nat)) t'

private theorem slope_eq_lin_or_const (db delta : DB) (table : Nat) (t : Int)
    (hdelta : ∀ r x y, r ≠ table → delta r x y = 0) (r u v : Nat) :
    slope db delta t r u v =
      if r == table then lin (db r u v) (delta r u v) t else db r u v := by
  by_cases h : r == table
  · have hr : r = table := Nat.eq_of_beq_eq_true h
    simp [h, slope, lin]
  · have hr : r ≠ table := by
      intro h'
      exact h (by simp [h'])
    simp [h, slope, lin, hdelta r u v hr]

private theorem sum_add_int {α : Type} (l : List α) (f g : α → Int) :
    (l.map (fun a => f a + g a)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

private theorem sum_congr {α : Type} (l : List α) (f g : α → Int)
    (h : ∀ a ∈ l, f a = g a) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun b hb => h b (by simp [hb]))]

private theorem hockey (t k : Nat) :
    ((List.range t).map (fun s => choose s k)).sum = choose t (k+1) := by
  induction t with
  | zero =>
      cases k <;> simp [choose]
  | succ t ih =>
      rw [List.range_succ (n := t), List.map_append, List.sum_append]
      simp only [List.map_singleton, List.sum_cons, List.sum_nil]
      rw [ih]
      rw [show choose (t+1) (k+1) = choose t k + choose t (k+1) by rfl]
      omega

private theorem list_sum_range_succ_int (n : Nat) (F : Nat → Int) :
    ((List.range (n+1)).map F).sum = ((List.range n).map F).sum + F n := by
  rw [List.range_succ (n := n), List.map_append, List.sum_append]
  simp

private theorem list_sum_range_succ_nat (n : Nat) (F : Nat → Nat) :
    ((List.range (n+1)).map F).sum = ((List.range n).map F).sum + F n := by
  rw [List.range_succ (n := n), List.map_append, List.sum_append]
  simp

private theorem list_sum_zero_int (l : List Int) :
    (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem list_sum_zero_natidx (l : List Nat) :
    (l.map (fun _ : Nat => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem range_shift (k : Nat) (F : Nat → Int) :
    ((List.range (k+2)).map F).sum = F 0 + ((List.range (k+1)).map (fun i => F (i+1))).sum := by
  induction k with
  | zero => simp [List.range_succ]
  | succ k ih =>
      rw [show (k+1)+2 = (k+2)+1 by omega, list_sum_range_succ_int (k+2) F, ih]
      rw [show (k+1)+1 = (k+2) by omega, list_sum_range_succ_int (k+1) (fun i => F (i+1))]
      grind

private theorem forecast_succ (f : Int → Int) (k t : Nat) :
    forecast f (k+1) t = f 0 +
      ((List.range (k+1)).map (fun i => (choose t (i+1) : Int) * diff (i+1) f 0)).sum := by
  unfold forecast
  rw [range_shift k (fun i => (choose t i : Int) * diff i f 0)]
  simp [diff, choose]

private theorem sum_swap_range (t k : Nat) (A : Nat → Int) :
    ((List.range t).map (fun s => ((List.range (k+1)).map (fun i => (choose s i : Int) * A i)).sum)).sum =
    ((List.range (k+1)).map (fun i => (((List.range t).map (fun s => choose s i)).sum : Int) * A i)).sum := by
  induction t with
  | zero =>
      simp
      exact (list_sum_zero_natidx (List.range (k+1))).symm
  | succ t ih =>
      rw [list_sum_range_succ_int t (fun s => ((List.range (k+1)).map
        (fun i => (choose s i : Int) * A i)).sum)]
      rw [ih]
      rw [show (t+1) = t+1 by rfl]
      -- rewrite the inner sums on the right via list_sum_range_succ_int (Nat-valued)
      have hnat : ∀ i, ((List.range (t+1)).map (fun s => choose s i)).sum =
          ((List.range t).map (fun s => choose s i)).sum + choose t i := by
        intro i
        exact list_sum_range_succ_nat t (fun s => choose s i)
      have hsum : ((List.range (k+1)).map (fun i =>
          (((List.range (t+1)).map (fun s => choose s i)).sum : Int) * A i)).sum =
        ((List.range (k+1)).map (fun i =>
          ((((List.range t).map (fun s => choose s i)).sum + choose t i : Nat) : Int) * A i)).sum := by
        apply sum_congr
        intro i hi
        rw [hnat i]
      rw [hsum]
      rw [← sum_add_int]
      apply sum_congr
      intro i hi
      rw [Int.natCast_add, hockey]
      grind

private theorem f_telescope (f : Int → Int) (t : Nat) :
    f (t : Int) = f 0 + ((List.range t).map (fun s : Nat => diff 1 f (s : Int))).sum := by
  induction t with
  | zero => simp
  | succ t ih =>
      rw [list_sum_range_succ_int t (fun s : Nat => diff 1 f (s : Int))]
      rw [← Int.add_assoc, ← ih]
      change f ((t+1 : Nat) : Int) = f (t : Int) + (f ((t : Int) + 1) - f (t : Int))
      rw [Int.natCast_succ]
      grind

private theorem newton_reconstruct (k : Nat) (f : Int → Int)
    (h : ∀ t, diff (k+1) f t = 0) :
    ∀ t : Nat, f (t : Int) = forecast f k t := by
  induction k generalizing f with
  | zero =>
      intro t
      have hfconst : ∀ t : Nat, f (t : Int) = f 0 := by
        intro t
        induction t with
        | zero => rfl
        | succ t iht =>
            have ht := h (t : Int)
            rw [diff_succ_eq] at ht
            simp only [diff] at ht
            have ht' : f ((t : Int) + 1) = f (t : Int) := by omega
            have ht'' : f ((t+1 : Nat) : Int) = f (t : Int) := by
              rw [Int.natCast_succ]
              exact ht'
            rw [ht'', iht]
      rw [hfconst t]
      simp [forecast, diff, choose]
  | succ k ih =>
      intro t
      let g : Int → Int := fun s => diff 1 f s
      have hg : ∀ s, diff (k+1) g s = 0 := by
        intro s
        rw [diff_k_diff1]
        exact h s
      have hih : ∀ t : Nat, g (t : Int) = forecast g k t := ih g hg
      rw [f_telescope f t]
      rw [forecast_succ f k t]
      have hg_sum : ((List.range t).map (fun s : Nat => g (s : Int))).sum =
          ((List.range t).map (fun s : Nat => ((List.range (k+1)).map
            (fun i => (choose s i : Int) * diff (i+1) f 0)).sum)).sum := by
        apply sum_congr
        intro s hs
        rw [hih s]
        unfold forecast
        apply sum_congr
        intro i hi
        have hgi : diff i g 0 = diff (i+1) f 0 := by
          rw [diff_k_diff1]
        rw [hgi]
      rw [hg_sum]
      rw [sum_swap_range t k (fun i => diff (i+1) f 0)]
      congr 1
      apply sum_congr
      intro i hi
      rw [hockey]

private theorem diff_atom_prod (atoms : List Atom) (db delta : DB) (table : Nat)
    (hdelta : ∀ r x y, r ≠ table → delta r x y = 0) (x : Bind) :
    ∀ t, diff (atoms.countP (fun a => a.rel == table) + 1)
      (fun t => (atoms.map (fun a => value (slope db delta t) a x)).prod) t = 0 := by
  induction atoms with
  | nil =>
      intro t
      simp [diff]
  | cons a as ih =>
      intro t
      simp only [List.countP_cons, List.map_cons, List.prod_cons]
      by_cases h : a.rel == table
      · simp [h]
        have hval : (fun t => value (slope db delta t) a x) =
            lin (db a.rel (x a.src) (x a.dst)) (delta a.rel (x a.src) (x a.dst)) := by
          funext s
          rfl
        have hfun : (fun t => value (slope db delta t) a x *
            (as.map (fun a => value (slope db delta t) a x)).prod) =
            (fun t => lin (db a.rel (x a.src) (x a.dst)) (delta a.rel (x a.src) (x a.dst)) t *
            (as.map (fun a => value (slope db delta t) a x)).prod) := by
          funext s
          rw [congrFun hval s]
        rw [hfun]
        simpa using (diff_succ_mul_lin (as.countP (fun a => a.rel == table) + 1)
          (fun t => (as.map (fun a => value (slope db delta t) a x)).prod) ih
            (db a.rel (x a.src) (x a.dst)) (delta a.rel (x a.src) (x a.dst)) t)
      · simp [h]
        have hval : (fun t => value (slope db delta t) a x) =
            (fun _ => db a.rel (x a.src) (x a.dst)) := by
          funext s
          simp [value, slope_eq_lin_or_const db delta table s hdelta a.rel (x a.src) (x a.dst), h]
        have hfun : (fun t => value (slope db delta t) a x *
            (as.map (fun a => value (slope db delta t) a x)).prod) =
            (fun t => db a.rel (x a.src) (x a.dst) *
            (as.map (fun a => value (slope db delta t) a x)).prod) := by
          funext s
          rw [congrFun hval s]
        rw [hfun]
        rw [diff_const_mul, ih t]
        simp

private theorem diff_eval_vanish (p : Pattern) (n : Nat) (db delta : DB) (b : Bind) (table : Nat)
    (hdelta : ∀ r x y, r ≠ table → delta r x y = 0) :
    ∀ t, diff (p.atoms.countP (fun a => a.rel == table) + 1)
      (fun t => reference p n (slope db delta t) b) t = 0 := by
  unfold reference
  apply diff_integrate
  intro x t
  exact diff_atom_prod p.atoms db delta table hdelta x t

theorem sound_DeltaForecast :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ParallelLineage.DeltaForecast.Goal := by
  intro n db delta b table t hdelta
  let f : Int → Int := fun u => reference pattern n (slope db delta u) b
  change f (t : Int) = forecast f (pattern.atoms.countP (fun a => a.rel == table)) t
  exact newton_reconstruct (pattern.atoms.countP (fun a => a.rel == table)) f
    (diff_eval_vanish pattern n db delta b table hdelta) t

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.ParallelLineage.Remaining
