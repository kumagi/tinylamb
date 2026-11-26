import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved (15 claims, including the following)

- `CastOverlap.AnchoredRepair` — `∀ n old new b, reference pattern n new b - reference
  pattern n old b = (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old
  new b i a)).sum`
- `CastOverlap.BoundaryMessages` — `∀ n db b, elimination pattern n db b = reference
  pattern n db b`
- `CastOverlap.GramSquare` — `∀ n db b, reference pattern n db b = total n (fun x =>
  total n (fun y => (overlap n db 0 x y)^2))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieJoinBoundaryMessages

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total dot)

/-! Small finite-sum and integration toolkit. -/

def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

theorem sum_map_congr {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

theorem sum_add {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

theorem sum_sub {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x - g x)).sum = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]; omega

theorem sum_mul_left {α} (l : List α) (c : Int) (f : α → Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.mul_add]

theorem sum_mul_right {α} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_mul]

theorem sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

theorem sum_swap {α β} (l : List α) (l' : List β) (F : α → β → Int) :
    (l.map (fun a => (l'.map (F a)).sum)).sum =
      (l'.map (fun b => (l.map (fun a => F a b)).sum)).sum := by
  induction l with
  | nil => simp [sum_zero]
  | cons a l ih => simp [ih, sum_add]

theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi)),
        h n (Nat.lt_succ_self n)]

theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

theorem rsum_sub (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i - g i) = rsum n f - rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := sum_mul_left (List.range n) c f

theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := sum_mul_right (List.range n) f c

theorem rsum_nonneg (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → 0 ≤ f i) :
    0 ≤ rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      have hn := h n (Nat.lt_succ_self n)
      omega

theorem rsum_le (n : Nat) (f g : Nat → Int) (h : ∀ i, i < n → f i ≤ g i) :
    rsum n f ≤ rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      have hn := h n (Nat.lt_succ_self n)
      omega

theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
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

theorem rsum_product (n m : Nat) (f g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr
            intro i hi
            exact rsum_const_mul m (f i) g
    _ = rsum n f * rsum m g := rsum_mul_const n f (rsum m g)

theorem rsum_pairs (n : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum n (fun j => f i j)) =
      rsum n (fun i => rsum n (fun j => f j i)) := rsum_comm n n f

theorem put_same (b : Bind) (v : Nat) : put b v (b v) = b := by
  funext i
  by_cases h : i = v
  · subst i; simp [put]
  · simp [put, show (i == v) = false by simpa using h]

theorem put_comm_ne (b : Bind) (v w x y : Nat) (h : v ≠ w) :
    put (put b v x) w y = put (put b w y) v x := by
  funext i
  by_cases hi : i = v
  · subst i
    simp [put, show (v == w) = false by simpa using h]
  · by_cases hj : i = w
    · subst i; simp [put, hi]
    · simp [put, show (i == v) = false by simpa using hi,
        show (i == w) = false by simpa using hj]

theorem integrate_congr (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ x, f x = g x) : integrate n vs b f = integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum
      exact sum_map_congr _ _ _ (fun x hx => ih (put b v x))

theorem integrate_swap (n v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: w :: vs) b f = integrate n (w :: v :: vs) b f := by
  by_cases hvw : v = w
  · subst w; rfl
  change ((List.range n).map (fun x =>
      ((List.range n).map (fun y => integrate n vs (put (put b v x) w y) f)).sum)).sum =
    ((List.range n).map (fun x =>
      ((List.range n).map (fun y => integrate n vs (put (put b w x) v y) f)).sum)).sum
  rw [sum_swap]
  apply sum_map_congr
  intro x hx
  apply sum_map_congr
  intro y hy
  rw [put_comm_ne b w v x y (fun h' => hvw h'.symm)]

theorem integrate_rotate (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: vs) b f = integrate n (vs ++ [v]) b f := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      rw [integrate_swap]
      change ((List.range n).map (fun x => integrate n (v :: ws) (put b w x) f)).sum =
        ((List.range n).map (fun x => integrate n (ws ++ [v]) (put b w x) f)).sum
      exact integrate_congr n [w] b _ _ ih

theorem integrate_snoc (n : Nat) (vs : List Nat) (v : Nat) (b : Bind) (f : Bind → Int) :
    integrate n (vs ++ [v]) b f = integrate n vs b (fun b' => integrate n [v] b' f) := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      change ((List.range n).map (fun x => integrate n (ws ++ [v]) (put b w x) f)).sum =
        ((List.range n).map (fun x => integrate n ws (put b w x) (fun b' => integrate n [v] b' f))).sum
      exact sum_map_congr _ _ _ (fun x hx => ih (put b w x))

theorem integrate_cons_singleton (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: vs) b f = integrate n vs b (fun b' => integrate n [v] b' f) := by
  rw [integrate_rotate, integrate_snoc]

theorem integrate_perm (n : Nat) {vs vs' : List Nat} (h : vs.Perm vs') :
    ∀ (b : Bind) (f : Bind → Int), integrate n vs b f = integrate n vs' b f := by
  induction h with
  | nil => intro b f; rfl
  | cons x h ih =>
      intro b f
      exact sum_map_congr _ _ _ (fun t ht => ih (put b x t) f)
  | swap x y l => intro b f; exact integrate_swap n y x l b f
  | trans h1 h2 ih1 ih2 => intro b f; rw [ih1 b f, ih2 b f]

theorem integrate_add (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x + g x) = integrate n vs b f + integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_add]

theorem integrate_sub (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x - g x) = integrate n vs b f - integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_sub]

theorem integrate_mul_left (n : Nat) (vs : List Nat) (b : Bind) (c : Int) (f : Bind → Int) :
    integrate n vs b (fun x => c * f x) = c * integrate n vs b f := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_mul_left]

theorem integrate_mul_right (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) (c : Int) :
    integrate n vs b (fun x => f x * c) = integrate n vs b f * c := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_mul_right]

theorem integrate_zero (n : Nat) (vs : List Nat) (b : Bind) :
    integrate n vs b (fun _ => (0 : Int)) = 0 := by
  induction vs generalizing b with
  | nil => rfl
  | cons v vs ih => simp [integrate, ih, sum_zero]

theorem integrate_list_sum {α} (n : Nat) (vs : List Nat) (b : Bind)
    (l : List α) (F : α → Bind → Int) :
    integrate n vs b (fun x => (l.map (fun a => F a x)).sum) =
      (l.map (fun a => integrate n vs b (F a))).sum := by
  induction l with
  | nil => simp [integrate_zero]
  | cons a l ih => simp only [List.map_cons, List.sum_cons, integrate_add, ih]

theorem integrate_fixed (n : Nat) (vs : List Nat) (b : Bind) (v : Nat)
    (hv : v ∉ vs) (F : Nat → Bind → Int) :
    integrate n vs b (fun x => F (x v) x) = integrate n vs b (fun x => F (b v) x) := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      have hvw : v ≠ w := by intro h; subst w; exact hv (by simp)
      have hvws : v ∉ ws := by intro h; exact hv (by simp [h])
      change ((List.range n).map (fun t => integrate n ws (put b w t) (fun x => F (x v) x))).sum =
        ((List.range n).map (fun t => integrate n ws (put b w t) (fun x => F (b v) x))).sum
      apply sum_map_congr
      intro t ht
      rw [ih (put b w t) hvws]
      rw [show (put b w t) v = b v by simp [put, show (v == w) = false by simpa using hvw]]

theorem integrate_fixed2 (n : Nat) (vs : List Nat) (b : Bind) (v w : Nat)
    (hv : v ∉ vs) (hw : w ∉ vs) (F : Nat → Nat → Bind → Int) :
    integrate n vs b (fun x => F (x v) (x w) x) =
      integrate n vs b (fun x => F (b v) (b w) x) := by
  calc
    integrate n vs b (fun x => F (x v) (x w) x)
        = integrate n vs b (fun x => F (b v) (x w) x) :=
          integrate_fixed n vs b v hv (fun u x => F u (x w) x)
    _ = integrate n vs b (fun x => F (b v) (b w) x) :=
          integrate_fixed n vs b w hw (fun u x => F (b v) u x)

/-! Variable elimination. -/

def Respects (f : Factor) : Prop :=
  ∀ b b', (∀ v, f.scope.contains v = true → b v = b' v) → f.get b = f.get b'

theorem respects_value (db : DB) (a : Atom) : Respects ⟨[a.src, a.dst], value db a⟩ := by
  intro b b' h
  simp only [value]
  rw [h a.src (by simp), h a.dst (by simp)]

theorem product_respects_eq {fs : List Factor} (h : ∀ f ∈ fs, Respects f)
    {b b' : Bind} (heq : ∀ f ∈ fs, ∀ v, f.scope.contains v = true → b v = b' v) :
    product fs b = product fs b' := by
  induction fs with
  | nil => rfl
  | cons f fs ih =>
      simp only [product, List.map_cons, List.prod_cons]
      rw [h f (by simp) b b' (heq f (by simp))]
      congr 1
      exact ih (fun g hg => h g (by simp [hg])) (fun g hg => heq g (by simp [hg]))

theorem product_put_eq {fs : List Factor} (h : ∀ f ∈ fs, Respects f)
    (hnot : ∀ f ∈ fs, f.scope.contains v = false) (b : Bind) (x : Nat) :
    product fs (put b v x) = product fs b := by
  apply product_respects_eq h
  intro f hf w hw
  by_cases hvw : w = v
  · subst w; rw [hnot f hf] at hw; simp at hw
  · simp [put, show (w == v) = false by simpa using hvw]

theorem partition_filters (p : α → Bool) (l : List α) :
    (List.partition p l).fst = l.filter p ∧
      (List.partition p l).snd = l.filter (fun x => !p x) := by
  have h : ∀ (l a b : List α),
      (List.partition.loop p l (a,b)).fst = a.reverse ++ l.filter p ∧
      (List.partition.loop p l (a,b)).snd = b.reverse ++ l.filter (fun x => !p x) := by
    intro l
    induction l with
    | nil => intro a b; simp [List.partition.loop]
    | cons x xs ih =>
        intro a b
        by_cases hx : p x = true <;> simp [List.partition.loop, hx, ih]
  simpa [List.partition] using h l [] []

theorem mem_partition_notp (p : Factor → Bool) {l : List Factor} {f : Factor}
    (hf : f ∈ (List.partition p l).snd) : p f = false := by
  rw [(partition_filters p l).2] at hf
  have hn := (List.mem_filter.mp hf).2
  simpa using hn

theorem mem_partition_fst {l : List Factor} {v : Nat} {f : Factor}
    (hf : f ∈ (List.partition (fun g : Factor => g.scope.contains v) l).fst) : f ∈ l := by
  rw [(partition_filters _ l).1] at hf
  exact (List.mem_filter.mp hf).1

theorem mem_partition_snd {l : List Factor} {v : Nat} {f : Factor}
    (hf : f ∈ (List.partition (fun g : Factor => g.scope.contains v) l).snd) : f ∈ l := by
  rw [(partition_filters _ l).2] at hf
  exact (List.mem_filter.mp hf).1

theorem product_partition_get (p : Factor → Bool) (l : List Factor) (b : Bind) :
    product (List.partition p l).fst b * product (List.partition p l).snd b = product l b := by
  rw [(partition_filters p l).1, (partition_filters p l).2]
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [product] at ih
      by_cases h : p a = true <;>
        simp [product, h, ih, Int.mul_assoc, Int.mul_left_comm]

theorem respects_msg (n v : Nat) (inc : List Factor) (hres : ∀ f ∈ inc, Respects f) :
    Respects ⟨(inc.flatMap Factor.scope).eraseDups.filter (fun w => w != v),
      fun b => integrate n [v] b (product inc)⟩ := by
  intro b b' hb
  apply sum_map_congr
  intro x hx
  apply product_respects_eq hres
  intro f hf w hw
  by_cases hwv : w = v
  · subst w; simp [put]
  · rw [put, if_neg (by simp [hwv]), put, if_neg (by simp [hwv])]
    apply hb
    have hwmem : w ∈ f.scope := by simpa using hw
    have hwflat : w ∈ inc.flatMap Factor.scope := List.mem_flatMap.mpr ⟨f, hf, hwmem⟩
    have hwer : w ∈ (inc.flatMap Factor.scope).eraseDups := List.mem_eraseDups.mpr hwflat
    exact List.contains_iff_mem.mpr ((List.mem_filter (p := fun z => z != v)).mpr ⟨hwer, by
      by_cases hbe : (w == v) = true
      · exact False.elim (hwv (beq_iff_eq.mp hbe))
      · simpa [hbe] using hwv⟩)

theorem product_eliminate (n v : Nat) (fs : List Factor) (b : Bind)
    (hres : ∀ f ∈ fs, Respects f) :
    product (eliminate n v fs) b = integrate n [v] b (product fs) := by
  dsimp only [eliminate]
  let inc := (fs.partition (fun f : Factor => f.scope.contains v)).fst
  let rest := (fs.partition (fun f : Factor => f.scope.contains v)).snd
  have hinc : ∀ f ∈ inc, Respects f := fun f hf => hres f (mem_partition_fst hf)
  have hrest : ∀ f ∈ rest, Respects f := fun f hf => hres f (mem_partition_snd hf)
  have hnot : ∀ f ∈ rest, f.scope.contains v = false := fun f hf =>
    mem_partition_notp _ hf
  change integrate n [v] b (product inc) * product rest b = integrate n [v] b (product fs)
  change ((List.range n).map (fun x => product inc (put b v x))).sum * product rest b =
    ((List.range n).map (fun x => product fs (put b v x))).sum
  rw [← sum_mul_right]
  apply sum_map_congr
  intro x hx
  rw [← product_partition_get (fun f : Factor => f.scope.contains v) fs (put b v x)]
  rw [product_put_eq hrest hnot b x]

theorem respects_eliminate (n v : Nat) (fs : List Factor) (hres : ∀ f ∈ fs, Respects f) :
    ∀ f ∈ eliminate n v fs, Respects f := by
  intro f hf
  simp only [eliminate, List.mem_cons] at hf
  rcases hf with rfl | hf
  · exact respects_msg n v _ (fun g hg => hres g (mem_partition_fst hg))
  · exact hres f (mem_partition_snd hf)

theorem product_fold_eliminate (n : Nat) (vs : List Nat) (fs : List Factor) (b : Bind)
    (hres : ∀ f ∈ fs, Respects f) :
    product (vs.foldl (fun fs v => eliminate n v fs) fs) b = integrate n vs b (product fs) := by
  induction vs generalizing fs with
  | nil => rfl
  | cons v vs ih =>
      change product (vs.foldl (fun fs v => eliminate n v fs) (eliminate n v fs)) b =
        integrate n (v :: vs) b (product fs)
      rw [ih _ (respects_eliminate n v fs hres), integrate_cons_singleton]
      exact integrate_congr n vs b _ _ (fun b' => product_eliminate n v fs b' hres)

theorem elimination_eq_reference (p : Pattern) (n : Nat) (db : DB) (b : Bind) :
    elimination p n db b = reference p n db b := by
  unfold elimination reference
  rw [product_fold_eliminate]
  · exact integrate_congr n p.freeVars b _ _ (fun x => by
      simp [factors, product, Function.comp_def])
  · intro f hf
    rcases List.mem_map.mp hf with ⟨a, ha, rfl⟩
    exact respects_value db a

/-! Telescoping and anchored repair. -/

def cpExcl (atoms : List Atom) (old new : DB) (i : Nat) (b : Bind) : Int :=
  ((atoms.take i).map (fun a => value new a b)).prod *
    ((atoms.drop (i+1)).map (fun a => value old a b)).prod

theorem cpExcl_zero (a : Atom) (as : List Atom) (old new : DB) (b : Bind) :
    cpExcl (a::as) old new 0 b = (as.map (fun t => value old t b)).prod := by simp [cpExcl]

theorem cpExcl_succ (a : Atom) (as : List Atom) (old new : DB) (i : Nat) (b : Bind) :
    cpExcl (a::as) old new (i+1) b = value new a b * cpExcl as old new i b := by
  simp [cpExcl, Int.mul_assoc]

theorem zipIdx_shift_from {α} (as : List α) : ∀ k : Nat,
    (as.zipIdx (k+1)).map (fun p => (p.1, p.2-1)) = as.zipIdx k := by
  induction as with
  | nil => intro k; rfl
  | cons a as ih =>
      intro k
      simp only [List.zipIdx_cons, List.map_cons]
      rw [ih (k+1)]
      simp only [List.cons.injEq, Prod.mk.injEq, true_and, and_true]
      omega

theorem zipIdx_shift (as : List Atom) :
    (as.zipIdx 1).map (fun p => (p.1, p.2-1)) = as.zipIdx := by
  simpa using zipIdx_shift_from as 0

theorem zipIdx_unshift (as : List Atom) :
    as.zipIdx = (as.zipIdx 1).map (fun p => (p.1, p.2-1)) := (zipIdx_shift as).symm

theorem sum_zipIdx_succ {α} (as : List α) (f : α → Nat → Int) : ∀ k : Nat,
    ((as.zipIdx (k+1)).map (fun p => f p.1 p.2)).sum =
      ((as.zipIdx k).map (fun p => f p.1 (p.2+1))).sum := by
  induction as with
  | nil => intro k; rfl
  | cons a as ih =>
      intro k
      simp only [List.zipIdx_cons, List.map_cons, List.sum_cons]
      rw [ih (k+1)]

theorem prod_diff_telescope (atoms : List Atom) (old new : DB) (b : Bind) :
    (atoms.map (fun a => value new a b)).prod - (atoms.map (fun a => value old a b)).prod =
      ((atoms.zipIdx.map (fun (a, i) =>
        (value new a b - value old a b) * cpExcl atoms old new i b)).sum) := by
  induction atoms with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.prod_cons, List.zipIdx_cons, Nat.zero_add]
      rw [cpExcl_zero a as old new b]
      change value new a b * (as.map (fun t => value new t b)).prod -
          value old a b * (as.map (fun t => value old t b)).prod =
        (value new a b - value old a b) * (as.map (fun t => value old t b)).prod +
          ((as.zipIdx 1).map (fun p =>
            (value new p.1 b - value old p.1 b) * cpExcl (a::as) old new p.2 b)).sum
      have hs := sum_zipIdx_succ as (fun t i =>
        (value new t b - value old t b) * cpExcl (a::as) old new i b) 0
      simp only [Nat.zero_add] at hs
      rw [hs]
      rw [show (as.zipIdx.map (fun p =>
            (value new p.1 b - value old p.1 b) * cpExcl (a::as) old new (p.2+1) b)) =
            (as.zipIdx.map (fun (t, i) =>
              value new a b * ((value new t b - value old t b) * cpExcl as old new i b))) by
        apply List.map_congr_left
        intro x hx
        rcases x with ⟨t, i⟩
        dsimp only
        rw [cpExcl_succ]
        ac_rfl]
      rw [sum_mul_left, ← ih, Int.sub_mul, Int.mul_sub]
      omega

theorem filter_ne_eq_erase (vs : List Nat) (a : Nat) (hnd : vs.Nodup) :
    vs.filter (fun v => v != a) = vs.erase a := by
  induction vs with
  | nil => rfl
  | cons x xs ih =>
      have hp := List.pairwise_cons.mp hnd
      by_cases hxa : x = a
      · subst x
        have hna : a ∉ xs := by intro h; exact hp.1 a h rfl
        simp [List.erase_cons_head, hna, ih hp.2]
      · rw [List.erase_cons_tail (a := a) (b := x) (by simp [hxa])]
        rw [List.filter_cons, if_pos (by simp [hxa]), ih hp.2]

theorem erase_erase_filter (vs : List Nat) (a b : Nat) (hnd : vs.Nodup) :
    (vs.erase a).erase b = vs.filter (fun v => v != a && v != b) := by
  induction vs with
  | nil => rfl
  | cons x xs ih =>
      have hp := List.pairwise_cons.mp hnd
      by_cases hxa : x = a
      · subst x
        have hna : a ∉ xs := by intro h; exact hp.1 a h rfl
        rw [List.erase_cons_head]
        change xs.erase b = (a::xs).filter (fun v => v != a && v != b)
        rw [List.filter_cons, if_neg (by simp [show (a != a) = false by simp])]
        have hf : xs.filter (fun v => v != a && v != b) = xs.filter (fun v => v != b) := by
          apply List.filter_congr
          intro v hv
          have hva : v ≠ a := by intro h; subst v; exact hna hv
          simp [hva]
        rw [hf, ← filter_ne_eq_erase xs b hp.2]
      · by_cases hxb : x = b
        · subst x
          have hnb : b ∉ xs := by intro h; exact hp.1 b h rfl
          have hbne : b ≠ a := by simpa using hxa
          have hnb' : b ∉ xs.erase a := fun h =>
            hnb ((List.mem_erase_of_ne (a := b) (b := a) hbne).mp h)
          have hih := ih hp.2
          rw [List.erase_of_not_mem hnb'] at hih
          rw [List.erase_cons_tail (a := a) (b := b) (by simp [hxa]),
            List.erase_cons_head (a := b)]
          change xs.erase a = (b::xs).filter (fun v => v != a && v != b)
          rw [List.filter_cons, if_neg (by simp [show (b != b) = false by simp])]
          exact hih
        · rw [List.erase_cons_tail (a := a) (b := x) (by simp [hxa]),
            List.erase_cons_tail (a := b) (b := x) (by simp [hxb])]
          simp [show (x != a) = true by simpa using hxa,
            show (x != b) = true by simpa using hxb, ih hp.2]

theorem filter_and_ne (vs : List Nat) (a b : Nat) :
    (vs.filter (fun v => v != a)).filter (fun v => v != b) =
      vs.filter (fun v => v != a && v != b) := by
  induction vs with
  | nil => rfl
  | cons x xs ih =>
      by_cases hxa : x = a
      · subst x; simp [ih]
      · by_cases hxb : x = b
        · subst x; simp [ih, Bool.and_comm]
        · simp [show (x != a) = true by simpa using hxa,
            show (x != b) = true by simpa using hxb, ih]

theorem integrate_pivot (n : Nat) (vs : List Nat) (src dst : Nat) (b : Bind) (f : Bind → Int)
    (hnd : vs.Nodup) (hne : src ≠ dst) :
    integrate n vs b f =
      let cand := fun v => if vs.contains v then List.range n else [b v]
      let vars := vs.filter (fun v => v != src && v != dst)
      ((cand src).map (fun u => ((cand dst).map (fun w =>
        integrate n vars (put (put b src u) dst w) f)).sum)).sum := by
  dsimp only
  by_cases hsrc : vs.contains src = true
  · have hsm : src ∈ vs := by simpa using hsrc
    by_cases hdst : vs.contains dst = true
    · have hdm : dst ∈ vs := by simpa using hdst
      have hde : dst ∈ vs.erase src :=
        (List.mem_erase_of_ne (a := dst) (b := src) hne.symm).mpr hdm
      rw [if_pos hsrc, if_pos hdst]
      change integrate n vs b f =
        ((List.range n).map (fun u => ((List.range n).map (fun w =>
          integrate n (vs.filter (fun v => v != src && v != dst))
            (put (put b src u) dst w) f)).sum)).sum
      rw [integrate_perm n (List.perm_cons_erase hsm) b f]
      apply sum_map_congr
      intro u hu
      rw [integrate_perm n (List.perm_cons_erase hde) (put b src u) f]
      change ((List.range n).map (fun w => integrate n ((vs.erase src).erase dst)
          (put (put b src u) dst w) f)).sum = _
      rw [erase_erase_filter vs src dst hnd]
    · have hdm : dst ∉ vs := by simpa using hdst
      rw [if_pos hsrc, if_neg hdst]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
      rw [integrate_perm n (List.perm_cons_erase hsm) b f]
      apply sum_map_congr
      intro u hu
      have hf : vs.filter (fun v => v != src && v != dst) = vs.erase src := by
        rw [← filter_and_ne, filter_ne_eq_erase vs src hnd]
        apply List.filter_eq_self.mpr
        intro v hv
        have hv' := List.mem_of_mem_erase hv
        have hvd : v ≠ dst := by intro h; subst v; exact hdm hv'
        simp [hvd]
      rw [hf]
      change integrate n (vs.erase src) (put b src u) f =
        integrate n (vs.erase src) (put (put b src u) dst (b dst)) f
      rw [show put (put b src u) dst (b dst) = put b src u by
        funext i; by_cases hi : i = dst <;> simp [put, hi, hne.symm]]
  · have hsm : src ∉ vs := by simpa using hsrc
    by_cases hdst : vs.contains dst = true
    · have hdm : dst ∈ vs := by simpa using hdst
      rw [if_neg hsrc, if_pos hdst]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
      rw [integrate_perm n (List.perm_cons_erase hdm) b f]
      apply sum_map_congr
      intro w hw
      have hf : vs.filter (fun v => v != src && v != dst) = vs.erase dst := by
        have hfs : vs.filter (fun v => v != src) = vs := by
          apply List.filter_eq_self.mpr
          intro v hv
          have hvs : v ≠ src := by intro h; subst v; exact hsm hv
          simp [hvs]
        rw [← filter_and_ne, hfs, filter_ne_eq_erase vs dst hnd]
      rw [hf]
      change integrate n (vs.erase dst) (put b dst w) f =
        integrate n (vs.erase dst) (put (put b src (b src)) dst w) f
      rw [put_same]
    · have hdm : dst ∉ vs := by simpa using hdst
      rw [if_neg hsrc, if_neg hdst]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
      have hf : vs.filter (fun v => v != src && v != dst) = vs := by
        apply List.filter_eq_self.mpr
        intro v hv
        have hvs : v ≠ src := by intro h; subst v; exact hsm hv
        have hvd : v ≠ dst := by intro h; subst v; exact hdm hv
        simp [hvs, hvd]
      rw [hf, put_same, put_same]

theorem pivot_point (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (j : Nat) (a : Atom) (hsd : a.src ≠ a.dst) (u w : Nat) :
    let vars := p.freeVars.filter (fun v => v != a.src && v != a.dst)
    let b' := put (put b a.src u) a.dst w
    integrate n vars b' (fun x => (value new a x - value old a x) * cpExcl p.atoms old new j x) =
      (new a.rel u w - old a.rel u w) *
        integrate n vars b' (fun x => cpExcl p.atoms old new j x) := by
  dsimp only
  have hs : a.src ∉ p.freeVars.filter (fun v => v != a.src && v != a.dst) := by simp
  have hd : a.dst ∉ p.freeVars.filter (fun v => v != a.src && v != a.dst) := by simp
  have hsb : put (put b a.src u) a.dst w a.src = u := by
    simp [put, show (a.src == a.dst) = false by simpa using hsd]
  have hdb : put (put b a.src u) a.dst w a.dst = w := by simp [put]
  change integrate n (p.freeVars.filter (fun v => v != a.src && v != a.dst))
      (put (put b a.src u) a.dst w)
      (fun x => (new a.rel (x a.src) (x a.dst) - old a.rel (x a.src) (x a.dst)) *
        cpExcl p.atoms old new j x) = _
  rw [integrate_fixed2 n _ _ a.src a.dst hs hd
    (fun u v x => (new a.rel u v - old a.rel u v) * cpExcl p.atoms old new j x), hsb, hdb]
  exact integrate_mul_left _ _ _ _ _

theorem sum_filter_eq {α} (l : List α) (p : α → Bool) (g : α → Int)
    (h : ∀ x ∈ l, p x = false → g x = 0) :
    ((l.filter p).map g).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases hx : p x = true
      · simp [hx, ih (fun y hy hp => h y (by simp [hy]) hp)]
      · have hf : p x = false := by cases hp : p x <;> simp_all
        have hz : g x = 0 := h x (by simp) hf
        simp [hx, hz, ih (fun y hy hp => h y (by simp [hy]) hp)]

theorem pivot_integral_eq (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (j : Nat) (a : Atom) (hnd : p.freeVars.Nodup) (hsd : a.src ≠ a.dst) :
    integrate n p.freeVars b (fun x =>
      (value new a x - value old a x) * cpExcl p.atoms old new j x) = pivotTerm p n old new b j a := by
  rw [integrate_pivot n p.freeVars a.src a.dst b _ hnd hsd]
  dsimp only
  apply sum_map_congr
  intro u hu
  rw [← sum_filter_eq (if p.freeVars.contains a.dst then List.range n else [b a.dst])
    (fun w => old a.rel u w != new a.rel u w)
    (fun w => integrate n (p.freeVars.filter (fun v => v != a.src && v != a.dst))
      (put (put b a.src u) a.dst w)
      (fun x => (value new a x - value old a x) * cpExcl p.atoms old new j x))
    (by
      intro w hw hne
      have heq : old a.rel u w = new a.rel u w := by
        by_cases h : old a.rel u w == new a.rel u w
        · exact beq_iff_eq.mp h
        · simpa [h] using hne
      rw [pivot_point p n old new b j a hsd u w]
      simp [heq])]
  apply sum_map_congr
  intro w hw
  exact pivot_point p n old new b j a hsd u w

theorem anchored_repair_generic (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (hnd : p.freeVars.Nodup) (hsd : ∀ a ∈ p.atoms, a.src ≠ a.dst) :
    reference p n new b - reference p n old b =
      (p.atoms.zipIdx.map (fun (a, i) => pivotTerm p n old new b i a)).sum := by
  calc
    reference p n new b - reference p n old b
        = integrate n p.freeVars b (fun x =>
            (p.atoms.map (fun a => value new a x)).prod -
              (p.atoms.map (fun a => value old a x)).prod) := by
              rw [integrate_sub]; rfl
    _ = integrate n p.freeVars b (fun x => (p.atoms.zipIdx.map (fun (a, i) =>
          (value new a x - value old a x) * cpExcl p.atoms old new i x)).sum) := by
            apply integrate_congr
            intro x
            exact prod_diff_telescope p.atoms old new x
    _ = (p.atoms.zipIdx.map (fun (a, i) => integrate n p.freeVars b (fun x =>
          (value new a x - value old a x) * cpExcl p.atoms old new i x))).sum := by
            rw [integrate_list_sum]
    _ = (p.atoms.zipIdx.map (fun (a, i) => pivotTerm p n old new b i a)).sum := by
          apply sum_map_congr
          intro ai hai
          rcases ai with ⟨a, i⟩
          have hz := List.mem_zipIdx' hai
          have ham : a ∈ p.atoms := by
            rw [hz.2]
            exact List.getElem_mem hz.1
          exact pivot_integral_eq p n old new b i a hnd (hsd a ham)

/-! Cauchy--Schwarz for integer vectors (Lagrange identity). -/

theorem sq_nonneg (z : Int) : 0 ≤ z ^ 2 := Int.sq_nonneg z

theorem lagrange_id (n : Nat) (a b : Nat → Int) :
    rsum n (fun i => a i * a i) * rsum n (fun i => b i * b i) -
        (rsum n (fun i => a i * b i)) ^ 2 =
      rsum n (fun j => rsum j (fun i => (a j * b i - a i * b j) ^ 2)) := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      let A := rsum n (fun i => a i * a i)
      let B := rsum n (fun i => b i * b i)
      let S := rsum n (fun i => a i * b i)
      rw [rsum_succ, rsum_succ, rsum_succ, rsum_succ]
      change (A + a n * a n) * (B + b n * b n) - (S + a n * b n) ^ 2 =
        rsum n (fun j => rsum j (fun i => (a j * b i - a i * b j) ^ 2)) +
          rsum n (fun i => (a n * b i - a i * b n) ^ 2)
      rw [show (A + a n * a n) * (B + b n * b n) - (S + a n * b n) ^ 2 =
          (A * B - S ^ 2) + (A * (b n * b n) + B * (a n * a n) - 2 * S * (a n * b n)) by grind]
      rw [show A * B - S ^ 2 =
          rsum n (fun j => rsum j (fun i => (a j * b i - a i * b j) ^ 2)) by exact ih]
      congr 1
      have hfun : (fun i => (a n * b i - a i * b n) ^ 2) =
          (fun i => (a n * a n) * (b i * b i) + (b n * b n) * (a i * a i) -
            2 * a n * b n * (a i * b i)) := by
        funext i; grind
      rw [hfun, rsum_sub, rsum_add, rsum_const_mul, rsum_const_mul, rsum_const_mul]
      dsimp only [A, B, S]
      grind

theorem cauchy_schwarz (n : Nat) (a b : Nat → Int) :
    (rsum n (fun i => a i * b i)) ^ 2 ≤
      rsum n (fun i => a i * a i) * rsum n (fun i => b i * b i) := by
  have h : 0 ≤ rsum n (fun i => a i * a i) * rsum n (fun i => b i * b i) -
      (rsum n (fun i => a i * b i)) ^ 2 := by
    rw [lagrange_id]
    apply rsum_nonneg
    intro j hj
    exact rsum_nonneg j _ (fun i hi => sq_nonneg _)
  omega

/-! List Cauchy--Schwarz and flattened pair sums. -/

theorem sum_sq_nonneg2 (l : List (Int × Int)) (a b : Int) :
    0 ≤ (l.map (fun p => (a*p.2 - p.1*b)^2)).sum := by
  induction l with
  | nil => simp
  | cons p l ih =>
      simp only [List.map_cons, List.sum_cons]
      have hp := sq_nonneg (a*p.2 - p.1*b)
      omega

theorem cs_aux (l : List (Int × Int)) (a b : Int) :
    (l.map (fun p => p.1*p.1)).sum * (b*b) + (l.map (fun p => p.2*p.2)).sum * (a*a) -
      2*(l.map (fun p => p.1*p.2)).sum*a*b =
    (l.map (fun p => (a*p.2 - p.1*b)^2)).sum := by
  induction l with
  | nil => simp
  | cons p l ih => simp only [List.map_cons, List.sum_cons]; grind

theorem cs_list : ∀ l : List (Int × Int),
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
        have h2 := sum_sq_nonneg2 l a b
        omega
      omega

theorem rsum_pairs_flat (n : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum n (fun j => f i j)) =
      (((List.range n).flatMap (fun i => (List.range n).map (fun j => (i,j)))).map
        (fun p => f p.1 p.2)).sum := by
  have h : ∀ l : List Nat,
      (l.map (fun i => rsum n (fun j => f i j))).sum =
        ((l.flatMap (fun i => (List.range n).map (fun j => (i,j)))).map
          (fun p => f p.1 p.2)).sum := by
    intro l
    induction l with
    | nil => simp
    | cons i l ih =>
        simp only [List.map_cons, List.sum_cons, List.flatMap_cons]
        rw [ih]
        simp [rsum, List.map_append, List.sum_append, Function.comp_def]
  exact h (List.range n)

/-! Namespace MovieCredits. -/

namespace MovieCredits
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.MovieCredits

theorem sound_BoundaryMessages : BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference pattern n db b

theorem sound_AnchoredRepair : AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic pattern n old new b (by native_decide) (by native_decide)

theorem sound_TriangleNorm : TriangleNorm.Goal := by
  intro n db b
  simp [reference, integrate, pattern, value, put, normSq, dot, total]
  change (rsum n (fun x => rsum n (fun y => rsum n (fun z =>
      db 0 z x * (db 1 x y * db 2 y z))))) ^ 2 ≤
    (rsum n (fun z => rsum n (fun x => db 0 z x * db 0 z x))) *
    (rsum n (fun x => rsum n (fun y => db 1 x y * db 1 x y))) *
    (rsum n (fun y => rsum n (fun z => db 2 y z * db 2 y z)))
  let A : Nat → Nat → Int := fun z x => db 0 z x
  let B : Nat → Nat → Int := fun x y => db 1 x y
  let C : Nat → Nat → Int := fun y z => db 2 y z
  let E : Nat → Nat → Int := fun y z => rsum n (fun x => A z x * B x y)
  change (rsum n (fun x => rsum n (fun y => rsum n (fun z =>
      A z x * (B x y * C y z))))) ^ 2 ≤
    (rsum n (fun z => rsum n (fun x => A z x * A z x))) *
    (rsum n (fun x => rsum n (fun y => B x y * B x y))) *
    (rsum n (fun y => rsum n (fun z => C y z * C y z)))
  have hT : rsum n (fun x => rsum n (fun y => rsum n (fun z =>
      A z x * (B x y * C y z)))) =
      rsum n (fun y => rsum n (fun z => C y z * E y z)) := by
    calc
      rsum n (fun x => rsum n (fun y => rsum n (fun z => A z x * (B x y * C y z))))
          = rsum n (fun y => rsum n (fun x => rsum n (fun z => A z x * (B x y * C y z)))) := by
              rw [rsum_comm]
      _ = rsum n (fun y => rsum n (fun z => rsum n (fun x => A z x * (B x y * C y z)))) := by
              apply rsum_congr; intro y hy; rw [rsum_comm]
      _ = rsum n (fun y => rsum n (fun z => C y z * rsum n (fun x => A z x * B x y))) := by
              apply rsum_congr; intro y hy
              apply rsum_congr; intro z hz
              rw [show (fun x => A z x * (B x y * C y z)) =
                  (fun x => C y z * (A z x * B x y)) by funext x; ac_rfl]
              rw [rsum_const_mul]
      _ = rsum n (fun y => rsum n (fun z => C y z * E y z)) := rfl
  rw [hT]
  have hcs1 : (rsum n (fun y => rsum n (fun z => C y z * E y z))) ^ 2 ≤
      (rsum n (fun y => rsum n (fun z => C y z * C y z))) *
      (rsum n (fun y => rsum n (fun z => E y z * E y z))) := by
    repeat rw [rsum_pairs_flat]
    let ps := (List.range n).flatMap (fun i => (List.range n).map (fun j => (i,j)))
    have h := cs_list (ps.map (fun p : Nat × Nat => (C p.1 p.2, E p.1 p.2)))
    simpa [ps, List.map_map, Function.comp_def] using h
  have hE2 : ∀ y z, (E y z) ^ 2 ≤
      (rsum n (fun x => A z x * A z x)) * (rsum n (fun x => B x y * B x y)) := by
    intro y z
    dsimp only [E]
    exact cauchy_schwarz n (fun x => A z x) (fun x => B x y)
  have hsumE : rsum n (fun y => rsum n (fun z => E y z * E y z)) ≤
      (rsum n (fun z => rsum n (fun x => A z x * A z x))) *
      (rsum n (fun y => rsum n (fun x => B x y * B x y))) := by
    calc
      rsum n (fun y => rsum n (fun z => E y z * E y z))
          ≤ rsum n (fun y => rsum n (fun z =>
              (rsum n (fun x => A z x * A z x)) * (rsum n (fun x => B x y * B x y)))) := by
              apply rsum_le; intro y hy
              apply rsum_le; intro z hz
              have hz' := hE2 y z
              rw [Int.pow_succ, Int.pow_succ] at hz'
              simpa using hz'
      _ = rsum n (fun y => (rsum n (fun x => B x y * B x y)) *
              rsum n (fun z => rsum n (fun x => A z x * A z x))) := by
              apply rsum_congr; intro y hy
              rw [show (fun z => (rsum n (fun x => A z x * A z x)) *
                    (rsum n (fun x => B x y * B x y))) =
                  (fun z => (rsum n (fun x => B x y * B x y)) *
                    (rsum n (fun x => A z x * A z x))) by funext z; ac_rfl]
              rw [rsum_const_mul]
      _ = (rsum n (fun z => rsum n (fun x => A z x * A z x))) *
              rsum n (fun y => rsum n (fun x => B x y * B x y)) := by
              rw [show (fun y => (rsum n (fun x => B x y * B x y)) *
                    (rsum n (fun z => rsum n (fun x => A z x * A z x)))) =
                  (fun y => (rsum n (fun z => rsum n (fun x => A z x * A z x))) *
                    (rsum n (fun x => B x y * B x y))) by funext y; ac_rfl]
              rw [rsum_const_mul]
  have hCnon : 0 ≤ rsum n (fun y => rsum n (fun z => C y z * C y z)) := by
    apply rsum_nonneg; intro y hy
    apply rsum_nonneg; intro z hz
    rw [show C y z * C y z = (C y z) ^ 2 by rw [Int.pow_succ, Int.pow_succ]; simp]
    exact Int.sq_nonneg _
  calc
    (rsum n (fun y => rsum n (fun z => C y z * E y z))) ^ 2
        ≤ (rsum n (fun y => rsum n (fun z => C y z * C y z))) *
            (rsum n (fun y => rsum n (fun z => E y z * E y z))) := hcs1
    _ ≤ (rsum n (fun y => rsum n (fun z => C y z * C y z))) *
          ((rsum n (fun z => rsum n (fun x => A z x * A z x))) *
           (rsum n (fun y => rsum n (fun x => B x y * B x y)))) :=
          Int.mul_le_mul_of_nonneg_left hsumE hCnon
    _ = (rsum n (fun z => rsum n (fun x => A z x * A z x))) *
        (rsum n (fun x => rsum n (fun y => B x y * B x y))) *
        (rsum n (fun y => rsum n (fun z => C y z * C y z))) := by
          rw [rsum_comm n n (fun x y => B x y * B x y)]
          ac_rfl

end MovieCredits

/-! Namespace KeywordCompany. -/

namespace KeywordCompany
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.KeywordCompany

theorem sound_BoundaryMessages : BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference pattern n db b

theorem sound_AnchoredRepair : AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic pattern n old new b (by native_decide) (by native_decide)

theorem sound_TerminalKey : TerminalKey.Goal := by
  intro n db b
  simp [reference, integrate, pattern, value, put, bridge, dot, total]
  change rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      db 0 (b 0) x1 * (db 1 (b 0) x2 * (db 2 x1 x3 * db 3 x2 x3))))) =
    rsum n (fun t => (rsum n (fun k => db 0 (b 0) k * db 2 k t)) *
                      (rsum n (fun k => db 1 (b 0) k * db 3 k t)))
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
        db 0 (b 0) x1 * (db 1 (b 0) x2 * (db 2 x1 x3 * db 3 x2 x3)))))
        = rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x2 =>
          db 0 (b 0) x1 * (db 1 (b 0) x2 * (db 2 x1 x3 * db 3 x2 x3))))) := by
            apply rsum_congr; intro x1 hx1; rw [rsum_comm]
    _ = rsum n (fun x3 => rsum n (fun x1 => rsum n (fun x2 =>
          db 0 (b 0) x1 * (db 1 (b 0) x2 * (db 2 x1 x3 * db 3 x2 x3))))) := by
          rw [rsum_comm]
    _ = rsum n (fun x3 => rsum n (fun x1 => rsum n (fun x2 =>
          (db 0 (b 0) x1 * db 2 x1 x3) * (db 1 (b 0) x2 * db 3 x2 x3)))) := by
          apply rsum_congr; intro x3 h3
          apply rsum_congr; intro x1 h1
          apply rsum_congr; intro x2 h2
          ac_rfl
    _ = rsum n (fun x3 => (rsum n (fun x1 => db 0 (b 0) x1 * db 2 x1 x3)) *
                           (rsum n (fun x2 => db 1 (b 0) x2 * db 3 x2 x3))) := by
          apply rsum_congr; intro x3 h3; rw [rsum_product]

end KeywordCompany

/-! Namespace CastOverlap. -/

namespace CastOverlap
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.CastOverlap

theorem sound_BoundaryMessages : BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference pattern n db b

theorem sound_AnchoredRepair : AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic pattern n old new b (by native_decide) (by native_decide)

theorem sound_GramSquare : GramSquare.Goal := by
  intro n db b
  simp [reference, integrate, pattern, value, put, dot, total, overlap]
  change rsum n (fun z1 => rsum n (fun z2 => rsum n (fun x => rsum n (fun y =>
      db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2)))))) =
    rsum n (fun x => rsum n (fun y => (rsum n (fun k => db 0 x k * db 0 y k)) ^ 2))
  calc
    rsum n (fun z1 => rsum n (fun z2 => rsum n (fun x => rsum n (fun y =>
        db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2))))))
        = rsum n (fun z1 => rsum n (fun x => rsum n (fun z2 => rsum n (fun y =>
          db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2)))))) := by
          apply rsum_congr; intro z1 hz1
          exact rsum_comm n n (fun z2 x => rsum n (fun y =>
            db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2))))
    _ = rsum n (fun x => rsum n (fun z1 => rsum n (fun z2 => rsum n (fun y =>
          db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2)))))) :=
          rsum_comm n n (fun z1 x => rsum n (fun z2 => rsum n (fun y =>
            db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2)))))
    _ = rsum n (fun x => rsum n (fun z1 => rsum n (fun y => rsum n (fun z2 =>
          db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2)))))) := by
          apply rsum_congr; intro x hx
          apply rsum_congr; intro z1 hz1
          exact rsum_comm n n (fun z2 y =>
            db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2)))
    _ = rsum n (fun x => rsum n (fun y => rsum n (fun z1 => rsum n (fun z2 =>
          db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2)))))) := by
          apply rsum_congr; intro x hx
          exact rsum_comm n n (fun z1 y => rsum n (fun z2 =>
            db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2))))
    _ = rsum n (fun x => rsum n (fun y =>
          (rsum n (fun z1 => db 0 x z1 * db 0 y z1)) *
          (rsum n (fun z2 => db 0 y z2 * db 0 x z2)))) := by
          apply rsum_congr; intro x hx
          apply rsum_congr; intro y hy
          rw [show rsum n (fun z1 => rsum n (fun z2 =>
                db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2)))) =
              rsum n (fun z1 => rsum n (fun z2 =>
                (db 0 x z1 * db 0 y z1) * (db 0 y z2 * db 0 x z2))) by
            apply rsum_congr; intro z1 h1
            apply rsum_congr; intro z2 h2
            ac_rfl]
          rw [rsum_product]
    _ = rsum n (fun x => rsum n (fun y => (rsum n (fun k => db 0 x k * db 0 y k)) ^ 2)) := by
          apply rsum_congr; intro x hx
          apply rsum_congr; intro y hy
          rw [show (fun z2 => db 0 y z2 * db 0 x z2) = (fun z2 => db 0 x z2 * db 0 y z2) by
            funext z2; ac_rfl]
          rw [Int.pow_succ, Int.pow_succ]
          simp

end CastOverlap

/-! Namespace DoubleBilling. -/

namespace DoubleBilling
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.DoubleBilling

theorem sound_BoundaryMessages : BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference pattern n db b

theorem sound_AnchoredRepair : AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic pattern n old new b (by native_decide) (by native_decide)

end DoubleBilling

/-! Namespace PostDiamond. -/

namespace PostDiamond
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.PostDiamond

theorem sound_BoundaryMessages : BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference pattern n db b

theorem sound_AnchoredRepair : AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic pattern n old new b (by native_decide) (by native_decide)

end PostDiamond

/-! Second differences and the two remaining factorization goals. -/

theorem diff2_add (f g : Int → Int) :
    diff 2 (fun u => f u + g u) 0 = diff 2 f 0 + diff 2 g 0 := by
  simp [diff]; omega

theorem diff2_rsum (n : Nat) (f : Nat → Int → Int) :
    diff 2 (fun u => rsum n (fun i => f i u)) 0 = rsum n (fun i => diff 2 (f i) 0) := by
  induction n with
  | zero => simp [rsum, diff]
  | succ n ih =>
      rw [show (fun u => rsum (n+1) (fun i => f i u)) =
          (fun u => rsum n (fun i => f i u) + f n u) by funext u; rw [rsum_succ]]
      rw [diff2_add, ih, rsum_succ]

theorem diff2_square_affine (A d K : Int) :
    diff 2 (fun u => (A + u*d) * (A + u*d) * K) 0 = 2*d*d*K := by
  simp [diff]; grind

namespace DoubleBilling
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.DoubleBilling

theorem double_eval (n : Nat) (db delta : DB) (b : Bind) (t : Int)
    (hdelta : ∀ r x y, r ≠ 0 → delta r x y = 0) :
    reference pattern n (slope db delta t) b =
      rsum n (fun v => (db 0 (b 0) v + t*delta 0 (b 0) v) *
        ((db 0 (b 0) v + t*delta 0 (b 0) v) *
          (rsum n (db 2 (b 0)) * rsum n (db 1 v)))) := by
  simp [reference, integrate, pattern, value, put, slope, hdelta]
  change rsum n (fun v => rsum n (fun u => rsum n (fun w =>
      (db 0 (b 0) v + t*delta 0 (b 0) v) *
      ((db 0 (b 0) v + t*delta 0 (b 0) v) *
        (db 1 v w * db 2 (b 0) u))))) = _
  apply rsum_congr
  intro v hv
  rw [rsum_comm]
  rw [show (fun j => rsum n (fun i =>
      (db 0 (b 0) v + t*delta 0 (b 0) v) *
      ((db 0 (b 0) v + t*delta 0 (b 0) v) *
        (db 1 v j * db 2 (b 0) i)))) =
      (fun j => rsum n (fun i =>
        ((db 0 (b 0) v + t*delta 0 (b 0) v) *
          ((db 0 (b 0) v + t*delta 0 (b 0) v) * db 2 (b 0) i)) * db 1 v j)) by
    funext j
    apply rsum_congr
    intro i hi
    ac_rfl]
  rw [← rsum_comm, rsum_product]
  rw [rsum_const_mul, rsum_const_mul]
  ac_rfl

theorem double_delta_eval (n : Nat) (db delta : DB) (b : Bind)
    (hdelta : ∀ r x y, r ≠ 0 → delta r x y = 0) :
    reference pattern n (fun r => if r == 0 then delta 0 else db r) b =
      rsum n (fun v => delta 0 (b 0) v *
        (delta 0 (b 0) v * (rsum n (db 2 (b 0)) * rsum n (db 1 v)))) := by
  simp [reference, integrate, pattern, value, put]
  change rsum n (fun v => rsum n (fun u => rsum n (fun w =>
      delta 0 (b 0) v * (delta 0 (b 0) v *
        (db 1 v w * db 2 (b 0) u))))) = _
  apply rsum_congr
  intro v hv
  rw [rsum_comm]
  rw [show (fun j => rsum n (fun i => delta 0 (b 0) v *
        (delta 0 (b 0) v * (db 1 v j * db 2 (b 0) i)))) =
      (fun j => rsum n (fun i =>
        (delta 0 (b 0) v * (delta 0 (b 0) v * db 2 (b 0) i)) * db 1 v j)) by
    funext j
    apply rsum_congr
    intro i hi
    ac_rfl]
  rw [← rsum_comm, rsum_product]
  rw [rsum_const_mul, rsum_const_mul]
  ac_rfl

theorem sound_SquaredDelta : SquaredDelta.Goal := by
  intro n db delta b hdelta
  change diff 2 (fun u => reference pattern n (slope db delta u) b) 0 = _
  have heval : (fun u => reference pattern n (slope db delta u) b) =
      (fun u => rsum n (fun v =>
        (db 0 (b 0) v + u*delta 0 (b 0) v) *
          ((db 0 (b 0) v + u*delta 0 (b 0) v) *
            (rsum n (db 2 (b 0)) * rsum n (db 1 v))))) := by
    funext u
    exact double_eval n db delta b u hdelta
  rw [heval]
  calc
    diff 2 (fun u => rsum n (fun v =>
        (db 0 (b 0) v + u*delta 0 (b 0) v) *
          ((db 0 (b 0) v + u*delta 0 (b 0) v) *
            (rsum n (db 2 (b 0)) * rsum n (db 1 v))))) 0
        = rsum n (fun v => diff 2 (fun u =>
            (db 0 (b 0) v + u*delta 0 (b 0) v) *
              ((db 0 (b 0) v + u*delta 0 (b 0) v) *
                (rsum n (db 2 (b 0)) * rsum n (db 1 v)))) 0) :=
          diff2_rsum n _
    _ = 2 * rsum n (fun v => delta 0 (b 0) v *
          (delta 0 (b 0) v * (rsum n (db 2 (b 0)) * rsum n (db 1 v)))) := by
          rw [← rsum_const_mul]
          apply rsum_congr
          intro v hv
          simpa [Int.mul_assoc] using
            diff2_square_affine (db 0 (b 0) v) (delta 0 (b 0) v)
              (rsum n (db 2 (b 0)) * rsum n (db 1 v))
    _ = 2 * reference pattern n (fun r => if r == 0 then delta 0 else db r) b := by
          rw [double_delta_eval n db delta b hdelta]

end DoubleBilling

namespace PostDiamond
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.PostDiamond

theorem sound_SquarePathCounts : SquarePathCounts.Goal := by
  intro n db b
  simp [reference, integrate, pattern, value, put, bridge, total]
  change rsum n (fun x => rsum n (fun y => rsum n (fun t =>
      db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t))))) =
    rsum n (fun t => (rsum n (fun k => db 0 (b 0) k * db 0 k t)) ^ 2)
  calc
    rsum n (fun x => rsum n (fun y => rsum n (fun t =>
        db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t)))))
        = rsum n (fun x => rsum n (fun t => rsum n (fun y =>
          db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t))))) := by
            apply rsum_congr; intro x hx; rw [rsum_comm]
    _ = rsum n (fun t => rsum n (fun x => rsum n (fun y =>
          db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t))))) :=
          rsum_comm n n (fun x t => rsum n (fun y =>
            db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t))))
    _ = rsum n (fun t => rsum n (fun x => rsum n (fun y =>
          (db 0 (b 0) x * db 0 x t) * (db 0 (b 0) y * db 0 y t)))) := by
          apply rsum_congr; intro t ht
          apply rsum_congr; intro x hx
          apply rsum_congr; intro y hy
          ac_rfl
    _ = rsum n (fun t => (rsum n (fun x => db 0 (b 0) x * db 0 x t)) *
          (rsum n (fun y => db 0 (b 0) y * db 0 y t))) := by
          apply rsum_congr; intro t ht; rw [rsum_product]
    _ = rsum n (fun t => (rsum n (fun k => db 0 (b 0) k * db 0 k t)) ^ 2) := by
          apply rsum_congr; intro t ht
          rw [Int.pow_succ, Int.pow_succ]; simp

end PostDiamond

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieJoinBoundaryMessages
