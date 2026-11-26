import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved (15 claims, including the following)

- `CoLikes.AnchoredRepair` — `∀ n old new b, reference pattern n new b - reference
  pattern n old b = (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old
  new b i a)).sum`
- `CoLikes.BoundaryMessages` — `∀ n db b, elimination pattern n db b = reference
  pattern n db b`
- `CoLikes.DistinctMessages` — `∀ n db b, integrate n pattern.freeVars b (fun x => if x
  2 == x 3 then 0 else product (factors pattern db) x) = total n (fun u => db 2 u (b
  1) * ((overlap n db 0 u (b 1))^2 - total n (fun j => (db 0 u j * db 0 (b 1)
  j)^2)))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.SocialJoinBoundaryMessages

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total dot)

/-! Basic finite sums and integration. -/

private abbrev rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem sum_congr {α} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem sum_add {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

private theorem sum_sub {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x - g x)).sum = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]; omega

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
  | nil =>
      simp only [List.map_nil, List.sum_nil]
      exact (sum_zero l').symm
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

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := sum_mul_left (List.range n) c f

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := sum_mul_right (List.range n) f c

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      simp only [rsum, List.range_zero, List.map_nil, List.sum_nil]
      exact (sum_zero (List.range m)).symm
  | succ n ih =>
      rw [rsum_succ, ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr
        intro j hj
        exact rsum_succ n (fun i => f i j)]
      exact (rsum_add m (fun j => rsum n (fun i => f i j)) (fun j => f n j)).symm

private theorem rsum_product (n m : Nat) (f g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr
            intro i hi
            exact rsum_const_mul m (f i) g
    _ = rsum n f * rsum m g := rsum_mul_const n f (rsum m g)

private theorem put_self (b : Bind) (v x : Nat) : put b v x v = x := by simp [put]

private theorem put_put_same (b : Bind) (v x y : Nat) : put (put b v x) v y = put b v y := by
  funext i
  by_cases h : i == v <;> simp [put, h]

private theorem put_same (b : Bind) (v : Nat) : put b v (b v) = b := by
  funext i
  by_cases h : i == v
  · have hi : i = v := Nat.eq_of_beq_eq_true h
    subst i
    simp [put]
  · simp [put, h]

private theorem put_comm_ne (b : Bind) (v w x y : Nat) (h : v ≠ w) :
    put (put b v x) w y = put (put b w y) v x := by
  funext i
  by_cases hi : i = v <;> by_cases hj : i = w <;> simp_all [put]

private theorem integrate_congr (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ x, f x = g x) : integrate n vs b f = integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum
      exact sum_congr _ _ _ (fun x hx => ih (put b v x))

private theorem integrate_swap (n v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: w :: vs) b f = integrate n (w :: v :: vs) b f := by
  by_cases hvw : v = w
  · subst w
    rfl
  change ((List.range n).map (fun x =>
      ((List.range n).map (fun y => integrate n vs (put (put b v x) w y) f)).sum)).sum =
    ((List.range n).map (fun x =>
      ((List.range n).map (fun y => integrate n vs (put (put b w x) v y) f)).sum)).sum
  rw [sum_swap]
  apply sum_congr
  intro x hx
  apply sum_congr
  intro y hy
  rw [put_comm_ne b v w y x hvw]

private theorem integrate_rotate (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
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
        ((List.range n).map (fun x => integrate n ws (put b w x)
          (fun b' => integrate n [v] b' f))).sum
      exact sum_congr _ _ _ (fun x hx => ih (put b w x))

private theorem integrate_cons_singleton (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: vs) b f = integrate n vs b (fun b' => integrate n [v] b' f) := by
  rw [integrate_rotate, integrate_snoc]

private theorem integrate_append (n : Nat) (xs ys : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (xs ++ ys) b f = integrate n xs b (fun b' => integrate n ys b' f) := by
  induction xs generalizing b with
  | nil => rfl
  | cons x xs ih =>
      change ((List.range n).map (fun a => integrate n (xs ++ ys) (put b x a) f)).sum =
        ((List.range n).map (fun a => integrate n xs (put b x a)
          (fun b' => integrate n ys b' f))).sum
      exact sum_congr _ _ _ (fun a ha => ih (put b x a))

private theorem integrate_perm (n : Nat) {xs ys : List Nat} (h : xs.Perm ys) :
    ∀ (b : Bind) (f : Bind → Int), integrate n xs b f = integrate n ys b f := by
  induction h with
  | nil => intro b f; rfl
  | cons x h ih =>
      intro b f
      change ((List.range n).map (fun t => integrate n _ (put b x t) f)).sum =
        ((List.range n).map (fun t => integrate n _ (put b x t) f)).sum
      exact sum_congr _ _ _ (fun t ht => ih (put b x t) f)
  | swap x y l => intro b f; exact integrate_swap n y x l b f
  | trans h1 h2 ih1 ih2 => intro b f; rw [ih1 b f, ih2 b f]

private theorem integrate_add (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x + g x) = integrate n vs b f + integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_add]

private theorem integrate_sub (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x - g x) = integrate n vs b f - integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_sub]

private theorem integrate_mul_left (n : Nat) (vs : List Nat) (b : Bind) (c : Int) (f : Bind → Int) :
    integrate n vs b (fun x => c * f x) = c * integrate n vs b f := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_mul_left]

private theorem integrate_mul_right (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) (c : Int) :
    integrate n vs b (fun x => f x * c) = integrate n vs b f * c := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_mul_right]

private theorem integrate_zero (n : Nat) (vs : List Nat) (b : Bind) :
    integrate n vs b (fun _ => (0 : Int)) = 0 := by
  induction vs generalizing b with
  | nil => rfl
  | cons v vs ih => simp [integrate, ih, sum_zero]

private theorem integrate_list_sum {α} (n : Nat) (vs : List Nat) (b : Bind)
    (l : List α) (F : α → Bind → Int) :
    integrate n vs b (fun x => (l.map (fun a => F a x)).sum) =
      (l.map (fun a => integrate n vs b (F a))).sum := by
  induction l with
  | nil => simp [integrate_zero]
  | cons a l ih => simp only [List.map_cons, List.sum_cons]; rw [integrate_add, ih]

/-! Variable elimination is exact for scoped factors. -/

private def Respects (f : Factor) : Prop :=
  ∀ b b', (∀ v, f.scope.contains v = true → b v = b' v) → f.get b = f.get b'

private theorem respects_value (db : DB) (a : Atom) :
    Respects ⟨[a.src, a.dst], value db a⟩ := by
  intro b b' h
  simp only [value]
  rw [h a.src (by simp), h a.dst (by simp)]

private theorem product_respects_eq {fs : List Factor} (h : ∀ f ∈ fs, Respects f)
    {b b' : Bind} (heq : ∀ f ∈ fs, ∀ v, f.scope.contains v = true → b v = b' v) :
    product fs b = product fs b' := by
  induction fs with
  | nil => rfl
  | cons f fs ih =>
      simp only [product, List.map_cons, List.prod_cons]
      rw [h f (by simp) b b' (heq f (by simp))]
      change f.get b' * product fs b = f.get b' * product fs b'
      rw [ih (fun g hg => h g (by simp [hg])) (fun g hg => heq g (by simp [hg]))]

private theorem product_put_eq {fs : List Factor} (h : ∀ f ∈ fs, Respects f)
    (hnot : ∀ f ∈ fs, f.scope.contains v = false) (b : Bind) (x : Nat) :
    product fs (put b v x) = product fs b := by
  apply product_respects_eq h
  intro f hf w hw
  by_cases hvw : w == v
  · have hwv : w = v := Nat.eq_of_beq_eq_true hvw
    subst w
    rw [hnot f hf] at hw
    simp at hw
  · simp [put, hvw]

private theorem partition_filters (p : α → Bool) (l : List α) :
    (List.partition p l).fst = l.filter p ∧
    (List.partition p l).snd = l.filter (fun x => !p x) := by
  have h : ∀ (l a b : List α),
      (List.partition.loop p l (a, b)).fst = a.reverse ++ l.filter p ∧
      (List.partition.loop p l (a, b)).snd = b.reverse ++ l.filter (fun x => !p x) := by
    intro l
    induction l with
    | nil => intro a b; simp [List.partition.loop]
    | cons x xs ih =>
        intro a b
        by_cases hx : p x = true <;> simp [List.partition.loop, hx, ih]
  simpa [List.partition] using h l [] []

private theorem mem_partition_fst {l : List Factor} {v : Nat} {f : Factor}
    (hf : f ∈ (List.partition (fun g : Factor => g.scope.contains v) l).fst) : f ∈ l := by
  rw [(partition_filters (fun g : Factor => g.scope.contains v) l).1] at hf
  exact (List.mem_filter.mp hf).1

private theorem mem_partition_snd {l : List Factor} {v : Nat} {f : Factor}
    (hf : f ∈ (List.partition (fun g : Factor => g.scope.contains v) l).snd) : f ∈ l := by
  rw [(partition_filters (fun g : Factor => g.scope.contains v) l).2] at hf
  exact (List.mem_filter.mp hf).1

private theorem mem_partition_notp (p : Factor → Bool) {l : List Factor} {a : Factor}
    (hf : a ∈ (List.partition p l).snd) : p a = false := by
  rw [(partition_filters p l).2] at hf
  have hn := (List.mem_filter.mp hf).2
  by_cases hp : p a = true
  · simp [hp] at hn
  · simpa using hp

private theorem product_filter_mul (p : α → Bool) (l : List α) (g : α → Int) :
    ((l.filter p).map g).prod * ((l.filter (fun x => !p x)).map g).prod = (l.map g).prod := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases h : p a = true <;>
        simp [h, ih, Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]

private theorem product_partition_get (p : Factor → Bool) (l : List Factor) (b : Bind) :
    product (List.partition p l).fst b * product (List.partition p l).snd b = product l b := by
  dsimp only [product]
  rw [(partition_filters p l).1, (partition_filters p l).2]
  exact product_filter_mul p l (fun f => f.get b)

private theorem respects_msg (n v : Nat) (inc : List Factor) (hres : ∀ f ∈ inc, Respects f) :
    Respects ⟨(inc.flatMap Factor.scope).eraseDups.filter (fun w => w != v),
      fun b => integrate n [v] b (product inc)⟩ := by
  intro b b' hb
  change ((List.range n).map (fun x => product inc (put b v x))).sum =
    ((List.range n).map (fun x => product inc (put b' v x))).sum
  apply sum_congr
  intro x hx
  apply product_respects_eq hres
  intro f hf w hw
  by_cases hwv : w == v
  · have hwv' : w = v := Nat.eq_of_beq_eq_true hwv
    subst w
    simp [put_self]
  · simp only [put, hwv, if_false]
    apply hb
    have hwmem : w ∈ f.scope := by simpa using hw
    have hwflat : w ∈ inc.flatMap Factor.scope := List.mem_flatMap.mpr ⟨f, hf, hwmem⟩
    have hwErase : w ∈ (inc.flatMap Factor.scope).eraseDups := List.mem_eraseDups.mpr hwflat
    have hwne : (w != v) = true := by simpa using hwv
    simpa only [List.contains_iff_mem] using
      (List.mem_filter (p := fun w : Nat => w != v)).mpr ⟨hwErase, hwne⟩

private theorem product_eliminate (n v : Nat) (fs : List Factor) (b : Bind)
    (hres : ∀ f ∈ fs, Respects f) :
    product (eliminate n v fs) b = integrate n [v] b (product fs) := by
  dsimp only [eliminate]
  let inc := (fs.partition (fun f : Factor => f.scope.contains v)).fst
  let rest := (fs.partition (fun f : Factor => f.scope.contains v)).snd
  have hinc_res : ∀ f ∈ inc, Respects f := fun f hf => hres f (mem_partition_fst hf)
  have hrest_res : ∀ f ∈ rest, Respects f := fun f hf => hres f (mem_partition_snd hf)
  have hrest_not : ∀ f ∈ rest, f.scope.contains v = false :=
    fun f hf => mem_partition_notp _ hf
  change (integrate n [v] b (fun x => product inc x)) * product rest b =
    integrate n [v] b (product fs)
  change ((List.range n).map (fun x => product inc (put b v x))).sum * product rest b =
    ((List.range n).map (fun x => product fs (put b v x))).sum
  rw [← sum_mul_right]
  apply sum_congr
  intro x hx
  rw [← product_partition_get (fun f : Factor => f.scope.contains v) fs (put b v x)]
  rw [product_put_eq hrest_res hrest_not b x]

private theorem respects_eliminate (n v : Nat) (fs : List Factor) (hres : ∀ f ∈ fs, Respects f) :
    ∀ f ∈ eliminate n v fs, Respects f := by
  intro f hf
  dsimp only [eliminate] at hf
  simp only [List.mem_cons] at hf
  rcases hf with rfl | hf
  · exact respects_msg n v _ (fun g hg => hres g (mem_partition_fst hg))
  · exact hres f (mem_partition_snd hf)

private theorem product_fold_eliminate (n : Nat) (vs : List Nat) (fs : List Factor) (b : Bind)
    (hres : ∀ f ∈ fs, Respects f) :
    product (vs.foldl (fun fs v => eliminate n v fs) fs) b = integrate n vs b (product fs) := by
  induction vs generalizing fs with
  | nil => rfl
  | cons v vs ih =>
      change product (vs.foldl (fun fs v => eliminate n v fs) (eliminate n v fs)) b =
        integrate n (v :: vs) b (product fs)
      rw [ih (eliminate n v fs) (respects_eliminate n v fs hres), integrate_cons_singleton]
      exact integrate_congr n vs b _ _ (fun x => product_eliminate n v fs x hres)

private theorem elimination_eq_reference (p : Pattern) (n : Nat) (db : DB) (b : Bind) :
    elimination p n db b = reference p n db b := by
  unfold elimination reference
  rw [product_fold_eliminate]
  · exact integrate_congr n p.freeVars b _ _ (fun x => by
      simp [factors, product, Function.comp_def])
  · intro f hf
    rcases List.mem_map.mp hf with ⟨a, ha, rfl⟩
    exact respects_value db a

/-! Anchored repair: telescope the product, then pin the changed atom. -/

private theorem put_get_other (b : Bind) (v w x : Nat) (h : w ≠ v) :
    put b v x w = b w := by
  have h' : (w == v) = false := by
    cases hw : w == v <;> simp_all
  simp [put, h']

private theorem put_src_dst_value (b : Bind) (src dst u v : Nat) (hne : src ≠ dst) :
    put (put b src u) dst v src = u := by
  rw [put_get_other (put b src u) dst src v hne]
  simp [put]

private theorem put_dst_dst_value (b : Bind) (src dst u v : Nat) :
    put (put b src u) dst v dst = v := by simp [put]

private theorem integrate_fixed (n : Nat) (vs : List Nat) (b : Bind) (v : Nat)
    (hv : v ∉ vs) (F : Nat → Bind → Int) :
    integrate n vs b (fun x => F (x v) x) = integrate n vs b (fun x => F (b v) x) := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      have hvw : v ≠ w := by intro h; subst w; exact hv (by simp)
      have hvws : v ∉ ws := by intro h; exact hv (by simp [h])
      change ((List.range n).map (fun t => integrate n ws (put b w t)
        (fun x => F (x v) x))).sum =
        ((List.range n).map (fun t => integrate n ws (put b w t)
          (fun x => F (b v) x))).sum
      apply sum_congr
      intro t ht
      rw [ih (put b w t) hvws, put_get_other b w v t hvw]

private theorem integrate_fixed2 (n : Nat) (vs : List Nat) (b : Bind) (v w : Nat)
    (hv : v ∉ vs) (hw : w ∉ vs) (F : Nat → Nat → Bind → Int) :
    integrate n vs b (fun x => F (x v) (x w) x) =
      integrate n vs b (fun x => F (b v) (b w) x) := by
  calc
    integrate n vs b (fun x => F (x v) (x w) x)
        = integrate n vs b (fun x => F (b v) (x w) x) :=
          integrate_fixed n vs b v hv (fun u x => F u (x w) x)
    _ = integrate n vs b (fun x => F (b v) (b w) x) :=
          integrate_fixed n vs b w hw (fun u x => F (b v) u x)

private theorem sum_filter_eq {α} (l : List α) (p : α → Bool) (g : α → Int)
    (h : ∀ x ∈ l, p x = false → g x = 0) :
    ((l.filter p).map g).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases hx : p x = true
      · simp [hx, ih (fun y hy hp => h y (by simp [hy]) hp)]
      · have hx0 : p x = false := by cases h' : p x <;> simp_all
        have hz : g x = 0 := h x (by simp) hx0
        simp [hx, hz, ih (fun y hy hp => h y (by simp [hy]) hp)]

private theorem filter_ne_eq_erase (vs : List Nat) (a : Nat) (hnd : vs.Nodup) :
    vs.filter (fun v => v != a) = vs.erase a := by
  induction vs with
  | nil => rfl
  | cons x xs ih =>
      have hp := List.pairwise_cons.mp hnd
      have hnd' : xs.Nodup := hp.2
      by_cases hxa : x == a
      · have hx : x = a := Nat.eq_of_beq_eq_true hxa
        subst x
        have hna : a ∉ xs := by intro h; exact hp.1 a h rfl
        simp [List.erase_cons_head, hna, ih hnd']
      · have hxa' : (x != a) = true := by simpa using hxa
        simp [List.erase_cons_tail hxa, hxa', ih hnd']

private theorem filter_and_ne_of_not_mem (l : List Nat) (a b : Nat) (ha : a ∉ l) :
    l.filter (fun v => v != a && v != b) = l.filter (fun v => v != b) := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have hxna : x ≠ a := by intro h; subst x; exact ha (by simp)
      have hxne : (x != a) = true := by simpa using hxna
      have haxs : a ∉ xs := by intro h; exact ha (by simp [h])
      by_cases hxb : x == b
      · have hx : x = b := Nat.eq_of_beq_eq_true hxb
        subst x
        simp [ih haxs]
      · have hxb' : (x != b) = true := by simpa using hxb
        simp [hxne, hxb', ih haxs]

private theorem filter_and_ne_of_not_mem2 (l : List Nat) (a b : Nat) (hb : b ∉ l) :
    l.filter (fun v => v != a && v != b) = l.filter (fun v => v != a) := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have hxb : x ≠ b := by intro h; subst x; exact hb (by simp)
      have hxb' : (x != b) = true := by simpa using hxb
      have hbxs : b ∉ xs := by intro h; exact hb (by simp [h])
      by_cases hxa : x == a
      · have hx : x = a := Nat.eq_of_beq_eq_true hxa
        subst x
        simp [ih hbxs]
      · have hxa' : (x != a) = true := by simpa using hxa
        simp [hxa', hxb', ih hbxs]

private theorem erase_erase_filter (vs : List Nat) (a b : Nat) (hnd : vs.Nodup) :
    (vs.erase a).erase b = vs.filter (fun v => v != a && v != b) := by
  induction vs with
  | nil => rfl
  | cons x xs ih =>
      have hp := List.pairwise_cons.mp hnd
      have hnd' : xs.Nodup := hp.2
      by_cases hxa : x == a
      · have hx : x = a := Nat.eq_of_beq_eq_true hxa
        subst x
        have hna : a ∉ xs := by intro h; exact hp.1 a h rfl
        rw [List.erase_cons_head]
        change xs.erase b = (a :: xs).filter (fun v => v != a && v != b)
        simp only [List.filter_cons]
        rw [if_neg (by simp)]
        rw [filter_and_ne_of_not_mem xs a b hna]
        exact (filter_ne_eq_erase xs b hnd').symm
      · by_cases hxb : x == b
        · have hx : x = b := Nat.eq_of_beq_eq_true hxb
          subst x
          have hnb : b ∉ xs := by intro h; exact hp.1 b h rfl
          have hbne : b ≠ a := by simpa using hxa
          have hnb' : b ∉ xs.erase a := by
            intro hb
            exact hnb ((List.mem_erase_of_ne (a := b) (b := a) hbne).mp hb)
          have hih := ih hnd'
          rw [List.erase_of_not_mem hnb'] at hih
          rw [List.erase_cons_tail hxa, List.erase_cons_head]
          change xs.erase a = (b :: xs).filter (fun v => v != a && v != b)
          simp only [List.filter_cons]
          rw [if_neg (by simp)]
          simpa using hih
        · have hxa' : (x != a) = true := by simpa using hxa
          have hxb' : (x != b) = true := by simpa using hxb
          rw [List.erase_cons_tail hxa, List.erase_cons_tail hxb]
          simp [hxa', hxb', ih hnd']

private theorem integrate_pivot (n : Nat) (vs : List Nat) (src dst : Nat) (b : Bind)
    (f : Bind → Int) (hnd : vs.Nodup) (hne : src ≠ dst) :
    integrate n vs b f =
      let cand := fun v => if vs.contains v then List.range n else [b v]
      let vars := vs.filter (fun v => v != src && v != dst)
      ((cand src).map (fun u => ((cand dst).map (fun w =>
        integrate n vars (put (put b src u) dst w) f)).sum)).sum := by
  dsimp only
  by_cases hsrc : vs.contains src = true
  · have hsrcm : src ∈ vs := by simpa using hsrc
    by_cases hdst : vs.contains dst = true
    · have hdstm : dst ∈ vs := by simpa using hdst
      have hvars : (vs.erase src).erase dst =
          vs.filter (fun v => v != src && v != dst) := erase_erase_filter vs src dst hnd
      have hdstm_erase : dst ∈ vs.erase src :=
        (List.mem_erase_of_ne (a := dst) (b := src) hne.symm).mpr hdstm
      have h1 : integrate n vs b f = integrate n (src :: vs.erase src) b f :=
        integrate_perm n (List.perm_cons_erase hsrcm) b f
      rw [h1, if_pos hsrc, if_pos hdst]
      change ((List.range n).map (fun u => integrate n (vs.erase src) (put b src u) f)).sum =
        ((List.range n).map (fun u => ((List.range n).map (fun w =>
          integrate n (vs.filter (fun v => v != src && v != dst))
            (put (put b src u) dst w) f)).sum)).sum
      apply sum_congr
      intro u hu
      have h2 : integrate n (vs.erase src) (put b src u) f =
          integrate n (dst :: (vs.erase src).erase dst) (put b src u) f :=
        integrate_perm n (List.perm_cons_erase hdstm_erase) (put b src u) f
      rw [h2]
      change ((List.range n).map (fun w => integrate n ((vs.erase src).erase dst)
        (put (put b src u) dst w) f)).sum =
        ((List.range n).map (fun w => integrate n (vs.filter (fun v => v != src && v != dst))
          (put (put b src u) dst w) f)).sum
      rw [hvars]
    · have hdstm : dst ∉ vs := by simpa using hdst
      have hvars : vs.filter (fun v => v != src && v != dst) = vs.erase src := by
        rw [filter_and_ne_of_not_mem2 vs src dst hdstm]
        exact filter_ne_eq_erase vs src hnd
      have h1 : integrate n vs b f = integrate n (src :: vs.erase src) b f :=
        integrate_perm n (List.perm_cons_erase hsrcm) b f
      rw [h1, if_pos hsrc, if_neg hdst]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
      change ((List.range n).map (fun u => integrate n (vs.erase src) (put b src u) f)).sum = _
      apply sum_congr
      intro u hu
      rw [hvars]
      have hp : put (put b src u) dst (b dst) = put b src u := by
        funext i
        by_cases hid : i == dst
        · have hi : i = dst := Nat.eq_of_beq_eq_true hid
          subst i
          have hds : (dst == src) = false := by
            cases h : dst == src <;> simp_all
          simp [put, hds]
        · simp [put, hid]
      rw [hp]
  · have hsrcm : src ∉ vs := by simpa using hsrc
    by_cases hdst : vs.contains dst = true
    · have hdstm : dst ∈ vs := by simpa using hdst
      have hvars : vs.filter (fun v => v != src && v != dst) = vs.erase dst := by
        rw [filter_and_ne_of_not_mem vs src dst hsrcm]
        exact filter_ne_eq_erase vs dst hnd
      have h1 : integrate n vs b f = integrate n (dst :: vs.erase dst) b f :=
        integrate_perm n (List.perm_cons_erase hdstm) b f
      rw [h1, if_neg hsrc, if_pos hdst]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
      change ((List.range n).map (fun w => integrate n (vs.erase dst) (put b dst w) f)).sum = _
      apply sum_congr
      intro w hw
      rw [hvars, put_same b src]
    · have hdstm : dst ∉ vs := by simpa using hdst
      rw [if_neg hsrc, if_neg hdst]
      have hvars : vs.filter (fun v => v != src && v != dst) = vs := by
        apply List.filter_eq_self.mpr
        intro v hv
        have hvs : v ≠ src := by intro h; subst v; exact hsrcm hv
        have hvd : v ≠ dst := by intro h; subst v; exact hdstm hv
        simp [hvs, hvd]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
      rw [hvars, put_same b src, put_same b dst]

private def cpExcl (atoms : List Atom) (old new : DB) (i : Nat) (b : Bind) : Int :=
  ((atoms.take i).map (fun a => value new a b)).prod *
    ((atoms.drop (i+1)).map (fun a => value old a b)).prod

private theorem cpExcl_zero (a : Atom) (as : List Atom) (old new : DB) (b : Bind) :
    cpExcl (a::as) old new 0 b = (as.map (fun t => value old t b)).prod := by simp [cpExcl]

private theorem cpExcl_succ (a : Atom) (as : List Atom) (old new : DB) (i : Nat) (b : Bind) :
    cpExcl (a::as) old new (i+1) b = value new a b * cpExcl as old new i b := by
  simp [cpExcl, Int.mul_assoc]

private theorem prod_diff_telescope (atoms : List Atom) (old new : DB) (b : Bind) :
    (atoms.map (fun a => value new a b)).prod - (atoms.map (fun a => value old a b)).prod =
      ((atoms.zipIdx.map (fun (a, i) =>
        (value new a b - value old a b) * cpExcl atoms old new i b)).sum) := by
  induction atoms with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.prod_cons, List.zipIdx_cons, List.zipIdx_succ,
        List.sum_cons, List.map_map, Function.comp_apply]
      rw [cpExcl_zero a as old new b]
      rw [show (List.map ((fun x : Atom × Nat =>
            (value new x.1 b - value old x.1 b) * cpExcl (a::as) old new x.2 b) ∘
            (fun x : Atom × Nat => (x.1, x.2 + 1))) as.zipIdx) =
            (as.zipIdx.map (fun x : Atom × Nat =>
              (value new x.1 b - value old x.1 b) * cpExcl (a::as) old new (x.2+1) b)) by rfl]
      rw [show (as.zipIdx.map (fun x : Atom × Nat =>
            (value new x.1 b - value old x.1 b) * cpExcl (a::as) old new (x.2+1) b)) =
            (as.zipIdx.map (fun x : Atom × Nat =>
              (value new x.1 b - value old x.1 b) * (value new a b * cpExcl as old new x.2 b))) by
        apply List.map_congr_left
        intro x hx
        rw [cpExcl_succ]]
      rw [show (as.zipIdx.map (fun x : Atom × Nat =>
            (value new x.1 b - value old x.1 b) * (value new a b * cpExcl as old new x.2 b))) =
            (as.zipIdx.map (fun (t, i) =>
              value new a b * ((value new t b - value old t b) * cpExcl as old new i b))) by
        apply List.map_congr_left
        intro x hx
        ac_rfl]
      rw [sum_mul_left, ← ih, Int.sub_mul, Int.mul_sub]
      omega

private theorem pivot_point (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (j : Nat) (a : Atom) (hnd : p.freeVars.Nodup) (hsd : a.src ≠ a.dst) (u w : Nat) :
    let vars := p.freeVars.filter (fun v => v != a.src && v != a.dst)
    let b' := put (put b a.src u) a.dst w
    integrate n vars b' (fun x => (value new a x - value old a x) * cpExcl p.atoms old new j x) =
      (new a.rel u w - old a.rel u w) *
        integrate n vars b' (fun x => cpExcl p.atoms old new j x) := by
  dsimp only
  let vars := p.freeVars.filter (fun v => v != a.src && v != a.dst)
  let b' := put (put b a.src u) a.dst w
  have hsrc_not : a.src ∉ vars := by
    dsimp only [vars]
    intro h
    simp at h
  have hdst_not : a.dst ∉ vars := by
    dsimp only [vars]
    intro h
    simp at h
  have hsrc_b : b' a.src = u := by
    dsimp only [b']
    exact put_src_dst_value b a.src a.dst u w hsd
  have hdst_b : b' a.dst = w := by
    dsimp only [b']
    exact put_dst_dst_value b a.src a.dst u w
  change integrate n vars b'
      (fun x => (new a.rel (x a.src) (x a.dst) - old a.rel (x a.src) (x a.dst)) *
        cpExcl p.atoms old new j x) =
    (new a.rel u w - old a.rel u w) * integrate n vars b' (fun x => cpExcl p.atoms old new j x)
  rw [integrate_fixed2 n vars b' a.src a.dst hsrc_not hdst_not
    (fun u v x => (new a.rel u v - old a.rel u v) * cpExcl p.atoms old new j x)]
  rw [hsrc_b, hdst_b, integrate_mul_left]

private theorem pivot_integral_eq (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (j : Nat) (a : Atom) (hnd : p.freeVars.Nodup) (hsd : a.src ≠ a.dst) :
    integrate n p.freeVars b (fun x =>
      (value new a x - value old a x) * cpExcl p.atoms old new j x) =
      pivotTerm p n old new b j a := by
  let src := a.src
  let dst := a.dst
  let vars := p.freeVars.filter (fun v => v != src && v != dst)
  let cand := fun v => if p.freeVars.contains v then List.range n else [b v]
  have hpiv := integrate_pivot n p.freeVars src dst b
    (fun x => (value new a x - value old a x) * cpExcl p.atoms old new j x) hnd hsd
  dsimp only at hpiv
  rw [hpiv]
  apply sum_congr
  intro u hu
  rw [← sum_filter_eq (if p.freeVars.contains dst then List.range n else [b dst])
        (fun w => old a.rel u w != new a.rel u w)
        (fun w => integrate n vars (put (put b src u) dst w)
          (fun x => (value new a x - value old a x) * cpExcl p.atoms old new j x))
        (by
          intro w hw hp
          have heq : old a.rel u w = new a.rel u w := by
            by_cases h' : old a.rel u w == new a.rel u w
            · exact beq_iff_eq.mp h'
            · simp [h'] at hp
              exact hp
          have hz := pivot_point p n old new b j a hnd hsd u w
          dsimp only [vars, src, dst] at hz
          rw [hz]
          simp [heq, integrate_zero])]
  apply sum_congr
  intro w hw
  have hz := pivot_point p n old new b j a hnd hsd u w
  dsimp only [vars, src, dst] at hz
  rw [hz]
  rfl

private theorem anchored_repair_generic (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (hnd : p.freeVars.Nodup) (hsd : ∀ a ∈ p.atoms, a.src ≠ a.dst) :
    reference p n new b - reference p n old b =
      (p.atoms.zipIdx.map (fun (a, i) => pivotTerm p n old new b i a)).sum := by
  have htel : ∀ x : Bind,
      (p.atoms.map (fun a => value new a x)).prod - (p.atoms.map (fun a => value old a x)).prod =
        ((p.atoms.zipIdx.map (fun (a, i) =>
          (value new a x - value old a x) * cpExcl p.atoms old new i x)).sum) := by
    intro x
    exact prod_diff_telescope p.atoms old new x
  calc
    reference p n new b - reference p n old b
        = integrate n p.freeVars b (fun x => (p.atoms.map (fun a => value new a x)).prod) -
            integrate n p.freeVars b (fun x => (p.atoms.map (fun a => value old a x)).prod) := rfl
    _ = integrate n p.freeVars b (fun x =>
          (p.atoms.map (fun a => value new a x)).prod -
            (p.atoms.map (fun a => value old a x)).prod) := by rw [integrate_sub]
    _ = integrate n p.freeVars b (fun x => (p.atoms.zipIdx.map (fun (a, i) =>
          (value new a x - value old a x) * cpExcl p.atoms old new i x)).sum) := by
            apply integrate_congr
            intro x
            exact htel x
    _ = (p.atoms.zipIdx.map (fun (a, i) => integrate n p.freeVars b (fun x =>
          (value new a x - value old a x) * cpExcl p.atoms old new i x))).sum := by
            rw [integrate_list_sum]
    _ = (p.atoms.zipIdx.map (fun (a, i) => pivotTerm p n old new b i a)).sum := by
            apply sum_congr
            intro ai hai
            rcases ai with ⟨a, i⟩
            have hmem : a ∈ p.atoms := by
              rcases List.mem_zipIdx hai with ⟨_, hlen, hval⟩
              simpa [hval] using List.get_mem p.atoms ⟨i, by simpa using hlen⟩
            exact pivot_integral_eq p n old new b i a hnd (hsd a hmem)

/-! Factored model-specific goals. -/

private theorem factor5 (n : Nat) (A D : Nat → Int) (B C E : Nat → Nat → Int) :
    rsum n (fun u => rsum n (fun x => rsum n (fun y => rsum n (fun v => rsum n (fun w =>
      A u * (B u x * (C u y * (D v * E v w)))))))) =
    (rsum n (fun u => A u * rsum n (B u) * rsum n (C u))) *
    (rsum n (fun v => D v * rsum n (E v))) := by
  let K : Int := rsum n (fun v => D v * rsum n (E v))
  have h1 : ∀ u x y v,
      rsum n (fun w => A u * (B u x * (C u y * (D v * E v w)))) =
        (A u * (B u x * (C u y * D v))) * rsum n (E v) := by
    intro u x y v
    rw [show rsum n (fun w => A u * (B u x * (C u y * (D v * E v w)))) =
        rsum n (fun w => (A u * (B u x * (C u y * D v))) * E v w) by
      apply rsum_congr
      intro w hw
      ac_rfl]
    exact rsum_const_mul n _ _
  simp only [h1]
  have h2 : ∀ u x y,
      rsum n (fun v => (A u * (B u x * (C u y * D v))) * rsum n (E v)) =
        (A u * (B u x * C u y)) * K := by
    intro u x y
    rw [show rsum n (fun v => (A u * (B u x * (C u y * D v))) * rsum n (E v)) =
        rsum n (fun v => (A u * (B u x * C u y)) * (D v * rsum n (E v))) by
      apply rsum_congr
      intro v hv
      ac_rfl]
    dsimp only [K]
    rw [rsum_const_mul]
  simp only [h2]
  have h3 : ∀ u x,
      rsum n (fun y => (A u * (B u x * C u y)) * K) =
        (A u * B u x * K) * rsum n (C u) := by
    intro u x
    rw [show rsum n (fun y => (A u * (B u x * C u y)) * K) =
        rsum n (fun y => (A u * B u x * K) * C u y) by
      apply rsum_congr
      intro y hy
      ac_rfl]
    rw [rsum_const_mul]
  simp only [h3]
  have h4 : ∀ u,
      rsum n (fun x => (A u * B u x * K) * rsum n (C u)) =
        (A u * rsum n (C u) * K) * rsum n (B u) := by
    intro u
    rw [show rsum n (fun x => (A u * B u x * K) * rsum n (C u)) =
        rsum n (fun x => (A u * rsum n (C u) * K) * B u x) by
      apply rsum_congr
      intro x hx
      ac_rfl]
    rw [rsum_const_mul]
  simp only [h4]
  rw [show rsum n (fun u => (A u * rsum n (C u) * K) * rsum n (B u)) =
      rsum n (fun u => K * (A u * rsum n (B u) * rsum n (C u))) by
    apply rsum_congr
    intro u hu
    ac_rfl]
  rw [rsum_const_mul]
  dsimp only [K]
  ac_rfl

private theorem factor_bowtie (n : Nat) (A D B C E F : Nat → Nat → Int) :
    rsum n (fun u => rsum n (fun x => rsum n (fun y => rsum n (fun v => rsum n (fun w =>
      A u x * (B x y * (C y u * (D u v * (E v w * F w u))))))))) =
    rsum n (fun u =>
      (rsum n (fun x => rsum n (fun y => A u x * (B x y * C y u)))) *
      (rsum n (fun v => rsum n (fun w => D u v * (E v w * F w u))))) := by
  have h1 : ∀ u x y v,
      rsum n (fun w => A u x * (B x y * (C y u * (D u v * (E v w * F w u))))) =
        (A u x * (B x y * (C y u * D u v))) * rsum n (fun w => E v w * F w u) := by
    intro u x y v
    rw [show rsum n (fun w => A u x * (B x y * (C y u * (D u v * (E v w * F w u))))) =
        rsum n (fun w => (A u x * (B x y * (C y u * D u v))) * (E v w * F w u)) by
      apply rsum_congr
      intro w hw
      ac_rfl]
    rw [rsum_const_mul]
  simp only [h1]
  have h2 : ∀ u x y,
      rsum n (fun v => (A u x * (B x y * (C y u * D u v))) * rsum n (fun w => E v w * F w u)) =
        (A u x * (B x y * C y u)) * rsum n (fun v => D u v * rsum n (fun w => E v w * F w u)) := by
    intro u x y
    rw [show rsum n (fun v => (A u x * (B x y * (C y u * D u v))) * rsum n (fun w => E v w * F w u)) =
        rsum n (fun v => (A u x * (B x y * C y u)) * (D u v * rsum n (fun w => E v w * F w u))) by
      apply rsum_congr
      intro v hv
      ac_rfl]
    rw [rsum_const_mul]
  simp only [h2]
  have h3 : ∀ u x,
      rsum n (fun y => (A u x * (B x y * C y u)) * rsum n (fun v => D u v * rsum n (fun w => E v w * F w u))) =
        (A u x * rsum n (fun v => D u v * rsum n (fun w => E v w * F w u))) *
          rsum n (fun y => B x y * C y u) := by
    intro u x
    rw [show rsum n (fun y => (A u x * (B x y * C y u)) * rsum n (fun v => D u v * rsum n (fun w => E v w * F w u))) =
        rsum n (fun y => (A u x * rsum n (fun v => D u v * rsum n (fun w => E v w * F w u))) * (B x y * C y u)) by
      apply rsum_congr
      intro y hy
      ac_rfl]
    rw [rsum_const_mul]
  simp only [h3]
  have h4 : ∀ u,
      rsum n (fun x => (A u x * rsum n (fun v => D u v * rsum n (fun w => E v w * F w u))) * rsum n (fun y => B x y * C y u)) =
        rsum n (fun v => D u v * rsum n (fun w => E v w * F w u)) *
          rsum n (fun x => A u x * rsum n (fun y => B x y * C y u)) := by
    intro u
    rw [show rsum n (fun x => (A u x * rsum n (fun v => D u v * rsum n (fun w => E v w * F w u))) * rsum n (fun y => B x y * C y u)) =
        rsum n (fun x => rsum n (fun v => D u v * rsum n (fun w => E v w * F w u)) *
          (A u x * rsum n (fun y => B x y * C y u))) by
      apply rsum_congr
      intro x hx
      ac_rfl]
    rw [rsum_const_mul]
  simp only [h4]
  have hL : ∀ u, rsum n (fun x => A u x * rsum n (fun y => B x y * C y u)) =
      rsum n (fun x => rsum n (fun y => A u x * (B x y * C y u))) := by
    intro u
    apply rsum_congr
    intro x hx
    exact (rsum_const_mul n (A u x) (fun y => B x y * C y u)).symm
  simp only [hL]
  have hR : ∀ u, rsum n (fun v => rsum n (fun w => D u v * (E v w * F w u))) =
      rsum n (fun v => D u v * rsum n (fun w => E v w * F w u)) := by
    intro u
    apply rsum_congr
    intro v hv
    exact rsum_const_mul n (D u v) (fun w => E v w * F w u)
  simp only [hR]
  apply rsum_congr
  intro u hu
  ac_rfl

theorem sound_VoteBadge_IndependentArms : VoteBadge.IndependentArms.Goal := by
  intro n db b
  unfold reference
  have hperm : VoteBadge.pattern.freeVars.Perm [1,2,3,4,5] := by native_decide
  rw [integrate_perm n hperm b]
  simp [integrate, VoteBadge.pattern, value, put, total]
  change rsum n (fun u => rsum n (fun x => rsum n (fun y => rsum n (fun v => rsum n (fun w =>
      db 0 (b 0) u * (db 1 u x * (db 2 u y * (db 3 (b 0) v * db 3 v w)))))))) =
    rsum n (fun u => db 0 (b 0) u * rsum n (db 1 u) * rsum n (db 2 u)) *
      rsum n (fun v => db 3 (b 0) v * rsum n (db 3 v))
  exact factor5 n (fun u => db 0 (b 0) u) (fun v => db 3 (b 0) v)
    (fun u x => db 1 u x) (fun u y => db 2 u y) (fun v w => db 3 v w)

theorem sound_ReplyCycle_UndirectedTrace : ReplyCycle.UndirectedTrace.Goal := by
  intro n db b hsym
  unfold reference
  have hperm : ReplyCycle.pattern.freeVars.Perm [0,1,2,3] := by native_decide
  rw [integrate_perm n hperm b]
  simp [integrate, ReplyCycle.pattern, value, put, bridge, total]
  change rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      db 0 x0 x1 * (db 0 x1 x2 * (db 0 x2 x3 * db 0 x3 x0)))))) =
    rsum n (fun i => rsum n (fun j => (rsum n (fun k => db 0 i k * db 0 k j))^2))
  have hsq : ∀ X : Int, X ^ 2 = X * X := by intro X; grind
  simp only [hsq]
  have hexp : ∀ i j,
      (rsum n (fun k => db 0 i k * db 0 k j)) * (rsum n (fun l => db 0 i l * db 0 l j)) =
      rsum n (fun k => rsum n (fun l => (db 0 i k * db 0 k j) * (db 0 i l * db 0 l j))) := by
    intro i j
    rw [rsum_product]
  simp only [hexp]
  apply rsum_congr
  intro i hi
  rw [rsum_comm]
  apply rsum_congr
  intro k hk
  apply rsum_congr
  intro j hj
  apply rsum_congr
  intro l hl
  have hi' : i < n := hi
  have hk' : k < n := hk
  have hj' : j < n := hj
  have hl' : l < n := hl
  rw [hsym l hl' i hi', hsym l hl' k hk']
  ac_rfl

theorem sound_FriendBridge_BidirectionalMeet : FriendBridge.BidirectionalMeet.Goal := by
  intro n db b
  simp [reference, integrate, FriendBridge.pattern, value, put, bridge, dot, total]
  change rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * db 3 x3 (b 4)))))) =
    rsum n (fun u => (rsum n (fun y => db 0 (b 0) y * db 1 y u)) *
      (rsum n (fun z => db 2 u z * db 3 z (b 4))))
  rw [rsum_comm]
  apply rsum_congr
  intro u hu
  have hreg : rsum n (fun x1 => rsum n (fun x3 =>
        db 0 (b 0) x1 * (db 1 x1 u * (db 2 u x3 * db 3 x3 (b 4))))) =
      rsum n (fun x1 => rsum n (fun x3 =>
        (db 0 (b 0) x1 * db 1 x1 u) * (db 2 u x3 * db 3 x3 (b 4)))) := by
    apply rsum_congr
    intro x1 h1
    apply rsum_congr
    intro x3 h3
    ac_rfl
  rw [hreg, rsum_product]

theorem sound_ForumBowtie_ArticulationSquare : ForumBowtie.ArticulationSquare.Goal := by
  intro n db b
  unfold reference
  have hperm : ForumBowtie.pattern.freeVars.Perm [0,1,2,3,4] := by native_decide
  rw [integrate_perm n hperm b]
  simp [integrate, ForumBowtie.pattern, value, put, bridge, loop3, total]
  change rsum n (fun u => rsum n (fun x => rsum n (fun y => rsum n (fun v => rsum n (fun w =>
      db 0 u x * (db 1 x y * (db 2 y u * (db 0 u v * (db 1 v w * db 2 w u))))))))) =
    rsum n (fun x => (rsum n (fun y => (rsum n (fun k => db 0 x k * db 1 k y)) * db 2 y x))^2)
  rw [factor_bowtie n (fun u x => db 0 u x) (fun u v => db 0 u v)
    (fun x y => db 1 x y) (fun y u => db 2 y u)
    (fun v w => db 1 v w) (fun w u => db 2 w u)]
  apply rsum_congr
  intro u hu
  have hloop : rsum n (fun x => rsum n (fun y => db 0 u x * (db 1 x y * db 2 y u))) =
      rsum n (fun y => (rsum n (fun k => db 0 u k * db 1 k y)) * db 2 y u) := by
    rw [rsum_comm]
    apply rsum_congr
    intro y hy
    rw [show rsum n (fun x => db 0 u x * (db 1 x y * db 2 y u)) =
        rsum n (fun x => (db 0 u x * db 1 x y) * db 2 y u) by
      apply rsum_congr
      intro x hx
      ac_rfl]
    rw [rsum_mul_const]
  rw [hloop]
  let X : Int := rsum n (fun y => (rsum n (fun k => db 0 u k * db 1 k y)) * db 2 y u)
  change X * X = X ^ 2
  have hs : X * X = X ^ 2 := by grind
  rw [hs]

private theorem distinct_pair_sum (n : Nat) (P : Nat → Int) :
    rsum n (fun u => rsum n (fun v => if u = v then 0 else P u * P v)) =
      (rsum n P)^2 - rsum n (fun u => (P u)^2) := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      have hleft : rsum n (fun u => if u = n then 0 else P u * P n) = rsum n P * P n := by
        rw [← rsum_mul_const]
        apply rsum_congr
        intro u hu
        have hne : u ≠ n := by omega
        simp [hne]
      have hright : rsum n (fun v => if n = v then 0 else P n * P v) = P n * rsum n P := by
        rw [← rsum_const_mul]
        apply rsum_congr
        intro v hv
        have hne : n ≠ v := by omega
        simp [hne]
      rw [rsum_succ n (fun u => rsum (n+1) (fun v => if u = v then 0 else P u * P v)),
        rsum_succ n P, rsum_succ n (fun u => (P u)^2),
        rsum_succ n (fun v => if n = v then 0 else P n * P v)]
      rw [show rsum n (fun u => rsum (n+1) (fun v => if u = v then 0 else P u * P v)) =
          rsum n (fun u => rsum n (fun v => if u = v then 0 else P u * P v) +
            (if u = n then 0 else P u * P n)) by
        apply rsum_congr
        intro u hu
        exact rsum_succ n (fun v => if u = v then 0 else P u * P v)]
      rw [rsum_add, ih, hleft, hright]
      simp
      grind

theorem sound_CoLikes_DistinctMessages : CoLikes.DistinctMessages.Goal := by
  intro n db b
  simp [integrate, CoLikes.pattern, factors, product, value, put, overlap, dot, total]
  change rsum n (fun u => rsum n (fun v => rsum n (fun x =>
      if u = v then 0 else db 0 x u * (db 0 (b 1) u *
        (db 0 (b 1) v * (db 0 x v * db 2 x (b 1))))))) =
    rsum n (fun x => db 2 x (b 1) *
      ((rsum n (fun j => db 0 x j * db 0 (b 1) j))^2 -
        rsum n (fun j => (db 0 x j * db 0 (b 1) j)^2)))
  calc
    rsum n (fun u => rsum n (fun v => rsum n (fun x =>
        if u = v then 0 else db 0 x u * (db 0 (b 1) u *
          (db 0 (b 1) v * (db 0 x v * db 2 x (b 1)))))))
    _ = rsum n (fun u => rsum n (fun x => rsum n (fun v =>
          if u = v then 0 else db 0 x u * (db 0 (b 1) u *
            (db 0 (b 1) v * (db 0 x v * db 2 x (b 1))))))) := by
            apply rsum_congr
            intro u hu
            exact rsum_comm n n (fun v x => if u = v then 0 else db 0 x u *
              (db 0 (b 1) u * (db 0 (b 1) v * (db 0 x v * db 2 x (b 1)))))
    _ = rsum n (fun x => rsum n (fun u => rsum n (fun v =>
          if u = v then 0 else db 0 x u * (db 0 (b 1) u *
            (db 0 (b 1) v * (db 0 x v * db 2 x (b 1))))))) := by
            exact rsum_comm n n (fun u x => rsum n (fun v =>
              if u = v then 0 else db 0 x u * (db 0 (b 1) u *
                (db 0 (b 1) v * (db 0 x v * db 2 x (b 1))))))
    _ = rsum n (fun x => db 2 x (b 1) * rsum n (fun u => rsum n (fun v =>
          if u = v then 0 else (db 0 x u * db 0 (b 1) u) * (db 0 x v * db 0 (b 1) v)))) := by
            apply rsum_congr
            intro x hx
            rw [show rsum n (fun u => rsum n (fun v =>
                  if u = v then 0 else db 0 x u * (db 0 (b 1) u *
                    (db 0 (b 1) v * (db 0 x v * db 2 x (b 1)))))) =
                rsum n (fun u => db 2 x (b 1) * rsum n (fun v =>
                  if u = v then 0 else (db 0 x u * db 0 (b 1) u) *
                    (db 0 x v * db 0 (b 1) v))) by
              apply rsum_congr
              intro u hu
              have hinner : rsum n (fun v => if u = v then 0 else db 0 x u * (db 0 (b 1) u *
                  (db 0 (b 1) v * (db 0 x v * db 2 x (b 1))))) =
                  db 2 x (b 1) * rsum n (fun v => if u = v then 0 else
                    (db 0 x u * db 0 (b 1) u) * (db 0 x v * db 0 (b 1) v)) := by
                rw [← rsum_const_mul n (db 2 x (b 1))
                  (fun v => if u = v then 0 else
                    (db 0 x u * db 0 (b 1) u) * (db 0 x v * db 0 (b 1) v))]
                apply rsum_congr
                intro v hv
                by_cases h : u = v <;> simp [h] <;> ac_rfl
              rw [hinner]]
            exact rsum_const_mul n (db 2 x (b 1))
              (fun u => rsum n (fun v => if u = v then 0 else
                (db 0 x u * db 0 (b 1) u) * (db 0 x v * db 0 (b 1) v)))
    _ = rsum n (fun x => db 2 x (b 1) *
          ((rsum n (fun j => db 0 x j * db 0 (b 1) j))^2 -
            rsum n (fun j => (db 0 x j * db 0 (b 1) j)^2))) := by
            apply rsum_congr
            intro x hx
            rw [distinct_pair_sum n (fun j => db 0 x j * db 0 (b 1) j)]

/-! Finite-support tools for the fractional edge-cover bound. -/

private theorem nodup_map {α β} (l : List α) (f : α → β)
    (hinj : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) (hnd : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a l ih =>
      rw [List.nodup_cons] at hnd
      rw [List.map_cons, List.nodup_cons]
      constructor
      · intro hm
        rcases List.mem_map.mp hm with ⟨b, hb, hfb⟩
        have hab : a = b := hinj a (by simp) b (by simp [hb]) hfb.symm
        subst b
        exact hnd.1 hb
      · exact ih (fun x hx y hy => hinj x (by simp [hx]) y (by simp [hy])) hnd.2

private theorem nodup_filter {α} (p : α → Bool) (l : List α) (hnd : l.Nodup) :
    (l.filter p).Nodup := hnd.filter p

private theorem nodup_flatMap {α β} (l : List α) (f : α → List β)
    (hnd : l.Nodup) (hf : ∀ a ∈ l, (f a).Nodup)
    (hd : ∀ a ∈ l, ∀ b ∈ l, a ≠ b → ∀ x ∈ f a, ∀ y ∈ f b, x ≠ y) :
    (l.flatMap f).Nodup := by
  induction l with
  | nil => simp
  | cons a l ih =>
      rw [List.flatMap_cons, List.nodup_append]
      refine ⟨hf a (by simp), ih (List.nodup_cons.mp hnd).2
        (fun b hb => hf b (by simp [hb]))
        (fun b hb c hc hbc x hx y hy => hd b (by simp [hb]) c (by simp [hc]) hbc x hx y hy), ?_⟩
      intro x hx y hy hxy
      rcases List.mem_flatMap.mp hy with ⟨c, hc, hyc⟩
      have hac : a ≠ c := by
        intro h
        subst c
        exact (List.nodup_cons.mp hnd).1 hc
      exact hd a (by simp) c (by simp [hc]) hac x hx y hyc hxy

private def bindings (n : Nat) : List Nat → Bind → List Bind
  | [], b => [b]
  | v :: vs, b => (List.range n).flatMap (fun x => bindings n vs (put b v x))

private theorem bindings_fixed (n : Nat) (vs : List Nat) (b : Bind) :
    ∀ y ∈ bindings n vs b, ∀ v ∉ vs, y v = b v := by
  induction vs generalizing b with
  | nil =>
      intro y hy v hv
      simp [bindings] at hy
      subst y
      rfl
  | cons w ws ih =>
      intro y hy v hv
      rcases List.mem_flatMap.mp hy with ⟨x, hx, hy⟩
      have hvw : v ≠ w := by intro h; subst v; exact hv (by simp)
      have hvws : v ∉ ws := by intro h; exact hv (by simp [h])
      rw [ih (put b w x) y hy v hvws, put_get_other b w v x hvw]

private theorem bindings_head (n : Nat) (v : Nat) (vs : List Nat) (b : Bind)
    (hvs : v ∉ vs) : ∀ y ∈ bindings n (v :: vs) b, ∃ x ∈ List.range n, y v = x := by
  intro y hy
  rcases List.mem_flatMap.mp hy with ⟨x, hx, hy⟩
  exact ⟨x, hx, (bindings_fixed n vs (put b v x) y hy v hvs).trans (put_self b v x)⟩

private theorem bindings_nodup (n : Nat) (vs : List Nat) (b : Bind) (hnd : vs.Nodup) :
    (bindings n vs b).Nodup := by
  induction vs generalizing b with
  | nil => simp [bindings]
  | cons v vs ih =>
      have hv : v ∉ vs := (List.nodup_cons.mp hnd).1
      apply nodup_flatMap
      · exact List.nodup_range
      · intro x hx
        exact ih (put b v x) (List.nodup_cons.mp hnd).2
      · intro x hx y hy hxy u hu z hz heq
        have haa := bindings_fixed n vs (put b v x) u hu v hv
        have hbb := bindings_fixed n vs (put b v y) z hz v hv
        simp only [put_self] at haa hbb
        subst z
        exact hxy (haa.symm.trans hbb)

private theorem sum_flatMap {α β} (l : List α) (f : α → List β) (g : β → Int) :
    ((l.flatMap f).map g).sum = (l.map (fun a => ((f a).map g).sum)).sum := by
  induction l with
  | nil => rfl
  | cons a l ih => simp only [List.flatMap_cons, List.map_append, List.sum_append, List.map_cons,
      List.sum_cons, ih]

private theorem bindings_sum (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n vs b f = ((bindings n vs b).map f).sum := by
  induction vs generalizing b with
  | nil => simp [integrate, bindings]
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        (((List.range n).flatMap (fun x => bindings n vs (put b v x))).map f).sum
      rw [sum_flatMap]
      apply sum_congr
      intro x hx
      exact ih (put b v x)

private theorem nat_sum_const {α} (l : List α) (c : Nat) :
    (l.map (fun _ => c)).sum = l.length * c := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Nat.add_mul, Nat.add_comm]

private theorem nat_sum_congr {α} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private def domain (p : Pattern) (n : Nat) (b : Bind) (v : Nat) : List Nat :=
  if p.freeVars.contains v then List.range n else [b v]

private theorem domain_nodup (p : Pattern) (n : Nat) (b : Bind) (v : Nat) :
    (domain p n b v).Nodup := by
  by_cases h : p.freeVars.contains v = true
  · rw [domain, if_pos h]
    exact List.nodup_range
  · have hf : p.freeVars.contains v = false := by cases h' : p.freeVars.contains v <;> simp_all
    rw [domain, if_neg h]
    simp

private def supportList (p : Pattern) (n : Nat) (db : DB) (b : Bind) (a : Atom) : List (Nat × Nat) :=
  (domain p n b a.src).flatMap (fun x =>
    ((domain p n b a.dst).filter (fun y => db a.rel x y != 0)).map (fun y => (x, y)))

private theorem supportList_length (p : Pattern) (n : Nat) (db : DB) (b : Bind) (a : Atom) :
    (supportList p n db b a).length = support n db a p b := by
  unfold supportList support domain
  rw [List.length_flatMap]
  apply nat_sum_congr
  intro x hx
  rw [List.length_map, ← List.countP_eq_length_filter]

private theorem supportList_nodup (p : Pattern) (n : Nat) (db : DB) (b : Bind) (a : Atom) :
    (supportList p n db b a).Nodup := by
  apply nodup_flatMap
  · exact domain_nodup p n b a.src
  · intro x hx
    apply nodup_map
    · intro y hy z hz h
      exact Prod.mk.inj h |>.2
    · exact nodup_filter _ _ (domain_nodup p n b a.dst)
  · intro x hx y hy hxy u hu v hv huv
    rcases List.mem_map.mp hu with ⟨uy, _, huy⟩
    rcases List.mem_map.mp hv with ⟨vy, _, hvy⟩
    rw [← huy, ← hvy] at huv
    exact hxy (Prod.mk.inj huv).1

private def carts {α} : List (List α) → List (List α)
  | [] => [[]]
  | l :: ls => l.flatMap (fun x => (carts ls).map (fun xs => x :: xs))

private theorem carts_length {α} (ls : List (List α)) :
    (carts ls).length = (ls.map List.length).prod := by
  induction ls with
  | nil => simp [carts]
  | cons l ls ih =>
      rw [carts, List.length_flatMap]
      rw [show l.map (fun a => ((carts ls).map (fun xs => a :: xs)).length) =
          l.map (fun _ => (carts ls).length) by
        apply List.map_congr_left
        intro a ha
        exact List.length_map _]
      rw [nat_sum_const, ih, List.map_cons, List.prod_cons]

private theorem carts_nodup {α} (ls : List (List α)) (h : ∀ l ∈ ls, l.Nodup) :
    (carts ls).Nodup := by
  induction ls with
  | nil => simp [carts]
  | cons l ls ih =>
      rw [carts]
      apply nodup_flatMap
      · exact h l (by simp)
      · intro x hx
        apply nodup_map
        · intro a ha b hb hab
          exact List.cons.inj hab |>.2
        · exact ih (fun k hk => h k (by simp [hk]))
      · intro x hx y hy hxy a ha b hb hab
        rcases List.mem_map.mp ha with ⟨as, _, has⟩
        rcases List.mem_map.mp hb with ⟨bs, _, hbs⟩
        rw [← has, ← hbs] at hab
        exact hxy (List.cons.inj hab).1

private theorem carts_mem_cons {α} (l : List α) (ls : List (List α)) (x : α) (xs : List α)
    (hx : x ∈ l) (hxs : xs ∈ carts ls) : x :: xs ∈ carts (l :: ls) := by
  rw [carts]
  exact List.mem_flatMap.mpr ⟨x, hx, List.mem_map.mpr ⟨xs, hxs, rfl⟩⟩

private theorem prod_perm {α} {l₁ l₂ : List α} (h : l₁.Perm l₂) (f : α → Nat) :
    (l₁.map f).prod = (l₂.map f).prod := by
  induction h with
  | nil => rfl
  | cons x h ih => simp [ih, Nat.mul_comm]
  | swap x y l => simp [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
  | trans h1 h2 ih1 ih2 => exact ih1.trans ih2

private theorem perm_filter_of_nodup {α} [DecidableEq α] (l l' : List α)
    (hnd : l.Nodup) (hnd' : l'.Nodup) (hsub : ∀ x ∈ l, x ∈ l') :
    l.Perm (l'.filter (fun x => x ∈ l)) := by
  induction l' generalizing l with
  | nil =>
      have hl : l = [] := by
        cases l with
        | nil => rfl
        | cons a as => exact False.elim (by simpa using hsub a (by simp))
      subst l
      simp
  | cons a l' ih =>
      have hnot : a ∉ l' := (List.nodup_cons.mp hnd').1
      by_cases ha : a ∈ l
      · have htail := ih (l.erase a) (hnd.erase a) (List.nodup_cons.mp hnd').2 (by
          intro x hx
          have hxa : x ≠ a := by
            intro h; subst x; exact hnd.not_mem_erase hx
          have hxl := (List.mem_erase_of_ne hxa).mp hx
          simpa [hxa] using hsub x hxl)
        have hab : decide (a ∈ l) = true := by simp [ha]
        have hfilter : (a :: l').filter (fun x => x ∈ l) =
            a :: l'.filter (fun x => x ∈ l.erase a) := by
          rw [List.filter_cons, if_pos hab]
          congr 1
          apply List.filter_congr
          intro x hx
          have hxa : x ≠ a := by intro h; subst x; exact hnot hx
          simp [List.mem_erase_of_ne hxa]
        rw [hfilter]
        exact (List.perm_cons_erase ha).trans (List.Perm.cons a htail)
      · have hab : decide (a ∈ l) = false := by simp [ha]
        rw [List.filter_cons, if_neg (by simp [ha])]
        apply ih l hnd (List.nodup_cons.mp hnd').2
        intro x hx
        have hxa : x ≠ a := by intro h; subst x; exact ha hx
        simpa [hxa] using hsub x hx

private theorem prod_map_filter {α} (l : List α) (p : α → Bool) (f : α → Nat) :
    (l.map (fun x => if p x then f x else 1)).prod = ((l.filter p).map f).prod := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases ha : p a = true <;> simp [ha, ih]

private theorem prod_map_mul {α} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x * g x)).prod = (l.map f).prod * (l.map g).prod := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]; ac_rfl

private theorem cover_product_range (m : Nat) (C : List Nat) (f : Nat → Nat)
    (hnd : C.Nodup) (hsub : ∀ i ∈ C, i < m) :
    (C.map f).prod = ((List.range m).map (fun i => if C.contains i then f i else 1)).prod := by
  rw [prod_map_filter (List.range m) C.contains f]
  apply prod_perm (l₁ := C) (l₂ := (List.range m).filter C.contains)
  · exact perm_filter_of_nodup C (List.range m) hnd List.nodup_range (by
      intro i hi
      exact List.mem_range.mpr (hsub i hi))

private theorem prod_map_one {α} (l : List α) : (l.map (fun _ => (1 : Nat))).prod = 1 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem products_of_covers (m : Nat) (cs : List (List Nat)) (f : Nat → Nat)
    (hnd : ∀ C ∈ cs, C.Nodup) (hsub : ∀ C ∈ cs, ∀ i ∈ C, i < m) :
    (cs.map (fun C => (C.map f).prod)).prod =
      ((List.range m).map (fun i => f i ^ cs.countP (fun C => C.contains i))).prod := by
  induction cs with
  | nil => simp [prod_map_one]
  | cons C cs ih =>
      rw [List.map_cons, List.prod_cons]
      rw [cover_product_range m C f (hnd C (by simp)) (fun i hi => hsub C (by simp) i hi)]
      rw [ih (fun D hD => hnd D (by simp [hD])) (fun D hD i hi => hsub D (by simp [hD]) i hi)]
      simp only [List.countP_cons]
      rw [← prod_map_mul]
      apply congrArg List.prod
      apply List.map_congr_left
      intro i hi
      rw [Nat.pow_add]
      by_cases hci : C.contains i = true
      · have him : i ∈ C := by simpa using hci
        simp [him, Nat.mul_comm]
      · have hnot : i ∉ C := by simpa using hci
        simp [hnot]

private theorem bindings_range (n : Nat) (vs : List Nat) (b : Bind) :
    ∀ y ∈ bindings n vs b, ∀ v ∈ vs, y v ∈ List.range n := by
  induction vs generalizing b with
  | nil => simp
  | cons w ws ih =>
      intro y hy v hv
      rcases List.mem_flatMap.mp hy with ⟨x, hx, hy⟩
      by_cases hvw : w = v
      · subst w
        by_cases htail : v ∈ ws
        · exact ih (put b v x) y hy v htail
        · have heq := bindings_fixed n ws (put b v x) y hy v htail
          have hx' : y v ∈ List.range n := by
            rw [heq]
            simpa [put] using hx
          exact hx'
      · have htail : v ∈ ws := by
          rcases List.mem_cons.mp hv with h | h
          · exact (hvw h.symm).elim
          · exact h
        exact ih (put b w x) y hy v htail

private theorem binding_domain_mem (p : Pattern) (n : Nat) (b : Bind) :
    ∀ y ∈ bindings n p.freeVars b, ∀ v, y v ∈ domain p n b v := by
  intro y hy v
  by_cases h : p.freeVars.contains v = true
  · have hv : v ∈ p.freeVars := by simpa using h
    rw [domain, if_pos h]
    exact bindings_range n p.freeVars b y hy v hv
  · have hf : p.freeVars.contains v = false := by cases h' : p.freeVars.contains v <;> simp_all
    have hv : v ∉ p.freeVars := by simpa using hf
    rw [domain, if_neg h]
    simp [bindings_fixed n p.freeVars b y hy v hv]

private def bin (db : DB) : DB := fun r x y => if db r x y == 0 then 0 else 1

private def satisfies (p : Pattern) (db : DB) (x : Bind) : Bool :=
  p.atoms.all (fun a => value db a x != 0)

private theorem prod_indicator {α} (l : List α) (q : α → Bool) :
    (l.map (fun a => if q a then (1 : Int) else 0)).prod = if l.all q then 1 else 0 := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases ha : q a = true <;> simp [ha, ih]

private theorem sum_filter_indicator {α} (l : List α) (q : α → Bool) :
    ((l.filter q).map (fun _ => (1 : Int))).sum = (l.filter q).length := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases ha : q a = true <;> simp [ha, ih] <;> omega

private def satBindings (p : Pattern) (n : Nat) (db : DB) (b : Bind) : List Bind :=
  (bindings n p.freeVars b).filter (satisfies p (bin db))

private theorem reference_bin_count (p : Pattern) (n : Nat) (db : DB) (b : Bind) :
    reference p n (bin db) b = (satBindings p n db b).length := by
  rw [reference, bindings_sum]
  change ((bindings n p.freeVars b).map
    (fun x => (p.atoms.map (fun a => value (bin db) a x)).prod)).sum = _
  rw [show (bindings n p.freeVars b).map
      (fun x => (p.atoms.map (fun a => value (bin db) a x)).prod) =
      (bindings n p.freeVars b).map (fun x => if satisfies p (bin db) x then 1 else 0) by
    apply List.map_congr_left
    intro x hx
    simpa [value, bin, satisfies, beq_iff_eq] using
      prod_indicator p.atoms (fun a => value (bin db) a x != 0)]
  rw [show ((bindings n p.freeVars b).map
      (fun x => if satisfies p (bin db) x then 1 else 0)).sum =
      ((satBindings p n db b).map (fun _ => (1 : Int))).sum by
    unfold satBindings
    induction bindings n p.freeVars b with
    | nil => simp
    | cons a l ih => by_cases ha : satisfies p (bin db) a = true <;> simp [ha, ih]]
  exact sum_filter_indicator _ _

private theorem supportList_mem (p : Pattern) (n : Nat) (db : DB) (b : Bind) (a : Atom)
    (x : Bind) (hx : x ∈ bindings n p.freeVars b) (hnz : db a.rel (x a.src) (x a.dst) != 0) :
    (x a.src, x a.dst) ∈ supportList p n db b a := by
  unfold supportList
  apply List.mem_flatMap.mpr
  refine ⟨x a.src, binding_domain_mem p n b x hx a.src, ?_⟩
  apply List.mem_map.mpr
  refine ⟨x a.dst, ?_, rfl⟩
  exact List.mem_filter.mpr ⟨binding_domain_mem p n b x hx a.dst, hnz⟩

private def encode (C : List Atom) (x : Bind) : List (Nat × Nat) :=
  C.map (fun a => (x a.src, x a.dst))

private theorem encode_mem_carts (p : Pattern) (n : Nat) (db : DB) (b : Bind)
    (C : List Atom) :
    (∀ a ∈ C, a ∈ p.atoms) → ∀ x, x ∈ satBindings p n db b →
      encode C x ∈ carts (C.map (supportList p n db b)) := by
  induction C with
  | nil => intro hCp x hx; simp [encode, carts]
  | cons a C ih =>
      intro hCp x hx
      have hsat : satisfies p (bin db) x = true := (List.mem_filter.mp hx).2
      have hb : x ∈ bindings n p.freeVars b := (List.mem_filter.mp hx).1
      have hsat' : p.atoms.all (fun t => value (bin db) t x != 0) = true := hsat
      have ha : value (bin db) a x != 0 :=
        List.all_eq_true.mp hsat' a (hCp a (by simp))
      have hnz : db a.rel (x a.src) (x a.dst) != 0 := by
        simpa [value, bin, beq_iff_eq] using ha
      rw [encode, List.map_cons, carts]
      exact carts_mem_cons _ _ _ _ (supportList_mem p n db b a x hb hnz)
        (ih (fun t ht => hCp t (by simp [ht])) x hx)

private theorem map_eq_on_mem {α β} {C : List α} {f g : α → β}
    (h : C.map f = C.map g) : ∀ a ∈ C, f a = g a := by
  induction C with
  | nil => simp
  | cons a C ih =>
      simp only [List.map_cons, List.cons.injEq] at h
      intro x hx
      rcases List.mem_cons.mp hx with rfl | hx
      · exact h.1
      · exact ih h.2 x hx

private theorem encode_injective (p : Pattern) (n : Nat) (b : Bind) (C : List Atom)
    (hcov : ∀ v ∈ p.freeVars, ∃ a ∈ C, a.src = v ∨ a.dst = v) :
    ∀ x ∈ bindings n p.freeVars b, ∀ y ∈ bindings n p.freeVars b,
      encode C x = encode C y → x = y := by
  intro x hx y hy heq
  funext v
  by_cases hv : v ∈ p.freeVars
  · rcases hcov v hv with ⟨a, ha, hend⟩
    have hp := map_eq_on_mem heq a ha
    rcases hend with rfl | rfl
    · exact Prod.mk.inj hp |>.1
    · exact Prod.mk.inj hp |>.2
  · rw [bindings_fixed n p.freeVars b x hx v hv, bindings_fixed n p.freeVars b y hy v hv]

private theorem ordinary_cover_bound (p : Pattern) (n : Nat) (db : DB) (b : Bind)
    (C : List Atom) (hcov : ∀ v ∈ p.freeVars, ∃ a ∈ C, a.src = v ∨ a.dst = v)
    (hCp : ∀ a ∈ C, a ∈ p.atoms) (hnd : p.freeVars.Nodup) :
    (satBindings p n db b).length ≤ (C.map (fun a => support n db a p b)).prod := by
  have hsnd : (satBindings p n db b).Nodup :=
    nodup_filter _ _ (bindings_nodup n p.freeVars b hnd)
  have hemb : (satBindings p n db b).map (encode C) ⊆
      carts (C.map (supportList p n db b)) := by
    intro z hz
    rcases List.mem_map.mp hz with ⟨x, hx, rfl⟩
    exact encode_mem_carts p n db b C hCp x hx
  have hmapnd : ((satBindings p n db b).map (encode C)).Nodup := by
    apply nodup_map
    · intro x hx y hy heq
      exact encode_injective p n b C hcov x (List.mem_filter.mp hx).1 y
        (List.mem_filter.mp hy).1 heq
    · exact hsnd
  have hle := hmapnd.length_le_of_subset hemb
  rw [List.length_map, carts_length, List.map_map] at hle
  rw [show C.map (List.length ∘ supportList p n db b) =
      C.map (fun a => (supportList p n db b a).length) by
    apply List.map_congr_left
    intro a ha
    rfl] at hle
  rw [show (C.map (fun a => (supportList p n db b a).length)) =
      C.map (fun a => support n db a p b) by
    apply List.map_congr_left
    intro a ha
    exact supportList_length p n db b a] at hle
  exact hle



/-! Five concrete boundary-message goals. -/

theorem sound_VoteBadge_BoundaryMessages : VoteBadge.BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference VoteBadge.pattern n db b

theorem sound_ReplyCycle_BoundaryMessages : ReplyCycle.BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference ReplyCycle.pattern n db b

theorem sound_FriendBridge_BoundaryMessages : FriendBridge.BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference FriendBridge.pattern n db b

theorem sound_CoLikes_BoundaryMessages : CoLikes.BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference CoLikes.pattern n db b

theorem sound_ForumBowtie_BoundaryMessages : ForumBowtie.BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference ForumBowtie.pattern n db b

theorem sound_VoteBadge_AnchoredRepair : VoteBadge.AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic VoteBadge.pattern n old new b (by native_decide) (by native_decide)

theorem sound_ReplyCycle_AnchoredRepair : ReplyCycle.AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic ReplyCycle.pattern n old new b (by native_decide) (by native_decide)

theorem sound_FriendBridge_AnchoredRepair : FriendBridge.AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic FriendBridge.pattern n old new b (by native_decide) (by native_decide)

theorem sound_CoLikes_AnchoredRepair : CoLikes.AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic CoLikes.pattern n old new b (by native_decide) (by native_decide)

theorem sound_ForumBowtie_AnchoredRepair : ForumBowtie.AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic ForumBowtie.pattern n old new b (by native_decide) (by native_decide)

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.SocialJoinBoundaryMessages
