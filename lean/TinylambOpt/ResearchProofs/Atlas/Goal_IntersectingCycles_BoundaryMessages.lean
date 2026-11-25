import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! BoundaryMessages for `IntersectingCycles`: eliminating the free variables one
by one with boundary-only messages equals the reference join.  Truth was checked
first by exhaustive evaluation on small inputs (synthetic databases x bindings,
n = 1..3).  The argument below is generic: it works for every `Pattern`, hence
in particular for the intersecting-cycles pattern. -/

namespace TinylambOpt.ResearchProofs.Atlas.Goal_IntersectingCycles_BoundaryMessages

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.IntersectingCycles

/-! Truth check on small inputs before proving: sweep synthetic databases and
bindings for n = 1..3 and compare the two evaluations exactly. -/

private def probeDB (seed r x y : Nat) : Int :=
  (((seed * 37 + r * 100 + x * 10 + y * 7) % 9 : Nat) : Int) - 4

#guard (List.range 60).all (fun seed =>
  (List.range 3).all (fun k =>
    let n := k + 1
    let b : Bind := fun v => (seed * (v + 1) + v) % n
    elimination pattern n (probeDB seed) b == reference pattern n (probeDB seed) b))

/-! Small finite-sum and integration toolkit. -/

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

private theorem sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

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

private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := sum_mul_left (List.range n) c f

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := sum_mul_right (List.range n) f c

private theorem put_same (b : Bind) (v : Nat) : put b v (b v) = b := by
  funext i
  by_cases h : i = v
  · subst i; simp [put]
  · simp [put, show (i == v) = false by simpa using h]

private theorem put_comm_ne (b : Bind) (v w x y : Nat) (h : v ≠ w) :
    put (put b v x) w y = put (put b w y) v x := by
  funext i
  by_cases hi : i = v
  · subst i
    simp [put, show (v == w) = false by simpa using h]
  · by_cases hj : i = w
    · subst i; simp [put, hi]
    · simp [put, show (i == v) = false by simpa using hi,
        show (i == w) = false by simpa using hj]

private theorem integrate_congr (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ x, f x = g x) : integrate n vs b f = integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum
      exact sum_map_congr _ _ _ (fun x hx => ih (put b v x))

private theorem integrate_swap (n v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
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
        ((List.range n).map (fun x => integrate n ws (put b w x) (fun b' => integrate n [v] b' f))).sum
      exact sum_map_congr _ _ _ (fun x hx => ih (put b w x))

private theorem integrate_cons_singleton (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: vs) b f = integrate n vs b (fun b' => integrate n [v] b' f) := by
  rw [integrate_rotate, integrate_snoc]

/-! Variable elimination. -/

private def Respects (f : Factor) : Prop :=
  ∀ b b', (∀ v, f.scope.contains v = true → b v = b' v) → f.get b = f.get b'

private theorem respects_value (db : DB) (a : Atom) : Respects ⟨[a.src, a.dst], value db a⟩ := by
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
      congr 1
      exact ih (fun g hg => h g (by simp [hg])) (fun g hg => heq g (by simp [hg]))

private theorem product_put_eq {fs : List Factor} (h : ∀ f ∈ fs, Respects f)
    (hnot : ∀ f ∈ fs, f.scope.contains v = false) (b : Bind) (x : Nat) :
    product fs (put b v x) = product fs b := by
  apply product_respects_eq h
  intro f hf w hw
  by_cases hvw : w = v
  · subst w; rw [hnot f hf] at hw; simp at hw
  · simp [put, show (w == v) = false by simpa using hvw]

private theorem partition_filters (p : α → Bool) (l : List α) :
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

private theorem mem_partition_notp (p : Factor → Bool) {l : List Factor} {f : Factor}
    (hf : f ∈ (List.partition p l).snd) : p f = false := by
  rw [(partition_filters p l).2] at hf
  have hn := (List.mem_filter.mp hf).2
  simpa using hn

private theorem mem_partition_fst {l : List Factor} {v : Nat} {f : Factor}
    (hf : f ∈ (List.partition (fun g : Factor => g.scope.contains v) l).fst) : f ∈ l := by
  rw [(partition_filters _ l).1] at hf
  exact (List.mem_filter.mp hf).1

private theorem mem_partition_snd {l : List Factor} {v : Nat} {f : Factor}
    (hf : f ∈ (List.partition (fun g : Factor => g.scope.contains v) l).snd) : f ∈ l := by
  rw [(partition_filters _ l).2] at hf
  exact (List.mem_filter.mp hf).1

private theorem product_partition_get (p : Factor → Bool) (l : List Factor) (b : Bind) :
    product (List.partition p l).fst b * product (List.partition p l).snd b = product l b := by
  rw [(partition_filters p l).1, (partition_filters p l).2]
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [product] at ih
      by_cases h : p a = true <;>
        simp [product, h, ih, Int.mul_assoc, Int.mul_left_comm]

private theorem respects_msg (n v : Nat) (inc : List Factor) (hres : ∀ f ∈ inc, Respects f) :
    Respects ⟨(inc.flatMap Factor.scope).eraseDups.filter (fun w => w != v),
      fun b => integrate n [v] b (product inc)⟩ := by
  intro b b' hb
  apply sum_map_congr
  intro x hx
  apply product_respects_eq hres
  intro f hf w hw
  by_cases hwv : w = v
  · subst w; simp [put]
  · rw [put, ite_eq_right (by simp [hwv]), put, ite_eq_right (by simp [hwv])]
    apply hb
    have hwmem : w ∈ f.scope := by simpa using hw
    have hwflat : w ∈ inc.flatMap Factor.scope := List.mem_flatMap.mpr ⟨f, hf, hwmem⟩
    have hwer : w ∈ (inc.flatMap Factor.scope).eraseDups := List.mem_eraseDups.mpr hwflat
    exact List.contains_iff_mem.mpr ((List.mem_filter (p := fun z => z != v)).mpr ⟨hwer, by
      by_cases hbe : (w == v) = true
      · exact False.elim (hwv (beq_iff_eq.mp hbe))
      · simpa [hbe] using hwv⟩)

private theorem product_eliminate (n v : Nat) (fs : List Factor) (b : Bind)
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

private theorem respects_eliminate (n v : Nat) (fs : List Factor) (hres : ∀ f ∈ fs, Respects f) :
    ∀ f ∈ eliminate n v fs, Respects f := by
  intro f hf
  simp only [eliminate, List.mem_cons] at hf
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
      rw [ih _ (respects_eliminate n v fs hres), integrate_cons_singleton]
      exact integrate_congr n vs b _ _ (fun b' => product_eliminate n v fs b' hres)

private theorem elimination_eq_reference (p : Pattern) (n : Nat) (db : DB) (b : Bind) :
    elimination p n db b = reference p n db b := by
  unfold elimination reference
  rw [product_fold_eliminate]
  · exact integrate_congr n p.freeVars b _ _ (fun x => by
      simp [factors, product, Function.comp_def])
  · intro f hf
    rcases List.mem_map.mp hf with ⟨a, ha, rfl⟩
    exact respects_value db a

theorem sound_IntersectingCycles_BoundaryMessages : BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference pattern n db b

end TinylambOpt.ResearchProofs.Atlas.Goal_IntersectingCycles_BoundaryMessages
