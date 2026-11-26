import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved (13 claims, including the following)

- `ChannelBridge.AnchoredRepair` — `∀ n old new b, reference pattern n new b -
  reference pattern n old b = (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm
  pattern n old new b i a)).sum`
- `ChannelBridge.BoundaryMessages` — `∀ n db b, elimination pattern n db b = reference
  pattern n db b`
- `ChannelBridge.TripleOverlap` — `∀ n db b, reference pattern n db b = total n (fun y
  => overlap n db 0 (b 0) y * overlap n db 1 (b 0) y * overlap n db 2 (b 0) y)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.SalesJoinBoundaryMessages

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total dot)
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Samples (choose)

/-! Finite sums and finite integrations. -/

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
      rw [ih (fun i hi => h i (Nat.lt_succ_of_lt hi)), h n (Nat.lt_succ_self n)]

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

private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g :=
  sum_add (List.range n) f g

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f :=
  sum_mul_left (List.range n) c f

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c :=
  sum_mul_right (List.range n) f c

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

private theorem integrate_rotate (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: vs) b f = integrate n (vs ++ [v]) b f := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      rw [integrate_swap]
      exact integrate_congr n [w] b _ _ ih

private theorem integrate_snoc (n : Nat) (vs : List Nat) (v : Nat) (b : Bind) (f : Bind → Int) :
    integrate n (vs ++ [v]) b f = integrate n vs b (fun b' => integrate n [v] b' f) := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      exact integrate_congr n [w] b _ _ ih

private theorem integrate_cons_singleton (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: vs) b f = integrate n vs b (fun b' => integrate n [v] b' f) := by
  rw [integrate_rotate, integrate_snoc]

private theorem integrate_perm (n : Nat) {vs vs' : List Nat} (h : vs.Perm vs') :
    ∀ (b : Bind) (f : Bind → Int), integrate n vs b f = integrate n vs' b f := by
  induction h with
  | nil => intro b f; rfl
  | cons x h ih => intro b f; exact integrate_congr n [x] b _ _ (ih · f)
  | swap x y l => intro b f; exact integrate_swap n y x l b f
  | trans h1 h2 ih1 ih2 => intro b f; exact (ih1 b f).trans (ih2 b f)

private theorem integrate_add (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x + g x) = integrate n vs b f + integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_add]

private theorem integrate_mul_left (n : Nat) (vs : List Nat) (b : Bind) (c : Int) (f : Bind → Int) :
    integrate n vs b (fun x => c * f x) = c * integrate n vs b f := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_mul_left]

private theorem integrate_neg (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n vs b (fun x => -f x) = -integrate n vs b f := by
  rw [show (fun x => -f x) = (fun x => (-1:Int) * f x) by funext x; simp]
  rw [integrate_mul_left]
  simp

private theorem integrate_sub (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x - g x) = integrate n vs b f - integrate n vs b g := by
  simp only [Int.sub_eq_add_neg]
  rw [integrate_add, integrate_neg]

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

private theorem integrate_singleton_rsum (n v : Nat) (b : Bind) (f : Bind → Int) :
    integrate n [v] b f = rsum n (fun x => f (put b v x)) := rfl

/-! Variable elimination. -/

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

private theorem partition_loop_spec (p : α → Bool) : ∀ (l a b : List α),
    (List.partition.loop p l (a, b)).fst = a.reverse ++ l.filter p ∧
    (List.partition.loop p l (a, b)).snd = b.reverse ++ l.filter (fun x => !p x)
  | [], a, b => by simp [List.partition.loop]
  | x :: xs, a, b => by
      by_cases h : p x = true
      · simp [List.partition.loop, h, partition_loop_spec p xs]
      · simp [List.partition.loop, h, partition_loop_spec p xs]

private theorem partition_filters (p : α → Bool) (l : List α) :
    (List.partition p l).fst = l.filter p ∧
    (List.partition p l).snd = l.filter (fun x => !p x) := by
  simpa [List.partition] using partition_loop_spec p l [] []

private theorem mem_partition_p (p : α → Bool) {l : List α} {a : α} :
    a ∈ (List.partition p l).fst → p a = true := by
  rw [(partition_filters p l).1]
  exact fun h => (List.mem_filter.mp h).2

private theorem mem_partition_notp (p : α → Bool) {l : List α} {a : α} :
    a ∈ (List.partition p l).snd → p a = false := by
  rw [(partition_filters p l).2]
  intro h
  have hn := (List.mem_filter.mp h).2
  by_cases hp : p a = true
  · simp [hp] at hn
  · simpa using hp

private theorem product_filter_mul (p : α → Bool) (l : List α) (g : α → Int) :
    ((l.filter p).map g).prod * ((l.filter (fun x => !p x)).map g).prod = (l.map g).prod := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases h : p a = true <;> simp [h, ih, Int.mul_assoc, Int.mul_left_comm]

private theorem product_partition_get (p : Factor → Bool) (l : List Factor) (b : Bind) :
    ((List.partition p l).fst.map (fun f => f.get b)).prod *
      ((List.partition p l).snd.map (fun f => f.get b)).prod = (l.map (fun f => f.get b)).prod := by
  rw [(partition_filters p l).1, (partition_filters p l).2]
  exact product_filter_mul p l (fun f => f.get b)

private theorem mem_of_partition_fst {l : List Factor} {v : Nat} {f : Factor}
    (hf : f ∈ (List.partition (fun g : Factor => g.scope.contains v) l).fst) : f ∈ l :=
  (List.mem_partition (p := fun g : Factor => g.scope.contains v) (l := l) (a := f)).mpr (Or.inl hf)

private theorem mem_of_partition_snd {l : List Factor} {v : Nat} {f : Factor}
    (hf : f ∈ (List.partition (fun g : Factor => g.scope.contains v) l).snd) : f ∈ l :=
  (List.mem_partition (p := fun g : Factor => g.scope.contains v) (l := l) (a := f)).mpr (Or.inr hf)

private theorem product_put_eq {fs : List Factor} (h : ∀ f ∈ fs, Respects f)
    (hnot : ∀ f ∈ fs, f.scope.contains v = false) (b : Bind) (x : Nat) :
    product fs (put b v x) = product fs b := by
  apply product_respects_eq h
  intro f hf w hw
  by_cases hvw : w = v
  · subst w; rw [hnot f hf] at hw; simp at hw
  · simp [put, hvw]

private theorem respects_msg (n v : Nat) (inc : List Factor) (hres : ∀ f ∈ inc, Respects f) :
    Respects ⟨(inc.flatMap Factor.scope).eraseDups.filter (fun w => w != v),
      fun b => integrate n [v] b (product inc)⟩ := by
  intro b b' hb
  apply sum_congr
  intro x _
  apply product_respects_eq hres
  intro f hf w hw
  by_cases hwv : w = v
  · subst w; simp [put_self]
  · simp [put, show (w == v) = false by simpa using hwv]
    apply hb
    have hwmem : w ∈ f.scope := by simpa using hw
    have hwflat : w ∈ inc.flatMap Factor.scope := List.mem_flatMap.mpr ⟨f, hf, hwmem⟩
    have hwErase : w ∈ (inc.flatMap Factor.scope).eraseDups := List.mem_eraseDups.mpr hwflat
    have hmem : w ∈ (inc.flatMap Factor.scope).eraseDups.filter (fun w => w != v) :=
      List.mem_filter.mpr ⟨hwErase, by simp [hwv]⟩
    simpa using hmem

private theorem product_eliminate (n v : Nat) (fs : List Factor) (b : Bind)
    (hres : ∀ f ∈ fs, Respects f) :
    product (eliminate n v fs) b = integrate n [v] b (product fs) := by
  dsimp only [eliminate]
  let inc := (fs.partition (fun f : Factor => f.scope.contains v)).fst
  let rest := (fs.partition (fun f : Factor => f.scope.contains v)).snd
  have hinc_res : ∀ f ∈ inc, Respects f := fun f hf => hres f (mem_of_partition_fst hf)
  have hrest_res : ∀ f ∈ rest, Respects f := fun f hf => hres f (mem_of_partition_snd hf)
  have hrest_not : ∀ f ∈ rest, f.scope.contains v = false :=
    fun f hf => mem_partition_notp (fun g : Factor => g.scope.contains v) hf
  change (integrate n [v] b (fun x => product inc x)) * product rest b =
    integrate n [v] b (product fs)
  change ((List.range n).map (fun x => product inc (put b v x))).sum * product rest b =
    ((List.range n).map (fun x => product fs (put b v x))).sum
  rw [← sum_mul_right]
  apply sum_congr
  intro x _
  dsimp only [product]
  rw [← product_partition_get (fun f : Factor => f.scope.contains v) fs (put b v x)]
  change (inc.map (fun f => f.get (put b v x))).prod * product rest b =
    (inc.map (fun f => f.get (put b v x))).prod *
      (product (fs.partition (fun f : Factor => f.scope.contains v)).snd (put b v x))
  have hp := product_put_eq hrest_res hrest_not b x
  dsimp only [product] at hp
  change (inc.map (fun f => f.get (put b v x))).prod * (rest.map (fun f => f.get b)).prod =
    (inc.map (fun f => f.get (put b v x))).prod * (rest.map (fun f => f.get (put b v x))).prod
  rw [hp]

private theorem respects_eliminate (n v : Nat) (fs : List Factor) (hres : ∀ f ∈ fs, Respects f) :
    ∀ f ∈ eliminate n v fs, Respects f := by
  intro f hf
  dsimp only [eliminate] at hf
  simp only [List.mem_cons] at hf
  rcases hf with rfl | hf
  · exact respects_msg n v (fs.partition (fun g : Factor => g.scope.contains v)).fst
      (fun g hg => hres g (mem_of_partition_fst hg))
  · exact hres f (mem_of_partition_snd hf)

private theorem product_fold_eliminate (n : Nat) (vs : List Nat) (fs : List Factor) (b : Bind)
    (hres : ∀ f ∈ fs, Respects f) :
    product (vs.foldl (fun fs v => eliminate n v fs) fs) b = integrate n vs b (product fs) := by
  induction vs generalizing fs with
  | nil => rfl
  | cons v vs ih =>
      change product (vs.foldl (fun fs v => eliminate n v fs) (eliminate n v fs)) b =
        integrate n (v :: vs) b (product fs)
      rw [ih (eliminate n v fs) (respects_eliminate n v fs hres)]
      rw [integrate_cons_singleton]
      exact integrate_congr n vs b _ _ (fun x => product_eliminate n v fs x hres)

private theorem elimination_eq_reference (p : Pattern) (n : Nat) (db : DB) (b : Bind) :
    elimination p n db b = reference p n db b := by
  unfold elimination reference
  rw [product_fold_eliminate]
  · exact integrate_congr n p.freeVars b _ _ (fun x => by simp [factors, product, Function.comp_def])
  · intro f hf
    rcases List.mem_map.mp hf with ⟨a, _, rfl⟩
    exact respects_value db a

/-! Anchored repair: product telescoping and endpoint extraction. -/

private theorem put_get_other (b : Bind) (v w x : Nat) (h : w ≠ v) : put b v x w = b w := by
  simp [put, show (w == v) = false by simpa using h]

private theorem put_pinned (b : Bind) (v : Nat) : put b v (b v) = b := put_same b v

private theorem put_dst_pinned (b : Bind) (src dst u : Nat) (hne : src ≠ dst) :
    put (put b src u) dst (b dst) = put b src u := by
  funext i
  by_cases hid : i = dst
  · subst i
    simp [put, hne.symm]
  · simp [put, hid]

private theorem filter_ne_eq_erase (vs : List Nat) (a : Nat) (hnd : vs.Nodup) :
    vs.filter (fun v => v != a) = vs.erase a := by
  induction vs with
  | nil => rfl
  | cons x xs ih =>
      have hp := List.pairwise_cons.mp hnd
      by_cases hxa : x == a
      · have hx : x = a := of_decide_eq_true hxa
        subst x
        have hna : a ∉ xs := fun h => hp.1 a h rfl
        simp [List.erase_cons_head, hna, ih hp.2]
      · simp [List.erase_cons_tail hxa, show (x != a) = true by simpa using hxa, ih hp.2]

private theorem filter_and_ne_of_not_mem (l : List Nat) (a b : Nat) (ha : a ∉ l) :
    l.filter (fun v => v != a && v != b) = l.filter (fun v => v != b) := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have hxna : x ≠ a := fun h => ha (by simp [h])
      have hxs : a ∉ xs := fun h => ha (by simp [h])
      by_cases hxb : x = b
      · subst x; simp [ih hxs]
      · simp [show (x != a) = true by simpa using hxna,
          show (x != b) = true by simpa using hxb, ih hxs]

private theorem filter_and_ne_of_not_mem2 (l : List Nat) (a b : Nat) (hb : b ∉ l) :
    l.filter (fun v => v != a && v != b) = l.filter (fun v => v != a) := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have hxb : x ≠ b := fun h => hb (by simp [h])
      have hxs : b ∉ xs := fun h => hb (by simp [h])
      by_cases hxa : x = a
      · subst x; simp [ih hxs]
      · simp [show (x != a) = true by simpa using hxa,
          show (x != b) = true by simpa using hxb, ih hxs]

private theorem bool_false_of_not_true {p : Bool} (h : ¬p = true) : p = false := by
  cases p <;> simp_all

private theorem erase_erase_filter (vs : List Nat) (a b : Nat) (hnd : vs.Nodup) :
    (vs.erase a).erase b = vs.filter (fun v => v != a && v != b) := by
  induction vs with
  | nil => rfl
  | cons x xs ih =>
      have hp := List.pairwise_cons.mp hnd
      by_cases hxa : x == a
      · have hx : x = a := of_decide_eq_true hxa
        subst x
        have hna : a ∉ xs := fun h => hp.1 a h rfl
        rw [List.erase_cons_head]
        have hf : (a != a && a != b) = false := by simp
        simp [hf]
        rw [filter_and_ne_of_not_mem xs a b hna, filter_ne_eq_erase xs b hp.2]
      · by_cases hxb : x == b
        · have hx : x = b := of_decide_eq_true hxb
          subst x
          have hnb : b ∉ xs := fun h => hp.1 b h rfl
          have hbne : b ≠ a := by simpa using hxa
          have hnb' : b ∉ xs.erase a := fun h =>
            hnb ((List.mem_erase_of_ne (a := b) (b := a) hbne).mp h)
          have hih := ih hp.2
          rw [List.erase_of_not_mem hnb'] at hih
          rw [List.erase_cons_tail hxa, List.erase_cons_head]
          have hf : (b != a && b != b) = false := by simp
          simp only [List.filter_cons, hf, ↓reduceIte]
          exact hih
        · simp [List.erase_cons_tail hxa, List.erase_cons_tail hxb,
            show (x != a) = true by simpa using hxa,
            show (x != b) = true by simpa using hxb, ih hp.2]

private theorem integrate_pivot (n : Nat) (vs : List Nat) (src dst : Nat) (b : Bind) (f : Bind → Int)
    (hnd : vs.Nodup) (hne : src ≠ dst) :
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
      have hvars : (vs.erase src).erase dst = vs.filter (fun v => v != src && v != dst) :=
        erase_erase_filter vs src dst hnd
      have hdstm_erase : dst ∈ vs.erase src :=
        (List.mem_erase_of_ne (a := dst) (b := src) hne.symm).mpr hdstm
      rw [integrate_perm n (List.perm_cons_erase hsrcm) b f, if_pos hsrc, if_pos hdst]
      apply sum_congr
      intro u _
      rw [integrate_perm n (List.perm_cons_erase hdstm_erase) (put b src u) f, hvars]
      change ((List.range n).map (fun w =>
        integrate n (vs.filter (fun v => v != src && v != dst))
          (put (put b src u) dst w) f)).sum =
        ((List.range n).map (fun w =>
          integrate n (vs.filter (fun v => v != src && v != dst))
            (put (put b src u) dst w) f)).sum
      rfl
    · have hdstm : dst ∉ vs := by simpa using hdst
      have hvars : vs.filter (fun v => v != src && v != dst) = vs.erase src := by
        rw [filter_and_ne_of_not_mem2 vs src dst hdstm, filter_ne_eq_erase vs src hnd]
      rw [integrate_perm n (List.perm_cons_erase hsrcm) b f, if_pos hsrc, if_neg hdst]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
      apply sum_congr
      intro u _
      rw [hvars, put_dst_pinned b src dst u hne]
  · have hsrcm : src ∉ vs := by simpa using hsrc
    by_cases hdst : vs.contains dst = true
    · have hdstm : dst ∈ vs := by simpa using hdst
      have hvars : vs.filter (fun v => v != src && v != dst) = vs.erase dst := by
        rw [filter_and_ne_of_not_mem vs src dst hsrcm, filter_ne_eq_erase vs dst hnd]
      rw [integrate_perm n (List.perm_cons_erase hdstm) b f, if_neg hsrc, if_pos hdst]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
      apply sum_congr
      intro w _
      rw [hvars, put_pinned b src]
    · have hdstm : dst ∉ vs := by simpa using hdst
      have hvars : vs.filter (fun v => v != src && v != dst) = vs := by
        apply List.filter_eq_self.mpr
        intro v hv
        simp [show v ≠ src from fun h => hsrcm (h ▸ hv),
          show v ≠ dst from fun h => hdstm (h ▸ hv)]
      rw [if_neg hsrc, if_neg hdst, hvars]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
      rw [show put (put b src (b src)) dst (b dst) = b by rw [put_pinned, put_pinned]]

private theorem integrate_fixed2 (n : Nat) (vs : List Nat) (b : Bind) (v w : Nat)
    (hv : v ∉ vs) (hw : w ∉ vs) (F : Nat → Nat → Bind → Int) :
    integrate n vs b (fun x => F (x v) (x w) x) = integrate n vs b (fun x => F (b v) (b w) x) := by
  have hfix : ∀ (u : Nat) (G : Nat → Bind → Int), u ∉ vs →
      integrate n vs b (fun x => G (x u) x) = integrate n vs b (fun x => G (b u) x) := by
    revert hv hw
    induction vs generalizing b with
    | nil => intro hv hw u G hu; rfl
    | cons z zs ih =>
        intro hv hw u G hu
        have hzu : u ≠ z := fun h => hu (by simp [h])
        have hzs : u ∉ zs := fun h => hu (by simp [h])
        have hvzs : v ∉ zs := fun h => hv (by simp [h])
        have hwzs : w ∉ zs := fun h => hw (by simp [h])
        exact sum_congr _ _ _ (fun t _ => by
          rw [ih (put b z t) hvzs hwzs u G hzs, put_get_other b z u t hzu])
  exact (hfix v (fun u x => F u (x w) x) hv).trans
    (hfix w (fun u x => F (b v) u x) hw)

private def cpExcl (atoms : List Atom) (old new : DB) (i : Nat) (b : Bind) : Int :=
  ((atoms.take i).map (fun a => value new a b)).prod *
    ((atoms.drop (i+1)).map (fun a => value old a b)).prod

private theorem cpExcl_zero (a : Atom) (as : List Atom) (old new : DB) (b : Bind) :
    cpExcl (a::as) old new 0 b = (as.map (fun t => value old t b)).prod := by simp [cpExcl]

private theorem cpExcl_succ (a : Atom) (as : List Atom) (old new : DB) (i : Nat) (b : Bind) :
    cpExcl (a::as) old new (i+1) b = value new a b * cpExcl as old new i b := by
  simp [cpExcl, Int.mul_assoc]

private theorem zipIdx_add_sum (l : List Atom) (F : Nat → Atom → Int) (k j : Nat) :
    ((l.zipIdx (k+j)).map (fun (a, i) => F i a)).sum =
      ((l.zipIdx k).map (fun (a, i) => F (i+j) a)).sum := by
  induction l generalizing k with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons, List.sum_cons]
      rw [show k + j + 1 = (k+1) + j by omega, ih (k+1)]

private theorem prod_diff_telescope (atoms : List Atom) (old new : DB) (b : Bind) :
    (atoms.map (fun a => value new a b)).prod - (atoms.map (fun a => value old a b)).prod =
      ((atoms.zipIdx.map (fun (a, i) =>
        (value new a b - value old a b) * cpExcl atoms old new i b)).sum) := by
  induction atoms with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.prod_cons, List.zipIdx_cons]
      rw [cpExcl_zero a as old new b]
      simp only [List.sum_cons, Nat.zero_add]
      let L : Int := value new a b * (as.map (fun t => value new t b)).prod -
        value old a b * (as.map (fun t => value old t b)).prod
      let base : Int := (value new a b - value old a b) *
        (as.map (fun t => value old t b)).prod
      let S : Int := ((as.zipIdx 1).map (fun (t, i) =>
        (value new t b - value old t b) * cpExcl (a :: as) old new i b)).sum
      let T : Int := ((as.zipIdx).map (fun (t, i) =>
        (value new t b - value old t b) * cpExcl (a :: as) old new (i+1) b)).sum
      let U : Int := ((as.zipIdx).map (fun (t, i) =>
        value new a b * ((value new t b - value old t b) * cpExcl as old new i b))).sum
      change L = base + S
      have hzip : S = T := by
        dsimp only [S, T]
        have hz := zipIdx_add_sum as (fun i atom =>
          (value new atom b - value old atom b) * cpExcl (a :: as) old new i b) 0 1
        simp only [Nat.zero_add] at hz
        change ((as.zipIdx 1).map (fun (t, i) =>
            (value new t b - value old t b) * cpExcl (a :: as) old new i b)).sum =
          ((as.zipIdx).map (fun (t, i) =>
            (value new t b - value old t b) * cpExcl (a :: as) old new (i+1) b)).sum at hz
        exact hz
      have hstep : T = U := by
        dsimp only [T, U]
        apply sum_congr
        intro t _
        rcases t with ⟨atom, i⟩
        change (value new atom b - value old atom b) * cpExcl (a :: as) old new (i+1) b =
          value new a b * ((value new atom b - value old atom b) * cpExcl as old new i b)
        rw [cpExcl_succ]
        ac_rfl
      have hlast : base + U = value new a b * (as.map (fun t => value new t b)).prod -
          value old a b * (as.map (fun t => value old t b)).prod := by
        dsimp only [base, U]
        rw [sum_mul_left, ← ih, Int.sub_mul, Int.mul_sub]
        omega
      have hfirst : value new a b * (as.map (fun t => value new t b)).prod -
          value old a b * (as.map (fun t => value old t b)).prod = base + S :=
        hlast.symm.trans ((congrArg (fun z => base + z) hstep.symm).trans
          (congrArg (fun z => base + z) hzip.symm))
      exact hfirst

private theorem sum_filter_eq {α} (l : List α) (p : α → Bool) (g : α → Int)
    (h : ∀ x ∈ l, p x = false → g x = 0) :
    ((l.filter p).map g).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases hx : p x = true
      · simp [hx, ih (fun y hy hp => h y (by simp [hy]) hp)]
      · have hx0 : g x = 0 := h x (by simp) (bool_false_of_not_true hx)
        simp [hx, hx0, ih (fun y hy hp => h y (by simp [hy]) hp)]

private theorem filter_singleton_sum (x : Nat) (p : Nat → Bool) (g : Nat → Int)
    (hzero : p x = false → g x = 0) : (([x].filter p).map g).sum = g x := by
  rw [List.filter_cons]
  by_cases hp : p x = true
  · simp [hp]
  · have hp0 := bool_false_of_not_true hp
    rw [hzero hp0]
    simp [hp]

private theorem pivot_point (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (j : Nat) (a : Atom) (hnd : p.freeVars.Nodup) (hsd : a.src ≠ a.dst) (u w : Nat) :
    let vars := p.freeVars.filter (fun v => v != a.src && v != a.dst)
    let b' := put (put b a.src u) a.dst w
    integrate n vars b' (fun x => (value new a x - value old a x) * cpExcl p.atoms old new j x) =
      (new a.rel u w - old a.rel u w) *
        integrate n vars b' (fun x => cpExcl p.atoms old new j x) := by
  dsimp only
  have hsrc_not : a.src ∉ p.freeVars.filter (fun v => v != a.src && v != a.dst) := by simp
  have hdst_not : a.dst ∉ p.freeVars.filter (fun v => v != a.src && v != a.dst) := by simp
  have hsrc_b : put (put b a.src u) a.dst w a.src = u := by
    rw [put_get_other (put b a.src u) a.dst a.src w hsd]
    simp [put]
  have hdst_b : put (put b a.src u) a.dst w a.dst = w := by simp [put]
  change integrate n _ (put (put b a.src u) a.dst w)
      (fun x => (new a.rel (x a.src) (x a.dst) - old a.rel (x a.src) (x a.dst)) *
        cpExcl p.atoms old new j x) = _
  rw [integrate_fixed2 n _ _ a.src a.dst hsrc_not hdst_not
    (fun u v x => (new a.rel u v - old a.rel u v) * cpExcl p.atoms old new j x)]
  rw [hsrc_b, hdst_b, integrate_mul_left]

private theorem candidates_mem (vs : List Nat) (n : Nat) (b : Bind) (v : Nat) (hv : v ∈ vs) :
    (if vs.contains v then List.range n else [b v]) = List.range n := by simp [hv]

private theorem candidates_notmem (vs : List Nat) (n : Nat) (b : Bind) (v : Nat) (hv : v ∉ vs) :
    (if vs.contains v then List.range n else [b v]) = [b v] := by simp [hv]

private theorem int_eq_of_ne_eq_false {a b : Int} (h : (a != b) = false) : a = b := by
  by_cases hab : a = b
  · exact hab
  · have hb : (a != b) = true := by simp [hab]
    simp [hb] at h

private theorem pivot_integral_eq (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (j : Nat) (a : Atom) (hnd : p.freeVars.Nodup)
    (hgood : a.src ≠ a.dst ∨ (a.src = a.dst ∧ a.src ∉ p.freeVars)) :
    integrate n p.freeVars b (fun x =>
      (value new a x - value old a x) * cpExcl p.atoms old new j x) = pivotTerm p n old new b j a := by
  rcases hgood with hsd | ⟨heq, hpinned⟩
  · let src := a.src
    let dst := a.dst
    let vars := p.freeVars.filter (fun v => v != src && v != dst)
    let cand := fun v => if p.freeVars.contains v then List.range n else [b v]
    have hpiv := integrate_pivot n p.freeVars src dst b
      (fun x => (value new a x - value old a x) * cpExcl p.atoms old new j x) hnd hsd
    dsimp only at hpiv
    rw [hpiv]
    apply sum_congr
    intro u _
    rw [← sum_filter_eq (if p.freeVars.contains dst then List.range n else [b dst])
      (fun w => old a.rel u w != new a.rel u w)
      (fun w => integrate n vars (put (put b src u) dst w)
        (fun x => (value new a x - value old a x) * cpExcl p.atoms old new j x))
      (by
        intro w _ hp
        have heq : old a.rel u w = new a.rel u w := int_eq_of_ne_eq_false hp
        have hz := pivot_point p n old new b j a hnd hsd u w
        dsimp only [vars, src, dst] at hz
        rw [hz, heq]
        simp)]
    apply sum_congr
    intro w _
    have hz := pivot_point p n old new b j a hnd hsd u w
    dsimp only [vars, src, dst] at hz
    exact hz
  · have hdst : a.dst ∉ p.freeVars := by simpa [heq] using hpinned
    have hvars : p.freeVars.filter (fun v => v != a.src && v != a.dst) = p.freeVars := by
      apply List.filter_eq_self.mpr
      intro v hv
      simp [show v ≠ a.src from fun h => hpinned (h ▸ hv),
        show v ≠ a.dst from fun h => hdst (h ▸ hv)]
    simp only [pivotTerm, hvars, candidates_notmem p.freeVars n b a.src hpinned,
      candidates_notmem p.freeVars n b a.dst hdst]
    simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
    change integrate n p.freeVars b
        (fun x => (new a.rel (x a.src) (x a.dst) - old a.rel (x a.src) (x a.dst)) *
          cpExcl p.atoms old new j x) =
      ((([b a.dst]).filter (fun v => old a.rel (b a.src) v != new a.rel (b a.src) v)).map
        (fun v => (new a.rel (b a.src) v - old a.rel (b a.src) v) *
          integrate n p.freeVars (put (put b a.src (b a.src)) a.dst v)
            (fun x => cpExcl p.atoms old new j x))).sum
    rw [filter_singleton_sum (b a.dst) (fun v => old a.rel (b a.src) v != new a.rel (b a.src) v)
      (fun v => (new a.rel (b a.src) v - old a.rel (b a.src) v) *
        integrate n p.freeVars (put (put b a.src (b a.src)) a.dst v)
          (fun x => cpExcl p.atoms old new j x))
      (by
        intro hp
        have heq' := int_eq_of_ne_eq_false hp
        simp [heq'])]
    rw [show put (put b a.src (b a.src)) a.dst (b a.dst) = b by
      rw [put_pinned, put_pinned]]
    rw [integrate_fixed2 n p.freeVars b a.src a.dst hpinned hdst
      (fun u v x => (new a.rel u v - old a.rel u v) * cpExcl p.atoms old new j x)]
    simpa [heq] using integrate_mul_left n p.freeVars b
      (new a.rel (b a.src) (b a.dst) - old a.rel (b a.src) (b a.dst))
      (fun x => cpExcl p.atoms old new j x)

private theorem anchored_repair_generic (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (hnd : p.freeVars.Nodup)
    (hgood : ∀ a ∈ p.atoms, a.src ≠ a.dst ∨ (a.src = a.dst ∧ a.src ∉ p.freeVars)) :
    reference p n new b - reference p n old b =
      (p.atoms.zipIdx.map (fun (a, i) => pivotTerm p n old new b i a)).sum := by
  have htel : ∀ x : Bind,
      (p.atoms.map (fun a => value new a x)).prod - (p.atoms.map (fun a => value old a x)).prod =
        ((p.atoms.zipIdx.map (fun (a, i) =>
          (value new a x - value old a x) * cpExcl p.atoms old new i x)).sum) :=
    fun x => prod_diff_telescope p.atoms old new x
  calc
    reference p n new b - reference p n old b
        = integrate n p.freeVars b (fun x => (p.atoms.map (fun a => value new a x)).prod) -
            integrate n p.freeVars b (fun x => (p.atoms.map (fun a => value old a x)).prod) := rfl
    _ = integrate n p.freeVars b (fun x =>
          (p.atoms.map (fun a => value new a x)).prod -
          (p.atoms.map (fun a => value old a x)).prod) := by rw [integrate_sub]
    _ = integrate n p.freeVars b (fun x => (p.atoms.zipIdx.map (fun (a, i) =>
          (value new a x - value old a x) * cpExcl p.atoms old new i x)).sum) :=
          integrate_congr n p.freeVars b _ _ htel
    _ = (p.atoms.zipIdx.map (fun (a, i) => integrate n p.freeVars b (fun x =>
          (value new a x - value old a x) * cpExcl p.atoms old new i x))).sum := by
          rw [integrate_list_sum]
    _ = (p.atoms.zipIdx.map (fun (a, i) => pivotTerm p n old new b i a)).sum := by
          apply sum_congr
          intro ai hai
          rcases ai with ⟨a, i⟩
          have hz := List.mem_zipIdx' hai
          have ha : a ∈ p.atoms := by
            rw [hz.2]
            exact List.get_mem p.atoms ⟨i, hz.1⟩
          exact pivot_integral_eq p n old new b i a hnd (hgood a ha)

/-! The five assigned models: boundary messages and anchored repair. -/

theorem sound_CountryReturn_BoundaryMessages : CountryReturn.BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference CountryReturn.pattern n db b

theorem sound_CountryReturn_AnchoredRepair : CountryReturn.AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic CountryReturn.pattern n old new b
    (by simp [CountryReturn.pattern])
    (by
      intro a ha
      simp only [CountryReturn.pattern, List.mem_cons, List.not_mem_nil, or_false] at ha
      rcases ha with rfl | rfl | rfl | rfl | rfl <;> simp)

theorem sound_OrderSupplier_BoundaryMessages : OrderSupplier.BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference OrderSupplier.pattern n db b

theorem sound_OrderSupplier_AnchoredRepair : OrderSupplier.AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic OrderSupplier.pattern n old new b
    (by simp [OrderSupplier.pattern])
    (by
      intro a ha
      simp only [OrderSupplier.pattern, List.mem_cons, List.not_mem_nil, or_false] at ha
      rcases ha with rfl | rfl | rfl | rfl | rfl <;> simp)

theorem sound_ReturnedItem_BoundaryMessages : ReturnedItem.BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference ReturnedItem.pattern n db b

theorem sound_ReturnedItem_AnchoredRepair : ReturnedItem.AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic ReturnedItem.pattern n old new b
    (by simp [ReturnedItem.pattern])
    (by
      intro a ha
      simp only [ReturnedItem.pattern, List.mem_cons, List.not_mem_nil, or_false] at ha
      rcases ha with rfl | rfl | rfl | rfl | rfl <;> simp)

theorem sound_ChannelBridge_BoundaryMessages : ChannelBridge.BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference ChannelBridge.pattern n db b

theorem sound_ChannelBridge_AnchoredRepair : ChannelBridge.AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic ChannelBridge.pattern n old new b
    (by simp [ChannelBridge.pattern])
    (by
      intro a ha
      simp only [ChannelBridge.pattern, List.mem_cons, List.not_mem_nil, or_false] at ha
      rcases ha with rfl | rfl | rfl | rfl | rfl | rfl <;> simp)

theorem sound_PromoInventory_BoundaryMessages : PromoInventory.BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference PromoInventory.pattern n db b

theorem sound_PromoInventory_AnchoredRepair : PromoInventory.AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic PromoInventory.pattern n old new b
    (by simp [PromoInventory.pattern])
    (by
      intro a ha
      simp only [PromoInventory.pattern, List.mem_cons, List.not_mem_nil, or_false] at ha
      rcases ha with rfl | rfl | rfl | rfl | rfl | rfl <;> simp)

/-! Structural join identities. -/

private theorem rsum_four_factor (n : Nat) (f g : Nat → Nat → Int) :
    rsum n (fun i => rsum n (fun j => rsum n (fun k => rsum n (fun l => f i j * g k l)))) =
      (rsum n (fun i => rsum n (f i))) * (rsum n (fun k => rsum n (g k))) := by
  calc
    rsum n (fun i => rsum n (fun j => rsum n (fun k => rsum n (fun l => f i j * g k l))))
        = rsum n (fun i => rsum n (fun j => rsum n (fun k => f i j * rsum n (g k)))) := by
          apply rsum_congr; intro i _
          apply rsum_congr; intro j _
          apply rsum_congr; intro k _
          exact rsum_const_mul n (f i j) (g k)
    _ = rsum n (fun i => rsum n (fun j => f i j * rsum n (fun k => rsum n (g k)))) := by
          apply rsum_congr; intro i _
          apply rsum_congr; intro j _
          exact rsum_const_mul n (f i j) (fun k => rsum n (g k))
    _ = rsum n (fun i => rsum n (f i) * rsum n (fun k => rsum n (g k))) := by
          apply rsum_congr; intro i _
          exact rsum_mul_const n (f i) _
    _ = (rsum n (fun i => rsum n (f i))) * (rsum n (fun k => rsum n (g k))) :=
          rsum_mul_const n _ _

private theorem rsum_three_factor (n : Nat) (f g h : Nat → Int) :
    rsum n (fun i => rsum n (fun j => rsum n (fun k => f i * (g j * h k)))) =
      rsum n f * rsum n g * rsum n h := by
  calc
    rsum n (fun i => rsum n (fun j => rsum n (fun k => f i * (g j * h k))))
        = rsum n (fun i => f i * rsum n (fun j => rsum n (fun k => g j * h k))) := by
          apply rsum_congr; intro i _
          rw [show rsum n (fun j => rsum n (fun k => f i * (g j * h k))) =
              rsum n (fun j => f i * rsum n (fun k => g j * h k)) by
                apply rsum_congr; intro j _
                exact rsum_const_mul n (f i) (fun k => g j * h k)]
          exact rsum_const_mul n (f i) (fun j => rsum n (fun k => g j * h k))
    _ = rsum n (fun i => f i * (rsum n g * rsum n h)) := by
          apply rsum_congr; intro i _
          rw [rsum_product]
    _ = rsum n f * (rsum n g * rsum n h) := rsum_mul_const n _ _
    _ = rsum n f * rsum n g * rsum n h := by ac_rfl

theorem sound_OrderSupplier_SharedDestination : OrderSupplier.SharedDestination.Goal := by
  intro n db b
  unfold reference
  have hperm : OrderSupplier.pattern.freeVars.Perm [3, 2, 1, 4] := by
    change [1, 2, 4, 3].Perm [3, 2, 1, 4]
    exact (List.Perm.swap 2 1 [4, 3]).trans
      ((List.Perm.cons 2 (List.Perm.cons 1 (List.Perm.swap 3 4 []))).trans
        ((List.Perm.cons 2 (List.Perm.swap 3 1 [4])).trans
          (List.Perm.swap 3 2 [1, 4])))
  rw [integrate_perm n hperm b]
  simp [reference, integrate, OrderSupplier.pattern, value, put, bridge, total]
  change rsum n (fun z => rsum n (fun y => rsum n (fun x1 => rsum n (fun x4 =>
      db 0 (b 0) x1 * (db 1 x1 y * (db 2 y z * (db 1 (b 0) x4 * db 2 x4 z))))))) =
    rsum n (fun z =>
      ((rsum n (fun y =>
        (rsum n (fun x1 => db 0 (b 0) x1 * db 1 x1 y)) * db 2 y z)) *
       rsum n (fun x4 => db 1 (b 0) x4 * db 2 x4 z)))
  apply rsum_congr
  intro z _
  rw [← rsum_mul_const n (fun y =>
    rsum n (fun x1 => db 0 (b 0) x1 * db 1 x1 y) * db 2 y z)
    (rsum n (fun x4 => db 1 (b 0) x4 * db 2 x4 z))]
  apply rsum_congr
  intro y _
  rw [show rsum n (fun x1 => rsum n (fun x4 =>
      db 0 (b 0) x1 * (db 1 x1 y * (db 2 y z * (db 1 (b 0) x4 * db 2 x4 z))))) =
      rsum n (fun x1 => rsum n (fun x4 =>
        (db 0 (b 0) x1 * db 1 x1 y * db 2 y z) * (db 1 (b 0) x4 * db 2 x4 z))) by
        apply rsum_congr; intro x1 _
        apply rsum_congr; intro x4 _
        ac_rfl]
  rw [rsum_product, rsum_mul_const]

theorem sound_ReturnedItem_AnchoredChord : ReturnedItem.AnchoredChord.Goal := by
  intro n db b
  unfold reference
  have hperm : ReturnedItem.pattern.freeVars.Perm [2, 1, 3] := by
    exact List.Perm.swap 2 1 [3]
  rw [integrate_perm n hperm b]
  simp [reference, integrate, ReturnedItem.pattern, value, put, bridge, total]
  change rsum n (fun y => rsum n (fun x1 => rsum n (fun x3 =>
      db 0 (b 0) x1 * (db 1 x1 y * (db 2 y x3 * (db 3 x3 (b 0) * db 2 (b 0) y)))))) =
    rsum n (fun y => db 2 (b 0) y *
      rsum n (fun x1 => db 0 (b 0) x1 * db 1 x1 y) *
      rsum n (fun x3 => db 2 y x3 * db 3 x3 (b 0)))
  apply rsum_congr
  intro y _
  rw [show rsum n (fun x1 => rsum n (fun x3 =>
      db 0 (b 0) x1 * (db 1 x1 y * (db 2 y x3 * (db 3 x3 (b 0) * db 2 (b 0) y))))) =
      rsum n (fun x1 => rsum n (fun x3 =>
        (db 0 (b 0) x1 * db 1 x1 y) *
          ((db 2 y x3 * db 3 x3 (b 0)) * db 2 (b 0) y))) by
        apply rsum_congr; intro x1 _
        apply rsum_congr; intro x3 _
        ac_rfl]
  rw [rsum_product]
  rw [show rsum n (fun x3 => (db 2 y x3 * db 3 x3 (b 0)) * db 2 (b 0) y) =
      rsum n (fun x3 => db 2 y x3 * db 3 x3 (b 0)) * db 2 (b 0) y by
        exact rsum_mul_const n _ _]
  ac_rfl

theorem sound_ChannelBridge_TripleOverlap : ChannelBridge.TripleOverlap.Goal := by
  intro n db b
  unfold reference
  have hperm : ChannelBridge.pattern.freeVars.Perm [1, 2, 3, 4] := by
    change [2, 3, 4, 1].Perm [1, 2, 3, 4]
    exact (List.Perm.cons 2 (List.Perm.cons 3 (List.Perm.swap 1 4 []))).trans
      ((List.Perm.cons 2 (List.Perm.swap 1 3 [4])).trans
        (List.Perm.swap 1 2 [3, 4]))
  rw [integrate_perm n hperm b]
  simp [reference, integrate, ChannelBridge.pattern, value, put, overlap, dot, total]
  change rsum n (fun y => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      db 0 (b 0) x2 * (db 0 y x2 * (db 1 (b 0) x3 *
        (db 1 y x3 * (db 2 (b 0) x4 * db 2 y x4)))))))) =
    rsum n (fun y => rsum n (fun x2 => db 0 (b 0) x2 * db 0 y x2) *
      rsum n (fun x3 => db 1 (b 0) x3 * db 1 y x3) *
      rsum n (fun x4 => db 2 (b 0) x4 * db 2 y x4))
  apply rsum_congr
  intro y _
  rw [show rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      db 0 (b 0) x2 * (db 0 y x2 * (db 1 (b 0) x3 *
        (db 1 y x3 * (db 2 (b 0) x4 * db 2 y x4))))))) =
      rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
        (db 0 (b 0) x2 * db 0 y x2) *
          ((db 1 (b 0) x3 * db 1 y x3) * (db 2 (b 0) x4 * db 2 y x4))))) by
        apply rsum_congr; intro x2 _
        apply rsum_congr; intro x3 _
        apply rsum_congr; intro x4 _
        ac_rfl]
  rw [rsum_three_factor]

private theorem sum_sub {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x - g x)).sum = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]; omega

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.SalesJoinBoundaryMessages
