import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `ParallelLineage.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
- `ParallelLineage.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.ParallelLineageCapsAndCollisionCorrection

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ParallelLineage

/-! ## Finite-range sum infrastructure (Std only). -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_zero (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
  induction m with
  | zero => simp [rsum]
  | succ m ih => rw [rsum_succ, ih]; simp

private theorem rsum_const_zero (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := rsum_zero m

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
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_sub (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i - g i) = rsum n f - rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.mul_add]

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.add_mul]

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      simp [rsum]
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

private theorem rsum_product (n m : Nat) (f g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr
            intro i hi
            rw [rsum_const_mul]
    _ = rsum n f * rsum m g := by rw [rsum_mul_const]

private theorem rsum_nonneg (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → 0 ≤ f i) :
    0 ≤ rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

private theorem rsum_le (n : Nat) (f g : Nat → Int) (h : ∀ i, i < n → f i ≤ g i) :
    rsum n f ≤ rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

private theorem rsum_le_total (n d : Nat) (f : Nat → Int) (hd : d < n)
    (hf : ∀ i, i < n → 0 ≤ f i) : f d ≤ rsum n f := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsum_succ]
      by_cases hdn : d < n
      · have hi := ih hdn (fun i hi => hf i (Nat.lt_succ_of_lt hi))
        have hn := hf n (Nat.lt_succ_self n)
        omega
      · have hdeq : d = n := by omega
        subst d
        have hi := rsum_nonneg n f (fun i hi => hf i (Nat.lt_succ_of_lt hi))
        omega

private theorem rsum_zero_of_all_zero (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → f i = 0) :
    rsum n f = 0 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi)), h n (Nat.lt_succ_self n)]
      simp

private theorem list_sum_congr {α} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem list_sum_mul_right {α} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons x l ih => simp [ih, Int.add_mul]

private theorem list_sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem cast_list_sum (l : List Nat) (f : Nat → Nat) :
    ((l.map f).sum : Int) = (l.map (fun x => (f x : Int))).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

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

/-! ## Finite-support injection machinery for integral edge-cover bounds. -/

private theorem put_self (b : Bind) (v x : Nat) : put b v x v = x := by simp [put]

private theorem put_get_other (b : Bind) (v w x : Nat) (h : w ≠ v) :
    put b v x w = b w := by
  have h' : (w == v) = false := by
    cases hw : w == v <;> simp_all
  simp [put, h']

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
      apply list_sum_congr
      intro x hx
      exact ih (put b v x)

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

/-! ## FractionalCap -/

private theorem list6_of_length6 {α : Type} (l : List α) (h : l.length = 6) :
    ∃ a b c d e f, l = [a, b, c, d, e, f] := by
  cases l with
  | nil => simp at h
  | cons a l =>
      cases l with
      | nil => simp at h
      | cons b l =>
          cases l with
          | nil => simp at h
          | cons c l =>
              cases l with
              | nil => simp at h
              | cons d l =>
                  cases l with
                  | nil => simp at h
                  | cons e l =>
                      cases l with
                      | nil => simp at h
                      | cons f l =>
                          cases l with
                          | nil => exact ⟨a, b, c, d, e, f, rfl⟩
                          | cons g l => simp at h

private theorem pow_mono_of_pos {x a b : Nat} (hx : 0 < x) (hab : a ≤ b) : x^a ≤ x^b := by
  have hs : b = a + (b - a) := by omega
  rw [hs, Nat.pow_add]
  exact Nat.le_mul_of_pos_right (x^a) (show 0 < x ^ (b - a) from Nat.pow_pos hx)

private def e0 : Atom := ⟨0, 0, 1⟩
private def e1 : Atom := ⟨0, 1, 2⟩
private def e2 : Atom := ⟨0, 0, 1⟩
private def e3 : Atom := ⟨1, 2, 3⟩
private def e4 : Atom := ⟨1, 3, 0⟩
private def e5 : Atom := ⟨2, 3, 4⟩

private def C1 : List Atom := [e1, e5]
private def C2 : List Atom := [e0, e3, e5]

theorem sound_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ParallelLineage.FractionalCap.Goal := by
  intro n db b weights den hcov
  rcases hcov with ⟨hdenpos, hlen, hc⟩
  rcases list6_of_length6 weights (by simpa [pattern] using hlen) with
    ⟨w0, w1, w2, w3, w4, w5, rfl⟩
  have h5 : den ≤ w5 := by
    have hh := hc 4 (by simp [pattern])
    simpa [pattern] using hh
  have h1 : den ≤ w0 + w1 + w2 := by
    have hh := hc 1 (by simp [pattern])
    simp [pattern] at hh
    omega
  have h2 : den ≤ w1 + w3 := by
    have hh := hc 2 (by simp [pattern])
    simpa [pattern] using hh
  let S (e : Atom) := support n db e pattern b
  let L := (satBindings pattern n db b).length
  have hb1 : L ≤ S e1 * S e5 := by
    dsimp [L, S]
    simpa [C1] using
      (ordinary_cover_bound pattern n db b C1
        (by
          intro v hv
          simp [pattern] at hv
          rcases hv with rfl | rfl | rfl | rfl <;>
            simp [C1, e0, e1, e2, e3, e4, e5])
        (by
          intro a ha
          simp [C1] at ha ⊢
          rcases ha with rfl | rfl <;> simp [e1, e5, pattern])
        (by decide))
  have hb2 : L ≤ S e0 * S e3 * S e5 := by
    dsimp [L, S]
    simpa [C2, Nat.mul_assoc] using
      (ordinary_cover_bound pattern n db b C2
        (by
          intro v hv
          simp [pattern] at hv
          rcases hv with rfl | rfl | rfl | rfl <;>
            simp [C2, e0, e1, e2, e3, e4, e5])
        (by
          intro a ha
          simp [C2] at ha ⊢
          rcases ha with rfl | rfl | rfl <;> simp [e0, e3, e5, pattern])
        (by decide))
  have href : (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat = L := by
    have hbin : (fun r x y : Nat => if db r x y == 0 then (0 : Int) else 1) = bin db := by
      funext r x y
      simp [bin]
    rw [hbin]
    dsimp [L]
    exact congrArg Int.toNat (reference_bin_count pattern n db b)
  have hRHS :
      ((pattern.atoms.zip [w0, w1, w2, w3, w4, w5]).map
        (fun (a, w) => support n db a pattern b ^ w)).prod =
      S e0 ^ w0 * S e1 ^ w1 * S e2 ^ w2 * S e3 ^ w3 * S e4 ^ w4 * S e5 ^ w5 := by
    simp [pattern, S, e0, e1, e2, e3, e4, e5, Nat.mul_assoc]
  rw [href, hRHS]
  by_cases hL0 : L = 0
  · rw [hL0]
    simp [hdenpos]
  · have hLpos : 0 < L := Nat.pos_of_ne_zero hL0
    have hSpos : ∀ e ∈ pattern.atoms, 0 < support n db e pattern b := by
      intro e he
      have hlenpos : 0 < (satBindings pattern n db b).length := by simpa [L] using hLpos
      obtain ⟨x, hx⟩ := List.exists_mem_of_ne_nil _ ((List.length_pos_iff).mp hlenpos)
      have hxbind : x ∈ bindings n pattern.freeVars b := (List.mem_filter.mp hx).1
      have hsat : satisfies pattern (bin db) x = true := (List.mem_filter.mp hx).2
      have hnz : db e.rel (x e.src) (x e.dst) != 0 := by
        have hall : pattern.atoms.all (fun t => value (bin db) t x != 0) = true := hsat
        simpa [value, bin, beq_iff_eq] using (List.all_eq_true.mp hall e he)
      have hmemsl := supportList_mem pattern n db b e x hxbind hnz
      have hslne : supportList pattern n db b e ≠ [] := by
        intro hnil
        have hnot : (x e.src, x e.dst) ∉ supportList pattern n db b e := by simp [hnil]
        exact hnot hmemsl
      have hslpos : 0 < (supportList pattern n db b e).length :=
        (List.length_pos_iff).mpr hslne
      rw [supportList_length] at hslpos
      exact hslpos
    have hS0p : 0 < S e0 := by dsimp [S]; exact hSpos e0 (by simp [e0, pattern])
    have hS1p : 0 < S e1 := by dsimp [S]; exact hSpos e1 (by simp [e1, pattern])
    have hS2p : 0 < S e2 := by dsimp [S]; exact hSpos e2 (by simp [e2, pattern])
    have hS3p : 0 < S e3 := by dsimp [S]; exact hSpos e3 (by simp [e3, pattern])
    have hS4p : 0 < S e4 := by dsimp [S]; exact hSpos e4 (by simp [e4, pattern])
    have hS5p : 0 < S e5 := by dsimp [S]; exact hSpos e5 (by simp [e5, pattern])
    let bb := min den (min w3 (w0 + w2))
    let aa := den - bb
    have harith : aa + bb = den ∧ aa ≤ w1 ∧ bb ≤ w3 ∧ bb ≤ w0 + w2 ∧ bb ≤ den := by
      dsimp [aa, bb]
      omega
    have hpow : L ^ den ≤ (S e1 * S e5) ^ aa * (S e0 * S e3 * S e5) ^ bb := by
      calc
        L ^ den = L ^ aa * L ^ bb := by
          rw [← harith.1, Nat.pow_add]
        _ ≤ (S e1 * S e5) ^ aa * (S e0 * S e3 * S e5) ^ bb :=
          Nat.mul_le_mul (Nat.pow_le_pow_left hb1 aa) (Nat.pow_le_pow_left hb2 bb)
    have hprod : (S e1 * S e5) ^ aa * (S e0 * S e3 * S e5) ^ bb =
        S e0 ^ bb * S e1 ^ aa * S e3 ^ bb * S e5 ^ den := by
      rw [Nat.mul_pow, Nat.mul_pow, Nat.mul_pow]
      rw [← harith.1, Nat.pow_add]
      ac_rfl
    have hm0 : S e0 ^ bb ≤ S e0 ^ (w0 + w2) := pow_mono_of_pos hS0p harith.2.2.2.1
    have hm1 : S e1 ^ aa ≤ S e1 ^ w1 := pow_mono_of_pos hS1p harith.2.1
    have hm3 : S e3 ^ bb ≤ S e3 ^ w3 := pow_mono_of_pos hS3p harith.2.2.1
    have hm5 : S e5 ^ den ≤ S e5 ^ w5 := pow_mono_of_pos hS5p h5
    have hmid : S e0 ^ bb * S e1 ^ aa * S e3 ^ bb * S e5 ^ den ≤
        S e0 ^ (w0 + w2) * S e1 ^ w1 * S e3 ^ w3 * S e5 ^ w5 := by
      apply Nat.mul_le_mul
      · apply Nat.mul_le_mul
        · apply Nat.mul_le_mul
          · exact hm0
          · exact hm1
        · exact hm3
      · exact hm5
    have hD : 0 < S e4 ^ w4 := Nat.pow_pos hS4p
    calc
      L ^ den ≤ (S e1 * S e5) ^ aa * (S e0 * S e3 * S e5) ^ bb := hpow
      _ = S e0 ^ bb * S e1 ^ aa * S e3 ^ bb * S e5 ^ den := hprod
      _ ≤ S e0 ^ (w0 + w2) * S e1 ^ w1 * S e3 ^ w3 * S e5 ^ w5 := hmid
      _ = (S e0 ^ w0 * S e1 ^ w1 * S e3 ^ w3 * S e5 ^ w5) * S e0 ^ w2 := by
          rw [Nat.pow_add]
          ac_rfl
      _ ≤ (S e0 ^ w0 * S e1 ^ w1 * S e3 ^ w3 * S e5 ^ w5) * (S e4 ^ w4 * S e0 ^ w2) := by
          apply Nat.mul_le_mul_left
          exact Nat.le_mul_of_pos_left (S e0 ^ w2) hD
      _ = S e0 ^ w0 * S e1 ^ w1 * S e2 ^ w2 * S e3 ^ w3 * S e4 ^ w4 * S e5 ^ w5 := by
          change (S e0 ^ w0 * S e1 ^ w1 * S e3 ^ w3 * S e5 ^ w5) * (S e4 ^ w4 * S e0 ^ w2) =
            S e0 ^ w0 * S e1 ^ w1 * S e0 ^ w2 * S e3 ^ w3 * S e4 ^ w4 * S e5 ^ w5
          ac_rfl

/-! ## Möbius inversion infrastructure for CollisionCorrection. -/

private theorem rsum4_swap34 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => f a b c d)))) =
      rsum n (fun a => rsum n (fun b => rsum n (fun d => rsum n (fun c => f a b c d)))) := by
  apply rsum_congr
  intro a ha
  apply rsum_congr
  intro b hb
  exact rsum_comm n n (fun c d => f a b c d)

private theorem rsum4_swap23 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => f a b c d)))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun b => rsum n (fun d => f a b c d)))) := by
  apply rsum_congr
  intro a ha
  exact rsum_comm n n (fun b c => rsum n (fun d => f a b c d))

private theorem rsum4_swap12 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => f a b c d)))) =
      rsum n (fun b => rsum n (fun a => rsum n (fun c => rsum n (fun d => f a b c d)))) := by
  exact rsum_comm n n (fun a b => rsum n (fun c => rsum n (fun d => f a b c d)))

private theorem rsum4_zero (n : Nat) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => (0 : Int))))) = 0 := by
  rw [show (fun c => rsum n (fun d => (0 : Int))) = (fun _ => 0) by
        funext c; exact rsum_const_zero n]
  rw [show (fun b => rsum n (fun c => (0 : Int))) = (fun _ => 0) by
        funext b; exact rsum_const_zero n]
  rw [show (fun a => rsum n (fun b => (0 : Int))) = (fun _ => 0) by
        funext a; exact rsum_const_zero n]
  exact rsum_const_zero n

private theorem sum_rsum4 {α} (n : Nat) (l : List α) (f : α → Nat → Nat → Nat → Nat → Int) :
    (l.map (fun x => rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e =>
      f x a c d e)))))).sum =
    rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e =>
      (l.map (fun x => f x a c d e)).sum)))) := by
  induction l with
  | nil => simpa using (rsum4_zero n).symm
  | cons x l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      rw [← rsum_add]
      apply rsum_congr
      intro a ha
      rw [← rsum_add]
      apply rsum_congr
      intro c hc
      rw [← rsum_add]
      apply rsum_congr
      intro d hd
      rw [← rsum_add]

private theorem rsum4_ext (n : Nat) (f g : Nat → Nat → Nat → Nat → Int)
    (h : ∀ a c d e, a < n → c < n → d < n → e < n → f a c d e = g a c d e) :
    rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => f a c d e)))) =
    rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => g a c d e)))) := by
  apply rsum_congr
  intro a ha
  apply rsum_congr
  intro c hc
  apply rsum_congr
  intro d hd
  apply rsum_congr
  intro e he
  exact h a c d e ha hc hd he

private theorem const_mul_rsum4 (n : Nat) (c : Int) (g : Nat → Nat → Nat → Nat → Int) :
    c * rsum n (fun a => rsum n (fun b => rsum n (fun d => rsum n (fun e => g a b d e)))) =
      rsum n (fun a => rsum n (fun b => rsum n (fun d => rsum n (fun e =>
        c * g a b d e)))) := by
  rw [← rsum_const_mul]
  apply rsum_congr
  intro a ha
  rw [← rsum_const_mul]
  apply rsum_congr
  intro b hb
  rw [← rsum_const_mul]
  apply rsum_congr
  intro d hd
  rw [← rsum_const_mul]

private def bind4 (b : Bind) (a c d e : Nat) : Bind := put (put (put (put b 4 a) 1 c) 2 d) 3 e

/-- Product of the six atom values under the standard binding of freeVars. -/
private def F4 (db : DB) (b : Bind) (a c d e : Nat) : Int :=
  db 0 (b 0) c * (db 0 c d * (db 0 (b 0) c *
    (db 1 d e * (db 1 e (b 0) * db 2 e a))))

private def constOnB (bs : List (List Nat)) (a c d e : Nat) : Bool :=
  let x := bind4 (fun _ => 0) a c d e
  pattern.freeVars.all (fun v =>
    x v == x (((bs.find? (fun w => decide (v ∈ w))).bind List.head?).getD v))

private theorem rsum_single (n u : Nat) (hu : u < n) (g : Nat → Int) :
    rsum n (fun t => if t == u then g t else 0) = g u := by
  induction n with
  | zero =>
      simp [rsum] at hu
  | succ n ih =>
      rw [show rsum (n+1) (fun t => if t == u then g t else 0) =
          rsum n (fun t => if t == u then g t else 0) + (if n == u then g n else 0) by
            simp [rsum, List.range_succ, List.sum_append]]
      by_cases hnu : n == u
      · have hnu' : n = u := Nat.eq_of_beq_eq_true hnu
        subst u
        have hz : rsum n (fun t => if t = n then g t else 0) = 0 := by
          apply rsum_zero_of_all_zero
          intro t ht
          have htn : t ≠ n := by omega
          simp [htn]
        simpa [hz]
      · have hne : n ≠ u := by
          intro h
          subst u
          simp at hnu
        have hu' : u < n := by omega
        rw [ih hu']
        simp [hnu]

private theorem ite_and_comm (P Q : Prop) [Decidable P] [Decidable Q] (t : Int) :
    (if P ∧ Q then t else 0) = if Q then (if P then t else 0) else 0 := by
  by_cases hP : P <;> by_cases hQ : Q <;> simp [hP, hQ]

private theorem rsum_chain1 (n u : Nat) (hu : u < n) (g : Nat → Int) :
    rsum n (fun x => if x = u then g x else 0) = g u := by
  simpa using rsum_single n u hu g

private theorem rsum_chain2 (n ux uy : Nat) (hux : ux < n) (huy : uy < n)
    (g : Nat → Nat → Int) :
    rsum n (fun x => rsum n (fun y =>
      if y = uy then (if x = ux then g x y else 0) else 0)) = g ux uy := by
  rw [show (fun x => rsum n (fun y =>
        if y = uy then (if x = ux then g x y else 0) else 0)) =
      (fun x => if x = ux then g x uy else 0) by
        funext x
        simpa using (rsum_single n uy huy (fun t => if x = ux then g x t else 0))]
  exact rsum_chain1 n ux hux (fun x => g x uy)

private theorem rsum_chain3 (n ux uy uz : Nat) (hux : ux < n) (huy : uy < n) (huz : uz < n)
    (g : Nat → Nat → Nat → Int) :
    rsum n (fun x => rsum n (fun y => rsum n (fun z =>
      if z = uz then (if y = uy then (if x = ux then g x y z else 0) else 0) else 0))) =
      g ux uy uz := by
  rw [show (fun x => rsum n (fun y => rsum n (fun z =>
        if z = uz then (if y = uy then (if x = ux then g x y z else 0) else 0) else 0))) =
      (fun x => rsum n (fun y =>
        if y = uy then (if x = ux then g x y uz else 0) else 0)) by
        funext x
        rw [show (fun y => rsum n (fun z =>
              if z = uz then (if y = uy then (if x = ux then g x y z else 0) else 0) else 0)) =
            (fun y => if y = uy then (if x = ux then g x y uz else 0) else 0) by
              funext y
              simpa using (rsum_single n uz huz (fun t =>
                if y = uy then (if x = ux then g x y t else 0) else 0))]]
  exact rsum_chain2 n ux uy hux huy (fun x y => g x y uz)

private theorem collapsed_eq_full (bs : List (List Nat)) (hmem : bs ∈ partitions pattern.freeVars)
    (n : Nat) (db : DB) (b : Bind) :
  collapsed pattern n db b bs =
  rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e =>
    if constOnB bs a c d e then F4 db b a c d e else 0)))) := by
  simp [partitions, pattern] at hmem
  rcases hmem with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => F4 db b a c d e)))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => F4 db b a c d e))))
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro c hc
    apply rsum_congr
    intro d hd
    apply rsum_congr
    intro e he
    rfl
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun d => rsum n (fun e => F4 db b a a d e))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if c = a then F4 db b a c d e else 0))))
    rw [rsum4_swap23, rsum4_swap34]
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro d hd
    apply rsum_congr
    intro e he
    simpa using (rsum_chain1 n a ha (fun c => F4 db b a c d e)).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun c => rsum n (fun a => rsum n (fun e => F4 db b a c a e))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if d = a then F4 db b a c d e else 0))))
    rw [rsum4_swap12, rsum4_swap34]
    apply rsum_congr
    intro c hc
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro e he
    simpa using (rsum_chain1 n a ha (fun d => F4 db b a c d e)).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun c => rsum n (fun d => rsum n (fun a => F4 db b a c d a))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if e = a then F4 db b a c d e else 0))))
    rw [rsum4_swap12, rsum4_swap23]
    apply rsum_congr
    intro c hc
    apply rsum_congr
    intro d hd
    apply rsum_congr
    intro a ha
    simpa using (rsum_chain1 n a ha (fun e => F4 db b a c d e)).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun c => rsum n (fun e => F4 db b a c c e))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if d = c then F4 db b a c d e else 0))))
    rw [rsum4_swap34]
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro c hc
    apply rsum_congr
    intro e he
    simpa using (rsum_chain1 n c hc (fun d => F4 db b a c d e)).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun e => F4 db b a a a e)) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if c = a ∧ d = a then F4 db b a c d e else 0))))
    rw [rsum4_swap34, rsum4_swap23]
    simp only [ite_and_comm]
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro e he
    simpa using (rsum_chain2 n a a ha ha (fun c => (fun d => F4 db b a c d e))).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun c => rsum n (fun a => F4 db b a c c a)) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if d = c ∧ e = a then F4 db b a c d e else 0))))
    rw [rsum4_swap12]
    simp only [ite_and_comm]
    apply rsum_congr
    intro c hc
    apply rsum_congr
    intro a ha
    simpa using (rsum_chain2 n c a hc ha (fun d => (fun e => F4 db b a c d e))).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun d => rsum n (fun c => F4 db b a c d c))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if e = c then F4 db b a c d e else 0))))
    rw [rsum4_swap23]
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro d hd
    apply rsum_congr
    intro c hc
    simpa using (rsum_chain1 n c hc (fun e => F4 db b a c d e)).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun c => F4 db b a c a c)) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if d = a ∧ e = c then F4 db b a c d e else 0))))
    simp only [ite_and_comm]
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro c hc
    simpa using (rsum_chain2 n a c ha hc (fun d => (fun e => F4 db b a c d e))).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun d => rsum n (fun a => F4 db b a a d a)) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if c = a ∧ e = a then F4 db b a c d e else 0))))
    rw [rsum4_swap23, rsum4_swap12]
    simp only [ite_and_comm]
    apply rsum_congr
    intro d hd
    apply rsum_congr
    intro a ha
    simpa using (rsum_chain2 n a a ha ha (fun c => (fun e => F4 db b a c d e))).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun c => rsum n (fun d => F4 db b a c d d))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if e = d then F4 db b a c d e else 0))))
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro c hc
    apply rsum_congr
    intro d hd
    simpa using (rsum_chain1 n d hd (fun e => F4 db b a c d e)).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun d => F4 db b a a d d)) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if c = a ∧ e = d then F4 db b a c d e else 0))))
    rw [rsum4_swap23]
    simp only [ite_and_comm]
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro d hd
    simpa using (rsum_chain2 n a d ha hd (fun c => (fun e => F4 db b a c d e))).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun c => rsum n (fun a => F4 db b a c a a)) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if d = a ∧ e = a then F4 db b a c d e else 0))))
    rw [rsum4_swap12]
    simp only [ite_and_comm]
    apply rsum_congr
    intro c hc
    apply rsum_congr
    intro a ha
    simpa using (rsum_chain2 n a a ha ha (fun d => (fun e => F4 db b a c d e))).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun c => F4 db b a c c c)) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if d = c ∧ e = c then F4 db b a c d e else 0))))
    simp only [ite_and_comm]
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro c hc
    simpa using (rsum_chain2 n c c hc hc (fun d => (fun e => F4 db b a c d e))).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => F4 db b a a a a) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if c = a ∧ d = a ∧ e = a then F4 db b a c d e else 0))))
    simp only [ite_and_comm]
    apply rsum_congr
    intro a ha
    simpa using (rsum_chain3 n a a a ha ha ha (fun c => (fun d => (fun e => F4 db b a c d e)))).symm

private theorem coeff_id (a c d e : Nat) :
    ((partitions pattern.freeVars).map (fun bs =>
      mobius bs * (if constOnB bs a c d e then (1 : Int) else 0))).sum =
    (if [a, c, d, e].Nodup then (1 : Int) else 0) := by
  by_cases hac : c = a <;> by_cases had : d = a <;> by_cases hae : e = a <;>
    by_cases hcd : d = c <;> by_cases hce : e = c <;> by_cases hde : e = d
  all_goals
    simp [*, constOnB, bind4, pattern, put, partitions, mobius, factorial] <;> omega

private theorem injective_eq_full (n : Nat) (db : DB) (b : Bind) :
    injective pattern n db b =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e =>
        if [a, c, d, e].Nodup then F4 db b a c d e else 0)))) := by
  simp [injective, integrate, pattern, value, put, F4, bind4, rsum]

theorem sound_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ParallelLineage.CollisionCorrection.Goal := by
  intro n db b
  have hR : ((partitions pattern.freeVars).map (fun bs =>
      mobius bs * collapsed pattern n db b bs)).sum =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e =>
        ((partitions pattern.freeVars).map (fun bs =>
          mobius bs * (if constOnB bs a c d e then F4 db b a c d e else 0))).sum)))) := by
    calc
      ((partitions pattern.freeVars).map (fun bs =>
          mobius bs * collapsed pattern n db b bs)).sum =
          ((partitions pattern.freeVars).map (fun bs =>
            rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e =>
              mobius bs * (if constOnB bs a c d e then F4 db b a c d e else 0))))))).sum := by
                apply list_sum_congr
                intro bs hbs
                rw [collapsed_eq_full bs hbs n db b]
                exact const_mul_rsum4 n (mobius bs) (fun a c d e =>
                  if constOnB bs a c d e then F4 db b a c d e else 0)
      _ = rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e =>
            ((partitions pattern.freeVars).map (fun bs =>
              mobius bs * (if constOnB bs a c d e then F4 db b a c d e else 0))).sum)))) := by
                exact sum_rsum4 n (partitions pattern.freeVars) (fun bs a c d e =>
                  mobius bs * (if constOnB bs a c d e then F4 db b a c d e else 0))
  rw [injective_eq_full, hR]
  apply rsum4_ext
  intro a c d e ha hc hd he
  rw [show ((partitions pattern.freeVars).map (fun bs =>
        mobius bs * (if constOnB bs a c d e then F4 db b a c d e else 0))).sum =
      ((partitions pattern.freeVars).map (fun bs =>
        mobius bs * (if constOnB bs a c d e then (1 : Int) else 0))).sum * F4 db b a c d e by
      rw [← list_sum_mul_right]
      apply list_sum_congr
      intro bs hbs
      by_cases h : constOnB bs a c d e <;> simp [h]]
  rw [coeff_id]
  by_cases h : [a, c, d, e].Nodup <;> simp [h]

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.ParallelLineageCapsAndCollisionCorrection
