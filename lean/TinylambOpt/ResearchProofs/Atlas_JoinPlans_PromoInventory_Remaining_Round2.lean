import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Round2.Task76

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.PromoInventory

private abbrev rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_zero (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
  induction m with
  | zero => simp [rsum]
  | succ m ih => rw [rsum_succ, ih]; simp

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

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [show rsum 0 (fun i => rsum m (fun j => f i j)) = 0 by simp [rsum]]
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

private theorem rsum_ite_eq_single (n a : Nat) (ha : a < n) (f : Nat → Int) :
    rsum n (fun x => if x = a then f x else 0) = f a := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsum_succ]
      by_cases h : a = n
      · subst a
        have hzero : rsum n (fun x => if x = n then f x else 0) =
            rsum n (fun _ => (0 : Int)) := by
          apply rsum_congr
          intro x hx
          have hxn : x ≠ n := by omega
          simp [hxn]
        rw [hzero, rsum_zero]
        simp
      · have han : a < n := by omega
        rw [ih han]
        have hne : ¬ n = a := by omega
        simp [h, hne]

/-- Triple sum in the physical elimination order of `pattern.freeVars = [2,3,0]`. -/
private def T (n : Nat) (F : Nat → Nat → Nat → Int) : Int :=
  rsum n (fun a => rsum n (fun c => rsum n (fun d => F a c d)))

private theorem T_add (n : Nat) (F G : Nat → Nat → Nat → Int) :
    T n (fun a c d => F a c d + G a c d) = T n F + T n G := by
  unfold T
  rw [show (fun a => rsum n (fun c => rsum n (fun d =>
      F a c d + G a c d))) =
      (fun a => rsum n (fun c => rsum n (fun d => F a c d)) +
        rsum n (fun c => rsum n (fun d => G a c d))) by
    funext a
    rw [show (fun c => rsum n (fun d => F a c d + G a c d)) =
        (fun c => rsum n (fun d => F a c d) + rsum n (fun d => G a c d)) by
      funext c
      rw [rsum_add]]
    rw [rsum_add]]
  rw [rsum_add]

private theorem T_sub (n : Nat) (F G : Nat → Nat → Nat → Int) :
    T n (fun a c d => F a c d - G a c d) = T n F - T n G := by
  unfold T
  rw [show (fun a => rsum n (fun c => rsum n (fun d =>
      F a c d - G a c d))) =
      (fun a => rsum n (fun c => rsum n (fun d => F a c d)) -
        rsum n (fun c => rsum n (fun d => G a c d))) by
    funext a
    rw [show (fun c => rsum n (fun d => F a c d - G a c d)) =
        (fun c => rsum n (fun d => F a c d) - rsum n (fun d => G a c d)) by
      funext c
      rw [rsum_sub]]
    rw [rsum_sub]]
  rw [rsum_sub]

private theorem T_const_mul (n : Nat) (c0 : Int) (F : Nat → Nat → Nat → Int) :
    T n (fun a c d => c0 * F a c d) = c0 * T n F := by
  unfold T
  rw [show (fun a => rsum n (fun c => rsum n (fun d =>
      c0 * F a c d))) =
      (fun a => c0 * rsum n (fun c => rsum n (fun d => F a c d))) by
    funext a
    rw [show (fun c => rsum n (fun d => c0 * F a c d)) =
        (fun c => c0 * rsum n (fun d => F a c d)) by
      funext c
      rw [rsum_const_mul]]
    rw [rsum_const_mul]]
  rw [rsum_const_mul]

private theorem T_congr (n : Nat) {F G : Nat → Nat → Nat → Int}
    (h : ∀ a c d, F a c d = G a c d) : T n F = T n G := by
  unfold T
  apply rsum_congr
  intro a ha
  apply rsum_congr
  intro c hc
  apply rsum_congr
  intro d hd
  exact h a c d

private theorem triple_ie_point (a c d : Nat) (F : Int) :
    (if a ≠ c ∧ a ≠ d ∧ c ≠ d then F else 0) =
      F - (if c = a then F else 0) - (if d = a then F else 0) -
        (if d = c then F else 0) +
          2 * (if a = c ∧ a = d ∧ c = d then F else 0) := by
  by_cases hac : a = c
  · by_cases had : a = d
    · have hcd : c = d := by omega
      simp [hac, had, hcd, eq_comm] <;> omega
    · by_cases hcd : c = d
      · have : a = d := by omega
        contradiction
      · simp [hac, had, hcd, eq_comm] <;> omega
  · by_cases had : a = d
    · by_cases hcd : c = d
      · have : a = c := by omega
        contradiction
      · simp [hac, had, hcd, eq_comm] <;> omega
    · by_cases hcd : c = d
      · simp [hac, had, hcd, eq_comm] <;> omega
      · simp [hac, had, hcd, eq_comm] <;> omega

private theorem T_distinct (n : Nat) (F : Nat → Nat → Nat → Int) :
    T n (fun a c d => if a ≠ c ∧ a ≠ d ∧ c ≠ d then F a c d else 0) =
      T n F - T n (fun a c d => if c = a then F a c d else 0) -
        T n (fun a c d => if d = a then F a c d else 0) -
          T n (fun a c d => if d = c then F a c d else 0) +
            2 * T n (fun a c d =>
              if a = c ∧ a = d ∧ c = d then F a c d else 0) := by
  rw [show (fun a c d =>
      (if a ≠ c ∧ a ≠ d ∧ c ≠ d then F a c d else 0)) =
      (fun a c d =>
        F a c d - (if c = a then F a c d else 0) -
          (if d = a then F a c d else 0) - (if d = c then F a c d else 0) +
            2 * (if a = c ∧ a = d ∧ c = d then F a c d else 0)) by
    funext a c d
    exact triple_ie_point a c d (F a c d)]
  rw [T_add]
  rw [T_sub, T_sub, T_sub, T_const_mul]

/-! ## Finite-support injection machinery for cover bounds -/

private theorem sum_congr {α} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

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

/-! ## FractionalCap: seven integral covers decompose every fractional cover -/

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

private def e0 : Atom := ⟨0, 0, 1⟩
private def e1 : Atom := ⟨0, 1, 2⟩
private def e2 : Atom := ⟨0, 2, 3⟩
private def e3 : Atom := ⟨0, 3, 0⟩
private def e4 : Atom := ⟨2, 0, 2⟩
private def e5 : Atom := ⟨1, 1, 3⟩

private def C1 : List Atom := [e0, e2]
private def C2 : List Atom := [e1, e3]
private def C3 : List Atom := [e1, e2, e4]
private def C4 : List Atom := [e0, e3, e4]
private def C5 : List Atom := [e0, e1, e5]
private def C6 : List Atom := [e2, e3, e5]
private def C7 : List Atom := [e4, e5]

private theorem cover_of_C1 : ∀ v ∈ pattern.freeVars, ∃ a ∈ C1, a.src = v ∨ a.dst = v := by
  intro v hv
  simp [pattern] at hv
  rcases hv with rfl | rfl | rfl | rfl <;> simp [C1, e0, e1, e2, e3, e4, e5]
private theorem cover_of_C2 : ∀ v ∈ pattern.freeVars, ∃ a ∈ C2, a.src = v ∨ a.dst = v := by
  intro v hv
  simp [pattern] at hv
  rcases hv with rfl | rfl | rfl | rfl <;> simp [C2, e0, e1, e2, e3, e4, e5]
private theorem cover_of_C3 : ∀ v ∈ pattern.freeVars, ∃ a ∈ C3, a.src = v ∨ a.dst = v := by
  intro v hv
  simp [pattern] at hv
  rcases hv with rfl | rfl | rfl | rfl <;> simp [C3, e0, e1, e2, e3, e4, e5]
private theorem cover_of_C4 : ∀ v ∈ pattern.freeVars, ∃ a ∈ C4, a.src = v ∨ a.dst = v := by
  intro v hv
  simp [pattern] at hv
  rcases hv with rfl | rfl | rfl | rfl <;> simp [C4, e0, e1, e2, e3, e4, e5]
private theorem cover_of_C5 : ∀ v ∈ pattern.freeVars, ∃ a ∈ C5, a.src = v ∨ a.dst = v := by
  intro v hv
  simp [pattern] at hv
  rcases hv with rfl | rfl | rfl | rfl <;> simp [C5, e0, e1, e2, e3, e4, e5]
private theorem cover_of_C6 : ∀ v ∈ pattern.freeVars, ∃ a ∈ C6, a.src = v ∨ a.dst = v := by
  intro v hv
  simp [pattern] at hv
  rcases hv with rfl | rfl | rfl | rfl <;> simp [C6, e0, e1, e2, e3, e4, e5]
private theorem cover_of_C7 : ∀ v ∈ pattern.freeVars, ∃ a ∈ C7, a.src = v ∨ a.dst = v := by
  intro v hv
  simp [pattern] at hv
  rcases hv with rfl | rfl | rfl | rfl <;> simp [C7, e0, e1, e2, e3, e4, e5]

private theorem atoms_of_C1 : ∀ a ∈ C1, a ∈ pattern.atoms := by
  intro a ha
  simp [C1, e0, e2, pattern] at ha ⊢
  rcases ha with rfl | rfl <;> simp
private theorem atoms_of_C2 : ∀ a ∈ C2, a ∈ pattern.atoms := by
  intro a ha
  simp [C2, e1, e3, pattern] at ha ⊢
  rcases ha with rfl | rfl <;> simp
private theorem atoms_of_C3 : ∀ a ∈ C3, a ∈ pattern.atoms := by
  intro a ha
  simp [C3, e1, e2, e4, pattern] at ha ⊢
  rcases ha with rfl | rfl | rfl <;> simp
private theorem atoms_of_C4 : ∀ a ∈ C4, a ∈ pattern.atoms := by
  intro a ha
  simp [C4, e0, e3, e4, pattern] at ha ⊢
  rcases ha with rfl | rfl | rfl <;> simp
private theorem atoms_of_C5 : ∀ a ∈ C5, a ∈ pattern.atoms := by
  intro a ha
  simp [C5, e0, e1, e5, pattern] at ha ⊢
  rcases ha with rfl | rfl | rfl <;> simp
private theorem atoms_of_C6 : ∀ a ∈ C6, a ∈ pattern.atoms := by
  intro a ha
  simp [C6, e2, e3, e5, pattern] at ha ⊢
  rcases ha with rfl | rfl | rfl <;> simp
private theorem atoms_of_C7 : ∀ a ∈ C7, a ∈ pattern.atoms := by
  intro a ha
  simp [C7, e4, e5, pattern] at ha ⊢
  rcases ha with rfl | rfl <;> simp

private theorem pattern_fv_nodup : pattern.freeVars.Nodup := by native_decide

private theorem cover_decomp : ∀ den w0 w1 w2 w3 w4 w5 : Nat,
    den ≤ w0 + w3 + w4 → den ≤ w0 + w1 + w5 →
    den ≤ w1 + w2 + w4 → den ≤ w2 + w3 + w5 →
    ∃ t1 t2 t3 t4 t5 t6 t7 : Nat,
      t1 + t2 + t3 + t4 + t5 + t6 + t7 = den ∧
      t1 + t4 + t5 ≤ w0 ∧ t2 + t3 + t5 ≤ w1 ∧
      t1 + t3 + t6 ≤ w2 ∧ t2 + t4 + t6 ≤ w3 ∧
      t3 + t4 + t7 ≤ w4 ∧ t5 + t6 + t7 ≤ w5 := by
  intro den
  induction den with
  | zero =>
      intro w0 w1 w2 w3 w4 w5 h0 h1 h2 h3
      exact ⟨0, 0, 0, 0, 0, 0, 0, by simp⟩
  | succ den ih =>
      intro w0 w1 w2 w3 w4 w5 h0 h1 h2 h3
      by_cases hC1 : 0 < w0 ∧ 0 < w2
      · rcases ih (w0-1) w1 (w2-1) w3 w4 w5 (by omega) (by omega) (by omega) (by omega)
          with ⟨a, b, c, d, e, f, g, hs, hu0, hu1, hu2, hu3, hu4, hu5⟩
        refine ⟨a+1, b, c, d, e, f, g, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩ <;> omega
      · by_cases hC2 : 0 < w1 ∧ 0 < w3
        · rcases ih w0 (w1-1) w2 (w3-1) w4 w5 (by omega) (by omega) (by omega) (by omega)
            with ⟨a, b, c, d, e, f, g, hs, hu0, hu1, hu2, hu3, hu4, hu5⟩
          refine ⟨a, b+1, c, d, e, f, g, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩ <;> omega
        · by_cases hC3 : 0 < w1 ∧ 0 < w2 ∧ 0 < w4
          · rcases ih w0 (w1-1) (w2-1) w3 (w4-1) w5 (by omega) (by omega) (by omega) (by omega)
              with ⟨a, b, c, d, e, f, g, hs, hu0, hu1, hu2, hu3, hu4, hu5⟩
            refine ⟨a, b, c+1, d, e, f, g, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩ <;> omega
          · by_cases hC4 : 0 < w0 ∧ 0 < w3 ∧ 0 < w4
            · rcases ih (w0-1) w1 w2 (w3-1) (w4-1) w5 (by omega) (by omega) (by omega) (by omega)
                with ⟨a, b, c, d, e, f, g, hs, hu0, hu1, hu2, hu3, hu4, hu5⟩
              refine ⟨a, b, c, d+1, e, f, g, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩ <;> omega
            · by_cases hC5 : 0 < w0 ∧ 0 < w1 ∧ 0 < w5
              · rcases ih (w0-1) (w1-1) w2 w3 w4 (w5-1) (by omega) (by omega) (by omega) (by omega)
                  with ⟨a, b, c, d, e, f, g, hs, hu0, hu1, hu2, hu3, hu4, hu5⟩
                refine ⟨a, b, c, d, e+1, f, g, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩ <;> omega
              · by_cases hC6 : 0 < w2 ∧ 0 < w3 ∧ 0 < w5
                · rcases ih w0 w1 (w2-1) (w3-1) w4 (w5-1) (by omega) (by omega) (by omega) (by omega)
                    with ⟨a, b, c, d, e, f, g, hs, hu0, hu1, hu2, hu3, hu4, hu5⟩
                  refine ⟨a, b, c, d, e, f+1, g, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩ <;> omega
                · have hC7 : 0 < w4 ∧ 0 < w5 := by omega
                  rcases ih w0 w1 w2 w3 (w4-1) (w5-1) (by omega) (by omega) (by omega) (by omega)
                    with ⟨a, b, c, d, e, f, g, hs, hu0, hu1, hu2, hu3, hu4, hu5⟩
                  refine ⟨a, b, c, d, e, f, g+1, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩ <;> omega

private theorem pow_mono_of_pos {x a b : Nat} (hx : 0 < x) (hab : a ≤ b) : x^a ≤ x^b := by
  have hs : b = a + (b - a) := by omega
  rw [hs, Nat.pow_add]
  exact Nat.le_mul_of_pos_right (x^a) (show 0 < x ^ (b - a) from Nat.pow_pos hx)

theorem sound_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.PromoInventory.FractionalCap.Goal := by
  intro n db b weights den hcov
  rcases hcov with ⟨hdenpos, hlen, hc⟩
  rcases list6_of_length6 weights (by simpa [pattern] using hlen) with
    ⟨w0, w1, w2, w3, w4, w5, rfl⟩
  have h0 : den ≤ w0 + w3 + w4 := by
    have hh := hc 0 (by simp [pattern])
    simp [pattern] at hh
    omega
  have h1 : den ≤ w0 + w1 + w5 := by
    have hh := hc 1 (by simp [pattern])
    simp [pattern] at hh
    omega
  have h2 : den ≤ w1 + w2 + w4 := by
    have hh := hc 2 (by simp [pattern])
    simp [pattern] at hh
    omega
  have h3 : den ≤ w2 + w3 + w5 := by
    have hh := hc 3 (by simp [pattern])
    simp [pattern] at hh
    omega
  rcases cover_decomp den w0 w1 w2 w3 w4 w5 h0 h1 h2 h3 with
    ⟨t1, t2, t3, t4, t5, t6, t7, hsum, hu0, hu1, hu2, hu3, hu4, hu5⟩
  let S (e : Atom) := support n db e pattern b
  let L := (satBindings pattern n db b).length
  have hb1 : L ≤ S e0 * S e2 := by
    dsimp [L, S]
    simpa [C1] using
      (ordinary_cover_bound pattern n db b C1 cover_of_C1 atoms_of_C1 pattern_fv_nodup)
  have hb2 : L ≤ S e1 * S e3 := by
    dsimp [L, S]
    simpa [C2] using
      (ordinary_cover_bound pattern n db b C2 cover_of_C2 atoms_of_C2 pattern_fv_nodup)
  have hb3 : L ≤ S e1 * S e2 * S e4 := by
    dsimp [L, S]
    simpa [C3, Nat.mul_assoc] using
      (ordinary_cover_bound pattern n db b C3 cover_of_C3 atoms_of_C3 pattern_fv_nodup)
  have hb4 : L ≤ S e0 * S e3 * S e4 := by
    dsimp [L, S]
    simpa [C4, Nat.mul_assoc] using
      (ordinary_cover_bound pattern n db b C4 cover_of_C4 atoms_of_C4 pattern_fv_nodup)
  have hb5 : L ≤ S e0 * S e1 * S e5 := by
    dsimp [L, S]
    simpa [C5, Nat.mul_assoc] using
      (ordinary_cover_bound pattern n db b C5 cover_of_C5 atoms_of_C5 pattern_fv_nodup)
  have hb6 : L ≤ S e2 * S e3 * S e5 := by
    dsimp [L, S]
    simpa [C6, Nat.mul_assoc] using
      (ordinary_cover_bound pattern n db b C6 cover_of_C6 atoms_of_C6 pattern_fv_nodup)
  have hb7 : L ≤ S e4 * S e5 := by
    dsimp [L, S]
    simpa [C7] using
      (ordinary_cover_bound pattern n db b C7 cover_of_C7 atoms_of_C7 pattern_fv_nodup)
  have href : (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat = L := by
    have hbin : (fun r x y : Nat => if db r x y == 0 then (0 : Int) else 1) = bin db := by
      funext r x y
      simp [bin, beq_iff_eq]
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
    have hp1 : S e0 ^ (t1 + t4 + t5) ≤ S e0 ^ w0 := pow_mono_of_pos hS0p hu0
    have hp2 : S e1 ^ (t2 + t3 + t5) ≤ S e1 ^ w1 := pow_mono_of_pos hS1p hu1
    have hp3 : S e2 ^ (t1 + t3 + t6) ≤ S e2 ^ w2 := pow_mono_of_pos hS2p hu2
    have hp4 : S e3 ^ (t2 + t4 + t6) ≤ S e3 ^ w3 := pow_mono_of_pos hS3p hu3
    have hp5 : S e4 ^ (t3 + t4 + t7) ≤ S e4 ^ w4 := pow_mono_of_pos hS4p hu4
    have hp6 : S e5 ^ (t5 + t6 + t7) ≤ S e5 ^ w5 := pow_mono_of_pos hS5p hu5
    have hpow : L ^ den ≤
        (S e0 * S e2) ^ t1 * (S e1 * S e3) ^ t2 *
        (S e1 * S e2 * S e4) ^ t3 * (S e0 * S e3 * S e4) ^ t4 *
        (S e0 * S e1 * S e5) ^ t5 * (S e2 * S e3 * S e5) ^ t6 *
        (S e4 * S e5) ^ t7 := by
      calc
        L ^ den = L ^ t1 * L ^ t2 * L ^ t3 * L ^ t4 * L ^ t5 * L ^ t6 * L ^ t7 := by
          rw [← hsum]
          repeat rw [Nat.pow_add]
        _ ≤ (S e0 * S e2) ^ t1 * (S e1 * S e3) ^ t2 *
            (S e1 * S e2 * S e4) ^ t3 * (S e0 * S e3 * S e4) ^ t4 *
            (S e0 * S e1 * S e5) ^ t5 * (S e2 * S e3 * S e5) ^ t6 *
            (S e4 * S e5) ^ t7 := by
          repeat' apply Nat.mul_le_mul
          · exact Nat.pow_le_pow_left hb1 t1
          · exact Nat.pow_le_pow_left hb2 t2
          · exact Nat.pow_le_pow_left hb3 t3
          · exact Nat.pow_le_pow_left hb4 t4
          · exact Nat.pow_le_pow_left hb5 t5
          · exact Nat.pow_le_pow_left hb6 t6
          · exact Nat.pow_le_pow_left hb7 t7
    have hprod :
        (S e0 * S e2) ^ t1 * (S e1 * S e3) ^ t2 *
        (S e1 * S e2 * S e4) ^ t3 * (S e0 * S e3 * S e4) ^ t4 *
        (S e0 * S e1 * S e5) ^ t5 * (S e2 * S e3 * S e5) ^ t6 *
        (S e4 * S e5) ^ t7 =
        S e0 ^ (t1 + t4 + t5) * S e1 ^ (t2 + t3 + t5) *
          S e2 ^ (t1 + t3 + t6) * S e3 ^ (t2 + t4 + t6) *
          S e4 ^ (t3 + t4 + t7) * S e5 ^ (t5 + t6 + t7) := by
      simp only [Nat.mul_pow]
      repeat rw [Nat.pow_add]
      ac_rfl
    calc
      L ^ den ≤ (S e0 * S e2) ^ t1 * (S e1 * S e3) ^ t2 *
          (S e1 * S e2 * S e4) ^ t3 * (S e0 * S e3 * S e4) ^ t4 *
          (S e0 * S e1 * S e5) ^ t5 * (S e2 * S e3 * S e5) ^ t6 *
          (S e4 * S e5) ^ t7 := hpow
      _ = S e0 ^ (t1 + t4 + t5) * S e1 ^ (t2 + t3 + t5) *
          S e2 ^ (t1 + t3 + t6) * S e3 ^ (t2 + t4 + t6) *
          S e4 ^ (t3 + t4 + t7) * S e5 ^ (t5 + t6 + t7) := hprod
      _ ≤ S e0 ^ w0 * S e1 ^ w1 * S e2 ^ w2 * S e3 ^ w3 * S e4 ^ w4 * S e5 ^ w5 := by
          repeat' apply Nat.mul_le_mul
          · exact hp1
          · exact hp2
          · exact hp3
          · exact hp4
          · exact hp5
          · exact hp6

end TinylambOpt.ResearchProofs.Round2.Task76
