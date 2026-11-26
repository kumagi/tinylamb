import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved (8 claims, including the following)

- `ChannelBridge.BoundaryMessages` — `∀ n db b, elimination pattern n db b = reference
  pattern n db b`
- `ChannelBridge.TripleOverlap` — `∀ n db b, reference pattern n db b = total n (fun y
  => overlap n db 0 (b 0) y * overlap n db 1 (b 0) y * overlap n db 2 (b 0) y)`
- `CountryReturn.BoundaryMessages` — `∀ n db b, elimination pattern n db b = reference
  pattern n db b`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReturnAndSupplierJoinMessages

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total dot)

/-! Generic finite-sum helpers (Std only). -/

theorem sum_map_add {α : Type} (f g : α → Int) (l : List α) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      grind

theorem sum_map_mul_right {α : Type} (f : α → Int) (c : Int) (l : List α) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      grind

theorem sum_map_mul_left {α : Type} (c : Int) (f : α → Int) (l : List α) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      grind

theorem sum_map_neg {α : Type} (f : α → Int) (l : List α) :
    (l.map (fun x => - f x)).sum = -(l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      grind

theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

theorem sum_sum_comm {α β : Type} (l₁ : List α) (l₂ : List β) (f : α → β → Int) :
    (l₁.map (fun a => (l₂.map (f a)).sum)).sum =
      (l₂.map (fun b => (l₁.map (fun a => f a b)).sum)).sum := by
  induction l₁ with
  | nil =>
      simp [sum_map_zero]
  | cons a xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      rw [sum_map_add]

theorem sum_map_congr {α : Type} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

theorem prod_map_partition {α : Type} (p : α → Bool) (f : α → Int) : ∀ l : List α,
    ((l.filter p).map f).prod * ((l.filter (fun a => !p a)).map f).prod =
      (l.map f).prod
  | [] => by simp
  | x :: xs => by
      by_cases h : p x = true
      · simp [h]
        rw [Int.mul_assoc (f x), prod_map_partition p f xs]
      · have hf : p x = false := by cases h' : p x <;> simp_all
        simp [hf]
        rw [← Int.mul_assoc (List.map f (List.filter p xs)).prod (f x)]
        rw [Int.mul_comm (List.map f (List.filter p xs)).prod (f x)]
        rw [Int.mul_assoc (f x)]
        rw [prod_map_partition p f xs]

theorem put_self (b : Bind) (v : Nat) : put b v (b v) = b := by
  funext i
  by_cases h : i == v
  · have hi : i = v := Nat.eq_of_beq_eq_true h
    subst i
    simp [put]
  · simp [put, h]

theorem put_put_same (b : Bind) (v x y : Nat) : put (put b v x) v y = put b v y := by
  funext i
  by_cases h : i == v <;> simp [put, h]

theorem put_comm_of_ne (b : Bind) (v w x y : Nat) (h : v ≠ w) :
    put (put b v x) w y = put (put b w y) v x := by
  funext i
  by_cases hi : i == v
  · have hi' : i = v := Nat.eq_of_beq_eq_true hi
    subst i
    by_cases hw : v == w
    · have : v = w := Nat.eq_of_beq_eq_true hw
      exact (h this).elim
    · simp [put, hw]
  · by_cases hj : i == w
    · have hj' : i = w := Nat.eq_of_beq_eq_true hj
      subst i
      simp [put, hi]
    · simp [put, hi, hj]

theorem integrate_add (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x + g x) =
      integrate n vs b f + integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      simp [integrate, ih, sum_map_add]

theorem integrate_neg (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n vs b (fun x => - f x) = - integrate n vs b f := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      simp only [integrate]
      rw [show ((List.range n).map (fun x => integrate n vs (put b v x) (fun x => -f x))).sum =
          ((List.range n).map (fun x => - integrate n vs (put b v x) f)).sum by
        apply sum_map_congr
        intro x hx
        exact ih (put b v x)]
      rw [sum_map_neg]

theorem integrate_sub (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x - g x) =
      integrate n vs b f - integrate n vs b g := by
  have hfun : (fun x => f x - g x) = (fun x => f x + (-1) * g x) := by
    funext x
    grind
  rw [hfun, integrate_add]
  rw [show integrate n vs b (fun x => (-1) * g x) = - integrate n vs b g by
    rw [show (fun x => (-1) * g x) = (fun x => - g x) by funext x; grind]
    exact integrate_neg n vs b g]
  grind

theorem integrate_zero (n : Nat) (vs : List Nat) (b : Bind) :
    integrate n vs b (fun _ => (0 : Int)) = 0 := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      simp [integrate, ih, sum_map_zero]

theorem integrate_sum (n : Nat) (vs : List Nat) (b : Bind) (fs : List (Bind → Int)) :
    integrate n vs b (fun x => (fs.map (fun f => f x)).sum) =
      (fs.map (fun f => integrate n vs b f)).sum := by
  induction fs with
  | nil => simp [integrate_zero]
  | cons f fs ih =>
      simp [integrate_add, ih]

theorem integrate_mul_left (n : Nat) (vs : List Nat) (b : Bind) (c : Int) (f : Bind → Int) :
    integrate n vs b (fun x => c * f x) = c * integrate n vs b f := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      simp [integrate, ih, sum_map_mul_left]

theorem integrate_mul_right (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) (c : Int) :
    integrate n vs b (fun x => f x * c) = integrate n vs b f * c := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      simp [integrate, ih, sum_map_mul_right]

theorem integrate_swap (n : Nat) (vs : List Nat) (v : Nat) (b : Bind) (f : Bind → Int)
    (hv : v ∉ vs) :
    integrate n vs b (fun x => integrate n [v] x f) = integrate n (v :: vs) b f := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons w ws ih =>
      have hvw : v ≠ w := by intro h; subst h; exact hv (by simp)
      have hwv : w ≠ v := hvw.symm
      have hvws : v ∉ ws := by intro h; exact hv (by simp [h])
      change ((List.range n).map (fun x => integrate n ws (put b w x) (fun y => integrate n [v] y f))).sum =
        ((List.range n).map (fun t => integrate n (w :: ws) (put b v t) f)).sum
      rw [show ((List.range n).map (fun x => integrate n ws (put b w x) (fun y => integrate n [v] y f))).sum =
          ((List.range n).map (fun x => integrate n (v :: ws) (put b w x) f)).sum by
        apply sum_map_congr
        intro x hx
        exact ih (put b w x) hvws]
      simp only [integrate]
      rw [sum_sum_comm]
      apply sum_map_congr
      intro t ht
      apply sum_map_congr
      intro x hx
      rw [put_comm_of_ne b w v x t hwv]


/-! Eliminating boundary variables preserves bag semantics. -/

private theorem bm_put_at (b : Bind) (v x : Nat) : put b v x v = x := by
  simp [put]

private theorem mem_iff_contains {α : Type} [BEq α] [LawfulBEq α] (a : α) (l : List α) :
    l.contains a = true ↔ a ∈ l := by
  induction l with
  | nil =>
      change (List.elem a ([] : List α) = true) ↔ a ∈ ([] : List α)
      simp [List.elem]
  | cons x xs ih =>
      change (List.elem a (x :: xs) = true) ↔ a ∈ x :: xs
      rw [List.elem_cons]
      by_cases h : a == x
      · have hx : a = x := LawfulBEq.eq_of_beq h
        subst x
        simp
      · have hax : a ≠ x := by
          intro heq
          subst x
          simp at h
        simp [h, hax, ih]

private def Respects (f : Factor) : Prop :=
  ∀ b b', (∀ v, f.scope.contains v = true → b v = b' v) → f.get b = f.get b'

private theorem respects_value (db : DB) (a : Atom) :
    Respects ⟨[a.src, a.dst], value db a⟩ := by
  intro b b' h
  simp only [value]
  rw [h a.src (by simp), h a.dst (by simp)]

private theorem respects_put (f : Factor) (hf : Respects f) (v x : Nat) (b : Bind)
    (hnot : f.scope.contains v = false) : f.get (put b v x) = f.get b := by
  apply hf
  intro w hw
  by_cases h : w == v
  · have hwv : w = v := Nat.eq_of_beq_eq_true h
    subst w
    rw [hnot] at hw
    simp at hw
  · simp [put, h]

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
  by_cases h' : w == v
  · have hwv : w = v := Nat.eq_of_beq_eq_true h'
    subst w
    rw [hnot f hf] at hw
    simp at hw
  · simp [put, h']

private theorem bm_integrate_congr (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ x, f x = g x) : integrate n vs b f = integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum
      apply sum_map_congr
      intro x hx
      exact ih (put b v x)

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
  rw [show (List.partition p l).fst = l.filter p from (partition_filters p l).1]
  intro h
  exact (List.mem_filter.mp h).2

private theorem mem_partition_notp (p : α → Bool) {l : List α} {a : α} :
    a ∈ (List.partition p l).snd → p a = false := by
  rw [show (List.partition p l).snd = l.filter (fun x => !p x) from (partition_filters p l).2]
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
      by_cases h : p a = true <;> simp [h, ih, Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]

private theorem product_partition_get (p : Factor → Bool) (l : List Factor) (b : Bind) :
    ((List.partition p l).fst.map (fun f => f.get b)).prod *
      ((List.partition p l).snd.map (fun f => f.get b)).prod =
    (l.map (fun f => f.get b)).prod := by
  rw [show (List.partition p l).fst = l.filter p from (partition_filters p l).1]
  rw [show (List.partition p l).snd = l.filter (fun x => !p x) from (partition_filters p l).2]
  exact product_filter_mul p l (fun f => f.get b)

private theorem respects_msg (n v : Nat) (inc : List Factor) (hres : ∀ f ∈ inc, Respects f) :
    Respects ⟨(inc.flatMap Factor.scope).eraseDups.filter (fun w => w != v),
      fun b => integrate n [v] b (product inc)⟩ := by
  intro b b' hb
  change ((List.range n).map (fun x => product inc (put b v x))).sum =
    ((List.range n).map (fun x => product inc (put b' v x))).sum
  apply sum_map_congr
  intro x hx
  apply product_respects_eq hres
  intro f hf w hw
  by_cases hwv : w == v
  · have hwv' : w = v := Nat.eq_of_beq_eq_true hwv
    subst w
    simp [bm_put_at]
  · simp [put, hwv]
    apply hb
    have hwmem : w ∈ f.scope := by simpa using hw
    have hwflat : w ∈ inc.flatMap Factor.scope := List.mem_flatMap.mpr ⟨f, hf, hwmem⟩
    have hwErase : w ∈ (inc.flatMap Factor.scope).eraseDups := List.mem_eraseDups.mpr hwflat
    have hwne : (w != v) = true := by simpa using hwv
    have hmem : w ∈ ((inc.flatMap Factor.scope).eraseDups.filter (fun w => w != v)) := by
      rw [List.mem_filter]
      exact ⟨hwErase, hwne⟩
    exact (mem_iff_contains w _).mpr hmem

private theorem mem_of_partition_fst {l : List Factor} {v : Nat} {f : Factor}
    (hf : f ∈ (List.partition (fun g : Factor => g.scope.contains v) l).fst) : f ∈ l := by
  rw [show (List.partition (fun g : Factor => g.scope.contains v) l).fst =
      l.filter (fun g : Factor => g.scope.contains v) from
      (partition_filters (fun g : Factor => g.scope.contains v) l).1] at hf
  exact (List.mem_filter.mp hf).1

private theorem mem_of_partition_snd {l : List Factor} {v : Nat} {f : Factor}
    (hf : f ∈ (List.partition (fun g : Factor => g.scope.contains v) l).snd) : f ∈ l := by
  rw [show (List.partition (fun g : Factor => g.scope.contains v) l).snd =
      l.filter (fun g : Factor => !(g.scope.contains v)) from
      (partition_filters (fun g : Factor => g.scope.contains v) l).2] at hf
  exact (List.mem_filter.mp hf).1

private theorem product_eliminate (n v : Nat) (fs : List Factor) (b : Bind)
    (hres : ∀ f ∈ fs, Respects f) :
    product (eliminate n v fs) b = integrate n [v] b (product fs) := by
  dsimp only [eliminate]
  let inc := (fs.partition (fun f : Factor => f.scope.contains v)).fst
  let rest := (fs.partition (fun f : Factor => f.scope.contains v)).snd
  have hinc_res : ∀ f ∈ inc, Respects f := by
    intro f hf
    exact hres f (mem_of_partition_fst hf)
  have hrest_res : ∀ f ∈ rest, Respects f := by
    intro f hf
    exact hres f (mem_of_partition_snd hf)
  have hrest_not : ∀ f ∈ rest, f.scope.contains v = false := by
    intro f hf
    exact mem_partition_notp (fun g : Factor => g.scope.contains v) hf
  change (integrate n [v] b (fun x => product inc x)) * product rest b =
    integrate n [v] b (product fs)
  change ((List.range n).map (fun x => product inc (put b v x))).sum * product rest b =
    ((List.range n).map (fun x => product fs (put b v x))).sum
  rw [← sum_map_mul_right]
  apply sum_map_congr
  intro x hx
  simp only [product]
  rw [← product_partition_get (fun f : Factor => f.scope.contains v) fs (put b v x)]
  change (inc.map (fun f => f.get (put b v x))).prod * (rest.map (fun f => f.get b)).prod =
    (inc.map (fun f => f.get (put b v x))).prod * (rest.map (fun f => f.get (put b v x))).prod
  congr 1
  exact (product_put_eq hrest_res hrest_not b x).symm

private theorem respects_eliminate (n v : Nat) (fs : List Factor) (hres : ∀ f ∈ fs, Respects f) :
    ∀ f ∈ eliminate n v fs, Respects f := by
  intro f hf
  dsimp only [eliminate] at hf
  simp only [List.mem_cons] at hf
  rcases hf with rfl | hf
  · exact respects_msg n v (fs.partition (fun g : Factor => g.scope.contains v)).fst (by
      intro g hg
      exact hres g (mem_of_partition_fst hg))
  · exact hres f (mem_of_partition_snd hf)

private theorem respects_fold_eliminate (n : Nat) (vs : List Nat) (fs : List Factor)
    (hres : ∀ f ∈ fs, Respects f) :
    ∀ f ∈ vs.foldl (fun fs v => eliminate n v fs) fs, Respects f := by
  induction vs generalizing fs with
  | nil => exact hres
  | cons v vs ih =>
      exact ih (eliminate n v fs) (respects_eliminate n v fs hres)

private theorem product_fold_eliminate (n : Nat) (vs : List Nat) (fs : List Factor) (b : Bind)
    (hres : ∀ f ∈ fs, Respects f) (hvs : vs.Nodup) :
    product (vs.foldl (fun fs v => eliminate n v fs) fs) b = integrate n vs b (product fs) := by
  induction vs generalizing fs with
  | nil => rfl
  | cons v vs ih =>
      have hpair := List.nodup_cons.mp hvs
      have hnv : v ∉ vs := hpair.1
      have hvs' : vs.Nodup := hpair.2
      change product (vs.foldl (fun fs v => eliminate n v fs) (eliminate n v fs)) b =
        integrate n (v :: vs) b (product fs)
      rw [ih (eliminate n v fs) (respects_eliminate n v fs hres) hvs']
      have hprod : product (eliminate n v fs) =
          (fun x => integrate n [v] x (product fs)) := by
        funext x
        exact product_eliminate n v fs x hres
      rw [hprod]
      exact integrate_swap n vs v b (product fs) hnv

private theorem boundary_messages_aux (p : Pattern) (hvs : p.freeVars.Nodup) (n : Nat)
    (db : DB) (b : Bind) : elimination p n db b = reference p n db b := by
  unfold elimination reference
  rw [product_fold_eliminate n p.freeVars (factors p db) b _ hvs]
  · rw [show product (factors p db) =
        (fun x => (p.atoms.map (fun a => value db a x)).prod) by
      funext x
      simp [factors, product, Function.comp_def]]
  · intro f hf
    simp only [factors] at hf
    rcases List.mem_map.mp hf with ⟨a, ha, rfl⟩
    exact respects_value db a


namespace CountryReturn
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.CountryReturn

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  intro n db b
  exact boundary_messages_aux pattern (by decide) n db b

end CountryReturn

namespace OrderSupplier
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.OrderSupplier

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  intro n db b
  exact boundary_messages_aux pattern (by decide) n db b

end OrderSupplier

namespace ReturnedItem
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReturnedItem

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  intro n db b
  exact boundary_messages_aux pattern (by decide) n db b

end ReturnedItem

namespace ChannelBridge
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ChannelBridge

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  intro n db b
  exact boundary_messages_aux pattern (by decide) n db b

end ChannelBridge

namespace PromoInventory
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.PromoInventory

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  intro n db b
  exact boundary_messages_aux pattern (by decide) n db b

end PromoInventory

/-! Finite-range sum helpers used by the closed-form join identities. -/

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
  | succ n ih =>
      rw [rsum_succ, rsum_succ, rsum_succ, ih]
      omega

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih, Int.mul_add]

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih, Int.add_mul]

private theorem rsum_neg (n : Nat) (f : Nat → Int) : rsum n (fun i => - f i) = - rsum n f := by
  simpa using (rsum_const_mul n (-1) f)

private theorem rsum_sub (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i - g i) = rsum n f - rsum n g := by
  simp only [Int.sub_eq_add_neg]
  rw [rsum_add, rsum_neg]

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

private theorem rsum_product (n m : Nat) (f : Nat → Int) (g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr
            intro i hi
            rw [rsum_const_mul]
    _ = rsum n f * rsum m g := by rw [rsum_mul_const]

private theorem rsum_mul_rsum (n m : Nat) (f g : Nat → Int) :
    rsum n f * rsum m g = rsum n (fun i => rsum m (fun j => f i * g j)) := by
  rw [rsum_product]

private theorem rsum_prod3 (n m k : Nat) (f : Nat → Int) (g : Nat → Int) (h : Nat → Int) :
    rsum n (fun i => rsum m (fun j => rsum k (fun l => f i * g j * h l))) =
      rsum n f * rsum m g * rsum k h := by
  calc
    rsum n (fun i => rsum m (fun j => rsum k (fun l => f i * g j * h l)))
        = rsum n (fun i => rsum m (fun j => (f i * g j) * rsum k h)) := by
            apply rsum_congr; intro i hi
            apply rsum_congr; intro j hj
            rw [rsum_const_mul]
    _ = rsum n (fun i => (rsum m (fun j => f i * g j)) * rsum k h) := by
            apply rsum_congr; intro i hi
            rw [rsum_mul_const]
    _ = rsum n (fun i => (f i * rsum m g) * rsum k h) := by
            apply rsum_congr; intro i hi
            rw [rsum_const_mul]
    _ = rsum n (fun i => f i * rsum m g) * rsum k h := by rw [rsum_mul_const]
    _ = (rsum n f * rsum m g) * rsum k h := by rw [rsum_mul_const]

namespace OrderSupplier
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.OrderSupplier

theorem SharedDestination_sound : SharedDestination.Goal := by
  intro n db b
  simp [reference, integrate, pattern, value, put, bridge, dot, total, rsum]
  change rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
      db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 1 (b 0) x4 * db 2 x4 x3))))))) =
    rsum n (fun z => rsum n (fun y =>
      rsum n (fun x1 => db 0 (b 0) x1 * db 1 x1 y) * db 2 y z) *
      rsum n (fun x4 => db 1 (b 0) x4 * db 2 x4 z))
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
        db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 1 (b 0) x4 * db 2 x4 x3)))))))
      = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
        db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 1 (b 0) x4 * db 2 x4 x3))))))) := by
          apply rsum_congr; intro x1 hx1
          apply rsum_congr; intro x2 hx2
          exact rsum_comm n n (fun x4 x3 =>
            db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 1 (b 0) x4 * db 2 x4 x3))))
    _ = rsum n (fun x3 => rsum n (fun x2 => rsum n (fun x1 => rsum n (fun x4 =>
        db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 1 (b 0) x4 * db 2 x4 x3))))))) := by
          calc
            rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
                db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 1 (b 0) x4 * db 2 x4 x3)))))))
              = rsum n (fun x2 => rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x4 =>
                db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 1 (b 0) x4 * db 2 x4 x3))))))) := by
                  apply rsum_comm
            _ = rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x1 => rsum n (fun x4 =>
                db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 1 (b 0) x4 * db 2 x4 x3))))))) := by
                  apply rsum_congr; intro x2 hx2
                  apply rsum_comm
            _ = rsum n (fun x3 => rsum n (fun x2 => rsum n (fun x1 => rsum n (fun x4 =>
                db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 1 (b 0) x4 * db 2 x4 x3))))))) := by
                  apply rsum_comm
    _ = rsum n (fun z => rsum n (fun y =>
          rsum n (fun x1 => db 0 (b 0) x1 * db 1 x1 y) * db 2 y z) *
          rsum n (fun x4 => db 1 (b 0) x4 * db 2 x4 z)) := by
          apply rsum_congr; intro z hz
          rw [← rsum_product]
          apply rsum_congr; intro y hy
          rw [show rsum n (fun a => rsum n (fun c =>
                db 0 (b 0) a * (db 1 a y * (db 2 y z * (db 1 (b 0) c * db 2 c z))))) =
              rsum n (fun c => rsum n (fun a =>
                db 0 (b 0) a * (db 1 a y * (db 2 y z * (db 1 (b 0) c * db 2 c z))))) by
            apply rsum_comm]
          apply rsum_congr; intro u hu
          rw [show ((rsum n (fun q => db 0 (b 0) q * db 1 q y)) * db 2 y z) *
                (db 1 (b 0) u * db 2 u z) =
              rsum n (fun q => (db 0 (b 0) q * db 1 q y) *
                (db 2 y z * (db 1 (b 0) u * db 2 u z))) by
            rw [Int.mul_assoc]
            rw [← rsum_mul_const]]
          apply rsum_congr; intro q hq
          grind

end OrderSupplier

namespace ReturnedItem
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReturnedItem

theorem AnchoredChord_sound : AnchoredChord.Goal := by
  intro n db b
  simp [reference, integrate, TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReturnedItem.pattern,
    value, put, bridge, dot, total, rsum]
  change rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 3 x3 (b 0) * db 2 (b 0) x2)))))) =
    rsum n (fun y => db 2 (b 0) y *
      (rsum n (fun x1 => db 0 (b 0) x1 * db 1 x1 y)) *
      (rsum n (fun x3 => db 2 y x3 * db 3 x3 (b 0))))
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
        db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 3 x3 (b 0) * db 2 (b 0) x2))))))
      = rsum n (fun x2 => rsum n (fun x1 => rsum n (fun x3 =>
        db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * (db 3 x3 (b 0) * db 2 (b 0) x2)))))) := by
          apply rsum_comm
    _ = rsum n (fun y => db 2 (b 0) y *
          (rsum n (fun x1 => db 0 (b 0) x1 * db 1 x1 y)) *
          (rsum n (fun x3 => db 2 y x3 * db 3 x3 (b 0)))) := by
          apply rsum_congr; intro y hy
          rw [show db 2 (b 0) y * (rsum n (fun x1 => db 0 (b 0) x1 * db 1 x1 y)) *
                (rsum n (fun x3 => db 2 y x3 * db 3 x3 (b 0))) =
              rsum n (fun x1 => rsum n (fun x3 =>
                (db 2 (b 0) y * (db 0 (b 0) x1 * db 1 x1 y)) *
                  (db 2 y x3 * db 3 x3 (b 0)))) by
            calc
              db 2 (b 0) y * (rsum n (fun x1 => db 0 (b 0) x1 * db 1 x1 y)) *
                  (rsum n (fun x3 => db 2 y x3 * db 3 x3 (b 0)))
                = (db 2 (b 0) y * (rsum n (fun x1 => db 0 (b 0) x1 * db 1 x1 y))) *
                  (rsum n (fun x3 => db 2 y x3 * db 3 x3 (b 0))) := by grind
              _ = (rsum n (fun x1 => db 2 (b 0) y * (db 0 (b 0) x1 * db 1 x1 y))) *
                  (rsum n (fun x3 => db 2 y x3 * db 3 x3 (b 0))) := by
                    rw [rsum_const_mul]
              _ = rsum n (fun x1 => (db 2 (b 0) y * (db 0 (b 0) x1 * db 1 x1 y)) *
                  (rsum n (fun x3 => db 2 y x3 * db 3 x3 (b 0)))) := by
                    rw [rsum_mul_const]
              _ = rsum n (fun x1 => rsum n (fun x3 =>
                    (db 2 (b 0) y * (db 0 (b 0) x1 * db 1 x1 y)) *
                      (db 2 y x3 * db 3 x3 (b 0)))) := by
                    apply rsum_congr; intro x1 hx1
                    rw [← rsum_const_mul]]
          apply rsum_congr; intro x1 hx1
          apply rsum_congr; intro x3 hx3
          grind

end ReturnedItem

namespace ChannelBridge
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ChannelBridge

theorem TripleOverlap_sound : TripleOverlap.Goal := by
  intro n db b
  simp [reference, integrate, TinylambOpt.ResearchGoals.Atlas.JoinPlans.ChannelBridge.pattern,
    value, put, bridge, dot, overlap, total, rsum]
  change rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 => rsum n (fun x1 =>
      db 0 (b 0) x2 * (db 0 x1 x2 * (db 1 (b 0) x3 * (db 1 x1 x3 *
        (db 2 (b 0) x4 * db 2 x1 x4)))))))) =
    rsum n (fun y => (rsum n (fun k => db 0 (b 0) k * db 0 y k)) *
      (rsum n (fun k => db 1 (b 0) k * db 1 y k)) *
      (rsum n (fun k => db 2 (b 0) k * db 2 y k)))
  calc
    rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 => rsum n (fun x1 =>
        db 0 (b 0) x2 * (db 0 x1 x2 * (db 1 (b 0) x3 * (db 1 x1 x3 *
          (db 2 (b 0) x4 * db 2 x1 x4))))))))
      = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
        db 0 (b 0) x2 * (db 0 x1 x2 * (db 1 (b 0) x3 * (db 1 x1 x3 *
          (db 2 (b 0) x4 * db 2 x1 x4)))))))) := by
          calc
            rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 => rsum n (fun x1 =>
                db 0 (b 0) x2 * (db 0 x1 x2 * (db 1 (b 0) x3 * (db 1 x1 x3 *
                  (db 2 (b 0) x4 * db 2 x1 x4))))))))
              = rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x1 => rsum n (fun x4 =>
                db 0 (b 0) x2 * (db 0 x1 x2 * (db 1 (b 0) x3 * (db 1 x1 x3 *
                  (db 2 (b 0) x4 * db 2 x1 x4)))))))) := by
                  apply rsum_congr; intro x2 hx2
                  apply rsum_congr; intro x3 hx3
                  apply rsum_comm
            _ = rsum n (fun x2 => rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x4 =>
                db 0 (b 0) x2 * (db 0 x1 x2 * (db 1 (b 0) x3 * (db 1 x1 x3 *
                  (db 2 (b 0) x4 * db 2 x1 x4)))))))) := by
                  apply rsum_congr; intro x2 hx2
                  apply rsum_comm
            _ = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
                db 0 (b 0) x2 * (db 0 x1 x2 * (db 1 (b 0) x3 * (db 1 x1 x3 *
                  (db 2 (b 0) x4 * db 2 x1 x4)))))))) := by
                  apply rsum_comm
    _ = rsum n (fun y => (rsum n (fun k => db 0 (b 0) k * db 0 y k)) *
          (rsum n (fun k => db 1 (b 0) k * db 1 y k)) *
          (rsum n (fun k => db 2 (b 0) k * db 2 y k))) := by
          apply rsum_congr; intro y hy
          rw [← rsum_prod3 n n n
            (fun x2 => db 0 (b 0) x2 * db 0 y x2)
            (fun x3 => db 1 (b 0) x3 * db 1 y x3)
            (fun x4 => db 2 (b 0) x4 * db 2 y x4)]
          apply rsum_congr; intro x2 hx2
          apply rsum_congr; intro x3 hx3
          apply rsum_congr; intro x4 hx4
          grind

end ChannelBridge

/-! Goals still open (recorded for a follow-up pass). -/

namespace CountryReturn
-- TODO(AnchoredRepair)
-- TODO(FractionalCap)
-- TODO(DeltaForecast)
-- TODO(CollisionCorrection)
end CountryReturn

namespace OrderSupplier
-- TODO(AnchoredRepair)
-- TODO(FractionalCap)
-- TODO(CollisionCorrection)
end OrderSupplier

namespace ReturnedItem
-- TODO(AnchoredRepair)
-- TODO(FractionalCap)
-- TODO(CollisionCorrection)
end ReturnedItem

namespace ChannelBridge
-- TODO(AnchoredRepair)
-- TODO(FractionalCap)
-- TODO(CollisionCorrection)
end ChannelBridge

namespace PromoInventory
-- TODO(AnchoredRepair)
-- TODO(FractionalCap)
-- TODO(DeltaForecast)
-- TODO(CollisionCorrection)
end PromoInventory

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReturnAndSupplierJoinMessages
