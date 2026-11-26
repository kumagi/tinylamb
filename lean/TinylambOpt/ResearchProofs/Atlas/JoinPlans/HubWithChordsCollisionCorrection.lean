import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `HubWithChords.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.HubWithChordsCollisionCorrection

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.HubWithChords

example : pattern.freeVars = [1, 2, 5, 4, 3, 0] := by rfl

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_zero_const (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
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
      rw [show rsum 0 (fun i => rsum m (fun j => f i j)) = 0 by simp [rsum]]
      exact (rsum_zero_const m).symm
  | succ n ih =>
      rw [rsum_succ, ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr
        intro j hj
        rw [rsum_succ]]
      rw [rsum_add]

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

/-- Partition-helper definitions mirroring the `collapsed` definition. -/
private def headOf : List Nat → Nat
  | h :: _ => h
  | [] => 0

private def Heads (bs : List (List Nat)) : List Nat := bs.filterMap List.head?

private def Root (bs : List (List Nat)) (v : Nat) : Nat :=
  ((bs.find? (fun bl => bl.contains v)).bind List.head?).getD v

private def constOn (bs : List (List Nat)) (x : Bind) : Bool :=
  bs.all (fun bl => bl.all (fun v => x v == x (headOf bl)))

/-- Boolean structural spec of a partition of the fixed variable list. -/
private def PSb (V : List Nat) (bs : List (List Nat)) : Bool :=
  decide bs.Nodup &&
  bs.all (fun bl => decide bl.Nodup && bl.all (fun v => V.contains v)) &&
  bs.all (fun bl1 => bs.all (fun bl2 =>
    bl1.all (fun v => if bl2.contains v then bl1 == bl2 else true))) &&
  V.all (fun v => bs.any (fun bl => bl.contains v))

private theorem ps_all : (partitions pattern.freeVars).all (fun bs => PSb pattern.freeVars bs) = true := by
  native_decide

private theorem ps_of_mem {bs : List (List Nat)} (hbs : bs ∈ partitions pattern.freeVars) :
    PSb pattern.freeVars bs = true :=
  List.all_eq_true.mp ps_all bs hbs

private theorem find_eq_some_unique {α} (p : α → Bool) (l : List α) (a : α)
    (ha : a ∈ l) (hp : p a = true) (huniq : ∀ b ∈ l, p b = true → b = a) :
    l.find? p = some a := by
  induction l with
  | nil => simp at ha
  | cons x xs ih =>
      by_cases hxa : x = a
      · subst x
        simp [hp]
      · have hax : a ≠ x := by intro h; exact hxa h.symm
        have haxs : a ∈ xs := by simpa [hax] using ha
        have hpx : p x = false := by
          cases hpx' : p x with
          | true =>
              have hxeq : x = a := huniq x (by simp) hpx'
              exact False.elim (hxa hxeq)
          | false => rfl
        simp [hpx, ih haxs (fun b hb hpb => huniq b (by simp [hb]) hpb)]

private theorem root_eq_head (bs : List (List Nat)) (hPS : PSb pattern.freeVars bs = true) :
    ∀ bl ∈ bs, ∀ v ∈ bl, Root bs v = headOf bl := by
  simp [PSb] at hPS
  rcases hPS with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  intro bl hbl v hv
  have hpbl : bl.contains v = true := by simpa using hv
  have huniq : ∀ b ∈ bs, b.contains v = true → b = bl := by
    intro b hb hbv
    have hdeq := hdisj bl hbl b hb v hv
    have hbmem : v ∈ b := by simpa using hbv
    rcases hdeq with hnot | heq
    · exact False.elim (hnot hbmem)
    · exact heq.symm
  have hfind : bs.find? (fun b : List Nat => b.contains v) = some bl :=
    find_eq_some_unique (fun b : List Nat => b.contains v) bs bl hbl hpbl huniq
  unfold Root
  rw [hfind]
  cases bl with
  | nil => simp at hv
  | cons h tl => rfl

private theorem head_mem_heads_cons (bl : List Nat) (bs : List (List Nat))
    (hs : bl.head?.isSome = true) : headOf bl ∈ Heads (bl :: bs) := by
  cases bl with
  | nil => simp at hs
  | cons h tl => simp [Heads, headOf]

private theorem head_mem_heads {bl : List Nat} {bs : List (List Nat)}
    (hbl : bl ∈ bs) : bl.head?.isSome = true → headOf bl ∈ Heads bs := by
  induction bs with
  | nil => simp at hbl
  | cons b bs ih =>
      intro hs
      rcases List.mem_cons.mp hbl with hbeq | hbl
      · subst bl
        exact head_mem_heads_cons b bs hs
      · have htail := ih hbl hs
        cases hb : b.head? with
        | none => simpa [Heads, hb] using htail
        | some h =>
            simp [Heads, hb]
            apply Or.inr
            exact List.mem_filterMap.mp htail

private def HPb (bs : List (List Nat)) : Bool :=
  decide (Heads bs).Nodup && (Heads bs).all (fun h => pattern.freeVars.contains h)

private theorem hp_all : (partitions pattern.freeVars).all HPb = true := by
  native_decide

private theorem heads_spec {bs : List (List Nat)} (hbs : bs ∈ partitions pattern.freeVars) :
    (Heads bs).Nodup ∧ (∀ h ∈ Heads bs, h ∈ pattern.freeVars) := by
  have h := List.all_eq_true.mp hp_all bs hbs
  simp [HPb] at h
  exact h

private theorem find_none_of_all_false {α} (p : α → Bool) (l : List α)
    (h : ∀ a ∈ l, p a = false) : l.find? p = none := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp [List.find?, h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem root_out {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) :
    ∀ v, v ∉ pattern.freeVars → Root bs v = v := by
  have hPS' := hPS
  simp [PSb] at hPS'
  rcases hPS' with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  intro v hv
  have hfalse : ∀ bl ∈ bs, bl.contains v = false := by
    intro bl hbl
    have hbsub := hsub bl hbl
    by_cases hne : bl.contains v = true
    · have hvbl : v ∈ bl := by simpa using hne
      exact False.elim (hv (hbsub.2 v hvbl))
    · cases hne' : bl.contains v <;> simp_all
  unfold Root
  rw [find_none_of_all_false (fun bl : List Nat => bl.contains v) bs hfalse]
  simp

private theorem root_head {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) :
    ∀ h ∈ Heads bs, Root bs h = h := by
  intro h hh
  rcases List.mem_filterMap.mp hh with ⟨bl, hbl, hhbl⟩
  have hhroot : headOf bl = h := by
    cases bl with
    | nil => simp at hhbl
    | cons a t => simpa [headOf] using hhbl
  have hvbl : headOf bl ∈ bl := by
    cases bl with
    | nil => simp at hhbl
    | cons a t => simp [headOf]
  rw [← hhroot]
  exact root_eq_head bs hPS bl hbl (headOf bl) hvbl

private theorem root_mem_heads {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) :
    ∀ v ∈ pattern.freeVars, Root bs v ∈ Heads bs := by
  have hPS' := hPS
  simp [PSb] at hPS'
  rcases hPS' with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  intro v hv
  rcases hcov v hv with ⟨bl, hbl, hvbl⟩
  rw [root_eq_head bs hPS bl hbl v hvbl]
  exact head_mem_heads hbl (by cases bl with | nil => simp at hvbl | cons h t => simp)

private theorem constOn_iff {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) (x : Bind) :
    constOn bs x = true ↔ ∀ v ∈ pattern.freeVars, x v = x (Root bs v) := by
  have hPS' := hPS
  simp [PSb] at hPS'
  rcases hPS' with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  constructor
  · intro hc v hv
    rcases hcov v hv with ⟨bl, hbl, hvbl⟩
    rw [root_eq_head bs hPS bl hbl v hvbl]
    have hbeq := List.all_eq_true.mp hc bl hbl
    have hveq := List.all_eq_true.mp hbeq v hvbl
    simpa using hveq
  · intro hx
    apply List.all_eq_true.mpr
    intro bl hbl
    apply List.all_eq_true.mpr
    intro v hvbl
    have hvV : v ∈ pattern.freeVars := (hsub bl hbl).2 v hvbl
    have hrv := hx v hvV
    rw [root_eq_head bs hPS bl hbl v hvbl] at hrv
    simpa using hrv

/-- Integration helpers (Std-only). -/
private theorem sum_congr {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem sum_eq_zero {α} (l : List α) (f : α → Int) (h : ∀ x ∈ l, f x = 0) :
    (l.map f).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]
      simp

private theorem put_self (b : Bind) (v x : Nat) : put b v x v = x := by simp [put]

private theorem put_same (b : Bind) (v : Nat) : put b v (b v) = b := by
  funext i
  by_cases hi : i == v
  · have hiv : i = v := Nat.eq_of_beq_eq_true hi
    subst i
    simp [put]
  · simp [put, hi]

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

private theorem integrate_swap (n v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: w :: vs) b f = integrate n (w :: v :: vs) b f := by
  by_cases h : v == w
  · have hvw : v = w := Nat.eq_of_beq_eq_true h
    subst w
    rfl
  · have hne : (v != w) = true := by cases h' : (v == w) <;> simp_all
    change ((List.range n).map (fun x =>
        ((List.range n).map (fun y => integrate n vs (put (put b v x) w y) f)).sum)).sum =
      ((List.range n).map (fun x =>
        ((List.range n).map (fun y => integrate n vs (put (put b w x) v y) f)).sum)).sum
    change rsum n (fun x => rsum n (fun y => integrate n vs (put (put b v x) w y) f)) =
      rsum n (fun x => rsum n (fun y => integrate n vs (put (put b w x) v y) f))
    rw [rsum_comm]
    apply sum_congr
    intro x hx
    apply sum_congr
    intro y hy
    rw [put_comm_ne b v w y x hne]

private theorem integrate_append (n : Nat) (xs ys : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (xs ++ ys) b f = integrate n xs b (fun b' => integrate n ys b' f) := by
  induction xs generalizing b with
  | nil => rfl
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n (vs ++ ys) (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) (fun b' => integrate n ys b' f))).sum
      apply sum_congr
      intro x hx
      exact ih (put b v x)

private theorem integrate_perm (n : Nat) {xs ys : List Nat} (h : xs.Perm ys)
    (b : Bind) (f : Bind → Int) : integrate n xs b f = integrate n ys b f := by
  induction h generalizing b with
  | nil => rfl
  | cons x h ih =>
      change ((List.range n).map (fun t => integrate n _ (put b x t) f)).sum =
        ((List.range n).map (fun t => integrate n _ (put b x t) f)).sum
      apply sum_congr
      intro t ht
      exact ih (put b x t)
  | swap x y l => exact (integrate_swap n x y l b f).symm
  | trans h1 h2 ih1 ih2 => exact (ih1 b).trans (ih2 b)

private def bindings (n : Nat) : List Nat → Bind → List Bind
  | [], b => [b]
  | v :: vs, b => (List.range n).flatMap (fun x => bindings n vs (put b v x))

private theorem bindings_sum (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n vs b f = ((bindings n vs b).map f).sum := by
  induction vs generalizing b with
  | nil => simp [integrate, bindings]
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        (((List.range n).flatMap (fun x => bindings n vs (put b v x))).map f).sum
      rw [show ((List.range n).flatMap (fun x => bindings n vs (put b v x))).map f =
          (List.range n).flatMap (fun x => (bindings n vs (put b v x)).map f) by
            induction List.range n with
            | nil => rfl
            | cons a l ih2 => simp [List.map_append, ih2]]
      rw [show ((List.range n).flatMap (fun x => (bindings n vs (put b v x)).map f)).sum =
          ((List.range n).map (fun x => ((bindings n vs (put b v x)).map f).sum)).sum by
            induction List.range n with
            | nil => rfl
            | cons a l ih2 => simp [List.sum_append, ih2]]
      apply sum_congr
      intro x hx
      exact ih (put b v x)

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
      rw [ih (put b w x) y hy v hvws]
      simp [put, hvw]

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
          rw [heq]
          simpa [put] using hx
      · have htail : v ∈ ws := by
          rcases List.mem_cons.mp hv with h | h
          · exact (hvw h.symm).elim
          · exact h
        exact ih (put b w x) y hy v htail

private theorem integrate_congr_bindings (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ y, y ∈ bindings n vs b → f y = g y) :
    integrate n vs b f = integrate n vs b g := by
  rw [bindings_sum, bindings_sum]
  apply sum_congr
  intro y hy
  exact h y hy

private theorem rsum_ite_eq_single (n a : Nat) (ha : a < n) (f : Nat → Int) :
    rsum n (fun x => if x = a then f x else 0) = f a := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsum_succ]
      by_cases h : a = n
      · subst a
        have hzero : rsum n (fun x => if x = n then f x else 0) = 0 := by
          rw [show rsum n (fun x => if x = n then f x else 0) = rsum n (fun _ => (0 : Int)) by
            apply rsum_congr
            intro x hx
            have hxn : x ≠ n := by omega
            simp [hxn]]
          exact rsum_zero_const n
        rw [hzero]
        simp
      · have han : a < n := by omega
        rw [ih han]
        have hne : ¬ n = a := by omega
        simp [hne]

private theorem integrate_zero_of_false (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int)
    (h : ∀ y ∈ bindings n vs b, f y = 0) : integrate n vs b f = 0 := by
  rw [bindings_sum]
  apply sum_eq_zero
  intro y hy
  exact h y hy

private theorem inner_collapse (n : Nat) (M : List Nat) (Root : Nat → Nat) (y : Bind) (C : Int)
    (hM : M.Nodup) (hRnot : ∀ m ∈ M, Root m ∉ M)
    (hy : ∀ m ∈ M, y (Root m) ∈ List.range n) :
    integrate n M y (fun x => if M.all (fun m => x m == y (Root m)) then C else 0) = C := by
  induction M generalizing y with
  | nil => simp [integrate]
  | cons m Ms ih =>
      have hmMs : m ∉ Ms := (List.nodup_cons.mp hM).1
      have hMnd : Ms.Nodup := (List.nodup_cons.mp hM).2
      have hRnot' : ∀ v ∈ Ms, Root v ∉ Ms := by
        intro v hv hmem
        exact hRnot v (by simp [hv]) (by simp [hmem])
      have hrootne : ∀ v ∈ Ms, Root v ≠ m := by
        intro v hv h_eq
        exact hRnot v (by simp [hv]) (by simp [h_eq])
      have hpoint : ∀ t < n,
          integrate n Ms (put y m t)
            (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0) =
          if t = y (Root m) then C else 0 := by
        intro t ht
        by_cases htm : t = y (Root m)
        · subst t
          have hyrange' : ∀ v ∈ Ms, (put y m (y (Root m))) (Root v) ∈ List.range n := by
            intro v hv
            have hne := hrootne v hv
            simpa [put, hne] using hy v (by simp [hv])
          calc
            integrate n Ms (put y m (y (Root m)))
                (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0)
                = integrate n Ms (put y m (y (Root m)))
                    (fun x => if Ms.all (fun v => x v == (put y m (y (Root m))) (Root v)) then C else 0) := by
                    apply integrate_congr_bindings
                    intro x hx
                    have hxm : x m = y (Root m) := by
                      rw [bindings_fixed n Ms (put y m (y (Root m))) x hx m hmMs]
                      simp [put]
                    simp [hxm]
                    by_cases hleft : ∀ v ∈ Ms, x v = y (Root v)
                    · have hright : ∀ v ∈ Ms, x v = (put y m (y (Root m))) (Root v) := by
                        intro v hv
                        have hv' := hleft v hv
                        have hneq : (Root v == m) = false := by
                          cases hb : Root v == m <;> simp_all
                        simpa [put, hneq] using hv'
                      rw [ite_eq_left hleft, ite_eq_left hright]
                    · have hrightnot : ¬ (∀ v ∈ Ms, x v = (put y m (y (Root m))) (Root v)) := by
                        intro hr
                        apply hleft
                        intro v hv
                        have hv' := hr v hv
                        have hneq : (Root v == m) = false := by
                          cases hb : Root v == m <;> simp_all
                        simpa [put, hneq] using hv'
                      rw [ite_eq_right hleft, ite_eq_right hrightnot]
            _ = C := ih (put y m (y (Root m))) hMnd hRnot' hyrange'
            _ = (if y (Root m) = y (Root m) then C else 0) := by simp
        · have hzero : integrate n Ms (put y m t)
              (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0) = 0 := by
            apply integrate_zero_of_false
            intro x hx
            have hxm : x m = t := by
              rw [bindings_fixed n Ms (put y m t) x hx m hmMs]
              simp [put]
            simp [hxm, htm]
          simpa [htm] using hzero
      change rsum n (fun t => integrate n Ms (put y m t)
        (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0)) = C
      rw [show (fun t => integrate n Ms (put y m t)
        (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0)) =
        (fun t => if t = y (Root m) then C else 0) by
          funext t
          by_cases ht : t < n
          · exact hpoint t ht
          · have htn : y (Root m) < n := by
              have := hy m (by simp)
              simpa using this
            have htne : t ≠ y (Root m) := by omega
            simp [htne]
            apply integrate_zero_of_false
            intro x hx
            have hxm : x m = t := by
              rw [bindings_fixed n Ms (put y m t) x hx m hmMs]
              simp [put]
            simp [hxm, htne]]
      rw [rsum_ite_eq_single n (y (Root m)) (by
        have := hy m (by simp)
        simpa using this)]

private theorem perm_append_left {α} {l1 l2 : List α} (h : l1.Perm l2) (l3 : List α) :
    (l1 ++ l3).Perm (l2 ++ l3) := by
  induction h with
  | nil => rfl
  | cons x h ih => exact .cons x ih
  | swap x y l => exact .swap x y (l ++ l3)
  | trans h1 h2 ih1 ih2 => exact ih1.trans ih2

private theorem perm_append_right {α} (l1 : List α) {l3 l4 : List α} (h : l3.Perm l4) :
    (l1 ++ l3).Perm (l1 ++ l4) := by
  induction l1 with
  | nil => simpa using h
  | cons a l1 ih => exact .cons a ih

private theorem perm_append {α} {l1 l2 l3 l4 : List α} (h1 : l1.Perm l2) (h2 : l3.Perm l4) :
    (l1 ++ l3).Perm (l2 ++ l4) := by
  exact (perm_append_left h1 l3).trans (perm_append_right l2 h2)

private theorem cons_append_perm (a : Nat) (l : List Nat) : (a :: l).Perm (l ++ [a]) := by
  induction l with
  | nil => simp
  | cons x l ih => exact (List.Perm.swap x a l).trans (.cons x ih)

private theorem filter_append_compl (p : Nat → Bool) (l : List Nat) :
    l.Perm ((l.filter p) ++ (l.filter (fun x => !p x))) := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases ha : p a = true
      · simpa [ha] using (List.Perm.cons a ih)
      · have ha' : p a = false := by cases h : p a <;> simp_all
        have hstep := (List.Perm.cons a ih).trans
          (perm_append (cons_append_perm a (l.filter p))
            (List.Perm.refl (l.filter (fun x => !p x))))
        simpa [ha'] using hstep

private theorem perm_filter_of_nodup {l l' : List Nat} (hnd : l.Nodup) (hnd' : l'.Nodup)
    (hsub : ∀ x ∈ l, x ∈ l') : l.Perm (l'.filter (fun x => x ∈ l)) := by
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
      · have htail := ih (hnd.erase a) (List.nodup_cons.mp hnd').2 (by
          intro x hx
          have hxa : x ≠ a := by intro h; subst x; exact hnd.not_mem_erase hx
          have hxl := (List.mem_erase_of_ne hxa).mp hx
          simpa [hxa] using hsub x hxl)
        have hab : decide (a ∈ l) = true := by simp [ha]
        have hfilter : (a :: l').filter (fun x => x ∈ l) =
            a :: l'.filter (fun x : Nat => x ∈ l.erase a) := by
          rw [List.filter_cons, ite_eq_left hab]
          congr 1
          apply List.filter_congr
          intro x hx
          have hxa : x ≠ a := by intro h; subst x; exact hnot hx
          simp [List.mem_erase_of_ne hxa]
        rw [hfilter]
        exact (List.perm_cons_erase ha).trans (.cons a htail)
      · rw [List.filter_cons]
        simp [ha]
        apply ih hnd (List.nodup_cons.mp hnd').2
        intro x hx
        have hxa : x ≠ a := by intro h; subst x; exact ha hx
        simpa [hxa] using hsub x hx

private theorem collapse_sum (n : Nat) (b : Bind) (F : Bind → Int)
    (bs : List (List Nat)) (hbs : bs ∈ partitions pattern.freeVars)
    (hF : ∀ x y, (∀ v ∈ pattern.freeVars, x v = y v) → F x = F y) :
    integrate n (Heads bs) b (fun y => F (fun v => y (Root bs v))) =
      integrate n pattern.freeVars b (fun x => if constOn bs x then F x else 0) := by
  have hPS := ps_of_mem hbs
  have hspec := heads_spec hbs
  have hVnd : pattern.freeVars.Nodup := by native_decide
  let H := Heads bs
  let M := pattern.freeVars.filter (fun v => !H.contains v)
  have hHsub : ∀ v ∈ H, v ∈ pattern.freeVars := hspec.2
  have hrootV : ∀ v ∈ pattern.freeVars, Root bs v ∈ H := by
    intro v hv
    exact root_mem_heads hPS v hv
  have hrootH : ∀ h ∈ H, Root bs h = h := by
    intro h hh
    exact root_head hPS h hh
  have hrootOut : ∀ v, v ∉ pattern.freeVars → Root bs v = v := root_out hPS
  have hconst : ∀ x, constOn bs x = true ↔ ∀ v ∈ pattern.freeVars, x v = x (Root bs v) := constOn_iff hPS
  have hfilt : (pattern.freeVars.filter (fun v => H.contains v)).Perm H := by
    have hp := perm_filter_of_nodup hspec.1 hVnd hHsub
    simpa using hp.symm
  have hpermV : pattern.freeVars.Perm (H ++ M) := by
    have h1 := filter_append_compl H.contains pattern.freeVars
    have h2 : (pattern.freeVars.filter (fun v => H.contains v) ++
        pattern.freeVars.filter (fun v => !H.contains v)).Perm
        (H ++ M) := perm_append hfilt (by rfl)
    exact h1.trans h2
  rw [integrate_perm n hpermV b (fun x => if constOn bs x then F x else 0)]
  rw [integrate_append]
  apply integrate_congr_bindings
  intro y hy
  have hyH : ∀ h ∈ H, y h ∈ List.range n := by
    intro h hh
    exact bindings_range n H b y hy h hh
  have hyfixed : ∀ v ∉ H, y v = b v := by
    intro v hv
    exact bindings_fixed n H b y hy v hv
  let x0 : Bind := fun v => y (Root bs v)
  have hx0V : ∀ v ∈ pattern.freeVars, x0 v ∈ List.range n := by
    intro v hv
    have hr := hrootV v hv
    simpa [x0] using hyH (Root bs v) hr
  have hMsub : ∀ v ∈ M, v ∈ pattern.freeVars := by
    intro v hv
    exact (List.mem_filter.mp hv).1
  have hMnotH : ∀ v ∈ M, v ∉ H := by
    intro v hv
    have hnot := (List.mem_filter.mp hv).2
    simpa using hnot
  have hMnd : M.Nodup := hVnd.filter _
  have hRnot : ∀ m ∈ M, Root bs m ∉ M := by
    intro m hm hmem
    have hr := hrootV m (hMsub m hm)
    exact hMnotH (Root bs m) hmem hr
  have hyrange : ∀ m ∈ M, y (Root bs m) ∈ List.range n := by
    intro m hm
    exact hyH (Root bs m) (hrootV m (hMsub m hm))
  have hinner : integrate n M y (fun x => if constOn bs x then F x else 0) = F x0 := by
    calc
      integrate n M y (fun x => if constOn bs x then F x else 0)
          = integrate n M y (fun x => if M.all (fun m => x m == y (Root bs m)) then F x0 else 0) := by
            apply integrate_congr_bindings
            intro x hx
            have hxfixedH : ∀ h ∈ H, x h = y h := by
              intro h hh
              have hnot : h ∉ M := by
                intro hm
                exact hMnotH h hm hh
              rw [bindings_fixed n M y x hx h hnot]
            by_cases hall : M.all (fun m => x m == y (Root bs m)) = true
            · rw [ite_eq_left hall]
              have hxFx0 : ∀ v ∈ pattern.freeVars, x v = x0 v := by
                intro v hv
                by_cases hvH : v ∈ H
                · have hrv := hrootH v hvH
                  change x v = y (Root bs v)
                  rw [hxfixedH v hvH, hrv]
                · have hvM : v ∈ M := by
                    simp [M, hv, hvH]
                  have hvEq := List.all_eq_true.mp hall v hvM
                  change x v = y (Root bs v)
                  simpa using hvEq
              have hcond : ∀ v ∈ pattern.freeVars, x v = x (Root bs v) := by
                intro v hv
                have hxv := hxFx0 v hv
                have hxroot : x (Root bs v) = y (Root bs v) :=
                  hxfixedH (Root bs v) (hrootV v hv)
                exact hxv.trans (by simpa [x0] using hxroot.symm)
              rw [ite_eq_left ((hconst x).2 hcond)]
              exact hF x x0 hxFx0
            · have hallnot : ¬ M.all (fun m => x m == y (Root bs m)) = true := by
                intro h
                exact hall h
              rw [ite_eq_right hallnot]
              have hcondnot : ¬ (∀ v ∈ pattern.freeVars, x v = x (Root bs v)) := by
                intro hc
                apply hall
                apply List.all_eq_true.mpr
                intro m hm
                have hcv := hc m (hMsub m hm)
                have hrv := hrootV m (hMsub m hm)
                have hxrv : x (Root bs m) = y (Root bs m) := hxfixedH (Root bs m) hrv
                simp [hcv, hxrv]
              have hconstnot : ¬ constOn bs x = true := by
                intro hx'
                exact hcondnot ((hconst x).1 hx')
              rw [ite_eq_right hconstnot]
      _ = F x0 := inner_collapse n M (fun v => Root bs v) y (F x0) hMnd hRnot hyrange
  simpa [x0] using hinner.symm

private theorem sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

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
    (l.map (fun a => (l'.map (F a)).sum)).sum = (l'.map (fun b => (l.map (fun a => F a b)).sum)).sum := by
  induction l' with
  | nil =>
      change (l.map (fun _ => (0 : Int))).sum = 0
      exact sum_zero l
  | cons b l' ih =>
      rw [show (fun a => (List.map (F a) (b :: l')).sum) =
          (fun a => F a b + (l'.map (F a)).sum) by funext a; rfl]
      rw [sum_add, ih]
      simp


/-- ## Generic Möbius inversion over equality partitions -/

private theorem sum_flatMap {α β : Type} (l : List α) (f : α → List β) (g : β → Int) :
    ((l.flatMap f).map g).sum = (l.map (fun a => ((f a).map g).sum)).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [List.flatMap_cons, ih]

private theorem sum_range_succ_shift (m : Nat) (f : Nat → Int) :
    f 0 + ((List.range m).map (fun i => f (i + 1))).sum =
      ((List.range (m + 1)).map f).sum := by
  induction m with
  | zero => simp
  | succ m ih =>
      calc
        f 0 + ((List.range (m + 1)).map (fun i => f (i + 1))).sum
            = f 0 + (((List.range m).map (fun i => f (i + 1))).sum + f (m + 1)) := by
                rw [List.range_succ]
                simp
        _ = (f 0 + ((List.range m).map (fun i => f (i + 1))).sum) + f (m + 1) := by
                rw [Int.add_assoc]
        _ = ((List.range (m + 1)).map f).sum + f (m + 1) := by rw [ih]
        _ = ((List.range (m + 2)).map f).sum := by
                rw [show List.range (m + 2) = List.range (m + 1) ++ [m + 1] by
                  simpa using (List.range_succ (n := m + 1))]
                simp

private theorem sum_zipIdx (bs : List (List Nat)) (f : Nat → Int) :
    (bs.zipIdx.map (fun p => f p.2)).sum = ((List.range bs.length).map f).sum := by
  induction bs generalizing f with
  | nil => simp
  | cons b bs ih =>
      rw [List.zipIdx_cons]
      simp only [List.map_cons, List.sum_cons]
      rw [show bs.zipIdx 1 = (bs.zipIdx).map (fun p => (p.1, p.2 + 1)) by
        simpa using (List.zipIdx_succ (l := bs) (i := 0))]
      rw [List.map_map]
      change f 0 + (bs.zipIdx.map (fun p => f (p.2 + 1))).sum =
        (List.map f (List.range (b :: bs).length)).sum
      rw [ih (fun i => f (i + 1))]
      rw [sum_range_succ_shift]
      simp

private theorem mobius_singleton (v : Nat) (bs : List (List Nat)) :
    mobius ([v] :: bs) = mobius bs := by
  simp [mobius, factorial]

private theorem mobius_cons (b : List Nat) (bs : List (List Nat)) :
    mobius (b :: bs) = ((-1 : Int)^(b.length-1) * (factorial (b.length-1) : Int)) * mobius bs := by
  simp [mobius]

private theorem block_weight_modify (k : Nat) (hk : 0 < k) :
    (-1 : Int)^k * (factorial k : Int) =
      -(k : Int) * ((-1 : Int)^(k-1) * (factorial (k-1) : Int)) := by
  cases k with
  | zero => omega
  | succ k =>
      cases k with
      | zero => simp [factorial]
      | succ k =>
          simp [factorial]
          rw [show k + 1 + 1 = (k + 1) + 1 by omega, Int.pow_succ]
          grind

private theorem modify_succ (b : List Nat) (bs : List (List Nat)) (i : Nat)
    (f : List Nat → List Nat) : (b :: bs).modify (i + 1) f = b :: bs.modify i f := by
  rw [List.modify_cons]
  simp

private theorem mobius_modify (bs : List (List Nat)) (i : Nat) (hi : i < bs.length)
    (v : Nat) (hn : ∀ bl ∈ bs, bl ≠ []) :
    mobius (bs.modify i (fun bl => v :: bl)) = -((bs.get ⟨i, hi⟩).length : Int) * mobius bs := by
  induction bs generalizing i with
  | nil => simp at hi
  | cons b bs ih =>
      cases i with
      | zero =>
          have hb : b ≠ [] := hn b (by simp)
          cases b with
          | nil => simp at hb
          | cons a as =>
              simp [List.modify, mobius]
              rw [block_weight_modify (as.length + 1) (by omega)]
              grind
      | succ i =>
          have hi' : i < bs.length := by simpa using hi
          have hn' : ∀ bl ∈ bs, bl ≠ [] := by intro bl hbl; exact hn bl (by simp [hbl])
          rw [modify_succ]
          rw [mobius_cons, mobius_cons, ih i hi' hn']
          simp only [List.get_cons_succ]
          grind

private def cOnP (bs : List (List Nat)) (x : Bind) : Prop :=
  ∀ bl ∈ bs, ∀ u ∈ bl, x u = x (headOf bl)

private theorem constOn_eq_cOnP (bs : List (List Nat)) (x : Bind) :
    constOn bs x = true ↔ cOnP bs x := by
  simp [constOn, cOnP, List.all_eq_true, beq_iff_eq]

private theorem constOn_singleton (v : Nat) (bs : List (List Nat)) (x : Bind) :
    constOn ([v] :: bs) x = constOn bs x := by
  simp [constOn, headOf]

private theorem cOnP_cons (b : List Nat) (bs : List (List Nat)) (x : Bind) :
    cOnP (b :: bs) x ↔ (∀ u ∈ b, x u = x (headOf b)) ∧ cOnP bs x := by
  simp [cOnP]

private theorem cOnP_modify (bs : List (List Nat)) (i : Nat) (hi : i < bs.length)
    (v : Nat) (x : Bind) (hn : ∀ bl ∈ bs, bl ≠ []) :
    cOnP (bs.modify i (fun bl => v :: bl)) x ↔
      (cOnP bs x ∧ ∀ u ∈ bs.get ⟨i, hi⟩, x u = x v) := by
  induction bs generalizing i with
  | nil => simp at hi
  | cons b bs ih =>
      cases i with
      | zero =>
          have hb : b ≠ [] := hn b (by simp)
          cases b with
          | nil => simp at hb
          | cons a as =>
              simp only [List.modify_cons]
              simp only [ite_true]
              unfold cOnP
              simp only [List.mem_cons, forall_eq_or_imp]
              constructor
              · intro h
                rcases h with ⟨hnew, hrest⟩
                simp [headOf] at hnew ⊢
                constructor
                · constructor
                  · intro u hu
                    exact (hnew.2 u hu).trans hnew.1.symm
                  · exact hrest
                · exact hnew
              · intro h
                rcases h with ⟨⟨_, hrest⟩, hnew⟩
                simp [headOf] at hnew ⊢
                constructor
                · exact hnew
                · exact hrest
      | succ i =>
          have hi' : i < bs.length := by simpa using hi
          have hn' : ∀ bl ∈ bs, bl ≠ [] := by intro bl hbl; exact hn bl (by simp [hbl])
          rw [modify_succ]
          rw [cOnP_cons, cOnP_cons, ih i hi' hn']
          simp only [List.get_cons_succ]
          constructor <;> intro h <;> rcases h with ⟨h1, h2⟩ <;> constructor <;> simp_all

private theorem constOn_modify (bs : List (List Nat)) (i : Nat) (hi : i < bs.length)
    (v : Nat) (x : Bind) (hn : ∀ bl ∈ bs, bl ≠ []) :
    constOn (bs.modify i (fun bl => v :: bl)) x =
      (constOn bs x && (bs.get ⟨i, hi⟩).all (fun u => x u == x v)) := by
  apply Bool.eq_iff_iff.mpr
  rw [constOn_eq_cOnP]
  rw [Bool.and_eq_true]
  rw [List.all_eq_true]
  rw [constOn_eq_cOnP]
  constructor
  · intro h
    have hh := (cOnP_modify bs i hi v x hn).mp h
    exact ⟨hh.1, by intro u hu; exact beq_iff_eq.mpr (hh.2 u hu)⟩
  · intro h
    apply (cOnP_modify bs i hi v x hn).mpr
    exact ⟨h.1, by intro u hu; exact beq_iff_eq.mp (h.2 u hu)⟩

private theorem partitions_nonempty : ∀ (V : List Nat) (bs : List (List Nat)),
    bs ∈ partitions V → ∀ bl ∈ bs, bl ≠ [] := by
  intro V
  induction V with
  | nil =>
      intro bs hbs
      simp [partitions] at hbs
      subst bs
      intro bl hbl
      simp at hbl
  | cons v vs ih =>
      intro bs hbs
      simp only [partitions, List.mem_flatMap] at hbs
      rcases hbs with ⟨bls, hbls, hmem⟩
      simp only [List.mem_cons, List.mem_map] at hmem
      rcases hmem with rfl | ⟨p, hp, rfl⟩
      · intro bl hbl
        simp only [List.mem_cons] at hbl
        rcases hbl with rfl | hbl
        · simp
        · exact ih bls hbls bl hbl
      · rcases p with ⟨_, i⟩
        have hi : i < bls.length := by simpa using (List.mem_zipIdx hp).2.1
        intro bl hbl
        rcases List.mem_iff_getElem.mp hbl with ⟨j, hj, hjEq⟩
        have hjbs : j < bls.length := by simpa using hj
        rw [List.getElem_modify] at hjEq
        by_cases hij : i = j
        · rw [if_pos hij] at hjEq
          rw [← hjEq]
          simp
        · rw [if_neg hij] at hjEq
          rw [← hjEq]
          exact ih bls hbls (bls[j]) (List.getElem_mem hjbs)

private theorem perm_append_cons (b : List Nat) (v : Nat) (l : List Nat) :
    (b ++ (v :: l)).Perm (v :: (b ++ l)) := by
  induction b with
  | nil => simp
  | cons x xs ih =>
      simp only [List.cons_append]
      exact (List.Perm.cons x ih).trans (List.Perm.swap v x (xs ++ l))

private theorem modify_flatten_perm (bs : List (List Nat)) (i : Nat) (hi : i < bs.length)
    (v : Nat) : (bs.modify i (fun bl => v :: bl)).flatten.Perm (v :: bs.flatten) := by
  induction bs generalizing i with
  | nil => simp at hi
  | cons b bs ih =>
      cases i with
      | zero => simp [List.modify]
      | succ i =>
          have hi' : i < bs.length := by simpa using hi
          rw [modify_succ]
          simp only [List.flatten_cons]
          exact (List.Perm.append_left b (ih i hi')).trans (perm_append_cons b v bs.flatten)

private theorem partitions_flatten_perm : ∀ (V : List Nat) (bs : List (List Nat)),
    bs ∈ partitions V → bs.flatten.Perm V := by
  intro V
  induction V with
  | nil =>
      intro bs hbs
      simp [partitions] at hbs
      simpa [hbs]
  | cons v vs ih =>
      intro bs hbs
      simp only [partitions, List.mem_flatMap] at hbs
      rcases hbs with ⟨bls, hbls, hmem⟩
      simp only [List.mem_cons, List.mem_map] at hmem
      rcases hmem with rfl | ⟨p, hp, rfl⟩
      · simpa using (List.Perm.cons v (ih bls hbls))
      · rcases p with ⟨_, i⟩
        have hi : i < bls.length := by simpa using (List.mem_zipIdx hp).2.1
        exact (modify_flatten_perm bls i hi v).trans (List.Perm.cons v (ih bls hbls))

private theorem countP_of_all_eq (l : List Nat) (x : Nat → Nat) (c : Nat)
    (h : ∀ u ∈ l, x u = c) : l.countP (fun u => x u == c) = l.length := by
  induction l with
  | nil => simp
  | cons a l ih =>
      have ha : x a = c := h a (by simp)
      have ht : ∀ u ∈ l, x u = c := by intro u hu; exact h u (by simp [hu])
      simp [ha, ih ht]

private theorem sum_block_count (bs : List (List Nat)) (x : Nat → Nat) (v : Nat)
    (hc : constOn bs x = true) :
    (bs.map (fun bl => if x v == x (headOf bl) then (bl.length : Int) else 0)).sum =
      (bs.flatten.countP (fun u => x u == x v) : Int) := by
  have hcp : cOnP bs x := (constOn_eq_cOnP bs x).mp hc
  induction bs with
  | nil => simp
  | cons b bs ih =>
      have hcb : ∀ u ∈ b, x u = x (headOf b) := by
        intro u hu
        exact hcp b (by simp) u hu
      have htailP : cOnP bs x := by
        intro bl hbl u hu
        exact hcp bl (by simp [hbl]) u hu
      have htail : constOn bs x = true := (constOn_eq_cOnP bs x).mpr htailP
      simp only [List.map_cons, List.sum_cons, List.flatten_cons]
      rw [ih htail htailP]
      by_cases h : x v == x (headOf b)
      · have hvh : x (headOf b) = x v := (beq_iff_eq.mp h).symm
        rw [if_pos h]
        have hcnt : b.countP (fun u => x u == x v) = b.length := by
          apply countP_of_all_eq
          intro u hu
          exact (hcb u hu).trans hvh
        simp [hcnt, Int.natCast_add]
      · rw [if_neg h]
        have hcnt : b.countP (fun u => x u == x v) = 0 := by
          rw [List.countP_eq_zero.mpr]
          intro u hu heq
          have huv : x u = x v := beq_iff_eq.mp heq
          have hub : x u = x (headOf b) := hcb u hu
          apply h
          simp [← hub, huv]
        simp [hcnt]

private theorem countP_le_one_of_map_nodup (l : List Nat) (v : Nat) (x : Nat → Nat)
    (h : (l.map x).Nodup) : l.countP (fun u => x u == x v) ≤ 1 := by
  induction l with
  | nil => simp
  | cons a l ih =>
      have ht : (l.map x).Nodup := (List.nodup_cons.mp h).2
      have hi := ih ht
      have hnot : x a ∉ l.map x := (List.nodup_cons.mp h).1
      simp only [List.map_cons, List.countP_cons]
      by_cases ha : x a == x v
      · have hzero : l.countP (fun u => x u == x v) = 0 := by
          rw [List.countP_eq_zero.mpr]
          intro u hu heq
          have huv : x u = x v := beq_iff_eq.mp heq
          have hva : x a = x v := beq_iff_eq.mp ha
          have : x u ∈ l.map x := List.mem_map.mpr ⟨u, hu, rfl⟩
          exact hnot (by simpa [huv, hva] using this)
        simp [ha, hzero]
      · simp [ha]
        omega

private theorem countP_pos_mem (l : List Nat) (v : Nat) (x : Nat → Nat)
    (h : 0 < l.countP (fun u => x u == x v)) : x v ∈ l.map x := by
  rcases List.countP_pos_iff.mp h with ⟨u, hu, huv⟩
  apply List.mem_map.mpr
  refine ⟨u, hu, ?_⟩
  exact beq_iff_eq.mp huv

private theorem zipIdx_fst (bs : List (List Nat)) : bs.zipIdx.map (fun p => p.1) = bs := by
  induction bs with
  | nil => simp
  | cons b bs ih => simp [List.zipIdx_cons, ih]

private theorem sum_zipIdx_fst (bs : List (List Nat)) (g : List Nat → Int) :
    (bs.zipIdx.map (fun p => g p.1)).sum = (bs.map g).sum := by
  induction bs with
  | nil => simp
  | cons b bs ih =>
      rw [List.zipIdx_cons]
      simp only [List.map_cons, List.sum_cons]
      rw [List.zipIdx_succ]
      simp only [List.map_map]
      change g b + (bs.zipIdx.map (fun p => g p.1)).sum = g b + (bs.map g).sum
      rw [ih]

private theorem mobius_point_gen (V : List Nat) (hV : V.Nodup) (x : Bind) :
    ((partitions V).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
      (if (V.map x).Nodup then (1 : Int) else 0) := by
  induction V with
  | nil => simp [partitions, mobius, constOn]
  | cons v vs ih =>
      have hvs : vs.Nodup := (List.nodup_cons.mp hV).2
      have ih' := ih hvs
      let k : Int := (vs.countP (fun u => x u == x v) : Int)
      have hmod (bs : List (List Nat)) (hn : ∀ bl ∈ bs, bl ≠ [])
          (i : Nat) (hi : i < bs.length) :
          mobius (bs.modify i (fun bl => v :: bl)) *
              (if constOn (bs.modify i (fun bl => v :: bl)) x then (1 : Int) else 0) =
            mobius bs * (if constOn bs x then
              (if x v == x (headOf (bs.get ⟨i, hi⟩)) then
                -((bs.get ⟨i, hi⟩).length : Int) else 0) else 0) := by
        rw [mobius_modify bs i hi v hn, constOn_modify bs i hi v x hn]
        by_cases hc : constOn bs x = true
        · have hcp : cOnP bs x := (constOn_eq_cOnP bs x).mp hc
          have hblock : bs.get ⟨i, hi⟩ ≠ [] := hn _ (List.getElem_mem hi)
          have hheadmem : headOf (bs.get ⟨i, hi⟩) ∈ bs.get ⟨i, hi⟩ := by
            cases hblk : bs.get ⟨i, hi⟩ with
            | nil =>
                rw [hblk] at hblock
                exact False.elim (hblock rfl)
            | cons a as => simp [headOf]
          by_cases hhd : x v == x (headOf (bs.get ⟨i, hi⟩))
          · have hall : (bs.get ⟨i, hi⟩).all (fun u => x u == x v) = true := by
              apply List.all_eq_true.mpr
              intro u hu
              have huEq := hcp (bs.get ⟨i, hi⟩) (List.getElem_mem hi) u hu
              have hhv : x (headOf (bs.get ⟨i, hi⟩)) = x v := (beq_iff_eq.mp hhd).symm
              exact beq_iff_eq.mpr (huEq.trans hhv)
            rw [if_pos hc, if_pos hhd]
            have hand : (constOn bs x && (bs.get ⟨i, hi⟩).all (fun u => x u == x v)) = true := by
              rw [hc, hall]
              rfl
            rw [if_pos hand]
            grind
          · have hallfalse : (bs.get ⟨i, hi⟩).all (fun u => x u == x v) = false := by
              cases hall' : (bs.get ⟨i, hi⟩).all (fun u => x u == x v)
              · rfl
              · have hhv' := List.all_eq_true.mp hall' (headOf (bs.get ⟨i, hi⟩)) hheadmem
                have hhv : x v = x (headOf (bs.get ⟨i, hi⟩)) := (beq_iff_eq.mp hhv').symm
                exact False.elim (hhd (by simp [hhv]))
            rw [if_pos hc, if_neg hhd]
            have hand : (constOn bs x && (bs.get ⟨i, hi⟩).all (fun u => x u == x v)) = false := by
              rw [hc, hallfalse]
              rfl
            have handnot : ¬ (constOn bs x && (bs.get ⟨i, hi⟩).all (fun u => x u == x v)) = true := by
              rw [hand]
              decide
            rw [if_neg handnot]
            grind
        · rw [if_neg hc]
          have handnot : ¬ (constOn bs x && (bs.get ⟨i, hi⟩).all (fun u => x u == x v)) = true := by
            intro hh
            rw [Bool.and_eq_true] at hh
            exact hc hh.1
          rw [if_neg handnot]
          grind
      have hterm (bs : List (List Nat)) (hbs : bs ∈ partitions vs) :
          ((([v] :: bs) :: bs.zipIdx.map (fun (_, i) => bs.modify i (fun bl => v :: bl))).map
              (fun bs' => mobius bs' * (if constOn bs' x then (1 : Int) else 0))).sum =
            (mobius bs * (if constOn bs x then (1 : Int) else 0)) * (1 - k) := by
        have hne := partitions_nonempty vs bs hbs
        simp only [List.map_cons, List.sum_cons]
        rw [mobius_singleton, constOn_singleton]
        rw [show (List.map (fun bs' => mobius bs' * (if constOn bs' x then (1 : Int) else 0))
                  (List.map (fun p => bs.modify p.2 (fun bl => v :: bl)) bs.zipIdx)).sum =
              (bs.zipIdx.map (fun p => mobius (bs.modify p.2 (fun bl => v :: bl)) *
                (if constOn (bs.modify p.2 (fun bl => v :: bl)) x then (1 : Int) else 0))).sum by
              rw [List.map_map]
              rfl]
        rw [show (bs.zipIdx.map (fun p => mobius (bs.modify p.2 (fun bl => v :: bl)) *
                (if constOn (bs.modify p.2 (fun bl => v :: bl)) x then (1 : Int) else 0))) =
              (bs.zipIdx.map (fun p => mobius bs *
                (if constOn bs x then
                  (if x v == x (headOf p.1) then -(p.1.length : Int) else 0) else 0))) by
              apply List.map_congr_left
              intro p hp
              have hi : p.2 < bs.length := by simpa using (List.mem_zipIdx hp).2.1
              have hpval : p.1 = bs.get ⟨p.2, hi⟩ := by simpa using (List.mem_zipIdx hp).2.2
              rw [hmod bs hne p.2 hi]
              rw [← hpval]]
        rw [show (bs.zipIdx.map (fun p => mobius bs *
                (if constOn bs x then
                  (if x v == x (headOf p.1) then -(p.1.length : Int) else 0) else 0))).sum =
              (bs.map (fun bl => mobius bs *
                (if constOn bs x then
                  (if x v == x (headOf bl) then -(bl.length : Int) else 0) else 0))).sum by
            exact sum_zipIdx_fst bs (fun bl => mobius bs *
              (if constOn bs x then
                (if x v == x (headOf bl) then -(bl.length : Int) else 0) else 0))]
        by_cases hc : constOn bs x = true
        · rw [if_pos hc]
          rw [show (bs.map (fun bl => mobius bs *
                (if constOn bs x then
                  (if x v == x (headOf bl) then -(bl.length : Int) else 0) else 0))).sum =
                (bs.map (fun bl => mobius bs *
                  (if x v == x (headOf bl) then -(bl.length : Int) else 0))).sum by
              apply congrArg List.sum
              apply List.map_congr_left
              intro bl hbl
              rw [if_pos hc]]
          rw [show (bs.map (fun bl => mobius bs *
                (if x v == x (headOf bl) then -(bl.length : Int) else 0))).sum =
                (-mobius bs) *
                  (bs.map (fun bl => if x v == x (headOf bl) then (bl.length : Int) else 0)).sum by
              rw [show (fun bl => mobius bs *
                    (if x v == x (headOf bl) then -(bl.length : Int) else 0)) =
                    (fun bl => (-mobius bs) *
                      (if x v == x (headOf bl) then (bl.length : Int) else 0)) by
                    funext bl
                    by_cases hhd : x v == x (headOf bl) <;> simp [hhd] <;> grind]
              rw [sum_mul_left]]
          rw [sum_block_count bs x v hc]
          have hperm := partitions_flatten_perm vs bs hbs
          have hcperm := List.Perm.countP_eq (fun u => x u == x v) hperm
          dsimp [k]
          rw [hcperm]
          grind
        · rw [if_neg hc]
          have hzero : (bs.map (fun bl => mobius bs *
                (if constOn bs x then
                  (if x v == x (headOf bl) then -(bl.length : Int) else 0) else 0))).sum = 0 := by
            rw [show (fun bl => mobius bs *
                  (if constOn bs x then
                    (if x v == x (headOf bl) then -(bl.length : Int) else 0) else 0)) =
                  (fun _ => (0 : Int)) by
                  funext bl
                  rw [if_neg hc]
                  simp]
            exact sum_zero bs
          rw [hzero]
          grind
      rw [show partitions (v :: vs) =
          (partitions vs).flatMap (fun bs =>
            ([v] :: bs) :: bs.zipIdx.map (fun (_, i) => bs.modify i (fun bl => v :: bl))) by rfl]
      rw [sum_flatMap]
      rw [show ((partitions vs).map (fun bs =>
            ((([v] :: bs) :: bs.zipIdx.map (fun (_, i) => bs.modify i (fun bl => v :: bl))).map
              (fun bs' => mobius bs' * (if constOn bs' x then (1 : Int) else 0))).sum)).sum =
            ((partitions vs).map (fun bs =>
              (mobius bs * (if constOn bs x then (1 : Int) else 0)) * (1 - k))).sum by
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          exact hterm bs hbs]
      rw [sum_mul_right]
      rw [ih']
      by_cases hN : (vs.map x).Nodup
      · have hk_le : vs.countP (fun u => x u == x v) ≤ 1 := countP_le_one_of_map_nodup vs v x hN
        have hk01 : vs.countP (fun u => x u == x v) = 0 ∨ vs.countP (fun u => x u == x v) = 1 := by omega
        rcases hk01 with hk0 | hk1
        · have hnot : x v ∉ vs.map x := by
            intro hm
            rcases List.mem_map.mp hm with ⟨u, hu, huv⟩
            have heq : x u == x v := by simp [huv]
            exact (List.countP_eq_zero.mp hk0) u hu heq
          have hnot' : ∀ u ∈ vs, x u ≠ x v := by
            intro u hu heq
            exact hnot (List.mem_map.mpr ⟨u, hu, heq⟩)
          have hnodup : ((v :: vs).map x).Nodup := by
            simp only [List.map_cons, List.nodup_cons]
            exact ⟨hnot, hN⟩
          rw [if_pos hN, if_pos hnodup]
          simp [k, hk0]
        · have hpos : 0 < vs.countP (fun u => x u == x v) := by omega
          have hmem := countP_pos_mem vs v x hpos
          have hnotnodup : ¬ ((v :: vs).map x).Nodup := by
            intro hn
            have hnot : x v ∉ vs.map x := (List.nodup_cons.mp (by simpa using hn)).1
            exact hnot hmem
          rcases List.mem_map.mp hmem with ⟨u, hu, huv⟩
          rw [if_pos hN, if_neg hnotnodup]
          simp [k, hk1]
      · have hnotnodup : ¬ ((v :: vs).map x).Nodup := by
          intro hn
          exact hN (List.nodup_cons.mp (by simpa using hn)).2
        rw [if_neg hN, if_neg hnotnodup]
        simp

private theorem mobius_point (x : Bind) :
    ((partitions pattern.freeVars).map (fun bs =>
      mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
    (if (pattern.freeVars.map x).Nodup then (1 : Int) else 0) := by
  exact mobius_point_gen pattern.freeVars (by native_decide) x
theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  let V := pattern.freeVars
  let F : Bind → Int := fun x => (pattern.atoms.map (fun a => value db a x)).prod
  have hF : ∀ x y, (∀ v ∈ V, x v = y v) → F x = F y := by
    intro x y hxy
    dsimp only [F, V]
    simp [pattern, value]
    rw [hxy 0 (by native_decide), hxy 2 (by native_decide),
      hxy 1 (by native_decide), hxy 3 (by native_decide),
      hxy 4 (by native_decide), hxy 5 (by native_decide)]
  have hcoll : ∀ bs ∈ partitions V,
      collapsed pattern n db b bs =
        integrate n V b (fun x => if constOn bs x then F x else 0) := by
    intro bs hbs
    unfold collapsed
    change integrate n (Heads bs) b (fun y => F (fun v => y (Root bs v))) = _
    exact collapse_sum n b F bs hbs hF
  change integrate n V b (fun x => if (V.map x).Nodup then F x else 0) =
    ((partitions V).map (fun bs => mobius bs * collapsed pattern n db b bs)).sum
  calc
    integrate n V b (fun x => if (V.map x).Nodup then F x else 0)
        = ((bindings n V b).map (fun x => if (V.map x).Nodup then F x else 0)).sum :=
            bindings_sum n V b _
    _ = ((bindings n V b).map (fun x => F x *
          ((partitions V).map (fun bs => mobius bs *
            (if constOn bs x then (1 : Int) else 0))).sum)).sum := by
          apply sum_congr
          intro x hx
          rw [mobius_point x]
          by_cases hN : (V.map x).Nodup <;> simp [hN, V]
    _ = ((bindings n V b).map (fun x => ((partitions V).map (fun bs =>
          mobius bs * (if constOn bs x then F x else 0))).sum)).sum := by
          apply sum_congr
          intro x hx
          rw [show (fun bs => mobius bs * (if constOn bs x then F x else 0)) =
              (fun bs => (mobius bs * (if constOn bs x then (1 : Int) else 0)) * F x) by
            funext bs
            by_cases h : constOn bs x <;> simp [h]]
          rw [sum_mul_right]
          ac_rfl
    _ = ((partitions V).map (fun bs => mobius bs *
          ((bindings n V b).map (fun x => if constOn bs x then F x else 0)).sum)).sum := by
          rw [show (fun x => ((partitions V).map (fun bs =>
              mobius bs * (if constOn bs x then F x else 0))).sum) =
              (fun x => ((partitions V).map (fun bs =>
                (fun x => mobius bs * (if constOn bs x then F x else 0)) x)).sum) by rfl]
          rw [sum_swap]
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          rw [sum_mul_left]
    _ = ((partitions V).map (fun bs => mobius bs *
          integrate n V b (fun x => if constOn bs x then F x else 0))).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          rw [bindings_sum]
    _ = ((partitions V).map (fun bs => mobius bs * collapsed pattern n db b bs)).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          rw [hcoll bs hbs]


end TinylambOpt.ResearchProofs.Atlas.JoinPlans.HubWithChordsCollisionCorrection
