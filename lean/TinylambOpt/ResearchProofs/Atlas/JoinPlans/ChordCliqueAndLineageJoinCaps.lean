import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.HubWithChordsFractionalCap
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.SupplierCliqueFractionalCap
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.SupplierCliqueCollisionAndOrbit
import TinylambOpt.ResearchProofs.Atlas.Goal_IntersectingCycles_FractionalCap
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.ParallelLineageDeltaForecast
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.ParallelLineageCapsAndCollisionCorrection

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved (10 claims, including the following)

- `HubWithChords.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
- `HubWithChords.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
- `IntersectingCycles.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.ChordCliqueAndLineageJoinCaps

namespace Model_hub
open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.HubWithChords

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



private theorem sum_map_congr_int {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem sum_zero_int {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with | nil => rfl | cons a l ih => simp [ih]

private theorem sum_add_int {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with | nil => simp | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

private theorem sum_mul_left_int {α} (l : List α) (c : Int) (f : α → Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with | nil => simp | cons a l ih => simp [ih, Int.mul_add]

private theorem integrate_congr (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ x, f x = g x) : integrate n vs b f = integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum
      apply sum_map_congr_int
      intro x hx
      exact ih (put b v x)

private theorem integrate_add (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x + g x) = integrate n vs b f + integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_add_int]

private theorem integrate_mul_left (n : Nat) (vs : List Nat) (b : Bind) (c : Int) (f : Bind → Int) :
    integrate n vs b (fun x => c * f x) = c * integrate n vs b f := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_mul_left_int]

private theorem integrate_zero (n : Nat) (vs : List Nat) (b : Bind) :
    integrate n vs b (fun _ => (0 : Int)) = 0 := by
  induction vs generalizing b with
  | nil => rfl
  | cons v vs ih => simp [integrate, ih, sum_zero_int]

private theorem integrate_list_sum {α} (n : Nat) (vs : List Nat) (b : Bind)
    (l : List α) (F : α → Bind → Int) :
    integrate n vs b (fun x => (l.map (fun a => F a x)).sum) =
      (l.map (fun a => integrate n vs b (F a))).sum := by
  induction l with
  | nil => simp [integrate_zero]
  | cons a l ih => simp only [List.map_cons, List.sum_cons, integrate_add, ih]

private theorem all_congr {α} (l : List α) (f g : α → Bool)
    (h : ∀ a ∈ l, f a = g a) : l.all f = l.all g := by
  induction l with
  | nil => rfl
  | cons a l ih => simp only [List.all_cons]; rw [h a (by simp), ih (fun b hb => h b (by simp [hb]))]

private theorem mem_modify_prepend (bs : List (List Nat)) (i : Nat) (v : Nat) :
    ∀ bl ∈ bs.modify i (fun a => v :: a), ∀ w ∈ bl, w = v ∨ ∃ b ∈ bs, w ∈ b := by
  induction bs generalizing i with
  | nil => simp
  | cons a bs ih =>
      cases i with
      | zero =>
          intro bl hbl w hw
          simp only [List.modify] at hbl
          rcases List.mem_cons.mp hbl with hEq | hbl
          · subst bl
            rcases List.mem_cons.mp hw with rfl | hw
            · exact Or.inl rfl
            · exact Or.inr ⟨a, by simp, hw⟩
          · exact Or.inr ⟨bl, by simp [hbl], hw⟩
      | succ i =>
          intro bl hbl w hw
          have hbl' : bl = a ∨ bl ∈ bs.modify i (fun x => v :: x) := by
            simpa [List.modify] using hbl
          rcases hbl' with hEq | hbl'
          · subst bl
            exact Or.inr ⟨a, by simp, hw⟩
          · rcases ih i bl hbl' w hw with rfl | ⟨b, hb, hwb⟩
            · exact Or.inl rfl
            · exact Or.inr ⟨b, by simp [hb], hwb⟩

private theorem part_mem_V (V : List Nat) (bs : List (List Nat)) (h : bs ∈ partitions V) :
    ∀ bl ∈ bs, ∀ w ∈ bl, w ∈ V := by
  induction V generalizing bs with
  | nil => simp [partitions] at h; subst bs; simp
  | cons v vs ih =>
      simp only [partitions, List.mem_flatMap] at h
      rcases h with ⟨blocks, hblocks, hmem⟩
      rcases List.mem_cons.mp hmem with hmem | hmem
      · subst bs
        intro bl hbl w hw
        rcases List.mem_cons.mp hbl with hEq | hbl
        · subst bl
          rcases List.mem_cons.mp hw with hEq | hw
          · subst w; simp
          · simp at hw
        · exact List.mem_cons_of_mem v (ih blocks hblocks bl hbl w hw)
      · rcases List.mem_map.mp hmem with ⟨p, hp, rfl⟩
        rcases p with ⟨a, i⟩
        intro bl hbl w hw
        rcases mem_modify_prepend blocks i v bl hbl w hw with hEq | ⟨b, hb, hwb⟩
        · subst w; simp
        · exact List.mem_cons_of_mem v (ih blocks hblocks b hb w hwb)

private theorem head_mem_V (V : List Nat) (bs : List (List Nat)) (h : bs ∈ partitions V)
    (bl : List Nat) (hbl : bl ∈ bs) (hne : bl ≠ []) : headOf bl ∈ V := by
  cases bl with
  | nil => simp at hne
  | cons b bl => simpa [headOf] using part_mem_V V bs h (b :: bl) hbl b (by simp)

private theorem constOn_eq_of_eqpattern (V : List Nat) (bs : List (List Nat))
    (hbs : bs ∈ partitions V) (x y : Bind)
    (hxy : ∀ v ∈ V, ∀ w ∈ V, (x v == x w) = (y v == y w)) :
    constOn bs x = constOn bs y := by
  unfold constOn
  apply all_congr
  intro bl hbl
  apply all_congr
  intro w hw
  have hwV : w ∈ V := part_mem_V V bs hbs bl hbl w hw
  have hbne : bl ≠ [] := by intro he; subst bl; simp at hw
  have hhV : headOf bl ∈ V := head_mem_V V bs hbs bl hbl hbne
  exact hxy w hwV (headOf bl) hhV

private def CO (V : List Nat) (x : Bind) : Int :=
  ((partitions V).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum

private theorem CO_eq_of_eqpattern (V : List Nat) (x y : Bind)
    (hxy : ∀ v ∈ V, ∀ w ∈ V, (x v == x w) = (y v == y w)) : CO V x = CO V y := by
  unfold CO
  apply sum_map_congr_int
  intro bs hbs
  rw [constOn_eq_of_eqpattern V bs hbs x y hxy]

private theorem nodup_map_eq_of_eqpattern (V : List Nat) (x y : Bind)
    (hxy : ∀ v ∈ V, ∀ w ∈ V, (x v == x w) = (y v == y w)) :
    (V.map x).Nodup ↔ (V.map y).Nodup := by
  induction V with
  | nil => simp
  | cons v vs ih =>
      simp only [List.map_cons, List.nodup_cons]
      constructor
      · intro h
        refine ⟨?_, (ih (fun a ha b hb => hxy a (by simp [ha]) b (by simp [hb]))).mp h.2⟩
        intro hy
        rcases List.mem_map.mp hy with ⟨w, hw, hweq⟩
        have hvw_y : y v == y w := by simp [hweq]
        have hvw_x : x v == x w := by simpa [hxy v (by simp) w (by simp [hw])] using hvw_y
        exact h.1 (List.mem_map.mpr ⟨w, hw, (beq_iff_eq.mp hvw_x).symm⟩)
      · intro h
        refine ⟨?_, (ih (fun a ha b hb => hxy a (by simp [ha]) b (by simp [hb]))).mpr h.2⟩
        intro hx
        rcases List.mem_map.mp hx with ⟨w, hw, hweq⟩
        have hvw_x : x v == x w := by simp [hweq]
        have hvw_y : y v == y w := by simpa [hxy v (by simp) w (by simp [hw])] using hvw_x
        exact h.1 (List.mem_map.mpr ⟨w, hw, (beq_iff_eq.mp hvw_y).symm⟩)

private theorem factor_constOn (l : List (List (List Nat))) (x : Bind) (K : Int) :
    (l.map (fun bs => mobius bs * (if constOn bs x then K else 0))).sum =
      K * (l.map (fun bs => mobius bs * (if constOn bs x then (1:Int) else 0))).sum := by
  induction l with
  | nil => simp
  | cons bs l ih =>
      by_cases hp : constOn bs x = true
      · have hif : (if constOn bs x then K else 0) = K := by simp [hp]
        have hif1 : (if constOn bs x then (1:Int) else 0) = 1 := by simp [hp]
        simp only [List.map_cons, List.sum_cons, hif, hif1]
        rw [ih, Int.mul_add]
        simp
        ac_rfl
      · have hp' : constOn bs x = false := by cases h : constOn bs x <;> simp_all
        have hif : (if constOn bs x then K else 0) = 0 := by simp [hp']
        have hif1 : (if constOn bs x then (1:Int) else 0) = 0 := by simp [hp']
        simp only [List.map_cons, List.sum_cons, hif, hif1]
        simp
        exact ih

private theorem atom_mem (a : Atom) (ha : a ∈ pattern.atoms) :
    a.src ∈ pattern.freeVars ∧ a.dst ∈ pattern.freeVars := by
  simp [pattern] at ha
  rcases ha with rfl | rfl | rfl | rfl | rfl | rfl | rfl <;> simp [pattern]


private abbrev Vhub : List Nat := [1, 2, 5, 4, 3, 0]
private abbrev xhub (a b c d e f : Nat) : Bind :=
  fun v => if v == 1 then a
  else if v == 2 then b
  else if v == 5 then c
  else if v == 4 then d
  else if v == 3 then e
  else if v == 0 then f else 0

private theorem xhub_of_mem (a b c d e f v : Nat) (hv : v ∈ Vhub) :
    xhub a b c d e f v = (match v with
      | 1 => a | 2 => b | 5 => c | 4 => d | 3 => e | 0 => f | _ => 0) := by
  simp [Vhub] at hv
  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;> simp [xhub]

private theorem coeff_hub (a b c d e f : Nat) :
    CO Vhub (xhub a b c d e f) =
      (if (Vhub.map (xhub a b c d e f)).Nodup then (1 : Int) else 0) := by
  by_cases hba : b = a
  ·
    by_cases hca : c = a
    ·
      by_cases hda : d = a
      ·
        by_cases hea : e = a
        ·
          by_cases hfa : f = a
          ·
            have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                (xhub a b c d e f v == xhub a b c d e f w) =
                (xhub 0 0 0 0 0 0 v == xhub 0 0 0 0 0 0 w) := by
              intro v hv w hw
              rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                xhub_of_mem 0 0 0 0 0 0 v hv, xhub_of_mem 0 0 0 0 0 0 w hw]
              simp [Vhub] at hv hw
              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xhub, beq_iff_eq, *] <;> omega
            rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
            have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 0 0 0 0) hpatt
            by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 0 0 0 0)).Nodup) (hn.mp hnd))
            · rw [if_neg hnd]; native_decide
          ·
            have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                (xhub a b c d e f v == xhub a b c d e f w) =
                (xhub 0 0 0 0 0 5 v == xhub 0 0 0 0 0 5 w) := by
              intro v hv w hw
              rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                xhub_of_mem 0 0 0 0 0 5 v hv, xhub_of_mem 0 0 0 0 0 5 w hw]
              simp [Vhub] at hv hw
              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xhub, beq_iff_eq, *] <;> omega
            rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
            have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 0 0 0 5) hpatt
            by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 0 0 0 5)).Nodup) (hn.mp hnd))
            · rw [if_neg hnd]; native_decide
        ·
          by_cases hfa : f = a
          ·
            have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                (xhub a b c d e f v == xhub a b c d e f w) =
                (xhub 0 0 0 0 4 0 v == xhub 0 0 0 0 4 0 w) := by
              intro v hv w hw
              rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                xhub_of_mem 0 0 0 0 4 0 v hv, xhub_of_mem 0 0 0 0 4 0 w hw]
              simp [Vhub] at hv hw
              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xhub, beq_iff_eq, *] <;> omega
            rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
            have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 0 0 4 0) hpatt
            by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 0 0 4 0)).Nodup) (hn.mp hnd))
            · rw [if_neg hnd]; native_decide
          ·
            by_cases hfe : f = e
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 0 0 0 4 4 v == xhub 0 0 0 0 4 4 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 0 0 0 4 4 v hv, xhub_of_mem 0 0 0 0 4 4 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 0 0 4 4) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 0 0 4 4)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 0 0 0 4 5 v == xhub 0 0 0 0 4 5 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 0 0 0 4 5 v hv, xhub_of_mem 0 0 0 0 4 5 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 0 0 4 5) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 0 0 4 5)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
      ·
        by_cases hea : e = a
        ·
          by_cases hfa : f = a
          ·
            have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                (xhub a b c d e f v == xhub a b c d e f w) =
                (xhub 0 0 0 3 0 0 v == xhub 0 0 0 3 0 0 w) := by
              intro v hv w hw
              rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                xhub_of_mem 0 0 0 3 0 0 v hv, xhub_of_mem 0 0 0 3 0 0 w hw]
              simp [Vhub] at hv hw
              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xhub, beq_iff_eq, *] <;> omega
            rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
            have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 0 3 0 0) hpatt
            by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 0 3 0 0)).Nodup) (hn.mp hnd))
            · rw [if_neg hnd]; native_decide
          ·
            by_cases hfd : f = d
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 0 0 3 0 3 v == xhub 0 0 0 3 0 3 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 0 0 3 0 3 v hv, xhub_of_mem 0 0 0 3 0 3 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 0 3 0 3) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 0 3 0 3)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 0 0 3 0 5 v == xhub 0 0 0 3 0 5 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 0 0 3 0 5 v hv, xhub_of_mem 0 0 0 3 0 5 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 0 3 0 5) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 0 3 0 5)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
        ·
          by_cases hed : e = d
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 0 0 3 3 0 v == xhub 0 0 0 3 3 0 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 0 0 3 3 0 v hv, xhub_of_mem 0 0 0 3 3 0 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 0 3 3 0) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 0 3 3 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfd : f = d
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 0 0 3 3 3 v == xhub 0 0 0 3 3 3 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 0 0 3 3 3 v hv, xhub_of_mem 0 0 0 3 3 3 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 0 3 3 3) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 0 3 3 3)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 0 0 3 3 5 v == xhub 0 0 0 3 3 5 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 0 0 3 3 5 v hv, xhub_of_mem 0 0 0 3 3 5 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 0 3 3 5) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 0 3 3 5)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 0 0 3 4 0 v == xhub 0 0 0 3 4 0 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 0 0 3 4 0 v hv, xhub_of_mem 0 0 0 3 4 0 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 0 3 4 0) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 0 3 4 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfd : f = d
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 0 0 3 4 3 v == xhub 0 0 0 3 4 3 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 0 0 3 4 3 v hv, xhub_of_mem 0 0 0 3 4 3 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 0 3 4 3) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 0 3 4 3)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfe : f = e
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 0 0 3 4 4 v == xhub 0 0 0 3 4 4 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 0 0 3 4 4 v hv, xhub_of_mem 0 0 0 3 4 4 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 0 3 4 4) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 0 3 4 4)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 0 0 3 4 5 v == xhub 0 0 0 3 4 5 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 0 0 3 4 5 v hv, xhub_of_mem 0 0 0 3 4 5 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 0 3 4 5) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 0 3 4 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
    ·
      by_cases hda : d = a
      ·
        by_cases hea : e = a
        ·
          by_cases hfa : f = a
          ·
            have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                (xhub a b c d e f v == xhub a b c d e f w) =
                (xhub 0 0 2 0 0 0 v == xhub 0 0 2 0 0 0 w) := by
              intro v hv w hw
              rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                xhub_of_mem 0 0 2 0 0 0 v hv, xhub_of_mem 0 0 2 0 0 0 w hw]
              simp [Vhub] at hv hw
              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xhub, beq_iff_eq, *] <;> omega
            rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
            have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 0 0 0) hpatt
            by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 0 0 0)).Nodup) (hn.mp hnd))
            · rw [if_neg hnd]; native_decide
          ·
            by_cases hfc : f = c
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 0 2 0 0 2 v == xhub 0 0 2 0 0 2 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 0 2 0 0 2 v hv, xhub_of_mem 0 0 2 0 0 2 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 0 0 2) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 0 0 2)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 0 2 0 0 5 v == xhub 0 0 2 0 0 5 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 0 2 0 0 5 v hv, xhub_of_mem 0 0 2 0 0 5 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 0 0 5) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 0 0 5)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
        ·
          by_cases hec : e = c
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 0 2 0 2 0 v == xhub 0 0 2 0 2 0 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 0 2 0 2 0 v hv, xhub_of_mem 0 0 2 0 2 0 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 0 2 0) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 0 2 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfc : f = c
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 0 2 0 2 2 v == xhub 0 0 2 0 2 2 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 0 2 0 2 2 v hv, xhub_of_mem 0 0 2 0 2 2 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 0 2 2) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 0 2 2)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 0 2 0 2 5 v == xhub 0 0 2 0 2 5 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 0 2 0 2 5 v hv, xhub_of_mem 0 0 2 0 2 5 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 0 2 5) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 0 2 5)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 0 2 0 4 0 v == xhub 0 0 2 0 4 0 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 0 2 0 4 0 v hv, xhub_of_mem 0 0 2 0 4 0 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 0 4 0) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 0 4 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfc : f = c
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 0 2 0 4 2 v == xhub 0 0 2 0 4 2 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 0 2 0 4 2 v hv, xhub_of_mem 0 0 2 0 4 2 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 0 4 2) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 0 4 2)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfe : f = e
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 0 2 0 4 4 v == xhub 0 0 2 0 4 4 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 0 2 0 4 4 v hv, xhub_of_mem 0 0 2 0 4 4 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 0 4 4) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 0 4 4)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 0 2 0 4 5 v == xhub 0 0 2 0 4 5 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 0 2 0 4 5 v hv, xhub_of_mem 0 0 2 0 4 5 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 0 4 5) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 0 4 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
      ·
        by_cases hdc : d = c
        ·
          by_cases hea : e = a
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 0 2 2 0 0 v == xhub 0 0 2 2 0 0 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 0 2 2 0 0 v hv, xhub_of_mem 0 0 2 2 0 0 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 2 0 0) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 2 0 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfc : f = c
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 0 2 2 0 2 v == xhub 0 0 2 2 0 2 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 0 2 2 0 2 v hv, xhub_of_mem 0 0 2 2 0 2 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 2 0 2) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 2 0 2)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 0 2 2 0 5 v == xhub 0 0 2 2 0 5 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 0 2 2 0 5 v hv, xhub_of_mem 0 0 2 2 0 5 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 2 0 5) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 2 0 5)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
          ·
            by_cases hec : e = c
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 0 2 2 2 0 v == xhub 0 0 2 2 2 0 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 0 2 2 2 0 v hv, xhub_of_mem 0 0 2 2 2 0 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 2 2 0) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 2 2 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfc : f = c
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 0 2 2 2 2 v == xhub 0 0 2 2 2 2 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 0 2 2 2 2 v hv, xhub_of_mem 0 0 2 2 2 2 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 2 2 2) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 2 2 2)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 0 2 2 2 5 v == xhub 0 0 2 2 2 5 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 0 2 2 2 5 v hv, xhub_of_mem 0 0 2 2 2 5 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 2 2 5) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 2 2 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 0 2 2 4 0 v == xhub 0 0 2 2 4 0 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 0 2 2 4 0 v hv, xhub_of_mem 0 0 2 2 4 0 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 2 4 0) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 2 4 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfc : f = c
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 0 2 2 4 2 v == xhub 0 0 2 2 4 2 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 0 2 2 4 2 v hv, xhub_of_mem 0 0 2 2 4 2 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 2 4 2) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 2 4 2)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfe : f = e
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 0 2 2 4 4 v == xhub 0 0 2 2 4 4 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 0 2 2 4 4 v hv, xhub_of_mem 0 0 2 2 4 4 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 2 4 4) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 2 4 4)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 0 2 2 4 5 v == xhub 0 0 2 2 4 5 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 0 2 2 4 5 v hv, xhub_of_mem 0 0 2 2 4 5 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 2 4 5) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 2 4 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
        ·
          by_cases hea : e = a
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 0 2 3 0 0 v == xhub 0 0 2 3 0 0 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 0 2 3 0 0 v hv, xhub_of_mem 0 0 2 3 0 0 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 3 0 0) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 3 0 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfc : f = c
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 0 2 3 0 2 v == xhub 0 0 2 3 0 2 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 0 2 3 0 2 v hv, xhub_of_mem 0 0 2 3 0 2 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 3 0 2) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 3 0 2)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfd : f = d
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 0 2 3 0 3 v == xhub 0 0 2 3 0 3 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 0 2 3 0 3 v hv, xhub_of_mem 0 0 2 3 0 3 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 3 0 3) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 3 0 3)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 0 2 3 0 5 v == xhub 0 0 2 3 0 5 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 0 2 3 0 5 v hv, xhub_of_mem 0 0 2 3 0 5 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 3 0 5) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 3 0 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
          ·
            by_cases hec : e = c
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 0 2 3 2 0 v == xhub 0 0 2 3 2 0 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 0 2 3 2 0 v hv, xhub_of_mem 0 0 2 3 2 0 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 3 2 0) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 3 2 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfc : f = c
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 0 2 3 2 2 v == xhub 0 0 2 3 2 2 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 0 2 3 2 2 v hv, xhub_of_mem 0 0 2 3 2 2 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 3 2 2) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 3 2 2)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfd : f = d
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 0 2 3 2 3 v == xhub 0 0 2 3 2 3 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 0 2 3 2 3 v hv, xhub_of_mem 0 0 2 3 2 3 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 3 2 3) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 3 2 3)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 0 2 3 2 5 v == xhub 0 0 2 3 2 5 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 0 2 3 2 5 v hv, xhub_of_mem 0 0 2 3 2 5 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 3 2 5) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 3 2 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
            ·
              by_cases hed : e = d
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 0 2 3 3 0 v == xhub 0 0 2 3 3 0 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 0 2 3 3 0 v hv, xhub_of_mem 0 0 2 3 3 0 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 3 3 0) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 3 3 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfc : f = c
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 0 2 3 3 2 v == xhub 0 0 2 3 3 2 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 0 2 3 3 2 v hv, xhub_of_mem 0 0 2 3 3 2 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 3 3 2) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 3 3 2)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfd : f = d
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 0 2 3 3 3 v == xhub 0 0 2 3 3 3 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 0 2 3 3 3 v hv, xhub_of_mem 0 0 2 3 3 3 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 3 3 3) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 3 3 3)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 0 2 3 3 5 v == xhub 0 0 2 3 3 5 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 0 2 3 3 5 v hv, xhub_of_mem 0 0 2 3 3 5 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 3 3 5) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 3 3 5)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 0 2 3 4 0 v == xhub 0 0 2 3 4 0 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 0 2 3 4 0 v hv, xhub_of_mem 0 0 2 3 4 0 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 3 4 0) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 3 4 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfc : f = c
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 0 2 3 4 2 v == xhub 0 0 2 3 4 2 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 0 2 3 4 2 v hv, xhub_of_mem 0 0 2 3 4 2 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 3 4 2) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 3 4 2)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfd : f = d
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 0 2 3 4 3 v == xhub 0 0 2 3 4 3 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 0 2 3 4 3 v hv, xhub_of_mem 0 0 2 3 4 3 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 3 4 3) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 3 4 3)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfe : f = e
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 0 2 3 4 4 v == xhub 0 0 2 3 4 4 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 0 2 3 4 4 v hv, xhub_of_mem 0 0 2 3 4 4 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 3 4 4) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 3 4 4)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 0 2 3 4 5 v == xhub 0 0 2 3 4 5 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 0 2 3 4 5 v hv, xhub_of_mem 0 0 2 3 4 5 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 0 2 3 4 5) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 0 2 3 4 5)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
  ·
    by_cases hca : c = a
    ·
      by_cases hda : d = a
      ·
        by_cases hea : e = a
        ·
          by_cases hfa : f = a
          ·
            have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                (xhub a b c d e f v == xhub a b c d e f w) =
                (xhub 0 1 0 0 0 0 v == xhub 0 1 0 0 0 0 w) := by
              intro v hv w hw
              rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                xhub_of_mem 0 1 0 0 0 0 v hv, xhub_of_mem 0 1 0 0 0 0 w hw]
              simp [Vhub] at hv hw
              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xhub, beq_iff_eq, *] <;> omega
            rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
            have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 0 0 0) hpatt
            by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 0 0 0)).Nodup) (hn.mp hnd))
            · rw [if_neg hnd]; native_decide
          ·
            by_cases hfb : f = b
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 1 0 0 0 1 v == xhub 0 1 0 0 0 1 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 1 0 0 0 1 v hv, xhub_of_mem 0 1 0 0 0 1 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 0 0 1) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 0 0 1)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 1 0 0 0 5 v == xhub 0 1 0 0 0 5 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 1 0 0 0 5 v hv, xhub_of_mem 0 1 0 0 0 5 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 0 0 5) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 0 0 5)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
        ·
          by_cases heb : e = b
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 1 0 0 1 0 v == xhub 0 1 0 0 1 0 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 1 0 0 1 0 v hv, xhub_of_mem 0 1 0 0 1 0 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 0 1 0) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 0 1 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfb : f = b
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 0 0 1 1 v == xhub 0 1 0 0 1 1 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 0 0 1 1 v hv, xhub_of_mem 0 1 0 0 1 1 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 0 1 1) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 0 1 1)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 0 0 1 5 v == xhub 0 1 0 0 1 5 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 0 0 1 5 v hv, xhub_of_mem 0 1 0 0 1 5 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 0 1 5) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 0 1 5)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 1 0 0 4 0 v == xhub 0 1 0 0 4 0 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 1 0 0 4 0 v hv, xhub_of_mem 0 1 0 0 4 0 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 0 4 0) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 0 4 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfb : f = b
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 0 0 4 1 v == xhub 0 1 0 0 4 1 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 0 0 4 1 v hv, xhub_of_mem 0 1 0 0 4 1 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 0 4 1) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 0 4 1)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfe : f = e
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 0 0 4 4 v == xhub 0 1 0 0 4 4 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 0 0 4 4 v hv, xhub_of_mem 0 1 0 0 4 4 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 0 4 4) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 0 4 4)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 0 0 4 5 v == xhub 0 1 0 0 4 5 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 0 0 4 5 v hv, xhub_of_mem 0 1 0 0 4 5 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 0 4 5) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 0 4 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
      ·
        by_cases hdb : d = b
        ·
          by_cases hea : e = a
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 1 0 1 0 0 v == xhub 0 1 0 1 0 0 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 1 0 1 0 0 v hv, xhub_of_mem 0 1 0 1 0 0 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 1 0 0) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 1 0 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfb : f = b
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 0 1 0 1 v == xhub 0 1 0 1 0 1 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 0 1 0 1 v hv, xhub_of_mem 0 1 0 1 0 1 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 1 0 1) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 1 0 1)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 0 1 0 5 v == xhub 0 1 0 1 0 5 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 0 1 0 5 v hv, xhub_of_mem 0 1 0 1 0 5 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 1 0 5) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 1 0 5)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
          ·
            by_cases heb : e = b
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 0 1 1 0 v == xhub 0 1 0 1 1 0 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 0 1 1 0 v hv, xhub_of_mem 0 1 0 1 1 0 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 1 1 0) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 1 1 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 0 1 1 1 v == xhub 0 1 0 1 1 1 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 0 1 1 1 v hv, xhub_of_mem 0 1 0 1 1 1 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 1 1 1) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 1 1 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 0 1 1 5 v == xhub 0 1 0 1 1 5 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 0 1 1 5 v hv, xhub_of_mem 0 1 0 1 1 5 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 1 1 5) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 1 1 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 0 1 4 0 v == xhub 0 1 0 1 4 0 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 0 1 4 0 v hv, xhub_of_mem 0 1 0 1 4 0 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 1 4 0) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 1 4 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 0 1 4 1 v == xhub 0 1 0 1 4 1 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 0 1 4 1 v hv, xhub_of_mem 0 1 0 1 4 1 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 1 4 1) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 1 4 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfe : f = e
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 0 1 4 4 v == xhub 0 1 0 1 4 4 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 0 1 4 4 v hv, xhub_of_mem 0 1 0 1 4 4 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 1 4 4) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 1 4 4)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 0 1 4 5 v == xhub 0 1 0 1 4 5 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 0 1 4 5 v hv, xhub_of_mem 0 1 0 1 4 5 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 1 4 5) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 1 4 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
        ·
          by_cases hea : e = a
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 1 0 3 0 0 v == xhub 0 1 0 3 0 0 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 1 0 3 0 0 v hv, xhub_of_mem 0 1 0 3 0 0 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 3 0 0) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 3 0 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfb : f = b
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 0 3 0 1 v == xhub 0 1 0 3 0 1 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 0 3 0 1 v hv, xhub_of_mem 0 1 0 3 0 1 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 3 0 1) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 3 0 1)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfd : f = d
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 0 3 0 3 v == xhub 0 1 0 3 0 3 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 0 3 0 3 v hv, xhub_of_mem 0 1 0 3 0 3 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 3 0 3) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 3 0 3)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 0 3 0 5 v == xhub 0 1 0 3 0 5 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 0 3 0 5 v hv, xhub_of_mem 0 1 0 3 0 5 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 3 0 5) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 3 0 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
          ·
            by_cases heb : e = b
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 0 3 1 0 v == xhub 0 1 0 3 1 0 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 0 3 1 0 v hv, xhub_of_mem 0 1 0 3 1 0 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 3 1 0) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 3 1 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 0 3 1 1 v == xhub 0 1 0 3 1 1 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 0 3 1 1 v hv, xhub_of_mem 0 1 0 3 1 1 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 3 1 1) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 3 1 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfd : f = d
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 0 3 1 3 v == xhub 0 1 0 3 1 3 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 0 3 1 3 v hv, xhub_of_mem 0 1 0 3 1 3 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 3 1 3) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 3 1 3)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 0 3 1 5 v == xhub 0 1 0 3 1 5 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 0 3 1 5 v hv, xhub_of_mem 0 1 0 3 1 5 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 3 1 5) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 3 1 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
            ·
              by_cases hed : e = d
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 0 3 3 0 v == xhub 0 1 0 3 3 0 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 0 3 3 0 v hv, xhub_of_mem 0 1 0 3 3 0 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 3 3 0) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 3 3 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 0 3 3 1 v == xhub 0 1 0 3 3 1 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 0 3 3 1 v hv, xhub_of_mem 0 1 0 3 3 1 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 3 3 1) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 3 3 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfd : f = d
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 0 3 3 3 v == xhub 0 1 0 3 3 3 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 0 3 3 3 v hv, xhub_of_mem 0 1 0 3 3 3 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 3 3 3) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 3 3 3)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 0 3 3 5 v == xhub 0 1 0 3 3 5 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 0 3 3 5 v hv, xhub_of_mem 0 1 0 3 3 5 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 3 3 5) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 3 3 5)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 0 3 4 0 v == xhub 0 1 0 3 4 0 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 0 3 4 0 v hv, xhub_of_mem 0 1 0 3 4 0 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 3 4 0) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 3 4 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 0 3 4 1 v == xhub 0 1 0 3 4 1 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 0 3 4 1 v hv, xhub_of_mem 0 1 0 3 4 1 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 3 4 1) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 3 4 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfd : f = d
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 0 3 4 3 v == xhub 0 1 0 3 4 3 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 0 3 4 3 v hv, xhub_of_mem 0 1 0 3 4 3 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 3 4 3) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 3 4 3)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfe : f = e
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 0 3 4 4 v == xhub 0 1 0 3 4 4 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 0 3 4 4 v hv, xhub_of_mem 0 1 0 3 4 4 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 3 4 4) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 3 4 4)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 0 3 4 5 v == xhub 0 1 0 3 4 5 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 0 3 4 5 v hv, xhub_of_mem 0 1 0 3 4 5 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 0 3 4 5) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 0 3 4 5)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
    ·
      by_cases hcb : c = b
      ·
        by_cases hda : d = a
        ·
          by_cases hea : e = a
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 1 1 0 0 0 v == xhub 0 1 1 0 0 0 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 1 1 0 0 0 v hv, xhub_of_mem 0 1 1 0 0 0 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 0 0 0) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 0 0 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfb : f = b
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 1 0 0 1 v == xhub 0 1 1 0 0 1 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 1 0 0 1 v hv, xhub_of_mem 0 1 1 0 0 1 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 0 0 1) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 0 0 1)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 1 0 0 5 v == xhub 0 1 1 0 0 5 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 1 0 0 5 v hv, xhub_of_mem 0 1 1 0 0 5 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 0 0 5) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 0 0 5)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
          ·
            by_cases heb : e = b
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 1 0 1 0 v == xhub 0 1 1 0 1 0 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 1 0 1 0 v hv, xhub_of_mem 0 1 1 0 1 0 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 0 1 0) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 0 1 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 1 0 1 1 v == xhub 0 1 1 0 1 1 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 1 0 1 1 v hv, xhub_of_mem 0 1 1 0 1 1 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 0 1 1) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 0 1 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 1 0 1 5 v == xhub 0 1 1 0 1 5 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 1 0 1 5 v hv, xhub_of_mem 0 1 1 0 1 5 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 0 1 5) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 0 1 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 1 0 4 0 v == xhub 0 1 1 0 4 0 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 1 0 4 0 v hv, xhub_of_mem 0 1 1 0 4 0 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 0 4 0) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 0 4 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 1 0 4 1 v == xhub 0 1 1 0 4 1 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 1 0 4 1 v hv, xhub_of_mem 0 1 1 0 4 1 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 0 4 1) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 0 4 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfe : f = e
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 1 0 4 4 v == xhub 0 1 1 0 4 4 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 1 0 4 4 v hv, xhub_of_mem 0 1 1 0 4 4 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 0 4 4) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 0 4 4)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 1 0 4 5 v == xhub 0 1 1 0 4 5 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 1 0 4 5 v hv, xhub_of_mem 0 1 1 0 4 5 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 0 4 5) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 0 4 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
        ·
          by_cases hdb : d = b
          ·
            by_cases hea : e = a
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 1 1 0 0 v == xhub 0 1 1 1 0 0 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 1 1 0 0 v hv, xhub_of_mem 0 1 1 1 0 0 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 1 0 0) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 1 0 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 1 1 0 1 v == xhub 0 1 1 1 0 1 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 1 1 0 1 v hv, xhub_of_mem 0 1 1 1 0 1 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 1 0 1) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 1 0 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 1 1 0 5 v == xhub 0 1 1 1 0 5 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 1 1 0 5 v hv, xhub_of_mem 0 1 1 1 0 5 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 1 0 5) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 1 0 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
            ·
              by_cases heb : e = b
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 1 1 1 0 v == xhub 0 1 1 1 1 0 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 1 1 1 0 v hv, xhub_of_mem 0 1 1 1 1 0 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 1 1 0) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 1 1 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 1 1 1 1 v == xhub 0 1 1 1 1 1 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 1 1 1 1 v hv, xhub_of_mem 0 1 1 1 1 1 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 1 1 1) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 1 1 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 1 1 1 5 v == xhub 0 1 1 1 1 5 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 1 1 1 5 v hv, xhub_of_mem 0 1 1 1 1 5 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 1 1 5) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 1 1 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 1 1 4 0 v == xhub 0 1 1 1 4 0 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 1 1 4 0 v hv, xhub_of_mem 0 1 1 1 4 0 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 1 4 0) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 1 4 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 1 1 4 1 v == xhub 0 1 1 1 4 1 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 1 1 4 1 v hv, xhub_of_mem 0 1 1 1 4 1 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 1 4 1) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 1 4 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfe : f = e
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 1 1 4 4 v == xhub 0 1 1 1 4 4 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 1 1 4 4 v hv, xhub_of_mem 0 1 1 1 4 4 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 1 4 4) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 1 4 4)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 1 1 4 5 v == xhub 0 1 1 1 4 5 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 1 1 4 5 v hv, xhub_of_mem 0 1 1 1 4 5 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 1 4 5) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 1 4 5)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
          ·
            by_cases hea : e = a
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 1 3 0 0 v == xhub 0 1 1 3 0 0 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 1 3 0 0 v hv, xhub_of_mem 0 1 1 3 0 0 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 3 0 0) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 3 0 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 1 3 0 1 v == xhub 0 1 1 3 0 1 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 1 3 0 1 v hv, xhub_of_mem 0 1 1 3 0 1 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 3 0 1) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 3 0 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfd : f = d
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 1 3 0 3 v == xhub 0 1 1 3 0 3 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 1 3 0 3 v hv, xhub_of_mem 0 1 1 3 0 3 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 3 0 3) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 3 0 3)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 1 3 0 5 v == xhub 0 1 1 3 0 5 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 1 3 0 5 v hv, xhub_of_mem 0 1 1 3 0 5 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 3 0 5) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 3 0 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
            ·
              by_cases heb : e = b
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 1 3 1 0 v == xhub 0 1 1 3 1 0 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 1 3 1 0 v hv, xhub_of_mem 0 1 1 3 1 0 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 3 1 0) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 3 1 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 1 3 1 1 v == xhub 0 1 1 3 1 1 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 1 3 1 1 v hv, xhub_of_mem 0 1 1 3 1 1 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 3 1 1) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 3 1 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfd : f = d
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 1 3 1 3 v == xhub 0 1 1 3 1 3 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 1 3 1 3 v hv, xhub_of_mem 0 1 1 3 1 3 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 3 1 3) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 3 1 3)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 1 3 1 5 v == xhub 0 1 1 3 1 5 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 1 3 1 5 v hv, xhub_of_mem 0 1 1 3 1 5 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 3 1 5) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 3 1 5)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
              ·
                by_cases hed : e = d
                ·
                  by_cases hfa : f = a
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 1 3 3 0 v == xhub 0 1 1 3 3 0 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 1 3 3 0 v hv, xhub_of_mem 0 1 1 3 3 0 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 3 3 0) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 3 3 0)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfb : f = b
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 1 3 3 1 v == xhub 0 1 1 3 3 1 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 1 3 3 1 v hv, xhub_of_mem 0 1 1 3 3 1 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 3 3 1) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 3 3 1)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfd : f = d
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 1 3 3 3 v == xhub 0 1 1 3 3 3 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 1 3 3 3 v hv, xhub_of_mem 0 1 1 3 3 3 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 3 3 3) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 3 3 3)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 1 3 3 5 v == xhub 0 1 1 3 3 5 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 1 3 3 5 v hv, xhub_of_mem 0 1 1 3 3 5 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 3 3 5) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 3 3 5)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfa : f = a
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 1 3 4 0 v == xhub 0 1 1 3 4 0 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 1 3 4 0 v hv, xhub_of_mem 0 1 1 3 4 0 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 3 4 0) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 3 4 0)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfb : f = b
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 1 3 4 1 v == xhub 0 1 1 3 4 1 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 1 3 4 1 v hv, xhub_of_mem 0 1 1 3 4 1 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 3 4 1) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 3 4 1)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfd : f = d
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 1 3 4 3 v == xhub 0 1 1 3 4 3 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 1 3 4 3 v hv, xhub_of_mem 0 1 1 3 4 3 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 3 4 3) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 3 4 3)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        by_cases hfe : f = e
                        ·
                          have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                              (xhub a b c d e f v == xhub a b c d e f w) =
                              (xhub 0 1 1 3 4 4 v == xhub 0 1 1 3 4 4 w) := by
                            intro v hv w hw
                            rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                              xhub_of_mem 0 1 1 3 4 4 v hv, xhub_of_mem 0 1 1 3 4 4 w hw]
                            simp [Vhub] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xhub, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 3 4 4) hpatt
                          by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 3 4 4)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                        ·
                          have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                              (xhub a b c d e f v == xhub a b c d e f w) =
                              (xhub 0 1 1 3 4 5 v == xhub 0 1 1 3 4 5 w) := by
                            intro v hv w hw
                            rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                              xhub_of_mem 0 1 1 3 4 5 v hv, xhub_of_mem 0 1 1 3 4 5 w hw]
                            simp [Vhub] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xhub, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 1 3 4 5) hpatt
                          by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 1 3 4 5)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
      ·
        by_cases hda : d = a
        ·
          by_cases hea : e = a
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                  (xhub a b c d e f v == xhub a b c d e f w) =
                  (xhub 0 1 2 0 0 0 v == xhub 0 1 2 0 0 0 w) := by
                intro v hv w hw
                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                  xhub_of_mem 0 1 2 0 0 0 v hv, xhub_of_mem 0 1 2 0 0 0 w hw]
                simp [Vhub] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xhub, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 0 0 0) hpatt
              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 0 0 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfb : f = b
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 2 0 0 1 v == xhub 0 1 2 0 0 1 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 2 0 0 1 v hv, xhub_of_mem 0 1 2 0 0 1 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 0 0 1) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 0 0 1)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfc : f = c
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 2 0 0 2 v == xhub 0 1 2 0 0 2 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 2 0 0 2 v hv, xhub_of_mem 0 1 2 0 0 2 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 0 0 2) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 0 0 2)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 2 0 0 5 v == xhub 0 1 2 0 0 5 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 2 0 0 5 v hv, xhub_of_mem 0 1 2 0 0 5 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 0 0 5) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 0 0 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
          ·
            by_cases heb : e = b
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 2 0 1 0 v == xhub 0 1 2 0 1 0 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 2 0 1 0 v hv, xhub_of_mem 0 1 2 0 1 0 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 0 1 0) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 0 1 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 2 0 1 1 v == xhub 0 1 2 0 1 1 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 2 0 1 1 v hv, xhub_of_mem 0 1 2 0 1 1 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 0 1 1) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 0 1 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfc : f = c
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 2 0 1 2 v == xhub 0 1 2 0 1 2 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 2 0 1 2 v hv, xhub_of_mem 0 1 2 0 1 2 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 0 1 2) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 0 1 2)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 2 0 1 5 v == xhub 0 1 2 0 1 5 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 2 0 1 5 v hv, xhub_of_mem 0 1 2 0 1 5 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 0 1 5) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 0 1 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
            ·
              by_cases hec : e = c
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 2 0 2 0 v == xhub 0 1 2 0 2 0 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 2 0 2 0 v hv, xhub_of_mem 0 1 2 0 2 0 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 0 2 0) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 0 2 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 2 0 2 1 v == xhub 0 1 2 0 2 1 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 2 0 2 1 v hv, xhub_of_mem 0 1 2 0 2 1 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 0 2 1) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 0 2 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfc : f = c
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 2 0 2 2 v == xhub 0 1 2 0 2 2 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 2 0 2 2 v hv, xhub_of_mem 0 1 2 0 2 2 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 0 2 2) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 0 2 2)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 2 0 2 5 v == xhub 0 1 2 0 2 5 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 2 0 2 5 v hv, xhub_of_mem 0 1 2 0 2 5 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 0 2 5) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 0 2 5)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 2 0 4 0 v == xhub 0 1 2 0 4 0 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 2 0 4 0 v hv, xhub_of_mem 0 1 2 0 4 0 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 0 4 0) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 0 4 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 2 0 4 1 v == xhub 0 1 2 0 4 1 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 2 0 4 1 v hv, xhub_of_mem 0 1 2 0 4 1 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 0 4 1) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 0 4 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfc : f = c
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 2 0 4 2 v == xhub 0 1 2 0 4 2 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 2 0 4 2 v hv, xhub_of_mem 0 1 2 0 4 2 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 0 4 2) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 0 4 2)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfe : f = e
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 2 0 4 4 v == xhub 0 1 2 0 4 4 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 2 0 4 4 v hv, xhub_of_mem 0 1 2 0 4 4 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 0 4 4) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 0 4 4)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 2 0 4 5 v == xhub 0 1 2 0 4 5 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 2 0 4 5 v hv, xhub_of_mem 0 1 2 0 4 5 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 0 4 5) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 0 4 5)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
        ·
          by_cases hdb : d = b
          ·
            by_cases hea : e = a
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                    (xhub a b c d e f v == xhub a b c d e f w) =
                    (xhub 0 1 2 1 0 0 v == xhub 0 1 2 1 0 0 w) := by
                  intro v hv w hw
                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                    xhub_of_mem 0 1 2 1 0 0 v hv, xhub_of_mem 0 1 2 1 0 0 w hw]
                  simp [Vhub] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xhub, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 1 0 0) hpatt
                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 1 0 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 2 1 0 1 v == xhub 0 1 2 1 0 1 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 2 1 0 1 v hv, xhub_of_mem 0 1 2 1 0 1 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 1 0 1) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 1 0 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfc : f = c
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 2 1 0 2 v == xhub 0 1 2 1 0 2 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 2 1 0 2 v hv, xhub_of_mem 0 1 2 1 0 2 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 1 0 2) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 1 0 2)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 2 1 0 5 v == xhub 0 1 2 1 0 5 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 2 1 0 5 v hv, xhub_of_mem 0 1 2 1 0 5 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 1 0 5) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 1 0 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
            ·
              by_cases heb : e = b
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 2 1 1 0 v == xhub 0 1 2 1 1 0 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 2 1 1 0 v hv, xhub_of_mem 0 1 2 1 1 0 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 1 1 0) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 1 1 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 2 1 1 1 v == xhub 0 1 2 1 1 1 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 2 1 1 1 v hv, xhub_of_mem 0 1 2 1 1 1 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 1 1 1) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 1 1 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfc : f = c
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 2 1 1 2 v == xhub 0 1 2 1 1 2 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 2 1 1 2 v hv, xhub_of_mem 0 1 2 1 1 2 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 1 1 2) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 1 1 2)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 2 1 1 5 v == xhub 0 1 2 1 1 5 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 2 1 1 5 v hv, xhub_of_mem 0 1 2 1 1 5 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 1 1 5) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 1 1 5)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
              ·
                by_cases hec : e = c
                ·
                  by_cases hfa : f = a
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 2 1 2 0 v == xhub 0 1 2 1 2 0 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 2 1 2 0 v hv, xhub_of_mem 0 1 2 1 2 0 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 1 2 0) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 1 2 0)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfb : f = b
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 2 1 2 1 v == xhub 0 1 2 1 2 1 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 2 1 2 1 v hv, xhub_of_mem 0 1 2 1 2 1 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 1 2 1) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 1 2 1)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfc : f = c
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 2 1 2 2 v == xhub 0 1 2 1 2 2 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 2 1 2 2 v hv, xhub_of_mem 0 1 2 1 2 2 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 1 2 2) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 1 2 2)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 2 1 2 5 v == xhub 0 1 2 1 2 5 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 2 1 2 5 v hv, xhub_of_mem 0 1 2 1 2 5 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 1 2 5) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 1 2 5)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfa : f = a
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 2 1 4 0 v == xhub 0 1 2 1 4 0 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 2 1 4 0 v hv, xhub_of_mem 0 1 2 1 4 0 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 1 4 0) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 1 4 0)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfb : f = b
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 2 1 4 1 v == xhub 0 1 2 1 4 1 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 2 1 4 1 v hv, xhub_of_mem 0 1 2 1 4 1 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 1 4 1) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 1 4 1)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfc : f = c
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 2 1 4 2 v == xhub 0 1 2 1 4 2 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 2 1 4 2 v hv, xhub_of_mem 0 1 2 1 4 2 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 1 4 2) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 1 4 2)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        by_cases hfe : f = e
                        ·
                          have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                              (xhub a b c d e f v == xhub a b c d e f w) =
                              (xhub 0 1 2 1 4 4 v == xhub 0 1 2 1 4 4 w) := by
                            intro v hv w hw
                            rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                              xhub_of_mem 0 1 2 1 4 4 v hv, xhub_of_mem 0 1 2 1 4 4 w hw]
                            simp [Vhub] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xhub, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 1 4 4) hpatt
                          by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 1 4 4)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                        ·
                          have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                              (xhub a b c d e f v == xhub a b c d e f w) =
                              (xhub 0 1 2 1 4 5 v == xhub 0 1 2 1 4 5 w) := by
                            intro v hv w hw
                            rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                              xhub_of_mem 0 1 2 1 4 5 v hv, xhub_of_mem 0 1 2 1 4 5 w hw]
                            simp [Vhub] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xhub, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 1 4 5) hpatt
                          by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 1 4 5)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
          ·
            by_cases hdc : d = c
            ·
              by_cases hea : e = a
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 2 2 0 0 v == xhub 0 1 2 2 0 0 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 2 2 0 0 v hv, xhub_of_mem 0 1 2 2 0 0 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 2 0 0) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 2 0 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 2 2 0 1 v == xhub 0 1 2 2 0 1 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 2 2 0 1 v hv, xhub_of_mem 0 1 2 2 0 1 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 2 0 1) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 2 0 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfc : f = c
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 2 2 0 2 v == xhub 0 1 2 2 0 2 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 2 2 0 2 v hv, xhub_of_mem 0 1 2 2 0 2 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 2 0 2) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 2 0 2)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 2 2 0 5 v == xhub 0 1 2 2 0 5 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 2 2 0 5 v hv, xhub_of_mem 0 1 2 2 0 5 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 2 0 5) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 2 0 5)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
              ·
                by_cases heb : e = b
                ·
                  by_cases hfa : f = a
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 2 2 1 0 v == xhub 0 1 2 2 1 0 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 2 2 1 0 v hv, xhub_of_mem 0 1 2 2 1 0 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 2 1 0) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 2 1 0)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfb : f = b
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 2 2 1 1 v == xhub 0 1 2 2 1 1 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 2 2 1 1 v hv, xhub_of_mem 0 1 2 2 1 1 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 2 1 1) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 2 1 1)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfc : f = c
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 2 2 1 2 v == xhub 0 1 2 2 1 2 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 2 2 1 2 v hv, xhub_of_mem 0 1 2 2 1 2 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 2 1 2) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 2 1 2)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 2 2 1 5 v == xhub 0 1 2 2 1 5 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 2 2 1 5 v hv, xhub_of_mem 0 1 2 2 1 5 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 2 1 5) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 2 1 5)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                ·
                  by_cases hec : e = c
                  ·
                    by_cases hfa : f = a
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 2 2 2 0 v == xhub 0 1 2 2 2 0 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 2 2 2 0 v hv, xhub_of_mem 0 1 2 2 2 0 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 2 2 0) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 2 2 0)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfb : f = b
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 2 2 2 1 v == xhub 0 1 2 2 2 1 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 2 2 2 1 v hv, xhub_of_mem 0 1 2 2 2 1 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 2 2 1) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 2 2 1)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        by_cases hfc : f = c
                        ·
                          have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                              (xhub a b c d e f v == xhub a b c d e f w) =
                              (xhub 0 1 2 2 2 2 v == xhub 0 1 2 2 2 2 w) := by
                            intro v hv w hw
                            rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                              xhub_of_mem 0 1 2 2 2 2 v hv, xhub_of_mem 0 1 2 2 2 2 w hw]
                            simp [Vhub] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xhub, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 2 2 2) hpatt
                          by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 2 2 2)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                        ·
                          have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                              (xhub a b c d e f v == xhub a b c d e f w) =
                              (xhub 0 1 2 2 2 5 v == xhub 0 1 2 2 2 5 w) := by
                            intro v hv w hw
                            rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                              xhub_of_mem 0 1 2 2 2 5 v hv, xhub_of_mem 0 1 2 2 2 5 w hw]
                            simp [Vhub] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xhub, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 2 2 5) hpatt
                          by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 2 2 5)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfa : f = a
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 2 2 4 0 v == xhub 0 1 2 2 4 0 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 2 2 4 0 v hv, xhub_of_mem 0 1 2 2 4 0 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 2 4 0) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 2 4 0)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfb : f = b
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 2 2 4 1 v == xhub 0 1 2 2 4 1 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 2 2 4 1 v hv, xhub_of_mem 0 1 2 2 4 1 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 2 4 1) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 2 4 1)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        by_cases hfc : f = c
                        ·
                          have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                              (xhub a b c d e f v == xhub a b c d e f w) =
                              (xhub 0 1 2 2 4 2 v == xhub 0 1 2 2 4 2 w) := by
                            intro v hv w hw
                            rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                              xhub_of_mem 0 1 2 2 4 2 v hv, xhub_of_mem 0 1 2 2 4 2 w hw]
                            simp [Vhub] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xhub, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 2 4 2) hpatt
                          by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 2 4 2)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                        ·
                          by_cases hfe : f = e
                          ·
                            have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                                (xhub a b c d e f v == xhub a b c d e f w) =
                                (xhub 0 1 2 2 4 4 v == xhub 0 1 2 2 4 4 w) := by
                              intro v hv w hw
                              rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                                xhub_of_mem 0 1 2 2 4 4 v hv, xhub_of_mem 0 1 2 2 4 4 w hw]
                              simp [Vhub] at hv hw
                              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                simp [xhub, beq_iff_eq, *] <;> omega
                            rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                            have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 2 4 4) hpatt
                            by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 2 4 4)).Nodup) (hn.mp hnd))
                            · rw [if_neg hnd]; native_decide
                          ·
                            have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                                (xhub a b c d e f v == xhub a b c d e f w) =
                                (xhub 0 1 2 2 4 5 v == xhub 0 1 2 2 4 5 w) := by
                              intro v hv w hw
                              rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                                xhub_of_mem 0 1 2 2 4 5 v hv, xhub_of_mem 0 1 2 2 4 5 w hw]
                              simp [Vhub] at hv hw
                              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                simp [xhub, beq_iff_eq, *] <;> omega
                            rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                            have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 2 4 5) hpatt
                            by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 2 4 5)).Nodup) (hn.mp hnd))
                            · rw [if_neg hnd]; native_decide
            ·
              by_cases hea : e = a
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                      (xhub a b c d e f v == xhub a b c d e f w) =
                      (xhub 0 1 2 3 0 0 v == xhub 0 1 2 3 0 0 w) := by
                    intro v hv w hw
                    rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                      xhub_of_mem 0 1 2 3 0 0 v hv, xhub_of_mem 0 1 2 3 0 0 w hw]
                    simp [Vhub] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xhub, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 0 0) hpatt
                  by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 0 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 2 3 0 1 v == xhub 0 1 2 3 0 1 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 2 3 0 1 v hv, xhub_of_mem 0 1 2 3 0 1 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 0 1) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 0 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfc : f = c
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 2 3 0 2 v == xhub 0 1 2 3 0 2 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 2 3 0 2 v hv, xhub_of_mem 0 1 2 3 0 2 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 0 2) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 0 2)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfd : f = d
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 2 3 0 3 v == xhub 0 1 2 3 0 3 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 2 3 0 3 v hv, xhub_of_mem 0 1 2 3 0 3 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 0 3) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 0 3)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 2 3 0 5 v == xhub 0 1 2 3 0 5 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 2 3 0 5 v hv, xhub_of_mem 0 1 2 3 0 5 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 0 5) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 0 5)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
              ·
                by_cases heb : e = b
                ·
                  by_cases hfa : f = a
                  ·
                    have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                        (xhub a b c d e f v == xhub a b c d e f w) =
                        (xhub 0 1 2 3 1 0 v == xhub 0 1 2 3 1 0 w) := by
                      intro v hv w hw
                      rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                        xhub_of_mem 0 1 2 3 1 0 v hv, xhub_of_mem 0 1 2 3 1 0 w hw]
                      simp [Vhub] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xhub, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 1 0) hpatt
                    by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 1 0)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfb : f = b
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 2 3 1 1 v == xhub 0 1 2 3 1 1 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 2 3 1 1 v hv, xhub_of_mem 0 1 2 3 1 1 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 1 1) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 1 1)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfc : f = c
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 2 3 1 2 v == xhub 0 1 2 3 1 2 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 2 3 1 2 v hv, xhub_of_mem 0 1 2 3 1 2 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 1 2) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 1 2)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        by_cases hfd : f = d
                        ·
                          have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                              (xhub a b c d e f v == xhub a b c d e f w) =
                              (xhub 0 1 2 3 1 3 v == xhub 0 1 2 3 1 3 w) := by
                            intro v hv w hw
                            rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                              xhub_of_mem 0 1 2 3 1 3 v hv, xhub_of_mem 0 1 2 3 1 3 w hw]
                            simp [Vhub] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xhub, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 1 3) hpatt
                          by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 1 3)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                        ·
                          have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                              (xhub a b c d e f v == xhub a b c d e f w) =
                              (xhub 0 1 2 3 1 5 v == xhub 0 1 2 3 1 5 w) := by
                            intro v hv w hw
                            rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                              xhub_of_mem 0 1 2 3 1 5 v hv, xhub_of_mem 0 1 2 3 1 5 w hw]
                            simp [Vhub] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xhub, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 1 5) hpatt
                          by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 1 5)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                ·
                  by_cases hec : e = c
                  ·
                    by_cases hfa : f = a
                    ·
                      have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                          (xhub a b c d e f v == xhub a b c d e f w) =
                          (xhub 0 1 2 3 2 0 v == xhub 0 1 2 3 2 0 w) := by
                        intro v hv w hw
                        rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                          xhub_of_mem 0 1 2 3 2 0 v hv, xhub_of_mem 0 1 2 3 2 0 w hw]
                        simp [Vhub] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xhub, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 2 0) hpatt
                      by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 2 0)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfb : f = b
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 2 3 2 1 v == xhub 0 1 2 3 2 1 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 2 3 2 1 v hv, xhub_of_mem 0 1 2 3 2 1 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 2 1) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 2 1)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        by_cases hfc : f = c
                        ·
                          have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                              (xhub a b c d e f v == xhub a b c d e f w) =
                              (xhub 0 1 2 3 2 2 v == xhub 0 1 2 3 2 2 w) := by
                            intro v hv w hw
                            rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                              xhub_of_mem 0 1 2 3 2 2 v hv, xhub_of_mem 0 1 2 3 2 2 w hw]
                            simp [Vhub] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xhub, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 2 2) hpatt
                          by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 2 2)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                        ·
                          by_cases hfd : f = d
                          ·
                            have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                                (xhub a b c d e f v == xhub a b c d e f w) =
                                (xhub 0 1 2 3 2 3 v == xhub 0 1 2 3 2 3 w) := by
                              intro v hv w hw
                              rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                                xhub_of_mem 0 1 2 3 2 3 v hv, xhub_of_mem 0 1 2 3 2 3 w hw]
                              simp [Vhub] at hv hw
                              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                simp [xhub, beq_iff_eq, *] <;> omega
                            rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                            have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 2 3) hpatt
                            by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 2 3)).Nodup) (hn.mp hnd))
                            · rw [if_neg hnd]; native_decide
                          ·
                            have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                                (xhub a b c d e f v == xhub a b c d e f w) =
                                (xhub 0 1 2 3 2 5 v == xhub 0 1 2 3 2 5 w) := by
                              intro v hv w hw
                              rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                                xhub_of_mem 0 1 2 3 2 5 v hv, xhub_of_mem 0 1 2 3 2 5 w hw]
                              simp [Vhub] at hv hw
                              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                simp [xhub, beq_iff_eq, *] <;> omega
                            rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                            have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 2 5) hpatt
                            by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 2 5)).Nodup) (hn.mp hnd))
                            · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hed : e = d
                    ·
                      by_cases hfa : f = a
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 2 3 3 0 v == xhub 0 1 2 3 3 0 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 2 3 3 0 v hv, xhub_of_mem 0 1 2 3 3 0 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 3 0) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 3 0)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        by_cases hfb : f = b
                        ·
                          have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                              (xhub a b c d e f v == xhub a b c d e f w) =
                              (xhub 0 1 2 3 3 1 v == xhub 0 1 2 3 3 1 w) := by
                            intro v hv w hw
                            rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                              xhub_of_mem 0 1 2 3 3 1 v hv, xhub_of_mem 0 1 2 3 3 1 w hw]
                            simp [Vhub] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xhub, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 3 1) hpatt
                          by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 3 1)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                        ·
                          by_cases hfc : f = c
                          ·
                            have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                                (xhub a b c d e f v == xhub a b c d e f w) =
                                (xhub 0 1 2 3 3 2 v == xhub 0 1 2 3 3 2 w) := by
                              intro v hv w hw
                              rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                                xhub_of_mem 0 1 2 3 3 2 v hv, xhub_of_mem 0 1 2 3 3 2 w hw]
                              simp [Vhub] at hv hw
                              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                simp [xhub, beq_iff_eq, *] <;> omega
                            rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                            have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 3 2) hpatt
                            by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 3 2)).Nodup) (hn.mp hnd))
                            · rw [if_neg hnd]; native_decide
                          ·
                            by_cases hfd : f = d
                            ·
                              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                                  (xhub a b c d e f v == xhub a b c d e f w) =
                                  (xhub 0 1 2 3 3 3 v == xhub 0 1 2 3 3 3 w) := by
                                intro v hv w hw
                                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                                  xhub_of_mem 0 1 2 3 3 3 v hv, xhub_of_mem 0 1 2 3 3 3 w hw]
                                simp [Vhub] at hv hw
                                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                  simp [xhub, beq_iff_eq, *] <;> omega
                              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 3 3) hpatt
                              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 3 3)).Nodup) (hn.mp hnd))
                              · rw [if_neg hnd]; native_decide
                            ·
                              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                                  (xhub a b c d e f v == xhub a b c d e f w) =
                                  (xhub 0 1 2 3 3 5 v == xhub 0 1 2 3 3 5 w) := by
                                intro v hv w hw
                                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                                  xhub_of_mem 0 1 2 3 3 5 v hv, xhub_of_mem 0 1 2 3 3 5 w hw]
                                simp [Vhub] at hv hw
                                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                  simp [xhub, beq_iff_eq, *] <;> omega
                              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 3 5) hpatt
                              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 3 5)).Nodup) (hn.mp hnd))
                              · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfa : f = a
                      ·
                        have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                            (xhub a b c d e f v == xhub a b c d e f w) =
                            (xhub 0 1 2 3 4 0 v == xhub 0 1 2 3 4 0 w) := by
                          intro v hv w hw
                          rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                            xhub_of_mem 0 1 2 3 4 0 v hv, xhub_of_mem 0 1 2 3 4 0 w hw]
                          simp [Vhub] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xhub, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 4 0) hpatt
                        by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 4 0)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        by_cases hfb : f = b
                        ·
                          have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                              (xhub a b c d e f v == xhub a b c d e f w) =
                              (xhub 0 1 2 3 4 1 v == xhub 0 1 2 3 4 1 w) := by
                            intro v hv w hw
                            rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                              xhub_of_mem 0 1 2 3 4 1 v hv, xhub_of_mem 0 1 2 3 4 1 w hw]
                            simp [Vhub] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xhub, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 4 1) hpatt
                          by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 4 1)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                        ·
                          by_cases hfc : f = c
                          ·
                            have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                                (xhub a b c d e f v == xhub a b c d e f w) =
                                (xhub 0 1 2 3 4 2 v == xhub 0 1 2 3 4 2 w) := by
                              intro v hv w hw
                              rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                                xhub_of_mem 0 1 2 3 4 2 v hv, xhub_of_mem 0 1 2 3 4 2 w hw]
                              simp [Vhub] at hv hw
                              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                simp [xhub, beq_iff_eq, *] <;> omega
                            rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                            have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 4 2) hpatt
                            by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 4 2)).Nodup) (hn.mp hnd))
                            · rw [if_neg hnd]; native_decide
                          ·
                            by_cases hfd : f = d
                            ·
                              have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                                  (xhub a b c d e f v == xhub a b c d e f w) =
                                  (xhub 0 1 2 3 4 3 v == xhub 0 1 2 3 4 3 w) := by
                                intro v hv w hw
                                rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                                  xhub_of_mem 0 1 2 3 4 3 v hv, xhub_of_mem 0 1 2 3 4 3 w hw]
                                simp [Vhub] at hv hw
                                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                  simp [xhub, beq_iff_eq, *] <;> omega
                              rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                              have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 4 3) hpatt
                              by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 4 3)).Nodup) (hn.mp hnd))
                              · rw [if_neg hnd]; native_decide
                            ·
                              by_cases hfe : f = e
                              ·
                                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                                    (xhub a b c d e f v == xhub a b c d e f w) =
                                    (xhub 0 1 2 3 4 4 v == xhub 0 1 2 3 4 4 w) := by
                                  intro v hv w hw
                                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                                    xhub_of_mem 0 1 2 3 4 4 v hv, xhub_of_mem 0 1 2 3 4 4 w hw]
                                  simp [Vhub] at hv hw
                                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                    simp [xhub, beq_iff_eq, *] <;> omega
                                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 4 4) hpatt
                                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vhub.map (xhub 0 1 2 3 4 4)).Nodup) (hn.mp hnd))
                                · rw [if_neg hnd]; native_decide
                              ·
                                have hpatt : ∀ v ∈ Vhub, ∀ w ∈ Vhub,
                                    (xhub a b c d e f v == xhub a b c d e f w) =
                                    (xhub 0 1 2 3 4 5 v == xhub 0 1 2 3 4 5 w) := by
                                  intro v hv w hw
                                  rw [xhub_of_mem a b c d e f v hv, xhub_of_mem a b c d e f w hw,
                                    xhub_of_mem 0 1 2 3 4 5 v hv, xhub_of_mem 0 1 2 3 4 5 w hw]
                                  simp [Vhub] at hv hw
                                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                    simp [xhub, beq_iff_eq, *] <;> omega
                                rw [CO_eq_of_eqpattern Vhub _ _ hpatt]
                                have hn := nodup_map_eq_of_eqpattern Vhub (xhub a b c d e f) (xhub 0 1 2 3 4 5) hpatt
                                by_cases hnd : (Vhub.map (xhub a b c d e f)).Nodup
                                · rw [if_pos hnd]; native_decide
                                · rw [if_neg hnd]; exact False.elim (hnd (hn.mpr (by native_decide : (Vhub.map (xhub 0 1 2 3 4 5)).Nodup)))


private theorem sound_CollisionCorrection_hub : CollisionCorrection.Goal := by
  intro n db b
  let F : Bind → Int := fun x => (pattern.atoms.map (fun a => value db a x)).prod
  have hF : ∀ x y, (∀ v ∈ pattern.freeVars, x v = y v) → F x = F y := by
    intro x y hxy
    dsimp [F]
    congr 1
    apply List.map_congr_left
    intro a ha
    dsimp [value]
    have haV := atom_mem a ha
    rw [hxy a.src haV.1, hxy a.dst haV.2]
  have hR : ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b bs)).sum =
      integrate n pattern.freeVars b (fun x =>
        ((partitions pattern.freeVars).map (fun bs => mobius bs * (if constOn bs x then F x else 0))).sum) := by
    rw [integrate_list_sum]
    apply sum_map_congr_int
    intro bs hbs
    rw [integrate_mul_left]
    have hcs := collapse_sum n b F bs hbs hF
    have hcoll : collapsed pattern n db b bs =
        integrate n (Heads bs) b (fun y => F (fun v => y (Root bs v))) := by rfl
    rw [hcoll]
    exact congrArg (fun z => mobius bs * z) hcs
  unfold injective
  rw [hR]
  symm
  apply integrate_congr
  intro x
  let y : Bind := xhub (x 1) (x 2) (x 5) (x 4) (x 3) (x 0)
  have hpatt : ∀ v ∈ pattern.freeVars, ∀ w ∈ pattern.freeVars, (x v == x w) = (y v == y w) := by
    intro v hv w hw
    simp [pattern] at hv hw
    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;> simp [y, xhub]
  have hco := coeff_hub (x 1) (x 2) (x 5) (x 4) (x 3) (x 0)
  have hco' : CO pattern.freeVars x = (if (pattern.freeVars.map x).Nodup then (1:Int) else 0) := by
    have hc := CO_eq_of_eqpattern pattern.freeVars x y hpatt
    have hn := nodup_map_eq_of_eqpattern pattern.freeVars x y hpatt
    rw [hc]
    change CO pattern.freeVars y = if (pattern.freeVars.map x).Nodup then (1:Int) else 0
    have hco_y : CO pattern.freeVars y = (if (pattern.freeVars.map y).Nodup then (1:Int) else 0) := by
      simpa [y, pattern] using hco
    by_cases hndx : (pattern.freeVars.map x).Nodup
    · have hndy : (pattern.freeVars.map y).Nodup := hn.mp hndx
      rw [if_pos hndx, hco_y, if_pos hndy]
    · have hndy : ¬ (pattern.freeVars.map y).Nodup := by intro h; exact hndx (hn.mpr h)
      rw [if_neg hndx, hco_y, if_neg hndy]
  change ((partitions pattern.freeVars).map (fun bs => mobius bs * (if constOn bs x then F x else 0))).sum =
    (if (pattern.freeVars.map x).Nodup then F x else 0)
  rw [factor_constOn]
  change F x * CO pattern.freeVars x = (if (pattern.freeVars.map x).Nodup then F x else 0)
  rw [hco']
  by_cases hnd : (pattern.freeVars.map x).Nodup <;> simp [hnd]

end Model_hub

namespace Model_icyc
open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.IntersectingCycles

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



private theorem sum_map_congr_int {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem sum_zero_int {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with | nil => rfl | cons a l ih => simp [ih]

private theorem sum_add_int {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with | nil => simp | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

private theorem sum_mul_left_int {α} (l : List α) (c : Int) (f : α → Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with | nil => simp | cons a l ih => simp [ih, Int.mul_add]

private theorem integrate_congr (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ x, f x = g x) : integrate n vs b f = integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum
      apply sum_map_congr_int
      intro x hx
      exact ih (put b v x)

private theorem integrate_add (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x + g x) = integrate n vs b f + integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_add_int]

private theorem integrate_mul_left (n : Nat) (vs : List Nat) (b : Bind) (c : Int) (f : Bind → Int) :
    integrate n vs b (fun x => c * f x) = c * integrate n vs b f := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_mul_left_int]

private theorem integrate_zero (n : Nat) (vs : List Nat) (b : Bind) :
    integrate n vs b (fun _ => (0 : Int)) = 0 := by
  induction vs generalizing b with
  | nil => rfl
  | cons v vs ih => simp [integrate, ih, sum_zero_int]

private theorem integrate_list_sum {α} (n : Nat) (vs : List Nat) (b : Bind)
    (l : List α) (F : α → Bind → Int) :
    integrate n vs b (fun x => (l.map (fun a => F a x)).sum) =
      (l.map (fun a => integrate n vs b (F a))).sum := by
  induction l with
  | nil => simp [integrate_zero]
  | cons a l ih => simp only [List.map_cons, List.sum_cons, integrate_add, ih]

private theorem all_congr {α} (l : List α) (f g : α → Bool)
    (h : ∀ a ∈ l, f a = g a) : l.all f = l.all g := by
  induction l with
  | nil => rfl
  | cons a l ih => simp only [List.all_cons]; rw [h a (by simp), ih (fun b hb => h b (by simp [hb]))]

private theorem mem_modify_prepend (bs : List (List Nat)) (i : Nat) (v : Nat) :
    ∀ bl ∈ bs.modify i (fun a => v :: a), ∀ w ∈ bl, w = v ∨ ∃ b ∈ bs, w ∈ b := by
  induction bs generalizing i with
  | nil => simp
  | cons a bs ih =>
      cases i with
      | zero =>
          intro bl hbl w hw
          simp only [List.modify] at hbl
          rcases List.mem_cons.mp hbl with hEq | hbl
          · subst bl
            rcases List.mem_cons.mp hw with rfl | hw
            · exact Or.inl rfl
            · exact Or.inr ⟨a, by simp, hw⟩
          · exact Or.inr ⟨bl, by simp [hbl], hw⟩
      | succ i =>
          intro bl hbl w hw
          have hbl' : bl = a ∨ bl ∈ bs.modify i (fun x => v :: x) := by
            simpa [List.modify] using hbl
          rcases hbl' with hEq | hbl'
          · subst bl
            exact Or.inr ⟨a, by simp, hw⟩
          · rcases ih i bl hbl' w hw with rfl | ⟨b, hb, hwb⟩
            · exact Or.inl rfl
            · exact Or.inr ⟨b, by simp [hb], hwb⟩

private theorem part_mem_V (V : List Nat) (bs : List (List Nat)) (h : bs ∈ partitions V) :
    ∀ bl ∈ bs, ∀ w ∈ bl, w ∈ V := by
  induction V generalizing bs with
  | nil => simp [partitions] at h; subst bs; simp
  | cons v vs ih =>
      simp only [partitions, List.mem_flatMap] at h
      rcases h with ⟨blocks, hblocks, hmem⟩
      rcases List.mem_cons.mp hmem with hmem | hmem
      · subst bs
        intro bl hbl w hw
        rcases List.mem_cons.mp hbl with hEq | hbl
        · subst bl
          rcases List.mem_cons.mp hw with hEq | hw
          · subst w; simp
          · simp at hw
        · exact List.mem_cons_of_mem v (ih blocks hblocks bl hbl w hw)
      · rcases List.mem_map.mp hmem with ⟨p, hp, rfl⟩
        rcases p with ⟨a, i⟩
        intro bl hbl w hw
        rcases mem_modify_prepend blocks i v bl hbl w hw with hEq | ⟨b, hb, hwb⟩
        · subst w; simp
        · exact List.mem_cons_of_mem v (ih blocks hblocks b hb w hwb)

private theorem head_mem_V (V : List Nat) (bs : List (List Nat)) (h : bs ∈ partitions V)
    (bl : List Nat) (hbl : bl ∈ bs) (hne : bl ≠ []) : headOf bl ∈ V := by
  cases bl with
  | nil => simp at hne
  | cons b bl => simpa [headOf] using part_mem_V V bs h (b :: bl) hbl b (by simp)

private theorem constOn_eq_of_eqpattern (V : List Nat) (bs : List (List Nat))
    (hbs : bs ∈ partitions V) (x y : Bind)
    (hxy : ∀ v ∈ V, ∀ w ∈ V, (x v == x w) = (y v == y w)) :
    constOn bs x = constOn bs y := by
  unfold constOn
  apply all_congr
  intro bl hbl
  apply all_congr
  intro w hw
  have hwV : w ∈ V := part_mem_V V bs hbs bl hbl w hw
  have hbne : bl ≠ [] := by intro he; subst bl; simp at hw
  have hhV : headOf bl ∈ V := head_mem_V V bs hbs bl hbl hbne
  exact hxy w hwV (headOf bl) hhV

private def CO (V : List Nat) (x : Bind) : Int :=
  ((partitions V).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum

private theorem CO_eq_of_eqpattern (V : List Nat) (x y : Bind)
    (hxy : ∀ v ∈ V, ∀ w ∈ V, (x v == x w) = (y v == y w)) : CO V x = CO V y := by
  unfold CO
  apply sum_map_congr_int
  intro bs hbs
  rw [constOn_eq_of_eqpattern V bs hbs x y hxy]

private theorem nodup_map_eq_of_eqpattern (V : List Nat) (x y : Bind)
    (hxy : ∀ v ∈ V, ∀ w ∈ V, (x v == x w) = (y v == y w)) :
    (V.map x).Nodup ↔ (V.map y).Nodup := by
  induction V with
  | nil => simp
  | cons v vs ih =>
      simp only [List.map_cons, List.nodup_cons]
      constructor
      · intro h
        refine ⟨?_, (ih (fun a ha b hb => hxy a (by simp [ha]) b (by simp [hb]))).mp h.2⟩
        intro hy
        rcases List.mem_map.mp hy with ⟨w, hw, hweq⟩
        have hvw_y : y v == y w := by simp [hweq]
        have hvw_x : x v == x w := by simpa [hxy v (by simp) w (by simp [hw])] using hvw_y
        exact h.1 (List.mem_map.mpr ⟨w, hw, (beq_iff_eq.mp hvw_x).symm⟩)
      · intro h
        refine ⟨?_, (ih (fun a ha b hb => hxy a (by simp [ha]) b (by simp [hb]))).mpr h.2⟩
        intro hx
        rcases List.mem_map.mp hx with ⟨w, hw, hweq⟩
        have hvw_x : x v == x w := by simp [hweq]
        have hvw_y : y v == y w := by simpa [hxy v (by simp) w (by simp [hw])] using hvw_x
        exact h.1 (List.mem_map.mpr ⟨w, hw, (beq_iff_eq.mp hvw_y).symm⟩)

private theorem factor_constOn (l : List (List (List Nat))) (x : Bind) (K : Int) :
    (l.map (fun bs => mobius bs * (if constOn bs x then K else 0))).sum =
      K * (l.map (fun bs => mobius bs * (if constOn bs x then (1:Int) else 0))).sum := by
  induction l with
  | nil => simp
  | cons bs l ih =>
      by_cases hp : constOn bs x = true
      · have hif : (if constOn bs x then K else 0) = K := by simp [hp]
        have hif1 : (if constOn bs x then (1:Int) else 0) = 1 := by simp [hp]
        simp only [List.map_cons, List.sum_cons, hif, hif1]
        rw [ih, Int.mul_add]
        simp
        ac_rfl
      · have hp' : constOn bs x = false := by cases h : constOn bs x <;> simp_all
        have hif : (if constOn bs x then K else 0) = 0 := by simp [hp']
        have hif1 : (if constOn bs x then (1:Int) else 0) = 0 := by simp [hp']
        simp only [List.map_cons, List.sum_cons, hif, hif1]
        simp
        exact ih

private theorem atom_mem (a : Atom) (ha : a ∈ pattern.atoms) :
    a.src ∈ pattern.freeVars ∧ a.dst ∈ pattern.freeVars := by
  simp [pattern] at ha
  rcases ha with rfl | rfl | rfl | rfl | rfl | rfl | rfl <;> simp [pattern]


private abbrev Vicyc : List Nat := [4, 5, 1, 2, 3, 0]
private abbrev xicyc (a b c d e f : Nat) : Bind :=
  fun v => if v == 4 then a
  else if v == 5 then b
  else if v == 1 then c
  else if v == 2 then d
  else if v == 3 then e
  else if v == 0 then f else 0

private theorem xicyc_of_mem (a b c d e f v : Nat) (hv : v ∈ Vicyc) :
    xicyc a b c d e f v = (match v with
      | 4 => a | 5 => b | 1 => c | 2 => d | 3 => e | 0 => f | _ => 0) := by
  simp [Vicyc] at hv
  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;> simp [xicyc]

private theorem coeff_icyc (a b c d e f : Nat) :
    CO Vicyc (xicyc a b c d e f) =
      (if (Vicyc.map (xicyc a b c d e f)).Nodup then (1 : Int) else 0) := by
  by_cases hba : b = a
  ·
    by_cases hca : c = a
    ·
      by_cases hda : d = a
      ·
        by_cases hea : e = a
        ·
          by_cases hfa : f = a
          ·
            have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                (xicyc a b c d e f v == xicyc a b c d e f w) =
                (xicyc 0 0 0 0 0 0 v == xicyc 0 0 0 0 0 0 w) := by
              intro v hv w hw
              rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                xicyc_of_mem 0 0 0 0 0 0 v hv, xicyc_of_mem 0 0 0 0 0 0 w hw]
              simp [Vicyc] at hv hw
              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xicyc, beq_iff_eq, *] <;> omega
            rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
            have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 0 0 0 0) hpatt
            by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 0 0 0 0)).Nodup) (hn.mp hnd))
            · rw [if_neg hnd]; native_decide
          ·
            have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                (xicyc a b c d e f v == xicyc a b c d e f w) =
                (xicyc 0 0 0 0 0 5 v == xicyc 0 0 0 0 0 5 w) := by
              intro v hv w hw
              rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                xicyc_of_mem 0 0 0 0 0 5 v hv, xicyc_of_mem 0 0 0 0 0 5 w hw]
              simp [Vicyc] at hv hw
              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xicyc, beq_iff_eq, *] <;> omega
            rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
            have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 0 0 0 5) hpatt
            by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 0 0 0 5)).Nodup) (hn.mp hnd))
            · rw [if_neg hnd]; native_decide
        ·
          by_cases hfa : f = a
          ·
            have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                (xicyc a b c d e f v == xicyc a b c d e f w) =
                (xicyc 0 0 0 0 4 0 v == xicyc 0 0 0 0 4 0 w) := by
              intro v hv w hw
              rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                xicyc_of_mem 0 0 0 0 4 0 v hv, xicyc_of_mem 0 0 0 0 4 0 w hw]
              simp [Vicyc] at hv hw
              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xicyc, beq_iff_eq, *] <;> omega
            rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
            have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 0 0 4 0) hpatt
            by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 0 0 4 0)).Nodup) (hn.mp hnd))
            · rw [if_neg hnd]; native_decide
          ·
            by_cases hfe : f = e
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 0 0 0 4 4 v == xicyc 0 0 0 0 4 4 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 0 0 0 4 4 v hv, xicyc_of_mem 0 0 0 0 4 4 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 0 0 4 4) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 0 0 4 4)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 0 0 0 4 5 v == xicyc 0 0 0 0 4 5 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 0 0 0 4 5 v hv, xicyc_of_mem 0 0 0 0 4 5 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 0 0 4 5) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 0 0 4 5)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
      ·
        by_cases hea : e = a
        ·
          by_cases hfa : f = a
          ·
            have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                (xicyc a b c d e f v == xicyc a b c d e f w) =
                (xicyc 0 0 0 3 0 0 v == xicyc 0 0 0 3 0 0 w) := by
              intro v hv w hw
              rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                xicyc_of_mem 0 0 0 3 0 0 v hv, xicyc_of_mem 0 0 0 3 0 0 w hw]
              simp [Vicyc] at hv hw
              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xicyc, beq_iff_eq, *] <;> omega
            rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
            have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 0 3 0 0) hpatt
            by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 0 3 0 0)).Nodup) (hn.mp hnd))
            · rw [if_neg hnd]; native_decide
          ·
            by_cases hfd : f = d
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 0 0 3 0 3 v == xicyc 0 0 0 3 0 3 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 0 0 3 0 3 v hv, xicyc_of_mem 0 0 0 3 0 3 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 0 3 0 3) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 0 3 0 3)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 0 0 3 0 5 v == xicyc 0 0 0 3 0 5 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 0 0 3 0 5 v hv, xicyc_of_mem 0 0 0 3 0 5 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 0 3 0 5) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 0 3 0 5)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
        ·
          by_cases hed : e = d
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 0 0 3 3 0 v == xicyc 0 0 0 3 3 0 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 0 0 3 3 0 v hv, xicyc_of_mem 0 0 0 3 3 0 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 0 3 3 0) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 0 3 3 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfd : f = d
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 0 0 3 3 3 v == xicyc 0 0 0 3 3 3 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 0 0 3 3 3 v hv, xicyc_of_mem 0 0 0 3 3 3 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 0 3 3 3) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 0 3 3 3)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 0 0 3 3 5 v == xicyc 0 0 0 3 3 5 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 0 0 3 3 5 v hv, xicyc_of_mem 0 0 0 3 3 5 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 0 3 3 5) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 0 3 3 5)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 0 0 3 4 0 v == xicyc 0 0 0 3 4 0 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 0 0 3 4 0 v hv, xicyc_of_mem 0 0 0 3 4 0 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 0 3 4 0) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 0 3 4 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfd : f = d
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 0 0 3 4 3 v == xicyc 0 0 0 3 4 3 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 0 0 3 4 3 v hv, xicyc_of_mem 0 0 0 3 4 3 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 0 3 4 3) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 0 3 4 3)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfe : f = e
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 0 0 3 4 4 v == xicyc 0 0 0 3 4 4 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 0 0 3 4 4 v hv, xicyc_of_mem 0 0 0 3 4 4 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 0 3 4 4) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 0 3 4 4)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 0 0 3 4 5 v == xicyc 0 0 0 3 4 5 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 0 0 3 4 5 v hv, xicyc_of_mem 0 0 0 3 4 5 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 0 3 4 5) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 0 3 4 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
    ·
      by_cases hda : d = a
      ·
        by_cases hea : e = a
        ·
          by_cases hfa : f = a
          ·
            have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                (xicyc a b c d e f v == xicyc a b c d e f w) =
                (xicyc 0 0 2 0 0 0 v == xicyc 0 0 2 0 0 0 w) := by
              intro v hv w hw
              rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                xicyc_of_mem 0 0 2 0 0 0 v hv, xicyc_of_mem 0 0 2 0 0 0 w hw]
              simp [Vicyc] at hv hw
              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xicyc, beq_iff_eq, *] <;> omega
            rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
            have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 0 0 0) hpatt
            by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 0 0 0)).Nodup) (hn.mp hnd))
            · rw [if_neg hnd]; native_decide
          ·
            by_cases hfc : f = c
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 0 2 0 0 2 v == xicyc 0 0 2 0 0 2 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 0 2 0 0 2 v hv, xicyc_of_mem 0 0 2 0 0 2 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 0 0 2) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 0 0 2)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 0 2 0 0 5 v == xicyc 0 0 2 0 0 5 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 0 2 0 0 5 v hv, xicyc_of_mem 0 0 2 0 0 5 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 0 0 5) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 0 0 5)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
        ·
          by_cases hec : e = c
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 0 2 0 2 0 v == xicyc 0 0 2 0 2 0 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 0 2 0 2 0 v hv, xicyc_of_mem 0 0 2 0 2 0 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 0 2 0) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 0 2 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfc : f = c
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 0 2 0 2 2 v == xicyc 0 0 2 0 2 2 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 0 2 0 2 2 v hv, xicyc_of_mem 0 0 2 0 2 2 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 0 2 2) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 0 2 2)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 0 2 0 2 5 v == xicyc 0 0 2 0 2 5 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 0 2 0 2 5 v hv, xicyc_of_mem 0 0 2 0 2 5 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 0 2 5) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 0 2 5)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 0 2 0 4 0 v == xicyc 0 0 2 0 4 0 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 0 2 0 4 0 v hv, xicyc_of_mem 0 0 2 0 4 0 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 0 4 0) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 0 4 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfc : f = c
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 0 2 0 4 2 v == xicyc 0 0 2 0 4 2 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 0 2 0 4 2 v hv, xicyc_of_mem 0 0 2 0 4 2 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 0 4 2) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 0 4 2)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfe : f = e
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 0 2 0 4 4 v == xicyc 0 0 2 0 4 4 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 0 2 0 4 4 v hv, xicyc_of_mem 0 0 2 0 4 4 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 0 4 4) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 0 4 4)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 0 2 0 4 5 v == xicyc 0 0 2 0 4 5 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 0 2 0 4 5 v hv, xicyc_of_mem 0 0 2 0 4 5 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 0 4 5) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 0 4 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
      ·
        by_cases hdc : d = c
        ·
          by_cases hea : e = a
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 0 2 2 0 0 v == xicyc 0 0 2 2 0 0 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 0 2 2 0 0 v hv, xicyc_of_mem 0 0 2 2 0 0 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 2 0 0) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 2 0 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfc : f = c
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 0 2 2 0 2 v == xicyc 0 0 2 2 0 2 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 0 2 2 0 2 v hv, xicyc_of_mem 0 0 2 2 0 2 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 2 0 2) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 2 0 2)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 0 2 2 0 5 v == xicyc 0 0 2 2 0 5 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 0 2 2 0 5 v hv, xicyc_of_mem 0 0 2 2 0 5 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 2 0 5) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 2 0 5)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
          ·
            by_cases hec : e = c
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 0 2 2 2 0 v == xicyc 0 0 2 2 2 0 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 0 2 2 2 0 v hv, xicyc_of_mem 0 0 2 2 2 0 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 2 2 0) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 2 2 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfc : f = c
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 0 2 2 2 2 v == xicyc 0 0 2 2 2 2 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 0 2 2 2 2 v hv, xicyc_of_mem 0 0 2 2 2 2 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 2 2 2) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 2 2 2)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 0 2 2 2 5 v == xicyc 0 0 2 2 2 5 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 0 2 2 2 5 v hv, xicyc_of_mem 0 0 2 2 2 5 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 2 2 5) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 2 2 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 0 2 2 4 0 v == xicyc 0 0 2 2 4 0 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 0 2 2 4 0 v hv, xicyc_of_mem 0 0 2 2 4 0 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 2 4 0) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 2 4 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfc : f = c
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 0 2 2 4 2 v == xicyc 0 0 2 2 4 2 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 0 2 2 4 2 v hv, xicyc_of_mem 0 0 2 2 4 2 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 2 4 2) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 2 4 2)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfe : f = e
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 0 2 2 4 4 v == xicyc 0 0 2 2 4 4 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 0 2 2 4 4 v hv, xicyc_of_mem 0 0 2 2 4 4 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 2 4 4) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 2 4 4)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 0 2 2 4 5 v == xicyc 0 0 2 2 4 5 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 0 2 2 4 5 v hv, xicyc_of_mem 0 0 2 2 4 5 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 2 4 5) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 2 4 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
        ·
          by_cases hea : e = a
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 0 2 3 0 0 v == xicyc 0 0 2 3 0 0 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 0 2 3 0 0 v hv, xicyc_of_mem 0 0 2 3 0 0 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 3 0 0) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 3 0 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfc : f = c
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 0 2 3 0 2 v == xicyc 0 0 2 3 0 2 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 0 2 3 0 2 v hv, xicyc_of_mem 0 0 2 3 0 2 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 3 0 2) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 3 0 2)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfd : f = d
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 0 2 3 0 3 v == xicyc 0 0 2 3 0 3 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 0 2 3 0 3 v hv, xicyc_of_mem 0 0 2 3 0 3 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 3 0 3) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 3 0 3)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 0 2 3 0 5 v == xicyc 0 0 2 3 0 5 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 0 2 3 0 5 v hv, xicyc_of_mem 0 0 2 3 0 5 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 3 0 5) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 3 0 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
          ·
            by_cases hec : e = c
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 0 2 3 2 0 v == xicyc 0 0 2 3 2 0 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 0 2 3 2 0 v hv, xicyc_of_mem 0 0 2 3 2 0 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 3 2 0) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 3 2 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfc : f = c
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 0 2 3 2 2 v == xicyc 0 0 2 3 2 2 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 0 2 3 2 2 v hv, xicyc_of_mem 0 0 2 3 2 2 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 3 2 2) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 3 2 2)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfd : f = d
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 0 2 3 2 3 v == xicyc 0 0 2 3 2 3 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 0 2 3 2 3 v hv, xicyc_of_mem 0 0 2 3 2 3 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 3 2 3) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 3 2 3)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 0 2 3 2 5 v == xicyc 0 0 2 3 2 5 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 0 2 3 2 5 v hv, xicyc_of_mem 0 0 2 3 2 5 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 3 2 5) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 3 2 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
            ·
              by_cases hed : e = d
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 0 2 3 3 0 v == xicyc 0 0 2 3 3 0 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 0 2 3 3 0 v hv, xicyc_of_mem 0 0 2 3 3 0 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 3 3 0) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 3 3 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfc : f = c
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 0 2 3 3 2 v == xicyc 0 0 2 3 3 2 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 0 2 3 3 2 v hv, xicyc_of_mem 0 0 2 3 3 2 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 3 3 2) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 3 3 2)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfd : f = d
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 0 2 3 3 3 v == xicyc 0 0 2 3 3 3 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 0 2 3 3 3 v hv, xicyc_of_mem 0 0 2 3 3 3 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 3 3 3) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 3 3 3)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 0 2 3 3 5 v == xicyc 0 0 2 3 3 5 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 0 2 3 3 5 v hv, xicyc_of_mem 0 0 2 3 3 5 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 3 3 5) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 3 3 5)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 0 2 3 4 0 v == xicyc 0 0 2 3 4 0 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 0 2 3 4 0 v hv, xicyc_of_mem 0 0 2 3 4 0 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 3 4 0) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 3 4 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfc : f = c
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 0 2 3 4 2 v == xicyc 0 0 2 3 4 2 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 0 2 3 4 2 v hv, xicyc_of_mem 0 0 2 3 4 2 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 3 4 2) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 3 4 2)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfd : f = d
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 0 2 3 4 3 v == xicyc 0 0 2 3 4 3 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 0 2 3 4 3 v hv, xicyc_of_mem 0 0 2 3 4 3 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 3 4 3) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 3 4 3)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfe : f = e
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 0 2 3 4 4 v == xicyc 0 0 2 3 4 4 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 0 2 3 4 4 v hv, xicyc_of_mem 0 0 2 3 4 4 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 3 4 4) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 3 4 4)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 0 2 3 4 5 v == xicyc 0 0 2 3 4 5 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 0 2 3 4 5 v hv, xicyc_of_mem 0 0 2 3 4 5 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 0 2 3 4 5) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 0 2 3 4 5)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
  ·
    by_cases hca : c = a
    ·
      by_cases hda : d = a
      ·
        by_cases hea : e = a
        ·
          by_cases hfa : f = a
          ·
            have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                (xicyc a b c d e f v == xicyc a b c d e f w) =
                (xicyc 0 1 0 0 0 0 v == xicyc 0 1 0 0 0 0 w) := by
              intro v hv w hw
              rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                xicyc_of_mem 0 1 0 0 0 0 v hv, xicyc_of_mem 0 1 0 0 0 0 w hw]
              simp [Vicyc] at hv hw
              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xicyc, beq_iff_eq, *] <;> omega
            rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
            have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 0 0 0) hpatt
            by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 0 0 0)).Nodup) (hn.mp hnd))
            · rw [if_neg hnd]; native_decide
          ·
            by_cases hfb : f = b
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 1 0 0 0 1 v == xicyc 0 1 0 0 0 1 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 1 0 0 0 1 v hv, xicyc_of_mem 0 1 0 0 0 1 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 0 0 1) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 0 0 1)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 1 0 0 0 5 v == xicyc 0 1 0 0 0 5 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 1 0 0 0 5 v hv, xicyc_of_mem 0 1 0 0 0 5 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 0 0 5) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 0 0 5)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
        ·
          by_cases heb : e = b
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 1 0 0 1 0 v == xicyc 0 1 0 0 1 0 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 1 0 0 1 0 v hv, xicyc_of_mem 0 1 0 0 1 0 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 0 1 0) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 0 1 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfb : f = b
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 0 0 1 1 v == xicyc 0 1 0 0 1 1 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 0 0 1 1 v hv, xicyc_of_mem 0 1 0 0 1 1 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 0 1 1) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 0 1 1)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 0 0 1 5 v == xicyc 0 1 0 0 1 5 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 0 0 1 5 v hv, xicyc_of_mem 0 1 0 0 1 5 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 0 1 5) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 0 1 5)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 1 0 0 4 0 v == xicyc 0 1 0 0 4 0 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 1 0 0 4 0 v hv, xicyc_of_mem 0 1 0 0 4 0 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 0 4 0) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 0 4 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfb : f = b
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 0 0 4 1 v == xicyc 0 1 0 0 4 1 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 0 0 4 1 v hv, xicyc_of_mem 0 1 0 0 4 1 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 0 4 1) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 0 4 1)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfe : f = e
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 0 0 4 4 v == xicyc 0 1 0 0 4 4 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 0 0 4 4 v hv, xicyc_of_mem 0 1 0 0 4 4 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 0 4 4) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 0 4 4)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 0 0 4 5 v == xicyc 0 1 0 0 4 5 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 0 0 4 5 v hv, xicyc_of_mem 0 1 0 0 4 5 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 0 4 5) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 0 4 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
      ·
        by_cases hdb : d = b
        ·
          by_cases hea : e = a
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 1 0 1 0 0 v == xicyc 0 1 0 1 0 0 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 1 0 1 0 0 v hv, xicyc_of_mem 0 1 0 1 0 0 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 1 0 0) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 1 0 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfb : f = b
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 0 1 0 1 v == xicyc 0 1 0 1 0 1 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 0 1 0 1 v hv, xicyc_of_mem 0 1 0 1 0 1 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 1 0 1) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 1 0 1)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 0 1 0 5 v == xicyc 0 1 0 1 0 5 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 0 1 0 5 v hv, xicyc_of_mem 0 1 0 1 0 5 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 1 0 5) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 1 0 5)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
          ·
            by_cases heb : e = b
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 0 1 1 0 v == xicyc 0 1 0 1 1 0 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 0 1 1 0 v hv, xicyc_of_mem 0 1 0 1 1 0 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 1 1 0) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 1 1 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 0 1 1 1 v == xicyc 0 1 0 1 1 1 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 0 1 1 1 v hv, xicyc_of_mem 0 1 0 1 1 1 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 1 1 1) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 1 1 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 0 1 1 5 v == xicyc 0 1 0 1 1 5 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 0 1 1 5 v hv, xicyc_of_mem 0 1 0 1 1 5 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 1 1 5) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 1 1 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 0 1 4 0 v == xicyc 0 1 0 1 4 0 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 0 1 4 0 v hv, xicyc_of_mem 0 1 0 1 4 0 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 1 4 0) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 1 4 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 0 1 4 1 v == xicyc 0 1 0 1 4 1 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 0 1 4 1 v hv, xicyc_of_mem 0 1 0 1 4 1 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 1 4 1) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 1 4 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfe : f = e
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 0 1 4 4 v == xicyc 0 1 0 1 4 4 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 0 1 4 4 v hv, xicyc_of_mem 0 1 0 1 4 4 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 1 4 4) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 1 4 4)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 0 1 4 5 v == xicyc 0 1 0 1 4 5 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 0 1 4 5 v hv, xicyc_of_mem 0 1 0 1 4 5 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 1 4 5) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 1 4 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
        ·
          by_cases hea : e = a
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 1 0 3 0 0 v == xicyc 0 1 0 3 0 0 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 1 0 3 0 0 v hv, xicyc_of_mem 0 1 0 3 0 0 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 3 0 0) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 3 0 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfb : f = b
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 0 3 0 1 v == xicyc 0 1 0 3 0 1 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 0 3 0 1 v hv, xicyc_of_mem 0 1 0 3 0 1 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 3 0 1) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 3 0 1)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfd : f = d
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 0 3 0 3 v == xicyc 0 1 0 3 0 3 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 0 3 0 3 v hv, xicyc_of_mem 0 1 0 3 0 3 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 3 0 3) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 3 0 3)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 0 3 0 5 v == xicyc 0 1 0 3 0 5 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 0 3 0 5 v hv, xicyc_of_mem 0 1 0 3 0 5 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 3 0 5) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 3 0 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
          ·
            by_cases heb : e = b
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 0 3 1 0 v == xicyc 0 1 0 3 1 0 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 0 3 1 0 v hv, xicyc_of_mem 0 1 0 3 1 0 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 3 1 0) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 3 1 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 0 3 1 1 v == xicyc 0 1 0 3 1 1 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 0 3 1 1 v hv, xicyc_of_mem 0 1 0 3 1 1 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 3 1 1) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 3 1 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfd : f = d
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 0 3 1 3 v == xicyc 0 1 0 3 1 3 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 0 3 1 3 v hv, xicyc_of_mem 0 1 0 3 1 3 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 3 1 3) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 3 1 3)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 0 3 1 5 v == xicyc 0 1 0 3 1 5 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 0 3 1 5 v hv, xicyc_of_mem 0 1 0 3 1 5 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 3 1 5) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 3 1 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
            ·
              by_cases hed : e = d
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 0 3 3 0 v == xicyc 0 1 0 3 3 0 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 0 3 3 0 v hv, xicyc_of_mem 0 1 0 3 3 0 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 3 3 0) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 3 3 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 0 3 3 1 v == xicyc 0 1 0 3 3 1 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 0 3 3 1 v hv, xicyc_of_mem 0 1 0 3 3 1 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 3 3 1) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 3 3 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfd : f = d
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 0 3 3 3 v == xicyc 0 1 0 3 3 3 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 0 3 3 3 v hv, xicyc_of_mem 0 1 0 3 3 3 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 3 3 3) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 3 3 3)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 0 3 3 5 v == xicyc 0 1 0 3 3 5 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 0 3 3 5 v hv, xicyc_of_mem 0 1 0 3 3 5 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 3 3 5) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 3 3 5)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 0 3 4 0 v == xicyc 0 1 0 3 4 0 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 0 3 4 0 v hv, xicyc_of_mem 0 1 0 3 4 0 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 3 4 0) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 3 4 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 0 3 4 1 v == xicyc 0 1 0 3 4 1 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 0 3 4 1 v hv, xicyc_of_mem 0 1 0 3 4 1 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 3 4 1) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 3 4 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfd : f = d
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 0 3 4 3 v == xicyc 0 1 0 3 4 3 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 0 3 4 3 v hv, xicyc_of_mem 0 1 0 3 4 3 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 3 4 3) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 3 4 3)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfe : f = e
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 0 3 4 4 v == xicyc 0 1 0 3 4 4 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 0 3 4 4 v hv, xicyc_of_mem 0 1 0 3 4 4 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 3 4 4) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 3 4 4)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 0 3 4 5 v == xicyc 0 1 0 3 4 5 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 0 3 4 5 v hv, xicyc_of_mem 0 1 0 3 4 5 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 0 3 4 5) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 0 3 4 5)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
    ·
      by_cases hcb : c = b
      ·
        by_cases hda : d = a
        ·
          by_cases hea : e = a
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 1 1 0 0 0 v == xicyc 0 1 1 0 0 0 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 1 1 0 0 0 v hv, xicyc_of_mem 0 1 1 0 0 0 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 0 0 0) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 0 0 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfb : f = b
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 1 0 0 1 v == xicyc 0 1 1 0 0 1 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 1 0 0 1 v hv, xicyc_of_mem 0 1 1 0 0 1 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 0 0 1) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 0 0 1)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 1 0 0 5 v == xicyc 0 1 1 0 0 5 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 1 0 0 5 v hv, xicyc_of_mem 0 1 1 0 0 5 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 0 0 5) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 0 0 5)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
          ·
            by_cases heb : e = b
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 1 0 1 0 v == xicyc 0 1 1 0 1 0 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 1 0 1 0 v hv, xicyc_of_mem 0 1 1 0 1 0 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 0 1 0) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 0 1 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 1 0 1 1 v == xicyc 0 1 1 0 1 1 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 1 0 1 1 v hv, xicyc_of_mem 0 1 1 0 1 1 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 0 1 1) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 0 1 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 1 0 1 5 v == xicyc 0 1 1 0 1 5 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 1 0 1 5 v hv, xicyc_of_mem 0 1 1 0 1 5 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 0 1 5) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 0 1 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 1 0 4 0 v == xicyc 0 1 1 0 4 0 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 1 0 4 0 v hv, xicyc_of_mem 0 1 1 0 4 0 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 0 4 0) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 0 4 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 1 0 4 1 v == xicyc 0 1 1 0 4 1 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 1 0 4 1 v hv, xicyc_of_mem 0 1 1 0 4 1 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 0 4 1) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 0 4 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfe : f = e
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 1 0 4 4 v == xicyc 0 1 1 0 4 4 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 1 0 4 4 v hv, xicyc_of_mem 0 1 1 0 4 4 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 0 4 4) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 0 4 4)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 1 0 4 5 v == xicyc 0 1 1 0 4 5 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 1 0 4 5 v hv, xicyc_of_mem 0 1 1 0 4 5 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 0 4 5) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 0 4 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
        ·
          by_cases hdb : d = b
          ·
            by_cases hea : e = a
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 1 1 0 0 v == xicyc 0 1 1 1 0 0 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 1 1 0 0 v hv, xicyc_of_mem 0 1 1 1 0 0 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 1 0 0) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 1 0 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 1 1 0 1 v == xicyc 0 1 1 1 0 1 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 1 1 0 1 v hv, xicyc_of_mem 0 1 1 1 0 1 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 1 0 1) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 1 0 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 1 1 0 5 v == xicyc 0 1 1 1 0 5 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 1 1 0 5 v hv, xicyc_of_mem 0 1 1 1 0 5 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 1 0 5) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 1 0 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
            ·
              by_cases heb : e = b
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 1 1 1 0 v == xicyc 0 1 1 1 1 0 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 1 1 1 0 v hv, xicyc_of_mem 0 1 1 1 1 0 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 1 1 0) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 1 1 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 1 1 1 1 v == xicyc 0 1 1 1 1 1 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 1 1 1 1 v hv, xicyc_of_mem 0 1 1 1 1 1 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 1 1 1) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 1 1 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 1 1 1 5 v == xicyc 0 1 1 1 1 5 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 1 1 1 5 v hv, xicyc_of_mem 0 1 1 1 1 5 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 1 1 5) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 1 1 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 1 1 4 0 v == xicyc 0 1 1 1 4 0 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 1 1 4 0 v hv, xicyc_of_mem 0 1 1 1 4 0 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 1 4 0) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 1 4 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 1 1 4 1 v == xicyc 0 1 1 1 4 1 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 1 1 4 1 v hv, xicyc_of_mem 0 1 1 1 4 1 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 1 4 1) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 1 4 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfe : f = e
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 1 1 4 4 v == xicyc 0 1 1 1 4 4 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 1 1 4 4 v hv, xicyc_of_mem 0 1 1 1 4 4 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 1 4 4) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 1 4 4)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 1 1 4 5 v == xicyc 0 1 1 1 4 5 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 1 1 4 5 v hv, xicyc_of_mem 0 1 1 1 4 5 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 1 4 5) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 1 4 5)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
          ·
            by_cases hea : e = a
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 1 3 0 0 v == xicyc 0 1 1 3 0 0 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 1 3 0 0 v hv, xicyc_of_mem 0 1 1 3 0 0 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 3 0 0) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 3 0 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 1 3 0 1 v == xicyc 0 1 1 3 0 1 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 1 3 0 1 v hv, xicyc_of_mem 0 1 1 3 0 1 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 3 0 1) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 3 0 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfd : f = d
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 1 3 0 3 v == xicyc 0 1 1 3 0 3 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 1 3 0 3 v hv, xicyc_of_mem 0 1 1 3 0 3 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 3 0 3) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 3 0 3)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 1 3 0 5 v == xicyc 0 1 1 3 0 5 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 1 3 0 5 v hv, xicyc_of_mem 0 1 1 3 0 5 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 3 0 5) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 3 0 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
            ·
              by_cases heb : e = b
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 1 3 1 0 v == xicyc 0 1 1 3 1 0 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 1 3 1 0 v hv, xicyc_of_mem 0 1 1 3 1 0 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 3 1 0) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 3 1 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 1 3 1 1 v == xicyc 0 1 1 3 1 1 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 1 3 1 1 v hv, xicyc_of_mem 0 1 1 3 1 1 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 3 1 1) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 3 1 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfd : f = d
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 1 3 1 3 v == xicyc 0 1 1 3 1 3 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 1 3 1 3 v hv, xicyc_of_mem 0 1 1 3 1 3 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 3 1 3) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 3 1 3)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 1 3 1 5 v == xicyc 0 1 1 3 1 5 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 1 3 1 5 v hv, xicyc_of_mem 0 1 1 3 1 5 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 3 1 5) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 3 1 5)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
              ·
                by_cases hed : e = d
                ·
                  by_cases hfa : f = a
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 1 3 3 0 v == xicyc 0 1 1 3 3 0 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 1 3 3 0 v hv, xicyc_of_mem 0 1 1 3 3 0 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 3 3 0) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 3 3 0)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfb : f = b
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 1 3 3 1 v == xicyc 0 1 1 3 3 1 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 1 3 3 1 v hv, xicyc_of_mem 0 1 1 3 3 1 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 3 3 1) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 3 3 1)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfd : f = d
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 1 3 3 3 v == xicyc 0 1 1 3 3 3 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 1 3 3 3 v hv, xicyc_of_mem 0 1 1 3 3 3 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 3 3 3) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 3 3 3)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 1 3 3 5 v == xicyc 0 1 1 3 3 5 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 1 3 3 5 v hv, xicyc_of_mem 0 1 1 3 3 5 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 3 3 5) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 3 3 5)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfa : f = a
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 1 3 4 0 v == xicyc 0 1 1 3 4 0 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 1 3 4 0 v hv, xicyc_of_mem 0 1 1 3 4 0 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 3 4 0) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 3 4 0)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfb : f = b
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 1 3 4 1 v == xicyc 0 1 1 3 4 1 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 1 3 4 1 v hv, xicyc_of_mem 0 1 1 3 4 1 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 3 4 1) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 3 4 1)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfd : f = d
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 1 3 4 3 v == xicyc 0 1 1 3 4 3 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 1 3 4 3 v hv, xicyc_of_mem 0 1 1 3 4 3 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 3 4 3) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 3 4 3)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        by_cases hfe : f = e
                        ·
                          have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                              (xicyc a b c d e f v == xicyc a b c d e f w) =
                              (xicyc 0 1 1 3 4 4 v == xicyc 0 1 1 3 4 4 w) := by
                            intro v hv w hw
                            rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                              xicyc_of_mem 0 1 1 3 4 4 v hv, xicyc_of_mem 0 1 1 3 4 4 w hw]
                            simp [Vicyc] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xicyc, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 3 4 4) hpatt
                          by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 3 4 4)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                        ·
                          have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                              (xicyc a b c d e f v == xicyc a b c d e f w) =
                              (xicyc 0 1 1 3 4 5 v == xicyc 0 1 1 3 4 5 w) := by
                            intro v hv w hw
                            rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                              xicyc_of_mem 0 1 1 3 4 5 v hv, xicyc_of_mem 0 1 1 3 4 5 w hw]
                            simp [Vicyc] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xicyc, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 1 3 4 5) hpatt
                          by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 1 3 4 5)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
      ·
        by_cases hda : d = a
        ·
          by_cases hea : e = a
          ·
            by_cases hfa : f = a
            ·
              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                  (xicyc 0 1 2 0 0 0 v == xicyc 0 1 2 0 0 0 w) := by
                intro v hv w hw
                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                  xicyc_of_mem 0 1 2 0 0 0 v hv, xicyc_of_mem 0 1 2 0 0 0 w hw]
                simp [Vicyc] at hv hw
                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xicyc, beq_iff_eq, *] <;> omega
              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 0 0 0) hpatt
              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 0 0 0)).Nodup) (hn.mp hnd))
              · rw [if_neg hnd]; native_decide
            ·
              by_cases hfb : f = b
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 2 0 0 1 v == xicyc 0 1 2 0 0 1 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 2 0 0 1 v hv, xicyc_of_mem 0 1 2 0 0 1 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 0 0 1) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 0 0 1)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfc : f = c
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 2 0 0 2 v == xicyc 0 1 2 0 0 2 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 2 0 0 2 v hv, xicyc_of_mem 0 1 2 0 0 2 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 0 0 2) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 0 0 2)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 2 0 0 5 v == xicyc 0 1 2 0 0 5 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 2 0 0 5 v hv, xicyc_of_mem 0 1 2 0 0 5 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 0 0 5) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 0 0 5)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
          ·
            by_cases heb : e = b
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 2 0 1 0 v == xicyc 0 1 2 0 1 0 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 2 0 1 0 v hv, xicyc_of_mem 0 1 2 0 1 0 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 0 1 0) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 0 1 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 2 0 1 1 v == xicyc 0 1 2 0 1 1 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 2 0 1 1 v hv, xicyc_of_mem 0 1 2 0 1 1 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 0 1 1) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 0 1 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfc : f = c
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 2 0 1 2 v == xicyc 0 1 2 0 1 2 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 2 0 1 2 v hv, xicyc_of_mem 0 1 2 0 1 2 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 0 1 2) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 0 1 2)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 2 0 1 5 v == xicyc 0 1 2 0 1 5 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 2 0 1 5 v hv, xicyc_of_mem 0 1 2 0 1 5 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 0 1 5) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 0 1 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
            ·
              by_cases hec : e = c
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 2 0 2 0 v == xicyc 0 1 2 0 2 0 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 2 0 2 0 v hv, xicyc_of_mem 0 1 2 0 2 0 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 0 2 0) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 0 2 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 2 0 2 1 v == xicyc 0 1 2 0 2 1 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 2 0 2 1 v hv, xicyc_of_mem 0 1 2 0 2 1 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 0 2 1) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 0 2 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfc : f = c
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 2 0 2 2 v == xicyc 0 1 2 0 2 2 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 2 0 2 2 v hv, xicyc_of_mem 0 1 2 0 2 2 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 0 2 2) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 0 2 2)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 2 0 2 5 v == xicyc 0 1 2 0 2 5 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 2 0 2 5 v hv, xicyc_of_mem 0 1 2 0 2 5 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 0 2 5) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 0 2 5)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 2 0 4 0 v == xicyc 0 1 2 0 4 0 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 2 0 4 0 v hv, xicyc_of_mem 0 1 2 0 4 0 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 0 4 0) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 0 4 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 2 0 4 1 v == xicyc 0 1 2 0 4 1 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 2 0 4 1 v hv, xicyc_of_mem 0 1 2 0 4 1 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 0 4 1) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 0 4 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfc : f = c
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 2 0 4 2 v == xicyc 0 1 2 0 4 2 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 2 0 4 2 v hv, xicyc_of_mem 0 1 2 0 4 2 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 0 4 2) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 0 4 2)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfe : f = e
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 2 0 4 4 v == xicyc 0 1 2 0 4 4 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 2 0 4 4 v hv, xicyc_of_mem 0 1 2 0 4 4 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 0 4 4) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 0 4 4)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 2 0 4 5 v == xicyc 0 1 2 0 4 5 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 2 0 4 5 v hv, xicyc_of_mem 0 1 2 0 4 5 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 0 4 5) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 0 4 5)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
        ·
          by_cases hdb : d = b
          ·
            by_cases hea : e = a
            ·
              by_cases hfa : f = a
              ·
                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                    (xicyc 0 1 2 1 0 0 v == xicyc 0 1 2 1 0 0 w) := by
                  intro v hv w hw
                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                    xicyc_of_mem 0 1 2 1 0 0 v hv, xicyc_of_mem 0 1 2 1 0 0 w hw]
                  simp [Vicyc] at hv hw
                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xicyc, beq_iff_eq, *] <;> omega
                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 1 0 0) hpatt
                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 1 0 0)).Nodup) (hn.mp hnd))
                · rw [if_neg hnd]; native_decide
              ·
                by_cases hfb : f = b
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 2 1 0 1 v == xicyc 0 1 2 1 0 1 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 2 1 0 1 v hv, xicyc_of_mem 0 1 2 1 0 1 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 1 0 1) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 1 0 1)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfc : f = c
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 2 1 0 2 v == xicyc 0 1 2 1 0 2 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 2 1 0 2 v hv, xicyc_of_mem 0 1 2 1 0 2 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 1 0 2) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 1 0 2)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 2 1 0 5 v == xicyc 0 1 2 1 0 5 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 2 1 0 5 v hv, xicyc_of_mem 0 1 2 1 0 5 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 1 0 5) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 1 0 5)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
            ·
              by_cases heb : e = b
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 2 1 1 0 v == xicyc 0 1 2 1 1 0 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 2 1 1 0 v hv, xicyc_of_mem 0 1 2 1 1 0 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 1 1 0) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 1 1 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 2 1 1 1 v == xicyc 0 1 2 1 1 1 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 2 1 1 1 v hv, xicyc_of_mem 0 1 2 1 1 1 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 1 1 1) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 1 1 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfc : f = c
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 2 1 1 2 v == xicyc 0 1 2 1 1 2 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 2 1 1 2 v hv, xicyc_of_mem 0 1 2 1 1 2 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 1 1 2) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 1 1 2)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 2 1 1 5 v == xicyc 0 1 2 1 1 5 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 2 1 1 5 v hv, xicyc_of_mem 0 1 2 1 1 5 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 1 1 5) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 1 1 5)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
              ·
                by_cases hec : e = c
                ·
                  by_cases hfa : f = a
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 2 1 2 0 v == xicyc 0 1 2 1 2 0 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 2 1 2 0 v hv, xicyc_of_mem 0 1 2 1 2 0 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 1 2 0) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 1 2 0)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfb : f = b
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 2 1 2 1 v == xicyc 0 1 2 1 2 1 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 2 1 2 1 v hv, xicyc_of_mem 0 1 2 1 2 1 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 1 2 1) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 1 2 1)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfc : f = c
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 2 1 2 2 v == xicyc 0 1 2 1 2 2 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 2 1 2 2 v hv, xicyc_of_mem 0 1 2 1 2 2 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 1 2 2) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 1 2 2)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 2 1 2 5 v == xicyc 0 1 2 1 2 5 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 2 1 2 5 v hv, xicyc_of_mem 0 1 2 1 2 5 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 1 2 5) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 1 2 5)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfa : f = a
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 2 1 4 0 v == xicyc 0 1 2 1 4 0 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 2 1 4 0 v hv, xicyc_of_mem 0 1 2 1 4 0 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 1 4 0) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 1 4 0)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfb : f = b
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 2 1 4 1 v == xicyc 0 1 2 1 4 1 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 2 1 4 1 v hv, xicyc_of_mem 0 1 2 1 4 1 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 1 4 1) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 1 4 1)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfc : f = c
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 2 1 4 2 v == xicyc 0 1 2 1 4 2 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 2 1 4 2 v hv, xicyc_of_mem 0 1 2 1 4 2 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 1 4 2) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 1 4 2)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        by_cases hfe : f = e
                        ·
                          have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                              (xicyc a b c d e f v == xicyc a b c d e f w) =
                              (xicyc 0 1 2 1 4 4 v == xicyc 0 1 2 1 4 4 w) := by
                            intro v hv w hw
                            rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                              xicyc_of_mem 0 1 2 1 4 4 v hv, xicyc_of_mem 0 1 2 1 4 4 w hw]
                            simp [Vicyc] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xicyc, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 1 4 4) hpatt
                          by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 1 4 4)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                        ·
                          have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                              (xicyc a b c d e f v == xicyc a b c d e f w) =
                              (xicyc 0 1 2 1 4 5 v == xicyc 0 1 2 1 4 5 w) := by
                            intro v hv w hw
                            rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                              xicyc_of_mem 0 1 2 1 4 5 v hv, xicyc_of_mem 0 1 2 1 4 5 w hw]
                            simp [Vicyc] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xicyc, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 1 4 5) hpatt
                          by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 1 4 5)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
          ·
            by_cases hdc : d = c
            ·
              by_cases hea : e = a
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 2 2 0 0 v == xicyc 0 1 2 2 0 0 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 2 2 0 0 v hv, xicyc_of_mem 0 1 2 2 0 0 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 2 0 0) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 2 0 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 2 2 0 1 v == xicyc 0 1 2 2 0 1 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 2 2 0 1 v hv, xicyc_of_mem 0 1 2 2 0 1 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 2 0 1) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 2 0 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfc : f = c
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 2 2 0 2 v == xicyc 0 1 2 2 0 2 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 2 2 0 2 v hv, xicyc_of_mem 0 1 2 2 0 2 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 2 0 2) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 2 0 2)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 2 2 0 5 v == xicyc 0 1 2 2 0 5 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 2 2 0 5 v hv, xicyc_of_mem 0 1 2 2 0 5 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 2 0 5) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 2 0 5)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
              ·
                by_cases heb : e = b
                ·
                  by_cases hfa : f = a
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 2 2 1 0 v == xicyc 0 1 2 2 1 0 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 2 2 1 0 v hv, xicyc_of_mem 0 1 2 2 1 0 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 2 1 0) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 2 1 0)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfb : f = b
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 2 2 1 1 v == xicyc 0 1 2 2 1 1 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 2 2 1 1 v hv, xicyc_of_mem 0 1 2 2 1 1 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 2 1 1) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 2 1 1)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfc : f = c
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 2 2 1 2 v == xicyc 0 1 2 2 1 2 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 2 2 1 2 v hv, xicyc_of_mem 0 1 2 2 1 2 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 2 1 2) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 2 1 2)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 2 2 1 5 v == xicyc 0 1 2 2 1 5 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 2 2 1 5 v hv, xicyc_of_mem 0 1 2 2 1 5 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 2 1 5) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 2 1 5)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                ·
                  by_cases hec : e = c
                  ·
                    by_cases hfa : f = a
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 2 2 2 0 v == xicyc 0 1 2 2 2 0 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 2 2 2 0 v hv, xicyc_of_mem 0 1 2 2 2 0 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 2 2 0) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 2 2 0)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfb : f = b
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 2 2 2 1 v == xicyc 0 1 2 2 2 1 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 2 2 2 1 v hv, xicyc_of_mem 0 1 2 2 2 1 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 2 2 1) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 2 2 1)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        by_cases hfc : f = c
                        ·
                          have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                              (xicyc a b c d e f v == xicyc a b c d e f w) =
                              (xicyc 0 1 2 2 2 2 v == xicyc 0 1 2 2 2 2 w) := by
                            intro v hv w hw
                            rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                              xicyc_of_mem 0 1 2 2 2 2 v hv, xicyc_of_mem 0 1 2 2 2 2 w hw]
                            simp [Vicyc] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xicyc, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 2 2 2) hpatt
                          by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 2 2 2)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                        ·
                          have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                              (xicyc a b c d e f v == xicyc a b c d e f w) =
                              (xicyc 0 1 2 2 2 5 v == xicyc 0 1 2 2 2 5 w) := by
                            intro v hv w hw
                            rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                              xicyc_of_mem 0 1 2 2 2 5 v hv, xicyc_of_mem 0 1 2 2 2 5 w hw]
                            simp [Vicyc] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xicyc, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 2 2 5) hpatt
                          by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 2 2 5)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfa : f = a
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 2 2 4 0 v == xicyc 0 1 2 2 4 0 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 2 2 4 0 v hv, xicyc_of_mem 0 1 2 2 4 0 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 2 4 0) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 2 4 0)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfb : f = b
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 2 2 4 1 v == xicyc 0 1 2 2 4 1 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 2 2 4 1 v hv, xicyc_of_mem 0 1 2 2 4 1 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 2 4 1) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 2 4 1)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        by_cases hfc : f = c
                        ·
                          have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                              (xicyc a b c d e f v == xicyc a b c d e f w) =
                              (xicyc 0 1 2 2 4 2 v == xicyc 0 1 2 2 4 2 w) := by
                            intro v hv w hw
                            rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                              xicyc_of_mem 0 1 2 2 4 2 v hv, xicyc_of_mem 0 1 2 2 4 2 w hw]
                            simp [Vicyc] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xicyc, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 2 4 2) hpatt
                          by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 2 4 2)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                        ·
                          by_cases hfe : f = e
                          ·
                            have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                                (xicyc a b c d e f v == xicyc a b c d e f w) =
                                (xicyc 0 1 2 2 4 4 v == xicyc 0 1 2 2 4 4 w) := by
                              intro v hv w hw
                              rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                                xicyc_of_mem 0 1 2 2 4 4 v hv, xicyc_of_mem 0 1 2 2 4 4 w hw]
                              simp [Vicyc] at hv hw
                              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                simp [xicyc, beq_iff_eq, *] <;> omega
                            rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                            have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 2 4 4) hpatt
                            by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 2 4 4)).Nodup) (hn.mp hnd))
                            · rw [if_neg hnd]; native_decide
                          ·
                            have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                                (xicyc a b c d e f v == xicyc a b c d e f w) =
                                (xicyc 0 1 2 2 4 5 v == xicyc 0 1 2 2 4 5 w) := by
                              intro v hv w hw
                              rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                                xicyc_of_mem 0 1 2 2 4 5 v hv, xicyc_of_mem 0 1 2 2 4 5 w hw]
                              simp [Vicyc] at hv hw
                              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                simp [xicyc, beq_iff_eq, *] <;> omega
                            rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                            have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 2 4 5) hpatt
                            by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 2 4 5)).Nodup) (hn.mp hnd))
                            · rw [if_neg hnd]; native_decide
            ·
              by_cases hea : e = a
              ·
                by_cases hfa : f = a
                ·
                  have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                      (xicyc a b c d e f v == xicyc a b c d e f w) =
                      (xicyc 0 1 2 3 0 0 v == xicyc 0 1 2 3 0 0 w) := by
                    intro v hv w hw
                    rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                      xicyc_of_mem 0 1 2 3 0 0 v hv, xicyc_of_mem 0 1 2 3 0 0 w hw]
                    simp [Vicyc] at hv hw
                    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xicyc, beq_iff_eq, *] <;> omega
                  rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                  have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 0 0) hpatt
                  by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                  · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 0 0)).Nodup) (hn.mp hnd))
                  · rw [if_neg hnd]; native_decide
                ·
                  by_cases hfb : f = b
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 2 3 0 1 v == xicyc 0 1 2 3 0 1 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 2 3 0 1 v hv, xicyc_of_mem 0 1 2 3 0 1 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 0 1) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 0 1)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfc : f = c
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 2 3 0 2 v == xicyc 0 1 2 3 0 2 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 2 3 0 2 v hv, xicyc_of_mem 0 1 2 3 0 2 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 0 2) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 0 2)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfd : f = d
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 2 3 0 3 v == xicyc 0 1 2 3 0 3 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 2 3 0 3 v hv, xicyc_of_mem 0 1 2 3 0 3 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 0 3) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 0 3)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 2 3 0 5 v == xicyc 0 1 2 3 0 5 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 2 3 0 5 v hv, xicyc_of_mem 0 1 2 3 0 5 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 0 5) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 0 5)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
              ·
                by_cases heb : e = b
                ·
                  by_cases hfa : f = a
                  ·
                    have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                        (xicyc a b c d e f v == xicyc a b c d e f w) =
                        (xicyc 0 1 2 3 1 0 v == xicyc 0 1 2 3 1 0 w) := by
                      intro v hv w hw
                      rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                        xicyc_of_mem 0 1 2 3 1 0 v hv, xicyc_of_mem 0 1 2 3 1 0 w hw]
                      simp [Vicyc] at hv hw
                      rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xicyc, beq_iff_eq, *] <;> omega
                    rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                    have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 1 0) hpatt
                    by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                    · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 1 0)).Nodup) (hn.mp hnd))
                    · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hfb : f = b
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 2 3 1 1 v == xicyc 0 1 2 3 1 1 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 2 3 1 1 v hv, xicyc_of_mem 0 1 2 3 1 1 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 1 1) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 1 1)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfc : f = c
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 2 3 1 2 v == xicyc 0 1 2 3 1 2 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 2 3 1 2 v hv, xicyc_of_mem 0 1 2 3 1 2 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 1 2) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 1 2)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        by_cases hfd : f = d
                        ·
                          have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                              (xicyc a b c d e f v == xicyc a b c d e f w) =
                              (xicyc 0 1 2 3 1 3 v == xicyc 0 1 2 3 1 3 w) := by
                            intro v hv w hw
                            rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                              xicyc_of_mem 0 1 2 3 1 3 v hv, xicyc_of_mem 0 1 2 3 1 3 w hw]
                            simp [Vicyc] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xicyc, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 1 3) hpatt
                          by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 1 3)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                        ·
                          have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                              (xicyc a b c d e f v == xicyc a b c d e f w) =
                              (xicyc 0 1 2 3 1 5 v == xicyc 0 1 2 3 1 5 w) := by
                            intro v hv w hw
                            rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                              xicyc_of_mem 0 1 2 3 1 5 v hv, xicyc_of_mem 0 1 2 3 1 5 w hw]
                            simp [Vicyc] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xicyc, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 1 5) hpatt
                          by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 1 5)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                ·
                  by_cases hec : e = c
                  ·
                    by_cases hfa : f = a
                    ·
                      have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                          (xicyc a b c d e f v == xicyc a b c d e f w) =
                          (xicyc 0 1 2 3 2 0 v == xicyc 0 1 2 3 2 0 w) := by
                        intro v hv w hw
                        rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                          xicyc_of_mem 0 1 2 3 2 0 v hv, xicyc_of_mem 0 1 2 3 2 0 w hw]
                        simp [Vicyc] at hv hw
                        rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xicyc, beq_iff_eq, *] <;> omega
                      rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                      have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 2 0) hpatt
                      by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                      · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 2 0)).Nodup) (hn.mp hnd))
                      · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfb : f = b
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 2 3 2 1 v == xicyc 0 1 2 3 2 1 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 2 3 2 1 v hv, xicyc_of_mem 0 1 2 3 2 1 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 2 1) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 2 1)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        by_cases hfc : f = c
                        ·
                          have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                              (xicyc a b c d e f v == xicyc a b c d e f w) =
                              (xicyc 0 1 2 3 2 2 v == xicyc 0 1 2 3 2 2 w) := by
                            intro v hv w hw
                            rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                              xicyc_of_mem 0 1 2 3 2 2 v hv, xicyc_of_mem 0 1 2 3 2 2 w hw]
                            simp [Vicyc] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xicyc, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 2 2) hpatt
                          by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 2 2)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                        ·
                          by_cases hfd : f = d
                          ·
                            have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                                (xicyc a b c d e f v == xicyc a b c d e f w) =
                                (xicyc 0 1 2 3 2 3 v == xicyc 0 1 2 3 2 3 w) := by
                              intro v hv w hw
                              rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                                xicyc_of_mem 0 1 2 3 2 3 v hv, xicyc_of_mem 0 1 2 3 2 3 w hw]
                              simp [Vicyc] at hv hw
                              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                simp [xicyc, beq_iff_eq, *] <;> omega
                            rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                            have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 2 3) hpatt
                            by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 2 3)).Nodup) (hn.mp hnd))
                            · rw [if_neg hnd]; native_decide
                          ·
                            have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                                (xicyc a b c d e f v == xicyc a b c d e f w) =
                                (xicyc 0 1 2 3 2 5 v == xicyc 0 1 2 3 2 5 w) := by
                              intro v hv w hw
                              rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                                xicyc_of_mem 0 1 2 3 2 5 v hv, xicyc_of_mem 0 1 2 3 2 5 w hw]
                              simp [Vicyc] at hv hw
                              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                simp [xicyc, beq_iff_eq, *] <;> omega
                            rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                            have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 2 5) hpatt
                            by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 2 5)).Nodup) (hn.mp hnd))
                            · rw [if_neg hnd]; native_decide
                  ·
                    by_cases hed : e = d
                    ·
                      by_cases hfa : f = a
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 2 3 3 0 v == xicyc 0 1 2 3 3 0 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 2 3 3 0 v hv, xicyc_of_mem 0 1 2 3 3 0 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 3 0) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 3 0)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        by_cases hfb : f = b
                        ·
                          have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                              (xicyc a b c d e f v == xicyc a b c d e f w) =
                              (xicyc 0 1 2 3 3 1 v == xicyc 0 1 2 3 3 1 w) := by
                            intro v hv w hw
                            rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                              xicyc_of_mem 0 1 2 3 3 1 v hv, xicyc_of_mem 0 1 2 3 3 1 w hw]
                            simp [Vicyc] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xicyc, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 3 1) hpatt
                          by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 3 1)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                        ·
                          by_cases hfc : f = c
                          ·
                            have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                                (xicyc a b c d e f v == xicyc a b c d e f w) =
                                (xicyc 0 1 2 3 3 2 v == xicyc 0 1 2 3 3 2 w) := by
                              intro v hv w hw
                              rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                                xicyc_of_mem 0 1 2 3 3 2 v hv, xicyc_of_mem 0 1 2 3 3 2 w hw]
                              simp [Vicyc] at hv hw
                              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                simp [xicyc, beq_iff_eq, *] <;> omega
                            rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                            have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 3 2) hpatt
                            by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 3 2)).Nodup) (hn.mp hnd))
                            · rw [if_neg hnd]; native_decide
                          ·
                            by_cases hfd : f = d
                            ·
                              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                                  (xicyc 0 1 2 3 3 3 v == xicyc 0 1 2 3 3 3 w) := by
                                intro v hv w hw
                                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                                  xicyc_of_mem 0 1 2 3 3 3 v hv, xicyc_of_mem 0 1 2 3 3 3 w hw]
                                simp [Vicyc] at hv hw
                                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                  simp [xicyc, beq_iff_eq, *] <;> omega
                              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 3 3) hpatt
                              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 3 3)).Nodup) (hn.mp hnd))
                              · rw [if_neg hnd]; native_decide
                            ·
                              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                                  (xicyc 0 1 2 3 3 5 v == xicyc 0 1 2 3 3 5 w) := by
                                intro v hv w hw
                                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                                  xicyc_of_mem 0 1 2 3 3 5 v hv, xicyc_of_mem 0 1 2 3 3 5 w hw]
                                simp [Vicyc] at hv hw
                                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                  simp [xicyc, beq_iff_eq, *] <;> omega
                              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 3 5) hpatt
                              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 3 5)).Nodup) (hn.mp hnd))
                              · rw [if_neg hnd]; native_decide
                    ·
                      by_cases hfa : f = a
                      ·
                        have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                            (xicyc a b c d e f v == xicyc a b c d e f w) =
                            (xicyc 0 1 2 3 4 0 v == xicyc 0 1 2 3 4 0 w) := by
                          intro v hv w hw
                          rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                            xicyc_of_mem 0 1 2 3 4 0 v hv, xicyc_of_mem 0 1 2 3 4 0 w hw]
                          simp [Vicyc] at hv hw
                          rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xicyc, beq_iff_eq, *] <;> omega
                        rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                        have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 4 0) hpatt
                        by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                        · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 4 0)).Nodup) (hn.mp hnd))
                        · rw [if_neg hnd]; native_decide
                      ·
                        by_cases hfb : f = b
                        ·
                          have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                              (xicyc a b c d e f v == xicyc a b c d e f w) =
                              (xicyc 0 1 2 3 4 1 v == xicyc 0 1 2 3 4 1 w) := by
                            intro v hv w hw
                            rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                              xicyc_of_mem 0 1 2 3 4 1 v hv, xicyc_of_mem 0 1 2 3 4 1 w hw]
                            simp [Vicyc] at hv hw
                            rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xicyc, beq_iff_eq, *] <;> omega
                          rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                          have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 4 1) hpatt
                          by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                          · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 4 1)).Nodup) (hn.mp hnd))
                          · rw [if_neg hnd]; native_decide
                        ·
                          by_cases hfc : f = c
                          ·
                            have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                                (xicyc a b c d e f v == xicyc a b c d e f w) =
                                (xicyc 0 1 2 3 4 2 v == xicyc 0 1 2 3 4 2 w) := by
                              intro v hv w hw
                              rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                                xicyc_of_mem 0 1 2 3 4 2 v hv, xicyc_of_mem 0 1 2 3 4 2 w hw]
                              simp [Vicyc] at hv hw
                              rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                simp [xicyc, beq_iff_eq, *] <;> omega
                            rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                            have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 4 2) hpatt
                            by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                            · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 4 2)).Nodup) (hn.mp hnd))
                            · rw [if_neg hnd]; native_decide
                          ·
                            by_cases hfd : f = d
                            ·
                              have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                                  (xicyc a b c d e f v == xicyc a b c d e f w) =
                                  (xicyc 0 1 2 3 4 3 v == xicyc 0 1 2 3 4 3 w) := by
                                intro v hv w hw
                                rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                                  xicyc_of_mem 0 1 2 3 4 3 v hv, xicyc_of_mem 0 1 2 3 4 3 w hw]
                                simp [Vicyc] at hv hw
                                rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                  rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                  simp [xicyc, beq_iff_eq, *] <;> omega
                              rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                              have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 4 3) hpatt
                              by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                              · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 4 3)).Nodup) (hn.mp hnd))
                              · rw [if_neg hnd]; native_decide
                            ·
                              by_cases hfe : f = e
                              ·
                                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                                    (xicyc 0 1 2 3 4 4 v == xicyc 0 1 2 3 4 4 w) := by
                                  intro v hv w hw
                                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                                    xicyc_of_mem 0 1 2 3 4 4 v hv, xicyc_of_mem 0 1 2 3 4 4 w hw]
                                  simp [Vicyc] at hv hw
                                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                    simp [xicyc, beq_iff_eq, *] <;> omega
                                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 4 4) hpatt
                                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                                · rw [if_pos hnd]; exact False.elim ((by native_decide : ¬ (Vicyc.map (xicyc 0 1 2 3 4 4)).Nodup) (hn.mp hnd))
                                · rw [if_neg hnd]; native_decide
                              ·
                                have hpatt : ∀ v ∈ Vicyc, ∀ w ∈ Vicyc,
                                    (xicyc a b c d e f v == xicyc a b c d e f w) =
                                    (xicyc 0 1 2 3 4 5 v == xicyc 0 1 2 3 4 5 w) := by
                                  intro v hv w hw
                                  rw [xicyc_of_mem a b c d e f v hv, xicyc_of_mem a b c d e f w hw,
                                    xicyc_of_mem 0 1 2 3 4 5 v hv, xicyc_of_mem 0 1 2 3 4 5 w hw]
                                  simp [Vicyc] at hv hw
                                  rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                    rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                    simp [xicyc, beq_iff_eq, *] <;> omega
                                rw [CO_eq_of_eqpattern Vicyc _ _ hpatt]
                                have hn := nodup_map_eq_of_eqpattern Vicyc (xicyc a b c d e f) (xicyc 0 1 2 3 4 5) hpatt
                                by_cases hnd : (Vicyc.map (xicyc a b c d e f)).Nodup
                                · rw [if_pos hnd]; native_decide
                                · rw [if_neg hnd]; exact False.elim (hnd (hn.mpr (by native_decide : (Vicyc.map (xicyc 0 1 2 3 4 5)).Nodup)))


private theorem sound_CollisionCorrection_icyc : CollisionCorrection.Goal := by
  intro n db b
  let F : Bind → Int := fun x => (pattern.atoms.map (fun a => value db a x)).prod
  have hF : ∀ x y, (∀ v ∈ pattern.freeVars, x v = y v) → F x = F y := by
    intro x y hxy
    dsimp [F]
    congr 1
    apply List.map_congr_left
    intro a ha
    dsimp [value]
    have haV := atom_mem a ha
    rw [hxy a.src haV.1, hxy a.dst haV.2]
  have hR : ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b bs)).sum =
      integrate n pattern.freeVars b (fun x =>
        ((partitions pattern.freeVars).map (fun bs => mobius bs * (if constOn bs x then F x else 0))).sum) := by
    rw [integrate_list_sum]
    apply sum_map_congr_int
    intro bs hbs
    rw [integrate_mul_left]
    have hcs := collapse_sum n b F bs hbs hF
    have hcoll : collapsed pattern n db b bs =
        integrate n (Heads bs) b (fun y => F (fun v => y (Root bs v))) := by rfl
    rw [hcoll]
    exact congrArg (fun z => mobius bs * z) hcs
  unfold injective
  rw [hR]
  symm
  apply integrate_congr
  intro x
  let y : Bind := xicyc (x 4) (x 5) (x 1) (x 2) (x 3) (x 0)
  have hpatt : ∀ v ∈ pattern.freeVars, ∀ w ∈ pattern.freeVars, (x v == x w) = (y v == y w) := by
    intro v hv w hw
    simp [pattern] at hv hw
    rcases hv with rfl | rfl | rfl | rfl | rfl | rfl <;>
      rcases hw with rfl | rfl | rfl | rfl | rfl | rfl <;> simp [y, xicyc]
  have hco := coeff_icyc (x 4) (x 5) (x 1) (x 2) (x 3) (x 0)
  have hco' : CO pattern.freeVars x = (if (pattern.freeVars.map x).Nodup then (1:Int) else 0) := by
    have hc := CO_eq_of_eqpattern pattern.freeVars x y hpatt
    have hn := nodup_map_eq_of_eqpattern pattern.freeVars x y hpatt
    rw [hc]
    change CO pattern.freeVars y = if (pattern.freeVars.map x).Nodup then (1:Int) else 0
    have hco_y : CO pattern.freeVars y = (if (pattern.freeVars.map y).Nodup then (1:Int) else 0) := by
      simpa [y, pattern] using hco
    by_cases hndx : (pattern.freeVars.map x).Nodup
    · have hndy : (pattern.freeVars.map y).Nodup := hn.mp hndx
      rw [if_pos hndx, hco_y, if_pos hndy]
    · have hndy : ¬ (pattern.freeVars.map y).Nodup := by intro h; exact hndx (hn.mpr h)
      rw [if_neg hndx, hco_y, if_neg hndy]
  change ((partitions pattern.freeVars).map (fun bs => mobius bs * (if constOn bs x then F x else 0))).sum =
    (if (pattern.freeVars.map x).Nodup then F x else 0)
  rw [factor_constOn]
  change F x * CO pattern.freeVars x = (if (pattern.freeVars.map x).Nodup then F x else 0)
  rw [hco']
  by_cases hnd : (pattern.freeVars.map x).Nodup <;> simp [hnd]

end Model_icyc

theorem HubWithChords_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.HubWithChords.CollisionCorrection.Goal :=
  Model_hub.sound_CollisionCorrection_hub

theorem IntersectingCycles_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.IntersectingCycles.CollisionCorrection.Goal :=
  Model_icyc.sound_CollisionCorrection_icyc

theorem HubWithChords_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.HubWithChords.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.HubWithChordsFractionalCap.sound_FractionalCap

theorem SupplierClique_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.SupplierCliqueFractionalCap.SupplierClique.sound_FractionalCap

theorem SupplierClique_OrbitRepresentative :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.OrbitRepresentative.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.SupplierCliqueCollisionAndOrbit.sound_OrbitRepresentative

theorem SupplierClique_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.CollisionCorrection.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.SupplierCliqueCollisionAndOrbit.sound_CollisionCorrection

theorem IntersectingCycles_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.IntersectingCycles.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.Goal_IntersectingCycles_FractionalCap.sound_IntersectingCycles_FractionalCap

theorem ParallelLineage_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ParallelLineage.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.ParallelLineageCapsAndCollisionCorrection.sound_FractionalCap

theorem ParallelLineage_DeltaForecast :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ParallelLineage.DeltaForecast.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.ParallelLineageDeltaForecast.sound_DeltaForecast

theorem ParallelLineage_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ParallelLineage.CollisionCorrection.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.ParallelLineageCapsAndCollisionCorrection.sound_CollisionCorrection

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.ChordCliqueAndLineageJoinCaps