import TinylambOpt.ResearchGoals.SqlFetchWithTies

/-!
Proved

- `Goal` — The stable tie order used here is stream concatenation order. For the
  incremental sorter, occurrence IDs encode that order.

The reference model is defined in `TinylambOpt.ResearchGoals.SqlFetchWithTies`.
-/

namespace TinylambOpt.ResearchProofs.SqlFetchWithTies

open TinylambOpt.ResearchGoals.SqlFetchWithTies hiding Goal
open TinylambOpt.Research
open List

attribute [local instance] boolRelToRel

/-!
# `FETCH FIRST k ROWS WITH TIES` on the watermark and the incremental page

Both goals compare an early-stopping engine against a full-scan SQL oracle.
The oracle sorts with `List.mergeSort` under `lex ... != gt`, keeps rows whose
strict rank (`lex ... == lt` count) is below `k`, and the engines stop at the
boundary key of the `k`-th row via `takeWhile`.  We show the emitted lists are
equal.  For `k = 0` the goals module already proves both sides empty
(`zero_sql`).

The `lex` order is only transitive and total on keys of full length, so the
merge-sort facts are transported through `padKey` (padding with `none`) onto
the TieWatermark `keyLe` order, exactly as in `DistributedPageFence.lean`.
-/

/-! ## Shared `List` helpers -/

private theorem rel_of_pairwise_append {α : Type} {R : α → α → Prop} {u v : List α}
    (h : List.Pairwise R (u ++ v)) :
    ∀ y ∈ u, ∀ z ∈ v, R y z := by
  induction u with
  | nil => intro y hy; cases hy
  | cons a as ih =>
      rw [List.cons_append] at h
      obtain ⟨h1, h2⟩ := List.pairwise_cons.mp h
      intro y hy z hz
      rcases List.mem_cons.mp hy with rfl | hy'
      · exact h1 z (List.mem_append.mpr (Or.inr hz))
      · exact ih h2 y hy' z hz

private theorem exists_append_last {α : Type} {l : List α} (h : l ≠ []) :
    ∃ w c, l = w ++ [c] := by
  cases l with
  | nil => exact absurd rfl h
  | cons a as =>
      cases as with
      | nil => exact ⟨[], a, by simp⟩
      | cons b bs =>
          obtain ⟨w, c, hw⟩ :=
            exists_append_last (show (b :: bs : List α) ≠ [] from by simp)
          exact ⟨a :: w, c, by rw [List.cons_append, hw]⟩

private theorem getLast?_snoc {α : Type} (l : List α) (a : α) :
    (l ++ [a]).getLast? = some a := by
  rw [List.getLast?_append]
  simp

private theorem filter_all {α : Type} (l : List α) (p : α → Bool)
    (h : ∀ x ∈ l, p x = true) : l.filter p = l := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      rw [List.filter_cons_of_pos (h x (by simp)),
        ih (fun y hy => h y (by simp [hy]))]

private theorem filter_eq_nil_of_false {α : Type} (l : List α) (p : α → Bool)
    (h : ∀ x ∈ l, p x = false) : l.filter p = [] := by
  refine List.filter_eq_nil_iff.mpr ?_
  intro x hx
  simp [h x hx]

private theorem filter_congr_on {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.filter p = l.filter q := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have hx : p x = q x := h x (by simp)
      cases hp : p x with
      | true =>
          rw [List.filter_cons_of_pos hp,
            List.filter_cons_of_pos (by rw [← hx]; exact hp)]
          congr 1
          exact ih (fun y hy => h y (by simp [hy]))
      | false =>
          have hpn : ¬ p x = true := by simpa using hp
          have hq : ¬ q x = true := fun hqx => hpn (by rw [hx]; exact hqx)
          rw [List.filter_cons_of_neg hpn, List.filter_cons_of_neg hq]
          exact ih (fun y hy => h y (by simp [hy]))

private theorem length_filter_lt {α : Type} (l : List α) (p : α → Bool) {x : α}
    (hx : x ∈ l) (hpx : p x = false) : (l.filter p).length < l.length := by
  induction l with
  | nil => cases hx
  | cons a as ih =>
      rcases List.mem_cons.mp hx with rfl | hx'
      · rw [List.filter_cons_of_neg (by rw [hpx]; decide)]
        have hle := List.length_filter_le p as
        simp only [List.length_cons] at hle ⊢
        omega
      · have h := ih hx'
        cases ha : p a with
        | true =>
            rw [List.filter_cons_of_pos ha]
            simp only [List.length_cons] at h ⊢
            omega
        | false =>
            rw [List.filter_cons_of_neg (by simpa using ha)]
            have hle := List.length_filter_le p as
            simp only [List.length_cons] at hle ⊢
            omega

private theorem decide_congr_bool {p q : Prop} [Decidable p] [Decidable q] (h : p ↔ q) :
    decide p = decide q := by
  by_cases hp : p
  · rw [decide_eq_true hp, decide_eq_true (h.mp hp)]
  · rw [decide_eq_false hp, decide_eq_false (fun hq => hp (h.mpr hq))]

/-! ## Padded keys (as in `DistributedPageFence.lean`) -/

section
open TinylambOpt.Research TieWatermark

private def padKey : Spec → Key → Key
  | [], _ => []
  | _ :: ds, [] => none :: padKey ds []
  | _ :: ds, x :: xs => x :: padKey ds xs

private theorem padKey_length (spec : Spec) (a : Key) :
    (padKey spec a).length = spec.length := by
  induction spec generalizing a with
  | nil => cases a <;> rfl
  | cons s spec ih =>
      cases a with
      | nil =>
          show (none :: padKey spec []).length = spec.length + 1
          rw [length_cons, ih]
      | cons x xs =>
          show (x :: padKey spec xs).length = spec.length + 1
          rw [length_cons, ih]

private theorem padKey_id {spec : Spec} (a : Key) (ha : a.length = spec.length) :
    padKey spec a = a := by
  induction spec generalizing a with
  | nil =>
      cases a with
      | nil => rfl
      | cons x xs => exact absurd ha (by simp [length_cons])
  | cons s spec ih =>
      cases a with
      | nil => exact absurd ha (by simp)
      | cons x xs =>
          have hxs : xs.length = spec.length := by
            rw [length_cons, length_cons] at ha; omega
          show x :: padKey spec xs = x :: xs
          rw [ih xs hxs]

/-! ## Integer / component comparison facts -/

private theorem boolNe {b : Bool} (h1 : b = true) (h2 : b = false) : False := by
  rw [h2] at h1
  exact absurd h1 (by decide)

private theorem intCmp_lt {x y : Int} : (compare x y = Ordering.lt) ↔ (x < y) := by
  show (compareOfLessAndEq x y = Ordering.lt) ↔ (x < y)
  by_cases h : x < y
  · rw [compareOfLessAndEq, if_pos h]
    exact ⟨fun _ => h, fun _ => rfl⟩
  · rw [compareOfLessAndEq, if_neg h]
    by_cases h' : x = y
    · rw [if_pos h']
      exact ⟨fun he => absurd he (by decide), fun hyx => absurd hyx h⟩
    · rw [if_neg h']
      exact ⟨fun he => absurd he (by decide), fun hyx => absurd hyx h⟩

private theorem intCmp_eq {x y : Int} : (compare x y = Ordering.eq) ↔ (x = y) := by
  show (compareOfLessAndEq x y = Ordering.eq) ↔ (x = y)
  by_cases h : x < y
  · rw [compareOfLessAndEq, if_pos h]
    exact ⟨fun he => absurd he (by decide), fun hyx => absurd (hyx ▸ h) (Int.lt_irrefl x)⟩
  · rw [compareOfLessAndEq, if_neg h]
    by_cases h' : x = y
    · rw [if_pos h']
      exact ⟨fun _ => h', fun _ => rfl⟩
    · rw [if_neg h']
      exact ⟨fun he => absurd he (by decide), fun hyx => absurd hyx h'⟩

private theorem intCmp_gt {x y : Int} : (compare x y = Ordering.gt) ↔ (y < x) := by
  show (compareOfLessAndEq x y = Ordering.gt) ↔ (y < x)
  by_cases h : x < y
  · rw [compareOfLessAndEq, if_pos h]
    exact ⟨fun he => absurd he (by decide), fun hyx => absurd h (Int.lt_asymm hyx)⟩
  · rw [compareOfLessAndEq, if_neg h]
    by_cases h' : x = y
    · rw [if_pos h']
      exact ⟨fun he => absurd he (by decide),
        fun hyx => absurd (by rw [h'] at hyx; exact hyx) (Int.lt_irrefl y)⟩
    · rw [if_neg h']
      refine ⟨fun _ => ?_, fun _ => rfl⟩
      rcases Int.lt_trichotomy x y with h1 | h1 | h1
      · exact absurd h1 h
      · exact absurd h1 h'
      · exact h1

private theorem intCmp_swap {x y : Int} : compare y x = (compare x y).swap := by
  rcases Int.lt_trichotomy x y with h | h | h
  · rw [intCmp_lt.mpr h, intCmp_gt.mpr h]
    rfl
  · subst h
    have hc : compare x x = Ordering.eq := intCmp_eq.mpr rfl
    rw [hc]
    rfl
  · rw [intCmp_gt.mpr h, intCmp_lt.mpr h]
    rfl

private theorem optCmp_trich {d : Dir} {n : Nulls} {a b : Option Int} :
    component d n a b = Ordering.lt ∨ component d n a b = Ordering.eq ∨
      component d n a b = Ordering.gt := by
  have key : ∀ (o : Ordering), o = Ordering.lt ∨ o = Ordering.eq ∨ o = Ordering.gt := by
    intro o; cases o <;> simp
  exact key (component d n a b)

private theorem optCmp_eq_iff {d : Dir} {n : Nulls} {a b : Option Int} :
    component d n a b = Ordering.eq ↔ a = b := by
  cases d <;> cases n <;> cases a <;> cases b <;>
    simp [component, intCmp_eq] <;> (try omega)

private theorem optCmp_lt_gt {d : Dir} {n : Nulls} {a b : Option Int} :
    (component d n a b = Ordering.lt) ↔ (component d n b a = Ordering.gt) := by
  cases d <;> cases n <;> cases a <;> cases b <;>
    simp [component, intCmp_lt, intCmp_gt]

private theorem valLt_comp {d : Dir} {n : Nulls} {a b : Option Int} :
    valLt d n a b = (component d n a b = Ordering.lt) := by
  cases d <;> cases n <;> cases a <;> cases b <;>
    simp [valLt, optLt, component, intCmp_lt] <;> try decide

private theorem lex_step {ds : Spec} {d : Dir} {n : Nulls} {x : Option Int} {xs : Key}
    {y : Option Int} {ys : Key} :
    lex ((d, n) :: ds) (x :: xs) (y :: ys) =
      match component d n x y with
      | Ordering.eq => lex ds xs ys
      | c => c := rfl

private theorem lex_nil_left (spec : Spec) (a : Key) : lex spec [] a = Ordering.eq := by
  cases spec <;> cases a <;> rfl

private theorem lex_nil_right (spec : Spec) (a : Key) : lex spec a [] = Ordering.eq := by
  cases spec <;> cases a <;> rfl

private theorem lex_refl (spec : Spec) (a : Key) : lex spec a a = Ordering.eq := by
  induction spec generalizing a with
  | nil => cases a <;> rfl
  | cons s spec ih =>
      cases s with
      | mk d n =>
          cases a with
          | nil => rfl
          | cons x xs =>
              rw [lex_step]
              have hxx : component d n x x = Ordering.eq :=
                (optCmp_eq_iff (d := d) (n := n)).mpr rfl
              rw [hxx]
              exact ih xs

private theorem lex_trich (spec : Spec) (a b : Key) :
    lex spec a b = Ordering.lt ∨ lex spec a b = Ordering.eq ∨ lex spec a b = Ordering.gt := by
  have key : ∀ (o : Ordering), o = Ordering.lt ∨ o = Ordering.eq ∨ o = Ordering.gt := by
    intro o; cases o <;> simp
  exact key (lex spec a b)

private theorem lex_swap (spec : Spec) : ∀ a b : Key, lex spec b a = (lex spec a b).swap := by
  induction spec with
  | nil =>
      intro a b
      rfl
  | cons s spec ih =>
      cases s with
      | mk d n =>
          intro a
          cases a with
          | nil =>
              intro b
              cases b with
              | nil => rfl
              | cons y ys => rw [lex_nil_left, lex_nil_right]; rfl
          | cons x as =>
              intro b
              cases b with
              | nil => rw [lex_nil_left, lex_nil_right]; rfl
              | cons y bs =>
                  rcases optCmp_trich (d := d) (n := n) (a := x) (b := y) with hc | hc | hc
                  · have h1 : lex ((d, n) :: spec) (y :: bs) (x :: as) = Ordering.gt := by
                      rw [lex_step, optCmp_lt_gt (d := d) (n := n) (a := x) (b := y) |>.mp hc]
                    have h2 : lex ((d, n) :: spec) (x :: as) (y :: bs) = Ordering.lt := by
                      rw [lex_step, hc]
                    rw [h1, h2]
                    rfl
                  · have hxy : x = y := (optCmp_eq_iff (d := d) (n := n)).mp hc
                    subst y
                    have hxx : component d n x x = Ordering.eq :=
                      (optCmp_eq_iff (d := d) (n := n)).mpr rfl
                    have h1 : lex ((d, n) :: spec) (x :: bs) (x :: as) = lex spec bs as := by
                      rw [lex_step, hxx]
                    have h2 : lex ((d, n) :: spec) (x :: as) (x :: bs) = lex spec as bs := by
                      rw [lex_step, hxx]
                    rw [h1, h2]
                    exact ih as bs
                  · have h1 : lex ((d, n) :: spec) (y :: bs) (x :: as) = Ordering.lt := by
                      rw [lex_step,
                        optCmp_lt_gt (d := d) (n := n) (a := y) (b := x) |>.mpr hc]
                    have h2 : lex ((d, n) :: spec) (x :: as) (y :: bs) = Ordering.gt := by
                      rw [lex_step, hc]
                    rw [h1, h2]
                    rfl

private theorem lex_eq_bounded {spec : Spec} {a b : Key}
    (ha : a.length = spec.length) (hb : b.length = spec.length)
    (h : lex spec a b = Ordering.eq) : a = b := by
  induction spec generalizing a b with
  | nil =>
      have : a = [] := eq_nil_of_length_eq_zero (by simpa using ha)
      have : b = [] := eq_nil_of_length_eq_zero (by simpa using hb)
      subst a; subst b; rfl
  | cons s spec ih =>
      cases s with
      | mk d n =>
          cases a with
          | nil => simp at ha
          | cons x xs =>
              cases b with
              | nil => simp at hb
              | cons y ys =>
                  have hxy : x = y := by
                    rcases optCmp_trich (d := d) (n := n) (a := x) (b := y) with hc | hc | hc
                    · have h2 : lex ((d, n) :: spec) (x :: xs) (y :: ys) = Ordering.lt := by
                        rw [lex_step, hc]
                      rw [h2] at h
                      exact absurd h (by decide)
                    · exact (optCmp_eq_iff (d := d) (n := n)).mp hc
                    · have h2 : lex ((d, n) :: spec) (x :: xs) (y :: ys) = Ordering.gt := by
                        rw [lex_step, hc]
                      rw [h2] at h
                      exact absurd h (by decide)
                  subst y
                  have hxs : xs.length = spec.length := by
                    rw [length_cons, length_cons] at ha hb; omega
                  have hys : ys.length = spec.length := by
                    rw [length_cons, length_cons] at ha hb; omega
                  have htail : lex spec xs ys = Ordering.eq := by
                    have hc : component d n x x = Ordering.eq :=
                      (optCmp_eq_iff (d := d) (n := n)).mpr rfl
                    rw [lex_step, hc] at h
                    exact h
                  exact congrArg (List.cons x) (ih (a := xs) (b := ys) hxs hys htail)

/-! ## Key orders: `kLe` (≤) and `kLt` (<) on keys -/

private def kLe (spec : Spec) (a b : Key) : Prop := lex spec a b ≠ Ordering.gt
private def kLt (spec : Spec) (a b : Key) : Prop := lex spec a b = Ordering.lt

private theorem kLe_of_eq (spec : Spec) {a b : Key} (h : a = b) : kLe spec a b := by
  subst h
  intro hc
  have hr := lex_refl spec a
  rw [hr] at hc
  exact absurd hc (by decide)

private theorem kLe_refl (spec : Spec) (a : Key) : kLe spec a a := kLe_of_eq _ rfl

private theorem kLt_swap {spec : Spec} {a b : Key} (h : kLt spec a b) :
    lex spec b a = Ordering.gt := by
  rw [lex_swap, h]
  rfl

private theorem kLe_of_kLt {spec : Spec} {a b : Key} (h : kLt spec a b) : kLe spec a b := by
  intro hc
  rw [kLt] at h
  rw [hc] at h
  exact absurd h (by decide)

private theorem kLe_antisym {spec : Spec} {a b : Key}
    (ha : a.length = spec.length) (hb : b.length = spec.length)
    (h1 : kLe spec a b) (h2 : kLe spec b a) : a = b := by
  have heq : lex spec a b = Ordering.eq := by
    rcases lex_trich spec a b with hlt | heq | hgt
    · exact absurd (kLt_swap hlt) h2
    · exact heq
    · exact absurd hgt h1
  exact lex_eq_bounded ha hb heq

/-! ### Bridge to the TieWatermark order `keyLe` (bounded keys). -/

private theorem bridge_le {spec : Spec} {a b : Key}
    (ha : a.length = spec.length) (hb : b.length = spec.length) :
    (kLe spec a b ↔ keyLe spec a b = true) := by
  induction spec generalizing a b with
  | nil =>
      have hna : a = [] := eq_nil_of_length_eq_zero (by simpa using ha)
      have hnb : b = [] := eq_nil_of_length_eq_zero (by simpa using hb)
      subst a; subst b
      exact ⟨fun _ => rfl, fun _ hc => by rw [lex_nil_right] at hc; exact absurd hc (by decide)⟩
  | cons s spec ih =>
      cases s with
      | mk d n =>
          cases a with
          | nil => exact absurd ha (by simp)
          | cons x xs =>
              cases b with
              | nil => exact absurd hb (by simp)
              | cons y ys =>
                  have hxs : xs.length = spec.length := by
                    rw [length_cons, length_cons] at ha hb; omega
                  have hys : ys.length = spec.length := by
                    rw [length_cons, length_cons] at ha hb; omega
                  have ih' := ih (a := xs) (b := ys) hxs hys
                  rw [kLe, lex_step, keyLe]
                  rcases optCmp_trich (d := d) (n := n) (a := x) (b := y) with hlt | heq | hgt
                  · have hv : valLt d n x y = true := by
                      rw [valLt_comp (d := d) (n := n) (a := x) (b := y), hlt]
                    rw [hlt, hv]
                    simp
                  · have hxy : x = y := (optCmp_eq_iff (d := d) (n := n)).mp heq
                    subst y
                    have hv : valLt d n x x = false := valLt_irrefl d n x
                    rw [heq, hv]
                    have h1 : decide (x = x) = true := decide_eq_true rfl
                    rw [h1, Bool.false_or, Bool.true_and]
                    exact ih'
                  · have hv : valLt d n x y = false :=
                      Bool.eq_false_of_ne_true fun htrue =>
                        absurd (valLt_comp ▸ htrue) (by rw [hgt]; decide)
                    have hne : x ≠ y := by
                      intro hxy
                      have he := (optCmp_eq_iff (d := d) (n := n)).mpr hxy
                      rw [hgt] at he
                      exact absurd he (by decide)
                    have hd : decide (x = y) = false := decide_eq_false hne
                    rw [hgt, hv, hd]
                    simp

/-! ### Padded bridge: global trans/total of `kLe` on pads. -/

private theorem bridge_leP {spec : Spec} (a b : Key) :
    (kLe spec (padKey spec a) (padKey spec b) ↔
      keyLe spec (padKey spec a) (padKey spec b) = true) :=
  bridge_le (padKey_length spec a) (padKey_length spec b)

private theorem kLe_trans {spec : Spec} {a b c : Key}
    (ha : a.length = spec.length) (hb : b.length = spec.length) (hc : c.length = spec.length)
    (h1 : kLe spec a b) (h2 : kLe spec b c) : kLe spec a c := by
  have k1 : keyLe spec a b = true := (bridge_le ha hb).mp h1
  have k2 : keyLe spec b c = true := (bridge_le hb hc).mp h2
  exact (bridge_le ha hc).mpr (keyLe_trans spec a b c k1 k2)

private theorem kLe_total {spec : Spec} (a b : Key)
    (ha : a.length = spec.length) (hb : b.length = spec.length) :
    kLe spec a b ∨ kLe spec b a := by
  have ht := keyLe_total spec a b
  rw [Bool.or_eq_true_iff] at ht
  rcases ht with h | h
  · exact Or.inl ((bridge_le ha hb).mpr h)
  · exact Or.inr ((bridge_le hb ha).mpr h)

/-! ### Bridge to `better` (strict order), for bounded keys. -/

private theorem kLt_of_kLe_not_kLe {spec : Spec} {a b : Key}
    (h1 : kLe spec a b) (h2 : ¬ kLe spec b a) : kLt spec a b := by
  rcases lex_trich spec a b with hlt | heq | hgt
  · exact hlt
  · exact (h2 fun hc =>
      absurd (by rw [lex_swap, heq]; rfl : lex spec b a = Ordering.eq) (by rw [hc]; decide)).elim
  · exact absurd hgt h1

private theorem boolEqFalse {b : Bool} (h : ¬(b = true)) : b = false := by
  cases b <;> simp_all

private theorem better_trans1 {spec : Spec} {a b c : Key}
    (k1 : keyLe spec a b = true) (h2 : better spec b c = true) : better spec a c = true := by
  rw [better] at h2
  obtain ⟨hbc, h2'⟩ := Bool.and_eq_true_iff.mp h2
  have hcb : keyLe spec c b = false := boolEqFalse fun hh => by
    rw [hh] at h2'; exact absurd h2' (by decide)
  have hac : keyLe spec a c = true := keyLe_trans spec a b c k1 hbc
  have hca : keyLe spec c a = false := boolEqFalse fun hca' =>
    boolNe (keyLe_trans spec c a b hca' k1) hcb
  rw [better, hac, hca]
  rfl

private theorem better_trans2 {spec : Spec} {a b c : Key}
    (h1 : better spec a b = true) (k2 : keyLe spec b c = true) : better spec a c = true := by
  rw [better] at h1
  obtain ⟨hab, h1'⟩ := Bool.and_eq_true_iff.mp h1
  have hba : keyLe spec b a = false := boolEqFalse fun hh => by
    rw [hh] at h1'; exact absurd h1' (by decide)
  have hac : keyLe spec a c = true := keyLe_trans spec a b c hab k2
  have hca : keyLe spec c a = false := boolEqFalse fun hca' =>
    boolNe (keyLe_trans spec b c a k2 hca') hba
  rw [better, hac, hca]
  rfl

private theorem bridge_lt {spec : Spec} {a b : Key}
    (ha : a.length = spec.length) (hb : b.length = spec.length) :
    (kLt spec a b ↔ better spec a b = true) := by
  constructor
  · intro h
    have h1 : keyLe spec a b = true := (bridge_le ha hb).mp (kLe_of_kLt h)
    have h2 : keyLe spec b a = false := boolEqFalse fun hh =>
      absurd (kLt_swap h) ((bridge_le hb ha).mpr hh)
    rw [better, h1, h2]
    rfl
  · intro h
    rw [better] at h
    obtain ⟨h1, h2⟩ := Bool.and_eq_true_iff.mp h
    have h3 : keyLe spec b a = false := boolEqFalse fun hh => by
      rw [hh] at h2; exact absurd h2 (by decide)
    exact kLt_of_kLe_not_kLe ((bridge_le ha hb).mpr h1)
      (fun hh => boolNe ((bridge_le hb ha).mp hh) h3)

private theorem not_kLt_of_kLe {spec : Spec} {a b : Key}
    (h : ¬ kLt spec a b) : kLe spec b a := by
  rcases lex_trich spec a b with hlt | heq | hgt
  · exact absurd hlt h
  · show lex spec b a ≠ Ordering.gt
    rw [lex_swap, heq]
    decide
  · intro hc
    have hba : lex spec b a = Ordering.lt := by rw [lex_swap, hgt]; rfl
    rw [hba] at hc
    exact absurd hc (by decide)

/-! ## Merge congruence -/

private theorem merge_congr {α : Type} :
    ∀ (xs ys : List α) {le₁ le₂ : α → α → Bool},
      (∀ x ∈ xs, ∀ y ∈ ys, le₁ x y = le₂ x y) → merge xs ys le₁ = merge xs ys le₂ := by
  intro xs
  induction xs with
  | nil => intro ys le₁ le₂ _; rw [nil_merge, nil_merge]
  | cons x xs hxs =>
      intro ys
      induction ys with
      | nil => intro le₁ le₂ _; rw [merge_right, merge_right]
      | cons y ys hys =>
          intro le₁ le₂ h
          rw [cons_merge_cons, cons_merge_cons]
          rw [h x (mem_cons_self) y (mem_cons_self)]
          cases _hxy : le₂ x y
          · rw [hys (fun z hz w hw => h z hz w (mem_cons_of_mem y hw))]
            simp
          · rw [hxs (y :: ys) (fun z hz w hw => h z (mem_cons_of_mem x hz) w hw)]
            simp

private theorem mergeSort_congr {α : Type} :
    ∀ (l : List α) {le₁ le₂ : α → α → Bool},
      (∀ x ∈ l, ∀ y ∈ l, le₁ x y = le₂ x y) → mergeSort l le₁ = mergeSort l le₂
  | [], _, _, _ => by simp only [List.mergeSort]
  | [_], _, _, _ => by simp only [List.mergeSort]
  | a :: b :: xs, le₁, le₂, h => by
      have : (MergeSort.Internal.splitInTwo ⟨a :: b :: xs, rfl⟩).1.1.length < xs.length + 1 + 1 :=
        by simp [MergeSort.Internal.splitInTwo_fst]; omega
      have : (MergeSort.Internal.splitInTwo ⟨a :: b :: xs, rfl⟩).2.1.length < xs.length + 1 + 1 :=
        by simp [MergeSort.Internal.splitInTwo_snd]; omega
      rw [mergeSort, mergeSort]
      have hl : (MergeSort.Internal.splitInTwo ⟨a :: b :: xs, rfl⟩).1.1 ++
          (MergeSort.Internal.splitInTwo ⟨a :: b :: xs, rfl⟩).2.1 = a :: b :: xs :=
        MergeSort.Internal.splitInTwo_fst_append_splitInTwo_snd ⟨a :: b :: xs, rfl⟩
      have hf : ∀ x ∈ (MergeSort.Internal.splitInTwo ⟨a :: b :: xs, rfl⟩).1.1,
          ∀ y ∈ (MergeSort.Internal.splitInTwo ⟨a :: b :: xs, rfl⟩).1.1, le₁ x y = le₂ x y := by
        intro x' hx' y' hy'
        exact h x' (hl ▸ mem_append_left _ hx') y' (hl ▸ mem_append_left _ hy')
      have hs : ∀ x ∈ (MergeSort.Internal.splitInTwo ⟨a :: b :: xs, rfl⟩).2.1,
          ∀ y ∈ (MergeSort.Internal.splitInTwo ⟨a :: b :: xs, rfl⟩).2.1, le₁ x y = le₂ x y := by
        intro x' hx' y' hy'
        exact h x' (hl ▸ mem_append_right _ hx') y' (hl ▸ mem_append_right _ hy')
      rw [mergeSort_congr _ hf, mergeSort_congr _ hs]
      refine merge_congr _ _ (fun x' hx' y' hy' => ?_)
      rw [List.mem_mergeSort] at hx' hy'
      exact h x' (hl ▸ mem_append_left _ hx') y' (hl ▸ mem_append_right _ hy')
  termination_by l => l.length

private theorem Pairwise.congr {α : Type} {r₁ r₂ : α → α → Prop} {l : List α}
    (h : Pairwise r₁ l) (hag : ∀ x ∈ l, ∀ y ∈ l, r₁ x y ↔ r₂ x y) : Pairwise r₂ l := by
  induction h with
  | nil => exact Pairwise.nil
  | @cons a t hhead htail ih =>
      refine Pairwise.cons (fun b hb => ?_)
        (ih fun x hx y hy => hag x (mem_cons_of_mem a hx) y (mem_cons_of_mem a hy))
      exact (hag a (mem_cons_self) b (mem_cons_of_mem a hb)).mp (hhead b hb)

/-! ## The watermark side: `isort` is the SQL merge sort -/

private def twLeB (spec : Spec) : Row → Row → Bool :=
  fun x y => lex spec x.key y.key != Ordering.gt

private def twLtB (spec : Spec) (x y : Row) : Bool :=
  lex spec x.key y.key == Ordering.lt

private def twLeBP (spec : Spec) (x y : Row) : Bool :=
  lex spec (padKey spec x.key) (padKey spec y.key) != Ordering.gt

private theorem tw_leBP_true (spec : Spec) (x y : Row) :
    (twLeBP spec x y = true) ↔ kLe spec (padKey spec x.key) (padKey spec y.key) := by
  unfold twLeBP kLe
  cases h : lex spec (padKey spec x.key) (padKey spec y.key) <;> simp [h]

private theorem tw_leBP_eq_twLeB (spec : Spec) (l : List Row)
    (hlen : ∀ r ∈ l, r.key.length = spec.length)
    (x : Row) (hx : x ∈ l) (y : Row) (hy : y ∈ l) :
    twLeBP spec x y = twLeB spec x y := by
  unfold twLeBP twLeB
  rw [padKey_id x.key (hlen x hx), padKey_id y.key (hlen y hy)]

private theorem ordering_lt_of_decide {o : Ordering} (h : (o == Ordering.lt) = true) :
    o = Ordering.lt := by
  cases o with
  | lt => rfl
  | eq => simp at h
  | gt => simp at h

private theorem tw_transBP (spec : Spec) :
    ∀ a b c : Row, twLeBP spec a b = true → twLeBP spec b c = true →
      twLeBP spec a c = true := by
  intro a b c h1 h2
  have k1 := (tw_leBP_true spec a b).mp h1
  have k2 := (tw_leBP_true spec b c).mp h2
  have k3 := kLe_trans (padKey_length spec a.key) (padKey_length spec b.key)
    (padKey_length spec c.key) k1 k2
  exact (tw_leBP_true spec a c).mpr k3

private theorem tw_totalBP (spec : Spec) (a b : Row) :
    twLeBP spec a b = true || twLeBP spec b a = true := by
  obtain h | h := kLe_total (padKey spec a.key) (padKey spec b.key)
    (padKey_length spec a.key) (padKey_length spec b.key)
  · rw [Bool.or_eq_true_iff]
    exact Or.inl ((tw_leBP_true spec a b).mpr h)
  · rw [Bool.or_eq_true_iff]
    exact Or.inr ((tw_leBP_true spec b a).mpr h)

private theorem tw_pairwiseBP (spec : Spec) (l : List Row) :
    (l.mergeSort (twLeBP spec)).Pairwise (twLeBP spec) :=
  List.pairwise_mergeSort (tw_transBP spec) (tw_totalBP spec) l

private theorem tw_sortedKey (spec : Spec) (l : List Row)
    (hlen : ∀ r ∈ l, r.key.length = spec.length) :
    SortedKey spec (l.mergeSort (twLeBP spec)) :=
  Pairwise.congr (tw_pairwiseBP spec l)
    (fun x hx y hy => by
      have hbddx := hlen x (List.mem_mergeSort.mp hx)
      have hbddy := hlen y (List.mem_mergeSort.mp hy)
      rw [tw_leBP_true, bridge_leP, padKey_id x.key hbddx, padKey_id y.key hbddy])

private theorem tw_insort_advance (spec : Spec) (a : Row) (l₁ l₂ : List Row)
    (h1 : ∀ x ∈ l₁, better spec x.key a.key = true)
    (h2 : ∀ b ∈ l₂, keyLe spec a.key b.key = true) :
    insort spec a (l₁ ++ l₂) = l₁ ++ [a] ++ l₂ := by
  induction l₁ with
  | nil =>
      cases l₂ with
      | nil => simp [insort]
      | cons x xs =>
          have hx : keyLe spec a.key x.key = true := h2 x (by simp)
          have hb : better spec x.key a.key = false := by rw [better, hx]; simp
          simp only [List.nil_append]
          show (if better spec x.key a.key = true then x :: insort spec a xs
            else a :: x :: xs) = [a] ++ (x :: xs)
          rw [if_neg (by simpa using hb)]
          rfl
  | cons y ys ih =>
      have hy : better spec y.key a.key = true := h1 y (by simp)
      show (if better spec y.key a.key = true then y :: insort spec a (ys ++ l₂)
        else a :: y :: (ys ++ l₂)) = (y :: ys) ++ [a] ++ l₂
      rw [if_pos hy]
      rw [ih (fun x hx => h1 x (by simp [hx]))]
      rw [List.cons_append, List.cons_append]

private theorem tw_isort_eq_mergeSortP (spec : Spec) (l : List Row)
    (hlen : ∀ r ∈ l, r.key.length = spec.length) :
    isort spec l = l.mergeSort (twLeBP spec) := by
  induction l with
  | nil => simp [isort]
  | cons a as ih =>
      have hlenAs : ∀ r ∈ as, r.key.length = spec.length :=
        fun r hr => hlen r (by simp [hr])
      obtain ⟨l₁, l₂, hcons, hmerge, hll⟩ :=
        List.mergeSort_cons (tw_transBP spec) (tw_totalBP spec) a as
      show insort spec a (isort spec as) = mergeSort (a :: as) (twLeBP spec)
      rw [ih hlenAs, hmerge, hcons]
      refine (tw_insort_advance spec a l₁ l₂ ?_ ?_).trans ?_
      · intro x hx
        have hbddx : x.key.length = spec.length :=
          hlenAs x (List.mem_mergeSort.mp (hmerge ▸ List.mem_append_left _ hx))
        have ha : a.key.length = spec.length := hlen a (by simp)
        have hax : keyLe spec a.key x.key = false := boolEqFalse fun hh => by
          have : twLeBP spec a x = true := by
            rw [tw_leBP_true, bridge_leP, padKey_id a.key ha, padKey_id x.key hbddx]
            exact hh
          exact absurd this (by simpa using hll x hx)
        have hxa : keyLe spec x.key a.key = true := by
          have ht := keyLe_total spec x.key a.key
          rw [Bool.or_eq_true_iff] at ht
          rcases ht with h | h
          · exact h
          · exact absurd h (by rw [hax]; decide)
        rw [better, hxa, hax]; rfl
      · intro b hb
        have s := List.pairwise_mergeSort (tw_transBP spec) (tw_totalBP spec) (a :: as)
        rw [hcons] at s
        have hbdd : b.key.length = spec.length :=
          hlenAs b (List.mem_mergeSort.mp (hmerge ▸ List.mem_append_right _ hb))
        have ha : a.key.length = spec.length := hlen a (by simp)
        have hle : twLeBP spec a b = true :=
          ((List.pairwise_cons.mp (List.pairwise_append.mp s).2.1).1 b hb)
        rw [tw_leBP_true, bridge_leP, padKey_id a.key ha, padKey_id b.key hbdd] at hle
        exact hle
      · rw [List.append_assoc, List.singleton_append]

private theorem tw_isort_eq_mergeSort (spec : Spec) (l : List Row)
    (hlen : ∀ r ∈ l, r.key.length = spec.length) :
    isort spec l = l.mergeSort (twLeB spec) :=
  (tw_isort_eq_mergeSortP spec l hlen).trans
    (mergeSort_congr l
      (fun x hx y hy => tw_leBP_eq_twLeB spec l hlen x hx y hy))

private theorem tw_boundaryKey_last (after : List Row) (k : Nat) (w : List Row) (c : Row)
    (h : after.take k.succ = w ++ [c]) : boundaryKey after k.succ = c.key := by
  rw [boundaryKey, h]
  cases w with
  | nil => show (([] : List Row).getLast?.getD c).key = c.key; simp
  | cons a w' =>
      show ((w' ++ [c] : List Row).getLast?.getD a).key = c.key
      rw [getLast?_snoc, Option.getD_some]

private theorem tw_wm_fetch_eq (spec : Spec) (offset k : Nat) (sorted : List Row)
    (hs : SortedKey spec sorted)
    (hbdd : ∀ r ∈ sorted, r.key.length = spec.length) :
    (wmRead spec offset k.succ sorted).1 = fetch (twLtB spec) offset k.succ sorted := by
  let after := sorted.drop offset
  have hs' : SortedKey spec after := List.Pairwise.drop hs
  have hbdd' : ∀ x ∈ after, x.key.length = spec.length :=
    fun x hx => hbdd x (mem_drop_subset sorted offset x hx)
  have hleft : (wmRead spec offset k.succ sorted).1 =
      if after.length ≤ k.succ then after
      else after.takeWhile (fun r => keyLe spec r.key (boundaryKey after k.succ) = true) := by
    simp [wmRead, after]
    split <;> simp
  have hright : fetch (twLtB spec) offset k.succ sorted =
      after.filter fun row => (after.filter fun other => twLtB spec other row).length < k.succ :=
    rfl
  by_cases hle : after.length ≤ k.succ
  · rw [hleft, if_pos hle, hright]
    refine (filter_all after (fun row =>
        (after.filter fun other => twLtB spec other row).length < k.succ) ?_).symm
    intro row hrow
    have hlt : (after.filter fun other => twLtB spec other row).length < after.length :=
      length_filter_lt after (fun other => twLtB spec other row) hrow
        (by rw [twLtB, lex_refl]; decide)
    exact decide_eq_true (Nat.lt_of_lt_of_le hlt hle)
  · rw [hleft, if_neg hle, hright]
    have hpos : after.take k.succ ≠ [] := by
      intro hc
      have hl := congrArg List.length hc
      rw [List.length_take,
        Nat.min_eq_left (Nat.le_of_lt (Nat.lt_of_not_le hle)), List.length_nil] at hl
      omega
    obtain ⟨w, c, hdecomp⟩ := exists_append_last hpos
    have hwlen : w.length = k := by
      have h1 := congrArg List.length hdecomp
      rw [List.length_take,
        Nat.min_eq_left (Nat.le_of_lt (Nat.lt_of_not_le hle)),
        List.length_append, List.length_cons, List.length_nil] at h1
      omega
    have hkey : boundaryKey after k.succ = c.key :=
      tw_boundaryKey_last after k w c hdecomp
    have hbndr : after = (w ++ [c]) ++ after.drop k.succ := by
      rw [← hdecomp, List.take_append_drop]
    have hsB : List.Pairwise (fun a b : Row => keyLe spec a.key b.key = true)
        ((w ++ [c]) ++ after.drop k.succ) := hbndr ▸ hs'
    obtain ⟨hs1, hs2, hcross⟩ := List.pairwise_append.mp hsB
    have hwc : ∀ y ∈ w, keyLe spec y.key c.key = true := by
      intro y hy
      exact rel_of_pairwise_append hs1 y hy c (by simp)
    have hsub : ∀ z ∈ w ++ [c], z ∈ after := by
      intro z hz
      rw [hbndr]
      exact List.mem_append.mpr (Or.inl hz)
    have hcBdd : c.key.length = spec.length := hbdd' c (hsub c (by simp))
    have hiffP : ∀ row ∈ after,
        (after.filter fun other => twLtB spec other row).length < k.succ ↔
          keyLe spec row.key c.key = true := by
      intro row hrow
      constructor
      · intro hrank
        by_cases hrc : keyLe spec row.key c.key = true
        · exact hrc
        · exfalso
          have hin : row ∈ w ++ [c] → False := by
            intro hmem
            rcases List.mem_append.mp hmem with hw | hsin
            · exact hrc (hwc row hw)
            · have heq : row = c := List.mem_singleton.mp hsin
              rw [heq] at hrc
              exact hrc (keyLe_refl spec c.key)
          have hrowB : row ∈ (w ++ [c]) ++ after.drop k.succ := hbndr ▸ hrow
          rcases List.mem_append.mp hrowB with hmem | hdrop
          · exact absurd hmem hin
          · have hp : ∀ y ∈ w ++ [c], twLtB spec y row = true := by
              intro y hy
              have k1 : keyLe spec y.key row.key = true := hcross y hy row hdrop
              have k2 : keyLe spec row.key y.key = false := by
                refine boolEqFalse (fun hh => ?_)
                have hyc : keyLe spec y.key c.key = true := by
                  rcases List.mem_append.mp hy with hyw | hsin
                  · exact hwc y hyw
                  · have heq : y = c := List.mem_singleton.mp hsin
                    rw [heq]
                    exact keyLe_refl spec c.key
                exact hrc (keyLe_trans spec row.key y.key c.key hh hyc)
              have hbetter : better spec y.key row.key = true := by
                rw [better, k1, k2]; rfl
              have hmemY : y ∈ after := hsub y hy
              have hky : y.key.length = spec.length := hbdd' y hmemY
              have hlty : lex spec y.key row.key = Ordering.lt :=
                (bridge_lt hky (hbdd' row hrow)).mpr hbetter
              rw [twLtB, hlty]; rfl
            have hfl : after.filter (fun other => twLtB spec other row) =
                (w ++ [c]) ++ (after.drop k.succ).filter (fun other => twLtB spec other row) := by
              have he := congrArg (List.filter fun other => twLtB spec other row) hbndr
              rw [List.filter_append, filter_all (w ++ [c]) _ hp] at he
              exact he
            rw [hfl, List.length_append] at hrank
            have hwc1 : ((w ++ [c]) : List Row).length = k.succ := by
              rw [List.length_append, List.length_cons, List.length_nil, hwlen]
            rw [hwc1] at hrank
            omega
      · intro hrc
        have hcN : twLtB spec c row = false := by
          refine boolEqFalse (fun htrue => ?_)
          rw [twLtB] at htrue
          have h2 : lex spec c.key row.key = Ordering.lt :=
            ordering_lt_of_decide htrue
          have hswap : lex spec row.key c.key = Ordering.gt := by
            rw [lex_swap, h2]; rfl
          exact absurd hswap ((bridge_le (hbdd' row hrow) hcBdd).mpr hrc)
        have hdN : ∀ y ∈ after.drop k.succ, twLtB spec y row = false := by
          intro y hy
          refine boolEqFalse (fun htrue => ?_)
          rw [twLtB] at htrue
          have h2 : lex spec y.key row.key = Ordering.lt :=
            ordering_lt_of_decide htrue
          have hcyc : keyLe spec c.key y.key = true :=
            hcross c (by simp) y hy
          have hry : keyLe spec row.key y.key = true :=
            keyLe_trans spec row.key c.key y.key hrc hcyc
          have hswap : lex spec row.key y.key = Ordering.gt := by
            rw [lex_swap, h2]; rfl
          have hmemY : y ∈ after := hbndr ▸ (List.mem_append.mpr (Or.inr hy))
          exact absurd hswap
            ((bridge_le (hbdd' row hrow) (hbdd' y hmemY)).mpr hry)
        have hfl : after.filter (fun other => twLtB spec other row) =
            w.filter (fun other => twLtB spec other row) := by
          rw [hbndr, List.filter_append, List.filter_append,
            filter_eq_nil_of_false ([c] : List Row) _ (fun z hz => by
              rw [List.mem_singleton.mp hz]; exact hcN),
            filter_eq_nil_of_false (after.drop k.succ) _ hdN,
            List.append_nil, List.append_nil]
        rw [hfl]
        have hl := List.length_filter_le (fun other => twLtB spec other row) w
        rw [hwlen] at hl
        omega
    rw [hkey]
    rw [← filter_eq_takeWhile_of_sorted spec hs']
    refine filter_congr_on after ?_ ?_ (fun row hrow => ?_)
    exact decide_congr_bool (hiffP row hrow).symm

/-! ## Conjunct 1: the watermark read equals the SQL oracle -/

private theorem conj1 (spec : Spec) (offset k : Nat) (streams : List (List Row))
    (hlen : ∀ r ∈ streams.flatten, r.key.length = spec.length) :
    (wmRead spec offset k.succ (mergeStreams spec streams)).1 =
      watermarkSQL spec offset k.succ streams.flatten := by
  have hsrt : mergeStreams spec streams = streams.flatten.mergeSort (twLeB spec) := by
    rw [mergeStreams]
    exact tw_isort_eq_mergeSort spec streams.flatten hlen
  have hsq : SortedKey spec (streams.flatten.mergeSort (twLeB spec)) :=
    hsrt ▸ mergeStreams_sorted spec streams
  have hbdd : ∀ r ∈ streams.flatten.mergeSort (twLeB spec), r.key.length = spec.length :=
    fun r hr => hlen r (List.mem_mergeSort.mp hr)
  rw [hsrt]
  have hsql : watermarkSQL spec offset k.succ streams.flatten =
      fetch (twLtB spec) offset k.succ (streams.flatten.mergeSort (twLeB spec)) := rfl
  rw [hsql]
  exact tw_wm_fetch_eq spec offset k (streams.flatten.mergeSort (twLeB spec)) hsq hbdd

end
section
open TinylambOpt.Research IncrementalSortPage

private def keyLt (a b : Row) : Prop :=
  a.pkey < b.pkey ∨ (a.pkey = b.pkey ∧ a.skey < b.skey)

private def keyEq (a b : Row) : Prop :=
  a.pkey = b.pkey ∧ a.skey = b.skey

/-- The key order refined by appearance id: a strict total order on any
    input whose ids strictly increase. -/
private def Ties (a b : Row) : Prop :=
  keyLt a b ∨ (keyEq a b ∧ a.id < b.id)

/-- The comparator used by `pageSQL`'s merge sort. -/
private def cmpR (a b : Row) : Ordering :=
  if a.pkey = b.pkey then compare a.skey b.skey else compare a.pkey b.pkey

private def kLeB (a b : Row) : Bool := cmpR a b != Ordering.gt

private def kLtB (a b : Row) : Bool := cmpR a b == Ordering.lt

private theorem keyLt_irrefl (a : Row) : ¬ keyLt a a := by
  unfold keyLt
  omega

private theorem keyLt_trans {a b c : Row} (hab : keyLt a b) (hbc : keyLt b c) :
    keyLt a c := by
  unfold keyLt at *
  omega

private theorem keyLt_asym {a b : Row} (h : keyLt a b) : ¬ keyLt b a :=
  fun hba => keyLt_irrefl a (keyLt_trans h hba)

private theorem keyLt_trichotomy (a b : Row) :
    keyLt a b ∨ keyEq a b ∨ keyLt b a := by
  unfold keyLt keyEq
  by_cases hp : a.pkey = b.pkey
  · by_cases hs : a.skey = b.skey
    · exact Or.inr (Or.inl ⟨hp, hs⟩)
    · have h : a.skey < b.skey ∨ b.skey < a.skey := by omega
      rcases h with h | h
      · exact Or.inl (Or.inr ⟨hp, h⟩)
      · exact Or.inr (Or.inr (Or.inr ⟨hp.symm, h⟩))
  · have h : a.pkey < b.pkey ∨ b.pkey < a.pkey := by omega
    rcases h with h | h
    · exact Or.inl (Or.inl h)
    · exact Or.inr (Or.inr (Or.inl h))

private theorem Ties_of_keyLt {a b : Row} (h : keyLt a b) : Ties a b := Or.inl h

private theorem Ties_trans {a b c : Row} (hab : Ties a b) (hbc : Ties b c) : Ties a c := by
  simp only [Ties, keyLt, keyEq] at *
  rcases hab with h1 | ⟨h2, hid⟩
  · rcases hbc with h3 | ⟨h4, _⟩
    · omega
    · omega
  · rcases h2 with ⟨p1, s1⟩
    rcases hbc with h3 | ⟨h4, _⟩
    · rcases h3 with h3 | ⟨p3, s3⟩
      · omega
      · omega
    · rcases h4 with ⟨p4, s4⟩
      omega

private theorem keyLt_keyEq_false {a b : Row} (hab : keyLt a b) (hba : keyEq b a) : False := by
  unfold keyLt keyEq at *
  rcases hab with h | ⟨hp, hs⟩
  · rcases hba with ⟨q1, q2⟩
    omega
  · rcases hba with ⟨q1, q2⟩
    omega

private theorem Ties_asym {a b : Row} (hab : Ties a b) : ¬ Ties b a := by
  intro hba
  rcases hab with h1 | ⟨h2, hid⟩
  · rcases hba with h3 | ⟨h4, _⟩
    · exact keyLt_asym h1 h3
    · exact keyLt_keyEq_false h1 h4
  · rcases hba with h3 | ⟨h4, hid'⟩
    · exact keyLt_keyEq_false (a := b) (b := a) h3 h2
    · omega

/-! Relating the spec comparators to `keyLt` / `keyEq`. -/

private theorem cmpInt_lt (a b : Int) : compare a b = Ordering.lt ↔ a < b := by
  show compareOfLessAndEq a b = Ordering.lt ↔ a < b
  rw [compareOfLessAndEq_eq_lt]

private theorem cmpInt_gt (a b : Int) : compare a b = Ordering.gt ↔ b < a := by
  show compareOfLessAndEq a b = Ordering.gt ↔ b < a
  rw [compareOfLessAndEq_eq_gt (α := Int)
    (by intro x y hx hy; omega) (by intro x y; omega) (by intro x y; omega)]

private theorem cmpR_lt (a b : Row) : cmpR a b = Ordering.lt ↔ keyLt a b := by
  unfold cmpR keyLt
  by_cases hp : a.pkey = b.pkey
  · rw [if_pos hp]
    constructor
    · intro h
      rw [cmpInt_lt] at h
      exact Or.inr ⟨hp, h⟩
    · intro h
      rcases h with h | ⟨heq, h⟩
      · omega
      · rw [cmpInt_lt]
        exact h
  · rw [if_neg hp]
    constructor
    · intro h
      rw [cmpInt_lt] at h
      exact Or.inl h
    · intro h
      rcases h with h | ⟨heq, _⟩
      · rw [cmpInt_lt]
        exact h
      · exfalso
        exact hp heq

private theorem cmpR_gt (a b : Row) : cmpR a b = Ordering.gt ↔ keyLt b a := by
  unfold cmpR keyLt
  by_cases hp : a.pkey = b.pkey
  · rw [if_pos hp]
    constructor
    · intro h
      rw [cmpInt_gt] at h
      exact Or.inr ⟨hp.symm, h⟩
    · intro h
      rcases h with h | ⟨heq, h⟩
      · omega
      · rw [cmpInt_gt]
        exact h
  · rw [if_neg hp]
    constructor
    · intro h
      rw [cmpInt_gt] at h
      exact Or.inl h
    · intro h
      rcases h with h | ⟨heq, _⟩
      · rw [cmpInt_gt]
        exact h
      · exfalso
        exact hp heq.symm

private theorem bneq_gt_true {x : Ordering} : (x != Ordering.gt) = true ↔ ¬ (x = Ordering.gt) := by
  cases x <;> decide

private theorem bneq_gt_false {x : Ordering} : (x != Ordering.gt) = false ↔ x = Ordering.gt := by
  cases x <;> decide

private theorem beq_lt_true {x : Ordering} : (x == Ordering.lt) = true ↔ x = Ordering.lt := by
  cases x <;> decide

private theorem kLeB_iff (a b : Row) : kLeB a b = true ↔ keyLt a b ∨ keyEq a b := by
  unfold kLeB
  rw [bneq_gt_true]
  constructor
  · intro h
    rcases keyLt_trichotomy a b with hab | heq | hba
    · exact Or.inl hab
    · exact Or.inr heq
    · exact absurd (cmpR_gt (a := a) (b := b) |>.mpr hba) h
  · rintro (hab | heq) hba
    · exact absurd (cmpR_gt (a := a) (b := b) |>.mp hba) (keyLt_asym hab)
    · exact keyLt_keyEq_false (a := b) (b := a) (cmpR_gt (a := a) (b := b) |>.mp hba) heq

private theorem kLtB_iff (a b : Row) : kLtB a b = true ↔ keyLt a b := by
  unfold kLtB
  rw [beq_lt_true, cmpR_lt]

private theorem not_kLeB_of_keyLt {a b : Row} (h : keyLt b a) : kLeB a b = false := by
  unfold kLeB
  rw [bneq_gt_false, cmpR_gt]
  exact h



private theorem keyLt_of_keyLt_keyEq {a b c : Row} (hab : keyLt a b) (hbc : keyEq b c) :
    keyLt a c := by
  unfold keyLt keyEq at *
  omega

private theorem keyLt_of_keyEq_keyLt {a b c : Row} (hab : keyEq a b) (hbc : keyLt b c) :
    keyLt a c := by
  unfold keyLt keyEq at *
  omega

private theorem keyEq_trans {a b c : Row} (hab : keyEq a b) (hbc : keyEq b c) :
    keyEq a c := by
  unfold keyEq at *
  exact ⟨hab.1.trans hbc.1, hab.2.trans hbc.2⟩

private theorem kLeB_trans_bool :
    ∀ a b c : Row, kLeB a b = true → kLeB b c = true → kLeB a c = true := by
  intro a b c h1 h2
  rcases kLeB_iff a b |>.mp h1 with hab | hab
  · rcases kLeB_iff b c |>.mp h2 with hbc | hbc
    · exact kLeB_iff a c |>.mpr (Or.inl (keyLt_trans hab hbc))
    · exact kLeB_iff a c |>.mpr (Or.inl (keyLt_of_keyLt_keyEq hab hbc))
  · rcases kLeB_iff b c |>.mp h2 with hbc | hbc
    · exact kLeB_iff a c |>.mpr (Or.inl (keyLt_of_keyEq_keyLt hab hbc))
    · exact kLeB_iff a c |>.mpr (Or.inr (keyEq_trans hab hbc))

private theorem kLeB_total_bool (a b : Row) : kLeB a b || kLeB b a = true := by
  rcases keyLt_trichotomy a b with hab | heq | hba
  · simp [kLeB_iff a b |>.mpr (Or.inl hab)]
  · simp [kLeB_iff a b |>.mpr (Or.inr heq)]
  · simp [kLeB_iff b a |>.mpr (Or.inl hba)]

private theorem kLeB_trans : ∀ a b c : Row, kLeB a b → kLeB b c → kLeB a c :=
  fun a b c h1 h2 => kLeB_trans_bool a b c h1 h2

private theorem kLeB_total : ∀ a b : Row, kLeB a b || kLeB b a :=
  fun a b => kLeB_total_bool a b

private theorem mem_zipIdxN {l : List α} {n : Nat} {p : α × Nat}
    (hp : p ∈ l.zipIdx n) :
    ∃ (i : Nat) (hi : i < l.length), p = (l[i]'hi, i + n) := by
  match l, n, p, hp with
  | [], _, _, h => cases h
  | x :: xs, n, (a, m), hp =>
      simp only [List.zipIdx_cons, List.mem_cons] at hp
      rcases hp with h | h
      · rcases Prod.mk.inj h with ⟨rfl, rfl⟩
        exact ⟨0, by simpa, by simp⟩
      · obtain ⟨i, hi, e⟩ := mem_zipIdxN (l := xs) (n := n + 1) (p := (a, m)) h
        refine ⟨i + 1, by simpa using hi, ?_⟩
        rw [e, List.getElem_cons_succ x xs i (by simpa using hi)]
        rw [show i + 1 + n = i + (n + 1) by omega]

private theorem nodup_zipIdx (l : List α) (n : Nat) : (l.zipIdx n).Nodup := by
  match l, n with
  | [], _ => simp
  | x :: xs, n =>
      rw [List.zipIdx_cons, List.nodup_cons]
      refine ⟨?_, nodup_zipIdx xs (n + 1)⟩
      intro hx
      obtain ⟨i, hi, e⟩ := mem_zipIdxN hx
      rcases Prod.mk.inj e with ⟨_, r2⟩
      omega

private theorem ids_of_index {l : List Row} (h : l.Pairwise (fun a b => a.id < b.id))
    {i j : Nat} (hi : i < l.length) (hj : j < l.length) (hij : i < j) :
    l[i].id < l[j].id :=
  List.Pairwise.rel_getElem_of_lt hi hj h hij

private theorem ids_ne_of_mem {l : List Row} (hind : l.Pairwise (fun a b => a.id < b.id))
    {a b : Row} (ha : a ∈ l) (hb : b ∈ l) (hab : a.id = b.id) : a = b := by
  obtain ⟨i, hi, eia⟩ := List.mem_iff_getElem.mp ha
  obtain ⟨j, hj, eib⟩ := List.mem_iff_getElem.mp hb
  rcases Nat.lt_trichotomy i j with h | h | h
  · rw [← eia, ← eib] at hab ⊢
    have h1 := ids_of_index hind hi hj h
    omega
  · subst h
    exact eia.symm.trans eib
  · rw [← eia, ← eib] at hab ⊢
    have h1 := ids_of_index hind hj hi h
    omega

private theorem nodup_of_ids {l : List Row} (hind : l.Pairwise (fun a b => a.id < b.id)) :
    l.Nodup := by
  refine List.nodup_iff_pairwise_ne.mpr ?_
  induction l with
  | nil => exact List.Pairwise.nil
  | cons x xs ih =>
      obtain ⟨h1, h2⟩ := List.pairwise_cons.mp hind
      refine List.pairwise_cons.mpr ⟨?_, ih h2⟩
      intro y hy heq
      have hlt := h1 y hy
      rw [← heq] at hlt
      omega

private theorem getElem_congr {l : List α} {i j : Nat} (hij : i = j)
    (hi : i < l.length) (hj : j < l.length) : l[i]'hi = l[j]'hj := by
  cases hij
  rfl

private theorem ties_zip_step
    {l : List Row} (hind : l.Pairwise (fun a b => a.id < b.id))
    {w : Row × Nat} {ws : List (Row × Nat)}
    (hmem : ∀ p ∈ w :: ws, ∃ (hi : p.2 < l.length), p = (l[p.2]'hi, p.2))
    (hznd : (w :: ws).Pairwise (· ≠ ·))
    (hz : (w :: ws).Pairwise (fun p q => List.zipIdxLE kLeB p q))
    {x : Row × Nat} (hx : x ∈ ws) :
    Ties w.1 x.1 := by
  obtain ⟨iw, ew⟩ := hmem w List.mem_cons_self
  obtain ⟨ix, ex⟩ := hmem x (List.mem_cons_of_mem w hx)
  have h1w : w.1 = l[w.2]'iw := (Prod.mk.inj ew).1
  have h1x : x.1 = l[x.2]'ix := (Prod.mk.inj ex).1
  have hwx := (List.pairwise_cons.mp hz).1 x hx
  simp only [List.zipIdxLE] at hwx
  by_cases hA : kLeB w.1 x.1 = true
  · rw [if_pos hA] at hwx
    by_cases hB : kLeB x.1 w.1 = true
    · rw [if_pos hB, decide_eq_true_eq] at hwx
      rcases keyLt_trichotomy w.1 x.1 with hlt | heq | hgt
      · exact Or.inl hlt
      · have hij : w.2 < x.2 := by
          rcases Nat.eq_or_lt_of_le hwx with hxy | hxy
          · exfalso
            exact (List.pairwise_cons.mp hznd).1 x hx
              (Prod.ext (h1w.trans (getElem_congr hxy iw ix) |>.trans h1x.symm) hxy)
          · exact hxy
        have hids := ids_of_index hind iw ix hij
        rw [← h1w, ← h1x] at hids
        exact Or.inr ⟨heq, hids⟩
      · exfalso
        have hnc := not_kLeB_of_keyLt hgt
        rw [hnc] at hA
        exact absurd hA (by simp)
    · rw [if_neg hB] at hwx
      rcases keyLt_trichotomy w.1 x.1 with hlt | heq | hgt
      · exact Or.inl hlt
      · exfalso
        exact absurd ((kLeB_iff x.1 w.1).mpr (Or.inr ⟨heq.1.symm, heq.2.symm⟩)) hB
      · exfalso
        exact absurd ((kLeB_iff x.1 w.1).mpr (Or.inl hgt)) hB
  · rw [if_neg hA] at hwx
    exact absurd hwx (by simp)

private theorem ties_zip_aux
    {l : List Row} (hind : l.Pairwise (fun a b => a.id < b.id))
    {z : List (Row × Nat)}
    (hmem : ∀ p ∈ z, ∃ (hi : p.2 < l.length), p = (l[p.2]'hi, p.2))
    (hznd : z.Pairwise (· ≠ ·))
    (hz : z.Pairwise (fun p q => List.zipIdxLE kLeB p q)) :
    (z.map (·.1)).Pairwise Ties := by
  induction z with
  | nil => exact List.Pairwise.nil
  | cons w ws ih =>
      obtain ⟨h1, h2⟩ := List.pairwise_cons.mp hz
      refine List.pairwise_cons.mpr ⟨?_, ih ?_ ?_ h2⟩
      · intro y hy
        obtain ⟨p, hp, rfl⟩ := List.mem_map.mp hy
        exact ties_zip_step hind hmem hznd hz hp
      · intro p hp
        exact hmem p (List.mem_cons_of_mem w hp)
      · exact (List.pairwise_cons.mp hznd).2


private theorem mergeSort_Ties (l : List Row)
    (hind : l.Pairwise (fun a b => a.id < b.id)) :
    (l.mergeSort kLeB).Pairwise Ties := by
  have hperm : ((l.zipIdx).mergeSort (List.zipIdxLE kLeB)).Perm l.zipIdx :=
    List.mergeSort_perm _ _
  have hz : ((l.zipIdx).mergeSort (List.zipIdxLE kLeB)).Pairwise
      (fun p q => List.zipIdxLE kLeB p q) :=
    List.pairwise_mergeSort (le := List.zipIdxLE kLeB)
      (List.zipIdxLE_trans kLeB_trans) (List.zipIdxLE_total kLeB_total) _
  have hnd : ((l.zipIdx).mergeSort (List.zipIdxLE kLeB)).Pairwise (· ≠ ·) :=
    List.nodup_iff_pairwise_ne.mp (hperm.nodup_iff.mpr (nodup_zipIdx l 0))
  have hmem : ∀ p ∈ (l.zipIdx).mergeSort (List.zipIdxLE kLeB),
      ∃ (hi : p.2 < l.length), p = (l[p.2]'hi, p.2) := by
    intro p hp
    obtain ⟨i, hi, e⟩ := mem_zipIdxN (hperm.mem_iff.mp hp)
    rcases Prod.mk.inj e with ⟨e1, e2⟩
    rw [Nat.add_zero] at e2
    subst e2
    exact ⟨by simpa using hi, by simp [← e1]⟩
  rw [← List.mergeSort_zipIdx (le := kLeB)]
  exact ties_zip_aux hind hmem hnd hz

private theorem Ties_rowLt {a b : Row} (h : Ties a b) : rowLt a b := by
  unfold Ties keyLt keyEq rowLt at *
  omega

private theorem pairwise_Ties_rowLt {l : List Row} (h : l.Pairwise Ties) : SortedRows l := by
  induction l with
  | nil => exact List.Pairwise.nil
  | cons x xs ih =>
      obtain ⟨h1, h2⟩ := List.pairwise_cons.mp h
      exact List.pairwise_cons.mpr ⟨fun b hb => Ties_rowLt (h1 b hb), ih h2⟩

private theorem isort_eq_mergeSort (l : List Row)
    (hind : l.Pairwise (fun a b => a.id < b.id)) :
    isort l = l.mergeSort kLeB :=
  sorted_perm_eq
    (isort_sorted l (nodup_of_ids hind))
    (pairwise_Ties_rowLt (mergeSort_Ties l hind))
    ((isort_perm l).trans (List.mergeSort_perm l kLeB).symm)

private theorem runSort_eq_mergeSort (files : List (List Row))
    (hps : PrefixSorted files.flatten)
    (hind : files.flatten.Pairwise (fun a b => a.id < b.id)) :
    runSort files = files.flatten.mergeSort kLeB :=
  (runSort_eq_isort files hps (nodup_of_ids hind)).trans (isort_eq_mergeSort files.flatten hind)

private theorem pageSQL_eq (off k : Nat) (l : List Row) :
    TinylambOpt.ResearchGoals.SqlFetchWithTies.pageSQL off k l =
      TinylambOpt.ResearchGoals.SqlFetchWithTies.fetch kLtB off k (l.mergeSort kLeB) := rfl

private abbrev KeyLeP (a b : KeyPair) : Prop :=
  a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 ≤ b.2)

private def pLt (a b : KeyPair) : Prop :=
  a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 < b.2)

private theorem KeyLeP_refl (a : KeyPair) : KeyLeP a a := Or.inr ⟨rfl, Int.le_refl _⟩

private theorem keyLeP_of_rowLt {x y : Row} (h : rowLt x y) : KeyLeP (rowKey x) (rowKey y) := by
  unfold rowKey KeyLeP
  rcases h with hlt | ⟨heq, hs⟩
  · exact Or.inl hlt
  · rcases hs with hs1 | ⟨hs2, _⟩
    · exact Or.inr ⟨heq, by omega⟩
    · exact Or.inr ⟨heq, by omega⟩

private theorem not_KeyLeP_of_pLt {a b : KeyPair} (h : pLt a b) : ¬ KeyLeP b a := by
  unfold pLt KeyLeP at *
  intro hh
  rcases hh with h2 | ⟨he, hle⟩ <;> rcases h with h3 | ⟨he3, hlt⟩ <;> omega

private theorem pLt_of_pLt_KeyLeP {a b c : KeyPair} (h1 : pLt a b) (h2 : KeyLeP b c) : pLt a c := by
  unfold pLt KeyLeP at *
  rcases h1 with h | ⟨e, h⟩ <;> rcases h2 with h' | ⟨e', h'⟩ <;>
    (first | exact Or.inl (by omega) | exact Or.inr ⟨by omega, by omega⟩)

private theorem KeyLeP_of_not_pLt {a b : KeyPair} (h : ¬ pLt b a) : KeyLeP a b := by
  by_cases hab : a.1 < b.1
  · exact Or.inl hab
  · by_cases h2 : b.1 < a.1
    · exact absurd (Or.inl h2) h
    · right
      have h1 : a.1 = b.1 := by omega
      have hlt : ¬ b.2 < a.2 := fun hh => h (Or.inr ⟨h1.symm, hh⟩)
      exact ⟨h1, by omega⟩

private theorem keyLe_iff_KeyLeP (r : Row) (b : KeyPair) : keyLe r b ↔ KeyLeP (rowKey r) b := by
  unfold keyLe rowKey KeyLeP
  omega

private theorem pLt_keyLtB (x y : Row) : kLtB x y = true ↔ pLt (rowKey x) (rowKey y) := by
  rw [kLtB_iff]
  unfold keyLt rowKey pLt
  omega

/-- In a sorted list every element of the first part precedes every element of
    the second part in key order. -/
private theorem keyOrd_sorted {l : List Row} (h : SortedRows l) :
    ∀ (u v : List Row), l = u ++ v → ∀ y ∈ u, ∀ z ∈ v, KeyLeP (rowKey y) (rowKey z) := by
  induction l with
  | nil =>
      intro u v hl y hy
      cases u with
      | nil => cases hy
      | cons a as => rw [List.cons_append] at hl; exact absurd hl (by simp)
  | cons x xs ih =>
      intro u v hl y hy z hz
      cases u with
      | nil => simp_all
      | cons u' u'' =>
          rw [List.cons_append] at hl
          obtain ⟨rfl, hxs⟩ := List.cons.inj hl
          subst hxs
          by_cases hyx : y = x
          · subst hyx
            exact keyLeP_of_rowLt ((List.pairwise_cons.mp h).1 z
              (List.mem_append.mpr (Or.inr hz)))
          · have hy' : y ∈ u'' := (List.mem_cons.mp hy).resolve_left hyx
            exact ih ((List.pairwise_cons.mp h).2) u'' v rfl y hy' z hz

private theorem beq_key_iff (u v : KeyPair) : (u == v) = true ↔ u = v := by
  cases u with
  | mk a b => cases v with
    | mk c d =>
        simp [beq_iff_eq]

private theorem keyEq_of_KeyLeP_both {a b : KeyPair} (h1 : KeyLeP a b) (h2 : KeyLeP b a) : a = b := by
  unfold KeyLeP at h1 h2
  refine Prod.ext ?_ ?_
  · rcases h1 with h | ⟨e, _⟩ <;> rcases h2 with h' | ⟨e', _⟩ <;> omega
  · rcases h1 with _ | ⟨_, h⟩ <;> rcases h2 with _ | ⟨_, h'⟩ <;> omega

private theorem KeyLeP_total (a b : KeyPair) : KeyLeP a b ∨ KeyLeP b a := by
  unfold KeyLeP
  omega

private theorem KeyLeP_trans {a b c : KeyPair} (h1 : KeyLeP a b) (h2 : KeyLeP b c) :
    KeyLeP a c := by
  unfold KeyLeP at *
  rcases h1 with h | ⟨e, h⟩ <;> rcases h2 with h' | ⟨e', h'⟩ <;>
    (first | exact Or.inl (by omega) | exact Or.inr ⟨by omega, by omega⟩)

private theorem pLt_of_KeyLeP_pLt {a b c : KeyPair} (h1 : KeyLeP a b) (h2 : pLt b c) :
    pLt a c := by
  unfold KeyLeP pLt at *
  rcases h1 with h | ⟨e, h⟩ <;> rcases h2 with h' | ⟨e', h'⟩ <;>
    (first | exact Or.inl (by omega) | exact Or.inr ⟨by omega, by omega⟩)

private theorem pLt_of_KeyLeP_not {a b : KeyPair} (h1 : KeyLeP a b) (h2 : ¬ KeyLeP b a) :
    pLt a b := by
  unfold KeyLeP pLt at *
  rcases h1 with h | ⟨e, h⟩
  · exact Or.inl h
  · refine Or.inr ⟨e, ?_⟩
    omega
private theorem isp_boundaryKey_last (after : List Row) (k : Nat) (w : List Row) (c : Row)
    (h : after.take k.succ = w ++ [c]) : boundaryKey after k.succ = rowKey c := by
  rw [boundaryKey, h]
  cases w with
  | nil => show rowKey (([] : List Row).getLast?.getD c) = rowKey c; simp
  | cons a w' =>
      show rowKey ((w' ++ [c] : List Row).getLast?.getD a) = rowKey c
      rw [getLast?_snoc, Option.getD_some]

private theorem isp_fetch (offset k : Nat) (sorted : List Row) (hs : SortedRows sorted) :
    (incPage offset k.succ sorted).1 =
      TinylambOpt.ResearchGoals.SqlFetchWithTies.fetch kLtB offset k.succ sorted := by
  let after := sorted.drop offset
  have hs' : SortedRows after := List.Pairwise.drop hs
  have hleft : (incPage offset k.succ sorted).1 =
      if after.length ≤ k.succ then after
      else after.takeWhile (fun r => decide (keyLe r (boundaryKey after k.succ))) := by
    simp [incPage, after]
    split <;> simp
  have hright :
      TinylambOpt.ResearchGoals.SqlFetchWithTies.fetch kLtB offset k.succ sorted =
        after.filter fun row => (after.filter fun other => kLtB other row).length < k.succ :=
    rfl
  by_cases hle : after.length ≤ k.succ
  · rw [hleft, if_pos hle, hright]
    refine (filter_all after (fun row =>
        (after.filter fun other => kLtB other row).length < k.succ) ?_).symm
    intro row hrow
    have hlt : (after.filter fun other => kLtB other row).length < after.length :=
      length_filter_lt after (fun other => kLtB other row) hrow
        (by refine boolEqFalse (fun h => ?_); rw [kLtB_iff] at h; exact keyLt_irrefl row h)
    exact decide_eq_true (Nat.lt_of_lt_of_le hlt hle)
  · rw [hleft, if_neg hle, hright]
    have hpos : after.take k.succ ≠ [] := by
      intro hc
      have hl := congrArg List.length hc
      rw [List.length_take,
        Nat.min_eq_left (Nat.le_of_lt (Nat.lt_of_not_le hle)), List.length_nil] at hl
      omega
    obtain ⟨w, c, hdecomp⟩ := exists_append_last hpos
    have hwlen : w.length = k := by
      have h1 := congrArg List.length hdecomp
      rw [List.length_take,
        Nat.min_eq_left (Nat.le_of_lt (Nat.lt_of_not_le hle)),
        List.length_append, List.length_cons, List.length_nil] at h1
      omega
    have hkey : boundaryKey after k.succ = rowKey c :=
      isp_boundaryKey_last after k w c hdecomp
    have hbndr : after = (w ++ [c]) ++ after.drop k.succ := by
      rw [← hdecomp, List.take_append_drop]
    have hsB : SortedRows ((w ++ [c]) ++ after.drop k.succ) := hbndr ▸ hs'
    obtain ⟨hs1, hs2, hcross⟩ := List.pairwise_append.mp hsB
    have hwc : ∀ y ∈ (w ++ [c] : List Row), keyLe y (rowKey c) := by
      intro y hy
      rcases List.mem_append.mp hy with hyw | hsin
      · have h := rel_of_pairwise_append hs1 y hyw c (by simp)
        unfold rowLt at h
        unfold keyLe rowKey
        omega
      · have heq : y = c := List.mem_singleton.mp hsin
        rw [heq]
        unfold keyLe rowKey
        omega
    have hiffP : ∀ row ∈ after,
        (after.filter fun other => kLtB other row).length < k.succ ↔
          keyLe row (rowKey c) := by
      intro row hrow
      constructor
      · intro hrank
        by_cases hrc : keyLe row (rowKey c)
        · exact hrc
        · rcases List.mem_append.mp (hbndr ▸ hrow) with hmem | hdrop
          · exact absurd (hwc row hmem) hrc
          · have hcr : keyLt c row := by
              unfold keyLe rowKey at hrc
              unfold keyLt
              omega
            have hp : ∀ y ∈ w ++ [c], kLtB y row = true := by
              intro y hy
              rcases List.mem_append.mp hy with hyw | hsin
              · have hle' : keyLt y c ∨ keyEq y c := by
                  have h := rel_of_pairwise_append hs1 y hyw c (by simp)
                  unfold rowLt at h
                  unfold keyLt keyEq
                  omega
                rcases hle' with hy1 | hy2
                · rw [kLtB_iff]
                  exact keyLt_trans hy1 hcr
                · rw [kLtB_iff]
                  exact keyLt_of_keyEq_keyLt hy2 hcr
              · have heq : y = c := List.mem_singleton.mp hsin
                rw [heq]
                rw [kLtB_iff]
                exact hcr
            have hfl : after.filter (fun other => kLtB other row) =
                (w ++ [c]) ++ (after.drop k.succ).filter (fun other => kLtB other row) := by
              have he := congrArg (List.filter fun other => kLtB other row) hbndr
              rw [List.filter_append, filter_all (w ++ [c]) _ hp] at he
              exact he
            rw [hfl, List.length_append] at hrank
            have hwlen1 : ((w ++ [c]) : List Row).length = k.succ := by
              rw [List.length_append, List.length_cons, List.length_nil, hwlen]
            rw [hwlen1] at hrank
            have hfalse : False := by omega
            exact False.elim hfalse
      · intro hrc
        have hkC : kLtB c row = false := by
          refine boolEqFalse (fun h => ?_)
          rw [kLtB_iff] at h
          unfold keyLt at h
          unfold keyLe rowKey at hrc
          omega
        have hdN : (after.drop k.succ).filter (fun other => kLtB other row) = [] :=
          filter_eq_nil_of_false (after.drop k.succ) (fun other => kLtB other row) <| by
            intro y hy
            refine boolEqFalse (fun h => ?_)
            rw [kLtB_iff] at h
            have hcy := hcross c (by simp) y hy
            unfold keyLt at h
            unfold rowLt at hcy
            unfold keyLe rowKey at hrc
            omega
        have hfl : after.filter (fun other => kLtB other row) =
            (w ++ [c]).filter (fun other => kLtB other row) ++
              (after.drop k.succ).filter (fun other => kLtB other row) := by
          have he := congrArg (List.filter fun other => kLtB other row) hbndr
          rw [List.filter_append] at he
          exact he
        have hwN : (w ++ [c]).filter (fun other => kLtB other row) =
            w.filter (fun other => kLtB other row) := by
          rw [List.filter_append,
            show (([c] : List Row).filter fun other => kLtB other row) = [] from
              List.filter_eq_nil_iff.mpr (by
                intro x hx
                have : x = c := List.mem_singleton.mp hx
                subst this
                exact fun htrue => absurd htrue (by rw [hkC]; decide)),
            List.append_nil]
        rw [hfl, hwN, hdN, List.length_append, List.length_nil, Nat.add_zero]
        have hlen := List.length_filter_le (fun other => kLtB other row) w
        rw [hwlen] at hlen
        exact Nat.lt_of_le_of_lt hlen (Nat.lt_succ_self k)
    have hpoint : ∀ row ∈ after,
        (decide ((after.filter fun other => kLtB other row).length < k.succ)) =
          decide (keyLe row (rowKey c)) := by
      intro row hrow
      exact decide_congr_bool (hiffP row hrow)
    rw [hkey]
    rw [← filter_eq_takeWhile_keyLe (rowKey c) hs']
    rw [filter_congr_on after
      (fun row => decide ((after.filter fun other => kLtB other row).length < k.succ))
      (fun r => decide (keyLe r (rowKey c))) hpoint]

private theorem conj2 (offset k : Nat) (files : List (List Row))
    (hps : PrefixSorted files.flatten)
    (hind : files.flatten.Pairwise (fun a b => a.id < b.id)) :
    (incPage offset k.succ (runSort files)).1 = pageSQL offset k.succ files.flatten := by
  have hnd : files.flatten.Nodup := nodup_of_ids hind
  rw [pageSQL_eq, ← runSort_eq_mergeSort files hps hind]
  exact isp_fetch offset k (runSort files) (runSort_sorted files hps hnd)

end

theorem sound : TinylambOpt.ResearchGoals.SqlFetchWithTies.Goal := by
  refine ⟨fun spec offset k streams hlen => ?_, fun offset k files hp hind => ?_⟩
  · cases k with
    | zero => exact (zero_sql offset spec streams []).1
    | succ k => exact conj1 spec offset k streams hlen
  · cases k with
    | zero => exact (zero_sql offset [] [] files).2
    | succ k => exact conj2 offset k files hp hind

end TinylambOpt.ResearchProofs.SqlFetchWithTies

-- C++ references to this model or its proof.
-- executor/detail/relational_sort_limit.cpp
