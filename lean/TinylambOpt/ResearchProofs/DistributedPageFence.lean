import TinylambOpt.ResearchGoals.DistributedPageFence

/-!
Proved

- `Goal`

The reference model is defined in `TinylambOpt.ResearchGoals.DistributedPageFence`.
-/

namespace TinylambOpt.ResearchProofs.DistributedPageFence

open TinylambOpt.ResearchGoals.DistributedPageFence hiding Spec Row
open TinylambOpt.ResearchGoals.SqlFetchWithTies hiding Goal watermarkSQL pageSQL
open TinylambOpt.Research TieWatermark
open List

attribute [local instance] boolRelToRel

/-!
# Distributed page fence: soundness

Blocks carry their exact minimum key `low` in the SQL `lex` order.
Walking the `low`-sorted directory, a block is opened exactly while its `low`
could still reach the page boundary computed from the rows read so far.
We show the opened rows produce the same SQL page as all rows, and that the
opened list is exactly the eligibility filter of the sorted directory.

All row keys have length `spec.length`, so the `lex` order is moved through
`padKey` (padding with `none`) onto the TieWatermark order `keyLe`, which is
transitive and total; the global `List.pairwise_mergeSort` machinery then
applies.
-/

/-! ## Padded keys -/

private def padKey : Spec → Key → Key
  | [], _ => []
  | _ :: ds, [] => none :: padKey ds []
  | _ :: ds, x :: xs => x :: padKey ds xs

private theorem padKey_nil' (ds : Spec) : padKey ds [] = replicate ds.length none := by
  induction ds with
  | nil => rfl
  | cons s ds ih =>
      show none :: padKey ds [] = none :: replicate ds.length none
      rw [ih]

private theorem padKey_cons' (s : Dir × Nulls) (ds : Spec) (x : Option Int) (xs : Key) :
    padKey (s :: ds) (x :: xs) = x :: padKey ds xs := rfl

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

/-! ## Key orders: kLe (≤) and kLt (<) -/

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

private theorem not_kLe_of_kLt (spec : Spec) {a b : Key} (h : kLt spec a b) :
    ¬ kLe spec b a := fun hge => absurd (kLt_swap h) hge

private theorem kLe_of_kLt {spec : Spec} {a b : Key} (h : kLt spec a b) : kLe spec a b := by
  intro hc
  rw [kLt] at h
  rw [hc] at h
  exact absurd h (by decide)

private theorem kLt_irrefl {spec : Spec} (a : Key) (ha : a.length = spec.length) :
    ¬ kLt spec a a := by
  intro h
  rw [kLt] at h
  exact absurd h (by rw [lex_refl]; decide)

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
                  · -- component lt: both sides true
                    have hv : valLt d n x y = true := by
                      rw [valLt_comp (d := d) (n := n) (a := x) (b := y), hlt]
                    rw [hlt, hv]
                    simp
                  · -- component eq: recurse on tails
                    have hxy : x = y := (optCmp_eq_iff (d := d) (n := n)).mp heq
                    subst y
                    have hv : valLt d n x x = false := valLt_irrefl d n x
                    rw [heq, hv]
                    have h1 : decide (x = x) = true := decide_eq_true rfl
                    rw [h1, Bool.false_or, Bool.true_and]
                    exact ih'
                  · -- component gt: both sides false
                    have hv : valLt d n x y = false :=
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

/-! ### Bridge to `keyLe` for padded keys; global trans/total of `kLe` on pads. -/

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

private theorem kLt_kLe_trans {spec : Spec} {a b c : Key}
    (ha : a.length = spec.length) (hb : b.length = spec.length) (hc : c.length = spec.length)
    (h1 : kLt spec a b) (h2 : kLe spec b c) : kLt spec a c := by
  rw [bridge_lt ha hb] at h1
  rw [bridge_le hb hc] at h2
  rw [bridge_lt ha hc]
  exact better_trans2 h1 h2

private theorem kLe_kLt_trans {spec : Spec} {a b c : Key}
    (ha : a.length = spec.length) (hb : b.length = spec.length) (hc : c.length = spec.length)
    (h1 : kLe spec a b) (h2 : kLt spec b c) : kLt spec a c := by
  rw [bridge_lt hb hc] at h2
  rw [bridge_le ha hb] at h1
  rw [bridge_lt ha hc]
  exact better_trans1 h1 h2

private theorem kLt_trans {spec : Spec} {a b c : Key}
    (ha : a.length = spec.length) (hb : b.length = spec.length) (hc : c.length = spec.length)
    (h1 : kLt spec a b) (h2 : kLt spec b c) : kLt spec a c :=
  kLt_kLe_trans ha hb hc h1 (kLe_of_kLt h2)

private theorem kLe_kLt_irrefl {spec : Spec} {a b : Key}
    (ha : a.length = spec.length) (hb : b.length = spec.length)
    (h1 : kLe spec a b) (h2 : kLt spec b a) : False :=
  absurd (kLe_kLt_trans ha hb ha h1 h2) (kLt_irrefl a ha)

private theorem kLt_kLe_irrefl {spec : Spec} {a b : Key}
    (ha : a.length = spec.length) (hb : b.length = spec.length)
    (h1 : kLt spec a b) (h2 : kLe spec b a) : False :=
  absurd (kLt_kLe_trans ha hb ha h1 h2) (kLt_irrefl a ha)

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

/-! ## The row orders -/

private def R (spec : Spec) : Row → Row → Prop := fun x y =>
  match lex spec x.key y.key with
  | Ordering.eq => x.id ≤ y.id
  | c => c == Ordering.lt

private def R' (spec : Spec) : Row → Row → Prop := fun x y =>
  match lex spec (padKey spec x.key) (padKey spec y.key) with
  | Ordering.eq => x.id ≤ y.id
  | c => c == Ordering.lt

private theorem R_refl {spec : Spec} (x : Row) : R spec x x := by
  have h : lex spec x.key x.key = Ordering.eq := lex_refl spec x.key
  simp [R, h]

private theorem R'_refl {spec : Spec} (x : Row) : R' spec x x := by
  have h : lex spec (padKey spec x.key) (padKey spec x.key) = Ordering.eq :=
    lex_refl spec (padKey spec x.key)
  simp [R', h]

private theorem R'_trans {spec : Spec} {x y z : Row}
    (h1 : R' spec x y) (h2 : R' spec y z) : R' spec x z := by
  have hlxy : lex spec (padKey spec x.key) (padKey spec y.key) ≠ Ordering.gt := by
    intro hc
    simp only [R'] at h1
    rw [hc] at h1
    simp at h1
  have hlyz : lex spec (padKey spec y.key) (padKey spec z.key) ≠ Ordering.gt := by
    intro hc
    simp only [R'] at h2
    rw [hc] at h2
    simp at h2
  have hxz : lex spec (padKey spec x.key) (padKey spec z.key) ≠ Ordering.gt :=
    kLe_trans (padKey_length spec x.key) (padKey_length spec y.key)
      (padKey_length spec z.key) hlxy hlyz
  cases h : lex spec (padKey spec x.key) (padKey spec z.key) with
  | gt => exact absurd h hxz
  | lt => simp [R', h]
  | eq =>
      have hxy : lex spec (padKey spec x.key) (padKey spec y.key) = Ordering.eq := by
        rcases lex_trich spec (padKey spec x.key) (padKey spec y.key) with hlt | he | hgt
        · exact absurd
            (kLt_kLe_trans (padKey_length spec x.key) (padKey_length spec y.key)
              (padKey_length spec z.key) hlt hlyz)
            (show ¬(lex spec (padKey spec x.key) (padKey spec z.key) = Ordering.lt) from by
              rw [h]; decide)
        · exact he
        · exact absurd hgt hlxy
      have hyz : lex spec (padKey spec y.key) (padKey spec z.key) = Ordering.eq := by
        rcases lex_trich spec (padKey spec y.key) (padKey spec z.key) with hlt | he | hgt
        · exact absurd
            (kLe_kLt_trans (padKey_length spec x.key) (padKey_length spec y.key)
              (padKey_length spec z.key)
              (fun hc => absurd hc (by rw [hxy]; decide)) hlt)
            (show ¬(lex spec (padKey spec x.key) (padKey spec z.key) = Ordering.lt) from by
              rw [h]; decide)
        · exact he
        · exact absurd hgt hlyz
      simp only [R', hxy] at h1
      simp only [R', hyz] at h2
      simp only [R', h]
      exact Nat.le_trans h1 h2

private theorem R'_total {spec : Spec} (x y : Row) : R' spec x y ∨ R' spec y x := by
  cases hxy : lex spec (padKey spec x.key) (padKey spec y.key) with
  | eq =>
      rcases Nat.le_total x.id y.id with h | h
      · exact Or.inl (by simp [R', hxy, h])
      · have : lex spec (padKey spec y.key) (padKey spec x.key) = Ordering.eq := by
          rw [lex_swap, hxy]
          rfl
        exact Or.inr (by simp [R', this, h])
  | lt => exact Or.inl (by simp [R', hxy])
  | gt =>
      have : lex spec (padKey spec y.key) (padKey spec x.key) = Ordering.lt := by
        rw [lex_swap, hxy]
        rfl
      exact Or.inr (by simp [R', this])

/-! ### Boolean view of the orders, matching the `mergeSort` argument. -/

private def ord (spec : Spec) : Row → Row → Bool := fun x y =>
  match lex spec x.key y.key with
  | .eq => x.id ≤ y.id
  | c => c == .lt

private def ordPad (spec : Spec) : Row → Row → Bool := fun x y =>
  match lex spec (padKey spec x.key) (padKey spec y.key) with
  | .eq => x.id ≤ y.id
  | c => c == .lt

private theorem ordered_eq_mergeSort (spec : Spec) (rows : List Row) :
    ordered spec rows = rows.mergeSort (ord spec) := rfl

private theorem ord_apply (spec : Spec) (x y : Row) :
    ord spec x y = (match lex spec x.key y.key with
      | .eq => (x.id ≤ y.id : Bool)
      | c => c == .lt) := rfl

private theorem ordPad_apply (spec : Spec) (x y : Row) :
    ordPad spec x y = (match lex spec (padKey spec x.key) (padKey spec y.key) with
      | .eq => (x.id ≤ y.id : Bool)
      | c => c == .lt) := rfl

private theorem ord_iff (spec : Spec) (x y : Row) : (ord spec x y = true) ↔ R spec x y := by
  cases h : lex spec x.key y.key <;> simp [ord, R, h]

private theorem ordPad_iff (spec : Spec) (x y : Row) :
    (ordPad spec x y = true) ↔ R' spec x y := by
  cases h : lex spec (padKey spec x.key) (padKey spec y.key) <;> simp [ordPad, R', h]

private theorem ordPad_trans_bool {spec : Spec} :
    ∀ a b c : Row, ordPad spec a b = true → ordPad spec b c = true → ordPad spec a c = true :=
  fun a b c h1 h2 => by
    rw [ordPad_iff] at h1 h2
    rw [ordPad_iff]
    exact R'_trans h1 h2

private theorem ordPad_total_bool {spec : Spec} :
    ∀ a b : Row, ordPad spec a b = true ∨ ordPad spec b a = true := fun a b => by
    rw [ordPad_iff, ordPad_iff]
    exact R'_total a b

private theorem merge_congr {α} :
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
          · rw [hys fun z hz w hw => h z hz w (mem_cons_of_mem y hw)]
            simp
          · rw [hxs (y :: ys) fun z hz w hw => h z (mem_cons_of_mem x hz) w hw]
            simp

private theorem mergeSort_congr {α} :
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

private theorem Pairwise.congr {α} {r₁ r₂ : α → α → Prop} {l : List α}
    (h : Pairwise r₁ l) (hag : ∀ x ∈ l, ∀ y ∈ l, r₁ x y ↔ r₂ x y) : Pairwise r₂ l := by
  induction h with
  | nil => exact Pairwise.nil
  | @cons a t hhead htail ih =>
      refine Pairwise.cons (fun b hb => ?_)
        (ih fun x hx y hy => hag x (mem_cons_of_mem a hx) y (mem_cons_of_mem a hy))
      exact (hag a (mem_cons_self) b (mem_cons_of_mem a hb)).mp (hhead b hb)

private theorem pairwise_mergeSort_ordPad (spec : Spec) (rows : List Row) :
    Pairwise (R' spec) (rows.mergeSort (ordPad spec)) := by
  have h := List.pairwise_mergeSort (le := ordPad spec)
    (fun a b c h1 h2 => ordPad_trans_bool (spec := spec) a b c h1 h2)
    (fun a b => by rw [Bool.or_eq_true_iff]; exact ordPad_total_bool (spec := spec) a b) rows
  exact Pairwise.congr h (fun x _ y _ => ordPad_iff spec x y)

private theorem ord_eq_ordPad {spec : Spec} (rows : List Row)
    (hb : ∀ r ∈ rows, r.key.length = spec.length) {x : Row} (hx : x ∈ rows) {y : Row}
    (hy : y ∈ rows) : ord spec x y = ordPad spec x y := by
  rw [ord_apply, ordPad_apply, padKey_id x.key (hb x hx), padKey_id y.key (hb y hy)]

private theorem ordered_mergeSortPad {spec : Spec} (rows : List Row)
    (hb : ∀ r ∈ rows, r.key.length = spec.length) :
    ordered spec rows = rows.mergeSort (ordPad spec) := by
  rw [ordered_eq_mergeSort]
  exact mergeSort_congr (le₁ := ord spec) (le₂ := ordPad spec) rows
    (fun x hx y hy => ord_eq_ordPad rows hb hx hy)

private theorem mem_ordered {spec : Spec} (rows : List Row) (y : Row) :
    y ∈ ordered spec rows ↔ y ∈ rows := by
  rw [ordered_eq_mergeSort]; exact List.mem_mergeSort

private theorem ordered_pairwise {spec : Spec} (rows : List Row)
    (hb : ∀ r ∈ rows, r.key.length = spec.length) :
    Pairwise (R spec) (ordered spec rows) := by
  rw [ordered_mergeSortPad rows hb]
  have h := pairwise_mergeSort_ordPad spec rows
  refine Pairwise.congr h (fun x hx y hy => ?_)
  have hxo : x ∈ rows := (mem_ordered rows x).mp (by rw [ordered_mergeSortPad rows hb]; exact hx)
  have hyo : y ∈ rows := (mem_ordered rows y).mp (by rw [ordered_mergeSortPad rows hb]; exact hy)
  have cx : ord spec x y = ordPad spec x y := ord_eq_ordPad rows hb hxo hyo
  rw [← ordPad_iff, ← ord_iff, cx]

private theorem length_ordered (spec : Spec) (rows : List Row) :
    (ordered spec rows).length = rows.length :=
  (List.mergeSort_perm rows (ord spec)).length_eq

private theorem rowNodup {l : List Row} (h : (l.map Row.id).Nodup)
    {a b : Row} (ha : a ∈ l) (hb : b ∈ l) (hij : a.id = b.id) : a = b := by
  induction l generalizing a b with
  | nil => cases ha
  | cons x xs ih =>
      have h2 : Pairwise (fun u v => u ≠ v) (Row.id x :: List.map Row.id xs) := h
      have hnin : ∀ (a' : Nat), a' ∈ List.map Row.id xs → Row.id x ≠ a' :=
        fun a' hmem => rel_of_pairwise_cons h2 hmem
      have hnd : Nodup (List.map Row.id xs) := Pairwise.of_cons h2
      rcases List.eq_or_mem_of_mem_cons ha with rfl | ha'
      · rcases List.eq_or_mem_of_mem_cons hb with rfl | hb'
        · rfl
        · exact absurd hij (hnin _ (List.mem_map_of_mem (f := Row.id) hb'))
      · rcases List.eq_or_mem_of_mem_cons hb with rfl | hb'
        · exact absurd hij.symm (hnin _ (List.mem_map_of_mem (f := Row.id) ha'))
        · exact ih hnd ha' hb' hij

private theorem ordered_nil (spec : Spec) : ordered spec [] = [] := by simp [ordered]

private theorem ordered_perm {spec : Spec} {rows rows' : List Row}
    (hbl : ∀ r ∈ rows, r.key.length = spec.length)
    (hbl' : ∀ r ∈ rows', r.key.length = spec.length)
    (hn : (rows.map Row.id).Nodup) (p : rows ~ rows') :
    ordered spec rows = ordered spec rows' := by
  refine (List.Perm.eq_of_pairwise (fun a b ham hbm hab hba => ?_)
    (ordered_pairwise rows hbl) (ordered_pairwise rows' hbl') ?_)
  · have hmemA : a ∈ rows := (mem_ordered rows a).mp ham
    have hmemB : b ∈ rows := p.symm.subset ((mem_ordered rows' b).mp hbm)
    cases h : lex spec a.key b.key with
    | gt =>
        simp only [R] at hab
        rw [h] at hab
        simp at hab
    | lt =>
        cases h' : lex spec b.key a.key with
        | lt =>
            exact absurd (kLt_trans (hbl a hmemA) (hbl b hmemB) (hbl a hmemA) h h')
              (kLt_irrefl a.key (hbl a hmemA))
        | eq =>
            have h2 : lex spec b.key a.key = Ordering.gt := by rw [lex_swap, h]; rfl
            rw [h2] at h'
            exact absurd h' (by decide)
        | gt =>
            simp only [R] at hba
            rw [h'] at hba
            simp at hba
    | eq =>
        have hkeq : lex spec b.key a.key = Ordering.eq := by rw [lex_swap, h]; rfl
        simp only [R] at hab hba
        rw [h] at hab
        rw [hkeq] at hba
        simp at hab hba
        exact rowNodup hn hmemA hmemB (Nat.le_antisymm hab hba)
  · rw [ordered_eq_mergeSort, ordered_eq_mergeSort]
    exact (List.mergeSort_perm rows (ord spec)).trans
      (p.trans (List.mergeSort_perm rows' (ord spec)).symm)


/-! # Part C: block ordering and the structure of `scan` -/

private theorem ofBoolNe {o : Ordering} (h : (o != Ordering.gt) = true) : o ≠ Ordering.gt := by
  cases o <;> simp_all (config := {decide := true})

private theorem toBoolNe {o : Ordering} (h : o ≠ Ordering.gt) : (o != Ordering.gt) = true := by
  cases o <;> simp_all (config := {decide := true})

/-- The low-key of a directory block is bounded. -/
private theorem low_bnd (spec : Spec) {directory : List Block} (b : Block) (hb : b ∈ directory)
    (hvalid : ∀ b' ∈ directory, valid spec b')
    (hbnd : ∀ r ∈ directory.flatMap Block.rows, r.key.length = spec.length) :
    b.low.length = spec.length := by
  obtain ⟨w, hw, hweq⟩ := show (∃ r ∈ b.rows, r.key = b.low) from (hvalid b hb).1
  have hw' : w ∈ directory.flatMap Block.rows := by
    rw [List.mem_flatMap]
    exact ⟨b, hb, hw⟩
  exact hweq ▸ hbnd w hw'

/-- Every row of a valid block is at least the block's low-key. -/
private theorem row_ge_low (spec : Spec) {b : Block} (hv : valid spec b)
    (r : Row) (hr : r ∈ b.rows) : kLe spec b.low r.key := ofBoolNe (hv.2 r hr)

private def lowOrdPad (spec : Spec) : Block → Block → Bool := fun x y =>
  lex spec (padKey spec x.low) (padKey spec y.low) != Ordering.gt

/-- The low-keys of the sorted directory are non-decreasing in `kLe`. -/
private theorem sorted_lows_pairwise (spec : Spec) (directory : List Block)
    (hvalid : ∀ b ∈ directory, valid spec b)
    (hbnd : ∀ r ∈ directory.flatMap Block.rows, r.key.length = spec.length) :
    Pairwise (fun x y => kLe spec x.low y.low)
      (directory.mergeSort (fun x y => lex spec x.low y.low != Ordering.gt)) := by
  have hlb : ∀ b ∈ directory, b.low.length = spec.length :=
    fun b hb => low_bnd spec b hb hvalid hbnd
  have htrans : ∀ x y z : Block, lowOrdPad spec x y = true → lowOrdPad spec y z = true →
      lowOrdPad spec x z = true := by
    intro x y z h1 h2
    rw [lowOrdPad] at h1 h2 ⊢
    exact toBoolNe (kLe_trans (padKey_length spec x.low) (padKey_length spec y.low)
      (padKey_length spec z.low) (ofBoolNe h1) (ofBoolNe h2))
  have htotal : ∀ x y : Block, lowOrdPad spec x y = true ∨ lowOrdPad spec y x = true := by
    intro x y
    rcases kLe_total (padKey spec x.low) (padKey spec y.low)
        (padKey_length spec x.low) (padKey_length spec y.low) with h | h
    · exact Or.inl (by rw [lowOrdPad]; exact toBoolNe h)
    · exact Or.inr (by rw [lowOrdPad]; exact toBoolNe h)
  have hc : ∀ x ∈ directory, ∀ y ∈ directory,
      (fun (x y : Block) => lex spec x.low y.low != Ordering.gt) x y = lowOrdPad spec x y := by
    intro x hx y hy
    show (lex spec x.low y.low != Ordering.gt) =
      (lex spec (padKey spec x.low) (padKey spec y.low) != Ordering.gt)
    rw [padKey_id x.low (hlb x hx), padKey_id y.low (hlb y hy)]
  have hsort : directory.mergeSort (fun x y => lex spec x.low y.low != Ordering.gt) =
      directory.mergeSort (lowOrdPad spec) :=
    mergeSort_congr directory hc
  have hp := List.pairwise_mergeSort (le := lowOrdPad spec)
    (fun a b c h1 h2 => htrans a b c h1 h2)
    (fun a b => by rw [Bool.or_eq_true_iff]; exact htotal a b) directory
  have hp2 : Pairwise (fun x y => lowOrdPad spec x y)
      (directory.mergeSort (fun x y => lex spec x.low y.low != Ordering.gt)) := by
    rw [hsort]
    exact hp
  refine Pairwise.congr hp2 (fun (x : Block) hx (y : Block) hy =>
    ⟨(fun h => by
        have hx' : x ∈ directory := by
          rw [List.mem_mergeSort] at hx
          exact hx
        have hy' : y ∈ directory := by
          rw [List.mem_mergeSort] at hy
          exact hy
        have h2 := ofBoolNe h
        rw [padKey_id x.low (hlb x hx'), padKey_id y.low (hlb y hy')] at h2
        exact h2),
      (fun h => by
        have hx' : x ∈ directory := by
          rw [List.mem_mergeSort] at hx
          exact hx
        have hy' : y ∈ directory := by
          rw [List.mem_mergeSort] at hy
          exact hy
        rw [lowOrdPad, padKey_id x.low (hlb x hx'), padKey_id y.low (hlb y hy')]
        exact toBoolNe h)⟩)

/-- For the sorted block list, every row of `blocks.drop i` dominates `blocks[i].low`. -/
private theorem low_le_rows (spec : Spec) :
    ∀ (blocks : List Block) (i : Nat),
      Pairwise (fun x y => kLe spec x.low y.low) blocks →
      (∀ b ∈ blocks, valid spec b) →
      (∀ b ∈ blocks, b.low.length = spec.length) →
      (∀ b ∈ blocks, ∀ r ∈ b.rows, r.key.length = spec.length) →
      ∀ (hi : i < blocks.length) (r : Row),
        r ∈ (blocks.drop i).flatMap Block.rows → kLe spec (blocks.get ⟨i, hi⟩).low r.key := by
  intro blocks i
  induction i generalizing blocks with
  | zero =>
      intro hp hv hl hb hi r hr
      rcases blocks with _ | ⟨hd, tl⟩
      · exact absurd hi (by simp)
      · change kLe spec hd.low r.key
        obtain ⟨bk, hbk, hr'⟩ := List.mem_flatMap.mp hr
        obtain rfl | hrest := List.mem_cons.mp hbk
        · exact row_ge_low spec (hv bk (by simp)) r hr'
        · have h1 : kLe spec hd.low bk.low := rel_of_pairwise_cons hp hrest
          have h2 : kLe spec bk.low r.key := row_ge_low spec (hv bk (by simp [hrest])) r hr'
          exact kLe_trans (hl hd (by simp)) (hl bk (by simp [hrest]))
            (hb bk (by simp [hrest]) r hr') h1 h2
  | succ i ih =>
      intro hp hv hl hb hi r hr
      rcases blocks with _ | ⟨b, rest⟩
      · exact absurd hi (by simp)
      · have h1 : (b :: rest).drop (i + 1) = rest.drop i := by simp [List.drop]
        have h2 : (b :: rest).get ⟨i + 1, hi⟩ = rest.get ⟨i, Nat.lt_of_succ_lt_succ hi⟩ := rfl
        rw [h1] at hr
        rw [h2]
        refine ih rest (Pairwise.tail hp) (fun x hx => hv x (by simp [hx]))
          (fun x hx => hl x (by simp [hx]))
          (fun x hx r' hr' => hb x (by simp [hx]) r' hr') (Nat.lt_of_succ_lt_succ hi) r hr

/-- The scan's first component is `seen₀` appended with the rows of the opened blocks. -/
private theorem scan_rows (spec : Spec) (off k : Nat) :
    ∀ (blocks : List Block) (seen₀ : List Row),
      (scan spec off k seen₀ blocks).1 =
        seen₀ ++ (scan spec off k seen₀ blocks).2.flatMap Block.rows := by
  intro blocks
  induction blocks with
  | nil => intro seen₀; simp [scan]
  | cons b rest ih =>
      intro seen₀
      by_cases hcond : k != 0 && eligible spec off k seen₀ b
      · have hstep : scan spec off k seen₀ (b :: rest) =
            ((scan spec off k (seen₀ ++ b.rows) rest).1,
              b :: (scan spec off k (seen₀ ++ b.rows) rest).2) := by
          simp only [scan]
          rw [if_pos hcond]
        rw [hstep, ih (seen₀ ++ b.rows)]
        rw [show ((b :: (scan spec off k (seen₀ ++ b.rows) rest).2).flatMap Block.rows) =
              b.rows ++ (scan spec off k (seen₀ ++ b.rows) rest).2.flatMap Block.rows from rfl]
        exact append_assoc _ _ _
      · have hstep : scan spec off k seen₀ (b :: rest) = (seen₀, []) := by
          simp only [scan]
          rw [if_neg hcond]
        rw [hstep]
        simp

/-- The open-set of the scan is a prefix of the block list; each opened block passed the
test at its prefix, and (unless the list ended) the first skipped block failed it. -/
private theorem scan_prefix (spec : Spec) (off k : Nat) :
    ∀ (blocks : List Block) (seen₀ : List Row),
      ∃ t : Nat, t ≤ blocks.length ∧
        scan spec off k seen₀ blocks =
          (seen₀ ++ (blocks.take t).flatMap Block.rows, blocks.take t) ∧
        (∀ (i : Nat) (hi : i < blocks.length), i < t →
            k != 0 && eligible spec off k (seen₀ ++ (blocks.take i).flatMap Block.rows)
              (blocks.get ⟨i, hi⟩)) ∧
        (t = blocks.length ∨
          ∀ (ht : t < blocks.length),
            ¬ (k != 0 && eligible spec off k
                (seen₀ ++ (blocks.take t).flatMap Block.rows) (blocks.get ⟨t, ht⟩))) := by
  intro blocks
  induction blocks with
  | nil =>
      intro seen₀
      refine ⟨0, Nat.le_refl 0, by simp [scan, List.take], fun i _ hi => absurd hi (by omega),
        Or.inl rfl⟩
  | cons b rest ih =>
      intro seen₀
      by_cases hcond : k != 0 && eligible spec off k seen₀ b
      · obtain ⟨t, htle, hscan, hpre, hstop⟩ := ih (seen₀ ++ b.rows)
        have hstep : scan spec off k seen₀ (b :: rest) =
            ((scan spec off k (seen₀ ++ b.rows) rest).1,
              b :: (scan spec off k (seen₀ ++ b.rows) rest).2) := by
          simp only [scan]
          rw [if_pos hcond]
        have htake : (b :: rest).take (t + 1) = b :: rest.take t := by simp [List.take]
        refine ⟨t + 1, by simp only [length_cons]; omega, ?_, ?_, ?_⟩
        · rw [hstep, hscan, htake]
          rw [show (b :: rest.take t).flatMap Block.rows =
              b.rows ++ (rest.take t).flatMap Block.rows from rfl]
          rw [← append_assoc]
        · intro i hi hlt
          rcases Nat.lt_or_ge i 1 with h0 | hj
          · have hi0 : i = 0 := by omega
            subst hi0
            rw [show ((b :: rest).take 0 : List Block) = [] from by simp [List.take],
              show (b :: rest).get ⟨0, hi⟩ = b from rfl]
            simp only [flatMap_nil, append_nil]
            exact hcond
          · obtain ⟨j, rfl⟩ := Nat.exists_eq_succ_of_ne_zero (by omega : i ≠ 0)
            rw [show (b :: rest).take (j + 1) = b :: rest.take j from by simp [List.take]]
            rw [show (b :: rest).get ⟨j + 1, hi⟩ = rest.get ⟨j, Nat.lt_of_succ_lt_succ hi⟩ from rfl]
            rw [show (b :: rest.take j).flatMap Block.rows =
                b.rows ++ (rest.take j).flatMap Block.rows from rfl]
            rw [← append_assoc]
            exact hpre j (Nat.lt_of_succ_lt_succ hi) (by omega)
        · rcases hstop with rfl | hstop
          · exact Or.inl (by simp)
          · refine Or.inr (fun ht hh => hstop (Nat.lt_of_succ_lt_succ ht) ?_)
            rw [show (b :: rest).take (t + 1) = b :: rest.take t from by simp [List.take],
                show (b :: rest).get ⟨t + 1, ht⟩ =
                  rest.get ⟨t, Nat.lt_of_succ_lt_succ ht⟩ from rfl,
                show (b :: rest.take t).flatMap Block.rows =
                  b.rows ++ (rest.take t).flatMap Block.rows from rfl,
                ← append_assoc] at hh
            exact hh
      · refine ⟨0, by simp only [length_cons]; omega, ?_, fun i _ hi => absurd hi (by omega),
          Or.inr (fun ht hh =>
            hcond (by
              rw [show ((b :: rest).take 0 : List Block) = [] from by simp [List.take],
                show (b :: rest).get ⟨0, ht⟩ = b from rfl] at hh
              simp only [flatMap_nil, append_nil] at hh
              exact hh))⟩
        have hstep : scan spec off k seen₀ (b :: rest) = (seen₀, []) := by
          simp only [scan]
          rw [if_neg hcond]
        rw [hstep]
        simp

/-! # Part D: list utilities -/


open List

private theorem drop_length_append {α : Type} (l₁ l₂ : List α) :
    (l₁ ++ l₂).drop l₁.length = l₂ := by
  induction l₁ with
  | nil => simp
  | cons hd tl ih => rw [List.length_cons, List.cons_append, List.drop_succ_cons, ih]

private theorem exists_split_get {α : Type} (l : List α) (i : Nat) (hi : i < l.length) :
    ∃ A B : List α, l = A ++ l.get ⟨i, hi⟩ :: B ∧ A.length = i := by
  induction l generalizing i with
  | nil => exact absurd hi (by simp)
  | cons hd tl ih =>
      cases i with
      | zero => exact ⟨[], tl, by simp, rfl⟩
      | succ i =>
          obtain ⟨A, B, hAB, hlen⟩ := ih i (Nat.lt_of_succ_lt_succ hi)
          refine ⟨hd :: A, B, ?_, by rw [List.length_cons, hlen]⟩
          have hg : (hd :: tl).get ⟨i + 1, hi⟩ = tl.get ⟨i, Nat.lt_of_succ_lt_succ hi⟩ := rfl
          rw [hg]
          show hd :: tl = hd :: (A ++ tl.get ⟨i, Nat.lt_of_succ_lt_succ hi⟩ :: B)
          rw [← hAB]

private theorem exists_split_get_eq {α : Type} (l : List α) (i : Nat) (hi : i < l.length) :
    ∃ A B : List α, l = A ++ l[i] :: B ∧ A.length = i :=
  exists_split_get l i hi

private theorem get_at_len_append {α : Type} : ∀ (A : List α) (c : α) (B : List α),
    (A ++ c :: B)[A.length] = c
  | [], c, _ => rfl
  | hd :: tl, c, B => by
      show (tl ++ c :: B)[tl.length] = c
      exact get_at_len_append tl c B

private theorem getElem?_at_length {α : Type} (A : List α) (c : α) (B : List α) :
    (A ++ c :: B)[A.length]? = some c := by
  have hlt : A.length < (A ++ c :: B).length := by
    rw [List.length_append]
    rw [List.length_cons]
    omega
  rw [getElem?_pos (A ++ c :: B) A.length hlt]
  rw [get_at_len_append]

private theorem lt_length_of_get?_some {α : Type} (l : List α) (i : Nat) {c : α}
    (h : l[i]? = some c) : i < l.length := by
  cases Nat.lt_or_ge i l.length with
  | inl hlt => exact hlt
  | inr hge =>
      rw [List.getElem?_eq_none hge] at h
      exact absurd h (by simp)

private theorem split_of_get?_some {α : Type} {l : List α} {i : Nat} {c : α}
    (h : l[i]? = some c) : ∃ A B : List α, l = A ++ c :: B ∧ A.length = i := by
  have hi : i < l.length := lt_length_of_get?_some l i h
  have hget : l[i] = c := by
    have h2 := getElem?_pos l i hi
    rw [h] at h2
    exact (Option.some.inj h2).symm
  obtain ⟨A, B, hAB, hlen⟩ := exists_split_get_eq l i hi
  refine ⟨A, B, ?_, hlen⟩
  rw [← hget]
  exact hAB

private theorem filter_ext {α : Type} {p q : α → Bool} : ∀ (l : List α),
    (∀ x ∈ l, p x = q x) → l.filter p = l.filter q
  | [], _ => rfl
  | x :: xs, h => by
      rw [List.filter_cons, List.filter_cons, h x (List.mem_cons_self),
        filter_ext xs (fun y hy => h y (List.mem_cons_of_mem x hy))]

private theorem filter_all_append {α : Type} (p : α → Bool) :
    ∀ (l₁ l₂ : List α), (∀ x ∈ l₁, p x = true) → (l₁ ++ l₂).filter p = l₁ ++ l₂.filter p
  | [], _, _ => rfl
  | x :: xs, l₂, h => by
      have key : (x :: xs ++ l₂).filter p = x :: (xs ++ l₂).filter p :=
        List.filter_cons_of_pos (h x (List.mem_cons_self))
      rw [key, filter_all_append p xs l₂ (fun y hy => h y (List.mem_cons_of_mem x hy))]
      rfl

private theorem filter_nil_of_false {α : Type} (p : α → Bool) :
    ∀ (l : List α), (∀ x ∈ l, p x = false) → l.filter p = []
  | [], _ => rfl
  | x :: xs, h => by
      have key : (x :: xs).filter p = xs.filter p :=
        List.filter_cons_of_neg (by simpa using h x (List.mem_cons_self))
      rw [key, filter_nil_of_false p xs (fun y hy => h y (List.mem_cons_of_mem x hy))]

private theorem filter_eq_take_of_prefix {α : Type} (p : α → Bool) :
    ∀ (l : List α) (s : Nat), s ≤ l.length →
      (∀ x ∈ l.take s, p x = true) → (∀ x ∈ l.drop s, p x = false) → l.filter p = l.take s
  | [], _, _, _, _ => by rw [List.take_nil, List.filter_nil]
  | x :: xs, 0, _, _, h2 => filter_nil_of_false p (x :: xs) h2
  | x :: xs, s + 1, hle, h1, h2 => by
      have hx : p x = true :=
        h1 x (by rw [List.take_succ_cons]; exact List.mem_cons_self)
      rw [List.filter_cons_of_pos hx, List.take_succ_cons,
        filter_eq_take_of_prefix p xs s (by simpa [List.length_cons] using hle)
          (fun y hy => h1 y (by rw [List.take_succ_cons]; exact List.mem_cons_of_mem x hy))
          (fun y hy => h2 y hy)]

private theorem filter_drop_swap {α : Type} (p : α → Bool) : ∀ (l : List α) (n : Nat),
    (∀ x ∈ l.take n, p x = true) → (l.drop n).filter p = (l.filter p).drop n
  | [], _, _ => by simp only [List.drop_nil, List.filter_nil]
  | x :: xs, 0, _ => rfl
  | x :: xs, n + 1, h => by
      have hx : p x = true := h x (by rw [List.take_succ_cons]; exact List.mem_cons_self)
      have ih := filter_drop_swap p xs n
        (fun y hy => h y (by rw [List.take_succ_cons]; exact List.mem_cons_of_mem x hy))
      rw [List.filter_cons_of_pos hx, List.drop_succ_cons]
      exact ih

private theorem split_first_occ {α : Type} :
    ∀ (l : List α) (x : α), x ∈ l → ∃ pre post : List α, l = pre ++ x :: post ∧ x ∉ pre
  | [], _, hmem => absurd hmem (by simp)
  | y :: ys, x, hmem => by
      by_cases hyx : x = y
      · exact ⟨[], ys, by rw [hyx]; simp, by simp⟩
      · obtain ⟨pre, post, h, hnp⟩ := split_first_occ ys x
          ((List.mem_cons.mp hmem).resolve_left hyx)
        exact ⟨y :: pre, post, by rw [h]; simp,
          fun hx => by
            rcases List.mem_cons.mp hx with h2 | h3
            · exact absurd h2 hyx
            · exact hnp h3⟩

private theorem take_append_of_le {α : Type} (l₁ l₂ : List α) (t : Nat) (hle : t ≤ l₁.length) :
    (l₁ ++ l₂).take t = l₁.take t := by
  induction l₁ generalizing t with
  | nil =>
      have ht : t = 0 := Nat.le_zero.mp hle
      subst ht
      simp
  | cons hd tl ih =>
      cases t with
      | zero => simp
      | succ t2 =>
          have hle' : t2 ≤ tl.length := by
            rw [List.length_cons] at hle
            omega
          have key : (hd :: tl ++ l₂).take (t2 + 1) = hd :: (tl ++ l₂).take t2 := rfl
          rw [key, ih t2 hle']
          rfl

private theorem take_append_len {α : Type} (a : α) (s₁ s₂ : List α) :
    ((a :: s₁) ++ s₂).take (s₁.length + 1) = a :: s₁ := by
  rw [take_append_of_le (a :: s₁) s₂ (s₁.length + 1) (Nat.le_refl _)]
  rw [← List.length_cons, List.take_length]

private theorem perm_mem {α : Type} {l₁ l₂ : List α} (h : l₁ ~ l₂) :
    ∀ x, x ∈ l₁ ↔ x ∈ l₂ := by
  intro x
  induction h with
  | nil => simp
  | @cons a t₁ t₂ _ ih => simp [List.mem_cons, ih]
  | @swap a b t => simp [List.mem_cons, or_comm, or_left_comm, or_assoc]
  | @trans t₁ t₂ t₃ _ _ ih₁ ih₂ => exact ih₁.trans ih₂

open TinylambOpt.ResearchGoals.DistributedPageFence hiding Spec Row
open TinylambOpt.Research TieWatermark

private theorem nodup_mapid_filter (l : List Row) (p : Row → Bool)
    (h : (l.map Row.id).Nodup) : ((l.filter p).map Row.id).Nodup := by
  induction l with
  | nil => exact h
  | cons x xs ih =>
      obtain ⟨hh, htl⟩ := List.nodup_cons.mp h
      cases hpx : p x with
      | true =>
          rw [List.filter_cons_of_pos hpx, List.map_cons]
          refine List.nodup_cons.mpr ⟨fun hy => ?_, ih htl⟩
          obtain ⟨y, hyf, hey⟩ := List.mem_map.mp hy
          exact hh (by rw [← hey]; exact List.mem_map_of_mem (List.mem_filter.mp hyf).1)
      | false =>
          rw [List.filter_cons_of_neg (by simpa using hpx)]
          exact ih htl

private theorem nodup_of_mapid (l : List Row) (h : (l.map Row.id).Nodup) : Nodup l := by
  induction l with
  | nil => exact List.nodup_nil
  | cons x xs ih =>
      obtain ⟨hh, htl⟩ := List.nodup_cons.mp h
      refine List.nodup_cons.mpr ⟨fun hy => hh (List.mem_map_of_mem hy), ih htl⟩



/-! # Part E: positions in `R`-sorted lists -/

private def kLtb (spec : Spec) : Row → Row → Bool :=
  fun x y => lex spec x.key y.key == Ordering.lt

private theorem kLtb_true {spec : Spec} {x y : Row} (h : kLt spec x.key y.key) :
    kLtb spec x y = true := by
  show (lex spec x.key y.key == Ordering.lt) = true
  rw [kLt] at h
  rw [h]
  decide

private theorem neLtBool {o : Ordering} (h : o ≠ Ordering.lt) : (o == Ordering.lt) = false := by
  cases o with
  | lt => exact absurd rfl h
  | eq | gt => decide

private theorem kLtb_false {spec : Spec} {x y : Row} (h : ¬ kLt spec x.key y.key) :
    kLtb spec x y = false :=
  neLtBool (show lex spec x.key y.key ≠ Ordering.lt from h)

private theorem R_imp_kLe {spec : Spec} {x y : Row} (h : R spec x y) : kLe spec x.key y.key := by
  intro hg
  simp only [R] at h
  rw [hg] at h
  simp at h

private theorem R_antisymm {spec : Spec} {x y : Row}
    (hx : x.key.length = spec.length) (hy : y.key.length = spec.length)
    (h1 : R spec x y) (h2 : R spec y x) : x.key = y.key ∧ x.id = y.id := by
  have hkeq : x.key = y.key := kLe_antisym hx hy (R_imp_kLe h1) (R_imp_kLe h2)
  refine ⟨hkeq, ?_⟩
  simp only [R] at h1 h2
  have e12 : lex spec x.key y.key = lex spec x.key x.key := by rw [hkeq]
  have e21 : lex spec y.key x.key = lex spec y.key y.key := by rw [hkeq]
  rw [e12, lex_refl] at h1
  rw [e21, lex_refl] at h2
  simp at h1 h2
  revert h1 h2
  omega

/-- In an `R`-sorted list, an element lying after `c` that is `R`-below `c` equals `c`. -/
private theorem after_notrel {spec : Spec} :
    ∀ (A : List Row) (c y : Row) (B : List Row),
      Pairwise (R spec) (A ++ c :: B) →
      ((A ++ c :: B).map Row.id).Nodup →
      (∀ r ∈ A ++ c :: B, r.key.length = spec.length) →
      y ∈ B → R spec y c → y = c
  | [], c, y, B, hp, hn, hb, hy, hR => by
      have hcy : R spec c y := rel_of_pairwise_cons hp hy
      have hc : c ∈ ([] : List Row) ++ c :: B := by simp
      have hy' : y ∈ ([] : List Row) ++ c :: B := by simp [hy]
      obtain ⟨_, hid⟩ := R_antisymm (hb c hc) (hb y hy') hcy hR
      exact (rowNodup hn hc hy' hid).symm
  | hd :: tl, c, y, B, hp, hn, hb, hy, hR => by
      refine after_notrel tl c y B (Pairwise.tail hp) ?_
        (fun r hr => hb r (List.mem_cons.mpr (Or.inr hr))) (by simpa using hy) hR
      exact (List.nodup_cons.mp hn).2

/-- In an `R`-sorted list, every element lying before `c` is `R`-below `c`. -/
private theorem before_rel {spec : Spec} :
    ∀ (A : List Row) (c : Row) (B : List Row),
      Pairwise (R spec) (A ++ c :: B) → ∀ y ∈ A, R spec y c
  | [], c, B, _, y, hy => absurd hy (by simp)
  | hd :: tl, c, B, hp, y, hy => by
      rcases List.mem_cons.mp hy with rfl | hy'
      · exact rel_of_pairwise_cons hp (by simp)
      · exact before_rel tl c B (Pairwise.tail hp) y hy'

private theorem card_le_of_mem_nodup : ∀ (m : List Row), (m.map Row.id).Nodup →
    ∀ (l : List Row), (∀ x ∈ m, x ∈ l) → m.length ≤ l.length
  | [], _, _, _ => Nat.zero_le _
  | x :: xs, hm, l, h => by
      obtain ⟨hh, htl⟩ := List.nodup_cons.mp hm
      obtain ⟨pre, post, hl, hnp⟩ :=
        split_first_occ l x (h x (List.mem_cons_self))
      have hxs : ∀ y ∈ xs, y ∈ pre ++ post := by
        intro y hy
        have hyl : y ∈ pre ++ x :: post := by
          rw [← hl]
          exact h y (List.mem_cons_of_mem x hy)
        have hyx : y ≠ x := fun e =>
          absurd (List.mem_map_of_mem (f := Row.id) (by rw [← e]; exact hy)) hh
        rcases List.mem_append.mp hyl with h1 | h2
        · exact List.mem_append_left post h1
        · rcases List.mem_cons.mp h2 with hyx0 | hpost
          · exact absurd (List.mem_map_of_mem (f := Row.id)
              (by rw [← hyx0]; exact hy)) hh
          · exact List.mem_append_right pre hpost
      have hlen : l.length = pre.length + post.length + 1 := by
        rw [hl, List.length_append, List.length_cons]
        omega
      have hlen2 : (pre ++ post).length = pre.length + post.length := List.length_append
      have := card_le_of_mem_nodup xs htl (pre ++ post) hxs
      rw [hlen2] at this
      rw [List.length_cons, hlen]
      omega

private theorem take_len_add_one_append {α : Type} (A : List α) (c : α) (B : List α) :
    (A ++ c :: B).take (A.length + 1) = A ++ [c] := by
  induction A with
  | nil => simp
  | cons hd tl ih =>
      show hd :: (tl ++ c :: B).take (tl.length + 1) = hd :: (tl ++ [c])
      rw [ih]

private theorem filter_all_id {α : Type} (p : α → Bool) :
    ∀ (l : List α), (∀ x ∈ l, p x = true) → l.filter p = l
  | [], _ => rfl
  | x :: xs, h => by
      rw [List.filter_cons_of_pos (h x (List.mem_cons_self)),
        filter_all_id p xs (fun y hy => h y (List.mem_cons_of_mem x hy))]

private theorem take_filter_of_all {α : Type} (p : α → Bool) : ∀ (l : List α) (n : Nat),
    (∀ x ∈ l.take n, p x = true) → (l.filter p).take n = l.take n
  | [], _, _ => by simp only [List.filter_nil, List.take_nil]
  | x :: xs, 0, _ => by simp only [List.take_zero]
  | x :: xs, n + 1, h => by
      have hx : p x = true :=
        h x (by rw [List.take_succ_cons]; exact List.mem_cons_self)
      have hxs : ∀ y ∈ xs.take n, p y = true :=
        fun y hy => h y (by rw [List.take_succ_cons]; exact List.mem_cons_of_mem x hy)
      rw [List.filter_cons_of_pos hx, List.take_succ_cons, List.take_succ_cons,
        take_filter_of_all p xs n hxs]

private theorem R_get_of_lt {spec : Spec} {l : List Row} (hp : Pairwise (R spec) l)
    {i j : Nat} (hi : i < l.length) (hj : j < l.length) (hij : i < j) :
    R spec l[i] l[j] :=
  (pairwise_iff_getElem.mp hp i j hi hj hij)

private theorem nodup_mapid_take {l : List Row} (h : (l.map Row.id).Nodup) (n : Nat) :
    ((l.take n).map Row.id).Nodup := by
  rw [List.map_take]
  exact (List.take_sublist n (l.map Row.id)).nodup h

/-! # Part F: the fence — page equality and the exact open set -/

private theorem neZeroBool (k : Nat) (h : k ≠ 0) : (k != 0) = true := by
  revert h
  cases k with
  | zero => intro h; exact absurd rfl h
  | succ k => intro _; simp

private theorem getElem_mem {α : Type} (l : List α) (i : Nat) (hi : i < l.length) :
    l[i] ∈ l := by
  match l, i, hi with
  | [], i, hi => exact absurd hi (Nat.not_lt_zero i)
  | _ :: _, 0, _ => exact List.mem_cons_self
  | _ :: xs, i + 1, hi => exact List.mem_cons_of_mem _ (getElem_mem xs i (by simpa using hi))

private theorem eqOfNeBoolFalse {o : Ordering} (h : (o != Ordering.gt) = false) :
    o = Ordering.gt := by
  revert h
  cases o with
  | lt | eq => intro h; exact absurd h (by decide)
  | gt => intro _; rfl

private theorem neGtBool {o : Ordering} (h : o = Ordering.gt) : (o != Ordering.gt) = false := by
  subst h
  decide

private theorem kLt_of_gt {spec : Spec} {a b : Key} (h : lex spec a b = Ordering.gt) :
    kLt spec b a := by
  show lex spec b a = Ordering.lt
  rw [lex_swap, h]
  rfl

private theorem gt_of_kLt {spec : Spec} {a b : Key} (h : kLt spec a b) :
    lex spec b a = Ordering.gt := by
  rw [lex_swap, h]
  rfl

private theorem kLe_lows_get {spec : Spec} {blocks : List Block}
    (hp : Pairwise (fun x y => kLe spec x.low y.low) blocks)
    {i j : Nat} (hi : i < blocks.length) (hj : j < blocks.length) (hij : i < j) :
    kLe spec blocks[i].low blocks[j].low :=
  (pairwise_iff_getElem.mp hp i j hi hj hij)

private theorem R_total {spec : Spec} {x y : Row} (hx : x.key.length = spec.length)
    (hy : y.key.length = spec.length) : R spec x y ∨ R spec y x := by
  have e1 : R spec x y = R' spec x y := by
    show (match lex spec x.key y.key with
        | Ordering.eq => x.id ≤ y.id
        | c => c == Ordering.lt) =
      (match lex spec (padKey spec x.key) (padKey spec y.key) with
        | Ordering.eq => x.id ≤ y.id
        | c => c == Ordering.lt)
    rw [padKey_id x.key hx, padKey_id y.key hy]
  have e2 : R spec y x = R' spec y x := by
    show (match lex spec y.key x.key with
        | Ordering.eq => y.id ≤ x.id
        | c => c == Ordering.lt) =
      (match lex spec (padKey spec y.key) (padKey spec x.key) with
        | Ordering.eq => y.id ≤ x.id
        | c => c == Ordering.lt)
    rw [padKey_id y.key hy, padKey_id x.key hx]
  rcases R'_total x y with h | h
  · exact Or.inl (by rw [e1]; exact h)
  · exact Or.inr (by rw [e2]; exact h)

/-- In an `R`-sorted nodup list, the elements strictly below the pivot at position `q`
are exactly the `take q` prefix. -/
private theorem filter_below_eq_take {spec : Spec} :
    ∀ (l : List Row) (x : Row) (q : Nat) (hq : q < l.length), l[q] = x →
      Pairwise (R spec) l → (∀ r ∈ l, r.key.length = spec.length) →
      ((l.map Row.id).Nodup) →
      l.filter (fun y => !ord spec x y) = l.take q := by
  intro l
  induction l with
  | nil =>
      intro x q hq
      exact absurd hq (by simp)
  | cons x xs ih =>
      intro y q hq heq hp hb hn
      cases q with
      | zero =>
          have hxy : x = y := heq
          show (x :: xs).filter (fun z => !ord spec y z) = []
          refine filter_nil_of_false (fun z => !ord spec y z) (x :: xs) ?_
          intro z hz
          show (!ord spec y z) = false
          rcases List.mem_cons.mp hz with hzx | hz'
          · have hx : ord spec y z = true := by
              rw [hzx]
              rw [hxy]
              exact (ord_iff spec y y).mpr (R_refl y)
            simp [hx]
          · have hR : R spec y z := by
              rw [← hxy]
              exact rel_of_pairwise_cons hp hz'
            have hx : ord spec y z = true := (ord_iff spec y z).mpr hR
            simp [hx]
      | succ q =>
          have hl : q < xs.length := by
            rw [List.length_cons] at hq
            omega
          have hh : Row.id x ∉ xs.map Row.id := (List.nodup_cons.mp hn).1
          have hn2 : (xs.map Row.id).Nodup := (List.nodup_cons.mp hn).2
          have hyq : xs[q] = y := heq
          have hcx : R spec x y := by
            rw [← hyq]
            have hg : R spec (x :: xs)[0] (x :: xs)[q + 1] :=
              R_get_of_lt hp (by omega) hq (by omega)
            have hc : (x :: xs)[q + 1] = xs[q] := rfl
            rw [hc] at hg
            exact hg
          have hnx : ¬ R spec y x := by
            intro h2
            have h1 : R spec x xs[q] := by
              rw [hyq]
              exact hcx
            have h2' : R spec xs[q] x := by
              rw [hyq]
              exact h2
            have hxy : x = xs[q] :=
              rowNodup hn (List.mem_cons_self)
                (List.mem_cons_of_mem _ (getElem_mem xs q hl))
                (R_antisymm (hb x (List.mem_cons_self))
                  (hb _ (List.mem_cons_of_mem _ (getElem_mem xs q hl))) h1 h2').2
            exact absurd (by
              rw [hxy]
              exact List.mem_map_of_mem (f := Row.id) (getElem_mem xs q hl)) hh
          have hp2 : ord spec y x = false := by
            have hnT : ¬ ord spec y x = true := fun he => hnx ((ord_iff spec y x).mp he)
            cases hoo : ord spec y x with
            | true => exact absurd hoo hnT
            | false => rfl
          show (x :: xs).filter (fun z => !ord spec y z) = x :: xs.take q
          rw [List.filter_cons_of_pos (by simp [hp2])]
          refine congrArg (List.cons x) (ih y q hl hyq (Pairwise.tail hp)
            (fun r hr => hb r (List.mem_cons_of_mem x hr)) hn2)

/-- Indices of a shared element only grow when passing to a larger sorted list. -/
private theorem index_le_index {spec : Spec} {l₁ l₂ : List Row} {q : Nat} {x : Row}
    (hl₁ : q < l₁.length) (h₁ : l₁[q] = x)
    (hp₁ : Pairwise (R spec) l₁) (hp₂ : Pairwise (R spec) l₂)
    (hb₁ : ∀ r ∈ l₁, r.key.length = spec.length)
    (hb₂ : ∀ r ∈ l₂, r.key.length = spec.length)
    (hn₁ : (l₁.map Row.id).Nodup) (hn₂ : (l₂.map Row.id).Nodup)
    (hsub : ∀ r ∈ l₁, r ∈ l₂)
    (r : Nat) (hr : r < l₂.length) (h₂ : l₂[r] = x) :
    q ≤ r := by
  have e₁ := filter_below_eq_take l₁ x q hl₁ h₁ hp₁ hb₁ hn₁
  have e₂ := filter_below_eq_take l₂ x r hr h₂ hp₂ hb₂ hn₂
  have hn1f : ((l₁.filter (fun y => !ord spec x y)).map Row.id).Nodup :=
    nodup_mapid_filter l₁ (fun y => !ord spec x y) hn₁
  have hmem : ∀ y ∈ l₁.filter (fun y => !ord spec x y),
      y ∈ l₂.filter (fun y => !ord spec x y) := by
    intro y hy
    obtain ⟨hy1, hy2⟩ := List.mem_filter.mp hy
    exact List.mem_filter.mpr ⟨hsub y hy1, hy2⟩
  have hcard := card_le_of_mem_nodup _ hn1f _ hmem
  rw [e₁, List.length_take, Nat.min_eq_left (Nat.le_of_lt hl₁)] at hcard
  rw [e₂, List.length_take, Nat.min_eq_left (Nat.le_of_lt hr)] at hcard
  exact hcard

/-- A strictly smaller key sits strictly earlier in an `R`-sorted nodup list. -/
private theorem lt_index_of_kLt {spec : Spec} {l : List Row} (hp : Pairwise (R spec) l)
    (hb : ∀ r ∈ l, r.key.length = spec.length)
    {p q : Nat} (hp' : p < l.length) (hq' : q < l.length)
    (h : kLt spec l[p].key l[q].key) : p < q := by
  rcases Nat.lt_or_ge p q with hlt | hge
  · exact hlt
  · rcases Nat.eq_or_lt_of_le hge with heq | hgt
    · subst heq
      exact absurd h (kLt_irrefl _ (hb _ (getElem_mem l q hq')))
    · have hR : R spec l[q] l[p] := R_get_of_lt hp hq' hp' hgt
      have hkLe : kLe spec l[q].key l[p].key := R_imp_kLe hR
      exact (kLe_kLt_irrefl (hb _ (getElem_mem l q hq')) (hb _ (getElem_mem l p hp'))
        hkLe h).elim

private theorem eligible_cases {spec : Spec} {off k : Nat} {seen : List Row} {next : Block} :
    (match (ordered spec seen)[off + k - 1]? with
      | none => true
      | some cutoff => (lex spec next.low cutoff.key != Ordering.gt)) =
      eligible spec off k seen next := rfl

private theorem eligible_none {spec : Spec} {off k : Nat} {seen : List Row} {next : Block}
    (h : (ordered spec seen)[off + k - 1]? = none) :
    eligible spec off k seen next = true := by
  rw [← eligible_cases, h]

private theorem eligible_some {spec : Spec} {off k : Nat} {seen : List Row} {next : Block}
    {cutoff : Row} (h : (ordered spec seen)[off + k - 1]? = some cutoff) :
    eligible spec off k seen next = (lex spec next.low cutoff.key != Ordering.gt) := by
  rw [← eligible_cases, h]

private theorem kLe_of_eligible_true {spec : Spec} {off k : Nat} {seen : List Row}
    {next : Block} {cutoff : Row} (hcut : (ordered spec seen)[off + k - 1]? = some cutoff)
    (h : eligible spec off k seen next = true) : kLe spec next.low cutoff.key := by
  rw [eligible_some hcut] at h
  exact ofBoolNe h

private theorem eligible_false_of_kLt {spec : Spec} {off k : Nat} {seen : List Row}
    {next : Block} {cutoff : Row} (hcut : (ordered spec seen)[off + k - 1]? = some cutoff)
    (h : kLt spec cutoff.key next.low) :
    eligible spec off k seen next = false := by
  rw [eligible_some hcut]
  exact neGtBool (gt_of_kLt h)

private theorem Sublist_prepend : ∀ (pre : List Row) {s t : List Row}, s <+ t → s <+ pre ++ t := by
  intro pre
  induction pre with
  | nil => intro s t h; exact h
  | cons a pre ih =>
      intro s t h
      exact Sublist.cons a (ih h)

private theorem Sublist_flatMap_rows : ∀ {s b : List Block}, s <+ b →
    s.flatMap Block.rows <+ b.flatMap Block.rows := by
  intro s b h
  induction h with
  | slnil => exact Sublist.refl _
  | @cons s' t' a hst ih =>
      show s'.flatMap Block.rows <+ a.rows ++ t'.flatMap Block.rows
      exact Sublist_prepend a.rows ih
  | @cons_cons s' t' a hst ih =>
      show a.rows ++ s'.flatMap Block.rows <+ a.rows ++ t'.flatMap Block.rows
      exact Sublist.append (Sublist.refl _) ih

/-! ## Part F (continued): utilities for the fence -/

private theorem kLt_of_lt {spec : Spec} {a b : Key} (h : lex spec a b = Ordering.lt) :
    kLt spec a b := h

private theorem beqLtTrue {o : Ordering} (h : (o == Ordering.lt) = true) : o = Ordering.lt := by
  cases o <;> simp_all (config := {decide := true})

private theorem kLtb_eq_true {spec : Spec} {x y : Row} (h : kLtb spec x y = true) :
    kLt spec x.key y.key :=
  kLt_of_lt (beqLtTrue h)

private theorem gt_of_not_kLe {spec : Spec} {a b : Key} (h : ¬ kLe spec a b) :
    lex spec a b = Ordering.gt := by
  cases hlex : lex spec a b with
  | lt =>
      exact absurd h (fun hle => by
        unfold kLe at hle
        rw [hlex] at hle
        exact hle (by decide))
  | eq =>
      exact absurd h (fun hle => by
        unfold kLe at hle
        rw [hlex] at hle
        exact hle (by decide))
  | gt => rfl

private theorem kLe_of_lex_eq {spec : Spec} {a b : Key} (h : lex spec a b = Ordering.eq) :
    kLe spec a b := by
  unfold kLe
  rw [h]
  decide

private theorem eligible_true_of_kLe {spec : Spec} {off k : Nat} {seen : List Row}
    {next : Block} {cutoff : Row} (hcut : (ordered spec seen)[off + k - 1]? = some cutoff)
    (h : kLe spec next.low cutoff.key) : eligible spec off k seen next = true := by
  rw [eligible_some hcut]
  exact toBoolNe h

private theorem decideBoolEq (p q : Prop) [inst₁ : Decidable p] [inst₂ : Decidable q]
    (h : p ↔ q) : decide p = decide q := by
  cases inst₁ with
  | isTrue hp =>
      have hq : q := h.mp hp
      cases inst₂ with
      | isTrue hq' => rfl
      | isFalse hqf => exact absurd hq hqf
  | isFalse hpf =>
      have hqf : ¬ q := fun hq2 => hpf (h.mpr hq2)
      cases inst₂ with
      | isTrue hq => exact absurd hq hqf
      | isFalse hqf' => rfl

private theorem page_zero (spec : Spec) (off : Nat) (rows : List Row) :
    page spec off 0 rows = [] := by
  show List.filter
      (fun row => (List.filter (fun other => kLtb spec other row)
        (List.drop off (ordered spec rows))).length < 0)
      (List.drop off (ordered spec rows)) = _
  refine filter_nil_of_false
    (fun row => (List.filter (fun other => kLtb spec other row)
      (List.drop off (ordered spec rows))).length < 0)
    (List.drop off (ordered spec rows)) ?_
  intro row _
  exact decide_eq_false (Nat.not_lt_zero _)

private theorem getElem?_drop_eq {α : Type} : ∀ (l : List α) (t i : Nat),
    (List.drop t l)[i]? = l[i + t]? := by
  intro l
  induction l with
  | nil =>
      intro t i
      cases t <;> rfl
  | cons hd tl ih =>
      intro t i
      cases t with
      | zero => simp [List.drop]
      | succ t =>
          show (List.drop t tl)[i]? = (hd :: tl)[(i + t) + 1]?
          rw [ih]
          rcases Nat.lt_or_ge (i + t) tl.length with hlt | hge
          · have h4 : (i + t) + 1 < (hd :: tl).length :=
              show (i + t) + 1 < tl.length + 1 from by omega
            rw [getElem?_pos tl (i + t) hlt, getElem?_pos (hd :: tl) ((i + t) + 1) h4]
            exact congrArg (Option.some : α → Option α) (getElem_cons_succ hd tl (i + t) h4).symm
          · have n1 : (hd :: tl)[(i + t) + 1]? = none :=
              getElem?_eq_none (show tl.length + 1 ≤ (i + t) + 1 from by omega)
            have n2 : tl[i + t]? = none := getElem?_eq_none hge
            rw [n1, n2]

private theorem drop_length_pos {α : Type} (l : List α) (t i : Nat)
    (h : i + t < l.length) : i < (List.drop t l).length := by
  rw [List.length_drop]
  omega

private theorem getElem_drop_eq {α : Type} (l : List α) (t i : Nat)
    (hlt : i + t < l.length) :
    (List.drop t l)[i]'(drop_length_pos l t i hlt) = l[i + t] := by
  have h1 : (List.drop t l)[i]? = some l[i + t] := by
    rw [getElem?_drop_eq, getElem?_pos l (i + t) hlt]
  have hb : i < (List.drop t l).length := drop_length_pos l t i hlt
  have h2 : (List.drop t l)[i]? = some ((List.drop t l)[i]'hb) :=
    getElem?_pos (List.drop t l) i hb
  exact Option.some.inj ((Eq.symm h2).trans h1)

private theorem page_eq_filter {spec : Spec} {off k : Nat} (hk : k ≠ 0) (rows : List Row)
    (e : Row) (hb : ∀ r ∈ rows, r.key.length = spec.length)
    (hn : (rows.map Row.id).Nodup)
    (hcut : (ordered spec rows)[off + k - 1]? = some e) :
    page spec off k rows =
      (List.drop off (ordered spec rows)).filter
        (fun row => (lex spec row.key e.key != Ordering.gt)) := by
  have hlen : (ordered spec rows).length = rows.length := length_ordered spec rows
  have hP : off + k - 1 < (ordered spec rows).length :=
    lt_length_of_get?_some _ _ hcut
  have hle : (ordered spec rows)[off + k - 1] = e := by
    have h2 := getElem?_pos (ordered spec rows) (off + k - 1) hP
    exact Option.some.inj ((Eq.symm h2).trans hcut)
  have hpord : Pairwise (R spec) (ordered spec rows) := ordered_pairwise rows hb
  have hnord : ((ordered spec rows).map Row.id).Nodup :=
    ((List.mergeSort_perm rows (ord spec)).symm.map Row.id).nodup hn
  have hmemrows : ∀ x ∈ ordered spec rows, x.key.length = spec.length := by
    intro x hx
    exact hb x ((mem_ordered rows x).mp hx)
  have helen : e.key.length = spec.length :=
    hmemrows e (by rw [← hle]; exact List.getElem_mem hP)
  have hlenrows : rows.length ≥ off + k := by omega
  have hafter : (List.drop off (ordered spec rows)).length = rows.length - off := by
    rw [List.length_drop, length_ordered]
  have hlenafter : (List.drop off (ordered spec rows)).length ≥ k := by
    rw [hafter]
    omega
  have hmemafter : ∀ x ∈ List.drop off (ordered spec rows), x.key.length = spec.length := by
    intro x hx
    exact hmemrows x (List.mem_of_mem_drop hx)
  have hnAft : ((List.drop off (ordered spec rows)).map Row.id).Nodup := by
    rw [List.map_drop]
    exact (List.drop_sublist off _).nodup hnord
  show List.filter
      (fun row => ((List.drop off (ordered spec rows)).filter
        (fun other => kLtb spec other row)).length < k)
      (List.drop off (ordered spec rows)) = _
  refine filter_ext (List.drop off (ordered spec rows)) ?_
  intro row hrow
  obtain ⟨i, hi, hri⟩ := List.mem_iff_getElem.mp hrow
  have hbound : i < rows.length - off := by
    have := hi
    rw [hafter] at this
    exact this
  have hli : (ordered spec rows)[i + off] = row :=
    getElem_drop_eq (ordered spec rows) off i (by omega) |>.symm.trans hri
  have hke : kLe spec row.key e.key →
      ((List.drop off (ordered spec rows)).filter (fun other => kLtb spec other row)).length < k := by
    intro hkLe
    have hsub : ∀ x ∈ (List.drop off (ordered spec rows)).filter (fun other => kLtb spec other row),
        x ∈ List.take (k - 1) (List.drop off (ordered spec rows)) := by
      intro x hx
      obtain ⟨hxm, hxk⟩ := List.mem_filter.mp hx
      obtain ⟨j, hj, hxj⟩ := List.mem_iff_getElem.mp hxm
      have hlx : kLt spec x.key row.key := kLtb_eq_true hxk
      have hjb : j < rows.length - off := by
        have := hj
        rw [hafter] at this
        exact this
      have hxP : (ordered spec rows)[j + off] = x :=
        getElem_drop_eq (ordered spec rows) off j (by omega) |>.symm.trans hxj
      have hpos : off + j < off + k - 1 := by
        rcases Nat.lt_or_ge (off + j) (off + k - 1) with hlt2 | hge2
        · exact hlt2
        · rcases Nat.eq_or_lt_of_le hge2 with heq | hgt2
          · have hp1 : (ordered spec rows)[j + off]? = some x :=
              (getElem?_pos (ordered spec rows) (j + off) (by omega)).trans
                (congrArg (Option.some : Row → Option Row) hxP)
            have hq : (ordered spec rows)[off + k - 1]? = (ordered spec rows)[j + off]? :=
              congrArg (fun a => (ordered spec rows)[a]?) (by omega)
            have hxe : x = e := Option.some.inj ((Eq.symm hp1).trans ((Eq.symm hq).trans hcut))
            rw [hxe] at hlx
            exact False.elim (kLt_kLe_irrefl helen (hmemafter row hrow) hlx hkLe)
          · have hRe : R spec e x := by
              rw [← hle, ← hxP]
              exact R_get_of_lt (i := off + k - 1) (j := j + off) hpord hP (by omega) (by omega)
            have h1 : kLt spec e.key row.key :=
              kLe_kLt_trans helen (hmemrows x (List.mem_of_mem_drop hxm))
                (hmemafter row hrow) (R_imp_kLe hRe) hlx
            exact False.elim (kLt_kLe_irrefl helen (hmemafter row hrow) h1 hkLe)
      have hj' : j < k - 1 := by omega
      have hltj : j < (List.take (k - 1) (List.drop off (ordered spec rows))).length := by
        rw [List.length_take]
        omega
      have heq : (List.take (k - 1) (List.drop off (ordered spec rows)))[j] = x := by
        rw [List.getElem_take]
        exact hxj
      exact heq ▸ List.getElem_mem hltj
    have hcard := card_le_of_mem_nodup
      ((List.drop off (ordered spec rows)).filter (fun other => kLtb spec other row))
      (nodup_mapid_filter _ _ hnAft) _ hsub
    have hlenT : (List.take (k - 1) (List.drop off (ordered spec rows))).length ≤ k - 1 :=
      List.length_take_le _ _
    exact Nat.lt_of_le_of_lt (Nat.le_trans hcard hlenT) (by omega)
  have hge : ¬ kLe spec row.key e.key →
      k ≤ ((List.drop off (ordered spec rows)).filter (fun other => kLtb spec other row)).length := by
    intro hnr
    have hlt : kLt spec e.key row.key := kLt_of_gt (gt_of_not_kLe hnr)
    have hrowlen : row.key.length = spec.length := hmemafter row hrow
    have hG : ∀ x ∈ List.take k (List.drop off (ordered spec rows)),
        kLtb spec x row = true := by
      intro x hx
      obtain ⟨m, hm, hxm⟩ := List.mem_iff_getElem.mp hx
      have hmb : m < rows.length - off := by
        have : m < (List.take k (List.drop off (ordered spec rows))).length := hm
        rw [List.length_take] at this
        omega
      have hmm2 : m + off < (ordered spec rows).length := by
        rw [hlen]
        omega
      have h1 : (List.take k (List.drop off (ordered spec rows)))[m]? = some x :=
        (getElem?_pos (List.take k (List.drop off (ordered spec rows))) m hm).trans
          (congrArg (Option.some : Row → Option Row) hxm)
      have hmbk : m < k :=
        Nat.lt_of_lt_of_le hm (List.length_take_le k _)
      have h2 : (List.drop off (ordered spec rows))[m]? =
          (List.take k (List.drop off (ordered spec rows)))[m]? :=
        Eq.trans (if_pos hmbk).symm (getElem?_take (l := List.drop off (ordered spec rows))
          (i := k) (j := m)).symm
      have h3 : (List.drop off (ordered spec rows))[m]? = (ordered spec rows)[m + off]? :=
        getElem?_drop_eq (ordered spec rows) off m
      have hxq : (ordered spec rows)[m + off]? = some x :=
        Eq.trans h3.symm (Eq.trans h2 h1)
      have hxg : x = (ordered spec rows)[m + off]'hmm2 :=
        Option.some.inj (hxq.symm.trans (getElem?_pos (ordered spec rows) (m + off) hmm2))
      have hkex : kLe spec x.key e.key := by
        rcases Nat.lt_or_ge (m + off) (off + k - 1) with hlt2 | hge2
        · rw [hxg, ← hle]
          exact R_imp_kLe (R_get_of_lt (i := m + off) (j := off + k - 1) hpord hmm2 hP hlt2)
        · have hq1 : (ordered spec rows)[m + off]? = (ordered spec rows)[off + k - 1]? :=
            congrArg (fun a => (ordered spec rows)[a]?) (by omega)
          have hxe : x = e := Option.some.inj (hxq.symm.trans (hq1.trans hcut))
          rw [hxe]
          exact kLe_refl _ _
      exact kLtb_true (kLe_kLt_trans (hmemrows x (List.mem_of_mem_drop (List.mem_of_mem_take hx)))
        helen hrowlen hkex hlt)
    have hcard := card_le_of_mem_nodup
      (List.take k (List.drop off (ordered spec rows)))
      (by
        rw [List.map_take]
        refine (List.take_sublist k _).nodup ?_
        rw [List.map_drop]
        exact (List.drop_sublist off _).nodup hnord)
      ((List.drop off (ordered spec rows)).filter (fun other => kLtb spec other row))
      (by
        intro x hx
        exact List.mem_filter.mpr ⟨List.mem_of_mem_take hx, hG x hx⟩)
    have hlenG : (List.take k (List.drop off (ordered spec rows))).length = k := by
      rw [List.length_take]
      exact Nat.min_eq_left hlenafter
    rw [hlenG] at hcard
    exact hcard
  have hiff : (((List.drop off (ordered spec rows)).filter
      (fun other => kLtb spec other row)).length < k) ↔ kLe spec row.key e.key := by
    constructor
    · intro h
      rcases Classical.em (kLe spec row.key e.key) with hkLe | hnr
      · exact hkLe
      · exact absurd h (Nat.not_lt_of_le (hge hnr))
    · exact hke
  rcases Nat.lt_or_ge ((List.drop off (ordered spec rows)).filter
      (fun other => kLtb spec other row)).length k with hcnt | hcnt
  · rw [decide_eq_true hcnt, toBoolNe (hiff.mp hcnt)]
  · rw [decide_eq_false (Nat.not_lt_of_le hcnt),
      neGtBool (gt_of_not_kLe (fun hkLe => Nat.not_lt_of_le hcnt (hiff.mpr hkLe)))]
/-! ## Part F (fence): position, permutation, and sorted-filter helpers -/

private theorem getElem_of_mem_take : ∀ (blocks : List Block) (t : Nat) (b : Block),
    b ∈ blocks.take t →
      ∃ i : Nat, ∃ hi : i < blocks.length, i < t ∧ blocks.get ⟨i, hi⟩ = b := by
  intro blocks
  induction blocks with
  | nil => intro t b hb; exact absurd hb (by simp)
  | cons hd tl ih =>
      intro t b hb
      cases t with
      | zero => exact absurd hb (by simp)
      | succ t =>
          rw [List.take_succ_cons] at hb
          rcases List.mem_cons.mp hb with rfl | hb'
          · exact ⟨0, by simp, Nat.zero_lt_succ _, rfl⟩
          · obtain ⟨i, hi, hit, hget⟩ := ih t b hb'
            refine ⟨i + 1, by simpa using hi, by omega, ?_⟩
            exact hget

private theorem neGtBoolFalse {o : Ordering} (h : (o != Ordering.gt) = false) :
    o = Ordering.gt := by
  cases o with
  | lt | eq => exact absurd h (by decide)
  | gt => rfl

private theorem perm_move_front {α : Type} (x : α) :
    ∀ (pre post : List α), x :: (pre ++ post) ~ pre ++ x :: post
  | [], post => List.Perm.refl _
  | a :: pre, post => by
      show x :: a :: (pre ++ post) ~ a :: (pre ++ x :: post)
      exact (List.Perm.swap a x (pre ++ post)).trans (List.Perm.cons a (perm_move_front x pre post))

private theorem perm_of_nodup_forall_mem {α : Type} :
    ∀ (l l' : List α), (∀ x, x ∈ l ↔ x ∈ l') → l.Nodup → l'.Nodup → l ~ l'
  | [], [], _, _, _ => List.Perm.refl _
  | [], y :: ys, h, _, hn' =>
      absurd ((h y).mpr List.mem_cons_self) (by simp)
  | x :: xs, [], h, hn, _ =>
      absurd ((h x).mp List.mem_cons_self) (by simp)
  | x :: xs, l', h, hn, hn' => by
      have hxl : x ∈ l' := (h x).mp List.mem_cons_self
      obtain ⟨pre, post, hl', hnp⟩ := split_first_occ l' x hxl
      have hswap : x :: (pre ++ post) ~ l' := hl' ▸ perm_move_front x pre post
      have hnp' : (x :: (pre ++ post)).Nodup := hswap.symm.nodup hn'
      have hnx : ¬ x ∈ pre ++ post := (List.nodup_cons.mp hnp').1
      have hmem2 : ∀ z, z ∈ xs ↔ z ∈ pre ++ post := by
        intro z
        constructor
        · intro hz
          rcases List.mem_append.mp (hl' ▸ ((h z).mp (List.mem_cons_of_mem x hz))) with hpre | hxp
          · exact List.mem_append.mpr (Or.inl hpre)
          · rcases List.mem_cons.mp hxp with hzx | hpost
            · exact absurd (hzx ▸ hz) ((List.nodup_cons.mp hn).1)
            · exact List.mem_append.mpr (Or.inr hpost)
        · intro hz
          rcases List.mem_append.mp hz with hpre | hpost
          · have hd := List.mem_cons.mp ((h z).mpr (hl' ▸ List.mem_append.mpr (Or.inl hpre)))
            exact hd.resolve_left (fun hzx => hnx (List.mem_append.mpr (Or.inl (hzx ▸ hpre))))
          · have hd := List.mem_cons.mp
              ((h z).mpr (hl' ▸ List.mem_append.mpr (Or.inr (List.mem_cons_of_mem x hpost))))
            exact hd.resolve_left (fun hzx => hnx (List.mem_append.mpr (Or.inr (hzx ▸ hpost))))
      exact (List.Perm.cons x
        (perm_of_nodup_forall_mem xs (pre ++ post) hmem2 (List.nodup_cons.mp hn).2
          (List.nodup_cons.mp hnp').2)).trans hswap

private theorem nodup_rows_of_mapid :
    ∀ (l : List Row), ((l.map Row.id).Nodup) → l.Nodup
  | [], _ => List.nodup_nil
  | x :: xs, hn => by
      obtain ⟨hh, htl⟩ := List.nodup_cons.mp hn
      refine List.nodup_cons.mpr
        ⟨fun hx => hh (List.mem_map_of_mem (f := Row.id) hx), nodup_rows_of_mapid xs htl⟩

private theorem rowEq_of_mapid_nodup : ∀ (l : List Row) (a b : Row),
    (l.map Row.id).Nodup → a ∈ l → b ∈ l → a.id = b.id → a = b
  | [], a, b, _, ha, _, _ => absurd ha (by simp)
  | x :: xs, a, b, hn, ha, hb, hij => by
      obtain ⟨hh, htl⟩ := List.nodup_cons.mp hn
      rcases List.mem_cons.mp ha with hx | ha'
      · rcases List.mem_cons.mp hb with hy | hb'
        · exact hx.trans hy.symm
        · have hxb : x.id = b.id := by rw [← hx]; exact hij
          have hm : x.id ∈ xs.map Row.id := by
            rw [hxb]; exact List.mem_map_of_mem (f := Row.id) hb'
          exact absurd hm hh
      · rcases List.mem_cons.mp hb with hy | hb'
        · have hax : a.id = x.id := hij.trans (congrArg Row.id hy)
          have hm : x.id ∈ xs.map Row.id := by
            rw [← hax]; exact List.mem_map_of_mem (f := Row.id) ha'
          exact absurd hm hh
        · exact rowEq_of_mapid_nodup xs a b htl ha' hb' hij

/-- Two `R`-sorted nodup bounded lists with the same members have equal `P`-filters. -/
private theorem sorted_filter_eq {spec : Spec} (P : Row → Bool) :
    ∀ (r₁ r₂ : List Row),
      Pairwise (R spec) r₁ → Pairwise (R spec) r₂ →
      (∀ r ∈ r₁, r.key.length = spec.length) → (∀ r ∈ r₂, r.key.length = spec.length) →
      ((r₁.map Row.id).Nodup) → ((r₂.map Row.id).Nodup) →
      (∀ x, x ∈ r₁ ↔ x ∈ r₂) →
      r₁.filter P = r₂.filter P := by
  intro r₁ r₂ hp₁ hp₂ hb₁ hb₂ hn₁ hn₂ hmem
  refine List.Perm.eq_of_pairwise
    (fun a b ham hbm hab hba => ?_)
    (Pairwise.filter P hp₁) (Pairwise.filter P hp₂)
    (perm_of_nodup_forall_mem (r₁.filter P) (r₂.filter P)
      (fun x => by rw [List.mem_filter, List.mem_filter, hmem])
      (nodup_rows_of_mapid _ (nodup_mapid_filter r₁ P hn₁))
      (nodup_rows_of_mapid _ (nodup_mapid_filter r₂ P hn₂)))
  · obtain ⟨_, hid⟩ :=
      R_antisymm (hb₁ a ((List.mem_filter.mp ham).1)) (hb₂ b ((List.mem_filter.mp hbm).1)) hab hba
    exact rowEq_of_mapid_nodup (r₁.filter P) a b (nodup_mapid_filter r₁ P hn₁)
      ham ((List.mem_filter.mpr ⟨(hmem b).mpr ((List.mem_filter.mp hbm).1),
        (List.mem_filter.mp hbm).2⟩)) hid
/-- Direction A: a block that passes the scan test against its accumulation prefix
also passes the same test against the fully flattened block list. -/
private theorem eligible_of_prefix {spec : Spec} {off k : Nat} (hk : k ≠ 0) :
    ∀ (blocks : List Block) (i : Nat),
      Pairwise (fun x y => kLe spec x.low y.low) blocks →
      (∀ b ∈ blocks, valid spec b) →
      (∀ b ∈ blocks, b.low.length = spec.length) →
      (∀ b ∈ blocks, ∀ r ∈ b.rows, r.key.length = spec.length) →
      ((blocks.flatMap Block.rows).map Row.id).Nodup →
      (hi : i < blocks.length) →
      (k != 0 && eligible spec off k ((blocks.take i).flatMap Block.rows)
        (blocks.get ⟨i, hi⟩)) = true →
      eligible spec off k (blocks.flatMap Block.rows) (blocks.get ⟨i, hi⟩) = true := by
  intro blocks i hp hval hlow hrows hn hi htest
  obtain ⟨_, hel_i⟩ := (Bool.and_eq_true _ _).mp htest
  let m := off + k - 1
  let b := blocks.get ⟨i, hi⟩
  let acc := (blocks.take i).flatMap Block.rows
  let allR := blocks.flatMap Block.rows
  have hb : b ∈ blocks := getElem_mem blocks i hi
  have hall : ∀ r ∈ allR, r.key.length = spec.length := by
    intro r hr
    obtain ⟨bl, hbl, hr'⟩ := List.mem_flatMap.mp hr
    exact hrows bl hbl r hr'
  have hacc : ∀ r ∈ acc, r.key.length = spec.length := by
    intro r hr
    obtain ⟨bl, hbl, hr'⟩ := List.mem_flatMap.mp hr
    exact hrows bl (List.mem_of_mem_take hbl) r hr'
  obtain ⟨w, hw, hwe⟩ := (hval b hb).1
  have hwbnd : w.key.length = spec.length :=
    hall w (List.mem_flatMap.mpr ⟨b, hb, hw⟩)
  cases hcut : (ordered spec allR)[m]? with
  | none => exact eligible_none hcut
  | some e =>
      have hkbe : kLe spec b.low e.key := by
        rcases Classical.em (kLe spec b.low e.key) with h | hneg
        · exact h
        · have hLwe : kLt spec e.key w.key := by
            rw [hwe]
            exact kLt_of_gt (gt_of_not_kLe hneg)
          let o₂ := ordered spec allR
          let o₁ := ordered spec acc
          have hlenA : m < o₂.length := lt_length_of_get?_some _ _ hcut
          have hpe : o₂[m] = e := by
            have h1 := hcut
            rw [getElem?_pos o₂ m hlenA] at h1
            exact Option.some.inj h1
          have hem : e ∈ o₂ := by
            rw [← hpe]
            exact getElem_mem o₂ m hlenA
          have hp₂ : Pairwise (R spec) o₂ := ordered_pairwise allR hall
          have hn₂ : (o₂.map Row.id).Nodup :=
            ((List.mergeSort_perm allR (ord spec)).symm.map Row.id).nodup hn
          have hall' : ∀ y ∈ o₂, y.key.length = spec.length :=
            fun y hy => hall y ((mem_ordered allR y).mp hy)
          have htail : ∀ r ∈ (blocks.drop i).flatMap Block.rows, kLe spec w.key r.key := by
            intro r hr
            have h1 := low_le_rows spec blocks i hp hval hlow hrows hi r hr
            rw [hwe]
            exact h1
          have hsplit : ∀ r ∈ allR, r ∈ acc ∨ r ∈ (blocks.drop i).flatMap Block.rows := by
            intro r hr
            obtain ⟨bl, hbl, hr'⟩ := List.mem_flatMap.mp hr
            have hbl2 : bl ∈ blocks.take i ++ blocks.drop i := by
              rw [List.take_append_drop i blocks]
              exact hbl
            rcases List.mem_append.mp hbl2 with ht | hd
            · exact Or.inl (List.mem_flatMap.mpr ⟨bl, ht, hr'⟩)
            · exact Or.inr (List.mem_flatMap.mpr ⟨bl, hd, hr'⟩)
          let qs := o₁.filter (fun y => lex spec y.key w.key == Ordering.lt)
          have hmemqs : ∀ y ∈ o₂.take (m + 1), y ∈ qs := by
            intro y hy
            obtain ⟨p, hp2, hop⟩ := List.mem_iff_getElem.mp hy
            have hp_le : p ≤ m := by
              rw [List.length_take] at hp2
              omega
            have hyl : y ∈ o₂ := List.mem_of_mem_take hy
            have hp2len : p < o₂.length := by omega
            have hopy : o₂[p]'hp2len = y := by
              rw [← List.getElem_take]
              exact hop
            have hry : R spec y e := by
              rcases Nat.eq_or_lt_of_le hp_le with rfl | hpm
              · have hye : y = e := hopy.symm.trans hpe
                rw [hye]
                exact R_refl e
              · rw [← hopy, ← hpe]
                exact R_get_of_lt hp₂ hp2len hlenA hpm
            have hky : kLt spec y.key w.key :=
              kLe_kLt_trans (hall' y hyl) (hall' e hem) hwbnd (R_imp_kLe hry) hLwe
            rcases hsplit y ((mem_ordered allR y).mp hyl) with haccy | htaily
            · exact List.mem_filter.mpr ⟨(mem_ordered acc y).mpr haccy, by
                show (lex spec y.key w.key == Ordering.lt) = true
                rw [hky]
                decide⟩
            · exact absurd (htail y htaily) (kLt_kLe_irrefl (hall' y hyl) hwbnd hky)
          have hqs_lb : m + 1 ≤ qs.length := by
            have hlen : (o₂.take (m + 1)).length = m + 1 := by
              rw [List.length_take, Nat.min_eq_left (Nat.succ_le_of_lt hlenA)]
            have hcard :=
              card_le_of_mem_nodup (o₂.take (m + 1)) (nodup_mapid_take hn₂ (m + 1)) qs hmemqs
            omega
          have hqs_ub : qs.length ≤ m := by
            cases hcut1 : (ordered spec acc)[m]? with
            | none =>
                have hl1 : o₁.length ≤ m := by
                  rcases Nat.lt_or_ge m o₁.length with hlt | hge
                  · have h1 := (getElem?_pos o₁ m hlt).symm
                    rw [hcut1] at h1
                    exact absurd h1 (by simp)
                  · exact hge
                exact Nat.le_trans (List.length_filter_le _ _) hl1
            | some c =>
                have hl1 : m < o₁.length := lt_length_of_get?_some _ _ hcut1
                have hcm : o₁[m] = c := by
                  have h1 := hcut1
                  rw [getElem?_pos o₁ m hl1] at h1
                  exact Option.some.inj h1
                have hcmem : c ∈ o₁ := by
                  rw [← hcm]
                  exact getElem_mem o₁ m hl1
                have hp₁ : Pairwise (R spec) o₁ := ordered_pairwise acc hacc
                have hbc : ∀ r ∈ o₁, r.key.length = spec.length :=
                  fun r hr => hacc r ((mem_ordered acc r).mp hr)
                have hn₁ : (o₁.map Row.id).Nodup := by
                  have hnm : (acc.map Row.id).Nodup :=
                    (List.Sublist.map Row.id
                      (Sublist_flatMap_rows (List.take_sublist i blocks))).nodup hn
                  exact ((List.mergeSort_perm acc (ord spec)).symm.map Row.id).nodup hnm
                have hle_wc : kLe spec w.key c.key := by
                  have h1 := kLe_of_eligible_true hcut1 hel_i
                  rw [hwe]
                  exact h1
                have hmemq : ∀ y ∈ qs, y ∈ o₁.take m := by
                  intro y hy
                  obtain ⟨hyo, hylt⟩ := List.mem_filter.mp hy
                  have hky : kLt spec y.key w.key := kLt_of_lt (beqLtTrue hylt)
                  have hyc : kLt spec y.key c.key :=
                    kLt_kLe_trans (hbc y hyo) hwbnd (hbc c hcmem) hky hle_wc
                  obtain ⟨p, hp2, hop⟩ := List.mem_iff_getElem.mp hyo
                  have hpm : p < m :=
                    lt_index_of_kLt hp₁ hbc hp2 hl1 (by rw [hop, hcm]; exact hyc)
                  have hpos : p < (o₁.take m).length := by
                    rw [List.length_take]
                    omega
                  have h2 : (o₁.take m)[p]'hpos = y := by
                    rw [List.getElem_take]
                    exact hop
                  exact List.mem_of_getElem h2
                have hcard :=
                  card_le_of_mem_nodup qs (nodup_mapid_filter o₁ _ hn₁) (o₁.take m) hmemq
                rw [List.length_take, Nat.min_eq_left (Nat.le_of_lt hl1)] at hcard
                exact hcard
          exact absurd hqs_ub (by omega)
      exact eligible_true_of_kLe hcut hkbe
/-- Same as `sorted_filter_eq` but the membership equivalence is only required after
filtering. -/
private theorem sorted_filter_eq' {spec : Spec} (P : Row → Bool) :
    ∀ (r₁ r₂ : List Row),
      Pairwise (R spec) r₁ → Pairwise (R spec) r₂ →
      (∀ r ∈ r₁, r.key.length = spec.length) → (∀ r ∈ r₂, r.key.length = spec.length) →
      ((r₁.map Row.id).Nodup) → ((r₂.map Row.id).Nodup) →
      (∀ x, x ∈ r₁.filter P ↔ x ∈ r₂.filter P) →
      r₁.filter P = r₂.filter P := by
  intro r₁ r₂ hp₁ hp₂ hb₁ hb₂ hn₁ hn₂ hmem
  refine List.Perm.eq_of_pairwise
    (fun a b ham hbm hab hba => ?_)
    (Pairwise.filter P hp₁) (Pairwise.filter P hp₂)
    (perm_of_nodup_forall_mem (r₁.filter P) (r₂.filter P) hmem
      (nodup_rows_of_mapid _ (nodup_mapid_filter r₁ P hn₁))
      (nodup_rows_of_mapid _ (nodup_mapid_filter r₂ P hn₂)))
  · obtain ⟨_, hid⟩ :=
      R_antisymm (hb₁ a ((List.mem_filter.mp ham).1)) (hb₂ b ((List.mem_filter.mp hbm).1)) hab hba
    exact rowEq_of_mapid_nodup (r₁.filter P) a b (nodup_mapid_filter r₁ P hn₁)
      ham ((hmem b).mpr hbm) hid

private theorem eligible_congr_ordered {spec : Spec} {off k : Nat} {rows rows' : List Row}
    (next : Block) (h : ordered spec rows = ordered spec rows') :
    eligible spec off k rows next = eligible spec off k rows' next := by
  rw [← eligible_cases, ← eligible_cases, h]

private theorem fence
    (spec : Spec) (off k : Nat) (directory : List Block)
    (hvalid : ∀ b ∈ directory, valid spec b)
    (hn : ((directory.flatMap Block.rows).map Row.id).Nodup)
    (hbnd : ∀ r ∈ directory.flatMap Block.rows, r.key.length = spec.length) :
    let blocks := directory.mergeSort (fun x y => lex spec x.low y.low != Ordering.gt)
    page spec off k (scan spec off k [] blocks).1 =
        page spec off k (directory.flatMap Block.rows) ∧
      (scan spec off k [] blocks).2 =
        blocks.filter (fun b => k != 0 && eligible spec off k (directory.flatMap Block.rows) b) := by
  intro blocks
  let allR := directory.flatMap Block.rows
  have hpermB : blocks ~ directory :=
    List.mergeSort_perm directory (fun x y => lex spec x.low y.low != Ordering.gt)
  have hpermF : blocks.flatMap Block.rows ~ allR := List.Perm.flatMap_right Block.rows hpermB
  have hnB : ((blocks.flatMap Block.rows).map Row.id).Nodup :=
    (hpermF.map Row.id).symm.nodup hn
  have hbndB : ∀ r ∈ blocks.flatMap Block.rows, r.key.length = spec.length := by
    intro r hr
    exact hbnd r ((perm_mem hpermF r).mp hr)
  have hvalB : ∀ b ∈ blocks, valid spec b := by
    intro b hb
    exact hvalid b ((perm_mem hpermB b).mp hb)
  have hlowB : ∀ b ∈ blocks, b.low.length = spec.length := by
    intro b hb
    exact low_bnd spec b ((perm_mem hpermB b).mp hb) hvalid hbnd
  have hrowsB : ∀ b ∈ blocks, ∀ r ∈ b.rows, r.key.length = spec.length := by
    intro b hb r hr
    exact hbnd r ((perm_mem hpermF r).mp (List.mem_flatMap.mpr ⟨b, hb, hr⟩))
  have hpB : Pairwise (fun x y => kLe spec x.low y.low) blocks :=
    sorted_lows_pairwise spec directory hvalid hbnd
  rcases Classical.em (k = 0) with hz | hkt
  · -- k = 0: nothing is opened and both pages are empty
    subst hz
    obtain ⟨t, htle, hscan, hpre, hstop⟩ := scan_prefix spec off 0 blocks []
    have htake : t = 0 := by
      rcases Nat.eq_zero_or_pos t with rfl | htp
      · rfl
      · have hpos : 0 < blocks.length := Nat.lt_of_lt_of_le htp htle
        obtain ⟨h1, _⟩ := (Bool.and_eq_true _ _).mp (hpre 0 hpos htp)
        exact absurd h1 (by decide)
    constructor
    · rw [page_zero, page_zero]
    · have hopened0 : (scan spec off 0 [] blocks).2 = blocks.take t := congrArg Prod.snd hscan
      have hnil :
          blocks.filter (fun b => 0 != 0 && eligible spec off 0 allR b) = [] := by
        refine filter_nil_of_false (fun b => 0 != 0 && eligible spec off 0 allR b) blocks ?_
        intro b _
        have h0 : ((0 : Nat) != 0) = false := by decide
        rw [h0]
        exact Bool.false_and _
      rw [hopened0, htake, List.take_zero, hnil]
  · -- k ≠ 0 for the rest of the proof
    obtain ⟨t, htle, hscan, hpre, hstop⟩ := scan_prefix spec off k blocks []
    let accT := (blocks.take t).flatMap Block.rows
    let allF := blocks.flatMap Block.rows
    have hseen : (scan spec off k [] blocks).1 = accT := congrArg Prod.fst hscan
    have hopened : (scan spec off k [] blocks).2 = blocks.take t := congrArg Prod.snd hscan
    have hmemAF : ∀ r ∈ accT, r ∈ allF := by
      intro r hr
      obtain ⟨b, hb, hr'⟩ := List.mem_flatMap.mp hr
      exact List.mem_flatMap.mpr ⟨b, List.mem_of_mem_take hb, hr'⟩
    have hbndA : ∀ r ∈ accT, r.key.length = spec.length := by
      intro r hr
      exact hbndB r (hmemAF r hr)
    have hnA : ((accT.map Row.id).Nodup) :=
      (List.Sublist.map Row.id (Sublist_flatMap_rows (List.take_sublist t blocks))).nodup hnB
    -- every block opened by the scan also passes the test against all rows
    have h1 : ∀ b ∈ blocks.take t,
        (fun b => k != 0 && eligible spec off k allR b) b = true := by
      intro b hb
      obtain ⟨i, hi, hit, hget⟩ := getElem_of_mem_take blocks t b hb
      have he := eligible_of_prefix hkt blocks i hpB hvalB hlowB hrowsB hnB hi (hpre i hi hit)
      rw [eligible_congr_ordered _ (ordered_perm hbndB hbnd hnB hpermF)] at he
      refine (Bool.and_eq_true _ _).mpr ⟨neZeroBool k hkt, ?_⟩
      exact hget ▸ he
    rw [hseen, hopened]
    rcases Nat.eq_or_lt_of_le htle with heq | hlt
    · -- the scan opened every block
      have haccEq : accT = allF := by
        show (blocks.take t).flatMap Block.rows = blocks.flatMap Block.rows
        rw [heq, List.take_length]
      constructor
      · rw [haccEq]
        exact congrArg
          (fun o => fetch (fun (x y : Row) => lex spec x.key y.key == Ordering.lt) off k o)
          (ordered_perm hbndB hbnd hnB hpermF)
      · exact (filter_eq_take_of_prefix (fun b => k != 0 && eligible spec off k allR b)
          blocks t htle h1 (fun x hx => absurd hx (by rw [heq]; simp))).symm
    · -- the scan stopped at the block `bt`
      have hfail : ¬ (k != 0 && eligible spec off k accT (blocks.get ⟨t, hlt⟩)) := by
        rcases hstop with heq' | hs2
        · exact absurd heq' (Nat.ne_of_lt hlt)
        · exact hs2 hlt
      let bt := blocks.get ⟨t, hlt⟩
      have helA : eligible spec off k accT bt = false := by
        refine boolEqFalse (fun he => ?_)
        exact hfail ((Bool.and_eq_true _ _).mpr ⟨neZeroBool k hkt, he⟩)
      let o₁ := ordered spec accT
      have hp₁ : Pairwise (R spec) o₁ := ordered_pairwise accT hbndA
      have hn₁ : ((o₁.map Row.id).Nodup) :=
        ((List.mergeSort_perm accT (ord spec)).symm.map Row.id).nodup hnA
      have hbo₁ : ∀ r ∈ o₁, r.key.length = spec.length := by
        intro r hr
        exact hbndA r ((mem_ordered accT r).mp hr)
      let o₂ := ordered spec allR
      have hp₂ : Pairwise (R spec) o₂ := ordered_pairwise allR hbnd
      have hn₂ : ((o₂.map Row.id).Nodup) :=
        ((List.mergeSort_perm allR (ord spec)).symm.map Row.id).nodup hn
      have hbo₂ : ∀ r ∈ o₂, r.key.length = spec.length := by
        intro r hr
        exact hbnd r ((mem_ordered allR r).mp hr)
      have hsub_mem : ∀ r ∈ o₁, r ∈ o₂ := by
        intro r hr
        exact (mem_ordered allR r).mpr
          ((perm_mem hpermF r).mp (hmemAF r ((mem_ordered accT r).mp hr)))
      have hsplit : ∀ r ∈ allF, r ∈ accT ∨ r ∈ (blocks.drop t).flatMap Block.rows := by
        intro r hr
        obtain ⟨b, hb, hr'⟩ := List.mem_flatMap.mp hr
        have hb2 : b ∈ blocks.take t ++ blocks.drop t := by
          rw [List.take_append_drop]
          exact hb
        rcases List.mem_append.mp hb2 with ht | hd
        · exact Or.inl (List.mem_flatMap.mpr ⟨b, ht, hr'⟩)
        · exact Or.inr (List.mem_flatMap.mpr ⟨b, hd, hr'⟩)
      cases hcut1 : o₁[off + k - 1]? with
      | none =>
          exact (boolNe (eligible_none hcut1) helA).elim
      | some d =>
          have hlen1 : off + k - 1 < o₁.length := lt_length_of_get?_some _ _ hcut1
          have hdgm : o₁[off + k - 1]'hlen1 = d := by
            have h1' := getElem?_pos o₁ (off + k - 1) hlen1
            rw [hcut1] at h1'
            exact (Option.some.inj h1').symm
          have hd : d ∈ o₁ := hdgm ▸ getElem_mem o₁ (off + k - 1) hlen1
          have hbd : d.key.length = spec.length := hbo₁ d hd
          have hbl : bt.low.length = spec.length := hlowB bt (getElem_mem blocks t hlt)
          have hLwd : kLt spec d.key bt.low :=
            kLt_of_gt (eqOfNeBoolFalse (by rw [← eligible_some hcut1, helA]))
          have htailLT :
              ∀ r ∈ (blocks.drop t).flatMap Block.rows, kLt spec d.key r.key := by
            intro r hr
            have hbr : r.key.length = spec.length := by
              obtain ⟨b, hb, hr'⟩ := List.mem_flatMap.mp hr
              exact hbndB r (List.mem_flatMap.mpr ⟨b, List.mem_of_mem_drop hb, hr'⟩)
            have h1 : kLe spec bt.low r.key :=
              low_le_rows spec blocks t hpB hvalB hlowB hrowsB hlt r hr
            exact kLt_kLe_trans hbd hbl hbr hLwd h1
          have hdR : d ∈ o₂ :=
            (mem_ordered allR d).mpr ((perm_mem hpermF d).mp (hmemAF d ((mem_ordered accT d).mp hd)))
          obtain ⟨pre, post, ho₂, hnp⟩ := split_first_occ o₂ d hdR
          have hprelt : pre.length < o₂.length := by
            rw [ho₂, List.length_append, List.length_cons]
            omega
          have hd2q : o₂[pre.length]? = some d := by
            rw [ho₂]
            exact getElem?_at_length pre d post
          have hd2p : o₂[pre.length]'hprelt = d :=
            (Option.some.inj (hd2q.symm.trans (getElem?_pos o₂ pre.length hprelt))).symm
          have hmlt_pre : off + k - 1 ≤ pre.length :=
            index_le_index hlen1 hdgm hp₁ hp₂ hbo₁ hbo₂ hn₁ hn₂ hsub_mem pre.length hprelt hd2p
          have hlen2 : off + k - 1 < o₂.length := Nat.lt_of_le_of_lt hmlt_pre hprelt
          let e := o₂[off + k - 1]'hlen2
          have hcutE : o₂[off + k - 1]? = some e := getElem?_pos o₂ (off + k - 1) hlen2
          have hb1 : o₁.filter (fun y => !ord spec d y) = o₁.take (off + k - 1) :=
            filter_below_eq_take o₁ d (off + k - 1) hlen1 hdgm hp₁ hbo₁ hn₁
          have hb2 : o₂.filter (fun y => !ord spec d y) = o₂.take pre.length :=
            filter_below_eq_take o₂ d pre.length hprelt hd2p hp₂ hbo₂ hn₂
          have hsub_keep : ∀ y ∈ o₂.filter (fun y => !ord spec d y),
              y ∈ o₁.filter (fun y => !ord spec d y) := by
            intro y hy
            obtain ⟨hyo, hon⟩ := List.mem_filter.mp hy
            have hyR : y ∈ allR := (mem_ordered allR y).mp hyo
            have hyF : y ∈ allF := (perm_mem hpermF y).mpr hyR
            rcases hsplit y hyF with haccy | htaity
            · exact List.mem_filter.mpr ⟨(mem_ordered accT y).mpr haccy, hon⟩
            · exfalso
              have hky : kLt spec d.key y.key := htailLT y htaity
              have hR : R spec d y := by
                unfold R
                rw [hky]
                rfl
              have hord : ord spec d y = true := (ord_iff spec d y).mpr hR
              rw [hord] at hon
              exact absurd hon (by decide)
          have hcard2 : pre.length ≤ off + k - 1 := by
            have hc := card_le_of_mem_nodup (o₂.filter (fun y => !ord spec d y))
              (nodup_mapid_filter o₂ (fun y => !ord spec d y) hn₂)
              (o₁.filter (fun y => !ord spec d y)) hsub_keep
            rw [hb2, hb1] at hc
            rw [List.length_take, Nat.min_eq_left (Nat.le_of_lt hprelt)] at hc
            rw [List.length_take, Nat.min_eq_left (Nat.le_of_lt hlen1)] at hc
            exact hc
          have hr_eq : pre.length = off + k - 1 := by omega
          have hfin : (⟨off + k - 1, hlen2⟩ : Fin o₂.length) = ⟨pre.length, hprelt⟩ := by
            simp [hr_eq]
          have hde : e = d :=
            congrArg (fun i : Fin o₂.length => o₂[i]) hfin |>.trans hd2p
          have hLwd2 : kLt spec e.key bt.low := by
            rw [hde]
            exact hLwd
          have htailE :
              ∀ r ∈ (blocks.drop t).flatMap Block.rows, kLt spec e.key r.key := by
            intro r hr
            rw [hde]
            exact htailLT r hr
          have hmem12' : ∀ x,
              x ∈ o₁.filter (fun row => lex spec row.key d.key != Ordering.gt) ↔
                x ∈ o₂.filter (fun row => lex spec row.key d.key != Ordering.gt) := by
            intro x
            constructor
            · intro hx
              obtain ⟨h1x, hPx⟩ := List.mem_filter.mp hx
              exact List.mem_filter.mpr ⟨(mem_ordered allR x).mpr
                ((perm_mem hpermF x).mp (hmemAF x ((mem_ordered accT x).mp h1x))), hPx⟩
            · intro hx
              obtain ⟨h2x, hPx⟩ := List.mem_filter.mp hx
              have hxR : x ∈ allR := (mem_ordered allR x).mp h2x
              have hxF : x ∈ allF := (perm_mem hpermF x).mpr hxR
              rcases hsplit x hxF with haccx | htaix
              · exact List.mem_filter.mpr ⟨(mem_ordered accT x).mpr haccx, hPx⟩
              · exfalso
                have hky : kLt spec e.key x.key := htailE x htaix
                have hg : lex spec x.key e.key = Ordering.gt := gt_of_kLt hky
                rw [← hde] at hPx
                rw [hg] at hPx
                exact absurd hPx (by decide)
          have hP1 : ∀ x ∈ List.take off o₁,
              (fun row => lex spec row.key d.key != Ordering.gt) x = true := by
            intro x hx
            obtain ⟨i, hi, hxi⟩ := List.mem_iff_getElem.mp hx
            have hilen : i < o₁.length := by
              have := hi
              rw [List.length_take] at this
              exact Nat.lt_of_lt_of_le this (Nat.min_le_right off o₁.length)
            have hlti : i < off := by
              have := hi
              rw [List.length_take] at this
              exact Nat.lt_of_lt_of_le this (Nat.min_le_left off o₁.length)
            have hleoff : off ≤ off + k - 1 := by omega
            have hilm : i < off + k - 1 := Nat.lt_of_lt_of_le hlti hleoff
            have hxg : o₁[i] = x :=
              (List.getElem_take (xs := o₁) (j := off) (i := i)).symm.trans hxi
            have hkx : kLe spec x.key d.key := by
              rw [← hxg, ← hdgm]
              exact R_imp_kLe (R_get_of_lt hp₁ hilen hlen1 hilm)
            exact toBoolNe hkx
          have hP2 : ∀ x ∈ List.take off o₂,
              (fun row => lex spec row.key d.key != Ordering.gt) x = true := by
            intro x hx
            obtain ⟨i, hi, hxi⟩ := List.mem_iff_getElem.mp hx
            have hilen : i < o₂.length := by
              have := hi
              rw [List.length_take] at this
              exact Nat.lt_of_lt_of_le this (Nat.min_le_right off o₂.length)
            have hlti : i < off := by
              have := hi
              rw [List.length_take] at this
              exact Nat.lt_of_lt_of_le this (Nat.min_le_left off o₂.length)
            have hleoff : off ≤ off + k - 1 := by omega
            have hilm : i < off + k - 1 := Nat.lt_of_lt_of_le hlti hleoff
            have hxg : o₂[i] = x :=
              (List.getElem_take (xs := o₂) (j := off) (i := i)).symm.trans hxi
            have hke : kLe spec x.key e.key := by
              rw [← hxg]
              exact R_imp_kLe (R_get_of_lt hp₂ hilen hlen2 hilm)
            have hkx : kLe spec x.key d.key := by
              rw [← hde]
              exact hke
            exact toBoolNe hkx
          constructor
          · rw [page_eq_filter hkt accT d hbndA hnA hcut1,
              page_eq_filter hkt allR e hbnd hn hcutE, hde]
            rw [filter_drop_swap (fun row => lex spec row.key d.key != Ordering.gt)
                (ordered spec accT) off hP1,
              filter_drop_swap (fun row => lex spec row.key d.key != Ordering.gt)
                (ordered spec allR) off hP2]
            exact congrArg (fun L => List.drop off L)
              (sorted_filter_eq' (fun row => lex spec row.key d.key != Ordering.gt)
                o₁ o₂ hp₁ hp₂ hbo₁ hbo₂ hn₁ hn₂ hmem12')
          · refine (filter_eq_take_of_prefix (fun b => k != 0 && eligible spec off k allR b)
              blocks t htle h1 ?_).symm
            intro b hb
            obtain ⟨j, hj, hbj⟩ := List.mem_iff_getElem.mp hb
            have hpos : j + t < blocks.length := by
              have := hj
              rw [List.length_drop] at this
              omega
            have hb_eq : blocks[j + t]'hpos = b :=
              (getElem_drop_eq blocks t j (by omega)).symm.trans hbj
            have hLebt : kLe spec bt.low b.low := by
              rcases Nat.eq_zero_or_pos j with rfl | hjp
              · have hbt : blocks[0 + t]'hpos = bt := by
                  show List.get blocks ⟨0 + t, hpos⟩ = List.get blocks ⟨t, hlt⟩
                  exact congrArg (List.get blocks) (by simp)
                rw [← hb_eq, hbt]
                exact kLe_refl _ _
              · have hjlt : t < j + t := by omega
                have h2 := kLe_lows_get hpB hlt hpos hjlt
                rw [hb_eq] at h2
                exact h2
            have hlc : b.low.length = spec.length := hlowB b (List.mem_of_mem_drop hb)
            have hbe : e.key.length = spec.length := hbo₂ e (getElem_mem o₂ (off + k - 1) hlen2)
            have hLb : kLt spec e.key b.low := kLt_kLe_trans hbe hbl hlc hLwd2 hLebt
            have heB : eligible spec off k allR b = false := eligible_false_of_kLt hcutE hLb
            show (k != 0 && eligible spec off k allR b) = false
            rw [neZeroBool k hkt]
            exact heB
/-- Part G.1: the first conjunct of the soundness theorem. -/
private theorem claim_a
    (spec : Spec) (off k : Nat) (directory : List Block)
    (hvalid : ∀ b ∈ directory, valid spec b)
    (hn : ((directory.flatMap Block.rows).map Row.id).Nodup)
    (hbnd : ∀ r ∈ directory.flatMap Block.rows, r.key.length = spec.length) :
    let blocks := directory.mergeSort (fun x y => lex spec x.low y.low != Ordering.gt)
    page spec off k (scan spec off k [] blocks).1 =
      page spec off k (directory.flatMap Block.rows) :=
  (fence spec off k directory hvalid hn hbnd).1

private theorem claim_b
    (spec : Spec) (off k : Nat) (directory : List Block)
    (hvalid : ∀ b ∈ directory, valid spec b)
    (hn : ((directory.flatMap Block.rows).map Row.id).Nodup)
    (hbnd : ∀ r ∈ directory.flatMap Block.rows, r.key.length = spec.length) :
    let blocks := directory.mergeSort (fun x y => lex spec x.low y.low != Ordering.gt)
    (scan spec off k [] blocks).2 =
      blocks.filter (fun b => k != 0 && eligible spec off k (directory.flatMap Block.rows) b) :=
  (fence spec off k directory hvalid hn hbnd).2

/-- Public soundness theorem for page selection and opened-block tracing. -/
theorem sound : Goal := by
  intro spec off k directory hvalid hn hbnd
  exact ⟨claim_a spec off k directory hvalid hn hbnd,
    claim_b spec off k directory hvalid hn hbnd⟩
end TinylambOpt.ResearchProofs.DistributedPageFence
