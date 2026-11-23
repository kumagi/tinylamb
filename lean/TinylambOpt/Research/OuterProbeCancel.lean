import Std.Tactic

/-!
# Exactly-once NULL padding for a parallel outer probe (`outer-probe-cancel`)

The right input is split into morsels.  For each left occurrence the ON
predicate is three-valued (`tTrue`, `tFalse`, `tNull`): only `tTrue` produces a
match, `tNull` is UNKNOWN and must **not** trigger NULL padding while a later
morsel may still return `tTrue`.  A per-left-row state records the matched
right occurrence ids, the number of completed morsels (watermark), a `seenTrue`
flag and a `cancelled` flag.  Padding is emitted only when all morsels are
complete and no `tTrue` was ever seen, i.e. exactly once.

The reference computes the NULL-extended outer join over the whole right
input.  The optimized computation folds the morsels in any arrival order and
finalizes afterwards; both give the same bag, and an early padding decision
based only on UNKNOWN is shown unsound (it produces an unnecessary pad).

C++ counterpart: `executor/detail/pipeline/ooc_hash_join_sink.cpp`.
-/

namespace TinylambOpt.Research.OuterProbeCancel

/-- SQL three-valued logic. -/
inductive Tri where
  | tTrue | tFalse | tNull
  deriving DecidableEq, BEq, Repr

structure LRow where
  id : Nat
  val : Nat
  deriving DecidableEq, Repr

structure RRow where
  id : Nat
  val : Nat
  deriving DecidableEq, Repr

abbrev Out := Nat × Option Nat

/-- TRUE matches of a left row inside one morsel. -/
def hits (on : Nat → Nat → Tri) (l : LRow) (m : List RRow) : List Nat :=
  m.filterMap (fun r => if on l.val r.val == Tri.tTrue then some r.id else none)

/-- A matched right occurrence in the output bag. -/
def pairs (on : Nat → Nat → Tri) (l : LRow) (r : RRow) : Option Out :=
  if on l.val r.val == Tri.tTrue then some (l.id, some r.id) else none

/-! ## Reference: full right input, non-parallel -/

def refLeft (on : Nat → Nat → Tri) (l : LRow) (rs : List RRow) : List Out :=
  let out := rs.filterMap (pairs on l)
  if out.isEmpty then [(l.id, none)] else out

def refOuter (on : Nat → Nat → Tri) (ls : List LRow) (rs : List RRow) : List Out :=
  ls.flatMap (fun l => refLeft on l rs)

/-! ## Stateful parallel fold -/

structure LState where
  matched : List Nat
  seenTrue : Bool
  seenNull : Bool
  done : Nat
  cancelled : Bool
  deriving DecidableEq, Repr

def initL : LState := ⟨[], false, false, 0, false⟩

/-- Absorb one morsel.  UNKNOWN records `seenNull` but never finalizes. -/
def absorb (on : Nat → Nat → Tri) (l : LRow) (st : LState) (m : List RRow) : LState :=
  let hs := hits on l m
  { matched := st.matched ++ hs
    seenTrue := st.seenTrue || !hs.isEmpty
    seenNull := st.seenNull || m.any (fun r => on l.val r.val == Tri.tNull)
    done := st.done + 1
    cancelled := st.cancelled }

/-- Finalize only after every morsel completed; pad iff nothing matched. -/
def finalize (l : LRow) (st : LState) (total : Nat) : List Out :=
  if st.done < total then []
  else if st.matched.isEmpty then [(l.id, none)]
  else st.matched.map (fun rid => (l.id, some rid))

def parOuter (on : Nat → Nat → Tri) (ls : List LRow) (ms : List (List RRow)) : List Out :=
  ls.flatMap (fun l => finalize l (ms.foldl (absorb on l) initL) ms.length)

/-! ## Folding facts -/

theorem matched_eq_aux (on : Nat → Nat → Tri) (l : LRow) (st : LState) :
    ∀ ms : List (List RRow),
      (ms.foldl (absorb on l) st).matched = st.matched ++ ms.flatMap (hits on l)
  | [] => by simp
  | m :: ms => by
      change (ms.foldl (absorb on l) (absorb on l st m)).matched =
        st.matched ++ (m :: ms).flatMap (hits on l)
      rw [matched_eq_aux on l (absorb on l st m) ms]
      simp [absorb, hits, List.append_assoc]

theorem done_eq_aux (on : Nat → Nat → Tri) (l : LRow) (st : LState) :
    ∀ ms : List (List RRow), (ms.foldl (absorb on l) st).done = st.done + ms.length
  | [] => by simp
  | m :: ms => by
      change (ms.foldl (absorb on l) (absorb on l st m)).done =
        st.done + (m :: ms).length
      rw [done_eq_aux on l (absorb on l st m) ms]
      simp [absorb, Nat.add_comm]
      omega

theorem matched_eq (on : Nat → Nat → Tri) (l : LRow) (ms : List (List RRow)) :
    (ms.foldl (absorb on l) initL).matched = ms.flatMap (hits on l) := by
  simpa [initL] using matched_eq_aux on l initL ms

theorem done_eq (on : Nat → Nat → Tri) (l : LRow) (ms : List (List RRow)) :
    (ms.foldl (absorb on l) initL).done = ms.length := by
  simpa [initL] using done_eq_aux on l initL ms

private theorem filterMap_append {α β : Type} (f : α → Option β) :
    ∀ (a b : List α), (a ++ b).filterMap f = a.filterMap f ++ b.filterMap f
  | [], b => rfl
  | x :: xs, b => by
      cases h : f x with
      | none => simp [h]
      | some y => simp [h]

private theorem isEmpty_append_eq {α : Type} (a b : List α) :
    (a ++ b).isEmpty = (a.isEmpty && b.isEmpty) := by
  induction a with
  | nil => simp
  | cons x xs ih => simp

private theorem per_morsel (on : Nat → Nat → Tri) (l : LRow) (m : List RRow) :
    m.filterMap (pairs on l) = (hits on l m).map (fun rid => (l.id, some rid)) := by
  induction m with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : on l.val r.val == Tri.tTrue
      · simp [pairs, hits, h, ih]
      · simp [pairs, hits, h, ih]

private theorem filterMap_flatten_eq_hits (on : Nat → Nat → Tri) (l : LRow)
    (ms : List (List RRow)) :
    ms.flatten.filterMap (pairs on l) =
      (ms.flatMap (hits on l)).map (fun rid => (l.id, some rid)) := by
  induction ms with
  | nil => rfl
  | cons m ms ih =>
      change (m ++ ms.flatten).filterMap (pairs on l) =
        (hits on l m ++ ms.flatMap (hits on l)).map (fun rid => (l.id, some rid))
      rw [filterMap_append, per_morsel, ih, List.map_append]

private theorem isEmpty_map {α β : Type} (f : α → β) (l : List α) :
    (l.map f).isEmpty = l.isEmpty := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp

private theorem refLeft_eq_flat (on : Nat → Nat → Tri) (l : LRow)
    (ms : List (List RRow)) :
    refLeft on l ms.flatten =
      if (ms.flatMap (hits on l)).isEmpty then [(l.id, none)]
      else (ms.flatMap (hits on l)).map (fun rid => (l.id, some rid)) := by
  unfold refLeft
  rw [filterMap_flatten_eq_hits]
  dsimp
  rw [isEmpty_map]

private theorem finalize_eq_refLeft (on : Nat → Nat → Tri) (l : LRow)
    (ms : List (List RRow)) :
    finalize l (ms.foldl (absorb on l) initL) ms.length = refLeft on l ms.flatten := by
  have hm := matched_eq on l ms
  have hd := done_eq on l ms
  have hr := refLeft_eq_flat on l ms
  unfold finalize
  rw [hm, hd]
  rw [hr]
  simp

/-! ## Main theorem -/

theorem par_eq_ref (on : Nat → Nat → Tri) (ls : List LRow) (ms : List (List RRow)) :
    parOuter on ls ms = refOuter on ls ms.flatten := by
  simp [parOuter, refOuter, finalize_eq_refLeft]

def uniqueIds (ls : List LRow) : Prop := ls.Pairwise (fun a b => a.id ≠ b.id)

/-- Exactly one pad after all morsels complete (never before). -/
def padCount (out : List Out) (l : LRow) : Nat :=
  (out.filter (fun x => x.1 == l.id && x.2.isNone)).length

private theorem padCount_map (l : LRow) (rs : List Nat) :
    padCount (rs.map (fun rid => (l.id, some rid))) l = 0 := by
  induction rs with
  | nil => simp [padCount]
  | cons r rs ih => simp [padCount]

private theorem padCount_refLeft (on : Nat → Nat → Tri) (l : LRow) (ms : List (List RRow)) :
    padCount (refLeft on l ms.flatten) l =
      if (ms.flatMap (hits on l)).isEmpty then 1 else 0 := by
  have hr := refLeft_eq_flat on l ms
  rw [hr]
  by_cases h : (ms.flatMap (hits on l)).isEmpty = true
  · simp only [ite_eq_left h]
    simp [padCount]
  · simp only [ite_eq_right h]
    exact padCount_map l (ms.flatMap (hits on l))

theorem pad_exactly_once (on : Nat → Nat → Tri) (l : LRow) (ms : List (List RRow)) :
    padCount (parOuter on [l] ms) l =
      if (ms.flatMap (hits on l)).isEmpty then 1 else 0 := by
  rw [par_eq_ref]
  simp only [refOuter, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  exact padCount_refLeft on l ms

/-- Permutation of a list preserves the flattened concatenation. -/
private theorem perm_flatMap_of_list_perm {α β : Type} (f : α → List β) :
    ∀ {l₁ l₂ : List α}, l₁.Perm l₂ → (l₁.flatMap f).Perm (l₂.flatMap f) := by
  intro l₁ l₂ hp
  induction hp with
  | nil => exact List.Perm.refl _
  | cons x h ih => exact List.Perm.append_left (f x) ih
  | swap x y l =>
      have hc : (f x ++ f y ++ l.flatMap f).Perm (f y ++ f x ++ l.flatMap f) := by
        simpa [List.append_assoc] using
          (List.Perm.append_right (l.flatMap f)
            (List.perm_append_comm : (f x ++ f y).Perm (f y ++ f x)))
      first | simpa using hc | simpa using hc.symm
  | trans h1 h2 ih1 ih2 => exact ih1.trans ih2

private theorem perm_flatMap_of_perm {α β : Type} (f g : α → List β) :
    ∀ l : List α, (∀ a ∈ l, (f a).Perm (g a)) → (l.flatMap f).Perm (l.flatMap g)
  | [], _ => by simp
  | a :: l, h => by
      simp only [List.flatMap_cons]
      exact (List.Perm.append_right _ (h a (by simp))).trans
        (List.Perm.append_left (g a) (perm_flatMap_of_perm f g l (fun x hx => h x (by simp [hx]))))

private theorem isEmpty_of_length_zero {α : Type} (l : List α) (h : l.length = 0) :
    l.isEmpty = true := by
  cases l with
  | nil => rfl
  | cons x xs => simp at h

private theorem length_zero_of_isEmpty {α : Type} (l : List α) (h : l.isEmpty = true) :
    l.length = 0 := by
  cases l with
  | nil => rfl
  | cons x xs => simp at h

/-- Any arrival order (including empty morsels and equal-valued left rows)
    gives the same output bag. -/
theorem arrival_order_invariance (on : Nat → Nat → Tri) (ls : List LRow)
    (ms ms' : List (List RRow)) (hp : ms.Perm ms') :
    (parOuter on ls ms).Perm (parOuter on ls ms') := by
  rw [par_eq_ref, par_eq_ref]
  apply perm_flatMap_of_perm
  intro l _
  have hperm : (ms.flatMap (hits on l)).Perm (ms'.flatMap (hits on l)) :=
    perm_flatMap_of_list_perm (hits on l) hp
  have hlen := hperm.length_eq
  rw [refLeft_eq_flat on l ms, refLeft_eq_flat on l ms']
  cases hf : ms.flatMap (hits on l) with
  | nil =>
      have h0 : (ms.flatMap (hits on l)).length = 0 := by simp [hf]
      have hf'len : (ms'.flatMap (hits on l)).length = 0 := by omega
      have hf'nil : ms'.flatMap (hits on l) = [] := List.length_eq_zero_iff.mp hf'len
      simp [hf, hf'nil]
  | cons x xs =>
      have hf'nil : ¬ms'.flatMap (hits on l) = [] := by
        intro hnil
        rw [hnil] at hlen
        simp [hf] at hlen
      have hp' := List.Perm.map (fun rid => (l.id, some rid)) hperm
      simpa [hf, hf'nil] using hp'

theorem outer_probe_cancel_sound
    (on : Nat → Nat → Tri) (ls : List LRow) (ms : List (List RRow))
    (_hid : uniqueIds ls) :
    let rs := ms.flatten
    parOuter on ls ms = refOuter on ls rs ∧
    (∀ l ∈ ls, padCount (parOuter on [l] ms) l =
      if (ms.flatMap (hits on l)).isEmpty then 1 else 0) := by
  dsimp
  exact ⟨par_eq_ref on ls ms, fun l _ => pad_exactly_once on l ms⟩

/-! ## Regression: UNKNOWN pad followed by a later TRUE -/

/-- ON used in the regression: a probe value equal to the left value is TRUE,
    a probe value 9 is UNKNOWN, everything else is FALSE. -/
def onEq (l r : Nat) : Tri :=
  if l == r then Tri.tTrue else if r == 9 then Tri.tNull else Tri.tFalse

/-- Naive early padding: as soon as one morsel has only UNKNOWN it emits a
    pad, even though a later morsel can still return TRUE. -/
def naiveEarly (on : Nat → Nat → Tri) (l : LRow) (ms : List (List RRow)) : List (Option Nat) :=
  ms.flatMap (fun m =>
    if (hits on l m).isEmpty then
      if m.any (fun r => on l.val r.val == Tri.tNull) then [none] else []
    else (hits on l m).map some)

def naivePads (on : Nat → Nat → Tri) (l : LRow) (ms : List (List RRow)) : Nat :=
  (ms.filterMap (fun m =>
    if (hits on l m).isEmpty && m.any (fun r => on l.val r.val == Tri.tNull)
    then some () else none)).length

/-- Padding rows produced in excess of the exactly-once requirement. -/
def unnecessaryPads (on : Nat → Nat → Tri) (l : LRow) (ms : List (List RRow)) : Nat :=
  naivePads on l ms - (if (ms.flatMap (hits on l)).isEmpty then 1 else 0)

/-- First worker sees only UNKNOWN and pads; last worker returns TRUE.  The
    naive stream has one spurious pad, the correct stream has none. -/
theorem early_unknown_then_true_counterexample :
    let l : LRow := ⟨1, 5⟩
    let m1 : List RRow := [⟨1, 9⟩]
    let m2 : List RRow := [⟨2, 5⟩]
    naiveEarly onEq l [m1, m2] = [none, some 2] ∧
    refOuter onEq [l] (m1 ++ m2) = [(1, some 2)] ∧
    parOuter onEq [l] [m1, m2] = [(1, some 2)] ∧
    naivePads onEq l [m1, m2] = 1 ∧
    unnecessaryPads onEq l [m1, m2] = 1 := by
  dsimp
  decide

end TinylambOpt.Research.OuterProbeCancel
