import Std.Tactic

namespace TinylambOpt.FirstErrorBallot

/-
# First failing (row, op) from several error masks

An expression evaluates `m` operations on each of `n` rows of a batch.  The
**reference** walks rows in order and, inside a row, operations in order; it
returns the first error under the short-circuit liveness mask.  The **ballot**
lowering computes one error mask per operation, ORs them into a row mask,
finds the first set row (LLVM `cttz`, guarded by a non-zero check), then finds
the first failing op of that row and adds the batch row offset.

Compared observation: `Option Found` = row index, op index, and error kind of
the first failure (or `none` if no live failure).

Hypotheses:

* the ballot mask is exactly `live && err.isSome` for every `(row, op)`, i.e.
  speculative errors on dead lanes are masked off before the ballot;
* lexicographic order is `(row, op)` — comparing op numbers first returns the
  wrong error kind (`op_first_is_wrong`);
* the reference order is row-major/op-minor (`seqFirst`), matching the C++
  scalar evaluator.

## C++ realization

`expression/jit.cpp`: build per-op error bitmasks over the batch, `or` them,
guard `mask != 0` before `cttz`, add `rowBase` to the extracted lane index,
and use the same lane index to select the error kind from the per-op masks.
-/

inductive ErrKind where
  | divZero | overflow
  deriving DecidableEq, Repr

structure Found where
  row : Nat
  op : Nat
  kind : ErrKind
  deriving DecidableEq, Repr

/-- First failing op of one row, in op order. -/
def rowFirst (f : Nat → Option α) (m : Nat) : Option (Nat × α) :=
  (List.range m).findSome? (fun j => (f j).map (fun e => (j, e)))

/-- `find?` + first value: the ballot's nonzero-guard step. -/
def firstOf {α β : Type} (xs : List α) (guard : α → Bool) (f : α → Option β) : Option β :=
  match xs.find? guard with
  | none => none
  | some a => f a

theorem findSome_eq_firstOf {α β : Type} (xs : List α) (f : α → Option β) :
    xs.findSome? f = firstOf xs (fun a => (f a).isSome) f := by
  induction xs with
  | nil => rfl
  | cons a xs ih =>
    by_cases h : (f a).isSome
    · rcases Option.isSome_iff_exists.mp h with ⟨b, hb⟩
      simp [List.findSome?, List.find?, firstOf, hb]
    · have hnone : f a = none := Option.not_isSome_iff_eq_none.mp h
      simp [List.findSome?, List.find?, firstOf, hnone, ih]

theorem findSome_eq_firstOf_guard {α β : Type} (xs : List α) (f : α → Option β)
    (guard : α → Bool) (h : ∀ a, (f a).isSome = guard a) :
    xs.findSome? f = firstOf xs guard f := by
  rw [findSome_eq_firstOf]
  have hg : (fun a => (f a).isSome) = guard := by
    funext a
    exact h a
  rw [hg]

theorem findSome_congr {α β : Type} (l : List α) (f g : α → Option β)
    (h : ∀ a ∈ l, f a = g a) : l.findSome? f = l.findSome? g := by
  induction l with
  | nil => rfl
  | cons a as ih =>
    have ha := h a (by simp)
    have htail : ∀ x ∈ as, f x = g x := by
      intro x hx
      exact h x (by simp [hx])
    simp [List.findSome?, ha, ih htail]

/-- Row mask: is there any live error in this row? (the `cttz` nonzero guard) -/
def ballotGuard (rowBase m : Nat) (mask : Nat → Nat → Bool)
    (err : Nat → Nat → Option ErrKind) (i : Nat) : Bool :=
  (rowFirst (fun j => if mask i j then err (rowBase + i) j else none) m).isSome

/-- First failing op in that row plus the batch row offset. -/
def ballotFound (rowBase m : Nat) (mask : Nat → Nat → Bool)
    (err : Nat → Nat → Option ErrKind) (i : Nat) : Option Found :=
  (rowFirst (fun j => if mask i j then err (rowBase + i) j else none) m).map
    (fun je => ⟨rowBase + i, je.1, je.2⟩)

/-- Reference: sequential row-major / op-minor evaluation with liveness. -/
def seqFirst (rowBase n m : Nat) (live : Nat → Nat → Bool)
    (err : Nat → Nat → Option ErrKind) : Option Found :=
  (List.range n).findSome? (fun i =>
    (rowFirst (fun j => if live (rowBase + i) j then err (rowBase + i) j else none) m).map
      (fun je => ⟨rowBase + i, je.1, je.2⟩))

/-- Optimized: first live row, then first failing op, then add `rowBase`. -/
def ballot (rowBase n m : Nat) (mask : Nat → Nat → Bool)
    (err : Nat → Nat → Option ErrKind) : Option Found :=
  firstOf (List.range n) (ballotGuard rowBase m mask err) (ballotFound rowBase m mask err)

/-- **Soundness**: with `mask = live && err.isSome` on every lane, the ballot
    returns exactly the reference's first error kind (and its row/op). -/
theorem first_error_ballot_sound (rowBase n m : Nat) (live : Nat → Nat → Bool)
    (err : Nat → Nat → Option ErrKind) (mask : Nat → Nat → Bool)
    (hmask : ∀ i j, mask i j = (live (rowBase + i) j && (err (rowBase + i) j).isSome)) :
    seqFirst rowBase n m live err = ballot rowBase n m mask err := by
  have hb := findSome_eq_firstOf_guard (List.range n) (ballotFound rowBase m mask err)
    (ballotGuard rowBase m mask err) (by
      intro i
      simp [ballotFound, ballotGuard])
  unfold ballot
  calc
    seqFirst rowBase n m live err = (List.range n).findSome? (ballotFound rowBase m mask err) := by
      unfold seqFirst ballotFound
      apply findSome_congr
      intro i hi
      congr 1
      congr 1
      funext j
      rw [hmask]
      by_cases hl : live (rowBase + i) j
      · by_cases he : (err (rowBase + i) j).isSome
        · simp [hl, he]
        · have hnone : err (rowBase + i) j = none := Option.not_isSome_iff_eq_none.mp he
          simp [hl, hnone]
      · simp [hl]
    _ = firstOf (List.range n) (ballotGuard rowBase m mask err)
          (ballotFound rowBase m mask err) := hb

/-- Trap from the issue: row 0 fails at op 1 (`overflow`), row 1 fails at
    op 0 (`divZero`). Comparing op numbers first returns the wrong kind. -/
def opFirst (n m : Nat) (err : Nat → Nat → Option ErrKind) : Option Found :=
  (List.range m).findSome? (fun j =>
    (List.range n).findSome? (fun i =>
      (err i j).map (fun e => ⟨i, j, e⟩)))

theorem op_first_is_wrong :
    opFirst 2 2 (fun i j => if i = 0 && j = 1 then some ErrKind.overflow
                  else if i = 1 && j = 0 then some ErrKind.divZero else none) ≠
    seqFirst 0 2 2 (fun _ _ => true)
      (fun i j => if i = 0 && j = 1 then some ErrKind.overflow
                  else if i = 1 && j = 0 then some ErrKind.divZero else none) := by
  decide

end TinylambOpt.FirstErrorBallot
