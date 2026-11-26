import Std

/-!
Reference hash join with full residual predicate verification (`collEq c`).

Optimized join with residual predicate elided into exact hash-table byte equality
(skipping bytecode/collation residual dispatch).

Modeled claims

- `Goal` — Binary collation residual-predicate elision for string equi-joins (STATS-CEB
  C4 / DictionaryJoinsAndLateLabels B2): under `Collation.binary`, eliding residual
  string equality into exact byte-equality inside the hash join matches `refJoin` for
  all hash functions (including total hash collisions) on both INNER and LEFT JOIN,
  whereas `asciiCaseInsensitive` and `padSpace` collations must keep residual checks.
-/

namespace TinylambOpt.ResearchGoals.BinaryCollationStringEquiJoinElision

inductive Collation where
  | binary
  | asciiCaseInsensitive
  | padSpace
  deriving DecidableEq

def asciiLower (bytes : List UInt8) : List UInt8 :=
  bytes.map fun b =>
    if 65 ≤ b && b ≤ 90 then UInt8.ofNat (b.toNat + 32) else b

def trimTrailingSpaces (bytes : List UInt8) : List UInt8 :=
  (bytes.reverse.dropWhile (· == (32 : UInt8))).reverse

def collEq (c : Collation) (a b : Option (List UInt8)) : Bool :=
  match a, b with
  | some xs, some ys =>
      match c with
      | .binary => xs == ys
      | .asciiCaseInsensitive => asciiLower xs == asciiLower ys
      | .padSpace => trimTrailingSpaces xs == trimTrailingSpaces ys
  | _, _ => false

structure LRow where
  id : Nat
  k : Option (List UInt8)
  deriving DecidableEq

structure RRow where
  id : Nat
  k : Option (List UInt8)
  deriving DecidableEq

/-- Reference hash join with full residual predicate verification (`collEq c`). -/
def refJoin (c : Collation) (isLeft : Bool) (h : List UInt8 → Nat)
    (ls : List LRow) (rs : List RRow) : List (Nat × Option Nat) :=
  ls.flatMap fun l =>
    match l.k with
    | none => if isLeft then [(l.id, none)] else []
    | some lk =>
        let bucket :=
          rs.filter fun r =>
            match r.k with
            | some rk => h lk == h rk
            | none => false
        let hits := bucket.filter (fun r => collEq c l.k r.k)
        if hits.isEmpty then
          if isLeft then [(l.id, none)] else []
        else
          hits.map (fun r => (l.id, some r.id))

/-- Optimized join with residual predicate elided into exact hash-table byte
    equality (skipping bytecode/collation residual dispatch). -/
def optElidedJoin (isLeft : Bool) (h : List UInt8 → Nat)
    (ls : List LRow) (rs : List RRow) : List (Nat × Option Nat) :=
  ls.flatMap fun l =>
    match l.k with
    | none => if isLeft then [(l.id, none)] else []
    | some lk =>
        let hits :=
          rs.filter fun r =>
            match r.k with
            | some rk => h lk == h rk && lk == rk
            | none => false
        if hits.isEmpty then
          if isLeft then [(l.id, none)] else []
        else
          hits.map (fun r => (l.id, some r.id))

private def ciUpper : List UInt8 := [83, 81, 76] -- "SQL"
private def ciLower : List UInt8 := [115, 113, 108] -- "sql"
private def padA : List UInt8 := [97] -- "a"
private def padASpace : List UInt8 := [97, 32] -- "a "

/-- Binary collation residual-predicate elision for string equi-joins (STATS-CEB
    C4 / DictionaryJoinsAndLateLabels B2): under `Collation.binary`, eliding residual string equality
    into exact byte-equality inside the hash join matches `refJoin` for all hash
    functions (including total hash collisions) on both INNER and LEFT JOIN,
    whereas `asciiCaseInsensitive` and `padSpace` collations must keep residual checks. -/
def Goal : Prop :=
  (∀ (isLeft : Bool) (h : List UInt8 → Nat) (ls : List LRow) (rs : List RRow),
    optElidedJoin isLeft h ls rs = refJoin .binary isLeft h ls rs) ∧
  (∃ (isLeft : Bool) (h : List UInt8 → Nat) (ls : List LRow) (rs : List RRow),
    optElidedJoin isLeft h ls rs ≠ refJoin .asciiCaseInsensitive isLeft h ls rs) ∧
  (∃ (isLeft : Bool) (h : List UInt8 → Nat) (ls : List LRow) (rs : List RRow),
    optElidedJoin isLeft h ls rs ≠ refJoin .padSpace isLeft h ls rs)

example :
    let hColliding : List UInt8 → Nat := fun _ => 0
    let ls : List LRow :=
      [ { id := 1, k := none }
      , { id := 2, k := some [97, 98] }
      , { id := 3, k := some [99, 100] }
      ]
    let rs : List RRow :=
      [ { id := 10, k := some [97, 98] }
      , { id := 11, k := some [97, 98] }
      , { id := 12, k := none }
      ]
    optElidedJoin true hColliding ls rs = refJoin .binary true hColliding ls rs ∧
    optElidedJoin false hColliding ls rs = refJoin .binary false hColliding ls rs := by
  decide

example :
    let hConst : List UInt8 → Nat := fun _ => 1
    optElidedJoin false hConst [{ id := 1, k := some ciUpper }] [{ id := 2, k := some ciLower }] ≠
      refJoin .asciiCaseInsensitive false hConst [{ id := 1, k := some ciUpper }] [{ id := 2, k := some ciLower }] ∧
    optElidedJoin true hConst [{ id := 1, k := some padA }] [{ id := 2, k := some padASpace }] ≠
      refJoin .padSpace true hConst [{ id := 1, k := some padA }] [{ id := 2, k := some padASpace }] := by
  decide

end TinylambOpt.ResearchGoals.BinaryCollationStringEquiJoinElision
