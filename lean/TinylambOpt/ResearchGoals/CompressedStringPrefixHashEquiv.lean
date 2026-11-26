import Std

/-!
Packs up to 7 bytes into an unsigned 56-bit big-endian integer padded on the right with
zero bytes (used to produce both `beWord7` and length-tagged `pack7`).

Inline 64-bit packed prefix+length representation for short strings `|s| ≤ 7`: upper 56
bits hold big-endian bytes zero-padded on the right, low 8 bits hold `s.length`, making
equality, hashing, and byte-lexicographic ordering a single integer operation.

Modeled claims

- `Goal` — Short-string inline packed prefix+length equivalence
  (DictionaryJoinsAndLateLabels B1, B2 / STATS-CEB C4 / JOB J4 / LDBC L1, L5): for
  every byte sequence with `s.length ≤ 7`, `pack7` is both injective (`pack7 s1 =
  pack7 s2 ↔ s1 = s2`) and strictly order-preserving for byte-lexicographic order
  (`pack7 s1 < pack7 s2 ↔ byteLexLt s1 s2`), whereas omitting the length byte
  (`beWord7`) collides `[]` and `[0]`.
-/

namespace TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv

/-- Packs up to 7 bytes into an unsigned 56-bit big-endian integer padded on the
    right with zero bytes (used to produce both `beWord7` and length-tagged
    `pack7`). -/
def beWord7Go : Nat → List UInt8 → Nat → Nat
  | 0, _, acc => acc
  | rem + 1, bytes, acc =>
      let b := match bytes with | x :: _ => x.toNat | [] => 0
      let rest := match bytes with | _ :: xs => xs | [] => []
      beWord7Go rem rest (acc * 256 + b)

def beWord7 (s : List UInt8) : Nat :=
  beWord7Go 7 s 0

/-- Inline 64-bit packed prefix+length representation for short strings `|s| ≤ 7`:
    upper 56 bits hold big-endian bytes zero-padded on the right, low 8 bits hold
    `s.length`, making equality, hashing, and byte-lexicographic ordering a
    single integer operation. -/
def pack7 (s : List UInt8) : Nat :=
  beWord7 s * 256 + s.length

def byteLexLt : List UInt8 → List UInt8 → Bool
  | [], [] => false
  | [], _ :: _ => true
  | _ :: _, [] => false
  | a :: as, b :: bs =>
      if a < b then true
      else if b < a then false
      else byteLexLt as bs

/-- Short-string inline packed prefix+length equivalence (DictionaryJoinsAndLateLabels B1, B2 /
    STATS-CEB C4 / JOB J4 / LDBC L1, L5): for every byte sequence with
    `s.length ≤ 7`, `pack7` is both injective (`pack7 s1 = pack7 s2 ↔ s1 = s2`)
    and strictly order-preserving for byte-lexicographic order (`pack7 s1 <
    pack7 s2 ↔ byteLexLt s1 s2`), whereas omitting the length byte (`beWord7`)
    collides `[]` and `[0]`. -/
def Goal : Prop :=
  (∀ (s1 s2 : List UInt8),
    s1.length ≤ 7 →
    s2.length ≤ 7 →
    (pack7 s1 = pack7 s2 ↔ s1 = s2) ∧
    (pack7 s1 < pack7 s2 ↔ byteLexLt s1 s2 = true)) ∧
  (beWord7 [] = beWord7 [0] ∧ pack7 [] ≠ pack7 [0])

private def sEmpty : List UInt8 := []
private def sZero : List UInt8 := [0]
private def sAZero : List UInt8 := [97, 0]
private def sA : List UInt8 := [97]
private def sAZeroB : List UInt8 := [97, 0, 98]
private def sSeven : List UInt8 := [97, 98, 99, 100, 101, 102, 255]

example :
    beWord7 sEmpty = beWord7 sZero ∧
    pack7 sEmpty < pack7 sZero ∧
    pack7 sA < pack7 sAZero ∧
    pack7 sAZero < pack7 sAZeroB ∧
    (byteLexLt sA sAZero = true) ∧
    (byteLexLt sAZero sAZeroB = true) := by
  decide

example :
    pack7 sSeven ≠ pack7 [97, 98, 99, 100, 101, 102, 254] ∧
    byteLexLt [97, 98, 99, 100, 101, 102, 254] sSeven = true ∧
    pack7 [97, 98, 99, 100, 101, 102, 254] < pack7 sSeven := by
  decide

end TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv
