import Std

namespace TinylambOpt.ResearchGoals.SubstrAsciiByteSliceEquiv

def isAsciiStr (bs : List UInt8) : Bool :=
  bs.all (fun b => decide (b.toNat < 128))

/-- Splits a byte stream at UTF-8 code-point start bytes (`(b & 0xC0) != 0x80`),
    matching `expression/bytecode.cpp` (`TryEvalSubstrView`). -/
def splitCodePoints (bs : List UInt8) : List (List UInt8) :=
  let rec finishCur (cur : List UInt8) (acc : List (List UInt8)) : List (List UInt8) :=
    if cur.isEmpty then acc else acc ++ [cur]
  let rec go (rest cur : List UInt8) (acc : List (List UInt8)) : List (List UInt8) :=
    match rest with
    | [] => finishCur cur acc
    | b :: bs' =>
        if (b &&& 0xC0) != 0x80 then
          go bs' [b] (finishCur cur acc)
        else
          go bs' (cur ++ [b]) acc
  go bs [] []

def refSubstr (bs : List UInt8) (start : Int) (optLen : Option Int) : Option (List UInt8) :=
  match optLen with
  | some l =>
      if l < 0 then none
      else if l == 0 then some []
      else
        let cps := splitCodePoints bs
        let n := Int.ofNat cps.length
        let raw :=
          if 0 < start then start - 1
          else if start < 0 then n + start
          else 0
        let actual := if raw < 0 then 0 else raw.toNat
        some (((cps.drop actual).take l.toNat).flatten)
  | none =>
      let cps := splitCodePoints bs
      let n := Int.ofNat cps.length
      let raw :=
        if 0 < start then start - 1
        else if start < 0 then n + start
        else 0
      let actual := if raw < 0 then 0 else raw.toNat
      some ((cps.drop actual).flatten)

def fastAsciiSubstr (bs : List UInt8) (start : Int) (optLen : Option Int) :
    Option (List UInt8) :=
  match optLen with
  | some l =>
      if l < 0 then none
      else if l == 0 then some []
      else
        let n := Int.ofNat bs.length
        let raw :=
          if 0 < start then start - 1
          else if start < 0 then n + start
          else 0
        let actual := if raw < 0 then 0 else raw.toNat
        some ((bs.drop actual).take l.toNat)
  | none =>
      let n := Int.ofNat bs.length
      let raw :=
        if 0 < start then start - 1
        else if start < 0 then n + start
        else 0
      let actual := if raw < 0 then 0 else raw.toNat
      some (bs.drop actual)

/-- Signed-start 1-based ASCII `SUBSTR` O(1) byte-slice equivalence
    (`expression/bytecode.cpp` `TryEvalSubstrView`,
    `executor/detail/columnar_tlcs_fast_path.cpp`): on any all-ASCII input,
    signed `start` (positive, `0`, negative, clamped) and optional `len`
    (negative -> error) match full UTF-8 code-point slicing byte-for-byte. -/
def Goal : Prop :=
  ∀ (bs : List UInt8) (start : Int) (optLen : Option Int),
    isAsciiStr bs = true →
    fastAsciiSubstr bs start optLen = refSubstr bs start optLen

private def asciiHi : List UInt8 := [0x68, 0x65, 0x6C, 0x6C, 0x6F]
private def utf8EAcute : List UInt8 := [0xC3, 0xA9, 0x61]

example :
    fastAsciiSubstr asciiHi 0 (some 2) = refSubstr asciiHi 0 (some 2) ∧
    fastAsciiSubstr asciiHi (-2) none = refSubstr asciiHi (-2) none ∧
    fastAsciiSubstr asciiHi (-99) (some 3) = refSubstr asciiHi (-99) (some 3) ∧
    fastAsciiSubstr asciiHi 10 (some (-1)) = none := by
  decide

example :
    fastAsciiSubstr utf8EAcute 1 (some 1) = some [0xC3] ∧
    refSubstr utf8EAcute 1 (some 1) = some [0xC3, 0xA9] := by
  decide

end TinylambOpt.ResearchGoals.SubstrAsciiByteSliceEquiv
