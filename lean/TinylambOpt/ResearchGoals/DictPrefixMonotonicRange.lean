import Std

namespace TinylambOpt.ResearchGoals.DictPrefixMonotonicRange

def lexLt : List UInt8 → List UInt8 → Bool
  | [], [] => false
  | [], _ :: _ => true
  | _ :: _, [] => false
  | a :: as, b :: bs =>
      if a < b then true else if a == b then lexLt as bs else false

def StrictlySorted (dict : List (List UInt8)) : Prop :=
  dict.Pairwise (fun a b => lexLt a b = true)

def startsWith (pref s : List UInt8) : Bool :=
  s.length ≥ pref.length && s.take pref.length == pref

def revNext : List UInt8 → Option (List UInt8)
  | [] => none
  | b :: rest =>
      if b == 255 then revNext rest
      else some ((b + 1) :: rest)

def nextPrefix (pref : List UInt8) : Option (List UInt8) :=
  (revNext pref.reverse).map List.reverse

def bsLower (dict : List (List UInt8)) (key : List UInt8) : Nat :=
  let rec go (l r fuel : Nat) : Nat :=
    match fuel with
    | 0 => l
    | f + 1 =>
        if r ≤ l then l
        else
          let mid := (l + r) / 2
          match dict[mid]? with
          | some w => if lexLt w key then go (mid + 1) r f else go l mid f
          | none => l
  go 0 dict.length (dict.length + 1)

def prefixRange (dict : List (List UInt8)) (pref : List UInt8) : Nat × Nat :=
  let cLo := bsLower dict pref
  let cHi :=
    match nextPrefix pref with
    | none => dict.length
    | some hi => bsLower dict hi
  (cLo, cHi)

/-- Monotone prefix interval on sorted dictionary (`columnar_tlcs_fast_path.cpp`,
    `pax_chunk_codecs.cpp`): binary-searching `pref` and carry-aware
    `nextPrefix pref` produces the exact contiguous dictionary ID interval
    `[cLo, cHi)` matching `LIKE 'prefix%'` (handling trailing `0xFF` bytes). -/
def Goal : Prop :=
  ∀ (dict : List (List UInt8)) (pref : List UInt8),
    StrictlySorted dict →
    let (cLo, cHi) := prefixRange dict pref
    cLo ≤ cHi ∧ cHi ≤ dict.length ∧
    ∀ (i : Nat) (w : List UInt8),
      dict[i]? = some w → startsWith pref w = decide (cLo ≤ i ∧ i < cHi)

/-- Naive last-byte increment `b + 1` wraps `0xFF` to `0x00`, yielding an
    upper bound strictly smaller than `pref`. -/
def badLastByteInc (pref : List UInt8) : Option (List UInt8) :=
  match pref.reverse with
  | [] => none
  | b :: rest => some ((b + 1) :: rest).reverse

private def exDict : List (List UInt8) :=
  [ [0x41]
  , [0x41, 0xFE]
  , [0x41, 0xFF]
  , [0x41, 0xFF, 0x05]
  , [0x42]
  ]

example :
    prefixRange exDict [0x41, 0xFF] = (2, 4) ∧
    (List.range exDict.length).filter (fun i =>
      match exDict[i]? with | some w => startsWith [0x41, 0xFF] w | none => false) = [2, 3] := by
  decide

example :
    badLastByteInc [0x41, 0xFF] = some [0x41, 0x00] ∧
    lexLt [0x41, 0x00] [0x41, 0xFF] = true ∧
    nextPrefix [0x41, 0xFF] = some [0x42] := by
  decide

end TinylambOpt.ResearchGoals.DictPrefixMonotonicRange
