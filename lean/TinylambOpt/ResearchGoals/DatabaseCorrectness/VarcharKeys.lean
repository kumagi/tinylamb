import Std

/-! The binary VARCHAR memcomparable field in type/value.cpp. The tag is 2.
An eight-byte block with marker 9 continues. The last block has marker 1..8;
only the first block may use marker 0, with eight zero padding bytes. -/
namespace TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys

abbrev Bytes := List UInt8

def body : Bytes → Bytes
  | a::b::c::d::e::f::g::h::i::rest => [a,b,c,d,e,f,g,h,9] ++ body (i::rest)
  | tail => tail ++ List.replicate (8-tail.length) 0 ++ [tail.length.toUInt8]
def encode (s : Bytes) : Bytes := 2 :: body s

def groups (initial : Bool) : Bytes → Option (Bytes × Bytes)
  | a::b::c::d::e::f::g::h::flag::rest => do
    let block := [a,b,c,d,e,f,g,h]
    if flag == 9 then
      let (more, suffix) ← groups false rest
      return (block++more,suffix)
    if flag.toNat > 8 || (flag == 0 && !initial) || !(block.drop flag.toNat).all (· == 0) then none
    else return (block.take flag.toNat,rest)
  | _ => none
def decode : Bytes → Option (Bytes × Bytes)
  | 2::rest => groups true rest
  | _ => none

/-- Decoding a component must stop before the next component of a compound index key. -/
def BinaryKeySelfDelimiting.Goal : Prop := ∀ s tail, decode (encode s ++ tail) = some (s,tail)

/-- Reject noncanonical encodings: a successful first field must re-encode to exactly the consumed bytes. -/
def AcceptedKeyPrefixCanonical.Goal : Prop := ∀ wire s tail, decode wire = some (s,tail) →
  wire = encode s ++ tail

/-- Unsigned lexicographic order of raw bytes agrees with index order through embedded NULs and group boundaries. -/
def BinaryKeyOrderEmbedding.Goal : Prop := ∀ a b : Bytes, (encode a < encode b ↔ a < b)

/-- A physical range comparison on compound keys cannot let the next field reverse the order of unequal leading fields. -/
def CompoundSuffixCannotReverseKey.Goal : Prop := ∀ (a b x y : Bytes), a ≠ b →
  (encode a ++ x < encode b ++ y ↔ a < b)

/-- A truncated first field must not be mistaken for a valid empty field at a nine-byte boundary. -/
def IncompleteFieldIsNotEmpty.Goal : Prop := ∀ s n, n < (encode s).length → decode ((encode s).take n) = none

/-- Continuation blocks cannot be closed by appending the otherwise valid empty-string terminator. -/
def EmptyTerminatorOnlyAtStart.Goal : Prop := ∀ (block rest : Bytes), block.length = 8 →
  decode ([2] ++ block ++ [9] ++ body [] ++ rest) = none

/-- Concatenated same-type fields preserve tuple equality, including the distinction between empty and zero bytes. -/
def CompoundKeyComponentIdentity.Goal : Prop := ∀ (a b : List Bytes),
  (a.flatMap encode = b.flatMap encode ↔ a = b)

#guard encode [97,0] == [2,97,0,0,0,0,0,0,0,2]
#guard decode [2,97,1,0,0,0,0,0,0,1] == none

end TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys
