import TinylambOpt.ResearchGoals.Compression.Wire

/-! Storage and scan costs when dictionaries are shared across row groups. Symbols
are raw byte strings. Charge each length prefix and the packed id width including
the zero-bit singleton case. Front coding refers only to an immediate predecessor. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
open TinylambOpt.ResearchGoals.Compression.Wire

abbrev Symbol := List Nat
def vocab (xs : List Symbol) := xs.eraseDups
def dictSize (d : List Symbol) := (d.map (fun s => ubytes s.length + s.length)).sum
def idSize (rows keys : Nat) := byteCount (rows * width (keys-1))
def charge (xs : List Symbol) := dictSize (vocab xs) + idSize xs.length (vocab xs).length
def ids (d xs : List Symbol) := xs.map d.idxOf

/-- A row-group dictionary need not survive compaction after translating its ids once into the shared dictionary. -/
def ComposedDictionaryRemap.Goal : Prop := ∀ xs others,
  let localDict := vocab xs
  let shared := vocab (others++xs)
  (ids localDict xs).map (fun i => (ids shared localDict)[i]?) = (ids shared xs).map some

/-- A predicate evaluated on dictionary entries can drive a selection vector for arbitrary repeating byte strings. -/
def VocabularyPredicateSelection.Goal : Prop := ∀ xs (p : Symbol → Bool),
  let d := vocab xs
  (ids d xs).map (fun i => (d.map p)[i]?.getD false) = xs.map p

/-- Count the dictionary bytes saved by sharing, including length prefixes of exactly the overlapping symbols. -/
def SharedDictionaryOverlapCharge.Goal : Prop := ∀ a b,
  dictSize (vocab a) + dictSize (vocab b) = dictSize (vocab (a++b)) +
    dictSize ((vocab a).filter (fun s => (vocab b).contains s))

/-- Dictionary payload savings alone suffice to decide whether merging two compressed groups saves disk space. -/
def OverlapPaysWiderIds.Goal : Prop := ∀ a b, (vocab a).any (fun s => (vocab b).contains s) = true →
  charge (a++b) ≤ charge a + charge b

/-- If both groups already pay the merged id width, sharing dictionaries cannot increase frame bytes. -/
def SharedWidthMergeAdmission.Goal : Prop := ∀ a b,
  let merged := width ((vocab (a++b)).length-1)
  width ((vocab a).length-1) = merged → width ((vocab b).length-1) = merged →
  charge (a++b) ≤ charge a + charge b

/-- An equality predicate whose literal is absent from the shared vocabulary can bypass every row id. -/
def AbsentLiteralSkipsIdStream.Goal : Prop := ∀ xs lit, ¬ lit ∈ vocab xs →
  xs.filter (fun s => s == lit) = []

def common : Symbol → Symbol → Nat
  | x::xs, y::ys => if x == y then 1+common xs ys else 0
  | _, _ => 0
def front (prev s : Symbol) := (common prev s, s.drop (common prev s))
def expand (prev : Symbol) (r : Nat × Symbol) := prev.take r.1 ++ r.2
def cost (prev s : Symbol) :=
  let (k,tail) := front prev s
  ubytes k + ubytes tail.length + tail.length
def chain : Symbol → List Symbol → Nat
  | _, [] => 0
  | prev, s::ss => cost prev s + chain s ss

/-- Decoding a front-coded entry needs only the previous byte string, including empty and prefix strings. -/
def FrontCodePredecessorSufficiency.Goal : Prop := ∀ prev s, expand prev (front prev s) = s

/-- Inserting a dictionary entry only changes the compressed costs of that entry and its immediate successor. -/
def DictionaryInsertLocalReprice.Goal : Prop := ∀ prev entry next tail,
  chain prev (entry::next::tail) + cost prev next =
  chain prev (next::tail) + cost prev entry + cost entry next

/-- A larger common prefix is sufficient to choose the cheaper predecessor despite varint size cliffs. -/
def LongestPrefixPredecessor.Goal : Prop := ∀ a b s, common a s ≤ common b s → cost b s ≤ cost a s

/-- With one local id per row, renumbering a dictionary cannot affect the packed size even under skew. -/
def RenumberedPackedIdWidth.Goal : Prop := ∀ d e xs, d.Perm e → d.Nodup → (∀ x ∈ d, x ∈ xs) →
  (∀ x ∈ xs, x ∈ d) → packedSize (ids d xs) = packedSize (ids e xs)

/-- Projecting a single row group never increases the dictionary payload when rebasing its surviving symbols. -/
def FilteredDictionaryFootprint.Goal : Prop := ∀ xs (p : Symbol → Bool),
  charge (xs.filter p) ≤ charge xs

#guard expand [97,98,99] (front [97,98,99] [97,98]) == [97,98]

#guard charge (List.replicate 100 [97] ++ [[98]]) == 17

#guard
  let a := List.replicate 100 [97]
  let b := [[97],[98]]
  charge a + charge b == 7 && charge (a++b) == 17

end TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
