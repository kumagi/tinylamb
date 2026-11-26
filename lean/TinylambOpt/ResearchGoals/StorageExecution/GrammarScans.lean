import TinylambOpt.ResearchGoals.Compression.Wire

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.
-/

namespace TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.Compression.Wire

/-! A grammar-compressed byte string is a concatenation/repetition tree. Each
node compiles to a finite transition table; squaring skips repeated phrases.
A query can start mid-row with a previously saved parser state. -/
structure Reader where
  states : Nat
  step : Nat → Nat → Nat
  accept : Nat → Bool
inductive Rope where
  | lit (bytes : Bytes)
  | cat (left right : Rope)
  | rep (times : Nat) (child : Rope)
deriving Inhabited
def expand : Rope → Bytes
  | .lit s => s
  | .cat a b => expand a ++ expand b
  | .rep k a => (List.replicate k (expand a)).flatten
def extent : Rope → Nat
  | .lit s => s.length
  | .cat a b => extent a + extent b
  | .rep k a => k * extent a
def reverse : Rope → Rope
  | .lit s => .lit s.reverse
  | .cat a b => .cat (reverse b) (reverse a)
  | .rep k a => .rep k (reverse a)
abbrev Table := List Nat
def applyT (t : Table) (q : Nat) := t[q]?.getD q
def chain (a b : Table) := a.map (applyT b)
def power (n : Nat) (t : Table) : Table :=
  if hzero : n=0 then List.range t.length else
    let h := power (n/2) t
    let sq := chain h h
    if n%2==0 then sq else chain sq t
termination_by n
decreasing_by exact Nat.div_lt_self (by omega) (by decide)
def compile (r : Reader) : Rope → Table
  | .lit s => (List.range r.states).map (fun q => s.foldl r.step q)
  | .cat a b => chain (compile r a) (compile r b)
  | .rep n a => power n (compile r a)
def partialScan (r : Reader) : Rope → Nat → Nat → Nat
  | .lit s,n,q => (s.take n).foldl r.step q
  | .cat a b,n,q => if n ≤ extent a then partialScan r a n q else
      partialScan r b (n-extent a) (applyT (compile r a) q)
  | .rep k a,n,q =>
    let size := extent a
    if size==0 then q else
      let full := min k (n/size)
      let next := applyT (power full (compile r a)) q
      if full==k then next else partialScan r a (n%size) next
def matched (r : Reader) (x : Rope) (q : Nat) := r.accept (applyT (compile r x) q)
def rows (r : Reader) (d : List Rope) (ids : List Nat) (q : Nat) :=
  let masks := d.map (fun x => matched r x q)
  ids.map (fun i => masks[i]?.getD false)

/-- Exponentiation of the table skips arbitrarily long repeated strings. -/
def repeatedState (r : Reader) : Prop := ∀ x n q, q<r.states → applyT (power n (compile r x)) q =
  ((List.replicate n (expand x)).flatten).foldl r.step q

/-- Only a partial final phrase needs to be interpreted to obtain the state at an offset. -/
def prefixSeek (r : Reader) : Prop := ∀ x q n, q<r.states → partialScan r x n q = ((expand x).take n).foldl r.step q

/-- A saved state at a prefix resumes scanning across further phrase boundaries. -/
def checkpoint (r : Reader) : Prop := ∀ x q a b, q<r.states → partialScan r x (a+b) q =
  (((expand x).drop a).take b).foldl r.step (partialScan r x a q)

/-- Build the reverse scan from reversed children for suffix inspection. -/
def reverseScan (r : Reader) : Prop := ∀ x q, q<r.states → matched r (reverse x) q =
  r.accept ((expand x).reverse.foldl r.step q)

/-- Cache acceptance once per phrase; a missing local dictionary code is rejected. -/
def dictionaryMask (r : Reader) : Prop := ∀ d ids q, q<r.states → rows r d ids q =
  ids.map (fun i => (d[i]?.map (fun x => r.accept ((expand x).foldl r.step q))).getD false)

/-- Substitute a different grammar with the same transition table inside a larger row. -/
def contextReuse (r : Reader) : Prop := ∀ a b x y q, q<r.states → compile r x = compile r y →
  (expand (.cat a (.cat x b))).foldl r.step q = (expand (.cat a (.cat y b))).foldl r.step q

/-- A cycle in a run of identical phrases permits skipping whole periods. -/
def periodJump (r : Reader) : Prop := ∀ x q period n, q<r.states → 0<period →
  applyT (power period (compile r x)) q = q →
  matched r (.rep n x) q = matched r (.rep (n%period) x) q

/-- Every successor state fits the packed dictionary of state identifiers. -/
def tableBytes (r : Reader) : Prop := ∀ x, let t := compile r x
  (List.range r.states).map (get (pack (width (r.states-1)) t) (width (r.states-1))) = t

namespace ContainsSlash

/-- Binary URL path delimiter search. -/
def reader : Reader := ⟨2, fun q b => if b==47 then 1 else q, (· == 1)⟩

def SearchOnce.Goal : Prop :=
  ∀ a k, 0<k → matched reader (.rep k a) 0 = matched reader a 0
def RepeatedState.Goal : Prop := repeatedState reader
def PrefixSeek.Goal : Prop := prefixSeek reader
def Checkpoint.Goal : Prop := checkpoint reader
def ReverseScan.Goal : Prop := reverseScan reader
def DictionaryMask.Goal : Prop := dictionaryMask reader
def ContextReuse.Goal : Prop := contextReuse reader
def PeriodJump.Goal : Prop := periodJump reader
def TableBytes.Goal : Prop := tableBytes reader

def SlashWitness.Goal : Prop := ∀ x, matched reader x 0 = (expand x).contains 47

end ContainsSlash

namespace EndsInDigit

/-- ASCII numeric suffix classifier. -/
def reader : Reader := ⟨2, fun _ b => (decide (48≤b ∧ b≤57)).toNat, (· == 1)⟩

def NonemptyTail.Goal : Prop :=
  ∀ a b, 0<extent b → matched reader (.cat a b) 0 = matched reader b 0
def RepeatedState.Goal : Prop := repeatedState reader
def PrefixSeek.Goal : Prop := prefixSeek reader
def Checkpoint.Goal : Prop := checkpoint reader
def ReverseScan.Goal : Prop := reverseScan reader
def DictionaryMask.Goal : Prop := dictionaryMask reader
def ContextReuse.Goal : Prop := contextReuse reader
def PeriodJump.Goal : Prop := periodJump reader
def TableBytes.Goal : Prop := tableBytes reader

def LastByte.Goal : Prop := ∀ x, matched reader x 0 = (expand x).getLast?.any (fun b => 48≤b && b≤57)

end EndsInDigit

namespace QuotedField

/-- Odd quote parity marks the interior of a delimited field. -/
def reader : Reader := ⟨2, fun q b => if b==34 then 1-q else q, (· == 1)⟩

def DoublePhrase.Goal : Prop :=
  ∀ x, compile reader (.rep 2 x) = List.range reader.states
def RepeatedState.Goal : Prop := repeatedState reader
def PrefixSeek.Goal : Prop := prefixSeek reader
def Checkpoint.Goal : Prop := checkpoint reader
def ReverseScan.Goal : Prop := reverseScan reader
def DictionaryMask.Goal : Prop := dictionaryMask reader
def ContextReuse.Goal : Prop := contextReuse reader
def PeriodJump.Goal : Prop := periodJump reader
def TableBytes.Goal : Prop := tableBytes reader

def QuoteParity.Goal : Prop := ∀ x, matched reader x 0 = decide ((expand x).count 34 % 2 = 1)

end QuotedField

namespace PercentEscape

/-- Accept exactly after a percent sign and two ASCII hex digits. -/
def reader : Reader where
  states := 4
  step := fun q b =>
    if b == 37 then 1
    else if (48≤b && b≤57) || (65≤b && b≤70) || (97≤b && b≤102) then
      if q == 1 || q == 2 then q+1 else 0
    else 0
  accept := (· == 3)

def LoneIntroducer.Goal : Prop :=
  ∀ x k, partialScan reader (.cat x (.lit [37,37])) (extent x+2) (k%4)=1
def RepeatedState.Goal : Prop := repeatedState reader
def PrefixSeek.Goal : Prop := prefixSeek reader
def Checkpoint.Goal : Prop := checkpoint reader
def ReverseScan.Goal : Prop := reverseScan reader
def DictionaryMask.Goal : Prop := dictionaryMask reader
def ContextReuse.Goal : Prop := contextReuse reader
def PeriodJump.Goal : Prop := periodJump reader
def TableBytes.Goal : Prop := tableBytes reader

def EscapeAcrossBoundary.Goal : Prop := ∀ x, matched reader (.cat x (.lit [37,52,70])) 0 = true

end PercentEscape

namespace LineBoundary

/-- Recognize CRLF even across phrase boundaries. -/
def reader : Reader := ⟨3, fun q b => if b==13 then 1 else if q==1 && b==10 then 2 else 0, (· == 2)⟩

def RunBoundaryCRLF.Goal : Prop :=
  ∀ x k, 1<k → extent x=1 → matched reader (.rep k x) 0=false
def RepeatedState.Goal : Prop := repeatedState reader
def PrefixSeek.Goal : Prop := prefixSeek reader
def Checkpoint.Goal : Prop := checkpoint reader
def ReverseScan.Goal : Prop := reverseScan reader
def DictionaryMask.Goal : Prop := dictionaryMask reader
def ContextReuse.Goal : Prop := contextReuse reader
def PeriodJump.Goal : Prop := periodJump reader
def TableBytes.Goal : Prop := tableBytes reader

def FinalPair.Goal : Prop := ∀ x, matched reader x 0 = [13,10].isSuffixOf (expand x)

end LineBoundary

namespace AsciiOnly

/-- Reject non-ASCII bytes with an absorbing state. -/
def reader : Reader := ⟨2, fun q b => if b<128 then q else 1, (· == 0)⟩

def RejectEarly.Goal : Prop :=
  ∀ a b, matched reader a 0=false → matched reader (.cat a b) 0=false
def RepeatedState.Goal : Prop := repeatedState reader
def PrefixSeek.Goal : Prop := prefixSeek reader
def Checkpoint.Goal : Prop := checkpoint reader
def ReverseScan.Goal : Prop := reverseScan reader
def DictionaryMask.Goal : Prop := dictionaryMask reader
def ContextReuse.Goal : Prop := contextReuse reader
def PeriodJump.Goal : Prop := periodJump reader
def TableBytes.Goal : Prop := tableBytes reader

def SevenBitFastPath.Goal : Prop := ∀ x, matched reader x 0 = (expand x).all (· < 128)

end AsciiOnly

namespace SlashDepth

/-- Saturate the count of URL path delimiters at two. -/
def reader : Reader := ⟨3, fun q b => if b==47 then min 2 (q+1) else q, (· == 2)⟩

def RepeatedDelimiter.Goal : Prop :=
  ∀ x n, 2≤n → matched reader (.rep n x) 0 = (expand x).contains 47
def RepeatedState.Goal : Prop := repeatedState reader
def PrefixSeek.Goal : Prop := prefixSeek reader
def Checkpoint.Goal : Prop := checkpoint reader
def ReverseScan.Goal : Prop := reverseScan reader
def DictionaryMask.Goal : Prop := dictionaryMask reader
def ContextReuse.Goal : Prop := contextReuse reader
def PeriodJump.Goal : Prop := periodJump reader
def TableBytes.Goal : Prop := tableBytes reader

def TwoDelimiters.Goal : Prop := ∀ x, matched reader x 0 = decide (2 ≤ (expand x).count 47)

end SlashDepth

namespace DecimalMod

/-- Residue of an ASCII decimal key; 7 is the invalid sink. -/
def reader : Reader := ⟨8, fun q b => if q==7 || b<48 || 57<b then 7 else (10*q+b-48)%7, (· == 0)⟩

def DecimalSuffix.Goal : Prop :=
  ∀ a x y, x<10 → y<10 → let q:=applyT (compile reader a) 0; q<7 → applyT (compile reader (.cat a (.lit [48+x,48+y]))) 0 = (100*q+10*x+y)%7
def RepeatedState.Goal : Prop := repeatedState reader
def PrefixSeek.Goal : Prop := prefixSeek reader
def Checkpoint.Goal : Prop := checkpoint reader
def ReverseScan.Goal : Prop := reverseScan reader
def DictionaryMask.Goal : Prop := dictionaryMask reader
def ContextReuse.Goal : Prop := contextReuse reader
def PeriodJump.Goal : Prop := periodJump reader
def TableBytes.Goal : Prop := tableBytes reader

def LeadingZero.Goal : Prop := ∀ x, matched reader (.cat (.lit [48]) x) 0 = matched reader x 0

end DecimalMod

namespace EscapedTail

/-- Parity of the trailing backslash run. -/
def reader : Reader := ⟨2, fun q b => if b==92 then 1-q else 0, (· == 1)⟩

def ResetAtDelimiter.Goal : Prop :=
  ∀ a, matched reader (.cat a (.lit [47,92])) 0=true
def RepeatedState.Goal : Prop := repeatedState reader
def PrefixSeek.Goal : Prop := prefixSeek reader
def Checkpoint.Goal : Prop := checkpoint reader
def ReverseScan.Goal : Prop := reverseScan reader
def DictionaryMask.Goal : Prop := dictionaryMask reader
def ContextReuse.Goal : Prop := contextReuse reader
def PeriodJump.Goal : Prop := periodJump reader
def TableBytes.Goal : Prop := tableBytes reader

def TrailingRun.Goal : Prop := ∀ x, matched reader x 0 = decide (((expand x).reverse.takeWhile (· == 92)).length%2=1)

end EscapedTail

namespace CStringEnd

/-- Recognize a single terminal NUL without any embedded NUL. -/
def reader : Reader := ⟨3, fun q b => if q==0 then (if b==0 then 1 else 0) else 2, (· == 1)⟩

def TruncateAtTerminator.Goal : Prop :=
  ∀ pre tail, !(expand pre).contains 0 → matched reader (.cat (.cat pre (.lit [0])) tail) 0 = decide (extent tail=0)
def RepeatedState.Goal : Prop := repeatedState reader
def PrefixSeek.Goal : Prop := prefixSeek reader
def Checkpoint.Goal : Prop := checkpoint reader
def ReverseScan.Goal : Prop := reverseScan reader
def DictionaryMask.Goal : Prop := dictionaryMask reader
def ContextReuse.Goal : Prop := contextReuse reader
def PeriodJump.Goal : Prop := periodJump reader
def TableBytes.Goal : Prop := tableBytes reader

def OneTerminator.Goal : Prop := ∀ x, matched reader (.cat x (.lit [0])) 0 = !(expand x).contains 0

end CStringEnd

#guard matched LineBoundary.reader (.cat (.lit [65,13]) (.lit [10])) 0
#guard partialScan QuotedField.reader (.rep 10001 (.lit [34])) 9999 0 == 1

end TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
