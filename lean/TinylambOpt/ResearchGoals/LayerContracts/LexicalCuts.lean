import Std

namespace TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts

/-! Streaming boundary recognizers over lexer symbols. The input alphabet
includes individual punctuation and, for nested comments, pretokenized open
and close markers. Each model handles just its documented lexical fragment.
Outputs are zero-based cut positions and the state to resume in the next chunk. -/
inductive Sym where
  | atom | semi | tick | dq | sq | slash | dash | openComment | closeComment
  | lf | cr | dot | dollar | comma | openArray | closeArray
  deriving BEq, DecidableEq
abbrev Trace := Nat × List Nat
structure Model where
  step : Nat → Sym → Nat × Bool
  special : Sym → Bool
  cacheKey : Nat → Nat
def scan (m : Model) (q : Nat) (xs : List Sym) : Trace :=
  let (z,cuts) := xs.zipIdx.foldl (fun (st,acc) (c,i) =>
    let (t,hit) := m.step st c
    (t, if hit then i::acc else acc)) (q,[])
  (z,cuts.reverse)
def joinAt (n : Nat) (left right : List Nat) := left ++ right.map (n+·)
def quoted (q : Nat) (c quote delim : Sym) : Nat × Bool :=
  if c == quote then (1-q,false) else (q,q==0 && c==delim)
def escaped (q : Nat) (c quote delim : Sym) : Nat × Bool :=
  if q==2 then (1,false) else if q==1 && c==.slash then (2,false)
  else quoted q c quote delim
def initialClass (q : Nat) := min 1 q
def reachable (m : Model) (q : Nat) : Prop := ∃ xs, (scan m 0 xs).1 = q

/-- Carry lexical state and shift all offsets to split an arbitrary input chunk. -/
def carry (m : Model) : Prop := ∀ q a b, let x := scan m q a; let y := scan m x.1 b
  scan m q (a++b) = (y.1,joinAt a.length x.2 y.2)

/-- Can each chunk be scanned independently from the neutral state? -/
def coldChunk (m : Model) : Prop := ∀ a b, (scan m 0 (a++b)).2 = joinAt a.length (scan m 0 a).2 (scan m 0 b).2

/-- Can a compiled chunk summary be indexed by only the proposed entry-state class? -/
def stateKey (m : Model) : Prop := ∀ q r xs, reachable m q → reachable m r → m.cacheKey q = m.cacheKey r →
  (scan m q xs).2 = (scan m r xs).2 ∧ m.cacheKey (scan m q xs).1 = m.cacheKey (scan m r xs).1

/-- Bypass scanning bytes with no control symbols, even while resuming a partial token. -/
def skipPlain (m : Model) : Prop := ∀ q xs, reachable m q → !(xs.any m.special) → scan m q xs = (q,[])

/-- Return the first statement/field boundary without looking beyond it. -/
def nextCut (m : Model) : Prop := ∀ q xs i, (scan m q xs).2.head? = some i →
  ∀ suffix, (scan m q (xs.take (i+1) ++ suffix)).2.head? = some i

/-- A newline supplies an unconditional restart point for independent workers. -/
def lineRestart (m : Model) : Prop := ∀ a b, (scan m 0 (a ++ [.lf] ++ b)).2 =
  joinAt (a.length+1) (scan m 0 (a++[.lf])).2 (scan m 0 b).2

/-- Two chunks equivalent from state zero can substitute at any scanner entry. -/
def neutralSignature (m : Model) : Prop := ∀ a b, a.length=b.length → scan m 0 a = scan m 0 b → ∀ q, reachable m q → scan m q a = scan m q b

/-- After the first emitted delimiter the suffix can be reparsed from zero. -/
def discardCutPrefix (m : Model) : Prop := ∀ q xs i, (scan m q xs).2.head? = some i →
  (scan m q xs).2.tail = ((scan m 0 (xs.drop (i+1))).2.map (i+1+·))

namespace StringStmt

/-- Semicolons outside single quotes, with doubled quotes toggling twice. -/
def model : Model :=
  ⟨fun q c => quoted q c .sq .semi,
   fun c => [.sq,.semi].contains c, id⟩

def Carry.Goal : Prop := carry model
def ColdChunk.Goal : Prop := coldChunk model
def StateKey.Goal : Prop := stateKey model
def SkipPlain.Goal : Prop := skipPlain model
def NextCut.Goal : Prop := nextCut model
def LineRestart.Goal : Prop := lineRestart model
def NeutralSignature.Goal : Prop := neutralSignature model
def DiscardCutPrefix.Goal : Prop := discardCutPrefix model

def InsideString.Goal : Prop := ∀ n, scan model 1 (List.replicate n .semi) = (1,[])

def DoubleQuote.Goal : Prop := ∀ q xs, q<2 → scan model q ([.sq,.sq]++xs) = ((scan model q xs).1,(scan model q xs).2.map (2+·))

end StringStmt

namespace QuotedField

/-- Qualified-name components split on dots outside double-quoted identifiers. -/
def model : Model :=
  ⟨fun q c => quoted q c .dq .dot,
   fun c => [.dq,.dot].contains c, id⟩

def Carry.Goal : Prop := carry model
def ColdChunk.Goal : Prop := coldChunk model
def StateKey.Goal : Prop := stateKey model
def SkipPlain.Goal : Prop := skipPlain model
def NextCut.Goal : Prop := nextCut model
def LineRestart.Goal : Prop := lineRestart model
def NeutralSignature.Goal : Prop := neutralSignature model
def DiscardCutPrefix.Goal : Prop := discardCutPrefix model

def EmbeddedDot.Goal : Prop := ∀ n, (scan model 0 ([.dq]++List.replicate n .dot++[.dq])).2=[]

def UnquotedDots.Goal : Prop := ∀ n, (scan model 0 (List.replicate n .dot)).2=List.range n

end QuotedField

namespace BacktickPath

/-- Backslash can quote the next character inside a backtick path component. -/
def model : Model :=
  ⟨fun q c => escaped q c .tick .dot,
   fun c => [.tick,.slash,.dot].contains c, initialClass⟩

def Carry.Goal : Prop := carry model
def ColdChunk.Goal : Prop := coldChunk model
def StateKey.Goal : Prop := stateKey model
def SkipPlain.Goal : Prop := skipPlain model
def NextCut.Goal : Prop := nextCut model
def LineRestart.Goal : Prop := lineRestart model
def NeutralSignature.Goal : Prop := neutralSignature model
def DiscardCutPrefix.Goal : Prop := discardCutPrefix model

def EscapedTick.Goal : Prop := ∀ xs, (scan model 1 ([.slash,.tick]++xs)).1 = (scan model 1 xs).1

def TerminalEscape.Goal : Prop := ∀ c, (model.step 2 c).1=1

end BacktickPath

namespace EscapedString

/-- Single-quoted literal with backslash escaping. -/
def model : Model :=
  ⟨fun q c => escaped q c .sq .semi,
   fun c => [.sq,.slash,.semi].contains c, initialClass⟩

def Carry.Goal : Prop := carry model
def ColdChunk.Goal : Prop := coldChunk model
def StateKey.Goal : Prop := stateKey model
def SkipPlain.Goal : Prop := skipPlain model
def NextCut.Goal : Prop := nextCut model
def LineRestart.Goal : Prop := lineRestart model
def NeutralSignature.Goal : Prop := neutralSignature model
def DiscardCutPrefix.Goal : Prop := discardCutPrefix model

def PendingByte.Goal : Prop := ∀ c, model.step 2 c = (1,false)

def OddSlash.Goal : Prop := ∀ n, (scan model 1 (List.replicate (2*n+1) .slash)).1=2

end EscapedString

namespace DashComment

/-- Two consecutive dashes open a line comment; newline closes it. -/
def model : Model :=
  ⟨fun q c => if q==2 then (if c==.lf then 0 else 2,false) else if c==.dash then (if q==1 then 2 else 1,false) else (0,c==.semi),
   fun c => [.dash,.lf,.semi].contains c, initialClass⟩

def Carry.Goal : Prop := carry model
def ColdChunk.Goal : Prop := coldChunk model
def StateKey.Goal : Prop := stateKey model
def SkipPlain.Goal : Prop := skipPlain model
def NextCut.Goal : Prop := nextCut model
def LineRestart.Goal : Prop := lineRestart model
def NeutralSignature.Goal : Prop := neutralSignature model
def DiscardCutPrefix.Goal : Prop := discardCutPrefix model

def NoCommentCut.Goal : Prop := ∀ n, scan model 2 (List.replicate n .semi)=(2,[])

def SingleDash.Goal : Prop := ∀ n, (scan model 1 (.atom::List.replicate n .semi)).2=(List.range n).map (1+·)

end DashComment

namespace NestedComment

/-- Already tokenized block-comment delimiters with arbitrary nesting depth. -/
def model : Model :=
  ⟨fun q c => if c==.openComment then (q+1,false) else if c==.closeComment then (q-1,false) else (q,q==0 && c==.semi),
   fun c => [.openComment,.closeComment,.semi].contains c, initialClass⟩

def Carry.Goal : Prop := carry model
def ColdChunk.Goal : Prop := coldChunk model
def StateKey.Goal : Prop := stateKey model
def SkipPlain.Goal : Prop := skipPlain model
def NextCut.Goal : Prop := nextCut model
def LineRestart.Goal : Prop := lineRestart model
def NeutralSignature.Goal : Prop := neutralSignature model
def DiscardCutPrefix.Goal : Prop := discardCutPrefix model

def Depth.Goal : Prop := ∀ q n, (scan model q (List.replicate n .closeComment)).1=q-n

def Balanced.Goal : Prop := ∀ n, (scan model 0 (List.replicate n .openComment ++ List.replicate n .closeComment ++ [.semi])).2=[2*n]

end NestedComment

namespace DollarPair

/-- The fixed delimiter $$ toggles a quoted body; a trailing single dollar needs a carry. -/
def model : Model :=
  ⟨fun q c => if c==.dollar then (match q with | 0 => 1 | 1 => 2 | 2 => 3 | _ => 0,false) else (if q<2 then 0 else 2,q<2 && c==.semi),
   fun c => [.dollar,.semi].contains c, fun q => q/2⟩

def Carry.Goal : Prop := carry model
def ColdChunk.Goal : Prop := coldChunk model
def StateKey.Goal : Prop := stateKey model
def SkipPlain.Goal : Prop := skipPlain model
def NextCut.Goal : Prop := nextCut model
def LineRestart.Goal : Prop := lineRestart model
def NeutralSignature.Goal : Prop := neutralSignature model
def DiscardCutPrefix.Goal : Prop := discardCutPrefix model

def FourDollars.Goal : Prop := ∀ n, (scan model 0 (List.replicate (4*n) .dollar)).1=0

def QuotedSemis.Goal : Prop := ∀ n, (scan model 2 (List.replicate n .semi)).2=[]

end DollarPair

namespace CsvRecord

/-- Line-feed record boundaries outside double-quoted CSV fields. -/
def model : Model :=
  ⟨fun q c => quoted q c .dq .lf,
   fun c => [.dq,.lf].contains c, id⟩

def Carry.Goal : Prop := carry model
def ColdChunk.Goal : Prop := coldChunk model
def StateKey.Goal : Prop := stateKey model
def SkipPlain.Goal : Prop := skipPlain model
def NextCut.Goal : Prop := nextCut model
def LineRestart.Goal : Prop := lineRestart model
def NeutralSignature.Goal : Prop := neutralSignature model
def DiscardCutPrefix.Goal : Prop := discardCutPrefix model

def QuotedNewlines.Goal : Prop := ∀ n, scan model 1 (List.replicate n .lf)=(1,[])

def JoinedLines.Goal : Prop := ∀ n, (scan model 0 (List.replicate n .lf)).2.length=n

end CsvRecord

namespace ArrayComma

/-- Commas at array depth one delimit elements; strings have been tokenized as atoms. -/
def model : Model :=
  ⟨fun q c => if c==.openArray then (q+1,false) else if c==.closeArray then (q-1,false) else (q,q==1 && c==.comma),
   fun c => [.openArray,.closeArray,.comma].contains c, initialClass⟩

def Carry.Goal : Prop := carry model
def ColdChunk.Goal : Prop := coldChunk model
def StateKey.Goal : Prop := stateKey model
def SkipPlain.Goal : Prop := skipPlain model
def NextCut.Goal : Prop := nextCut model
def LineRestart.Goal : Prop := lineRestart model
def NeutralSignature.Goal : Prop := neutralSignature model
def DiscardCutPrefix.Goal : Prop := discardCutPrefix model

def DepthOne.Goal : Prop := ∀ n, (scan model 1 (List.replicate n .comma)).2=List.range n

def Nested.Goal : Prop := ∀ n q, 1<q → (scan model q (List.replicate n .comma)).2=[]

end ArrayComma

namespace CrLf

/-- Frame a line only when LF immediately follows CR. -/
def model : Model :=
  ⟨fun q c => (if c==.cr then 1 else 0,q==1 && c==.lf),
   fun c => [.cr,.lf].contains c, id⟩

def Carry.Goal : Prop := carry model
def ColdChunk.Goal : Prop := coldChunk model
def StateKey.Goal : Prop := stateKey model
def SkipPlain.Goal : Prop := skipPlain model
def NextCut.Goal : Prop := nextCut model
def LineRestart.Goal : Prop := lineRestart model
def NeutralSignature.Goal : Prop := neutralSignature model
def DiscardCutPrefix.Goal : Prop := discardCutPrefix model

def NoLoneLf.Goal : Prop := ∀ n, (scan model 0 (List.replicate n .lf)).2=[]

def EndsOnLf.Goal : Prop := ∀ q xs i, i∈(scan model q xs).2 → xs[i]?=some .lf

end CrLf

#guard scan NestedComment.model 1 [.closeComment,.semi] == (0,[1])
#guard scan NestedComment.model 2 [.closeComment,.semi] == (1,[])
#guard scan DashComment.model 0 [.dash,.dash,.semi,.lf,.semi] == (0,[4])

end TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
