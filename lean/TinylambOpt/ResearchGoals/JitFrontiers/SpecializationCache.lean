import TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.
-/

namespace TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache

open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits)
/-! An ORC entry captures a specialization descriptor. A cache hit must use
the new call's inputs and obey the original compilation assumptions. Entry
guards and retained descriptors are explicit. A retained artifact is immutable,
and the result distinguishes SQL NULL from a fault. -/
structure Cfg where
  literal : Int
  scale : Int
  slot : Nat
  mode : Bool
  epoch : Nat
  table : List Int
deriving Inhabited, Repr
abbrev Row := List (Option Int)
inductive Out where | null | fault | val (n : Int)
deriving DecidableEq, BEq, Repr
def get (r : Row) (i : Nat) : Out := match r[i]? with
  | none => .fault | some none => .null | some (some x) => .val x
def mapV (f : Int → Int) : Out → Out | .val x => .val (f x) | v => v
def checked (x : Int) : Out := if fits 64 x then .val x else .fault
structure Shape where
  key : Cfg → List Int
  code : Cfg → Row → Out
  accepts : Cfg → Row → Bool
  reference : Cfg → Row → Out
def compatible (s : Shape) (a b : Cfg) := s.key a == s.key b
def lookup (s : Shape) (pool : List Cfg) (cfg : Cfg) := pool.find? (compatible s cfg)
def call (s : Shape) (pool : List Cfg) (c : Cfg) (r : Row) :=
  if s.accepts c r then s.code ((lookup s pool c).getD c) r else s.reference c r
def warm (s : Shape) (pool : List Cfg) (c : Cfg) :=
  if (lookup s pool c).isSome then pool else c::pool
def first (rows : List Out) := rows.findIdx? (· == .fault)
def signature (cfg : Cfg) := cfg.table.sum % 65536
def word (v : Int) := (BitVec.ofInt 64 v).toInt

/-- The generated table load uses the captured descriptor and a numeric code. -/
def dictionaryLoad (c : Cfg) (r : Row) : Out :=
  match get r c.slot with
  | .val x =>
    if x < 0 then .null else ((c.table.toArray[x.toNat]?).map Out.val).getD .null
  | v => v

/-- Interpret the requested column's code against the current ordered dictionary. -/
def dictionaryReference (c : Cfg) (r : Row) : Out :=
  match get r c.slot with
  | .val x =>
    let entry := c.table.zipIdx.find? (fun (_, i) => (i : Int) == x)
    (entry.map (fun (v, _) => Out.val v)).getD .null
  | v => v

/-- A fresh specialization is correct under its caller-side entry check. -/
def oneVersion (s : Shape) : Prop := ∀ c r, s.accepts c r → s.code c r=s.reference c r

/-- A cached function compiled from another descriptor is safe on the new guard domain. -/
def substitute (s : Shape) : Prop := ∀ a b r, compatible s a b → s.accepts b r → s.code a r=s.reference b r

/-- Aggressive claim: different keys have distinguishable behavior on a mutually eligible row. -/
def keyMinimal (s : Shape) : Prop := ∀ a b, s.key a≠s.key b →
  ∃ r, s.accepts a r=true ∧ s.accepts b r=true ∧ s.code a r≠s.code b r

/-- Reuse in a whole batch preserves the first failing ordinal as well as the value sequence. -/
def replayBatch (s : Shape) : Prop := ∀ pool c (rs : List Row), let out:=rs.map (call s pool c)
  out=rs.map (s.reference c) ∧ first out=first (rs.map (s.reference c))

/-- A retained artifact and a fresh lookup after eviction agree on a guarded call. -/
def evictWhileHeld (s : Shape) : Prop := ∀ pool c held r, lookup s pool c=some held → s.accepts c r →
  s.code held r=call s [] c r

/-- A warm-up pass compiles exactly the number of distinct absent specialization keys. -/
def compileOnce (s : Shape) : Prop := ∀ (pool cs : List Cfg), (cs.foldl (warm s) pool).length = pool.length +
  (((cs.map s.key).eraseDups).filter (fun k => !(pool.map s.key).contains k)).length

/-- Two published equivalent entries can be discovered in either order without changing the result. -/
def publishRace (s : Shape) : Prop := ∀ a b pool c r, compatible s a b →
  call s (a::b::pool) c r=call s (b::a::pool) c r

/-- Speculative cache compaction: is removing the final key component observationally safe? -/
def dropLastField (s : Shape) : Prop := ∀ a b r, (s.key a).dropLast=(s.key b).dropLast → s.accepts b r →
  s.code a r=s.reference b r

namespace BoundComparison

/-- Bake a signed threshold and dense column slot into a predicate. -/
def shape : Shape where
  key := fun c => [c.slot,c.literal]
  code := fun c r => mapV (fun x => if x<c.literal then 1 else 0) (get r c.slot)
  accepts := fun _ _ => true
  reference := fun c r => mapV (fun x => if c.literal≤x then 0 else 1) (get r c.slot)

def OneVersion.Goal : Prop := oneVersion shape
def Substitute.Goal : Prop := substitute shape
def KeyMinimal.Goal : Prop := keyMinimal shape
def ReplayBatch.Goal : Prop := replayBatch shape
def EvictWhileHeld.Goal : Prop := evictWhileHeld shape
def CompileOnce.Goal : Prop := compileOnce shape
def PublishRace.Goal : Prop := publishRace shape
def DropLastField.Goal : Prop := dropLastField shape

def UnusedScale.Goal : Prop := ∀ c p, shape.key {c with scale:=p}=shape.key c

def ThresholdEdge.Goal : Prop := ∀ c, c.slot=0 → shape.code c [some c.literal]=.val 0

end BoundComparison

namespace CheckedAffine

/-- Select a wrapping kernel only under a checked multiply and add guard. -/
def shape : Shape where
  key := fun c => [c.slot,c.scale,c.literal]
  code := fun c r => mapV (fun x => word (word (x*c.scale)+c.literal)) (get r c.slot)
  accepts := fun c r => match get r c.slot with | .val x => fits 64 (x*c.scale) && fits 64 (x*c.scale+c.literal) | _ => true
  reference := fun c r => match get r c.slot with | .val x => if fits 64 (x*c.scale) then checked (x*c.scale+c.literal) else .fault | v => v

def OneVersion.Goal : Prop := oneVersion shape
def Substitute.Goal : Prop := substitute shape
def KeyMinimal.Goal : Prop := keyMinimal shape
def ReplayBatch.Goal : Prop := replayBatch shape
def EvictWhileHeld.Goal : Prop := evictWhileHeld shape
def CompileOnce.Goal : Prop := compileOnce shape
def PublishRace.Goal : Prop := publishRace shape
def DropLastField.Goal : Prop := dropLastField shape

def ZeroScale.Goal : Prop := ∀ c x, c.slot=0 → c.scale=0 → fits 64 c.literal → shape.code c [some x]=.val c.literal

def GuardPriority.Goal : Prop := ∀ c x, c.slot=0 → fits 64 (x*c.scale)=false → shape.reference c [some x]=.fault

end CheckedAffine

namespace NullPolicy

/-- Specialize COALESCE defaults; dead defaults should disappear from the key. -/
def shape : Shape where
  key := fun c => [(c.slot:Int),c.mode.toNat] ++ (if c.mode then [c.literal] else [])
  code := fun c r => match get r c.slot with | .null => if c.mode then .val c.literal else .null | v => v
  accepts := fun _ _ => true
  reference := fun c r => if c.mode && get r c.slot == .null then .val c.literal else get r c.slot

def OneVersion.Goal : Prop := oneVersion shape
def Substitute.Goal : Prop := substitute shape
def KeyMinimal.Goal : Prop := keyMinimal shape
def ReplayBatch.Goal : Prop := replayBatch shape
def EvictWhileHeld.Goal : Prop := evictWhileHeld shape
def CompileOnce.Goal : Prop := compileOnce shape
def PublishRace.Goal : Prop := publishRace shape
def DropLastField.Goal : Prop := dropLastField shape

def DeadLiteral.Goal : Prop := ∀ c k, c.mode=false → shape.key {c with literal:=k}=shape.key c

def NullWitness.Goal : Prop := ∀ c, c.slot=0 → c.mode=true → shape.code c [none]=.val c.literal

end NullPolicy

namespace SignDirection

/-- Select ascending or descending signed comparison to a literal. -/
def shape : Shape where
  key := fun c => [c.slot,c.literal,c.mode.toNat]
  code := fun c r => mapV (fun x => (decide (if c.mode then x>c.literal else x<c.literal)).toNat) (get r c.slot)
  accepts := fun _ _ => true
  reference := fun c r => mapV (fun x => if x==c.literal then 0 else if (x<c.literal) == !c.mode then 1 else 0) (get r c.slot)

def OneVersion.Goal : Prop := oneVersion shape
def Substitute.Goal : Prop := substitute shape
def KeyMinimal.Goal : Prop := keyMinimal shape
def ReplayBatch.Goal : Prop := replayBatch shape
def EvictWhileHeld.Goal : Prop := evictWhileHeld shape
def CompileOnce.Goal : Prop := compileOnce shape
def PublishRace.Goal : Prop := publishRace shape
def DropLastField.Goal : Prop := dropLastField shape

def Tie.Goal : Prop := ∀ c, c.slot=0 → shape.code c [some c.literal]=.val 0

def Flip.Goal : Prop := ∀ c x, c.slot=0 → x≠c.literal → shape.code {c with mode:=!c.mode} [some x]≠shape.code c [some x]

end SignDirection

namespace PowerRemainder

/-- Lower unsigned modulo by a power of two to a bit mask. -/
def shape : Shape where
  key := fun c => [c.slot,c.scale]
  code := fun c r => mapV (fun x => (x.toNat &&& (2^c.scale.toNat-1):Nat)) (get r c.slot)
  accepts := fun c r => c.scale.toNat<64 && (match get r c.slot with | .val x => 0≤x | _ => true)
  reference := fun c r => mapV (fun x => x%(2^c.scale.toNat:Int)) (get r c.slot)

def OneVersion.Goal : Prop := oneVersion shape
def Substitute.Goal : Prop := substitute shape
def KeyMinimal.Goal : Prop := keyMinimal shape
def ReplayBatch.Goal : Prop := replayBatch shape
def EvictWhileHeld.Goal : Prop := evictWhileHeld shape
def CompileOnce.Goal : Prop := compileOnce shape
def PublishRace.Goal : Prop := publishRace shape
def DropLastField.Goal : Prop := dropLastField shape

def ZeroBits.Goal : Prop := ∀ c x, c.slot=0 → c.scale=0 → shape.code c [some x]=.val 0

def NegativeScaleAlias.Goal : Prop := ∀ c, c.scale<0 → ∀ r, shape.code c r=shape.code {c with scale:=0} r

end PowerRemainder

namespace ProjectPair

/-- Bake adjacent column selection and direction into a load. -/
def shape : Shape where
  key := fun c => [c.slot+c.mode.toNat]
  code := fun c r => get r (c.slot+c.mode.toNat)
  accepts := fun _ _ => true
  reference := fun c r => get r (if c.mode then c.slot+1 else c.slot)

def OneVersion.Goal : Prop := oneVersion shape
def Substitute.Goal : Prop := substitute shape
def KeyMinimal.Goal : Prop := keyMinimal shape
def ReplayBatch.Goal : Prop := replayBatch shape
def EvictWhileHeld.Goal : Prop := evictWhileHeld shape
def CompileOnce.Goal : Prop := compileOnce shape
def PublishRace.Goal : Prop := publishRace shape
def DropLastField.Goal : Prop := dropLastField shape

def EquivalentSlots.Goal : Prop := ∀ (c : Cfg) r, shape.code {c with mode:=true} r=shape.code {c with mode:=false,slot:=c.slot+1} r

def MissingSlot.Goal : Prop := ∀ c r, r.length≤c.slot → shape.code c r=.fault

end ProjectPair

namespace DictionaryContents

/-- Include dictionary values as captured constants. -/
def shape : Shape where
  key := fun c => (c.slot:Int)::c.table
  code := dictionaryLoad
  accepts := fun _ _ => true
  reference := dictionaryReference

def OneVersion.Goal : Prop := oneVersion shape
def Substitute.Goal : Prop := substitute shape
def KeyMinimal.Goal : Prop := keyMinimal shape
def ReplayBatch.Goal : Prop := replayBatch shape
def EvictWhileHeld.Goal : Prop := evictWhileHeld shape
def CompileOnce.Goal : Prop := compileOnce shape
def PublishRace.Goal : Prop := publishRace shape
def DropLastField.Goal : Prop := dropLastField shape

def EpochIndependent.Goal : Prop := ∀ c g, shape.key {c with epoch:=g}=shape.key c

def NullOrBadCode.Goal : Prop := ∀ c x, c.slot=0 → x<0 → shape.code c [some x]=.null

end DictionaryContents

namespace DictionaryChecksum

/-- Speculative key using only the epoch, column and a short dictionary fingerprint. -/
def shape : Shape where
  key := fun c => [c.slot,c.epoch,signature c]
  code := dictionaryLoad
  accepts := fun _ _ => true
  reference := dictionaryReference

def OneVersion.Goal : Prop := oneVersion shape
def Substitute.Goal : Prop := substitute shape
def KeyMinimal.Goal : Prop := keyMinimal shape
def ReplayBatch.Goal : Prop := replayBatch shape
def EvictWhileHeld.Goal : Prop := evictWhileHeld shape
def CompileOnce.Goal : Prop := compileOnce shape
def PublishRace.Goal : Prop := publishRace shape
def DropLastField.Goal : Prop := dropLastField shape

def Collision.Goal : Prop := ∃ a b r, compatible shape a b=true ∧ shape.code a r≠shape.code b r

def FreshEpoch.Goal : Prop := ∀ a b, a.epoch≠b.epoch → compatible shape a b=false

end DictionaryChecksum

namespace ConstantLane

/-- Propagate a page constant only when the current row really contains it. -/
def shape : Shape where
  key := fun c => [c.literal]
  code := fun c _ => .val c.literal
  accepts := fun c r => get r c.slot == .val c.literal
  reference := fun c r => get r c.slot

def OneVersion.Goal : Prop := oneVersion shape
def Substitute.Goal : Prop := substitute shape
def KeyMinimal.Goal : Prop := keyMinimal shape
def ReplayBatch.Goal : Prop := replayBatch shape
def EvictWhileHeld.Goal : Prop := evictWhileHeld shape
def CompileOnce.Goal : Prop := compileOnce shape
def PublishRace.Goal : Prop := publishRace shape
def DropLastField.Goal : Prop := dropLastField shape

def RebindSlot.Goal : Prop := ∀ c j, shape.key {c with slot:=j}=shape.key c

def ColdMiss.Goal : Prop := ∀ c r, get r c.slot≠.val c.literal → call shape [] c r=get r c.slot

end ConstantLane

namespace TruncateKey

/-- Use the low 16 bits of a literal for a modulo-65536 projection. -/
def shape : Shape where
  key := fun c => [c.slot,c.literal%65536]
  code := fun c r => mapV (fun x => ((x%65536)+(c.literal%65536))%65536) (get r c.slot)
  accepts := fun _ _ => true
  reference := fun c r => mapV (fun x => (x+c.literal)%65536) (get r c.slot)

def OneVersion.Goal : Prop := oneVersion shape
def Substitute.Goal : Prop := substitute shape
def KeyMinimal.Goal : Prop := keyMinimal shape
def ReplayBatch.Goal : Prop := replayBatch shape
def EvictWhileHeld.Goal : Prop := evictWhileHeld shape
def CompileOnce.Goal : Prop := compileOnce shape
def PublishRace.Goal : Prop := publishRace shape
def DropLastField.Goal : Prop := dropLastField shape

def Period.Goal : Prop := ∀ c n, shape.key {c with literal:=c.literal+65536*n}=shape.key c

def CanonicalRange.Goal : Prop := ∀ c r v, shape.code c r=.val v → 0≤v ∧ v<65536

end TruncateKey

private def left : Cfg := ⟨0,1,0,false,0,[1,2]⟩
private def right : Cfg := {left with table:=[2,1]}
#guard compatible DictionaryChecksum.shape left right
#guard call DictionaryChecksum.shape [left] right [some 0] != DictionaryChecksum.shape.reference right [some 0]

end TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
