import Std

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.
-/

namespace TinylambOpt.ResearchGoals.LayerContracts.RedoPatches

/-! Committed updates to one logical cell. A strictly larger LSN applies a
patch once. Patch families are research alternatives to full after-images;
a cell's stamp alone does not reconstruct a skipped prerequisite patch. -/
abbrev Arg := Nat × Nat
structure Model where
  patch : Arg → Nat → Nat
  fuse : Arg → Arg → Option Arg
  exchange : Arg → Arg → Bool
structure Cell where
  lsn : Nat
  value : Nat
  deriving BEq, DecidableEq
abbrev Record := Nat × Arg
def replay (m : Model) (s : Cell) (r : Record) : Cell :=
  if s.lsn < r.1 then ⟨r.1,m.patch r.2 s.value⟩ else s
def logRun (m : Model) (s : Cell) (rs : List Record) := rs.foldl (replay m) s
def mergeMask (mask bits old : Nat) := (old - (old &&& mask)) ||| (bits &&& mask)

/-- A successful pair-fusion produces the chronological after-image on any base value. -/
def fusion (m : Model) : Prop := ∀ a b c x, m.fuse a b = some c → m.patch c x = m.patch b (m.patch a x)

/-- Can a fused record also replay on a snapshot already containing its first half? -/
def restartInside (m : Model) : Prop := ∀ (s : Cell) i j a b c, s.lsn < i → i < j → m.fuse a b = some c →
  let mid := replay m s (i,a)
  replay m mid (j,c) = replay m mid (j,b)

/-- When marked independent, patches may be scheduled in either order. -/
def exchangeGuard (m : Model) : Prop := ∀ a b x, m.exchange a b → m.patch b (m.patch a x) = m.patch a (m.patch b x)

/-- Is the proposed conflict test complete as well as sound? -/
def exactExchange (m : Model) : Prop := ∀ a b, (∀ x, m.patch b (m.patch a x) = m.patch a (m.patch b x)) → m.exchange a b

/-- A last-record-per-cell index reconstructs both value and stamp. -/
def newestOnly (m : Model) : Prop := ∀ s i j a b, s.lsn < i → i < j → replay m s (j,b) = logRun m s [(i,a),(j,b)]

/-- Blindly repeating a patch after recovery is observationally idempotent. -/
def noStamp (m : Model) : Prop := ∀ a x, m.patch a (m.patch a x) = m.patch a x

/-- Replaying an already-applied sorted prefix before the suffix is harmless. -/
def suffixRetry (m : Model) : Prop := ∀ s a b, (a.map Prod.fst).Pairwise (· < ·) →
  logRun m (logRun m s a) (a++b) = logRun m s (a++b)

/-- Three records can be fused using either successful parenthesization. -/
def parallelFold (m : Model) : Prop := ∀ a b c l r p q x, m.fuse a b = some l → m.fuse l c = some p →
  m.fuse b c = some r → m.fuse a r = some q → m.patch p x = m.patch q x

namespace Overwrite

/-- Full after-image. -/
def model : Model :=
  ⟨fun a _ => a.1,
   fun _ b => some b,
   fun a b => a.1 == b.1⟩

def Fusion.Goal : Prop := fusion model
def RestartInside.Goal : Prop := restartInside model
def ExchangeGuard.Goal : Prop := exchangeGuard model
def ExactExchange.Goal : Prop := exactExchange model
def NewestOnly.Goal : Prop := newestOnly model
def NoStamp.Goal : Prop := noStamp model
def SuffixRetry.Goal : Prop := suffixRetry model
def ParallelFold.Goal : Prop := parallelFold model

def Constant.Goal : Prop := ∀ a x y, model.patch a x = model.patch a y

def Collision.Goal : Prop := ∀ a b x, model.patch a x = model.patch b x ↔ a.1=b.1

end Overwrite

namespace Increment

/-- Counter delta; repeated application changes the result. -/
def model : Model :=
  ⟨fun a x => x+a.1,
   fun a b => some (a.1+b.1,0),
   fun _ _ => true⟩

def Fusion.Goal : Prop := fusion model
def RestartInside.Goal : Prop := restartInside model
def ExchangeGuard.Goal : Prop := exchangeGuard model
def ExactExchange.Goal : Prop := exactExchange model
def NewestOnly.Goal : Prop := newestOnly model
def NoStamp.Goal : Prop := noStamp model
def SuffixRetry.Goal : Prop := suffixRetry model
def ParallelFold.Goal : Prop := parallelFold model

def Rebase.Goal : Prop := ∀ a x d, model.patch a (x+d) = model.patch a x+d

def DuplicateCost.Goal : Prop := ∀ a x, model.patch a (model.patch a x)-model.patch a x=a.1

end Increment

namespace Affine

/-- Scale and add in unbounded integer magnitude. -/
def model : Model :=
  ⟨fun a x => a.1*x+a.2,
   fun a b => some (b.1*a.1,b.1*a.2+b.2),
   fun a b => b.1*a.2+b.2 == a.1*b.2+a.2⟩

def Fusion.Goal : Prop := fusion model
def RestartInside.Goal : Prop := restartInside model
def ExchangeGuard.Goal : Prop := exchangeGuard model
def ExactExchange.Goal : Prop := exactExchange model
def NewestOnly.Goal : Prop := newestOnly model
def NoStamp.Goal : Prop := noStamp model
def SuffixRetry.Goal : Prop := suffixRetry model
def ParallelFold.Goal : Prop := parallelFold model

def NoBase.Goal : Prop := ∀ a, (∀ x, model.patch a x = model.patch a 0) ↔ a.1=0

def Difference.Goal : Prop := ∀ a x y, x ≤ y → model.patch a y-model.patch a x = a.1*(y-x)

end Affine

namespace HighWater

/-- Raise allocator high-water mark. -/
def model : Model :=
  ⟨fun a x => max a.1 x,
   fun a b => some (max a.1 b.1,0),
   fun _ _ => true⟩

def Fusion.Goal : Prop := fusion model
def RestartInside.Goal : Prop := restartInside model
def ExchangeGuard.Goal : Prop := exchangeGuard model
def ExactExchange.Goal : Prop := exactExchange model
def NewestOnly.Goal : Prop := newestOnly model
def NoStamp.Goal : Prop := noStamp model
def SuffixRetry.Goal : Prop := suffixRetry model
def ParallelFold.Goal : Prop := parallelFold model

def NeverShrink.Goal : Prop := ∀ a x, x ≤ model.patch a x

def Dominated.Goal : Prop := ∀ a b x, a.1 ≤ b.1 → model.patch b (model.patch a x) = model.patch b x

end HighWater

namespace Ceiling

/-- Clamp remaining credit to a ceiling. -/
def model : Model :=
  ⟨fun a x => min a.1 x,
   fun a b => some (min a.1 b.1,0),
   fun _ _ => true⟩

def Fusion.Goal : Prop := fusion model
def RestartInside.Goal : Prop := restartInside model
def ExchangeGuard.Goal : Prop := exchangeGuard model
def ExactExchange.Goal : Prop := exactExchange model
def NewestOnly.Goal : Prop := newestOnly model
def NoStamp.Goal : Prop := noStamp model
def SuffixRetry.Goal : Prop := suffixRetry model
def ParallelFold.Goal : Prop := parallelFold model

def NeverGrow.Goal : Prop := ∀ a x, model.patch a x ≤ x

def SmallerLimit.Goal : Prop := ∀ a b x, b.1 ≤ a.1 → model.patch b (model.patch a x) = model.patch b x

end Ceiling

namespace XorFlags

/-- Toggle a word of flags. -/
def model : Model :=
  ⟨fun a x => x ^^^ a.1,
   fun a b => some (a.1 ^^^ b.1,0),
   fun _ _ => true⟩

def Fusion.Goal : Prop := fusion model
def RestartInside.Goal : Prop := restartInside model
def ExchangeGuard.Goal : Prop := exchangeGuard model
def ExactExchange.Goal : Prop := exactExchange model
def NewestOnly.Goal : Prop := newestOnly model
def NoStamp.Goal : Prop := noStamp model
def SuffixRetry.Goal : Prop := suffixRetry model
def ParallelFold.Goal : Prop := parallelFold model

def Undo.Goal : Prop := ∀ a x, model.patch a (model.patch a x) = x

def CancelDelta.Goal : Prop := ∀ a b x, model.patch a x = model.patch b x → a.1=b.1

end XorFlags

namespace FieldStore

/-- Set selected bits from a payload. -/
def model : Model :=
  ⟨fun a x => mergeMask a.1 a.2 x,
   fun a b => some (a.1 ||| b.1,mergeMask b.1 b.2 (a.1 &&& a.2)),
   fun a b => (a.1 &&& b.1) == 0⟩

def Fusion.Goal : Prop := fusion model
def RestartInside.Goal : Prop := restartInside model
def ExchangeGuard.Goal : Prop := exchangeGuard model
def ExactExchange.Goal : Prop := exactExchange model
def NewestOnly.Goal : Prop := newestOnly model
def NoStamp.Goal : Prop := noStamp model
def SuffixRetry.Goal : Prop := suffixRetry model
def ParallelFold.Goal : Prop := parallelFold model

def PreserveUnmasked.Goal : Prop := ∀ a x i, a.1.testBit i = false → (model.patch a x).testBit i = x.testBit i

def MaskedValue.Goal : Prop := ∀ a x, (model.patch a x &&& a.1) = a.2 &&& a.1

end FieldStore

namespace Consume

/-- Saturating subtraction of a released amount. -/
def model : Model :=
  ⟨fun a x => x-a.1,
   fun a b => some (a.1+b.1,0),
   fun _ _ => true⟩

def Fusion.Goal : Prop := fusion model
def RestartInside.Goal : Prop := restartInside model
def ExchangeGuard.Goal : Prop := exchangeGuard model
def ExactExchange.Goal : Prop := exactExchange model
def NewestOnly.Goal : Prop := newestOnly model
def NoStamp.Goal : Prop := noStamp model
def SuffixRetry.Goal : Prop := suffixRetry model
def ParallelFold.Goal : Prop := parallelFold model

def Underflow.Goal : Prop := ∀ a x, model.patch a x = 0 ↔ x ≤ a.1

def Residual.Goal : Prop := ∀ a x, a.1 ≤ x → model.patch a x+a.1=x

end Consume

namespace CompareSwap

/-- Rewrite an old value to a new one only on exact equality. -/
def model : Model :=
  ⟨fun a x => if x == a.1 then a.2 else x,
   fun a b => if a.1 == b.1 then some (a.1,if a.2 == b.1 then b.2 else a.2) else none,
   fun a b => a.1 != b.1 && a.2 != b.1 && b.2 != a.1⟩

def Fusion.Goal : Prop := fusion model
def RestartInside.Goal : Prop := restartInside model
def ExchangeGuard.Goal : Prop := exchangeGuard model
def ExactExchange.Goal : Prop := exactExchange model
def NewestOnly.Goal : Prop := newestOnly model
def NoStamp.Goal : Prop := noStamp model
def SuffixRetry.Goal : Prop := suffixRetry model
def ParallelFold.Goal : Prop := parallelFold model

def Skipped.Goal : Prop := ∀ a x, x ≠ a.1 → model.patch a x=x

def Source.Goal : Prop := ∀ a x, model.patch a x ≠ x → x=a.1

end CompareSwap

namespace Modulo

/-- Repartition an ordinal modulo a positive extent. -/
def model : Model :=
  ⟨fun a x => x%(a.1+1),
   fun a b => if (a.1+1)%(b.1+1)==0 then some b else none,
   fun a b => (max a.1 b.1+1)%(min a.1 b.1+1) == 0⟩

def Fusion.Goal : Prop := fusion model
def RestartInside.Goal : Prop := restartInside model
def ExchangeGuard.Goal : Prop := exchangeGuard model
def ExactExchange.Goal : Prop := exactExchange model
def NewestOnly.Goal : Prop := newestOnly model
def NoStamp.Goal : Prop := noStamp model
def SuffixRetry.Goal : Prop := suffixRetry model
def ParallelFold.Goal : Prop := parallelFold model

def FitsExtent.Goal : Prop := ∀ a x, model.patch a x ≤ a.1

def Period.Goal : Prop := ∀ a x, model.patch a (x+a.1+1)=model.patch a x

end Modulo

#guard logRun Increment.model ⟨0,10⟩ [(1,(2,0)),(2,(3,0))] == ⟨2,15⟩
#guard replay Increment.model ⟨1,12⟩ (2,(5,0)) == ⟨2,17⟩

end TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
