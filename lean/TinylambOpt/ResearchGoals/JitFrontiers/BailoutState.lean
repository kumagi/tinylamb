import Std

/-!
An expression JIT must carry sufficient cursor and accumulator state when it resumes
execution in a slower path.
-/

namespace TinylambOpt.ResearchGoals.JitFrontiers.BailoutState

/-! A compact grouped state backed by an association list. Bailout reports the
first uncommitted input ordinal and the state of its exact prefix. Hitting an
existing group is still legal at capacity. Each group transition is pure,
so speculative state can be discarded before handing off to scalar code. -/
abbrev Row := Nat × Option Int
abbrev State := List (Nat × Int)
structure Policy where
  key : Row → Option Nat
  bump : Option Int → Option Int → Int
  hot : Row → Bool
def step (p : Policy) (s : State) (r : Row) := match p.key r with
  | none => s
  | some k => (k,p.bump (s.lookup k) r.2)::s.filter (fun e => e.1 != k)
def refRun (p : Policy) (s : State) (rs : List Row) := rs.foldl (step p) s
def canCommit (p : Policy) (cap : Nat) (s : State) (r : Row) := p.hot r &&
  (p.key r).all (fun k => (s.lookup k).isSome || s.length<cap)
structure Exit where
  cursor : Nat
  state : State
deriving BEq, Repr
def drive (p : Policy) (cap : Nat) : Nat → State → List Row → Exit
  | i,s,[] => ⟨i,s⟩
  | i,s,r::rs => if canCommit p cap s r then drive p cap (i+1) (step p s r) rs else ⟨i,s⟩
def run (p : Policy) (cap : Nat) := drive p cap 0
def resume (p : Policy) (e : Exit) (rs : List Row) := refRun p e.state (rs.drop e.cursor)
def keys (p : Policy) (s : State) (rs : List Row) := ((s.map Prod.fst)++rs.filterMap p.key).eraseDups

/-- The returned state contains every committed row exactly once. -/
def committedPrefix (p : Policy) : Prop := ∀ cap s rs, let e:=run p cap s rs
  e.cursor≤rs.length ∧ e.state=refRun p s (rs.take e.cursor)

/-- Resume on the reported ordinal even if preceding updates were non-idempotent. -/
def restartRow (p : Policy) : Prop := ∀ cap s rs, resume p (run p cap s rs) rs = refRun p s rs

/-- A short return witnesses the first row that cannot be committed in the reported state. -/
def bailReason (p : Policy) : Prop := ∀ cap s rs i r, let e:=run p cap s rs
  i=e.cursor → rs[i]?=some r → canCommit p cap e.state r=false

/-- At capacity, updating a previously established key must not cause a spurious resize. -/
def fullTableHit (p : Policy) : Prop := ∀ cap s r k, p.hot r → p.key r=some k → (s.lookup k).isSome →
  run p cap s [r]=⟨1,step p s r⟩

/-- Increasing available slots cannot move a bailout to an earlier row. -/
def increaseCapacity (p : Policy) : Prop := ∀ a b s rs, a≤b → (run p a s rs).cursor≤(run p b s rs).cursor

/-- Reserve space for distinct live keys to avoid any capacity bailout. -/
def reserveExactly (p : Policy) : Prop := ∀ rs s, (s.map Prod.fst).Nodup → rs.all p.hot →
  run p (keys p s rs).length s rs=⟨rs.length,refRun p s rs⟩

/-- A successful first morsel can hand its state to a second compiled morsel. -/
def chunkHandoff (p : Policy) : Prop := ∀ s a b cap, let e:=run p cap s a
  e.cursor=a.length → run p cap s (a++b)=drive p cap a.length e.state b

/-- Strong hypothesis: moving the cursor past the uncommitted bailout row is harmless. -/
def earlyAdvance (p : Policy) : Prop := ∀ cap s rs, let e:=run p cap s rs
  refRun p e.state (rs.drop (e.cursor+1))=refRun p s rs

namespace CountRows

/-- COUNT(*) per key, including NULL payloads. -/
def policy : Policy where
  key := fun r => some r.1
  bump := fun old _ => old.getD 0+1
  hot := fun _ => true

def CommittedPrefix.Goal : Prop := committedPrefix policy
def RestartRow.Goal : Prop := restartRow policy
def BailReason.Goal : Prop := bailReason policy
def FullTableHit.Goal : Prop := fullTableHit policy
def IncreaseCapacity.Goal : Prop := increaseCapacity policy
def ReserveExactly.Goal : Prop := reserveExactly policy
def ChunkHandoff.Goal : Prop := chunkHandoff policy
def EarlyAdvance.Goal : Prop := earlyAdvance policy

def ReplayDoubles.Goal : Prop := ∀ k, ((refRun policy [] [(k,none),(k,none)]).lookup k)=some 2

def CountByKey.Goal : Prop := ∀ rs k, ((refRun policy [] rs).lookup k).getD 0 = (rs.countP (fun r => r.1==k):Int)

end CountRows

namespace SumValid

/-- Skip NULL contributions before group admission. -/
def policy : Policy where
  key := fun (k,v) => v.map (fun _ => k)
  bump := fun old v => old.getD 0+v.getD 0
  hot := fun _ => true

def CommittedPrefix.Goal : Prop := committedPrefix policy
def RestartRow.Goal : Prop := restartRow policy
def BailReason.Goal : Prop := bailReason policy
def FullTableHit.Goal : Prop := fullTableHit policy
def IncreaseCapacity.Goal : Prop := increaseCapacity policy
def ReserveExactly.Goal : Prop := reserveExactly policy
def ChunkHandoff.Goal : Prop := chunkHandoff policy
def EarlyAdvance.Goal : Prop := earlyAdvance policy

def NoNullAllocation.Goal : Prop := ∀ s k, run policy 0 s [(k,none)]=⟨1,s⟩

def CancelInGroup.Goal : Prop := ∀ k x, (refRun policy [] [(k,some x),(k,some (-x))]).lookup k=some 0

end SumValid

namespace MinValid

/-- Aggregate MIN on valid payloads; an exact replay may be idempotent. -/
def policy : Policy where
  key := fun (k,v) => v.map (fun _ => k)
  bump := fun old v => min (old.getD (v.getD 0)) (v.getD 0)
  hot := fun _ => true

def CommittedPrefix.Goal : Prop := committedPrefix policy
def RestartRow.Goal : Prop := restartRow policy
def BailReason.Goal : Prop := bailReason policy
def FullTableHit.Goal : Prop := fullTableHit policy
def IncreaseCapacity.Goal : Prop := increaseCapacity policy
def ReserveExactly.Goal : Prop := reserveExactly policy
def ChunkHandoff.Goal : Prop := chunkHandoff policy
def EarlyAdvance.Goal : Prop := earlyAdvance policy

def RepeatMin.Goal : Prop := ∀ s r, step policy (step policy s r) r=step policy s r

def DominatedPayload.Goal : Prop := ∀ s k x y, x≤y → (refRun policy s [(k,some x),(k,some y)]).lookup k=(step policy s (k,some x)).lookup k

end MinValid

namespace LastValue

/-- Last-write order matters for imputed measurements. -/
def policy : Policy where
  key := fun r => some r.1
  bump := fun _ v => v.getD 0
  hot := fun _ => true

def CommittedPrefix.Goal : Prop := committedPrefix policy
def RestartRow.Goal : Prop := restartRow policy
def BailReason.Goal : Prop := bailReason policy
def FullTableHit.Goal : Prop := fullTableHit policy
def IncreaseCapacity.Goal : Prop := increaseCapacity policy
def ReserveExactly.Goal : Prop := reserveExactly policy
def ChunkHandoff.Goal : Prop := chunkHandoff policy
def EarlyAdvance.Goal : Prop := earlyAdvance policy

def LastWins.Goal : Prop := ∀ s k a b, (refRun policy s [(k,a),(k,b)]).lookup k=some (b.getD 0)

def DistinctOrder.Goal : Prop := ∀ k a b, a≠b → refRun policy [] [(k,some a),(k,some b)] ≠ refRun policy [] [(k,some b),(k,some a)]

end LastValue

namespace FirstValue

/-- Initialize once; later duplicates must not overwrite the first value. -/
def policy : Policy where
  key := fun r => some r.1
  bump := fun old v => old.getD (v.getD 0)
  hot := fun _ => true

def CommittedPrefix.Goal : Prop := committedPrefix policy
def RestartRow.Goal : Prop := restartRow policy
def BailReason.Goal : Prop := bailReason policy
def FullTableHit.Goal : Prop := fullTableHit policy
def IncreaseCapacity.Goal : Prop := increaseCapacity policy
def ReserveExactly.Goal : Prop := reserveExactly policy
def ChunkHandoff.Goal : Prop := chunkHandoff policy
def EarlyAdvance.Goal : Prop := earlyAdvance policy

def FirstWins.Goal : Prop := ∀ k x rs, (refRun policy [] ((k,x)::rs)).lookup k=some (x.getD 0)

def InitialHit.Goal : Prop := ∀ k x y, (step policy [(k,x)] (k,some y)).lookup k=some x

end FirstValue

namespace Packed32

/-- Narrow only keys fitting a 32-bit fast path. -/
def policy : Policy where
  key := fun r => some r.1
  bump := fun old v => old.getD 0+v.getD 1
  hot := fun r => r.1<2^32

def CommittedPrefix.Goal : Prop := committedPrefix policy
def RestartRow.Goal : Prop := restartRow policy
def BailReason.Goal : Prop := bailReason policy
def FullTableHit.Goal : Prop := fullTableHit policy
def IncreaseCapacity.Goal : Prop := increaseCapacity policy
def ReserveExactly.Goal : Prop := reserveExactly policy
def ChunkHandoff.Goal : Prop := chunkHandoff policy
def EarlyAdvance.Goal : Prop := earlyAdvance policy

def FirstWide.Goal : Prop := ∀ s v cap, run policy cap s [(2^32,v)]=⟨0,s⟩

def LastNarrow.Goal : Prop := ∀ v, (run policy 1 [] [(2^32-1,v)]).cursor=1

end Packed32

namespace BucketCount

/-- Coarsened keys can share a slot across a capacity boundary. -/
def policy : Policy where
  key := fun r => some (r.1/64)
  bump := fun old _ => old.getD 0+1
  hot := fun _ => true

def CommittedPrefix.Goal : Prop := committedPrefix policy
def RestartRow.Goal : Prop := restartRow policy
def BailReason.Goal : Prop := bailReason policy
def FullTableHit.Goal : Prop := fullTableHit policy
def IncreaseCapacity.Goal : Prop := increaseCapacity policy
def ReserveExactly.Goal : Prop := reserveExactly policy
def ChunkHandoff.Goal : Prop := chunkHandoff policy
def EarlyAdvance.Goal : Prop := earlyAdvance policy

def SingleBucket.Goal : Prop := ∀ k, (run policy 1 [] [(64*k,none),(64*k+63,none)]).cursor=2

def BoundaryGrow.Goal : Prop := ∀ k, (run policy 1 [] [(64*k+63,none),(64*(k+1),none)]).cursor=1

end BucketCount

namespace SignedDigit

/-- Noncommutative polynomial update of ordered rows within a group. -/
def policy : Policy where
  key := fun r => some r.1
  bump := fun old v => 10*old.getD 0+v.getD 0
  hot := fun r => r.2.all (fun x => 0≤x && x<10)

def CommittedPrefix.Goal : Prop := committedPrefix policy
def RestartRow.Goal : Prop := restartRow policy
def BailReason.Goal : Prop := bailReason policy
def FullTableHit.Goal : Prop := fullTableHit policy
def IncreaseCapacity.Goal : Prop := increaseCapacity policy
def ReserveExactly.Goal : Prop := reserveExactly policy
def ChunkHandoff.Goal : Prop := chunkHandoff policy
def EarlyAdvance.Goal : Prop := earlyAdvance policy

def TwoDigits.Goal : Prop := ∀ k a b, (refRun policy [] [(k,some a),(k,some b)]).lookup k=some (10*a+b)

def EscapeDigit.Goal : Prop := ∀ k, (run policy 1 [] [(k,some 9),(k,some 10)]).cursor=1

end SignedDigit

namespace ChangedOnly

/-- Collapse adjacent repeated qualifying keys into set membership. -/
def policy : Policy where
  key := fun (k,v) => if v==some 0 then none else some k
  bump := fun _ _ => 1
  hot := fun _ => true

def CommittedPrefix.Goal : Prop := committedPrefix policy
def RestartRow.Goal : Prop := restartRow policy
def BailReason.Goal : Prop := bailReason policy
def FullTableHit.Goal : Prop := fullTableHit policy
def IncreaseCapacity.Goal : Prop := increaseCapacity policy
def ReserveExactly.Goal : Prop := reserveExactly policy
def ChunkHandoff.Goal : Prop := chunkHandoff policy
def EarlyAdvance.Goal : Prop := earlyAdvance policy

def ZeroSkip.Goal : Prop := ∀ k s, step policy s (k,some 0)=s

def Admission.Goal : Prop := ∀ k rs, (refRun policy [] rs).lookup k=some 1 ↔ ∃ r∈rs, r.1=k ∧ r.2≠some 0

end ChangedOnly

namespace ParityCounter

/-- Toggle a bit per selected event; replaying a row changes parity. -/
def policy : Policy where
  key := fun r => some r.1
  bump := fun old _ => 1-old.getD 0
  hot := fun _ => true

def CommittedPrefix.Goal : Prop := committedPrefix policy
def RestartRow.Goal : Prop := restartRow policy
def BailReason.Goal : Prop := bailReason policy
def FullTableHit.Goal : Prop := fullTableHit policy
def IncreaseCapacity.Goal : Prop := increaseCapacity policy
def ReserveExactly.Goal : Prop := reserveExactly policy
def ChunkHandoff.Goal : Prop := chunkHandoff policy
def EarlyAdvance.Goal : Prop := earlyAdvance policy

def EvenPairs.Goal : Prop := ∀ k v, (refRun policy [] [(k,v),(k,v)]).lookup k=some 0

def ClosedCount.Goal : Prop := ∀ rs k, ((refRun policy [] rs).lookup k).getD 0 = ((rs.countP (fun r => r.1==k)%2:Nat):Int)

end ParityCounter

#guard run CountRows.policy 1 [] [(0,none),(0,none),(1,none)] == ⟨2,[(0,2)]⟩
#guard resume CountRows.policy ⟨2,[(0,2)]⟩ [(0,none),(0,none),(1,none)] == [(1,1),(0,2)]

end TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
