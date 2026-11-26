import Std

/-!
SQL expression lowering must preserve which values and errors are demanded on each
active control-flow path.
-/

namespace TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

/-! Predicate SSA over nullable booleans and numbered expression faults.
Trace entries are reads of deterministic, side-effect-free slots. Missing slots
fault; eager reads from unreachable expressions are therefore observable. -/
inductive V where | yes | no | null | fault (site : Nat)
deriving BEq, DecidableEq, Repr, Inhabited
inductive E where
  | arg (id : Nat) | lit (v : V) | neg (e : E) | isTrue (e : E) | missing (e : E)
  | conj (a b : E) | disj (a b : E) | co (a b : E) | branch (p a b : E)
def flip : V → V | .yes => .no | .no => .yes | v => v
def known : V → V | .yes => .yes | .fault n => .fault n | _ => .no
def isNull : V → V | .null => .yes | .fault n => .fault n | _ => .no
abbrev Trace := V × List Nat
def after (t : Trace) (f : V → Trace) : Trace := match t.1 with
  | .fault _ => t
  | v => let (w,reads) := f v; (w,t.2++reads)
def run (env : List V) : E → Trace
  | .arg i => (env[i]?.getD (.fault i),[i])
  | .lit v => (v,[])
  | .neg e => let (v,r):=run env e; (flip v,r)
  | .missing e => let (v,r):=run env e; (isNull v,r)
  | .isTrue e => let (v,r):=run env e; (known v,r)
  | .branch p a b => after (run env p) (fun v => if v==.yes then run env a else run env b)
  | .co a b => after (run env a) (fun v => if v==.null then run env b else (v,[]))
  | .conj a b => after (run env a) fun v =>
    if v==.no then (.no,[]) else after (run env b) (fun w =>
      (if w==.no then .no else if v==.null then .null else w,[]))
  | .disj a b => after (run env a) fun v =>
    if v==.yes then (.yes,[]) else after (run env b) (fun w =>
      (if w==.yes then .yes else if v==.null then .null else w,[]))
structure Rewrite where
  src : E
  dst : E
  guard : List V → Bool
def nonfault : V → Bool | .fault _ => false | _ => true
def slot (xs : List V) (i : Nat) := xs[i]?.getD (.fault i)
def usable (xs : List V) (i : Nat) := nonfault (slot xs i)
def present (xs : List V) (i : Nat) := usable xs i && slot xs i != .null
def accepted (v : V) := known v
def dispatch (r : Rewrite) (xs : List V) := run xs (if r.guard xs then r.dst else r.src)
def rowsFail (k : List V → Trace) (xs : List (List V)) :=
  xs.zipIdx.findSome? (fun (e,i) => match (k e).1 with | .fault s => some (i,s) | _ => none)
abbrev a := E.arg 0
abbrev b := E.arg 1
abbrev c := E.arg 2

/-- Preserve full three-valued results and fault site in value context. -/
def projection (r : Rewrite) : Prop := ∀ e, r.guard e → (run e r.dst).1 = (run e r.src).1

/-- WHERE merges FALSE and UNKNOWN, but still observes faults. -/
def whereMask (r : Rewrite) : Prop := ∀ e, r.guard e → accepted (run e r.dst).1=accepted (run e r.src).1

/-- Can the guard be removed specifically at the WHERE consumer? -/
def dropGuardInFilter (r : Rewrite) : Prop := ∀ e, accepted (run e r.dst).1=accepted (run e r.src).1

/-- Guarded lowering must not touch a slot skipped by scalar short circuit. -/
def noNewRead (r : Rewrite) : Prop := ∀ e, r.guard e → ∀ i ∈ (run e r.dst).2, i ∈ (run e r.src).2

/-- Cached common subexpressions may reduce repeated evaluations of the same slot. -/
def reuseBudget (r : Rewrite) : Prop := ∀ e i, r.guard e → (run e r.dst).2.count i ≤ (run e r.src).2.count i

/-- Batch speculation must return the same row and expression fault before publishing results. -/
def faultOrder (r : Rewrite) : Prop := ∀ es, rowsFail (dispatch r) es = rowsFail (fun e => run e r.src) es

/-- A successful predicate can authorize a later branch without a second scalar guard. -/
def truthDominates (r : Rewrite) : Prop := ∀ e, (run e r.src).1=.yes →
  (run e (.branch r.dst c (.lit .no))).1=(run e c).1

/-- A cached decision is reusable when all source and target read slots stay unchanged. -/
def readSetGuard (r : Rewrite) : Prop := ∀ e f, r.guard e →
  (∀ i∈(run e r.src).2++(run e r.dst).2, slot e i=slot f i) → (run f r.dst).1=(run f r.src).1

namespace AndCommute

/-- Swap the two branch tests when both operands are total. -/
def rule : Rewrite :=
  ⟨.conj a b, .conj b a, fun e => usable e 0 && usable e 1⟩

def Projection.Goal : Prop := projection rule
def WhereMask.Goal : Prop := whereMask rule
def DropGuardInFilter.Goal : Prop := dropGuardInFilter rule
def NoNewRead.Goal : Prop := noNewRead rule
def ReuseBudget.Goal : Prop := reuseBudget rule
def FaultOrder.Goal : Prop := faultOrder rule
def TruthDominates.Goal : Prop := truthDominates rule
def ReadSetGuard.Goal : Prop := readSetGuard rule

def MaskedFault.Goal : Prop := ∀ s, (run [.no,.fault s] rule.src).1=.no ∧ (run [.no,.fault s] rule.dst).1=.fault s

def CostCriterion.Goal : Prop := ∀ e, present e 0 → present e 1 → ((run e rule.dst).2.length < (run e rule.src).2.length ↔ slot e 1=.no ∧ slot e 0=.yes)

end AndCommute

namespace OrFactor

/-- Distribute short-circuit OR over a repeated common AND prefix. -/
def rule : Rewrite :=
  ⟨.disj (.conj a b) (.conj a c), .conj a (.disj b c), fun _ => true⟩

def Projection.Goal : Prop := projection rule
def WhereMask.Goal : Prop := whereMask rule
def DropGuardInFilter.Goal : Prop := dropGuardInFilter rule
def NoNewRead.Goal : Prop := noNewRead rule
def ReuseBudget.Goal : Prop := reuseBudget rule
def FaultOrder.Goal : Prop := faultOrder rule
def TruthDominates.Goal : Prop := truthDominates rule
def ReadSetGuard.Goal : Prop := readSetGuard rule

def RepeatedPrefix.Goal : Prop := ∀ e, (run e rule.dst).2.count 0=1

def SkipBoth.Goal : Prop := ∀ e, slot e 0=.no → run e rule.dst=(.no,[0])

end OrFactor

namespace CaseAnd

/-- Turn CASE WHEN a THEN b ELSE FALSE into a masked conjunction. -/
def rule : Rewrite :=
  ⟨.branch a b (.lit .no), .conj a b, fun e => slot e 0 != .null⟩

def Projection.Goal : Prop := projection rule
def WhereMask.Goal : Prop := whereMask rule
def DropGuardInFilter.Goal : Prop := dropGuardInFilter rule
def NoNewRead.Goal : Prop := noNewRead rule
def ReuseBudget.Goal : Prop := reuseBudget rule
def FaultOrder.Goal : Prop := faultOrder rule
def TruthDominates.Goal : Prop := truthDominates rule
def ReadSetGuard.Goal : Prop := readSetGuard rule

def NullPredicate.Goal : Prop := (run [.null,.yes] rule.src).1=.no ∧ (run [.null,.yes] rule.dst).1=.null

def MaskedRhs.Goal : Prop := ∀ s, run [.null,.fault s] rule.src=(.no,[0])

end CaseAnd

namespace TrueGuard

/-- Lower CASE WHEN a THEN b ELSE FALSE with is-true normalization. -/
def rule : Rewrite :=
  ⟨.branch a b (.lit .no), .conj (.isTrue a) b, fun _ => true⟩

def Projection.Goal : Prop := projection rule
def WhereMask.Goal : Prop := whereMask rule
def DropGuardInFilter.Goal : Prop := dropGuardInFilter rule
def NoNewRead.Goal : Prop := noNewRead rule
def ReuseBudget.Goal : Prop := reuseBudget rule
def FaultOrder.Goal : Prop := faultOrder rule
def TruthDominates.Goal : Prop := truthDominates rule
def ReadSetGuard.Goal : Prop := readSetGuard rule

def TraceParity.Goal : Prop := ∀ e, run e rule.src=run e rule.dst

def IgnoreMissing.Goal : Prop := ∀ e, slot e 0=.null → (run e rule.dst).2=[0]

end TrueGuard

namespace CoalesceDispatch

/-- Inline COALESCE with a null test, which may duplicate reads. -/
def rule : Rewrite :=
  ⟨.co a b, .branch (.missing a) b a, fun _ => true⟩

def Projection.Goal : Prop := projection rule
def WhereMask.Goal : Prop := whereMask rule
def DropGuardInFilter.Goal : Prop := dropGuardInFilter rule
def NoNewRead.Goal : Prop := noNewRead rule
def ReuseBudget.Goal : Prop := reuseBudget rule
def FaultOrder.Goal : Prop := faultOrder rule
def TruthDominates.Goal : Prop := truthDominates rule
def ReadSetGuard.Goal : Prop := readSetGuard rule

def ExtraRead.Goal : Prop := ∀ e, present e 0 → (run e rule.dst).2.length=(run e rule.src).2.length+1

def FallbackTrace.Goal : Prop := ∀ e, slot e 0=.null → run e rule.src=run e rule.dst

end CoalesceDispatch

namespace AbsorbAnd

/-- Simplify a AND (a OR b); an UNKNOWN a can still demand b. -/
def rule : Rewrite :=
  ⟨.conj a (.disj a b), a, fun e => slot e 0 != .null || usable e 1⟩

def Projection.Goal : Prop := projection rule
def WhereMask.Goal : Prop := whereMask rule
def DropGuardInFilter.Goal : Prop := dropGuardInFilter rule
def NoNewRead.Goal : Prop := noNewRead rule
def ReuseBudget.Goal : Prop := reuseBudget rule
def FaultOrder.Goal : Prop := faultOrder rule
def TruthDominates.Goal : Prop := truthDominates rule
def ReadSetGuard.Goal : Prop := readSetGuard rule

def FaultWhenUnknown.Goal : Prop := ∀ s, (run [.null,.fault s] rule.src).1=.fault s

def OneLoad.Goal : Prop := ∀ e, (run e rule.dst).2=[0]

end AbsorbAnd

namespace DoubleNegation

/-- Lower NOT NOT a to a boolean slot, including NULL. -/
def rule : Rewrite :=
  ⟨.neg (.neg a), a, fun _ => true⟩

def Projection.Goal : Prop := projection rule
def WhereMask.Goal : Prop := whereMask rule
def DropGuardInFilter.Goal : Prop := dropGuardInFilter rule
def NoNewRead.Goal : Prop := noNewRead rule
def ReuseBudget.Goal : Prop := reuseBudget rule
def FaultOrder.Goal : Prop := faultOrder rule
def TruthDominates.Goal : Prop := truthDominates rule
def ReadSetGuard.Goal : Prop := readSetGuard rule

def FaultSlot.Goal : Prop := ∀ s, run [.fault s] rule.src=(.fault s,[0])

def NullRemains.Goal : Prop := run [.null] rule.dst=(.null,[0])

end DoubleNegation

namespace DeMorgan

/-- Generate the inverted branch condition directly in negative polarity. -/
def rule : Rewrite :=
  ⟨.neg (.conj a b), .disj (.neg a) (.neg b), fun _ => true⟩

def Projection.Goal : Prop := projection rule
def WhereMask.Goal : Prop := whereMask rule
def DropGuardInFilter.Goal : Prop := dropGuardInFilter rule
def NoNewRead.Goal : Prop := noNewRead rule
def ReuseBudget.Goal : Prop := reuseBudget rule
def FaultOrder.Goal : Prop := faultOrder rule
def TruthDominates.Goal : Prop := truthDominates rule
def ReadSetGuard.Goal : Prop := readSetGuard rule

def NegatedTrace.Goal : Prop := ∀ e, (run e rule.src).2=(run e rule.dst).2

def EarlyTrue.Goal : Prop := ∀ e, slot e 0=.no → run e rule.dst=(.yes,[0])

end DeMorgan

namespace IdenticalArms

/-- Remove CASE condition when identical arms and no predicate failure. -/
def rule : Rewrite :=
  ⟨.branch a b b, b, fun e => usable e 0⟩

def Projection.Goal : Prop := projection rule
def WhereMask.Goal : Prop := whereMask rule
def DropGuardInFilter.Goal : Prop := dropGuardInFilter rule
def NoNewRead.Goal : Prop := noNewRead rule
def ReuseBudget.Goal : Prop := reuseBudget rule
def FaultOrder.Goal : Prop := faultOrder rule
def TruthDominates.Goal : Prop := truthDominates rule
def ReadSetGuard.Goal : Prop := readSetGuard rule

def KilledCondition.Goal : Prop := ∀ e, usable e 0 → (run e rule.src).2=0::(run e rule.dst).2

def FaultPriority.Goal : Prop := ∀ s t, (run [.fault s,.fault t] rule.src).1=.fault s

end IdenticalArms

namespace NestedChoice

/-- Combine nested CASE conditions into a joint guard. -/
def rule : Rewrite :=
  ⟨.branch a (.branch b c (.lit .no)) (.lit .no), .branch (.conj (.isTrue a) (.isTrue b)) c (.lit .no), fun _ => true⟩

def Projection.Goal : Prop := projection rule
def WhereMask.Goal : Prop := whereMask rule
def DropGuardInFilter.Goal : Prop := dropGuardInFilter rule
def NoNewRead.Goal : Prop := noNewRead rule
def ReuseBudget.Goal : Prop := reuseBudget rule
def FaultOrder.Goal : Prop := faultOrder rule
def TruthDominates.Goal : Prop := truthDominates rule
def ReadSetGuard.Goal : Prop := readSetGuard rule

def PayloadDemand.Goal : Prop := ∀ e, (2∈(run e rule.dst).2 ↔ slot e 0=.yes ∧ slot e 1=.yes)

def ColdPath.Goal : Prop := ∀ e, slot e 0=.null → run e rule.dst=(.no,[0])

end NestedChoice

#guard run [.null,.fault 7] CaseAnd.rule.src == (.no,[0])
#guard run [.null,.fault 7] CaseAnd.rule.dst == (.fault 7,[0,1])

#guard run [.null,.yes,.fault 7] OrFactor.rule.src == (.fault 7,[0,1,0,2])
#guard run [.null,.yes,.fault 7] OrFactor.rule.dst == (.null,[0,1])

end TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
