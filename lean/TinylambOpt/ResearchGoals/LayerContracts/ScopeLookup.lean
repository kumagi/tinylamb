import Std

namespace TinylambOpt.ResearchGoals.LayerContracts.ScopeLookup

/-! A binder searches lexical tiers from nearest to farthest. One syntactic
match binds, several are ambiguous even if they point to one catalog ID.
Each lookup mode proposes a small posting key and a residual name test. -/
structure Name where
  text : String := ""
  owner : String := ""
  pos : Nat := 0
  kind : Nat := 0
  hidden : Bool := false
  deriving BEq, Inhabited
structure Entry extends Name where
  id : Nat
abbrev Env := List (List Entry)
inductive Result where | missing | found (id : Nat) | ambiguous
  deriving BEq, DecidableEq
structure Model where
  accept : Name → Name → Bool
  key : Name → String
def classify : List Nat → Result
  | [] => .missing | [id] => .found id | _ => .ambiguous
def hits (m : Model) (q : Name) (tier : List Entry) :=
  (tier.filter (fun e => m.accept q e.toName)).map Entry.id
def lookup (m : Model) (q : Name) (env : Env) :=
  classify ((env.map (hits m q)).find? (!·.isEmpty) |>.getD [])
def posted (m : Model) (q : Name) (env : Env) :=
  env.map (List.filter (fun e => m.key e.toName == m.key q))
def foldName (s : String) := s.toLower
def visible (n : Name) := !n.hidden

/-- Generated posting key never excludes an eligible declaration. -/
def posting (m : Model) : Prop := ∀ q e, m.accept q e → m.key q = m.key e

/-- Posting lookup preserves missing, unique and ambiguous outcomes. -/
def indexedScope (m : Model) : Prop := ∀ q env, lookup m q (posted m q env) = lookup m q env

/-- Aggressive binder: concatenate all scopes before testing ambiguity. -/
def flatten (m : Model) : Prop := ∀ q env, lookup m q [env.flatten] = lookup m q env

/-- After finding a nonempty inner scope no outer suffix need be inspected. -/
def shadowCut (m : Model) : Prop := ∀ q a b, lookup m q a ≠ .missing → lookup m q (a++b) = lookup m q a

/-- Can repeated catalog IDs be merged before diagnosing syntactic ambiguity? -/
def coalesceId (m : Model) : Prop := ∀ q tier, classify (hits m q tier).eraseDups = classify (hits m q tier)

/-- A posting bucket alone identifies interchangeable lookup requests. -/
def keyOnlyCache (m : Model) : Prop := ∀ p q env, m.key p = m.key q → lookup m p env = lookup m q env

/-- Permuting a tier changes neither successful binding nor ambiguity. -/
def withinTier (m : Model) : Prop := ∀ q a b rest, a.Perm b → lookup m q (a::rest) = lookup m q (b::rest)

/-- Inserting a declaration in an outer tier cannot invalidate a resolved inner binding. -/
def incrementalInvalidate (m : Model) : Prop := ∀ q inner tier e rest, lookup m q inner ≠ .missing →
  lookup m q (inner ++ (e::tier)::rest) = lookup m q (inner ++ tier::rest)

namespace BareColumn

/-- Case-folded unqualified visible column. -/
def model : Model :=
  ⟨fun q e => foldName q.text == foldName e.text && visible e,
   fun n => foldName n.text⟩

def Posting.Goal : Prop := posting model
def IndexedScope.Goal : Prop := indexedScope model
def Flatten.Goal : Prop := flatten model
def ShadowCut.Goal : Prop := shadowCut model
def CoalesceId.Goal : Prop := coalesceId model
def KeyOnlyCache.Goal : Prop := keyOnlyCache model
def WithinTier.Goal : Prop := withinTier model
def IncrementalInvalidate.Goal : Prop := incrementalInvalidate model

def IgnoreOwner.Goal : Prop := ∀ (q e : Name) s, model.accept {q with owner := s} e = model.accept q e

def HideColumn.Goal : Prop := ∀ (q e : Name), model.accept q {e with hidden := true} = false

end BareColumn

namespace QualifiedColumn

/-- Both relation qualifier and column spelling are significant. -/
def model : Model :=
  ⟨fun q e => q.owner == e.owner && q.text == e.text && visible e,
   Name.text⟩

def Posting.Goal : Prop := posting model
def IndexedScope.Goal : Prop := indexedScope model
def Flatten.Goal : Prop := flatten model
def ShadowCut.Goal : Prop := shadowCut model
def CoalesceId.Goal : Prop := coalesceId model
def KeyOnlyCache.Goal : Prop := keyOnlyCache model
def WithinTier.Goal : Prop := withinTier model
def IncrementalInvalidate.Goal : Prop := incrementalInvalidate model

def WrongTable.Goal : Prop := ∀ q e, q.owner ≠ e.owner → model.accept q e = false

def OwnerPosting.Goal : Prop := ∀ q e, model.accept q e → q.owner = e.owner

end QualifiedColumn

namespace QuotedLabel

/-- Quoted labels preserve exact spelling, including case. -/
def model : Model :=
  ⟨fun q e => q.text == e.text,
   Name.text⟩

def Posting.Goal : Prop := posting model
def IndexedScope.Goal : Prop := indexedScope model
def Flatten.Goal : Prop := flatten model
def ShadowCut.Goal : Prop := shadowCut model
def CoalesceId.Goal : Prop := coalesceId model
def KeyOnlyCache.Goal : Prop := keyOnlyCache model
def WithinTier.Goal : Prop := withinTier model
def IncrementalInvalidate.Goal : Prop := incrementalInvalidate model

def CaseSplit.Goal : Prop := ∀ q e, q.text ≠ e.text → foldName q.text = foldName e.text → model.accept q e = false

def QuotedHidden.Goal : Prop := ∀ q e, model.accept q {e with hidden := true} = model.accept q e

end QuotedLabel

namespace OrderOrdinal

/-- Resolve by projected position independently of output aliases. -/
def model : Model :=
  ⟨fun q e => q.pos == e.pos && visible e,
   fun n => toString n.pos⟩

def Posting.Goal : Prop := posting model
def IndexedScope.Goal : Prop := indexedScope model
def Flatten.Goal : Prop := flatten model
def ShadowCut.Goal : Prop := shadowCut model
def CoalesceId.Goal : Prop := coalesceId model
def KeyOnlyCache.Goal : Prop := keyOnlyCache model
def WithinTier.Goal : Prop := withinTier model
def IncrementalInvalidate.Goal : Prop := incrementalInvalidate model

def Rename.Goal : Prop := ∀ q e s, model.accept q {e with text := s} = model.accept q e

def OtherOrdinal.Goal : Prop := ∀ q e, q.pos ≠ e.pos → model.accept q e = false

end OrderOrdinal

namespace TypedFunction

/-- Function name plus exact arity and argument family. -/
def model : Model :=
  ⟨fun q e => foldName q.text == foldName e.text && q.pos == e.pos && q.kind == e.kind,
   fun n => foldName n.text⟩

def Posting.Goal : Prop := posting model
def IndexedScope.Goal : Prop := indexedScope model
def Flatten.Goal : Prop := flatten model
def ShadowCut.Goal : Prop := shadowCut model
def CoalesceId.Goal : Prop := coalesceId model
def KeyOnlyCache.Goal : Prop := keyOnlyCache model
def WithinTier.Goal : Prop := withinTier model
def IncrementalInvalidate.Goal : Prop := incrementalInvalidate model

def ArityGate.Goal : Prop := ∀ q e, q.pos ≠ e.pos → model.accept q e = false

def TypeGate.Goal : Prop := ∀ q e, q.kind ≠ e.kind → model.accept q e = false

end TypedFunction

namespace Variadic

/-- A variadic candidate declares minimum arity. -/
def model : Model :=
  ⟨fun q e => q.text == e.text && e.pos ≤ q.pos,
   Name.text⟩

def Posting.Goal : Prop := posting model
def IndexedScope.Goal : Prop := indexedScope model
def Flatten.Goal : Prop := flatten model
def ShadowCut.Goal : Prop := shadowCut model
def CoalesceId.Goal : Prop := coalesceId model
def KeyOnlyCache.Goal : Prop := keyOnlyCache model
def WithinTier.Goal : Prop := withinTier model
def IncrementalInvalidate.Goal : Prop := incrementalInvalidate model

def LargerCall.Goal : Prop := ∀ q e n, model.accept q e → q.pos ≤ n → model.accept {q with pos := n} e

def ArityIsNotExact.Goal : Prop := ∀ q e, q.text = e.text → e.pos < q.pos → model.accept q e

end Variadic

namespace WildcardOwner

/-- A missing qualifier accepts declarations from multiple owners. -/
def model : Model :=
  ⟨fun q e => q.text == e.text && (q.owner.isEmpty || q.owner == e.owner),
   Name.text⟩

def Posting.Goal : Prop := posting model
def IndexedScope.Goal : Prop := indexedScope model
def Flatten.Goal : Prop := flatten model
def ShadowCut.Goal : Prop := shadowCut model
def CoalesceId.Goal : Prop := coalesceId model
def KeyOnlyCache.Goal : Prop := keyOnlyCache model
def WithinTier.Goal : Prop := withinTier model
def IncrementalInvalidate.Goal : Prop := incrementalInvalidate model

def NarrowOwner.Goal : Prop := ∀ (q e : Name) s, model.accept {q with owner := s} e → model.accept {q with owner := ""} e

def WrongQualifier.Goal : Prop := ∀ q e, q.owner ≠ "" → q.owner ≠ e.owner → model.accept q e = false

end WildcardOwner

namespace FieldNumber

/-- Struct field number, scoped to a receiver type; names may be reused. -/
def model : Model :=
  ⟨fun q e => q.kind == e.kind && q.pos == e.pos,
   fun n => toString n.pos⟩

def Posting.Goal : Prop := posting model
def IndexedScope.Goal : Prop := indexedScope model
def Flatten.Goal : Prop := flatten model
def ShadowCut.Goal : Prop := shadowCut model
def CoalesceId.Goal : Prop := coalesceId model
def KeyOnlyCache.Goal : Prop := keyOnlyCache model
def WithinTier.Goal : Prop := withinTier model
def IncrementalInvalidate.Goal : Prop := incrementalInvalidate model

def RenameField.Goal : Prop := ∀ q e s, model.accept q {e with text := s} = model.accept q e

def ReceiverType.Goal : Prop := ∀ q e, q.kind ≠ e.kind → model.accept q e = false

end FieldNumber

namespace CompatibleType

/-- Type zero is an unconstrained parameter; keying by exact type is speculative. -/
def model : Model :=
  ⟨fun q e => q.text == e.text && (q.kind == 0 || q.kind == e.kind),
   fun n => toString n.kind⟩

def Posting.Goal : Prop := posting model
def IndexedScope.Goal : Prop := indexedScope model
def Flatten.Goal : Prop := flatten model
def ShadowCut.Goal : Prop := shadowCut model
def CoalesceId.Goal : Prop := coalesceId model
def KeyOnlyCache.Goal : Prop := keyOnlyCache model
def WithinTier.Goal : Prop := withinTier model
def IncrementalInvalidate.Goal : Prop := incrementalInvalidate model

def Unconstrained.Goal : Prop := ∀ q e, q.kind=0 → q.text=e.text → model.accept q e

def ConcreteType.Goal : Prop := ∀ q e, 0 < q.kind → model.accept q e → q.kind = e.kind

end CompatibleType

namespace VisiblePrefix

/-- Proposed prefix lookup for expanding a projected field family. -/
def model : Model :=
  ⟨fun q e => e.text.startsWith q.text && visible e,
   fun n => (n.text.take 1).toString⟩

def Posting.Goal : Prop := posting model
def IndexedScope.Goal : Prop := indexedScope model
def Flatten.Goal : Prop := flatten model
def ShadowCut.Goal : Prop := shadowCut model
def CoalesceId.Goal : Prop := coalesceId model
def KeyOnlyCache.Goal : Prop := keyOnlyCache model
def WithinTier.Goal : Prop := withinTier model
def IncrementalInvalidate.Goal : Prop := incrementalInvalidate model

def EmptyPrefix.Goal : Prop := ∀ e, visible e → model.accept {} e

def ExactIncluded.Goal : Prop := ∀ q e, q.text=e.text → visible e → model.accept q e

end VisiblePrefix

private def q : Name := {text := "x"}
private def e : Entry := {text := "x", kind := 7, id := 9}
#guard lookup CompatibleType.model q [[e]] == .found 9
#guard lookup CompatibleType.model q (posted CompatibleType.model q [[e]]) == .missing

end TinylambOpt.ResearchGoals.LayerContracts.ScopeLookup
