import Std

/-! Prepared projection bindings across schema churn. A name lookup uses the first
visible binding; positive and negative dependencies can both be recorded. Bound
column ids and physical ordinals differ when an earlier column is dropped. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies

structure Column where
  name : Nat
  id : Nat
  kind : Nat
  deriving DecidableEq, Repr
abbrev Schema := List Column
def bind (s : Schema) (name : Nat) := s.find? (fun c => c.name == name)
def signature (s : Schema) (names : List Nat) := names.map (bind s)
def project (s : Schema) (row : List Nat) (names : List Nat) :=
  names.map (fun name => (bind s name).bind (fun c => row[s.idxOf c]?))

/-- An appended column outside a query's positive and negative bindings permits prepared signature reuse. -/
def UnusedAppendSignatureReuse.Goal : Prop := ∀ s names c, ¬ c.name ∈ names →
  signature (s++[c]) names = signature s names

/-- Checking just names that resolved at prepare time is sufficient for caching a name-resolution failure. -/
def PositiveOnlyBinderCache.Goal : Prop := ∀ old fresh names,
  (∀ n ∈ names, (bind old n).isSome → bind fresh n = bind old n) →
  signature fresh names = signature old names

/-- Matching ids and types of projected columns suffice to reuse physical ordinals after an unrelated DDL. -/
def SignatureImpliesOrdinalReuse.Goal : Prop := ∀ old fresh names, signature old names = signature fresh names →
  ∀ n ∈ names, (bind old n).map old.idxOf = (bind fresh n).map fresh.idxOf

/-- Rebinding by stable column identity across a physical row permutation preserves projected values. -/
def ReboundProjectionPermutation.Goal : Prop := ∀ old fresh names (value : Column → Nat),
  signature old names = signature fresh names →
  project old (old.map value) names = project fresh (fresh.map value) names

/-- A wildcard projection can share a cache entry when the explicitly named predicate dependencies match. -/
def WildcardProjectionSignature.Goal : Prop := ∀ old fresh filters, signature old filters = signature fresh filters →
  old.map Column.id = fresh.map Column.id

/-- Dependency checks performed per query fragment compose without requiring global catalog equality. -/
def FragmentDependencyUnion.Goal : Prop := ∀ old fresh a b,
  signature old (a++b).eraseDups = signature fresh (a++b).eraseDups ↔
    signature old a = signature fresh a ∧ signature old b = signature fresh b

/-- Reordering shadowed bindings can change lookup even if the catalog contains exactly the same columns. -/
def ShadowedNamePermutationReuse.Goal : Prop := ∀ (old fresh : Schema) names,
  old.Perm fresh → signature old names = signature fresh names

/-- An ordinal-specialized executor can reuse its offsets when schema changes append a suffix and old bindings succeeded. -/
def AppendOnlyOrdinalProjection.Goal : Prop := ∀ schema suffix names (row added : List Nat),
  row.length = schema.length → (∀ n ∈ names, (bind schema n).isSome) →
  project (schema++suffix) (row++added) names = project schema row names

/-- Check once per distinct projected name, then reconstruct repeated references in their original positions. -/
def DistinctBindingChecks.Goal : Prop := ∀ schema names,
  let uniqueNames := names.eraseDups
  names.map (fun n => (signature schema uniqueNames)[uniqueNames.idxOf n]?.join) = signature schema names

#guard project [⟨1,10,0⟩,⟨2,20,0⟩] [31,42] [2,2,7] == [some 42,some 42,none]

end TinylambOpt.ResearchGoals.ResourcePolicies.CatalogDependencies
