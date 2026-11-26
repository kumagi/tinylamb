import TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

/-! Compile null handling and control flow against actual lane demand. Inputs are nullable, fallible slots of the existing demand-SSA interpreter. Partial specialization replaces an argument by a literal at compile time. -/
namespace TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
def specialize (i : Nat) (v : V) : E → E
  | .arg j => if j == i then .lit v else .arg j
  | .lit w => .lit w | .neg a => .neg (specialize i v a)
  | .isTrue a => .isTrue (specialize i v a) | .missing a => .missing (specialize i v a)
  | .conj a b => .conj (specialize i v a) (specialize i v b)
  | .disj a b => .disj (specialize i v a) (specialize i v b)
  | .co a b => .co (specialize i v a) (specialize i v b)
  | .branch p a b => .branch (specialize i v p) (specialize i v a) (specialize i v b)
def reads (env : List V) (e : E) := (run env e).2
def answer (env : List V) (e : E) := (run env e).1

/-- A literal can replace an argument on rows that either match its guard or never actually demand that argument. -/
def DemandScopedArgumentGuard.Goal : Prop := ∀ env e i v, (i ∈ reads env e → slot env i = v) → answer env (specialize i v e) = answer env e

/-- A row whose final value is NULL needs no specialization guards for arguments supplied on the fast path. -/
def FinalNullImpliesUnusedArguments.Goal : Prop := ∀ env e i v, answer env e = .null → answer env (specialize i v e) = .null

/-- On matching invocations the compiled expression skips exactly the reads of the specialized slot, including faulting reads. -/
def SpecializedReadFootprint.Goal : Prop := ∀ env e i v, slot env i = v → reads env (specialize i v e) = (reads env e).filter (· != i)

/-- Identical case arms allow the condition and its error checks to be removed. -/
def ConstantArmResultPrunesGuardFault.Goal : Prop := ∀ env p arm, answer env (.branch p arm arm) = answer env arm

/-- A slot demanded by both branches can be hoisted before evaluating the predicate without changing the first observable fault. -/
def BranchReadUnionForHoisting.Goal : Prop := ∀ env p a b i err, i ∈ reads env a → i ∈ reads env b → slot env i = .fault err →
  answer env (.branch p a b) = .fault err

/-- Specializing one matching slot before checking a second guard lets the compiler use the new trace as its demand certificate. -/
def DeoptGuardOrderAfterSubstitution.Goal : Prop := ∀ env e i j x y, slot env i = x →
  (j ∈ reads env (specialize i x e) → slot env j = y) →
  answer env (specialize j y (specialize i x e)) = answer env e

/-- Replay the last row's SQL result when all slots it actually read still have the same values on this row. -/
def DictionaryByTraceValue.Goal : Prop := ∀ prev fresh e, (∀ i ∈ reads prev e, slot prev i = slot fresh i) →
  run fresh e = run prev e

/-- The set of demanded slots alone suffices for memoizing a compiled nullable predicate across rows with identical control flow. -/
def TraceSetAsResultCacheKey.Goal : Prop := ∀ a b e, (reads a e).eraseDups = (reads b e).eraseDups → answer a e = answer b e

/-- Specializations that preserve WHERE acceptance may also be used under NOT in another expression. -/
def FilterAcceptanceForNestedNegation.Goal : Prop := ∀ env x y, (answer env x == .yes) = (answer env y == .yes) →
  (answer env (.neg x) == .yes) = (answer env (.neg y) == .yes)

/-- Distinct matching literals can be substituted in any order without changing read order at a side exit. -/
def PermutedLiteralGuards.Goal : Prop := ∀ env e i j x y, i ≠ j → run env (specialize i x (specialize j y e)) = run env (specialize j y (specialize i x e))

end TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
