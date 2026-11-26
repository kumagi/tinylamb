import Std

/-! Guarded code versions specialize on sets of 64-bit feature facts. A mask/value equality describes each legal invocation, including negative facts. Dispatch uses ordered first match and an explicit generic miss. -/
namespace TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

abbrev Word := BitVec 64
structure Entry where
  mask : Word
  value : Word
  code : Nat
  deriving DecidableEq, Repr
def accepts (e : Entry) (w : Word) := (w &&& e.mask) == (e.value &&& e.mask)
def dispatch (es : List Entry) (w : Word) := (es.find? (accepts · w)).map Entry.code
def clash (a b : Entry) := ((a.value ^^^ b.value) &&& a.mask &&& b.mask) != 0
def weaker (a b : Entry) := (a.mask &&& b.mask) == a.mask && !clash a b

/-- Use mask containment and common required bits to remove a dominated second guard. -/
def BitGuardImpliesWeaker.Goal : Prop := ∀ a b, weaker a b = true ↔ ∀ w, accepts b w → accepts a w

/-- Determine statically whether two compiled versions can ever both be selected. -/
def CommonBitsDetectDisjointness.Goal : Prop := ∀ a b, clash a b = true ↔ ¬ ∃ w, accepts a w ∧ accepts b w

/-- Sort the code cache by decreasing number of guarded features while preserving chosen code identities. -/
def SpecificitySortDispatch.Goal : Prop := ∀ a b w, weaker a b → dispatch [a,b] w = dispatch [b,a] w

/-- Bits outside the mask need not appear in a specialization cache key. -/
def CanonicalGuardCodeKey.Goal : Prop := ∀ e w, accepts {e with value := e.value &&& e.mask} w = accepts e w

/-- Two guards differing only in one masked bit and sharing a body may be coalesced by dropping that bit. -/
def FoldNeighborVersions.Goal : Prop := ∀ a b (i : Fin 64) w, a.mask = b.mask → a.code = b.code → a.mask.getLsbD i →
  (a.value ^^^ b.value) = ((1 : Word) <<< i.val) → dispatch [a,b] w =
    dispatch [{a with mask := a.mask &&& ~~~((1 : Word) <<< i.val)}] w

/-- Combine arbitrary equal-body versions by testing only the bits on which their guards agree. -/
def CommonRequirementsCoverUnion.Goal : Prop := ∀ a b w, a.mask = b.mask → a.code = b.code → dispatch [a,b] w =
    dispatch [{a with mask := a.mask &&& ~~~(a.value ^^^ b.value)}] w

/-- Remove a covered entry from any later position in an ordered dispatch chain. -/
def ShadowChainPruning.Goal : Prop := ∀ a b rest tail w, weaker a b → dispatch (a::rest++b::tail) w = dispatch (a::rest++tail) w

/-- A cached last-hit entry needs only its own guard rechecked to bypass a higher-priority dispatch chain on the next call. -/
def ReuseLastPassingEntry.Goal : Prop := ∀ es old fresh e, es.find? (accepts · old) = some e → accepts e fresh → dispatch es fresh = some e.code

/-- Hoist a dispatch outside a batch when all rows agree on the union of every tested bit. -/
def PrecomputeGuardOnUniformRows.Goal : Prop := ∀ (es : List Entry) (a b : Word), let m := (es.map Entry.mask).foldl (· ||| ·) 0
  a &&& m = b &&& m → dispatch es a = dispatch es b

/-- Cache one branch target per unordered matching set when every overlapping pair names the same code. -/
def DistinctBodiesMutualExclusion.Goal : Prop := ∀ (a b : List Entry) w, a.Perm b → (∀ e ∈ a, ∀ f ∈ a, !clash e f → e.code = f.code) →
  dispatch a w = dispatch b w

end TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
