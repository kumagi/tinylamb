import Std

/-! Evicting process-lifetime compiled kernels would require a model of active frames, parked continuations and embedded direct calls. Immutable code objects use generation-qualified identities. The graph is finite and may contain recursive edges. -/
namespace TinylambOpt.ResearchGoals.JitKernels.CodeReachability

abbrev Key := Nat × Nat
structure Obj where
  key : Key
  calls : List Key
  bytes : Nat
  deriving DecidableEq, Repr
def edges (heap : List Obj) (k : Key) := ((heap.find? (fun o => o.key == k)).map Obj.calls).getD []
def grow (heap : List Obj) (live : List Key) := (live ++ live.flatMap (edges heap)).eraseDups
def mark (heap : List Obj) (roots : List Key) (fuel : Nat) : List Key := Nat.rec roots.eraseDups (fun _ prev => grow heap prev) fuel
def retain (heap : List Obj) (live : List Key) := heap.filter (fun o => live.contains o.key)

/-- One edge expansion per resident object suffices to reach a fixed point from arbitrary frame and cache roots. -/
def BoundedCodeClosure.Goal : Prop := ∀ heap roots, mark heap roots (heap.length+1) = mark heap roots heap.length

/-- Once active-frame count for a body hits zero, that body cannot be reached by a compiled call from another live body. -/
def ActiveFramesAsOnlyRoots.Goal : Prop := ∀ heap roots k, k ∉ roots → k ∉ mark heap roots heap.length

/-- Marking live code before reclaiming thunks preserves every direct-call table reachable from roots. -/
def EvictionPreservesDirectTargets.Goal : Prop := ∀ heap roots, let live := mark heap roots heap.length
  ∀ k ∈ live, edges (retain heap live) k = edges heap k

/-- Mark worker stacks and parked continuations independently and take their union to avoid a global root traversal. -/
def IndirectRootUnion.Goal : Prop := ∀ heap a b n k, k ∈ mark heap (a++b) n ↔ k ∈ mark heap a n ∨ k ∈ mark heap b n

/-- Keeping only code objects that appear as call targets reproduces tracing from external entry points. -/
def UnrootedRecursionRefcount.Goal : Prop := ∀ (heap : List Obj) roots (o : Obj), o ∈ heap → (o.key ∈ mark heap roots heap.length ↔ o.key ∈ roots ∨ o.key ∈ heap.flatMap Obj.calls)

/-- A direct call compiled against an old generation can use the latest generation in that address slot without relinking dependencies. -/
def ReplaceBodyBySameSlot.Goal : Prop := ∀ heap slot g h, g < h → (slot,g) ∈ heap.map Obj.key → (slot,h) ∈ heap.map Obj.key →
  edges heap (slot,g) = edges heap (slot,h)

/-- A second collection after roots have shrunk can use the already trimmed compilation cache. -/
def SnapshotEvictionThenRootShrinks.Goal : Prop := ∀ heap a b, (∀ k ∈ b, k ∈ a) → let kept := retain heap (mark heap a heap.length)
  retain kept (mark kept b kept.length) = retain heap (mark heap b heap.length)

/-- Charge one additional maximum-size callee per root when every compiled body embeds at most one direct call. -/
def StubOnlySpaceBudget.Goal : Prop := ∀ (heap : List Obj) roots, (∀ o ∈ heap, o.calls.length ≤ 1) → (heap.map Obj.key).Nodup →
  ((retain heap (mark heap roots heap.length)).map Obj.bytes).sum ≤
    ((retain heap roots).map Obj.bytes).sum + roots.length*((heap.map Obj.bytes).max?.getD 0)

/-- Replacing a direct branch with a generic stub can only shrink reachable code if no new outgoing target is added. -/
def PatchOneJumpUpdatesMarkSet.Goal : Prop := ∀ a b roots, (∀ k, ∀ x ∈ edges b k, x ∈ edges a k) → ∀ n k, k ∈ mark b roots n → k ∈ mark a roots n

/-- Relinking every immutable direct call under an injective code-id relocation preserves all reachable targets. -/
def IdentityPreservingRelocation.Goal : Prop := ∀ heap roots (f : Key → Key), (∀ a b, f a = f b → a = b) → ∀ n k,
  f k ∈ mark (heap.map (fun o => {o with key := f o.key, calls := o.calls.map f})) (roots.map f) n ↔ k ∈ mark heap roots n

end TinylambOpt.ResearchGoals.JitKernels.CodeReachability
