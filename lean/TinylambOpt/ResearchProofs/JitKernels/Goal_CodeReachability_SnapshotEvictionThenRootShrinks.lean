import TinylambOpt.ResearchGoals.JitKernels.CodeReachability
import TinylambOpt.ResearchProofs.JitKernels.Common_CodeReachability
import TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_BoundedCodeClosure

/-! A second collection after the roots shrink can reuse the already trimmed
cache: the objects reached from the smaller root set are exactly those that
would be reached from it in the original heap. Reclamation therefore preserves
the SQL-visible direct-call targets. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_SnapshotEvictionThenRootShrinks

open TinylambOpt.ResearchGoals.JitKernels.CodeReachability
open TinylambOpt.ResearchProofs.JitKernels.Common_CodeReachability

theorem sound : SnapshotEvictionThenRootShrinks.Goal := by
  intro heap a b hsub
  let liveA := mark heap a heap.length
  let kept := retain heap liveA
  have hfixA : mark heap a (heap.length + 1) = mark heap a heap.length :=
    Goal_CodeReachability_BoundedCodeClosure.sound heap a
  have hfixB : mark heap b (heap.length + 1) = mark heap b heap.length :=
    Goal_CodeReachability_BoundedCodeClosure.sound heap b
  have hclosedA : ∀ k ∈ liveA, ∀ x ∈ edges heap k, x ∈ liveA := by
    dsimp only [liveA]
    exact mark_closed_of_fixed heap a hfixA
  have hclosedB : ∀ k ∈ mark heap b heap.length, ∀ x ∈ edges heap k,
      x ∈ mark heap b heap.length :=
    mark_closed_of_fixed heap b hfixB
  have hrootsB_A : ∀ k ∈ b, k ∈ liveA := by
    intro k hk
    have hkB : k ∈ mark heap b heap.length := mem_mark_of_mem_roots heap b k hk heap.length
    dsimp only [liveA]
    exact mark_mono_roots heap hsub heap.length k hkB
  have hclosedA_kept : ∀ k ∈ liveA, ∀ x ∈ edges kept k, x ∈ liveA := by
    intro k hk x hxk
    change x ∈ edges (retain heap liveA) k at hxk
    rw [edges_retain heap liveA k hk] at hxk
    exact hclosedA k hk x hxk
  have hkept_in_A : ∀ n k, k ∈ mark kept b n → k ∈ liveA :=
    mark_minimal kept b liveA hclosedA_kept hrootsB_A
  have hfixKept : mark kept b (kept.length + 1) = mark kept b kept.length :=
    Goal_CodeReachability_BoundedCodeClosure.sound kept b
  have hclosedKept : ∀ k ∈ mark kept b kept.length, ∀ x ∈ edges kept k,
      x ∈ mark kept b kept.length :=
    mark_closed_of_fixed kept b hfixKept
  have hrootsB_kept : ∀ k ∈ b, k ∈ mark kept b kept.length := by
    intro k hk
    exact mem_mark_of_mem_roots kept b k hk kept.length
  have hclosedKept_heap : ∀ k ∈ mark kept b kept.length, ∀ x ∈ edges heap k,
      x ∈ mark kept b kept.length := by
    intro k hk x hxk
    have hkA : k ∈ liveA := hkept_in_A kept.length k hk
    have hxk' : x ∈ edges kept k := by
      change x ∈ edges (retain heap liveA) k
      rw [edges_retain heap liveA k hkA]
      exact hxk
    exact hclosedKept k hk x hxk'
  have hheap_in_kept : ∀ n k, k ∈ mark heap b n → k ∈ mark kept b kept.length :=
    mark_minimal heap b (mark kept b kept.length) hclosedKept_heap hrootsB_kept
  have hB_in_A : ∀ k ∈ mark heap b heap.length, k ∈ liveA := by
    intro k hk
    dsimp only [liveA]
    exact mark_mono_roots heap hsub heap.length k hk
  have hclosedB_kept : ∀ k ∈ mark heap b heap.length, ∀ x ∈ edges kept k,
      x ∈ mark heap b heap.length := by
    intro k hk x hxk
    have hkA : k ∈ liveA := hB_in_A k hk
    have hxk' : x ∈ edges heap k := by
      change x ∈ edges (retain heap liveA) k at hxk
      rw [edges_retain heap liveA k hkA] at hxk
      exact hxk
    exact hclosedB k hk x hxk'
  have hrootsB_heap : ∀ k ∈ b, k ∈ mark heap b heap.length := by
    intro k hk
    exact mem_mark_of_mem_roots heap b k hk heap.length
  have hkept_in_heap : ∀ n k, k ∈ mark kept b n → k ∈ mark heap b heap.length :=
    mark_minimal kept b (mark heap b heap.length) hclosedB_kept hrootsB_heap
  have hmem : ∀ k, k ∈ mark kept b kept.length ↔ k ∈ mark heap b heap.length := by
    intro k
    constructor
    · exact hkept_in_heap kept.length k
    · exact hheap_in_kept heap.length k
  calc
    retain kept (mark kept b kept.length) =
        retain kept (mark heap b heap.length) :=
      retain_eq_of_mem_iff kept (mark kept b kept.length) (mark heap b heap.length) hmem
    _ = retain heap (mark heap b heap.length) := by
      dsimp only [kept]
      exact retain_retain heap liveA (mark heap b heap.length) hB_in_A

end TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_SnapshotEvictionThenRootShrinks
