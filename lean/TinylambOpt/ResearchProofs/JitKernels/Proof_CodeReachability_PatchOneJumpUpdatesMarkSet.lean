import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# PatchOneJumpUpdatesMarkSet (proved)

Replacing a direct branch with a generic stub removes outgoing targets but adds
none.  Modeled as heap `b` whose every edge set is included in heap `a`'s
(`∀ k x ∈ edges b k, x ∈ edges a k`), the claim says marking never grows:
`k ∈ mark b roots n → k ∈ mark a roots n` for every fuel, so the sweeper may
conservatively keep the pre-patch live set.

Proof: induction on the fuel.  Fuel `0` is membership in `roots.eraseDups`,
independent of the heap.  At a `n+1` step, `grow`'s membership characterization
splits a newly marked key into "already marked at fuel `n`" (induction
hypothesis) or "an edge target of a key marked at fuel `n`" (induction
hypothesis on the witness plus the edge inclusion). -/

namespace TinylambOpt.ResearchProofs.JitKernels.CodeReachability_PatchOneJumpUpdatesMarkSet
open TinylambOpt.ResearchGoals.JitKernels.CodeReachability

private theorem mark_succ (heap : List Obj) (roots : List Key) (n : Nat) :
    mark heap roots (n+1) = grow heap (mark heap roots n) := rfl

private theorem grow_mem (heap : List Obj) (live : List Key) (k : Key) :
    k ∈ grow heap live ↔ k ∈ live ∨ ∃ x ∈ live, k ∈ edges heap x := by
  show k ∈ (live ++ live.flatMap (edges heap)).eraseDups ↔ _
  rw [List.mem_eraseDups, List.mem_append, List.mem_flatMap]

theorem sound : PatchOneJumpUpdatesMarkSet.Goal := by
  intro a b roots hsub n
  induction n with
  | zero =>
      intro k hk
      exact hk
  | succ n ih =>
      intro k hk
      have hkg : k ∈ grow b (mark b roots n) := hk
      rw [grow_mem] at hkg
      rw [mark_succ, grow_mem]
      cases hkg with
      | inl h => exact Or.inl (ih k h)
      | inr h =>
          obtain ⟨x, hx, he⟩ := h
          exact Or.inr ⟨x, ih x hx, hsub x k he⟩

end TinylambOpt.ResearchProofs.JitKernels.CodeReachability_PatchOneJumpUpdatesMarkSet
