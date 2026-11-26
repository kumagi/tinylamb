import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# IndirectRootUnion (proved)

Scanning worker stacks and parked continuations as two root lists is equivalent
to scanning their concatenation: for every fuel,
`k ∈ mark heap (a ++ b) n ↔ k ∈ mark heap a n ∨ k ∈ mark heap b n`.
This justifies parallel root processing with a set union instead of a global
root traversal, since one shared grow pass over the combined roots gives exactly
the union of the individual marks.

Proof: `grow` satisfies `k ∈ grow h l ↔ k ∈ l ∨ ∃ x ∈ l, k ∈ edges h x`
(membership through `eraseDups`/`append`/`flatMap`).  Induction on the fuel: at
fuel `0` the claim is membership in `(a ++ b).eraseDups`, i.e. `a ++ b`, and the
step follows by pushing the disjunction through the two cases (the already-marked
case via the induction hypothesis, the edge case by transporting the witness). -/

namespace TinylambOpt.ResearchProofs.JitKernels.CodeReachability_IndirectRootUnion
open TinylambOpt.ResearchGoals.JitKernels.CodeReachability

private theorem grow_mem (heap : List Obj) (live : List Key) (k : Key) :
    k ∈ grow heap live ↔ k ∈ live ∨ ∃ x ∈ live, k ∈ edges heap x := by
  show k ∈ (live ++ live.flatMap (edges heap)).eraseDups ↔ _
  rw [List.mem_eraseDups, List.mem_append, List.mem_flatMap]

theorem sound : IndirectRootUnion.Goal := by
  intro heap a b n
  induction n with
  | zero =>
      intro k
      show k ∈ (a ++ b).eraseDups ↔ k ∈ a.eraseDups ∨ k ∈ b.eraseDups
      rw [List.mem_eraseDups, List.mem_eraseDups, List.mem_eraseDups, List.mem_append]
  | succ n ih =>
      intro k
      show k ∈ grow heap (mark heap (a ++ b) n)
        ↔ k ∈ grow heap (mark heap a n) ∨ k ∈ grow heap (mark heap b n)
      rw [grow_mem, grow_mem, grow_mem]
      constructor
      · intro h
        cases h with
        | inl h =>
            cases (ih k).mp h with
            | inl ha => exact Or.inl (Or.inl ha)
            | inr hb => exact Or.inr (Or.inl hb)
        | inr h =>
            obtain ⟨x, hx, he⟩ := h
            cases (ih x).mp hx with
            | inl ha => exact Or.inl (Or.inr ⟨x, ha, he⟩)
            | inr hb => exact Or.inr (Or.inr ⟨x, hb, he⟩)
      · intro h
        cases h with
        | inl h =>
            cases h with
            | inl ha => exact Or.inl ((ih k).mpr (Or.inl ha))
            | inr hbe =>
                obtain ⟨x, hx, he⟩ := hbe
                exact Or.inr ⟨x, (ih x).mpr (Or.inl hx), he⟩
        | inr h =>
            cases h with
            | inl hb => exact Or.inl ((ih k).mpr (Or.inr hb))
            | inr hbe =>
                obtain ⟨x, hx, he⟩ := hbe
                exact Or.inr ⟨x, (ih x).mpr (Or.inr hx), he⟩

end TinylambOpt.ResearchProofs.JitKernels.CodeReachability_IndirectRootUnion
