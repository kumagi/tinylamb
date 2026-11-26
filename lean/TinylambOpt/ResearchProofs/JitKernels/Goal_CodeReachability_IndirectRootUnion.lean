import TinylambOpt.ResearchGoals.JitKernels.CodeReachability
import TinylambOpt.ResearchProofs.JitKernels.Common_CodeReachability
set_option linter.unusedSimpArgs false

/-! Marking worker stacks and parked continuations separately and taking the
union equals one traversal from their concatenation: growth distributes over
unions of live key sets. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_IndirectRootUnion

open TinylambOpt.ResearchGoals.JitKernels.CodeReachability
open TinylambOpt.ResearchProofs.JitKernels.Common_CodeReachability

theorem sound : IndirectRootUnion.Goal := by
  intro heap a b n
  induction n with
  | zero =>
      intro k
      change k ∈ (a ++ b).eraseDups ↔ k ∈ a.eraseDups ∨ k ∈ b.eraseDups
      simp
  | succ n ih =>
      intro k
      change k ∈ grow heap (mark heap (a ++ b) n) ↔
        k ∈ grow heap (mark heap a n) ∨ k ∈ grow heap (mark heap b n)
      simp only [mem_grow]
      constructor
      · intro hx
        rcases hx with hx | ⟨x, hx, hxk⟩
        · rcases (ih k).mp hx with hka | hkb
          · exact Or.inl (Or.inl hka)
          · exact Or.inr (Or.inl hkb)
        · rcases (ih x).mp hx with hxa | hxb
          · exact Or.inl (Or.inr ⟨x, hxa, hxk⟩)
          · exact Or.inr (Or.inr ⟨x, hxb, hxk⟩)
      · intro hx
        rcases hx with ((hka | ⟨x, hxa, hxk⟩) | (hkb | ⟨x, hxb, hxk⟩))
        · exact Or.inl ((ih k).mpr (Or.inl hka))
        · exact Or.inr ⟨x, (ih x).mpr (Or.inl hxa), hxk⟩
        · exact Or.inl ((ih k).mpr (Or.inr hkb))
        · exact Or.inr ⟨x, (ih x).mpr (Or.inr hxb), hxk⟩

end TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_IndirectRootUnion
