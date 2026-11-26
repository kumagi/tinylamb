import TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Dominance

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Proved

- `LastBlue.PlainDepth` — Aggressive recursion cap: one round per graph vertex even
  with path constraints.
-/

set_option maxHeartbeats 0
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_LastBlue_PlainDepth

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Generic

theorem sound : LastBlue.PlainDepth.Goal := by
  intro g v n x hx
  rw [query_mem] at hx ⊢
  obtain ⟨q, hq, hacc⟩ := hx
  obtain ⟨t, ht, hr⟩ := (It_iff_Rt LastBlue.model g _ n _).mp hq
  cases t with
  | zero =>
    -- (x,q) = (v,0) is not accepting
    have h0 : (x, q) = (v, 0) := hr
    have : q = 0 := (Prod.mk.inj h0).2
    subst this
    simp [LastBlue.model] at hacc
  | succ t =>
    obtain ⟨pm, hrm, e, he, h1, h2, h3⟩ := hr
    obtain ⟨t', ht', pm', hpm', hpm1⟩ :=
      reach_shorten LastBlue.model g v (fun _ => 0) (by intro q x r h; exact Nat.le_refl _)
        (by
          intro q q' x r' _ h
          simp only [LastBlue.model] at h ⊢
          exact ⟨_, rfl, Nat.le_refl _⟩) t pm hrm
    refine ⟨q, (It_iff_Rt LastBlue.model g _ _ _).mpr ⟨t' + 1, by omega, pm', hpm', e, he, ?_, ?_, h3⟩, hacc⟩
    · rw [hpm1]; exact h1
    · simp only [LastBlue.model] at h2 ⊢
      exact h2

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_LastBlue_PlainDepth
