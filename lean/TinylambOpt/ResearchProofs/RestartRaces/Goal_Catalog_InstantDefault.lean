import TinylambOpt.ResearchGoals.RestartRaces.Catalog
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.Catalog_InstantDefault
open TinylambOpt.ResearchGoals.RestartRaces.Catalog.InstantDefault

theorem sound : Goal := by
  classical
  intro rows visible defs k
  constructor
  · rintro ⟨slot, decode, h⟩
    refine ⟨slot, ?_, ?_⟩
    · intro e s he
      exact (h e s he).1
    · intro e f s he hf hne heq
      have hde := (h e s he).2
      have hdf := (h f s hf).2
      rw [heq] at hde
      exact hne (hde.symm.trans hdf)
  · rintro ⟨color, hbound, hsep⟩
    let decode : Nat → Nat → Val := fun s c =>
      if h : ∃ e, latent rows visible e s ∧ color e = c then defs s (Classical.choose h) else none
    refine ⟨color, decode, ?_⟩
    intro e s he
    constructor
    · exact hbound e s he
    · dsimp [decode]
      have hex : ∃ e', latent rows visible e' s ∧ color e' = color e := ⟨e, he, rfl⟩
      rw [dif_pos hex]
      let e' := Classical.choose hex
      have he' : latent rows visible e' s := (Classical.choose_spec hex).1
      have hcol : color e' = color e := (Classical.choose_spec hex).2
      by_cases hval : defs s e' = defs s e
      · exact hval
      · exfalso
        exact (hsep e' e s he' he hval) hcol
