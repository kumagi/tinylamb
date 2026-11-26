import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_BothTypes_StateDomain

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

theorem slow_bound {m : Model}
    (h0 : 0 < m.states)
    (hnext : ∀ q l r, q < m.states → m.next q l = some r → r < m.states) :
    ∀ (g : List Edge) (v n w q : Nat), (w,q) ∈ slow m g (v,0) n → q < m.states := by
  intro g v n
  induction n with
  | zero =>
      intro w q h
      simp [slow] at h
      exact h.2 ▸ h0
  | succ n ih =>
      intro w q h
      simp [slow] at h
      rcases h with hprev | hex
      · exact ih w q hprev
      · simp [expand] at hex
        rcases hex with ⟨a,b,hab,e,heg,hesrc,hnextq,hedst⟩
        exact hnext b e.label q (ih a b hab) hnextq

example : EvenHops.StateDomain.Goal := by
  apply slow_bound
  · decide
  · intro q l r hq h
    change q < 2 at hq
    change r < 2
    have h' : 1 - q = r := by simpa [EvenHops.model] using h
    omega

example : AtLeastOneRed.StateDomain.Goal := by
  apply slow_bound
  · decide
  · intro q l r hq h
    change q < 2 at hq
    change r < 2
    by_cases hl : l = 1 <;> simp [AtLeastOneRed.model, hl] at h <;> subst r <;> omega

example : Alternating.StateDomain.Goal := by
  apply slow_bound
  · decide
  · intro q l r hq h
    change q < 2 at hq
    change r < 2
    simp [Alternating.model] at h
    rcases h with ⟨_, hr⟩
    omega

example : SingleRed.StateDomain.Goal := by
  apply slow_bound
  · decide
  · intro q l r hq h
    change q < 2 at hq
    change r < 2
    by_cases hl : l = 1 <;> by_cases hq0 : q = 0 <;>
      simp [SingleRed.model, hl, hq0] at h <;>
      try (subst r; omega)

example : TollBudget.StateDomain.Goal := by
  apply slow_bound
  · decide
  · intro q l r hq h
    change q < 4 at hq
    change r < 4
    simp [TollBudget.model] at h
    rcases h with ⟨_, hr⟩
    omega

example : OrderedClass.StateDomain.Goal := by
  apply slow_bound
  · decide
  · intro q l r hq h
    change q < 3 at hq
    change r < 3
    simp [OrderedClass.model] at h
    rcases h with ⟨_, hr⟩
    omega

example : LastBlue.StateDomain.Goal := by
  apply slow_bound
  · decide
  · intro q l r hq h
    change q < 2 at hq
    change r < 2
    by_cases hl : l = 2 <;> simp [LastBlue.model, hl] at h <;> subst r <;> omega

example : PatternAB.StateDomain.Goal := by
  apply slow_bound
  · decide
  · intro q l r hq h
    change q < 3 at hq
    change r < 3
    by_cases h2 : q = 2 <;> by_cases h1q : q = 1 <;> by_cases h1l : l = 2 <;> by_cases h1l' : l = 1 <;>
      simp [PatternAB.model, h2, h1q, h1l, h1l'] at h <;> subst r <;> omega

example : ModuloCost.StateDomain.Goal := by
  apply slow_bound
  · decide
  · intro q l r hq h
    change q < 3 at hq
    change r < 3
    have hr : r = (q + l) % 3 := by simpa [ModuloCost.model] using h.symm
    rw [hr]
    exact Nat.mod_lt _ (by decide)

theorem sound : BothTypes.StateDomain.Goal := by
  apply slow_bound
  · decide
  · intro q l r hq h
    change q < 4 at hq
    change r < 4
    let m : Nat := if l == 1 then 1 else if l == 2 then 2 else 0
    have hm : m ≤ 2 := by
      dsimp [m]
      by_cases h1 : l == 1 <;> by_cases h2 : l == 2 <;> simp [h1, h2] <;> omega
    have h' : q ||| m = r := by simpa [BothTypes.model, m] using h
    have hm' : m < 4 := by omega
    have hb : q ||| m < 4 := by
      have hh := Nat.or_lt_two_pow (n := 2) hq hm'
      have hp : (2 : Nat) ^ 2 = 4 := by decide
      simpa [hp] using hh
    rw [← h']
    exact hb

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_BothTypes_StateDomain
