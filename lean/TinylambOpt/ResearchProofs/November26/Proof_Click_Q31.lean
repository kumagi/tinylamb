import TinylambOpt.ResearchGoals.November26.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.November26.Click.Goal_Q31
open TinylambOpt.ResearchGoals.November26.Click
open TinylambOpt.ResearchGoals.November26

theorem divmod256_eq (a b : Nat) :
    (a / 256, a % 256) = (b / 256, b % 256) ↔ a = b := by
  constructor
  · intro h
    have h1 : a / 256 = b / 256 := by simpa using congrArg Prod.fst h
    have h2 : a % 256 = b % 256 := by simpa using congrArg Prod.snd h
    rw [← Nat.div_add_mod a 256, ← Nat.div_add_mod b 256, h1, h2]
  · intro h
    rw [h]

theorem key_eq (e a engine ip : Nat) :
    ((e, a / 256, a % 256) == (engine, ip / 256, ip % 256)) = ((e, a) == (engine, ip)) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have heq : (e, a / 256, a % 256) = (engine, ip / 256, ip % 256) := beq_iff_eq.mp h
    have h1 : e = engine := by simpa using congrArg Prod.fst heq
    have h2 : (a / 256, a % 256) = (ip / 256, ip % 256) := by simpa using congrArg Prod.snd heq
    exact beq_iff_eq.mpr (Prod.ext h1 ((divmod256_eq a ip).mp h2))
  · intro h
    have heq : (e, a) = (engine, ip) := beq_iff_eq.mp h
    have h1 : e = engine := congrArg Prod.fst heq
    have h2 : a = ip := congrArg Prod.snd heq
    exact beq_iff_eq.mpr (Prod.ext h1 ((divmod256_eq a ip).mpr h2))

theorem sound : TinylambOpt.ResearchGoals.November26.Click.Q31.Goal := by
  intro rows engine ip
  induction rows with
  | nil => simp [forKey]
  | cons row rows ih =>
      rcases row with ⟨⟨e, a⟩, v⟩
      dsimp [forKey]
      simp only [List.map_cons]
      by_cases h : ((e, a / 256, a % 256) == (engine, ip / 256, ip % 256))
      · have hp : ((e, a) == (engine, ip)) = true := by
          rw [← key_eq e a engine ip]
          exact h
        simp [h, hp]
        simpa [forKey] using ih
      · have hp : ¬ ((e, a) == (engine, ip)) = true := by
          intro hp
          apply h
          rw [key_eq e a engine ip]
          exact hp
        simp [h, hp]
        simpa [forKey] using ih
end TinylambOpt.ResearchProofs.November26.Click.Goal_Q31
