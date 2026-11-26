import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_B3DuckDB

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics.B3DuckDB.Goal := by
  intro r c y r' c' y' cats hc hc' hy hy'
  constructor
  · intro h
    have hm := congrArg (fun n : Nat => n % 6) h
    simp [Nat.add_mod, Nat.mod_eq_of_lt hy, Nat.mod_eq_of_lt hy'] at hm
    subst y'
    have hmul : (r * cats + c) * 6 = (r' * cats + c') * 6 := by omega
    have hlin : r * cats + c = r' * cats + c' := Nat.mul_right_cancel (by decide : 0 < 6) hmul
    have hcats : 0 < cats := by omega
    have hm2 := congrArg (fun n : Nat => n % cats) hlin
    simp [Nat.add_mod, Nat.mod_eq_of_lt hc, Nat.mod_eq_of_lt hc'] at hm2
    have hmul2 : r * cats = r' * cats := by omega
    have hr : r = r' := Nat.mul_right_cancel hcats hmul2
    subst r'
    subst c'
    rfl
  · intro h
    cases h
    rfl

end TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_B3DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_B3DuckDB.sound
