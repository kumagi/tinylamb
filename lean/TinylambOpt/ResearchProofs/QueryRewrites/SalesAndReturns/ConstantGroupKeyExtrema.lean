import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# ConstantGroupKeyExtrema

A nonempty group's minimum and maximum dependent count are both the key's dependent
count when that value is constant within the group. The executor can omit these
aggregate states for such groups.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ConstantGroupKeyExtrema

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.ConstantGroupKeyExtrema.Goal := by
  intro v n hn
  cases v with
  | none => simp
  | some x => simp [hn]

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ConstantGroupKeyExtrema

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ConstantGroupKeyExtrema.sound
