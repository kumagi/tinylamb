import TinylambOpt.ResearchGoals.QueryRewrites.Common
import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.AggregateReplay.Goal_RepartitionPartialConservation

open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay


/-! Scratch proof: merging two partial records with a shared group key is
    replay-equivalent, because the two states are combined associatively and
    records with a different key are ignored on both sides. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

/-- Pair addition is associative. -/
theorem combine_assoc (x y z : State) : combine (combine x y) z = combine x (combine y z) := by
  rcases x with ⟨sx, cx⟩
  rcases y with ⟨sy, cy⟩
  rcases z with ⟨sz, cz⟩
  simp [combine, Int.add_assoc, Nat.add_assoc]

/-- Zero is a left identity. -/
theorem combine_zero (s : State) : combine (0, 0) s = s := by
  rcases s with ⟨ss, cs⟩
  simp [combine]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay.RepartitionPartialConservation.Goal := by
  intro a b tail k hkey
  let merged : Record := { a with partialState := combine a.partialState b.partialState }
  by_cases hk : a.key == k
  · have hbk : b.key == k := by simpa [hkey] using hk
    have hmk : merged.key == k := by simpa [merged] using hk
    simp only [replay, List.filter_cons, hk, hbk]
    simp [combine_zero]
  · have hbk : ¬ b.key == k := by simpa [hkey] using hk
    simp [replay, hk, hbk]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.AggregateReplay.Goal_RepartitionPartialConservation
