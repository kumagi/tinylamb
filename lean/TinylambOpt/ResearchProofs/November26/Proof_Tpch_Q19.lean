import TinylambOpt.ResearchGoals.November26.Tpch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q19
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpch
open TinylambOpt.ResearchGoals.BenchmarkRematch


def bands : List (Nat × Nat) := [(5,15),(15,25),(29,39)]

theorem accepts_iff (air : Bool) (tag q : Nat) :
    (air && (bands[tag]?).any fun (l, h) => l ≤ q && q ≤ h) =
    (air && ((tag == 0 && 5 ≤ q && q ≤ 15) ||
      (tag == 1 && 15 ≤ q && q ≤ 25) || (tag == 2 && 29 ≤ q && q ≤ 39))) := by
  cases air <;> simp [bands]
  cases tag with
  | zero => simp
  | succ t =>
      cases t with
      | zero => simp
      | succ u =>
          cases u with
          | zero => simp
          | succ v =>
              have hget : bands[v + 3]? = none := by
                apply List.getElem?_eq_none
                simp [bands]
              simpa [Nat.add_assoc] using hget


theorem sound : TinylambOpt.ResearchGoals.November26.Tpch.Q19.Goal := by
  intro rows
  dsimp
  apply List.filter_congr
  intro r hr
  rcases r with ⟨air, tag, q⟩
  simpa [TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q19.bands] using TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q19.accepts_iff air tag q
end TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q19
