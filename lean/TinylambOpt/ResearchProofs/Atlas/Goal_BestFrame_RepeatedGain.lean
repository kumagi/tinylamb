import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
import TinylambOpt.ResearchProofs.Atlas_OrderedMeasures_BestFrame_Remaining

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_BestFrame_RepeatedGain

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! Brute-force truth check on all series of length ≤ 3 over [-3, 3] and n ∈ {2,3,4}. -/
#eval do
  let vals : List Int := [-3, -2, -1, 0, 1, 2, 3]
  let lists : List (List Int) :=
    [[]] ++
    vals.map (fun a => [a]) ++
    vals.flatMap (fun a => vals.flatMap (fun b => [[a, b]])) ++
    vals.flatMap (fun a => vals.flatMap (fun b => vals.flatMap (fun c => [[a, b, c]])))
  let mut bad : List (List Int × Nat × Int × Int) := []
  for x in lists do
    for n in [2, 3, 4] do
      let lhs := BestFrame.measure ((List.replicate n x).flatten)
      let rhs := max (BestFrame.measure x)
        (max0 (suffs x) + max0 (prefs x) + ((n - 2 : Nat) : Int) * max 0 x.sum)
      if lhs != rhs then bad := (x, n, lhs, rhs) :: bad
  pure bad

/-! Any nonempty frame of `x` repeated `n` times either stays inside a single
    copy (贡献 ≤ `measure x`) or consists of a suffix of one copy, `k` whole
    copies, and a prefix of another (贡献 ≤ `max0 (suffs x) + max0 (prefs x) +
    (n-2)*max 0 x.sum`).  Both bounds are attainable, so the goal holds. -/
theorem sound_BestFrame_RepeatedGain : BestFrame.RepeatedGain.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.Remaining.sound_RepeatedGain

end TinylambOpt.ResearchProofs.Atlas.Goal_BestFrame_RepeatedGain
