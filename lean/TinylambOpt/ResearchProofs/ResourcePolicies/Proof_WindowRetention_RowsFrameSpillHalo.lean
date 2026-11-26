import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.WindowRetention.Goal_RowsFrameSpillHalo

open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention


/-!
A bounded ROWS frame crossing a spill boundary depends only on the retained halo:
after cutting `a | b`, the frame at the first row of `b` and at all later rows is
the same when `a` is replaced by its last `w` rows. This justifies discarding
predecessors older than the frame width at a morsel/spill boundary.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.RowsFrameSpillHalo.Goal := by
  intro a b w j hj
  unfold TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.frame
  unfold TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.tailRows
  simp only [List.length_drop]
  have htakeL : (a ++ b).take (a.length + j + 1) = a ++ b.take (j + 1) := by
    rw [List.take_append]
    have h1 : a.length + j + 1 - a.length = j + 1 := by omega
    rw [h1, List.take_of_length_le (by omega : a.length ≤ a.length + j + 1)]
  have hlenH : (a.drop (a.length - w)).length = a.length - (a.length - w) := by
    rw [List.length_drop]
  have htakeR : (a.drop (a.length - w) ++ b).take
      ((a.length - (a.length - w)) + j + 1) =
      a.drop (a.length - w) ++ b.take (j + 1) := by
    rw [List.take_append]
    rw [hlenH]
    have h1 : (a.length - (a.length - w)) + j + 1 - (a.length - (a.length - w)) = j + 1 := by omega
    rw [h1]
    rw [List.take_of_length_le (by rw [hlenH]; omega)]
  rw [htakeL, htakeR]
  by_cases ha : a.length ≤ w
  · have hsub : a.length - w = 0 := by omega
    have hlenh : a.length - (a.length - w) = a.length := by omega
    simp [hsub]
  · have haw : w < a.length := Nat.lt_of_not_ge ha
    have hlenh : a.length - (a.length - w) = w := by omega
    have hidxL : a.length + j - w = (a.length - w) + j := by omega
    have hidxR : w + j - w = j := by omega
    rw [hidxL, hlenh, hidxR]
    have hp : (a.take (a.length - w)).length = a.length - w := by simp
    have hdecomp : a = a.take (a.length - w) ++ a.drop (a.length - w) :=
      (List.take_append_drop (a.length - w) a).symm
    have hcongr := congrArg
      (fun L : List (Option Int) => List.drop ((a.length - w) + j) (L ++ b.take (j + 1))) hdecomp
    have hleftEq : List.drop ((a.length - w) + j) (a ++ b.take (j + 1)) =
        List.drop ((a.take (a.length - w)).length + j)
          ((a.take (a.length - w) ++ a.drop (a.length - w)) ++ b.take (j + 1)) := by
      rw [hp]
      exact hcongr
    calc
      List.drop ((a.length - w) + j) (a ++ b.take (j + 1))
          = List.drop ((a.take (a.length - w)).length + j)
              ((a.take (a.length - w) ++ a.drop (a.length - w)) ++ b.take (j + 1)) := hleftEq
      _ = List.drop j (a.drop (a.length - w) ++ b.take (j + 1)) := by
              rw [List.append_assoc, List.drop_append,
                List.drop_eq_nil_of_le (by omega : (a.take (a.length - w)).length ≤ (a.take (a.length - w)).length + j)]
              simp
end TinylambOpt.ResearchProofs.ResourcePolicies.WindowRetention.Goal_RowsFrameSpillHalo
