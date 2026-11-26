import Std
import TinylambOpt.ProposedGoals.QualifyPushdown

/-!
Proved

- `Goal` — `∀ (w : Row → Bool) (q : Int → Bool) (rows : List Row), PartitionOnly w →
  whereFilter w (qualify q rows) = qualify q (whereFilter w rows)`

The reference model is defined in `TinylambOpt.ProposedGoals.QualifyPushdown`.
-/

open TinylambOpt.ProposedGoals.QualifyPushdown

namespace TinylambOpt.ProposedProofs.QualifyPushdown

private theorem filter_comm {α : Type} (p q : α → Bool) (l : List α) :
    (l.filter p).filter q = (l.filter q).filter p := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      by_cases hp : p a = true <;> by_cases hq : q a = true <;>
        simp [hp, hq, ih]

/-- A partition-only predicate keeps whole partitions: if it accepts one row
    of key k it accepts every row of key k, so the window sum for k is intact. -/
private theorem partSum_filter_of_w (w : Row → Bool) (hw : PartitionOnly w)
    (rows : List Row) (r : Row) (hr : w r = true) :
    partSum (rows.filter w) r.1 = partSum rows r.1 := by
  simp only [partSum, List.filter_filter]
  apply congrArg (fun l : List Int => l.sum)
  apply congrArg (fun l : List Row => l.map fun r => r.2.1)
  apply List.filter_congr
  intro s hs
  by_cases hsk : s.1 == r.1
  · have hws : w s = true := by
      rw [hw s r hsk, hr]
    simp [hws, hsk]
  · simp [hsk]

theorem sound : TinylambOpt.ProposedGoals.QualifyPushdown.Goal := by
  intro w q rows hw
  unfold whereFilter qualify
  rw [filter_comm (fun r : Row => q (partSum rows r.1)) w rows]
  apply List.filter_congr
  intro r hr
  have hwr : w r = true := (List.mem_filter.mp hr).2
  rw [partSum_filter_of_w w hw rows r hwr]

end TinylambOpt.ProposedProofs.QualifyPushdown

-- C++ references to this model or its proof.
-- query/select_grouped_rewrites.cpp
-- query/select_rewrites.hpp
