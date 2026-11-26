import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_TailEvacuationShrinksFile

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents


/-! Scratch proof: moving the highest live block into a strictly lower hole
    lowers the fence of the backing file. Every mapped id stays below the old
    maximum, so the new roof is strictly smaller. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

private theorem max_add_one_le_iff (x m limit : Nat) :
    max x m + 1 ≤ limit ↔ x < limit ∧ m + 1 ≤ limit := by
  omega

private theorem roof_le_iff (ids : List Nat) (limit : Nat) :
    roof ids ≤ limit ↔ ∀ id ∈ ids, id < limit := by
  induction ids with
  | nil => simp [roof]
  | cons x xs ih =>
      cases xs with
      | nil =>
          simp [roof]
          omega
      | cons y ys =>
          have hroof_cons : roof (y :: ys) = ((y :: ys).max?.getD 0) + 1 := by
            simp [roof, List.max?_cons]
          have ih' : ((y :: ys).max?.getD 0) + 1 ≤ limit ↔
              ∀ id ∈ y :: ys, id < limit := by
            simpa [hroof_cons] using ih
          have hroof_all : roof (x :: y :: ys) = max x ((y :: ys).max?.getD 0) + 1 := by
            simp [roof, List.max?_cons]
          rw [hroof_all, max_add_one_le_iff, ih']
          simp

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents.TailEvacuationShrinksFile.Goal := by
  intro ids last hole hmax hhole_not hhole_lt
  have hroof_ids : roof ids = last + 1 := by
    simp [roof, hmax]
  have hall_le : ∀ id ∈ ids, id ≤ last := by
    have hle : roof ids ≤ last + 1 := by omega
    have h' : ∀ id ∈ ids, id < last + 1 := (roof_le_iff ids (last + 1)).mp hle
    intro id hid
    have := h' id hid
    omega
  have hmapped : ∀ i ∈ ids.map (fun i => if i == last then hole else i), i < last := by
    intro j hj
    rcases List.mem_map.mp hj with ⟨i, hi, rfl⟩
    by_cases hilast : i == last
    · simpa [hilast] using hhole_lt
    · have hne : i ≠ last := by
        intro hi_eq
        subst i
        simp at hilast
      have hile : i ≤ last := hall_le i hi
      have hilt : i < last := by omega
      simpa [hilast] using hilt
  have hroof_mapped : roof (ids.map (fun i => if i == last then hole else i)) ≤ last :=
    (roof_le_iff _ last).mpr hmapped
  rw [hroof_ids]
  omega
end TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_TailEvacuationShrinksFile
