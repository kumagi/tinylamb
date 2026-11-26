import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

/-! A parent-rank mask can be translated to source ordinals before the final gather, so two nested compresses equal one gather. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.SelectionNetworks.Goal_SelectionNetworks_CascadeMaskCoordinates

open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.CascadeMaskCoordinates

theorem positions_mem_lt (m : List Bool) : ∀ i ∈ positions m, i < m.length := by
  intro i hi
  simp only [positions] at hi
  rcases List.mem_map.mp hi with ⟨p, hp, hp2⟩
  rcases List.mem_filter.mp hp with ⟨hpz, hptrue⟩
  rcases p with ⟨val, idx⟩
  simp at hptrue hp2
  subst val
  subst idx
  have h := (List.mem_zipIdx hpz).2.1
  simpa using h

theorem getElem_gather_valid (xs a : List Nat) (hvalid : ∀ i ∈ a, i < xs.length) :
    ∀ (j : Nat), (gather xs a)[j]? = (a[j]?).bind (fun i => xs[i]?) := by
  induction a with
  | nil => intro j; simp [gather]
  | cons i a ih =>
      have hi_lt : i < xs.length := hvalid i (by simp)
      have hi : xs[i]? = some (xs[i]'hi_lt) := List.getElem?_eq_getElem hi_lt
      have htail : ∀ j ∈ a, j < xs.length := by
        intro j hj
        exact hvalid j (by simp [hj])
      intro j
      cases j with
      | zero => simp [gather, hi]
      | succ j =>
          simpa [gather, hi] using ih htail j

theorem sound : CascadeMaskCoordinates.Goal := by
  intro xs a b hlen
  let P := positions a
  have hP : ∀ i ∈ P, i < xs.length := by
    intro i hi
    have h := positions_mem_lt a i hi
    omega
  have hget := getElem_gather_valid xs P hP
  have hget' : ∀ (j : Nat), (List.filterMap (fun i => xs[i]?) P)[j]? = (P[j]?).bind (fun i => xs[i]?) := by
    intro j
    simpa [P, gather] using hget j
  have hcomp : compress xs a = gather xs P := by rfl
  rw [hcomp]
  simp [compress, gather, List.filterMap_filterMap, hget', P]

end TinylambOpt.ResearchProofs.JitKernels.SelectionNetworks.Goal_SelectionNetworks_CascadeMaskCoordinates
