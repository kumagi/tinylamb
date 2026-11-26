import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace Nov26.Proof_SelectionNetworks_CascadeMaskCoordinates
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.CascadeMaskCoordinates
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.CascadeMaskCoordinates
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.CascadeMaskCoordinates
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.CascadeMaskCoordinates

/-- Membership characterization of the position list: indices of the set bits. -/
private theorem mem_positions : ∀ (m : List Bool) (i : Nat),
    i ∈ positions m ↔ m[i]? = some true := by
  intro m i
  unfold positions
  rw [List.mem_map]
  simp only [List.mem_filter]
  constructor
  · rintro ⟨p, ⟨⟨hzip, hb⟩, hi⟩⟩
    obtain ⟨b, j⟩ := p
    rw [List.mk_mem_zipIdx_iff_le_and_getElem?_sub] at hzip
    obtain ⟨_, hm⟩ := hzip
    cases b <;> simp_all
  · intro h
    refine ⟨(true, i), ⟨⟨?_, by simp⟩, rfl⟩⟩
    rw [List.mk_mem_zipIdx_iff_le_and_getElem?_sub]
    exact ⟨Nat.zero_le _, by simpa using h⟩

/-- Every selected position is an in-range source ordinal. -/
private theorem lt_length_positions (m : List Bool) :
    ∀ i ∈ positions m, i < m.length := by
  intro i h
  rw [mem_positions] at h
  rcases Nat.lt_or_ge i m.length with hlt | hge
  · exact hlt
  · have := List.getElem?_eq_none hge
    rw [this] at h
    exact absurd h (by simp)

/-- Lane-wise view of a gathered vector under an in-range hypothesis. -/
private theorem get?_gather (xs : List Nat) : ∀ (a : List Nat),
    (∀ i ∈ a, i < xs.length) →
    ∀ (j : Nat), (gather xs a)[j]? = (a[j]? : Option Nat) >>= fun i => xs[i]? := by
  intro a
  induction a with
  | nil => intro _ j; simp [gather]
  | cons x as ih =>
      intro hall j
      have hx : x < xs.length := hall x (by simp)
      have has : ∀ i ∈ as, i < xs.length := fun i m => hall i (by simp [m])
      have ho : xs[x]? = some (xs[x]) := List.getElem?_eq_getElem hx
      simp only [gather, List.filterMap_cons, ho]
      cases j with
      | zero => simp [List.getElem?_cons_zero]
      | succ k =>
          rw [List.getElem?_cons_succ]
          show (gather xs as)[k]? = (as[k]? : Option Nat) >>= fun i => xs[i]?
          exact ih has k

/-- Two-stage gather fusion under an in-range hypothesis on the first stage. -/
private theorem two_stage (xs a b : List Nat) (hall : ∀ i ∈ a, i < xs.length) :
    gather (gather xs a) b = gather xs (gather a b) := by
  show b.filterMap (fun j => (gather xs a)[j]?)
      = (b.filterMap (fun j => (a[j]? : Option Nat))).filterMap (fun i => xs[i]?)
  rw [List.filterMap_filterMap]
  congr 1
  funext j
  exact get?_gather xs a hall j

/--
Cascading two compaction masks: compressing by mask `a` and then by mask `b` equals a single
gather of the source ordinals obtained by indexing the parent mask's position vector at the
child mask's position vector. This lets the code generator address a child compact batch in
parent-rank space and emit final source ordinals directly, fusing the two selections into one
gather. The length hypothesis makes every parent position a valid source ordinal, which is what
enables the two-stage gather fusion.
-/
theorem sound : Goal := by
  intro xs a b hlen
  have hv : ∀ i ∈ positions a, i < xs.length := by
    intro i h
    have h2 := lt_length_positions a i h
    rw [← hlen] at h2
    exact h2
  show gather (gather xs (positions a)) (positions b)
      = gather xs (gather (positions a) (positions b))
  rw [two_stage xs (positions a) (positions b) hv]

end Nov26.Proof_SelectionNetworks_CascadeMaskCoordinates