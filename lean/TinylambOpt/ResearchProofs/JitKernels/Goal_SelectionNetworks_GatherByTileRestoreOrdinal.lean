import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

/-! Tile sorting only permutes requests; mapping each ordinal to its rank in that permutation restores source order. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.SelectionNetworks.Goal_SelectionNetworks_GatherByTileRestoreOrdinal

open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.GatherByTileRestoreOrdinal

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

theorem filterMap_congr {α β : Type} (f g : α → Option β) (l : List α) (h : ∀ x ∈ l, f x = g x) :
    l.filterMap f = l.filterMap g := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, f y = g y := by intro y hy; exact h y (by simp [hy])
      rw [List.filterMap_cons, List.filterMap_cons, hx, ih ht]

theorem sound : GatherByTileRestoreOrdinal.Goal := by
  intro xs sel n hn hvalid
  let T := tile sel n
  have hperm : T.Perm sel := by
    simpa [T, tile] using List.mergeSort_perm sel (fun a b : Nat => decide (a / n ≤ b / n))
  have hTvalid : ∀ i ∈ T, i < xs.length := by
    intro i hi
    exact hvalid i (hperm.mem_iff.mp hi)
  have hget := getElem_gather_valid xs T hTvalid
  have hfun : ∀ i ∈ sel, (gather xs T)[T.idxOf i]? = xs[i]? := by
    intro i hi
    have hmemT : i ∈ T := hperm.mem_iff.mpr hi
    have hlt : T.idxOf i < T.length := List.idxOf_lt_length_of_mem hmemT
    have hval : T[T.idxOf i] = i := List.getElem_idxOf hlt
    have hsome : T[T.idxOf i]? = some i := by
      rw [List.getElem?_eq_getElem hlt, hval]
    rw [hget (T.idxOf i), hsome]
    simp
  have hmap : (sel.map (fun i => T.idxOf i)).filterMap (fun j => (gather xs T)[j]?) = sel.filterMap (fun i => xs[i]?) := by
    rw [List.filterMap_map]
    apply filterMap_congr
    intro i hi
    exact hfun i hi
  have htarget : gather (gather xs T) (sel.map (fun i => T.idxOf i)) = gather xs sel := by
    rw [gather, List.filterMap_map]
    simpa [gather] using hmap
  simpa [T] using htarget

end TinylambOpt.ResearchProofs.JitKernels.SelectionNetworks.Goal_SelectionNetworks_GatherByTileRestoreOrdinal
