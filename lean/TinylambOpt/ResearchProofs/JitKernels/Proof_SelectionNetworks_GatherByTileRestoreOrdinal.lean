import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace Nov26.Proof_SelectionNetworks_GatherByTileRestoreOrdinal
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.GatherByTileRestoreOrdinal
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.GatherByTileRestoreOrdinal
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.GatherByTileRestoreOrdinal
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.GatherByTileRestoreOrdinal

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

/-- `filterMap` after `map` fuses into a single `filterMap`. -/
private theorem filterMap_map : ∀ (l : List α) (g : α → β) (f : β → Option γ),
    List.filterMap f (List.map g l) = List.filterMap (fun x => f (g x)) l := by
  intro l g f
  induction l with
  | nil => rfl
  | cons a t ih => rw [List.map_cons, List.filterMap_cons, List.filterMap_cons, ih]

/-- Two functions that agree on the members of a list produce equal `filterMap` outputs. -/
private theorem filterMap_congr_mem : ∀ (l : List α) (f g : α → Option β),
    (∀ x ∈ l, f x = g x) → List.filterMap f l = List.filterMap g l := by
  intro l
  induction l with
  | nil => intro f g _; rfl
  | cons a t ih =>
      intro f g h
      rw [List.filterMap_cons, List.filterMap_cons, h a (by simp),
        ih f g (fun x m => h x (by simp [m]))]

/--
Gathering by tile order and then restoring each request's rank (its `idxOf` in the tile-sorted
ordinal list) yields exactly the source-order gather. Even with repeated ordinals, `idxOf` always
names a lane of the sorted vector carrying the requested ordinal, so the restored lane reads back
the same source row. This validates tile-sorted gathers for unordered consumers without keeping a
separate permutation vector.
-/
theorem sound : Goal := by
  intro xs sel n hn hsel
  have mem_t : ∀ i ∈ sel, i ∈ tile sel n := fun i m => List.mem_mergeSort.mpr m
  have hv_t : ∀ x ∈ tile sel n, x < xs.length := fun x m => hsel x (List.mem_mergeSort.mp m)
  have idx_get? : ∀ i ∈ sel, (tile sel n)[(tile sel n).idxOf i]? = some i := fun i m => by
    have hb : (tile sel n).idxOf i < (tile sel n).length :=
      List.idxOf_lt_length_of_mem (mem_t i m)
    rw [List.getElem?_eq_getElem hb, List.getElem_idxOf hb]
  have elem : ∀ i ∈ sel, (gather xs (tile sel n))[(tile sel n).idxOf i]? = xs[i]? := fun i m => by
    rw [get?_gather xs (tile sel n) hv_t ((tile sel n).idxOf i), idx_get? i m]
    simp
  show List.filterMap (fun p => (gather xs (tile sel n))[p]?)
      (List.map (fun i => (tile sel n).idxOf i) sel)
      = List.filterMap (fun i => xs[i]?) sel
  rw [filterMap_map, filterMap_congr_mem sel _ _ elem]

end Nov26.Proof_SelectionNetworks_GatherByTileRestoreOrdinal