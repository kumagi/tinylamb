import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_DyadicTile_PayloadAlignment

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.DyadicTile

-- Indexing a list by the full range reconstructs the list itself.
private theorem map_getElemD_range {α : Type} (d : α) (rs : List α) :
    (List.range rs.length).map (fun i => rs[i]?.getD d) = rs := by
  apply List.ext_getElem?
  intro i
  rw [List.getElem?_map]
  by_cases hi : i < rs.length
  · rw [List.getElem?_range hi]
    simp [List.getElem?_eq_getElem hi]
  · have hle : rs.length ≤ i := by omega
    have hnone : rs[i]? = none := List.getElem?_eq_none hle
    have hnoneRange : (List.range rs.length)[i]? = none := by
      exact List.getElem?_eq_none (by simpa using hle)
    rw [hnone, hnoneRange]
    simp

-- Zipping two maps over the same list pairs them pointwise.
private theorem zip_map_self {α β γ : Type} (f : α → β) (g : α → γ) : ∀ l : List α,
    (l.map f).zip (l.map g) = l.map (fun x => (f x, g x))
  | [] => rfl
  | x :: xs => by simp [zip_map_self f g xs]

-- mergeSort only reorders its input; this holds for every key, in particular
-- for the dyadic-tile key below. Hence the serialized ordinals are exactly
-- the range of original positions.
private theorem perm_isPerm (key : Key) (rs : List Row) :
    (perm key rs).Perm (List.range rs.length) := by
  unfold perm
  have h := List.mergeSort_perm (rs.zipIdx)
    (fun (x : Row × Nat) (y : Row × Nat) =>
      let a := key x.1
      let b := key y.1
      a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && x.2 ≤ y.2))))
  have h2 := h.map Prod.snd
  simpa [List.zipIdx_map_snd, List.range_eq_range'] using h2

theorem sound_PayloadAlignment : PayloadAlignment.Goal := by
  intro rs payload hlen
  unfold physical
  let ids := perm key rs
  change ((ids.map (fun i => rs[i]?.getD (0,0))).zip
    (ids.map (fun i => payload[i]?.getD 0))).Perm (rs.zip payload)
  rw [zip_map_self]
  have hp : ids.Perm (List.range rs.length) := by
    simpa [ids] using perm_isPerm key rs
  refine (hp.map (fun i => (rs[i]?.getD (0,0), payload[i]?.getD 0))).trans ?_
  rw [← zip_map_self]
  rw [map_getElemD_range (0,0) rs]
  rw [show List.range rs.length = List.range payload.length by rw [hlen]]
  rw [map_getElemD_range 0 payload]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_DyadicTile_PayloadAlignment
