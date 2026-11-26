import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `GrayPair.NeighborMask` — `∀ rs x y, let s := physical key rs; ((s.map key).count
  (key (x,y))) = rs.count (x,y)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_GrayPair_NeighborMask

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.GrayPair

private theorem range'_eq_range (n : Nat) : List.range' 0 n = List.range n := by
  rw [List.range'_eq_map_range]
  simp

private theorem range_getD (rs : List Row) :
    (List.range rs.length).map (fun i => rs[i]?.getD (0, 0)) = rs := by
  apply List.ext_getElem?
  intro k
  by_cases hk : k < rs.length <;> simp [hk]

private def gray (r : Nat) : Nat := r ^^^ (r / 2)

private theorem xor_div2 (a b : Nat) : (a ^^^ b) / 2 = a / 2 ^^^ b / 2 := by
  apply Nat.eq_of_testBit_eq
  intro i
  simp [Nat.testBit_xor, Nat.testBit_div_two]

private theorem gray_xor (a b : Nat) : gray (a ^^^ b) = gray a ^^^ gray b := by
  unfold gray
  rw [xor_div2]
  apply Nat.eq_of_testBit_eq
  intro i
  simp only [Nat.testBit_xor]
  cases a.testBit i <;> cases b.testBit i <;> cases (a / 2).testBit i <;> cases (b / 2).testBit i <;> rfl

private theorem gray_inj {a b : Nat} (h : gray a = gray b) : a = b := by
  have h0 : gray (a ^^^ b) = 0 := by rw [gray_xor, h]; simp
  have hd : (a ^^^ b) = (a ^^^ b) / 2 := by
    unfold gray at h0
    have hh := congrArg (fun x => x ^^^ ((a ^^^ b) / 2)) h0
    simpa [Nat.xor_assoc] using hh
  have hz : (a ^^^ b) = 0 := by omega
  have hh := congrArg (fun x => x ^^^ b) hz
  simpa [Nat.xor_assoc] using hh

private theorem key_inj {a b : Row} (h : key a = key b) : a = b := by
  rcases a with ⟨x, y⟩
  rcases b with ⟨x', y'⟩
  simp only [key, Prod.mk.injEq] at h ⊢
  exact ⟨gray_inj h.1, h.2⟩

private theorem count_key (rs : List Row) (x y : Nat) :
    (rs.map key).count (key (x, y)) = rs.count (x, y) := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      have hbeq : (key r == key (x, y)) = (r == (x, y)) := by
        apply Bool.eq_iff_iff.2
        constructor
        · intro hk
          exact (beq_iff_eq).2 (key_inj ((beq_iff_eq).1 hk))
        · intro hr
          simp [(beq_iff_eq).1 hr]
      simp only [List.map_cons]
      rw [List.count_cons, List.count_cons]
      simp [hbeq, ih]

private theorem perm_indices (rs : List Row) : (perm key rs).Perm (List.range rs.length) := by
  have hperm0 :
      (rs.zipIdx.mergeSort (fun (x : Row × Nat) (y : Row × Nat) =>
        let a := key x.1
        let b := key y.1
        a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && x.2 ≤ y.2))))).Perm rs.zipIdx :=
    List.mergeSort_perm _ _
  have hperm1 := List.Perm.map Prod.snd hperm0
  unfold perm
  simpa [List.zipIdx_map_snd, range'_eq_range] using hperm1

private theorem perm_physical (rs : List Row) : (physical key rs).Perm rs := by
  unfold physical
  have hmap := (perm_indices rs).map (fun i => rs[i]?.getD (0, 0))
  rw [range_getD] at hmap
  exact hmap

theorem sound_NeighborMask : GrayPair.NeighborMask.Goal := by
  intro rs x y
  dsimp
  calc
    ((physical key rs).map key).count (key (x, y)) = (rs.map key).count (key (x, y)) :=
      (perm_physical rs).map key |>.count_eq (key (x, y))
    _ = rs.count (x, y) := count_key rs x y

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_GrayPair_NeighborMask
