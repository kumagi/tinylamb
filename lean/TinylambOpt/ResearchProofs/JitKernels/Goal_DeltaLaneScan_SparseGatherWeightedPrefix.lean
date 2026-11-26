import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-! A sparse gather of decoded rows contributes the base once per selected row
and each delta once per selected suffix position that contains it. Exchanging
the two finite sums gives the weighted formula, so SQL can answer a duplicated
selection without decoding every row. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_SparseGatherWeightedPrefix

open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

theorem sumMapAddConst (sel : List Nat) (b : Int) (f : Nat → Int) :
    (sel.map (fun s => b + f s)).sum = (sel.length : Int) * b + (sel.map f).sum := by
  induction sel with
  | nil => simp
  | cons s sel ih =>
      simp [List.map_cons, List.sum_cons, ih, Int.add_mul]
      simp only [Int.add_assoc, Int.add_comm, Int.add_left_comm, Int.mul_comm]

theorem zipIndicatorAux (ds : List Int) (start s : Nat) :
    ((ds.zipIdx start).map (fun p => p.1 * (if p.2 ≤ s then (1 : Int) else 0))).sum =
      (ds.take (s + 1 - start)).sum := by
  induction ds generalizing start with
  | nil => simp
  | cons d ds ih =>
      by_cases hs : start ≤ s
      · simp [List.zipIdx_cons, hs, ih]
        have h1 : s + 1 - start = (s - start) + 1 := by omega
        rw [h1]
        simp [List.take]
      · simp [List.zipIdx_cons, hs, ih]
        have h1 : s + 1 - start = 0 := by omega
        have h3 : s - start = 0 := by omega
        rw [h1, h3]
        simp

theorem sumZeros {α : Type} (l : List α) : (l.map fun _ => (0 : Int)).sum = 0 := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

theorem sumCountPCons (l : List (Int × Nat)) (s : Nat) (sel : List Nat) :
    (l.map fun p => p.1 * (((s :: sel).countP (p.2 ≤ ·) : Nat) : Int)).sum =
      (l.map fun p => p.1 * ((sel.countP (p.2 ≤ ·) : Nat) : Int)).sum +
      (l.map fun p => p.1 * (if p.2 ≤ s then (1 : Int) else 0)).sum := by
  induction l with
  | nil => simp
  | cons p l ih =>
      have hc : (s :: sel).countP (fun x => p.2 ≤ x) =
          sel.countP (fun x => p.2 ≤ x) + (if p.2 ≤ s then 1 else 0) := by
        simp [List.countP_cons]
      by_cases hps : p.2 ≤ s <;>
        simp [hps, List.map_cons, List.sum_cons, hc, ih, Int.mul_add,
          Int.add_assoc, Int.add_comm, Int.add_left_comm]

theorem swapSums (ds : List Int) (sel : List Nat) :
    (sel.map fun s => (ds.take (s + 1)).sum).sum =
    (ds.zipIdx.map fun p => p.1 * ((sel.countP (p.2 ≤ ·) : Nat) : Int)).sum := by
  induction sel with
  | nil => simp [sumZeros]
  | cons s sel ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      rw [sumCountPCons]
      rw [zipIndicatorAux]
      simp [Int.add_comm]

theorem sound : TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.SparseGatherWeightedPrefix.Goal := by
  intro b ds sel hsel
  unfold TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.get
  rw [sumMapAddConst]
  rw [swapSums]

end TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_SparseGatherWeightedPrefix
