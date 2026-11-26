import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-! Summing decoded rows applies each delta once per row at or after its
position, giving the reverse ordinal weight `(n-i)`. SQL can fuse decode and
SUM into a single weighted pass over the encoded deltas. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_TriangularDeltaSum

open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

theorem scanlEqConsValues (b : Int) (ds : List Int) :
    List.scanl (· + ·) b ds = b :: values b ds := by
  cases ds with
  | nil => simp [values]
  | cons d ds => simp [values, List.scanl_cons]

theorem weightedZip (ds : List Int) (start : Nat) :
    ((ds.zipIdx start).map (fun p => p.1 * ((ds.length + start - p.2 : Nat) : Int))).sum = weighted ds := by
  induction ds generalizing start with
  | nil => simp [weighted]
  | cons d ds ih =>
      unfold weighted
      simp [List.zipIdx_cons, List.map_cons, List.sum_cons]
      have heq : ds.length + 1 + start = ds.length + (start + 1) := by omega
      rw [heq]
      rw [ih (start := start + 1)]
      rw [ih (start := 1)]

theorem weightedCons (d : Int) (ds : List Int) :
    weighted (d :: ds) = d * ((ds.length + 1 : Nat) : Int) + weighted ds := by
  have h := weightedZip (d :: ds) 0
  simp only [List.zipIdx_cons, List.map_cons, List.sum_cons, List.length_cons,
    Nat.add_zero, Nat.sub_zero] at h
  have h1 := weightedZip ds 1
  rw [h1] at h
  exact h.symm

theorem sound : TriangularDeltaSum.Goal := by
  intro b ds
  induction ds generalizing b with
  | nil => simp [values, weighted]
  | cons d ds ih =>
      have hv : values b (d :: ds) = List.scanl (· + ·) (b + d) ds := by
        simp [values, List.scanl_cons]
      rw [hv]
      rw [scanlEqConsValues]
      simp only [List.sum_cons]
      rw [ih]
      rw [weightedCons]
      have hw : ((ds.length + 1 : Nat) : Int) = (ds.length : Int) + 1 := by simp
      rw [hw]
      let n : Int := ds.length
      let w : Int := weighted ds
      let b' : Int := b
      let d' : Int := d
      change b' + d' + (n * (b' + d') + w) = (n + 1) * b' + (d' * (n + 1) + w)
      simp only [Int.mul_add, Int.add_mul, Int.add_assoc, Int.add_comm, Int.add_left_comm, Int.mul_comm, Int.mul_one]

end TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_TriangularDeltaSum
