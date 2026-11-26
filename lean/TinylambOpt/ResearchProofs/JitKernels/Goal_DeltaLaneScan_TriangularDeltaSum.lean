import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_TriangularDeltaSum

open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

theorem sound : TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.TriangularDeltaSum.Goal := by
  have hwoff : ∀ (ds : List Int) (k : Nat),
      ((ds.zipIdx k).map (fun (p : Int × Nat) => p.1 * ((ds.length + k - p.2 : Nat) : Int))).sum =
        weighted ds := by
    intro ds
    induction ds with
    | nil => intro k; simp [weighted]
    | cons x xs ih =>
        intro k
        simp only [List.zipIdx_cons, List.map_cons, List.sum_cons, List.length_cons]
        rw [show xs.length + 1 + k = xs.length + (k + 1) by omega]
        rw [ih (k + 1)]
        have hlen : xs.length + (k + 1) - k = xs.length + 1 := by omega
        rw [hlen]
        change x * (((xs.length + 1 : Nat) : Int)) + weighted xs =
          x * (((xs.length + 1 : Nat) : Int)) +
            ((xs.zipIdx 1).map (fun p : Int × Nat => p.1 * (((xs.length + 1 - p.2 : Nat) : Int)))).sum
        rw [ih 1]
  have hweighted : ∀ (d : Int) (ds : List Int),
      weighted (d :: ds) = ((ds.length + 1 : Nat) : Int) * d + weighted ds := by
    intro d ds
    unfold weighted
    rw [List.zipIdx_cons]
    simp only [List.length_cons, List.map_cons, List.sum_cons, Nat.zero_add]
    rw [hwoff ds 1]
    change d * (((ds.length + 1 : Nat) : Int)) + weighted ds =
      ((ds.length + 1 : Nat) : Int) * d + weighted ds
    rw [Int.mul_comm ((ds.length + 1 : Nat) : Int) d]
  intro b ds
  induction ds generalizing b with
  | nil => simp [values, weighted]
  | cons d ds ih =>
      rw [show values b (d :: ds) = (b + d) :: values (b + d) ds by
        cases ds <;> simp [values]]
      change ((b + d) :: values (b + d) ds).sum =
        ((ds.length + 1 : Nat) : Int) * b + weighted (d :: ds)
      simp only [List.sum_cons]
      rw [ih (b + d), hweighted d ds]
      rw [Int.mul_add]
      simp only [Int.natCast_add, Int.natCast_one]
      rw [Int.add_mul, Int.add_mul]
      simp only [Int.one_mul]
      omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_TriangularDeltaSum
