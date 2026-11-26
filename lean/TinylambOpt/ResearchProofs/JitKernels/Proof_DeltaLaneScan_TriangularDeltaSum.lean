import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-!
The sum of a decoded delta column equals the base replicated by the row count
plus a triangular weighting of the deltas: each delta `ds[i]` contributes to
exactly the `length - i` rows at or after it, so
`(values b ds).sum = (ds.length : Int) * b + weighted ds`. SUM over a scanned
delta run may therefore be fused with decoding using per-lane reverse-ordinal
weights, with no row-by-row accumulation.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_TriangularDeltaSum
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

private theorem map_congr' {α β : Type} (f g : α → β) (l : List α)
    (h : ∀ x ∈ l, f x = g x) : List.map f l = List.map g l := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      rw [List.map_cons, List.map_cons, h a (by simp),
        ih (fun x hx => h x (by simp [hx]))]

private theorem scanl_head (acc : Int) (l : List Int) :
    l.scanl (·+·) acc = acc :: (l.scanl (·+·) acc).drop 1 := by
  induction l with
  | nil => rfl
  | cons d t ih => rw [List.scanl_cons]; rfl

private theorem values_cons (b d : Int) (t : List Int) :
    values b (d::t) = (b+d) :: values (b+d) t := by
  unfold values
  rw [List.scanl_cons]
  rw [show List.drop 1 (b :: List.scanl (·+·) (b+d) t) = List.scanl (·+·) (b+d) t from rfl,
    ← scanl_head]

private theorem zipIdx_succ (l : List Int) (i : Nat) :
    l.zipIdx (i+1) = (l.zipIdx i).map (fun p => (p.1, p.2 + 1)) := by
  induction l generalizing i with
  | nil => rfl
  | cons a as ih => rw [List.zipIdx_cons, List.zipIdx_cons, List.map_cons, ih (i+1)]

private theorem zipIdx_head (d : Int) (t : List Int) :
    (d :: t).zipIdx = (d, 0) :: List.map (fun p : Int × Nat => (p.1, p.2 + 1)) (List.zipIdx t) := by
  rw [List.zipIdx_cons, zipIdx_succ]

private theorem weighted_cons (d : Int) (t : List Int) :
    weighted (d::t) = ((t.length : Int) + 1) * d + weighted t := by
  unfold weighted
  rw [zipIdx_head]
  have hmerge : List.map (fun x : Int × Nat => match x with | (v, i) => v * ((List.length (d :: t) - i : Nat) : Int))
      (List.map (fun p : Int × Nat => (p.1, p.2 + 1)) (List.zipIdx t))
      = List.map (fun p : Int × Nat => p.1 * ((List.length (d :: t) - (p.2 + 1) : Nat) : Int))
        (List.zipIdx t) := by
    rw [List.map_map]
    refine map_congr' _ _ _ ?_
    intro x _
    obtain ⟨v, j⟩ := x
    simp
  rw [List.map_cons, hmerge, List.sum_cons]
  have hlen : ((d :: t).length : Nat) = t.length + 1 := rfl
  rw [hlen]
  have htail : List.map (fun p : Int × Nat => p.1 * ((t.length + 1 - (p.2 + 1) : Nat) : Int))
      (List.zipIdx t)
      = List.map (fun x : Int × Nat => match x with | (v, i) => v * ((List.length t - i : Nat) : Int))
        (List.zipIdx t) := by
    refine map_congr' _ _ _ ?_
    intro x hx
    obtain ⟨v, i⟩ := x
    simp [Nat.succ_sub_succ]
  rw [htail]
  simp [Int.mul_comm]

theorem sound : TriangularDeltaSum.Goal := by
  intro b ds
  induction ds generalizing b with
  | nil => simp [values, weighted]
  | cons d t ih =>
      rw [values_cons, List.sum_cons, ih, weighted_cons]
      have hL : (((d :: t).length : Nat) : Int) = (t.length : Int) + 1 := by simp
      rw [hL, Int.mul_add]
      have h1 : ((t.length : Int) + 1) * d = (t.length : Int) * d + d := by
        rw [Int.add_mul, Int.one_mul]
      have h2 : ((t.length : Int) + 1) * b = (t.length : Int) * b + b := by
        rw [Int.add_mul, Int.one_mul]
      rw [h1, h2]
      omega

end TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_TriangularDeltaSum
