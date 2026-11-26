import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-!
A gather that reads rows at (possibly repeated, unsorted) indices `sel` has
total `|sel| * b` plus each delta weighted by how many gathered rows are at or
after it: `sel.map (get b ds) |>.sum = (sel.length : Int) * b +
Σ p ∈ ds.zipIdx, p.1 * sel.countP (p.2 ≤ ·)`. Arbitrary duplicate-carrying
selections over a delta column may therefore be summed without materializing
any row: one pass over the deltas with gather-frequency weights.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_SparseGatherWeightedPrefix
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

private theorem map_congr' {α β : Type} (f g : α → β) (l : List α)
    (h : ∀ x ∈ l, f x = g x) : List.map f l = List.map g l := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      rw [List.map_cons, List.map_cons, h a (by simp),
        ih (fun x hx => h x (by simp [hx]))]

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Int) :
    (l.map (fun p => f p + g p)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      rw [List.map_cons, List.sum_cons, List.map_cons, List.sum_cons, ih,
        List.map_cons, List.sum_cons]
      omega

private theorem sum_map_zero {α : Type} (l : List α) :
    (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      rw [List.map_cons, List.sum_cons]
      simp [ih]

private theorem zipIdx_succ (l : List Int) (i : Nat) :
    l.zipIdx (i+1) = (l.zipIdx i).map (fun p => (p.1, p.2 + 1)) := by
  induction l generalizing i with
  | nil => rfl
  | cons a as ih => rw [List.zipIdx_cons, List.zipIdx_cons, List.map_cons, ih (i+1)]

private theorem zipIdx_head (d : Int) (t : List Int) :
    (d :: t).zipIdx = (d, 0) :: List.map (fun p : Int × Nat => (p.1, p.2 + 1)) (List.zipIdx t) := by
  rw [List.zipIdx_cons, zipIdx_succ]

private theorem prefixW (ds : List Int) (i : Nat) :
    (ds.zipIdx.map (fun p => p.1 * (if decide (p.2 ≤ i) then (1 : Int) else 0))).sum
      = (ds.take (i+1)).sum := by
  induction ds generalizing i with
  | nil => simp
  | cons d t ih =>
      rw [zipIdx_head]
      have hmerge : List.map (fun p : Int × Nat => p.1 * (if decide (p.2 ≤ i) then (1 : Int) else 0))
          (List.map (fun p : Int × Nat => (p.1, p.2 + 1)) (List.zipIdx t))
          = List.map (fun p : Int × Nat => p.1 * (if decide (p.2 + 1 ≤ i) then (1 : Int) else 0))
            (List.zipIdx t) := by
        rw [List.map_map]
        refine map_congr' _ _ _ ?_
        intro x _
        obtain ⟨v, j⟩ := x
        simp
      rw [List.map_cons, hmerge, List.sum_cons]
      have hhead : ((d, 0).fst * (if decide ((d, 0).snd ≤ i) then (1 : Int) else 0)) = d := by
        simp
      rw [hhead]
      match i with
      | 0 =>
          have hz : ∀ x ∈ (List.zipIdx t : List (Int × Nat)),
              x.1 * (if decide (x.2 + 1 ≤ 0) then (1 : Int) else 0) = 0 := by
            intro x hx
            obtain ⟨v, j⟩ := x
            have hn : ¬ (j + 1 ≤ 0) := by omega
            simp [hn]
          rw [show List.map (fun x : Int × Nat => x.1 * (if decide (x.2 + 1 ≤ 0) then (1 : Int) else 0))
                (List.zipIdx t) = List.map (fun _ => (0 : Int)) (List.zipIdx t)
              from map_congr' _ _ _ hz, sum_map_zero]
          rw [show List.take (0 + 1) (d :: t) = [d] from rfl, List.sum_singleton]
          exact Int.add_zero d
      | k+1 =>
          have hc : ∀ x ∈ (List.zipIdx t : List (Int × Nat)),
              x.1 * (if decide (x.2 + 1 ≤ k + 1) then (1 : Int) else 0)
                = x.1 * (if decide (x.2 ≤ k) then (1 : Int) else 0) := by
            intro x hx
            obtain ⟨v, j⟩ := x
            by_cases hj : j ≤ k
            · have hj2 : j + 1 ≤ k + 1 := by omega
              simp [hj, hj2]
            · have hj2 : ¬ (j + 1 ≤ k + 1) := by omega
              simp [hj, hj2]
          rw [show List.map (fun x : Int × Nat => x.1 * (if decide (x.2 + 1 ≤ k + 1) then (1 : Int) else 0))
                (List.zipIdx t)
                = List.map (fun x : Int × Nat => x.1 * (if decide (x.2 ≤ k) then (1 : Int) else 0))
                  (List.zipIdx t)
              from map_congr' _ _ _ hc, ih k]
          rw [show List.take (k + 1 + 1) (d :: t) = d :: List.take (k + 1) t from rfl,
            List.sum_cons]

theorem sound : SparseGatherWeightedPrefix.Goal := by
  intro b ds sel h
  induction sel with
  | nil =>
      rw [List.map_nil, List.sum_nil]
      rw [show List.map (fun p : Int × Nat =>
            p.1 * ((List.countP (fun y => decide (p.2 ≤ y)) [] : Nat) : Int)) (List.zipIdx ds)
          = List.map (fun _ => (0 : Int)) (List.zipIdx ds)
        from map_congr' _ _ _ (by intro z hz; rw [List.countP_nil]; simp), sum_map_zero]
      simp
  | cons i t ih =>
      rw [List.map_cons, List.sum_cons, ih (fun x hx => h x (by simp [hx]))]
      unfold TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.get
      have hL : (((i :: t).length : Nat) : Int) = (t.length : Int) + 1 := by simp
      have hcnt : ∀ p : Int × Nat,
          List.countP (fun y => decide (p.2 ≤ y)) (i :: t)
            = List.countP (fun y => decide (p.2 ≤ y)) t + if decide (p.2 ≤ i) then 1 else 0 := by
        intro p
        rw [List.countP_cons]
      have hmap : List.map (fun p : Int × Nat =>
            p.1 * ((List.countP (fun y => decide (p.2 ≤ y)) (i :: t) : Nat) : Int)) (List.zipIdx ds)
          = List.map (fun p : Int × Nat =>
              p.1 * ((List.countP (fun y => decide (p.2 ≤ y)) t : Nat) : Int)
                + p.1 * (((if decide (p.2 ≤ i) then 1 else 0 : Nat)) : Int)) (List.zipIdx ds) := by
        refine map_congr' _ _ _ ?_
        intro z hz
        rw [hcnt z]
        obtain ⟨v, j⟩ := z
        by_cases hj : j ≤ i <;> simp [hj, Int.mul_add, Int.mul_one, Int.mul_zero]
      have hind : List.map (fun p : Int × Nat =>
            p.1 * (((if decide (p.2 ≤ i) then 1 else 0 : Nat)) : Int)) (List.zipIdx ds)
          = List.map (fun p : Int × Nat =>
              p.1 * (if decide (p.2 ≤ i) then (1 : Int) else 0)) (List.zipIdx ds) := by
        refine map_congr' _ _ _ ?_
        intro z hz
        obtain ⟨v, j⟩ := z
        by_cases hj : j ≤ i <;> simp [hj]
      rw [hL, hmap, sum_map_add, hind, prefixW ds i, Int.add_mul, Int.one_mul]
      try omega

end TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_SparseGatherWeightedPrefix
