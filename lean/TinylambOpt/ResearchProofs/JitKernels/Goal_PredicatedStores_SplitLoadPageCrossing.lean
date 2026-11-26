import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
A wide load assembled from two adjacent readable ranges equals a single wide
load. The proof reduces both sides to a map over `List.range` and uses a range
split lemma: `range k ++ map (k + ·) (range (n-k)) = range n` for `k ≤ n`.

Proved

- `SplitLoadPageCrossing` — the assembled wide load can replace a speculative
  crossing load without changing the bytes observed by SQL execution.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_SplitLoadPageCrossing

open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-- The index split used by adjacent loads: the first `k` indices followed by
`k + i` for `i` in `range d` enumerate exactly `range (k+d)`. -/
theorem range_split_aux (k d : Nat) :
    List.range k ++ (List.range d).map (fun i => k + i) = List.range (k + d) := by
  induction d with
  | zero => simp
  | succ d ih =>
      rw [List.range_succ, List.map_append, ← List.append_assoc, ih]
      simp [show k + (d + 1) = k + d + 1 by omega, List.range_succ]

theorem sound : SplitLoadPageCrossing.Goal := by
  intro m a n k h
  unfold TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.read
  have hsecond : List.map (fun i => m (a + k + i)) (List.range (n - k)) =
      List.map (fun j => m (a + j)) ((List.range (n - k)).map (fun i => k + i)) := by
    simp [Nat.add_assoc]
  rw [hsecond, ← List.map_append]
  have haux : List.range k ++ (List.range (n - k)).map (fun i => k + i) = List.range n := by
    simpa [Nat.add_sub_of_le h] using range_split_aux k (n - k)
  rw [haux]

end TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_SplitLoadPageCrossing
