import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `AdjacentBucket.PruneExtent` — Elide a posting list when its stored endpoints miss
  every interval.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_AdjacentBucket_PruneExtent

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket

private def sublists : List Nat → List (List Nat)
  | [] => [[]]
  | a :: as => sublists as ++ (sublists as).map (a :: ·)

/-- Truth check: enumerate all sorted right lists over small domains, all
    left parameters and all candidate extents [lo, hi). -/
private def boundedCheck : Bool :=
  (List.range 8).all fun u =>
    (List.range 5).all fun t =>
      (List.range 12).all fun x =>
        (sublists (List.range u)).all fun ys =>
          (List.range (u + 2)).all fun lo =>
            (List.range (u + 2)).all fun hi =>
              if decide (ys.Pairwise (fun a b : Nat => a ≤ b)) &&
                  ys.all (fun y => y < u) && ys.all (fun y => lo ≤ y && y < hi) then
                if (search.spans u t x).all (fun p => p.2 ≤ lo || hi ≤ p.1) then
                  decide (ys.filter (search.test t x) = [])
                else true
              else true

example : boundedCheck = true := by native_decide

/-- If every stored neighbor-bucket interval misses `[lo, hi)`, no key in that
    extent can match the probe key. -/
theorem sound_PruneExtent : AdjacentBucket.PruneExtent.Goal := by
  intro u t x ys lo hi hv hys hspan
  rw [List.filter_eq_nil_iff]
  intro y hy htest
  have hyu : y < u := hv.2 y hy
  have hlo : lo ≤ y := (hys y hy).1
  have hhi : y < hi := (hys y hy).2
  have hcases : (x / (t + 1) + 1 = y / (t + 1)) ∨
      (y / (t + 1) + 1 = x / (t + 1)) := by
    simpa [search, beq_iff_eq] using htest
  let w := t + 1
  let A := x / w * w
  have hwpos : 0 < w := by dsimp [w]; omega
  rcases hcases with hnext | hprev
  · -- y / w = x / w + 1: the upper neighbor bucket slice covers y.
    have hb : y / w = x / w + 1 := by dsimp [w] at hnext ⊢; omega
    have hylow : A + w ≤ y := by
      have hdiv : y / w * w ≤ y := Nat.div_mul_le_self y w
      rw [hb, Nat.succ_mul] at hdiv
      simpa [A] using hdiv
    have hyhigh : y < min u (A + 2 * w) := by
      have hlt : y < y / w * w + w := Nat.lt_div_mul_add hwpos
      rw [hb, Nat.succ_mul] at hlt
      have hlt' : y < A + 2 * w := by
        dsimp [A]
        omega
      exact Nat.lt_min.mpr ⟨hyu, hlt'⟩
    have hab : A + w < min u (A + 2 * w) := by omega
    have hmem : (A + w, min u (A + 2 * w)) ∈ search.spans u t x := by
      simp [search, w, A, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hab]
    rcases hspan (A + w, min u (A + 2 * w)) hmem with hle | hge <;> omega
  · -- x / w = y / w + 1: the lower neighbor bucket slice covers y.
    have hb : x / w = y / w + 1 := by dsimp [w] at hprev ⊢; omega
    have hA : A = y / w * w + w := by
      dsimp [A]
      rw [hb, Nat.succ_mul]
    have hylow : A - w ≤ y := by
      rw [hA]
      have hdiv : y / w * w ≤ y := Nat.div_mul_le_self y w
      omega
    have hyhigh : y < min u A := by
      have hlt : y < y / w * w + w := Nat.lt_div_mul_add hwpos
      rw [← hA] at hlt
      exact Nat.lt_min.mpr ⟨hyu, hlt⟩
    have hab : A - w < min u A := by omega
    have hmem : (A - w, min u A) ∈ search.spans u t x := by
      simp [search, w, A, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hab]
    rcases hspan (A - w, min u A) hmem with hle | hge <;> omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_AdjacentBucket_PruneExtent
