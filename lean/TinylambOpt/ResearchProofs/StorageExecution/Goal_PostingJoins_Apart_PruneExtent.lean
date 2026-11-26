import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `Apart.PruneExtent` — Elide a posting list when its stored endpoints miss every
  interval.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Apart_PruneExtent

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart

private def sublists : List Nat → List (List Nat)
  | [] => [[]]
  | a :: as => sublists as ++ (sublists as).map (a :: ·)

/-- Truth check: enumerate all sorted right lists over small domains, all
    left parameters and all candidate extents [lo, hi). -/
private def boundedCheck : Bool :=
  (List.range 7).all fun u =>
    (List.range 5).all fun t =>
      (List.range 9).all fun x =>
        (sublists (List.range u)).all fun ys =>
          (List.range (u + 2)).all fun lo =>
            (List.range (u + 2)).all fun hi =>
              if decide (ys.Pairwise (fun a b : Nat => a ≤ b)) &&
                  ys.all (fun y => y < u) && ys.all (fun y => lo ≤ y && y < hi) then
                if (search.spans u t x).all (fun p => p.2 ≤ lo || hi ≤ p.1) then
                  decide (ys.filter (search.test t x) = [])
                else true
              else true

#eval boundedCheck

example : boundedCheck = true := by native_decide

/-- If every stored interval misses `[lo, hi)`, no key of that extent can be
    "apart" from the left key: a match on the right is covered by the upper
    slice `[x+t+1, u)`, a match on the left by the lower slice `[0, x-t)`. -/
theorem sound_PruneExtent : Apart.PruneExtent.Goal := by
  intro u t x ys lo hi hv hys hspan
  rw [List.filter_eq_nil_iff]
  intro y hy
  cases htest : search.test t x y with
  | false => simp
  | true =>
      have hyu : y < u := hv.2 y hy
      have hlo : lo ≤ y := (hys y hy).1
      have hhi : y < hi := (hys y hy).2
      have hcases : x + t < y ∨ y + t < x := by
        simpa [search] using htest
      rcases hcases with hright | hleft
      · -- `x + t < y`: the right-hand slice `(x+t+1, u)` contains y.
        have hb : x + t + 1 < u := by omega
        have hmem : (x + t + 1, u) ∈ search.spans u t x := by
          simp [search, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hb]
        rcases hspan (x + t + 1, u) hmem with hp | hp <;> omega
      · -- `y + t < x`: the left-hand slice `(0, min u (x-t))` contains y.
        have hxt : t < x := by omega
        have hyxt : y < x - t := by omega
        have hb : 0 < min u (x - t) := by omega
        have hmem : (0, min u (x - t)) ∈ search.spans u t x := by
          simp [search, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hb]
        rcases hspan (0, min u (x - t)) hmem with hp | hp <;> omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Apart_PruneExtent
