import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `Following.PruneExtent` — Elide a posting list when its stored endpoints miss every
  interval.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Following_PruneExtent

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following

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

example : search.spans 12 2 4 = [(5, 7)] := by native_decide

theorem sound_PruneExtent : Following.PruneExtent.Goal := by
  intro u t x ys lo hi hvalid hbounds hspan
  rw [List.filter_eq_nil_iff]
  intro y hy
  have hyu : y < u := hvalid.2 y hy
  have hlo : lo ≤ y := (hbounds y hy).1
  have hhi : y < hi := (hbounds y hy).2
  have hnot : search.test t x y ≠ true := by
    intro ht
    simp only [search, Bool.and_eq_true, decide_eq_true_eq] at ht
    rcases ht with ⟨hxy, hyle⟩
    have hx1 : x + 1 ≤ y := by omega
    have hylim : y < x + t + 1 := by omega
    have hyltmin : y < min u (x + t + 1) := by omega
    have hx1ltmin : x + 1 < min u (x + t + 1) := by omega
    have hmem : (x + 1, min u (x + t + 1)) ∈ search.spans u t x := by
      simp [search, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hx1ltmin]
    rcases hspan (x + 1, min u (x + t + 1)) hmem with hle | hge
    · omega
    · omega
  cases h : search.test t x y <;> simp_all

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Following_PruneExtent
