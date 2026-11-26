import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Annulus_PruneExtent

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus

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

#eval boundedCheck

#eval (search.spans 12 2 5)
#eval (List.range 12).filter (fun y => search.test 2 5 y)

theorem sound_PruneExtent : Annulus.PruneExtent.Goal := by
  intro u t x ys lo hi hvalid hbounds hspan
  rw [List.filter_eq_nil_iff]
  intro y hy htest
  have hyu : y < u := hvalid.2 y hy
  have hlo : lo ≤ y := (hbounds y hy).1
  have hhi : y < hi := (hbounds y hy).2
  by_cases hxy : y ≤ x
  · -- y lies to the left of x: the first slice covers it.
    have hyx0 : y - x = 0 := Nat.sub_eq_zero_of_le hxy
    simp [search, hyx0] at htest
    have hylim : y < x - t := by omega
    have hlow : x - (2 * t + 1) ≤ y := by omega
    have hymin : y < min u (x - t) := by omega
    have hab : x - (2 * t + 1) < min u (x - t) := by omega
    have hmem : (x - (2 * t + 1), min u (x - t)) ∈ search.spans u t x := by
      simp [search, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hab]
    rcases hspan (x - (2 * t + 1), min u (x - t)) hmem with hle | hge
    · omega
    · omega
  · -- y lies to the right of x: the second slice covers it.
    have hxy' : x < y := Nat.lt_of_not_ge hxy
    have hxsub0 : x - y = 0 := Nat.sub_eq_zero_of_le (Nat.le_of_lt hxy')
    simp [search, hxsub0] at htest
    have hlow : x + t + 1 ≤ y := by omega
    have hylim : y < x + 2 * t + 2 := by omega
    have hymin : y < min u (x + 2 * t + 2) := by omega
    have hab : x + t + 1 < min u (x + 2 * t + 2) := by omega
    have hmem : (x + t + 1, min u (x + 2 * t + 2)) ∈ search.spans u t x := by
      simp [search, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hab]
    rcases hspan (x + t + 1, min u (x + 2 * t + 2)) hmem with hle | hge
    · omega
    · omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Annulus_PruneExtent
