import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SharedPrefix_PruneExtent

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix

private def sublists : List Nat → List (List Nat)
  | [] => [[]]
  | a :: as => sublists as ++ (sublists as).map (a :: ·)

/-- Truth check: enumerate sorted right lists over small domains, all left
    parameters and all candidate extents [lo, hi). -/
private def boundedCheck : Bool :=
  (List.range 7).all fun u =>
    (List.range 5).all fun t =>
      (List.range 9).all fun x =>
        (sublists (List.range u)).all fun ys =>
          (List.range (u + 2)).all fun lo =>
            (List.range (u + 2)).all fun hi =>
              if decide (ys.all (fun y => y < u)) &&
                  ys.all (fun y => lo ≤ y && y < hi) then
                if (search.spans u t x).all (fun p => p.2 ≤ lo || hi ≤ p.1) then
                  decide (ys.filter (search.test t x) = [])
                else true
              else true

#eval boundedCheck

example : search.spans 8 2 11 = [] := by native_decide
example : search.spans 12 2 4 = [(4, 8)] := by native_decide

/-- If the stored shared-prefix interval misses `[lo, hi)`, no key in that
    extent can share the high-order bits of the probe key. -/
theorem sound_PruneExtent :
    TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix.PruneExtent.Goal := by
  intro u t x ys lo hi hv hys hspan
  rw [List.filter_eq_nil_iff]
  intro y hy
  have hyu : y < u := hv.2 y hy
  have hlo : lo ≤ y := (hys y hy).1
  have hhi : y < hi := (hys y hy).2
  cases htest : search.test t x y with
  | false => simp
  | true =>
      have hxw : x / 2 ^ t = y / 2 ^ t := by
        simpa [search, beq_iff_eq] using htest
      let a := x / 2 ^ t * 2 ^ t
      let b := min u (a + 2 ^ t)
      have hya : a ≤ y := by
        dsimp [a]
        rw [hxw]
        exact Nat.div_mul_le_self y (2 ^ t)
      have hyb : y < b := by
        have hlt : y < y / (2 ^ t) * (2 ^ t) + (2 ^ t) :=
          Nat.lt_div_mul_add (Nat.two_pow_pos t)
        rw [← hxw] at hlt
        dsimp [b, a]
        exact Nat.lt_min.mpr ⟨hyu, hlt⟩
      have hab : a < b := by omega
      have hab' : x / 2 ^ t * 2 ^ t <
          min u (x / 2 ^ t * 2 ^ t + 2 ^ t) := by
        simpa [a, b] using hab
      have hspans : search.spans u t x = [(a, b)] := by
        simp [search, a, b,
          TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hab']
      have hp := hspan (a, b) (by simp [hspans])
      rcases hp with hp | hp <;> omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SharedPrefix_PruneExtent
