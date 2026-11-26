import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_PruneExtent

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass

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

theorem sound_PruneExtent : ModuloClass.PruneExtent.Goal := by
  intro u t x ys lo hi hv hbounds hspan
  rw [List.filter_eq_nil_iff]
  intro y hy
  cases htest : search.test t x y with
  | false => simp
  | true =>
      let w := t + 1
      let r := x % w
      have hw : 0 < w := by omega
      have hxy : x % (t + 1) = y % (t + 1) := by
        simpa [search, beq_iff_eq] using htest
      have hry : y % w = r := by
        simpa [w, r] using hxy.symm
      have hru : r < w := Nat.mod_lt _ hw
      have hyu : y < u := hv.2 y hy
      have hdecomp : y = r + (y / w) * w := by
        have hdiv := Nat.mod_add_div y w
        rw [hry, Nat.mul_comm w (y / w)] at hdiv
        exact hdiv.symm
      have hmul : (y / w + 1) * w ≤ u + w - 1 - r := by
        rw [Nat.succ_mul]
        omega
      have hidx : y / w < (u + w - 1 - r) / w := by
        have hsucc : y / w + 1 ≤ (u + w - 1 - r) / w :=
          (Nat.le_div_iff_mul_le hw).mpr hmul
        omega
      have hmem : (y, y + 1) ∈ search.spans u t x := by
        change (y, y + 1) ∈ (List.range ((u + w - 1 - r) / w)).map
          (fun i => (r + i * w, r + i * w + 1))
        refine List.mem_map.mpr ⟨y / w, List.mem_range.mpr hidx, ?_⟩
        exact Prod.ext hdecomp.symm (by omega)
      have hlelo : lo ≤ y := (hbounds y hy).1
      have hyhi : y < hi := (hbounds y hy).2
      rcases hspan (y, y + 1) hmem with hle | hge
      · omega
      · omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_PruneExtent
