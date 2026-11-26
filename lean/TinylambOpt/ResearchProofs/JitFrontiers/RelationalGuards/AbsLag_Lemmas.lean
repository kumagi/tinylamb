import TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.AbsLagLemmas

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas

abbrev al := TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards.AbsLag.expr

theorem natAbs_le_max' (A B v : Int) (hlo : A ≤ v) (hhi : v ≤ B) :
    (v.natAbs : Int) ≤ ((max A.natAbs B.natAbs : Nat) : Int) := by
  by_cases hAB : A.natAbs ≤ B.natAbs
  · rw [Nat.max_eq_right hAB]
    rcases Int.natAbs_eq v with hv | hv <;>
    rcases Int.natAbs_eq B with hB | hB <;> omega
  · have hBA : B.natAbs ≤ A.natAbs := by omega
    rw [Nat.max_eq_left hBA]
    rcases Int.natAbs_eq v with hv | hv <;>
    rcases Int.natAbs_eq A with hA | hA <;> omega

theorem zero_le_natAbs (v : Int) : 0 ≤ (v.natAbs : Int) := by
  rcases Int.natAbs_eq v with h | h <;> omega

theorem upper_al {t : Int} {z : Zone} {p : Point} (hp : inZone z p = true) :
    al.score t p ≤ ((max (max z.dl (z.xl-z.yh) - t).natAbs
      (min z.dh (z.xh-z.yl) - t).natAbs : Nat) : Int) := by
  rcases score_range hp with ⟨hlo, hhi⟩
  change (((p.1-p.2-t).natAbs : Nat) : Int) ≤ _
  exact natAbs_le_max' _ _ _ (by omega) (by omega)

theorem hi_of_estimate {t : Int} {z : Zone} {l h : Int}
    (he : estimate al t z = some (l,h)) :
    h = ((max (max z.dl (z.xl-z.yh) - t).natAbs
      (min z.dh (z.xh-z.yl) - t).natAbs : Nat) : Int) := by
  classical
  by_cases hne : ∃ p, inZone z p = true
  · rcases minDiff_vertex hne with ⟨vlo, hvlo0, _, hslo⟩
    rcases maxDiff_vertex hne with ⟨vhi, hvhi0, _, hshi⟩
    have hvhi : vhi ∈ vertices z [t] := vertices_mono_ds (z := z) (ds := []) (ds' := [t]) (by simp) hvhi0
    have hvlo : vlo ∈ vertices z [t] := vertices_mono_ds (z := z) (ds := []) (ds' := [t]) (by simp) hvlo0
    have hbnd : ∀ e, e ∈ (vertices z [t]).map (al.score t) →
        e ≤ ((max (max z.dl (z.xl-z.yh) - t).natAbs (min z.dh (z.xh-z.yl) - t).natAbs : Nat) : Int) := by
      intro e he'
      rcases List.mem_map.mp he' with ⟨v, hv, rfl⟩
      exact upper_al (vertex_inZone hv)
    have hattain : ((max (max z.dl (z.xl-z.yh) - t).natAbs (min z.dh (z.xh-z.yl) - t).natAbs : Nat) : Int) ∈
        (vertices z [t]).map (al.score t) := by
      by_cases hAB : (max z.dl (z.xl-z.yh) - t).natAbs ≤ (min z.dh (z.xh-z.yl) - t).natAbs
      · rw [Nat.max_eq_right hAB]
        refine List.mem_map.mpr ⟨vhi, hvhi, ?_⟩
        change (((vhi.1-vhi.2-t).natAbs : Nat) : Int) = _
        rw [hshi]
      · have hBA : (min z.dh (z.xh-z.yl) - t).natAbs ≤ (max z.dl (z.xl-z.yh) - t).natAbs := by omega
        rw [Nat.max_eq_left hBA]
        refine List.mem_map.mpr ⟨vlo, hvlo, ?_⟩
        change (((vlo.1-vlo.2-t).natAbs : Nat) : Int) = _
        rw [hslo]
    have hmem : h ∈ (vertices z [t]).map (al.score t) :=
      (List.max?_eq_some_iff.mp (bounds_eq_some_iff.mp he).2).1
    have hle : h ≤ ((max (max z.dl (z.xl-z.yh) - t).natAbs (min z.dh (z.xh-z.yl) - t).natAbs : Nat) : Int) := hbnd h hmem
    have hge : ((max (max z.dl (z.xl-z.yh) - t).natAbs (min z.dh (z.xh-z.yl) - t).natAbs : Nat) : Int) ≤ h :=
      (List.max?_eq_some_iff.mp (bounds_eq_some_iff.mp he).2).2 _ hattain
    omega
  · rw [(deadEdge_general al t z).2 (by
        intro p; by_cases hp : inZone z p = true
        · exact False.elim (hne ⟨p, hp⟩)
        · simpa using hp)] at he
    contradiction

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.AbsLagLemmas
