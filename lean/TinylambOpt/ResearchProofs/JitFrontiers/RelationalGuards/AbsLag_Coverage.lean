import TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.AbsLagCoverage

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.AbsLagLemmas

abbrev al := TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards.AbsLag.expr

theorem al_dom_hi (t : Int) (z : Zone) (p : Point) (hp : inZone z p = true) :
    ∃ v, v ∈ vertices z (al.cuts t) ∧ al.score t p ≤ al.score t v := by
  have hne : ∃ q, inZone z q = true := ⟨p, hp⟩
  rcases score_range hp with ⟨hlo, hhi⟩
  rcases minDiff_vertex (z := z) hne with ⟨vlo, hvlo, _, hslo⟩
  rcases maxDiff_vertex (z := z) hne with ⟨vhi, hvhi, _, hshi⟩
  have hvlo' : vlo ∈ vertices z (al.cuts t) := vertices_mono_ds (z := z) (ds := []) (ds' := al.cuts t) (by intro d hd; simp at hd) hvlo
  have hvhi' : vhi ∈ vertices z (al.cuts t) := vertices_mono_ds (z := z) (ds := []) (ds' := al.cuts t) (by intro d hd; simp at hd) hvhi
  have hm : al.score t p ≤ ((max (max z.dl (z.xl-z.yh) - t).natAbs (min z.dh (z.xh-z.yl) - t).natAbs : Nat) : Int) :=
    upper_al (t := t) hp
  by_cases hAB : (max z.dl (z.xl-z.yh) - t).natAbs ≤ (min z.dh (z.xh-z.yl) - t).natAbs
  · refine ⟨vhi, hvhi', ?_⟩
    rw [Nat.max_eq_right hAB] at hm
    change al.score t p ≤ (((vhi.1-vhi.2-t).natAbs : Nat) : Int)
    rw [hshi]
    exact hm
  · have hBA : (min z.dh (z.xh-z.yl) - t).natAbs ≤ (max z.dl (z.xl-z.yh) - t).natAbs := by omega
    refine ⟨vlo, hvlo', ?_⟩
    rw [Nat.max_eq_left hBA] at hm
    change al.score t p ≤ (((vlo.1-vlo.2-t).natAbs : Nat) : Int)
    rw [hslo]
    exact hm

theorem al_dom_lo (t : Int) (z : Zone) (p : Point) (hp : inZone z p = true) :
    ∃ v, v ∈ vertices z (al.cuts t) ∧ al.score t v ≤ al.score t p := by
  have hne : ∃ q, inZone z q = true := ⟨p, hp⟩
  rcases score_range hp with ⟨hlo, hhi⟩
  rcases minDiff_vertex (z := z) hne with ⟨vlo, hvlo, _, hslo⟩
  rcases maxDiff_vertex (z := z) hne with ⟨vhi, hvhi, _, hshi⟩
  have hvlo' : vlo ∈ vertices z (al.cuts t) := vertices_mono_ds (z := z) (ds := []) (ds' := al.cuts t) (by intro d hd; simp at hd) hvlo
  have hvhi' : vhi ∈ vertices z (al.cuts t) := vertices_mono_ds (z := z) (ds := []) (ds' := al.cuts t) (by intro d hd; simp at hd) hvhi
  by_cases htlo : t ≤ max z.dl (z.xl-z.yh)
  · -- t is left of the interval: natAbs(d-t)=d-t is increasing, min at Dlo
    refine ⟨vlo, hvlo', ?_⟩
    change (((vlo.1-vlo.2-t).natAbs : Nat) : Int) ≤ (((p.1-p.2-t).natAbs : Nat) : Int)
    rw [hslo]
    rcases Int.natAbs_eq (max z.dl (z.xl-z.yh) - t) with hA | hA <;>
      rcases Int.natAbs_eq (p.1-p.2-t) with hP | hP <;> omega
  · by_cases hthi : t ≤ min z.dh (z.xh-z.yl)
    · -- t inside the interval: a vertex at d=t has score zero
      have hxlxh : z.xl ≤ z.xh := by rcases inZone_iff.mp hp with ⟨h1,h2,_⟩; omega
      have hylyh : z.yl ≤ z.yh := by rcases inZone_iff.mp hp with ⟨_,_,h1,h2,_⟩; omega
      rcases diff_vertex (z := z) (ds := al.cuts t) (d := t) hxlxh hylyh ⟨by omega, by omega⟩
          (by right; right; change t ∈ [t]; simp) with ⟨v, hv, hvz, hvs⟩
      refine ⟨v, hv, ?_⟩
      change (((v.1-v.2-t).natAbs : Nat) : Int) ≤ (((p.1-p.2-t).natAbs : Nat) : Int)
      rw [hvs]
      simp
    · -- t right of the interval: natAbs(d-t)=t-d is decreasing, min at Dhi
      refine ⟨vhi, hvhi', ?_⟩
      change (((vhi.1-vhi.2-t).natAbs : Nat) : Int) ≤ (((p.1-p.2-t).natAbs : Nat) : Int)
      rw [hshi]
      rcases Int.natAbs_eq (min z.dh (z.xh-z.yl) - t) with hA | hA <;>
        rcases Int.natAbs_eq (p.1-p.2-t) with hP | hP <;> omega

theorem minimalHull_al : minimalHull al := minimalHull_of_dom al_dom_hi al_dom_lo

theorem coverage_al : ∀ t z p, inZone z p = true → ∀ lo hi, estimate al t z = some (lo,hi) →
    lo ≤ al.score t p ∧ al.score t p ≤ hi := coverage_of_dom al_dom_hi al_dom_lo

theorem zeroPossible_al : ∀ t z hi, estimate al t z = some (0,hi) ↔
    (∃ p, inZone z p = true ∧ p.1-p.2=t) ∧ (estimate al t z).map Prod.snd = some hi := by
  intro t z hi
  constructor
  · intro he
    have hm : 0 ∈ (vertices z (al.cuts t)).map (al.score t) := by
      simpa [estimate] using bounds_min_mem (vs := (vertices z (al.cuts t)).map (al.score t)) he
    rcases List.mem_map.mp hm with ⟨v, hv, hvsc⟩
    have hvz := vertex_inZone hv
    refine ⟨⟨v, hvz, ?_⟩, ?_⟩
    · change ((v.1-v.2-t).natAbs : Int) = 0 at hvsc
      rcases Int.natAbs_eq (v.1-v.2-t) with h | h <;> omega
    · simp [he]
  · rintro ⟨⟨p, hp, hdp⟩, hsnd⟩
    rcases hE : estimate al t z with _ | ⟨l,h'⟩
    · have hf : False := by simp [hE] at hsnd
      exact False.elim hf
    · have hsnd' : h' = hi := by simpa [hE] using hsnd
      subst hi
      have hext : extrema al t z = some (l, h') := by
        rw [← minimalHull_al t z]
        exact hE
      have hpMem : p ∈ points z := point_mem_points hp
      have hscore : al.score t p = 0 := by
        change ((p.1-p.2-t).natAbs : Int) = 0
        rw [hdp]
        simp
      have hmin : ((points z).map (al.score t)).min? = some 0 := by
        apply List.min?_eq_some_iff.mpr
        constructor
        · exact List.mem_map.mpr ⟨p, hpMem, hscore⟩
        · intro e he
          rcases List.mem_map.mp he with ⟨q, hq, rfl⟩
          change 0 ≤ ((q.1-q.2-t).natAbs : Int)
          rcases Int.natAbs_eq (q.1-q.2-t) with h | h <;> omega
      have hmin' : l = 0 := by
        have hx : ((points z).map (al.score t)).min? = some l := (bounds_eq_some_iff.mp hext).1
        rw [hmin] at hx
        cases hx
        rfl
      subst l
      simp

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.AbsLagCoverage
