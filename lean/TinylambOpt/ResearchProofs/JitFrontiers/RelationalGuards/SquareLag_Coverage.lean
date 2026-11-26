import TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.SquareLagCoverage

open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Common
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.CorrelatedSubLemmas
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.AbsLagLemmas
open TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.AbsLagCoverage

abbrev sl := TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards.SquareLag.expr
abbrev al0 := TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards.AbsLag.expr

theorem sq_of_natAbs (a : Int) : (((a.natAbs : Nat) : Int) * ((a.natAbs : Nat) : Int)) = a^2 := by
  calc
    ((a.natAbs : Nat) : Int) * ((a.natAbs : Nat) : Int) = ((a.natAbs * a.natAbs : Nat) : Int) := by simp
    _ = a*a := Int.natAbs_mul_self
    _ = a^2 := by rw [Int.pow_succ, Int.pow_one]

theorem sl_dom_hi (t : Int) (z : Zone) (p : Point) (hp : inZone z p = true) :
    ∃ v, v ∈ vertices z (sl.cuts t) ∧ sl.score t p ≤ sl.score t v := by
  rcases al_dom_hi t z p hp with ⟨v, hv, hle⟩
  have hc1 : al0.cuts t = [t] := by rfl
  have hc2 : sl.cuts t = [t] := by rfl
  rw [hc1] at hv
  refine ⟨v, by rwa [hc2], ?_⟩
  have hnn : 0 ≤ al0.score t p := by
    change 0 ≤ ((p.1-p.2-t).natAbs : Int)
    exact Int.natCast_nonneg _
  have hsq := Int.mul_self_le_mul_self hnn hle
  change (p.1-p.2-t)^2 ≤ (v.1-v.2-t)^2
  rw [← sq_of_natAbs (p.1-p.2-t), ← sq_of_natAbs (v.1-v.2-t)]
  exact hsq

theorem sl_dom_lo (t : Int) (z : Zone) (p : Point) (hp : inZone z p = true) :
    ∃ v, v ∈ vertices z (sl.cuts t) ∧ sl.score t v ≤ sl.score t p := by
  rcases al_dom_lo t z p hp with ⟨v, hv, hle⟩
  have hc1 : al0.cuts t = [t] := by rfl
  have hc2 : sl.cuts t = [t] := by rfl
  rw [hc1] at hv
  refine ⟨v, by rwa [hc2], ?_⟩
  have hnn : 0 ≤ al0.score t v := by
    change 0 ≤ ((v.1-v.2-t).natAbs : Int)
    exact Int.natCast_nonneg _
  have hsq := Int.mul_self_le_mul_self hnn hle
  change (v.1-v.2-t)^2 ≤ (p.1-p.2-t)^2
  rw [← sq_of_natAbs (v.1-v.2-t), ← sq_of_natAbs (p.1-p.2-t)]
  exact hsq

theorem minimalHull_sl : minimalHull sl := minimalHull_of_dom sl_dom_hi sl_dom_lo

theorem sq_nonneg_sl (a : Int) : 0 ≤ a^2 := by
  have h : a^2 = ((a.natAbs : Nat) : Int) * ((a.natAbs : Nat) : Int) := by
    rw [Int.pow_succ, Int.pow_one, sq_of_natAbs a, Int.pow_succ, Int.pow_one]
  rw [h]
  exact Int.mul_nonneg (Int.natCast_nonneg _) (Int.natCast_nonneg _)

theorem score_nonneg_sl : ∀ t p, 0 ≤ sl.score t p := fun _ _ => sq_nonneg_sl _

theorem coverage_sl : ∀ t z p, inZone z p = true → ∀ lo hi, estimate sl t z = some (lo,hi) →
    lo ≤ sl.score t p ∧ sl.score t p ≤ hi := coverage_of_dom sl_dom_hi sl_dom_lo

theorem squareHull_sl : ∀ z t l h, estimate al0 t z = some (l,h) → estimate sl t z = some (l*l,h*h) := by
  intro z t l h he
  classical
  by_cases hne : ∃ p, inZone z p = true
  · have hMal := minimalHull_al t z
    have hMsl := minimalHull_sl t z
    have hal : extrema al0 t z = some (l,h) := by rwa [← hMal]
    have halb : bounds ((points z).map (al0.score t)) = some (l,h) := by simpa [extrema] using hal
    have hnl : 0 ≤ l := by
      have hm : l ∈ (points z).map (al0.score t) := by
        simpa [extrema] using bounds_min_mem (vs := (points z).map (al0.score t)) hal
      rcases List.mem_map.mp hm with ⟨p, hp, rfl⟩
      change 0 ≤ ((p.1-p.2-t).natAbs : Int)
      exact Int.natCast_nonneg _
    have hsqext : bounds ((points z).map (sl.score t)) = some (l*l, h*h) := by
      apply bounds_eq_minmax
      · rcases List.mem_map.mp (bounds_min_mem halb) with ⟨p, hp, hpval⟩
        refine List.mem_map.mpr ⟨p, hp, ?_⟩
        change (p.1-p.2-t)^2 = l*l
        change ((p.1-p.2-t).natAbs : Int) = l at hpval
        rw [← sq_of_natAbs (p.1-p.2-t), hpval]
      · intro e he
        rcases List.mem_map.mp he with ⟨p, hp, rfl⟩
        have hcov := extrema_covers (f := al0) (t := t) (z := z) (p := p) (List.mem_filter.mp hp).2 hal
        have hnnp : 0 ≤ al0.score t p := by
          change 0 ≤ ((p.1-p.2-t).natAbs : Int)
          exact Int.natCast_nonneg _
        have hnnl : 0 ≤ l := hnl
        have hsq := Int.mul_self_le_mul_self hnnl hcov.1
        change l*l ≤ (p.1-p.2-t)^2
        rw [← sq_of_natAbs (p.1-p.2-t)]
        exact hsq
      · rcases List.mem_map.mp (bounds_max_mem halb) with ⟨p, hp, hpval⟩
        refine List.mem_map.mpr ⟨p, hp, ?_⟩
        change (p.1-p.2-t)^2 = h*h
        change ((p.1-p.2-t).natAbs : Int) = h at hpval
        rw [← sq_of_natAbs (p.1-p.2-t), hpval]
      · intro e he
        rcases List.mem_map.mp he with ⟨p, hp, rfl⟩
        have hcov := extrema_covers (f := al0) (t := t) (z := z) (p := p) (List.mem_filter.mp hp).2 hal
        have hnnp : 0 ≤ al0.score t p := by
          change 0 ≤ ((p.1-p.2-t).natAbs : Int)
          exact Int.natCast_nonneg _
        have hhnon : 0 ≤ h := by
          have hm : h ∈ (points z).map (al0.score t) := by
            simpa [extrema] using bounds_max_mem (vs := (points z).map (al0.score t)) hal
          rcases List.mem_map.mp hm with ⟨q, hq, rfl⟩
          change 0 ≤ ((q.1-q.2-t).natAbs : Int)
          exact Int.natCast_nonneg _
        have hsq := Int.mul_self_le_mul_self hnnp hcov.2
        change (p.1-p.2-t)^2 ≤ h*h
        rw [← sq_of_natAbs (p.1-p.2-t)]
        exact hsq
    have hesl : extrema sl t z = some (l*l,h*h) := by simpa [extrema] using hsqext
    simpa [hMsl] using hesl
  · have henone : estimate al0 t z = none := (deadEdge_general al0 t z).2 (by
      intro p
      by_cases hp : inZone z p = true
      · exact False.elim (hne ⟨p, hp⟩)
      · simpa using hp)
    rw [henone] at he
    contradiction

end TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.SquareLagCoverage
