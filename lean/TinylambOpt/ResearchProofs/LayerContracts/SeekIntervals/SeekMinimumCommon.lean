import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common_SeekIntervals

/-!
An index range plan must cover every qualifying key and preserve any gaps excluded by
the SQL predicate.

Named results include `start_span`, `countP_rising_of_sep`, `separated_clamped_strict`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.SeekMinimumCommon

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common

theorem start_span (s : Span) (hsl : s.1 < s.2) (x : Nat) :
    (inside s x && (x = 0 || !inside s (x-1))) = (s.1 == x) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    simp only [Bool.and_eq_true, Bool.or_eq_true, Bool.not_eq_true, beq_iff_eq] at h
    rcases h with ⟨hx, hx0 | hprev⟩
    · have hx' : s.1 ≤ x ∧ x < s.2 := by simpa [inside] using hx
      have hx0' : x = 0 := by simpa using hx0
      subst x
      have hs10 : s.1 = 0 := by omega
      simpa [hs10]
    · have hx' : s.1 ≤ x ∧ x < s.2 := by simpa [inside] using hx
      have hp' : x-1 < s.1 ∨ s.2 ≤ x-1 := by simpa [inside] using hprev
      have hs1x : s.1 = x := by rcases hp' with hp1 | hp2 <;> omega
      simpa [hs1x]
  · intro h
    have hx : s.1 = x := by simpa using h
    subst x
    have hsxin : inside s s.1 = true := by simp [inside, hsl]
    have hstart : (s.1 = 0 || !inside s (s.1-1)) = true := by
      by_cases h0 : s.1 = 0
      · simp [h0]
      · have hf : inside s (s.1 - 1) = false := by
          apply Bool.eq_false_iff.mpr
          intro hh
          simp [inside] at hh
          omega
        simp [h0, hf]
    simp [hsxin, hstart]

theorem countP_rising_of_sep (rs : List Span) (cap : Nat)
    (hsep : rs.Pairwise (fun s t : Span => s.2 < t.1))
    (hne : ∀ s ∈ rs, s.1 < s.2)
    (hle : ∀ s ∈ rs, s.2 ≤ cap) :
    (List.range cap).countP (fun x => rs.any (inside · x) && (x = 0 || !rs.any (inside · (x-1)))) = rs.length := by
  induction rs with
  | nil => simp
  | cons s rs ih =>
      rw [List.pairwise_cons] at hsep
      have hsl : s.1 < s.2 := hne s (by simp)
      have hs2cap : s.2 ≤ cap := hle s (by simp)
      have hnoRprev : ∀ y : Nat, inside s (y+1) = true → rs.any (inside · y) = false := by
        intro y hy
        apply Bool.eq_false_iff.mpr
        intro hr
        rw [List.any_eq_true] at hr
        rcases hr with ⟨t, ht, hty⟩
        have hst := hsep.1 t ht
        simp [inside] at hy hty
        omega
      have hnoSprev : ∀ y : Nat, rs.any (inside · (y+1)) = true → inside s y = false := by
        intro y hy
        apply Bool.eq_false_iff.mpr
        intro hs
        rw [List.any_eq_true] at hy
        rcases hy with ⟨t, ht, hty⟩
        have hst := hsep.1 t ht
        simp [inside] at hs hty
        omega
      have hpoint : ∀ x : Nat,
          ((s :: rs).any (inside · x) && (x = 0 || !(s :: rs).any (inside · (x-1)))) =
          ((s.1 == x) || (rs.any (inside · x) && (x = 0 || !rs.any (inside · (x-1))))) := by
        intro x
        by_cases hx0 : x = 0
        · subst x
          have hs0 : inside s 0 = (s.1 == 0) := by
            by_cases h0 : s.1 = 0
            · have : 0 < s.snd := by omega
              simp [inside, h0, this]
            · simp [inside, h0]
          simp [List.any_cons, hs0]
        · have hx0' : x = (x-1)+1 := by omega
          by_cases hsx : inside s x = true
          · have hprevR : rs.any (inside · (x-1)) = false := by
              exact hnoRprev (x-1) (by simpa [show (x-1)+1 = x by omega] using hsx)
            have hA : (x = 0 || !inside s (x-1)) = (s.1 == x) := by
              have hh := start_span s hsl x
              simpa [hsx] using hh
            have hrsx_false : rs.any (inside · x) = false := by
              apply Bool.eq_false_iff.mpr
              intro hr
              rw [List.any_eq_true] at hr
              rcases hr with ⟨t, ht, htx⟩
              have hst := hsep.1 t ht
              simp [inside] at htx hsx
              omega
            simp [List.any_cons, hsx, hprevR, hA, hrsx_false]
          · by_cases hrsx : rs.any (inside · x) = true
            · have hprevS : inside s (x-1) = false := by
                exact hnoSprev (x-1) (by simpa [show (x-1)+1 = x by omega] using hrsx)
              have hs1x_false : (s.1 == x) = false := by
                apply Bool.eq_false_iff.mpr
                intro he
                have he' : s.1 = x := by simpa using he
                subst x
                exact hsx (by simp [inside, hsl])
              simp [List.any_cons, hsx, hrsx, hprevS, hx0, hs1x_false]
            · have hs1x_false : (s.1 == x) = false := by
                apply Bool.eq_false_iff.mpr
                intro he
                have he' : s.1 = x := by simpa using he
                subst x
                exact hsx (by simp [inside, hsl])
              simp [List.any_cons, hsx, hrsx, hs1x_false]
      rw [show (List.range cap).countP (fun x => (s :: rs).any (inside · x) && (x = 0 || !(s :: rs).any (inside · (x-1)))) =
          (List.range cap).countP (fun x => (s.1 == x) || (rs.any (inside · x) && (x = 0 || !rs.any (inside · (x-1))))) by
        apply List.countP_congr
        intro x hx
        exact Bool.eq_iff_iff.mp (hpoint x)]
      rw [countP_or_of_disjoint]
      · rw [ih hsep.2 (by intro t ht; exact hne t (by simp [ht])) (by intro t ht; exact hle t (by simp [ht]))]
        have hs1cap : s.1 + 1 ≤ cap := by omega
        have h0 := countP_span_range_le s.1 (s.1+1) cap hs1cap
        have h1 : (List.range cap).countP (inside (s.1, s.1+1)) = 1 := by simpa using h0
        have hc : (List.range cap).countP (fun x => s.1 == x) = 1 := by
          rw [← h1]
          apply List.countP_congr
          intro x hx
          simp [inside]
          omega
        rw [hc]
        simp
        omega
      · intro x hx
        apply Bool.eq_false_iff.mpr
        intro hq
        have hx1 : s.1 = x := by simpa using hx
        have hq' : rs.any (inside · x) = true ∧ (x = 0 || !rs.any (inside · (x-1))) = true := by
          simpa only [Bool.and_eq_true] using hq
        have hrsx : rs.any (inside · x) = true := hq'.1
        rw [List.any_eq_true] at hrsx
        rcases hrsx with ⟨t, ht, htx⟩
        have hst := hsep.1 t ht
        simp [inside] at htx ⊢
        omega

theorem separated_clamped_strict (ps : List Span) (cap : Nat)
    (hsep : ps.Pairwise (fun s t : Span => s.2 < t.1)) :
    ((ps.map (fun (l,h) => (min l cap, min h cap))).filter (fun (l,h) => l < h)).Pairwise
      (fun s t : Span => s.2 < t.1) := by
  induction ps with
  | nil => simp
  | cons s ps ih =>
      rcases s with ⟨l,h⟩
      rw [List.pairwise_cons] at hsep
      rw [List.map_cons, List.filter_cons]
      by_cases hm : min l cap < min h cap
      · simp [hm]
        constructor
        · intro a b x x1 hmem ha hb hab
          have hlt : h < x := hsep.1 (x, x1) hmem
          subst a
          subst b
          omega
        · apply ih hsep.2
      · simp [hm, ih hsep.2]

theorem ranges_nonempty (m : Model) (p : Args) : ∀ s ∈ ranges m p, s.1 < s.2 := by
  intro s hs
  unfold ranges at hs
  have hb := (List.mem_filter.mp hs).2
  simpa using hb

theorem ranges_upper_le (m : Model) (p : Args) : ∀ s ∈ ranges m p, s.2 ≤ p.cap := by
  intro s hs
  unfold ranges at hs
  have hm : s ∈ (m.plan p).map (fun (l,h) => (min l p.cap, min h p.cap)) := (List.mem_filter.mp hs).1
  rw [List.mem_map] at hm
  rcases hm with ⟨⟨l,h⟩, hl, rfl⟩
  exact Nat.min_le_right _ _

theorem seekMinimum_of_exact_strict (m : Model) (hex : exactSeek m)
    (hsep : ∀ p, (m.plan p).Pairwise (fun s t : Span => s.2 < t.1)) : seekMinimum m := by
  intro p
  symm
  rw [runs_eq_countP_rising]
  have hcongr : (List.range p.cap).countP (fun x => m.test p x && (x = 0 || !m.test p (x-1))) =
      (List.range p.cap).countP (fun x => (ranges m p).any (inside · x) && (x = 0 || !(ranges m p).any (inside · (x-1)))) := by
    apply List.countP_congr
    intro x hx
    have hxcap : x < p.cap := by simpa using (List.mem_range.mp hx)
    by_cases hx0 : x = 0
    · subst x
      simp [hex p 0, hxcap]
    · have hxprevcap : x-1 < p.cap := by omega
      have h1 := hex p x
      have h2 := hex p (x-1)
      simp [h1, h2, hxcap, hxprevcap]
  rw [hcongr]
  exact countP_rising_of_sep (ranges m p) p.cap
    (by unfold ranges; apply separated_clamped_strict; exact hsep p)
    (ranges_nonempty m p) (ranges_upper_le m p)

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.SeekMinimumCommon
