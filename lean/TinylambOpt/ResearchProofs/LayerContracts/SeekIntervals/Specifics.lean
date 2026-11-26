import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common_SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores

/-!
An index range plan must cover every qualifying key and preserve any gaps excluded by
the SQL predicate.

Proved (20 claims, including the following)

- `DistanceBand.LowerClip` — `∀ p, p.a ≤ p.b → model.test p 0`
- `DistanceBand.ZeroRadius` — `∀ p x, p.b = 0 → (model.test p x = true ↔ x = p.a)`
- `MinClamp.EmptyAbove` — `∀ p, p.a < p.b → ranges model p = []`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores

theorem notEqual_hole : NotEqual.Hole.Goal := by intro p; simp [NotEqual.model]

theorem notEqual_twoSeeks : NotEqual.TwoSeeks.Goal := by
  intro p ha hb
  unfold ranges
  have h1 : p.a ≤ p.cap := by omega
  have h2 : p.a + 1 ≤ p.cap := by omega
  simp [NotEqual.model, Nat.min_def, h1, h2, ha, hb]

theorem outsideClosed_reversed : OutsideClosed.Reversed.Goal := by
  intro p x hba
  simp [OutsideClosed.model]
  omega

theorem outsideClosed_equalEnds : OutsideClosed.EqualEnds.Goal := by
  intro p h
  unfold refScan
  simp [OutsideClosed.model, h]

theorem ringSlice_sameEdge : RingSlice.SameEdge.Goal := by
  intro p h
  unfold ranges
  simp [RingSlice.model, h]

theorem countP_lt_range (k cap : Nat) (hk : k ≤ cap) :
    (List.range cap).countP (fun x => x < k) = k := by
  have h0 := countP_span_range_le 0 k cap hk
  have h : (List.range cap).countP (inside (0,k)) = k := by simpa using h0
  calc
    (List.range cap).countP (fun x => x < k) = (List.range cap).countP (inside (0,k)) := by
      apply List.countP_congr
      intro x hx
      simp [inside]
    _ = k := h

theorem countP_ge_range (k cap : Nat) (hk : k ≤ cap) :
    (List.range cap).countP (fun x => k ≤ x) = cap - k := by
  induction cap generalizing k with
  | zero => simp
  | succ cap ih =>
      rw [List.range_succ, List.countP_append]
      by_cases h : k ≤ cap
      · rw [ih k h]
        simp [h]
        omega
      · have hk' : k = cap + 1 := by omega
        subst k
        simp
        intro a ha
        omega

theorem ringSlice_wrapSize : RingSlice.WrapSize.Goal := by
  intro p hba hac
  unfold refScan
  rw [← List.countP_eq_length_filter]
  have hab : ¬ p.a ≤ p.b := by omega
  have hcongr : (List.range p.cap).countP (fun x => x < p.cap && RingSlice.model.test p x) =
      (List.range p.cap).countP (fun x => x < p.b || p.a ≤ x) := by
    apply List.countP_congr
    intro x hx
    have hxcap : x < p.cap := by simpa using (List.mem_range.mp hx)
    simp [RingSlice.model, hab, hxcap]
  rw [hcongr]
  rw [countP_or_of_disjoint]
  · rw [countP_lt_range p.b p.cap (by omega)]
    rw [countP_ge_range p.a p.cap hac]
    omega
  · intro x hx
    simp at hx
    have : ¬ p.a ≤ x := by omega
    simp [this]

theorem distanceBand_zeroRadius : DistanceBand.ZeroRadius.Goal := by
  intro p x hb
  simp [DistanceBand.model, hb]
  constructor <;> intro h <;> omega

theorem distanceBand_lowerClip : DistanceBand.LowerClip.Goal := by
  intro p h
  simp [DistanceBand.model]
  omega

theorem quotientBand_quotientEdge : QuotientBand.QuotientEdge.Goal := by
  intro p
  simp [QuotientBand.model]

theorem quotientBand_fullBucket : QuotientBand.FullBucket.Goal := by
  intro p hcap
  unfold refScan
  rw [← List.countP_eq_length_filter]
  have hcongr : (List.range p.cap).countP (fun x => x < p.cap && QuotientBand.model.test p x) =
      (List.range p.cap).countP (inside (p.a*(p.b+1), (p.a+1)*(p.b+1))) := by
    apply List.countP_congr
    intro x hx
    have hxcap : x < p.cap := by simpa using (List.mem_range.mp hx)
    simp [QuotientBand.model, div_band, inside, hxcap]
  rw [hcongr, countP_span_range_le]
  · have : (p.a+1)*(p.b+1) - p.a*(p.b+1) = p.b+1 := by
      rw [Nat.succ_mul]
      omega
    rw [this]
  · exact hcap

theorem residue_invalidDigit : Residue.InvalidDigit.Goal := by
  intro p h
  unfold ranges
  simp [Residue.model, h, Function.comp_def]
  intro
  omega

theorem residue_period : Residue.Period.Goal := by
  intro p x
  simp [Residue.model]
  rw [show x + p.b + 1 = x + (p.b+1) by omega]
  rw [Nat.add_mod_right]

theorem twoLiterals_duplicateLiteral : TwoLiterals.DuplicateLiteral.Goal := by
  intro p h hcap
  unfold ranges
  have he : [p.a, p.b].eraseDups = [p.a] := by
    rw [h]
    rw [List.eraseDups_cons]
    simp
  have hcapa : p.a < p.cap := by omega
  have hcapa1 : p.a + 1 ≤ p.cap := by omega
  simp [TwoLiterals.model, he, Nat.min_def, Nat.le_of_lt hcapa, hcapa1]

theorem twoLiterals_adjacentMerge : TwoLiterals.AdjacentMerge.Goal := by
  intro p hb hcap
  rw [runs_eq_countP_rising]
  have hcnt := countP_span_range_le p.a (p.a+1) p.cap (by omega)
  have hcnt1 : (List.range p.cap).countP (inside (p.a,p.a+1)) = 1 := by simpa using hcnt
  rw [← hcnt1]
  apply List.countP_congr
  intro x hx
  simp [TwoLiterals.model, inside]
  by_cases hxa : x = p.a <;> by_cases hxb : x = p.a+1 <;> simp [hxa, hxb, hb] <;> omega

theorem saturatingSubtract_plateau : SaturatingSubtract.Plateau.Goal := by
  intro p hb x hx
  simp [SaturatingSubtract.model, hb]
  omega

theorem saturatingSubtract_strictTail : SaturatingSubtract.StrictTail.Goal := by
  intro p x hb
  simp [SaturatingSubtract.model]
  constructor
  · intro h
    by_cases hxa : p.a ≤ x
    · omega
    · have : x - p.a = 0 := Nat.sub_eq_zero_of_le (Nat.le_of_lt (Nat.lt_of_not_ge hxa))
      omega
  · intro h
    subst x
    simp [hb]

theorem minClamp_emptyAbove : MinClamp.EmptyAbove.Goal := by
  intro p h
  unfold ranges
  simp [MinClamp.model, h, Nat.not_lt_of_ge (Nat.le_of_lt h)]
  intro
  omega

theorem minClamp_plateauStart : MinClamp.PlateauStart.Goal := by
  intro p x h
  simp [MinClamp.model, h]
  constructor <;> intro hx <;> omega

theorem xorOne_involutive (x : Nat) : (x ^^^ 1) ^^^ 1 = x := by
  have hdm := Nat.div_add_mod x 2
  by_cases hx : x % 2 = 0
  · rw [xor_one_even x hx]
    have hodd : (x+1) % 2 = 1 := by omega
    rw [xor_one_odd (x+1) hodd]
    omega
  · have hx1 : x % 2 = 1 := by omega
    rw [xor_one_odd x hx1]
    by_cases hx0 : x = 0
    · subst x
      simp at hx1
    · have heven : (x-1) % 2 = 0 := by omega
      rw [xor_one_even (x-1) heven]
      omega

theorem xorLowBit_pairClosed : XorLowBit.PairClosed.Goal := by
  intro p x ha hb
  simp [XorLowBit.model]
  have hda := Nat.div_add_mod p.a 2
  have hdb := Nat.div_add_mod p.b 2
  have hdx := Nat.div_add_mod x 2
  have ha2 : 2*(p.a/2) = p.a := by omega
  have hb2 : 2*(p.b/2) = p.b - 1 := by omega
  by_cases hx : x % 2 = 0
  · rw [xor_one_even x hx]
    have hx2 : 2*(x/2) = x := by omega
    apply Bool.eq_iff_iff.mpr
    simp
    omega
  · have hx1 : x % 2 = 1 := by omega
    rw [xor_one_odd x hx1]
    have hx2 : 2*(x/2) = x - 1 := by omega
    apply Bool.eq_iff_iff.mpr
    simp
    omega

theorem xorLowBit_singletonSwap : XorLowBit.SingletonSwap.Goal := by
  intro p
  have hx : (p.a ^^^ 1) ^^^ 1 = p.a := xorOne_involutive p.a
  simp [XorLowBit.model, hx]
  intro
  omega

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics
