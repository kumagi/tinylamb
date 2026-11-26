import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

namespace TinylambOpt.ResearchProofs.StorageExecution.DerivedZones

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! ## Generic helpers -/

theorem inside_of_sound {f : Kernel} (hs : soundInterval f) {t : Nat} {p : List (Option Row)}
    {a b : Bounds} (hc : covers p a b) {r : Option Row} (hr : r ∈ p) {x y : Nat}
    (heq : r = some (x, y)) : inside (f.bounds t a b) (f.score t (x, y)) := by
  exact hs t a b x y (hc r hr x y heq).1 (hc r hr x y heq).2

theorem skipRange_of_sound {f : Kernel} (hs : soundInterval f) : skipRange f := by
  intro t p a b lo hi hc ha
  unfold selected scores
  rw [List.filter_eq_nil_iff]
  intro v hv
  simp only [List.mem_filterMap] at hv
  rcases hv with ⟨r, hrm, hr⟩
  rcases r with _ | ⟨x, y⟩
  · simp at hr
  · simp only [Option.map_some, Option.some.injEq] at hr
    subst hr
    have hin := inside_of_sound (t := t) hs hc hrm rfl
    simp only [Bool.and_eq_true, decide_eq_true_eq] at ha ⊢
    intro hvsel
    have hlo : lo ≤ (f.bounds t a b).hi := Nat.le_trans hvsel.1 hin.2
    have hhi : (f.bounds t a b).lo < hi := Nat.lt_of_le_of_lt hin.1 hvsel.2
    have hlt : lo < hi := Nat.lt_of_le_of_lt hvsel.1 hvsel.2
    simp [allows, hlo, hhi, hlt] at ha

theorem takeAll_of_sound {f : Kernel} (hs : soundInterval f) : takeAll f := by
  intro t p a b lo hi hc hlo hhi
  unfold selected
  rw [List.filter_eq_self]
  intro v hv
  simp only [List.mem_filterMap, scores] at hv
  rcases hv with ⟨r, hrm, hr⟩
  rcases r with _ | ⟨x, y⟩
  · simp at hr
  · simp only [Option.map_some, Option.some.injEq] at hr
    subst hr
    have hin := inside_of_sound (t := t) hs hc hrm rfl
    simp only [Bool.and_eq_true, decide_eq_true_eq]
    exact ⟨Nat.le_trans hlo hin.1, Nat.lt_of_le_of_lt hin.2 hhi⟩

theorem crossPageJoin_of_sound {f : Kernel} (hs : soundInterval f) : crossPageJoin f := by
  intro t u p q a b c d hcp hcq hlt x hx hxq
  simp only [scores, List.mem_filterMap] at hx hxq
  rcases hx with ⟨r, hrm, hr⟩
  rcases hxq with ⟨r2, hrm2, hr2⟩
  rcases r with _ | ⟨x1, y1⟩ <;> simp at hr
  rcases r2 with _ | ⟨x2, y2⟩ <;> simp at hr2
  subst hr
  have h1 := (inside_of_sound (t := t) hs hcp hrm rfl).2
  have h2 := (inside_of_sound (t := u) hs hcq hrm2 rfl).1
  rw [hr2] at h2
  omega

theorem semijoinFilter_of_sound {f : Kernel} (hs : soundInterval f) : semijoinFilter f := by
  intro t p a b ks hc
  simp only [semijoinFilter, scores]
  apply List.filter_congr
  intro v hv
  simp only [List.mem_filterMap] at hv
  rcases hv with ⟨row, hrm, hr⟩
  rcases row with _ | ⟨x, y⟩
  · simp at hr
  · simp only [Option.map_some, Option.some.injEq] at hr
    subst hr
    have hin := inside_of_sound (t := t) hs hc hrm rfl
    have hlo := hin.1
    have hhi := hin.2
    simp [List.contains_eq_mem, List.mem_filter, hlo, hhi]

theorem sumCeiling_of_sound {f : Kernel} (hs : soundInterval f) : sumCeiling f := by
  intro t p a b hc
  let vs := scores f t p
  let r := f.bounds t a b
  suffices hgoal : vs.length * r.lo ≤ vs.sum ∧ vs.sum ≤ vs.length * r.hi by
    simpa [sumCeiling, vs, r]
  have hmem : ∀ v ∈ vs, r.lo ≤ v ∧ v ≤ r.hi := by
    intro v hv
    simp only [vs, scores, List.mem_filterMap] at hv
    rcases hv with ⟨row, hrm, hr⟩
    rcases row with _ | ⟨x, y⟩
    · simp at hr
    · simp only [Option.map_some, Option.some.injEq] at hr
      subst hr
      exact inside_of_sound (t := t) hs hc hrm rfl
  constructor
  · clear hc hs
    have hlo : ∀ v ∈ vs, r.lo ≤ v := fun v hv => (hmem v hv).1
    clear hmem
    revert hlo
    induction vs with
    | nil => intro _; simp
    | cons hd tl ih =>
      intro hlo
      have hhd : r.lo ≤ hd := hlo hd (by simp)
      have ih' : tl.length * r.lo ≤ tl.sum := ih (fun w hw => hlo w (by simp [hw]))
      simp only [List.length_cons, List.sum_cons, Nat.add_mul, Nat.one_mul]
      omega
  · clear hc hs
    have hhi : ∀ v ∈ vs, v ≤ r.hi := fun v hv => (hmem v hv).2
    clear hmem
    revert hhi
    induction vs with
    | nil => intro _; simp
    | cons hd tl ih =>
      intro hhi
      have hhd : hd ≤ r.hi := hhi hd (by simp)
      have ih' : tl.sum ≤ tl.length * r.hi := ih (fun w hw => hhi w (by simp [hw]))
      simp only [List.length_cons, List.sum_cons, Nat.add_mul, Nat.one_mul]
      omega

theorem thresholdCount_of_sound {f : Kernel} (hs : soundInterval f) : thresholdCount f := by
  intro t ps cut hcov
  constructor
  · -- lower bound
    induction ps with
    | nil => simp [thresholdCount]
    | cons e es ih =>
      have hc : covers e.1 e.2.1 e.2.2 := hcov e (by simp)
      have ih' := ih (fun x hx => hcov x (List.mem_cons_of_mem e hx))
      simp only [List.filter_cons, List.map_cons, List.sum_cons, List.flatten_cons, List.filterMap_append,
        List.countP_append, List.map_append]
      by_cases hlo : cut ≤ (f.bounds t e.2.1 e.2.2).lo
      · simp only [hlo, decide_true, ↓reduceIte, List.map_cons, List.sum_cons]
        have hall : ∀ v ∈ e.1.filterMap (fun r => r.map (f.score t)), cut ≤ v := by
          intro v hv
          simp only [List.mem_filterMap] at hv
          rcases hv with ⟨r, hrm, hr⟩
          rcases r with _ | ⟨x, y⟩
          · simp at hr
          · simp only [Option.map_some, Option.some.injEq] at hr
            subst hr
            exact Nat.le_trans hlo (inside_of_sound (t := t) hs hc hrm rfl).1
        have hcount : (e.1.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut) =
            (e.1.filterMap (fun r => r.map (f.score t))).length := by
          rw [List.countP_eq_length_filter]
          congr 1
          rw [List.filter_eq_self]
          intro v hv; simp only [decide_eq_true_eq]; exact hall v hv
        have hlen_eq : (e.1.filterMap (fun r => r.map (f.score t))).length = (e.1.filterMap id).length := by
          induction e.1 with
          | nil => simp
          | cons r rs ih2 => cases r <;> simp [List.filterMap, ih2]
        have hne : (e.1.filterMap id).length ≤ (e.1.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut) := by
          rw [hcount, hlen_eq]; exact Nat.le_refl _
        have ih'' := ih'
        omega
      · simp only [hlo, decide_false, Bool.false_eq_true, ↓reduceIte]
        omega
  · -- upper bound
    induction ps with
    | nil => simp [thresholdCount]
    | cons e es ih =>
      have hc : covers e.1 e.2.1 e.2.2 := hcov e (by simp)
      have ih' := ih (fun x hx => hcov x (List.mem_cons_of_mem e hx))
      simp only [List.filter_cons, List.map_cons, List.sum_cons, List.flatten_cons, List.filterMap_append,
        List.countP_append, List.map_append]
      by_cases hhi : cut ≤ (f.bounds t e.2.1 e.2.2).hi
      · simp only [hhi, decide_true, ↓reduceIte, List.map_cons, List.sum_cons]
        have hlen_eq : (e.1.filterMap (fun r => r.map (f.score t))).length = (e.1.filterMap id).length := by
          induction e.1 with
          | nil => simp
          | cons r rs ih2 => cases r <;> simp [List.filterMap, ih2]
        have hle : (e.1.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut) ≤ (e.1.filterMap id).length := by
          calc (e.1.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut)
              ≤ (e.1.filterMap (fun r => r.map (f.score t))).length := List.countP_le_length
            _ = (e.1.filterMap id).length := hlen_eq
        omega
      · simp only [hhi, decide_false, Bool.false_eq_true, ↓reduceIte]
        have hzero : (e.1.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut) = 0 := by
          rw [List.countP_eq_zero]
          intro v hv
          simp only [decide_eq_true_eq]
          intro hge
          simp only [List.mem_filterMap] at hv
          rcases hv with ⟨r, hrm, hr⟩
          rcases r with _ | ⟨x, y⟩
          · simp at hr
          · simp only [Option.map_some, Option.some.injEq] at hr
            subst hr
            have hin := inside_of_sound (t := t) hs hc hrm rfl
            have := hin.2
            omega
        omega

theorem dominatedTop_of_sound {f : Kernel} (hs : soundInterval f) : dominatedTop f := by
  intro t p a b seen k hc hk
  have hbound : ∀ v ∈ scores f t p, v ≤ (f.bounds t a b).hi := by
    intro v hv
    simp only [scores, List.mem_filterMap] at hv
    rcases hv with ⟨r, hrm, hr⟩
    rcases r with _ | ⟨x, y⟩
    · simp at hr
    · simp only [Option.map_some, Option.some.injEq] at hr
      subst hr
      exact (inside_of_sound (t := t) hs hc hrm rfl).2
  let rhi := (f.bounds t a b).hi
  let ge : Nat → Nat → Bool := fun x1 x2 => decide (x1 ≥ x2)
  have htrans : ∀ a b c, ge a b = true → ge b c = true → ge a c = true := by
    intro a b c hab hbc; simp only [ge, ge_iff_le, decide_eq_true_eq] at hab hbc ⊢; omega
  have htotal : ∀ a b, (ge a b || ge b a) = true := by
    intro a b; simp only [ge, ge_iff_le, Bool.or_eq_true, decide_eq_true_eq]; omega
  let s1 := (seen ++ scores f t p).mergeSort ge
  let s2 := seen.mergeSort ge
  have hperm1 : s1.Perm (seen ++ scores f t p) := List.mergeSort_perm _ _
  have hperm2 : s2.Perm seen := List.mergeSort_perm _ _
  have hpair1 : s1.Pairwise (fun a b => ge a b = true) := List.pairwise_mergeSort htrans htotal _
  have hpair2 : s2.Pairwise (fun a b => ge a b = true) := List.pairwise_mergeSort htrans htotal _
  have hcount_scores : (scores f t p).countP (fun x => rhi < x) = 0 := by
    rw [List.countP_eq_zero]
    intro v hv
    simp only [decide_eq_true_eq]; intro hlt; exact Nat.not_lt.mpr (hbound v hv) hlt
  have hcount1 : k ≤ s1.countP (fun x => rhi < x) := by
    rw [hperm1.countP_eq, List.countP_append, hcount_scores]; omega
  have hcount2 : k ≤ s2.countP (fun x => rhi < x) := by
    rw [hperm2.countP_eq]; exact hk
  -- take k from a desc-sorted list where >=k elements are > rhi
  suffices htake : ∀ (l : List Nat), l.Pairwise (fun a b => ge a b = true) →
      k ≤ l.countP (fun x => rhi < x) →
      l.take k = (l.filter (fun x => rhi < x)).take k by
    rw [htake s1 hpair1 hcount1, htake s2 hpair2 hcount2]
    congr 1
    have hscores_filter : (scores f t p).filter (fun x => rhi < x) = [] := by
      rw [List.filter_eq_nil_iff]
      intro x hx; simp only [decide_eq_true_eq]; exact Nat.not_lt.mpr (hbound x hx)
    have happ : (seen ++ scores f t p).filter (fun x => rhi < x) = seen.filter (fun x => rhi < x) := by
      rw [List.filter_append, hscores_filter, List.append_nil]
    have hf1 : (s1.filter (fun x => rhi < x)).Perm (seen.filter (fun x => rhi < x)) :=
      (hperm1.filter _).trans (by rw [happ])
    have hf2 : (s2.filter (fun x => rhi < x)).Perm (seen.filter (fun x => rhi < x)) :=
      hperm2.filter _
    have hs1 : (s1.filter (fun x => rhi < x)).Pairwise (fun a b => ge a b = true) := hpair1.filter _
    have hs2 : (s2.filter (fun x => rhi < x)).Pairwise (fun a b => ge a b = true) := hpair2.filter _
    exact (hf1.trans hf2.symm).eq_of_pairwise (by
      intro a b ha hb hab hba
      simp only [ge, ge_iff_le, decide_eq_true_eq] at hab hba
      exact Nat.le_antisymm hba hab) hs1 hs2
  intro l hpw hkc
  -- all elements in take k are > rhi
  have hall : ∀ x ∈ l.take k, rhi < x := by
    intro x hx
    rw [List.mem_take_iff_getElem] at hx
    rcases hx with ⟨i, hi, hix⟩
    have hik : i < k := by simpa [List.length_take] using hi
    by_contra hng
    have hng' : l[i]'(by have := List.length_take (l:=l) (i:=k); omega) ≤ rhi := by omega
    have hall_after : ∀ m (hm : m < l.length), i ≤ m → l[m] ≤ rhi := by
      intro m hm him
      rcases Nat.eq_or_lt_of_le him with rfl | hlt
      · exact hng'
      · have hrel := hpw.rel_of_lt hlt
        simp only [ge, ge_iff_le, decide_eq_true_eq] at hrel
        exact Nat.le_trans hrel hng'
    have hcp : l.countP (fun x => rhi < x) ≤ i := by
      rw [← List.take_append_drop i l, List.countP_append]
      have hdrop : (l.drop i).countP (fun x => rhi < x) = 0 := by
        rw [List.countP_eq_zero]
        intro y hy
        simp only [decide_eq_true_eq]
        intro hgt
        obtain ⟨m, hm, rfl⟩ := List.getElem_of_mem hy
        have hml : i + m < l.length := by have h := List.length_drop (l:=l) (i:=i); omega
        have := hall_after (i + m) hml (Nat.le_add_right _ _)
        simp only [List.getElem_drop] at hgt
        omega
      rw [hdrop, Nat.add_zero]
      exact Nat.le_trans List.countP_le_length (by simp [List.length_take]; omega)
    omega
  have hft : (l.take k).filter (fun x => rhi < x) = l.take k :=
    List.filter_eq_self.mpr (by intro x hx; simp only [decide_eq_true_eq]; exact hall x hx)
  rw [← List.take_append_drop k l, List.filter_append, List.take_append, hft]
  by_cases hklen : k ≤ l.length
  · simp [List.length_take, hklen]
  · simp [List.take_of_length_le (Nat.le_of_not_ge hklen), List.drop_eq_nil_of_le (Nat.le_of_not_ge hklen)]

/-! ## Product -/

namespace Product
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product

theorem sound_SoundInterval : SoundInterval.Goal := by
  intro t a b x y hax hby
  unfold inside kernel
  simp only
  constructor
  · exact Nat.mul_le_mul hax.1 hby.1
  · exact Nat.mul_le_mul hax.2 hby.2

theorem sound_SkipRange : SkipRange.Goal := skipRange_of_sound sound_SoundInterval
theorem sound_TakeAll : TakeAll.Goal := takeAll_of_sound sound_SoundInterval
theorem sound_ThresholdCount : ThresholdCount.Goal := thresholdCount_of_sound sound_SoundInterval
theorem sound_DominatedTop : DominatedTop.Goal := dominatedTop_of_sound sound_SoundInterval
theorem sound_CrossPageJoin : CrossPageJoin.Goal := crossPageJoin_of_sound sound_SoundInterval

theorem sound_ProductDivisor : ProductDivisor.Goal := by
  intro x y k hx
  unfold kernel
  simp only
  rw [Nat.div_le_iff_le_mul_add_pred hx]
  constructor
  · intro hk
    have hkx : k + x - 1 = k + (x - 1) := by omega
    have hyx : y * x + x - 1 = x * y + (x - 1) := by rw [Nat.mul_comm y x]; omega
    omega
  · intro hk
    omega

theorem sound_SemijoinFilter : SemijoinFilter.Goal := semijoinFilter_of_sound sound_SoundInterval
theorem sound_SumCeiling : SumCeiling.Goal := sumCeiling_of_sound sound_SoundInterval

theorem sound_UpperWitness : UpperWitness.Goal := by
  intro a b
  rfl

end Product

/-! ## SaturatingLoss -/

namespace SaturatingLoss
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss

theorem sound_SoundInterval : SoundInterval.Goal := by
  intro t a b x y hax hby
  unfold inside kernel
  simp only
  constructor
  · exact Nat.le_trans (Nat.sub_le_sub_left (Nat.add_le_add_right hby.2 t) a.lo) (Nat.sub_le_sub_right hax.1 (y + t))
  · exact Nat.le_trans (Nat.sub_le_sub_right hax.2 (y + t)) (Nat.sub_le_sub_left (Nat.add_le_add_right hby.1 t) a.hi)

theorem sound_SkipRange : SkipRange.Goal := skipRange_of_sound sound_SoundInterval
theorem sound_TakeAll : TakeAll.Goal := takeAll_of_sound sound_SoundInterval
theorem sound_ThresholdCount : ThresholdCount.Goal := thresholdCount_of_sound sound_SoundInterval
theorem sound_DominatedTop : DominatedTop.Goal := dominatedTop_of_sound sound_SoundInterval
theorem sound_CrossPageJoin : CrossPageJoin.Goal := crossPageJoin_of_sound sound_SoundInterval

theorem sound_ThresholdInverse : ThresholdInverse.Goal := by
  intro t x y k hk
  unfold kernel
  simp only
  constructor
  · intro h
    have hle : y + t ≤ x := by
      by_contra hgt
      have hgt' : x < y + t := Nat.lt_of_not_ge hgt
      have : x - (y + t) = 0 := Nat.sub_eq_zero_of_le (Nat.le_of_lt hgt')
      omega
    omega
  · intro h
    omega

theorem sound_SemijoinFilter : SemijoinFilter.Goal := semijoinFilter_of_sound sound_SoundInterval
theorem sound_SumCeiling : SumCeiling.Goal := sumCeiling_of_sound sound_SoundInterval

theorem sound_ZeroCertificate : ZeroCertificate.Goal := by
  intro t a b
  unfold kernel
  simp only
  exact Nat.sub_eq_zero_iff_le

end SaturatingLoss

/-! ## Distance -/

namespace Distance
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance

theorem gap_le_gapMax {a b : Bounds} {x y : Nat} (hax : inside a x) (hby : inside b y) :
    gap x y ≤ gapMax a b := by
  unfold gap gapMax inside at *
  omega

theorem gapMin_le_gap {a b : Bounds} {x y : Nat} (hax : inside a x) (hby : inside b y) :
    gapMin a b ≤ gap x y := by
  unfold gap gapMin inside at *
  omega

theorem sound_SoundInterval : SoundInterval.Goal := by
  intro t a b x y hax hby
  unfold inside kernel
  simp only
  exact ⟨gapMin_le_gap hax hby, gap_le_gapMax hax hby⟩

theorem sound_SkipRange : SkipRange.Goal := skipRange_of_sound sound_SoundInterval
theorem sound_TakeAll : TakeAll.Goal := takeAll_of_sound sound_SoundInterval
theorem sound_ThresholdCount : ThresholdCount.Goal := thresholdCount_of_sound sound_SoundInterval
theorem sound_DominatedTop : DominatedTop.Goal := dominatedTop_of_sound sound_SoundInterval
theorem sound_CrossPageJoin : CrossPageJoin.Goal := crossPageJoin_of_sound sound_SoundInterval

theorem sound_BandInverse : BandInverse.Goal := by
  intro t x y
  unfold kernel gap
  simp only
  omega

theorem sound_SemijoinFilter : SemijoinFilter.Goal := semijoinFilter_of_sound sound_SoundInterval
theorem sound_SumCeiling : SumCeiling.Goal := sumCeiling_of_sound sound_SoundInterval

theorem sound_OverlapFloor : OverlapFloor.Goal := by
  intro a b
  unfold kernel gapMin
  simp only
  constructor
  · intro h
    rw [Nat.max_eq_zero_iff] at h
    omega
  · intro h
    rw [Nat.max_eq_zero_iff]
    omega

end Distance

/-! ## FloorRatio -/

namespace FloorRatio
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio

theorem sound_SoundInterval : SoundInterval.Goal := by
  intro t a b x y hax hby
  unfold inside kernel
  simp only
  constructor
  · apply Nat.le_trans (Nat.div_le_div_right (Nat.mul_le_mul_right t hax.1))
    exact Nat.div_le_div_left (Nat.add_le_add_right hby.2 1) (Nat.succ_pos _)
  · apply Nat.le_trans (Nat.div_le_div_right (Nat.mul_le_mul_right t hax.2))
    exact Nat.div_le_div_left (Nat.add_le_add_right hby.1 1) (Nat.succ_pos _)

theorem sound_SkipRange : SkipRange.Goal := skipRange_of_sound sound_SoundInterval
theorem sound_TakeAll : TakeAll.Goal := takeAll_of_sound sound_SoundInterval
theorem sound_ThresholdCount : ThresholdCount.Goal := thresholdCount_of_sound sound_SoundInterval
theorem sound_DominatedTop : DominatedTop.Goal := dominatedTop_of_sound sound_SoundInterval
theorem sound_CrossPageJoin : CrossPageJoin.Goal := crossPageJoin_of_sound sound_SoundInterval

theorem sound_DenominatorPage : DenominatorPage.Goal := by
  intro t a b k
  unfold kernel
  simp only
  exact Nat.le_div_iff_mul_le (Nat.succ_pos _)

theorem sound_SemijoinFilter : SemijoinFilter.Goal := semijoinFilter_of_sound sound_SoundInterval
theorem sound_SumCeiling : SumCeiling.Goal := sumCeiling_of_sound sound_SoundInterval

theorem sound_QuotientThreshold : QuotientThreshold.Goal := by
  intro t a b k
  unfold kernel
  simp only
  exact Nat.div_lt_iff_lt_mul (Nat.succ_pos _)

end FloorRatio

/-! ## BucketSum -/

namespace BucketSum
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum

theorem sound_SoundInterval : SoundInterval.Goal := by
  intro t a b x y hax hby
  unfold inside kernel
  simp only
  constructor
  · exact Nat.div_le_div_right (Nat.add_le_add hax.1 hby.1)
  · exact Nat.div_le_div_right (Nat.add_le_add hax.2 hby.2)

theorem sound_SkipRange : SkipRange.Goal := skipRange_of_sound sound_SoundInterval
theorem sound_TakeAll : TakeAll.Goal := takeAll_of_sound sound_SoundInterval
theorem sound_ThresholdCount : ThresholdCount.Goal := thresholdCount_of_sound sound_SoundInterval
theorem sound_DominatedTop : DominatedTop.Goal := dominatedTop_of_sound sound_SoundInterval
theorem sound_CrossPageJoin : CrossPageJoin.Goal := crossPageJoin_of_sound sound_SoundInterval

theorem sound_BucketInverse : BucketInverse.Goal := by
  intro t x y k
  unfold kernel
  simp only
  have hpos : 0 < t + 1 := Nat.succ_pos _
  constructor
  · intro h
    subst h
    constructor
    · exact Nat.div_mul_le_self (x + y) (t + 1)
    · rw [Nat.add_mul, Nat.one_mul]
      exact Nat.lt_div_mul_add hpos
  · intro h
    rcases h with ⟨hlo, hhi⟩
    apply Nat.eq_of_le_of_lt_succ
    · exact (Nat.le_div_iff_mul_le hpos).mpr hlo
    · exact (Nat.div_lt_iff_lt_mul hpos).mpr hhi

theorem sound_SemijoinFilter : SemijoinFilter.Goal := semijoinFilter_of_sound sound_SoundInterval
theorem sound_SumCeiling : SumCeiling.Goal := sumCeiling_of_sound sound_SoundInterval

theorem sound_SingleGroup : SingleGroup.Goal := by
  intro t a b h
  unfold kernel
  simp only [h]

end BucketSum

/-! ## WrappedSum -/

namespace WrappedSum
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum

theorem sound_SoundInterval : SoundInterval.Goal := by
  intro t a b x y hax hby
  unfold inside kernel
  simp only
  set lo := a.lo + b.lo
  set hi := a.hi + b.hi
  set m := t + 1
  have hm : 0 < m := Nat.succ_pos _
  have hxy_lo : lo ≤ x + y := Nat.add_le_add hax.1 hby.1
  have hxy_hi : x + y ≤ hi := Nat.add_le_add hax.2 hby.2
  by_cases hq : lo / m = hi / m
  · simp only [hq, ite_true]
    have hqxy : (x + y) / m = lo / m := by
      apply Nat.le_antisymm
      · rw [← hq]
        exact Nat.div_le_div_right hxy_hi
      · exact Nat.div_le_div_right hxy_lo
    constructor
    · rw [Nat.mod_def, Nat.mod_def, hqxy]
      have h1 : lo / m * m ≤ lo := Nat.div_mul_le_self lo m
      have h2 : lo / m * m ≤ x + y := Nat.le_trans h1 hxy_lo
      omega
    · rw [Nat.mod_def, Nat.mod_def, hqxy, ← hq]
      have h1 : hi / m * m ≤ x + y := by rw [hq]; exact Nat.le_trans (Nat.div_mul_le_self lo m) hxy_lo
      omega
  · simp only [hq, ite_false]
    constructor
    · exact Nat.zero_le _
    · have : (x + y) % m < m := Nat.mod_lt _ hm
      omega

theorem sound_SkipRange : SkipRange.Goal := skipRange_of_sound sound_SoundInterval
theorem sound_TakeAll : TakeAll.Goal := takeAll_of_sound sound_SoundInterval
theorem sound_ThresholdCount : ThresholdCount.Goal := thresholdCount_of_sound sound_SoundInterval
theorem sound_DominatedTop : DominatedTop.Goal := dominatedTop_of_sound sound_SoundInterval
theorem sound_CrossPageJoin : CrossPageJoin.Goal := crossPageJoin_of_sound sound_SoundInterval

theorem sound_TranslateCycle : TranslateCycle.Goal := by
  intro t n x y
  unfold kernel
  simp only
  have h : x + n * (t + 1) + y = x + y + n * (t + 1) := by ac_rfl
  rw [h, Nat.add_mul_mod_self_right]

theorem sound_SemijoinFilter : SemijoinFilter.Goal := semijoinFilter_of_sound sound_SoundInterval
theorem sound_SumCeiling : SumCeiling.Goal := sumCeiling_of_sound sound_SoundInterval

theorem sound_Rollover : Rollover.Goal := by
  intro t a b h
  unfold kernel
  simp only
  have hne : ¬ ((a.lo + b.lo) / (t + 1) = (a.hi + b.hi) / (t + 1)) := by omega
  simp [hne]

end WrappedSum

/-! ## Manhattan -/

namespace Manhattan
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan

theorem inside_self (t : Nat) : inside ⟨t, t⟩ t := ⟨Nat.le_refl t, Nat.le_refl t⟩

theorem gap_eq_max (x y : Nat) : gap x y = max (x - y) (y - x) := by
  unfold gap
  omega

theorem gapMax_point (a : Bounds) (t : Nat) (h : a.lo ≤ a.hi) :
    gapMax a ⟨t, t⟩ = max (gap a.lo t) (gap a.hi t) := by
  unfold gapMax gap
  simp only
  omega

theorem gapMin_point (a : Bounds) (t : Nat) :
    gapMin a ⟨t, t⟩ = 0 ↔ inside a t := by
  unfold gapMin inside
  simp only
  constructor
  · intro h
    rw [Nat.max_eq_zero_iff] at h
    omega
  · intro h
    rw [Nat.max_eq_zero_iff]
    omega

theorem sound_SoundInterval : SoundInterval.Goal := by
  intro t a b x y hax hby
  unfold inside kernel
  simp only
  exact ⟨Nat.add_le_add (Distance.gapMin_le_gap hax (inside_self t)) (Distance.gapMin_le_gap hby (inside_self t)),
         Nat.add_le_add (Distance.gap_le_gapMax hax (inside_self t)) (Distance.gap_le_gapMax hby (inside_self t))⟩

theorem sound_SkipRange : SkipRange.Goal := skipRange_of_sound sound_SoundInterval
theorem sound_TakeAll : TakeAll.Goal := takeAll_of_sound sound_SoundInterval
theorem sound_ThresholdCount : ThresholdCount.Goal := thresholdCount_of_sound sound_SoundInterval
theorem sound_DominatedTop : DominatedTop.Goal := dominatedTop_of_sound sound_SoundInterval
theorem sound_CrossPageJoin : CrossPageJoin.Goal := crossPageJoin_of_sound sound_SoundInterval

theorem sound_WitnessCorners : WitnessCorners.Goal := by
  intro t a b ha hb
  unfold kernel peak
  simp only [List.flatMap_cons, List.flatMap_nil, List.map_cons, List.map_nil, List.append_nil,
    List.max?]
  simp only [Option.getD]
  have hga : gapMax a ⟨t, t⟩ = max (gap a.lo t) (gap a.hi t) := gapMax_point a t ha
  have hgb : gapMax b ⟨t, t⟩ = max (gap b.lo t) (gap b.hi t) := gapMax_point b t hb
  rw [hga, hgb]
  simp (config := { failIfUnchanged := false }) only [List.max?_cons, List.foldl]
  omega

theorem sound_SemijoinFilter : SemijoinFilter.Goal := semijoinFilter_of_sound sound_SoundInterval
theorem sound_SumCeiling : SumCeiling.Goal := sumCeiling_of_sound sound_SoundInterval

theorem sound_InteriorMinimum : InteriorMinimum.Goal := by
  intro t a b ha hb
  unfold kernel
  simp only
  have h1 : gapMin a ⟨t, t⟩ = 0 := (gapMin_point a t).mpr ha
  have h2 : gapMin b ⟨t, t⟩ = 0 := (gapMin_point b t).mpr hb
  simp [h1, h2]

end Manhattan

/-! ## CappedPair -/

namespace CappedPair
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair

theorem sound_SoundInterval : SoundInterval.Goal := by
  intro t a b x y hax hby
  unfold inside kernel
  simp only
  constructor <;> (simp only [Nat.min_def]; omega)

theorem sound_SkipRange : SkipRange.Goal := skipRange_of_sound sound_SoundInterval
theorem sound_TakeAll : TakeAll.Goal := takeAll_of_sound sound_SoundInterval
theorem sound_ThresholdCount : ThresholdCount.Goal := thresholdCount_of_sound sound_SoundInterval
theorem sound_DominatedTop : DominatedTop.Goal := dominatedTop_of_sound sound_SoundInterval
theorem sound_CrossPageJoin : CrossPageJoin.Goal := crossPageJoin_of_sound sound_SoundInterval

theorem sound_PushBoth : PushBoth.Goal := by
  intro t k x y hkt
  unfold kernel
  simp only
  constructor
  · intro h
    exact ⟨Nat.le_trans h (Nat.le_trans (Nat.min_le_right _ _) (Nat.min_le_left _ _)),
           Nat.le_trans h (Nat.le_trans (Nat.min_le_right _ _) (Nat.min_le_right _ _))⟩
  · intro h
    exact Nat.le_min.mpr ⟨hkt, Nat.le_min.mpr ⟨h.1, h.2⟩⟩

theorem sound_SemijoinFilter : SemijoinFilter.Goal := semijoinFilter_of_sound sound_SoundInterval
theorem sound_SumCeiling : SumCeiling.Goal := sumCeiling_of_sound sound_SoundInterval

theorem sound_SaturatedPage : SaturatedPage.Goal := by
  intro t a b ha hb
  unfold kernel
  simp only
  omega

end CappedPair

/-! ## QuadraticGap -/

namespace QuadraticGap
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap

theorem sound_SoundInterval : SoundInterval.Goal := by
  intro t a b x y hax hby
  unfold inside kernel
  simp only
  constructor
  · exact Nat.add_le_add_right (Nat.pow_le_pow_left (Distance.gapMin_le_gap hax hby) 2) t
  · exact Nat.add_le_add_right (Nat.pow_le_pow_left (Distance.gap_le_gapMax hax hby) 2) t

theorem sound_SkipRange : SkipRange.Goal := skipRange_of_sound sound_SoundInterval
theorem sound_TakeAll : TakeAll.Goal := takeAll_of_sound sound_SoundInterval
theorem sound_ThresholdCount : ThresholdCount.Goal := thresholdCount_of_sound sound_SoundInterval
theorem sound_DominatedTop : DominatedTop.Goal := dominatedTop_of_sound sound_SoundInterval
theorem sound_CrossPageJoin : CrossPageJoin.Goal := crossPageJoin_of_sound sound_SoundInterval

theorem sound_SquaredRadius : SquaredRadius.Goal := by
  intro t r x y
  unfold kernel gap
  simp only
  constructor
  · intro h
    have hg : (x - y + (y - x)) ^ 2 ≤ r * r := by omega
    have hsq : x - y + (y - x) ≤ r := by
      by_contra hc
      push_neg at hc
      have h2 : r * r < (x - y + (y - x)) * (x - y + (y - x)) := Nat.mul_self_lt_mul_self hc
      simp only [Nat.pow_two] at hg
      omega
    constructor <;> omega
  · intro h
    rcases h with ⟨h1, h2⟩
    have hg : x - y + (y - x) ≤ r := by omega
    have : (x - y + (y - x)) * (x - y + (y - x)) ≤ r * r := Nat.mul_self_le_mul_self hg
    simp only [Nat.pow_two]
    omega

theorem sound_SemijoinFilter : SemijoinFilter.Goal := semijoinFilter_of_sound sound_SoundInterval
theorem sound_SumCeiling : SumCeiling.Goal := sumCeiling_of_sound sound_SoundInterval

theorem sound_GapWitness : ¬ GapWitness.Goal := by
  intro hg
  have h := hg 0 ⟨10, 0⟩ ⟨1, 2⟩ (by decide)
  simp [kernel, gapMin, gap] at h
  omega

end QuadraticGap


/-! ## BitwiseXor -/

namespace BitwiseXor
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor

theorem width_zero : width 0 = 0 := by simp [width]

theorem width_pos {n : Nat} (h : 0 < n) : width n = n.log2 + 1 := by
  simp [width, Nat.pos_iff_ne_zero.mp h]

theorem lt_pow_width (n : Nat) : n < 2 ^ width n := by
  by_cases h : n = 0
  · simp [h, width]
  · rw [width_pos (Nat.pos_of_ne_zero h)]
    exact Nat.lt_log2_self

theorem xor_eq_zero_iff {a b : Nat} : a ^^^ b = 0 ↔ a = b := by
  constructor
  · intro h
    calc a = a ^^^ 0 := by simp
      _ = a ^^^ (b ^^^ b) := by simp [Nat.xor_self]
      _ = (a ^^^ b) ^^^ b := by rw [Nat.xor_assoc]
      _ = 0 ^^^ b := by rw [h]
      _ = b := by simp
  · intro h; subst h; simp [Nat.xor_self]

theorem high_bits_eq_of_xor_lt {a b w : Nat} (h : a ^^^ b < 2 ^ w) : a / 2 ^ w = b / 2 ^ w := by
  have hdiv : (a ^^^ b) / 2 ^ w = 0 := Nat.div_eq_of_lt h
  rw [Nat.xor_div_two_pow] at hdiv
  exact xor_eq_zero_iff.mp hdiv

theorem high_bits_eq_of_inside {a b x w : Nat}
    (hax : inside ⟨a, b⟩ x) (hw : width (a ^^^ b) ≤ w) : x / 2 ^ w = a / 2 ^ w := by
  have hlt : a ^^^ b < 2 ^ w := Nat.lt_of_lt_of_le (lt_pow_width _) (Nat.pow_le_pow_right (by omega) hw)
  have hhi : a / 2 ^ w = b / 2 ^ w := high_bits_eq_of_xor_lt hlt
  have hlo : a / 2 ^ w ≤ x / 2 ^ w := Nat.div_le_div_right hax.1
  have hhi2 : x / 2 ^ w ≤ b / 2 ^ w := Nat.div_le_div_right hax.2
  omega

theorem sound_SoundInterval : SoundInterval.Goal := by
  intro t a b x y hax hby
  unfold inside kernel
  simp only
  set w := max (width (a.lo ^^^ a.hi)) (width (b.lo ^^^ b.hi))
  set base := ((a.lo ^^^ b.lo) / 2 ^ w) * 2 ^ w
  have hw1 : width (a.lo ^^^ a.hi) ≤ w := Nat.le_max_left _ _
  have hw2 : width (b.lo ^^^ b.hi) ≤ w := Nat.le_max_right _ _
  have hxa : x / 2 ^ w = a.lo / 2 ^ w := high_bits_eq_of_inside hax hw1
  have hyb : y / 2 ^ w = b.lo / 2 ^ w := high_bits_eq_of_inside hby hw2
  have hxy : (x ^^^ y) / 2 ^ w = (a.lo ^^^ b.lo) / 2 ^ w := by
    rw [Nat.xor_div_two_pow, hxa, hyb, ← Nat.xor_div_two_pow]
  constructor
  · rw [← hxy]
    exact Nat.div_mul_le_self _ _
  · rw [← hxy]
    have hmod : (x ^^^ y) % 2 ^ w < 2 ^ w := Nat.mod_lt _ ((Nat.two_pow_pos _))
    have hdecomp : x ^^^ y = (x ^^^ y) / 2 ^ w * 2 ^ w + (x ^^^ y) % 2 ^ w := (Nat.div_add_mod _ _).symm
    omega

theorem sound_SkipRange : SkipRange.Goal := skipRange_of_sound sound_SoundInterval
theorem sound_TakeAll : TakeAll.Goal := takeAll_of_sound sound_SoundInterval
theorem sound_ThresholdCount : ThresholdCount.Goal := thresholdCount_of_sound sound_SoundInterval
theorem sound_DominatedTop : DominatedTop.Goal := dominatedTop_of_sound sound_SoundInterval
theorem sound_CrossPageJoin : CrossPageJoin.Goal := crossPageJoin_of_sound sound_SoundInterval

theorem xor_mul_two_pow (a b k : Nat) : (a * 2 ^ k) ^^^ (b * 2 ^ k) = (a ^^^ b) * 2 ^ k := by
  simpa [Nat.shiftLeft_eq] using (Nat.shiftLeft_xor_distrib a b k).symm

theorem xor_add_same_high {k a v : Nat} (hv : v < 2 ^ k) :
    (a * 2 ^ k) ^^^ (a * 2 ^ k + v) = v := by
  apply Nat.eq_of_testBit_eq
  intro i
  rw [Nat.testBit_xor]
  by_cases hik : i < k
  · have h1 : (a * 2 ^ k).testBit i = false := by
      rw [Nat.mul_comm, ← Nat.shiftLeft_eq]
      simp [Nat.testBit_shiftLeft, hik]
    have h2 : (a * 2 ^ k + v).testBit i = v.testBit i := by
      have hk : k = i + (k - i) := (Nat.add_sub_cancel' (Nat.le_of_lt hik)).symm
      have hpow : 2 ^ k = 2 ^ i * 2 ^ (k - i) := by rw [hk, Nat.pow_add]
      have hrewr : a * 2 ^ k + v = 2 ^ i * (2 ^ (k - i) * a) + v := by rw [hpow]; ring
      rw [hrewr, Nat.testBit_mul_two_pow_add_eq]
      have heven : (2 ^ (k - i) * a) % 2 = 0 := by
        have hk1 : 1 ≤ k - i := by omega
        have : 2 ^ (k - i) % 2 = 0 := by
          rcases Nat.exists_eq_succ_of_ne_zero (by omega : k - i ≠ 0) with ⟨n, hn⟩
          rw [hn, Nat.pow_succ]
          simp [Nat.mul_mod]
        simp [Nat.mul_mod, this]
      simp [heven]
    rw [h1, h2]; simp
  · push_neg at hik
    have hv0 : v.testBit i = false := Nat.testBit_lt_two_pow (Nat.lt_of_lt_of_le hv (Nat.pow_le_pow_right (by omega) hik))
    have hk_pos : 0 < 2 ^ k := Nat.pos_of_ne_zero ((Nat.two_pow_pos _))
    have hdiv_base : (a * 2 ^ k) / 2 ^ k = a := by
      rw [Nat.mul_comm, Nat.mul_div_cancel _ hk_pos]
    have hdiv_add : (a * 2 ^ k + v) / 2 ^ k = a := by
      have h1 : a * 2 ^ k + v = v + a * 2 ^ k := by omega
      rw [h1, Nat.add_mul_div_left _ _ hk_pos, Nat.div_eq_of_lt hv, Nat.zero_add]
    have h1 : (a * 2 ^ k).testBit i = a.testBit (i - k) := by
      have hi : i = (i - k) + k := (Nat.sub_add_cancel hik).symm
      conv_lhs => rw [hi]
      rw [← Nat.testBit_div_two_pow, hdiv_base]
    have h2 : (a * 2 ^ k + v).testBit i = a.testBit (i - k) := by
      have hi : i = (i - k) + k := (Nat.sub_add_cancel hik).symm
      conv_lhs => rw [hi]
      rw [← Nat.testBit_div_two_pow, hdiv_add]
    rw [h1, h2, hv0]; simp

theorem sound_AlignedSiblings : AlignedSiblings.Goal := by
  intro k a b
  unfold kernel
  simp only
  have hk : 0 < 2 ^ k := Nat.pos_of_ne_zero ((Nat.two_pow_pos _))
  have hhi_a : (a + 1) * 2 ^ k - 1 = a * 2 ^ k + (2 ^ k - 1) := by
    have : (a + 1) * 2 ^ k = a * 2 ^ k + 2 ^ k := by ring
    omega
  have hhi_b : (b + 1) * 2 ^ k - 1 = b * 2 ^ k + (2 ^ k - 1) := by
    have : (b + 1) * 2 ^ k = b * 2 ^ k + 2 ^ k := by ring
    omega
  have hlt : 2 ^ k - 1 < 2 ^ k := by omega
  have hxor_a : a * 2 ^ k ^^^ ((a + 1) * 2 ^ k - 1) = 2 ^ k - 1 := by
    rw [hhi_a]; exact xor_add_same_high hlt
  have hxor_b : b * 2 ^ k ^^^ ((b + 1) * 2 ^ k - 1) = 2 ^ k - 1 := by
    rw [hhi_b]; exact xor_add_same_high hlt
  simp only [hxor_a, hxor_b, Nat.max_self]
  have hwidth : width (2 ^ k - 1) = k := by
    by_cases hk0 : k = 0
    · simp [hk0, width]
    · have hpos : 0 < 2 ^ k - 1 := by omega
      rw [width_pos hpos]
      have hlog : (2 ^ k - 1).log2 = k - 1 := by
        rw [Nat.log2_eq_iff (by omega)]
        constructor <;> omega
      omega
  simp only [hwidth]
  have hxor_ab : a * 2 ^ k ^^^ b * 2 ^ k = (a ^^^ b) * 2 ^ k := xor_mul_two_pow a b k
  rw [hxor_ab, Nat.mul_div_cancel _ hk, Nat.mul_comm]

theorem sound_SemijoinFilter : SemijoinFilter.Goal := semijoinFilter_of_sound sound_SoundInterval
theorem sound_SumCeiling : SumCeiling.Goal := sumCeiling_of_sound sound_SoundInterval

theorem sound_FixedPrefix : FixedPrefix.Goal := by
  intro x y
  unfold kernel
  simp only [Nat.xor_self, width_zero, Nat.max_self, Nat.pow_zero, Nat.div_one, Nat.mul_one,
    Nat.add_sub_cancel]

end BitwiseXor

end TinylambOpt.ResearchProofs.StorageExecution.DerivedZones
