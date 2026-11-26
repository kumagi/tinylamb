import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

namespace TinylambOpt.ResearchProofs.StorageExecution.DerivedZonesA

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! ## Generic consequences of a certified scalar interval -/

theorem score_mem {f : Kernel} {t : Nat} {p : List (Option Row)} {v : Nat}
    (hv : v ∈ scores f t p) :
    ∃ r ∈ p, ∃ x y, r = some (x,y) ∧ v = f.score t (x,y) := by
  rcases List.mem_filterMap.mp hv with ⟨r, hr, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x,y⟩
  exact ⟨r, hr, x, y, hrxy, hscore.symm⟩

theorem score_bounds {f : Kernel} {t : Nat} {p : List (Option Row)} {a b : Bounds}
    (hs : soundInterval f) (hc : covers p a b) {v : Nat} (hv : v ∈ scores f t p) :
    inside (f.bounds t a b) v := by
  rcases score_mem hv with ⟨r, hr, x, y, hrxy, rfl⟩
  exact hs t a b x y (hc r hr x y hrxy).1 (hc r hr x y hrxy).2

theorem skipRange_of_sound {f : Kernel} (hs : soundInterval f) : skipRange f := by
  intro t p a b lo hi hc hn
  apply List.filter_eq_nil_iff.mpr
  intro v hv
  intro hpass
  have hpred : (lo ≤ v && v < hi) = true := hpass
  simp only [Bool.and_eq_true, decide_eq_true_eq] at hpred
  have hb := score_bounds hs hc hv
  simp only [inside] at hb
  simp [allows] at hn
  omega

theorem takeAll_of_sound {f : Kernel} (hs : soundInterval f) : takeAll f := by
  intro t p a b lo hi hc hlo hhi
  change (scores f t p).filter (fun v => lo ≤ v && v < hi) = scores f t p
  rw [List.filter_eq_self.mpr]
  intro v hv
  have hb := score_bounds hs hc hv
  simp only [Bool.and_eq_true, decide_eq_true_eq]
  exact ⟨Nat.le_trans hlo hb.1, Nat.lt_of_le_of_lt hb.2 hhi⟩

theorem crossPageJoin_of_sound {f : Kernel} (hs : soundInterval f) : crossPageJoin f := by
  intro t u p q a b c d hcp hcq hsep x hx hxq
  have hb1 := score_bounds hs hcp hx
  have hb2 := score_bounds hs hcq hxq
  simp only [inside] at hb1 hb2
  omega

theorem semijoinFilter_of_sound {f : Kernel} (hs : soundInterval f) : semijoinFilter f := by
  intro t p a b ks hc
  dsimp only
  apply List.filter_congr
  intro v hv
  have hb := score_bounds hs hc hv
  by_cases hmem : v ∈ ks
  · simp [List.contains_eq_mem, hmem, hb.1, hb.2]
  · have hnot : v ∉ ks.filter (fun k => (f.bounds t a b).lo ≤ k && k ≤ (f.bounds t a b).hi) := by
      simp [List.mem_filter, hmem]
    simp [List.contains_eq_mem, hmem, hnot]

theorem sum_bounds {lo hi : Nat} {vs : List Nat}
    (hv : ∀ v ∈ vs, lo ≤ v ∧ v ≤ hi) :
    vs.length * lo ≤ vs.sum ∧ vs.sum ≤ vs.length * hi := by
  induction vs with
  | nil => simp
  | cons v vs ih =>
    have hhead := hv v (by simp)
    have htail := ih (fun x hx => hv x (by simp [hx]))
    simp only [List.length_cons, List.sum_cons, Nat.add_mul, Nat.mul_add, Nat.mul_one]
    omega

theorem sumCeiling_of_sound {f : Kernel} (hs : soundInterval f) : sumCeiling f := by
  intro t p a b hc
  exact sum_bounds (fun v hv => score_bounds hs hc hv)

theorem scores_length {f : Kernel} (t : Nat) (p : List (Option Row)) :
    (scores f t p).length = (p.filterMap id).length := by
  unfold scores
  induction p with
  | nil => rfl
  | cons r p ih =>
    cases r with
    | none => simp [ih]
    | some xy => simp [ih]

theorem flatten_scores {f : Kernel} (t : Nat) (ps : List (List (Option Row) × Bounds × Bounds)) :
    ((ps.map Prod.fst).flatten.filterMap (fun r => r.map (f.score t))) =
      ps.flatMap (fun e => scores f t e.1) := by
  induction ps with
  | nil => rfl
  | cons e ps ih =>
    simp only [List.map_cons, List.flatten_cons, List.filterMap_append, List.flatMap_cons]
    rw [ih]
    rfl

theorem countP_flatMap {α β : Type} (xs : List α) (g : α → List β) (q : β → Bool) :
    (xs.flatMap g).countP q = (xs.map (fun x => (g x).countP q)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [List.flatMap_cons, List.countP_append, ih, Nat.add_comm]

theorem sum_filter_le_sum {α : Type} (xs : List α) (p : α → Bool) (a b : α → Nat)
    (h : ∀ x ∈ xs, p x = true → a x ≤ b x) :
    ((xs.filter p).map a).sum ≤ (xs.map b).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    have htail : ∀ y ∈ xs, p y = true → a y ≤ b y :=
      fun y hy => h y (by simp [hy])
    by_cases hp : p x = true
    · simp only [List.filter_cons, hp, ite_true, List.map_cons, List.sum_cons]
      exact Nat.add_le_add (h x (by simp) hp) (ih htail)
    · simp only [List.filter_cons, hp, Bool.false_eq_true, ite_false, List.map_cons, List.sum_cons]
      exact Nat.le_trans (ih htail) (Nat.le_add_left _ _)

theorem sum_le_sum_filter {α : Type} (xs : List α) (p : α → Bool) (a b : α → Nat)
    (hab : ∀ x ∈ xs, b x ≤ a x)
    (hz : ∀ x ∈ xs, p x ≠ true → b x = 0) :
    (xs.map b).sum ≤ ((xs.filter p).map a).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    have htail : ∀ y ∈ xs, b y ≤ a y := fun y hy => hab y (by simp [hy])
    have hztail : ∀ y ∈ xs, p y ≠ true → b y = 0 := fun y hy => hz y (by simp [hy])
    by_cases hp : p x = true
    · simp only [List.filter_cons, hp, ite_true, List.map_cons, List.sum_cons]
      exact Nat.add_le_add (hab x (by simp)) (ih htail hztail)
    · have hx := hz x (by simp) hp
      simp only [List.filter_cons, hp, ite_false, List.map_cons, List.sum_cons, hx, Nat.zero_add]
      exact ih htail hztail

theorem thresholdCount_of_sound {f : Kernel} (hs : soundInterval f) : thresholdCount f := by
  intro t ps cut hc
  dsimp only
  rw [flatten_scores, countP_flatMap]
  constructor
  · apply sum_filter_le_sum
    intro e he hp
    have hlow : cut ≤ (f.bounds t e.2.1 e.2.2).lo := by simpa using hp
    have hall : ∀ v ∈ scores f t e.1, cut ≤ v := by
      intro v hv
      exact Nat.le_trans hlow (score_bounds hs (hc e he) hv).1
    have hfilter : (scores f t e.1).filter (fun v => decide (cut ≤ v)) = scores f t e.1 := by
      apply List.filter_eq_self.mpr
      intro v hv
      simpa only [decide_eq_true_eq] using hall v hv
    rw [← scores_length (f := f) (t := t) (p := e.1),
      List.countP_eq_length_filter, hfilter]
    exact Nat.le_refl _
  · apply sum_le_sum_filter
    · intro e he
      rw [← scores_length (f := f) (t := t) (p := e.1)]
      exact List.countP_le_length
    · intro e he hp
      have hhigh : (f.bounds t e.2.1 e.2.2).hi < cut := by simpa using hp
      apply List.countP_eq_zero.mpr
      intro v hv hpass
      have hpass' : cut ≤ v := by simpa using hpass
      have hb := score_bounds hs (hc e he) hv
      simp only [inside] at hb
      omega

/-! Sorting is used only for the generic top-k consequence. -/

def desc (a b : Nat) : Bool := decide (a ≥ b)

theorem desc_trans (a b c : Nat) (h1 : desc a b = true) (h2 : desc b c = true) :
    desc a c = true := by
  simp only [desc, decide_eq_true_eq] at *
  exact Nat.le_trans h2 h1

theorem desc_total (a b : Nat) : (desc a b || desc b a) = true := by
  simp only [desc, Bool.or_eq_true, decide_eq_true_eq]
  omega

theorem mergeSort_perm_eq {l₁ l₂ : List Nat} (h : l₁.Perm l₂) :
    l₁.mergeSort desc = l₂.mergeSort desc := by
  apply List.Perm.eq_of_pairwise (le := fun a b => desc a b = true)
  · intro a b _ _ hab hba
    simp only [desc, decide_eq_true_eq] at hab hba
    exact Nat.le_antisymm hba hab
  · exact List.pairwise_mergeSort desc_trans desc_total l₁
  · exact List.pairwise_mergeSort desc_trans desc_total l₂
  · exact (List.mergeSort_perm l₁ desc).trans (h.trans (List.mergeSort_perm l₂ desc).symm)

theorem take_high_of_sorted {l : List Nat} {k upper : Nat}
    (hs : List.Pairwise (fun a b => desc a b = true) l)
    (hk : k ≤ l.countP (fun x => upper < x)) :
    ∀ x ∈ l.take k, upper < x := by
  induction l generalizing k with
  | nil =>
    simp only [List.countP_nil, Nat.le_zero] at hk
    subst k
    simp
  | cons a as ih =>
    cases k with
    | zero => simp
    | succ k =>
      have htail : List.Pairwise (fun a b => desc a b = true) as := hs.tail
      by_cases ha : upper < a
      · have htailcount : k ≤ as.countP (fun x => upper < x) := by
          have hcons := List.countP_cons (p := fun x => upper < x) (a := a) (l := as)
          simp only [decide_eq_true_eq, ha, ite_true] at hcons
          omega
        have hrec := ih htail htailcount
        intro x hx
        simp only [List.take_succ_cons, List.mem_cons] at hx
        rcases hx with rfl | hx
        · exact ha
        · exact hrec x hx
      · have hz : as.countP (fun x => upper < x) = 0 := by
          apply List.countP_eq_zero.mpr
          intro b hb hbh
          have hbh' : upper < b := by simpa using hbh
          have hab : a ≥ b := by
            simpa only [desc, decide_eq_true_eq] using (List.pairwise_cons.mp hs).1 b hb
          exfalso
          omega
        simp [ha, hz] at hk

theorem top_prefix_dominated (seen extras : List Nat) (k upper : Nat)
    (hk : k ≤ seen.countP (fun x => upper < x))
    (hextra : ∀ x ∈ extras, x ≤ upper) :
    ((seen ++ extras).mergeSort desc).take k = (seen.mergeSort desc).take k := by
  cases k with
  | zero => simp
  | succ k =>
    let s := seen.mergeSort desc
    let es := extras.mergeSort desc
    have hsorted : List.Pairwise (fun a b => desc a b = true) s :=
      List.pairwise_mergeSort desc_trans desc_total seen
    have hks : k + 1 ≤ s.countP (fun x => upper < x) := by
      change k + 1 ≤ (seen.mergeSort desc).countP (fun x => upper < x)
      rw [(List.mergeSort_perm seen desc).countP_eq (fun x => upper < x)]
      exact hk
    have hlen : k + 1 ≤ s.length := by
      simpa only [s] using Nat.le_trans hks List.countP_le_length
    have hhi := take_high_of_sorted hsorted hks
    let low := (s.drop (k + 1) ++ es).mergeSort desc
    have hlow_sorted : List.Pairwise (fun a b => desc a b = true) low :=
      List.pairwise_mergeSort desc_trans desc_total (s.drop (k + 1) ++ es)
    have hp0 : (seen ++ extras).Perm (s ++ es) := by
      change (seen ++ extras).Perm ((seen.mergeSort desc) ++ (extras.mergeSort desc))
      have h2 : (seen ++ extras).Perm ((seen.mergeSort desc) ++ (extras.mergeSort desc)) :=
        (List.mergeSort_perm seen desc).symm.append (List.mergeSort_perm extras desc).symm
      exact h2
    rw [mergeSort_perm_eq hp0]
    have hsplit : s = s.take (k + 1) ++ s.drop (k + 1) :=
      (List.take_append_drop (k + 1) s).symm
    have hp1 : (s ++ es).Perm (s.take (k + 1) ++ low) := by
      have hsplit' : s ++ es = s.take (k + 1) ++ (s.drop (k + 1) ++ es) := by
        calc
          s ++ es = (s.take (k + 1) ++ s.drop (k + 1)) ++ es :=
            congrArg (fun xs => xs ++ es) hsplit
          _ = s.take (k + 1) ++ (s.drop (k + 1) ++ es) := List.append_assoc _ _ _
      exact (List.Perm.of_eq hsplit').trans
        (List.Perm.append (List.Perm.refl (s.take (k + 1)))
          (List.mergeSort_perm (s.drop (k + 1) ++ es) desc).symm)
    rw [mergeSort_perm_eq hp1]
    have hpair : List.Pairwise (fun a b => desc a b = true)
        (s.take (k + 1) ++ low) := by
      apply List.pairwise_append.mpr
      refine ⟨?_, hlow_sorted, ?_⟩
      · exact hsorted.sublist (List.take_sublist (k + 1) s)
      · intro x hx y hy
        have hxhigh := hhi x hx
        have hymem := (List.mergeSort_perm (s.drop (k + 1) ++ es) desc).mem_iff.mp hy
        rcases List.mem_append.mp hymem with hy | hy
        · have hrel := hsorted.rel_of_mem_take_of_mem_drop hx hy
          simpa only [desc, decide_eq_true_eq] using hrel
        · have hymem := (List.mergeSort_perm extras desc).mem_iff.mp hy
          have hyle := hextra y hymem
          simp only [desc, decide_eq_true_eq]
          omega
    rw [List.mergeSort_of_pairwise hpair]
    rw [List.take_append_of_le_length (i := k + 1) (by
      simp only [List.length_take, Nat.min_eq_left hlen]
      exact Nat.le_refl (k + 1))]
    simp only [List.take_take, Nat.min_self, s]

theorem dominatedTop_of_sound {f : Kernel} (hs : soundInterval f) : dominatedTop f := by
  intro t p a b seen k hc hk
  refine top_prefix_dominated seen (scores f t p) k (f.bounds t a b).hi hk ?_
  intro v hv
  exact (score_bounds hs hc hv).2

/-! ## Product -/

namespace Product

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product

theorem sound_Product_SoundInterval : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.SoundInterval.Goal := by
  intro t a b x y hax hby
  unfold inside kernel
  simp only
  constructor
  · exact Nat.mul_le_mul hax.1 hby.1
  · exact Nat.mul_le_mul hax.2 hby.2

theorem sound_Product_SkipRange : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.SkipRange.Goal := skipRange_of_sound sound_Product_SoundInterval
theorem sound_Product_TakeAll : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.TakeAll.Goal := takeAll_of_sound sound_Product_SoundInterval
theorem sound_Product_ThresholdCount : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.ThresholdCount.Goal := thresholdCount_of_sound sound_Product_SoundInterval
theorem sound_Product_DominatedTop : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.DominatedTop.Goal := dominatedTop_of_sound sound_Product_SoundInterval
theorem sound_Product_CrossPageJoin : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.CrossPageJoin.Goal := crossPageJoin_of_sound sound_Product_SoundInterval

theorem sound_Product_ProductDivisor : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.ProductDivisor.Goal := by
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
    have hkx : k + x - 1 = k + (x - 1) := by omega
    have hyx : y * x + x - 1 = x * y + (x - 1) := by rw [Nat.mul_comm y x]; omega
    rw [hkx] at hk
    exact Nat.le_of_add_le_add_right hk

theorem sound_Product_SemijoinFilter : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.SemijoinFilter.Goal := semijoinFilter_of_sound sound_Product_SoundInterval
theorem sound_Product_SumCeiling : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.SumCeiling.Goal := sumCeiling_of_sound sound_Product_SoundInterval

theorem sound_Product_UpperWitness : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product.UpperWitness.Goal := by
  intro a b
  rfl

#guard (Product.kernel.score 0 (3, 4) : Nat) = 12
#guard ((4 + 3 - 1) / 3 : Nat) = 2

end Product

/-! ## SaturatingLoss -/

namespace SaturatingLoss

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss

theorem sound_SaturatingLoss_SoundInterval : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.SoundInterval.Goal := by
  intro t a b x y hax hby
  unfold inside kernel
  simp only
  constructor
  · exact Nat.le_trans
      (Nat.sub_le_sub_left (Nat.add_le_add_right hby.2 t) a.lo)
      (Nat.sub_le_sub_right hax.1 (y + t))
  · exact Nat.le_trans
      (Nat.sub_le_sub_right hax.2 (y + t))
      (Nat.sub_le_sub_left (Nat.add_le_add_right hby.1 t) a.hi)

theorem sound_SaturatingLoss_SkipRange : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.SkipRange.Goal := skipRange_of_sound sound_SaturatingLoss_SoundInterval
theorem sound_SaturatingLoss_TakeAll : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.TakeAll.Goal := takeAll_of_sound sound_SaturatingLoss_SoundInterval
theorem sound_SaturatingLoss_ThresholdCount : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.ThresholdCount.Goal := thresholdCount_of_sound sound_SaturatingLoss_SoundInterval
theorem sound_SaturatingLoss_DominatedTop : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.DominatedTop.Goal := dominatedTop_of_sound sound_SaturatingLoss_SoundInterval
theorem sound_SaturatingLoss_CrossPageJoin : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.CrossPageJoin.Goal := crossPageJoin_of_sound sound_SaturatingLoss_SoundInterval

theorem sound_SaturatingLoss_ThresholdInverse : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.ThresholdInverse.Goal := by
  intro t x y k hk
  unfold kernel
  simp only
  constructor
  · intro h
    have hpos : 0 < x - (y + t) := Nat.lt_of_lt_of_le hk h
    have hx : y + t ≤ x := Nat.le_of_lt (Nat.lt_of_sub_pos hpos)
    have hle := (Nat.le_sub_iff_add_le hx).mp h
    omega
  · intro h
    have hx : y + t ≤ x := Nat.le_trans (Nat.le_add_right (y + t) k) h
    exact (Nat.le_sub_iff_add_le hx).mpr (by omega)

theorem sound_SaturatingLoss_SemijoinFilter : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.SemijoinFilter.Goal := semijoinFilter_of_sound sound_SaturatingLoss_SoundInterval
theorem sound_SaturatingLoss_SumCeiling : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.SumCeiling.Goal := sumCeiling_of_sound sound_SaturatingLoss_SoundInterval

theorem sound_SaturatingLoss_ZeroCertificate : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss.ZeroCertificate.Goal := by
  intro t a b
  unfold kernel
  simp only
  exact Nat.sub_eq_zero_iff_le

#guard (SaturatingLoss.kernel.score 3 (5, 1) : Nat) = 1
#guard (SaturatingLoss.kernel.score 3 (2, 1) : Nat) = 0

end SaturatingLoss

/-! ## Distance -/

namespace Distance

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance

theorem gap_eq_max (x y : Nat) : gap x y = max (x - y) (y - x) := by
  unfold gap
  omega

theorem gap_le_gapMax {a b : Bounds} {x y : Nat} (hax : inside a x) (hby : inside b y) :
    gap x y ≤ gapMax a b := by
  unfold gap gapMax
  simp only [inside] at hax hby
  omega

theorem gapMin_le_gap {a b : Bounds} {x y : Nat} (hax : inside a x) (hby : inside b y) :
    gapMin a b ≤ gap x y := by
  unfold gap gapMin
  simp only [inside] at hax hby
  omega

theorem sound_Distance_SoundInterval : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.SoundInterval.Goal := by
  intro t a b x y hax hby
  unfold inside kernel
  simp only
  exact ⟨gapMin_le_gap hax hby, gap_le_gapMax hax hby⟩

theorem sound_Distance_SkipRange : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.SkipRange.Goal := skipRange_of_sound sound_Distance_SoundInterval
theorem sound_Distance_TakeAll : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.TakeAll.Goal := takeAll_of_sound sound_Distance_SoundInterval
theorem sound_Distance_ThresholdCount : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.ThresholdCount.Goal := thresholdCount_of_sound sound_Distance_SoundInterval
theorem sound_Distance_DominatedTop : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.DominatedTop.Goal := dominatedTop_of_sound sound_Distance_SoundInterval
theorem sound_Distance_CrossPageJoin : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.CrossPageJoin.Goal := crossPageJoin_of_sound sound_Distance_SoundInterval

theorem sound_Distance_BandInverse : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.BandInverse.Goal := by
  intro t x y
  unfold kernel gap
  simp only
  omega

theorem sound_Distance_SemijoinFilter : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.SemijoinFilter.Goal := semijoinFilter_of_sound sound_Distance_SoundInterval
theorem sound_Distance_SumCeiling : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.SumCeiling.Goal := sumCeiling_of_sound sound_Distance_SoundInterval

theorem sound_Distance_OverlapFloor : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance.OverlapFloor.Goal := by
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

#guard (Distance.kernel.score 0 (3, 7) : Nat) = 4
#guard (Distance.kernel.bounds 0 ⟨2, 3⟩ ⟨5, 8⟩ : Bounds).lo = 2

end Distance

/-! ## FloorRatio -/

namespace FloorRatio

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio

theorem sound_FloorRatio_SoundInterval : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.SoundInterval.Goal := by
  intro t a b x y hax hby
  unfold inside kernel
  simp only
  constructor
  · apply Nat.le_trans (Nat.div_le_div_right (Nat.mul_le_mul_right t hax.1) (c := b.hi + 1))
    exact Nat.div_le_div_left (Nat.add_le_add_right hby.2 1) (Nat.succ_pos _)
  · apply Nat.le_trans (Nat.div_le_div_right (Nat.mul_le_mul_right t hax.2) (c := y + 1))
    exact Nat.div_le_div_left (Nat.add_le_add_right hby.1 1) (Nat.succ_pos _)

theorem sound_FloorRatio_SkipRange : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.SkipRange.Goal := skipRange_of_sound sound_FloorRatio_SoundInterval
theorem sound_FloorRatio_TakeAll : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.TakeAll.Goal := takeAll_of_sound sound_FloorRatio_SoundInterval
theorem sound_FloorRatio_ThresholdCount : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.ThresholdCount.Goal := thresholdCount_of_sound sound_FloorRatio_SoundInterval
theorem sound_FloorRatio_DominatedTop : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.DominatedTop.Goal := dominatedTop_of_sound sound_FloorRatio_SoundInterval
theorem sound_FloorRatio_CrossPageJoin : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.CrossPageJoin.Goal := crossPageJoin_of_sound sound_FloorRatio_SoundInterval

theorem sound_FloorRatio_DenominatorPage : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.DenominatorPage.Goal := by
  intro t a b k
  unfold kernel
  simp only
  exact Nat.le_div_iff_mul_le (Nat.succ_pos _)

theorem sound_FloorRatio_SemijoinFilter : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.SemijoinFilter.Goal := semijoinFilter_of_sound sound_FloorRatio_SoundInterval
theorem sound_FloorRatio_SumCeiling : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.SumCeiling.Goal := sumCeiling_of_sound sound_FloorRatio_SoundInterval

theorem sound_FloorRatio_QuotientThreshold : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio.QuotientThreshold.Goal := by
  intro t a b k
  unfold kernel
  simp only
  exact Nat.div_lt_iff_lt_mul (Nat.succ_pos _)

#guard (FloorRatio.kernel.score 1 (23, 3) : Nat) = 5
#guard (FloorRatio.kernel.bounds 1 ⟨23, 23⟩ ⟨3, 3⟩ : Bounds).lo = 5

end FloorRatio

/-! ## BucketSum -/

namespace BucketSum

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum

theorem sound_BucketSum_SoundInterval : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.SoundInterval.Goal := by
  intro t a b x y hax hby
  unfold inside kernel
  simp only
  constructor
  · exact Nat.div_le_div_right (Nat.add_le_add hax.1 hby.1) (c := t + 1)
  · exact Nat.div_le_div_right (Nat.add_le_add hax.2 hby.2) (c := t + 1)

theorem sound_BucketSum_SkipRange : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.SkipRange.Goal := skipRange_of_sound sound_BucketSum_SoundInterval
theorem sound_BucketSum_TakeAll : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.TakeAll.Goal := takeAll_of_sound sound_BucketSum_SoundInterval
theorem sound_BucketSum_ThresholdCount : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.ThresholdCount.Goal := thresholdCount_of_sound sound_BucketSum_SoundInterval
theorem sound_BucketSum_DominatedTop : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.DominatedTop.Goal := dominatedTop_of_sound sound_BucketSum_SoundInterval
theorem sound_BucketSum_CrossPageJoin : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.CrossPageJoin.Goal := crossPageJoin_of_sound sound_BucketSum_SoundInterval

theorem sound_BucketSum_BucketInverse : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.BucketInverse.Goal := by
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

theorem sound_BucketSum_SemijoinFilter : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.SemijoinFilter.Goal := semijoinFilter_of_sound sound_BucketSum_SoundInterval
theorem sound_BucketSum_SumCeiling : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.SumCeiling.Goal := sumCeiling_of_sound sound_BucketSum_SoundInterval

theorem sound_BucketSum_SingleGroup : TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum.SingleGroup.Goal := by
  intro t a b h
  unfold kernel
  simp only [h]

#guard (BucketSum.kernel.score 9 (12, 8) : Nat) = 2
#guard (BucketSum.kernel.bounds 9 ⟨1, 2⟩ ⟨3, 4⟩ : Bounds) = ⟨0, 0⟩

end BucketSum

end TinylambOpt.ResearchProofs.StorageExecution.DerivedZonesA
