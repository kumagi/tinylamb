import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

namespace TinylambOpt.ResearchProofs.StorageExecution.DerivedZonesB

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! Generic consequences of a certified scalar interval. -/

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

/-! WrappedSum -/

theorem mod_le_of_div_eq {a b d : Nat} (hab : a ≤ b) (hdiv : a / d = b / d) :
    a % d ≤ b % d := by
  have ha := Nat.mod_add_div a d
  have hb := Nat.mod_add_div b d
  rw [hdiv] at ha
  omega

theorem wrappedSum_sound : WrappedSum.SoundInterval.Goal := by
  intro t a b x y hax hby
  by_cases hq : (a.lo + b.lo) / (t + 1) = (a.hi + b.hi) / (t + 1)
  · have hd : 0 < t + 1 := by omega
    have hsumlo : a.lo + b.lo ≤ x + y := Nat.add_le_add hax.1 hby.1
    have hsumhi : x + y ≤ a.hi + b.hi := Nat.add_le_add hax.2 hby.2
    have hqlo := Nat.div_le_div_right (c := t + 1) hsumlo
    have hqhi := Nat.div_le_div_right (c := t + 1) hsumhi
    have hqxlo : (a.lo + b.lo) / (t + 1) = (x + y) / (t + 1) :=
      Nat.le_antisymm hqlo (by rw [hq]; exact hqhi)
    have hqxhi : (x + y) / (t + 1) = (a.hi + b.hi) / (t + 1) :=
      Nat.le_antisymm hqhi (by rw [← hq]; exact hqlo)
    simp only [WrappedSum.kernel, hq, ite_true, inside]
    exact ⟨mod_le_of_div_eq hsumlo hqxlo,
      mod_le_of_div_eq hsumhi hqxhi⟩
  · simp only [WrappedSum.kernel, hq, ite_false, inside]
    have hd : 0 < t + 1 := by omega
    have hm := Nat.mod_lt (x + y) hd
    exact ⟨Nat.zero_le _, by omega⟩

theorem sound_WrappedSum_SoundInterval : WrappedSum.SoundInterval.Goal := wrappedSum_sound
theorem sound_WrappedSum_SkipRange : WrappedSum.SkipRange.Goal := skipRange_of_sound wrappedSum_sound
theorem sound_WrappedSum_TakeAll : WrappedSum.TakeAll.Goal := takeAll_of_sound wrappedSum_sound
theorem sound_WrappedSum_ThresholdCount : WrappedSum.ThresholdCount.Goal := thresholdCount_of_sound wrappedSum_sound
theorem sound_WrappedSum_DominatedTop : WrappedSum.DominatedTop.Goal := dominatedTop_of_sound wrappedSum_sound
theorem sound_WrappedSum_CrossPageJoin : WrappedSum.CrossPageJoin.Goal := crossPageJoin_of_sound wrappedSum_sound

theorem sound_WrappedSum_TranslateCycle : WrappedSum.TranslateCycle.Goal := by
  intro t n x y
  simp only [WrappedSum.kernel]
  have heq : x + n * (t + 1) + y = x + y + n * (t + 1) := by omega
  rw [heq, Nat.add_mul_mod_self_right]

theorem sound_WrappedSum_SemijoinFilter : WrappedSum.SemijoinFilter.Goal :=
  semijoinFilter_of_sound wrappedSum_sound
theorem sound_WrappedSum_SumCeiling : WrappedSum.SumCeiling.Goal := sumCeiling_of_sound wrappedSum_sound

theorem sound_WrappedSum_Rollover : WrappedSum.Rollover.Goal := by
  intro t a b h
  have hne : (a.lo + b.lo) / (t + 1) ≠ (a.hi + b.hi) / (t + 1) := Nat.ne_of_lt h
  simp [WrappedSum.kernel, hne]

#guard WrappedSum.kernel.score 3 (2 + 4 * (3 + 1), 7) == WrappedSum.kernel.score 3 (2, 7)
#guard WrappedSum.kernel.bounds 3 ⟨2,8⟩ ⟨0,1⟩ == ⟨0,3⟩

/-! Manhattan -/

theorem gapMin_point_le {a : Bounds} {t x : Nat} (hx : inside a x) :
    gapMin a ⟨t,t⟩ ≤ gap x t := by
  change max (a.lo - t) (t - a.hi) ≤ x - t + (t - x)
  simp only [inside] at hx
  apply Nat.max_le.mpr
  constructor <;> omega

theorem gap_point_le_max {a : Bounds} {t x : Nat} (hx : inside a x) :
    gap x t ≤ gapMax a ⟨t,t⟩ := by
  change x - t + (t - x) ≤ max (a.hi - t) (t - a.lo)
  simp only [inside] at hx
  by_cases hxt : x ≤ t
  · apply Nat.le_trans _ (Nat.le_max_right _ _)
    omega
  · apply Nat.le_trans _ (Nat.le_max_left _ _)
    omega

theorem gapMax_point_witness {a : Bounds} {t : Nat} (ha : a.lo ≤ a.hi) :
    ∃ x, (x = a.lo ∨ x = a.hi) ∧ gap x t = gapMax a ⟨t,t⟩ := by
  by_cases hcmp : t - a.lo ≤ a.hi - t
  · refine ⟨a.hi, Or.inr rfl, ?_⟩
    change a.hi - t + (t - a.hi) = max (a.hi - t) (t - a.lo)
    rw [Nat.max_eq_left hcmp]
    have ht : t ≤ a.hi := by omega
    have hz : t - a.hi = 0 := Nat.sub_eq_zero_of_le ht
    simp [hz]
  · refine ⟨a.lo, Or.inl rfl, ?_⟩
    change a.lo - t + (t - a.lo) = max (a.hi - t) (t - a.lo)
    rw [Nat.max_eq_right (by omega)]
    have ht : a.lo ≤ t := by omega
    have hz : a.lo - t = 0 := Nat.sub_eq_zero_of_le ht
    simp [hz]

theorem manhattan_sound : Manhattan.SoundInterval.Goal := by
  intro t a b x y hax hby
  simp only [Manhattan.kernel, inside]
  exact ⟨Nat.add_le_add (gapMin_point_le hax) (gapMin_point_le hby),
    Nat.add_le_add (gap_point_le_max hax) (gap_point_le_max hby)⟩

theorem sound_Manhattan_SoundInterval : Manhattan.SoundInterval.Goal := manhattan_sound
theorem sound_Manhattan_SkipRange : Manhattan.SkipRange.Goal := skipRange_of_sound manhattan_sound
theorem sound_Manhattan_TakeAll : Manhattan.TakeAll.Goal := takeAll_of_sound manhattan_sound
theorem sound_Manhattan_ThresholdCount : Manhattan.ThresholdCount.Goal := thresholdCount_of_sound manhattan_sound
theorem sound_Manhattan_DominatedTop : Manhattan.DominatedTop.Goal := dominatedTop_of_sound manhattan_sound
theorem sound_Manhattan_CrossPageJoin : Manhattan.CrossPageJoin.Goal := crossPageJoin_of_sound manhattan_sound

theorem peak_le_of_forall {xs : List Nat} {m : Nat} (hne : xs ≠ [])
    (h : ∀ x ∈ xs, x ≤ m) : peak xs ≤ m := by
  cases hmax : xs.max? with
  | none => exact False.elim (hne (List.max?_eq_none_iff.mp hmax))
  | some v =>
    simpa [peak, hmax] using (List.max?_le_iff hmax).mpr h

theorem sound_Manhattan_WitnessCorners : Manhattan.WitnessCorners.Goal := by
  intro t a b ha hb
  let corners := [a.lo,a.hi].flatMap (fun x => [b.lo,b.hi].map (fun y => Manhattan.kernel.score t (x,y)))
  change (Manhattan.kernel.bounds t a b).hi = peak corners
  have hupper : ∀ v ∈ corners, v ≤ (Manhattan.kernel.bounds t a b).hi := by
    intro v hv
    simp only [corners, List.mem_flatMap, List.mem_map] at hv
    rcases hv with ⟨x, hx, y, hy, rfl⟩
    simp only [List.mem_cons, List.not_mem_nil, or_false] at hx hy
    rcases hx with rfl | rfl <;> rcases hy with rfl | rfl <;>
      simpa only [Manhattan.kernel] using
        Nat.add_le_add (gap_point_le_max (by simp [inside]; omega))
          (gap_point_le_max (by simp [inside]; omega))
  obtain ⟨x, hx, hgx⟩ := gapMax_point_witness (t := t) ha
  obtain ⟨y, hy, hgy⟩ := gapMax_point_witness (t := t) hb
  have hmem : Manhattan.kernel.score t (x,y) ∈ corners := by
    simp only [corners, List.mem_flatMap, List.mem_map]
    exact ⟨x, by simp [hx], y, by simp [hy], rfl⟩
  have hmax : corners.max? = some (Manhattan.kernel.bounds t a b).hi := by
    apply List.max?_eq_some_iff.mpr
    refine ⟨?_, ?_⟩
    · simpa only [Manhattan.kernel, hgx, hgy] using hmem
    · intro v hv
      simpa only [Manhattan.kernel, hgx, hgy] using hupper v hv
  simp [peak, hmax]

theorem sound_Manhattan_SemijoinFilter : Manhattan.SemijoinFilter.Goal :=
  semijoinFilter_of_sound manhattan_sound
theorem sound_Manhattan_SumCeiling : Manhattan.SumCeiling.Goal := sumCeiling_of_sound manhattan_sound

theorem sound_Manhattan_InteriorMinimum : Manhattan.InteriorMinimum.Goal := by
  intro t a b ha hb
  change max (a.lo - t) (t - a.hi) + max (b.lo - t) (t - b.hi) = 0
  simp only [Nat.sub_eq_zero_of_le ha.1, Nat.sub_eq_zero_of_le ha.2,
    Nat.sub_eq_zero_of_le hb.1, Nat.sub_eq_zero_of_le hb.2, Nat.max_self, Nat.add_zero]

#guard peak [3,1,4,1] == 4
#guard Manhattan.kernel.bounds 5 ⟨3,7⟩ ⟨4,8⟩ == ⟨0,5⟩

/-! CappedPair -/

theorem nat_min_le_min {a b c d : Nat} (h1 : a ≤ c) (h2 : b ≤ d) :
    min a b ≤ min c d := by
  apply Nat.le_min.mpr
  constructor
  · exact Nat.le_trans (Nat.min_le_left a b) h1
  · exact Nat.le_trans (Nat.min_le_right a b) h2

theorem cappedPair_sound : CappedPair.SoundInterval.Goal := by
  intro t a b x y hax hby
  simp only [CappedPair.kernel, inside]
  exact ⟨nat_min_le_min (Nat.le_refl t) (nat_min_le_min hax.1 hby.1),
    nat_min_le_min (Nat.le_refl t) (nat_min_le_min hax.2 hby.2)⟩

theorem sound_CappedPair_SoundInterval : CappedPair.SoundInterval.Goal := cappedPair_sound
theorem sound_CappedPair_SkipRange : CappedPair.SkipRange.Goal := skipRange_of_sound cappedPair_sound
theorem sound_CappedPair_TakeAll : CappedPair.TakeAll.Goal := takeAll_of_sound cappedPair_sound
theorem sound_CappedPair_ThresholdCount : CappedPair.ThresholdCount.Goal := thresholdCount_of_sound cappedPair_sound
theorem sound_CappedPair_DominatedTop : CappedPair.DominatedTop.Goal := dominatedTop_of_sound cappedPair_sound
theorem sound_CappedPair_CrossPageJoin : CappedPair.CrossPageJoin.Goal := crossPageJoin_of_sound cappedPair_sound

theorem sound_CappedPair_PushBoth : CappedPair.PushBoth.Goal := by
  intro t k x y hk
  simp only [CappedPair.kernel, Nat.le_min]
  omega

theorem sound_CappedPair_SemijoinFilter : CappedPair.SemijoinFilter.Goal :=
  semijoinFilter_of_sound cappedPair_sound
theorem sound_CappedPair_SumCeiling : CappedPair.SumCeiling.Goal := sumCeiling_of_sound cappedPair_sound

theorem sound_CappedPair_SaturatedPage : CappedPair.SaturatedPage.Goal := by
  intro t a b ha hb
  simp only [CappedPair.kernel]
  exact Nat.min_eq_left (Nat.le_min.mpr ⟨ha, hb⟩)

#guard CappedPair.kernel.score 10 (7,12) == 7
#guard CappedPair.kernel.bounds 10 ⟨10,12⟩ ⟨11,13⟩ == ⟨10,10⟩

/-! QuadraticGap -/

theorem gapMin_le_gap {a b : Bounds} {x y : Nat} (hax : inside a x) (hby : inside b y) :
    gapMin a b ≤ gap x y := by
  change max (a.lo - b.hi) (b.lo - a.hi) ≤ x - y + (y - x)
  simp only [inside] at hax hby
  apply Nat.max_le.mpr
  constructor <;> omega

theorem gap_le_gapMax {a b : Bounds} {x y : Nat} (hax : inside a x) (hby : inside b y) :
    gap x y ≤ gapMax a b := by
  change x - y + (y - x) ≤ max (a.hi - b.lo) (b.hi - a.lo)
  simp only [inside] at hax hby
  by_cases hxy : x ≤ y
  · apply Nat.le_trans _ (Nat.le_max_right _ _)
    omega
  · apply Nat.le_trans _ (Nat.le_max_left _ _)
    omega

theorem quadraticGap_sound : QuadraticGap.SoundInterval.Goal := by
  intro t a b x y hax hby
  simpa only [QuadraticGap.kernel, inside, Nat.pow_two, Nat.add_comm] using
    ⟨Nat.add_le_add_left (Nat.mul_self_le_mul_self (gapMin_le_gap hax hby)) t,
      Nat.add_le_add_left (Nat.mul_self_le_mul_self (gap_le_gapMax hax hby)) t⟩

theorem sound_QuadraticGap_SoundInterval : QuadraticGap.SoundInterval.Goal := quadraticGap_sound
theorem sound_QuadraticGap_SkipRange : QuadraticGap.SkipRange.Goal := skipRange_of_sound quadraticGap_sound
theorem sound_QuadraticGap_TakeAll : QuadraticGap.TakeAll.Goal := takeAll_of_sound quadraticGap_sound
theorem sound_QuadraticGap_ThresholdCount : QuadraticGap.ThresholdCount.Goal := thresholdCount_of_sound quadraticGap_sound
theorem sound_QuadraticGap_DominatedTop : QuadraticGap.DominatedTop.Goal := dominatedTop_of_sound quadraticGap_sound
theorem sound_QuadraticGap_CrossPageJoin : QuadraticGap.CrossPageJoin.Goal := crossPageJoin_of_sound quadraticGap_sound

theorem sq_le_sq_iff (a r : Nat) : a * a ≤ r * r ↔ a ≤ r := by
  constructor
  · intro h
    apply Nat.le_of_not_gt
    intro har
    exact Nat.not_le_of_gt (Nat.mul_self_lt_mul_self har) h
  · intro h
    exact Nat.mul_self_le_mul_self h

theorem sound_QuadraticGap_SquaredRadius : QuadraticGap.SquaredRadius.Goal := by
  intro t r x y
  simp only [QuadraticGap.kernel, Nat.pow_two]
  change gap x y * gap x y + t ≤ r * r + t ↔ x ≤ y + r ∧ y ≤ x + r
  have hcancel : gap x y * gap x y + t ≤ r * r + t ↔ gap x y * gap x y ≤ r * r := by
    omega
  rw [hcancel, sq_le_sq_iff]
  unfold gap
  omega

theorem sound_QuadraticGap_SemijoinFilter : QuadraticGap.SemijoinFilter.Goal :=
  semijoinFilter_of_sound quadraticGap_sound
theorem sound_QuadraticGap_SumCeiling : QuadraticGap.SumCeiling.Goal := sumCeiling_of_sound quadraticGap_sound

theorem refuted_QuadraticGap_GapWitness : ¬ QuadraticGap.GapWitness.Goal := by
  intro h
  have hw := h 0 ⟨10,11⟩ ⟨12,5⟩ (by decide)
  simp [QuadraticGap.kernel, gapMin, gap] at hw

#guard (QuadraticGap.kernel.bounds 0 ⟨10,11⟩ ⟨12,5⟩).lo == 25
#guard QuadraticGap.kernel.score 0 (11,12) == 1

#guard QuadraticGap.kernel.score 7 (3,10) == 56
#guard QuadraticGap.kernel.bounds 7 ⟨3,4⟩ ⟨8,9⟩ == ⟨23,43⟩

/-! BitwiseXor -/

theorem lt_two_pow_of_width_le {n w : Nat} (h : width n ≤ w) : n < 2 ^ w := by
  by_cases hn : n = 0
  · subst n
    exact Nat.two_pow_pos w
  · have hw : n.log2 + 1 ≤ w := by simpa [width, hn] using h
    exact (Nat.log2_lt hn).mp (by omega)

theorem eq_of_xor_eq_zero {a b : Nat} (h : (a ^^^ b) = 0) : a = b := by
  calc
    a = a ^^^ 0 := (Nat.xor_zero a).symm
    _ = a ^^^ (b ^^^ b) := by rw [Nat.xor_self b]
    _ = (a ^^^ b) ^^^ b := (Nat.xor_assoc a b b).symm
    _ = b := by rw [h, Nat.zero_xor]

theorem div_eq_of_xor_lt {a b w : Nat} (h : (a ^^^ b) < 2 ^ w) :
    a / 2 ^ w = b / 2 ^ w := by
  apply eq_of_xor_eq_zero
  rw [← Nat.xor_div_two_pow]
  exact Nat.div_eq_of_lt h

theorem bitwiseXor_sound : BitwiseXor.SoundInterval.Goal := by
  intro t a b x y hax hby
  let w := max (width (a.lo ^^^ a.hi)) (width (b.lo ^^^ b.hi))
  let base := ((a.lo ^^^ b.lo) / 2 ^ w) * 2 ^ w
  change base ≤ x ^^^ y ∧ x ^^^ y ≤ base + 2 ^ w - 1
  have hd : 0 < 2 ^ w := Nat.two_pow_pos w
  have hwa : width (a.lo ^^^ a.hi) ≤ w := Nat.le_max_left _ _
  have hwb : width (b.lo ^^^ b.hi) ≤ w := Nat.le_max_right _ _
  have hqax : a.lo / 2 ^ w = a.hi / 2 ^ w :=
    div_eq_of_xor_lt (lt_two_pow_of_width_le hwa)
  have hqby : b.lo / 2 ^ w = b.hi / 2 ^ w :=
    div_eq_of_xor_lt (lt_two_pow_of_width_le hwb)
  have hqxx : a.lo / 2 ^ w = x / 2 ^ w :=
    Nat.le_antisymm (Nat.div_le_div_right (c := 2 ^ w) hax.1)
      (by rw [hqax]; exact Nat.div_le_div_right (c := 2 ^ w) hax.2)
  have hqyy : b.lo / 2 ^ w = y / 2 ^ w :=
    Nat.le_antisymm (Nat.div_le_div_right (c := 2 ^ w) hby.1)
      (by rw [hqby]; exact Nat.div_le_div_right (c := 2 ^ w) hby.2)
  have hqx : a.lo / 2 ^ w = x / 2 ^ w := hqxx
  have hqy : b.lo / 2 ^ w = y / 2 ^ w := hqyy
  have hq : (x ^^^ y) / 2 ^ w = base / 2 ^ w := by
    rw [Nat.xor_div_two_pow, ← hqx, ← hqy]
    change (a.lo / 2 ^ w) ^^^ (b.lo / 2 ^ w) =
      (((a.lo ^^^ b.lo) / 2 ^ w) * 2 ^ w) / 2 ^ w
    rw [Nat.mul_div_left _ hd]
    exact (Nat.xor_div_two_pow (a := a.lo) (b := b.lo) (n := w)).symm
  have hm : (x ^^^ y) % 2 ^ w < 2 ^ w := by
    rw [Nat.xor_mod_two_pow]
    exact Nat.xor_lt_two_pow (Nat.mod_lt x hd) (Nat.mod_lt y hd)
  have hbase : base = (base / 2 ^ w) * 2 ^ w := by
    simp [base, Nat.mul_div_left _ hd]
  have hdec := Nat.div_add_mod (x ^^^ y) (2 ^ w)
  rw [hq, Nat.mul_comm (2 ^ w) (base / 2 ^ w)] at hdec
  constructor
  · rw [hbase]
    omega
  · rw [hbase]
    omega

theorem sound_BitwiseXor_SoundInterval : BitwiseXor.SoundInterval.Goal := bitwiseXor_sound
theorem sound_BitwiseXor_SkipRange : BitwiseXor.SkipRange.Goal := skipRange_of_sound bitwiseXor_sound
theorem sound_BitwiseXor_TakeAll : BitwiseXor.TakeAll.Goal := takeAll_of_sound bitwiseXor_sound
theorem sound_BitwiseXor_ThresholdCount : BitwiseXor.ThresholdCount.Goal := thresholdCount_of_sound bitwiseXor_sound
theorem sound_BitwiseXor_DominatedTop : BitwiseXor.DominatedTop.Goal := dominatedTop_of_sound bitwiseXor_sound
theorem sound_BitwiseXor_CrossPageJoin : BitwiseXor.CrossPageJoin.Goal := crossPageJoin_of_sound bitwiseXor_sound

theorem xor_aligned_block (a k : Nat) :
    (a * 2 ^ k) ^^^ ((a + 1) * 2 ^ k - 1) = 2 ^ k - 1 := by
  let d := 2 ^ k
  have hd : 0 < d := Nat.two_pow_pos k
  have hdm : d - 1 < d := by omega
  have haeq : (a + 1) * d - 1 = (d - 1) + d * a := by
    rw [Nat.add_mul, Nat.one_mul, Nat.mul_comm d a]
    omega
  have hdivlo : a * d / d = a := Nat.mul_div_left a hd
  have hdivhi : ((a + 1) * d - 1) / d = a := by
    rw [haeq, Nat.add_mul_div_left _ _ hd, Nat.div_eq_of_lt hdm, Nat.zero_add]
  have hmodlo : a * d % d = 0 := Nat.mul_mod_left a d
  have hmodhi : ((a + 1) * d - 1) % d = d - 1 := by
    rw [haeq, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hdm]
  have hq : ((a * d) ^^^ ((a + 1) * d - 1)) / d = 0 := by
    rw [Nat.xor_div_two_pow, hdivlo, hdivhi, Nat.xor_self]
  have hm : ((a * d) ^^^ ((a + 1) * d - 1)) % d = d - 1 := by
    rw [Nat.xor_mod_two_pow, hmodlo, hmodhi, Nat.zero_xor]
  have hdec := Nat.div_add_mod ((a * d) ^^^ ((a + 1) * d - 1)) d
  rw [hq, hm] at hdec
  simpa using hdec.symm

theorem width_two_pow_sub_one (k : Nat) : width (2 ^ k - 1) = k := by
  cases k with
  | zero => simp [width]
  | succ k =>
    have hn : 2 ^ (k + 1) - 1 ≠ 0 := by
      have hpos := Nat.two_pow_pos (k + 1)
      omega
    have hlog : (2 ^ (k + 1) - 1).log2 = k := by
      apply (Nat.log2_eq_iff hn).mpr
      constructor
      · have hlt := Nat.two_pow_pred_lt_two_pow (w := k + 1) (by omega)
        omega
      · omega
    simp [width, hn, hlog]

theorem sound_BitwiseXor_AlignedSiblings : BitwiseXor.AlignedSiblings.Goal := by
  intro k a b
  let d := 2 ^ k
  have hd : 0 < d := Nat.two_pow_pos k
  have hxa : (a * 2 ^ k) ^^^ ((a + 1) * 2 ^ k - 1) = 2 ^ k - 1 := xor_aligned_block a k
  have hxb : (b * 2 ^ k) ^^^ ((b + 1) * 2 ^ k - 1) = 2 ^ k - 1 := xor_aligned_block b k
  have hshift : (a * d) ^^^ (b * d) = (a ^^^ b) * d := by
    calc
      (a * d) ^^^ (b * d) = (a <<< k) ^^^ (b <<< k) := by
        rw [Nat.shiftLeft_eq, Nat.shiftLeft_eq]
      _ = (a ^^^ b) <<< k := Nat.shiftLeft_xor_distrib.symm
      _ = (a ^^^ b) * d := Nat.shiftLeft_eq _ _
  simp only [BitwiseXor.kernel]
  simp only [hxa, hxb, width_two_pow_sub_one, Nat.max_self]
  change (((a * d) ^^^ (b * d)) / d * d) = (a ^^^ b) * d
  rw [hshift, Nat.mul_div_left _ hd]

theorem sound_BitwiseXor_SemijoinFilter : BitwiseXor.SemijoinFilter.Goal :=
  semijoinFilter_of_sound bitwiseXor_sound
theorem sound_BitwiseXor_SumCeiling : BitwiseXor.SumCeiling.Goal := sumCeiling_of_sound bitwiseXor_sound

theorem sound_BitwiseXor_FixedPrefix : BitwiseXor.FixedPrefix.Goal := by
  intro x y
  simp [BitwiseXor.kernel, width]

#guard (BitwiseXor.kernel.bounds 0 ⟨8,11⟩ ⟨4,7⟩).lo == 12
#guard (BitwiseXor.kernel.bounds 0 ⟨8,11⟩ ⟨4,7⟩).hi == 15
#guard BitwiseXor.kernel.bounds 0 ⟨9,9⟩ ⟨6,6⟩ == ⟨15,15⟩
#guard (BitwiseXor.kernel.bounds 0 ⟨2*8,3*8-1⟩ ⟨1*8,2*8-1⟩).lo == 3*8

end TinylambOpt.ResearchProofs.StorageExecution.DerivedZonesB
