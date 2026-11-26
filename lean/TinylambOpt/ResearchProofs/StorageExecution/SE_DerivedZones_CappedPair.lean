import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved (10 claims, including the following)

- `CappedPair.CrossPageJoin` — Disjoint derived score intervals imply an empty equijoin
  between pages.
- `CappedPair.DominatedTop` — A bounded page cannot change the descending top k once k
  strictly better witnesses are known.
- `CappedPair.PushBoth` — `∀ t k x y, k≤t → (k≤kernel.score t (x,y) ↔ k≤x ∧ k≤y)`
-/
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.DerivedZones.CappedPair
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-! Generic derivations: every Kernel property except `soundInterval` follows
from `soundInterval` by interval reasoning over the page row list. -/
namespace Aux

abbrev DGe (x y : Nat) : Prop := (· ≥ · : Nat → Nat → Bool) x y = true

theorem geR_trans : ∀ {a b c : Nat}, DGe a b → DGe b c → DGe a c := by
  intro a b c h1 h2
  have h1' : a ≥ b := by simpa [DGe] using h1
  have h2' : b ≥ c := by simpa [DGe] using h2
  simpa [DGe] using Nat.le_trans h2' h1'

abbrev GeB (x y : Nat) : Bool := decide (x ≥ y)

theorem geB_trans : ∀ a b c, GeB a b = true → GeB b c = true → GeB a c = true := by
  intro a b c h1 h2
  have h1' : a ≥ b := by simpa [GeB] using h1
  have h2' : b ≥ c := by simpa [GeB] using h2
  simpa [GeB] using Nat.le_trans h2' h1'

theorem geB_total : ∀ a b, GeB a b || GeB b a = true := by
  intro a b
  have h : a ≥ b ∨ b ≥ a := by omega
  cases h with
  | inl h =>
    rw [show GeB a b = true from by simpa [GeB] using h]
    simp
  | inr h =>
    rw [show GeB b a = true from by simpa [GeB] using h]
    simp

theorem ge_total_bool : ∀ a b : Nat,
    ((· ≥ ·) : Nat → Nat → Bool) a b || (· ≥ ·) b a = true := by
  intro a b
  have h : a ≥ b ∨ b ≥ a := by omega
  cases h with
  | inl h =>
    have h1 : ((· ≥ ·) : Nat → Nat → Bool) a b = true := by simpa using h
    simp [h1]
  | inr h =>
    have h1 : ((· ≥ ·) : Nat → Nat → Bool) b a = true := by simpa using h
    simp [h1]

theorem filter_nil_of {l : List Nat} {p : Nat → Bool} (h : ∀ v ∈ l, p v = false) : l.filter p = [] := by
  rw [List.filter_eq_nil_iff]; intro v hv; simpa using h v hv

theorem countP_zero_of {l : List Nat} {p : Nat → Bool} (h : ∀ v ∈ l, ¬ p v) : l.countP p = 0 := by
  rw [List.countP_eq_zero]; exact h

theorem countP_len_of {l : List Nat} {p : Nat → Bool} (h : ∀ v ∈ l, p v) : l.countP p = l.length := by
  rw [List.countP_eq_length]; exact h

theorem countP_eq_of_perm {l₁ l₂ : List Nat} (h : List.Perm l₁ l₂) (p : Nat → Bool) :
    l₁.countP p = l₂.countP p := by
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter, (List.Perm.filter p h).length_eq]

theorem sum_le_sum_of {α : Type} (ps : List α) {g h : α → Nat} (hh : ∀ e ∈ ps, g e ≤ h e) :
    (ps.map g).sum ≤ (ps.map h).sum := by
  induction ps with
  | nil => simp
  | cons a t ih =>
    rw [List.map_cons, List.sum_cons, List.map_cons, List.sum_cons]
    exact Nat.add_le_add (hh a (List.mem_cons_self (l := t)))
      (ih (fun e he => hh e (List.mem_cons_of_mem a he)))

theorem sum_filtered_eq_if {α : Type} (ps : List α) (c : α → Prop) [DecidablePred c] (n : α → Nat) :
    ((ps.filter c).map n).sum = (ps.map (fun e => if c e then n e else 0)).sum := by
  induction ps with
  | nil => rfl
  | cons a t ih =>
    by_cases h : c a
    · rw [List.filter_cons_of_pos (by simpa using h), List.map_cons, List.sum_cons, ih,
        List.map_cons, List.sum_cons, if_pos h]
    · rw [List.filter_cons_of_neg (by simpa using h), List.map_cons, List.sum_cons, ih, if_neg h]
      rw [Nat.zero_add]

theorem filter_congr_of_mem {l : List Nat} {p q : Nat → Bool} (h : ∀ v ∈ l, p v = q v) :
    l.filter p = l.filter q := by
  induction l with
  | nil => rfl
  | cons a t ih =>
    by_cases hp : (p a) = true
    · have hq : q a = true := by rw [← h a (List.mem_cons_self (l := t))]; exact hp
      rw [List.filter_cons_of_pos hp, List.filter_cons_of_pos hq]
      rw [ih (fun v hv => h v (List.mem_cons_of_mem a hv))]
    · by_cases hq : (q a) = true
      · exfalso
        have hp' : p a = true := by rw [h a (List.mem_cons_self (l := t))]; exact hq
        contradiction
      · rw [List.filter_cons_of_neg hp, List.filter_cons_of_neg hq]
        rw [ih (fun v hv => h v (List.mem_cons_of_mem a hv))]

theorem inside_scores {f : Kernel} (hf : soundInterval f) {t p a b} (hc : covers p a b) :
    ∀ v ∈ scores f t p, inside (f.bounds t a b) v := by
  intro v hv
  obtain ⟨r, hr, hm⟩ : ∃ r ∈ p, r.map (f.score t) = some v := by
    simpa [scores] using hv
  obtain ⟨x, y, hr2, hscore⟩ : ∃ x y, some (x, y) ∈ p ∧ f.score t (x, y) = v := by
    cases r with
    | none => cases hm
    | some xy =>
      cases xy with
      | mk x y => exact ⟨x, y, hr, by simpa using hm⟩
  obtain ⟨hax, hby⟩ := hc (some (x, y)) hr2 x y rfl
  rw [← hscore]
  exact hf t a b x y hax hby

theorem skipRange_of (f : Kernel) (hf : soundInterval f) : skipRange f := by
  intro t p a b lo hi hc hallow
  rw [selected, List.filter_eq_nil_iff]
  intro v hv
  by_cases hcj : (lo ≤ v && v < hi)
  · obtain ⟨hl, hu⟩ := inside_scores hf hc v hv
    have hboth : lo ≤ v ∧ v < hi := by simpa [Bool.and_eq_true] using hcj
    have c1 : ((lo ≤ (f.bounds t a b).hi) : Bool) = true := by
      simpa using Nat.le_trans hboth.1 hu
    have c2 : (((f.bounds t a b).lo < hi) : Bool) = true := by
      simpa using Nat.lt_of_le_of_lt hl hboth.2
    have c3 : ((lo < hi) : Bool) = true := by simpa using Nat.lt_of_le_of_lt hboth.1 hboth.2
    rw [allows] at hallow
    rw [c1, c2, c3] at hallow
    simp at hallow
  · exact hcj

theorem takeAll_of (f : Kernel) (hf : soundInterval f) : takeAll f := by
  intro t p a b lo hi hc hlo hhi
  rw [selected]
  refine List.filter_eq_self.mpr (fun v hv => ?_)
  obtain ⟨hl, hu⟩ := inside_scores hf hc v hv
  rw [Bool.and_eq_true]
  exact ⟨by simpa using Nat.le_trans hlo hl, by simpa using Nat.lt_of_le_of_lt hu hhi⟩

theorem length_scores {f : Kernel} {t p} : (scores f t p).length = (p.filterMap id).length := by
  induction p with
  | nil => rfl
  | cons o rest ih =>
    cases o with
    | none => simpa [scores] using ih
    | some xy => simpa [scores] using ih

theorem countP_map_flatten {f : Kernel} {t : Nat} {cut : Nat}
    (ps : List (List (Option Row) × Bounds × Bounds)) :
    ((ps.map Prod.fst).flatten.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut)
      = (ps.map (fun e => (scores f t e.1).countP (· ≥ cut))).sum := by
  induction ps with
  | nil => rfl
  | cons e rest ih =>
    obtain ⟨p, ab, bb⟩ := e
    change List.countP (fun x => decide (x ≥ cut))
        ((p ++ (List.map Prod.fst rest).flatten).filterMap (fun r => Option.map (f.score t) r)) = _
    rw [List.filterMap_append, List.countP_append, ih, List.map_cons, List.sum_cons]
    simp only [scores]

theorem thresholdCount_of (f : Kernel) (hf : soundInterval f) : thresholdCount f := by
  intro t ps cut hall
  show ((ps.filter (fun e => cut ≤ (f.bounds t e.2.1 e.2.2).lo)).map
        (fun e => (e.1.filterMap id).length)).sum
      ≤ ((ps.map Prod.fst).flatten.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut)
      ∧ ((ps.map Prod.fst).flatten.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut)
      ≤ ((ps.filter (fun e => cut ≤ (f.bounds t e.2.1 e.2.2).hi)).map
        (fun e => (e.1.filterMap id).length)).sum
  have hact : ((ps.map Prod.fst).flatten.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut)
      = (ps.map (fun e => (scores f t e.1).countP (· ≥ cut))).sum :=
    countP_map_flatten ps
  rw [hact]
  refine ⟨?_, ?_⟩
  · have h1 : (ps.map (fun e => if cut ≤ (f.bounds t e.2.1 e.2.2).lo
        then (e.1.filterMap id).length else 0)).sum
        ≤ (ps.map (fun e => (scores f t e.1).countP (· ≥ cut))).sum :=
      sum_le_sum_of ps (fun e he => by
        by_cases hc2 : cut ≤ (f.bounds t e.2.1 e.2.2).lo
        · rw [if_pos hc2]
          rw [← length_scores (f := f) (t := t) (p := e.1)]
          rw [countP_len_of (fun v hv => by
            simpa using Nat.le_trans hc2 (inside_scores hf (hall e he) v hv).1)]
          exact Nat.le_refl _
        · rw [if_neg hc2]
          exact Nat.zero_le _)
    have h2 : List.sum ((ps.filter (fun e => cut ≤ (f.bounds t e.2.1 e.2.2).lo)).map
          (fun e => (e.1.filterMap id).length))
        = List.sum (ps.map (fun e => if cut ≤ (f.bounds t e.2.1 e.2.2).lo
            then (e.1.filterMap id).length else 0)) :=
      sum_filtered_eq_if ps (fun e => cut ≤ (f.bounds t e.2.1 e.2.2).lo)
        (fun e => (e.1.filterMap id).length)
    rw [h2]; exact h1
  · have h1 : (ps.map (fun e => (scores f t e.1).countP (· ≥ cut))).sum
        ≤ (ps.map (fun e => if cut ≤ (f.bounds t e.2.1 e.2.2).hi
            then (e.1.filterMap id).length else 0)).sum :=
      sum_le_sum_of ps (fun e he => by
        by_cases hc2 : cut ≤ (f.bounds t e.2.1 e.2.2).hi
        · rw [if_pos hc2, ← length_scores (f := f) (t := t) (p := e.1)]
          exact List.countP_le_length
        · rw [if_neg hc2]
          rw [countP_zero_of (fun v hv hcut => by
            simpa using hc2 (Nat.le_trans (by simpa using hcut)
              (inside_scores hf (hall e he) v hv).2))]
          exact Nat.le_refl _)
    have h2 : List.sum ((ps.filter (fun e => cut ≤ (f.bounds t e.2.1 e.2.2).hi)).map
          (fun e => (e.1.filterMap id).length))
        = List.sum (ps.map (fun e => if cut ≤ (f.bounds t e.2.1 e.2.2).hi
            then (e.1.filterMap id).length else 0)) :=
      sum_filtered_eq_if ps (fun e => cut ≤ (f.bounds t e.2.1 e.2.2).hi)
        (fun e => (e.1.filterMap id).length)
    rw [h2]; exact h1

theorem take_eq_filter (hi : Nat) : ∀ (l : List Nat), List.Pairwise DGe l →
    ∀ k, k ≤ l.countP (fun x => hi < x) → l.take k = (l.filter (fun x => hi < x)).take k := by
  intro l
  induction l with
  | nil =>
    intro _ k hk
    have : k = 0 := Nat.eq_zero_of_le_zero (by simpa using hk)
    subst this; rfl
  | cons a t ih =>
    intro hl k hk
    have hall : ∀ b ∈ t, DGe a b := by
      intro b hb
      exact List.rel_of_pairwise_cons hl hb
    have hrest : List.Pairwise DGe t := by
      cases hl with
      | cons _ ht => exact ht
    rcases k with _ | k
    · rfl
    · by_cases hgt : hi < a
      · have hpos : ((fun x => hi < x) : Nat → Bool) a = true := by simpa using hgt
        have hc : (a :: t).countP (fun x => hi < x) = t.countP (fun x => hi < x) + 1 :=
          List.countP_cons_of_pos hpos
        rw [hc] at hk
        rw [List.take_cons (show 0 < k + 1 by omega),
          List.filter_cons_of_pos (p := fun x => decide (hi < x)) hpos,
          List.take_cons (show 0 < k + 1 by omega)]
        rw [Nat.add_sub_cancel_right]
        exact congrArg (a :: ·) (ih hrest k (by omega))
      · have hneg : ¬ ((fun x => hi < x) : Nat → Bool) a := by simpa using hgt
        have hz : t.countP (fun x => hi < x) = 0 := by
          refine countP_zero_of (fun x hx hxlt => ?_)
          have hax : DGe a x := hall x hx
          have hax' : a ≥ x := by simpa [DGe] using hax
          have hxlt' : hi < x := by simpa using hxlt
          exact hgt (Nat.lt_of_lt_of_le hxlt' hax')
        have hc : (a :: t).countP (fun x => hi < x) = 0 := by
          rw [List.countP_cons_of_neg (p := fun x => decide (hi < x)) hneg, hz]
        rw [hc] at hk
        exact absurd hk (Nat.not_succ_le_zero k)

theorem antisymm_DGe (u v : List Nat) :
    ∀ a b : Nat, a ∈ u → b ∈ v → DGe a b → DGe b a → a = b := by
  intro a b _ _ h1 h2
  have h1' : a ≥ b := by simpa [DGe] using h1
  have h2' : b ≥ a := by simpa [DGe] using h2
  exact Nat.le_antisymm h2' h1'

theorem dominatedTop_of (f : Kernel) (hf : soundInterval f) : dominatedTop f := by
  intro t p a b seen k hc hk
  let hi := (f.bounds t a b).hi
  have hy : ∀ v ∈ scores f t p, ¬ ((fun x => hi < x) : Nat → Bool) v := by
    intro v hv hlt
    exact Nat.not_lt_of_le (inside_scores hf hc v hv).2 (by simpa using hlt)
  have hpS : List.Perm (List.mergeSort (seen ++ scores f t p) (· ≥ ·)) (seen ++ scores f t p) :=
    List.mergeSort_perm _ _
  have hpA : List.Perm (List.mergeSort seen (· ≥ ·)) seen := List.mergeSort_perm _ _
  have hsortS : List.Pairwise DGe (List.mergeSort (seen ++ scores f t p) (· ≥ ·)) :=
    List.pairwise_mergeSort (fun a b c h1 h2 => geB_trans a b c h1 h2) geB_total _
  have hsortA : List.Pairwise DGe (List.mergeSort seen (· ≥ ·)) :=
    List.pairwise_mergeSort (fun a b c h1 h2 => geB_trans a b c h1 h2) geB_total _
  have hS : k ≤ (List.mergeSort (seen ++ scores f t p) (· ≥ ·)).countP (fun x => hi < x) := by
    rw [countP_eq_of_perm hpS (fun x => hi < x), List.countP_append,
      countP_zero_of (fun v hv hlt => hy v hv hlt)]
    exact hk
  have hA : k ≤ (List.mergeSort seen (· ≥ ·)).countP (fun x => hi < x) := by
    rw [countP_eq_of_perm hpA (fun x => hi < x)]
    exact hk
  have hzero : List.filter (fun x => hi < x) (scores f t p) = [] :=
    filter_nil_of (fun v hv => by simpa using hy v hv)
  have hpf : List.Perm (List.filter (fun x => hi < x)
      (List.mergeSort (seen ++ scores f t p) (· ≥ ·))) (List.filter (fun x => hi < x) seen) := by
    refine (List.Perm.filter (fun x => hi < x) hpS).trans ?_
    rw [List.filter_append, hzero, List.append_nil]
  have hpA' : List.Perm (List.filter (fun x => hi < x) (List.mergeSort seen (· ≥ ·)))
      (List.filter (fun x => hi < x) seen) :=
    List.Perm.filter (fun x => hi < x) hpA
  have hfilt : List.filter (fun x => hi < x) (List.mergeSort (seen ++ scores f t p) (· ≥ ·))
      = List.filter (fun x => hi < x) (List.mergeSort seen (· ≥ ·)) :=
    List.Perm.eq_of_pairwise (antisymm_DGe _ _)
      (List.Pairwise.sublist List.filter_sublist hsortS)
      (List.Pairwise.sublist List.filter_sublist hsortA)
      (hpf.trans hpA'.symm)
  rw [take_eq_filter hi _ hsortS k hS, hfilt, ← take_eq_filter hi _ hsortA k hA]

theorem crossPageJoin_of (f : Kernel) (hf : soundInterval f) : crossPageJoin f := by
  intro t u p q a b c d hcp hcq hlt x hx hxq
  obtain ⟨_, hup⟩ := inside_scores hf hcp x hx
  obtain ⟨hlo, _⟩ := inside_scores hf hcq x hxq
  exact Nat.lt_irrefl x (Nat.lt_of_lt_of_le (Nat.lt_of_le_of_lt hup hlt) hlo)

theorem contains_filter_zone {ks : List Nat} {v lo hi : Nat} (h : lo ≤ v ∧ v ≤ hi) :
    ks.contains v = (ks.filter (fun k => lo ≤ k && k ≤ hi)).contains v := by
  induction ks with
  | nil => rfl
  | cons k t ih =>
    by_cases hk : k = v
    · subst hk
      have hw : ((fun z : Nat => lo ≤ z && z ≤ hi) k) = true := by simpa using h
      rw [List.filter_cons_of_pos (p := fun z : Nat => lo ≤ z && z ≤ hi) hw,
        List.contains_cons, List.contains_cons]
      simp
    · have hkv : (v == k) = false := by
        simpa using (show ¬ (v = k) from fun e => hk e.symm)
      by_cases hw : (lo ≤ k && k ≤ hi)
      · have hw' : ((fun z : Nat => lo ≤ z && z ≤ hi) k) = true := hw
        rw [List.filter_cons_of_pos (p := fun z : Nat => lo ≤ z && z ≤ hi) hw',
          List.contains_cons, List.contains_cons, hkv,
          Bool.false_or, Bool.false_or, ih]
      · have hn : ¬ ((fun z : Nat => lo ≤ z && z ≤ hi) k) := hw
        rw [List.filter_cons_of_neg (p := fun z : Nat => lo ≤ z && z ≤ hi) hn,
          List.contains_cons, hkv, Bool.false_or, ih]

theorem semijoinFilter_of (f : Kernel) (hf : soundInterval f) : semijoinFilter f := by
  intro t p a b ks hc
  refine filter_congr_of_mem (fun v hv => ?_)
  exact contains_filter_zone (inside_scores hf hc v hv)

theorem sumCeiling_of (f : Kernel) (hf : soundInterval f) : sumCeiling f := by
  intro t p a b hc
  have key : ∀ (l : List Nat) (lo hi : Nat), (∀ v ∈ l, lo ≤ v ∧ v ≤ hi) →
      l.length * lo ≤ l.sum ∧ l.sum ≤ l.length * hi := by
    intro l
    induction l with
    | nil => intro lo hi _; simp
    | cons x t ih =>
      intro lo hi h
      obtain ⟨hx1, hx2⟩ := h x (List.mem_cons_self (l := t))
      obtain ⟨i1, i2⟩ := ih lo hi (fun v hv => h v (List.mem_cons_of_mem x hv))
      constructor
      · rw [List.length_cons, List.sum_cons, Nat.add_mul, Nat.one_mul]
        omega
      · rw [List.length_cons, List.sum_cons, Nat.add_mul, Nat.one_mul]
        omega
  exact key (scores f t p) (f.bounds t a b).lo (f.bounds t a b).hi (inside_scores hf hc)

end Aux

theorem sound_SoundInterval : CappedPair.SoundInterval.Goal := by
  intro t a b x y hx hy
  obtain ⟨ha1, ha2⟩ := hx
  obtain ⟨hb1, hb2⟩ := hy
  show min t (min a.lo b.lo) ≤ min t (min x y) ∧ min t (min x y) ≤ min t (min a.hi b.hi)
  omega

theorem sound_SkipRange : CappedPair.SkipRange.Goal :=
  Aux.skipRange_of CappedPair.kernel sound_SoundInterval
theorem sound_TakeAll : CappedPair.TakeAll.Goal :=
  Aux.takeAll_of CappedPair.kernel sound_SoundInterval
theorem sound_ThresholdCount : CappedPair.ThresholdCount.Goal :=
  Aux.thresholdCount_of CappedPair.kernel sound_SoundInterval
theorem sound_DominatedTop : CappedPair.DominatedTop.Goal :=
  Aux.dominatedTop_of CappedPair.kernel sound_SoundInterval
theorem sound_CrossPageJoin : CappedPair.CrossPageJoin.Goal :=
  Aux.crossPageJoin_of CappedPair.kernel sound_SoundInterval
theorem sound_SemijoinFilter : CappedPair.SemijoinFilter.Goal :=
  Aux.semijoinFilter_of CappedPair.kernel sound_SoundInterval
theorem sound_SumCeiling : CappedPair.SumCeiling.Goal :=
  Aux.sumCeiling_of CappedPair.kernel sound_SoundInterval

theorem sound_PushBoth : CappedPair.PushBoth.Goal := by
  intro t k x y hkt
  show k ≤ min t (min x y) ↔ k ≤ x ∧ k ≤ y
  omega

theorem sound_SaturatedPage : CappedPair.SaturatedPage.Goal := by
  intro t a b h1 h2
  show min t (min a.lo b.lo) = t
  omega

end TinylambOpt.ResearchProofs.StorageExecution.DerivedZones.CappedPair
