import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.DerivedZones.BitwiseXor
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
open TinylambOpt.ResearchGoals.Compression.Wire

abbrev WX (a b : Bounds) : Nat := max (width (a.lo ^^^ a.hi)) (width (b.lo ^^^ b.hi))

theorem width_lt_two_pow (n : Nat) : n < 2 ^ width n := by
  by_cases hz : n = 0
  · subst hz
    simp [width]
  · have hw : width n = n.log2 + 1 := by
      unfold width
      rw [if_neg (by simpa using hz)]
    rw [hw]
    exact (Nat.log2_lt hz).mp (Nat.lt_succ_self _)

theorem div_eq_of_lt_xor {k m n : Nat} (h : m ^^^ n < 2 ^ k) : m / 2 ^ k = n / 2 ^ k :=
  Nat.eq_of_testBit_eq fun i => by
    have hb : m ^^^ n < 2 ^ (i + k) :=
      Nat.lt_of_lt_of_le h (Nat.pow_le_pow_of_le (by decide : 1 < 2) (Nat.le_add_left k i))
    rw [Nat.testBit_div_two_pow, Nat.testBit_div_two_pow]
    have hbit : (m ^^^ n).testBit (i + k) = false := Nat.testBit_lt_two_pow hb
    rw [Nat.testBit_xor] at hbit
    cases hm : m.testBit (i + k) <;> cases hn : n.testBit (i + k) <;> simp_all

theorem xor_lt_two_pow {k m n : Nat} (hm : m < 2 ^ k) (hn : n < 2 ^ k) : m ^^^ n < 2 ^ k := by
  rw [← Nat.div_eq_zero_iff_lt (Nat.two_pow_pos k)] at hm hn ⊢
  rw [Nat.xor_div_two_pow, hm, hn]
  exact Nat.xor_self 0

theorem xor_two_pow (k a b : Nat) : a * 2 ^ k ^^^ b * 2 ^ k = (a ^^^ b) * 2 ^ k :=
  Nat.eq_of_testBit_eq fun i => by
    simp only [Nat.testBit_xor, Nat.testBit_mul_two_pow]
    by_cases hd : k ≤ i
    · simp [hd]
    · simp [hd]

theorem width_two_pow_sub_one (k : Nat) : width (2 ^ k - 1) = k := by
  cases k with
  | zero => rfl
  | succ j =>
    have hP : 2 ^ (j + 1) = 2 ^ j * 2 := Nat.pow_succ 2 j
    have hq : 1 ≤ 2 ^ j := Nat.one_le_two_pow
    have hne : 2 ^ (j + 1) - 1 ≠ 0 := by
      rw [hP]; omega
    have hlog : (2 ^ (j + 1) - 1).log2 = j := by
      refine (Nat.log2_eq_iff hne).mpr ⟨?_, ?_⟩ <;> rw [hP] <;> omega
    unfold width
    rw [if_neg (by simpa using hne), hlog]

theorem xor_corner (k a : Nat) :
    a * 2 ^ k ^^^ (a * 2 ^ k + (2 ^ k - 1)) = 2 ^ k - 1 := by
  have hl : 2 ^ k - 1 < 2 ^ k :=
    Nat.sub_lt_of_pos_le (by decide) Nat.one_le_two_pow
  have ho : 2 ^ k * a + (2 ^ k - 1) = 2 ^ k * a ||| (2 ^ k - 1) :=
    Nat.two_pow_add_eq_or_of_lt hl a
  rw [show a * 2 ^ k = 2 ^ k * a from Nat.mul_comm _ _, ho]
  refine Nat.eq_of_testBit_eq fun i => ?_
  rw [Nat.testBit_xor, Nat.testBit_or, Nat.testBit_two_pow_sub_one, Nat.testBit_two_pow_mul]
  by_cases hd : i < k
  · have hn : ¬k ≤ i := Nat.not_le_of_lt hd
    simp [hd, hn]
  · have hk : k ≤ i := Nat.le_of_not_lt hd
    simp [hd, hk]

theorem succ_mul_two_pow_sub (k a : Nat) :
    (a + 1) * 2 ^ k - 1 = a * 2 ^ k + (2 ^ k - 1) := by
  rw [Nat.add_mul, Nat.one_mul, Nat.add_sub_assoc (Nat.one_le_two_pow (n := k))]

theorem sound_SoundInterval : BitwiseXor.SoundInterval.Goal := by
  intro t a b x y hx hy
  obtain ⟨hal, hax⟩ := hx
  obtain ⟨hbl, hbx⟩ := hy
  have hW1 : width (a.lo ^^^ a.hi) ≤ WX a b := Nat.le_max_left _ _
  have hW2 : width (b.lo ^^^ b.hi) ≤ WX a b := Nat.le_max_right _ _
  have haX : a.lo ^^^ a.hi < 2 ^ WX a b :=
    Nat.lt_of_lt_of_le (width_lt_two_pow _) (Nat.pow_le_pow_of_le (by decide : 1 < 2) hW1)
  have hbX : b.lo ^^^ b.hi < 2 ^ WX a b :=
    Nat.lt_of_lt_of_le (width_lt_two_pow _) (Nat.pow_le_pow_of_le (by decide : 1 < 2) hW2)
  have haq : a.lo / 2 ^ WX a b = a.hi / 2 ^ WX a b := div_eq_of_lt_xor haX
  have hbq : b.lo / 2 ^ WX a b = b.hi / 2 ^ WX a b := div_eq_of_lt_xor hbX
  have hxq : x / 2 ^ WX a b = a.lo / 2 ^ WX a b := by
    have h2 : x / 2 ^ WX a b ≤ a.hi / 2 ^ WX a b := Nat.div_le_div_right hax
    rw [← haq] at h2
    exact Nat.le_antisymm h2 (Nat.div_le_div_right hal)
  have hyq : y / 2 ^ WX a b = b.lo / 2 ^ WX a b := by
    have h2 : y / 2 ^ WX a b ≤ b.hi / 2 ^ WX a b := Nat.div_le_div_right hbx
    rw [← hbq] at h2
    exact Nat.le_antisymm h2 (Nat.div_le_div_right hbl)
  have hxa : x ^^^ a.lo < 2 ^ WX a b := by
    rw [← Nat.div_eq_zero_iff_lt (Nat.two_pow_pos (WX a b))]
    rw [Nat.xor_div_two_pow, hxq, Nat.xor_self]
  have hyb : y ^^^ b.lo < 2 ^ WX a b := by
    rw [← Nat.div_eq_zero_iff_lt (Nat.two_pow_pos (WX a b))]
    rw [Nat.xor_div_two_pow, hyq, Nat.xor_self]
  have hrew : (x ^^^ y) ^^^ (a.lo ^^^ b.lo) = (x ^^^ a.lo) ^^^ (y ^^^ b.lo) :=
    Nat.eq_of_testBit_eq fun i => by
      rw [Nat.testBit_xor, Nat.testBit_xor, Nat.testBit_xor, Nat.testBit_xor]
      cases h1 : (Nat.testBit x i) <;> cases h2 : (Nat.testBit y i)
        <;> cases h3 : (Nat.testBit a.lo i) <;> cases h4 : (Nat.testBit b.lo i) <;> simp_all
  have hX : (x ^^^ y) ^^^ (a.lo ^^^ b.lo) < 2 ^ WX a b := by
    rw [hrew]; exact xor_lt_two_pow hxa hyb
  have hq : (x ^^^ y) / 2 ^ WX a b = (a.lo ^^^ b.lo) / 2 ^ WX a b :=
    div_eq_of_lt_xor hX
  have hbnd : BitwiseXor.kernel.bounds t a b =
      Bounds.mk ((a.lo ^^^ b.lo) / 2 ^ WX a b * 2 ^ WX a b)
        ((a.lo ^^^ b.lo) / 2 ^ WX a b * 2 ^ WX a b + 2 ^ WX a b - 1) := rfl
  have hsc : BitwiseXor.kernel.score t (x, y) = x ^^^ y := rfl
  rw [hbnd, hsc]
  simp only [inside]
  have A1 : (x ^^^ y) / 2 ^ WX a b * 2 ^ WX a b ≤ x ^^^ y := Nat.div_mul_le_self _ _
  have A2 : x ^^^ y < (x ^^^ y) / 2 ^ WX a b * 2 ^ WX a b + 2 ^ WX a b :=
    Nat.lt_div_mul_add (Nat.two_pow_pos (WX a b))
  have A3 : (x ^^^ y) / 2 ^ WX a b * 2 ^ WX a b = (a.lo ^^^ b.lo) / 2 ^ WX a b * 2 ^ WX a b :=
    congrArg (fun z => z * 2 ^ WX a b) hq
  exact ⟨by omega, by omega⟩

theorem sound_SkipRange : BitwiseXor.SkipRange.Goal :=
  Aux.skipRange_of BitwiseXor.kernel sound_SoundInterval
theorem sound_TakeAll : BitwiseXor.TakeAll.Goal :=
  Aux.takeAll_of BitwiseXor.kernel sound_SoundInterval
theorem sound_ThresholdCount : BitwiseXor.ThresholdCount.Goal :=
  Aux.thresholdCount_of BitwiseXor.kernel sound_SoundInterval
theorem sound_DominatedTop : BitwiseXor.DominatedTop.Goal :=
  Aux.dominatedTop_of BitwiseXor.kernel sound_SoundInterval
theorem sound_CrossPageJoin : BitwiseXor.CrossPageJoin.Goal :=
  Aux.crossPageJoin_of BitwiseXor.kernel sound_SoundInterval
theorem sound_SemijoinFilter : BitwiseXor.SemijoinFilter.Goal :=
  Aux.semijoinFilter_of BitwiseXor.kernel sound_SoundInterval
theorem sound_SumCeiling : BitwiseXor.SumCeiling.Goal :=
  Aux.sumCeiling_of BitwiseXor.kernel sound_SoundInterval

theorem sound_AlignedSiblings : BitwiseXor.AlignedSiblings.Goal := by
  intro k a b
  have hea : (a + 1) * 2 ^ k - 1 = a * 2 ^ k + (2 ^ k - 1) := succ_mul_two_pow_sub k a
  have heb : (b + 1) * 2 ^ k - 1 = b * 2 ^ k + (2 ^ k - 1) := succ_mul_two_pow_sub k b
  have hw1 : (a * 2 ^ k) ^^^ ((a + 1) * 2 ^ k - 1) = 2 ^ k - 1 := by
    rw [hea, xor_corner]
  have hw2 : (b * 2 ^ k) ^^^ ((b + 1) * 2 ^ k - 1) = 2 ^ k - 1 := by
    rw [heb, xor_corner]
  have hwW : max (width ((a * 2 ^ k) ^^^ ((a + 1) * 2 ^ k - 1)))
      (width ((b * 2 ^ k) ^^^ ((b + 1) * 2 ^ k - 1))) = k := by
    rw [hw1, hw2, width_two_pow_sub_one, Nat.max_self]
  have h1 : (BitwiseXor.kernel.bounds 0 ⟨a * 2 ^ k, (a + 1) * 2 ^ k - 1⟩
      ⟨b * 2 ^ k, (b + 1) * 2 ^ k - 1⟩).lo
      = ((a * 2 ^ k) ^^^ (b * 2 ^ k))
        / 2 ^ max (width ((a * 2 ^ k) ^^^ ((a + 1) * 2 ^ k - 1)))
          (width ((b * 2 ^ k) ^^^ ((b + 1) * 2 ^ k - 1)))
        * 2 ^ max (width ((a * 2 ^ k) ^^^ ((a + 1) * 2 ^ k - 1)))
          (width ((b * 2 ^ k) ^^^ ((b + 1) * 2 ^ k - 1))) := rfl
  rw [h1, hwW, xor_two_pow, Nat.mul_div_cancel _ (Nat.two_pow_pos k)]

theorem sound_FixedPrefix : BitwiseXor.FixedPrefix.Goal := by
  intro x y
  have hw : max (width (x ^^^ x)) (width (y ^^^ y)) = 0 := by
    rw [Nat.xor_self x, Nat.xor_self y]
    show max (width 0) (width 0) = 0
    simp [width]
  show (⟨(x ^^^ y) / 2 ^ max (width (x ^^^ x)) (width (y ^^^ y))
        * 2 ^ max (width (x ^^^ x)) (width (y ^^^ y)),
      (x ^^^ y) / 2 ^ max (width (x ^^^ x)) (width (y ^^^ y))
        * 2 ^ max (width (x ^^^ x)) (width (y ^^^ y))
        + 2 ^ max (width (x ^^^ x)) (width (y ^^^ y)) - 1⟩ : Bounds)
      = ⟨x ^^^ y, x ^^^ y⟩
  rw [hw, Nat.pow_zero, Nat.div_one, Nat.mul_one, Nat.add_sub_cancel]

end TinylambOpt.ResearchProofs.StorageExecution.DerivedZones.BitwiseXor
