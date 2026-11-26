import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common_SeekIntervals

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals

theorem div_band (x a b : Nat) :
    (x / (b+1) == a) = (a*(b+1) ≤ x && x < (a+1)*(b+1)) := by
  apply Bool.eq_iff_iff.mpr
  simp
  constructor
  · intro h
    have h' := (Nat.div_eq_iff (by omega : 0 < b+1)).mp h
    constructor
    · exact h'.1
    · rw [Nat.succ_mul]
      omega
  · intro h
    apply Nat.div_eq_of_lt_le
    · exact h.1
    · simpa [Nat.succ_mul] using h.2

theorem mod_band (x a b : Nat) :
    x % (b+1) = a ↔ a ≤ b ∧ ∃ j, x = a + j*(b+1) := by
  constructor
  · intro h
    have hlt : x % (b+1) < b+1 := Nat.mod_lt _ (by omega)
    constructor
    · omega
    · refine ⟨x/(b+1), ?_⟩
      have hdm := Nat.div_add_mod x (b+1)
      rw [h] at hdm
      rw [Nat.mul_comm (b+1) (x/(b+1))] at hdm
      omega
  · rintro ⟨hab, j, rfl⟩
    rw [Nat.add_mul_mod_self_right]
    exact Nat.mod_eq_of_lt (by omega)

theorem xor_one_decomp (x : Nat) : x ^^^ 1 = 2*(x/2) + (1 - x%2) := by
  have hdiv : (x ^^^ 1)/2 = x/2 := by
    rw [Nat.xor_div_two]
    simp
  have hmiff : ((x ^^^ 1) % 2 = 1) ↔ x % 2 ≠ 1 := by
    simp
  have hm : (x ^^^ 1) % 2 = 1 - x % 2 := by
    by_cases hx1 : x % 2 = 1
    · have hn : (x ^^^ 1) % 2 ≠ 1 := by intro h; exact hmiff.mp h hx1
      have hlt : (x ^^^ 1) % 2 < 2 := Nat.mod_lt _ (by decide)
      omega
    · have hy : (x ^^^ 1) % 2 = 1 := hmiff.mpr hx1
      omega
  have hdm := Nat.div_add_mod (x ^^^ 1) 2
  omega

theorem xor_one_even (x : Nat) (h : x % 2 = 0) : x ^^^ 1 = x + 1 := by
  rw [xor_one_decomp]
  have hx : 2*(x/2) = x := by have hdm := Nat.div_add_mod x 2; omega
  omega

theorem xor_one_odd (x : Nat) (h : x % 2 = 1) : x ^^^ 1 = x - 1 := by
  rw [xor_one_decomp]
  have hx : 2*(x/2) = x - 1 := by have hdm := Nat.div_add_mod x 2; omega
  omega

theorem range_ap_sep (a step n : Nat) (hstep : 1 ≤ step) :
    ((List.range n).map (fun j => (a+j*step, a+j*step+1))).Pairwise (fun s t : Span => s.2 ≤ t.1) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append]
      rw [List.pairwise_append]
      constructor
      · exact ih
      constructor
      · simp
      · intro s hs t ht
        rw [List.mem_map] at hs
        rcases hs with ⟨j, hj, rfl⟩
        simp at ht
        subst t
        have hjlt : j < n := by simpa using (List.mem_range.mp hj)
        have hmul := Nat.mul_le_mul_right step (Nat.succ_le_of_lt hjlt)
        rw [Nat.succ_mul] at hmul
        simp
        omega

theorem sing_sep (xs : List Nat) (hs : xs.Pairwise (· ≤ ·)) (hn : xs.Nodup) :
    (xs.map (fun x => (x,x+1))).Pairwise (fun s t : Span => s.2 ≤ t.1) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.nodup_cons] at hn
      rw [List.pairwise_cons] at hs
      rw [List.map_cons, List.pairwise_cons]
      constructor
      · intro s hs'
        rw [List.mem_map] at hs'
        rcases hs' with ⟨y, hy, rfl⟩
        have hxy : x ≤ y := hs.1 y hy
        have hne : y ≠ x := by intro h; subst y; exact hn.1 hy
        omega
      · exact ih hs.2 hn.2

theorem nodup_eraseDups_pair (a b : Nat) : ([a,b].eraseDups).Nodup := by
  rw [List.eraseDups_cons]
  by_cases h : b = a
  · rw [h]
    simp
  · have h' : a ≠ b := by omega
    simp [h, h']
    rw [List.eraseDups_cons]
    simp

theorem notEqual_exact : exactSeek NotEqual.model := by
  intro p x; apply Bool.eq_iff_iff.mpr
  rw [ranges, Common.any_ranges_eq]
  by_cases hc : x < p.cap <;> by_cases h : x = p.a <;>
    simp [hc, NotEqual.model, h] <;> omega

theorem outsideClosed_exact : exactSeek OutsideClosed.model := by
  intro p x; apply Bool.eq_iff_iff.mpr
  rw [ranges, Common.any_ranges_eq]
  by_cases hc : x < p.cap
  · by_cases hab : p.a ≤ p.b <;> by_cases hax : p.a ≤ x <;> by_cases hxb : x ≤ p.b <;>
      simp [hc, OutsideClosed.model, hab, hax, hxb] <;> omega
  · simp [hc]

theorem ringSlice_exact : exactSeek RingSlice.model := by
  intro p x; apply Bool.eq_iff_iff.mpr
  rw [ranges, Common.any_ranges_eq]
  by_cases hc : x < p.cap
  · by_cases hab : p.a ≤ p.b <;> by_cases hax : p.a ≤ x <;> by_cases hxb : x < p.b <;>
      simp [hc, RingSlice.model, hab, hax, hxb] <;> omega
  · simp [hc]

theorem distanceBand_exact : exactSeek DistanceBand.model := by
  intro p x; apply Bool.eq_iff_iff.mpr
  rw [ranges, Common.any_ranges_eq]
  by_cases hc : x < p.cap <;> by_cases hax : p.a ≤ x <;>
    simp [hc, DistanceBand.model, hax] <;> omega

theorem quotientBand_exact : exactSeek QuotientBand.model := by
  intro p x; apply Bool.eq_iff_iff.mpr
  rw [ranges, Common.any_ranges_eq]
  by_cases hc : x < p.cap <;> simp [hc, QuotientBand.model, div_band]

theorem residue_exact : exactSeek Residue.model := by
  intro p x; apply Bool.eq_iff_iff.mpr
  rw [ranges, Common.any_ranges_eq]
  by_cases hc : x < p.cap
  · simp [hc, Residue.model, Function.comp_def]
    constructor
    · rintro ⟨j, hjlt, hmem⟩
      by_cases hab : p.a ≤ p.b
      · simp [hab] at hmem
        have hx : x = p.a + j*(p.b+1) := by omega
        exact (mod_band x p.a p.b).mpr ⟨hab, j, hx⟩
      · simp [hab] at hmem
    · intro hmod
      rcases (mod_band x p.a p.b).mp hmod with ⟨hab, j, hj⟩
      refine ⟨j, ?_, ?_⟩
      · have hmul : j*(p.b+1) ≤ p.cap := by omega
        exact Nat.lt_succ_of_le ((Nat.le_div_iff_mul_le (by omega)).mpr hmul)
      · simp [hab]
        simp [hj]
  · simp [hc]

theorem twoLiterals_exact : exactSeek TwoLiterals.model := by
  intro p x; apply Bool.eq_iff_iff.mpr
  rw [ranges, Common.any_ranges_eq]
  by_cases hc : x < p.cap
  · simp [hc, TwoLiterals.model]
    by_cases hxa : x = p.a <;> by_cases hxb : x = p.b <;> simp [hxa, hxb] <;> omega
  · simp [hc]

theorem saturatingSubtract_exact : exactSeek SaturatingSubtract.model := by
  intro p x; apply Bool.eq_iff_iff.mpr
  rw [ranges, Common.any_ranges_eq]
  by_cases hc : x < p.cap
  · by_cases hb : p.b = 0 <;> by_cases hxa : p.a ≤ x <;>
      simp [hc, SaturatingSubtract.model, hb, hxa] <;> omega
  · simp [hc]

theorem minClamp_exact : exactSeek MinClamp.model := by
  intro p x; apply Bool.eq_iff_iff.mpr
  rw [ranges, Common.any_ranges_eq]
  by_cases hc : x < p.cap
  · by_cases hba : p.b < p.a <;> by_cases heq : p.a = p.b <;> by_cases hxb : x = p.b <;> by_cases hax : p.a ≤ x <;>
      simp [hc, MinClamp.model, hba, heq, hxb, hax] <;> omega
  · simp [hc]

theorem notEqual_sep : ∀ p, (NotEqual.model.plan p).Pairwise (fun s t : Span => s.2 ≤ t.1) := by
  intro p; simp [NotEqual.model]

theorem outsideClosed_sep : ∀ p, (OutsideClosed.model.plan p).Pairwise (fun s t : Span => s.2 ≤ t.1) := by
  intro p; by_cases h : p.a ≤ p.b <;> simp [OutsideClosed.model, h] <;> omega

theorem ringSlice_sep : ∀ p, (RingSlice.model.plan p).Pairwise (fun s t : Span => s.2 ≤ t.1) := by
  intro p; by_cases h : p.a ≤ p.b <;> simp [RingSlice.model, h] <;> omega

theorem distanceBand_sep : ∀ p, (DistanceBand.model.plan p).Pairwise (fun s t : Span => s.2 ≤ t.1) := by
  intro p; simp [DistanceBand.model]

theorem quotientBand_sep : ∀ p, (QuotientBand.model.plan p).Pairwise (fun s t : Span => s.2 ≤ t.1) := by
  intro p; simp [QuotientBand.model]

theorem residue_sep : ∀ p, (Residue.model.plan p).Pairwise (fun s t : Span => s.2 ≤ t.1) := by
  intro p
  by_cases h : p.a ≤ p.b
  · simp [Residue.model, h, Function.comp_def]
    exact range_ap_sep p.a (p.b+1) (p.cap/(p.b+1)+1) (by omega)
  · simp [Residue.model, h, Function.comp_def]
    have hfm : (List.range (p.cap/(p.b+1)+1)).filterMap (fun _ : Nat => (none : Option Span)) = [] := by
      apply List.filterMap_eq_nil_iff.mpr
      intro a ha; rfl
    rw [hfm]
    simp

theorem twoLiterals_sep : ∀ p, (TwoLiterals.model.plan p).Pairwise (fun s t : Span => s.2 ≤ t.1) := by
  intro p
  apply sing_sep
  · simpa using (List.pairwise_mergeSort
      (le := fun a b : Nat => decide (a ≤ b))
      (by intro a b c hab hbc; simp at hab hbc ⊢; omega)
      (by intro a b; by_cases h : a ≤ b
          · simp [h]
          · have hb : b ≤ a := by omega
            simp [h, hb])
      ([p.a, p.b].eraseDups))
  · have hperm := List.mergeSort_perm ([p.a, p.b].eraseDups) (fun a b : Nat => decide (a ≤ b))
    exact hperm.symm.nodup (nodup_eraseDups_pair p.a p.b)

theorem saturatingSubtract_sep : ∀ p, (SaturatingSubtract.model.plan p).Pairwise (fun s t : Span => s.2 ≤ t.1) := by
  intro p; by_cases h : p.b = 0 <;> simp [SaturatingSubtract.model, h]

theorem minClamp_sep : ∀ p, (MinClamp.model.plan p).Pairwise (fun s t : Span => s.2 ≤ t.1) := by
  intro p; by_cases h1 : p.b < p.a <;> by_cases h2 : p.a = p.b <;> simp [MinClamp.model, h1, h2]

theorem xorLowBit_sep : ∀ p, (XorLowBit.model.plan p).Pairwise (fun s t : Span => s.2 ≤ t.1) := by
  intro p
  by_cases hba : p.b < p.a
  · simp [XorLowBit.model, hba]
  · simp [XorLowBit.model, hba]
    by_cases ha : p.a % 2 = 1 <;> by_cases hb : p.b % 2 = 0 <;> simp [ha, hb] <;> omega


theorem notEqual_shrink : shrinkDomain NotEqual.model := by
  apply Common.shrinkDomain_of_raw
  intro p n x hx hn
  by_cases hxa : x < p.a <;> by_cases h : p.a + 1 ≤ x <;> simp [NotEqual.model, hxa, h] <;> omega

theorem outsideClosed_shrink : shrinkDomain OutsideClosed.model := by
  apply Common.shrinkDomain_of_raw
  intro p n x hx hn
  by_cases hab : p.a ≤ p.b
  · by_cases hxa : x < p.a <;> by_cases hb1 : p.b + 1 ≤ x <;>
      simp [OutsideClosed.model, hab, hxa, hb1] <;> omega
  · simp [OutsideClosed.model, hab, hx, Nat.lt_of_lt_of_le hx hn]

theorem ringSlice_shrink : shrinkDomain RingSlice.model := by
  apply Common.shrinkDomain_of_raw
  intro p n x hx hn
  by_cases hab : p.a ≤ p.b <;> by_cases hax : p.a ≤ x <;> by_cases hxb : x < p.b <;>
    simp [RingSlice.model, hab, hax, hxb] <;> omega

theorem distanceBand_shrink : shrinkDomain DistanceBand.model := by
  apply Common.shrinkDomain_of_raw
  intro p n x hx hn
  rfl

theorem quotientBand_shrink : shrinkDomain QuotientBand.model := by
  apply Common.shrinkDomain_of_raw
  intro p n x hx hn
  rfl

theorem residue_shrink : shrinkDomain Residue.model := by
  apply Common.shrinkDomain_of_raw
  intro p n x hx hn
  by_cases hab : p.a ≤ p.b
  · simp [Residue.model, hab, Function.comp_def]
    apply Bool.eq_iff_iff.mpr
    constructor
    · intro h
      rw [List.any_eq_true] at h ⊢
      rcases h with ⟨j, hj, hmem⟩
      refine ⟨j, ?_, hmem⟩
      exact List.mem_range.mpr (Nat.lt_of_lt_of_le (by simpa using List.mem_range.mp hj) (Nat.add_le_add_right (Nat.div_le_div_right hn) 1))
    · intro h
      rw [List.any_eq_true] at h ⊢
      rcases h with ⟨j, hj, hmem⟩
      refine ⟨j, ?_, hmem⟩
      have hmem' : p.a + j*(p.b+1) ≤ x ∧ x < p.a + j*(p.b+1)+1 := by simpa using hmem
      have hjle : j*(p.b+1) ≤ x := by omega
      exact List.mem_range.mpr (Nat.lt_succ_of_le ((Nat.le_div_iff_mul_le (by omega)).mpr (Nat.le_trans hjle (Nat.le_of_lt hx))))
  · simp [Residue.model, hab, Function.comp_def]
    have hanyfalse : ∀ l : List Nat, l.any (fun _ : Nat => false) = false := by
      intro l; induction l <;> simp_all
    rw [hanyfalse, hanyfalse]

theorem twoLiterals_shrink : shrinkDomain TwoLiterals.model := by
  apply Common.shrinkDomain_of_raw
  intro p n x hx hn
  rfl

theorem saturatingSubtract_shrink : shrinkDomain SaturatingSubtract.model := by
  apply Common.shrinkDomain_of_raw
  intro p n x hx hn
  rfl

theorem minClamp_shrink : shrinkDomain MinClamp.model := by
  apply Common.shrinkDomain_of_raw
  intro p n x hx hn
  by_cases h1 : p.b < p.a
  · by_cases hxb : x = p.b <;> simp [MinClamp.model, h1, hxb] <;> omega
  · by_cases h2 : p.a = p.b
    · simp [MinClamp.model, h1, h2, hx, Nat.lt_of_lt_of_le hx hn]
    · simp [MinClamp.model, h1, h2]

theorem xorLowBit_shrink : shrinkDomain XorLowBit.model := by
  apply Common.shrinkDomain_of_raw
  intro p n x hx hn
  rfl

theorem length_filter_le_one (l : List Span) : (l.filter (fun s : Span => s.1 < s.2)).length ≤ l.length := by
  exact List.length_filter_le _ _

theorem distanceBand_single : ∀ p, (ranges DistanceBand.model p).length ≤ 1 := by
  intro p; unfold ranges; simp [DistanceBand.model]; exact length_filter_le_one _

theorem quotientBand_single : ∀ p, (ranges QuotientBand.model p).length ≤ 1 := by
  intro p; unfold ranges; simp [QuotientBand.model]; exact length_filter_le_one _

theorem saturatingSubtract_single : ∀ p, (ranges SaturatingSubtract.model p).length ≤ 1 := by
  intro p; unfold ranges
  by_cases h : p.b == 0
  · have hb : p.b = 0 := by simpa using h
    simp [SaturatingSubtract.model, hb]
    simpa using (List.length_filter_le (fun s : Span => s.1 < s.2) [(min 0 p.cap, min (p.a+1) p.cap)])
  · have hb : p.b ≠ 0 := by simpa using h
    simp [SaturatingSubtract.model, hb]
    simpa using (List.length_filter_le (fun s : Span => s.1 < s.2) [(min (p.a+p.b) p.cap, min (p.a+p.b+1) p.cap)])

theorem minClamp_single : ∀ p, (ranges MinClamp.model p).length ≤ 1 := by
  intro p; unfold ranges
  by_cases h1 : p.b < p.a
  · simp [MinClamp.model, h1]
    simpa using (List.length_filter_le (fun s : Span => s.1 < s.2) [(min p.b p.cap, min (p.b+1) p.cap)])
  · by_cases h2 : p.a == p.b
    · have he : p.a = p.b := by simpa using h2
      simp [MinClamp.model, h1, he]
      simpa using (List.length_filter_le (fun s : Span => s.1 < s.2) [(min p.b p.cap, min p.cap p.cap)])
    · have he : p.a ≠ p.b := by simpa using h2
      simp [MinClamp.model, h1, he]

theorem h_even_aodd_beven (a b x : Nat) (hab : a ≤ b) (ha : a % 2 = 1) (hb : b % 2 = 0) (hx : x % 2 = 0) :
  (∃ l h, (l = a-1 ∧ h = a ∨ l = a+1 ∧ h = b ∨ l = b+1 ∧ h = b+2) ∧ l ≤ x ∧ x < h) ↔
    (a ≤ x+1 ∧ x+1 ≤ b) := by
  have hda := Nat.div_add_mod a 2
  have hdb := Nat.div_add_mod b 2
  have hdx := Nat.div_add_mod x 2
  have ha2 : 2*(a/2) = a-1 := by omega
  have hb2 : 2*(b/2) = b := by omega
  have hx2 : 2*(x/2) = x := by omega
  constructor
  · rintro ⟨l, h, hl, hlx, hxh⟩
    rcases hl with hL | hM | hR
    · rcases hL with ⟨rfl, rfl⟩; omega
    · rcases hM with ⟨rfl, rfl⟩; omega
    · rcases hR with ⟨rfl, rfl⟩; omega
  · intro hr
    by_cases hxa : x = a-1
    · refine ⟨a-1, a, Or.inl ⟨rfl,rfl⟩, by omega, by omega⟩
    · by_cases hxb : x = b+1
      · refine ⟨b+1, b+2, Or.inr (Or.inr ⟨rfl,rfl⟩), by omega, by omega⟩
      · refine ⟨a+1, b, Or.inr (Or.inl ⟨rfl,rfl⟩), by omega, by omega⟩

theorem h_even_aodd_bodd (a b x : Nat) (hab : a ≤ b) (ha : a % 2 = 1) (hb : b % 2 = 1) (hx : x % 2 = 0) :
  (∃ l h, (l = a-1 ∧ h = a ∨ l = a+1 ∧ h = b+1) ∧ l ≤ x ∧ x < h) ↔
    (a ≤ x+1 ∧ x+1 ≤ b) := by
  have hda := Nat.div_add_mod a 2
  have hdb := Nat.div_add_mod b 2
  have hdx := Nat.div_add_mod x 2
  have ha2 : 2*(a/2) = a-1 := by omega
  have hb2 : 2*(b/2) = b-1 := by omega
  have hx2 : 2*(x/2) = x := by omega
  constructor
  · rintro ⟨l, h, hl, hlx, hxh⟩
    rcases hl with hL | hM
    · rcases hL with ⟨rfl, rfl⟩; omega
    · rcases hM with ⟨rfl, rfl⟩; omega
  · intro hr
    by_cases hxa : x = a-1
    · refine ⟨a-1, a, Or.inl ⟨rfl,rfl⟩, by omega, by omega⟩
    · refine ⟨a+1, b+1, Or.inr ⟨rfl,rfl⟩, by omega, by omega⟩

theorem h_even_aeven_beven (a b x : Nat) (hab : a ≤ b) (ha : a % 2 = 0) (hb : b % 2 = 0) (hx : x % 2 = 0) :
  (∃ l h, l = a ∧ h = b ∧ l ≤ x ∧ x < h) ↔ (a ≤ x+1 ∧ x+1 ≤ b) := by
  have hda := Nat.div_add_mod a 2
  have hdb := Nat.div_add_mod b 2
  have hdx := Nat.div_add_mod x 2
  have ha2 : 2*(a/2) = a := by omega
  have hb2 : 2*(b/2) = b := by omega
  have hx2 : 2*(x/2) = x := by omega
  constructor
  · rintro ⟨l, h, rfl, rfl, hlx, hxh⟩; omega
  · intro hr; refine ⟨a, b, rfl, rfl, ?_, ?_⟩ <;> omega

theorem h_even_aeven_beven_right (a b x : Nat) (hab : a ≤ b) (ha : a % 2 = 0) (hb : b % 2 = 0) (hx : x % 2 = 0) :
  (∃ l h, (l = a ∧ h = b ∨ l = b+1 ∧ h = b+2) ∧ l ≤ x ∧ x < h) ↔ (a ≤ x+1 ∧ x+1 ≤ b) := by
  have hda := Nat.div_add_mod a 2
  have hdb := Nat.div_add_mod b 2
  have hdx := Nat.div_add_mod x 2
  have ha2 : 2*(a/2) = a := by omega
  have hb2 : 2*(b/2) = b := by omega
  have hx2 : 2*(x/2) = x := by omega
  constructor
  · rintro ⟨l, h, hl, hlx, hxh⟩
    rcases hl with hM | hR
    · rcases hM with ⟨rfl, rfl⟩
      omega
    · rcases hR with ⟨rfl, rfl⟩
      omega
  · intro hr
    refine ⟨a, b, Or.inl ⟨rfl,rfl⟩, ?_, ?_⟩ <;> omega

theorem h_even_aeven_bodd (a b x : Nat) (hab : a ≤ b) (ha : a % 2 = 0) (hb : b % 2 = 1) (hx : x % 2 = 0) :
  (∃ l h, l = a ∧ h = b+1 ∧ l ≤ x ∧ x < h) ↔ (a ≤ x+1 ∧ x+1 ≤ b) := by
  have hda := Nat.div_add_mod a 2
  have hdb := Nat.div_add_mod b 2
  have hdx := Nat.div_add_mod x 2
  have ha2 : 2*(a/2) = a := by omega
  have hb2 : 2*(b/2) = b-1 := by omega
  have hx2 : 2*(x/2) = x := by omega
  constructor
  · rintro ⟨l, h, rfl, rfl, hlx, hxh⟩; omega
  · intro hr; refine ⟨a, b+1, rfl, rfl, ?_, ?_⟩ <;> omega

theorem h_odd_aodd_beven (a b x : Nat) (hab : a ≤ b) (ha : a % 2 = 1) (hb : b % 2 = 0) (hx : x % 2 = 1) :
  (∃ l h, (l = a-1 ∧ h = a ∨ l = a+1 ∧ h = b ∨ l = b+1 ∧ h = b+2) ∧ l ≤ x ∧ x < h) ↔
    (a ≤ x-1 ∧ x ≤ b+1) := by
  have hda := Nat.div_add_mod a 2
  have hdb := Nat.div_add_mod b 2
  have hdx := Nat.div_add_mod x 2
  have ha2 : 2*(a/2) = a-1 := by omega
  have hb2 : 2*(b/2) = b := by omega
  have hx2 : 2*(x/2) = x-1 := by omega
  constructor
  · rintro ⟨l, h, hl, hlx, hxh⟩
    rcases hl with hL | hM | hR
    · rcases hL with ⟨rfl, rfl⟩; omega
    · rcases hM with ⟨rfl, rfl⟩; omega
    · rcases hR with ⟨rfl, rfl⟩; omega
  · intro hr
    by_cases hxb : x = b+1
    · refine ⟨b+1, b+2, Or.inr (Or.inr ⟨rfl,rfl⟩), by omega, by omega⟩
    · refine ⟨a+1, b, Or.inr (Or.inl ⟨rfl,rfl⟩), by omega, by omega⟩

theorem h_odd_aodd_bodd (a b x : Nat) (hab : a ≤ b) (ha : a % 2 = 1) (hb : b % 2 = 1) (hx : x % 2 = 1) :
  (∃ l h, (l = a-1 ∧ h = a ∨ l = a+1 ∧ h = b+1) ∧ l ≤ x ∧ x < h) ↔
    (a ≤ x-1 ∧ x ≤ b+1) := by
  have hda := Nat.div_add_mod a 2
  have hdb := Nat.div_add_mod b 2
  have hdx := Nat.div_add_mod x 2
  have ha2 : 2*(a/2) = a-1 := by omega
  have hb2 : 2*(b/2) = b-1 := by omega
  have hx2 : 2*(x/2) = x-1 := by omega
  constructor
  · rintro ⟨l, h, hl, hlx, hxh⟩
    rcases hl with hL | hM
    · rcases hL with ⟨rfl, rfl⟩; omega
    · rcases hM with ⟨rfl, rfl⟩; omega
  · intro hr
    refine ⟨a+1, b+1, Or.inr ⟨rfl,rfl⟩, ?_, ?_⟩ <;> omega

theorem h_odd_aeven_beven (a b x : Nat) (hab : a ≤ b) (ha : a % 2 = 0) (hb : b % 2 = 0) (hx : x % 2 = 1) :
  (∃ l h, (l = a ∧ h = b ∨ l = b+1 ∧ h = b+2) ∧ l ≤ x ∧ x < h) ↔
    (a ≤ x-1 ∧ x ≤ b+1) := by
  have hda := Nat.div_add_mod a 2
  have hdb := Nat.div_add_mod b 2
  have hdx := Nat.div_add_mod x 2
  have ha2 : 2*(a/2) = a := by omega
  have hb2 : 2*(b/2) = b := by omega
  have hx2 : 2*(x/2) = x-1 := by omega
  constructor
  · rintro ⟨l, h, hl, hlx, hxh⟩
    rcases hl with hM | hR
    · rcases hM with ⟨rfl, rfl⟩; omega
    · rcases hR with ⟨rfl, rfl⟩; omega
  · intro hr
    by_cases hxb : x = b+1
    · refine ⟨b+1, b+2, Or.inr ⟨rfl,rfl⟩, by omega, by omega⟩
    · refine ⟨a, b, Or.inl ⟨rfl,rfl⟩, by omega, by omega⟩

theorem h_odd_aeven_bodd (a b x : Nat) (hab : a ≤ b) (ha : a % 2 = 0) (hb : b % 2 = 1) (hx : x % 2 = 1) :
  (∃ l h, l = a ∧ h = b+1 ∧ l ≤ x ∧ x < h) ↔ (a ≤ x-1 ∧ x ≤ b+1) := by
  have hda := Nat.div_add_mod a 2
  have hdb := Nat.div_add_mod b 2
  have hdx := Nat.div_add_mod x 2
  have ha2 : 2*(a/2) = a := by omega
  have hb2 : 2*(b/2) = b-1 := by omega
  have hx2 : 2*(x/2) = x-1 := by omega
  constructor
  · rintro ⟨l, h, rfl, rfl, hlx, hxh⟩; omega
  · intro hr; refine ⟨a, b+1, rfl, rfl, ?_, ?_⟩ <;> omega


theorem xorLowBit_exact : exactSeek XorLowBit.model := by
  intro p x; apply Bool.eq_iff_iff.mpr
  rw [ranges, TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common.any_ranges_eq]
  by_cases hc : x < p.cap
  · simp [hc, XorLowBit.model]
    by_cases hab : p.a ≤ p.b
    · simp [hab]
      by_cases hx : x % 2 = 0
      · rw [xor_one_even x hx]
        by_cases ha : p.a % 2 = 1
        · by_cases hb : p.b % 2 = 0
          · have hhalf : (p.b+1)/2*2 = p.b := by omega
            simp [ha, hb, hhalf]
            exact h_even_aodd_beven p.a p.b x hab ha hb hx
          · have hhalf : (p.b+1)/2*2 = p.b+1 := by omega
            simp [ha, hb, hhalf]
            exact h_even_aodd_bodd p.a p.b x hab ha (by omega) hx
        · have ha0 : p.a % 2 = 0 := by omega
          by_cases hb : p.b % 2 = 0
          · have hhalf : (p.b+1)/2*2 = p.b := by omega
            simp [ha, ha0, hb, hhalf]
            exact h_even_aeven_beven_right p.a p.b x hab ha0 hb hx
          · have hhalf : (p.b+1)/2*2 = p.b+1 := by omega
            simp [ha, ha0, hb, hhalf]
            simpa only [and_assoc] using (h_even_aeven_bodd p.a p.b x hab ha0 (by omega) hx)
      · have hx1 : x % 2 = 1 := by omega
        rw [xor_one_odd x hx1]
        by_cases ha : p.a % 2 = 1
        · by_cases hb : p.b % 2 = 0
          · have hhalf : (p.b+1)/2*2 = p.b := by omega
            simp [ha, hb, hhalf]
            exact h_odd_aodd_beven p.a p.b x hab ha hb hx1
          · have hhalf : (p.b+1)/2*2 = p.b+1 := by omega
            simp [ha, hb, hhalf]
            exact h_odd_aodd_bodd p.a p.b x hab ha (by omega) hx1
        · have ha0 : p.a % 2 = 0 := by omega
          by_cases hb : p.b % 2 = 0
          · have hhalf : (p.b+1)/2*2 = p.b := by omega
            simp [ha, ha0, hb, hhalf]
            exact h_odd_aeven_beven p.a p.b x hab ha0 hb hx1
          · have hhalf : (p.b+1)/2*2 = p.b+1 := by omega
            simp [ha, ha0, hb, hhalf]
            simpa only [and_assoc] using (h_odd_aeven_bodd p.a p.b x hab ha0 (by omega) hx1)
    · simp [hab]
      omega
  · simp [hc]

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
