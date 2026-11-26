import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RotatedPlane_MaskedPatch

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane

private theorem mem_lanes {b w : Nat} : b ∈ lanes w ↔ b < w := by
  simp [lanes, List.mem_range]

private theorem testBit_or_iff (x y i : Nat) : (x ||| y).testBit i = (x.testBit i || y.testBit i) := by
  simp [Nat.testBit_or]

private theorem testBit_two_pow (i j : Nat) : (2 ^ i).testBit j = decide (i = j) := by
  simp [Nat.testBit_two_pow]

-- testBit of a foldl-or over one lane: sets bit p iff b is in bs and cond b holds
private theorem testBit_foldl_row (f : Nat → Nat) (cond : Nat → Bool) (bs : List Nat) (acc : Nat) (p : Nat)
    (hinj : ∀ a ∈ bs, ∀ b ∈ bs, f a = f b → a = b) :
    ((bs.foldl (fun a b => if cond b then a ||| 2 ^ f b else a) acc).testBit p) =
    (acc.testBit p || (bs.any (fun b => cond b && decide (f b = p)))) := by
  induction bs generalizing acc with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons, List.any_cons]
      have hinj_bs : ∀ a ∈ bs, ∀ c ∈ bs, f a = f c → a = c :=
        fun a ha c hc => hinj a (List.mem_cons_of_mem _ ha) c (List.mem_cons_of_mem _ hc)
      by_cases hc : cond b = true
      · rw [if_pos hc]
        rw [ih (acc ||| 2 ^ f b) hinj_bs]
        rw [testBit_or_iff, testBit_two_pow]
        simp only [hc, Bool.true_and]
        by_cases hfb : f b = p
        · subst hfb
          simp
        · have hd : decide (f b = p) = false := decide_eq_false hfb
          rw [hd]
          cases acc.testBit p <;> cases bs.any (fun a => cond a && decide (f a = p)) <;> simp
      · have hc' : cond b = false := Bool.eq_false_iff.mpr hc
        rw [if_neg (by simp [hc'])]
        rw [ih acc hinj_bs]
        simp [hc']

-- testBit of the whole image, assuming bit cells are pairwise distinct
private theorem testBit_image (l : Layout) (w : Nat) (xs : List Nat) (p : Nat)
    (hno : ∀ i j a b, i < xs.length → j < xs.length → a < w → b < w →
      l.bit xs.length w i a = l.bit xs.length w j b → i = j ∧ a = b) :
    (image l w xs).testBit p =
      (xs.zipIdx.any (fun (x,i) => (lanes w).any (fun b => x.testBit b && decide (l.bit xs.length w i b = p)))) := by
  unfold image
  have hgen : ∀ (lxs : List (Nat × Nat)) (acc : Nat),
      (∀ x i, (x,i) ∈ lxs → i < xs.length) →
      (∀ i j a b, (∃ x, (x,i) ∈ lxs) → j < xs.length → a < w → b < w →
        l.bit xs.length w i a = l.bit xs.length w j b → i = j ∧ a = b) →
      ((lxs.foldl (fun acc (x,i) => (lanes w).foldl
        (fun a b => if x.testBit b then a ||| 2^(l.bit xs.length w i b) else a) acc) acc).testBit p =
       (acc.testBit p || lxs.any (fun (x,i) => (lanes w).any (fun b => x.testBit b && decide (l.bit xs.length w i b = p))))) := by
    intro lxs
    induction lxs with
    | nil => intro acc _ _; simp
    | cons xi rest ih =>
        intro acc hmem hno'
        obtain ⟨x, i⟩ := xi
        simp only [List.foldl_cons, List.any_cons]
        have hi : i < xs.length := hmem x i (List.mem_cons_self ..)
        have hmem_rest : ∀ x' i', (x',i') ∈ rest → i' < xs.length :=
          fun x' i' hm => hmem x' i' (List.mem_cons_of_mem _ hm)
        have hno_rest : ∀ i' j a b, (∃ x, (x,i') ∈ rest) → j < xs.length → a < w → b < w →
            l.bit xs.length w i' a = l.bit xs.length w j b → i' = j ∧ a = b :=
          fun i' j a b hex hj ha hb heq => hno' i' j a b (by obtain ⟨x,hx⟩ := hex; exact ⟨x, List.mem_cons_of_mem _ hx⟩) hj ha hb heq
        rw [ih _ hmem_rest hno_rest]
        rw [testBit_foldl_row (fun b => l.bit xs.length w i b) (fun b => x.testBit b) (lanes w) acc p]
        · simp only [Bool.or_assoc]
        · intro a ha b hb heq
          have ha' : a < w := by simpa [mem_lanes] using ha
          have hb' : b < w := by simpa [mem_lanes] using hb
          exact (hno i i a b hi hi ha' hb' heq).2
  have hmem : ∀ x i, (x,i) ∈ xs.zipIdx → i < xs.length := by
    intro x i h
    have hlt := List.snd_lt_of_mem_zipIdx h
    simpa [List.length_zipIdx] using hlt
  have hno' : ∀ i j a b, (∃ x, (x,i) ∈ xs.zipIdx) → j < xs.length → a < w → b < w →
      l.bit xs.length w i a = l.bit xs.length w j b → i = j ∧ a = b :=
    fun i j a b hex hj ha hb heq => by
      obtain ⟨x, hx⟩ := hex
      exact hno i j a b (hmem x i hx) hj ha hb heq
  rw [hgen xs.zipIdx 0 hmem hno']
  simp

private theorem testBit_rowMask (l : Layout) (n w i p : Nat)
    (hno : ∀ j a b, j < n → a < w → b < w → l.bit n w j a = l.bit n w i b → j = i ∧ a = b) :
    (rowMask l n w i).testBit p = (lanes w).any (fun b => decide (l.bit n w i b = p)) := by
  unfold rowMask lanes
  have hgen : ∀ (bs : List Nat) (acc : Nat),
      (∀ b ∈ bs, b < w) →
      ((bs.foldl (fun a b => a ||| 2 ^ l.bit n w i b) acc).testBit p =
       (acc.testBit p || bs.any (fun b => decide (l.bit n w i b = p)))) := by
    intro bs acc
    induction bs generalizing acc with
    | nil => simp
    | cons b bs ih =>
        intro hmem
        simp only [List.foldl_cons, List.any_cons]
        have hb : b < w := hmem b (List.mem_cons_self ..)
        have hbs : ∀ c ∈ bs, c < w := fun c hc => hmem c (List.mem_cons_of_mem _ hc)
        rw [ih (acc ||| 2 ^ l.bit n w i b) hbs, Nat.testBit_or, Nat.testBit_two_pow]
        by_cases hbp : l.bit n w i b = p
        · subst hbp; simp
        · simp [hbp]
  rw [hgen (List.range w) 0 (fun b hb => by simpa [List.mem_range] using hb)]
  simp

private theorem testBit_rowBits (l : Layout) (n w i x p : Nat)
    (hno : ∀ j a b, j < n → a < w → b < w → l.bit n w j a = l.bit n w i b → j = i ∧ a = b) :
    (rowBits l n w i x).testBit p = (lanes w).any (fun b => x.testBit b && decide (l.bit n w i b = p)) := by
  unfold rowBits lanes
  have hgen : ∀ (bs : List Nat) (acc : Nat),
      (∀ b ∈ bs, b < w) →
      ((bs.foldl (fun a b => if x.testBit b then a ||| 2 ^ l.bit n w i b else a) acc).testBit p =
       (acc.testBit p || bs.any (fun b => x.testBit b && decide (l.bit n w i b = p)))) := by
    intro bs acc
    induction bs generalizing acc with
    | nil => simp
    | cons b bs ih =>
        intro hmem
        simp only [List.foldl_cons, List.any_cons]
        have hb : b < w := hmem b (List.mem_cons_self ..)
        have hbs : ∀ c ∈ bs, c < w := fun c hc => hmem c (List.mem_cons_of_mem _ hc)
        by_cases hx : x.testBit b = true
        · rw [if_pos hx, ih (acc ||| 2 ^ l.bit n w i b) hbs, Nat.testBit_or, Nat.testBit_two_pow]
          simp only [hx, Bool.true_and]
          by_cases hbp : l.bit n w i b = p
          · subst hbp; cases acc.testBit (l.bit n w i b) <;> cases bs.any (fun c => x.testBit c && decide (l.bit n w i c = l.bit n w i b)) <;> simp
          · simp [hbp]
        · have hx' : x.testBit b = false := Bool.eq_false_iff.mpr hx
          rw [if_neg (by simp [hx']), ih acc hbs]
          simp [hx']
  rw [hgen (List.range w) 0 (fun b hb => by simpa [List.mem_range] using hb)]
  simp

private theorem maskedPatch_of_nonoverlap (l : Layout)
    (hno : ∀ n w i j a b, i < n → j < n → a < w → b < w →
      l.bit n w i a = l.bit n w j b → i = j ∧ a = b) :
    maskedPatch l := by
  intro w xs i x hi
  apply Nat.eq_of_testBit_eq
  intro p
  rw [Nat.testBit_or, Nat.testBit_xor, Nat.testBit_and]
  have hlen : (xs.set i x).length = xs.length := List.length_set ..
  have hno' : ∀ j a b, j < xs.length → a < w → b < w →
      l.bit xs.length w j a = l.bit xs.length w i b → j = i ∧ a = b :=
    fun j a b hj ha hb heq => hno xs.length w j i a b hj hi ha hb heq
  rw [testBit_rowMask l xs.length w i p hno', testBit_rowBits l xs.length w i x p hno']
  have himg_orig : (image l w xs).testBit p =
      (xs.zipIdx.any (fun (y,j) => (lanes w).any (fun b => y.testBit b && decide (l.bit xs.length w j b = p)))) :=
    testBit_image l w xs p (fun i j a b hi hj ha hb => hno xs.length w i j a b hi hj ha hb)
  have himg_set : (image l w (xs.set i x)).testBit p =
      ((xs.set i x).zipIdx.any (fun (y,j) => (lanes w).any (fun b => y.testBit b && decide (l.bit xs.length w j b = p)))) := by
    rw [testBit_image l w (xs.set i x) p (by
      intro j1 j2 a b hj1 hj2 ha hb heq
      rw [hlen] at hj1 hj2 heq
      exact hno xs.length w j1 j2 a b hj1 hj2 ha hb heq)]
    rw [hlen]
  rw [himg_orig, himg_set]
  by_cases hpi : (lanes w).any (fun b => decide (l.bit xs.length w i b = p)) = true
  · rw [List.any_eq_true] at hpi
    rcases hpi with ⟨b0, hb0, hbp0⟩
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hbp0
    have hb0' : b0 < w := by simpa [mem_lanes] using hb0
    have horig : xs.zipIdx.any (fun (y,j) => (lanes w).any (fun b => y.testBit b && decide (l.bit xs.length w j b = p))) =
        (xs[i]?.getD 0).testBit b0 := by
      rw [Bool.eq_iff_iff]; constructor
      · intro h; rw [List.any_eq_true] at h
        rcases h with ⟨⟨y,j⟩, hjmem, hj⟩
        rw [List.any_eq_true] at hj
        rcases hj with ⟨b, hb, hcond⟩
        simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
        have hjl : j < xs.length := by
          have := List.mem_zipIdx_iff_getElem?.mp hjmem
          exact (List.getElem?_eq_some_iff.mp this).1
        have hb' : b < w := by simpa [mem_lanes] using hb
        have heq := hno xs.length w j i b b0 hjl hi hb' hb0' (hcond.2.trans hbp0.symm)
        obtain ⟨hji, hbb⟩ := heq
        subst j
        subst b
        have hsome : xs[i]? = some y := List.mem_zipIdx_iff_getElem?.mp hjmem
        have hy : y = xs[i]?.getD 0 := by rw [hsome]; rfl
        rw [← hy]; exact hcond.1
      · intro h; rw [List.any_eq_true]
        refine ⟨(xs[i]?.getD 0, i), ?_, ?_⟩
        · rw [List.mem_zipIdx_iff_getElem?]; simp [List.getElem?_eq_some_iff, hi]
        · rw [List.any_eq_true]; exact ⟨b0, hb0, by simp [hbp0, h]⟩
    have hset : (xs.set i x).zipIdx.any (fun (y,j) => (lanes w).any (fun b => y.testBit b && decide (l.bit xs.length w j b = p))) =
        x.testBit b0 := by
      rw [Bool.eq_iff_iff]; constructor
      · intro h; rw [List.any_eq_true] at h
        rcases h with ⟨⟨y,j⟩, hjmem, hj⟩
        rw [List.any_eq_true] at hj
        rcases hj with ⟨b, hb, hcond⟩
        simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
        have hjl : j < (xs.set i x).length := by
          have := List.mem_zipIdx_iff_getElem?.mp hjmem
          exact (List.getElem?_eq_some_iff.mp this).1
        rw [hlen] at hjl
        have hb' : b < w := by simpa [mem_lanes] using hb
        have heq := hno xs.length w j i b b0 hjl hi hb' hb0' (hcond.2.trans hbp0.symm)
        obtain ⟨hji, hbb⟩ := heq
        subst j
        subst b
        have hsome : (xs.set i x)[i]? = some y := List.mem_zipIdx_iff_getElem?.mp hjmem
        have hy : y = x := by
          have hs : some x = some y := by
            simpa [List.getElem?_set_self hi] using hsome
          cases hs
          rfl
        rw [← hy]; exact hcond.1
      · intro h; rw [List.any_eq_true]
        refine ⟨(x, i), ?_, ?_⟩
        · rw [List.mem_zipIdx_iff_getElem?]
          simp [List.getElem?_eq_some_iff, hlen, hi, List.getElem?_set_self]
        · rw [List.any_eq_true]; exact ⟨b0, hb0, by simp [hbp0, h]⟩
    have hrb : (lanes w).any (fun b => x.testBit b && decide (l.bit xs.length w i b = p)) = x.testBit b0 := by
      rw [Bool.eq_iff_iff]; constructor
      · intro h; rw [List.any_eq_true] at h
        rcases h with ⟨b, hb, hcond⟩
        simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
        have hb' : b < w := by simpa [mem_lanes] using hb
        obtain ⟨_, rfl⟩ := hno xs.length w i i b b0 hi hi hb' hb0' (hcond.2.trans hbp0.symm)
        exact hcond.1
      · intro h; rw [List.any_eq_true]; exact ⟨b0, hb0, by simp [hbp0, h]⟩
    rw [show (lanes w).any (fun b => decide (l.bit xs.length w i b = p)) = true from by
      rw [List.any_eq_true]; exact ⟨b0, hb0, by simp [hbp0]⟩, horig, hset, hrb]
    cases (xs[i]?.getD 0).testBit b0 <;> cases x.testBit b0 <;> simp
  · have hmask : (lanes w).any (fun b => decide (l.bit xs.length w i b = p)) = false := by
      revert hpi; cases (lanes w).any (fun b => decide (l.bit xs.length w i b = p)) <;> simp
    have hrb : (lanes w).any (fun b => x.testBit b && decide (l.bit xs.length w i b = p)) = false := by
      rw [Bool.eq_false_iff, ne_eq, List.any_eq_true]
      rintro ⟨b, hb, hcond⟩
      simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
      have : (lanes w).any (fun b => decide (l.bit xs.length w i b = p)) = true := by
        rw [List.any_eq_true]; exact ⟨b, hb, by simp [hcond.2]⟩
      rw [hmask] at this; cases this
    have hagree : (xs.set i x).zipIdx.any (fun (y,j) => (lanes w).any (fun b => y.testBit b && decide (l.bit xs.length w j b = p))) =
        xs.zipIdx.any (fun (y,j) => (lanes w).any (fun b => y.testBit b && decide (l.bit xs.length w j b = p))) := by
      rw [Bool.eq_iff_iff]
      constructor
      · intro h
        rw [List.any_eq_true] at h ⊢
        rcases h with ⟨⟨y,j⟩, hjmem, hj⟩
        rw [List.any_eq_true] at hj
        rcases hj with ⟨b, hb, hcond⟩
        simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
        have hb' : b < w := by simpa [mem_lanes] using hb
        have hji : j ≠ i := by
          intro hji'
          subst j
          have ht : (lanes w).any (fun b => decide (l.bit xs.length w i b = p)) = true := by
            rw [List.any_eq_true]; exact ⟨b, hb, by simp [hcond.2]⟩
          rw [hmask] at ht
          cases ht
        have hsome_set : (xs.set i x)[j]? = some y := List.mem_zipIdx_iff_getElem?.mp hjmem
        have hget : (xs.set i x)[j]? = xs[j]? := List.getElem?_set_ne (Ne.symm hji)
        have hsome_orig : xs[j]? = some y := by rw [← hget]; exact hsome_set
        refine ⟨(y, j), ?_, ?_⟩
        · rw [List.mem_zipIdx_iff_getElem?]
          exact hsome_orig
        · rw [List.any_eq_true]
          exact ⟨b, hb, by simp [hcond.1, hcond.2]⟩
      · intro h
        rw [List.any_eq_true] at h ⊢
        rcases h with ⟨⟨y,j⟩, hjmem, hj⟩
        rw [List.any_eq_true] at hj
        rcases hj with ⟨b, hb, hcond⟩
        simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
        have hb' : b < w := by simpa [mem_lanes] using hb
        have hsome_orig : xs[j]? = some y := List.mem_zipIdx_iff_getElem?.mp hjmem
        have hjl : j < xs.length := (List.getElem?_eq_some_iff.mp hsome_orig).1
        have hji : j ≠ i := by
          intro hji'
          subst j
          have ht : (lanes w).any (fun b => decide (l.bit xs.length w i b = p)) = true := by
            rw [List.any_eq_true]; exact ⟨b, hb, by simp [hcond.2]⟩
          rw [hmask] at ht
          cases ht
        have hget : (xs.set i x)[j]? = xs[j]? := List.getElem?_set_ne (Ne.symm hji)
        refine ⟨(y, j), ?_, ?_⟩
        · rw [List.mem_zipIdx_iff_getElem?, hget]
          exact hsome_orig
        · rw [List.any_eq_true]
          exact ⟨b, hb, by simp [hcond.1, hcond.2]⟩
    rw [hmask, hrb, hagree]
    cases xs.zipIdx.any (fun (y,j) => (lanes w).any (fun b => y.testBit b && decide (l.bit xs.length w j b = p))) <;> simp

private theorem rotated_nonoverlap : ∀ n w i j a b, i < n → j < n → a < w → b < w →
    layout.bit n w i a = layout.bit n w j b → i = j ∧ a = b := by
  intro n w i j a b hi hj ha hb h
  have hn : 0 < n := by omega
  have hri : (i + a) % n < n := Nat.mod_lt _ hn
  have hrj : (j + b) % n < n := Nat.mod_lt _ hn
  have hab : a = b := by
    by_cases heq : a = b
    · exact heq
    · by_cases halt : a < b
      · have hs : a + 1 ≤ b := by omega
        have hmul : (a + 1) * n ≤ b * n := Nat.mul_le_mul_right n hs
        have hmul' : a * n + n ≤ b * n := by simpa [Nat.succ_mul] using hmul
        have hlt1 : a * n + (i + a) % n < a * n + n := Nat.add_lt_add_left hri _
        have hle : b * n ≤ b * n + (j + b) % n := by omega
        have hlt : a * n + (i + a) % n < b * n + (j + b) % n :=
          Nat.lt_of_lt_of_le (Nat.lt_of_lt_of_le hlt1 hmul') hle
        have hh : a * n + (i + a) % n = b * n + (j + b) % n := by
          simpa [RotatedPlane.layout] using h
        rw [hh] at hlt
        exact (Nat.lt_irrefl _ hlt).elim
      · have hblt : b < a := by omega
        have hs : b + 1 ≤ a := by omega
        have hmul : (b + 1) * n ≤ a * n := Nat.mul_le_mul_right n hs
        have hmul' : b * n + n ≤ a * n := by simpa [Nat.succ_mul] using hmul
        have hlt1 : b * n + (j + b) % n < b * n + n := Nat.add_lt_add_left hrj _
        have hle : a * n ≤ a * n + (i + a) % n := by omega
        have hlt : b * n + (j + b) % n < a * n + (i + a) % n :=
          Nat.lt_of_lt_of_le (Nat.lt_of_lt_of_le hlt1 hmul') hle
        have hh : b * n + (j + b) % n = a * n + (i + a) % n := by
          simpa [RotatedPlane.layout] using h.symm
        rw [hh] at hlt
        exact (Nat.lt_irrefl _ hlt).elim
  subst b
  have hh : a * n + (i + a) % n = a * n + (j + a) % n := by
    simpa [RotatedPlane.layout] using h
  have hmod : (i + a) % n = (j + a) % n := Nat.add_left_cancel hh
  rcases (Nat.mod_eq_mod_iff.mp hmod) with ⟨k₁, k₂, hk⟩
  have hk' : i + k₁ * n = j + k₂ * n := by
    have hk'' : a + (i + k₁ * n) = a + (j + k₂ * n) := by
      calc
        a + (i + k₁ * n) = (i + a) + k₁ * n := by ac_rfl
        _ = (j + a) + k₂ * n := hk
        _ = a + (j + k₂ * n) := by ac_rfl
    exact Nat.add_left_cancel hk''
  have hi_mod : (i + k₁ * n) % n = i := by
    rw [Nat.mul_comm k₁ n, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hi]
  have hj_mod : (j + k₂ * n) % n = j := by
    rw [Nat.mul_comm k₂ n, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hj]
  have hij : i = j := by
    rw [← hi_mod, hk', hj_mod]
  exact ⟨hij, rfl⟩

theorem sound_MaskedPatch : RotatedPlane.MaskedPatch.Goal := by
  exact maskedPatch_of_nonoverlap layout rotated_nonoverlap

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RotatedPlane_MaskedPatch
