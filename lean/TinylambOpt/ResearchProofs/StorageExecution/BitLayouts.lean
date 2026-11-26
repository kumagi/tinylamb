import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.BitLayouts

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-! Proofs/disproofs for all 100 BitLayouts goals. -/


/-! ## Generic lemmas -/

private theorem mem_lanes {b w : Nat} : b ∈ lanes w ↔ b < w := by
  simp [lanes, List.mem_range]

private theorem footprint_mem (l : Layout) (n w i j : Nat) (hj : j < w) :
    l.bit n w i j / 8 ∈ footprint l n w i := by
  unfold footprint
  exact List.mem_eraseDups.mpr (List.mem_map.mpr ⟨j, by simp [lanes, List.mem_range, hj], rfl⟩)

private theorem addressSet_mem (l : Layout) (n w : Nat) (rows : List Nat) {i j : Nat}
    (hi : i ∈ rows) (hj : j < w) : l.bit n w i j / 8 ∈ addressSet l n w rows := by
  unfold addressSet
  refine List.mem_eraseDups.mpr ?_
  refine List.mem_flatMap.mpr ⟨i, hi, footprint_mem l n w i j hj⟩

private theorem fetch_eq_of_agree (l : Layout) (n w i : Nat) (a b : Nat → Nat)
    (h : ∀ j, j < w → a (l.bit n w i j / 8) = b (l.bit n w i j / 8)) :
    fetch l n w i a = fetch l n w i b := by
  unfold fetch lanes
  congr 1
  apply List.map_congr_left
  intro j hj
  have hjw : j < w := by simpa [List.mem_range] using hj
  simp [h j hjw]

theorem sparseGather_of_def (l : Layout) : sparseGather l := by
  intro n w rows a b h
  apply List.map_congr_left
  intro i hi
  exact fetch_eq_of_agree l n w i a b (fun j hj => h _ (addressSet_mem l n w rows hi hj))


private theorem word_congr_mod (w : Nat) (xs ys : List Nat)
    (h : List.map (· % 2 ^ w) xs = List.map (· % 2 ^ w) ys) :
    word w xs = word w ys := by
  induction xs generalizing ys with
  | nil =>
      cases ys with
      | nil => rfl
      | cons y ys => simp at h
  | cons x xs ih =>
      cases ys with
      | nil => simp at h
      | cons y ys =>
          simp only [List.map_cons, List.cons.injEq] at h
          simp [word, h.1, ih ys h.2]

private theorem getD_eq_map (xs : List Nat) (f : Nat → Nat) :
    (List.range xs.length).map (fun i => f (xs[i]?.getD 0)) = xs.map f := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have h : (List.range (x :: xs).length).map (fun i => f ((x :: xs)[i]?.getD 0)) =
          f x :: (List.range xs.length).map (fun i => f (xs[i]?.getD 0)) := by
        rw [show (x :: xs).length = xs.length + 1 from rfl, List.range_succ_eq_map]
        simp [List.getElem?_cons_succ]
      rw [h, ih]
      rfl

theorem transcode_of_lossless (l : Layout) (hl : lossless l) : transcode l := by
  intro w xs
  show pack w ((List.range xs.length).map
    (fun i => fetch l xs.length w i (atByte (file l w xs)))) = pack w xs
  have hfetch : (List.range xs.length).map (fun i => fetch l xs.length w i (atByte (file l w xs))) =
      xs.map (· % 2 ^ w) := by
    rw [← getD_eq_map xs (· % 2 ^ w)]
    apply List.map_congr_left
    intro i hi
    have hiw : i < xs.length := by simpa [List.mem_range] using hi
    exact hl w xs i hiw
  rw [hfetch]
  unfold pack
  simp [word_congr_mod w (xs.map (· % 2 ^ w)) xs (by simp [Nat.mod_mod])]

theorem dictionaryProbe_of_lossless (l : Layout) (hl : lossless l) : dictionaryProbe l := by
  intro w xs dict keys _hkeys
  apply List.map_congr_left
  intro i hi
  have hki := _hkeys i hi
  simp [hl w xs i hki]

/-! ## Bit-level infrastructure -/

private theorem atByte_little (n k j : Nat) (hj : j < k) :
    atByte (little n k) j = (n / 256 ^ j) % 256 := by
  unfold atByte little
  have hj' : j < (List.range k).length := by simpa [List.length_range]
  simp [List.getElem?_eq_getElem (by simp [List.length_map, List.length_range, hj] : j < ((List.range k).map (fun i => (n / 256 ^ i) % 256)).length)]

private theorem file_testBit (l : Layout) (w : Nat) (xs : List Nat) (j r : Nat)
    (hj : j < byteCount (l.capacity xs.length w)) (hr : r < 8) :
    (atByte (file l w xs) j).testBit r = (image l w xs).testBit (j * 8 + r) := by
  unfold file
  rw [atByte_little _ _ j hj]
  have h256 : (256 : Nat) = 2 ^ 8 := by decide
  have hp : (2 ^ 8) ^ j = 2 ^ (8 * j) := by rw [← Nat.pow_mul]
  rw [h256, hp, Nat.testBit_mod_two_pow, Nat.testBit_div_two_pow]
  have hr8 : r < 8 := hr
  simp only [hr8, decide_true, Bool.true_and]
  congr 1
  omega


/-! ## eraseDups helpers -/

private theorem length_eraseDups_le [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.length ≤ l.length := by
  match l with
  | [] => simp
  | a :: as =>
      rw [List.eraseDups_cons]
      simp only [List.length_cons]
      have h1 : (as.filter fun b => !b == a).eraseDups.length ≤ (as.filter fun b => !b == a).length :=
        length_eraseDups_le (as.filter fun b => !b == a)
      have h2 : (as.filter fun b => !b == a).length ≤ as.length := List.length_filter_le _ _
      omega
termination_by l.length
decreasing_by
  simp_wf
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)

private theorem nodup_eraseDups [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  match l with
  | [] => simp
  | a :: as =>
      rw [List.eraseDups_cons]
      rw [List.nodup_cons]
      refine ⟨?_, nodup_eraseDups (as.filter fun b => !b == a)⟩
      intro hmem
      rw [List.mem_eraseDups] at hmem
      rw [List.mem_filter] at hmem
      have h := hmem.2
      simp at h
termination_by l.length
decreasing_by
  simp_wf
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)

private theorem sum_testBit_eq_mod (x w : Nat) :
    ((List.range w).map (fun b => if x.testBit b then 2 ^ b else 0)).sum = x % 2 ^ w := by
  induction w with
  | zero => simp [Nat.mod_one]
  | succ w ih =>
      rw [List.range_succ, List.map_append, List.map_cons, List.map_nil, List.sum_append, List.sum_cons,
        List.sum_nil, Nat.add_zero]
      have hdiv : x / 2 ^ w % 2 = if x.testBit w then 1 else 0 := by
        rw [Nat.testBit_eq_decide_div_mod_eq]
        by_cases h : x / 2 ^ w % 2 = 1
        · simp [h]
        · have h0 : x / 2 ^ w % 2 = 0 := by omega
          simp [h0, h]
      have hmod2 : x % 2 ^ (w + 1) = x % 2 ^ w + 2 ^ w * (x / 2 ^ w % 2) := by
        simpa [Nat.pow_succ, Nat.mul_comm] using (Nat.mod_mul (a := 2 ^ w) (b := 2) (x := x))
      rw [ih, hmod2, hdiv]
      split <;> omega

/-! ## Per-layout proofs -/

/-! ## Generic image characterization -/

private theorem testBit_or_iff (x y i : Nat) : (x ||| y).testBit i = (x.testBit i || y.testBit i) := by
  simp [Nat.testBit_or]

private theorem testBit_two_pow (i j : Nat) : (2 ^ i).testBit j = decide (i = j) := by
  simp [Nat.testBit_two_pow]

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
        rw [ih (acc ||| 2 ^ f b) hinj_bs, testBit_or_iff, testBit_two_pow]
        simp only [hc, Bool.true_and]
        by_cases hfb : f b = p
        · subst hfb; simp
        · have hd : decide (f b = p) = false := decide_eq_false hfb
          rw [hd]
          cases acc.testBit p <;> cases bs.any (fun a => cond a && decide (f a = p)) <;> rfl
      · have hc' : cond b = false := Bool.eq_false_iff.mpr hc
        rw [if_neg (by simp [hc'])]
        rw [ih acc hinj_bs]
        simp [hc']

private theorem testBit_image (l : Layout) (w : Nat) (xs : List Nat) (p : Nat)
    (hno : ∀ r1 r2 c1 c2, r1 < xs.length → r2 < xs.length → c1 < w → c2 < w →
      l.bit xs.length w r1 c1 = l.bit xs.length w r2 c2 → r1 = r2 ∧ c1 = c2) :
    (image l w xs).testBit p =
      (xs.zipIdx.any (fun (xv,iv) => (lanes w).any (fun bv => xv.testBit bv && decide (l.bit xs.length w iv bv = p)))) := by
  unfold image
  have hgen : ∀ (lxs : List (Nat × Nat)) (acc : Nat),
      (∀ xv iv, (xv,iv) ∈ lxs → iv < xs.length) →
      ((lxs.foldl (fun acc (xi) => (lanes w).foldl
        (fun a b => if xi.1.testBit b then a ||| 2^(l.bit xs.length w xi.2 b) else a) acc) acc).testBit p =
       (acc.testBit p || lxs.any (fun (xv,iv) => (lanes w).any (fun bv => xv.testBit bv && decide (l.bit xs.length w iv bv = p))))) := by
    intro lxs
    induction lxs with
    | nil => intro acc _; simp
    | cons xi rest ih =>
        intro acc hmem
        obtain ⟨xv, iv⟩ := xi
        simp only [List.foldl_cons, List.any_cons]
        have hiv : iv < xs.length := hmem xv iv (List.mem_cons_self)
        have hmem_rest : ∀ x' i', (x',i') ∈ rest → i' < xs.length :=
          fun x' i' hm => hmem x' i' (List.mem_cons_of_mem _ hm)
        rw [ih _ hmem_rest]
        rw [testBit_foldl_row (fun b => l.bit xs.length w iv b) (fun b => xv.testBit b) (lanes w) acc p]
        · simp only [Bool.or_assoc]
        · intro a ha bv2 hb2 heq
          have ha' : a < w := by simpa [mem_lanes] using ha
          have hb2' : bv2 < w := by simpa [mem_lanes] using hb2
          exact (hno iv iv a bv2 hiv hiv ha' hb2' heq).2
  have hmem : ∀ xv iv, (xv,iv) ∈ xs.zipIdx → iv < xs.length := by
    intro xv iv h
    have hlt := List.snd_lt_of_mem_zipIdx h
    simpa [List.length_zipIdx] using hlt
  rw [hgen xs.zipIdx 0 hmem]
  simp

private theorem testBit_image_eq (l : Layout) (w : Nat) (xs : List Nat) (i b : Nat)
    (hi : i < xs.length) (hb : b < w)
    (hno : ∀ r1 r2 c1 c2, r1 < xs.length → r2 < xs.length → c1 < w → c2 < w →
      l.bit xs.length w r1 c1 = l.bit xs.length w r2 c2 → r1 = r2 ∧ c1 = c2) :
    (image l w xs).testBit (l.bit xs.length w i b) = (xs[i]?.getD 0).testBit b := by
  rw [testBit_image l w xs _ hno, Bool.eq_iff_iff]
  constructor
  · intro h
    rw [List.any_eq_true] at h
    rcases h with ⟨⟨xv, jv⟩, hjmem, hj⟩
    rw [List.any_eq_true] at hj
    rcases hj with ⟨cv, hc, hcond⟩
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
    have hjl : jv < xs.length := by
      have hm := List.mem_zipIdx_iff_getElem?.mp hjmem
      exact (List.getElem?_eq_some_iff.mp hm).1
    have hc' : cv < w := by simpa [mem_lanes] using hc
    obtain ⟨hjv_eq, hcv_eq⟩ := hno jv i cv b hjl hi hc' hb hcond.2
    subst hjv_eq; subst hcv_eq
    have hmem2 := List.mem_zipIdx_iff_getElem?.mp hjmem
    have hget := (List.getElem?_eq_some_iff.mp hmem2).2
    have hgetd : xs[jv]?.getD 0 = xv := by simp [hget, Option.getD_some]
    rw [hgetd]; exact hcond.1
  · intro h
    rw [List.any_eq_true]
    refine ⟨(xs[i]?.getD 0, i), ?_, ?_⟩
    · rw [List.mem_zipIdx_iff_getElem?]
      exact List.getElem?_eq_some_iff.mpr ⟨hi, by cases hx : xs[i]? <;> simp_all⟩
    · rw [List.any_eq_true]
      refine ⟨b, by simp [mem_lanes, hb], ?_⟩
      simp only [Bool.and_eq_true, decide_eq_true_eq]
      exact ⟨h, by simp⟩

theorem lossless_of_nonoverlap (l : Layout)
    (hno : ∀ n w r1 r2 c1 c2, r1 < n → r2 < n → c1 < w → c2 < w →
      l.bit n w r1 c1 = l.bit n w r2 c2 → r1 = r2 ∧ c1 = c2)
    (hbound : ∀ n w r c, r < n → c < w → l.bit n w r c < 8 * byteCount (l.capacity n w)) :
    lossless l := by
  intro w xs i hi
  have hfetch : fetch l xs.length w i (atByte (file l w xs)) =
      ((List.range w).map (fun b => if (xs[i]?.getD 0).testBit b then 2 ^ b else 0)).sum := by
    unfold fetch lanes
    congr 1
    apply List.map_congr_left
    intro b hb
    have hb' : b < w := by simpa [List.mem_range] using hb
    have hbyte : l.bit xs.length w i b / 8 < byteCount (l.capacity xs.length w) := by
      have := hbound xs.length w i b hi hb'; omega
    have hrem : l.bit xs.length w i b % 8 < 8 := Nat.mod_lt _ (by decide)
    have hread : (atByte (file l w xs) (l.bit xs.length w i b / 8)).testBit (l.bit xs.length w i b % 8) =
        (image l w xs).testBit (l.bit xs.length w i b) := by
      have hf := file_testBit l w xs (l.bit xs.length w i b / 8) (l.bit xs.length w i b % 8) hbyte hrem
      rw [hf]
      congr 1
      omega
    simp [hread, testBit_image_eq l w xs i b hi hb' (fun r1 r2 c1 c2 hr1 hr2 hc1 hc2 => hno xs.length w r1 r2 c1 c2 hr1 hr2 hc1 hc2)]
  rw [hfetch, sum_testBit_eq_mod]

/-! maskedPatch/planePredicate/pageBudget generics -/

private theorem testBit_rowMask (l : Layout) (n w i p : Nat) :
    (rowMask l n w i).testBit p = (lanes w).any (fun b => decide (l.bit n w i b = p)) := by
  unfold rowMask lanes
  have hgen : ∀ (bs : List Nat) (acc : Nat),
      ((bs.foldl (fun a b => a ||| 2 ^ l.bit n w i b) acc).testBit p =
       (acc.testBit p || bs.any (fun b => decide (l.bit n w i b = p)))) := by
    intro bs
    induction bs generalizing acc with
    | nil => simp
    | cons b bs ih =>
        simp only [List.foldl_cons, List.any_cons]
        rw [ih, Nat.testBit_or, Nat.testBit_two_pow]
        by_cases hbp : l.bit n w i b = p <;> simp [hbp]
  simp [hgen]

private theorem testBit_rowBits (l : Layout) (n w i x p : Nat) :
    (rowBits l n w i x).testBit p = (lanes w).any (fun b => x.testBit b && decide (l.bit n w i b = p)) := by
  unfold rowBits lanes
  have hgen : ∀ (bs : List Nat) (acc : Nat),
      ((bs.foldl (fun a b => if x.testBit b then a ||| 2 ^ l.bit n w i b else a) acc).testBit p =
       (acc.testBit p || bs.any (fun b => x.testBit b && decide (l.bit n w i b = p)))) := by
    intro bs
    induction bs generalizing acc with
    | nil => simp
    | cons b bs ih =>
        simp only [List.foldl_cons, List.any_cons]
        by_cases hx : x.testBit b = true
        · rw [if_pos hx, ih, Nat.testBit_or, Nat.testBit_two_pow]
          simp only [hx, Bool.true_and]
          by_cases hbp : l.bit n w i b = p
          · subst hbp; cases acc.testBit (l.bit n w i b) <;> cases bs.any (fun c => x.testBit c && decide (l.bit n w i c = l.bit n w i b)) <;> simp
          · simp [hbp]
        · have hx' : x.testBit b = false := Bool.eq_false_iff.mpr hx
          rw [if_neg (by simp [hx']), ih]; simp [hx']
  simp [hgen]

theorem maskedPatch_of_nonoverlap (l : Layout)
    (hno : ∀ n w r1 r2 c1 c2, r1 < n → r2 < n → c1 < w → c2 < w →
      l.bit n w r1 c1 = l.bit n w r2 c2 → r1 = r2 ∧ c1 = c2) :
    maskedPatch l := by
  intro w xs i x hi
  apply Nat.eq_of_testBit_eq
  intro p
  rw [Nat.testBit_or, Nat.testBit_xor, Nat.testBit_and]
  rw [testBit_rowMask, testBit_rowBits]
  have hlen : (xs.set i x).length = xs.length := List.length_set ..
  have himg_orig : (image l w xs).testBit p =
      (xs.zipIdx.any (fun (y,j) => (lanes w).any (fun b => y.testBit b && decide (l.bit xs.length w j b = p)))) :=
    testBit_image l w xs p (fun r1 r2 c1 c2 hr1 hr2 hc1 hc2 => hno xs.length w r1 r2 c1 c2 hr1 hr2 hc1 hc2)
  have himg_set : (image l w (xs.set i x)).testBit p =
      ((xs.set i x).zipIdx.any (fun (y,j) => (lanes w).any (fun b => y.testBit b && decide (l.bit xs.length w j b = p)))) := by
    have h := testBit_image l w (xs.set i x) p (by intro r1 r2 c1 c2 hr1 hr2 hc1 hc2; rw [hlen] at hr1 hr2 ⊢; exact hno xs.length w r1 r2 c1 c2 hr1 hr2 hc1 hc2); simp only [hlen] at h; rw [h]
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
        rcases hj with ⟨bv, hbv, hcond⟩
        simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
        have hjl : j < xs.length := by
          have hm := List.mem_zipIdx_iff_getElem?.mp hjmem
          exact (List.getElem?_eq_some_iff.mp hm).1
        have hbv' : bv < w := by simpa [mem_lanes] using hbv
        have heq := hno xs.length w j i bv b0 hjl hi hbv' hb0' (hcond.2.trans hbp0.symm)
        obtain ⟨rfl, rfl⟩ := heq
        have hmem2 := List.mem_zipIdx_iff_getElem?.mp hjmem
        have hget := (List.getElem?_eq_some_iff.mp hmem2).2
        simp only [hget, Option.getD_some] at hcond
        exact hcond.1
      · intro h; rw [List.any_eq_true]
        refine ⟨(xs[i]?.getD 0, i), ?_, ?_⟩
        · rw [List.mem_zipIdx_iff_getElem?]; exact List.getElem?_eq_some_iff.mpr ⟨hi, by simp⟩
        · rw [List.any_eq_true]; exact ⟨b0, hb0, by simp [hbp0, h]⟩
    have hset : (xs.set i x).zipIdx.any (fun (y,j) => (lanes w).any (fun b => y.testBit b && decide (l.bit xs.length w j b = p))) =
        x.testBit b0 := by
      rw [Bool.eq_iff_iff]; constructor
      · intro h; rw [List.any_eq_true] at h
        rcases h with ⟨⟨y,j⟩, hjmem, hj⟩
        rw [List.any_eq_true] at hj
        rcases hj with ⟨bv, hbv, hcond⟩
        simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
        have hjl : j < (xs.set i x).length := by
          have hm := List.mem_zipIdx_iff_getElem?.mp hjmem
          exact (List.getElem?_eq_some_iff.mp hm).1
        rw [hlen] at hjl
        have hbv' : bv < w := by simpa [mem_lanes] using hbv
        have heq := hno xs.length w j i bv b0 hjl hi hbv' hb0' (hcond.2.trans hbp0.symm)
        obtain ⟨rfl, rfl⟩ := heq
        have hmem2 := List.mem_zipIdx_iff_getElem?.mp hjmem
        have hget := (List.getElem?_eq_some_iff.mp hmem2).2
        simp only [hlen, List.getElem?_set_self, Option.getD_some] at hget
        simp only [hget] at hcond
        exact hcond.1
      · intro h; rw [List.any_eq_true]
        refine ⟨(x, i), ?_, ?_⟩
        · rw [List.mem_zipIdx_iff_getElem?]; simp [List.getElem?_eq_some_iff, hlen, hi, List.getElem?_set_self]
        · rw [List.any_eq_true]; exact ⟨b0, hb0, by simp [hbp0, h]⟩
    have hrb : (lanes w).any (fun b => x.testBit b && decide (l.bit xs.length w i b = p)) = x.testBit b0 := by
      rw [Bool.eq_iff_iff]; constructor
      · intro h; rw [List.any_eq_true] at h
        rcases h with ⟨bv, hbv, hcond⟩
        simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
        have hbv' : bv < w := by simpa [mem_lanes] using hbv
        obtain ⟨_, rfl⟩ := hno xs.length w i i bv b0 hi hi hbv' hb0' (hcond.2.trans hbp0.symm)
        exact hcond.1
      · intro h; rw [List.any_eq_true]; exact ⟨b0, hb0, by simp [hbp0, h]⟩
    rw [show (lanes w).any (fun b => decide (l.bit xs.length w i b = p)) = true from by rw [List.any_eq_true]; exact ⟨b0, hb0, by simp [hbp0]⟩,
        horig, hset, hrb]
    cases (xs[i]?.getD 0).testBit b0 <;> cases x.testBit b0 <;> simp
  · have hmask : (lanes w).any (fun b => decide (l.bit xs.length w i b = p)) = false := by
      revert hpi; cases (lanes w).any (fun b => decide (l.bit xs.length w i b = p)) <;> simp
    have hrb : (lanes w).any (fun b => x.testBit b && decide (l.bit xs.length w i b = p)) = false := by
      rw [Bool.eq_false_iff, ne_eq, List.any_eq_true]
      rintro ⟨bv, hbv, hcond⟩
      simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
      have : (lanes w).any (fun b => decide (l.bit xs.length w i b = p)) = true :=
        by rw [List.any_eq_true]; exact ⟨bv, hbv, by simp [hcond.2]⟩
      rw [hmask] at this; cases this
    have hagree : (xs.set i x).zipIdx.any (fun (y,j) => (lanes w).any (fun b => y.testBit b && decide (l.bit xs.length w j b = p))) =
        xs.zipIdx.any (fun (y,j) => (lanes w).any (fun b => y.testBit b && decide (l.bit xs.length w j b = p))) := by
      rw [Bool.eq_iff_iff]; constructor <;> intro h <;> rw [List.any_eq_true] at h ⊢ <;> {
        rcases h with ⟨⟨y,j⟩, hjmem, hj⟩
        rw [List.any_eq_true] at hj
        rcases hj with ⟨bv, hbv, hcond⟩
        simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
        have hbv' : bv < w := by simpa [mem_lanes] using hbv
        have hm := List.mem_zipIdx_iff_getElem?.mp hjmem
        have hjl : j < xs.length := by
          have h1 := (List.getElem?_eq_some_iff.mp hm).1
          simpa [hlen] using h1
        have hji : j ≠ i := by
          rintro rfl
          have : (lanes w).any (fun b => decide (l.bit xs.length w i b = p)) = true :=
            by rw [List.any_eq_true]; exact ⟨bv, hbv, by simp [hcond.2]⟩
          rw [hmask] at this; cases this
        refine ⟨(xs[j]?.getD 0, j), ?_, ?_⟩
        · rw [List.mem_zipIdx_iff_getElem?]; exact List.getElem?_eq_some_iff.mpr ⟨hjl, by simp⟩
        · rw [List.any_eq_true]; refine ⟨bv, hbv, ?_⟩
          simp only [Bool.and_eq_true, decide_eq_true_eq]
          refine ⟨?_, hcond.2⟩
          have hgetOrig : y = (xs.set i x)[j]?.getD 0 := by
            have := (List.getElem?_eq_some_iff.mp hm).2
            simpa [hlen] using this
          have hgetOrig2 : y = xs[j]?.getD 0 := by
            rw [hgetOrig]; simp [List.getElem?_set_ne _ _ _ _ hji]
          rw [← hgetOrig2]; exact hcond.1
      }
    rw [hmask, hrb, hagree]
    cases xs.zipIdx.any (fun (y,j) => (lanes w).any (fun b => y.testBit b && decide (l.bit xs.length w j b = p))) <;> simp

theorem planePredicate_of_nonoverlap (l : Layout)
    (hno : ∀ n w r1 r2 c1 c2, r1 < n → r2 < n → c1 < w → c2 < w →
      l.bit n w r1 c1 = l.bit n w r2 c2 → r1 = r2 ∧ c1 = c2)
    (hbound : ∀ n w r c, r < n → c < w → l.bit n w r c < 8 * byteCount (l.capacity n w)) :
    planePredicate l := by
  intro w xs b hb
  show planeRows l xs.length w b (file l w xs) =
    (List.range xs.length).filter (fun i => (xs[i]?.getD 0).testBit b)
  unfold planeRows
  apply List.filter_congr
  intro i hi
  have hi' : i < xs.length := by simpa [List.mem_range] using hi
  have hbyte : l.bit xs.length w i b / 8 < byteCount (l.capacity xs.length w) := by
    have := hbound xs.length w i b hi' hb; omega
  have hrem : l.bit xs.length w i b % 8 < 8 := Nat.mod_lt _ (by decide)
  have hf := file_testBit l w xs (l.bit xs.length w i b / 8) (l.bit xs.length w i b % 8) hbyte hrem
  rw [hf]
  have hidx : l.bit xs.length w i b / 8 * 8 + l.bit xs.length w i b % 8 = l.bit xs.length w i b := by
    have := Nat.div_add_mod (l.bit xs.length w i b) 8; omega
  rw [hidx]
  exact testBit_image_eq l w xs i b hi' hb (fun r1 r2 c1 c2 hr1 hr2 hc1 hc2 => hno xs.length w r1 r2 c1 c2 hr1 hr2 hc1 hc2)

private theorem le_align (n x : Nat) (hn : 0 < n) : x ≤ align n x := by
  simp only [align]
  have hlt : x + n - 1 < n * ((x + n - 1) / n + 1) := Nat.lt_mul_div_succ (x + n - 1) hn
  have hmul : n * ((x + n - 1) / n + 1) = ((x + n - 1) / n) * n + n := by
    rw [Nat.mul_add, Nat.mul_one, Nat.mul_comm]
  rw [hmul] at hlt
  omega

private theorem nodup_map_of_inj {α β : Type} [DecidableEq β] {l : List α} {f : α → β}
    (hn : l.Nodup) (hinj : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
    simp only [List.map_cons, List.nodup_cons] at hn ⊢
    refine ⟨?_, ih hn.2 (fun a ha b hb h => hinj a (List.mem_cons_of_mem _ ha) b (List.mem_cons_of_mem _ hb) h)⟩
    intro hfx
    rw [List.mem_map] at hfx
    rcases hfx with ⟨y, hy, hfy⟩
    have := hinj x (List.mem_cons_self) y (List.mem_cons_of_mem _ hy) hfy.symm
    subst this
    exact hn.1 hy

private theorem perm_of_nodup_nodup_subset {α : Type} [DecidableEq α]
    {l1 l2 : List α} (h1 : l1.Nodup) (h2 : l2.Nodup) (hs : l1 ⊆ l2) (hl : l2.length ≤ l1.length) :
    l1.Perm l2 := by
  induction l1 generalizing l2 with
  | nil =>
    cases l2 with
    | nil => exact List.Perm.nil
    | cons x xs => simp at hl
  | cons x xs ih =>
    have hx2 : x ∈ l2 := hs (List.mem_cons_self)
    have hperm2 : l2.Perm (x :: l2.erase x) := List.perm_cons_erase hx2
    have h2' : (l2.erase x).Nodup := (hperm2.nodup h2).of_cons
    have h1' : xs.Nodup := (List.nodup_cons.mp h1).2
    have hxnot : x ∉ xs := (List.nodup_cons.mp h1).1
    have hs' : xs ⊆ l2.erase x := by
      intro y hy
      rw [List.Nodup.mem_erase_iff h2]
      exact ⟨fun hxy => hxnot (hxy ▸ hy), hs (List.mem_cons_of_mem _ hy)⟩
    have hl' : (l2.erase x).length ≤ xs.length := by
      have hl2 : l2.length = (x :: l2.erase x).length := hperm2.length_eq
      simp at hl2
      simp at hl
      omega
    have hxs : xs.Perm (l2.erase x) := ih h1' h2' hs' hl'
    exact (List.Perm.cons x hxs).trans hperm2.symm

theorem pageBudget_of_bound (l : Layout)
    (hbound : ∀ n w r c, r < n → c < w → l.bit n w r c / 8 < byteCount (l.capacity n w)) :
    pageBudget l := by
  intro n w rows hrows
  refine Nat.le_min.mpr ⟨?_, ?_⟩
  · have hsub : (rows.flatMap (footprint l n w)).eraseDups ⊆ List.range (byteCount (l.capacity n w)) := by
      intro x hx
      rw [List.mem_eraseDups, List.mem_flatMap] at hx
      rcases hx with ⟨i, hi, hxi⟩
      unfold footprint at hxi
      rw [List.mem_eraseDups, List.mem_map] at hxi
      rcases hxi with ⟨b, hb, rfl⟩
      have hb' : b < w := by simpa [mem_lanes] using hb
      exact List.mem_range.mpr (hbound n w i b (hrows i hi) hb')
    have hnodup1 : (addressSet l n w rows).Nodup := nodup_eraseDups _
    calc (addressSet l n w rows).length
        ≤ (List.range (byteCount (l.capacity n w))).length := hnodup1.length_le_of_subset hsub
      _ = byteCount (l.capacity n w) := List.length_range
  · unfold addressSet
    have hsub_rows : ((rows.flatMap (footprint l n w)).eraseDups) ⊆
        ((rows.eraseDups.flatMap (footprint l n w)).eraseDups) := by
      intro x hx
      rw [List.mem_eraseDups, List.mem_flatMap] at hx
      rcases hx with ⟨i, hi, hxi⟩
      rw [List.mem_eraseDups, List.mem_flatMap]
      exact ⟨i, List.mem_eraseDups.mpr hi, hxi⟩
    have hnodup1 : ((rows.flatMap (footprint l n w)).eraseDups).Nodup := nodup_eraseDups _
    calc ((rows.flatMap (footprint l n w)).eraseDups).length
        ≤ ((rows.eraseDups.flatMap (footprint l n w)).eraseDups).length :=
          hnodup1.length_le_of_subset hsub_rows
      _ ≤ (rows.eraseDups.flatMap (footprint l n w)).length := length_eraseDups_le _
      _ = (rows.eraseDups.map (fun i => (footprint l n w i).length)).sum := List.length_flatMap ..
      _ ≤ (rows.eraseDups.map (fun _ => w)).sum := by
          have hle : ∀ i ∈ rows.eraseDups, (footprint l n w i).length ≤ w := by
            intro i _
            unfold footprint
            calc ((lanes w).map (fun b => l.bit n w i b / 8)).eraseDups.length
                ≤ ((lanes w).map (fun b => l.bit n w i b / 8)).length := length_eraseDups_le _
              _ = w := by simp [lanes]
          have hsum_le : ∀ (l : List Nat) (f g : Nat → Nat),
              (∀ x ∈ l, f x ≤ g x) → (l.map f).sum ≤ (l.map g).sum := by
            intro l f g h
            induction l with
            | nil => simp
            | cons a as ih => simp only [List.map_cons, List.sum_cons]; exact Nat.add_le_add (h a (List.mem_cons_self)) (ih (fun x hx => h x (List.mem_cons_of_mem _ hx)))
          exact hsum_le rows.eraseDups (fun i => (footprint l n w i).length) (fun _ => w) hle
      _ = rows.eraseDups.length * w := by
          have hsum : ∀ (l : List Nat) (c : Nat), (l.map (fun _ => c)).sum = l.length * c := by
            intro l c; induction l with
            | nil => simp
            | cons a as ih => simp [ih, Nat.add_mul, Nat.add_comm]
          rw [hsum]

/-! ## Per-layout proofs -/

namespace RowMajorP
open RowMajor
private theorem hno : ∀ n w r1 r2 c1 c2, r1 < n → r2 < n → c1 < w → c2 < w →
    layout.bit n w r1 c1 = layout.bit n w r2 c2 → r1 = r2 ∧ c1 = c2 := by
  intro n w i j a b hi hj ha hb h
  simp only [layout] at h
  have hw : 0 < w := by omega
  have hab : a = b := by
    have := congrArg (· % w) h
    simpa [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt ha, Nat.mod_eq_of_lt hb] using this
  subst hab
  exact ⟨Nat.mul_right_cancel hw (Nat.add_right_cancel h), rfl⟩
private theorem hbound : ∀ n w r c, r < n → c < w → layout.bit n w r c < 8 * byteCount (layout.capacity n w) := by
  intro n w i b hi hb; simp only [layout, byteCount]; omega
private theorem hbound8 : ∀ n w r c, r < n → c < w → layout.bit n w r c / 8 < byteCount (layout.capacity n w) := by
  intro n w i b hi hb; have := hbound n w i b hi hb; omega
theorem sound_RowMajor_Lossless : Lossless.Goal := lossless_of_nonoverlap layout hno hbound
theorem sound_RowMajor_Nonoverlap : Nonoverlap.Goal := by intro n w r1 r2 c1 c2 hr1 hr2 hc1 hc2; constructor; · intro h; exact hno n w r1 r2 c1 c2 hr1 hr2 hc1 hc2 h; · rintro ⟨rfl, rfl⟩; rfl
theorem sound_RowMajor_MaskedPatch : MaskedPatch.Goal := maskedPatch_of_nonoverlap layout hno
theorem sound_RowMajor_PlanePredicate : PlanePredicate.Goal := planePredicate_of_nonoverlap layout hno hbound
theorem sound_RowMajor_SparseGather : SparseGather.Goal := sparseGather_of_def layout
theorem sound_RowMajor_Transcode : Transcode.Goal := transcode_of_lossless layout sound_RowMajor_Lossless
theorem sound_RowMajor_DictionaryProbe : DictionaryProbe.Goal := dictionaryProbe_of_lossless layout sound_RowMajor_Lossless
theorem sound_RowMajor_PageBudget : PageBudget.Goal := pageBudget_of_bound layout hbound8
theorem sound_RowMajor_SharedEdgeByte : SharedEdgeByte.Goal := by
  intro n w i hw
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    rw [List.any_eq_true] at h
    rcases h with ⟨x, hx, hxc⟩
    rw [List.mem_eraseDups, List.mem_map] at hx
    rcases hx with ⟨b, hb, rfl⟩
    rw [List.contains_iff_mem, List.mem_eraseDups, List.mem_map] at hxc
    rcases hxc with ⟨c, hc, hcx⟩
    have hb' : b < w := by simpa [mem_lanes] using hb
    have hc' : c < w := by simpa [mem_lanes] using hc
    simp only [layout] at hcx
    omega
  · intro h
    rw [List.any_eq_true]
    refine ⟨(i * w + (w - 1)) / 8, ?_, ?_⟩
    · rw [List.mem_eraseDups, List.mem_map]
      exact ⟨w - 1, by simp [mem_lanes]; omega, by simp [layout]⟩
    · rw [List.contains_iff_mem, List.mem_eraseDups, List.mem_map]
      refine ⟨0, by simp [mem_lanes]; omega, ?_⟩
      simp only [layout]; omega
theorem sound_RowMajor_PointSpan : PointSpan.Goal := by
  intro n w i
  unfold footprint layout lanes byteCount
  have hsub : ((List.range w).map (fun b => (i * w + b) / 8)).eraseDups ⊆
      (List.range ((w + 14) / 8)).map (fun k => i * w / 8 + k) := by
    intro x hx
    rw [List.mem_eraseDups, List.mem_map] at hx
    rcases hx with ⟨b, hb, rfl⟩
    rw [List.mem_range] at hb
    rw [List.mem_map]
    refine ⟨(i * w + b) / 8 - i * w / 8, ?_, ?_⟩
    · rw [List.mem_range]; omega
    · omega
  have hnodup : ((List.range ((w + 14) / 8)).map (fun k => i * w / 8 + k)).Nodup :=
    List.Nodup.map (fun a b h => by omega) (List.nodup_range _)
  calc ((List.range w).map (fun b => (i * w + b) / 8)).eraseDups.length
      ≤ ((List.range ((w + 14) / 8)).map (fun k => i * w / 8 + k)).length :=
        hnodup.length_le_of_subset hsub
    _ = (w + 14) / 8 := by simp
end RowMajorP

namespace VerticalP
open Vertical
private theorem hno : ∀ n w r1 r2 c1 c2, r1 < n → r2 < n → c1 < w → c2 < w →
    layout.bit n w r1 c1 = layout.bit n w r2 c2 → r1 = r2 ∧ c1 = c2 := by
  intro n w i j a b hi hj ha hb h; simp only [layout] at h
  have hn : 0 < n := by omega
  have hab : a = b := by
    by_contra hab
    rcases Nat.lt_or_gt_of_ne hab with hlt | hgt
    · have : (a + 1) * n ≤ b * n := Nat.mul_le_mul_right n (by omega); omega
    · have : (b + 1) * n ≤ a * n := Nat.mul_le_mul_right n (by omega); omega
  subst hab; exact ⟨by omega, rfl⟩
private theorem hbound : ∀ n w r c, r < n → c < w → layout.bit n w r c < 8 * byteCount (layout.capacity n w) := by
  intro n w i b hi hb; simp only [layout, byteCount]; omega
private theorem hbound8 : ∀ n w r c, r < n → c < w → layout.bit n w r c / 8 < byteCount (layout.capacity n w) := by
  intro n w i b hi hb; have := hbound n w i b hi hb; omega
theorem sound_Vertical_Lossless : Lossless.Goal := lossless_of_nonoverlap layout hno hbound
theorem sound_Vertical_Nonoverlap : Nonoverlap.Goal := by intro n w r1 r2 c1 c2 hr1 hr2 hc1 hc2; constructor; · intro h; exact hno n w r1 r2 c1 c2 hr1 hr2 hc1 hc2 h; · rintro ⟨rfl, rfl⟩; rfl
theorem sound_Vertical_MaskedPatch : MaskedPatch.Goal := maskedPatch_of_nonoverlap layout hno
theorem sound_Vertical_PlanePredicate : PlanePredicate.Goal := planePredicate_of_nonoverlap layout hno hbound
theorem sound_Vertical_SparseGather : SparseGather.Goal := sparseGather_of_def layout
theorem sound_Vertical_Transcode : Transcode.Goal := transcode_of_lossless layout sound_Vertical_Lossless
theorem sound_Vertical_DictionaryProbe : DictionaryProbe.Goal := dictionaryProbe_of_lossless layout sound_Vertical_Lossless
theorem sound_Vertical_PageBudget : PageBudget.Goal := pageBudget_of_bound layout hbound8
theorem sound_Vertical_PlaneScanSpan : PlaneScanSpan.Goal := by
  intro n w b hn hb
  simp only [PlaneScanSpan.Goal, layout]
  have hn8 : 8 ∣ n := Nat.dvd_of_mod_eq_zero hn
  have hnd : n / 8 * 8 = n := Nat.div_mul_cancel hn8
  have hmap : (List.range n).map (fun i => (b * n + i) / 8) =
      (List.range n).map (fun i => b * (n / 8) + i / 8) := by
    apply List.map_congr_left; intro i hi
    have : b * n + i = b * (n / 8) * 8 + i := by rw [← hnd]; simp [Nat.mul_assoc, Nat.mul_left_comm, Nat.mul_comm]
    rw [this]; omega
  rw [hmap]
  have hnodup_shift : ((List.range (n / 8)).map (fun k => b * (n / 8) + k)).Nodup :=
    List.Nodup.map (fun a b h => by omega) (List.nodup_range _)
  apply le_antisymm
  · have hsub : ((List.range n).map (fun i => b * (n / 8) + i / 8)).eraseDups ⊆
        (List.range (n / 8)).map (fun k => b * (n / 8) + k) := by
      intro x hx; rw [List.mem_eraseDups, List.mem_map] at hx
      rcases hx with ⟨i, hi, rfl⟩; rw [List.mem_range] at hi
      rw [List.mem_map]; exact ⟨i / 8, List.mem_range.mpr (by omega), rfl⟩
    calc ((List.range n).map (fun i => b * (n / 8) + i / 8)).eraseDups.length
        ≤ ((List.range (n / 8)).map (fun k => b * (n / 8) + k)).length := hnodup_shift.length_le_of_subset hsub
      _ = n / 8 := by simp
  · have hsub : (List.range (n / 8)).map (fun k => b * (n / 8) + k) ⊆
        ((List.range n).map (fun i => b * (n / 8) + i / 8)).eraseDups := by
      intro x hx; rw [List.mem_map] at hx; rcases hx with ⟨k, hk, rfl⟩; rw [List.mem_range] at hk
      rw [List.mem_eraseDups, List.mem_map]
      exact ⟨k * 8, List.mem_range.mpr (by omega), by have : k * 8 / 8 = k := by omega; simp [this]⟩
    calc n / 8
        = ((List.range (n / 8)).map (fun k => b * (n / 8) + k)).length := by simp
      _ ≤ ((List.range n).map (fun i => b * (n / 8) + i / 8)).eraseDups.length := hnodup_shift.length_le_of_subset hsub
theorem sound_Vertical_AlignedPlane : AlignedPlane.Goal := by
  intro n w b hn hb; simp only [AlignedPlane.Goal, layout]; simp [Nat.mul_mod, hn]
end VerticalP

namespace SerpentineRowP
open SerpentineRow
private theorem hno : ∀ n w r1 r2 c1 c2, r1 < n → r2 < n → c1 < w → c2 < w →
    layout.bit n w r1 c1 = layout.bit n w r2 c2 → r1 = r2 ∧ c1 = c2 := by
  intro n w i j a b hi hj ha hb h; simp only [layout] at h
  have hij : i = j := by
    by_contra hij
    rcases Nat.lt_or_gt_of_ne hij with hlt | hgt
    · have : (i + 1) * w ≤ j * w := Nat.mul_le_mul_right w (by omega); omega
    · have : (j + 1) * w ≤ i * w := Nat.mul_le_mul_right w (by omega); omega
  subst hij; refine ⟨rfl, ?_⟩
  by_cases hp : i % 2 == 0 <;> simp [hp] at h <;> omega
private theorem hbound : ∀ n w r c, r < n → c < w → layout.bit n w r c < 8 * byteCount (layout.capacity n w) := by
  intro n w i b hi hb; simp only [layout, byteCount]; split <;> omega
private theorem hbound8 : ∀ n w r c, r < n → c < w → layout.bit n w r c / 8 < byteCount (layout.capacity n w) := by
  intro n w i b hi hb; have := hbound n w i b hi hb; omega
theorem sound_SerpentineRow_Lossless : Lossless.Goal := lossless_of_nonoverlap layout hno hbound
theorem sound_SerpentineRow_Nonoverlap : Nonoverlap.Goal := by intro n w r1 r2 c1 c2 hr1 hr2 hc1 hc2; constructor; · intro h; exact hno n w r1 r2 c1 c2 hr1 hr2 hc1 hc2 h; · rintro ⟨rfl, rfl⟩; rfl
theorem sound_SerpentineRow_MaskedPatch : MaskedPatch.Goal := maskedPatch_of_nonoverlap layout hno
theorem sound_SerpentineRow_PlanePredicate : PlanePredicate.Goal := planePredicate_of_nonoverlap layout hno hbound
theorem sound_SerpentineRow_SparseGather : SparseGather.Goal := sparseGather_of_def layout
theorem sound_SerpentineRow_Transcode : Transcode.Goal := transcode_of_lossless layout sound_SerpentineRow_Lossless
theorem sound_SerpentineRow_DictionaryProbe : DictionaryProbe.Goal := dictionaryProbe_of_lossless layout sound_SerpentineRow_Lossless
theorem sound_SerpentineRow_PageBudget : PageBudget.Goal := pageBudget_of_bound layout hbound8
theorem sound_SerpentineRow_DoubleStride : DoubleStride.Goal := by
  intro n w i b; simp only [DoubleStride.Goal, layout]; have h2 : (i + 2) % 2 = i % 2 := by omega; simp [h2]; omega
theorem sound_SerpentineRow_FacingEndpoints : FacingEndpoints.Goal := by
  intro n w i hi hw; simp only [FacingEndpoints.Goal, layout]
  have hi0 : i % 2 == 0 = true := by simpa using hi
  have hi1 : (i + 1) % 2 == 0 = false := by have : (i + 1) % 2 ≠ 0 := by omega; simp [this]
  simp [hi0, hi1]; omega
end SerpentineRowP

namespace SerpentinePlaneP
open SerpentinePlane
private theorem hno : ∀ n w r1 r2 c1 c2, r1 < n → r2 < n → c1 < w → c2 < w →
    layout.bit n w r1 c1 = layout.bit n w r2 c2 → r1 = r2 ∧ c1 = c2 := by
  intro n w i j a b hi hj ha hb h; simp only [layout] at h
  have hn : 0 < n := by omega
  have hab : a = b := by
    by_contra hab
    rcases Nat.lt_or_gt_of_ne hab with hlt | hgt
    · have : (a + 1) * n ≤ b * n := Nat.mul_le_mul_right n (by omega); omega
    · have : (b + 1) * n ≤ a * n := Nat.mul_le_mul_right n (by omega); omega
  subst hab; refine ⟨?_, rfl⟩
  by_cases hp : a % 2 == 0 <;> simp [hp] at h <;> omega
private theorem hbound : ∀ n w r c, r < n → c < w → layout.bit n w r c < 8 * byteCount (layout.capacity n w) := by
  intro n w i b hi hb; simp only [layout, byteCount]; split <;> omega
private theorem hbound8 : ∀ n w r c, r < n → c < w → layout.bit n w r c / 8 < byteCount (layout.capacity n w) := by
  intro n w i b hi hb; have := hbound n w i b hi hb; omega
theorem sound_SerpentinePlane_Lossless : Lossless.Goal := lossless_of_nonoverlap layout hno hbound
theorem sound_SerpentinePlane_Nonoverlap : Nonoverlap.Goal := by intro n w r1 r2 c1 c2 hr1 hr2 hc1 hc2; constructor; · intro h; exact hno n w r1 r2 c1 c2 hr1 hr2 hc1 hc2 h; · rintro ⟨rfl, rfl⟩; rfl
theorem sound_SerpentinePlane_MaskedPatch : MaskedPatch.Goal := maskedPatch_of_nonoverlap layout hno
theorem sound_SerpentinePlane_PlanePredicate : PlanePredicate.Goal := planePredicate_of_nonoverlap layout hno hbound
theorem sound_SerpentinePlane_SparseGather : SparseGather.Goal := sparseGather_of_def layout
theorem sound_SerpentinePlane_Transcode : Transcode.Goal := transcode_of_lossless layout sound_SerpentinePlane_Lossless
theorem sound_SerpentinePlane_DictionaryProbe : DictionaryProbe.Goal := dictionaryProbe_of_lossless layout sound_SerpentinePlane_Lossless
theorem sound_SerpentinePlane_PageBudget : PageBudget.Goal := pageBudget_of_bound layout hbound8
theorem sound_SerpentinePlane_SharedPlaneEdge : SharedPlaneEdge.Goal := by
  intro n w b hn hb hbe
  simp only [SharedPlaneEdge.Goal, layout]
  have hb0 : b % 2 == 0 = true := by simpa using hbe
  have hb1 : (b + 1) % 2 == 0 = false := by have : (b + 1) % 2 ≠ 0 := by omega; simp [this]
  simp [hb0, hb1]; constructor <;> intro h <;> omega
theorem sound_SerpentinePlane_FoldedBoundary : FoldedBoundary.Goal := by
  intro n w b hn hbe
  simp only [FoldedBoundary.Goal, layout]
  have hb0 : b % 2 == 0 = true := by simpa using hbe
  have hb1 : (b + 1) % 2 == 0 = false := by have : (b + 1) % 2 ≠ 0 := by omega; simp [this]
  simp [hb0, hb1]; omega
end SerpentinePlaneP

namespace TileTransposeP
open TileTranspose
private theorem hno : ∀ n w r1 r2 c1 c2, r1 < n → r2 < n → c1 < w → c2 < w →
    layout.bit n w r1 c1 = layout.bit n w r2 c2 → r1 = r2 ∧ c1 = c2 := by
  intro n w i j a b hi hj ha hb h
  simp only [layout] at h
  have hw : 0 < w := by omega
  have h8w : 0 < 8 * w := by omega
  have htile : i / 8 = j / 8 := by
    have h1 : (i / 8) * (8 * w) + (a * 8 + i % 8) = (j / 8) * (8 * w) + (b * 8 + j % 8) := by
      calc (i / 8) * (8 * w) + (a * 8 + i % 8)
          = (i / 8) * 8 * w + (a * 8 + i % 8) := by rw [Nat.mul_assoc]
        _ = (j / 8) * 8 * w + (b * 8 + j % 8) := h
        _ = (j / 8) * (8 * w) + (b * 8 + j % 8) := by rw [Nat.mul_assoc]
    have hra : a * 8 + i % 8 < 8 * w := by
      have : a * 8 + i % 8 ≤ (w - 1) * 8 + 7 := by
        have ha1 : a ≤ w - 1 := by omega
        have hi8 : i % 8 ≤ 7 := by omega
        calc a * 8 + i % 8 ≤ (w - 1) * 8 + i % 8 := Nat.add_le_add_right (Nat.mul_le_mul_right 8 ha1) _
          _ ≤ (w - 1) * 8 + 7 := Nat.add_le_add_left hi8 _
      omega
    have hrb : b * 8 + j % 8 < 8 * w := by
      have : b * 8 + j % 8 ≤ (w - 1) * 8 + 7 := by
        have hb1 : b ≤ w - 1 := by omega
        have hj8 : j % 8 ≤ 7 := by omega
        calc b * 8 + j % 8 ≤ (w - 1) * 8 + j % 8 := Nat.add_le_add_right (Nat.mul_le_mul_right 8 hb1) _
          _ ≤ (w - 1) * 8 + 7 := Nat.add_le_add_left hj8 _
      omega
    have hmod1 := congrArg (· % (8 * w)) h1
    simp [Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hra, Nat.mod_eq_of_lt hrb] at hmod1
    have hdiv1 := congrArg (· / (8 * w)) h1
    simp [Nat.add_mul_div_left, Nat.div_eq_of_lt hra, Nat.div_eq_of_lt hrb, h8w] at hdiv1
    exact hdiv1
  have hrem : a * 8 + i % 8 = b * 8 + j % 8 := by
    have h1 : (i / 8) * (8 * w) + (a * 8 + i % 8) = (j / 8) * (8 * w) + (b * 8 + j % 8) := by
      calc (i / 8) * (8 * w) + (a * 8 + i % 8)
          = (i / 8) * 8 * w + (a * 8 + i % 8) := by rw [Nat.mul_assoc]
        _ = (j / 8) * 8 * w + (b * 8 + j % 8) := h
        _ = (j / 8) * (8 * w) + (b * 8 + j % 8) := by rw [Nat.mul_assoc]
    rw [htile] at h1; omega
  have hab : a = b := by
    have ha8 : a = (a * 8 + i % 8) / 8 := by
      have := Nat.add_mul_div_left (i % 8) a (by decide : 0 < 8)
      simpa [Nat.div_eq_of_lt (show i % 8 < 8 by omega)] using this.symm
    have hb8 : b = (b * 8 + j % 8) / 8 := by
      have := Nat.add_mul_div_left (j % 8) b (by decide : 0 < 8)
      simpa [Nat.div_eq_of_lt (show j % 8 < 8 by omega)] using this.symm
    rw [ha8, hb8, hrem]
  have hi8 : i % 8 = j % 8 := by omega
  have hi' : i = i / 8 * 8 + i % 8 := (Nat.div_add_mod i 8).symm
  have hj' : j = j / 8 * 8 + j % 8 := (Nat.div_add_mod j 8).symm
  constructor
  · omega
  · exact hab
private theorem hbound : ∀ n w r c, r < n → c < w → layout.bit n w r c < 8 * byteCount (layout.capacity n w) := by
  intro n w i b hi hb
  simp only [layout, byteCount]
  have hw : 0 < w := by omega
  have htile : (i / 8 + 1) * 8 ≤ align 8 n := by
    have h1 : i / 8 < align 8 n / 8 := by
      have hi' : i < align 8 n := Nat.lt_of_lt_of_le hi (le_align 8 n (by decide))
      omega
    omega
  have hrem : b * 8 + i % 8 < 8 * w := by
    have hb1 : b ≤ w - 1 := by omega
    have hi8 : i % 8 ≤ 7 := by omega
    have : b * 8 + i % 8 ≤ (w - 1) * 8 + 7 := Nat.add_le_add (Nat.mul_le_mul_right 8 hb1) hi8
    omega
  have hbit : layout.bit n w i b < align 8 n * w := by
    simp only [layout]
    calc (i / 8) * 8 * w + b * 8 + i % 8
        < (i / 8) * 8 * w + 8 * w := by omega
      _ = ((i / 8) * 8 + 8) * w := by rw [Nat.right_distrib]; simp [Nat.mul_assoc]
      _ = ((i / 8 + 1) * 8) * w := by congr 1; omega
      _ ≤ align 8 n * w := Nat.mul_le_mul_right w htile
  have hcap : align 8 n * w ≤ align 8 (align 8 n * w) := le_align 8 (align 8 n * w) (by decide)
  exact Nat.lt_of_lt_of_le hbit (by have hc := hcap; simp only [align, byteCount, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] at hc ⊢; omega)
private theorem hbound8 : ∀ n w r c, r < n → c < w → layout.bit n w r c / 8 < byteCount (layout.capacity n w) := by
  intro n w i b hi hb; have := hbound n w i b hi hb; omega
theorem sound_TileTranspose_Lossless : Lossless.Goal := lossless_of_nonoverlap layout hno hbound
theorem sound_TileTranspose_Nonoverlap : Nonoverlap.Goal := by intro n w r1 r2 c1 c2 hr1 hr2 hc1 hc2; constructor; · intro h; exact hno n w r1 r2 c1 c2 hr1 hr2 hc1 hc2 h; · rintro ⟨rfl, rfl⟩; rfl
theorem sound_TileTranspose_MaskedPatch : MaskedPatch.Goal := maskedPatch_of_nonoverlap layout hno
theorem sound_TileTranspose_PlanePredicate : PlanePredicate.Goal := planePredicate_of_nonoverlap layout hno hbound
theorem sound_TileTranspose_SparseGather : SparseGather.Goal := sparseGather_of_def layout
theorem sound_TileTranspose_Transcode : Transcode.Goal := transcode_of_lossless layout sound_TileTranspose_Lossless
theorem sound_TileTranspose_DictionaryProbe : DictionaryProbe.Goal := dictionaryProbe_of_lossless layout sound_TileTranspose_Lossless
theorem sound_TileTranspose_PageBudget : PageBudget.Goal := pageBudget_of_bound layout hbound8
theorem sound_TileTranspose_OneBytePerPlane : OneBytePerPlane.Goal := by
  intro n w tile
  simp only [OneBytePerPlane.Goal, addressSet, footprint, layout]
  let fp := (List.range w).map (fun b => tile * w + b)
  have hfp : ∀ j, j < 8 →
      ((List.range w).map (fun b => (tile * 8 * w + b * 8 + j) / 8)).eraseDups = fp.eraseDups := by
    intro j hj
    congr 1
    apply List.map_congr_left
    intro b hb
    have hb' : b < w := List.mem_range.mp hb
    have hdiv : (tile * 8 * w + b * 8 + j) / 8 = tile * w + b := by
      have h8 : 0 < 8 := by decide
      have hmul : tile * 8 * w + b * 8 + j = 8 * (tile * w + b) + j := by
        calc tile * 8 * w + b * 8 + j
            = 8 * tile * w + 8 * b + j := by simp [Nat.mul_assoc, Nat.mul_left_comm, Nat.mul_comm]
          _ = 8 * (tile * w + b) + j := by rw [← Nat.mul_add]
      rw [hmul, Nat.add_mul_div_left j (tile * w + b) h8, Nat.div_eq_of_lt hj]
      simp
    exact hdiv
  -- flatMap gives 8 copies of fp.eraseDups flattened
  have hflat : ((List.range 8).map (fun j => ((List.range w).map (fun b => (tile * 8 * w + b * 8 + j) / 8)).eraseDups)).flatten.eraseDups = fp.eraseDups := by
    have hmap : (List.range 8).map (fun j => ((List.range w).map (fun b => (tile * 8 * w + b * 8 + j) / 8)).eraseDups)
        = List.replicate 8 fp.eraseDups := by
      apply List.ext_getElem
      · simp
      · intro i hi1 hi2
        simp at hi1
        simp [List.getElem_replicate, hfp i hi1]
    rw [hmap]
    -- eraseDups (replicate 8 fp.eraseDups).flatten = fp.eraseDups
    have hrep : ∀ k, 0 < k → (List.replicate k fp.eraseDups).flatten.eraseDups = fp.eraseDups.eraseDups := by
      intro k hk
      induction k with
      | zero => omega
      | succ k ih =>
        cases k with
        | zero => simp
        | succ k =>
          simp [List.replicate, List.flatten, List.eraseDups_append]
          rw [ih (by omega)]
          simp [List.filter_eq_nil_iff]
    rw [hrep 8 (by decide)]
    exact List.eraseDups_idem fp
  -- rewrite goal
  have hgoal : (((List.range 8).map (tile * 8 + ·)).flatMap
      (fun x => ((List.range w).map (fun b => (x / 8 * 8 * w + b * 8 + x % 8) / 8)).eraseDups)).eraseDups.length = w := by
    have h1 : ((List.range 8).map (tile * 8 + ·)).flatMap
        (fun x => ((List.range w).map (fun b => (x / 8 * 8 * w + b * 8 + x % 8) / 8)).eraseDups)
        = ((List.range 8).map (fun j => ((List.range w).map (fun b => (tile * 8 * w + b * 8 + j) / 8)).eraseDups)).flatten := by
      rw [List.flatMap_eq_map_flatten]
      congr 1
      apply List.map_congr_left
      intro j hj
      have hj' : j < 8 := List.mem_range.mp hj
      simp [Nat.add_mul_div_left j (tile * 8) (by decide : 0 < 8), Nat.div_eq_of_lt hj', Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hj']
    rw [h1, hflat]
    -- fp.eraseDups length = w
    have hnfp : fp.Nodup := by
      simp only [fp]
      apply (List.nodup_range.map ?_).of_map
      intro a b hab
      omega
    rw [List.eraseDups_eq_self.mpr hnfp]
    simp [fp]
  -- the goal as stated
  simpa [List.map_map, Function.comp] using hgoal
theorem sound_TileTranspose_TileCharge : TileCharge.Goal := by
  intro n w tile b i hi
  simp only [TileCharge.Goal, layout]
  have h8 : 0 < 8 := by decide
  have hmul : tile * 8 * w + b * 8 + i = 8 * (tile * w + b) + i := by
    have h1 : tile * 8 * w = 8 * (tile * w) := by rw [Nat.mul_assoc, Nat.mul_comm 8 tile, ← Nat.mul_assoc]
    have h2 : b * 8 = 8 * b := by rw [Nat.mul_comm]
    omega
  rw [hmul, Nat.add_mul_div_left i (tile * w + b) h8, Nat.div_eq_of_lt hi]
  simp
end TileTransposeP

namespace ByteAlignedRowP
open ByteAlignedRow

private theorem align8_eq8 (w : Nat) : align 8 w = 8 * byteCount w := by
  simp only [align, byteCount]
  have h1 : w + 8 - 1 = w + 7 := by omega
  rw [h1, Nat.mul_comm]

private theorem hno : ∀ n w r1 r2 c1 c2, r1 < n → r2 < n → c1 < w → c2 < w →
    layout.bit n w r1 c1 = layout.bit n w r2 c2 → r1 = r2 ∧ c1 = c2 := by
  intro n w i j a b hi hj ha hb h
  simp only [layout] at h
  have haw : 0 < align 8 w := by have : w ≤ align 8 w := le_align 8 w (by decide); omega
  have ha_lt : a < align 8 w := Nat.lt_of_lt_of_le ha (le_align 8 w (by decide))
  have hb_lt : b < align 8 w := Nat.lt_of_lt_of_le hb (le_align 8 w (by decide))
  have hdiv : i = j := by
    have h1 := congrArg (· / align 8 w) h
    have ha1 : (i * align 8 w + a) / align 8 w = i := by
      rw [Nat.add_comm, Nat.mul_comm, Nat.add_mul_div_left a i haw, Nat.div_eq_of_lt ha_lt]; simp
    have hb1 : (j * align 8 w + b) / align 8 w = j := by
      rw [Nat.add_comm, Nat.mul_comm, Nat.add_mul_div_left b j haw, Nat.div_eq_of_lt hb_lt]; simp
    rw [ha1, hb1] at h1; exact h1
  have hmod : a = b := by
    have h1 := congrArg (· % align 8 w) h
    have ha1 : (i * align 8 w + a) % align 8 w = a := by
      rw [Nat.add_comm, Nat.mul_comm, Nat.add_mul_mod_self_left a (align 8 w) i, Nat.mod_eq_of_lt ha_lt]
    have hb1 : (j * align 8 w + b) % align 8 w = b := by
      rw [Nat.add_comm, Nat.mul_comm, Nat.add_mul_mod_self_left b (align 8 w) j, Nat.mod_eq_of_lt hb_lt]
    rw [ha1, hb1] at h1; exact h1
  exact ⟨hdiv, hmod⟩

private theorem hbound : ∀ n w r c, r < n → c < w → layout.bit n w r c < 8 * byteCount (layout.capacity n w) := by
  intro n w i b hi hb
  simp only [layout, byteCount]
  have hb' : b < align 8 w := Nat.lt_of_lt_of_le hb (le_align 8 w (by decide))
  have hbit : i * align 8 w + b < n * align 8 w := by
    calc i * align 8 w + b
        < i * align 8 w + align 8 w := Nat.add_lt_add_left hb' _
      _ = (i + 1) * align 8 w := by rw [Nat.succ_mul]
      _ ≤ n * align 8 w := Nat.mul_le_mul_right (align 8 w) (by omega)
  have hcap : n * align 8 w ≤ align 8 (n * align 8 w) := le_align 8 (n * align 8 w) (by decide)
  have hcap' : align 8 (n * align 8 w) ≤ 8 * ((n * align 8 w + 7) / 8) := by
    simp only [align]; omega
  exact Nat.lt_of_lt_of_le hbit (Nat.le_trans hcap hcap')

private theorem hbound8 : ∀ n w r c, r < n → c < w → layout.bit n w r c / 8 < byteCount (layout.capacity n w) := by
  intro n w i b hi hb; have := hbound n w i b hi hb; omega

theorem sound_ByteAlignedRow_Lossless : Lossless.Goal := lossless_of_nonoverlap layout hno hbound
theorem sound_ByteAlignedRow_Nonoverlap : Nonoverlap.Goal := by intro n w r1 r2 c1 c2 hr1 hr2 hc1 hc2; constructor; · intro h; exact hno n w r1 r2 c1 c2 hr1 hr2 hc1 hc2 h; · rintro ⟨rfl, rfl⟩; rfl
theorem sound_ByteAlignedRow_MaskedPatch : MaskedPatch.Goal := maskedPatch_of_nonoverlap layout hno
theorem sound_ByteAlignedRow_PlanePredicate : PlanePredicate.Goal := planePredicate_of_nonoverlap layout hno hbound
theorem sound_ByteAlignedRow_SparseGather : SparseGather.Goal := sparseGather_of_def layout
theorem sound_ByteAlignedRow_Transcode : Transcode.Goal := transcode_of_lossless layout sound_ByteAlignedRow_Lossless
theorem sound_ByteAlignedRow_DictionaryProbe : DictionaryProbe.Goal := dictionaryProbe_of_lossless layout sound_ByteAlignedRow_Lossless
theorem sound_ByteAlignedRow_PageBudget : PageBudget.Goal := pageBudget_of_bound layout hbound8

private theorem footprint_mem_iff (n w i x : Nat) :
    x ∈ footprint layout n w i ↔ ∃ k, k < byteCount w ∧ x = i * byteCount w + k := by
  simp only [footprint, layout, List.mem_eraseDups, List.mem_map, List.mem_range]
  constructor
  · rintro ⟨b, hb, rfl⟩
    refine ⟨b / 8, ?_, ?_⟩
    · simp only [byteCount]; rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 8)]
      have hw : w ≤ ((w + 7) / 8) * 8 := le_align 8 w (by decide)
      simp only [align] at hw; omega
    · have ha : align 8 w = 8 * byteCount w := align8_eq8 w
      rw [ha]
      have hmul : i * (8 * byteCount w) = 8 * (i * byteCount w) := by
        calc i * (8 * byteCount w) = i * 8 * byteCount w := by rw [Nat.mul_assoc]
          _ = 8 * i * byteCount w := by rw [Nat.mul_comm i 8]
          _ = 8 * (i * byteCount w) := by rw [Nat.mul_assoc]
      rw [hmul, Nat.add_mul_div_left b (i * byteCount w) (by decide : 0 < 8)]
  · rintro ⟨k, hk, rfl⟩
    refine ⟨k * 8, ?_, ?_⟩
    · have hk' : k < (w + 7) / 8 := by simpa [byteCount] using hk
      have hle : k + 1 ≤ (w + 7) / 8 := by omega
      have hmul8 := Nat.mul_le_mul_right 8 hle
      have h3 : (w + 7) / 8 * 8 ≤ w + 7 := Nat.div_mul_le_self (w + 7) 8
      have h4 : (k + 1) * 8 ≤ w + 7 := Nat.le_trans hmul8 h3
      have h5 : k * 8 + 8 ≤ w + 7 := by rw [Nat.add_mul] at h4; simpa using h4
      omega
    · have ha : align 8 w = 8 * byteCount w := align8_eq8 w
      rw [ha]
      have hmul : i * (8 * byteCount w) = 8 * (i * byteCount w) := by
        calc i * (8 * byteCount w) = i * 8 * byteCount w := by rw [Nat.mul_assoc]
          _ = 8 * i * byteCount w := by rw [Nat.mul_comm i 8]
          _ = 8 * (i * byteCount w) := by rw [Nat.mul_assoc]
      rw [hmul]
      have hk8 : k * 8 / 8 = k := by
        rw [Nat.mul_comm, Nat.mul_div_right k (by decide : 0 < 8)]
      have h2 : 8 * (i * byteCount w) + k * 8 = 8 * (i * byteCount w + k) := by
        rw [Nat.mul_add, Nat.mul_comm k 8]
      rw [h2, Nat.mul_div_right (i * byteCount w + k) (by decide : 0 < 8)]

theorem sound_ByteAlignedRow_DisjointRowWrites : DisjointRowWrites.Goal := by
  intro n w i j hij b hb hbj
  rw [footprint_mem_iff] at hb hbj
  rcases hb with ⟨k1, hk1, rfl⟩
  rcases hbj with ⟨k2, hk2, hEq⟩
  have hbc : 0 < byteCount w := by omega
  have hij' : i = j := by
    have h1 := congrArg (· / byteCount w) hEq
    have ha1 : (i * byteCount w + k1) / byteCount w = i := by
      rw [Nat.add_comm, Nat.mul_comm, Nat.add_mul_div_left k1 i hbc, Nat.div_eq_of_lt hk1]; simp
    have hb1 : (j * byteCount w + k2) / byteCount w = j := by
      rw [Nat.add_comm, Nat.mul_comm, Nat.add_mul_div_left k2 j hbc, Nat.div_eq_of_lt hk2]; simp
    rw [ha1, hb1] at h1; exact h1
  exact hij hij'

theorem sound_ByteAlignedRow_PointBytes : PointBytes.Goal := by
  intro n w i
  simp only [PointBytes.Goal]
  have hmem : ∀ x, x ∈ footprint layout n w i ↔ x ∈ (List.range (byteCount w)).map (fun k => i * byteCount w + k) := by
    intro x; rw [footprint_mem_iff]; simp [List.mem_map, List.mem_range]
  have hnodup : ((List.range (byteCount w)).map (fun k => i * byteCount w + k)).Nodup := by
    apply nodup_map_of_inj List.nodup_range
    intro a ha b hb h; omega
  have heq : footprint layout n w i = ((List.range (byteCount w)).map (fun k => i * byteCount w + k)).eraseDups := by
    apply List.eq_of_mem_of_nodup
    · intro x; simp only [List.mem_eraseDups]; exact hmem x
    · exact nodup_eraseDups _
    · exact nodup_eraseDups _
  rw [heq, List.eraseDups_eq_self.mpr hnodup]
  simp [List.length_map, List.length_range]
end ByteAlignedRowP

namespace ByteAlignedPlaneP
open ByteAlignedPlane
private theorem halign8_eq (n : Nat) : align 8 n = byteCount n * 8 := by
  simp [align, byteCount]
private theorem hno : ∀ n w r1 r2 c1 c2, r1 < n → r2 < n → c1 < w → c2 < w →
    layout.bit n w r1 c1 = layout.bit n w r2 c2 → r1 = r2 ∧ c1 = c2 := by
  intro n w i j a b hi hj ha hb h
  simp only [layout] at h
  have han : 0 < align 8 n := by have : n ≤ align 8 n := le_align 8 n (by decide); omega
  have hi_lt : i < align 8 n := Nat.lt_of_lt_of_le hi (le_align 8 n (by decide))
  have hj_lt : j < align 8 n := Nat.lt_of_lt_of_le hj (le_align 8 n (by decide))
  have hdiv : a = b := by
    have h1 := congrArg (· / align 8 n) h
    have h1' := h1
    rw [Nat.mul_comm a, Nat.mul_comm b] at h1'
    rw [Nat.add_comm (align 8 n * a) i, Nat.add_comm (align 8 n * b) j] at h1'
    rw [Nat.add_mul_div_left i a han, Nat.add_mul_div_left j b han, Nat.div_eq_of_lt hi_lt, Nat.div_eq_of_lt hj_lt] at h1'
    simpa using h1'
  have hmod : i = j := by
    have h1 := congrArg (· % align 8 n) h
    have h1' := h1
    rw [Nat.mul_comm a, Nat.mul_comm b] at h1'
    rw [Nat.add_comm (align 8 n * a) i, Nat.add_comm (align 8 n * b) j] at h1'
    rw [Nat.add_mul_mod_self_left i (align 8 n) a, Nat.add_mul_mod_self_left j (align 8 n) b, Nat.mod_eq_of_lt hi_lt, Nat.mod_eq_of_lt hj_lt] at h1'
    simpa [hdiv] using h1'
  exact ⟨hmod, hdiv⟩
private theorem hbound : ∀ n w r c, r < n → c < w → layout.bit n w r c < 8 * byteCount (layout.capacity n w) := by
  intro n w i b hi hb
  simp only [layout, byteCount]
  have hi' : i < align 8 n := Nat.lt_of_lt_of_le hi (le_align 8 n (by decide))
  have hbit : b * align 8 n + i < w * align 8 n := by
    calc b * align 8 n + i
        < b * align 8 n + align 8 n := Nat.add_lt_add_left hi' _
      _ = (b + 1) * align 8 n := by rw [Nat.succ_mul]
      _ ≤ w * align 8 n := Nat.mul_le_mul_right (align 8 n) (by omega)
  have hcap : w * align 8 n ≤ align 8 (w * align 8 n) := le_align 8 (w * align 8 n) (by decide)
  exact Nat.lt_of_lt_of_le hbit (by have hc := hcap; simp only [align, byteCount, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] at hc ⊢; omega)
private theorem hbound8 : ∀ n w r c, r < n → c < w → layout.bit n w r c / 8 < byteCount (layout.capacity n w) := by
  intro n w i b hi hb; have := hbound n w i b hi hb; omega
theorem sound_ByteAlignedPlane_Lossless : Lossless.Goal := lossless_of_nonoverlap layout hno hbound
theorem sound_ByteAlignedPlane_Nonoverlap : Nonoverlap.Goal := by intro n w r1 r2 c1 c2 hr1 hr2 hc1 hc2; constructor; · intro h; exact hno n w r1 r2 c1 c2 hr1 hr2 hc1 hc2 h; · rintro ⟨rfl, rfl⟩; rfl
theorem sound_ByteAlignedPlane_MaskedPatch : MaskedPatch.Goal := maskedPatch_of_nonoverlap layout hno
theorem sound_ByteAlignedPlane_PlanePredicate : PlanePredicate.Goal := planePredicate_of_nonoverlap layout hno hbound
theorem sound_ByteAlignedPlane_SparseGather : SparseGather.Goal := sparseGather_of_def layout
theorem sound_ByteAlignedPlane_Transcode : Transcode.Goal := transcode_of_lossless layout sound_ByteAlignedPlane_Lossless
theorem sound_ByteAlignedPlane_DictionaryProbe : DictionaryProbe.Goal := dictionaryProbe_of_lossless layout sound_ByteAlignedPlane_Lossless
theorem sound_ByteAlignedPlane_PageBudget : PageBudget.Goal := pageBudget_of_bound layout hbound8
theorem sound_ByteAlignedPlane_PlaneDirectory : PlaneDirectory.Goal := by
  intro n w
  simp only [PlaneDirectory.Goal, layout, byteCount, align]
  have h1 : n + 8 - 1 = n + 7 := by omega
  rw [h1]
  have h8 : 0 < 8 := by decide
  have hmul : w * ((n + 7) / 8 * 8) = 8 * (w * ((n + 7) / 8)) := by
    calc w * ((n + 7) / 8 * 8) = w * (8 * ((n + 7) / 8)) := by rw [Nat.mul_comm ((n+7)/8) 8]
      _ = w * 8 * ((n + 7) / 8) := by rw [Nat.mul_assoc]
      _ = 8 * w * ((n + 7) / 8) := by rw [Nat.mul_comm w 8]
      _ = 8 * (w * ((n + 7) / 8)) := by rw [Nat.mul_assoc]
  rw [hmul, Nat.mul_add_div h8 (w * ((n + 7) / 8)) 7, Nat.div_eq_of_lt (by decide : 7 < 8)]
  simp
theorem sound_ByteAlignedPlane_AppendWithinPad : AppendWithinPad.Goal := by
  intro n w i b hn hmod
  simp only [AppendWithinPad.Goal, layout]
  have h8 : 0 < 8 := by decide
  have hn8 : n % 8 ≠ 0 := by simpa using hmod
  have hal : align 8 (n + 1) = align 8 n := by
    simp only [align]
    have heq : n + 1 + 8 - 1 = n + 7 + 1 := by omega
    have heq2 : n + 8 - 1 = n + 7 := by omega
    rw [heq, heq2]
    have hnm : (n + 7) % 8 ≠ 7 := by
      intro h7
      have : n % 8 = 0 := by omega
      exact hn8 this
    have h1 : (n + 7 + 1) / 8 = (n + 7) / 8 := by
      have hdiv : n + 7 = 8 * ((n + 7) / 8) + (n + 7) % 8 := (Nat.div_add_mod (n + 7) 8).symm
      have hlt : (n + 7) % 8 < 7 := by omega
      omega
    rw [h1]
  simp [hal]
end ByteAlignedPlaneP

namespace NetworkBitOrderP
open NetworkBitOrder

private theorem hno : ∀ n w r1 r2 c1 c2, r1 < n → r2 < n → c1 < w → c2 < w →
    layout.bit n w r1 c1 = layout.bit n w r2 c2 → r1 = r2 ∧ c1 = c2 := by
  intro n w i j a b hi hj ha hb h
  simp only [layout] at h
  let p1 := i * w + a
  let p2 := j * w + b
  change p1 / 8 * 8 + (7 - p1 % 8) = p2 / 8 * 8 + (7 - p2 % 8) at h
  have hbyte : p1 / 8 = p2 / 8 := by
    have hd1 : (p1 / 8 * 8 + (7 - p1 % 8)) / 8 = p1 / 8 := by
      have h8 : 0 < 8 := by decide
      rw [Nat.add_comm, Nat.mul_comm (p1 / 8) 8, Nat.add_mul_div_left (7 - p1 % 8) (p1 / 8) h8, Nat.div_eq_of_lt (by omega)]
      simp
    have hd2 : (p2 / 8 * 8 + (7 - p2 % 8)) / 8 = p2 / 8 := by
      have h8 : 0 < 8 := by decide
      rw [Nat.add_comm, Nat.mul_comm (p2 / 8) 8, Nat.add_mul_div_left (7 - p2 % 8) (p2 / 8) h8, Nat.div_eq_of_lt (by omega)]
      simp
    have h1 := congrArg (· / 8) h
    rw [hd1, hd2] at h1
    exact h1
  have hbit : p1 % 8 = p2 % 8 := by
    have hm1 : (p1 / 8 * 8 + (7 - p1 % 8)) % 8 = 7 - p1 % 8 := by
      have h8 : 0 < 8 := by decide
      rw [Nat.add_comm, Nat.mul_comm (p1 / 8) 8, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt (by omega)]
    have hm2 : (p2 / 8 * 8 + (7 - p2 % 8)) % 8 = 7 - p2 % 8 := by
      have h8 : 0 < 8 := by decide
      rw [Nat.add_comm, Nat.mul_comm (p2 / 8) 8, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt (by omega)]
    have h1 := congrArg (· % 8) h
    rw [hm1, hm2] at h1
    omega
  have hp : p1 = p2 := by
    have h1 : p1 = p1 / 8 * 8 + p1 % 8 := by have := (Nat.div_add_mod p1 8).symm; rw [Nat.mul_comm 8 (p1/8)] at this; exact this
    have h2 : p2 = p2 / 8 * 8 + p2 % 8 := by have := (Nat.div_add_mod p2 8).symm; rw [Nat.mul_comm 8 (p2/8)] at this; exact this
    omega
  have hw : 0 < w := by omega
  have hi' : i = j := by
    have h1 := congrArg (· / w) hp
    simp only [p1, p2, Nat.add_mul_div_left, Nat.div_eq_of_lt ha, Nat.div_eq_of_lt hb, hw] at h1
    simpa using h1
  have hab : a = b := by
    have h1 := congrArg (· % w) hp
    simp only [p1, p2, Nat.mul_comm w i, Nat.mul_comm w j, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt ha, Nat.mod_eq_of_lt hb] at h1
    simpa using h1
  exact ⟨hi', hab⟩

private theorem hbound : ∀ n w r c, r < n → c < w → layout.bit n w r c < 8 * byteCount (layout.capacity n w) := by
  intro n w i b hi hb
  simp only [layout, byteCount]
  let p := i * w + b
  change p / 8 * 8 + (7 - p % 8) < 8 * ((align 8 (n * w) + 7) / 8)
  have h1 : p / 8 * 8 + (7 - p % 8) < (p / 8 + 1) * 8 := by omega
  have h2 : (p / 8 + 1) * 8 ≤ align 8 (n * w) := by
    have hp_lt : p < n * w := by
      calc i * w + b < i * w + w := Nat.add_lt_add_left hb _
        _ = (i + 1) * w := by rw [Nat.succ_mul]
        _ ≤ n * w := Nat.mul_le_mul_right w (by omega)
    have hp8 : p / 8 < (n * w + 7) / 8 := by omega
    simp only [align]; omega
  have h3 : align 8 (n * w) ≤ 8 * ((align 8 (n * w) + 7) / 8) := by
    simp only [align]; omega
  omega

private theorem hbound8 : ∀ n w r c, r < n → c < w → layout.bit n w r c / 8 < byteCount (layout.capacity n w) := by
  intro n w i b hi hb; have := hbound n w i b hi hb; omega

theorem sound_NetworkBitOrder_Lossless : Lossless.Goal := lossless_of_nonoverlap layout hno hbound
theorem sound_NetworkBitOrder_Nonoverlap : Nonoverlap.Goal := by intro n w r1 r2 c1 c2 hr1 hr2 hc1 hc2; constructor; · intro h; exact hno n w r1 r2 c1 c2 hr1 hr2 hc1 hc2 h; · rintro ⟨rfl, rfl⟩; rfl
theorem sound_NetworkBitOrder_MaskedPatch : MaskedPatch.Goal := maskedPatch_of_nonoverlap layout hno
theorem sound_NetworkBitOrder_PlanePredicate : PlanePredicate.Goal := planePredicate_of_nonoverlap layout hno hbound
theorem sound_NetworkBitOrder_SparseGather : SparseGather.Goal := sparseGather_of_def layout
theorem sound_NetworkBitOrder_Transcode : Transcode.Goal := transcode_of_lossless layout sound_NetworkBitOrder_Lossless
theorem sound_NetworkBitOrder_DictionaryProbe : DictionaryProbe.Goal := dictionaryProbe_of_lossless layout sound_NetworkBitOrder_Lossless
theorem sound_NetworkBitOrder_PageBudget : PageBudget.Goal := pageBudget_of_bound layout hbound8

theorem sound_NetworkBitOrder_BitReversal : BitReversal.Goal := by
  intro n w i b
  simp only [BitReversal.Goal, layout]
  let p := i * w + b
  constructor
  · have hmod : (p / 8 * 8 + (7 - p % 8)) % 8 = 7 - p % 8 := by
      have h8 : 0 < 8 := by decide
      rw [Nat.add_comm, Nat.mul_comm (p / 8) 8, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt (by omega)]
    omega
  · have h8 : 0 < 8 := by decide
    rw [Nat.add_comm, Nat.mul_comm (p / 8) 8, Nat.add_mul_div_left (7 - p % 8) (p / 8) h8, Nat.div_eq_of_lt (by omega)]
    simp [p]

theorem sound_NetworkBitOrder_ByteFootprint : ByteFootprint.Goal := by
  intro n w i
  simp only [ByteFootprint.Goal, footprint, layout, RowMajor.layout]
  apply congrArg List.eraseDups
  apply List.map_congr_left
  intro b hb
  let p := i * w + b
  have h8 : 0 < 8 := by decide
  have hdiv : (p / 8 * 8 + (7 - p % 8)) / 8 = p / 8 := by
    rw [Nat.add_comm, Nat.mul_comm (p / 8) 8, Nat.add_mul_div_left (7 - p % 8) (p / 8) h8, Nat.div_eq_of_lt (by omega)]
    simp
  exact hdiv
end NetworkBitOrderP

namespace RotatedPlaneP
open RotatedPlane
private theorem hno : ∀ n w r1 r2 c1 c2, r1 < n → r2 < n → c1 < w → c2 < w →
    layout.bit n w r1 c1 = layout.bit n w r2 c2 → r1 = r2 ∧ c1 = c2 := by
  intro n w i j a b hi hj ha hb h
  simp only [layout] at h
  have hn : 0 < n := by omega
  have hdiv : a = b := by
    have h1 := congrArg (· / n) h
    have hia : (i + a) % n < n := Nat.mod_lt _ hn
    have hjb : (j + b) % n < n := Nat.mod_lt _ hn
    simpa [Nat.add_mul_div_left, Nat.div_eq_of_lt hia, Nat.div_eq_of_lt hjb, hn] using h1
  subst hdiv
  have hmod : (i + b) % n = (j + b) % n := by
    have h1 := congrArg (· % n) h
    have hia : (i + b) % n < n := Nat.mod_lt _ hn
    have hjb : (j + b) % n < n := Nat.mod_lt _ hn
    simpa [Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hia, Nat.mod_eq_of_lt hjb] using h1
  have hi' : i % n = j % n := by
    have h1 := Nat.add_mod_right i b
    have h2 := Nat.add_mod_right j b
    -- (i+b)%n = (i%n+b%n)%n = (j%n+b%n)%n = (j+b)%n
    have h3 : (i % n + b % n) % n = (j % n + b % n) % n := by
      rw [← Nat.add_mod, ← Nat.add_mod]; exact hmod
    -- cancel b%n
    have h4 := h3
    rw [Nat.add_mod, Nat.add_mod (j % n)] at h4
    simp only [Nat.mod_mod_of_dvd, dvd_refl] at h4
    -- easier: use Nat.mod_eq_of_lt
    omega
  have : i = j := by
    rw [Nat.mod_eq_of_lt hi, Nat.mod_eq_of_lt hj] at hi'
    exact hi'
  exact ⟨this, rfl⟩
private theorem hbound : ∀ n w r c, r < n → c < w → layout.bit n w r c < 8 * byteCount (layout.capacity n w) := by
  intro n w i b hi hb
  simp only [layout, byteCount]
  have hn : 0 < n := by omega
  have hbit : b * n + (i + b) % n < n * w := by
    have hmod : (i + b) % n < n := Nat.mod_lt _ hn
    calc b * n + (i + b) % n
        < b * n + n := Nat.add_lt_add_left hmod _
      _ = (b + 1) * n := by rw [Nat.succ_mul, Nat.mul_comm n b, Nat.mul_comm n (b+1)]; omega
      _ ≤ w * n := Nat.mul_le_mul_right n (by omega)
      _ = n * w := by rw [Nat.mul_comm]
  have hcap : n * w ≤ align 8 (n * w) := le_align 8 (n * w) (by decide)
  exact Nat.lt_of_lt_of_le hbit (by have hc := hcap; simp only [align, byteCount, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] at hc ⊢; omega)
private theorem hbound8 : ∀ n w r c, r < n → c < w → layout.bit n w r c / 8 < byteCount (layout.capacity n w) := by
  intro n w i b hi hb; have := hbound n w i b hi hb; omega
theorem sound_RotatedPlane_Lossless : Lossless.Goal := lossless_of_nonoverlap layout hno hbound
theorem sound_RotatedPlane_Nonoverlap : Nonoverlap.Goal := by intro n w r1 r2 c1 c2 hr1 hr2 hc1 hc2; constructor; · intro h; exact hno n w r1 r2 c1 c2 hr1 hr2 hc1 hc2 h; · rintro ⟨rfl, rfl⟩; rfl
theorem sound_RotatedPlane_MaskedPatch : MaskedPatch.Goal := maskedPatch_of_nonoverlap layout hno
theorem sound_RotatedPlane_PlanePredicate : PlanePredicate.Goal := planePredicate_of_nonoverlap layout hno hbound
theorem sound_RotatedPlane_SparseGather : SparseGather.Goal := sparseGather_of_def layout
theorem sound_RotatedPlane_Transcode : Transcode.Goal := transcode_of_lossless layout sound_RotatedPlane_Lossless
theorem sound_RotatedPlane_DictionaryProbe : DictionaryProbe.Goal := dictionaryProbe_of_lossless layout sound_RotatedPlane_Lossless
theorem sound_RotatedPlane_PageBudget : PageBudget.Goal := pageBudget_of_bound layout hbound8
theorem sound_RotatedPlane_RotatedLocations : RotatedLocations.Goal := by
  intro n w b hn
  simp only [RotatedLocations.Goal, layout]
  have hnodup1 : ((List.range n).map (fun i => (i + b) % n)).Nodup := by
    apply nodup_map_of_inj List.nodup_range
    intro i hi j hj h
    have hi' : i < n := List.mem_range.mp hi
    have hj' : j < n := List.mem_range.mp hj
    have hmod : (i + b) % n = (j + b) % n := h
    have h1 : (i % n + b % n) % n = (j % n + b % n) % n := by
      simpa [Nat.add_mod] using hmod
    -- cancel
    have : i % n = j % n := by
      have hi0 : i % n < n := Nat.mod_lt _ hn
      have hj0 : j % n < n := Nat.mod_lt _ hn
      omega
    simpa [Nat.mod_eq_of_lt hi', Nat.mod_eq_of_lt hj'] using this
  have hsub : ((List.range n).map (fun i => (i + b) % n)) ⊆ List.range n := by
    intro x hx
    rw [List.mem_map] at hx
    rcases hx with ⟨i, hi, rfl⟩
    exact List.mem_range.mpr (Nat.mod_lt _ hn)
  have hlen : ((List.range n).map (fun i => (i + b) % n)).length ≤ (List.range n).length := by simp
  have hpermInner : ((List.range n).map (fun i => (i + b) % n)).Perm (List.range n) :=
    perm_of_nodup_nodup_subset hnodup1 List.nodup_range hsub hlen
  -- map (b*n + ·) over perm
  have hperm : ((List.range n).map (fun i => b * n + (i + b) % n)).Perm ((List.range n).map (fun i => b * n + i)) := by
    simpa [List.map_map, Function.comp] using hpermInner.map (fun x => b * n + x)
  exact hperm
theorem sound_RotatedPlane_BankCycle : BankCycle.Goal := by
  intro n w i b hn
  simp only [BankCycle.Goal, layout]
  have h : (i + n + b) % n = (i + b) % n := by
    have h1 : i + n + b = (i + b) + n := by omega
    rw [h1, Nat.add_mod_right]
  rw [h]
end RotatedPlaneP

namespace SplitNibbleP
open SplitNibble
private theorem hno : ∀ n w r1 r2 c1 c2, r1 < n → r2 < n → c1 < w → c2 < w →
    layout.bit n w r1 c1 = layout.bit n w r2 c2 → r1 = r2 ∧ c1 = c2 := by
  intro n w i j a b hi hj ha hb h
  simp only [layout] at h
  have hmin : min 4 w ≤ 4 := Nat.min_le_left _ _
  by_cases ha4 : a < 4 <;> by_cases hb4 : b < 4
  · -- both low nibble
    simp [ha4, hb4] at h
    have hia : a < min 4 w := by omega
    have hjb : b < min 4 w := by omega
    have hij : i = j := by
      have h1 := congrArg (· / min 4 w) h
      have hmw : 0 < min 4 w := by omega
      simpa [Nat.add_mul_div_left, Nat.div_eq_of_lt hia, Nat.div_eq_of_lt hjb, hmw] using h1
    have hab : a = b := by
      have h1 := congrArg (· % min 4 w) h
      have hmw : 0 < min 4 w := by omega
      simpa [Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hia, Nat.mod_eq_of_lt hjb, hij] using h1
    exact ⟨hij, hab⟩
  · -- a low, b high - impossible since ranges disjoint
    simp [ha4, hb4] at h
    have hlow : i * min 4 w + a < n * min 4 w := by
      have ha' : a < min 4 w := by omega
      calc i * min 4 w + a < i * min 4 w + min 4 w := Nat.add_lt_add_left ha' _
        _ = (i + 1) * min 4 w := by rw [Nat.succ_mul]
        _ ≤ n * min 4 w := Nat.mul_le_mul_right (min 4 w) (by omega)
    omega
  · -- a high, b low
    simp [ha4, hb4] at h
    have hlow : j * min 4 w + b < n * min 4 w := by
      have hb' : b < min 4 w := by omega
      calc j * min 4 w + b < j * min 4 w + min 4 w := Nat.add_lt_add_left hb' _
        _ = (j + 1) * min 4 w := by rw [Nat.succ_mul]
        _ ≤ n * min 4 w := Nat.mul_le_mul_right (min 4 w) (by omega)
    omega
  · -- both high
    simp [ha4, hb4] at h
    have hn : 0 < n := by omega
    have ha' : a - 4 < w - 4 := by omega
    have hb' : b - 4 < w - 4 := by omega
    have hdiv : a - 4 = b - 4 := by
      have h1 := congrArg (fun x => (x - n * min 4 w) / n) h
      have hia : i < n := hi
      have hjb : j < n := hj
      simp only [Nat.add_sub_cancel_left] at h1
      have h1' : (a - 4) * n / n + i / n = (b - 4) * n / n + j / n := by
        simpa [Nat.add_mul_div_left, hn] using h1
      simp [Nat.div_eq_of_lt hia, Nat.div_eq_of_lt hjb, Nat.mul_div_right, hn] at h1'
      exact h1'
    have hmod : i = j := by
      have h1 := congrArg (fun x => (x - n * min 4 w) % n) h
      simp only [Nat.add_sub_cancel_left, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hi, Nat.mod_eq_of_lt hj] at h1
      exact h1
    have hab : a = b := by omega
    exact ⟨hmod, hab⟩
private theorem hbound : ∀ n w r c, r < n → c < w → layout.bit n w r c < 8 * byteCount (layout.capacity n w) := by
  intro n w i b hi hb
  simp only [layout, byteCount]
  have hcap : n * w ≤ align 8 (n * w) := le_align 8 (n * w) (by decide)
  have hbit : (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) < n * w := by
    split
    · rename_i hb4
      have hmw : min 4 w ≤ w := Nat.min_le_right _ _
      have ha : b < min 4 w := by omega
      calc i * min 4 w + b
          < i * min 4 w + min 4 w := Nat.add_lt_add_left ha _
        _ = (i + 1) * min 4 w := by rw [Nat.succ_mul]
        _ ≤ n * min 4 w := Nat.mul_le_mul_right (min 4 w) (by omega)
        _ ≤ n * w := Nat.mul_le_mul_left n hmw
    · rename_i hb4
      have hb4' : 4 ≤ b := by omega
      have hbw : b - 4 < w - 4 := by omega
      have hmw : min 4 w ≤ 4 := Nat.min_le_left _ _
      calc n * min 4 w + (b - 4) * n + i
          < n * min 4 w + (b - 4) * n + n := Nat.add_lt_add_left hi _
        _ = n * min 4 w + ((b - 4) + 1) * n := by rw [Nat.succ_mul]; omega
        _ ≤ n * 4 + ((b - 4) + 1) * n := by
            apply Nat.add_le_add_right
            exact Nat.mul_le_mul_left n hmw
        _ = n * (4 + ((b - 4) + 1)) := by rw [Nat.mul_add]; omega
        _ ≤ n * w := by
            apply Nat.mul_le_mul_left
            omega
  exact Nat.lt_of_lt_of_le hbit (by have hc := hcap; simp only [align, byteCount, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] at hc ⊢; omega)
private theorem hbound8 : ∀ n w r c, r < n → c < w → layout.bit n w r c / 8 < byteCount (layout.capacity n w) := by
  intro n w i b hi hb; have := hbound n w i b hi hb; omega
theorem sound_SplitNibble_Lossless : Lossless.Goal := lossless_of_nonoverlap layout hno hbound
theorem sound_SplitNibble_Nonoverlap : Nonoverlap.Goal := by intro n w r1 r2 c1 c2 hr1 hr2 hc1 hc2; constructor; · intro h; exact hno n w r1 r2 c1 c2 hr1 hr2 hc1 hc2 h; · rintro ⟨rfl, rfl⟩; rfl
theorem sound_SplitNibble_MaskedPatch : MaskedPatch.Goal := maskedPatch_of_nonoverlap layout hno
theorem sound_SplitNibble_PlanePredicate : PlanePredicate.Goal := planePredicate_of_nonoverlap layout hno hbound
theorem sound_SplitNibble_SparseGather : SparseGather.Goal := sparseGather_of_def layout
theorem sound_SplitNibble_Transcode : Transcode.Goal := transcode_of_lossless layout sound_SplitNibble_Lossless
theorem sound_SplitNibble_DictionaryProbe : DictionaryProbe.Goal := dictionaryProbe_of_lossless layout sound_SplitNibble_Lossless
theorem sound_SplitNibble_PageBudget : PageBudget.Goal := pageBudget_of_bound layout hbound8
theorem sound_SplitNibble_MixedPointCost : MixedPointCost.Goal := by
  intro n w i hw
  simp only [MixedPointCost.Goal, footprint, layout]
  -- low nibble bits b<4 give bytes in same block, high bits b>=4 give individual bytes
  -- crude bound: eraseDups length <= list length <= w
  -- need <= 1 + (w-4)
  have hlen : ((List.range w).map (fun b => (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) / 8)).eraseDups.length
      ≤ 1 + (w - 4) := by
    -- split into low and high
    have hsplit : (List.range w) = (List.range 4) ++ (List.range (w - 4)).map (· + 4) := by
      have : w = 4 + (w - 4) := by omega
      conv_lhs => rw [this]
      rw [List.range_add, Nat.add_comm]
    rw [hsplit, List.map_append, List.eraseDups_append]
    calc (((List.range 4).map (fun b => (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) / 8)).eraseDups ++
          (((List.range (w - 4)).map (· + 4)).map (fun b => (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) / 8)).eraseDups.filter
            (fun x => !(((List.range 4).map (fun b => (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) / 8)).eraseDups.contains x))).length
        ≤ (((List.range 4).map (fun b => (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) / 8)).eraseDups).length +
          ((((List.range (w - 4)).map (· + 4)).map (fun b => (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) / 8)).eraseDups.filter _).length := by
            rw [List.length_append]
      _ ≤ (((List.range 4).map (fun b => (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) / 8)).eraseDups).length +
          ((((List.range (w - 4)).map (· + 4)).map (fun b => (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) / 8)).eraseDups).length := by
            apply Nat.add_le_add_left
            exact List.length_filter_le _ _
      _ ≤ 1 + (w - 4) := by
            have hlow : (((List.range 4).map (fun b => (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) / 8)).eraseDups).length ≤ 1 := by
              -- all 4 low bits map to same byte when w>=4: min 4 w = 4, so i*4+b, b<4 => same /8 = i*4/8
              have hmw : min 4 w = 4 := by omega
              have hall : ∀ b ∈ List.range 4, (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) / 8 = (i * 4) / 8 := by
                intro b hb
                have hb' : b < 4 := List.mem_range.mp hb
                simp [hb', hmw]
                omega
              -- eraseDups of list where all elements equal gives length <= 1
              have hnodup := nodup_eraseDups ((List.range 4).map (fun b => (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) / 8))
              have hsub : ∀ x ∈ ((List.range 4).map (fun b => (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) / 8)).eraseDups, x = (i * 4) / 8 := by
                intro x hx
                rw [List.mem_eraseDups, List.mem_map] at hx
                rcases hx with ⟨b, hb, rfl⟩
                exact hall b hb
              match h : ((List.range 4).map (fun b => (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) / 8)).eraseDups with
              | [] => simp
              | [x] => simp
              | x :: y :: xs =>
                have hx := hsub x (by simp [h])
                have hy := hsub y (by simp [h])
                have hnd : (x :: y :: xs).Nodup := by rw [← h]; exact hnodup
                simp at hnd
                exact absurd (hx.trans hy.symm) hnd.1
            have hhigh : ((((List.range (w - 4)).map (· + 4)).map (fun b => (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) / 8)).eraseDups).length ≤ (w - 4) := by
              calc ((((List.range (w - 4)).map (· + 4)).map (fun b => (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) / 8)).eraseDups).length
                  ≤ (((List.range (w - 4)).map (· + 4)).map (fun b => (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) / 8)).length := length_eraseDups_le _
                _ = w - 4 := by simp
            omega
  exact hlen
theorem sound_SplitNibble_LowNibbleLocal : LowNibbleLocal.Goal := by
  intro n w i b hw hb
  simp only [LowNibbleLocal.Goal, layout]
  have hmw : min 4 w = 4 := by omega
  have hb4 : b < 4 := hb
  simp [hb4, hmw]
  omega

end SplitNibbleP

end TinylambOpt.ResearchProofs.StorageExecution.BitLayouts
