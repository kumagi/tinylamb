import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `ByteAlignedPlane.Lossless` — Read one lane from serialized bytes, including zero
  width.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedPlane_Lossless

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedPlane

-- Small-input truth checks.
#eval (fetch layout 3 4 0 (atByte (file layout 4 [1, 2, 3]))) == 1 % 16
#eval (fetch layout 3 4 1 (atByte (file layout 4 [1, 2, 3]))) == 2 % 16
#eval (fetch layout 3 4 2 (atByte (file layout 4 [1, 2, 3]))) == 3 % 16
#eval (fetch layout 3 0 2 (atByte (file layout 0 [1, 2, 3]))) == 0
#eval (fetch layout 9 5 8 (atByte (file layout 5 [31, 0, 7, 255, 16, 3, 9, 2, 77]))) == 77 % 32

private theorem mem_lanes {b w : Nat} : b ∈ lanes w ↔ b < w := by
  simp [lanes, List.mem_range]

private theorem atByte_little (n k j : Nat) (hj : j < k) :
    atByte (little n k) j = (n / 256 ^ j) % 256 := by
  unfold atByte little
  have hj' : j < (List.range k).length := by simpa [List.length_range] using hj
  simp [List.getElem?_eq_getElem (by simp [List.length_map, List.length_range, hj] :
    j < ((List.range k).map (fun i => (n / 256 ^ i) % 256)).length)]

private theorem file_testBit (l : Layout) (w : Nat) (xs : List Nat) (j r : Nat)
    (hj : j < byteCount (l.capacity xs.length w)) (hr : r < 8) :
    (atByte (file l w xs) j).testBit r = (image l w xs).testBit (j * 8 + r) := by
  unfold file
  rw [atByte_little _ _ j hj]
  have h256 : (256 : Nat) = 2 ^ 8 := by decide
  have hp : (2 ^ 8) ^ j = 2 ^ (8 * j) := by rw [← Nat.pow_mul]
  rw [h256, hp, Nat.testBit_mod_two_pow, Nat.testBit_div_two_pow]
  simp only [hr, decide_true, Bool.true_and]
  congr 1
  omega

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
      · rw [ite_eq_left hc]
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
        rw [ite_eq_right (by simp [hc'])]
        rw [ih acc hinj_bs]
        simp [hc']

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

private theorem testBit_image_eq (l : Layout) (w : Nat) (xs : List Nat) (i b : Nat)
    (hi : i < xs.length) (hb : b < w)
    (hno : ∀ r1 r2 c1 c2, r1 < xs.length → r2 < xs.length → c1 < w → c2 < w →
      l.bit xs.length w r1 c1 = l.bit xs.length w r2 c2 → r1 = r2 ∧ c1 = c2) :
    (image l w xs).testBit (l.bit xs.length w i b) = (xs[i]?.getD 0).testBit b := by
  rw [testBit_image l w xs _ hno]
  rw [Bool.eq_iff_iff]
  constructor
  · intro h
    rw [List.any_eq_true] at h
    rcases h with ⟨⟨xv, jv⟩, hjmem, hj⟩
    rw [List.any_eq_true] at hj
    rcases hj with ⟨cv, hc, hcond⟩
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
    have hmemsome := List.mem_zipIdx_iff_getElem?.mp hjmem
    rcases List.getElem?_eq_some_iff.mp hmemsome with ⟨hjvlt, hgetval⟩
    have hc' : cv < w := by simpa [mem_lanes] using hc
    have heq := hno jv i cv b hjvlt hi hc' hb hcond.2
    rcases heq with ⟨hjv_eq, hcv_eq⟩
    have hgetD : xs[jv]?.getD 0 = xv := by
      rw [List.getElem?_eq_some_iff.mpr ⟨hjvlt, hgetval⟩]
      simp
    rw [hjv_eq] at hgetD
    rw [hcv_eq] at hcond
    simpa [hgetD] using hcond.1
  · intro h
    rw [List.any_eq_true]
    refine ⟨(xs[i]?.getD 0, i), ?_, ?_⟩
    · exact List.mem_zipIdx_iff_getElem?.mpr (List.getElem?_eq_some_iff.mpr ⟨hi, by simp [hi]⟩)
    · rw [List.any_eq_true]
      refine ⟨b, by simp [mem_lanes, hb], ?_⟩
      simp [h]

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
          simp [h0]
      have hmod2 : x % 2 ^ (w + 1) = x % 2 ^ w + 2 ^ w * (x / 2 ^ w % 2) := by
        simpa [Nat.pow_succ, Nat.mul_comm] using (Nat.mod_mul (a := 2 ^ w) (b := 2) (x := x))
      rw [ih, hmod2, hdiv]
      split <;> omega

private theorem hno : ∀ n w i j a b, i < n → j < n → a < w → b < w →
    layout.bit n w i a = layout.bit n w j b → i = j ∧ a = b := by
  intro n w i j a b hi hj ha hb h
  simp only [layout] at h
  let A := align 8 n
  change a * A + i = b * A + j at h
  have hApos : 0 < A := by
    have : n ≤ A := by
      dsimp [A, align]
      omega
    omega
  have hiA : i < A := by
    have : n ≤ A := by
      dsimp [A, align]
      omega
    omega
  have hjA : j < A := by
    have : n ≤ A := by
      dsimp [A, align]
      omega
    omega
  have hab : a = b := by
    by_cases hlt : a < b
    · have hle : (a + 1) * A ≤ b * A := Nat.mul_le_mul_right A (by omega)
      have hle' : a * A + A ≤ b * A := by simpa [Nat.add_mul] using hle
      omega
    · by_cases hgt : b < a
      · have hle : (b + 1) * A ≤ a * A := Nat.mul_le_mul_right A (by omega)
        have hle' : b * A + A ≤ a * A := by simpa [Nat.add_mul] using hle
        omega
      · omega
  rw [hab] at h ⊢
  exact ⟨by omega, rfl⟩

private theorem hbound : ∀ n w i b, i < n → b < w →
    layout.bit n w i b < 8 * byteCount (layout.capacity n w) := by
  intro n w i b hi hb
  simp only [layout, byteCount]
  let A := align 8 n
  change b * A + i < 8 * ((w * A + 7) / 8)
  have hnA : n ≤ A := by
    dsimp [A, align]
    omega
  have hiA : i < A := by omega
  have hb1 : b + 1 ≤ w := by omega
  have hbit : b * A + i < w * A := by
    have hle : (b + 1) * A ≤ w * A := Nat.mul_le_mul_right A hb1
    have hle' : b * A + A ≤ w * A := by simpa [Nat.add_mul] using hle
    omega
  have hcap : w * A ≤ 8 * ((w * A + 7) / 8) := by omega
  omega

theorem sound_Lossless : Lossless.Goal := by
  intro w xs i hi
  unfold fetch lanes
  dsimp only
  rw [← sum_testBit_eq_mod (xs[i]?.getD 0) w]
  apply congrArg List.sum
  apply List.map_congr_left
  intro b hb
  have hb' : b < w := by simpa [List.mem_range] using hb
  have hbit := hbound xs.length w i b hi hb'
  have hbyte : layout.bit xs.length w i b / 8 < byteCount (layout.capacity xs.length w) := by omega
  have hrem : layout.bit xs.length w i b % 8 < 8 := Nat.mod_lt _ (by decide)
  have hread : (atByte (file layout w xs) (layout.bit xs.length w i b / 8)).testBit
        (layout.bit xs.length w i b % 8) = (image layout w xs).testBit (layout.bit xs.length w i b) := by
    rw [file_testBit layout w xs (layout.bit xs.length w i b / 8) (layout.bit xs.length w i b % 8) hbyte hrem]
    congr 1
    have := Nat.div_add_mod (layout.bit xs.length w i b) 8
    omega
  rw [hread]
  rw [testBit_image_eq layout w xs i b hi hb' (fun r1 r2 c1 c2 hr1 hr2 hc1 hc2 heq =>
    hno xs.length w r1 r2 c1 c2 hr1 hr2 hc1 hc2 heq)]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedPlane_Lossless
