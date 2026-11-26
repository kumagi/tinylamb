import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `RotatedPlane.Transcode` — Convert from this physical order to tight row-major bytes
  using lane reads.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RotatedPlane_Transcode

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane

/-! The rotated-plane layout is a permutation of bit cells inside each plane,
so it is lossless; losslessness implies transcoding because `pack w` only
depends on each value modulo `2^w`. -/

/-! Sanity check on small inputs: every lane round-trips. -/
#guard (List.range 5).all (fun n =>
  (List.range 5).all (fun w =>
    let xs := (List.range n).map (fun i => (i * 3 + 1) % 2 ^ (w + 1))
    let bs := file layout w xs
    (List.range n).all (fun i => fetch layout n w i (atByte bs) == xs[i]?.getD 0 % 2 ^ w)))

/-! ## Generic transcode infrastructure -/

private theorem mem_lanes {b w : Nat} : b ∈ lanes w ↔ b < w := by
  simp [lanes, List.mem_range]

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

private theorem transcode_of_lossless (l : Layout) (hl : lossless l) : transcode l := by
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
  have hw : word w (xs.map (· % 2 ^ w)) = word w xs :=
    word_congr_mod w (xs.map (· % 2 ^ w)) xs (by simp)
  rw [hw]
  simp

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

/-! ## General image characterization -/

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

/-! ## Generic lossless from injectivity and bounds -/

private theorem sum_testBit_eq_mod (x w : Nat) :
    ((List.range w).map (fun b => if x.testBit b then 2 ^ b else 0)).sum = x % 2 ^ w := by
  induction w with
  | zero => simp [Nat.mod_one]
  | succ w ih =>
      rw [List.range_succ, List.map_append, List.map_cons, List.map_nil, List.sum_append, List.sum_cons,
        List.sum_nil, Nat.add_zero]
      have hpow : 2 ^ (w + 1) = 2 * 2 ^ w := by rw [Nat.pow_succ, Nat.mul_comm]
      have hdiv : x / 2 ^ w % 2 = if x.testBit w then 1 else 0 := by
        rw [Nat.testBit_eq_decide_div_mod_eq]
        by_cases h : x / 2 ^ w % 2 = 1
        · simp [h]
        · have h0 : x / 2 ^ w % 2 = 0 := by omega
          simp [h0]
      have hmod2 : x % 2 ^ (w + 1) = x % 2 ^ w + 2 ^ w * (x / 2 ^ w % 2) :=
        Nat.mod_pow_succ
      rw [ih, hmod2, hdiv]
      split <;> omega

private theorem testBit_image_eq (l : Layout) (w : Nat) (xs : List Nat) (i b : Nat)
    (hi : i < xs.length) (hb : b < w)
    (hno : ∀ i j a b, i < xs.length → j < xs.length → a < w → b < w →
      l.bit xs.length w i a = l.bit xs.length w j b → i = j ∧ a = b) :
    (image l w xs).testBit (l.bit xs.length w i b) = (xs[i]?.getD 0).testBit b := by
  rw [testBit_image l w xs _ hno]
  have htrue : xs.zipIdx.any (fun (x,j) => (lanes w).any (fun c => x.testBit c && decide (l.bit xs.length w j c = l.bit xs.length w i b))) = (xs[i]?.getD 0).testBit b := by
    rw [Bool.eq_iff_iff]
    constructor
    · intro h
      rw [List.any_eq_true] at h
      rcases h with ⟨⟨x, j⟩, hjmem, hj⟩
      rw [List.any_eq_true] at hj
      rcases hj with ⟨c, hc, hcond⟩
      simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
      have hjl : j < xs.length := by
        have := List.mem_zipIdx_iff_getElem?.mp hjmem
        exact List.getElem?_eq_some_iff.mp this |>.1
      have hc' : c < w := by simpa [mem_lanes] using hc
      have heq := hno j i c b hjl hi hc' hb hcond.2
      have hji : j = i := heq.1
      have hcb : c = b := heq.2
      subst j; subst c
      have hx : x = xs[i]?.getD 0 := by
        have hsome := List.mem_zipIdx_iff_getElem?.mp hjmem
        rw [hsome]
        simp
      rw [← hx]
      exact hcond.1
    · intro h
      rw [List.any_eq_true]
      refine ⟨(xs[i]?.getD 0, i), ?_, ?_⟩
      · rw [List.mem_zipIdx_iff_getElem?]
        simp [hi]
      · rw [List.any_eq_true]
        exact ⟨b, by simp [mem_lanes, hb], by simp [h]⟩
  rw [htrue]

private theorem lossless_of_nonoverlap (l : Layout)
    (hno : ∀ n w i j a b, i < n → j < n → a < w → b < w →
      l.bit n w i a = l.bit n w j b → i = j ∧ a = b)
    (hbound : ∀ n w i b, i < n → b < w → l.bit n w i b < 8 * byteCount (l.capacity n w)) :
    lossless l := by
  intro w xs i hi
  have hfetch : fetch l xs.length w i (atByte (file l w xs)) =
      ((List.range w).map (fun b => if (xs[i]?.getD 0).testBit b then 2 ^ b else 0)).sum := by
    unfold fetch lanes
    congr 1
    apply List.map_congr_left
    intro b hb
    have hb' : b < w := by simpa [List.mem_range] using hb
    have hbit := hbound xs.length w i b hi hb'
    have hbyte : l.bit xs.length w i b / 8 < byteCount (l.capacity xs.length w) := by omega
    have hrem : l.bit xs.length w i b % 8 < 8 := Nat.mod_lt _ (by decide)
    have hread : (atByte (file l w xs) (l.bit xs.length w i b / 8)).testBit (l.bit xs.length w i b % 8) =
        (image l w xs).testBit (l.bit xs.length w i b) := by
      have hf := file_testBit l w xs (l.bit xs.length w i b / 8) (l.bit xs.length w i b % 8) hbyte hrem
      rw [hf]
      congr 1
      have := Nat.div_add_mod (l.bit xs.length w i b) 8
      omega
    change (if (atByte (file l w xs) (l.bit xs.length w i b / 8)).testBit (l.bit xs.length w i b % 8)
      then 2 ^ b else 0) = (if (xs[i]?.getD 0).testBit b then 2 ^ b else 0)
    rw [hread, testBit_image_eq l w xs i b hi hb' (hno xs.length w)]
  rw [hfetch, sum_testBit_eq_mod]

/-! ## Rotated plane: `(i+a) % n` cancels on the left -/

private theorem mod_add_right_cancel {n i j a : Nat} (hn : 0 < n) (hi : i < n) (hj : j < n)
    (h : (i + a) % n = (j + a) % n) : i = j := by
  have hmod_i : (i + a) % n = (i + a % n) % n := by
    rw [Nat.add_mod]
    have : i % n = i := Nat.mod_eq_of_lt hi
    rw [this]
  have hmod_j : (j + a) % n = (j + a % n) % n := by
    rw [Nat.add_mod]
    have : j % n = j := Nat.mod_eq_of_lt hj
    rw [this]
  rw [hmod_i, hmod_j] at h
  have hr : a % n < n := Nat.mod_lt a hn
  have hi2 : i + a % n < 2 * n := by omega
  have hj2 : j + a % n < 2 * n := by omega
  by_cases his : i + a % n < n
  · have hival : (i + a % n) % n = i + a % n := Nat.mod_eq_of_lt his
    by_cases hjs : j + a % n < n
    · have hjval : (j + a % n) % n = j + a % n := Nat.mod_eq_of_lt hjs
      rw [hival, hjval] at h
      omega
    · have hjge : n ≤ j + a % n := by omega
      have hjval : (j + a % n) % n = (j + a % n) - n := by
        rw [Nat.mod_eq_sub_mod hjge]
        exact Nat.mod_eq_of_lt (by omega)
      rw [hival, hjval] at h
      omega
  · have hige : n ≤ i + a % n := by omega
    have hival : (i + a % n) % n = (i + a % n) - n := by
      rw [Nat.mod_eq_sub_mod hige]
      exact Nat.mod_eq_of_lt (by omega)
    by_cases hjs : j + a % n < n
    · have hjval : (j + a % n) % n = j + a % n := Nat.mod_eq_of_lt hjs
      rw [hival, hjval] at h
      omega
    · have hjge : n ≤ j + a % n := by omega
      have hjval : (j + a % n) % n = (j + a % n) - n := by
        rw [Nat.mod_eq_sub_mod hjge]
        exact Nat.mod_eq_of_lt (by omega)
      rw [hival, hjval] at h
      omega

private theorem rno : ∀ n w i j a b, i < n → j < n → a < w → b < w →
    layout.bit n w i a = layout.bit n w j b → i = j ∧ a = b := by
  intro n w i j a b hi hj ha hb h
  simp only [layout] at h
  have hn : 0 < n := by omega
  have hia : (i + a) % n < n := Nat.mod_lt _ hn
  have hjb : (j + b) % n < n := Nat.mod_lt _ hn
  have hab : a = b := by
    by_cases hlt : a < b
    · have hle0 : a + 1 ≤ b := by omega
      have hle' : a * n + n ≤ b * n := by
        have := Nat.mul_le_mul_right n hle0
        simpa [Nat.add_mul, Nat.one_mul] using this
      have hlt' : a * n + (i + a) % n < b * n + (j + b) % n := by omega
      omega
    · by_cases hgt : b < a
      · have hle0 : b + 1 ≤ a := by omega
        have hle' : b * n + n ≤ a * n := by
          have := Nat.mul_le_mul_right n hle0
          simpa [Nat.add_mul, Nat.one_mul] using this
        have hlt' : b * n + (j + b) % n < a * n + (i + a) % n := by omega
        omega
      · omega
  subst b
  have hmod : (i + a) % n = (j + a) % n := by omega
  have hij := mod_add_right_cancel hn hi hj hmod
  exact ⟨hij, rfl⟩

private theorem rbound : ∀ n w i b, i < n → b < w →
    layout.bit n w i b < 8 * byteCount (layout.capacity n w) := by
  intro n w i b hi hb
  simp only [layout, byteCount]
  have hn : 0 < n := by omega
  have hmod : (i + b) % n < n := Nat.mod_lt _ hn
  have hle0 : b + 1 ≤ w := by omega
  have hle' : b * n + n ≤ w * n := by
    have := Nat.mul_le_mul_right n hle0
    simpa [Nat.add_mul, Nat.one_mul] using this
  have hcomm : w * n = n * w := Nat.mul_comm w n
  have hlt : b * n + (i + b) % n < n * w := by omega
  have hceil : n * w ≤ 8 * ((n * w + 7) / 8) := by omega
  omega

theorem sound_Transcode : RotatedPlane.Transcode.Goal := by
  exact transcode_of_lossless layout (lossless_of_nonoverlap layout rno rbound)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RotatedPlane_Transcode
