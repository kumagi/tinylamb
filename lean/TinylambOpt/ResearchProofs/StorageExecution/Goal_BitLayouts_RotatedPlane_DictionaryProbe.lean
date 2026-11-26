import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `RotatedPlane.DictionaryProbe` — Read only lanes requested by a probe and translate
  them through a possibly short dictionary.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RotatedPlane_DictionaryProbe

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane

/-- ## Generic infrastructure: a dictionary probe follows from lossless reads -/

private theorem dictionaryProbe_of_lossless (l : Layout) (hl : lossless l) : dictionaryProbe l := by
  intro w xs dict keys hkeys
  apply List.map_congr_left
  intro i hi
  rw [hl w xs i (hkeys i hi)]

private theorem mem_lanes {b w : Nat} : b ∈ lanes w ↔ b < w := by
  simp [lanes, List.mem_range]

private theorem atByte_little (n k j : Nat) (hj : j < k) :
    atByte (little n k) j = (n / 256 ^ j) % 256 := by
  unfold atByte little
  simp [hj]

private theorem file_testBit (l : Layout) (w : Nat) (xs : List Nat) (j r : Nat)
    (hj : j < byteCount (l.capacity xs.length w)) (hr : r < 8) :
    (atByte (file l w xs) j).testBit r = (image l w xs).testBit (j * 8 + r) := by
  unfold file
  rw [atByte_little]
  · have h256 : (256 : Nat) = 2 ^ 8 := by decide
    have hp : (2 ^ 8) ^ j = 2 ^ (8 * j) := by rw [← Nat.pow_mul]
    rw [h256, hp, Nat.testBit_mod_two_pow, Nat.testBit_div_two_pow]
    simp [hr]
    congr 1
    omega
  · exact hj

private theorem testBit_foldl_row (f : Nat → Nat) (cond : Nat → Bool) (bs : List Nat)
    (acc : Nat) (p : Nat) (hinj : ∀ a ∈ bs, ∀ b ∈ bs, f a = f b → a = b) :
    ((bs.foldl (fun a b => if cond b then a ||| 2 ^ f b else a) acc).testBit p) =
      (acc.testBit p || (bs.any (fun b => cond b && decide (f b = p)))) := by
  induction bs generalizing acc with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons, List.any_cons]
      have hinj_bs : ∀ a ∈ bs, ∀ c ∈ bs, f a = f c → a = c :=
        fun a ha c hc => hinj a (by simp [ha]) c (by simp [hc])
      by_cases hc : cond b = true
      · rw [ite_eq_left hc]
        rw [ih (acc ||| 2 ^ f b) hinj_bs]
        simp only [Nat.testBit_or, Nat.testBit_two_pow, hc, Bool.true_and, Bool.or_assoc]
      · have hc' : cond b = false := Bool.eq_false_iff.mpr hc
        simp [hc', ih acc hinj_bs]

private theorem testBit_image_fold (l : Layout) (w : Nat) (xs : List Nat) (p : Nat)
    (lxs : List (Nat × Nat)) (acc : Nat)
    (hmem : ∀ x i, (x, i) ∈ lxs → i < xs.length)
    (hinj : ∀ i j a b, (∃ x, (x, i) ∈ lxs) → j < xs.length → a < w → b < w →
      l.bit xs.length w i a = l.bit xs.length w j b → i = j ∧ a = b) :
    ((lxs.foldl (fun acc (x, i) => (lanes w).foldl
      (fun a b => if x.testBit b then a ||| 2 ^ (l.bit xs.length w i b) else a) acc) acc).testBit p) =
      (acc.testBit p || lxs.any (fun (x, i) =>
        (lanes w).any (fun b => x.testBit b && decide (l.bit xs.length w i b = p)))) := by
  induction lxs generalizing acc with
  | nil => simp
  | cons xi rest ih =>
      obtain ⟨x, i⟩ := xi
      simp only [List.foldl_cons, List.any_cons]
      have hi : i < xs.length := hmem x i (by simp)
      have hmem_rest : ∀ x' i', (x', i') ∈ rest → i' < xs.length :=
        fun x' i' hm => hmem x' i' (by simp [hm])
      have hinj_rest : ∀ i' j a b, (∃ x, (x, i') ∈ rest) → j < xs.length → a < w → b < w →
          l.bit xs.length w i' a = l.bit xs.length w j b → i' = j ∧ a = b :=
        fun i' j a b hex hj ha hb heq => by
          obtain ⟨x', hx'⟩ := hex
          exact hinj i' j a b ⟨x', by simp [hx']⟩ hj ha hb heq
      rw [ih _ hmem_rest hinj_rest]
      rw [testBit_foldl_row (fun b => l.bit xs.length w i b) (fun b => x.testBit b)
        (lanes w) acc p]
      · simp [Bool.or_assoc]
      · intro a ha b hb heq
        have ha' : a < w := (mem_lanes.mp ha)
        have hb' : b < w := (mem_lanes.mp hb)
        exact (hinj i i a b ⟨x, by simp⟩ hi ha' hb' heq).2

private theorem testBit_image (l : Layout) (w : Nat) (xs : List Nat) (p : Nat)
    (hno : ∀ i j a b, i < xs.length → j < xs.length → a < w → b < w →
      l.bit xs.length w i a = l.bit xs.length w j b → i = j ∧ a = b) :
    (image l w xs).testBit p =
      (xs.zipIdx.any (fun (x, i) => (lanes w).any
        (fun b => x.testBit b && decide (l.bit xs.length w i b = p)))) := by
  unfold image
  have hmem : ∀ x i, (x, i) ∈ xs.zipIdx → i < xs.length := by
    intro x i h
    have hlt := List.snd_lt_of_mem_zipIdx h
    simpa [List.length_zipIdx] using hlt
  have hinj : ∀ i j a b, (∃ x, (x, i) ∈ xs.zipIdx) → j < xs.length → a < w → b < w →
      l.bit xs.length w i a = l.bit xs.length w j b → i = j ∧ a = b := by
    intro i j a b hex hj ha hb heq
    obtain ⟨x, hx⟩ := hex
    exact hno i j a b (hmem x i hx) hj ha hb heq
  rw [testBit_image_fold l w xs p xs.zipIdx 0 hmem hinj]
  simp

private theorem testBit_image_eq (l : Layout) (w : Nat) (xs : List Nat) (i b : Nat)
    (hi : i < xs.length) (hb : b < w)
    (hno : ∀ i j a b, i < xs.length → j < xs.length → a < w → b < w →
      l.bit xs.length w i a = l.bit xs.length w j b → i = j ∧ a = b) :
    (image l w xs).testBit (l.bit xs.length w i b) = (xs[i]?.getD 0).testBit b := by
  rw [testBit_image l w xs (l.bit xs.length w i b) hno]
  rw [Bool.eq_iff_iff]
  constructor
  · intro h
    rw [List.any_eq_true] at h
    rcases h with ⟨⟨x, j⟩, hjmem, hj⟩
    rw [List.any_eq_true] at hj
    rcases hj with ⟨c, hc, hcond⟩
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
    have hsome : xs[j]? = some x := (List.mem_zipIdx_iff_getElem?).mp hjmem
    rcases (List.getElem?_eq_some_iff.mp hsome) with ⟨hjl, hval⟩
    have hc' : c < w := mem_lanes.mp hc
    have heq := hno j i c b hjl hi hc' hb hcond.2
    have hji : j = i := heq.1
    have hcb : c = b := heq.2
    have hsome' : xs[i]? = some x := by
      rw [← hji]
      exact hsome
    have hx : x = xs[i]?.getD 0 := by simp [hsome']
    rw [← hx, ← hcb]
    exact hcond.1
  · intro h
    rw [List.any_eq_true]
    refine ⟨(xs[i]?.getD 0, i), ?_, ?_⟩
    · rw [List.mem_zipIdx_iff_getElem?]
      simp [hi]
    · rw [List.any_eq_true]
      exact ⟨b, mem_lanes.mpr hb, by simp [h]⟩

private theorem mod_two_mul (x p : Nat) (hp : 0 < p) :
    x % (2 * p) = x % p + p * (x / p % 2) := by
  have hq : x / p = 2 * (x / p / 2) + x / p % 2 := (Nat.div_add_mod (x / p) 2).symm
  have hmod : x % p < p := Nat.mod_lt x hp
  have hr : x / p % 2 = 0 ∨ x / p % 2 = 1 := by omega
  rcases hr with h0 | h1
  · rw [h0]
    simp
    calc
      x % (2 * p) = (p * (x / p) + x % p) % (2 * p) := by rw [Nat.div_add_mod]
      _ = (p * (2 * (x / p / 2)) + x % p) % (2 * p) := by
        have hq0 : x / p = 2 * (x / p / 2) := by
          have ht := hq
          rw [h0, Nat.add_zero] at ht
          exact ht
        exact congrArg (fun t => (p * t + x % p) % (2 * p)) hq0
      _ = (x % p + (2 * p) * (x / p / 2)) % (2 * p) := by
        congr 1
        simp [Nat.mul_add, Nat.add_mul, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
        omega
      _ = x % p := by
        rw [Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt]
        omega
  · rw [h1]
    simp
    calc
      x % (2 * p) = (p * (x / p) + x % p) % (2 * p) := by rw [Nat.div_add_mod]
      _ = (p * (2 * (x / p / 2) + 1) + x % p) % (2 * p) := by
        have hq1 : x / p = 2 * (x / p / 2) + 1 := by
          have ht := hq
          rw [h1] at ht
          exact ht
        exact congrArg (fun t => (p * t + x % p) % (2 * p)) hq1
      _ = ((p + x % p) + (2 * p) * (x / p / 2)) % (2 * p) := by
        congr 1
        simp [Nat.mul_add, Nat.add_mul, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
        omega
      _ = p + x % p := by
        rw [Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt]
        omega
      _ = x % p + p := by omega

private theorem mod_two_pow_succ (x w : Nat) :
    x % 2 ^ (w + 1) = x % 2 ^ w + 2 ^ w * (x / 2 ^ w % 2) := by
  have hp : 0 < 2 ^ w := Nat.pow_pos (by decide : (0 : Nat) < 2)
  simpa [Nat.pow_succ, Nat.mul_comm] using (mod_two_mul x (2 ^ w) hp)

private theorem sum_testBit_eq_mod (x w : Nat) :
    ((List.range w).map (fun b => if x.testBit b then 2 ^ b else 0)).sum = x % 2 ^ w := by
  induction w with
  | zero => simp [Nat.mod_one]
  | succ w ih =>
      rw [List.range_succ, List.map_append, List.map_cons, List.map_nil, List.sum_append,
        List.sum_cons, List.sum_nil, Nat.add_zero, ih]
      have hdiv : x / 2 ^ w % 2 = if x.testBit w then 1 else 0 := by
        rw [Nat.testBit_eq_decide_div_mod_eq]
        by_cases h : x / 2 ^ w % 2 = 1
        · simp [h]
        · have h0 : x / 2 ^ w % 2 = 0 := by omega
          simp [h0, h]
      rw [mod_two_pow_succ, hdiv]
      by_cases h : x.testBit w <;> simp [h]

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
    have hread : (atByte (file l w xs) (l.bit xs.length w i b / 8)).testBit
          (l.bit xs.length w i b % 8) = (image l w xs).testBit (l.bit xs.length w i b) := by
      rw [file_testBit l w xs (l.bit xs.length w i b / 8) (l.bit xs.length w i b % 8) hbyte hrem]
      congr 1
      have := Nat.div_add_mod (l.bit xs.length w i b) 8
      omega
    change (if (atByte (file l w xs) (l.bit xs.length w i b / 8)).testBit
          (l.bit xs.length w i b % 8) then 2 ^ b else 0) =
      (if (xs[i]?.getD 0).testBit b then 2 ^ b else 0)
    rw [hread]
    rw [testBit_image_eq l w xs i b hi hb' (fun i j a b hi hj ha hb => hno xs.length w i j a b hi hj ha hb)]
  rw [hfetch, sum_testBit_eq_mod]

/-- ## RotatedPlane: the rotation `i ↦ (i + a) % n` is injective below `n` -/

private theorem rotated_mod_injective {n i j a : Nat} (hi : i < n) (hj : j < n)
    (h : (i + a) % n = (j + a) % n) : i = j := by
  have hn : 0 < n := by omega
  have h1 : (i + a) % n = (i + a % n) % n := by
    rw [Nat.add_mod, Nat.mod_eq_of_lt hi]
  have h2 : (j + a) % n = (j + a % n) % n := by
    rw [Nat.add_mod, Nat.mod_eq_of_lt hj]
  rw [h1, h2] at h
  let c := a % n
  have hc : c < n := Nat.mod_lt _ hn
  by_cases hilt : i + c < n
  · have hi_mod : (i + c) % n = i + c := Nat.mod_eq_of_lt hilt
    by_cases hjlt : j + c < n
    · have hj_mod : (j + c) % n = j + c := Nat.mod_eq_of_lt hjlt
      rw [hi_mod, hj_mod] at h
      omega
    · have hjge : j + c ≥ n := by omega
      have hj_mod := Nat.mod_eq_sub_mod hjge
      have hjlt2 : j + c - n < n := by omega
      rw [hi_mod, hj_mod, Nat.mod_eq_of_lt hjlt2] at h
      omega
  · have hige : i + c ≥ n := by omega
    have hi_mod := Nat.mod_eq_sub_mod hige
    have hilt2 : i + c - n < n := by omega
    rw [hi_mod, Nat.mod_eq_of_lt hilt2] at h
    by_cases hjlt : j + c < n
    · have hj_mod : (j + c) % n = j + c := Nat.mod_eq_of_lt hjlt
      rw [hj_mod] at h
      omega
    · have hjge : j + c ≥ n := by omega
      have hj_mod := Nat.mod_eq_sub_mod hjge
      have hjlt2 : j + c - n < n := by omega
      rw [hj_mod, Nat.mod_eq_of_lt hjlt2] at h
      omega

private theorem rotated_nonoverlap : ∀ n w i j a b, i < n → j < n → a < w → b < w →
    layout.bit n w i a = layout.bit n w j b → i = j ∧ a = b := by
  intro n w i j a b hi hj ha hb h
  simp only [layout] at h
  have hn : 0 < n := by omega
  have hri : (i + a) % n < n := Nat.mod_lt _ hn
  have hrj : (j + b) % n < n := Nat.mod_lt _ hn
  have hdivL : (a * n + (i + a) % n) / n = a := by
    rw [Nat.mul_comm a n, Nat.mul_add_div hn, Nat.div_eq_of_lt hri, Nat.add_zero]
  have hdivR : (b * n + (j + b) % n) / n = b := by
    rw [Nat.mul_comm b n, Nat.mul_add_div hn, Nat.div_eq_of_lt hrj, Nat.add_zero]
  have hab : a = b := by
    have hcong := congrArg (fun x => x / n) h
    rw [hdivL, hdivR] at hcong
    exact hcong
  subst b
  have hmodEq : (i + a) % n = (j + a) % n := by omega
  exact ⟨rotated_mod_injective hi hj hmodEq, rfl⟩

private theorem rotated_bound : ∀ n w i b, i < n → b < w →
    layout.bit n w i b < 8 * byteCount (layout.capacity n w) := by
  intro n w i b hi hb
  have hn : 0 < n := by omega
  have hr : (i + b) % n < n := Nat.mod_lt _ hn
  have hmul : (b + 1) * n ≤ w * n := Nat.mul_le_mul_right n (by omega)
  have hceil : n * w ≤ 8 * byteCount (n * w) := by
    unfold byteCount
    omega
  simp only [layout, Nat.succ_mul] at hmul ⊢
  rw [Nat.mul_comm w n] at hmul
  omega

private theorem rotated_lossless : lossless layout :=
  lossless_of_nonoverlap layout rotated_nonoverlap rotated_bound

theorem sound_DictionaryProbe : DictionaryProbe.Goal :=
  dictionaryProbe_of_lossless layout rotated_lossless

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RotatedPlane_DictionaryProbe
