import TinylambOpt.ResearchGoals.Compression.BitPatterns

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.BitPatterns

open TinylambOpt.ResearchGoals.Compression.BitPatterns
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Proofs and counterexamples for the binary64 bit-pattern compression goals.
The helpers below reconstruct a value from its bit planes (respectively its
little-endian bytes) and locate the lowest set bit of a raw bit image. -/

/-- The first set bit of a nonzero value below `2^64`, and the bits below it. -/
private theorem tz_spec (x : Nat) (hxpos : x ≠ 0) (hx : x < 2 ^ 64) :
    tz x < 64 ∧ x.testBit (tz x) = true ∧ ∀ j, j < tz x → x.testBit j = false := by
  have hex : ∃ j, j < 64 ∧ x.testBit j = true := by
    by_cases h : ∃ j, j < 64 ∧ x.testBit j = true
    · exact h
    · exfalso
      apply hxpos
      apply Nat.eq_of_testBit_eq
      intro j
      by_cases hj : j < 64
      · by_cases hb : x.testBit j = true
        · exact False.elim (h ⟨j, hj, hb⟩)
        · simpa using Bool.eq_false_of_ne_true hb
      · have hf : x.testBit j = false := Nat.testBit_lt_two_pow
          (Nat.lt_of_lt_of_le hx (Nat.pow_le_pow_right (by decide) (by omega)))
        simpa using hf
  obtain ⟨j, hj, hbit⟩ := hex
  have hlt : tz x < 64 := by
    unfold tz
    exact List.findIdx_lt_length_of_exists ⟨j, List.mem_range.mpr hj, hbit⟩
  have hbitt : x.testBit (tz x) = true := by
    unfold tz
    have := @List.findIdx_getElem Nat (fun i => x.testBit i) (List.range 64)
      (by simpa [tz] using hlt)
    simpa using this
  have hlow : ∀ i, i < tz x → x.testBit i = false := by
    intro i hi
    unfold tz at hi
    have hi' : i < (List.range 64).findIdx (fun i => x.testBit i) := hi
    have := List.not_of_lt_findIdx (xs := List.range 64) (p := fun i => x.testBit i) hi'
    simpa using this
  exact ⟨hlt, hbitt, hlow⟩

/-- Appending bit `k` of `x` to the low `k` bits gives the low `k+1` bits. -/
private theorem mod_two_pow_succ (x k : Nat) :
    x % 2 ^ k + (if x.testBit k then 2 ^ k else 0) = x % 2 ^ (k + 1) := by
  let q := x / 2 ^ k
  let r := x % 2 ^ k
  have hr : r < 2 ^ k := by exact Nat.mod_lt _ (Nat.pow_pos (by decide))
  have hx : x = 2 ^ k * q + r := by
    dsimp [q, r]
    exact (Nat.div_add_mod x (2 ^ k)).symm
  have hq : q = 2 * (q / 2) + q % 2 := (Nat.div_add_mod q 2).symm
  have ht : x.testBit k = decide (q % 2 = 1) := by
    simpa [q] using (Nat.testBit_eq_decide_div_mod_eq (x := x) (i := k))
  have halg (m : Nat) :
      2 ^ k * (2 * m + q % 2) + r = m * (2 ^ k * 2) + (2 ^ k * (q % 2) + r) := by
    calc
      2 ^ k * (2 * m + q % 2) + r = (2 ^ k * 2) * m + 2 ^ k * (q % 2) + r := by
        rw [Nat.mul_add, ← Nat.mul_assoc]
      _ = m * (2 ^ k * 2) + (2 ^ k * (q % 2) + r) := by ac_rfl
  have hx' : x = (q / 2) * (2 ^ k * 2) + (2 ^ k * (q % 2) + r) := by
    calc
      x = 2 ^ k * q + r := hx
      _ = 2 ^ k * (2 * (q / 2) + q % 2) + r := congrArg (fun t => 2 ^ k * t + r) hq
      _ = (q / 2) * (2 ^ k * 2) + (2 ^ k * (q % 2) + r) := halg (q / 2)
  have hsmall : 2 ^ k * (q % 2) + r < 2 ^ k * 2 := by
    have hpos : 0 < 2 ^ k := Nat.pow_pos (by decide)
    rcases Nat.mod_two_eq_zero_or_one q with h0 | h1
    · rw [h0]
      simp
      omega
    · rw [h1]
      simp
      omega
  have hxmod : x % 2 ^ (k + 1) = 2 ^ k * (q % 2) + r := by
    rw [hx', Nat.pow_succ, Nat.mul_add_mod_of_lt hsmall]
  rw [hxmod]
  rcases Nat.mod_two_eq_zero_or_one q with h0 | h1
  · simp [ht, h0, r]
  · simp [ht, h1]
    omega

private theorem sum_bits_mod : ∀ k x : Nat,
    ((List.range k).map (fun p => if x.testBit p then 2 ^ p else 0)).sum = x % 2 ^ k
  | 0, x => by simp [Nat.mod_one]
  | k + 1, x => by
      rw [List.range_succ, List.map_append, List.sum_append, sum_bits_mod k x]
      exact mod_two_pow_succ x k

private theorem foldl_eq_add_sum (ps : List Nat) (f : Nat → Bool) (a : Nat) :
    ps.foldl (fun acc p => if f p then acc + 2 ^ p else acc) a =
      a + (ps.map (fun p => if f p then 2 ^ p else 0)).sum := by
  induction ps generalizing a with
  | nil => simp
  | cons p ps ih =>
      rw [List.foldl_cons, ih]
      cases h : f p <;> simp [h, Nat.add_assoc] <;> omega

/-- A `rebuild` over all 64 bit planes returns the value whose bits are tested. -/
private theorem rebuild_eq_of_test (row : Nat) (x : Nat) (hx : x < 2 ^ 64) (test : Nat → Nat → Bool)
    (htest : ∀ p, test p row = x.testBit p) :
    rebuild row test = x := by
  unfold rebuild
  rw [foldl_eq_add_sum]
  have hsum : ((List.range 64).map (fun p => if x.testBit p then 2 ^ p else 0)).sum = x := by
    rw [sum_bits_mod, Nat.mod_eq_of_lt hx]
  simp only [htest]
  rw [hsum, Nat.zero_add]

private theorem mod_256_succ (x n : Nat) :
    x % 256 + 256 * ((x / 256) % 256 ^ n) = x % 256 ^ (n + 1) := by
  let q := x / 256
  let r := x % 256
  have hr : r < 256 := Nat.mod_lt _ (by decide)
  have hx : x = 256 * q + r := by
    dsimp [q, r]
    exact (Nat.div_add_mod x 256).symm
  have hq : q = 256 ^ n * (q / 256 ^ n) + q % 256 ^ n := (Nat.div_add_mod q (256 ^ n)).symm
  have hqn : q % 256 ^ n < 256 ^ n := Nat.mod_lt _ (Nat.pow_pos (by decide))
  have hlt : 256 * (q % 256 ^ n) + r < 256 * 256 ^ n := by omega
  have hx' : x = (q / 256 ^ n) * (256 * 256 ^ n) + (256 * (q % 256 ^ n) + r) := by
    calc
      x = 256 * q + r := hx
      _ = 256 * (256 ^ n * (q / 256 ^ n) + q % 256 ^ n) + r :=
        congrArg (fun t => 256 * t + r) hq
      _ = (q / 256 ^ n) * (256 * 256 ^ n) + (256 * (q % 256 ^ n) + r) := by
        rw [Nat.mul_add]
        ac_rfl
  have hxmod : x % 256 ^ (n + 1) = 256 * (q % 256 ^ n) + r := by
    rw [hx', Nat.pow_succ, Nat.mul_comm (256 ^ n) 256, Nat.mul_add_mod_of_lt hlt]
  rw [hxmod]
  dsimp [q, r]
  omega

private theorem fromLittle_digits : ∀ n x : Nat,
    fromLittle ((List.range n).map (fun j => (x / 256 ^ j) % 256)) = x % 256 ^ n
  | 0, x => by simp [Nat.mod_one, fromLittle]
  | n + 1, x => by
      rw [List.range_succ_eq_map, List.map_cons, List.map_map]
      change (x / 256 ^ 0 % 256) + 256 *
        fromLittle ((List.range n).map (fun j => (x / 256 ^ (j + 1)) % 256)) = x % 256 ^ (n + 1)
      have hmap : (List.range n).map (fun j => (x / 256 ^ (j + 1)) % 256) =
          (List.range n).map (fun j => ((x / 256) / 256 ^ j) % 256) := by
        apply List.map_congr_left
        intro j hj
        congr 1
        rw [Nat.pow_succ', ← Nat.div_div_eq_div_mul]
      rw [hmap, fromLittle_digits n (x / 256)]
      simpa using mod_256_succ x n

private theorem flatMap_map_succ (f : Nat → List Nat) : ∀ l : List Nat,
    (l.map Nat.succ).flatMap f = l.flatMap (fun j => f (j + 1))
  | [] => rfl
  | j :: js => by simp [flatMap_map_succ f js]

/-- A byte lane in the byte-shuffled stream is the requested byte of a row. -/
private theorem flatMap_range_getElem (f : Nat → List Nat) (m : Nat)
    (hf : ∀ j, (f j).length = m) : ∀ n j i : Nat, j < n → i < m →
    ((List.range n).flatMap f)[j * m + i]? = (f j)[i]?
  | 0, j, i, hj, hi => by simp at hj
  | n + 1, j, i, hj, hi => by
      rw [List.range_succ_eq_map, List.flatMap_cons, flatMap_map_succ]
      cases j with
      | zero =>
          have hi0 : i < (f 0).length := by simpa [hf] using hi
          rw [Nat.zero_mul, Nat.zero_add, List.getElem?_append_left hi0]
      | succ j' =>
          have hjn : j' < n := by omega
          have hge0 : m ≤ (j' + 1) * m + i := by
            rw [Nat.succ_mul]
            omega
          have hge : (f 0).length ≤ (j' + 1) * m + i := by
            rw [hf 0]
            exact hge0
          rw [List.getElem?_append_right hge]
          rw [hf 0]
          have hsub : (j' + 1) * m + i - m = j' * m + i := by
            rw [Nat.succ_mul]
            omega
          rw [hsub]
          exact flatMap_range_getElem (fun j => f (j + 1)) m (fun j => hf (j + 1)) n j' i hjn hi

/-- XOR is its own inverse, so the difference chain is reversible. -/
theorem XorChain_sound : TinylambOpt.ResearchGoals.Compression.BitPatterns.XorChain.Goal := by
  intro prev xs
  induction xs generalizing prev with
  | nil => rfl
  | cons x xs ih =>
      change (prev ^^^ (prev ^^^ x)) :: undo (prev ^^^ (prev ^^^ x)) (delta x xs) = x :: xs
      have hx : prev ^^^ (prev ^^^ x) = x := by
        rw [← Nat.xor_assoc, Nat.xor_self, Nat.zero_xor]
      rw [hx]
      simp [ih]

/-- The significant window drops only leading and trailing zero bits. -/
theorem SignificantWindow_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.SignificantWindow.Goal := by
  intro x hx
  by_cases hzero : x = 0
  · subst x
    simp [tz, activeBits, significant, width, List.findIdx_eq_length_of_false]
  · let t := tz x
    have hs := tz_spec x hzero hx
    have htlt : t < 64 := by simpa [t] using hs.1
    have hbitt : x.testBit t = true := by simpa [t] using hs.2.1
    have hlow : ∀ j, j < t → x.testBit j = false := by simpa [t] using hs.2.2
    have hmod0 : x % 2 ^ t = 0 := by
      apply Nat.eq_of_testBit_eq
      intro i
      rw [Nat.testBit_mod_two_pow]
      by_cases hi : i < t
      · have hi0 : x.testBit i = false := hlow i hi
        simp [hi, hi0]
      · simp [hi]
    have hdvd : 2 ^ t ∣ x := Nat.dvd_of_mod_eq_zero hmod0
    have hxdecomp : (x / 2 ^ t) * 2 ^ t = x := Nat.div_mul_cancel hdvd
    have hxltm : x < 2 ^ (x.log2 + 1) := Nat.lt_log2_self
    have hq : x / 2 ^ t < 2 ^ (x.log2 + 1 - t) := by
      apply Nat.lt_pow_two_of_testBit
      intro i hi
      have hji : x.log2 + 1 ≤ i + t := by omega
      have hxlt : x < 2 ^ (i + t) :=
        Nat.lt_of_lt_of_le hxltm (Nat.pow_le_pow_right (by decide) hji)
      have hf : x.testBit (i + t) = false := Nat.testBit_lt_two_pow hxlt
      simpa [Nat.testBit_div_two_pow] using hf
    have hwidth : width x = x.log2 + 1 := by simp [width, hzero]
    dsimp only [significant, activeBits]
    change (x / 2 ^ t) % 2 ^ (width x - t) * 2 ^ t = x
    rw [hwidth]
    rw [Nat.mod_eq_of_lt hq]
    exact hxdecomp

/-- Equal high bits cancel in the XOR; the result fits in the low window. -/
theorem CommonHighBits_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.CommonHighBits.Goal := by
  intro a b k hab
  let c := a / 2 ^ k
  let r1 := a % 2 ^ k
  let r2 := b % 2 ^ k
  have hr1 : r1 < 2 ^ k := by exact Nat.mod_lt _ (Nat.pow_pos (by decide))
  have hr2 : r2 < 2 ^ k := by exact Nat.mod_lt _ (Nat.pow_pos (by decide))
  have ha : a = 2 ^ k * c + r1 := by
    dsimp [c, r1]
    exact (Nat.div_add_mod a (2 ^ k)).symm
  have hb : b = 2 ^ k * c + r2 := by
    calc
      b = 2 ^ k * (b / 2 ^ k) + b % 2 ^ k := (Nat.div_add_mod b (2 ^ k)).symm
      _ = 2 ^ k * (a / 2 ^ k) + b % 2 ^ k := by rw [hab]
      _ = 2 ^ k * c + r2 := by rfl
  have hxor : a ^^^ b = r1 ^^^ r2 := by
    apply Nat.eq_of_testBit_eq
    intro i
    rw [ha, hb, Nat.testBit_xor, Nat.testBit_xor]
    rw [Nat.testBit_two_pow_mul_add c hr1 i, Nat.testBit_two_pow_mul_add c hr2 i]
    by_cases hi : i < k
    · simp [hi]
    · have hik : k ≤ i := Nat.le_of_not_gt hi
      have hp : 2 ^ k ≤ 2 ^ i := Nat.pow_le_pow_right (by decide) hik
      have h1 : r1.testBit i = false := Nat.testBit_lt_two_pow (Nat.lt_of_lt_of_le hr1 hp)
      have h2 : r2.testBit i = false := Nat.testBit_lt_two_pow (Nat.lt_of_lt_of_le hr2 hp)
      simp [hi, h1, h2]
  rw [hxor]
  exact Nat.xor_lt_two_pow hr1 hr2

/-- A value fits the window exactly when its low bits are zero and high bits fit. -/
theorem PreviousWindow_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.PreviousWindow.Goal := by
  intro lo w x
  constructor
  · intro h
    simp only [fitsWindow, Bool.and_eq_true, decide_eq_true_eq] at h
    rcases h with ⟨hlo, hhi⟩
    have hmod0 : x % 2 ^ lo = 0 := by simpa using hlo
    have hdvd : 2 ^ lo ∣ x := Nat.dvd_of_mod_eq_zero hmod0
    have hx : x / 2 ^ lo * 2 ^ lo = x := Nat.div_mul_cancel hdvd
    rw [Nat.mod_eq_of_lt hhi, hx]
  · intro h
    simp only [fitsWindow, Bool.and_eq_true, decide_eq_true_eq]
    constructor
    · have hm : ((x / 2 ^ lo % 2 ^ w) * 2 ^ lo) % 2 ^ lo = x % 2 ^ lo :=
        congrArg (fun t => t % 2 ^ lo) h
      have hmod0 : x % 2 ^ lo = 0 := by simpa [Nat.mul_mod_right] using hm.symm
      simpa using hmod0
    · have hm : ((x / 2 ^ lo % 2 ^ w) * 2 ^ lo) % 2 ^ lo = x % 2 ^ lo :=
        congrArg (fun t => t % 2 ^ lo) h
      have hmod0 : x % 2 ^ lo = 0 := by simpa [Nat.mul_mod_right] using hm.symm
      have hdvd : 2 ^ lo ∣ x := Nat.dvd_of_mod_eq_zero hmod0
      have hxd : x = x / 2 ^ lo * 2 ^ lo := (Nat.div_mul_cancel hdvd).symm
      rw [hxd] at h
      have hpos : 0 < 2 ^ lo := Nat.pow_pos (by decide)
      have hdiv : x / 2 ^ lo * 2 ^ lo / 2 ^ lo = x / 2 ^ lo := Nat.mul_div_cancel _ hpos
      rw [hdiv] at h
      have hmul : ((x / 2 ^ lo % 2 ^ w) * 2 ^ lo) / 2 ^ lo =
          (x / 2 ^ lo * 2 ^ lo) / 2 ^ lo := congrArg (fun t => t / 2 ^ lo) h
      rw [Nat.mul_div_cancel _ hpos, Nat.mul_div_cancel _ hpos] at hmul
      rw [← hmul]
      exact Nat.mod_lt _ (Nat.pow_pos (by decide))

/-- Arithmetic rearrangement of the header plus per-row width saving. -/
theorem AmortizedWindow_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.AmortizedWindow.Goal := by
  intro n oldWidth newWidth header hle
  constructor
  · intro h
    have hsub : n * newWidth + n * (oldWidth - newWidth) = n * oldWidth := by
      rw [← Nat.mul_add, Nat.add_sub_cancel' hle]
    omega
  · intro h
    have hsub : n * newWidth + n * (oldWidth - newWidth) = n * oldWidth := by
      rw [← Nat.mul_add, Nat.add_sub_cancel' hle]
    omega

/-- The deduplicated dictionary index of a member recovers that member. -/
theorem RawDictionary_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.RawDictionary.Goal := by
  intro xs
  dsimp only
  let d := xs.eraseDups
  calc
    xs.map (fun x => d[d.idxOf x]?.getD 0) = xs.map id := by
      apply List.map_congr_left
      intro a ha
      have hmem : a ∈ d := by exact List.mem_eraseDups.mpr ha
      have hidx : d.idxOf a < d.length := List.idxOf_lt_length_of_mem hmem
      rw [List.getElem?_eq_getElem hidx, Option.getD_some, List.getElem_idxOf hidx]
      rfl
    _ = xs := by simp

/-- Counterexample: the raw image `2^63` (negative zero) is not `0`. -/
theorem refutation_ZeroAlias :
    ¬ TinylambOpt.ResearchGoals.Compression.BitPatterns.ZeroAlias.Goal := by
  intro h
  -- For xs = [0, 2^63] the map sends 2^63 to 0, changing the list:
  -- [0, 2^63] |> map (fun x => if x == 2^63 then 0 else x) = [0, 0] != [0, 2^63].
  have hh := h [0, 2 ^ 63]
  have hne : ([0, 2 ^ 63] : List Nat).map (fun x => if x == 2 ^ 63 then 0 else x) ≠
      [0, 2 ^ 63] := by
    native_decide
  exact hne hh

/-- A plane that is constant over the column supplies every row's bit. -/
theorem ConstantPlanes_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.ConstantPlanes.Goal := by
  intro xs i hxs hi
  dsimp only
  simp only [List.getElem?_eq_getElem hi]
  unfold plane
  apply rebuild_eq_of_test (row := i) (x := xs[i]'hi)
  · exact hxs _ (List.getElem_mem hi)
  · intro p
    by_cases hall : (xs.map (fun x => x.testBit p)).all id = true
    · rw [ite_eq_left hall]
      have hmem : (xs[i]'hi).testBit p ∈ (xs.map (fun x => x.testBit p)) :=
        List.mem_map.mpr ⟨xs[i]'hi, List.getElem_mem hi, rfl⟩
      have hbit : (xs[i]'hi).testBit p = true := (List.all_eq_true.mp hall) _ hmem
      exact hbit.symm
    · rw [ite_eq_right hall]
      simp [List.getElem?_eq_getElem hi]

/-- Storing the minority bit positions reconstructs each plane's bit exactly. -/
theorem SparsePlanes_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.SparsePlanes.Goal := by
  intro xs i hxs hi
  dsimp only
  simp only [List.getElem?_eq_getElem hi]
  unfold plane
  apply rebuild_eq_of_test (row := i) (x := xs[i]'hi)
  · exact hxs _ (List.getElem_mem hi)
  · intro p
    let bs := xs.map (fun x => x.testBit p)
    let mark := decide (bs.count true ≤ bs.count false)
    let v := (xs[i]'hi).testBit p
    change xor (!mark) (((bs.zipIdx.filter (fun (v, _) => v == mark)).map Prod.snd).contains i) = v
    have hi' : i < bs.length := by simpa [bs] using hi
    have hvget : bs[i]? = some v := by
      rw [List.getElem?_eq_getElem hi']
      simp [bs, v]
    have hcontain : (((bs.zipIdx.filter (fun (v, _) => v == mark)).map Prod.snd).contains i) =
        (v == mark) := by
      apply Bool.eq_iff_iff.mpr
      constructor
      · intro ht
        obtain ⟨e, he, heq⟩ := List.mem_map.mp (List.contains_iff_mem.mp ht)
        have hef := List.mem_filter.mp he
        have hez : bs[e.2]? = some e.1 := List.mem_zipIdx_iff_getElem?.mp hef.1
        have he2 : e.2 = i := by simpa using heq
        have he1 : e.1 = v := by
          rw [he2] at hez
          rw [hvget] at hez
          exact (Option.some.inj hez).symm
        have hmark : (e.1 == mark) = true := hef.2
        rw [he1] at hmark
        exact hmark
      · intro h
        apply List.contains_iff_mem.mpr
        apply List.mem_map.mpr
        refine ⟨(v, i), ?_, rfl⟩
        apply List.mem_filter.mpr
        exact ⟨List.mem_zipIdx_iff_getElem?.mpr hvget, h⟩
    rw [hcontain]
    cases mark <;> cases v <;> rfl

/-- Byte shuffling keeps each row's eight bytes at a stride of the row count. -/
theorem ByteShuffle_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.ByteShuffle.Goal := by
  intro xs i hi hxs
  simp only [List.getElem?_eq_getElem hi]
  unfold bytePlanes
  have hf : ∀ j, (xs.map (fun x => (x / 256 ^ j) % 256)).length = xs.length := by
    intro j
    simp
  have hget : ∀ j : Nat, j < 8 →
      ((List.range 8).flatMap (fun j => xs.map (fun x => (x / 256 ^ j) % 256)))[j * xs.length + i]? =
        some ((xs[i]'hi / 256 ^ j) % 256) := by
    intro j hj
    rw [flatMap_range_getElem (fun j => xs.map (fun x => (x / 256 ^ j) % 256)) xs.length hf 8 j i hj hi]
    simp [List.getElem?_eq_getElem hi]
  have hmap : (List.range 8).map (fun j =>
      ((List.range 8).flatMap (fun j => xs.map (fun x => (x / 256 ^ j) % 256)))[j * xs.length + i]?.getD 0) =
      (List.range 8).map (fun j => (xs[i]'hi / 256 ^ j) % 256) := by
    apply List.map_congr_left
    intro j hj
    rw [hget j (List.mem_range.mp hj)]
    simp
  rw [hmap, fromLittle_digits]
  simp only [Option.getD_some]
  rw [Nat.mod_eq_of_lt]
  exact hxs _ (List.getElem_mem hi)

/-- Only the first difference depends on the chosen anchor value. -/
theorem Reanchor_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.Reanchor.Goal := by
  intro a b xs
  cases xs with
  | nil => rfl
  | cons x xs => simp [delta]

/-- A shared low-bit tail can be folded back into every value. -/
theorem SharedMantissaTail_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.SharedMantissaTail.Goal := by
  intro k xs rem hrem
  calc
    xs.map (fun x => x / 2 ^ k * 2 ^ k + rem) = xs.map id := by
      apply List.map_congr_left
      intro x hx
      change x / 2 ^ k * 2 ^ k + rem = x
      rw [Nat.mul_comm, ← hrem x hx]
      exact Nat.div_add_mod x (2 ^ k)
    _ = xs := by simp

/-- Exceptions patch exactly the values that the codec does not round-trip. -/
theorem PatchedFloatConversion_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.PatchedFloatConversion.Goal := by
  intro xs encode decode
  dsimp only
  apply List.ext_getElem
  · simp [List.length_zipIdx]
  · intro i hi1 hi2
    let es := xs.zipIdx.filter (fun (x, _) => decode (encode x) != x)
    let p : Nat × Nat → Bool := fun e => e.2 == i
    rw [List.getElem_map]
    have hzlen : i < xs.zipIdx.length := by simpa using hi2
    have hzi : xs.zipIdx[i]'hzlen = (xs[i]'hi2, i) := by
      simp
    rw [hzi]
    change (Option.map Prod.fst (es.find? p)).getD (decode (encode (xs[i]'hi2))) = xs[i]'hi2
    have hmemval : ∀ (e : Nat × Nat), e ∈ xs.zipIdx → (e.2 == i) = true → e.1 = xs[i] := by
      intro e he hei
      have hjv : xs[e.2]? = some e.1 := List.mem_zipIdx_iff_getElem?.mp he
      have hji : e.2 = i := by simpa using hei
      have hjvi : xs[i]? = some e.1 := by simpa [hji] using hjv
      rw [List.getElem?_eq_getElem hi2] at hjvi
      exact (Option.some.inj hjvi).symm
    by_cases hbad : (decode (encode xs[i]) != xs[i]) = true
    · have hwit : (xs[i], i) ∈ es := by
        apply List.mem_filter.mpr
        refine ⟨?_, hbad⟩
        apply List.mem_zipIdx_iff_getElem?.mpr
        simp
      have hfind : es.find? p = some (xs[i], i) := by
        cases hf : es.find? p with
        | none =>
            have hnone := (List.find?_eq_none.mp hf) (xs[i], i) hwit
            simp [p] at hnone
        | some e =>
            rcases (List.find?_eq_some_iff_append.mp hf) with ⟨hp, as, bs, happ, has⟩
            have he : e ∈ es := by rw [happ]; simp
            have hez := (List.mem_filter.mp he).1
            have hp' : (e.2 == i) = true := by simpa [p] using hp
            have hval := hmemval e hez hp'
            have hidx : e.2 = i := by simpa using hp'
            have heq : e = (xs[i], i) := Prod.ext hval hidx
            simp [heq]
      rw [hfind]
      simp
    · have hgood : decode (encode xs[i]) = xs[i] := by
        have hf : (decode (encode xs[i]) != xs[i]) = false := Bool.eq_false_of_ne_true hbad
        simpa using hf
      have hfind : es.find? p = none := by
        rw [List.find?_eq_none]
        intro e he hp
        have hez := (List.mem_filter.mp he).1
        have hbad' := (List.mem_filter.mp he).2
        have hp' : (e.2 == i) = true := by simpa [p] using hp
        have hval := hmemval e hez hp'
        have hidx : e.2 = i := by simpa using hp'
        have hbad'' : (decode (encode xs[i]) != xs[i]) = true := by
          simpa [hval, hidx] using hbad'
        exact hbad hbad''
      rw [hfind]
      simp [hgood]

/-- The exception overhead cancels from both sides of the break-even test. -/
theorem NarrowWithExceptions_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.NarrowWithExceptions.Goal := by
  intro n h ordinals
  constructor <;> intro hlt <;> omega

end TinylambOpt.ResearchProofs.Compression.BitPatterns
