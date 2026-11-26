import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `Vertical.Lossless` — Read one lane from serialized bytes, including zero width.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_Vertical_Lossless

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.Vertical

/-! Vertical bit cells are `b*n+i`: one bitmap per significance. Writer lanes
land on distinct cells, `little` keeps every bit below `byteCount (n*w)`, and
the weighted sum of the low `w` bits is `xs[i] % 2^w`. -/

private theorem byte_testBit (n p : Nat) :
    (((n / 256 ^ (p / 8)) % 256).testBit (p % 8)) = n.testBit p := by
  have h256 : 256 ^ (p / 8) = 2 ^ (8 * (p / 8)) := by
    change (2^8) ^ (p/8) = 2 ^ (8 * (p/8))
    rw [Nat.pow_mul]
  rw [h256]
  change (n / 2 ^ (8 * (p/8)) % 2^8).testBit (p%8) = n.testBit p
  rw [Nat.testBit_mod_two_pow]
  have hlt : p % 8 < 8 := Nat.mod_lt _ (by decide)
  simp [hlt, Nat.testBit_div_two_pow]
  rw [Nat.mod_add_div p 8]

private theorem atByte_little (n k j : Nat) :
    atByte (little n k) j = if j < k then (n / 256 ^ j) % 256 else 0 := by
  simp [atByte, little]
  split
  · simp [*]
  · simp [*]

private theorem file_bit (n k p : Nat) (hp : p < 8*k) :
    (atByte (little n k) (p/8)).testBit (p%8) = n.testBit p := by
  have hj : p/8 < k := by omega
  simp [atByte_little, hj, byte_testBit]

private theorem bit_sum_mod (x w : Nat) :
    ((List.range w).map (fun b => if x.testBit b then 2^b else 0)).sum = x % 2^w := by
  induction w with
  | zero => simp [Nat.mod_one]
  | succ w ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      rw [Nat.mod_pow_succ]
      change x % 2^w + (if x.testBit w then 2^w else 0) =
        x % 2^w + 2^w * (x / 2^w % 2)
      have hbit : x.testBit w = decide (x / 2^w % 2 = 1) := by
        simpa [Nat.testBit_zero] using (Nat.testBit_add x 0 w)
      rw [hbit]
      by_cases h : x / 2^w % 2 = 1
      · simp [h]
      · have hlt : x / 2^w % 2 < 2 := Nat.mod_lt _ (by decide)
        have hz : x / 2^w % 2 = 0 := by omega
        simp [h, hz]

private def planeStep (n w j : Nat) (x acc : Nat) : Nat :=
  (List.range w).foldl (fun a b => if x.testBit b then a ||| 2 ^ (b*n+j) else a) acc

private theorem planeStep_testBit (n w j x p acc : Nat) :
    (planeStep n w j x acc).testBit p =
      (acc.testBit p || (List.range w).any (fun c => x.testBit c && decide (c*n+j = p))) := by
  unfold planeStep
  generalize hs : List.range w = cs
  clear hs
  induction cs generalizing acc with
  | nil => simp
  | cons c cs ih =>
      change (List.foldl (fun a c => if x.testBit c then a ||| 2 ^ (c*n+j) else a)
        (if x.testBit c then acc ||| 2 ^ (c*n+j) else acc) cs).testBit p =
        (acc.testBit p || ((c::cs).any (fun c => x.testBit c && decide (c*n+j = p))))
      rw [ih]
      by_cases hxc : x.testBit c
      · simp [hxc, Nat.testBit_or, Nat.testBit_two_pow, Bool.or_assoc, Bool.or_left_comm, Bool.or_comm]
      · simp [hxc]

/-! Two lanes of different rows cannot share a cell: if `b<c` then
`b*n+i < (b+1)*n ≤ c*n ≤ c*n+j` (and symmetrically), while `b=c` forces
`i=j`. -/
private theorem plane_pos_ne (n w j i b c : Nat) (hi : i < n) (hj : j < n)
    (_hb : b < w) (hc : c < w) (hij : i ≠ j) : c*n + j ≠ b*n + i := by
  intro h
  rcases Nat.lt_trichotomy b c with hlt | heq | hgt
  · have hmul : (b+1)*n ≤ c*n := Nat.mul_le_mul_right n (by omega)
    have hbi : b*n+i < (b+1)*n := by rw [Nat.succ_mul]; omega
    omega
  · subst c
    exact hij (Nat.add_left_cancel h).symm
  · have hmul : (c+1)*n ≤ b*n := Nat.mul_le_mul_right n (by omega)
    have hcj : c*n+j < (c+1)*n := by rw [Nat.succ_mul]; omega
    omega

private theorem planeStep_bit (n w j x i b acc : Nat) (hi : i < n) (hj : j < n)
    (hb : b < w) :
    (planeStep n w j x acc).testBit (b*n+i) =
      (acc.testBit (b*n+i) || (if i = j then x.testBit b else false)) := by
  rw [planeStep_testBit]
  congr 1
  by_cases hij : i = j
  · subst j
    simp only [ite_true]
    apply Bool.eq_iff_iff.mpr
    rw [List.any_eq_true]
    constructor
    · rintro ⟨c, hc, hpred⟩
      simp only [Bool.and_eq_true] at hpred
      have hcw : c < w := by simpa using hc
      have hmul : c*n = b*n := Nat.add_right_cancel (of_decide_eq_true hpred.2)
      have hn : 0 < n := by omega
      have hcb : c = b := Nat.eq_of_mul_eq_mul_right hn hmul
      simpa [hcb] using hpred.1
    · intro hxb
      refine ⟨b, by simp [hb], ?_⟩
      simp [hxb]
  · simp only [hij, ite_false]
    rw [List.any_eq_false]
    intro c hc hpred
    simp only [Bool.and_eq_true] at hpred
    have hcw : c < w := by simpa using hc
    exact plane_pos_ne n w j i b c hi hj hb hcw hij (of_decide_eq_true hpred.2)

private def imgAcc (n w s acc : Nat) : List Nat → Nat
  | [] => acc
  | x :: xs => imgAcc n w (s+1) (planeStep n w s x acc) xs

private theorem zipIdx_foldl_eq (n w s acc : Nat) (xs : List Nat) :
    (xs.zipIdx s).foldl (fun a (x,j) => planeStep n w j x a) acc = imgAcc n w s acc xs := by
  induction xs generalizing s acc with
  | nil => rfl
  | cons x xs ih =>
      change (xs.zipIdx (s+1)).foldl (fun a (x,j) => planeStep n w j x a) (planeStep n w s x acc) =
        imgAcc n w (s+1) (planeStep n w s x acc) xs
      exact ih (s+1) (planeStep n w s x acc)

private theorem image_eq_imgAcc (w : Nat) (xs : List Nat) :
    image layout w xs = imgAcc xs.length w 0 0 xs := by
  simpa [image, layout, lanes, planeStep] using (zipIdx_foldl_eq xs.length w 0 0 xs)

private theorem imgAcc_bit (n w s acc : Nat) (xs : List Nat) (i b : Nat)
    (hlen : s + xs.length = n) (hi : i < n) (hb : b < w) :
    (imgAcc n w s acc xs).testBit (b*n+i) =
      (if s ≤ i then (acc.testBit (b*n+i) || (xs[i-s]?.getD 0).testBit b)
       else acc.testBit (b*n+i)) := by
  induction xs generalizing s acc with
  | nil =>
      simp [imgAcc]
  | cons x xs ih =>
      simp only [imgAcc]
      have hlen0 : s + (xs.length + 1) = n := by simpa using hlen
      have hlen' : s + 1 + xs.length = n := by omega
      have hs : s < n := by omega
      rw [ih (s+1) (planeStep n w s x acc) hlen']
      rw [planeStep_bit n w s x i b acc hi hs hb]
      by_cases his : s ≤ i
      · by_cases hsieq : i = s
        · subst i
          have hnle : ¬ s+1 ≤ s := by omega
          simp [hnle, Bool.or_comm]
        · have hs1i : s+1 ≤ i := by omega
          have hsub : i - s = (i - (s+1)) + 1 := by omega
          simp [his, hsieq, hs1i, hsub, Bool.or_left_comm, Bool.or_comm]
      · have hn : ¬ s+1 ≤ i := by omega
        have hsieq : i ≠ s := by omega
        simp [his, hn, hsieq]

theorem sound_Lossless : Vertical.Lossless.Goal := by
  intro w xs i hi
  let n := xs.length
  have himage_bit : ∀ b, b < w →
      (atByte (file layout w xs) ((b*n+i)/8)).testBit ((b*n+i)%8) =
        (image layout w xs).testBit (b*n+i) := by
    intro b hb
    have hle : (b+1)*n ≤ n*w := by
      have hmul := Nat.mul_le_mul_right n (show b+1 ≤ w by omega)
      simpa [Nat.mul_comm] using hmul
    have hstep : b*n+i < (b+1)*n := by rw [Nat.succ_mul]; omega
    have hcap : n*w ≤ 8*byteCount (n*w) := by
      unfold byteCount
      omega
    have hp : b*n+i < 8*byteCount (n*w) := by omega
    simpa [file, layout, n] using
      (file_bit (image layout w xs) (byteCount (n*w)) (b*n+i) hp)
  have hsum1 :
      ((List.range w).map (fun b =>
        if (atByte (file layout w xs) ((b*n+i)/8)).testBit ((b*n+i)%8) then 2^b else 0)).sum =
      ((List.range w).map (fun b =>
        if (image layout w xs).testBit (b*n+i) then 2^b else 0)).sum := by
    congr 1
    apply List.map_congr_left
    intro b hbm
    have hb : b < w := by simpa using hbm
    rw [himage_bit b hb]
  have hsum2 :
      ((List.range w).map (fun b =>
        if (image layout w xs).testBit (b*n+i) then 2^b else 0)).sum =
      xs[i]?.getD 0 % 2^w := by
    rw [image_eq_imgAcc]
    have hmap :
        ((List.range w).map (fun b =>
          if (imgAcc xs.length w 0 0 xs).testBit (b*xs.length+i) then 2^b else 0)) =
        ((List.range w).map (fun b =>
          if (xs[i]?.getD 0).testBit b then 2^b else 0)) := by
      apply List.map_congr_left
      intro b hbm
      have hb : b < w := by simpa using hbm
      have ht := imgAcc_bit xs.length w 0 0 xs i b (by simp) hi hb
      simp at ht
      rw [ht]
    rw [hmap]
    exact bit_sum_mod (xs[i]?.getD 0) w
  change ((List.range w).map (fun b =>
    if (atByte (file layout w xs) ((b*xs.length+i)/8)).testBit ((b*xs.length+i)%8) then 2^b else 0)).sum =
    xs[i]?.getD 0 % 2^w
  simpa [n] using hsum1.trans hsum2

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_Vertical_Lossless
