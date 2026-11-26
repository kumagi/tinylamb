import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RotatedPlane_Lossless

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane

/-! In the rotated-plane layout row `i`, lane `b` lives at `b*n + (i+b)%n`.
Each bit plane owns a block of `n` cells and `i ↦ (i+b)%n` is injective on
`i < n`, so distinct writers never collide.  `little` preserves every bit
below the capacity and the weighted sum of the low `w` lanes recovers
`xs[i] % 2^w`. -/

private def pos (n i b : Nat) : Nat := b*n + (i+b)%n

-- Truth check on small inputs (the hypothesis `i < xs.length` is respected).
private def check (N W : Nat) : Bool :=
  (List.range N).all fun n =>
  (List.range W).all fun w =>
  let xs := (List.range n).map (fun k => k*5 + w)
  (List.range n).all fun i =>
    decide (fetch layout n w i (atByte (file layout w xs)) = xs[i]?.getD 0 % 2^w)

#eval check 8 8

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

private theorem pos_lt (n i b : Nat) (hi : i < n) : pos n i b < (b+1)*n := by
  unfold pos
  rw [Nat.succ_mul]
  have hn : 0 < n := by omega
  have hmod : (i+b)%n < n := Nat.mod_lt _ hn
  omega

private theorem pos_ge (n i b : Nat) : b*n ≤ pos n i b := by
  unfold pos
  omega

/-! Both addends below lie in `[0,n)`, so two of them with the same residue
modulo `n` are either equal or differ by exactly `n`; the latter is excluded
by `i,j < n`. -/
private theorem mod_inj_add (n i j c : Nat) (hi : i < n) (hj : j < n) (hc : c < n)
    (h : (i+c)%n = (j+c)%n) : i = j := by
  by_cases hic : i + c < n
  · have hi' : (i+c)%n = i+c := Nat.mod_eq_of_lt hic
    by_cases hjc : j + c < n
    · have hj' : (j+c)%n = j+c := Nat.mod_eq_of_lt hjc
      rw [hi', hj'] at h
      omega
    · have hjc_ge : n ≤ j + c := by omega
      have hj' : (j+c)%n = (j+c-n)%n := Nat.mod_eq_sub_mod hjc_ge
      have hjlt : j + c - n < n := by omega
      have hj'' : (j+c-n)%n = j+c-n := Nat.mod_eq_of_lt hjlt
      rw [hi', hj', hj''] at h
      omega
  · have hic_ge : n ≤ i + c := by omega
    have hi' : (i+c)%n = (i+c-n)%n := Nat.mod_eq_sub_mod hic_ge
    have hilt : i + c - n < n := by omega
    have hi'' : (i+c-n)%n = i+c-n := Nat.mod_eq_of_lt hilt
    by_cases hjc : j + c < n
    · have hj' : (j+c)%n = j+c := Nat.mod_eq_of_lt hjc
      rw [hi', hi'', hj'] at h
      omega
    · have hjc_ge : n ≤ j + c := by omega
      have hj' : (j+c)%n = (j+c-n)%n := Nat.mod_eq_sub_mod hjc_ge
      have hjlt : j + c - n < n := by omega
      have hj'' : (j+c-n)%n = j+c-n := Nat.mod_eq_of_lt hjlt
      rw [hi', hi'', hj', hj''] at h
      omega

private theorem rot_mod_inj (n i j b : Nat) (hn : 0 < n) (hi : i < n) (hj : j < n)
    (h : (i+b)%n = (j+b)%n) : i = j := by
  have hbmod : b % n < n := Nat.mod_lt _ hn
  have himod : i % n = i := Nat.mod_eq_of_lt hi
  have hjmod : j % n = j := Nat.mod_eq_of_lt hj
  have h' : (i + b % n) % n = (j + b % n) % n := by
    simpa [Nat.add_mod, himod, hjmod] using h
  exact mod_inj_add n i j (b%n) hi hj hbmod h'

/-! Distinct cells: different planes are disjoint blocks of size `n`, and inside
one plane the rotation `i ↦ (i+b)%n` is injective on `i < n`. -/
private theorem pos_ne_of_ne (n i b j c : Nat) (hi : i < n) (hj : j < n)
    (hne : i ≠ j ∨ b ≠ c) : pos n i b ≠ pos n j c := by
  intro h
  by_cases hbc : b = c
  · subst c
    have hij : i ≠ j := by simpa using hne
    unfold pos at h
    have hmod : (i+b)%n = (j+b)%n := Nat.add_left_cancel h
    have hn : 0 < n := by omega
    exact hij (rot_mod_inj n i j b hn hi hj hmod)
  · rcases Nat.lt_or_gt_of_ne hbc with hlt | hgt
    · have hmul : (b+1)*n ≤ c*n := Nat.mul_le_mul_right n (by omega)
      have hbi : pos n i b < (b+1)*n := pos_lt n i b hi
      have hge : c*n ≤ pos n j c := pos_ge n j c
      omega
    · have hmul : (c+1)*n ≤ b*n := Nat.mul_le_mul_right n (by omega)
      have hcj : pos n j c < (c+1)*n := pos_lt n j c hj
      have hge : b*n ≤ pos n i b := pos_ge n i b
      omega

private def rowStep (n w j : Nat) (x acc : Nat) : Nat :=
  (List.range w).foldl (fun a b => if x.testBit b then a ||| 2 ^ (pos n j b) else a) acc

private theorem rowStep_testBit (n w j x p acc : Nat) :
    (rowStep n w j x acc).testBit p =
      (acc.testBit p || (List.range w).any (fun c => x.testBit c && decide (pos n j c = p))) := by
  unfold rowStep
  generalize hs : List.range w = cs
  clear hs
  induction cs generalizing acc with
  | nil => simp
  | cons c cs ih =>
      change (List.foldl (fun a c => if x.testBit c then a ||| 2 ^ (pos n j c) else a)
        (if x.testBit c then acc ||| 2 ^ (pos n j c) else acc) cs).testBit p =
        (acc.testBit p || ((c::cs).any (fun c => x.testBit c && decide (pos n j c = p))))
      rw [ih]
      by_cases hxc : x.testBit c
      · simp [hxc, Nat.testBit_or, Nat.testBit_two_pow, Bool.or_assoc, Bool.or_left_comm, Bool.or_comm]
      · simp [hxc]

private theorem rowStep_bit (n w j x i b acc : Nat) (hi : i < n) (hj : j < n)
    (hb : b < w) :
    (rowStep n w j x acc).testBit (pos n i b) =
      (acc.testBit (pos n i b) || (if i = j then x.testBit b else false)) := by
  rw [rowStep_testBit]
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
      by_cases hc_eq_b : c = b
      · simpa [hc_eq_b] using hpred.1
      · exfalso
        exact pos_ne_of_ne n i c i b hi hi (Or.inr hc_eq_b) (of_decide_eq_true hpred.2)
    · intro hxb
      refine ⟨b, by simp [hb], ?_⟩
      simp [hxb]
  · simp only [hij, ite_false]
    rw [List.any_eq_false]
    intro c hc hpred
    simp only [Bool.and_eq_true] at hpred
    have hcw : c < w := by simpa using hc
    exact pos_ne_of_ne n i b j c hi hj (Or.inl hij) (of_decide_eq_true hpred.2).symm

private def imgAcc (n w s acc : Nat) : List Nat → Nat
  | [] => acc
  | x :: xs => imgAcc n w (s+1) (rowStep n w s x acc) xs

private theorem zipIdx_foldl_eq (n w s acc : Nat) (xs : List Nat) :
    (xs.zipIdx s).foldl (fun a (x,j) => rowStep n w j x a) acc = imgAcc n w s acc xs := by
  induction xs generalizing s acc with
  | nil => rfl
  | cons x xs ih =>
      change (xs.zipIdx (s+1)).foldl (fun a (x,j) => rowStep n w j x a) (rowStep n w s x acc) =
        imgAcc n w (s+1) (rowStep n w s x acc) xs
      exact ih (s+1) (rowStep n w s x acc)

private theorem image_eq_imgAcc (w : Nat) (xs : List Nat) :
    image layout w xs = imgAcc xs.length w 0 0 xs := by
  simpa [image, layout, lanes, rowStep, pos] using (zipIdx_foldl_eq xs.length w 0 0 xs)

private theorem imgAcc_bit (n w s acc : Nat) (xs : List Nat) (i b : Nat)
    (hlen : s + xs.length = n) (hi : i < n) (hb : b < w) :
    (imgAcc n w s acc xs).testBit (pos n i b) =
      (if s ≤ i then (acc.testBit (pos n i b) || (xs[i-s]?.getD 0).testBit b)
       else acc.testBit (pos n i b)) := by
  induction xs generalizing s acc with
  | nil =>
      simp [imgAcc]
  | cons x xs ih =>
      simp only [imgAcc]
      have hlen0 : s + (xs.length + 1) = n := by simpa using hlen
      have hlen' : s + 1 + xs.length = n := by omega
      have hs : s < n := by omega
      rw [ih (s+1) (rowStep n w s x acc) hlen']
      rw [rowStep_bit n w s x i b acc hi hs hb]
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

theorem sound_Lossless : RotatedPlane.Lossless.Goal := by
  intro w xs i hi
  let n := xs.length
  have himage_bit : ∀ b, b < w →
      (atByte (file layout w xs) ((pos n i b)/8)).testBit ((pos n i b)%8) =
        (image layout w xs).testBit (pos n i b) := by
    intro b hb
    have hle : (b+1)*n ≤ n*w := by
      have hmul := Nat.mul_le_mul_right n (show b+1 ≤ w by omega)
      simpa [Nat.mul_comm] using hmul
    have hpos : pos n i b < (b+1)*n := pos_lt n i b hi
    have hcap : n*w ≤ 8*byteCount (n*w) := by
      unfold byteCount
      omega
    have hp : pos n i b < 8*byteCount (n*w) := by omega
    simpa [file, layout, n, pos] using
      (file_bit (image layout w xs) (byteCount (n*w)) (pos n i b) hp)
  have hsum1 :
      ((List.range w).map (fun b =>
        if (atByte (file layout w xs) ((pos n i b)/8)).testBit ((pos n i b)%8) then 2^b else 0)).sum =
      ((List.range w).map (fun b =>
        if (image layout w xs).testBit (pos n i b) then 2^b else 0)).sum := by
    congr 1
    apply List.map_congr_left
    intro b hbm
    have hb : b < w := by simpa using hbm
    rw [himage_bit b hb]
  have hsum2 :
      ((List.range w).map (fun b =>
        if (image layout w xs).testBit (pos n i b) then 2^b else 0)).sum =
      xs[i]?.getD 0 % 2^w := by
    rw [image_eq_imgAcc]
    have hmap :
        ((List.range w).map (fun b =>
          if (imgAcc n w 0 0 xs).testBit (pos n i b) then 2^b else 0)) =
        ((List.range w).map (fun b =>
          if (xs[i]?.getD 0).testBit b then 2^b else 0)) := by
      apply List.map_congr_left
      intro b hbm
      have hb : b < w := by simpa using hbm
      have ht := imgAcc_bit n w 0 0 xs i b (by simp [n]) hi hb
      simp at ht
      rw [ht]
    rw [hmap]
    exact bit_sum_mod (xs[i]?.getD 0) w
  change ((List.range w).map (fun b =>
    if (atByte (file layout w xs) ((pos xs.length i b)/8)).testBit ((pos xs.length i b)%8) then 2^b else 0)).sum =
    xs[i]?.getD 0 % 2^w
  simpa [n] using hsum1.trans hsum2

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RotatedPlane_Lossless
