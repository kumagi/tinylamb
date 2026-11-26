import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `SerpentineRow.Transcode` — Convert from this physical order to tight row-major bytes
  using lane reads.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentineRow_Transcode

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentineRow

/-! Transcoding a serpentine-row column is lossless: each row owns a block of
`w` bit cells (reversed on odd rows), so `fetch` recovers `xs[i] % 2^w`.
`word_congr_mod` then makes the two `pack` streams identical. -/

private def pos (w i b : Nat) : Nat := i*w + (if i % 2 = 0 then b else w-1-b)

-- Truth check on small inputs (the hypothesis `i < xs.length` is respected).
private def check (N W : Nat) : Bool :=
  (List.range N).all fun n =>
  (List.range W).all fun w =>
  let xs := (List.range n).map (fun k => (k*7 + w*3 + 1) % (2^(w+1)))
  decide (pack w ((List.range xs.length).map
    (fun i => fetch layout xs.length w i (atByte (file layout w xs)))) = pack w xs)

#eval check 5 5

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

private theorem pos_lt (w i b : Nat) (hb : b < w) : pos w i b < (i+1)*w := by
  unfold pos
  rw [Nat.succ_mul]
  split <;> omega

private theorem pos_ne_of_row_ne (w j i b c : Nat) (hb : b < w) (hc : c < w)
    (hij : i ≠ j) : pos w j c ≠ pos w i b := by
  intro h
  have hw : 0 < w := by omega
  unfold pos at h
  rcases Nat.lt_or_gt_of_ne hij with hlt | hgt
  · have hmul : i*w + w ≤ j*w := by
      rw [← Nat.succ_mul]
      exact Nat.mul_le_mul_right w (by omega)
    by_cases hpi : i % 2 = 0 <;> by_cases hpj : j % 2 = 0 <;>
      simp [hpi, hpj] at h <;> omega
  · have hmul : j*w + w ≤ i*w := by
      rw [← Nat.succ_mul]
      exact Nat.mul_le_mul_right w (by omega)
    by_cases hpi : i % 2 = 0 <;> by_cases hpj : j % 2 = 0 <;>
      simp [hpi, hpj] at h <;> omega

private theorem pos_inj (w i b c : Nat) (hb : b < w) (hc : c < w)
    (h : pos w i b = pos w i c) : b = c := by
  unfold pos at h
  by_cases hp : i % 2 = 0
  · simp [hp] at h
    exact h
  · simp [hp] at h
    omega

private def rowStep (w j x acc : Nat) : Nat :=
  (List.range w).foldl (fun a c => if x.testBit c then a ||| 2 ^ (pos w j c) else a) acc

private theorem rowStep_testBit (w j x p acc : Nat) :
    (rowStep w j x acc).testBit p =
      (acc.testBit p || (List.range w).any (fun c => x.testBit c && decide (pos w j c = p))) := by
  unfold rowStep
  generalize hs : List.range w = cs
  clear hs
  induction cs generalizing acc with
  | nil => simp
  | cons c cs ih =>
      change (List.foldl (fun a c => if x.testBit c then a ||| 2 ^ (pos w j c) else a)
        (if x.testBit c then acc ||| 2 ^ (pos w j c) else acc) cs).testBit p =
        (acc.testBit p || ((c::cs).any (fun c => x.testBit c && decide (pos w j c = p))))
      rw [ih]
      by_cases hxc : x.testBit c
      · simp [hxc, Nat.testBit_or, Nat.testBit_two_pow, Bool.or_assoc, Bool.or_left_comm, Bool.or_comm]
      · simp [hxc]

private theorem rowStep_bit (w j x i b acc : Nat) (hb : b < w) :
    (rowStep w j x acc).testBit (pos w i b) =
      (acc.testBit (pos w i b) || (if i = j then x.testBit b else false)) := by
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
      have hcb : c = b := pos_inj w i c b hcw hb (of_decide_eq_true hpred.2)
      simpa [hcb] using hpred.1
    · intro hxb
      refine ⟨b, by simp [hb], ?_⟩
      simp [hxb]
  · simp only [hij, ite_false]
    rw [List.any_eq_false]
    intro c hc hpred
    simp only [Bool.and_eq_true] at hpred
    have hcw : c < w := by simpa using hc
    exact pos_ne_of_row_ne w j i b c hb hcw hij (of_decide_eq_true hpred.2)

private def imgAcc (w s acc : Nat) : List Nat → Nat
  | [] => acc
  | x :: xs => imgAcc w (s+1) (rowStep w s x acc) xs

private theorem zipIdx_foldl_eq (w s acc : Nat) (xs : List Nat) :
    (xs.zipIdx s).foldl (fun a (x,j) => rowStep w j x a) acc = imgAcc w s acc xs := by
  induction xs generalizing s acc with
  | nil => rfl
  | cons x xs ih =>
      change (xs.zipIdx (s+1)).foldl (fun a (x,j) => rowStep w j x a) (rowStep w s x acc) =
        imgAcc w (s+1) (rowStep w s x acc) xs
      exact ih (s+1) (rowStep w s x acc)

private theorem image_eq_imgAcc (w : Nat) (xs : List Nat) :
    image layout w xs = imgAcc w 0 0 xs := by
  simpa [image, layout, lanes, rowStep, pos] using (zipIdx_foldl_eq w 0 0 xs)

private theorem imgAcc_bit (w s acc : Nat) (xs : List Nat) (i b : Nat) (hb : b < w) :
    (imgAcc w s acc xs).testBit (pos w i b) =
      (if s ≤ i then (acc.testBit (pos w i b) || (xs[i-s]?.getD 0).testBit b)
       else acc.testBit (pos w i b)) := by
  induction xs generalizing s acc with
  | nil =>
      simp [imgAcc]
  | cons x xs ih =>
      simp only [imgAcc]
      rw [ih]
      rw [rowStep_bit w s x i b acc hb]
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

private theorem lossless_serpentine : lossless layout := by
  intro w xs i hi
  have hcap : xs.length*w ≤ 8*byteCount (xs.length*w) := by
    unfold byteCount
    omega
  have himage_bit : ∀ b, b < w →
      (atByte (file layout w xs) ((pos w i b)/8)).testBit ((pos w i b)%8) =
        (image layout w xs).testBit (pos w i b) := by
    intro b hb
    have hprow : pos w i b < i*w + w := by
      rw [← Nat.succ_mul]
      exact pos_lt w i b hb
    have hiw : i*w + w ≤ xs.length*w := by
      rw [← Nat.succ_mul]
      exact Nat.mul_le_mul_right w (by omega)
    have hp : pos w i b < 8*byteCount (xs.length*w) := by omega
    simpa [file, layout, pos] using
      (file_bit (image layout w xs) (byteCount (xs.length*w)) (pos w i b) hp)
  have hsum1 :
      ((List.range w).map (fun b =>
        if (atByte (file layout w xs) ((pos w i b)/8)).testBit ((pos w i b)%8) then 2^b else 0)).sum =
      ((List.range w).map (fun b =>
        if (image layout w xs).testBit (pos w i b) then 2^b else 0)).sum := by
    congr 1
    apply List.map_congr_left
    intro b hbm
    have hb : b < w := by simpa using hbm
    rw [himage_bit b hb]
  have hsum2 :
      ((List.range w).map (fun b =>
        if (image layout w xs).testBit (pos w i b) then 2^b else 0)).sum =
      xs[i]?.getD 0 % 2^w := by
    rw [image_eq_imgAcc]
    have hmap :
        ((List.range w).map (fun b =>
          if (imgAcc w 0 0 xs).testBit (pos w i b) then 2^b else 0)) =
        ((List.range w).map (fun b =>
          if (xs[i]?.getD 0).testBit b then 2^b else 0)) := by
      apply List.map_congr_left
      intro b hbm
      have hb : b < w := by simpa using hbm
      have ht := imgAcc_bit w 0 0 xs i b hb
      simp at ht
      rw [ht]
    rw [hmap]
    exact bit_sum_mod (xs[i]?.getD 0) w
  change ((List.range w).map (fun b =>
    if (atByte (file layout w xs) ((pos w i b)/8)).testBit ((pos w i b)%8) then 2^b else 0)).sum =
    xs[i]?.getD 0 % 2^w
  exact hsum1.trans hsum2

private theorem word_congr_mod (w : Nat) (xs ys : List Nat)
    (h : List.map (fun x => x % 2 ^ w) xs = List.map (fun x => x % 2 ^ w) ys) :
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

theorem sound_Transcode : SerpentineRow.Transcode.Goal := by
  intro w xs
  have hfetch : (List.range xs.length).map
      (fun i => fetch layout xs.length w i (atByte (file layout w xs))) =
      xs.map (fun x => x % 2 ^ w) := by
    apply List.ext_getElem?
    intro k
    by_cases hk : k < xs.length
    · simp [hk, lossless_serpentine w xs k hk]
    · simp [hk]
  rw [hfetch]
  unfold pack
  rw [word_congr_mod w (xs.map (fun x => x % 2 ^ w)) xs]
  · simp
  · simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentineRow_Transcode
