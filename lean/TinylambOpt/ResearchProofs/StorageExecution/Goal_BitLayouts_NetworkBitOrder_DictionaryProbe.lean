import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `NetworkBitOrder.DictionaryProbe` — Read only lanes requested by a probe and
  translate them through a possibly short dictionary.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_NetworkBitOrder_DictionaryProbe

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.NetworkBitOrder

/-! NetworkBitOrder stores tight-stream position `p = i*w+b` at physical cell
`brev p`, reversing the bit order inside each byte. `brev` is injective, so
each `(i,b)` cell keeps its own value; reading the serialized bytes at the
reversed position therefore recovers `xs[i] % 2^w`. A dictionary lookup only
depends on that value, so probing through the physical layout agrees with
probing through the logical values. -/

private def brev (p : Nat) := p/8*8 + (7 - p%8)

private theorem brev_mod (p : Nat) : brev p % 8 = 7 - p%8 := by
  unfold brev
  have hp8 : p % 8 < 8 := Nat.mod_lt _ (by decide)
  omega

private theorem brev_div (p : Nat) : brev p / 8 = p/8 := by
  unfold brev
  have hp8 : p % 8 < 8 := Nat.mod_lt _ (by decide)
  omega

private theorem brev_inj {p q : Nat} (h : brev p = brev q) : p = q := by
  have hm : p % 8 = q % 8 := by
    have h' := congrArg (fun x => x % 8) h
    rw [brev_mod, brev_mod] at h'
    omega
  have hd : p / 8 = q / 8 := by
    have h' := congrArg (fun x => x / 8) h
    rw [brev_div, brev_div] at h'
    exact h'
  omega

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

private def rowStep (w j : Nat) (x : Nat) (acc : Nat) : Nat :=
  (List.range w).foldl (fun a c => if x.testBit c then a ||| 2 ^ (brev (j*w+c)) else a) acc

private theorem rowStep_testBit (w j x p acc : Nat) :
    (rowStep w j x acc).testBit p =
      (acc.testBit p || (List.range w).any (fun c => x.testBit c && decide (brev (j*w+c) = p))) := by
  unfold rowStep
  generalize hs : List.range w = cs
  clear hs
  induction cs generalizing acc with
  | nil => simp
  | cons c cs ih =>
      change (List.foldl (fun a c => if x.testBit c then a ||| 2 ^ (brev (j*w+c)) else a)
        (if x.testBit c then acc ||| 2 ^ (brev (j*w+c)) else acc) cs).testBit p =
        (acc.testBit p || ((c::cs).any (fun c => x.testBit c && decide (brev (j*w+c) = p))))
      rw [ih]
      by_cases hxc : x.testBit c
      · simp [hxc, Nat.testBit_or, Nat.testBit_two_pow, Bool.or_assoc, Bool.or_left_comm, Bool.or_comm]
      · simp [hxc]

private theorem row_pos_ne (w j i b c : Nat) (hb : b < w) (hc : c < w)
    (hij : i ≠ j) : j*w + c ≠ i*w + b := by
  intro h
  rcases Nat.lt_or_gt_of_ne hij with hij' | hij'
  · have h1 : i*w + b < (i+1)*w := by rw [Nat.succ_mul]; omega
    have h2 : (i+1)*w ≤ j*w := Nat.mul_le_mul_right w (by omega)
    omega
  · have h1 : j*w + c < (j+1)*w := by rw [Nat.succ_mul]; omega
    have h2 : (j+1)*w ≤ i*w := Nat.mul_le_mul_right w (by omega)
    omega

private theorem rowStep_bit (w j x i b acc : Nat) (hb : b < w) :
    (rowStep w j x acc).testBit (brev (i*w+b)) =
      (acc.testBit (brev (i*w+b)) || (if i = j then x.testBit b else false)) := by
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
      have : c = b := by
        have hpos : i*w + c = i*w + b := brev_inj (of_decide_eq_true hpred.2)
        exact Nat.add_left_cancel hpos
      simpa [this] using hpred.1
    · intro hxb
      refine ⟨b, by simp [hb], ?_⟩
      simp [hxb]
  · simp only [hij, ite_false]
    rw [List.any_eq_false]
    intro c hc hpred
    simp only [Bool.and_eq_true] at hpred
    have hcw : c < w := by simpa using hc
    have hpos : j*w + c = i*w + b := brev_inj (of_decide_eq_true hpred.2)
    exact row_pos_ne w j i b c hb hcw hij hpos

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
  simpa [image, layout, lanes, rowStep, brev] using (zipIdx_foldl_eq w 0 0 xs)

private theorem imgAcc_bit (w s acc : Nat) (xs : List Nat) (i b : Nat) (hb : b < w) :
    (imgAcc w s acc xs).testBit (brev (i*w+b)) =
      (if s ≤ i then (acc.testBit (brev (i*w+b)) || (xs[i-s]?.getD 0).testBit b)
       else acc.testBit (brev (i*w+b))) := by
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

/-! Physical point reads recover the logical value of any in-range row. -/
private theorem fetch_eq_value (w : Nat) (xs : List Nat) (i : Nat) (hi : i < xs.length) :
    fetch layout xs.length w i (atByte (file layout w xs)) = xs[i]?.getD 0 % 2^w := by
  let cap := align 8 (xs.length*w)
  have hcap_mul : cap % 8 = 0 := by
    dsimp [cap, align]
    omega
  have hcap_ge : xs.length*w ≤ cap := by
    dsimp [cap, align]
    omega
  have hbytes : 8 * byteCount cap = cap := by
    dsimp [byteCount]
    omega
  have himage_bit : ∀ b, b < w →
      (atByte (file layout w xs) ((brev (i*w+b))/8)).testBit ((brev (i*w+b))%8) =
        (image layout w xs).testBit (brev (i*w+b)) := by
    intro b hb
    have h1 : i*w + b < i*w + w := Nat.add_lt_add_left hb (i*w)
    have h2 : i*w + w = (i+1)*w := by rw [Nat.succ_mul]
    have hs : (i+1) ≤ xs.length := by omega
    have h3 : (i+1)*w ≤ xs.length*w := Nat.mul_le_mul_right w hs
    have hp_lt : i*w+b < xs.length*w := by omega
    have hp_lt_cap : i*w+b < cap := by omega
    have hq_lt_cap : brev (i*w+b) < cap := by
      dsimp [brev]
      omega
    have hq : brev (i*w+b) < 8*byteCount cap := by omega
    simpa [file, layout, cap] using
      (file_bit (image layout w xs) (byteCount cap) (brev (i*w+b)) hq)
  have hsum1 :
      ((List.range w).map (fun b =>
        if (atByte (file layout w xs) ((brev (i*w+b))/8)).testBit ((brev (i*w+b))%8) then 2^b else 0)).sum =
      ((List.range w).map (fun b =>
        if (image layout w xs).testBit (brev (i*w+b)) then 2^b else 0)).sum := by
    congr 1
    apply List.map_congr_left
    intro b hbm
    have hb : b < w := by simpa using hbm
    rw [himage_bit b hb]
  have hsum2 :
      ((List.range w).map (fun b =>
        if (image layout w xs).testBit (brev (i*w+b)) then 2^b else 0)).sum =
      xs[i]?.getD 0 % 2^w := by
    rw [image_eq_imgAcc]
    have hmap :
        ((List.range w).map (fun b =>
          if (imgAcc w 0 0 xs).testBit (brev (i*w+b)) then 2^b else 0)) =
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
  simpa [fetch, layout, lanes, brev] using hsum1.trans hsum2

theorem sound_DictionaryProbe : NetworkBitOrder.DictionaryProbe.Goal := by
  intro w xs dict keys hkeys
  apply List.map_congr_left
  intro i hi
  have hfetch := fetch_eq_value w xs i (hkeys i hi)
  rw [hfetch]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_NetworkBitOrder_DictionaryProbe
