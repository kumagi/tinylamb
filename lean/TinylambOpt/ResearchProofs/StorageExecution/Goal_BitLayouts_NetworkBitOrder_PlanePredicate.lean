import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_NetworkBitOrder_PlanePredicate

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.NetworkBitOrder

/-! The physical cell of tight-stream position `p = i*w+b` is `brev p`, the
bit-reversal of `p` inside its byte. `brev` is an involution on byte cells, so
reading the physical bit at `brev p` from the serialized image recovers
exactly the bit that the writer put there for `(i,b)`. -/

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

theorem sound_PlanePredicate : NetworkBitOrder.PlanePredicate.Goal := by
  intro w xs b hb
  unfold planeRows
  dsimp [layout, brev]
  unfold file
  apply List.filter_congr
  intro i hi
  simp only [List.mem_range] at hi
  let cap := align 8 (xs.length*w)
  have hcap_mod : cap % 8 = 0 := by
    dsimp [cap, align]
    omega
  have hcap_ge : xs.length*w ≤ cap := by
    dsimp [cap, align]
    omega
  have hbytes : 8 * byteCount cap = cap := by
    dsimp [byteCount]
    omega
  have hp_lt : i*w + b < xs.length*w := by
    have hi' : i + 1 ≤ xs.length := Nat.succ_le_of_lt hi
    have hmul : (i+1)*w ≤ xs.length*w := Nat.mul_le_mul_right w hi'
    have hp_le : i*w + b < (i+1)*w := by
      rw [Nat.succ_mul]
      omega
    omega
  have hp_lt_cap : i*w + b < cap := Nat.lt_of_lt_of_le hp_lt hcap_ge
  have hq_lt_cap : brev (i*w+b) < cap := by
    dsimp [brev]
    omega
  have hq : brev (i*w+b) < 8 * byteCount cap := by omega
  change (atByte (little (image layout w xs) (byteCount cap)) (brev (i*w+b)/8)).testBit
      (brev (i*w+b)%8) = (xs[i]?.getD 0).testBit b
  rw [file_bit (image layout w xs) (byteCount cap) (brev (i*w+b)) hq]
  rw [image_eq_imgAcc]
  have ht := imgAcc_bit w 0 0 xs i b hb
  simp at ht
  rw [ht]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_NetworkBitOrder_PlanePredicate
