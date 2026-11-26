import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_Transcode

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow

/-- `align 8 w` rounds `w` up to a multiple of eight, hence is at least `w`. -/
private theorem align8_ge (w : Nat) : w ≤ align 8 w := by
  unfold align
  have hdiv := Nat.div_add_mod (w + 7) 8
  rw [Nat.mul_comm 8 ((w + 7) / 8)] at hdiv
  have hmod := Nat.mod_lt (w + 7) (by omega : 0 < 8)
  omega

private theorem testBit_foldl_generic {α : Type} (step : Nat → α → Nat) (contrib : α → Nat → Bool)
    (hstep : ∀ a z q, (step a z).testBit q = (a.testBit q || contrib z q))
    (ps : List α) (a q : Nat) :
    (ps.foldl step a).testBit q = (a.testBit q || ps.any (fun z => contrib z q)) := by
  induction ps generalizing a with
  | nil => simp
  | cons z zs ih =>
      rw [List.foldl_cons, ih, hstep]
      simp [Bool.or_assoc]

private def laneFold (w off : Nat) (x a : Nat) : Nat :=
  (lanes w).foldl (fun acc b => if x.testBit b then acc ||| 2 ^ (off + b) else acc) a

private theorem laneFold_testBit (w off x a q : Nat) :
    (laneFold w off x a).testBit q =
      (a.testBit q || (lanes w).any (fun b => x.testBit b && decide (off + b = q))) := by
  unfold laneFold
  apply testBit_foldl_generic
  intro acc b r
  by_cases hb : x.testBit b = true
  · simp [hb, Nat.testBit_or, Nat.testBit_two_pow]
  · simp [hb]

/-- `image` with row stride `A = align 8 w` instead of `w`. -/
private def imageFrom (w A off : Nat) (xs : List Nat) (a : Nat) : Nat :=
  (xs.zipIdx off).foldl (fun acc p => laneFold w (p.2 * A) p.1 acc) a

private theorem zipIdx_ge {xs : List Nat} {k : Nat} {p : Nat × Nat} (hp : p ∈ xs.zipIdx k) : k ≤ p.2 := by
  induction xs generalizing k with
  | nil => simp at hp
  | cons x xs ih =>
      simp only [List.zipIdx_cons, List.mem_cons] at hp
      rcases hp with rfl | hp
      · simp
      · have := ih hp
        omega

private theorem lanes_any_same (w off b : Nat) (x : Nat) (hb : b < w) :
    (lanes w).any (fun z => x.testBit z && decide (off + z = off + b)) = x.testBit b := by
  by_cases hxb : x.testBit b = true
  · have htrue : (lanes w).any (fun z => x.testBit z && decide (off + z = off + b)) = true := by
      rw [List.any_eq_true]
      refine ⟨b, by simp [lanes, hb], ?_⟩
      simp [hxb]
    rw [hxb]
    exact htrue
  · have hxb' : x.testBit b = false := by cases h : x.testBit b <;> simp_all
    have hfalse : (lanes w).any (fun z => x.testBit z && decide (off + z = off + b)) = false := by
      rw [List.any_eq_false]
      intro z hz hz'
      have hzw : z < w := by simpa [lanes] using hz
      simp only [Bool.and_eq_true] at hz'
      have heq0 : off + z = off + b := by simpa using (decide_eq_true_eq.mp hz'.2)
      have hzb : z = b := by omega
      subst z
      exact hxb hz'.1
    rw [hxb']
    exact hfalse

private theorem succ_mul (k A : Nat) : (k + 1) * A = k * A + A := by
  rw [Nat.add_mul, Nat.one_mul]

private theorem imageFrom_testBit (w A off i b : Nat) (xs : List Nat) (a : Nat)
    (hAw : w ≤ A) (hbA : b < A) (hioff : off ≤ i) (hi : i < off + xs.length) (hb : b < w) :
    (imageFrom w A off xs a).testBit (i * A + b) =
      (a.testBit (i * A + b) || (xs[i - off]?.getD 0).testBit b) := by
  induction xs generalizing off a i with
  | nil => simp at hi; omega
  | cons x xs ih =>
      simp only [List.length_cons] at hi
      unfold imageFrom
      rw [List.zipIdx_cons, List.foldl_cons]
      simp
      change (imageFrom w A (off + 1) xs (laneFold w (off * A) x a)).testBit (i * A + b) =
        (a.testBit (i * A + b) || ((x :: xs)[i - off]?.getD 0).testBit b)
      by_cases hcase : i = off
      · subst i
        unfold imageFrom
        rw [testBit_foldl_generic
          (step := fun acc (p : Nat × Nat) => laneFold w (p.2 * A) p.1 acc)
          (contrib := fun (p : Nat × Nat) q => (lanes w).any (fun z => p.1.testBit z && decide (p.2 * A + z = q)))
          (hstep := by
            intro acc p r
            exact laneFold_testBit w (p.2 * A) p.1 acc r)]
        have htail : (xs.zipIdx (off + 1)).any (fun p =>
            (lanes w).any (fun z => p.1.testBit z && decide (p.2 * A + z = off * A + b))) = false := by
          rw [List.any_eq_false]
          intro p hp hptrue
          have hge := zipIdx_ge hp
          rw [List.any_eq_true] at hptrue
          rcases hptrue with ⟨z, hz, hz'⟩
          have hzw : z < w := by simpa [lanes] using hz
          have hzA : z < A := by omega
          simp only [Bool.and_eq_true] at hz'
          have heq : p.2 * A + z = off * A + b := by simpa using (decide_eq_true_eq.mp hz'.2)
          have hmul := succ_mul off A
          have hlt : off * A + z < (off + 1) * A := by rw [hmul]; omega
          have hle : (off + 1) * A ≤ p.2 * A := Nat.mul_le_mul_right A hge
          omega
        rw [laneFold_testBit, lanes_any_same w (off * A) b x hb, htail]
        simp [Nat.sub_self]
      · have higt : off < i := Nat.lt_of_le_of_ne hioff (by intro h; apply hcase; omega)
        have hheadfalse : (lanes w).any (fun z => x.testBit z && decide (off * A + z = i * A + b)) = false := by
          rw [List.any_eq_false]
          intro z hz hz'
          have hzw : z < w := by simpa [lanes] using hz
          have hzA : z < A := by omega
          simp only [Bool.and_eq_true] at hz'
          have heq : off * A + z = i * A + b := by simpa using (decide_eq_true_eq.mp hz'.2)
          have hmul := succ_mul off A
          have hlt1 : off * A + z < (off + 1) * A := by rw [hmul]; omega
          have hle : (off + 1) * A ≤ i * A := Nat.mul_le_mul_right A (by omega)
          omega
        have ha' : (laneFold w (off * A) x a).testBit (i * A + b) = a.testBit (i * A + b) := by
          rw [laneFold_testBit, hheadfalse, Bool.or_false]
        have htail := ih (off := off + 1) (a := laneFold w (off * A) x a) (i := i) (by omega) (by omega)
        unfold imageFrom at htail
        rw [ha'] at htail
        unfold imageFrom
        rw [htail]
        have hidx : i - off = (i - (off + 1)) + 1 := by omega
        rw [hidx]
        simp

private theorem image_eq_imageFrom (w : Nat) (xs : List Nat) :
    image layout w xs = imageFrom w (align 8 w) 0 xs 0 := by
  simp [image, imageFrom, laneFold, layout]

private theorem atByte_little (v k j : Nat) (hj : j < k) :
    atByte (little v k) j = (v / 256 ^ j) % 256 := by
  unfold atByte little
  rw [List.getElem?_eq_getElem (by simpa using hj)]
  simp

private theorem byte_testBit (v j r : Nat) (hr : r < 8) :
    ((v / 256 ^ j) % 256).testBit r = v.testBit (8 * j + r) := by
  have h256 : (256 : Nat) = 2 ^ 8 := by decide
  have hpow : 256 ^ j = 2 ^ (8 * j) := by rw [h256, Nat.pow_mul]
  rw [hpow, h256]
  rw [Nat.testBit_mod_two_pow]
  simp [hr]
  rw [← Nat.testBit_add]
  congr 1 <;> omega

private theorem sum_powers_lt (f : Nat → Bool) (w : Nat) :
    ((lanes w).map (fun z => if f z then 2 ^ z else 0)).sum < 2 ^ w := by
  induction w with
  | zero => simp [lanes]
  | succ w ih =>
      simp only [lanes, List.range_succ, List.map_append, List.sum_append,
        List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero] at ih ⊢
      by_cases hfw : f w = true
      · simp [hfw]
        rw [Nat.pow_succ']
        omega
      · simp [hfw]
        exact Nat.lt_trans ih (Nat.pow_lt_pow_right (by decide : 1 < 2) (by omega))

private theorem testBit_sum_powers (f : Nat → Bool) (w b : Nat) (hb : b < w) :
    ((lanes w).map (fun z => if f z then 2 ^ z else 0)).sum.testBit b = f b := by
  induction w with
  | zero => omega
  | succ w ih =>
      simp only [lanes, List.range_succ, List.map_append, List.sum_append,
        List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
      by_cases hbw : b < w
      · have hi := ih hbw
        by_cases hfw : f w = true
        · simp [hfw]
          rw [Nat.add_comm, Nat.testBit_two_pow_add_gt hbw]
          exact hi
        · simp [hfw, show ((List.range w).map (fun z => if f z then 2 ^ z else 0)).sum.testBit b = f b by simpa [lanes] using hi]
      · have hb_eq : b = w := by omega
        subst b
        by_cases hfw : f w = true
        · simp [hfw]
          rw [Nat.add_comm]
          have hlt := sum_powers_lt f w
          have hdiv : ((List.range w).map (fun z => if f z then 2 ^ z else 0)).sum / 2 ^ w = 0 := Nat.div_eq_of_lt hlt
          have hdivsum : (2 ^ w + ((List.range w).map (fun z => if f z then 2 ^ z else 0)).sum) / 2 ^ w = 1 := by
            apply Nat.div_eq_of_lt_le
            · omega
            · have hlt' : ((List.range w).map (fun z => if f z then 2 ^ z else 0)).sum < 2 ^ w := by simpa [lanes] using hlt
              omega
          rw [show w = 0 + w by omega, Nat.testBit_add]
          simp only [Nat.zero_add]
          rw [hdivsum]
          simp
        · simp [hfw]
          have hlt := sum_powers_lt f w
          exact Nat.testBit_lt_two_pow (by simpa [lanes] using hlt)

private theorem fetch_bit (w : Nat) (xs : List Nat) (i b : Nat) (hi : i < xs.length) (hb : b < w) :
    (atByte (file layout w xs) ((i * align 8 w + b) / 8)).testBit ((i * align 8 w + b) % 8) =
      (image layout w xs).testBit (i * align 8 w + b) := by
  let p := i * align 8 w + b
  let V := image layout w xs
  let k := byteCount (xs.length * align 8 w)
  have hAw : w ≤ align 8 w := align8_ge w
  have hbA : b < align 8 w := by omega
  have hp_lt : p < xs.length * align 8 w := by
    dsimp [p]
    have hsucc : (i + 1) * align 8 w = i * align 8 w + align 8 w := by rw [Nat.add_mul, Nat.one_mul]
    have hle : (i + 1) * align 8 w ≤ xs.length * align 8 w := Nat.mul_le_mul_right (align 8 w) (Nat.succ_le_of_lt hi)
    omega
  have hj : p / 8 < k := by
    dsimp [k]
    unfold byteCount
    omega
  have hfile : file layout w xs = little V k := by
    rfl
  rw [hfile, atByte_little V k (p / 8) hj]
  rw [byte_testBit V (p / 8) (p % 8) (Nat.mod_lt _ (by decide : 0 < 8))]
  have hdecomp : 8 * (p / 8) + p % 8 = p := by
    have hdm := Nat.div_add_mod p 8
    omega
  rw [hdecomp]

private theorem lossless_bytealignedrow : lossless layout := by
  intro w xs i hi
  have hAw : w ≤ align 8 w := align8_ge w
  apply Nat.eq_of_testBit_eq
  intro b
  by_cases hb : b < w
  · have hbA : b < align 8 w := by omega
    have hfetch : (fetch layout xs.length w i (atByte (file layout w xs))).testBit b =
        (image layout w xs).testBit (i * align 8 w + b) := by
      have hsum := testBit_sum_powers
        (fun z => (atByte (file layout w xs) ((i * align 8 w + z) / 8)).testBit ((i * align 8 w + z) % 8)) w b hb
      have hfeq : fetch layout xs.length w i (atByte (file layout w xs)) =
          ((lanes w).map (fun z =>
            if (atByte (file layout w xs) ((i * align 8 w + z) / 8)).testBit ((i * align 8 w + z) % 8)
            then 2 ^ z else 0)).sum := by
        rfl
      rw [hfeq]
      exact hsum.trans (fetch_bit w xs i b hi hb)
    rw [hfetch, image_eq_imageFrom]
    have himg := imageFrom_testBit w (align 8 w) 0 i b xs 0 hAw hbA (by omega) (by simpa using hi) hb
    rw [himg]
    have hmod : (xs[i]?.getD 0 % 2 ^ w).testBit b = (xs[i]?.getD 0).testBit b := by
      rw [Nat.testBit_mod_two_pow]
      simp [hb]
    rw [hmod]
    simp
  · have hlt := sum_powers_lt
      (fun z => (atByte (file layout w xs) ((i * align 8 w + z) / 8)).testBit ((i * align 8 w + z) % 8)) w
    have hfetchlt : fetch layout xs.length w i (atByte (file layout w xs)) < 2 ^ w := by
      have hfeq : fetch layout xs.length w i (atByte (file layout w xs)) =
          ((lanes w).map (fun z =>
            if (atByte (file layout w xs) ((i * align 8 w + z) / 8)).testBit ((i * align 8 w + z) % 8)
            then 2 ^ z else 0)).sum := by
        rfl
      rw [hfeq]
      exact hlt
    have hfetchlt_b : fetch layout xs.length w i (atByte (file layout w xs)) < 2 ^ b :=
      Nat.lt_of_lt_of_le hfetchlt (Nat.pow_le_pow_right (by decide : 0 < 2) (by omega))
    rw [Nat.testBit_lt_two_pow hfetchlt_b]
    rw [Nat.testBit_mod_two_pow]
    simp [hb]

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

theorem sound_Transcode : ByteAlignedRow.Transcode.Goal := by
  intro w xs
  have hfetch : (List.range xs.length).map
      (fun i => fetch layout xs.length w i (atByte (file layout w xs))) =
      xs.map (fun x => x % 2 ^ w) := by
    apply List.ext_getElem?
    intro k
    by_cases hk : k < xs.length
    · simp [hk, lossless_bytealignedrow w xs k hk]
    · simp [hk]
  rw [hfetch]
  unfold pack
  rw [word_congr_mod w (xs.map (fun x => x % 2 ^ w)) xs]
  · simp
  · simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_Transcode
