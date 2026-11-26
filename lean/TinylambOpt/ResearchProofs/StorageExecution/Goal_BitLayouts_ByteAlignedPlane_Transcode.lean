import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedPlane_Transcode

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedPlane

-- Small-input truth checks (transcode round-trips through the physical layout).
#eval pack 4 ((List.range 3).map (fun i => fetch layout 3 4 i (atByte (file layout 4 [1, 2, 3])))) == pack 4 [1, 2, 3]
#eval pack 5 ((List.range 9).map (fun i => fetch layout 9 5 i (atByte (file layout 5 [31, 0, 7, 255, 16, 3, 9, 2, 77])))) == pack 5 [31, 0, 7, 255, 16, 3, 9, 2, 77]
#eval pack 0 ((List.range 3).map (fun i => fetch layout 3 0 i (atByte (file layout 0 [1, 2, 3])))) == pack 0 [1, 2, 3]

/-- `align 8 n` rounds `n` up to a multiple of eight, hence is at least `n`. -/
private theorem align8_ge (n : Nat) : n ≤ align 8 n := by
  unfold align
  have hdiv := Nat.div_add_mod (n + 7) 8
  rw [Nat.mul_comm 8 ((n + 7) / 8)] at hdiv
  have hmod := Nat.mod_lt (n + 7) (by omega : 0 < 8)
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

/-- One value's lanes written at plane positions `b*A + i`. -/
private def laneFold (w A i : Nat) (x a : Nat) : Nat :=
  (lanes w).foldl (fun acc b => if x.testBit b then acc ||| 2 ^ (b * A + i) else acc) a

private theorem laneFold_testBit (w A i x a q : Nat) :
    (laneFold w A i x a).testBit q =
      (a.testBit q || (lanes w).any (fun z => x.testBit z && decide (z * A + i = q))) := by
  unfold laneFold
  apply testBit_foldl_generic
  intro acc z r
  by_cases hz : x.testBit z = true
  · simp [hz, Nat.testBit_or, Nat.testBit_two_pow]
  · simp [hz]

/-- The rows of a byte-aligned plane occupy disjoint intervals `[b*A, b*A+n)`. -/
private theorem lane_pos_eq (A n z b i j : Nat) (_hApos : 0 < A) (hnA : n ≤ A)
    (hi : i < n) (hj : j < n) (h : z * A + i = b * A + j) : z = b ∧ i = j := by
  have hiA : i < A := by omega
  have hjA : j < A := by omega
  by_cases hzb : z = b
  · subst b
    exact ⟨rfl, by omega⟩
  · rcases Nat.lt_or_gt_of_ne hzb with hlt | hgt
    · have hle : (z + 1) * A ≤ b * A := Nat.mul_le_mul_right A (Nat.succ_le_of_lt hlt)
      have hsucc : (z + 1) * A = z * A + A := by rw [Nat.add_mul, Nat.one_mul]
      omega
    · have hle : (b + 1) * A ≤ z * A := Nat.mul_le_mul_right A (Nat.succ_le_of_lt hgt)
      have hsucc : (b + 1) * A = b * A + A := by rw [Nat.add_mul, Nat.one_mul]
      omega

/-- `image` with plane stride `A = align 8 n` instead of the row-major stride. -/
private def imageFrom (w A off : Nat) (xs : List Nat) (a : Nat) : Nat :=
  (xs.zipIdx off).foldl (fun acc p => laneFold w A p.2 p.1 acc) a

private theorem zipIdx_ge {xs : List Nat} {k : Nat} {p : Nat × Nat} (hp : p ∈ xs.zipIdx k) : k ≤ p.2 := by
  induction xs generalizing k with
  | nil => simp at hp
  | cons x xs ih =>
      simp only [List.zipIdx_cons, List.mem_cons] at hp
      rcases hp with rfl | hp
      · simp
      · have := ih hp
        omega

private theorem imageFrom_testBit (w A off i b : Nat) (xs : List Nat) (a : Nat)
    (hAidx : off + xs.length ≤ A) (hApos : 0 < A) (hioff : off ≤ i) (hi : i < off + xs.length) (hb : b < w) :
    (imageFrom w A off xs a).testBit (b * A + i) =
      (a.testBit (b * A + i) || (xs[i - off]?.getD 0).testBit b) := by
  induction xs generalizing off a i with
  | nil => simp at hi; omega
  | cons x xs ih =>
      simp only [List.length_cons] at hAidx hi
      unfold imageFrom
      rw [List.zipIdx_cons, List.foldl_cons]
      simp
      change (imageFrom w A (off + 1) xs (laneFold w A off x a)).testBit (b * A + i) =
        (a.testBit (b * A + i) || ((x :: xs)[i - off]?.getD 0).testBit b)
      by_cases hcase : i = off
      · subst i
        unfold imageFrom
        rw [testBit_foldl_generic
          (step := fun acc (p : Nat × Nat) => laneFold w A p.2 p.1 acc)
          (contrib := fun (p : Nat × Nat) q => (lanes w).any (fun z => p.1.testBit z && decide (z * A + p.2 = q)))
          (hstep := by
            intro acc p r
            exact laneFold_testBit w A p.2 p.1 acc r)]
        have htail : (xs.zipIdx (off + 1)).any (fun p =>
            (lanes w).any (fun z => p.1.testBit z && decide (z * A + p.2 = b * A + off))) = false := by
          rw [List.any_eq_false]
          intro p hp hptrue
          have hge : off + 1 ≤ p.2 := zipIdx_ge hp
          have hplt : p.2 < off + 1 + xs.length := by
            have hlt := List.snd_lt_of_mem_zipIdx hp
            simpa [List.length_zipIdx, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hlt
          have hpA : p.2 < A := by omega
          rw [List.any_eq_true] at hptrue
          rcases hptrue with ⟨z, hz, hz'⟩
          have hzw : z < w := by simpa [lanes] using hz
          simp only [Bool.and_eq_true] at hz'
          have heq : z * A + p.2 = b * A + off := by simpa using (decide_eq_true_eq.mp hz'.2)
          have hres := lane_pos_eq A A z b p.2 off hApos (by omega) hpA (by omega) heq
          have hp_eq : p.2 = off := hres.2
          omega
        have hhead : (lanes w).any (fun z => x.testBit z && decide (z * A + off = b * A + off)) = x.testBit b := by
          by_cases hxb : x.testBit b = true
          · rw [hxb, List.any_eq_true]
            refine ⟨b, by simp [lanes, hb], ?_⟩
            simp [hxb]
          · have hxb' : x.testBit b = false := by cases h : x.testBit b <;> simp_all
            rw [hxb', List.any_eq_false]
            intro z hz hz'
            have hzw : z < w := by simpa [lanes] using hz
            simp only [Bool.and_eq_true] at hz'
            have heq : z * A + off = b * A + off := by simpa using (decide_eq_true_eq.mp hz'.2)
            have hres := lane_pos_eq A A z b off off hApos (by omega) (by omega) (by omega) heq
            have hzb : z = b := hres.1
            subst z
            exact hxb hz'.1
        rw [laneFold_testBit, hhead, htail]
        simp
      · have higt : off < i := by omega
        have hheadfalse : (lanes w).any (fun z => x.testBit z && decide (z * A + off = b * A + i)) = false := by
          rw [List.any_eq_false]
          intro z hz hz'
          have hzw : z < w := by simpa [lanes] using hz
          simp only [Bool.and_eq_true] at hz'
          have heq : z * A + off = b * A + i := by simpa using (decide_eq_true_eq.mp hz'.2)
          have hres := lane_pos_eq A A z b off i hApos (by omega) (by omega) (by omega) heq
          have hi_eq : off = i := hres.2
          omega
        have ha' : (laneFold w A off x a).testBit (b * A + i) = a.testBit (b * A + i) := by
          rw [laneFold_testBit, hheadfalse, Bool.or_false]
        have htail := ih (off := off + 1) (a := laneFold w A off x a) (i := i) (by omega) (by omega) (by omega)
        unfold imageFrom at htail
        rw [ha'] at htail
        unfold imageFrom
        rw [htail]
        have hidx : i - off = (i - (off + 1)) + 1 := by omega
        rw [hidx]
        simp

private theorem image_eq_imageFrom (w : Nat) (xs : List Nat) :
    image layout w xs = imageFrom w (align 8 xs.length) 0 xs 0 := by
  simp [image, imageFrom, laneFold, layout]

private theorem atByte_little (n k j : Nat) (hj : j < k) :
    atByte (little n k) j = (n / 256 ^ j) % 256 := by
  unfold atByte little
  have hj'' : j < ((List.range k).map (fun i => (n / 256 ^ i) % 256)).length := by
    simpa [List.length_map, List.length_range] using hj
  rw [List.getElem?_eq_getElem hj'']
  simp

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

private theorem bit_bound (n w i b : Nat) (hi : i < n) (hb : b < w) :
    layout.bit n w i b < 8 * byteCount (layout.capacity n w) := by
  simp only [layout, byteCount]
  have hnA : n ≤ align 8 n := align8_ge n
  have hiA : i < align 8 n := by omega
  have hb1 : b + 1 ≤ w := by omega
  have hle : (b + 1) * align 8 n ≤ w * align 8 n := Nat.mul_le_mul_right (align 8 n) hb1
  have hsucc : (b + 1) * align 8 n = b * align 8 n + align 8 n := by rw [Nat.add_mul, Nat.one_mul]
  have hbit_lt : b * align 8 n + i < w * align 8 n := by omega
  have hcap : w * align 8 n ≤ 8 * ((w * align 8 n + 7) / 8) := by omega
  omega

/-- The physical layout is lossless; in particular the fetched lanes recover each value mod `2^w`. -/
private theorem lossless_plane : lossless layout := by
  intro w xs i hi
  unfold fetch lanes
  dsimp only
  rw [← sum_testBit_eq_mod (xs[i]?.getD 0) w]
  apply congrArg List.sum
  apply List.map_congr_left
  intro b hb
  have hb' : b < w := by simpa [List.mem_range] using hb
  have hbit := bit_bound xs.length w i b hi hb'
  have hbyte : layout.bit xs.length w i b / 8 < byteCount (layout.capacity xs.length w) := by omega
  have hrem : layout.bit xs.length w i b % 8 < 8 := Nat.mod_lt _ (by decide)
  have hread : (atByte (file layout w xs) (layout.bit xs.length w i b / 8)).testBit
        (layout.bit xs.length w i b % 8) = (image layout w xs).testBit (layout.bit xs.length w i b) := by
    rw [file_testBit layout w xs (layout.bit xs.length w i b / 8) (layout.bit xs.length w i b % 8) hbyte hrem]
    congr 1
    have hdm := Nat.div_add_mod (layout.bit xs.length w i b) 8
    omega
  rw [hread, image_eq_imageFrom]
  simp only [layout]
  have hApos : 0 < align 8 xs.length := by
    have hnA := align8_ge xs.length
    omega
  have himg := imageFrom_testBit w (align 8 xs.length) 0 i b xs 0 (by simpa using align8_ge xs.length) hApos (by omega) (by simpa using hi) hb'
  simp [himg, hi]

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

theorem sound_Transcode : Transcode.Goal := by
  intro w xs
  have hfetch : (List.range xs.length).map
      (fun i => fetch layout xs.length w i (atByte (file layout w xs))) =
      xs.map (fun x => x % 2 ^ w) := by
    apply List.ext_getElem?
    intro k
    by_cases hk : k < xs.length
    · simp [hk, lossless_plane w xs k hk]
    · simp [hk]
  rw [hfetch]
  unfold pack
  rw [word_congr_mod w (xs.map (fun x => x % 2 ^ w)) xs]
  · simp
  · simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedPlane_Transcode
