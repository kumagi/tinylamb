import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `NetworkBitOrder.Transcode` — Convert from this physical order to tight row-major
  bytes using lane reads.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_NetworkBitOrder_Transcode

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.NetworkBitOrder

-- Small-input truth checks for the transcode round-trip.
#eval pack 4 ((List.range 3).map (fun i => fetch layout 3 4 i (atByte (file layout 4 [1, 2, 3])))) == pack 4 [1, 2, 3]
#eval pack 5 ((List.range 9).map (fun i => fetch layout 9 5 i (atByte (file layout 5 [31, 0, 7, 255, 16, 3, 9, 2, 77])))) == pack 5 [31, 0, 7, 255, 16, 3, 9, 2, 77]
#eval pack 0 ((List.range 3).map (fun i => fetch layout 3 0 i (atByte (file layout 0 [1, 2, 3])))) == pack 0 [1, 2, 3]
#eval pack 1 ((List.range 8).map (fun i => fetch layout 8 1 i (atByte (file layout 1 [1, 0, 1, 1, 0, 0, 1, 1])))) == pack 1 [1, 0, 1, 1, 0, 0, 1, 1]

/-- Bit reversal inside one byte. -/
private def rev8 (p : Nat) : Nat := p / 8 * 8 + (7 - p % 8)

private theorem rev8_involutive (p : Nat) : rev8 (rev8 p) = p := by
  unfold rev8
  have hp := Nat.div_add_mod p 8
  have hr : p % 8 < 8 := Nat.mod_lt _ (by decide : 0 < 8)
  omega

private theorem rev8_inj {a b : Nat} (h : rev8 a = rev8 b) : a = b := by
  have h' := congrArg rev8 h
  simpa [rev8_involutive] using h'

private theorem testBit_foldl_generic {α : Type} (step : Nat → α → Nat) (contrib : α → Nat → Bool)
    (hstep : ∀ a z q, (step a z).testBit q = (a.testBit q || contrib z q))
    (ps : List α) (a q : Nat) :
    (ps.foldl step a).testBit q = (a.testBit q || ps.any (fun z => contrib z q)) := by
  induction ps generalizing a with
  | nil => simp
  | cons z zs ih =>
      rw [List.foldl_cons, ih, hstep]
      simp [Bool.or_assoc]

/-- One value's lanes written at the byte-reversed positions. -/
private def laneFold (w off : Nat) (x a : Nat) : Nat :=
  (lanes w).foldl (fun acc b => if x.testBit b then acc ||| 2 ^ rev8 (off * w + b) else acc) a

private theorem laneFold_testBit (w off x a q : Nat) :
    (laneFold w off x a).testBit q =
      (a.testBit q || (lanes w).any (fun z => x.testBit z && decide (rev8 (off * w + z) = q))) := by
  unfold laneFold
  apply testBit_foldl_generic
  intro acc z r
  by_cases hz : x.testBit z = true
  · simp [hz, Nat.testBit_or, Nat.testBit_two_pow]
  · simp [hz]

private theorem lanes_rev_any_same (w off b : Nat) (x : Nat) (hb : b < w) :
    (lanes w).any (fun z => x.testBit z && decide (rev8 (off * w + z) = rev8 (off * w + b))) =
      x.testBit b := by
  by_cases hxb : x.testBit b = true
  · rw [hxb, List.any_eq_true]
    refine ⟨b, by simp [lanes, hb], ?_⟩
    simp [hxb]
  · have hxb' : x.testBit b = false := by cases h : x.testBit b <;> simp_all
    rw [hxb', List.any_eq_false]
    intro z hz hz'
    have hzw : z < w := by simpa [lanes] using hz
    simp only [Bool.and_eq_true] at hz'
    have heq0 : rev8 (off * w + z) = rev8 (off * w + b) := by simpa using (decide_eq_true_eq.mp hz'.2)
    have heq : off * w + z = off * w + b := rev8_inj heq0
    have hzb : z = b := by omega
    subst z
    exact hxb hz'.1

private def imageFrom (w off : Nat) (xs : List Nat) (a : Nat) : Nat :=
  (xs.zipIdx off).foldl (fun acc p => laneFold w p.2 p.1 acc) a

private theorem zipIdx_ge {xs : List Nat} {k : Nat} {p : Nat × Nat} (hp : p ∈ xs.zipIdx k) : k ≤ p.2 := by
  induction xs generalizing k with
  | nil => simp at hp
  | cons x xs ih =>
      simp only [List.zipIdx_cons, List.mem_cons] at hp
      rcases hp with rfl | hp
      · simp
      · have := ih hp
        omega

private theorem succ_mul (k w : Nat) : (k + 1) * w = k * w + w := by
  rw [Nat.add_mul, Nat.one_mul]

private theorem imageFrom_testBit (w off i b : Nat) (xs : List Nat) (a : Nat)
    (hioff : off ≤ i) (hi : i < off + xs.length) (hb : b < w) :
    (imageFrom w off xs a).testBit (rev8 (i * w + b)) =
      (a.testBit (rev8 (i * w + b)) || (xs[i - off]?.getD 0).testBit b) := by
  induction xs generalizing off a i with
  | nil => simp at hi; omega
  | cons x xs ih =>
      simp only [List.length_cons] at hi
      unfold imageFrom
      rw [List.zipIdx_cons, List.foldl_cons]
      simp
      change (imageFrom w (off + 1) xs (laneFold w off x a)).testBit (rev8 (i * w + b)) =
        (a.testBit (rev8 (i * w + b)) || ((x :: xs)[i - off]?.getD 0).testBit b)
      by_cases hcase : i = off
      · subst i
        unfold imageFrom
        rw [testBit_foldl_generic
          (step := fun acc (p : Nat × Nat) => laneFold w p.2 p.1 acc)
          (contrib := fun (p : Nat × Nat) q =>
            (lanes w).any (fun z => p.1.testBit z && decide (rev8 (p.2 * w + z) = q)))
          (hstep := by
            intro acc p r
            exact laneFold_testBit w p.2 p.1 acc r)]
        have htail : (xs.zipIdx (off + 1)).any (fun p =>
            (lanes w).any (fun z => p.1.testBit z && decide (rev8 (p.2 * w + z) = rev8 (off * w + b)))) = false := by
          rw [List.any_eq_false]
          intro p hp hptrue
          have hge := zipIdx_ge hp
          rw [List.any_eq_true] at hptrue
          rcases hptrue with ⟨z, hz, hz'⟩
          have hzw : z < w := by simpa [lanes] using hz
          simp only [Bool.and_eq_true] at hz'
          have heq0 : rev8 (p.2 * w + z) = rev8 (off * w + b) := by simpa using (decide_eq_true_eq.mp hz'.2)
          have heq : p.2 * w + z = off * w + b := rev8_inj heq0
          have hlt : off * w + z < off * w + w := by omega
          have hle : off * w + w ≤ p.2 * w := by
            rw [← succ_mul off w]
            exact Nat.mul_le_mul_right w hge
          omega
        rw [laneFold_testBit, lanes_rev_any_same w off b x hb, htail]
        simp [Nat.sub_self]
      · have higt : off < i := Nat.lt_of_le_of_ne hioff (by intro h; apply hcase; omega)
        have hheadfalse : (lanes w).any (fun z => x.testBit z && decide (rev8 (off * w + z) = rev8 (i * w + b))) = false := by
          rw [List.any_eq_false]
          intro z hz hz'
          have hzw : z < w := by simpa [lanes] using hz
          simp only [Bool.and_eq_true] at hz'
          have heq0 : rev8 (off * w + z) = rev8 (i * w + b) := by simpa using (decide_eq_true_eq.mp hz'.2)
          have heq : off * w + z = i * w + b := rev8_inj heq0
          have hlt1 : off * w + z < off * w + w := by omega
          have hle : off * w + w ≤ i * w := by
            rw [← succ_mul off w]
            exact Nat.mul_le_mul_right w (by omega)
          omega
        have ha' : (laneFold w off x a).testBit (rev8 (i * w + b)) = a.testBit (rev8 (i * w + b)) := by
          rw [laneFold_testBit, hheadfalse, Bool.or_false]
        have htail := ih (off := off + 1) (a := laneFold w off x a) (i := i) (by omega) (by omega)
        unfold imageFrom at htail
        rw [ha'] at htail
        unfold imageFrom
        rw [htail]
        have hidx : i - off = (i - (off + 1)) + 1 := by omega
        rw [hidx]
        simp

private theorem image_eq_imageFrom (w : Nat) (xs : List Nat) :
    image layout w xs = imageFrom w 0 xs 0 := by
  simp [image, imageFrom, laneFold, layout, rev8]

private theorem atByte_little (n k j : Nat) (hj : j < k) :
    atByte (little n k) j = (n / 256 ^ j) % 256 := by
  unfold atByte little
  rw [List.getElem?_eq_getElem (by simpa using hj)]
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
  let p := i * w + b
  let c := align 8 (n * w)
  have hp_lt : p < n * w := by
    dsimp [p]
    have hsucc : (i + 1) * w = i * w + w := by rw [Nat.add_mul, Nat.one_mul]
    have hle : (i + 1) * w ≤ n * w := Nat.mul_le_mul_right w (Nat.succ_le_of_lt hi)
    omega
  have hnc : n * w ≤ c := by
    dsimp [c]
    unfold align
    have hdiv := Nat.div_add_mod (n * w + 7) 8
    have hmod := Nat.mod_lt (n * w + 7) (by decide : 0 < 8)
    omega
  have hc : c ≤ 8 * ((c + 7) / 8) := by omega
  change p / 8 * 8 + (7 - p % 8) < 8 * ((c + 7) / 8)
  omega

/-- Byte-reversed physical order is still lossless. -/
private theorem lossless_nbo : lossless layout := by
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
  have hrem : layout.bit xs.length w i b % 8 < 8 := Nat.mod_lt _ (by decide : 0 < 8)
  have hread : (atByte (file layout w xs) (layout.bit xs.length w i b / 8)).testBit
        (layout.bit xs.length w i b % 8) = (image layout w xs).testBit (layout.bit xs.length w i b) := by
    rw [file_testBit layout w xs (layout.bit xs.length w i b / 8) (layout.bit xs.length w i b % 8) hbyte hrem]
    congr 1
    have hdm := Nat.div_add_mod (layout.bit xs.length w i b) 8
    omega
  rw [hread, image_eq_imageFrom]
  have hpos : layout.bit xs.length w i b = rev8 (i * w + b) := by
    rfl
  rw [hpos]
  have himg := imageFrom_testBit w 0 i b xs 0 (by omega) (by simpa using hi) hb'
  rw [himg]
  simp [hi]

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
    · simp [hk, lossless_nbo w xs k hk]
    · simp [hk]
  rw [hfetch]
  unfold pack
  rw [word_congr_mod w (xs.map (fun x => x % 2 ^ w)) xs]
  · simp
  · simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_NetworkBitOrder_Transcode
