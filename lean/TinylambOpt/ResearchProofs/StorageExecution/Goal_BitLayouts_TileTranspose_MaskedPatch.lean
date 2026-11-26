import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `TileTranspose.MaskedPatch` — Replace one lane by clearing its mask and inserting a
  new value.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_MaskedPatch

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose

/-! ## Generic test-bit infrastructure -/

private theorem testBit_foldl_if_or {α : Type} (l : List α) (f : α → Nat)
    (c : α → Bool) (acc p : Nat) :
    (l.foldl (fun a x => if c x then a ||| f x else a) acc).testBit p =
      (acc.testBit p || l.any (fun x => c x && (f x).testBit p)) := by
  induction l generalizing acc with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons, List.any_cons]
      have hih := ih (if c x then acc ||| f x else acc)
      exact hih.trans (by
        by_cases hc : c x = true
        · simp [hc, Nat.testBit_or, Bool.or_assoc]
        · have hc' : c x = false := by cases h : c x <;> simp_all
          simp [hc'])

private theorem testBit_foldl_or {α : Type} (l : List α) (f : α → Nat)
    (acc p : Nat) :
    (l.foldl (fun a x => a ||| f x) acc).testBit p =
      (acc.testBit p || l.any (fun x => (f x).testBit p)) := by
  simpa using (testBit_foldl_if_or l f (fun _ => true) acc p)

private theorem rowMask_testBit (l : Layout) (n w i p : Nat) :
    (rowMask l n w i).testBit p =
      (lanes w).any (fun b => decide (l.bit n w i b = p)) := by
  unfold rowMask
  simpa [Nat.testBit_two_pow] using
    (testBit_foldl_or (lanes w) (fun b : Nat => 2 ^ l.bit n w i b) 0 p)

private theorem rowBits_testBit (l : Layout) (n w i x p : Nat) :
    (rowBits l n w i x).testBit p =
      (lanes w).any (fun b => x.testBit b && decide (l.bit n w i b = p)) := by
  unfold rowBits
  simpa [Nat.testBit_two_pow] using
    (testBit_foldl_if_or (lanes w) (fun b : Nat => 2 ^ l.bit n w i b)
      (fun b : Nat => x.testBit b) 0 p)

private theorem any_zipIdx_eq_range {α : Type} [OfNat α 0] (xs : List α) (f : α → Nat → Bool) :
    xs.zipIdx.any (fun z => f z.1 z.2) =
      (List.range xs.length).any (fun j => f (xs[j]?.getD 0) j) := by
  apply Bool.eq_iff_iff.mpr
  simp only [List.any_eq_true]
  constructor
  · rintro ⟨⟨x, j⟩, hz, hf⟩
    have hmem := List.mem_zipIdx_iff_getElem?.mp hz
    have hlt : j < xs.length := (List.getElem?_eq_some_iff.mp hmem).1
    have hx : xs[j]?.getD 0 = x := by
      rw [hmem]
      simp
    exact ⟨j, List.mem_range.mpr hlt, by simpa [hx] using hf⟩
  · rintro ⟨j, hj, hf⟩
    have hlt : j < xs.length := List.mem_range.mp hj
    have hmem : xs[j]? = some (xs[j]?.getD 0) := by
      rw [List.getElem?_eq_getElem hlt]
      simp
    exact ⟨(xs[j]?.getD 0, j), List.mem_zipIdx_iff_getElem?.mpr hmem, hf⟩

/-- Conditional OR folding from a nonzero accumulator is the accumulator OR
    the fold from zero. -/
private theorem foldl_if_or_acc (bs : List Nat) (c : Nat → Bool) (f : Nat → Nat)
    (acc : Nat) :
    bs.foldl (fun z b => if c b then z ||| f b else z) acc =
      (acc ||| bs.foldl (fun z b => if c b then z ||| f b else z) 0) := by
  induction bs generalizing acc with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons]
      by_cases hc : c b = true
      · simp only [if_pos hc, Nat.zero_or]
        rw [ih (acc ||| f b), ih (f b)]
        simp [Nat.or_assoc, Nat.or_comm]
      · simp only [if_neg hc, Nat.zero_or]
        exact ih acc

/-- The nested fold used by `image` is homomorphic to OR-ing whole rows. -/
private theorem fold_image (l : Layout) (w n : Nat) (lzs : List (Nat × Nat)) :
    lzs.foldl (fun a z => (lanes w).foldl
        (fun b x => if z.1.testBit x then b ||| 2 ^ l.bit n w z.2 x else b) a) 0 =
      lzs.foldl (fun a z => a ||| (lanes w).foldl
        (fun b x => if z.1.testBit x then b ||| 2 ^ l.bit n w z.2 x else b) 0) 0 := by
  refine List.foldl_hom id ?_
  intro acc z
  exact foldl_if_or_acc (lanes w) (fun b => z.1.testBit b)
    (fun b => 2 ^ l.bit n w z.2 b) acc

private theorem image_eq_foldl_or (l : Layout) (w : Nat) (xs : List Nat) :
    image l w xs =
      xs.zipIdx.foldl (fun acc z => acc ||| rowBits l xs.length w z.2 z.1) 0 := by
  unfold image rowBits
  exact fold_image l w xs.length xs.zipIdx

private theorem image_testBit (l : Layout) (w : Nat) (xs : List Nat) (p : Nat) :
    (image l w xs).testBit p =
      (List.range xs.length).any (fun i => (lanes w).any
        (fun b => (xs[i]?.getD 0).testBit b && decide (l.bit xs.length w i b = p))) := by
  rw [image_eq_foldl_or]
  have hfold : (xs.zipIdx.foldl
      (fun acc z => acc ||| rowBits l xs.length w z.2 z.1) 0).testBit p =
      xs.zipIdx.any (fun z => (rowBits l xs.length w z.2 z.1).testBit p) := by
    simpa using (testBit_foldl_or xs.zipIdx
      (fun z : Nat × Nat => rowBits l xs.length w z.2 z.1) 0 p)
  refine hfold.trans ?_
  calc
    xs.zipIdx.any (fun z => (rowBits l xs.length w z.2 z.1).testBit p)
        = xs.zipIdx.any (fun z => (lanes w).any
            (fun b => z.1.testBit b && decide (l.bit xs.length w z.2 b = p))) := by
              congr 1
              funext z
              exact rowBits_testBit l xs.length w z.2 z.1 p
    _ = (List.range xs.length).any (fun i => (lanes w).any
          (fun b => (xs[i]?.getD 0).testBit b && decide (l.bit xs.length w i b = p))) := by
            exact any_zipIdx_eq_range xs (fun x i => (lanes w).any
              (fun b => x.testBit b && decide (l.bit xs.length w i b = p)))

/-! ## TileTranspose injectivity -/

private theorem injective_bits : ∀ n w i j a b, i < n → j < n → a < w → b < w →
    layout.bit n w i a = layout.bit n w j b → i = j ∧ a = b := by
  intro n w i j a b hi hj ha hb h
  simp only [layout] at h
  have hmod8_i : ((i / 8) * 8 * w + a * 8 + i % 8) % 8 = i % 8 := by
    have h1 : (i / 8) * 8 * w + a * 8 = 8 * ((i / 8) * w + a) := by
      simp [Nat.mul_add, Nat.add_mul, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
    rw [h1, Nat.add_comm, Nat.add_mul_mod_self_left,
      Nat.mod_eq_of_lt (Nat.mod_lt i (by decide))]
  have hmod8_j : ((j / 8) * 8 * w + b * 8 + j % 8) % 8 = j % 8 := by
    have h1 : (j / 8) * 8 * w + b * 8 = 8 * ((j / 8) * w + b) := by
      simp [Nat.mul_add, Nat.add_mul, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
    rw [h1, Nat.add_comm, Nat.add_mul_mod_self_left,
      Nat.mod_eq_of_lt (Nat.mod_lt j (by decide))]
  have hij8 : i % 8 = j % 8 := by
    have := congrArg (· % 8) h
    simpa [hmod8_i, hmod8_j] using this
  have hdiv : (i / 8) * w + a = (j / 8) * w + b := by
    have h1 : (i / 8) * 8 * w + a * 8 + i % 8 =
        8 * ((i / 8) * w + a) + i % 8 := by
      simp [Nat.mul_add, Nat.add_mul, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
    have h2 : (j / 8) * 8 * w + b * 8 + j % 8 =
        8 * ((j / 8) * w + b) + j % 8 := by
      simp [Nat.mul_add, Nat.add_mul, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
    rw [h1, h2, hij8] at h
    omega
  have hw : 0 < w := by omega
  have hab : a = b := by
    have := congrArg (· % w) hdiv
    simpa [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt ha, Nat.mod_eq_of_lt hb] using this
  subst hab
  have ht : i / 8 = j / 8 := by
    exact Nat.eq_of_mul_eq_mul_right hw (Nat.add_right_cancel hdiv)
  have hi_eq : i = j := by
    have h1 := Nat.div_add_mod i 8
    have h2 := Nat.div_add_mod j 8
    omega
  exact ⟨hi_eq, rfl⟩

private theorem nonoverlap_tile : nonoverlap layout := by
  intro n w i j a b hi hj ha hb
  constructor
  · exact injective_bits n w i j a b hi hj ha hb
  · rintro ⟨rfl, rfl⟩
    rfl

/-! ## Reading a known bit cell from the image -/

private theorem image_bit_at (l : Layout) (w : Nat) (xs : List Nat) {i b p : Nat}
    (hi : i < xs.length) (hb : b < w)
    (hno : ∀ j k a c, j < xs.length → k < xs.length → a < w → c < w →
      l.bit xs.length w j a = l.bit xs.length w k c → j = k ∧ a = c)
    (hp : l.bit xs.length w i b = p) :
    (image l w xs).testBit p = (xs[i]?.getD 0).testBit b := by
  rw [image_testBit]
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    obtain ⟨j, hj, hrow⟩ := List.any_eq_true.mp h
    have hjlen : j < xs.length := List.mem_range.mp hj
    obtain ⟨a, ha, hval⟩ := List.any_eq_true.mp hrow
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hval
    rcases hval with ⟨hxa, heq⟩
    have haw : a < w := by simpa [lanes] using ha
    have hji : j = i ∧ a = b := hno j i a b hjlen hi haw hb (heq.trans hp.symm)
    obtain ⟨rfl, rfl⟩ := hji
    simpa using hxa
  · intro h
    rw [List.any_eq_true]
    refine ⟨i, List.mem_range.mpr hi, ?_⟩
    rw [List.any_eq_true]
    refine ⟨b, by simp [lanes, hb], ?_⟩
    simp [h, hp]

/-- If row `i` has no bit cell at `p`, changing it leaves `p` untouched. -/
private theorem image_away_from_row (l : Layout) (w : Nat) (xs : List Nat) {i : Nat}
    (hi : i < xs.length) (x p : Nat)
    (haway : ∀ b, b < w → l.bit xs.length w i b ≠ p) :
    (image l w (xs.set i x)).testBit p = (image l w xs).testBit p := by
  have hlen : (xs.set i x).length = xs.length := List.length_set ..
  rw [image_testBit, image_testBit]
  rw [hlen]
  have hrowAway : ∀ y : Nat,
      (lanes w).any (fun b => y.testBit b && decide (l.bit xs.length w i b = p)) = false := by
    intro y
    apply Bool.eq_false_iff.mpr
    intro h
    obtain ⟨b, hb, hval⟩ := List.any_eq_true.mp h
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hval
    exact haway b (by simpa [lanes] using hb) hval.2
  congr 1
  funext j
  by_cases hji : j = i
  · subst j
    rw [show (xs.set i x)[i]?.getD 0 = x by simp [List.getElem?_set, hi]]
    rw [hrowAway x, hrowAway (xs[i]?.getD 0)]
  · have hget : (xs.set i x)[j]?.getD 0 = xs[j]?.getD 0 := by
      simp [List.getElem?_set, hji, Ne.symm hji]
    simp [hget]

/-! ## The goal -/

theorem sound_MaskedPatch : TileTranspose.MaskedPatch.Goal := by
  intro w xs i x hi
  let n := xs.length
  have hlen : (xs.set i x).length = n := by simp [n]
  have hno : ∀ j k a c, j < n → k < n → a < w → c < w →
      layout.bit n w j a = layout.bit n w k c → j = k ∧ a = c := by
    intro j k a c hj hk ha hc h
    exact injective_bits n w j k a c hj hk ha hc h
  apply Nat.eq_of_testBit_eq
  intro p
  rw [Nat.testBit_or, Nat.testBit_xor, Nat.testBit_and]
  rw [rowMask_testBit, rowBits_testBit]
  by_cases hm : (lanes w).any (fun b => decide (layout.bit n w i b = p)) = true
  · obtain ⟨b0, hb0mem, hdec⟩ := List.any_eq_true.mp hm
    have hb0 : b0 < w := by simpa [lanes] using hb0mem
    have hp0 : layout.bit n w i b0 = p := by simpa using hdec
    have hIold : (image layout w xs).testBit p = (xs[i]?.getD 0).testBit b0 := by
      exact image_bit_at layout w xs hi hb0 hno hp0
    have hInew : (image layout w (xs.set i x)).testBit p = x.testBit b0 := by
      have hres := image_bit_at layout w (xs.set i x) (by simpa [hlen] using hi) hb0
        (by simpa [hlen] using hno) hp0
      simpa [List.getElem?_set, hi] using hres
    have hRnew : (lanes w).any
        (fun b => x.testBit b && decide (layout.bit n w i b = p)) = x.testBit b0 := by
      apply Bool.eq_iff_iff.mpr
      constructor
      · intro h
        obtain ⟨b, hbmem, hval⟩ := List.any_eq_true.mp h
        simp only [Bool.and_eq_true, decide_eq_true_eq] at hval
        rcases hval with ⟨hxb, heq⟩
        have hb : b < w := by simpa [lanes] using hbmem
        have hbb0 : b = b0 := (hno i i b b0 hi hi hb hb0 (heq.trans hp0.symm)).2
        simpa [hbb0] using hxb
      · intro h
        exact List.any_eq_true.mpr ⟨b0, hb0mem, by simp [h, hp0]⟩
    rw [hInew, hIold, hRnew, hm]
    cases (xs[i]?.getD 0).testBit b0 <;> cases x.testBit b0 <;> rfl
  · have hmf : (lanes w).any (fun b => decide (layout.bit n w i b = p)) = false := by
      cases h : (lanes w).any (fun b => decide (layout.bit n w i b = p)) <;> simp_all
    have hRnew : (lanes w).any
        (fun b => x.testBit b && decide (layout.bit n w i b = p)) = false := by
      apply Bool.eq_false_iff.mpr
      intro h
      obtain ⟨b, hbmem, hval⟩ := List.any_eq_true.mp h
      simp only [Bool.and_eq_true, decide_eq_true_eq] at hval
      have hmtrue : (lanes w).any (fun b => decide (layout.bit n w i b = p)) = true :=
        List.any_eq_true.mpr ⟨b, hbmem, by simp [hval.2]⟩
      rw [hmf] at hmtrue
      cases hmtrue
    have haway : ∀ b, b < w → layout.bit n w i b ≠ p := by
      intro b hb heq
      have : (lanes w).any (fun a => decide (layout.bit n w i a = p)) = true :=
        List.any_eq_true.mpr ⟨b, by simp [lanes, hb], by simp [heq]⟩
      rw [hmf] at this
      cases this
    have hIagree : (image layout w (xs.set i x)).testBit p =
        (image layout w xs).testBit p := by
      exact image_away_from_row layout w xs hi x p haway
    rw [hIagree, hmf, hRnew]
    simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_MaskedPatch
